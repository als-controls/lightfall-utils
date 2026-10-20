"""Qt-free asyncio client side of the services pattern.

A :class:`ServiceClient` follows one service at ``{scope}.svc.{name}``: it listens on the
heartbeat, fetches ``meta.info``, checks ``svc_protocol`` and the contract, performs the
``auth.request`` handshake on the advertised ``priv_root`` and then keeps the session leased
with ``rpc.session.ping``.  Every request to a ``_PRIV`` subject goes through
:meth:`NatsCore.request_private`.

Health (``health``): ``unknown`` -> ``starting`` -> ``up``; heartbeats drive ``degraded``
(one missed beat) and ``down`` (three missed beats); a beat restores ``up`` without a new
handshake.  ``incompatible`` and ``unauthorized`` are terminal for the current instance; a
new ``instance_id`` on the heartbeat starts over (fresh ``meta.info``, fresh delivery token,
new handshake) and fires ``on_restarted`` if a previous instance was known.

Timing uses the injectable ``clock``; a background loop calls :meth:`tick` every
``POLL_S`` real seconds, and tests may call :meth:`tick` directly after advancing a fake
clock.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from typing import Any

from nats.errors import NoRespondersError

from lightfall_utils.ipc.capability import _PRIV_ROOT_RE, PrivateNoResponders, new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = P.DEFAULT_TIMEOUT_S

# Heartbeat grace: a beat counts as missed half an interval after it was due.
_DEGRADED_AFTER_S = 1.5 * P.HEARTBEAT_S  # 1 missed beat
_DOWN_AFTER_S = 3.5 * P.HEARTBEAT_S  # 3 missed beats
_STREAM_PREFIX = "stream."


class ServiceCallError(RuntimeError):
    """A service call that must return a value (e.g. ``stream.open``) got an error reply."""

    def __init__(self, reply: dict) -> None:
        super().__init__(f"{reply.get('code')}: {reply.get('message')}")
        self.reply = reply


def _bad_token(tok: Any) -> bool:
    return not isinstance(tok, str) or not tok or any(c in tok for c in ".*> \t\r\n")


class ServiceClient:
    POLL_S = 0.25

    def __init__(
        self,
        descriptor: ServiceDescriptor,
        core: NatsCore,
        *,
        scope: str,
        app_name: str,
        app_version: str,
        user: str | None = None,
        session_token: str | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.descriptor = descriptor
        self._core = core
        self.scope = scope
        self.app_name = app_name
        self.app_version = app_version
        self.user = user
        self._session_token_in = session_token
        self._clock = clock
        self._root = P.root(scope, descriptor.name)

        self.health = "unknown"
        self.info: dict | None = None
        self.last_error = ""

        self._state_cbs: list[Callable[[str], Any]] = []
        self._restart_cbs: list[Callable[[], Any]] = []
        self._event_cbs: dict[str, list[Callable[[dict], Any]]] = {}
        self._stream_cbs: list[Callable[[str, int, bytes, bool], Any]] = []

        self._instance_id: str | None = None
        self._retired: set[str] = set()
        self._session_subject: str | None = None
        self._delivery_root: str | None = None
        self._delivery_sub: Any = None
        self._hb_sub: Any = None
        self._streams: dict[str, str] = {}  # stream_id -> kind
        self._last_beat = 0.0
        self._last_ping = 0.0
        self._ping_task: asyncio.Task | None = None
        self._connect_task: asyncio.Task | None = None
        self._connect_lock = asyncio.Lock()
        self._loop_task: asyncio.Task | None = None
        self._closed = False

    # -- callbacks ----------------------------------------------------------------------

    def on_state(self, cb: Callable[[str], Any]) -> None:
        self._state_cbs.append(cb)

    def on_restarted(self, cb: Callable[[], Any]) -> None:
        self._restart_cbs.append(cb)

    def on_event(self, name: str, cb: Callable[[dict], Any]) -> None:
        self._event_cbs.setdefault(name, []).append(cb)

    def on_stream(self, cb: Callable[[str, int, bytes, bool], Any]) -> None:
        self._stream_cbs.append(cb)

    @staticmethod
    def _fire(cbs: list, *args: Any) -> None:
        for cb in list(cbs):
            try:
                cb(*args)
            except Exception:
                log.exception("ServiceClient callback failed")

    def _set_health(self, health: str, error: str | None = None) -> None:
        if error is not None:
            self.last_error = error
        if health != self.health:
            self.health = health
            self._fire(self._state_cbs, health)

    # -- properties ---------------------------------------------------------------------

    @property
    def root(self) -> str:
        return self._root

    @property
    def session_subject(self) -> str | None:
        return self._session_subject

    @property
    def delivery_root(self) -> str | None:
        return self._delivery_root

    @property
    def connected(self) -> bool:
        return self._session_subject is not None and self.health in ("up", "degraded")

    # -- lifecycle ----------------------------------------------------------------------

    async def start(self) -> None:
        """Subscribe to the heartbeat, connect (meta.info, checks, handshake), start the loop.

        Never raises for service-side problems; they are reported through ``health`` and
        ``last_error``.  If the service is absent, the first heartbeat triggers a connect.
        """
        if self._loop_task is not None:
            raise RuntimeError("ServiceClient already started")
        self._closed = False
        self._set_health("starting")
        self._hb_sub = await self._core.subscribe(P.heartbeat(self._root), self._on_beat)
        await self._core.flush()
        await self._connect(restarted=False)
        self._loop_task = asyncio.create_task(self._monitor_loop())

    async def close(self) -> None:
        """``rpc.session.close`` (best effort), then unsubscribe and stop background work."""
        if self._closed:
            return
        self._closed = True
        tasks = [t for t in (self._loop_task, self._ping_task, self._connect_task) if t]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._loop_task = self._ping_task = self._connect_task = None
        if self._session_subject is not None:
            try:
                await self._core.request_private(
                    P.rpc_subject(self._session_subject, "session.close"), {}, DEFAULT_TIMEOUT_S
                )
            except Exception as e:
                log.debug("session.close failed: %s", e)
        self._session_subject = None
        for sub in (self._delivery_sub, self._hb_sub):
            if sub is not None:
                try:
                    await sub.unsubscribe()
                except Exception:
                    log.debug("unsubscribe failed", exc_info=True)
        self._delivery_sub = self._hb_sub = None
        self._delivery_root = None
        self._streams.clear()

    async def _connect(self, *, restarted: bool) -> None:
        async with self._connect_lock:
            if self._closed:
                return
            await self._do_connect(restarted)

    async def _drop_session(self) -> None:
        self._session_subject = None
        self._streams.clear()
        sub, self._delivery_sub = self._delivery_sub, None
        self._delivery_root = None
        if sub is not None:
            try:
                await sub.unsubscribe()
            except Exception:
                log.debug("unsubscribe failed", exc_info=True)

    async def _do_connect(self, restarted: bool) -> None:
        await self._drop_session()
        try:
            info = await self._core.request_public(P.meta_info(self._root), {}, DEFAULT_TIMEOUT_S)
        except (TimeoutError, NoRespondersError) as e:
            self._set_health("down", f"meta.info: {type(e).__name__}")
            return
        except (ValueError, UnicodeDecodeError) as e:
            self._set_health("down", f"meta.info: bad reply ({e})")
            return
        if not isinstance(info, dict):
            self._set_health("down", "meta.info: reply is not an object")
            return
        old = self._instance_id
        iid = info.get("instance_id")
        if old is not None and old != iid:
            self._retired.add(old)
        self._instance_id = iid if isinstance(iid, str) else None
        self.info = info
        self._last_beat = self._clock()

        proto, contract = info.get("svc_protocol"), info.get("contract")
        if proto != P.SVC_PROTOCOL or isinstance(proto, bool):
            self._set_health("incompatible", f"svc_protocol {proto!r} != {P.SVC_PROTOCOL}")
            return
        if not isinstance(contract, str) or not P.contract_ok(contract, self.descriptor.contract):
            self._set_health(
                "incompatible",
                f"contract {contract!r} does not satisfy {self.descriptor.contract}",
            )
            return
        priv_root = info.get("priv_root")
        if not isinstance(priv_root, str) or not _PRIV_ROOT_RE.fullmatch(priv_root):
            self._set_health("unauthorized", "bad priv_root")
            return

        delivery_token = new_token()
        delivery_root = P.delivery_root(self._core.priv_root, delivery_token)
        # Subscribe before the handshake so no early event is lost.
        self._delivery_sub = await self._core.subscribe(f"{delivery_root}.>", self._on_delivery)
        await self._core.flush()
        self._delivery_root = delivery_root
        req: dict[str, Any] = {
            "app_name": self.app_name,
            "app_version": self.app_version,
            "delivery_token": delivery_token,
            "client_priv_root": self._core.priv_root,
        }
        if self.user is not None:
            req["user"] = self.user
        if self._session_token_in is not None:
            req["session_token"] = self._session_token_in
        try:
            reply = await self._core.request_private(
                P.auth_subject(priv_root), req, DEFAULT_TIMEOUT_S
            )
        except (TimeoutError, PrivateNoResponders) as e:
            await self._drop_session()
            self._set_health("down", f"auth.request: {type(e).__name__}")
            return
        except (ValueError, UnicodeDecodeError) as e:
            await self._drop_session()
            self._set_health("down", f"auth.request: bad reply ({e})")
            return
        if not isinstance(reply, dict) or reply.get("status") != "approved":
            await self._drop_session()
            msg = reply.get("message", "") if isinstance(reply, dict) else ""
            self._set_health("unauthorized", f"handshake denied: {msg}".rstrip(": "))
            return
        subj = reply.get("session_subject")
        if not isinstance(subj, str) or not subj.startswith(f"{priv_root}.session."):
            await self._drop_session()
            self._set_health("unauthorized", "handshake reply has a bad session_subject")
            return
        cv = reply.get("contract_version")
        if not isinstance(cv, str) or not P.contract_ok(cv, self.descriptor.contract):
            await self._drop_session()
            self._set_health("incompatible", f"contract_version {cv!r} in handshake reply")
            return
        self._session_subject = subj
        self._last_ping = self._clock()
        self._last_beat = self._clock()
        self.last_error = ""
        self._set_health("up")
        if restarted:
            self._fire(self._restart_cbs)

    # -- heartbeat / monitor ------------------------------------------------------------

    async def _on_beat(self, msg: Any) -> None:
        try:
            beat = json.loads(msg.data)
        except (ValueError, UnicodeDecodeError):
            return
        if not isinstance(beat, dict) or self._closed:
            return
        iid = beat.get("instance_id")
        if not isinstance(iid, str) or iid in self._retired:
            return
        stopping = beat.get("state") == "stopping"
        if iid == self._instance_id:
            self._last_beat = self._clock()
            if self.info is not None:
                self.info = {**self.info, **beat}
            if stopping:
                self._set_health("down", "service stopping")
                return
            if self.health in ("degraded", "down") and self._session_subject is not None:
                self._set_health("up")
            return
        if stopping or self._connect_task is not None:
            return
        restarted = self._instance_id is not None
        self._connect_task = asyncio.create_task(self._reconnect(restarted))

    async def _reconnect(self, restarted: bool) -> None:
        try:
            if self.health not in ("unknown", "starting"):
                self._set_health("starting")
            await self._connect(restarted=restarted)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("reconnect failed")
            self._set_health("down", f"reconnect failed: {e}")
        finally:
            self._connect_task = None

    async def _monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(self.POLL_S)
            try:
                self._tick_nowait()
            except Exception:
                log.exception("ServiceClient tick failed")

    def _tick_nowait(self) -> None:
        if self._closed or self._connect_task is not None or self._session_subject is None:
            return
        now = self._clock()
        gap = now - self._last_beat
        if self.health in ("up", "degraded"):
            if gap > _DOWN_AFTER_S:
                self._set_health("down", "heartbeat lost")
            elif gap > _DEGRADED_AFTER_S and self.health == "up":
                self._set_health("degraded", "heartbeat late")
        if (
            self.health in ("up", "degraded")
            and now - self._last_ping >= P.PING_S
            and (self._ping_task is None or self._ping_task.done())
        ):
            self._last_ping = now
            self._ping_task = asyncio.create_task(self._ping())

    async def tick(self) -> None:
        """Evaluate heartbeat health and send a lease ping if one is due; await that ping."""
        self._tick_nowait()
        t = self._ping_task
        if t is not None and not t.done():
            await asyncio.gather(asyncio.shield(t), return_exceptions=True)

    async def _ping(self) -> None:
        subj = self._session_subject
        if subj is None:
            return
        reply = await self.call("session.ping", {})
        if reply.get("status") == "ok" or self._closed or self._session_subject != subj:
            return
        if reply.get("code") == "timeout" and self.health in ("up", "degraded"):
            # Heartbeats are fresh but the session does not answer: the lease was lost.
            log.info("session ping timed out; re-handshaking")
            if self._connect_task is None:
                self._connect_task = asyncio.create_task(self._reconnect(False))

    # -- delivery -----------------------------------------------------------------------

    async def _on_delivery(self, msg: Any) -> None:
        dr = self._delivery_root
        if dr is None or not msg.subject.startswith(dr + "."):
            return
        suffix = msg.subject[len(dr) + 1 :]
        if suffix.startswith(_STREAM_PREFIX):
            sid = suffix[len(_STREAM_PREFIX) :]
            headers = msg.headers or {}
            try:
                seq = int(headers.get(P.HDR_SEQ, ""))
            except ValueError:
                log.warning("delivery chunk without a valid %s dropped", P.HDR_SEQ)
                return
            final = headers.get(P.HDR_FINAL) == "1"
            self._fire(self._stream_cbs, sid, seq, bytes(msg.data), final)
            return
        try:
            ev = json.loads(msg.data)
        except (ValueError, UnicodeDecodeError):
            log.warning("non-JSON delivery event %r dropped", suffix)
            return
        if not isinstance(ev, dict):
            return
        self._fire(self._event_cbs.get(suffix, []), ev)

    # -- commands -----------------------------------------------------------------------

    def _contract(self) -> str:
        c = (self.info or {}).get("contract")
        return c if isinstance(c, str) else self.descriptor.contract

    def _error(self, code: str, message: str) -> dict:
        return P.svc_error(self._contract(), code, message)

    async def call(self, action: str, payload: dict, timeout: float = DEFAULT_TIMEOUT_S) -> dict:
        """Send ``rpc.<action>`` on the session; always via ``request_private``.

        Returns the reply dict, or an ``svc_error``-style dict on failure (``timeout`` on
        timeouts; no request is sent without an established session).
        """
        if not isinstance(action, str) or any(_bad_token(part) for part in action.split(".")):
            return self._error("bad_request", f"invalid action {action!r}")
        subj = self._session_subject
        if subj is None:
            code = {"incompatible": "incompatible", "unauthorized": "denied"}.get(
                self.health, "unknown"
            )
            return self._error(code, f"no session (health={self.health})")
        try:
            reply = await self._core.request_private(P.rpc_subject(subj, action), payload, timeout)
        except TimeoutError:
            return self._error("timeout", f"{action} timed out after {timeout}s")
        except PrivateNoResponders:
            if self._session_subject == subj:
                self._set_health("down", f"no responders for {action}")
            return self._error("unknown", f"no responders for {action}")
        except (ValueError, UnicodeDecodeError) as e:
            return self._error("bad_request", f"bad reply to {action}: {e}")
        if not isinstance(reply, dict):
            return self._error("bad_request", f"reply to {action} is not an object")
        return reply

    async def open_stream(self, kind: str, params: dict | None = None) -> str:
        reply = await self.call("stream.open", {"kind": kind, "params": params or {}})
        sid = reply.get("stream_id")
        if reply.get("status") != "ok" or _bad_token(sid):
            raise ServiceCallError(reply)
        self._streams[sid] = kind
        return sid

    async def send_chunk(self, stream_id: str, seq: int, data: bytes, final: bool = False) -> None:
        subj = self._session_subject
        if subj is None:
            raise RuntimeError(f"no session (health={self.health})")
        if _bad_token(stream_id):
            raise ValueError(f"invalid stream_id {stream_id!r}")
        headers = {
            P.HDR_SEQ: str(int(seq)),
            P.HDR_KIND: self._streams.get(stream_id, ""),
            P.HDR_FINAL: "1" if final else "0",
        }
        await self._core.publish(P.stream_subject(subj, stream_id), bytes(data), headers)
        if final:
            self._streams.pop(stream_id, None)

    async def close_stream(self, stream_id: str) -> None:
        self._streams.pop(stream_id, None)
        reply = await self.call("stream.close", {"stream_id": stream_id})
        if reply.get("status") != "ok":
            raise ServiceCallError(reply)
