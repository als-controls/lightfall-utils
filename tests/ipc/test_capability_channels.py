"""Unit tests for IPCService capability-channel trust plumbing.

Uses the same fake-IPCService construction pattern as tests/ipc/test_integration.py:
IPCService.__new__ + manual attribute injection, capturing outbound replies.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading

import nats
import pytest

from lightfall_utils.ipc import LocalNatsServer
from lightfall_utils.ipc.capability import PrivateNoResponders, new_capability, private_request
from lightfall_utils.ipc.service import IPCService


@pytest.fixture(scope="module")
def broker_url():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = LocalNatsServer(port=port, host="127.0.0.1")
    server.start(timeout_s=10.0)
    try:
        yield f"nats://127.0.0.1:{port}"
    finally:
        server.stop()


def _make_ipc(prefix: str = "als.test") -> tuple[IPCService, list[tuple[str, dict]]]:
    """Build a disconnected IPCService whose publish() captures messages."""
    ipc = IPCService.__new__(IPCService)
    ipc._topic_prefix = prefix
    ipc._capability = new_capability()
    ipc._subscriptions = {}
    ipc._action_catalog = {}
    ipc._event_catalog = {}
    ipc._trusted_actions = {}
    ipc._session_channels = {}
    ipc._trust = None
    ipc._instance_id = "test-1"
    ipc._display_name = None
    ipc._loop = None
    ipc._nc = None
    ipc._connected = False
    ipc._connected_lock = threading.Lock()

    sent: list[tuple[str, dict]] = []
    ipc.publish = lambda subject, data: sent.append((subject, data))  # type: ignore[method-assign]
    return ipc, sent


def _call_subscription(ipc: IPCService, subject: str, data: dict, reply: str) -> None:
    """Invoke the callback registered for *subject* directly (as _make_handler would)."""
    ipc._subscriptions[subject].callback(subject, data, reply)


class TestMint:
    def test_mint_returns_unguessable_token_and_subscribes_wildcard(self):
        ipc, _ = _make_ipc()
        token = ipc.mint_session_channel("pystxm")
        assert len(token) >= 22  # token_urlsafe(32) -> 43 chars; 128-bit floor
        assert f"{ipc.priv_root}.session.{token}.>" in ipc._subscriptions

    def test_mint_twice_gives_distinct_tokens(self):
        ipc, _ = _make_ipc()
        assert ipc.mint_session_channel("a") != ipc.mint_session_channel("a")

    def test_reauth_tears_down_old_channel_for_same_app(self):
        """Re-auth (second mint for the same app) must leave exactly one live
        channel: the old token stops working and the new one takes over."""
        ipc, sent = _make_ipc()
        seen: list[dict] = []
        ipc.register_action(
            "commands.t.d", lambda s, d, r: seen.append(d), trusted=True, main_thread=False
        )

        old_token = ipc.mint_session_channel("pystxm")
        new_token = ipc.mint_session_channel("pystxm")

        assert ipc.session_channel_count == 1
        assert old_token != new_token

        old_wildcard = f"{ipc.priv_root}.session.{old_token}.>"
        assert old_wildcard not in ipc._subscriptions

        # Old token routes to denied (its subscription is gone; simulate a
        # stray in-flight message by calling the router logic directly via
        # _route_session_message, which looks the token up in the dict).
        ipc._route_session_message(
            f"{ipc.priv_root}.session.{old_token}.commands.t.d", {}, "_INBOX.old"
        )
        assert sent[-1][1]["status"] == "error"
        assert sent[-1][1]["code"] == "denied"

        # New token still works.
        new_wildcard = f"{ipc.priv_root}.session.{new_token}.>"
        router = ipc._subscriptions[new_wildcard].callback
        router(f"{ipc.priv_root}.session.{new_token}.commands.t.d", {"x": 1}, "_INBOX.new")
        assert seen == [{"x": 1, "_identity": {"app_name": "pystxm", "session_token": new_token}}]

    def test_reauth_does_not_affect_other_apps_channel(self):
        ipc, _ = _make_ipc()
        other_token = ipc.mint_session_channel("other_app")
        ipc.mint_session_channel("pystxm")
        ipc.mint_session_channel("pystxm")

        other_wildcard = f"{ipc.priv_root}.session.{other_token}.>"
        assert other_wildcard in ipc._subscriptions
        assert ipc.session_channel_count == 2


class TestRouting:
    def test_trusted_action_reachable_via_channel_with_identity(self):
        ipc, sent = _make_ipc()
        calls: list[tuple[str, dict]] = []
        ipc.register_action(
            "commands.thing.do",
            lambda s, d, r: calls.append((s, d)),
            trusted=True,
            main_thread=False,
        )
        token = ipc.mint_session_channel("pystxm")
        wildcard = f"{ipc.priv_root}.session.{token}.>"
        assert wildcard in ipc._subscriptions
        # Deliver a request the way _make_handler would: the wildcard
        # subscription's callback invoked with the REAL message subject.
        ipc._subscriptions[wildcard].callback(
            f"{ipc.priv_root}.session.{token}.commands.thing.do", {"x": 1}, "_INBOX.1"
        )
        assert calls == [
            (
                "commands.thing.do",
                {"x": 1, "_identity": {"app_name": "pystxm", "session_token": token}},
            )
        ]

    def test_router_dispatches_by_real_subject(self):
        ipc, sent = _make_ipc()
        seen: list[tuple[str, dict]] = []
        ipc.register_action(
            "commands.thing.do",
            lambda s, d, r: seen.append((s, d)),
            trusted=True,
            main_thread=False,
        )
        token = ipc.mint_session_channel("pystxm")
        router = ipc._subscriptions[f"{ipc.priv_root}.session.{token}.>"].callback
        router(f"{ipc.priv_root}.session.{token}.commands.thing.do", {"x": 1}, "_INBOX.1")
        assert len(seen) == 1
        subject, data = seen[0]
        assert subject == "commands.thing.do"
        assert data["x"] == 1
        assert data["_identity"] == {"app_name": "pystxm", "session_token": token}

    def test_client_supplied_identity_is_stripped(self):
        ipc, _ = _make_ipc()
        seen: list[dict] = []
        ipc.register_action(
            "commands.t.d", lambda s, d, r: seen.append(d), trusted=True, main_thread=False
        )
        token = ipc.mint_session_channel("appA")
        router = ipc._subscriptions[f"{ipc.priv_root}.session.{token}.>"].callback
        router(
            f"{ipc.priv_root}.session.{token}.commands.t.d",
            {"_identity": {"app_name": "fake"}},
            "_INBOX.1",
        )
        assert seen[0]["_identity"]["app_name"] == "appA"

    def test_unknown_suffix_on_channel_gets_unknown_error(self):
        ipc, sent = _make_ipc()
        token = ipc.mint_session_channel("appA")
        router = ipc._subscriptions[f"{ipc.priv_root}.session.{token}.>"].callback
        router(f"{ipc.priv_root}.session.{token}.commands.nope", {}, "_INBOX.9")
        assert sent[-1][0] == "_INBOX.9"
        assert sent[-1][1]["status"] == "error"
        assert sent[-1][1]["code"] == "unknown"

    def test_untrusted_action_not_reachable_via_channel(self):
        ipc, sent = _make_ipc()
        ipc.register_action("meta.actions", lambda s, d, r: None)  # untrusted
        token = ipc.mint_session_channel("appA")
        router = ipc._subscriptions[f"{ipc.priv_root}.session.{token}.>"].callback
        router(f"{ipc.priv_root}.session.{token}.meta.actions", {}, "_INBOX.2")
        assert sent[-1][1]["code"] == "unknown"

    def test_version_mismatch_rejected(self):
        ipc, sent = _make_ipc()
        ipc.register_action("commands.t.d", lambda s, d, r: None, trusted=True, main_thread=False)
        token = ipc.mint_session_channel("appA")
        router = ipc._subscriptions[f"{ipc.priv_root}.session.{token}.>"].callback
        router(f"{ipc.priv_root}.session.{token}.commands.t.d", {"contract_version": 2}, "_INBOX.3")
        assert sent[-1][1]["code"] == "version_mismatch"


class TestBareSubjectRejection:
    def test_bare_commands_subject_replies_denied(self):
        ipc, sent = _make_ipc()
        ipc.register_action("commands.plan.run", lambda s, d, r: None, trusted=True)
        _call_subscription(ipc, "als.test.commands.plan.run", {"plan_name": "x"}, "_INBOX.4")
        assert sent[-1][0] == "_INBOX.4"
        assert sent[-1][1]["status"] == "error"
        assert sent[-1][1]["code"] == "denied"

    def test_trusted_action_still_in_catalog(self):
        ipc, _ = _make_ipc()
        ipc.register_action("commands.plan.run", lambda s, d, r: None, trusted=True)
        assert any(a["subject"] == "commands.plan.run" for a in ipc.list_actions())

    def test_unregister_trusted_action_removes_both(self):
        ipc, _ = _make_ipc()
        handle = ipc.register_action("commands.plan.run", lambda s, d, r: None, trusted=True)
        handle.unregister()
        assert "commands.plan.run" not in ipc._trusted_actions
        assert "als.test.commands.plan.run" not in ipc._subscriptions


class TestTeardown:
    def test_teardown_all(self):
        ipc, _ = _make_ipc()
        t1 = ipc.mint_session_channel("a")
        t2 = ipc.mint_session_channel("b")
        ipc.teardown_session_channels()
        assert ipc.session_channel_count == 0
        assert f"{ipc.priv_root}.session.{t1}.>" not in ipc._subscriptions
        assert f"{ipc.priv_root}.session.{t2}.>" not in ipc._subscriptions

    def test_teardown_single_app(self):
        ipc, _ = _make_ipc()
        ta = ipc.mint_session_channel("a")
        tb = ipc.mint_session_channel("b")
        ipc.teardown_session_channels("a")
        assert ipc.session_channel_count == 1
        assert f"{ipc.priv_root}.session.{ta}.>" not in ipc._subscriptions
        assert f"{ipc.priv_root}.session.{tb}.>" in ipc._subscriptions

    def test_dead_token_routes_nowhere(self):
        ipc, sent = _make_ipc()
        seen = []
        ipc.register_action(
            "commands.t.d", lambda s, d, r: seen.append(d), trusted=True, main_thread=False
        )
        token = ipc.mint_session_channel("a")
        router = ipc._subscriptions[f"{ipc.priv_root}.session.{token}.>"].callback
        ipc.teardown_session_channels()
        router(f"{ipc.priv_root}.session.{token}.commands.t.d", {}, "_INBOX.5")
        assert seen == []
        assert sent[-1][1]["code"] == "denied"


class TestAuthResponse:
    def test_approved_response_carries_session_token_and_version(self):
        ipc, _ = _make_ipc()

        resp = ipc.build_auth_response(
            approved=True, extra={"tiled_url": "http://t"}, app_name="pystxm"
        )
        assert resp["status"] == "approved"
        assert resp["contract_version"] == 1
        assert resp["tiled_url"] == "http://t"
        token = resp["session_token"]
        assert resp["session_subject"] == f"{ipc.priv_root}.session.{token}"
        assert f"{ipc.priv_root}.session.{token}.>" in ipc._subscriptions

    def test_denied_response_has_no_token(self):
        ipc, _ = _make_ipc()
        resp = ipc.build_auth_response(approved=False, reason="denied")
        assert resp == {"status": "denied", "reason": "denied", "contract_version": 1}


class TestPrivateRoot:
    def test_meta_actions_advertises_priv_root(self):
        ipc, sent = _make_ipc()
        ipc._handle_meta_actions("als.test.meta.actions", {}, "_INBOX.m")
        assert sent[-1][1]["priv_root"] == ipc.priv_root
        assert ipc.priv_root.startswith("_PRIV.")

    def test_meta_events_and_discover_advertise_priv_root(self):
        ipc, sent = _make_ipc()
        ipc._handle_meta_events("s", {}, "_INBOX.e")
        ipc._handle_discover("s", {}, "_INBOX.d")
        by_subject = dict(sent)
        assert by_subject["_INBOX.e"]["priv_root"] == ipc.priv_root
        assert by_subject["_INBOX.d"]["priv_root"] == ipc.priv_root

    def test_private_action_only_on_priv_root(self):
        ipc, _ = _make_ipc()
        ipc.register_action("auth.request", lambda s, d, r: None, private=True)
        assert ipc.topic("auth.request") not in ipc._subscriptions
        assert f"{ipc.priv_root}.auth.request" in ipc._subscriptions

    def test_private_action_request_path_only_answers_on_priv_root(self, broker_url):
        """Broker roundtrip: a request to the public topic(suffix) has no responder;
        the same request to {priv_root}.suffix is answered."""
        ipc, _ = _make_ipc()
        ipc.register_action("auth.request", lambda s, d, r: None, private=True)
        public = ipc.topic("auth.request")
        private = f"{ipc.priv_root}.auth.request"
        # Subscribe on the broker to exactly the subjects the service registered.
        assert public not in ipc._subscriptions and private in ipc._subscriptions

        async def run():
            cap = new_capability()
            nc = await nats.connect(broker_url, **cap.connect_kwargs())
            try:

                async def answer(msg):
                    await nc.publish(msg.reply, json.dumps({"ok": True}).encode())

                for subject in ipc._subscriptions:
                    await nc.subscribe(subject, cb=answer)
                await nc.flush()
                with pytest.raises(PrivateNoResponders):
                    await private_request(nc, public, {}, timeout=2)
                assert await private_request(nc, private, {}, timeout=2) == {"ok": True}
            finally:
                await nc.close()

        asyncio.run(run())

    def test_private_action_flagged_in_catalog(self):
        ipc, _ = _make_ipc()
        ipc.register_action("auth.request", lambda s, d, r: None, private=True)
        ipc.register_action("ping", lambda s, d, r: None)
        entries = {e["subject"]: e for e in ipc.list_actions()}
        assert entries["auth.request"]["private"] is True
        assert "private" not in entries["ping"]

    def test_private_and_trusted_exclusive(self):
        ipc, _ = _make_ipc()
        with pytest.raises(ValueError):
            ipc.register_action("x", lambda s, d, r: None, private=True, trusted=True)
