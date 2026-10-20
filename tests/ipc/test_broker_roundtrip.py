"""Real-broker round-trip tests for IPCService.

Every other test in this package fakes the NATS client; these tests start an
actual ``nats-server`` subprocess via :class:`LocalNatsServer` and drive a
real :class:`IPCService` against it, using ``nats-py`` directly (from a
helper that runs coroutines on its own fresh event loop) to stand in for an
external client application.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time

import nats
import pytest

from lightfall_utils.ipc import IPCService, LocalNatsServer, TrustManager, TrustState, ok_reply

# No manual invoker workaround needed here: IPCService.start() now calls
# initialize_main_thread_invoker() itself (see lightfall_utils.threads) before
# spawning the NATS background thread, so these tests exercise the production
# fix for the invoker-thread-affinity bug rather than papering over it.


def _free_port() -> int:
    """Bind to port 0 to get an OS-assigned free port (never collides with 4222)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _nats_request_async(url: str, subject: str, data: dict, timeout: float = 2.0):
    nc = await nats.connect(url)
    try:
        msg = await nc.request(subject, json.dumps(data).encode(), timeout=timeout)
        return json.loads(msg.data.decode())
    finally:
        await nc.close()


def nats_request(url: str, subject: str, data: dict, timeout: float = 2.0) -> dict | None:
    """Send one request/reply as a plain ``nats-py`` client on a fresh event loop.

    Returns the decoded reply, or ``None`` on timeout/no-responders.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_nats_request_async(url, subject, data, timeout))
    except Exception:
        return None
    finally:
        loop.close()


@pytest.fixture(scope="module")
def broker_url():
    port = _free_port()
    server = LocalNatsServer(port=port, host="127.0.0.1")
    server.start(timeout_s=10.0)
    try:
        yield f"nats://127.0.0.1:{port}"
    finally:
        server.stop()


def _wait_until(predicate, qapp, timeout_s: float = 5.0) -> bool:
    """Poll *predicate*, pumping the Qt event loop so main-thread callbacks land."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        qapp.processEvents()
        time.sleep(0.02)
    return predicate()


@pytest.fixture
def service(broker_url, qapp):
    svc = IPCService(broker_url, topic_prefix="als.test")
    svc.start()
    assert _wait_until(lambda: svc.is_connected, qapp), "service failed to connect to test broker"
    yield svc
    svc.stop()


class TestAuthHandshake:
    def test_approved_app_gets_session_token(self, service, broker_url, qapp):
        trust = TrustManager()
        trust.approve("camphor")
        service.set_trust_manager(trust)

        def handle_auth(subject, data, reply):
            state = service.evaluate_trust(data["app_name"])
            resp = service.build_auth_response(
                approved=(state == TrustState.APPROVED), app_name=data["app_name"]
            )
            service.reply(reply, resp)

        service.register_action("auth.request", handle_auth, main_thread=False)

        result = nats_request(
            broker_url, "als.test.auth.request", {"app_name": "camphor", "version": "1.0"}
        )
        assert result is not None
        assert result["status"] == "approved"
        assert isinstance(result["session_token"], str) and result["session_token"]


class TestTrustedActionOnBareSubject:
    def test_bare_subject_is_denied(self, service, broker_url):
        seen = []
        service.register_action(
            "commands.run", lambda s, d, r: seen.append(d), trusted=True, main_thread=False
        )

        result = nats_request(broker_url, "als.test.commands.run", {"x": 1})
        assert result is not None
        assert result["status"] == "error"
        assert result["code"] == "denied"
        assert seen == []


class TestTrustedActionOnSessionChannel:
    def test_session_subject_runs_with_identity(self, service, broker_url):
        seen = []

        def handler(subject, data, reply):
            seen.append(data)
            service.reply(reply, ok_reply(echo=data.get("x")))

        service.register_action("commands.run", handler, trusted=True, main_thread=False)
        token = service.mint_session_channel("camphor")

        result = nats_request(
            broker_url, f"{service.priv_root}.session.{token}.commands.run", {"x": 42}
        )
        assert result is not None
        assert result["contract_version"] == 1
        assert result["echo"] == 42
        assert seen and seen[0]["_identity"]["app_name"] == "camphor"


class TestSessionTeardown:
    def test_after_teardown_channel_is_denied_or_times_out(self, service, broker_url):
        service.register_action(
            "commands.run2", lambda s, d, r: None, trusted=True, main_thread=False
        )
        token = service.mint_session_channel("camphor")
        service.teardown_session_channels(app_name="camphor")

        result = nats_request(
            broker_url, f"{service.priv_root}.session.{token}.commands.run2", {}, timeout=0.5
        )
        assert result is None or result.get("code") == "denied"


class TestPeerDiscovery:
    def test_two_instances_are_found(self, broker_url, qapp):
        # instance_id defaults to "{hostname}-{pid}", so two instances in this
        # same test process would otherwise collide; give them distinct ids,
        # as they'd have running as separate application processes.
        svc_a = IPCService(broker_url, topic_prefix="als.a")
        svc_a._instance_id = "host-a-1111"
        svc_a.display_name = "instance-a"
        svc_a.start()
        svc_b = IPCService(broker_url, topic_prefix="als.b")
        svc_b._instance_id = "host-b-2222"
        svc_b.display_name = "instance-b"
        svc_b.start()
        try:
            assert _wait_until(lambda: svc_a.is_connected, qapp)
            assert _wait_until(lambda: svc_b.is_connected, qapp)
            svc_a.register_meta_endpoints()
            svc_b.register_meta_endpoints()
            # register_meta_endpoints() schedules the "_lightfall.discover"
            # subscription onto each service's NATS loop asynchronously; give
            # both a moment to actually land before scattering the discovery
            # request, or replies get missed by a startup race.
            _wait_until(lambda: False, qapp, timeout_s=0.3)

            found: list[list[dict]] = []
            svc_a.discover_peers(found.append, timeout_ms=800)
            assert _wait_until(lambda: bool(found), qapp, timeout_s=5.0)

            peers = found[0]
            ids = {p["instance_id"] for p in peers}
            assert svc_a.instance_id in ids
            assert svc_b.instance_id in ids
            assert len(ids) == 2
        finally:
            svc_a.stop()
            svc_b.stop()
