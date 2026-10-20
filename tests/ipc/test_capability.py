"""Tests for lightfall_utils.ipc.capability (connection capability helpers)."""

from __future__ import annotations

import asyncio
import json
import re
import socket

import nats
import pytest

from lightfall_utils.ipc import LocalNatsServer
from lightfall_utils.ipc.capability import (
    VENDORED_FROM,
    PrivateNoResponders,
    PrivateRootUnavailable,
    derive_prefix,
    new_capability,
    new_token,
    private_auth_request,
    private_request,
)

GOOD_ROOT = "_PRIV." + "a" * 26


def test_vectors():
    assert derive_prefix("test-secret-0001") == "r5itqnjn2uhkfjylxqpf7fwjv7"
    assert derive_prefix("A" * 43) == "3dcidrblqoesmah3mp7u5jx4cx"


def test_vendored_from():
    assert VENDORED_FROM == "lightfall_utils.ipc.capability v2"


def test_capability_shape():
    cap = new_capability()
    assert len(cap.secret) == 43 and cap.prefix == derive_prefix(cap.secret)
    assert cap.inbox_prefix == f"_INBOX.{cap.prefix}"
    assert cap.priv_root == f"_PRIV.{cap.prefix}"
    assert cap.connect_kwargs() == {"token": cap.secret, "inbox_prefix": cap.inbox_prefix}


def test_capabilities_unique():
    assert len({new_capability().prefix for _ in range(200)}) == 200


def test_new_token():
    tok = new_token()
    assert len(tok) == 43
    assert tok != new_token()
    # URL-safe base64 alphabet, no "=" padding
    assert re.fullmatch(r"[A-Za-z0-9_-]+", tok)
    assert "=" not in tok


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def broker_url():
    port = _free_port()
    server = LocalNatsServer(port=port, host="127.0.0.1")
    server.start(timeout_s=10.0)
    try:
        yield f"nats://127.0.0.1:{port}"
    finally:
        server.stop()


def test_private_auth_request_roundtrip(broker_url):
    async def run():
        nc = await nats.connect(broker_url)
        try:

            async def actions(msg):
                await nc.publish(msg.reply, json.dumps({"priv_root": GOOD_ROOT}).encode())

            async def auth(msg):
                await nc.publish(msg.reply, msg.data)

            await nc.subscribe("t.meta.actions", cb=actions)
            await nc.subscribe(f"{GOOD_ROOT}.auth.request", cb=auth)
            await nc.flush()
            payload = {"a": 1, "b": ["x"]}
            assert await private_auth_request(nc, "t", payload) == payload
        finally:
            await nc.close()

    asyncio.run(run())


def test_private_auth_request_missing_priv_root(broker_url):
    async def run():
        nc = await nats.connect(broker_url)
        try:

            async def actions(msg):
                await nc.publish(msg.reply, json.dumps({"actions": []}).encode())

            await nc.subscribe("u.meta.actions", cb=actions)
            await nc.flush()
            with pytest.raises(PrivateRootUnavailable):
                await private_auth_request(nc, "u", {})
        finally:
            await nc.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "root",
    ["als.x", "_PRIV.x", "_PRIV." + "a" * 25, "_PRIV." + "A" * 26, "_PRIV." + "a" * 26 + ".x",
     "_PRIV." + "a" * 26 + "\n", "_INBOX." + "a" * 26, 7],
)  # fmt: skip
def test_private_auth_request_rejects_bad_priv_root(broker_url, root):
    async def run():
        nc = await nats.connect(broker_url)
        try:
            hits = []

            async def actions(msg):
                await nc.publish(msg.reply, json.dumps({"priv_root": root}).encode())

            async def anything(msg):
                hits.append(msg.subject)

            await nc.subscribe("v.meta.actions", cb=actions)
            await nc.subscribe("als.>", cb=anything)
            await nc.flush()
            with pytest.raises(PrivateRootUnavailable):
                await private_auth_request(nc, "v", {"secret": 1}, timeout=1)
            await nc.flush()
            assert hits == []
        finally:
            await nc.close()

    asyncio.run(run())


def test_private_request_uses_unguessable_reply(broker_url):
    async def run():
        cap = new_capability()
        nc = await nats.connect(broker_url, **cap.connect_kwargs())
        try:
            replies = []

            async def echo(msg):
                replies.append(msg.reply)
                await nc.publish(msg.reply, msg.data)

            await nc.subscribe("w.echo", cb=echo)
            await nc.flush()
            assert await private_request(nc, "w.echo", {"n": 1}) == {"n": 1}
            assert await private_request(nc, "w.echo", {"n": 2}, inbox_prefix=cap.inbox_prefix) == {
                "n": 2
            }
            for reply in replies:
                head, token = reply.rsplit(".", 1)
                assert head == cap.inbox_prefix
                assert len(token) == 43
            assert replies[0] != replies[1]
        finally:
            await nc.close()

    asyncio.run(run())


def test_private_request_timeout(broker_url):
    async def run():
        nc = await nats.connect(broker_url, **new_capability().connect_kwargs())
        try:

            async def silent(msg):
                pass

            await nc.subscribe("x.silent", cb=silent)
            await nc.flush()
            with pytest.raises(TimeoutError):
                await private_request(nc, "x.silent", {}, timeout=0.3)
        finally:
            await nc.close()

    asyncio.run(run())


def test_private_request_no_responders(broker_url):
    async def run():
        nc = await nats.connect(broker_url, **new_capability().connect_kwargs())
        try:
            with pytest.raises(PrivateNoResponders):
                await private_request(nc, "y.nobody", {}, timeout=2)
        finally:
            await nc.close()

    asyncio.run(run())


def test_private_request_needs_inbox_prefix():
    class Bare:
        pass

    with pytest.raises(ValueError):
        asyncio.run(private_request(Bare(), "z", {}))
