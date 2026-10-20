"""Tests for lightfall_utils.ipc.capability (connection capability helpers)."""

from __future__ import annotations

import asyncio
import json
import socket

import nats
import pytest

from lightfall_utils.ipc import LocalNatsServer
from lightfall_utils.ipc.capability import (
    PrivateRootUnavailable,
    derive_prefix,
    new_capability,
    new_token,
    private_auth_request,
)


def test_vectors():
    assert derive_prefix("test-secret-0001") == "r5itqnjn2uhkfjylxqpf7fwjv7"
    assert derive_prefix("A" * 43) == "3dcidrblqoesmah3mp7u5jx4cx"


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
                await nc.publish(msg.reply, json.dumps({"priv_root": "_PRIV.x"}).encode())

            async def auth(msg):
                await nc.publish(msg.reply, msg.data)

            await nc.subscribe("t.meta.actions", cb=actions)
            await nc.subscribe("_PRIV.x.auth.request", cb=auth)
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
