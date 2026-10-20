import socket

import pytest

from lightfall_utils.ipc import LocalNatsServer


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
