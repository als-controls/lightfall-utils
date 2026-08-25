"""Integration tests: PV bridge against a live caproto soft IOC."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from lightfall_utils.ca import PV, SharedContext

_LOCAL_ENV = {
    "EPICS_CA_ADDR_LIST": "127.0.0.1",
    "EPICS_CA_AUTO_ADDR_LIST": "NO",
}


@pytest.fixture()
def softioc(monkeypatch):
    for key, value in _LOCAL_ENV.items():
        monkeypatch.setenv(key, value)
    prefix = f"lfutest{os.getpid()}:"
    proc = subprocess.Popen(
        [sys.executable, str(Path(__file__).parent / "ca_ioc.py"), prefix],
        env={**os.environ, **_LOCAL_ENV},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Wait until the IOC answers a synchronous read (up to ~10 s).
    from caproto.sync.client import read

    deadline = time.monotonic() + 10.0
    while True:
        try:
            read(prefix + "value", timeout=1.0)
            break
        except Exception:
            if time.monotonic() > deadline:
                proc.terminate()
                pytest.fail("soft IOC did not come up within 10 s")
    yield prefix
    # Teardown: drop the client context first, then the server process.
    try:
        instance = SharedContext._instance
        if instance is not None and instance._context is not None:
            instance.context.disconnect(wait=False)
    except Exception:
        pass
    SharedContext.reset()
    proc.terminate()
    proc.wait(timeout=5)


def test_pv_connects_and_reads_initial_value(qtbot, softioc):
    pv = PV(softioc + "value")
    with qtbot.waitSignal(pv.connection_changed, timeout=10000) as connected:
        pv.connect_pv()
    assert connected.args == [True]
    assert pv.connected
    qtbot.waitUntil(lambda: pv.value is not None, timeout=10000)
    assert float(pv.value) == 42.0


def test_pv_put_roundtrip(qtbot, softioc):
    pv = PV(softioc + "value")
    with qtbot.waitSignal(pv.connection_changed, timeout=10000):
        pv.connect_pv()
    # Drain the subscription's initial monitor event (the server sends the
    # current value, 42.0, on every new subscription; it is queued behind
    # Qt's event loop) before waiting for the put's own monitor event.
    qtbot.waitUntil(lambda: pv.value is not None, timeout=10000)
    with qtbot.waitSignal(
        pv.value_changed, timeout=10000, check_params_cb=lambda v: float(v) == 7.5
    ):
        pv.put(7.5, wait=True)
    assert float(pv.value) == 7.5


def test_pv_metadata_extracted(qtbot, softioc):
    pv = PV(softioc + "value")
    with qtbot.waitSignal(pv.metadata_changed, timeout=10000) as blocker:
        pv.connect_pv()
    assert isinstance(blocker.args[0], dict)
