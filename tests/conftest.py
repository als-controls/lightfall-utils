"""Pytest configuration and fixtures for lightfall-utils tests."""

import pytest

# pytest-qt provides the `qapp` and `qtbot` fixtures automatically.
# No custom qapp fixture needed - pytest-qt handles QApplication lifecycle
# including proper cleanup and CI/headless environment support.


@pytest.fixture(scope="session", autouse=True)
def _shutdown_thread_manager():
    """Cancel any QThreadFutures still running when the session ends.

    In production ThreadManager.shutdown() runs on QApplication.aboutToQuit,
    but pytest never finishes a Qt event loop, so that hookup never fires.
    Threads leaked by tests are then destroyed at interpreter exit while
    still running, which aborts the process ("QThread: Destroyed while
    thread is still running") and breaks the exit code even when every test
    passed.

    Ported from Lightfall's tests/conftest.py (same fixture name and
    docstring intent), adapted to import only from lightfall_utils.
    """
    yield
    from lightfall_utils.threads import get_thread_manager

    get_thread_manager().shutdown()


@pytest.fixture(autouse=True)
def _drain_qt_events_after_test(qapp):
    """Pump the Qt event loop once after each test.

    A QThreadFuture's ``finished`` signal is delivered to the main thread as
    a queued event; if nothing pumps the event loop before the next test
    starts, that delivery (and the ``_deferred_unregister`` slot it drives)
    stays queued and fires later -- inside whichever future test next spins
    the event loop (e.g. via ``qtbot.waitUntil``). Firing against a stale
    QThreadFuture from an unrelated, already-finished test crashes the
    process with an access violation (0xC0000005) rather than raising a
    catchable Python exception. Draining events right after each test keeps
    that delivery local to the test that produced it.
    """
    yield
    qapp.processEvents()
