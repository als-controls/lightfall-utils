"""Tests for GUI-thread affinity assertions."""

import threading

from lightfall_utils.qt_affinity import assert_gui_thread, assert_object_thread, gui_thread_only


def test_assert_gui_thread_permissive_without_qapp():
    # No QApplication in this process yet -> must not raise.
    # (Run first, before any qtbot/qapp fixture creates one.)
    assert_gui_thread()


def test_gui_thread_only_passes_on_gui_thread(qapp):
    @gui_thread_only
    def touch() -> str:
        return "ok"

    assert touch() == "ok"


def test_gui_thread_only_raises_off_gui_thread(qapp):
    @gui_thread_only
    def touch() -> None:
        pass

    errors: list[Exception] = []

    def worker() -> None:
        try:
            touch()
        except RuntimeError as e:
            errors.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(errors) == 1
    assert "GUI thread assertion failed" in str(errors[0])


def test_assert_object_thread(qapp):
    from PySide6.QtCore import QObject

    obj = QObject()
    assert_object_thread(obj)  # same thread -> no raise

    errors: list[Exception] = []

    def worker() -> None:
        try:
            assert_object_thread(obj)
        except RuntimeError as e:
            errors.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(errors) == 1
