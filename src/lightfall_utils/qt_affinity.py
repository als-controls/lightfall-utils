"""Qt thread-affinity assertions.

Helpers to catch the classic Qt failure mode of touching GUI objects from
a worker thread. Extracted from Lightfall's crash diagnostics.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

__all__ = ["assert_gui_thread", "assert_object_thread", "gui_thread_only"]


def assert_gui_thread(obj: Any | None = None) -> None:
    """Raise ``RuntimeError`` if not running on the QApplication's thread.

    Args:
        obj: Optional QObject. If supplied, its ``thread()`` is included
            in the error message — useful when a widget appears to belong
            to the wrong thread.
    """
    from PySide6.QtCore import QCoreApplication, QThread

    app = QCoreApplication.instance()
    if app is None:
        # No QApplication yet — affinity isn't meaningful. Be permissive.
        return

    current = QThread.currentThread()
    gui_thread = app.thread()
    if current is gui_thread:
        return

    py_thread = threading.current_thread()
    parts = [
        f"GUI thread assertion failed: called from "
        f"{py_thread.name!r} (QThread={current!r}), "
        f"expected GUI thread (QThread={gui_thread!r})",
    ]
    if obj is not None:
        try:
            obj_thread = obj.thread()
            parts.append(f"; object {obj!r}.thread()={obj_thread!r}")
        except Exception as e:
            parts.append(f"; object thread() raised: {e!r}")

    raise RuntimeError("".join(parts))


def assert_object_thread(obj: Any) -> None:
    """Raise ``RuntimeError`` if the current thread is not ``obj.thread()``.

    The mirror of ``assert_gui_thread`` for QObjects that legitimately
    live on a non-GUI thread (a worker, a background QObject moved to
    a QThread, etc).
    """
    from PySide6.QtCore import QThread

    obj_thread = obj.thread()
    current = QThread.currentThread()
    if current is obj_thread:
        return

    py_thread = threading.current_thread()
    raise RuntimeError(
        f"Object-thread assertion failed: called from "
        f"{py_thread.name!r} (QThread={current!r}), "
        f"expected {obj!r}.thread()={obj_thread!r}"
    )


def gui_thread_only(func: F) -> F:
    """Wrap a method/slot so it raises if called off the GUI thread.

    The wrapped callable is otherwise unchanged — ``@Slot`` decorators
    can be stacked above or below.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Pass the receiver (first positional, when wrapping a bound method)
        # to assert_gui_thread so its thread() is included in the error.
        receiver = args[0] if args else None
        assert_gui_thread(receiver)
        return func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
