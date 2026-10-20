"""NATS-backed IPC for NCS applications: service, trust handshake, protocol, local broker."""

from typing import TYPE_CHECKING

from lightfall_utils.ipc.local_server import (
    LocalNatsServer,
    LocalNatsServerError,
    NatsBinaryNotFoundError,
    NatsPortInUseError,
    NatsReadinessTimeoutError,
    nats_binary_version,
    probe_nats,
    resolve_nats_binary,
)
from lightfall_utils.ipc.protocol import (
    BASE_ERROR_CODES,
    CONTRACT_VERSION,
    ERROR_CODES,
    ErrorCodes,
    error_reply,
    ok_reply,
)

if TYPE_CHECKING:  # pragma: no cover
    from lightfall_utils.ipc.service import ActionInfo, EventInfo, IPCService
    from lightfall_utils.ipc.trust import TrustDialog, TrustManager, TrustState

# Qt-dependent names are resolved lazily (PEP 562) so ``lightfall_utils.ipc.capability``
# and ``lightfall_utils.ipc.services`` import without PySide6.
_LAZY = {
    "IPCService": "lightfall_utils.ipc.service",
    "ActionInfo": "lightfall_utils.ipc.service",
    "EventInfo": "lightfall_utils.ipc.service",
    "TrustDialog": "lightfall_utils.ipc.trust",
    "TrustManager": "lightfall_utils.ipc.trust",
    "TrustState": "lightfall_utils.ipc.trust",
}


def __getattr__(name: str):
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(mod), name)
    globals()[name] = value
    return value

__all__ = [
    "IPCService",
    "ActionInfo",
    "EventInfo",
    "TrustDialog",
    "TrustManager",
    "TrustState",
    "LocalNatsServer",
    "LocalNatsServerError",
    "NatsBinaryNotFoundError",
    "NatsPortInUseError",
    "NatsReadinessTimeoutError",
    "resolve_nats_binary",
    "nats_binary_version",
    "probe_nats",
    "BASE_ERROR_CODES",
    "ERROR_CODES",
    "CONTRACT_VERSION",
    "ErrorCodes",
    "error_reply",
    "ok_reply",
]
