"""NATS-backed IPC for NCS applications: service, trust handshake, protocol, local broker."""

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
    ErrorCodes,
    error_reply,
    ok_reply,
)
from lightfall_utils.ipc.trust import TrustDialog, TrustManager, TrustState

__all__ = [
    "BASE_ERROR_CODES",
    "CONTRACT_VERSION",
    "ErrorCodes",
    "error_reply",
    "ok_reply",
    "TrustDialog",
    "TrustManager",
    "TrustState",
    "LocalNatsServer",
    "LocalNatsServerError",
    "NatsBinaryNotFoundError",
    "NatsPortInUseError",
    "NatsReadinessTimeoutError",
    "nats_binary_version",
    "probe_nats",
    "resolve_nats_binary",
]
