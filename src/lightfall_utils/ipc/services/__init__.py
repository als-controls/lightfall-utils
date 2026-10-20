"""Out-of-process service framework over NATS (Qt-free names only)."""

from lightfall_utils.ipc.services import protocol
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor
from lightfall_utils.ipc.services.launcher import LocalLauncher

__all__ = ["LocalLauncher", "ServiceDescriptor", "protocol"]
