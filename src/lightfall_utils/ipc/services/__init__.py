"""Out-of-process service framework over NATS (Qt-free names only)."""

from lightfall_utils.ipc.services import protocol
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor

__all__ = ["ServiceDescriptor", "protocol"]
