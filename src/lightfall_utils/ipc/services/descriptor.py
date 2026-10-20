"""Declarative description of an out-of-process service (Qt-free)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from lightfall_utils.ipc.services.protocol import sanitize_token

_CONTRACT_RE = re.compile(r"^\d+\.\d+\Z")
_FORBIDDEN_ROOTS = {"", "_INBOX", "_PRIV"}


def _check_scope(scope: str) -> None:
    if scope.split(".", 1)[0] in _FORBIDDEN_ROOTS:
        raise ValueError(f"invalid scope {scope!r}: first token must not be empty, _INBOX or _PRIV")


@dataclass(frozen=True)
class ServiceDescriptor:
    name: str
    scope: str
    contract: str
    launch: Literal["local", "external"]
    command: tuple[str, ...] = ()
    stop_with_client: bool = True
    single_instance: bool = True
    interpreter_pref: str | None = None
    hint: str = ""

    def __post_init__(self) -> None:
        if not self.name or sanitize_token(self.name) != self.name:
            raise ValueError(f"service name {self.name!r} is not token-safe")
        if not _CONTRACT_RE.match(self.contract):
            raise ValueError(f"contract {self.contract!r} must match major.minor")
        if self.launch not in ("local", "external"):
            raise ValueError(f"launch must be 'local' or 'external', got {self.launch!r}")
        if self.launch == "local" and not self.command:
            raise ValueError("local services require a non-empty command")
        # Validate the template with a placeholder prefix; the real prefix is rechecked
        # in resolved_scope().
        _check_scope(self._format("p", "h", "i"))

    def _format(self, prefix: str, hostname: str, client_instance: str) -> str:
        return self.scope.format(
            prefix=prefix,
            hostname=sanitize_token(hostname),
            client_instance=client_instance,
        )

    def resolved_scope(self, prefix: str, hostname: str, client_instance: str) -> str:
        scope = self._format(prefix, hostname, client_instance)
        _check_scope(scope)
        return scope
