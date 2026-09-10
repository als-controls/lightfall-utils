"""Reply protocol for NCS IPC contracts (v1).

Every reply -- success or error -- carries ``contract_version`` so a client
can detect a mismatch. Errors are structured: ``{status: "error", code,
message, contract_version}``. The base code set is Lightfall's remote-control
contract; an application adds its own with :class:`ErrorCodes` rather than by
editing this set, so two apps on one bus never disagree about a base code.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

__all__ = ["BASE_ERROR_CODES", "CONTRACT_VERSION", "ErrorCodes", "error_reply", "ok_reply"]

CONTRACT_VERSION = 1

BASE_ERROR_CODES = frozenset(
    {"busy", "limits", "timeout", "unknown", "denied", "bad_request", "version_mismatch"}
)


def ok_reply(**fields: Any) -> dict:
    """A success reply carrying ``contract_version``."""
    return {**fields, "contract_version": CONTRACT_VERSION}


def error_reply(code: str, message: str, *, extra_codes: Iterable[str] = ()) -> dict:
    """A structured error reply. ``code`` must be a base code or one of ``extra_codes``."""
    if code not in BASE_ERROR_CODES and code not in set(extra_codes):
        raise ValueError(f"Unknown error code: {code!r}")
    return {
        "status": "error",
        "code": code,
        "message": message,
        "contract_version": CONTRACT_VERSION,
    }


class ErrorCodes:
    """An application's error vocabulary: the base set plus its own codes."""

    def __init__(self, *app_codes: str) -> None:
        self.codes: frozenset[str] = BASE_ERROR_CODES | frozenset(app_codes)

    def reply(self, code: str, message: str) -> dict:
        return error_reply(code, message, extra_codes=self.codes)
