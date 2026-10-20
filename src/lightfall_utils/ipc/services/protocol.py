"""Qt-free wire protocol for out-of-process services over NATS.

Subject builders, timing constants, version negotiation and reply helpers.
"""

from __future__ import annotations

import re

from lightfall_utils.ipc import protocol as _base
from lightfall_utils.ipc.capability import _PRIV_ROOT_RE

SVC_PROTOCOL = 1
HEARTBEAT_S = 2.0
PING_S = 5.0
LEASE_S = 30.0
STREAM_IDLE_S = 10.0
DEFAULT_TIMEOUT_S = 2.0

HDR_SEQ = "Svc-Seq"
HDR_KIND = "Svc-Kind"
HDR_FINAL = "Svc-Final"

ERROR_CODES = _base.BASE_ERROR_CODES | frozenset({"incompatible"})

_BAD_CHARS = re.compile(r"[^a-z0-9_-]")
_INBOX_RE = re.compile(r"^_INBOX\.([^.]+)\.")


def sanitize_token(s: str) -> str:
    """Lowercase and replace every char outside ``[a-z0-9_-]`` with ``-``."""
    return _BAD_CHARS.sub("-", s.lower())


def local_scope(prefix: str, hostname: str, client_instance: str) -> str:
    return f"{prefix}.host.{sanitize_token(hostname)}.{client_instance}"


def root(scope: str, service: str) -> str:
    return f"{scope}.svc.{service}"


def meta_info(root: str) -> str:
    return f"{root}.meta.info"


def heartbeat(root: str) -> str:
    return f"{root}.heartbeat"


def event(root: str, name: str) -> str:
    return f"{root}.events.{name}"


def discover(scope: str) -> str:
    return f"{scope}.svc.discover"


def auth_subject(priv_root: str) -> str:
    return f"{priv_root}.auth.request"


def session_subject(priv_root: str, token: str) -> str:
    return f"{priv_root}.session.{token}"


def rpc_subject(session_subject: str, action: str) -> str:
    return f"{session_subject}.rpc.{action}"


def stream_subject(session_subject: str, stream_id: str) -> str:
    return f"{session_subject}.stream.{stream_id}"


def delivery_root(client_priv_root: str, delivery_token: str) -> str:
    return f"{client_priv_root}.d.{delivery_token}"


def prefix_of_priv_root(priv_root: str) -> str:
    """The capability prefix ``p`` of ``_PRIV.<p>``; ValueError if malformed."""
    if not isinstance(priv_root, str) or not _PRIV_ROOT_RE.fullmatch(priv_root):
        raise ValueError(f"malformed priv_root: {priv_root!r}")
    return priv_root.split(".", 1)[1]


def prefix_of_inbox(reply_subject: str) -> str | None:
    """The ``p`` of a reply subject ``_INBOX.<p>.…``, or None if not of that shape."""
    m = _INBOX_RE.match(reply_subject) if isinstance(reply_subject, str) else None
    return m.group(1) if m else None


def _parse_contract(c: str) -> tuple[int, int]:
    major, minor = c.split(".")
    return int(major), int(minor)


def contract_ok(service_contract: str, minimum: str) -> bool:
    """Same major, and service minor >= required minimum minor."""
    try:
        smaj, smin = _parse_contract(service_contract)
        mmaj, mmin = _parse_contract(minimum)
    except (ValueError, AttributeError):
        return False
    return smaj == mmaj and smin >= mmin


def svc_reply(contract: str, **fields) -> dict:
    return {"status": "ok", **fields, "contract_version": contract}


def svc_error(contract: str, code: str, message: str) -> dict:
    if code not in ERROR_CODES:
        raise ValueError(f"Unknown error code: {code!r}")
    return {
        "status": "error",
        "code": code,
        "message": message,
        "contract_version": contract,
    }
