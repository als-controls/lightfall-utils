# `lightfall_utils.ipc` Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Lightfall's NATS IPC layer — `IPCService`, `TrustManager`/`TrustDialog`, the reply protocol, `LocalNatsServer` — into `lightfall-utils` so Camphor (and any NCS app) can be a participant on the bus with the same trust handshake and capability channels.

**Architecture:** A *move*, not a rewrite. Four modules come across from `lightfall/src/lightfall/ipc/` and `lightfall/src/lightfall/remote/protocol.py` with their tests; the only code changes are the two Lightfall couplings — `invoke_in_main_thread` (already in `lightfall_utils.threads`) and `build_auth_response`, which stops importing `SessionManager` and takes the app-specific reply fields as a parameter. `get_ipc_service()` (a `ServiceRegistry` lookup) stays in Lightfall. The error-code set becomes extensible per app.

**Tech Stack:** Python 3.11+, PySide6, `nats-py>=2.0`, `loguru`; `nats-server-bin` optional for tests and the local broker; pytest + pytest-qt.

**Spec:** `C:\Users\rp\workspace\catfish\docs\superpowers\specs\2026-09-10-agent-support-design.md` §3.1, §5.5, §8 step 1. Source of the moved code: `C:\Users\rp\PycharmProjects\ncs\lightfall\src\lightfall\ipc\{service,trust,local_server}.py`, `...\lightfall\remote\protocol.py`, tests under `...\lightfall\tests\ipc\`.

## Global Constraints

- Public names and behaviour are preserved: `IPCService`, `ActionInfo`, `EventInfo`, `TrustManager`, `TrustState`, `TrustDialog`, `LocalNatsServer` and its three error classes, `resolve_nats_binary`, `nats_binary_version`, `probe_nats`, `CONTRACT_VERSION = 1`, `ok_reply`, `error_reply`. Lightfall must be able to re-export them unchanged (its plan, step 2).
- Wire contract unchanged: subjects, `auth.request` reply shape (`status`, `session_token`, `contract_version`, optional `reason`), `meta.actions`/`meta.events`, discover reply, capability-channel routing, `denied` on bare trusted subjects.
- No `lightfall.*` import anywhere under `src/lightfall_utils/ipc/`. A hygiene test enforces it.
- `nats-py>=2.0` becomes a core dependency of lightfall-utils; `nats-server-bin>=2.14` is a new optional extra `local-nats` and a `dev` dependency.
- Repo conventions: ruff (line length 100), pyright, pytest-qt; commits carry the attribution footer in use in this session.
- Release as `0.2.0` (tag) when the plan is complete; Lightfall and Camphor floor on it.

---

### Task 1: Dependencies and the extensible reply protocol

**Files:**
- Modify: `pyproject.toml` (dependencies, optional-dependencies)
- Create: `src/lightfall_utils/ipc/__init__.py`, `src/lightfall_utils/ipc/protocol.py`
- Test: `tests/ipc/__init__.py`, `tests/ipc/test_protocol.py`

**Interfaces:**
- Produces: `CONTRACT_VERSION: int = 1`; `BASE_ERROR_CODES: frozenset[str]`; `ok_reply(**fields) -> dict`; `error_reply(code, message, *, extra_codes: Iterable[str] = ()) -> dict`; `ErrorCodes` helper (`ErrorCodes(*app_codes).reply(code, message)`).

- [ ] **Step 1: Add the dependencies**

In `pyproject.toml`:

```toml
dependencies = [
    "PySide6>=6.6",
    "loguru>=0.7",
    "pydantic>=2.0",
    "pyyaml>=6.0",
    "nats-py>=2.0",
]

[project.optional-dependencies]
ca = ["caproto>=1.1"]
local-nats = ["nats-server-bin>=2.14"]
multihomed = ["netifaces>=0.11; sys_platform != 'darwin'"]
dev = [
    "caproto>=1.1",
    "nats-server-bin>=2.14",
    "pytest>=8.0",
    "pytest-cov>=4.0",
    "pytest-qt>=4.2",
    "ruff>=0.1",
    "pyright>=1.1",
]
```

Run: `.venv/Scripts/python.exe -m pip install -e ".[dev]"`

- [ ] **Step 2: Write the failing protocol tests**

```python
# tests/ipc/test_protocol.py
import pytest

from lightfall_utils.ipc.protocol import (
    BASE_ERROR_CODES,
    CONTRACT_VERSION,
    ErrorCodes,
    error_reply,
    ok_reply,
)


def test_ok_reply_carries_the_contract_version():
    assert ok_reply(status="ok", value=3) == {"status": "ok", "value": 3, "contract_version": 1}
    assert CONTRACT_VERSION == 1


def test_error_reply_shape_for_a_base_code():
    assert error_reply("denied", "nope") == {
        "status": "error", "code": "denied", "message": "nope", "contract_version": 1,
    }


def test_lightfall_s_codes_are_all_in_the_base_set():
    # The seven codes Lightfall's remote-control contract v1 documents.
    assert {"busy", "limits", "timeout", "unknown", "denied", "bad_request",
            "version_mismatch"} <= BASE_ERROR_CODES


def test_an_unknown_code_is_a_programming_error():
    with pytest.raises(ValueError):
        error_reply("not_a_code", "x")


def test_an_app_extends_the_set_without_touching_the_base():
    codes = ErrorCodes("too_large", "confirmation_required")
    assert codes.reply("too_large", "1.2 MiB > 1 MiB")["code"] == "too_large"
    assert codes.reply("denied", "bare subject")["code"] == "denied"  # base still valid
    with pytest.raises(ValueError):
        codes.reply("busy_beaver", "x")
    assert "too_large" not in BASE_ERROR_CODES
```

- [ ] **Step 3: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/ipc/test_protocol.py -q`
Expected: FAIL — `ModuleNotFoundError: lightfall_utils.ipc`

- [ ] **Step 4: Implement `protocol.py`**

```python
# src/lightfall_utils/ipc/protocol.py
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
```

`src/lightfall_utils/ipc/__init__.py` for now:

```python
"""NATS-backed IPC for NCS applications: service, trust handshake, protocol, local broker."""

from lightfall_utils.ipc.protocol import (
    BASE_ERROR_CODES,
    CONTRACT_VERSION,
    ErrorCodes,
    error_reply,
    ok_reply,
)

__all__ = ["BASE_ERROR_CODES", "CONTRACT_VERSION", "ErrorCodes", "error_reply", "ok_reply"]
```

- [ ] **Step 5: Run to verify pass, lint, commit**

Run: `.venv/Scripts/python.exe -m pytest tests/ipc -q && .venv/Scripts/python.exe -m ruff check src tests`
Expected: 5 passed, lint clean.

```bash
git add pyproject.toml src/lightfall_utils/ipc tests/ipc
git commit -m "feat(ipc): reply protocol with an app-extensible error vocabulary; nats-py dependency"
```

---

### Task 2: Move `TrustManager` / `TrustDialog`

**Files:**
- Create: `src/lightfall_utils/ipc/trust.py` (from `lightfall/src/lightfall/ipc/trust.py`, 144 lines)
- Test: `tests/ipc/test_trust.py` (from `lightfall/tests/ipc/test_trust.py`)

**Interfaces:**
- Produces: `TrustState` (`UNKNOWN`, `APPROVED`, `DENIED`), `TrustManager` (`check`, `approve`, `deny`, `revoke`, `clear`, `list_approved`/whatever the source exposes — preserved verbatim), `TrustDialog(app_name, app_version, parent=None)`.

- [ ] **Step 1: Copy the source module and its test verbatim**

```bash
cp ../lightfall/src/lightfall/ipc/trust.py src/lightfall_utils/ipc/trust.py
cp ../lightfall/tests/ipc/test_trust.py tests/ipc/test_trust.py
```

- [ ] **Step 2: Fix imports**

In `tests/ipc/test_trust.py`: `from lightfall.ipc.trust import ...` → `from lightfall_utils.ipc.trust import ...`. `trust.py` itself imports only stdlib and `PySide6.QtWidgets`; verify with `grep -n "^from lightfall" src/lightfall_utils/ipc/trust.py` → no output.

- [ ] **Step 3: Run the moved tests**

Run: `.venv/Scripts/python.exe -m pytest tests/ipc/test_trust.py -q`
Expected: all pass (the module is unchanged).

- [ ] **Step 4: Export and commit**

Add to `src/lightfall_utils/ipc/__init__.py`: `from lightfall_utils.ipc.trust import TrustDialog, TrustManager, TrustState` and extend `__all__`.

```bash
git add src/lightfall_utils/ipc/trust.py src/lightfall_utils/ipc/__init__.py tests/ipc/test_trust.py
git commit -m "feat(ipc): move TrustManager/TrustDialog from Lightfall"
```

---

### Task 3: Move `LocalNatsServer`

**Files:**
- Create: `src/lightfall_utils/ipc/local_server.py` (from `lightfall/src/lightfall/ipc/local_server.py`, 175 lines)
- Test: `tests/ipc/test_local_server.py` (from Lightfall)

**Interfaces:**
- Produces: `LocalNatsServer(port=4222, host="127.0.0.1")` with `start(timeout_s)`, `stop()`, `url`; `LocalNatsServerError`, `NatsBinaryNotFoundError`, `NatsPortInUseError`, `NatsReadinessTimeoutError`; `resolve_nats_binary()`, `nats_binary_version(path)`, `probe_nats(host, port, timeout)`.

- [ ] **Step 1: Copy module and test; fix imports**

```bash
cp ../lightfall/src/lightfall/ipc/local_server.py src/lightfall_utils/ipc/local_server.py
cp ../lightfall/tests/ipc/test_local_server.py tests/ipc/test_local_server.py
```

`local_server.py` imports stdlib + `loguru` only. In the test replace `lightfall.ipc` with `lightfall_utils.ipc` (both the `from lightfall.ipc import local_server` and `from lightfall.ipc.local_server import (...)` forms).

- [ ] **Step 2: Run**

Run: `.venv/Scripts/python.exe -m pytest tests/ipc/test_local_server.py -q`
Expected: pass (`nats-server-bin` is in `dev`, so the bundled binary resolves).

- [ ] **Step 3: Export and commit**

Add the class, errors and three functions to `__init__.py` exports.

```bash
git add src/lightfall_utils/ipc/local_server.py src/lightfall_utils/ipc/__init__.py tests/ipc/test_local_server.py
git commit -m "feat(ipc): move LocalNatsServer from Lightfall"
```

---

### Task 4: Move `IPCService`, decoupled from Lightfall

**Files:**
- Create: `src/lightfall_utils/ipc/service.py` (from `lightfall/src/lightfall/ipc/service.py`, 932 lines)
- Test: `tests/ipc/test_service.py`, `tests/ipc/test_actions.py`, `tests/ipc/test_capability_channels.py`, `tests/ipc/test_discover_peers.py` (from Lightfall's `tests/ipc/`)

**Interfaces:**
- Produces: `IPCService(nats_url, topic_prefix="", parent=None)` with every existing method; **changed**: `build_auth_response(*, approved: bool, reason: str = "", app_name: str | None = None, extra: Mapping[str, Any] | None = None) -> dict` — no `session`/`tiled_url`; the approved reply is `{"status": "approved", **extra, "contract_version": 1, "session_token": ...}`. **Removed here**: `get_ipc_service()` (Lightfall keeps it; it needs `ServiceRegistry`).
- Consumes: `lightfall_utils.threads.invoke_in_main_thread`; `lightfall_utils.ipc.trust`; `lightfall_utils.ipc.protocol` (`CONTRACT_VERSION`, `error_reply`).

- [ ] **Step 1: Copy the module and the four tests**

```bash
cp ../lightfall/src/lightfall/ipc/service.py src/lightfall_utils/ipc/service.py
for t in test_service test_actions test_capability_channels test_discover_peers; do
  cp ../lightfall/tests/ipc/$t.py tests/ipc/$t.py
done
```

- [ ] **Step 2: Rewrite the imports in `service.py`**

Replace:

```python
from lightfall.ipc.trust import TrustManager, TrustState
from lightfall.remote.protocol import CONTRACT_VERSION, error_reply
from lightfall.utils.threads import invoke_in_main_thread
```

with:

```python
from lightfall_utils.ipc.protocol import CONTRACT_VERSION, error_reply
from lightfall_utils.ipc.trust import TrustManager, TrustState
from lightfall_utils.threads import invoke_in_main_thread
```

Delete `get_ipc_service` (lines 923–932 of the source) and drop it from `__all__`.

- [ ] **Step 3: Decouple `build_auth_response`**

Replace the method body (source lines 329–395) with:

```python
    def build_auth_response(
        self,
        *,
        approved: bool,
        reason: str = "",
        app_name: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> dict:
        """Build the reply to an ``auth.request`` handshake.

        ``extra`` carries the application's own approved-reply fields --
        Lightfall adds ``tiled_token``, ``tiled_url`` and ``session_id``; an
        app with nothing to add passes none. When ``app_name`` is given and the
        request is approved, a session capability channel is minted for it and
        its token returned as ``session_token``. A denial carries ``reason``
        when non-empty.
        """
        if approved:
            response: dict = {"status": "approved", **(dict(extra) if extra else {})}
            response["contract_version"] = CONTRACT_VERSION
            if app_name:
                response["session_token"] = self.mint_session_channel(app_name)
            return response
        response = {"status": "denied", "contract_version": CONTRACT_VERSION}
        if reason:
            response["reason"] = reason
        return response
```

Add `from collections.abc import Mapping` to the imports if missing.

- [ ] **Step 4: Fix the moved tests**

- Every `from lightfall.ipc.service import` → `from lightfall_utils.ipc.service import`; `from lightfall.ipc.trust` → `from lightfall_utils.ipc.trust`.
- `tests/ipc/test_service.py` around source line 285 patches `lightfall.auth.session.SessionManager` to test the Tiled fields of `build_auth_response`. Replace that test with one for the new signature:

```python
def test_build_auth_response_approved_mints_a_channel_and_carries_extra_fields():
    svc = IPCService("nats://unused", topic_prefix="als.test")
    svc.set_trust_manager(TrustManager())
    reply = svc.build_auth_response(
        approved=True, app_name="camphor-mcp", extra={"tiled_url": "https://t"}
    )
    assert reply["status"] == "approved"
    assert reply["tiled_url"] == "https://t"
    assert reply["contract_version"] == 1
    assert isinstance(reply["session_token"], str) and len(reply["session_token"]) > 20
    assert svc.session_channel_count == 1


def test_build_auth_response_denied_carries_reason_only_when_given():
    svc = IPCService("nats://unused")
    assert svc.build_auth_response(approved=False) == {"status": "denied", "contract_version": 1}
    assert svc.build_auth_response(approved=False, reason="timeout")["reason"] == "timeout"
```

- `tests/ipc/test_capability_channels.py` imports `lightfall.auth.session` at source line ~235 for a logout-teardown test. That test stays in Lightfall (it tests Lightfall's `_wire_session_trust`); delete it from the copy here. Every other test in the file exercises `IPCService` alone and stays.
- Any test that calls `get_ipc_service` moves back to Lightfall (delete from the copy).

- [ ] **Step 5: Hygiene test**

```python
# tests/ipc/test_no_lightfall_imports.py
import pathlib
import re

PKG = pathlib.Path(__file__).resolve().parents[2] / "src" / "lightfall_utils" / "ipc"


def test_ipc_package_imports_nothing_from_lightfall():
    offenders = []
    for path in PKG.glob("*.py"):
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.match(r"\s*(from|import)\s+lightfall(\.|\s|$)", line):
                offenders.append(f"{path.name}:{n}: {line.strip()}")
    assert offenders == []
```

- [ ] **Step 6: Run everything**

Run: `.venv/Scripts/python.exe -m pytest tests/ipc -q -p no:cacheprovider`
Expected: all pass. (`test_discover_peers` and the integration-style tests in `test_actions` start a real `nats-server` via `LocalNatsServer`; `nats-server-bin` is in `dev`.)

Run: `.venv/Scripts/python.exe -m ruff check src tests && .venv/Scripts/python.exe -m pyright src/lightfall_utils/ipc`
Expected: clean.

- [ ] **Step 7: Export and commit**

`__init__.py` final export list: `IPCService, ActionInfo, EventInfo, TrustDialog, TrustManager, TrustState, LocalNatsServer, LocalNatsServerError, NatsBinaryNotFoundError, NatsPortInUseError, NatsReadinessTimeoutError, resolve_nats_binary, nats_binary_version, probe_nats, BASE_ERROR_CODES, CONTRACT_VERSION, ErrorCodes, error_reply, ok_reply`.

```bash
git add src/lightfall_utils/ipc tests/ipc
git commit -m "feat(ipc): move IPCService from Lightfall; app-neutral auth reply"
```

---

### Task 5: Docs, README, release

**Files:**
- Modify: `README.md` (feature list + one paragraph), `docs/` (new page `docs/ipc.md` if the docs tree has per-module pages; otherwise a section in the existing API page)
- Modify: `tests/test_package_hygiene.py` if it enumerates public modules

- [ ] **Step 1: Document**

In `README.md` add a bullet under the feature list: "**IPC** — NATS-backed `IPCService` with a trust handshake, per-session capability channels, discovery and a structured reply protocol; optional `LocalNatsServer` for broker-less development (`pip install lightfall-utils[local-nats]`)." Add `docs/ipc.md` containing: the subject shapes (`{prefix}.auth.request`, `{prefix}.meta.actions/events`, `{prefix}.session.{token}.<suffix>`, discover), the handshake sequence, `build_auth_response(extra=...)` for app fields, `ErrorCodes` for app vocabularies, and a 20-line "register an action" example (adapted from Lightfall's `ipc-architecture.md` "Registering a New Action", with `ipc.reply(reply, ok_reply(status="ok", ...))`).

- [ ] **Step 2: Full suite, tag**

Run: `.venv/Scripts/python.exe -m pytest -q -p no:cacheprovider`
Expected: all green (the existing suites plus `tests/ipc`).

```bash
git add README.md docs
git commit -m "docs: lightfall_utils.ipc"
git tag -a v0.2.0 -m "lightfall-utils 0.2.0: NATS IPC layer extracted from Lightfall"
```

(Do **not** push; Ron decides when.)
