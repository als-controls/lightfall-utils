# IPC: NATS-backed Inter-Process Communication

The `lightfall_utils.ipc` module provides a Qt-integrated IPC service built on NATS, with trust handshaking, per-session capability channels, and a structured reply protocol.

## Overview

An `IPCService` manages a connection to a NATS broker, publishes and subscribes to a topic tree, handles request/reply patterns for actions, and mints per-session capability tokens for trusted applications. The protocol uses JSON and versioning to ensure compatibility across services.

## Connection Capability

Every `IPCService` connects with a per-instance **connection capability** from
`lightfall_utils.ipc.capability`: a random secret sent as the NATS connect `token`, and the
matching `inbox_prefix="_INBOX.<p>"`, where
`p = base32(sha256("lightfall-nats-v1:" + secret))`, lowercase, unpadded, first 26 characters.
On bcgnats the broker's auth callout derives the same `p` and lets the connection subscribe only to
`_INBOX.<p>.>`, its **private root** `_PRIV.<p>.>`, and the allow-listed public roots. The
reference model is Lightfall's developer-guide page *NATS Use Model*
(`lightfall/docs/developer-guide/nats-use-model.md`).

The capability is created once per `IPCService` and reused across reconnects, so
`service.priv_root` (`_PRIV.<p>`) is stable for the service's lifetime.

`lightfall_utils.ipc.capability` is **stdlib only**, so it can be used by clients that don't run Qt:

```python
import nats
from lightfall_utils.ipc.capability import new_capability, private_auth_request, private_request

cap = new_capability()            # .secret, .prefix, .inbox_prefix, .priv_root
nc = await nats.connect(url, **cap.connect_kwargs())   # {"token": ..., "inbox_prefix": ...}

# Discover priv_root via {prefix}.meta.actions, then request {priv_root}.auth.request
reply = await private_auth_request(nc, "als.7011", {"app_name": "my-tool", "app_version": "1.0"})
session_subject = reply["session_subject"]

# Every session call goes through private_request (never nc.request):
status = await private_request(nc, f"{session_subject}.device.status", {"device": "m1"})
```

**Why `private_request`.** Inbox isolation protects *reads*: no other connection can subscribe
to your `_INBOX.<p>`. It does not stop *writes*. nats-server lets any connection answer a message
it received with a reply subject (`allow_responses`), and a publisher may set any reply subject,
so a connection can reflect a forged message into your inbox if it can guess the exact reply
subject. The reply subjects nats-py's `request()` generates are predictable to anyone who has
seen one of your public requests. `private_request` replies to `<inbox_prefix>.<new_token()>`
instead, a subject only the `_PRIV` owner you sent it to ever sees. Replies to *public* requests
(`meta.actions`, discovery, public actions) can still be forged; treat them like spoofable
discovery.

| Name | Purpose |
|---|---|
| `new_capability()` | Fresh `ConnectionCapability(secret, prefix)` with `inbox_prefix`, `priv_root`, `connect_kwargs()` |
| `derive_prefix(secret)` | The secret → `p` derivation the callout uses |
| `new_token()` | 32 random bytes, URL-safe (43 chars); used for secrets, session and delivery tokens |
| `private_request(nc, subject, payload, timeout=5.0, inbox_prefix=None)` | One JSON request on an unguessable reply subject `<inbox_prefix>.<token>`; use it for every `{priv_root}.*` and `{session_subject}.*` call. `inbox_prefix` defaults to the connection's own |
| `private_auth_request(nc, topic_prefix, payload, timeout=5.0)` | Two-step private handshake (the `auth.request` leg goes through `private_request`); returns the decoded reply |
| `PrivateRootUnavailable` | Raised when `meta.actions` has no `priv_root` (a pre-callout peer) or one not shaped `_PRIV.<26 base32 chars>` |
| `PrivateNoResponders` | Raised by `private_request` when nobody is subscribed to the subject |
| `VENDORED_FROM` | Version marker checked by vendored copies |

**Vendoring rule.** Importing `lightfall_utils.ipc` pulls in PySide6, so headless repositories copy
`capability.py` **byte-for-byte** as `nats_capability.py` and pin it with a test that checks known
`derive_prefix` vectors and `VENDORED_FROM`. Keep `capability.py` free of Qt, Lightfall and
third-party imports, and change it only here, then re-copy it everywhere.

## Topic Structure

Public subjects live under a configurable prefix (e.g., `"als.lightfall"`). Well-known discovery
uses no prefix. Private subjects live under the instance's private root `_PRIV.<p>`:

- **Meta discovery (no auth required):** `{prefix}.meta.actions` — list registered actions; `{prefix}.meta.events` — list registered events. Both replies carry `priv_root`; catalog entries for `private=True` actions carry `"private": true`
- **Peer discovery (no prefix):** `_lightfall.discover` — scatter-gather request/reply to find all live services; replies carry `priv_root`
- **Auth handshake (private):** `{priv_root}.auth.request` when registered with `private=True` (as Lightfall does) — app sends `{"app_name", "app_version"}`, service replies with approval/denial, `session_token` and `session_subject`
- **Session capability channels:** `{priv_root}.session.{token}.>` — per-app capability subject after successful auth; routes to trusted actions with identity attached

## Trust Handshake Sequence

1. App discovers `priv_root` (from `meta.actions` or `_lightfall.discover`) and sends a request to `{priv_root}.auth.request` with `{"app_name", "app_version"}`; the reply goes to an unguessable subject in its own `_INBOX.<p>` (`private_request`)
2. Service evaluates trust via `evaluate_trust(app_name)` (delegates to a configured `TrustManager`)
   - If `UNKNOWN`, the service may prompt the user with a `TrustDialog`
   - If `DENIED`, reply with `{"status": "denied", ...}`
   - If `APPROVED`, mint a session capability channel token and reply with `{"status": "approved", "session_token": "...", "session_subject": "_PRIV.<p>.session.<token>", ...}`
3. App sends requests for trusted actions to `{session_subject}.action_name` (with `private_request`, so the reply subject is unguessable) instead of the bare `{prefix}.action_name` (which answers `denied`)
4. Service attaches identity to the request: `data["_identity"] = {"app_name", "session_token"}`

When an app re-authenticates, existing tokens for that app are revoked.

## Registering an Action

```python
from lightfall_utils.ipc import IPCService, ErrorCodes, ok_reply

# Initialize the service (connecting to NATS server at construction)
service = IPCService("nats://localhost:4222", topic_prefix="als.myapp")
service.start()  # Connect to the NATS broker on a background thread

# Define app-specific error codes
app_errors = ErrorCodes("not_found", "validation_failed")

# Register an action
def handle_compute(subject: str, data: dict, reply: str | None) -> None:
    """Process incoming 'compute' action requests."""
    try:
        value = data.get("value")
        if value is None:
            response = app_errors.reply("validation_failed", "Missing 'value'")
        else:
            result = value * 2
            response = ok_reply(result=result)
    except Exception as e:
        response = app_errors.reply("unknown", str(e))
    
    if reply:
        service.reply(reply, response)

handle = service.register_action(
    "compute",
    handle_compute,
    description="Double a numeric value",
    schema={
        "type": "object",
        "properties": {"value": {"type": "number"}},
        "required": ["value"],
    },
)
```

Later, unregister with `handle.unregister()`.

### Trusted (capability-channel-only) actions

Pass `trusted=True` to `register_action()` to make an action reachable only
through a session capability channel (see "Trust Handshake Sequence" above).
A request sent to its bare, non-session subject gets a structured `denied`
error reply instead of reaching the handler; the request must instead be
sent to `{priv_root}.session.{token}.<suffix>` (the `session_subject` from a
completed `auth.request` handshake).

### Private actions

Pass `private=True` to subscribe an action on the service's private root,
`{priv_root}.{suffix}`, instead of `{prefix}.{suffix}`. On bcgnats only this
connection can subscribe there, so only it receives the request and only it
can answer. Use it for pre-handshake endpoints whose payloads must stay private,
above all `auth.request`:

```python
service.register_action("auth.request", handle_auth, private=True)
# subscribed at f"{service.priv_root}.auth.request"
```

`private` and `trusted` are mutually exclusive (`ValueError`).

## Building Auth Responses

Use `build_auth_response()` to construct handshake replies:

```python
# Approved with extra fields (e.g., session metadata)
response = service.build_auth_response(
    approved=True,
    app_name="remote-app",
    extra={"session_id": "s-12345"},
)
# Returns: {"status": "approved", "session_token": "...",
#           "session_subject": "_PRIV.<p>.session.<token>", "session_id": "s-12345", ...}

# Denied
response = service.build_auth_response(
    approved=False,
    reason="Application not registered",
)
# Returns: {"status": "denied", "reason": "Application not registered", ...}
```

## Error Codes

The protocol defines base error codes that all services understand:

```python
from lightfall_utils.ipc import BASE_ERROR_CODES

# BASE_ERROR_CODES = {"busy", "limits", "timeout", "unknown", "denied", "bad_request", "version_mismatch"}
```

Applications extend this set with their own codes:

```python
from lightfall_utils.ipc import ErrorCodes, error_reply

app_codes = ErrorCodes("custom_error_1", "custom_error_2")

# Use in error replies
reply_data = app_codes.reply("custom_error_1", "Something went wrong")
# Returns: {"status": "error", "code": "custom_error_1", "message": "...", "contract_version": 1}
```

## Peer Discovery

Discover all services on the bus:

```python
def on_peers_found(peers):
    """Called with a list of {instance_id, display_name, prefix, is_self} dicts."""
    for peer in peers:
        print(f"Peer: {peer['display_name']} at {peer['prefix']}")

# Non-blocking; callback runs on the Qt main thread
service.discover_peers(on_peers_found, timeout_ms=500)
```

## Events

Register events for meta-discovery (no subscription created; only for catalog):

```python
service.register_event(
    "events.scan.completed",
    description="Emitted when a scan finishes",
    schema={"type": "object", "properties": {"scan_id": {"type": "string"}}},
)

# Later, publish events directly; note that publish() takes the full subject
# (not a suffix), so use service.topic() to apply the prefix
service.publish(service.topic("events.scan.completed"), {"scan_id": "s-789"})
```

## Local Development: LocalNatsServer

For testing without an external NATS broker, use `LocalNatsServer`:

```python
from lightfall_utils.ipc import LocalNatsServer, IPCService

server = LocalNatsServer()
server.start()  # Starts a managed nats-server process

service = IPCService("nats://localhost:4222", topic_prefix="als.test")
service.start()  # Connect to the local server

# ...

service.stop()
server.stop()
```

Requires `pip install lightfall-utils[local-nats]`.

The local broker listens on 127.0.0.1 and has no auth callout: everything on the host is trusted.
`IPCService` still sends its token and inbox prefix, which a plain `nats-server` ignores.

## Trust Management

Use a `TrustManager` to track which applications are trusted:

```python
from lightfall_utils.ipc import TrustManager, TrustState

trust = TrustManager()
service.set_trust_manager(trust)

# In auth handler
state = service.evaluate_trust("remote-app")
if state == TrustState.UNKNOWN:
    # Prompt user with TrustDialog
    from PySide6.QtWidgets import QDialog
    from lightfall_utils.ipc import TrustDialog
    dialog = TrustDialog("remote-app", app_version="1.0", parent=main_window)
    if dialog.exec() == QDialog.Accepted:
        trust.approve("remote-app")
    else:
        trust.deny("remote-app")
elif state == TrustState.APPROVED:
    # Grant access
    pass
elif state == TrustState.DENIED:
    # Reject
    pass
```

## Protocol Contract

All replies carry a `contract_version` field (currently `1`). Applications should check this to detect incompatibilities.
