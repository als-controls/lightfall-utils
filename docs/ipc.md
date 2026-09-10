# IPC: NATS-backed Inter-Process Communication

The `lightfall_utils.ipc` module provides a Qt-integrated IPC service built on NATS, with trust handshaking, per-session capability channels, and a structured reply protocol.

## Overview

An `IPCService` manages a connection to a NATS broker, publishes and subscribes to a topic tree, handles request/reply patterns for actions, and mints per-session capability tokens for trusted applications. The protocol uses JSON and versioning to ensure compatibility across services.

## Topic Structure

All user-visible subjects live under a configurable prefix (e.g., `"als.lightfall"`). Well-known discovery uses no prefix:

- **Auth handshake:** `{prefix}.auth.request` — app sends `{"app_name", "version"}`, service replies with approval/denial and session token
- **Meta discovery (no auth required):** `{prefix}.meta.actions` — list registered actions; `{prefix}.meta.events` — list registered events
- **Peer discovery (no prefix):** `_lightfall.discover` — scatter-gather request/reply to find all live services
- **Session capability channels:** `{prefix}.session.{token}.>` — per-app capability subject after successful auth; routes to trusted actions with identity attached

## Trust Handshake Sequence

1. App sends a request to `{prefix}.auth.request` with `{"app_name", "version"}` and a reply subject
2. Service evaluates trust via `evaluate_trust(app_name)` (delegates to a configured `TrustManager`)
   - If `UNKNOWN`, the service may prompt the user with a `TrustDialog`
   - If `DENIED`, reply with `{"status": "denied", ...}`
   - If `APPROVED`, mint a session capability channel token and reply with `{"status": "approved", "session_token": "...", ...}`
3. App uses the session token in requests to trusted actions: sends on `{prefix}.session.{token}.action_name` instead of the bare `{prefix}.action_name`
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

## Building Auth Responses

Use `build_auth_response()` to construct handshake replies:

```python
from lightfall_utils.ipc import ok_reply

# Approved with extra fields (e.g., session metadata)
response = service.build_auth_response(
    approved=True,
    app_name="remote-app",
    extra={"session_id": "s-12345"},
)
# Returns: {"status": "approved", "session_token": "...", "session_id": "s-12345", ...}

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
