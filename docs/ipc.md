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

## Services

`lightfall_utils.ipc.services` is a Qt-free asyncio pattern for out-of-process services: a
`ServiceHost` inside the service, a `ServiceClient` inside the caller (Lightfall wraps it with
the `ServiceManager`; `lightfall_utils.ipc.services.qt` is the only Qt module). It builds on the
connection capability above. The handshake, every command, every per-session event and every
stream chunk travel on `_PRIV.*` subjects. Four kinds of subject are public under the scope's
root: discovery, `meta.info`, the heartbeat, and `events.<name>` broadcasts. Anyone on the
broker can read or forge a public subject, so it never carries session data. See the Lightfall
[NATS use model](https://github.com/als-controls/lightfall/blob/master/docs/developer-guide/nats-use-model.md)
for the security rationale.

| Subject | Kind |
|---|---|
| `{scope}.svc.{service}.meta.info`, `{scope}.svc.discover` | public request/reply |
| `{scope}.svc.{service}.heartbeat` | public event (state, summary) |
| `{scope}.svc.{service}.events.<name>` | public broadcast (`host.publish_event`) |
| `{priv_root}.auth.request` | private handshake |
| `{priv_root}.session.{token}.rpc.<action>` | private commands |
| `{priv_root}.session.{token}.stream.{stream_id}` | private client-to-service chunks |
| `{client_priv_root}.d.{delivery_token}.<name>` | private per-session events (`host.emit`) |
| `{client_priv_root}.d.{delivery_token}.stream.{stream_id}` | private service-to-client chunks |

### Host

```python
from lightfall_utils.ipc.services.host import ServiceError, ServiceHost
from lightfall_utils.ipc.services.cli import run_service

host = ServiceHost("echo", "lightfall.local", contract="1.0", version="0.1")

@host.rpc("echo")
async def echo(session, payload: dict) -> dict:
    return {"echo": payload}

run_service(host)   # python -m ... --scope S --nats-url URL [--token-stdin]
```

`lightfall_utils.ipc.services.echo` is the reference service (`echo`, `slow`, and a `bytes`
stream that emits a `stream_total` event). Use `host.emit(session, name, payload)` for
per-session events. `name` may be dotted, for example `utterance.transcript`, but may not start
with `stream`. Each event carries `instance_id` and a per-session `seq`. Use
`host.emit_chunk(...)` for binary chunks and `host.publish_event(name, payload)` for public
broadcasts.

An rpc handler (sync or async) returns a dict, or raises `ServiceError(code, message)` to
send a structured error such as `busy`. Any other exception becomes an `unknown` error reply.

Concurrency: rpc handlers run as tasks. Each stream has its own ordered queue and task, so a
slow stream handler, such as a transcription on the final chunk, never delays another
session's lease ping or rpc. A `stream.close` is queued behind the chunks published before it,
and it is answered as soon as the final call is queued.

Streams can be opened by the built-in `rpc.stream.open {kind, params}` or by service code:

```python
@host.stream("audio", on_open=lambda session, params: None)  # raise ServiceError to refuse
async def on_audio(session, stream_id, seq, data, final): ...

@host.rpc("utterance.start")
async def start(session, payload):
    if busy():
        raise ServiceError("busy", "an utterance is already active")
    return {"stream_id": host.open_stream(session, "audio", payload)}

# finalize after pending chunks / discard now
await host.end_stream(stream_id, final=True)
await host.end_stream(stream_id)

# once per stream: final | close | cancel | idle | session_end | stop
host.on_stream_end(lambda session, stream_id, reason: ...)
```

Startup: let `run_service` start the host first. Load heavy resources (models) in the
background, and report progress with `host.set_state(state, summary)`, which publishes a
heartbeat right away. Pass `ServiceHost(auto_ready=False)` to keep `state="starting"` until
you call `set_state("ready")`. A host with a stdin token, which is what a locally launched
service has, skips the single-instance probe. Its scope already contains the launching
client's instance id.

### Client

```python
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.client import ServiceClient
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor

core = NatsCore("nats://127.0.0.1:4223")
await core.connect()
desc = ServiceDescriptor(name="echo", scope="lightfall.local", contract="1.0", launch="external")
client = ServiceClient(desc, core, scope="lightfall.local", app_name="me", app_version="1.0")
client.on_state(print)                 # unknown/starting/up/degraded/down/incompatible/unauthorized
client.on_event("stream_total", print)
client.on_info(print)                  # service-reported state/summary changed
client.on_session_reset(print)         # lease lost on the same instance; dropped stream ids
client.on_public_event("state", print) # {root}.events.state
await client.start()
reply = await client.call("echo", {"x": 1})   # {"status": "ok", "echo": {"x": 1}, ...}
await client.close()
```

The client checks `svc_protocol` (exact) and `contract` (same major, minor at least the
descriptor's), performs the handshake, then leases the session with a ping every 5 s. The
service expires a session after 30 s without one. Heartbeats are every 2 s; one missed beat is
`degraded`, three is `down`.

Leases and resets: one slow ping is tolerated. The client handshakes again after 3 timed-out
pings in a row, or after `LEASE_S - PING_S` (25 s) without a successful one. It also handshakes
again when beats resume after a pause longer than the lease. On the same instance that fires
`on_session_reset(dropped_stream_ids)`; for a new instance it fires `on_restarted`. Both mean
"resync": the service no longer knows your streams or settings.

Streams: `open_stream(kind, params)` uses the built-in rpc. A stream opened through a
service's own rpc must be registered with `adopt_stream(stream_id, kind=None)`; chunks of a
stream with an unknown kind carry no `Svc-Kind` header. `send_chunk` raises
`StreamClosedError` for an id that is not open here (never opened or adopted, already finished,
or lost in a reset or restart). `call()` never raises for transport problems. It returns an
error dict: `timeout`, or `unknown` for a closed connection or no responders.

After a launcher has started the process, call `client.expect_launch()`. Until the first
instance answers, a missing responder then reports `starting`, not `down`.

The Qt adapter (`QtServiceClient`) re-emits all of this on the main thread as the signals
`state_changed`, `restarted`, `session_reset(list)`, `info_changed(dict)`, `event(str, dict)`,
`public_event(str, dict)`, `stream_chunk` and `connection_changed(bool)`. Register event names
with `watch_event` / `watch_public_event`.

### CLI flags

`run_service` accepts `--scope`, `--nats-url`, `--token-stdin` and `--log-level`.
`--token-stdin` reads the session token from one stdin line and exits on stdin EOF; Lightfall
uses it when it launches the service. The service also exits on `rpc.shutdown`, SIGTERM or
SIGINT. With `--token-stdin` the broker connection reconnects forever, so a local child
survives a restart of its host-local broker. Without it, the process exits with code 1 when
nats-py gives up reconnecting, so systemd can restart it.

### Descriptor

`ServiceDescriptor(name, scope, contract, launch, command, stop_with_client, single_instance,
interpreter_pref, hint)`. `launch` is either `"local"` or `"external"`:

- `"local"`: Lightfall spawns the service. `command` omits the interpreter, for example
  `("-m", "lightfall_utils.ipc.services.echo")`.
- `"external"`: the service is already running, and Lightfall reaches it over the main broker.

`scope` may use `{prefix}`, `{hostname}` and `{client_instance}`. Every resolved token must be
a non-empty literal subject token (no `*`, `>` or whitespace), and the first token may not be
`_INBOX` or `_PRIV`. A local scope is `{prefix}.host.{hostname}.{client_instance}`.

### Local broker and supervision

Local services use a private broker on `127.0.0.1:4223` (preference `services_local_port`),
over plain `nats://`. Each Lightfall starts it if nothing answers on that port. It has no auth
callout, so everything on the host is trusted. External services go through the main broker
and its callout. Lightfall's `ServiceManager` adds three kinds of recovery:

- It restarts a local child that crashes, with a 1, 2, 5, 10 s backoff, and gives up after 5
  exits in 60 s.
- It kills and restarts a child that had been `up` and then stopped heartbeating while its
  process kept running.
- If the local broker goes away, it starts the broker again on the same port. This covers its
  own broker crashing and another Lightfall's broker exiting. Clients and children reconnect
  to the new broker by themselves.

### Security notes

- Every request to a `_PRIV.*` subject uses `capability.private_request`; never `nc.request`.
- The service refuses an `auth.request` whose `client_priv_root` prefix differs from the one in
  the request's reply subject, so a client cannot redirect deliveries to someone else.
- Session and delivery tokens are unguessable subject tokens. On bcgnats, the auth callout's
  per-connection permissions are what stop a spy from subscribing to another connection's
  `_PRIV.<p>.>`. Public subjects (heartbeat, `meta.info`, `events.*`) are forgeable, and a
  forged `meta.info` can impersonate a service (an accepted gap until CSM registration).
- `tests/ipc/services/test_replica_isolation.py` runs against the `bcgnats-auth-callout`
  replica (`go run ./cmd/replica --port 4299`, then `BCGNATS_REPLICA_URL=nats://127.0.0.1:4299`).
  It checks two things, and only these:
  - Subscribing to the service's `_PRIV.<p>.>` from another connection is refused with a
    permissions violation.
  - An event published to the client's `_PRIV` with a guessed delivery token does not reach
    the client's handler, while the genuine one does.

  It does not show that every subject is isolated. It also cannot tell a refused forged
  publish from one that the client ignored.
- On the local broker none of this holds: any process on the host can read and publish
  everything.

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
