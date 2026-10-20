"""Reference service for the services pattern (Qt-free).

Run it with ``python -m lightfall_utils.ipc.services.echo --scope S --nats-url URL``
(plus ``--token-stdin`` when launched locally by Lightfall).  It offers:

- ``rpc echo {...}`` -> ``{"echo": {...}}``
- ``rpc slow {seconds}`` -> sleeps, then ``{"slept": seconds}``
- stream kind ``bytes``: counts the bytes and chunks of a client-to-service stream and, when
  the stream ends (final chunk or ``rpc.stream.close``), emits the per-session event
  ``stream_total {stream_id, bytes, chunks}`` to the session's delivery root.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

from lightfall_utils._version import __version__
from lightfall_utils.ipc.services.cli import run_service
from lightfall_utils.ipc.services.host import ServiceHost, Session

NAME = "echo"
CONTRACT = "1.0"
DEFAULT_SCOPE = "lightfall.local"
MAX_SLOW_S = 60.0


def make_host(scope: str = DEFAULT_SCOPE, **kw: Any) -> ServiceHost:
    """Build the echo :class:`ServiceHost` (not started); ``kw`` go to ``ServiceHost``."""
    kw.setdefault("contract", CONTRACT)
    kw.setdefault("version", __version__)
    host = ServiceHost(NAME, scope, **kw)
    totals: dict[str, list[int]] = {}

    @host.rpc("echo")
    async def echo(session: Session, payload: dict) -> dict:
        return {"echo": payload}

    @host.rpc("slow")
    async def slow(session: Session, payload: dict) -> dict:
        seconds = payload.get("seconds", 1.0)
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise ValueError("seconds must be a number")
        if not 0 <= seconds <= MAX_SLOW_S:
            raise ValueError(f"seconds must be within [0, {MAX_SLOW_S}]")
        await asyncio.sleep(seconds)
        return {"slept": seconds}

    @host.stream("bytes")
    async def on_bytes(
        session: Session, stream_id: str, seq: int, data: bytes, final: bool
    ) -> None:
        live = host.streams
        for sid in [s for s in totals if s != stream_id and s not in live]:
            del totals[sid]  # dropped (idle / session end) streams
        t = totals.setdefault(stream_id, [0, 0])
        if data or not final:
            t[0] += len(data)
            t[1] += 1
        if final:
            del totals[stream_id]
            await host.emit(
                session, "stream_total", {"stream_id": stream_id, "bytes": t[0], "chunks": t[1]}
            )

    return host


def main(argv: list[str] | None = None) -> int:
    return run_service(make_host(), argv)


if __name__ == "__main__":
    sys.exit(main())
