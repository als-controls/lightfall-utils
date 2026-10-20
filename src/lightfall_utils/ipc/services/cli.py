"""Qt-free standard command line for a :class:`ServiceHost` process.

``run_service(host, argv)`` parses ``--scope``, ``--nats-url``, ``--token-stdin`` and
``--log-level``, connects, starts the host and serves until one of: ``rpc.shutdown``,
SIGTERM, SIGINT (Ctrl-C), or stdin EOF when ``--token-stdin`` is given.

Exit codes: 0 clean shutdown, 1 broker unreachable or startup failure (or the broker
connection closed for good), 2 bad arguments or a bad stdin token, 3 another instance already
serves the root (single-instance services).

Broker loss: a locally launched service (``--token-stdin``) reconnects forever, so it
survives its host-local broker being restarted (it still exits on stdin EOF).  Any other
service exits with code 1 when nats-py gives up reconnecting, so its supervisor (systemd)
can restart it instead of leaving a process that serves nothing.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import threading
from collections.abc import Callable

from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.host import InstanceConflictError, ServiceHost

log = logging.getLogger(__name__)

DEFAULT_NATS_URL = "nats://127.0.0.1:4223"
CONNECT_TIMEOUT_S = 10.0  # nats-py retries an unreachable broker for minutes otherwise
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_CONFLICT = 3

_LEVELS = ("debug", "info", "warning", "error", "critical")


def build_parser(host: ServiceHost) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"{host.name}-service", description=f"Run the {host.name!r} service over NATS."
    )
    p.add_argument("--scope", default=host.scope, help="subject scope (default: %(default)s)")
    p.add_argument("--nats-url", default=DEFAULT_NATS_URL, help="broker (default: %(default)s)")
    p.add_argument(
        "--token-stdin",
        action="store_true",
        help="read the session token as one line from stdin; exit on stdin EOF",
    )
    p.add_argument("--log-level", default="info", type=str.lower, choices=_LEVELS)
    return p


def run_service(host: ServiceHost, argv: list[str] | None = None) -> int:
    """Run ``host`` as a process with the standard service CLI; returns the exit code."""
    try:
        args = build_parser(host).parse_args(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else EXIT_USAGE
    logging.basicConfig(
        level=args.log_level.upper(),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        host.configure(scope=args.scope)
        if args.token_stdin:
            host.configure(stdin_token=sys.stdin.readline().strip())
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE
    return asyncio.run(_serve(host, args))


async def _serve(host: ServiceHost, args: argparse.Namespace) -> int:
    lost = {"closed": False, "stopping": False}

    def on_closed() -> None:
        if lost["stopping"]:
            return  # our own close at exit
        log.error("broker connection closed for good; stopping")
        lost["closed"] = True
        host.request_stop()

    core = NatsCore(
        args.nats_url,
        name=f"{host.name}-service",
        max_reconnect_attempts=-1 if args.token_stdin else 60,
        on_closed=on_closed,
    )
    try:
        await asyncio.wait_for(core.connect(), CONNECT_TIMEOUT_S)
    except Exception as e:
        print(f"error: cannot connect to {args.nats_url}: {e!r}", file=sys.stderr)
        return EXIT_FAILED
    loop = asyncio.get_running_loop()
    restore: Callable[[], None] = lambda: None  # noqa: E731
    try:
        try:
            await host.start(core)
        except InstanceConflictError as e:
            print(f"error: {e}", file=sys.stderr)
            return EXIT_CONFLICT
        if host.state == "starting" and host.auto_ready:
            host.state = "ready"
        log.info("%s serving %s (instance %s)", host.name, host.root, host.instance_id)
        restore = _install_signal_handlers(loop, host)
        if args.token_stdin:
            threading.Thread(
                target=_watch_stdin, args=(loop, host), name="stdin-eof", daemon=True
            ).start()
        await host.serve_forever()
        log.info("%s stopped", host.name)
        return EXIT_FAILED if lost["closed"] else EXIT_OK
    finally:
        restore()
        await host.stop()
        lost["stopping"] = True
        await core.close()


def _stop_from_thread(loop: asyncio.AbstractEventLoop, host: ServiceHost) -> None:
    try:
        loop.call_soon_threadsafe(host.request_stop)
    except RuntimeError:
        pass  # loop already closed


def _watch_stdin(loop: asyncio.AbstractEventLoop, host: ServiceHost) -> None:
    try:
        while sys.stdin.readline():
            pass
    except (OSError, ValueError):
        pass
    log.info("stdin closed; stopping")
    _stop_from_thread(loop, host)


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop, host: ServiceHost
) -> Callable[[], None]:
    """Route SIGINT/SIGTERM to ``host.request_stop``; returns a function restoring the old ones.

    Uses :func:`signal.signal` (not ``loop.add_signal_handler``) so it also works on Windows,
    where SIGINT is Ctrl-C; SIGTERM cannot be delivered to a Windows process by another
    process, so there ``rpc.shutdown`` or stdin EOF are the graceful paths.
    """
    previous: dict[int, object] = {}

    def on_signal(signum: int, frame: object) -> None:
        log.info("signal %d; stopping", signum)
        _stop_from_thread(loop, host)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[sig] = signal.signal(sig, on_signal)
        except (ValueError, OSError):  # not the main thread, or unsupported
            pass

    def restore() -> None:
        for sig, h in previous.items():
            try:
                signal.signal(sig, h)  # type: ignore[arg-type]
            except (ValueError, OSError, TypeError):
                pass

    return restore
