# lightfall-utils

Shared Qt/EPICS infrastructure for ALS control applications (Lightfall, CAtfish).

Extracted from [Lightfall](https://git.als.lbl.gov/ncs/lightfall). Modules:

- `lightfall_utils.threads` — managed Qt thread pool, `QThreadFuture`, main-thread marshalling
- `lightfall_utils.logging` / `log_buffer` — loguru configuration, timing, in-process log ring buffer
- `lightfall_utils.qt_affinity` — GUI-thread assertion helpers (`gui_thread_only`)
- `lightfall_utils.config` — priority-layered YAML config with pydantic validation
- `lightfall_utils.theming` — semantic design tokens, theme registry/manager, QSS generation
- `lightfall_utils.ca` — caproto → Qt signal bridge (`SharedContext`, `PV`); requires the `ca` extra
- `lightfall_utils.caproto_shutdown` — drains caproto's user-callback thread pools cleanly at application shutdown

Install: `pip install lightfall-utils[ca]` (not yet on PyPI — pending LBNL software disclosure). Add the `multihomed` extra alongside `ca` for caproto multi-homed interface enumeration (`pip install lightfall-utils[ca,multihomed]`).
Dev: `uv venv; uv pip install -e ".[dev]"; uv run pytest`
