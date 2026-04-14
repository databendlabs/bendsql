# AGENTS.md

Read this file first. Then open only the relevant file(s) under `agents/`.

This repository is a Rust workspace for BendSQL plus a separate frontend, language bindings, and Docker-based integration tests.

## Repo Map

- `cli/`: `bendsql` CLI, REPL, output formatting, and web UI host
- `core/`: low-level Databend REST client
- `driver/`: public Rust driver API and query execution layer
- `sql/`: shared SQL/value decoding and encoding logic
- `macros/`: proc macros used by the Rust driver stack
- `frontend/`: editable browser UI source
- `cli/frontend/`: generated frontend assets embedded by the CLI; do not edit by hand
- `bindings/python/`: Python client bindings
- `bindings/nodejs/`: Node.js client bindings
- `tests/`: Docker-based integration test harness
- `ttc/`: tcp test container utilities

## Global Rules

- Prefer source edits over generated output.
- Do not manually edit `cli/frontend`; rebuild it from `frontend/`.
- Keep changes inside the owning subsystem unless the task clearly crosses boundaries.
- Check for uncommitted user changes before editing and do not revert unrelated work.
- Report the exact verification commands you ran, plus anything you skipped and why.

## Task Routing

- Rust behavior, CLI flags, REPL, output, or public Rust API changes: read `agents/rust-workspace.md`
- Browser UI, embedded assets, or frontend dev/proxy flow: read `agents/frontend.md`
- Docker integration tests, Python bindings, or Node.js bindings: read `agents/integration-and-bindings.md`

Some tasks span multiple areas. In that case, read the relevant files before editing.
