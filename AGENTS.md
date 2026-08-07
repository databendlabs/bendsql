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
- PR title must follow the Conventional Commits format validated by `amannn/action-semantic-pull-request@v5`.
  Format: `type(scope): description` or `type: description`.
  Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.

## Python Binding Compatibility Tests

- Databend CI can run the latest Python binding tests against older driver versions, and BendSQL keeps a similar compatibility matrix so failures are caught here without blocking the main Databend repository.
- When adding or changing tests in `bindings/python/tests/**/steps/binding.py`, guard queries, API usage, and assertions that depend on a particular driver or server version with the existing `DRIVER_VERSION` and/or `DB_VERSION` checks. Refer to the existing uses of these variables in the `binding.py` files: execute only the version-dependent portion when the tested version supports it, while leaving version-independent coverage unconditional.
- Keep the latest released `databend-driver` version in the `new_test_with_old_drivers` matrix in `tests/nox/noxfile.py` whenever the matrix is updated. Representative older versions may be rotated, but the latest release must not be omitted.
- Set each version boundary to the first release that supports the behavior. Do not make the older-driver/newer-test compatibility jobs require behavior that is only available in newer driver or server releases.

## Task Routing

- Rust behavior, CLI flags, REPL, output, or public Rust API changes: read `agents/rust-workspace.md`
- Browser UI, embedded assets, or frontend dev/proxy flow: read `agents/frontend.md`
- Docker integration tests, Python bindings, or Node.js bindings: read `agents/integration-and-bindings.md`

Some tasks span multiple areas. In that case, read the relevant files before editing.
