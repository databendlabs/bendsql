# Rust Workspace

Use this guide for work in `cli/`, `core/`, `driver/`, `sql/`, `macros/`, and `ttc/`.

## Ownership

- `cli/`: CLI flags, REPL behavior, output formatting, local web server wiring
- `core/`: low-level Databend REST client behavior
- `driver/`: public Rust driver API, query execution, parameter binding
- `sql/`: shared SQL parsing/value decoding/encoding logic
- `macros/`: proc-macro behavior
- `ttc/`: test-container utilities; touch only when the task is explicitly about TTC

## Change Routing

- CLI UX changes usually belong in `cli/`.
- Protocol behavior, row decoding, type mapping, and parameter binding usually belong in `driver/`, `sql/`, or `core/`.
- Do not move reusable logic into `cli/` if bindings or driver users should share it.
- If a change affects a public API or observable query behavior, check whether bindings or docs also need updates.

## Validation

- Prefer targeted Rust validation first when the scope is narrow.
- For general Rust changes, run `make test`.
- For broad or user-facing changes, run `make check`.
- This repo ignores `Cargo.lock`. If local Rust results differ from CI and the change may depend on dependency resolution, check whether a stale local `Cargo.lock` is masking the issue. Regenerate or remove it only as a targeted troubleshooting step, not as a default workflow.

`make check` currently runs:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo deny check`
- `cargo machete`
- `hawkeye check`
- `typos`

If local tooling is missing, report that explicitly instead of silently skipping it.

## Response Expectations

- Summarize the behavior change, not just the files touched.
- List the exact test or check commands that ran.
- If you skipped validation, say what was skipped and why.
