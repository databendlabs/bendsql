# Integration And Bindings

Use this guide for Docker-based integration tests and for work in `bindings/python/` or `bindings/nodejs/`.

## Integration Harness

- Integration commands are slower and more side-effectful than unit tests.
- `tests/Makefile` starts Docker Compose services.
- The setup may append `127.0.0.1 minio` to `/etc/hosts` if it is missing.
- Prefer the narrowest integration target that matches the change.

## Integration Commands

- `make integration-core`
- `make integration-driver`
- `make integration-bendsql`
- `make integration-bindings-python`
- `make integration-bindings-nodejs`
- `make integration` only when multiple layers changed or broad final validation is needed

## Bindings Guidance

- Treat Python and Node.js bindings as separate deliverables.
- Do not assume a Rust-layer fix is complete if the same behavior is exposed through bindings.
- For externally visible API or type-mapping changes, inspect whether bindings need matching updates or verification.
- Python binding integration tests run with `behave`.
- Node.js binding integration tests run with `pnpm run test`.

## Response Expectations

- Call out any Docker or local-environment prerequisites for the commands you ran.
- If integration or binding validation was skipped, say exactly what was skipped and why.
