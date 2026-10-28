# Databend Driver Binding Tests

## Cloud DSN regressions (no credentials or Docker)

Build/install the current Python wheel or Node addon first, then from the repo root:

```bash
python3 bindings/tests/cloud_dsn.py --python bindings/python/.venv/bin/python
python3 bindings/tests/cloud_dsn.py --node node
```

Equivalent Make targets are `make -C tests test-cloud-bindings-python` (prepares
its local venv) and `make -C tests test-cloud-bindings-nodejs` (requires the current
addon, built with `pnpm run build:debug` in `bindings/nodejs` after `pnpm install`,
with `NAPI_TARGET` set to the Rust host triple).
CI uses these same targets with the freshly downloaded wheel/addon.

The shared harness runs the actual Python blocking/asyncio/cursor APIs and Node
`Client`, not a Rust-only substitute. It reuses the CLI's temporary-CA HTTPS
fixture through a loopback-only, non-forwarding CONNECT proxy for the official
hosts. It does not change DNS/hosts or disable TLS/origin checks.

Coverage includes global/China/default/case-insensitive endpoints, DSN token and
org decoding, ignored SQL userinfo, two isolated connections per client with two
queries each, page retries, GET final, session-header rotation, unchanged private
authentication, and errors for missing/duplicate/competing credentials or
unsupported transports/origins.
Ambient `BENDSQL_*` credentials must not affect either Driver. Worker inputs go
through stdin and failures withhold credential/session-bearing diagnostics.

Both binding CI workflows run this suite against their freshly built artifacts.
It is separate from the old-driver/server BDD compatibility matrix: no unreleased
version boundary is guessed, and that matrix's `latest` entry remains unchanged.

## Shared BDD suite

Databend Driver will use [Cucumber](https://github.com/cucumber) for [BDD](Behaviour-Driven Development) tests. This module will provide [Gherkin](https://cucumber.io/docs/gherkin/) files and all bindings should implement againest them.
