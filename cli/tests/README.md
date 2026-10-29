# Cloud WebAPI integration tests

## Ordinary PR checks: no Cloud credentials or Docker

Resolver unit tests own the CLI option/credential matrix. `cloud_api_token.rs`
checks real process, environment and pre-startup rejection boundaries.
`cloud_webapi_https.py` checks the actual CLI using a temporary-CA HTTPS mock and
loopback-only CONNECT proxy for the official host/443. It never forwards traffic,
changes DNS/hosts or disables certificate/origin checks. Coverage includes page
retries/final, opaque session rotation, no Cookie fallback, logout and native auth.
`test_cloud_webapi_e2e.py` tests the live harness with mocked subprocesses.

```bash
cargo test -p bendsql --test cloud_api_token --all-features
cargo build -p bendsql --bin bendsql
python3 cli/tests/cloud_webapi_https.py target/debug/bendsql
python3 -m unittest discover -s cli/tests -p 'test_cloud_webapi_e2e.py' -v
```

## Live Cloud E2E: trusted upstream main only

The manual **Cloud WebAPI E2E** workflow and the Cloud job in **Daily Integration
Tests** use these repository Actions Secrets in `databendlabs/bendsql`:

- `BENDSQL_CLOUD_API_TOKEN`: a dedicated least-privilege test account token.
- `BENDSQL_DSN`: `databend://api.databend.com/default?org=<ORG>`; use
  `api.databend.cn` for China. No credentials or custom options in this DSN.

The harness validates the official target and pins Warehouse `default`, retry
count and disabled login. The account needs query and temporary-table permissions
in that database. Keep it out of production organizations: account tokens can
reach every organization their account belongs to. These jobs consume real Cloud
compute, including Warehouse auto-resume; use a small test Warehouse.

Once the workflow is on the default branch, select **Actions → Cloud WebAPI E2E →
Run workflow → main**, or run:

```bash
gh workflow run cloud.e2e.yml --repo databendlabs/bendsql --ref main
```

Daily Integration Tests reuses the same job for its schedule/manual runs; PR jobs
skip live execution. Missing Secrets skip scheduled runs with a notice, but fail
manual runs clearly. Configuring the Secrets and enabling the daily job is the
opt-in; there is no confirmation checkbox or protected-Environment approval gate.
This documentation does not merge/deploy code or configure/rotate Secrets.

### Coverage and side effects

The suite checks scalar/string decoding, nonempty SQL identity (values withheld),
5,000 ordered rows without assuming page size, read-only transaction commit and
rollback, one randomly named temporary table across requests, and an intentional
unknown-column error. Each CLI invocation has a 90-second deadline. No persistent
tables, grants or Cloud resources are created. The temporary table is dropped on
success; abnormal termination relies on session cleanup/server expiry. Cloud can
still record query history and retain results.

### Credential boundary

- Caller and reusable workflow require upstream `main` and manual/scheduled
  events. No `pull_request_target`, arbitrary checkout ref or PR build artifact.
- Checkout pins the event SHA without persisting Git credentials. Cloud Secrets
  enter only the final test step, not checkout/build. The CLI receives credentials
  via environment, with fresh HOME and filtered inherited connection settings.
- Captured output, identity and full server errors are never printed or uploaded.
  The workflow GitHub token has read-only contents permission.
- If extra approval is required, move credentials to a protected GitHub
  Environment with reviewers and bind the job to it; duplicate repository Secrets
  would bypass that gate.

Secrets/log masking are not a sandbox: trusted-main code, dependencies and the
runner can read credentials. These controls exclude unreviewed PR execution, not
compromised trusted code or privileged maintainers. Branch protection, least
privilege and credential rotation remain necessary.
