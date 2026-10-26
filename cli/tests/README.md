# Cloud WebAPI integration tests

## Ordinary PR checks: no Cloud credentials

`cloud_api_token.rs` checks CLI credential selection and rejected combinations.
`cloud_webapi_https.py` runs the real CLI against a local HTTPS server through a
loopback CONNECT proxy. The CLI still uses the official API hostname/443 and
production origin checks; the proxy never forwards traffic. Temporary test
certificates cover the official hostnames and are trusted only by the child CLI
via `tls_ca_file`, without modifying DNS/hosts or the system trust store. Tests
cover certificate verification, query/page/final routing, retries and private-auth
isolation. `test_cloud_webapi_e2e.py` tests the live harness with mocked subprocess
results. These tests run on PRs without production credentials or Docker:

```bash
cargo test -p bendsql --test cloud_api_token --all-features
cargo build -p bendsql --bin bendsql
python3 cli/tests/cloud_webapi_https.py target/debug/bendsql
python3 -m unittest discover -s cli/tests -p 'test_cloud_webapi_e2e.py' -v
```

## Live Cloud E2E: trusted main only

Configure repository Actions Secrets in `databendlabs/bendsql`:

- `BENDSQL_CLOUD_API_TOKEN`: a dedicated, least-privilege Cloud test account token.
- `BENDSQL_ORG`: that account's test organization slug.

The test explicitly selects Warehouse `default` and the default global WebAPI
endpoint. The account must have permission to query that Warehouse. Do not use a
personal administrator or production account; Cloud API tokens may reach every
organization to which their account belongs. Keep the account out of production
organizations and rotate the token regularly.

### Manual entry

Once `.github/workflows/cloud.e2e.yml` is on the default branch:

1. Open the repository's **Actions** tab.
2. Select **Cloud WebAPI E2E**, then **Run workflow**.
3. Select **main**, start the workflow and inspect its pass/fail results.

Equivalent CLI command:

```bash
gh workflow run cloud.e2e.yml --repo databendlabs/bendsql --ref main
```

The entry will not appear in the default-branch UI until the workflow is merged.
This configuration does not merge, deploy, rotate credentials or run live tests
as part of an ordinary PR.

### Daily entry

**Daily Integration Tests** reuses the same Cloud job on its existing daily
schedule and when manually dispatched on upstream `main`. It uses the same two
Secrets and `default` Warehouse, independently of the Docker/bindings jobs, and
participates in the existing failure notification. It therefore consumes real
Cloud compute after this configuration is merged; use a small test Warehouse.
Its PR-triggered YAML checks skip the live job. If either Secret is absent, a
scheduled run emits a notice and skips checkout/build/testing instead of causing
a recurring failure notification. A manual run without the Secrets fails clearly.
There is no confirmation checkbox: configuring the Secrets and enabling the daily
job is the opt-in, not a hard-coded caller confirmation.

### What is tested

Only bounded, read-only SQL is submitted:

- Scalar/string result decoding.
- Nonempty `current_user()` / `current_role()` results (identity values withheld).
- Complete, ordered retrieval of 5,000 generated rows. This checks the result
  stream but does not assume a particular server page size.
- Propagation of an intentional unknown-column SQL error.

Each CLI invocation has a 90-second deadline. No tables, grants or resources are
created or modified. WebAPI can still record query history/results and resume the
Warehouse as it does for normal queries.

### Credential boundary

- No `pull_request_target`, arbitrary checkout-ref input, or PR build artifacts.
  Both caller and reusable job require upstream `main` and a manual/scheduled
  event; organization membership or a PR review alone does not enable live tests.
- Checkout uses the event's exact commit with Git credentials not persisted.
  Cloud Secrets are injected only into the final test step, not checkout/build.
- The CLI receives token/Org via environment, not argv. A fresh HOME and filtered
  environment prevent inherited DSNs, custom origins or other credentials from
  changing the test target. Captured output, identity and full server errors are
  never printed or uploaded as artifacts.
- The workflow token has read-only repository contents permission. There is no
  additional Environment approval gate configured here. For enforced extra
  approval, move these credentials from repository Secrets to a protected GitHub
  Environment with required reviewers and bind the live job to that Environment.
  Leaving duplicate repository Secrets would let other workflows bypass that gate.

GitHub Secrets and log masking are not a sandbox. Trusted workflow code and the
built executable can read the token during the test. These safeguards exclude
unreviewed PR execution, but do not defend against malicious trusted-main code,
a compromised dependency/runner, or privileged repository maintainers. Repository
branch protection, least privilege and credential rotation remain necessary.
