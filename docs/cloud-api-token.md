# Connect with a Cloud API token

BendSQL can use a personal API token from the Databend Cloud console instead of
SQL username/password. This is a Cloud-only REST authentication method. It does
not change password, Access Token or Key Pair authentication for private
Databend deployments.

Create/manage the token at <https://app.databend.com/databend/security/api-token>.
The token belongs to your Cloud account, not just the organization shown in the
page URL. The target Gateway selects the tenant; the exchange service checks
that the account is an active member of its organization.

## Recommended usage

Keep the Gateway DSN from the Cloud connection dialog, remove its SQL credentials,
and inject the token from a secret manager or another secure environment source:

```bash
export BENDSQL_DSN='databend://<tenant-gateway>:443/default?warehouse=<warehouse>'
# Inject BENDSQL_CLOUD_API_TOKEN securely, then run:
bendsql
```

The CLI also accepts `--cloud-api-token <TOKEN>`. For Driver/DSN callers:

```text
databend://<tenant-gateway>:443/default?warehouse=<warehouse>&cloud_api_token=<TOKEN>
```

Use URL encoding for DSN parameter values. The CLI token source precedence is:

1. `--cloud-api-token`
2. DSN/configuration `cloud_api_token`
3. `BENDSQL_CLOUD_API_TOKEN`

An explicitly empty token is an error, not a request to fall back. The environment
variable is a CLI feature; library callers should supply the DSN parameter.
Unset it when connecting to a private deployment: a Cloud token on an unsupported
host is rejected rather than silently ignored.

No `--authenticator`, organization slug or separate tenant ID is required.
Database and Warehouse remain normal connection options. When using `--host`
instead of a DSN, also pass `--tls true`.

## SQL identity and conflicting credentials

When `cloud_api_token` is set, DSN username/password and CLI/config password
credentials are ignored; Basic credentials are not sent. A legacy DSN containing
`cloudapp:password@` is accepted, but removing those unused credentials is
recommended.

The SQL user is the Cloud account mapped by the token, such as
`alice@example.com`, not the DSN username `cloudapp`. The default role and visible
objects may therefore differ from the old password connection:

```sql
SELECT current_user(), current_role();
```

Combining `cloud_api_token` with `access_token`, `access_token_file`,
`private_key_file`, or `private_key_passphrase_file` is an error. Authentication or
exchange failures never fall back to another identity. FlightSQL is not supported
by this authentication method.

The exchange service deliberately caps available roles at the current organization
role and its related roles. Additional roles granted directly to the SQL user may
be usable in Worksheet but not with this credential. Do not grant a sensitive role
to `public` as a per-user workaround: that broadens access for all users who can
use `public`.

## Supported endpoints

This initial implementation accepts standard HTTPS Gateway hosts:

```text
<tenant>.gw.<region>.<deployment>.databend.com
<tenant>--<warehouse>.gw.<region>.<deployment>.databend.com
<tenant>.gw.<region>.<deployment>.databend.cn
```

It derives the tenant from the first label and chooses the fixed exchange endpoint:

- Global: `https://api.databend.com/api/v1/auth/token/exchange`
- China: `https://api.databend.cn/api/v1/auth/token/exchange`

Unknown hosts, localhost, HTTP (`sslmode=disable`), and unsupported domain layouts
are rejected before credentials are sent. Azure, custom domains, PrivateLink-only
hostnames, proxies that change the endpoint hostname, and TiDB Cloud Lake are not
automatically inferred. Existing non-Cloud authentication remains available for
those connections; further Cloud endpoint discovery is outside this change.

The original API token is sent **only to the fixed Cloud exchange endpoint**,
with `X-DatabendCloud-Token` and a JSON `tenant_id`. It is never sent as a Gateway
password or Gateway header. Exchange requests have no console cookies, do not
follow redirects, have a 30-second total deadline, and reject oversized responses.
SQL JWTs are restricted to the original Gateway origin; automatic redirects are
disabled for Cloud-token connections. Presigned object-storage requests retain
their existing independent authentication.

## Exchange and renewal

```text
API token + target tenant -> Cloud exchange -> short-lived SQL JWT
BendSQL -> existing Gateway REST API using that JWT
```

Cloud must deploy the token-exchange API before this method can work. A successful legacy `/v1/query` WebAPI proxy call does not prove
that the exchange endpoint is available. An undeployed or rejected exchange
endpoint causes a clear error, not a switch to the Worksheet query proxy.

The client validates the response envelope, tenant, identity metadata, Bearer type
and expiry. It uses the shorter of `expires_in` and `expires_at`, accounting for
exchange latency, and renews before expiry (normally 60 seconds early). Concurrent
requests on one auth object share renewal; connection pools using the same Gateway
and token share it through a bounded process-local registry of weak references.
No background refresh task or refresh token is required. The raw API token is not
retained by that registry after the last connection drops.

Renewal happens before sending authenticated requests, including login, query
start, pages, heartbeat, uploads and logout. A rejected or timed-out SQL request is
not automatically resubmitted *because of* Cloud credential renewal; existing
transport/503 retry behavior is unchanged. Active sessions and transactions are
not recreated to refresh authentication.

If renewal fails, the operation fails rather than silently using another
credential. Exchange HTTP errors preserve status (including 401, 403, 404, 429 and
5xx) without echoing upstream bodies or secrets. There is no automatic exchange
retry loop; callers receiving 429 should back off and respect the reported
Retry-After. Disabling an API token blocks future exchanges, not necessarily
already issued JWTs or existing server sessions immediately.

CLI argument masking covers the token flag, DSN and `--set` forms, but cannot
prevent every observation before process startup. Prefer secret injection over
literal shell commands, avoid printing DSNs, and treat a JWT saved to disk as a
secret.

## Verification

Unit tests use local HTTP mocks, not real accounts, API tokens or a Cloud cluster.
They cover target validation, credential conflicts, identity selection, concurrent
refresh, expiration, malformed/error responses, redirects, request credential
routing, no authentication replay of SQL, CLI precedence and masking. Production
rollout still needs a dedicated test account to verify deployed Cloud exchange,
Gateway role enforcement and renewal.
