# Connect to Cloud WebAPI with an API token

Use a personal Cloud API token and organization **slug** (the console's
Organization ID, not its display name). BendSQL connects directly to WebAPI;
no Gateway address, tenant ID, browser login or client-side JWT exchange is needed.
WebAPI determines the Cloud account, SQL identity, permissions and revocation policy.

## Usage

Inject the token from a secret manager, then select the organization:

```bash
export BENDSQL_CLOUD_API_TOKEN='<TOKEN>'
export BENDSQL_ORG='org-example'
bendsql
```

With default connection settings this uses `https://api.databend.com:443`.
For China, add `--host api.databend.cn`. Select a Warehouse with
`--set warehouse=my-warehouse`; otherwise WebAPI applies its default.
Without an explicit `--org`, saved host, port and TLS settings remain
unchanged. With `--org`, saved native endpoint/credentials are ignored and Cloud
HTTPS/443 defaults apply; explicit `--host`, `--port` and `--tls` still take
precedence and must satisfy the Cloud endpoint/TLS rules.

CLI and Driver DSN alternatives:

```bash
bendsql --cloud-api-token '<TOKEN>' --org=org-example
```

```text
databend://api.databend.com:443/default?cloud_api_token=<TOKEN>&org_slug=org-example&warehouse=my-warehouse
```

URL-encode DSN values. Prefer environment injection to command-line secrets.

## Credential selection

- `--org=org-example` explicitly selects Cloud WebAPI. Bare `--org` also
  selects Cloud, reading the slug from config/`BENDSQL_ORG`. Explicit values must
  use `=` so a bare flag never consumes another argument. This selection ignores
  Gateway/private `BENDSQL_DSN`, `BENDSQL_PASSWORD`, and saved native endpoint/user/Access Token/
  Key Pair/tenant settings. Database, Warehouse and other query options are kept.
  Supply the Cloud token through CLI, Cloud config or `BENDSQL_CLOUD_API_TOKEN`;
  missing credentials fail instead of falling back to password authentication.
- A DSN with host `api.databend.com` or `api.databend.cn` also selects Cloud,
  even without a token in the DSN. Token/Org may come from CLI or environment;
  `--org` may override the DSN organization. SQL user/password in the DSN or
  supplied as override flags are ignored, never used as a fallback.
- `--org` conflicts with an explicit Gateway/private `--dsn`, but not a Cloud
  API DSN. Key Pair, Access Token, `tenant` and FlightSQL cannot be combined with
  Cloud authentication. Unsafe explicit combinations fail; error messages name
  options, never credential values. Explicit HTTP/nonstandard ports also fail.
- Without `--org`, `--dsn` takes precedence over `BENDSQL_DSN`. Gateway/private
  DSNs are not overridden by environment Cloud credentials.
- Explicit `--cloud-api-token` overrides the `cloud_api_token` DSN/config value.
- Without explicit `--org` or a Cloud API DSN, `BENDSQL_CLOUD_API_TOKEN` is a
  fallback only when there is no Gateway DSN, explicit user/password, configured
  user, Access Token or Key Pair. It does not take over private connections.
- Rust driver/binding DSNs require `cloud_api_token` and `org_slug` in the DSN;
  environment fallback and CLI overrides are CLI features. Official API hosts
  with missing credentials fail rather than attempting SQL password login.
- Empty environment tokens mean unset. Empty CLI/DSN tokens are errors.
- Explicit `--org` supplies the organization. Otherwise, `org_slug` in DSN/config
  takes precedence over `BENDSQL_ORG`. An empty environment Org is unset;
  duplicate DSN token/Org keys are rejected.
- In Cloud mode username/password are ignored. Access Token, Key Pair and
  separate `tenant` parameters cannot be combined with a Cloud API token.
  There is no `--authenticator` selector.

Private/Gateway authentication is unchanged when Cloud mode is not selected.
The connection label is `cloud_account` / `CloudWebAPI`, not the ignored SQL
username or an API-token parameter name. Use `SELECT current_user(), current_role()`
to inspect the server's actual SQL identity.

## Endpoints

Only `https://api.databend.com:443` and `https://api.databend.cn:443` are supported.
This policy also applies to the Rust driver and its Python/Node.js bindings.
HTTP, including `databend+http` and `sslmode=disable`, is rejected. Normal
certificate verification remains enabled. Redirects and cross-origin polling URLs
are rejected rather than forwarding credentials.

## Compatibility and limitations

- Cloud requests use `X-DatabendCloud-Token`, `X-DatabendCloud-Org` and optional
  `X-DatabendCloud-Warehouse`. No Basic/Bearer credentials or browser login
  cookies are sent. Each connection has its own exact-origin SQL Cookie store:
  `cookie_enabled=true`, `session_id` and `last_refresh_time` only. Cookie expiry
  and deletion are honored; other domains, ports, protocols and connections do
  not share this state. API Token authentication remains required on every
  request; SQL cookies never replace it.
- Login 404 is compatible with servers without `/v1/session/login`;
  `login=disable` skips that probe. Other login/auth failures are returned.
- SQL uses `POST /v1/query`, GET polling/page/final, and POST kill. Results use
  the server's default string-field format; no explicit `string_fields` override
  is needed. There is no generic management REST command interface.
- Login, page and final requests retain normal bounded retries. Query submission
  retries only connect errors before any bytes are sent, not timeout/503 or
  ambiguous request/body failures. Check a write's outcome before retrying it.
- Transactions use the returned session state and sticky-node routing.
  `BEGIN`, `COMMIT` and `ROLLBACK` operate across requests on one connection;
  transaction-control statements are not blocked or warned on.
- SQL session cookies are enabled for temporary-table workloads on one connection.
  Do not expect temporary tables to be visible from another connection, even
  when the account, organization and Warehouse are identical.
- SQL session heartbeat/refresh/logout are not implemented in Cloud mode. The
  transaction check does not establish long-idle session or failover guarantees.
- FlightSQL, bound parameters, stage attachments, Arrow, pagination tuning,
  stage upload and streaming load are unsupported. Presign probing is disabled.
- `--ui` is rejected: its web Python runner could expose the personal token.
- Worksheet query history, result retention and other server side effects apply.

Token headers are marked sensitive; argv/DSN/Debug and AI-active logs are masked.
Raw and JSON-escaped token echoes in errors are redacted; SQL result data is not
rewritten. These protections do not make it safe to share the token or a full DSN.
