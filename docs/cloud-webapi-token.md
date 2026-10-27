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
Explicit host, port and TLS settings, including values in the config file, are
never replaced by Cloud defaults.

CLI and Driver DSN alternatives:

```bash
bendsql --cloud-api-token '<TOKEN>' --org org-example
```

```text
databend://api.databend.com:443/default?cloud_api_token=<TOKEN>&org_slug=org-example&warehouse=my-warehouse
```

URL-encode DSN values. Prefer environment injection to command-line secrets.

## Credential selection

- Explicit `--cloud-api-token` overrides the `cloud_api_token` DSN/config value.
- `BENDSQL_CLOUD_API_TOKEN` is a CLI fallback only when there is no
  `--dsn`/`BENDSQL_DSN`, explicit user/password, configured user, Access Token or
  Key Pair. It does not take over existing private connections. For an explicit
  Cloud DSN, include its token or pass `--cloud-api-token` separately.
- Empty environment tokens mean unset. Empty CLI/DSN tokens are errors.
- Organization precedence is `--org` > `org_slug` in DSN/config > `BENDSQL_ORG`.
  An empty environment Org is unset; duplicate DSN token/Org keys are rejected.
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
