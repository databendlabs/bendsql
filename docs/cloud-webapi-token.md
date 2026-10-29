# Connect to Cloud WebAPI with an API token

Use a personal Cloud API token and the **Organization ID（组织 ID）** from console
organization settings, not the display name or an internal numeric OrgID/tenant ID.
WebAPI determines the Cloud account, SQL identity, permissions and revocation
policy; no Gateway address, browser login or client-side JWT exchange is needed.

## Usage

Inject the credential from a secret manager:

```bash
export BENDSQL_CLOUD_API_TOKEN='<TOKEN>'
bendsql --org org-example
```

The default is `https://api.databend.com:443`; use `--host api.databend.cn` for
China. Select a Warehouse with `--set warehouse=my-warehouse`, or let WebAPI use
its default. For CLI automation, target and credential can remain separate:

```bash
export BENDSQL_DSN='databend://api.databend.com/default?org=org-example&warehouse=my-warehouse'
export BENDSQL_CLOUD_API_TOKEN='<TOKEN>'
bendsql
```

Rust/Python/Node drivers require both credentials in their DSN, with values URL-encoded:

```text
databend://api.databend.com:443/default?org=org-example&cloud_api_token=<URL-encoded-token>&warehouse=my-warehouse
```

Interactive connections display the official host, Organization ID and
`with Cloud API token`, never the token/session value. Driver metadata uses
`cloud_account` / `CloudWebAPI`; use `SELECT current_user(), current_role()` for
the actual server SQL identity.

## Connection and credential selection

- Only `--org <ORG>` or DSN `org=` selects Cloud. Tokens and official hosts alone
  do not select it. Org must be nonempty; there is no `BENDSQL_ORG` or valueless
  `--org`. The CLI org overrides a Cloud DSN's org.
- Credential precedence is CLI `--cloud-api-token` > DSN `cloud_api_token` >
  `BENDSQL_CLOUD_API_TOKEN`. Missing/empty explicit credentials fail; an empty
  environment token is unset. Ambient tokens are ignored outside Cloud mode.
  Bindings do not read CLI environment credentials. Duplicate token/org DSN keys
  are rejected; `org_slug` is not an alias.
- `--dsn` takes precedence over `BENDSQL_DSN`. `--org` conflicts with an explicit
  private/Gateway DSN, but ignores an ambient private DSN with a source-only
  warning. Cloud also ignores saved `[connection]` and `BENDSQL_PASSWORD` with
  source-only warnings; private credentials/settings are not carried over.
- Config and `--set` cannot supply `org`, `org_slug` or `cloud_api_token`; use
  CLI/DSN instead. Warehouse/database/query options must be supplied explicitly.
- Explicit SQL user/password, Key Pair, Access Token, tenant and FlightSQL options
  conflict with Cloud. User/password embedded in a Cloud DSN are ignored, never
  fallback credentials. Native authentication and precedence are unchanged.
- Only the two official HTTPS/443 endpoints are allowed. Explicit HTTP,
  `databend+http`, `sslmode=disable`, nonstandard ports and other targets fail
  rather than being corrected. Certificate verification remains enabled;
  redirects/cross-origin polling cannot forward credentials.

The console can rename the Organization ID (its dialog calls it a slug). Update
saved DSNs/`--org` values afterwards. Explicit `OrgNotFound` responses explain this;
unrelated 404/authentication failures keep their original meaning.

## Queries and session lifecycle

- Every request uses `X-DatabendCloud-Token`, `X-DatabendCloud-Org` and optional
  `X-DatabendCloud-Warehouse`. Cloud declares `session_header` capability and
  stores the server's sensitive `X-DATABEND-SESSION` header per connection,
  opaque and same-origin only. There is no Cookie, synthetic session ID or
  Basic/Bearer fallback; the session header never replaces API-token auth.
- Cloud login is always disabled, even with `login=enable`, because it reports
  the manage service version rather than the query Warehouse version.
  Authentication and database/role validation happen on the first SQL request.
- SQL uses POST query, GET polling/page/final and POST kill, with the server's
  default string-field result format. There is no generic management REST command.
- `wait_time_secs`, `max_rows_in_buffer` and `max_rows_per_page` are forwarded in
  the query's `pagination` object; unset fields stay omitted. Current WebAPI
  ignores them. They will take effect when server support is deployed, without
  a client update; requested page sizes/wait times are not guaranteed yet.
- Submission retries only pre-send connection failures, not ambiguous
  timeout/503/body failures. Safe page/final/heartbeat requests have bounded
  retries and use the latest session header. Check a write's outcome before retrying.
- The first successful SQL response must establish a valid session header or
  the connection fails closed, without returning rows, Cookie fallback or SQL
  replay. **The query may already have executed: check before retrying.**
  HTTP/SQL/decode errors retain their original meaning. Missing refresh headers
  retain an established session; invalid initial headers make it unusable.
- Transactions and temporary tables use returned SQL/session state and sticky
  routing on one connection. Requests are not serialized; use session-sensitive
  statements sequentially. Temporary tables are not shared across connections.
- Heartbeats keep tracked unread results and sticky sessions alive after session
  establishment. Sticky sessions include node routing even without active queries;
  idle stateless connections do not heartbeat. This requires deployed WebAPI
  session routes. Long-idle recovery and failover are not guaranteed.
- Explicit/spawned close sends one best-effort logout when the server reports
  `need_keep_alive`, with the latest session header and sticky-node routing.
  Logout has a bounded timeout, no retries/redirects, and no response-body logging.
  Forced process termination can still leave cleanup to server expiry.
- Databend session-token refresh, FlightSQL, Arrow, bound parameters, stage
  attachments/uploads and streaming load remain unsupported; presign probing is
  disabled. `--ui` is rejected because its Python runner could expose the token.

Tokens/session headers are sensitive; argv/DSN/Debug and AI-active logs are masked.
Raw and JSON-escaped token echoes in errors are redacted, but SQL results are not.
Do not share a token or full DSN. Normal Cloud query history, result retention and
Warehouse auto-resume side effects still apply.
