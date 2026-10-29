# Connect to Cloud WebAPI with an API token

Use a personal Cloud API token and the **Organization ID（组织 ID）** shown in
the console's organization settings, not its display name or an internal numeric
OrgID/tenant ID. BendSQL connects directly to WebAPI;
no Gateway address, tenant ID, browser login or client-side JWT exchange is needed.
WebAPI determines the Cloud account, SQL identity, permissions and revocation policy.

## Usage

Inject the token from a secret manager, then select the organization:

```bash
export BENDSQL_CLOUD_API_TOKEN='<TOKEN>'
bendsql --org org-example
```

Cloud defaults to `https://api.databend.com:443`. For China, add
`--host api.databend.cn`. Select a Warehouse with `--set warehouse=my-warehouse`;
otherwise WebAPI applies its default. Only `--org <ORG>` or a DSN `org=` selects
Cloud. A token or official host alone does not select it. Cloud parameters in
config and `--set` are not supported.

CLI and Driver DSN alternatives:

```bash
bendsql --cloud-api-token '<TOKEN>' --org org-example
```

```text
databend://api.databend.com:443/default?cloud_api_token=<TOKEN>&org=org-example&warehouse=my-warehouse
```

URL-encode DSN values. In CLI automation, keep the target and credential separate:

```bash
export BENDSQL_DSN='databend://api.databend.com/default?org=org-example&warehouse=my-warehouse'
export BENDSQL_CLOUD_API_TOKEN='<TOKEN>'
bendsql
```

Prefer environment injection to command-line secrets. The token environment
variable supplies a credential only; it never selects or changes a target.

### Organization changes

The console allows this Organization ID to be changed (its rename dialog calls
it a slug). Changing it changes the organization's access address: update saved
DSNs and `--org` arguments afterwards; old saved DSNs/configurations no longer work.
An explicit WebAPI `OrgNotFound` response is reported as "Organization ID does
not exist or has been changed". Other 404/authentication failures are not
misclassified as an organization rename.

## Credential selection

- Only `--org <ORG>` or `org=` in `--dsn` / `BENDSQL_DSN` selects Cloud.
  `--org` requires a nonempty value and overrides the DSN organization.
  There is no `BENDSQL_ORG` or valueless `--org`.
- In Cloud mode credentials resolve as: CLI `--cloud-api-token` > DSN
  `cloud_api_token` > `BENDSQL_CLOUD_API_TOKEN`. Missing/empty explicit credentials
  fail; an empty environment token is unset. Outside Cloud the environment token
  is ignored, including for otherwise default local settings. An explicit token
  without `org` fails. An official API host without `org` also fails.
- Config `[connection]` is private/Gateway configuration only. Cloud parameters
  `org`, `org_slug` and `cloud_api_token` in `[connection.args]` are rejected,
  as are those keys in `--set`. Use CLI/DSN for Cloud parameters.
- `--org` conflicts with an explicit private/Gateway `--dsn`. An ambient private
  `BENDSQL_DSN` is ignored instead, with a warning naming `BENDSQL_DSN` only.
  Cloud mode ignores saved `[connection]` and `BENDSQL_PASSWORD` with warnings
  naming their sources, not values. Pass database/Warehouse/query settings
  explicitly for Cloud; no saved private connection fields are carried over.
- CLI `--user`, `--password`, Key Pair flags and `--flight` conflict with Cloud.
  Explicit Access Token/Key Pair/tenant options also fail. User/password embedded
  in a Cloud DSN are ignored for compatibility, never used as fallback credentials.
- Explicit HTTP, TLS disable and nonstandard Cloud ports fail instead of being
  corrected. `--host` must be an official WebAPI host. Cloud defaults are global
  HTTPS/443 even when saved private configuration specifies HTTP/8000.
- `--dsn` takes precedence over `BENDSQL_DSN`. Native authentication, CLI password
  overrides and `BENDSQL_PASSWORD` retain their behavior outside Cloud mode.
- Rust driver, Python and Node bindings require both `org` and `cloud_api_token`
  in the DSN. They do not read CLI environment credentials. Duplicate token/org
  DSN keys are rejected. The unreleased `org_slug` key is not a supported alias.

Interactive connections identify the actual mode: Cloud displays the official
host, Organization ID and `with Cloud API token`; private mode continues to show
its host/user. Tokens and session values are never displayed. Driver metadata
identifies Cloud as `cloud_account` / `CloudWebAPI`.
Use `SELECT current_user(), current_role()` for the server's actual SQL identity.

## Endpoints

Only `https://api.databend.com:443` and `https://api.databend.cn:443` are supported.
This policy also applies to the Rust driver and its Python/Node.js bindings.
HTTP, including `databend+http` and `sslmode=disable`, is rejected. Normal
certificate verification remains enabled. Redirects and cross-origin polling URLs
are rejected rather than forwarding credentials.

## Compatibility and limitations

- Cloud requests use `X-DatabendCloud-Token`, `X-DatabendCloud-Org` and optional
  `X-DatabendCloud-Warehouse`. SQL session state uses
  `X-DATABEND-CLIENT-CAPS: session_header` and the server's `X-DATABEND-SESSION`
  response header. The latest value is kept opaque and echoed on subsequent
  requests to the same origin, including safe retries. Each connection starts
  without a session header; no local session ID or Cookie is pre-generated.
  No Cookie or Basic/Bearer credentials are sent. API Token authentication is
  required on every request; the session header never replaces it.
- Cloud mode always disables login, even with `login=enable`: the WebAPI login
  endpoint reports the manage service version, not the query Warehouse version.
  Authentication and database/role validation happen on the first SQL request.
- SQL uses `POST /v1/query`, GET polling/page/final, and POST kill. Results use
  the server's default string-field format; no explicit `string_fields` override
  is needed. There is no generic management REST command interface.
- Page and final requests retain normal bounded retries. Query submission
  retries only connect errors before any bytes are sent, not timeout/503 or
  ambiguous request/body failures. Check a write's outcome before retrying it.
- Transactions use the returned session state and sticky-node routing.
  `BEGIN`, `COMMIT` and `ROLLBACK` operate across requests on one connection;
  transaction-control statements are not blocked or warned on.
- Temporary tables use the server-issued session header on one connection.
  Do not expect temporary tables to be visible from another connection, even
  when the account, organization and Warehouse are identical.
- The backend must support `session_header`; there is no Cookie fallback.
  If no valid `X-DATABEND-SESSION` has been received by the first successful SQL
  query response, the driver reports an error and makes the connection unusable.
  No rows are returned and SQL is not automatically replayed.
  **The query may already have executed:** check its outcome before retrying,
  especially for writes. HTTP failures and SQL errors without a session header
  retain their original semantics rather than being reported as missing capability support.
  After a valid session is established, a response without a new header keeps
  the previous value; refreshed response headers are saved automatically.
  An invalid initial session header also makes the connection unusable.
  Requests are not serialized to establish a session: use one connection
  sequentially for transactions and temporary tables. Concurrent session-state
  conflicts are a separate issue shared with native mode.
- Cloud uses `POST /v1/session/heartbeat` for tracked unread results and sticky
  sessions, after the first SQL response establishes a session header. Heartbeats
  retain bounded safe retries and use the latest header; sticky session heartbeats
  include node routing even without active queries. Idle stateless connections do
  not heartbeat. This requires the deployed WebAPI session routes.
- Closing a Cloud connection with server-reported `need_keep_alive` sends one
  best-effort `POST /v1/session/logout`, including its latest session header and
  sticky-node routing. Both explicit and spawned close paths are supported.
  Logout has a bounded request timeout, does not retry or follow redirects, and
  does not expose response bodies in logs. A process forcibly killed before
  cleanup completes still relies on normal server expiry.
- Cloud login and Databend session-token refresh remain disabled; every lifecycle
  request still uses the Cloud API token. Long-idle recovery and failover are not
  guaranteed. Native login, heartbeat, logout and retry behavior are unchanged.
- FlightSQL, bound parameters, stage attachments, Arrow, pagination tuning,
  stage upload and streaming load are unsupported. Presign probing is disabled.
- `--ui` is rejected: its web Python runner could expose the personal token.
- Worksheet query history, result retention and other server side effects apply.

Token and session headers are marked sensitive; argv/DSN/Debug and AI-active logs are masked.
Raw and JSON-escaped token echoes in errors are redacted; SQL result data is not
rewritten. These protections do not make it safe to share the token or a full DSN.
