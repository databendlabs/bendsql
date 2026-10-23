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
Use `SELECT current_user(), current_role()` to inspect the server's SQL identity.

## Staging and local HTTPS endpoints

By default only `https://api.databend.com:443` and `https://api.databend.cn:443`
are trusted. Authorize **one additional exact HTTPS origin** for staging or a
local HTTPS service with `BENDSQL_CLOUD_API_ORIGIN`, then select that endpoint:

```bash
export BENDSQL_CLOUD_API_ORIGIN='https://staging.example:8443'
bendsql --host staging.example --port 8443 --tls=true --org org-example
```

The override adds trust; it does not redirect requests or change connection
settings. The origin cannot contain userinfo, a path, query or fragment. HTTP is
never allowed, including `databend+http` and `sslmode=disable`. Normal certificate
verification remains enabled; use `--set tls_ca_file=/path/to/test-ca.pem` for a
private CA. Only authorize servers you control: your personal API token is sent
there and may grant access across organizations. Redirects and cross-origin
polling URLs are rejected.

## Compatibility and limitations

- Cloud requests use `X-DatabendCloud-Token`, `X-DatabendCloud-Org` and optional
  `X-DatabendCloud-Warehouse`. No Basic/Bearer credentials or cookies are sent.
- Login 404 is compatible with servers without `/v1/session/login`;
  `login=disable` skips that probe. Other login/auth failures are returned.
- SQL uses `POST /v1/query` with `string_fields=true`, GET polling/page/final,
  and POST kill. There is no generic management REST command interface.
- Login, page and final requests retain normal bounded retries. Query submission
  retries only connect errors before any bytes are sent, not timeout/503 or
  ambiguous request/body failures. Check a write's outcome before retrying it.
- Transactions and temporary tables are **not guaranteed across requests**.
  Cookies and SQL session heartbeat/refresh/logout are not used. Choose a native
  Gateway/private connection for session-dependent workloads.
- FlightSQL, bound parameters, stage attachments, Arrow, pagination tuning,
  stage upload and streaming load are unsupported. Presign probing is disabled.
- `--ui` is rejected: its web Python runner could expose the personal token.
- Worksheet query history, result retention and other server side effects apply.

Token headers are marked sensitive; argv/DSN/Debug and AI-active logs are masked.
Raw and JSON-escaped token echoes in errors are redacted; SQL result data is not
rewritten. These protections do not make it safe to share the token or a full DSN.
