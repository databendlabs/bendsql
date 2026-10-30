# BendSQL

Databend Native Client in Rust

## Components

- [**core**](core): Databend RestAPI Rust Client

- [**driver**](driver): Databend SQL Client for both RestAPI and FlightSQL in Rust

- [**cli**](cli): Databend Native CLI

### Bindings

- [**python**](bindings/python): Databend Python Client

- [**nodejs**](bindings/nodejs): Databend Node.js Client

- [**java**](bindings/java): Databend Java Client (upcoming)

## Installation for BendSQL

### Installation script

```bash
curl -fsSL https://repo.databend.com/install/bendsql.sh | bash
```

or

```bash
curl -fsSL https://repo.databend.com/install/bendsql.sh | bash -s -- -y --prefix /usr/local
```

### Cargo:

[cargo-binstall](https://github.com/cargo-bins/cargo-binstall) is recommended:

```bash
cargo binstall bendsql
```

Or alternatively build from source:

```bash
cargo install bendsql
```

### Homebrew:

```bash
brew install databendcloud/homebrew-tap/bendsql
```

### Apt:

- Using DEB822-STYLE format on Ubuntu-22.04/Debian-12 and later:

```bash
sudo curl -L -o /etc/apt/sources.list.d/databend.sources https://repo.databend.com/deb/databend.sources
```

- Using old format on Ubuntu-20.04/Debian-11 and earlier:

```bash
sudo curl -L -o /usr/share/keyrings/databend-keyring.gpg https://repo.databend.com/deb/databend.gpg
sudo curl -L -o /etc/apt/sources.list.d/databend.list https://repo.databend.com/deb/databend.list
```

Then install bendsql:

```bash
sudo apt update

sudo apt install bendsql
```

### Manually:

Check for latest version on [GitHub Release](https://github.com/databendlabs/bendsql/releases)

## Usage

To use a personal Databend Cloud API token, set `BENDSQL_CLOUD_API_TOKEN` and
select `--org <ORG>` (the Organization ID in console organization settings), or
set `BENDSQL_DSN=databend://api.databend.com/?org=<ORG>`.
Only `--org <ORG>` or DSN `org=` selects Cloud; config Cloud parameters are not supported.
Cloud defaults to HTTPS/443 at `api.databend.com`,
without a Gateway address or JWT exchange. See [Cloud WebAPI token connections](docs/cloud-webapi-token.md)
for DSN examples, credential precedence, login compatibility and supported operations.

```
❯ bendsql --help
Databend Native Command Line Tool

Usage: bendsql [OPTIONS]

Options:
      --help                       Print help information
      --flight                     Using flight sql protocol, ignored when --dsn is set
      --tls <TLS>                  Enable TLS, ignored when --dsn is set [possible values: true, false]
  -h, --host <HOST>                Databend Server host, Default: 127.0.0.1, ignored when --dsn is set
  -P, --port <PORT>                Databend Server port, Default: 8000, ignored when --dsn is set
  -u, --user <USER>                Default: root, overrides username in DSN
  -p, --password <PASSWORD>        Password, overrides password in DSN [env: BENDSQL_PASSWORD]
  -r, --role <ROLE>                Downgrade role name, overrides role in DSN
  -D, --database <DATABASE>        Database name, overrides database in DSN
      --set <SET>                  Settings, overrides settings in DSN
      --dsn <DSN>                  Data source name [env: BENDSQL_DSN]
  -n, --non-interactive            Force non-interactive mode
  -A, --no-auto-complete           Disable loading tables and fields for auto-completion, which offers a quicker start
      --check                      Check for server status and exit
      --query=<QUERY>              Query to execute
  -d, --data <DATA>                Data to load, @file or @- for stdin. The `--query` should use the syntax: `INSERT FROM <table> from @_databend_load file_format=(<file_format_options>)`
  -o, --output <OUTPUT>            Output format [possible values: table, csv, tsv, null]
      --quote-style <QUOTE_STYLE>  Output quote style, applies to `csv` and `tsv` output formats [possible values: always, necessary, non-numeric, never]
      --progress                   Show progress for query execution in stderr, only works with output format `table` and `null`.
      --stats                      Show stats after query execution in stderr, only works with non-interactive mode.
      --time[=<TIME>]              Only show execution time without results, will implicitly set output format to `null`. [possible values: local, server]
  -l, --log-level <LOG_LEVEL>      [default: info]
  -V, --version                    Print version
```

## Custom configuration

By default bendsql will read configuration from `~/.bendsql/config.toml` and `~/.config/bendsql/config.toml`
sequentially if exists.

- Example file

```
❯ cat ~/.bendsql/config.toml
[connection]
host = "127.0.0.1"
tls = false

[connection.args]
connect_timeout = "30"

[settings]
display_pretty_sql = true
progress_color = "green"
no_auto_complete = true
prompt = ":) "
```

- Connection section

| Parameter  | Description                 |
| ---------- | --------------------------- |
| `host`     | Server host to connect.     |
| `port`     | Server port to connect.     |
| `user`     | User name.                  |
| `database` | Which database to connect.  |
| `args`     | Additional connection args. |

- Settings section

| Parameter            | Description                                                                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `display_pretty_sql` | Whether to display SQL queries in a formatted way.                                                                  |
| `prompt`             | Complete prompt template (default `{mode}> `). Supports `{mode}`, `{database}`, `{user}`, `{host}`, `{port}`, `{catalog}` and `{warehouse}`. |
| `progress_color`     | The color to use for the progress bar.                                                                              |
| `show_progress`      | Whether to show a progress bar when executing queries.                                                              |
| `show_stats`         | Whether to show statistics after executing queries.                                                                 |
| `no_auto_complete`   | Whether to disable loading tables and fields for auto-completion on startup.                                        |
| `max_display_rows`   | The maximum number of rows to display in table output format.                                                       |
| `max_width`          | Limit display render box max width, 0 means default to the size of the terminal. 65535 means no limit for max_width |
| `max_col_width`      | Limit display render each column max width, smaller than 3 means disable the limit.                                 |
| `output_format`      | The output format to use.                                                                                           |
| `expand`             | Expand table format display, default auto, could be on/off/auto.                                                    |
| `time`               | Whether to show the time elapsed when executing queries.                                                            |
| `multi_line`         | Whether to allow multi-line input.                                                                                  |
| `quote_string`       | Whether to quote string values in table output format, default false.                                               |
| `sql_delimiter`      | SQL delimiter, default `;`.                                                                                         |

## Commands in REPL

| Command | Description |
| --- | --- |
| `!exit` / `!quit` | Exit BendSQL |
| `!configs` | Show current settings |
| `!set` | Change settings |
| `!source file` | Execute SQL from a file |
| `!help` | Show available commands |
| `!mode [sql\|smart\|agent]` | Show or switch mode (Tab completion) |
| `!backend [name]` | Show or select an AI backend |
| `!ask question` | Ask AI explicitly |
| `!sql query` | Execute SQL explicitly |
| `!context [clear]` | Inspect or clear query/dialogue context |

## Interactive SQL + AI

BendSQL starts in **SQL mode**. Use `!mode` to see the choices (Tab completion),
or select a mode directly:

| Mode | Behavior |
| --- | --- |
| `sql` (default) | Run SQL; keyword and file completion are enabled. |
| `smart` | Route SQL to Databend and questions to AI; ambiguous input needs `!sql` or `!ask`. |
| `agent` | Ask AI using cached queries and results; suggested SQL is not executed. |

```text
...> !mode smart
Allow AI data sharing with ...? [y/N] y
...> SELECT count(*) FROM orders;
... query result ...
...> Explain the previous result.
AI> ...
...> !mode sql
```

SQL mode does not cache query data for AI. Pure SQL sessions retain their normal
`~/.bendsql_history` and configured logging. First AI use requires confirmation;
only subsequent smart/agent-mode queries enter memory, not earlier SQL history.
AI activation stops disk history for the rest of that session, without clearing
SQL navigation in memory. SQL-only history/logs retain their ordinary contents;
use `save_history = false` / `--log-level off` when appropriate. Use `!context` to
inspect AI memory and `!context clear` to reset it, not SQL input history. Completion is
disabled in smart/agent mode, except for `!mode` choices. Batch and piped SQL do not
invoke AI.

To type a multi-line question, press **Alt/Option+Enter** or **Ctrl+J** for a line
break; Enter submits. Most terminals send the same key code for Shift+Enter and
Enter, so Shift+Enter inserts a line break only after you map it to send `ESC CR`
(`\x1b\r`, the Alt+Enter sequence) in the terminal, for example:

| Terminal | Setting |
| --- | --- |
| iTerm2 | Settings → Profiles → Keys → Key Mappings: add `Shift+Return` → *Send Hex Codes* `0x1b 0x0d` |
| VS Code | `keybindings.json`: `{"key": "shift+enter", "command": "workbench.action.terminal.sendSequence", "args": {"text": "\u001b\r"}, "when": "terminalFocus"}` |
| WezTerm | `{ key = 'Enter', mods = 'SHIFT', action = wezterm.action.SendString '\x1b\r' }` |
| kitty | `map shift+enter send_text all \x1b\r` |
| Ghostty | `keybind = shift+enter=text:\x1b\r` |
| Alacritty | `[keyboard]` `bindings = [{ key = "Enter", mods = "Shift", chars = "\u001b\r" }]` |
| Windows Terminal | `{"command": {"action": "sendInput", "input": "\u001b\r"}, "keys": "shift+enter"}` |

On macOS, Option+Enter needs "Use Option as Meta key" (Terminal.app) or
"Option key: Esc+" (iTerm2).

### Choose an AI backend

By default, BendSQL uses the first installed CLI: **Codex → Claude Code → Pi → evot**.
Install and authenticate it separately. Select another backend with `!backend`
or at startup:

```bash
bendsql --mode agent --backend claude
```

Codex 0.145.0 or newer is required (verified end to end with 0.154.0). BendSQL
checks `codex --version` before sending any context; an older Codex reports an
upgrade hint and SQL stays available. Claude Code and Pi are checked for the
required flags through `--help`.

evot (`--backend evot`, or `local-evot`) runs as
`evot acp --tools readonly --permission ask --ephemeral`: evot asks BendSQL before
every tool call and BendSQL always refuses, so no tool runs, and sessions stay in
memory. If the model tries a tool, the refused call is skipped and the model
answers without it; an answer is discarded only if a tool reports that it ran. It needs an evot
release with `acp --permission`; BendSQL checks `evot --help` before sending context. Answers stream as they are generated.
evot reads its usual `~/.evotai/evot.env`; `EVOT_LLM_*` variables are also forwarded.

Local CLIs start with a minimal environment rather than BendSQL's full
environment, and database variables are never forwarded. Proxy and CA settings
(`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY` and lowercase forms,
`SSL_CERT_FILE`, `SSL_CERT_DIR`, `NODE_EXTRA_CA_CERTS`) are forwarded as-is. Any
other variable a CLI needs must be listed explicitly:

```toml
[agent.backends.local-claude]
env_allowlist = ["ANTHROPIC_BASE_URL"]
```

Built-in overrides accept either name: `[agent.backends.claude]` and
`[agent.backends.local-claude]` (likewise `codex`, `pi`, `evot`) are the same
backend. Use only one of them.

For an OpenAI-compatible service, add a named backend to
`~/.config/bendsql/config.toml`:

```toml
[agent]
backend = "my-api"
# Optional: explicit permission instead of the per-session confirmation.
# allow_data_sharing = true

[agent.backends.my-api]
type = "openai-compatible"
base_url = "https://your-model-service.example/v1"
model = "your-model"
api_key_env = "MY_LLM_API_KEY" # omit for a local service without authentication
```

Explicit `--backend` selection overrides configuration. A failed request does not
switch providers automatically; SQL remains available without an AI backend.
Local CLI/ACP backends currently require Unix; HTTP backends are also available.

**After approval, AI requests share questions and selected smart/agent-mode SQL,
errors and result previews with the configured backend.** Known credential-bearing
statements/results are excluded; this is not a general personal-data detector. Use trusted services/CLIs and clear context before changing topics.
External CLIs can access local files; BendSQL's restrictions are not a sandbox.
Incomplete previews are not full results, and cancellation does not guarantee a
server-side rollback. The bundled Databend skill provides documentation and
troubleshooting guidance; it does not execute SQL or grant tools.

### Prompt and answer style

```toml
[settings]
prompt = "🐳 @{database} [{mode}]:) "
ai_output = "markdown" # or "plain"
ai_stream = true       # show answers while they are generated
```

The default connection/database prompt is unchanged:
`"{user}@{warehouse}/{catalog}/{database}> "`. Add `{mode}` to display the mode,
or use `prompt = "{mode}> "` for the short form. Pending SQL uses
`continuation_prompt = "> "`, which also supports the same template variables.
Use `save_history = false` to disable SQL disk history explicitly. Custom templates
are used as-is; omit `{mode}` to hide the mode. AI answers use terminal Markdown, with a temporary spinner that
clears before the answer. Switch styles with `!set ai_output plain` or
`!set ai_output markdown`; SQL result formatting is unchanged.

On an interactive terminal, answers from OpenAI-compatible, ACP and local Codex
backends (including evot) stream by default. Claude Code and Pi wait for the final answer. While an
answer is generated only its last screenful is shown; the complete answer is printed
when the turn finishes. A failed, interrupted or timed-out turn erases the partial
text and is not added to the conversation.
Use `!set ai_stream false` (or `ai_stream = false`) to wait for complete answers.

Authentication recovery reconnects but does not automatically replay a statement;
verify execution state before retrying writes. See [compatibility notes](CHANGELOG.md).

Use `!help` for commands. See [the maintainer reference](agents/agent-repl.md) for
advanced backend configuration, protocols, cache limits, embedded skills and tests.

## Setting commands in REPL

We can use `!set CMD_NAME VAL` to update the `Settings` above in runtime, example:

```
❯ bendsql

:) !set display_pretty_sql false
:) !set max_display_rows 10
:) !set expand auto
```

## DSN

Format:

```
databend[+flight]://user:[password]@host[:port]/[database][?sslmode=disable][&arg1=value1]
```

Examples:

- `databend://root:@localhost:8000/?sslmode=disable&presign=detect`

- `databend://user1:password1@tnxxxx.gw.aws-us-east-2.default.databend.com:443/benchmark?warehouse=default&enable_dphyp=1`

- `databend+flight://root:@localhost:8900/database1?connect_timeout=10`

### Available Args

#### Common

| Arg               | Description                          |
| ----------------- | ------------------------------------ |
| `warehouse`       | Warehouse name, Databend Cloud only. |
| `sslmode`         | Set to `disable` if not using tls.   |
| `tls_ca_file`     | Custom root CA certificate path.     |
| `connect_timeout` | Connect timeout in seconds           |

#### RestAPI Client

| Arg                         | Description                                                                                                                                                      | Default   |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| `query_result_format`       | (Since v0.33.1) Format to fetch result set, available arguments are `json`/`arrow`.                                                                              | `JSON`    |
| `sslmode`                   | SSL mode, available values are `enable`/`disable`.                                                                                                               | `disable` |
| `wait_time_secs`            | Request wait time for page.                                                                                                                                      | `1`       |
| `max_rows_per_page`         | Max result rows for a single page.                                                                                                                               | `10000`   |
| `page_request_timeout_secs` | Timeout for a single page request.                                                                                                                               | `30`      |
| `presign`                   | Whether to enable presign for data loading, available arguments are `auto`/`detect`/`on`/`off`. Default to `auto` which only enable presign for `Databend Cloud` | `auto`    |

#### FlightSQL Client

| Arg                         | Description                                                               |
|-----------------------------| ------------------------------------------------------------------------- |
| `query_timeout`             | Query timeout seconds                                                     |
| `tcp_nodelay`               | Default to `true`                                                         |
| `tcp_keepalive`             | Tcp keepalive seconds, default to `3600`, set to `0` to disable keepalive |
| `http2_keep_alive_interval` | Keep alive interval in seconds, default to `300`                          |
| `keep_alive_timeout`        | Keep alive timeout in seconds, default to `20`                            |
| `keep_alive_while_idle`     | Default to `true`                                                         |

#### Query Settings

see: [Databend Query Settings](https://docs.databend.com/sql/sql-commands/administration-cmds/show-settings)

## Development

### Cargo fmt, clippy, deny

```bash
make check
```

### Development mode

- For fast development: Run `cd frontend && pnpm run dev` in one terminal, then `make dev-run` in another
- For production builds: Use `make build-frontend` to create embedded assets
- Development mode uses `BENDSQL_DEV_MODE=1` environment variable to proxy requests to Next.js dev server

### Unit tests

```bash
make test
```

### integration tests

_Note: Docker and Docker Compose needed_

```bash
make integration
```
