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
| `prompt`             | The prompt to display before asking for input.                                                                      |
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

| Commands       | Description             |
| -------------- | ----------------------- |
| `!exit`        | Exit bendsql            |
| `!quit`        | Exit bendsql            |
| `!configs`     | Show current settings   |
| `!set`         | Set settings            |
| `!source file` | Source file and execute |

## Interactive SQL + AI session

Start the opt-in interactive session with `bendsql --agent`. It requires a terminal
on both stdin and stdout and cannot be combined with batch execution, `--check`,
`--time`, data loading flags, or `--ui`. The ordinary SQL REPL is unchanged.

The session has three modes, sharing the same query memory and conversation:

| Mode | Prompt | Input behavior |
| --- | --- | --- |
| `smart` (default) | `smart>` | Validates the whole SQL batch with the Databend AST parser before executing any prefix. Incomplete SQL can continue; mixed/invalid input requires explicit routing. Other questions go to AI. |
| `sql` | `sql>` | Treats input as SQL, without a model call. Finish SQL with the configured delimiter (normally `;`). |
| `agent` | `agent>` | Sends input to AI with cached query context, even if it looks like SQL. Does not execute the input. |

Select an initial mode with `bendsql --agent --mode sql`, or switch during the
session with `/mode smart`, `/mode sql`, or `/mode agent`. Pending multi-line SQL
uses a prompt such as `smart(sql)>`; finish it or press Ctrl+C before switching.
Mode switching does not clear context.

Smart mode is conservative: `SELECT 1; Explain the result` executes **nothing**,
not the SQL prefix. The same whole-input check applies to multi-line continuation,
custom delimiters and single-line settings. A complete batch containing an unfinished
or invalid tail is rejected before execution, with the existing pending SQL unchanged.
This is atomic **classification**, not a transaction: a fully valid SQL batch can
still partially succeed if a later statement fails on the server.
Use `/ask` / agent mode to discuss SQL, or `/sql` / sql mode to run unsupported/new
server syntax without local AST validation. Client operations such as `PUT`, `GET`
and `GENDATA` also require an explicit SQL path if the AST does not recognize them.
These explicit paths keep their normal CLI behavior; AI suggestions are never
automatically submitted.

Quoted delimiters, `\G`, closed comments and valid multi-statement batches use the
existing REPL splitter. Standalone closed comments do not start/hold a SQL buffer;
unclosed comments keep accumulating. `!set sql_delimiter ...` and `!set multi_line ...`
apply immediately to both smart validation and execution. Smart mode cannot infer
intent for text that is valid SQL but intended as prose: choose agent mode or `/ask`
when you want explanation rather than execution. Local AST/server version differences
are handled through explicit routing, not model-based execution guesses.

### Model backends

Configure named Chat Completions compatible backends in the existing
`~/.config/bendsql/config.toml` file. If `~/.bendsql/config.toml` exists, it takes
precedence, following the usual BendSQL config loading rules.

```toml
[agent]
backend = "my-api"
timeout_secs = 60

[agent.backends.my-api]
type = "openai-compatible"
base_url = "https://your-model-service.example/v1"
model = "your-model-name"
api_key_env = "MY_LLM_API_KEY"
max_tokens = 2048
# Optional custom headers. Use environment references for sensitive values.
headers = { "x-project" = "analytics" }
header_envs = { "x-gateway-key" = "MY_GATEWAY_KEY" }

[agent.backends.local]
type = "openai-compatible"
base_url = "http://127.0.0.1:8000/v1"
model = "local-model"
# No api_key_env is needed for an unauthenticated local service.
```

Select a backend with `bendsql --agent --backend local`, or `/backend local`
inside the session. `/backend` lists names and the current selection without
printing URLs, headers or credentials. `--backend` overrides `agent.backend`.
Switching succeeds only after validating the selected configuration; on failure,
the existing backend and conversation remain active. On success, conversation
history is cleared while query evidence is retained. Switching back does not
restore the old conversation. `/clear` clears both kinds of context and drops the
backend client; the selected name remains unchanged.

The reserved backend `env` uses the original environment-variable configuration.
When no named backends and no selection are configured, it is the default:

```bash
export BENDSQL_AGENT_BASE_URL='https://your-model-service.example/v1'
export BENDSQL_AGENT_MODEL='your-model-name'
# Optional for services that require authentication; do not pass keys in the URL.
export BENDSQL_AGENT_API_KEY='your-api-key'
bendsql --agent
```

For a named backend, legacy `BENDSQL_AGENT_*` variables do not override its URL,
model or credentials. Select `env` explicitly to use them. Declaring named backends
without selecting one requires an explicit choice; there is no implicit selection
or fallback. Invalid AI configuration disables AI without resetting valid database
configuration. Unreadable or syntactically invalid config files use SQL defaults
and disable AI; diagnostics do not print source lines that might contain secrets.

The base URL is extended with `/chat/completions`. Remote services require HTTPS;
HTTP is allowed only for loopback services. The configured model name is sent
unchanged. Credentials are resolved when a backend is initialized; `api_key_env`
and `header_envs` references must be present and nonempty. Configure either an
Authorization header or `api_key_env`, not both. Header names are case-insensitive;
duplicates and overrides of HTTP transport/JSON content headers are rejected.

SQL execution works without model configuration and remains available after model
errors. `timeout_secs` defaults to 60 and supports 1–3600 seconds; `max_tokens`
defaults to 2048 and supports 1–65536 (the service may have a lower limit).
AI requests can be interrupted with Ctrl+C. This initial implementation returns
text answers without streaming and never exposes a BendSQL query execution tool to a backend.
`openai-compatible` and the local `cli` adapters `codex`, `claude-code`, `pi` and
`amp` are implemented, together with a configurable stdio ACP v1 backend.

### Built-in local coding CLI backends

Local CLI invocation means starting an installed program, not local model inference.
These are trusted external agents, with their own authentication, file access and
service retention policies. **Explicitly selecting a built-in CLI opts into running
it under those policies.** Neither the temporary directory nor an environment
allowlist is an OS security sandbox. Codex can still use tools in its read-only sandbox; BendSQL
does not provide a SQL execution tool or execute SQL generated in an answer.

Codex, Claude Code, Pi and Amp are built in: **no backend configuration tables are needed**.
Install and authenticate the CLI, then select it:

```bash
bendsql --agent --backend local-claude
bendsql --agent --backend local-codex
bendsql --agent --backend local-pi
bendsql --agent --backend local-amp
# Short aliases are also accepted: claude, claude-code, codex, pi, amp
bendsql --agent --backend pi
bendsql --agent --backend amp
```

Inside the session, `/backend` lists built-in and custom names. Use
`/backend claude`, `/backend codex`, `/backend pi`, or `/backend amp` to select one.
Selection validates the executable without starting a model request; the program
starts only when a question is submitted. Missing CLIs produce an error,
never a switch to another service. Existing HTTP/env defaults remain unchanged:
BendSQL never automatically selects a CLI just because it is installed.

To set a default, only the selection is needed:

```toml
[agent]
backend = "local-claude"
timeout_secs = 120
```

Optional overrides can customize a built-in without repeating its type, adapter or
consent fields:

```toml
[agent.backends.local-claude]
command = "/absolute/path/to/claude"
model = "your-model"
env_allowlist = ["HTTPS_PROXY", "CUSTOM_AUTH_KEY"]

[agent.backends.local-codex]
# Optional: disable this built-in, including its codex alias.
allow_external_agent = false
```

Exact custom names take precedence over built-ins. Aliases honor canonical-name
customizations and disable controls; malformed overrides do not fall back to the
built-in defaults. Independently named custom CLI profiles still require
`type = "cli"`, an `adapter` and `allow_external_agent = true`.

BendSQL starts a fresh process for each question and supplies the bounded
conversation/query evidence through stdin, not command arguments or context files.
No CLI session IDs are resumed; switching backends or `/clear` does not recover
previous remote conversation state. Model selection is optional and is passed as
`--model` for Codex, Claude Code and Pi. Amp controls its own model routing; setting
`model` on an Amp backend is rejected instead of passing an unverified flag.
Install and authenticate the CLI separately; BendSQL never downloads
an adapter or invokes a shell to interpret `command`. An absolute executable path
or a bare executable name is accepted; arbitrary arguments and relative paths are
rejected. The executable itself must be trusted.

The adapters use distinct structured-output protocols:

- **Claude Code:** print mode with JSON output, `--tools ""`, `--safe-mode`, disabled
  slash commands, empty strict MCP config, disabled hooks, no Chrome integration
  and `--no-session-persistence`. Only a successful result supplies answer text.
- **Codex:** `exec --json --ephemeral`, `--ignore-user-config`, `--ignore-rules`,
  `--strict-config`, read-only sandbox, approval policy `never`, disabled web search,
  hooks/plugins/apps, the shell feature, multi-agent spawning, memories,
  project instruction discovery and history persistence. These switches reduce
  available capabilities but do not disable every tool: for example, the installed
  Codex still reports unified exec as enabled even when its flag is set to false.
  Only completed assistant-message text from a successfully completed turn is shown;
  reasoning/tool events are not included in the answer.
- **Pi:** one-shot JSON mode with `--no-session`, `--no-tools`, disabled extensions,
  MCP, skills, templates, themes and context-file discovery. `--no-approve` ignores
  project-local resources. `--offline` / `PI_OFFLINE` disable automatic catalog and
  update activity, **not model requests**. Only authoritative assistant text from
  `message_end`, followed by a completed run, is shown; thinking/deltas are ignored.
  Aborts, final errors, incomplete retries and reported tool use do not become answers.
- **Amp:** `--execute --stream-json` with a private, temporary settings file containing
  only static policy: disable tools with `"*"`, reject all MCP servers, disable
  auto-updates and remote creation. Query context and credentials are never written
  to that file. Only a successful final `result` is displayed. Reported active tools,
  MCP connections, tool calls or permission denials cause an error. Amp plugins and
  managed settings can still affect startup; reporting a violation is not prevention
  or rollback of actions an external program may already have taken.

Pi and Amp perform a bounded `--help` check before their first question, with empty
stdin and the same restrictive options/settings. Required advertised flags must be
present; old/unsupported versions receive no conversation or query context. Successful
checks are cached for that backend instance, not globally, and reset when the backend
is rebuilt. This is protocol compatibility checking, not a security attestation of
the executable. Native credential helpers and other installed CLI code are still
trusted external programs.

Compatibility was checked against the help of Codex `0.154.0` and Claude Code
`2.1.203`, and Codex's published schema/events. Older releases may lack required
flags. Unsupported versions, failed authentication or invalid structured output
produce an error, never a downgrade to weaker permissions or another backend.
Pi/Amp adapters follow their published CLI/JSON protocols and are tested with mock
executables; neither executable was installed for live verification. Real model
execution is not part of the mock test suite.

The child environment is cleared and rebuilt from basic runtime variables
(`PATH`, `HOME`, user/locale/temp variables) and CLI-specific auth variables:
`CODEX_HOME`, `OPENAI_API_KEY`, `CODEX_API_KEY` for Codex; `CLAUDE_CONFIG_DIR`,
`ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN` for Claude Code; `AMP_API_KEY` for Amp;
and Pi's config/package directory variables plus a fixed set of common provider
credentials (including `OPENAI_API_KEY`, Anthropic keys/tokens, `GEMINI_API_KEY`,
Groq/Mistral/OpenRouter keys). Additional provider, ambient cloud, proxy and certificate
variables must be explicitly named in `env_allowlist`; missing/empty references are errors.
Database-related names such as `BENDSQL_*`, `DATABEND_*`, `DSN`, `DATABASE_URL` and
`DB_PASSWORD` cannot be forwarded through that list. Other secret variables are
not inherited automatically. `HOME` allows native CLI login credentials to work,
so the environment restriction does not isolate the filesystem.

Input is capped at 256 KiB; captured stdout at 1 MiB, each JSONL event at 128 KiB,
and stderr at 32 KiB. Raw diagnostics are suppressed because they can contain
credentials or SQL. Answers share the HTTP backend's 8 KiB limit and terminal
control-character filtering. Any CLI failure leaves the existing conversation
unchanged, and SQL remains available.

Local CLI backends currently require **Unix**, where each invocation receives its
own process group. Ctrl+C, deadlines, output-budget failures and normal completion
clean up the group and temporary directory. A malicious process can detach from
its group; this cleanup is not containment, and abrupt termination of BendSQL
cannot guarantee cleanup. On other platforms, use the HTTP backend.

BendSQL itself does not persist CLI input/output. Ephemeral/no-session flags request
that Codex/Claude Code/Pi not save conversation history, but CLI diagnostics, managed
settings and upstream service retention can still apply. Amp has no verified ephemeral
flag in this integration and may retain threads locally/remotely; a warning is shown
when it is selected. Amp's noninteractive mode may require `AMP_API_KEY` containing
an access token, per its CLI authentication documentation. `/clear` removes local
BendSQL context, not files or history previously retained by an external CLI/service.

### ACP adapters

BendSQL can act as a text-only ACP Client for an **already installed, trusted
stdio adapter**. Configure it explicitly; the four native CLI built-ins above
remain zero-config and unchanged.

```toml
[agent.backends.my-acp]
type = "acp"
command = "/absolute/path/to/installed-acp-adapter"
# Optional static arguments, passed directly without a shell.
args = ["--stdio"]
allow_external_agent = true
# Generic adapters receive no implicit model API keys. Forward only what this
# adapter requires, or use its existing native login credentials via HOME.
env_allowlist = ["ANTHROPIC_API_KEY"]
```

Use `bendsql --agent --backend my-acp` or `/backend my-acp`. The adapter must speak
ACP on stdout and use stderr for diagnostics; normal CLI text/JSON output is not
ACP. BendSQL does not download an adapter or construct package-manager commands.
If you configure a launcher or script, its behavior (including downloads) is your
explicit trusted configuration. Do not put credentials in `args`; use the environment.

This implementation uses the official `agent-client-protocol` Rust SDK (`3.3`,
Apache-2.0, requires Rust 1.88+) with a bounded line transport. Only stable ACP v1
is negotiated; experimental v2 and SDK-managed process spawning are not enabled.

For each question it:

1. Starts a fresh process in a private temporary directory, without writing query
   context, credentials or responses to files.
2. Sends `initialize` with filesystem/terminal capabilities disabled and validates
   the negotiated protocol version before sending context.
3. Creates a fresh session with an empty MCP-server list. It does not resume/load
   sessions, select remote models/modes or perform interactive authentication.
4. Sends the same bounded BendSQL conversation/query evidence as a text prompt.
5. Collects only `agent_message_chunk` text for that session. Thinking, plans and
   other metadata are not displayed. Only `end_turn` or `max_tokens` completion
   permits an answer; model/output truncation is marked explicitly.

Permission requests always receive the ACP `cancelled` outcome; filesystem,
terminal, elicitation and extension requests receive `Method not found`. Any such
request or reported tool use taints the turn: its answer is suppressed and is not
saved in conversation history. Unexpected session IDs, non-text answer content,
malformed frames, errors or unsuccessful completion also fail without fallback.
Authenticate and configure the adapter outside BendSQL if necessary.

Abandoning a question (Ctrl+C or timeout) cancels its worker. Once a session exists,
it tries `session/cancel`, allows at most 200 ms for protocol completion, and cleans
up the process group. The cancelled worker has a 500 ms cleanup deadline before
being aborted. This is **best-effort protocol cancellation**, not confirmation that
a remote model request stopped. Fresh processes/sessions mean `/clear` and backend
switches cannot accidentally resume old local conversation state; they cannot erase
history retained by the adapter or its upstream services.

Transport limits are 128 KiB per incoming JSON-RPC line, 1 MiB total incoming data
and 4096 frames per invocation. Outgoing data is capped at 1 MiB (512 KiB per line)
including the escaped prompt. Only individual newline-delimited JSON-RPC 2.0
objects are accepted, not batches. Prompt text remains capped at 256 KiB, retained
answer text at 8 KiB, and stderr at 32 KiB. Pipes are serviced concurrently. All
protocol/process diagnostics are sanitized rather than exposing server error text,
private paths or credentials. Static arguments are limited to 32 / 16 KiB total.

Like the native CLI backends, process ACP requires Unix. Its environment starts
with basic runtime/HOME variables and explicit `env_allowlist` references, not
BendSQL database variables. **Capability denial is not a security sandbox:** an
external agent can have its own filesystem/Shell tools, hooks and retention even
without asking the ACP Client. Reported violations are detected after execution,
not containment or rollback. Only run trusted adapters, and prefer an HTTP backend
when you require a model-only interface.

ACP verification uses a mock stdio adapter to exercise initialization, session
creation, streaming text, capability denial, errors, byte budgets and cancellation
with descendant-process cleanup. No real ACP adapter/model service has been invoked.

```text
smart> SELECT channel, sum(amount) AS revenue FROM orders GROUP BY channel;
... normal SQL output ...
[Q1] Query context cached. Use /context to inspect.
smart> Which channel has the highest revenue?
AI> ... explanation based on Q1 ...
smart> /mode agent
Switched to agent mode.
agent> Suggest a query that groups revenue by week.
AI> ... suggested SQL, not executed ...
agent> /mode sql
Switched to sql mode.
sql> SELECT ...;
```

Explicit commands are available in all three modes:

| Command | Behavior |
| --- | --- |
| `/mode [smart\|sql\|agent]` | Show or change the input mode. |
| `/backend [name]` | List/select model backends; switching resets conversation, not query memory. |
| `/sql <SQL>` | Execute SQL explicitly; a trailing delimiter is not required. |
| `/ask <question>` | Ask AI explicitly, without executing generated SQL. |
| `/context` | List cached query IDs, status and preview completeness. |
| `/clear` | Clear query records, conversation and in-memory input history. |
| `/help` | Show mode and command help. |

SQL uses the existing execution and output paths, without rewriting it, injecting
`LIMIT`, or rerunning it to collect context. Each query records SQL, status,
available Query ID, database/warehouse, elapsed time, available server statistics,
errors and a bounded result preview. Failed queries retain available partial results.
Client-observed status is not proof of remote termination. In agent sessions, Ctrl+C
can interrupt a pending ordinary SQL submission even before a response arrives; if
no usable current Query ID is known, no previous query is cancelled and the server
may still execute or commit. Verify state before retrying writes. For a known current
Query ID, cancellation has a 3-second request deadline; acceptance does not confirm
termination or rollback. Remaining statements in an interrupted batch are skipped.
Interrupted results and incomplete previews must not be treated as complete evidence.
File transfer/generation and password-change commands retain their existing driver
execution behavior; remote cancellation is not guaranteed for every operation.

Previews retain at most 30 rows, 64 columns and 16 KiB of schema/cell text, with
512-byte cell limits. Query memory keeps at most 20 records and a 256 KiB serialized
payload budget; old records are evicted. These are retained-context limits, not
limits on database execution, network transfer or the existing output renderer.

AI receives selected records, prioritizing explicit `Q<n>` references and recent
queries, plus bounded conversation history. Incomplete results are marked explicitly;
the assistant is instructed not to infer whole-result aggregates from previews.
Large records may be reduced further to fit the 32 KiB query-context budget.
Missing/evicted records cannot be recovered by AI, and query IDs are not reused
when `/clear` is invoked. Questions have an 8 KiB limit; displayed/stored AI answers
are also limited to 8 KiB and marked if truncated.

**Privacy:** asking a question sends selected SQL, previews, errors and recent
conversation to the configured model backend (or external CLI). Review its data-handling policy before
using sensitive data. SQL and cell contents are treated as untrusted evidence, not
instructions; this does not guarantee the model's answers are correct. BendSQL agent sessions
do not load/save disk history, and BendSQL client file logging is disabled in these sessions.
Normal database-side logging and model-service retention policies still apply.
Use `/clear` to remove local context before changing topics.

For local verification (the terminal smoke test requires Unix and Python 3, but no
live database or model service):

```bash
cargo test -p bendsql --bin bendsql
cargo build -p bendsql --bin bendsql
python3 cli/tests/agent_repl.py target/debug/bendsql
```

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
