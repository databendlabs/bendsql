# Interactive REPL and AI maintainer reference

For the user quick start, see [README.md](../README.md#interactive-sql--ai).
Read [rust-workspace.md](rust-workspace.md) for Rust ownership and verification
rules before changing implementation. This file keeps the detailed behavior,
protocol contracts and test guidance out of the user-facing README.

## Implementation map

- `cli/src/session.rs`: shared REPL, SQL execution/capture and mode switching.
- `cli/src/helper.rs`: SQL-only completion and mode-command hints.
- `cli/src/agent/`: routing, context, providers, skills and AI presentation.
- `cli/skills/databend/SKILL.md`: the bundled Databend reference.
- `cli/tests/agent_repl.py`: mock OpenAI (JSON and SSE) and CLI/ACP PTY integration.
- `.github/workflows/ci.yml`: the `agent-provider-mock` job.

The following sections document the full implementation and configuration surface.
No real service credentials should be added to examples or CI.

## Interactive SQL + AI session

Start `bendsql` normally: every interactive session opens in **SQL mode** and can
switch to smart or agent mode with `!mode`. No feature flag is required; `--agent`
and slash-prefixed commands are not accepted. Batch execution, piped SQL, `--query`,
`--check`, time-only execution and data loading keep their existing non-AI paths.
Explicit `--mode` / `--backend` require terminal stdin/stdout and conflict with
batch options. All interactive modes share one REPL implementation.
The session starts directly at its mode prompt; usage is available through `!help`
and backend details through `!backend`, rather than printed at startup.

Without an explicit backend, BendSQL checks installed executables in order:
**`codex` → `claude` (Claude Code) → `pi`**. `--backend` takes precedence over
`agent.backend` in the config file; either explicit selection bypasses discovery.
No model process starts until a question is submitted. Missing all three CLIs
produces an actionable error without blocking SQL. Amp is not supported. Merely
defining HTTP profiles or setting legacy model
environment variables does not select them.

SQL mode never captures query data. Smart/agent capture and every backend request
require session confirmation or `agent.allow_data_sharing = true`. No pre-approval
SQL is recovered from history. Built-in execution profiles do not bypass consent.
Switching backends resets session permission unless configuration authorizes it.

The modes share only approved query memory and conversation:

| Mode | Prompt | Input behavior |
| --- | --- | --- |
| `smart` | `smart>` | Validates the whole SQL batch with the Databend AST parser before executing any prefix. Incomplete SQL can continue; mixed/invalid input requires explicit routing. Other questions go to AI. |
| `sql` (default) | `sql>` | Treats input as SQL, without a model call. SQL keyword hints and file completion are enabled. Finish SQL with the configured delimiter (normally `;`). |
| `agent` | `agent>` | Sends input to AI with cached query context, even if it looks like SQL. Does not execute the input. |

Select an initial mode with `bendsql --mode sql`, or switch during the
session with `!mode smart`, `!mode sql`, or `!mode agent`. Pending multi-line SQL
uses `settings.continuation_prompt` (default `"> "`); finish it or press Ctrl+C before switching.
Mode switching does not clear context. Typing `!mode` displays the three choices
as an inline hint, and Tab completes the mode argument. Executing `!mode` lists
`sql | smart | agent`; use `!mode <choice>` to switch.

SQL keyword hints, filename completion and SQL highlighting are disabled in
smart/agent modes. The `!mode` choice helper remains available in every mode.
Server table/field keywords are loaded only when SQL mode is first entered (unless
`--no-auto-complete` is set), so starting in smart/agent mode avoids that catalog
query. Returning to SQL restores completion without reloading the same catalog.
Enabled SQL history is loaded for navigation even when starting with `--mode smart`
or `--mode agent`; it is never copied into AI query memory. Such sessions still do
not write history. AI-active log masking covers both core and REST-driver SQL entrypoints.

The complete prompt is controlled by `settings.prompt`, whose default template
is `"{user}@{warehouse}/{catalog}/{database}> "`. No mode prefix or suffix is added outside the template. `{mode}`
expands to `sql`, `smart` or `agent` on each input; omitting it intentionally hides
the mode (use `!mode` to inspect it). Existing `{host}`, `{port}`, `{user}`,
`{catalog}`, `{database}` and `{warehouse}` placeholders remain supported.

```toml
[settings]
prompt = "🐳 @{database} [{mode}]:) "
```

This renders `🐳 @default [sql]:) ` and updates after mode switches. With
`prompt = "🐳 @{database}:) "`, it renders `🐳 @default:) ` in all modes, without
an extra `sql>`/`smart>`/`agent>` prefix. Configured whitespace is preserved; include
a trailing space if desired. Pending SQL uses the separate `settings.continuation_prompt` template (default
`"> "`, supporting the same variables), rather than repeating the full template.

Smart mode is conservative: `SELECT 1; Explain the result` executes **nothing**,
not the SQL prefix. The same whole-input check applies to multi-line continuation,
custom delimiters and single-line settings. A complete batch containing an unfinished
or invalid tail is rejected before execution, with the existing pending SQL unchanged.
This is atomic **classification**, not a transaction: a fully valid SQL batch can
still partially succeed if a later statement fails on the server.
Use `!ask` / agent mode to discuss SQL, or `!sql` / sql mode to run unsupported/new
server syntax without local AST validation. Client operations such as `PUT`, `GET`
and `GENDATA` also require an explicit SQL path if the AST does not recognize them.
These explicit paths keep their normal CLI behavior; AI suggestions are never
automatically submitted.

Quoted delimiters, `\G`, closed comments and valid multi-statement batches use the
existing REPL splitter. Standalone closed comments do not start/hold a SQL buffer;
unclosed comments keep accumulating. `!set sql_delimiter ...` and `!set multi_line ...`
apply immediately to both smart validation and execution. Smart mode cannot infer
intent for text that is valid SQL but intended as prose: choose agent mode or `!ask`
when you want explanation rather than execution. Local AST/server version differences
are handled through explicit routing, not model-based execution guesses.

### AI answer presentation

AI answers use basic terminal Markdown rendering by default on a capable interactive
terminal. Headings/emphasis, lists, quotations, inline/code blocks and HTTP(S) links
are formatted; links are shown as text URLs, never opened. Multi-line/structured
answers put `AI>` on its own line. SQL code is not reformatted, wrapped, highlighted
as executable input or run. HTML/images are omitted rather than rendered/fetched.
Table pipes remain readable text; advanced table layout and token-level code syntax
highlighting are not implemented. Spinner cleanup happens before final rendering.

### Streaming answers

`settings.ai_stream` (default `true`, `!set ai_stream true|false`) shows answers
while they are generated. `cli/src/agent/live.rs` owns the presentation:

- Live output requires Unix, terminal stdout, `TERM` other than `dumb`, and at least
  20 columns and 5 rows; otherwise the request is not streamed and the spinner
  stays until the final answer, as before.
- Local echo is disabled while streaming, so type-ahead or `^C` cannot write into
  the region and desync its row count. Typed input stays buffered for the next prompt.
- Deltas are display-only (`AnswerStream`). The backend's returned answer stays
  authoritative and is the only text recorded, only on success.
- Every 50 ms the accumulated text (control-filtered, at most 8 KiB) is re-rendered
  with the final renderer into a transient region of at most terminal height − 1
  rows, wrapped by BendSQL to width − 1 columns. Only the tail is shown, with a
  marker row for hidden lines. Ambiguous-width characters are counted as wide
  to prevent terminal-side wrapping in CJK configurations. The region never reaches
  scrollback, so relative cursor movement (`CSI n A`, `CSI J`) can always erase it.
  Frames use synchronized output (`CSI ? 2026 h/l`), which other terminals ignore.
- Success erases the region and prints the complete rendered answer. Errors,
  Ctrl+C, timeouts and invalidated turns (for example, ACP tool use) erase it and
  print only the error.
- A terminal resize after live rows have been drawn can reflow them, so BendSQL
  erases what it can, stops live drawing and restores the spinner for the rest of
  that turn. A resize before the first frame just updates the drawing size if
  the terminal remains large enough. Drawing failures also restore the spinner.
- Erasing is presentation only: the user, terminal recorders or screen readers may
  already have seen the partial text. Screen-reader users may prefer `ai_stream = false`.

Streaming backends in this phase:

- `openai-compatible` / `env`: sends `"stream": true` and parses SSE
  `choices[0].delta.content`. A turn completes with `finish_reason` or `[DONE]`;
  a stream that ends otherwise, an `error` event or invalid JSON is an error.
  A final event without a trailing blank line is accepted at EOF.
  Responses that are not `text/event-stream` are parsed as ordinary JSON completions,
  for gateways that ignore `stream`. The wire budget is 4 MiB (128 KiB per event);
  reading stops, closing the connection, once the 8 KiB answer budget is exceeded.
  Gateways that reject streaming with HTTP 400/415/422 get a hint to use
  `!set ai_stream false`; requests are never automatically retried.
- ACP: `agent_message_chunk` text is published as it arrives, but no further text
  is published after unsupported capability requests or reported tool use.
- Codex, Claude Code and Pi keep the non-streaming path (spinner, then answer).

```toml
[settings]
ai_output = "markdown" # or "plain"; independent of SQL output_format
```

```text
!set ai_output plain
!set ai_output markdown
```

Plain mode preserves Markdown syntax. Non-terminal output and `TERM=dumb` fall back
to plain text; `NO_COLOR` disables generated styles while retaining Markdown layout.
Conversation stores the original (control-filtered) Markdown, never renderer ANSI
codes. Incoming control sequences are filtered before presentation; only fixed
renderer-generated styles may reach the terminal.

### Model backends

Configure named Chat Completions compatible backends in the existing
`~/.config/bendsql/config.toml` file. If `~/.bendsql/config.toml` exists, it takes
precedence, following the usual BendSQL config loading rules.

```toml
[agent]
backend = "my-api"
# Optional explicit permission instead of an interactive confirmation:
# allow_data_sharing = true
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

Select a backend with `bendsql --backend local`, or `!backend local`
inside the session. `!backend` lists names and the current selection without
printing URLs, headers or credentials. `--backend` overrides `agent.backend`.
Switching succeeds only after validating the selected configuration; on failure,
the existing backend and conversation remain active. On success, conversation
history is cleared while query evidence is retained. Switching back does not
restore the old conversation. `!context clear` clears both kinds of context and drops the
backend client; the selected name remains unchanged.

The reserved backend `env` explicitly uses the original environment-variable
configuration; it is **not** the default backend:

```bash
export BENDSQL_AGENT_BASE_URL='https://your-model-service.example/v1'
export BENDSQL_AGENT_MODEL='your-model-name'
# Optional for services that require authentication; do not pass keys in the URL.
export BENDSQL_AGENT_API_KEY='your-api-key'
bendsql --backend env
```

For a named backend, legacy `BENDSQL_AGENT_*` variables do not override its URL,
model or credentials. Select `env` explicitly to use them. Declaring profiles without
selecting one leaves local CLI discovery active; it never implicitly selects an HTTP
profile. Once a backend has been selected, authentication, configuration and request
failures do not trigger another backend. Invalid AI configuration disables AI without
resetting valid database configuration. Unreadable or syntactically invalid config files use SQL defaults
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
AI requests can be interrupted with Ctrl+C. HTTP and ACP backends can stream answers
as described in [Streaming answers](#streaming-answers); Codex, Claude Code and Pi
show a spinner followed by the final answer instead. No BendSQL query execution
tool is exposed to a backend.
`openai-compatible` and the local `cli` adapters `codex`, `claude-code` and `pi`
are implemented, together with a configurable stdio ACP v1 backend.

### Built-in local coding CLI backends

Local CLI invocation means starting an installed program, not local model inference.
These are trusted external agents, with their own authentication, file access and
service retention policies. **Entering agent/smart mode or using `!ask` and
submitting a question runs the selected trusted program under those policies.** Neither the temporary directory nor an environment
allowlist is an OS security sandbox. Codex can still use tools in its read-only sandbox; BendSQL
does not provide a SQL execution tool or execute SQL generated in an answer.

Codex, Claude Code and Pi are built in: **no backend configuration tables are needed**.
Install and authenticate the CLI, then use default discovery or select one:

```bash
bendsql  # SQL mode; !mode agent then a question uses codex, then claude, then pi
bendsql --backend local-claude
bendsql --backend local-codex
bendsql --backend local-pi
# Short aliases are also accepted: claude, claude-code, codex, pi
bendsql --mode agent --backend pi
```

Inside the session, `!backend` lists built-in and custom names. Use
`!backend claude`, `!backend codex`, or `!backend pi` to select one.
Selection validates the executable without starting a model request; the program
starts only when a question is submitted. Missing CLIs produce an error,
never a switch to another service. Default discovery checks configured built-in
command overrides as well as PATH, skips disabled/non-CLI profiles, and fixes the
selected backend before making a request. Authentication/request failures and
`!context clear` do not advance to another provider. Privacy/retention details are available
through `!backend` and `!help`; per-request backend notices are not printed.

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
env_allowlist = ["CUSTOM_AUTH_KEY"]

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
No CLI session IDs are resumed; switching backends or `!context clear` does not recover
previous remote conversation state. Model selection is optional: Codex uses the
`thread/start` model field, while Claude Code and Pi receive `--model`.
Install and authenticate the CLI separately; BendSQL never downloads
an adapter or invokes a shell to interpret `command`. An absolute executable path
or a bare executable name is accepted; arbitrary arguments and relative paths are
rejected. The executable itself must be trusted.

The adapters use distinct structured-output protocols:

- **Claude Code:** print mode with JSON output, `--tools ""`, `--safe-mode`, disabled
  slash commands, empty strict MCP config, disabled hooks, no Chrome integration
  and `--no-session-persistence`. Only a successful result supplies answer text.
- **Codex:** `codex app-server --listen stdio://` with `initialize`, an ephemeral
  `thread/start` and `turn/start`. Each question uses a fresh process/thread; bounded
  multi-turn context is owned by BendSQL, not an ever-growing Codex thread. Native
  Codex model/provider/auth configuration is preserved rather than discarded with
  `--ignore-user-config`. `--strict-config` is not used on native configuration,
  because legacy non-routing fields may otherwise reject an ordinarily usable CLI.
  Execution-policy overrides disable notify, configured MCP servers, hooks, plugins,
  apps, web search, multi-agent spawning, memories and project instruction discovery.
  Read-only sandbox / approval policy `never` and ephemeral acknowledgement are
  verified before query context is sent. Approval/client-tool requests are rejected;
  reported tool use invalidates the answer. Only completed assistant-message text
  from a successful turn is returned; thinking and partial deltas are not committed.
  Cancellation attempts `turn/interrupt` before bounded process-group cleanup.
  A new OS session isolates the headless server from the REPL's controlling terminal.
  These controls are not an OS security guarantee: Codex may still have native tools
  not fully disabled by feature flags.
- **Pi:** one-shot JSON mode with `--no-session`, `--no-tools`, disabled extensions,
  MCP, skills, templates, themes and context-file discovery. `--no-approve` ignores
  project-local resources. `--offline` / `PI_OFFLINE` disable automatic catalog and
  update activity, **not model requests**. Only authoritative assistant text from
  `message_end`, followed by a completed run, is shown; thinking/deltas are ignored.
  Aborts, final errors, incomplete retries and reported tool use do not become answers.

Pi and Claude Code perform a bounded `--help` check before its first question, with empty
stdin and the same restrictive options/settings. Required advertised flags must be
present; old/unsupported versions receive no conversation or query context. Successful
checks are cached for that backend instance, not globally, and reset when the backend
is rebuilt. This is protocol compatibility checking, not a security attestation of
the executable. Native credential helpers and other installed CLI code are still
trusted external programs.

Codex `0.145.0` is the minimum supported version: it is the first release that
accepts every `policy_args` override (0.144.x rejects `agents.enabled=false` with
`expected struct AgentRoleToml`), confirmed by `initialize` + `thread/start` against
npm releases. `CodexServer` runs `codex --version` once per backend instance before
sending context; older or unparsable versions get an upgrade hint and the result is
not cached on failure. Raise `MIN_CODEX_VERSION` when adding a newer override.
Codex `0.154.0` was verified with its generated app-server schema and opt-in live
`hi` checks (no database records), including returning to the interactive prompt.
Claude Code `2.1.203` was checked through its help and mock print/JSON protocol. Older releases may lack required
flags. Unsupported versions, failed authentication or invalid structured output
produce an error, never a downgrade to weaker permissions or another backend.
AI requests display a temporary stderr spinner (`AI thinking`) rather than a
percentage or stage log. It is stopped and erased before printing an answer/error,
including cancellation and timeout. Backend-selection notices and Codex startup/
generation messages do not remain in the output. Non-terminal stderr hides the
animation. Timeout errors still identify the phase; normal replies remain clean.
The Pi adapter follows its published CLI/JSON protocol and is tested with a mock
executable; it was not installed for live verification. Real model
execution is not part of the mock test suite. A deliberately ignored live Codex
smoke test can be run with `cargo test -p bendsql --bin bendsql real_codex_hi -- --ignored --nocapture`;
it sends only `hi`, requires an authenticated installed CLI and may incur model cost.

The child environment is cleared and rebuilt from basic runtime variables
(`PATH`, `HOME`, user/locale/temp variables) and CLI-specific auth variables:
`CODEX_HOME`, `OPENAI_API_KEY`, `CODEX_API_KEY` for Codex; `CLAUDE_CONFIG_DIR`,
`ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN` for Claude Code; and Pi's config/package directory variables plus a fixed set of common provider
credentials (including `OPENAI_API_KEY`, Anthropic keys/tokens, `GEMINI_API_KEY`,
Groq/Mistral/OpenRouter keys). Every CLI/ACP child also receives the network
settings `HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY` (and lowercase forms),
`SSL_CERT_FILE`, `SSL_CERT_DIR` and `NODE_EXTRA_CA_CERTS` when set, so a CLI that
works standalone behind a proxy/custom CA also works from BendSQL. Additional provider
or ambient cloud variables must be explicitly named in `env_allowlist`; missing/empty references are errors.
Database-related names such as `BENDSQL_*`, `DATABEND_*`, `DSN`, `DATABASE_URL` and
`DB_PASSWORD` cannot be forwarded through that list. Other secret variables are
not inherited automatically. `HOME` allows native CLI login credentials to work,
so the environment restriction does not isolate the filesystem.

Input is capped at 256 KiB; captured stdout at 1 MiB, each JSONL event at 128 KiB,
while stderr is continuously drained/discarded in fixed-size buffers. Verbose
warnings do not invalidate a successful answer. Raw diagnostics can contain
credentials or SQL and are never displayed. Answers share the HTTP backend's 8 KiB limit and terminal
control-character filtering. Any CLI failure leaves the existing conversation
unchanged, and SQL remains available.

Local CLI backends currently require **Unix**, where each invocation receives its
own process group. Ctrl+C, deadlines, output-budget failures and normal completion
clean up the group and temporary directory. A malicious process can detach from
its group; this cleanup is not containment, and abrupt termination of BendSQL
cannot guarantee cleanup. On other platforms, use the HTTP backend.

Pure SQL sessions load/save `~/.bendsql_history` and honor `--log-level` without
AI sensitive-word filtering, including column names such as token/secret and DDL.
`settings.save_history = false` disables disk history; true re-enables it unless
AI has already been activated. Starting in an AI mode or activating AI stops disk
writes for the rest of the session, but never clears in-memory SQL navigation.
Questions and consent responses are not history entries. SQL history is navigation
only, not AI evidence. AI memory still excludes known credentials independently.
File logging is unchanged before AI; after activation, query/payload and credential
values are masked without discarding diagnostic message structure, level or target.
The logger remains active; authentication/connection messages without payload values
remain useful. This is not a universal log DLP mechanism. Ephemeral/no-session flags request
that Codex/Claude Code/Pi not save conversation history, but CLI diagnostics, managed
settings and upstream service retention can still apply. `!context clear` removes local
BendSQL context, not files or history previously retained by an external CLI/service.

### Embedded skill resources

`cli/skills/` is the source directory for built-in skills. Add future skills under
`<name>/SKILL.md` with optional Markdown reference files. The build script embeds
all resource bytes into the CLI in **debug and release**, tracks directory changes,
and rejects symlinks. This does not change the frontend's asset-loading behavior.

Compiled resources are supplied inline with no runtime extraction or advertised
local path, avoiding orphan directories on SIGKILL. Only Markdown within SKILL.md
subtrees is sent; the top-level maintainer README is excluded. Clearing context
does not remove built-in references.

Every backend's system message lists available SKILL.md files and bounded text excerpts (16 KiB total, at most 4 KiB per Markdown document). This
also works for HTTP-only models, which cannot read a local path. Missing/truncated
content is explicitly identified. The single bundled `databend/SKILL.md` points to
https://docs.databend.com/ and covers SQL-reference lookup plus common troubleshooting:

- `EXPLAIN <statement>` for plans, not actual operator runtimes.
- `EXPLAIN ANALYZE <statement>` for actual execution statistics, with cost/side-effect warnings.
- `EXPLAIN ANALYZE GRAPHICAL <statement>` for BendSQL's browser profile (BendSQL
  v0.22.2+; the docs mark the page introduced/updated in Databend v1.2.647, not
  necessarily the first supported server release); it also executes the statement.
- Evidence-based slow/failed-query triage, missing schema, partial previews and unknown
  execution state after cancellation. No invented counts, URLs, diagnoses or dry-run claims.

The references are embedded in full within the per-document budget. Documentation
links are not automatically fetched, and a reference does not grant browsing or
live database access. Static resources are reference material, not new tool permissions: no scripts are executed,
and skills cannot override the rules preventing automatic SQL or Shell execution.
Future resources are capped at 256 files / 4 MiB total and 1 MiB per file.

### Mock provider integration and CI

The `agent-provider-mock` CI job runs CLI binary unit tests (including the embedded
Databend skill), builds BendSQL, then exercises a local OpenAI-compatible Chat
Completions service and the full mock CLI/ACP REPL suite. No real model credentials,
external provider requests or Docker services are needed.

```bash
cargo test -p bendsql --bin bendsql --all-features
cargo build -p bendsql --bin bendsql
python3 cli/tests/agent_repl.py target/debug/bendsql --provider-only
python3 cli/tests/agent_repl.py target/debug/bendsql
```

The focused provider test verifies arbitrary model/deployment names, endpoint/auth/
custom-header settings, system skill text and cached query evidence, multi-turn
history, reset, response control filtering and answer-size limits. HTTP 401/429/503,
malformed/empty completions and oversized responses must not leak raw diagnostics,
enter dialogue history, trigger fallback/retries or execute suggested SQL. The full
suite additionally verifies timeouts/cancellation, spinner cleanup, input modes and
native/ACP process cleanup. Live Codex tests remain opt-in and ignored by default.

### ACP adapters

BendSQL can act as a text-only ACP Client for an **already installed, trusted
stdio adapter**. Configure it explicitly; the three native CLI built-ins above
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

Use `bendsql --backend my-acp` or `!backend my-acp`. The adapter must speak
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
a remote model request stopped. Fresh processes/sessions mean `!context clear` and backend
switches cannot accidentally resume old local conversation state; they cannot erase
history retained by the adapter or its upstream services.

Transport limits are 128 KiB per incoming JSON-RPC line, 1 MiB total incoming data
and 4096 frames per invocation. Outgoing data is capped at 1 MiB (512 KiB per line)
including the escaped prompt. Only individual newline-delimited JSON-RPC 2.0
objects are accepted, not batches. Prompt text remains capped at 256 KiB, retained
answer text at 8 KiB. Stderr is drained/discarded with fixed memory; diagnostic
volume does not invalidate a successful result. Pipes are serviced concurrently. All
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
sql> !mode smart
Switched to smart mode.
smart> SELECT channel, sum(amount) AS revenue FROM orders GROUP BY channel;
... normal SQL output ...
[Q1] Query context cached. Use !context to inspect.
smart> Which channel has the highest revenue?
AI> ... explanation based on Q1 ...
smart> !mode agent
Switched to agent mode.
agent> Suggest a query that groups revenue by week.
AI> ... suggested SQL, not executed ...
agent> !mode sql
Switched to sql mode.
sql> SELECT ...;
```

SQL mode never caches query context. Use approved smart/agent mode to execute
queries for AI analysis. A cache ID is printed only if the record is retained.
Known credential statements, sensitive columns/preview values and credential-bearing
errors are excluded entirely. This is not a general personal-data detector.

Explicit commands are available in all three modes:

| Command | Behavior |
| --- | --- |
| `!mode [sql\|smart\|agent]` | Show choices / Tab-complete them, or switch the input mode. |
| `!backend [name]` | List/select model backends; switching resets conversation, not query memory. |
| `!sql <SQL>` | Execute SQL explicitly; a trailing delimiter is not required. |
| `!ask <question>` | Ask AI explicitly, without executing generated SQL. |
| `!context` | List cached query IDs, status and preview completeness. |
| `!context clear` | Clear query records and conversation, preserving SQL input navigation. |
| `!help` | Show mode and command help. |

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
when `!context clear` is invoked. Questions have an 8 KiB limit; displayed/stored AI answers
are also limited to 8 KiB and marked if truncated.

**Privacy:** asking a question sends selected SQL, previews, errors and recent
conversation to the configured model backend (or external CLI). Review its data-handling policy before
using sensitive data. SQL and cell contents are treated as untrusted evidence, not
instructions; this does not guarantee correct answers. Pure SQL history/logging
preserve ordinary behavior; AI activation stops disk history and protects logged
payloads. Query/conversation cache remains RAM-only. There is no skills extraction
or SQL-mode cache. SQL input has no AI-specific 64 KiB limit. Reconnect does not
automatically replay statements (see CHANGELOG.md).
Normal database-side logging and model-service retention policies still apply.
Use `!context clear` to remove local context before changing topics.

For local verification (the terminal smoke test requires Unix and Python 3, but no
live database or model service):

```bash
cargo test -p bendsql --bin bendsql
cargo build -p bendsql --bin bendsql
python3 cli/tests/agent_repl.py target/debug/bendsql
```
