# Changelog

## Unreleased

### Dependencies

- Upgrade Arrow to 60 and tonic to 0.14, which moves zstd to 0.14.3 / zstd-safe
  8.2.0 and clears RUSTSEC-2026-0391, RUSTSEC-2026-0398 and RUSTSEC-2026-0403.
  FlightSQL errors keep their previous `ArrowError` shapes; an invalid IPC schema
  from the server is now an error instead of a panic.

### Multi-line input

- Alt/Option+Enter and Ctrl+J insert a line break in the REPL instead of
  submitting, so smart/agent-mode questions (and SQL) can span lines. Shift+Enter
  does the same once the terminal maps it to `ESC CR`; README lists settings for
  common terminals. Ctrl+J no longer submits the line.

### Streaming AI answers

- On an interactive terminal, OpenAI-compatible, ACP and local Codex answers now
  stream by default. Only the last screenful is shown while generating; the full
  answer is printed when the turn completes, and failed/interrupted turns erase
  partial text.
  Disable with `settings.ai_stream = false` or `!set ai_stream false`.
- Local Codex streams assistant answer text, not commentary, thinking or tool
  output. Completed assistant-message text from a successful turn remains the
  authoritative answer saved in conversation history. Claude Code and Pi continue
  to show a spinner followed by the final answer.
- OpenAI-compatible backends receive `"stream": true` in that case. Gateways that
  ignore it and return JSON keep working; gateways that reject it need `ai_stream = false`.

### evot backend

- New zero-config built-in backend `local-evot` (alias `evot`), discovered after
  Pi. BendSQL starts `evot acp --tools readonly --permission ask --ephemeral` per
  question through its ACP client: evot asks before every tool call and BendSQL
  always refuses, so no tool runs, and evot keeps no session on disk. Answers
  stream live; a turn where the model tried a tool is discarded. `evot --help`
  must advertise `acp`, `--tools`, `--permission`, `--ephemeral` and `--model`;
  older evot releases receive no context.
- Like other built-ins it accepts `command`, `model` (passed as `--model`),
  `env_allowlist` and `allow_external_agent` overrides. `EVOT_LLM_*` variables
  are forwarded so environment-based evot provider settings keep working.
- Built-in overrides now accept the short alias as section name for every
  built-in: `[agent.backends.codex]` is the same as `[agent.backends.local-codex]`
  and applies under both names and to discovery. Configuring one built-in under
  both names is rejected as an invalid AI config.
- ACP answers (evot and `type = "acp"`) stop at the 8 KiB answer limit: BendSQL
  cancels the adapter and keeps the truncated answer instead of waiting for, or
  timing out on, the rest of a long generation. The per-turn ACP transport budget
  rises from 1 MiB / 4096 frames to 16 MiB / 65536 frames for token-sized and
  thought frames, which are parsed and dropped.

### Interactive SQL and AI modes

- SQL remains the default input mode. The default connection/database prompt,
  SQL history file (`~/.bendsql_history`) and `--log-level` behavior are preserved:
  SQL-only history/log messages are not subjected to AI credential-word filters.
  This includes statements containing token/secret column names and credential DDL;
  secure the normal SQL history/log files or disable them explicitly as needed.
  SQL mode does not capture query data for AI. Large interactive SQL is no longer
  subject to the AI interface's former 64 KiB input cap.
- Entering smart/agent mode or asking AI requires confirmation before sharing,
  unless `agent.allow_data_sharing = true` explicitly authorizes it. Queries made
  before approval are not recovered from SQL history. AI activation stops disk
  history for the session but preserves in-memory SQL navigation. Returning to SQL
  does not resume disk writes after AI activation. `settings.save_history = false`
  disables disk history explicitly; setting it back to true resumes SQL-only writes
  when AI has not been activated. `!context clear` affects AI memory, not SQL navigation.
- Credential-bearing SQL and known sensitive preview/error fields are excluded
  from AI context, including compound identifiers such as `password_hash`,
  `user_api_key`, `aws_secret` or `apiToken` (but not `token_count`). SQL output itself is not redacted or rewritten. This is not a
  universal personal-data detector; only use trusted backends after approval.
  AI-session logging masks query/credential/payload values while retaining message
  context, level and target; token-refresh/connection diagnostics are not dropped
  merely because their text contains a credential-related word.
- Local Codex requires 0.145.0 or newer; the version is checked before any context
  is sent and older releases get an upgrade hint. Local CLI/ACP children receive
  proxy/CA variables (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY`,
  `SSL_CERT_FILE`, `SSL_CERT_DIR`, `NODE_EXTRA_CA_CERTS`) by default.
- Autocomplete keyword loading failures are warnings; they no longer end the REPL,
  and any REPL error still saves history and closes the session.
- Authentication recovery **reconnects without automatically replaying the
  statement**, unlike the old REPL. A query may already have executed before a
  stream/authentication failure. Verify state before manually retrying writes.
- `settings.prompt` remains the complete prompt template. `{mode}` is optional;
  `settings.continuation_prompt` controls pending SQL (default `"> "`).
- Embedded skills are supplied inline from the binary; no runtime extraction or
  inaccessible local skills path is advertised. Only SKILL.md subtrees contribute
  Markdown, not the maintainer-level `cli/skills/README.md`.
- SQL statement splitting is iterative and uses bounded stack space, including
  multi-megabyte literals; it no longer spawns large-stack parsing threads. Smart
  AST classification of oversized input asks for explicit SQL instead of risking
  recursive lexer failure. Transient readline IO errors retry; EOF and permanent
  terminal failures exit without a busy error loop.
- Preserve the existing invalid-token diagnostic and prefixed SQL literals across
  the bounded lexer window (including large `X'...'` dump values); neither case
  changes the SQL golden results or drops an INSERT statement.
- AI-active masking covers the REST driver's exec/query/load/stream SQL log
  entrypoints as well as the core client; SQL-only and batch logs remain unchanged.
- Smart/agent startup loads enabled SQL history for navigation without enabling
  disk writes or adding that history to AI context.
- Codex requires stable 0.145.0 or newer. Prerelease precedence is respected:
  0.145.0-alpha.1 does not satisfy that minimum.
- Future Codex tool/execution/change-like items and requests are rejected, while
  unknown passive metadata remains compatible. Cache rejection messages distinguish
  sensitive content from budget exhaustion.
