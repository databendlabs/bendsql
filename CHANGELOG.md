# Changelog

## Unreleased

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
