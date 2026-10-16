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
  from AI context. SQL output itself is not redacted or rewritten. This is not a
  universal personal-data detector; only use trusted backends after approval.
  AI-session logging masks query/credential/payload values while retaining message
  context, level and target; token-refresh/connection diagnostics are not dropped
  merely because their text contains a credential-related word.
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
- Future Codex tool/execution/change-like items and requests are rejected, while
  unknown passive metadata remains compatible. Cache rejection messages distinguish
  sensitive content from budget exhaustion.
