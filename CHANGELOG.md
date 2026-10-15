# Changelog

## Unreleased

### Interactive SQL and AI modes

- SQL remains the default input mode. The default connection/database prompt,
  SQL history file (`~/.bendsql_history`) and `--log-level` behavior are preserved.
  SQL mode does not capture query data for AI. Large interactive SQL is no longer
  subject to the AI interface's former 64 KiB input cap.
- Entering smart/agent mode or asking AI requires confirmation before sharing,
  unless `agent.allow_data_sharing = true` explicitly authorizes it. Queries made
  before approval are not recovered from SQL history. AI activation stops disk
  history for the session; `settings.save_history = false` disables it explicitly.
- Credential-bearing SQL and known sensitive preview/error fields are excluded
  from AI context. SQL output itself is not redacted or rewritten. This is not a
  universal personal-data detector; only use trusted backends after approval.
- Authentication recovery **reconnects without automatically replaying the
  statement**, unlike the old REPL. A query may already have executed before a
  stream/authentication failure. Verify state before manually retrying writes.
- `settings.prompt` remains the complete prompt template. `{mode}` is optional;
  `settings.continuation_prompt` controls pending SQL (default `"> "`).
- Embedded skills are supplied inline from the binary; no runtime extraction or
  inaccessible local skills path is advertised. Only SKILL.md subtrees contribute
  Markdown, not the maintainer-level `cli/skills/README.md`.
