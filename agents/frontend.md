# Frontend

Use this guide for work in `frontend/` and for changes that affect how the CLI serves the web UI.

## Source of Truth

- Edit `frontend/` for browser UI code.
- Do not manually edit `cli/frontend`; it is generated output copied from the frontend build.
- Touch `cli/` only when changing how the CLI serves embedded assets or proxies the dev server.

## Workflow

- Interactive development: run `cd frontend && pnpm run dev` in one terminal and `make dev-run` in another.
- Production asset rebuild: run `make build-frontend`.
- `make build` and `make run` also rebuild frontend assets before building or running the CLI.

## Repo-Specific Behavior

- Development proxy mode depends on `BENDSQL_DEV_MODE=1`.
- The CLI serves generated assets from `cli/frontend` for production-style runs.
- If a task changes both browser UI and CLI wiring, validate both sides.

## Validation

- At minimum, run `cd frontend && pnpm run build` for frontend changes.
- If UI serving or proxy behavior changed, also validate the relevant CLI flow with `make dev-run` or a normal CLI run.

## Response Expectations

- State whether you changed source UI code, CLI embedding logic, or both.
- Report whether you rebuilt generated assets.
