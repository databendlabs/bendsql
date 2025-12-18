.PHONY: check build test integration bump-major bump-minor bump-patch

default: build

lint: check

check:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
	cargo deny check
	# cargo install cargo-machete
	cargo machete
	# cargo install hawkeye
	hawkeye check
	# cargo install typos-cli
	typos

build-frontend:
	rm -rf cli/frontend
	mkdir -p cli/frontend
	cd frontend && \
	if [ ! -d node_modules ]; then pnpm install; fi && \
	pnpm run build && cp -rf build ../cli/frontend/

run:
	make build-frontend
	cargo run

dev-run:
	@echo "Starting development mode..."
	@echo "1. Make sure to run 'cd frontend && pnpm run dev' in another terminal"
	@echo "2. Frontend will be available at http://localhost:3000"
	@echo "3. BendSQL CLI will proxy to frontend dev server"
	BENDSQL_DEV_MODE=1 cargo run

build:
	make build-frontend
	cargo build --release

test:
	cargo test --all --all-features --lib -- --nocapture

integration:
	make -C tests

integration-down:
	make -C tests down

integration-core:
	make -C tests test-core

integration-driver:
	make -C tests test-driver

integration-bendsql:
	make -C tests test-bendsql

integration-bindings-python:
	make -C tests test-bindings-python

integration-bindings-nodejs:
	make -C tests test-bindings-nodejs

bump-major:
	./scripts/bump_version.py major

bump-minor:
	./scripts/bump_version.py minor

bump-patch:
	./scripts/bump_version.py patch
