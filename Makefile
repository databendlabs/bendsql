.PHONY: check build test integration

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
	pnpm build && cp -rf build ../cli/frontend/

run:
	make build-frontend
	cargo run

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
