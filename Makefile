.PHONY: check build test integration

default: build

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
	cd frontend && yarn && yarn build
	cd ../
	mkdir -p target/release/frontend
	rm -rf target/release/frontend/*
	cp -r frontend/build/* target/release/frontend/

build:
	cargo build --release
	make build-frontend

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
