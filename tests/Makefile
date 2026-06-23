DATABEND_META_VERSION ?= nightly
DATABEND_QUERY_VERSION ?= nightly
PYTHON ?= python3
DOCKER_PULL_RETRIES ?= 3

default: run

run: test-core test-driver test-bendsql down

prepare:
	mkdir -p data/databend

pull-images:
	@attempt=1; \
	while [ $$attempt -le $(DOCKER_PULL_RETRIES) ]; do \
		echo "Pulling Docker images, attempt $$attempt/$(DOCKER_PULL_RETRIES)"; \
		if docker compose pull --policy always --quiet; then \
			exit 0; \
		fi; \
		if [ $$attempt -eq $(DOCKER_PULL_RETRIES) ]; then \
			echo "Docker image pull failed after $(DOCKER_PULL_RETRIES) attempts"; \
			exit 1; \
		fi; \
		sleep_seconds=$$((attempt * 10)); \
		echo "Docker image pull failed; retrying in $${sleep_seconds}s"; \
		sleep $$sleep_seconds; \
		attempt=$$((attempt + 1)); \
	done

up: prepare pull-images
	docker compose up --scale query=3 --pull never --force-recreate --quiet-pull -d  --wait
	grep -q '127.0.0.1 minio' /etc/hosts || echo '127.0.0.1 minio' | sudo tee -a /etc/hosts > /dev/null
	curl  -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select version()",  "pagination": { "wait_time_secs": 10}}'

start: up

test-core: up
	cargo test --test core

test-driver: up
	cargo test --test driver
	TEST_DATABEND_DSN=databend+flight://root:@localhost:8900/default?sslmode=disable cargo test --features flight-sql --test driver

test-bendsql: up
	cd .. && ./cli/test.sh http
	cd .. && ./cli/test.sh flight
	QUERY_RESULT_FORMAT=arrow cd .. && ./cli/test.sh http

prepare-bindings-python:
	if [ -n "$$CI" ]; then \
		echo "Skipping local Python bindings setup in CI"; \
	else \
		command -v uv >/dev/null || { echo "uv is required for local Python binding tests"; exit 1; }; \
		cd ../bindings/python && uv sync && uv run maturin develop --uv; \
	fi

test-bindings-python: up prepare-bindings-python
	cd ../bindings/python && \
	if [ -n "$$CI" ]; then \
		python_bin="$(PYTHON)"; \
	else \
		python_bin=".venv/bin/python"; \
	fi; \
	for suite in tests/asyncio tests/blocking tests/cursor; do \
		"$$python_bin" -m behave "$$suite" || exit $$?; \
	done

test-bindings-nodejs: up
	cd ../bindings/nodejs && pnpm run test

down:
	docker compose down

stop: down
