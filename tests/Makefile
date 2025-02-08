default: run

run: test-core test-driver test-bendsql down

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --scale query=3 --quiet-pull -d  --wait
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

test-bindings-python: up
	cd ../bindings/python && behave tests/asyncio
	cd ../bindings/python && behave tests/blocking
	cd ../bindings/python && behave tests/cursor

test-bindings-nodejs: up
	cd ../bindings/nodejs && pnpm run test

down:
	docker compose down

stop: down
