#!/bin/bash
set -eo pipefail

user="bendsql_keypair_ci"
host="${DATABEND_HOST:-localhost}"
port="${DATABEND_PORT:-8000}"
private_key_file="cli/tests/fixtures/keypair_rsa_private.pem"
public_key_file="cli/tests/fixtures/keypair_rsa_public.pem"

public_key_sql=$(python3 - "$public_key_file" <<'PY'
import pathlib
import sys

pem = pathlib.Path(sys.argv[1]).read_text()
print(pem.replace("'", "''"))
PY
)

${BENDSQL} --output null --query="DROP USER IF EXISTS ${user}"
${BENDSQL} --output null --query="CREATE USER ${user} IDENTIFIED WITH key_pair BY '${public_key_sql}'"

cleanup() {
    ${BENDSQL} --output null --query="DROP USER IF EXISTS ${user}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

env -u BENDSQL_DSN ${BENDSQL} \
    --host "${host}" \
    --port "${port}" \
    --user "${user}" \
    --private-key-file "${private_key_file}" \
    --output tsv \
    --query="SELECT current_user(), 1 + 1"

private_key_file_encoded=$(python3 - "$private_key_file" <<'PY'
import sys
import urllib.parse

print(urllib.parse.quote(sys.argv[1], safe=""))
PY
)
key_dsn="databend://${user}:@${host}:${port}/?sslmode=disable&presign=on&private_key_file=${private_key_file_encoded}"
BENDSQL_DSN="${key_dsn}" ${BENDSQL} --output tsv --query="SELECT current_user(), 'dsn'"
