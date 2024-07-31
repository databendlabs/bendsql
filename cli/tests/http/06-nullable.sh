#!/bin/bash

DATABEND_USER=${DATABEND_USER:-root}
DATABEND_PASSWORD=${DATABEND_PASSWORD:-}
DATABEND_HOST=${DATABEND_HOST:-localhost}

export BENDSQL_DSN="databend+http://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8000/?sslmode=disable&presign=on&format_null_as_str=0"

cat <<SQL | ${BENDSQL} --quote-style=always
select NULL;
SQL

cat <<SQL | ${BENDSQL} --quote-style=always
select "NULL";
SQL

export BENDSQL_DSN="databend+http://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8000/?sslmode=disable&presign=on&format_null_as_str=1"

cat <<SQL | ${BENDSQL} --quote-style=always
select NULL;
SQL

cat <<SQL | ${BENDSQL} --quote-style=always
select "NULL";
SQL
