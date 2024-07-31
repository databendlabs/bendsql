#!/bin/bash

DATABEND_USER=${DATABEND_USER:-root}
DATABEND_PASSWORD=${DATABEND_PASSWORD:-}
DATABEND_HOST=${DATABEND_HOST:-localhost}

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS books_06;
CREATE TABLE books_06
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR,
    comment VARCHAR
);
SQL

cat <<SQL | ${BENDSQL}
INSERT INTO books_06 (title, author, date) VALUES ('foo', 'bar', '2021-01-01');
SQL

export BENDSQL_DSN="databend+http://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8000/?sslmode=disable&presign=on&format_null_as_str=0"

cat <<SQL | ${BENDSQL} --output=csv
select * from books_06;
SQL

export BENDSQL_DSN="databend+http://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8000/?sslmode=disable&presign=on&format_null_as_str=1"

cat <<SQL | ${BENDSQL} --output=csv
select * from books_06;
SQL
