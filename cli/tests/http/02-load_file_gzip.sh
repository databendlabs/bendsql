#!/bin/bash

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS test_ontime;
SQL

${BENDSQL} <cli/tests/data/ontime.sql

${BENDSQL} \
    --query='INSERT INTO test_ontime VALUES;' \
    --format=csv \
    --format-opt compression=gzip \
    --format-opt skip_header=1 \
    --data=@cli/tests/data/ontime_200.csv.gz

${BENDSQL} --query='SELECT * FROM test_ontime LIMIT 1;'
