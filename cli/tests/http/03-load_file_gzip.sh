#!/bin/bash

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS http_ontime_03;
SQL

${BENDSQL} <cli/tests/data/ontime.sql

${BENDSQL} \
    --query='INSERT INTO http_ontime_03 VALUES from @_databend_load file_format=(type=csv, compression=gzip, skip_header=1);' \
    --load-method="streaming" \
    --data=@cli/tests/data/ontime_200.csv.gz

echo "SELECT COUNT(*) FROM http_ontime_03;" | ${BENDSQL} --output=tsv
echo 'SELECT * FROM http_ontime_03 LIMIT 1;' | ${BENDSQL} --output=csv

cat <<SQL | ${BENDSQL}
DROP TABLE http_ontime_03;
SQL
