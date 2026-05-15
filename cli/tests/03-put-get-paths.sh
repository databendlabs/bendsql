#!/bin/bash

set -euo pipefail

stage="ss_03_put_get_paths"
workdir="/tmp/bendsql_put_get_paths"
src="${workdir}/src"
dst="${workdir}/dst"
file_dst="${workdir}/file-dst"

rm -rf "${workdir}"
mkdir -p "${src}/nested/a" "${src}/nested/b"
printf 'name\nroot\n' >"${src}/c.csv"
printf 'name\na\n' >"${src}/nested/a/same.csv"
printf 'name\nb\n' >"${src}/nested/b/same.csv"
printf 'not a dir\n' >"${file_dst}"

cleanup() {
	echo "DROP STAGE IF EXISTS ${stage}" | ${BENDSQL} >/dev/null 2>&1 || true
	rm -rf "${workdir}"
}
trap cleanup EXIT

echo "DROP STAGE IF EXISTS ${stage}" | ${BENDSQL} >/dev/null
echo "CREATE STAGE ${stage}" | ${BENDSQL} >/dev/null

run_query() {
	local query="$1"
	set +e
	echo "${query}" | BENDSQL_ERROR_NO_VERSION=1 RUST_BACKTRACE=0 ${BENDSQL} --output=tsv 2>&1 |
		sed "s#${workdir}#<workdir>#g"
	set -e
}

echo "---- put no match ----"
run_query "put fs://${src}/missing*.csv @${stage}/prefix"

echo "---- put stage dir ----"
run_query "put fs://${src}/c.csv @${stage}/prefix"

echo "---- get prefix ----"
run_query "get @${stage}/prefix/c fs://${dst}"

echo "---- get single file ----"
rm -rf "${dst}"
run_query "get @${stage}/prefix/c.csv fs://${dst}"

echo "---- get local file ----"
run_query "get @${stage}/prefix/c fs://${file_dst}"

echo "---- get no match ----"
run_query "get @${stage}/missing fs://${dst}"

echo "---- duplicate basename ----"
run_query "put fs://${src}/nested/a/same.csv @${stage}/dups/a"
run_query "put fs://${src}/nested/b/same.csv @${stage}/dups/b"
run_query "get @${stage}/dups fs://${dst}"
