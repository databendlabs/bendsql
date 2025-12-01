#!/bin/bash
set -eo pipefail

out="$(${BENDSQL} --qid --output tsv --query='select 1' 2>&1 || true)"
if echo "$out" | grep -q "Query ID:"; then
    echo "has_query_id"
else
    echo "$out"
    echo "missing_query_id"
fi
