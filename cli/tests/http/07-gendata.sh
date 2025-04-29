#!/bin/bash

cat <<SQL | ${BENDSQL}
CREATE or replace DATABASE test;
use test;

select '==========TPCH=========';
gendata(tpch, sf = 0.01, override = 1);
select '==========TPCDS=========';
gendata(tpcds, sf = 0.01, override = 1);

use default;
DROP DATABASE test;
SQL
