#!/bin/bash

echo "CREATE STAGE s_temp" | ${BENDSQL}

echo "ABCD" > /tmp/a1.txt
echo "ABCD" > /tmp/a2.txt

echo 'put fs:///tmp/a*.txt @s_temp/abc' | ${BENDSQL}
