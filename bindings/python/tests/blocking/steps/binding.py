# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from datetime import datetime, date, timedelta
from decimal import Decimal

from behave import given, when, then
import databend_driver


@given("A new Databend Driver Client")
def _(context):
    dsn = os.getenv(
        "TEST_DATABEND_DSN",
        "databend://root:root@localhost:8000/?sslmode=disable",
    )
    client = databend_driver.BlockingDatabendClient(dsn)
    context.conn = client.get_conn()


@when("Create a test table")
def _(context):
    context.conn.exec("DROP TABLE IF EXISTS test")
    context.conn.exec(
        """
        CREATE TABLE test (
            i64 Int64,
            u64 UInt64,
            f64 Float64,
            s   String,
            s2  String,
            d   Date,
            t   DateTime
        )
        """
    )


@then("Select string {input} should be equal to {output}")
def _(context, input, output):
    row = context.conn.query_row(f"SELECT '{input}'")
    value = row.values()[0]
    assert output == value, f"output: {output}"


@then("Select params binding")
def _(context):
    # Test with positional parameters
    row = context.conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"

    # Test with named parameters
    row = context.conn.query_row(
        "SELECT :a, :b, :c, :d", {"a": 3, "b": False, "c": 4, "d": "55"}
    )
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"

    row = context.conn.query_row("SELECT ?", 4)
    assert row.values() == (4,), f"output: {row.values()}"

    # Test with positional parameters again
    row = context.conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"


@then("Select types should be expected native types")
async def _(context):
    # Binary
    row = context.conn.query_row("select to_binary('xyz')")
    assert row.values() == (b"xyz",), f"Binary: {row.values()}"

    # Interval
    row = context.conn.query_row("select to_interval('1 microseconds')")
    assert row.values() == (timedelta(microseconds=1),), f"Interval: {row.values()}"

    # Decimal
    row = context.conn.query_row("SELECT 15.7563::Decimal(?,?), 2.0+3.0", params=[8, 4])
    assert row.values() == (
        Decimal("15.7563"),
        Decimal("5.0"),
    ), f"Decimal: {row.values()}"

    # Array
    row = context.conn.query_row("select [10::Decimal(15,2), 1.1+2.3]")
    assert row.values() == ([Decimal("10.00"), Decimal("3.40")],), (
        f"Array: {row.values()}"
    )

    # Map
    row = context.conn.query_row("select {'xx':to_date('2020-01-01')}")
    assert row.values() == ({"xx": date(2020, 1, 1)},), f"Map: {row.values()}"

    # Tuple
    row = context.conn.query_row(
        "select (10, '20', to_datetime('2024-04-16 12:34:56.789'))"
    )
    assert row.values() == ((10, "20", datetime(2024, 4, 16, 12, 34, 56, 789000)),), (
        f"Tuple: {row.values()}"
    )


@then("Select numbers should iterate all rows")
def _(context):
    rows = context.conn.query_iter("SELECT number FROM numbers(5)")
    ret = []
    for row in rows:
        ret.append(row.values()[0])
    expected = [0, 1, 2, 3, 4]
    assert ret == expected, f"ret: {ret}"


@then("Insert and Select should be equal")
def _(context):
    context.conn.exec(
        """
        INSERT INTO test VALUES
            (-1, 1, 1.0, '1', '1', '2011-03-06', '2011-03-06 06:20:00'),
            (-2, 2, 2.0, '2', '2', '2012-05-31', '2012-05-31 11:20:00'),
            (-3, 3, 3.0, '3', '2', '2016-04-04', '2016-04-04 11:30:00')
        """
    )
    rows = context.conn.query_iter("SELECT * FROM test")
    ret = []
    for row in rows:
        ret.append(row.values())
    expected = [
        (-1, 1, 1.0, "1", "1", date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, "2", "2", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "3", "2", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


@then("Stream load and Select should be equal")
def _(context):
    values = [
        ["-1", "1", "1.0", "1", "1", "2011-03-06", "2011-03-06T06:20:00Z"],
        ["-2", "2", "2.0", "2", "2", "2012-05-31", "2012-05-31T11:20:00Z"],
        ["-3", "3", "3.0", "3", "2", "2016-04-04", "2016-04-04T11:30:00Z"],
    ]
    progress = context.conn.stream_load("INSERT INTO test VALUES", values)
    assert progress.write_rows == 3, f"progress.write_rows: {progress.write_rows}"
    assert progress.write_bytes == 193, f"progress.write_bytes: {progress.write_bytes}"

    rows = context.conn.query_iter("SELECT * FROM test")
    ret = []
    for row in rows:
        ret.append(row.values())
    expected = [
        (-1, 1, 1.0, "1", "1", date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, "2", "2", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "3", "2", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


@then("Load file and Select should be equal")
def _(context):
    progress = context.conn.load_file(
        "INSERT INTO test VALUES", "tests/data/test.csv", {"type": "CSV"}
    )
    assert progress.write_rows == 3, f"progress.write_rows: {progress.write_rows}"
    assert progress.write_bytes == 194, f"progress.write_bytes: {progress.write_bytes}"

    rows = context.conn.query_iter("SELECT * FROM test")
    ret = []
    for row in rows:
        ret.append(row.values())
    expected = [
        (-1, 1, 1.0, "1", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, "2", "", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "3", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


@then("Temp table should work with cluster")
async def _(context):
    context.conn.exec("create or replace temp table temp_1(a int)")
    context.conn.exec("INSERT INTO temp_1 VALUES (1),(2)")
    rows = context.conn.query_iter("SELECT * FROM temp_1")
    ret = []
    for row in rows:
        ret.append(row.values())
    expected = [(1), (2)]
    assert ret == expected, f"ret: {ret}"
    context.conn.exec("DROP TABLE temp_1")
