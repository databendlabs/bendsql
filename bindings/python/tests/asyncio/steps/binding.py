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
from behave.api.async_step import async_run_until_complete
import databend_driver


@given("A new Databend Driver Client")
@async_run_until_complete
async def _(context):
    dsn = os.getenv(
        "TEST_DATABEND_DSN",
        "databend://root:root@localhost:8000/?sslmode=disable",
    )
    client = databend_driver.AsyncDatabendClient(dsn)
    context.conn = await client.get_conn()
    context.client = client


@when("Create a test table")
@async_run_until_complete
async def _(context):
    await context.conn.exec("DROP TABLE IF EXISTS test")
    await context.conn.exec(
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
@async_run_until_complete
async def _(context, input, output):
    row = await context.conn.query_row("SELECT ?", input)
    value = row.values()[0]
    assert output == value, f"output: {output}"


@then("Select params binding")
@async_run_until_complete
async def _(context):
    # Test with positional parameters
    row = await context.conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"

    # Test with named parameters
    row = await context.conn.query_row(
        "SELECT :a, :b, :c, :d", {"a": 3, "b": False, "c": 4, "d": "55"}
    )
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"

    # Test with positional parameters again
    row = await context.conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
    assert row.values() == (3, False, 4, "55"), f"output: {row.values()}"


@then("Select types should be expected native types")
@async_run_until_complete
async def _(context):
    # Binary
    row = await context.conn.query_row("select to_binary(?)", "xyz")
    assert row.values() == (b"xyz",), f"Binary: {row.values()}"

    # Array
    row = await context.conn.query_row("select to_binary(?)", ["xyz"])
    assert row.values() == (b"xyz",), f"Binary: {row.values()}"

    # Tuple
    row = await context.conn.query_row("select to_binary(?)", params=("xyz"))
    assert row.values() == (b"xyz",), f"Binary: {row.values()}"

    # Interval
    row = await context.conn.query_row("select to_interval('1 hours')")
    assert row.values() == (timedelta(hours=1),), f"Interval: {row.values()}"

    # Decimal
    row = await context.conn.query_row("SELECT 15.7563::Decimal(8,4), 2.0+3.0")
    assert row.values() == (
        Decimal("15.7563"),
        Decimal("5.0"),
    ), f"Decimal: {row.values()}"

    # Array
    row = await context.conn.query_row("select [10::Decimal(15,2), 1.1+2.3]")
    assert row.values() == ([Decimal("10.00"), Decimal("3.40")],), (
        f"Array: {row.values()}"
    )

    # Map
    row = await context.conn.query_row("select {'xx':to_date('2020-01-01')}")
    assert row.values() == ({"xx": date(2020, 1, 1)},), f"Map: {row.values()}"

    # Tuple
    row = await context.conn.query_row(
        "select (10, '20', to_datetime('2024-04-16 12:34:56.789'))"
    )
    assert row.values() == ((10, "20", datetime(2024, 4, 16, 12, 34, 56, 789000)),), (
        f"Tuple: {row.values()}"
    )


@then("Select numbers should iterate all rows")
@async_run_until_complete
async def _(context):
    rows = await context.conn.query_iter("SELECT number FROM numbers(5)")
    ret = [row.values()[0] for row in rows]
    expected = [0, 1, 2, 3, 4]
    assert ret == expected, f"ret: {ret}"


@then("Insert and Select should be equal")
@async_run_until_complete
async def _(context):
    await context.conn.exec(
        r"""
        INSERT INTO test VALUES
            (-1, 1, 1.0, '\'', NULL, '2011-03-06', '2011-03-06 06:20:00'),
            (-2, 2, 2.0, '"', '', '2012-05-31', '2012-05-31 11:20:00'),
            (-3, 3, 3.0, '\\', 'NULL', '2016-04-04', '2016-04-04 11:30:00')
        """
    )
    rows = await context.conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', "", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


@then("Stream load and Select should be equal")
@async_run_until_complete
async def _(context):
    values = [
        ["-1", "1", "1.0", "'", "\\N", "2011-03-06", "2011-03-06T06:20:00Z"],
        ["-2", "2", "2.0", '"', "", "2012-05-31", "2012-05-31T11:20:00Z"],
        ["-3", "3", "3.0", "\\", "NULL", "2016-04-04", "2016-04-04T11:30:00Z"],
    ]
    progress = await context.conn.stream_load("INSERT INTO test VALUES", values)
    assert progress.write_rows == 3, f"progress.write_rows: {progress.write_rows}"
    assert progress.write_bytes == 194, f"progress.write_bytes: {progress.write_bytes}"

    rows = await context.conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


async def test_load_file(context, load_method):
    progress = await context.conn.load_file(
        "INSERT INTO test VALUES from @_databend_load file_format = (type=csv)",
        "tests/data/test.csv",
        load_method,
    )
    assert progress.write_rows == 3, (
        f"{load_method} progress.write_rows: {progress.write_rows}"
    )
    assert progress.write_bytes == 194, (
        f"{load_method} progress.write_bytes: {progress.write_bytes}"
    )

    rows = await context.conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"{load_method} ret: {ret}"


@then("Load file with Stage and Select should be equal")
@async_run_until_complete
async def _(context):
    await test_load_file(context, "stage")


@then("Load file with Streaming and Select should be equal")
@async_run_until_complete
async def _(context):
    await test_load_file(context, "streaming")


@then("Temp table should work with cluster")
@async_run_until_complete
async def _(context):
    conn = await context.client.get_conn()
    for i in range(10):
        await conn.exec(f"create or replace temp table temp_{i}(a int)")
        await conn.exec(f"INSERT INTO temp_{i} VALUES (1),({i})")
        rows = await conn.query_iter(f"SELECT * FROM temp_{i}")
        ret = [row.values() for row in rows]
        expected = [(1,), (i,)]
        assert ret == expected, f"ret: {ret}"

    await conn.exec("DROP TABLE temp_1")

    # use conn which is stickied to the node
    rows = await conn.query_iter("SELECT COUNT(*) FROM system.temporary_tables")
    temp_table_count = list(rows)[0].values()[0]
    assert temp_table_count == 9, f"temp_table_count before close = {temp_table_count}"
    await conn.close()

    # check 3 nodes behind nginx
    for _ in range(3):
        rows = await context.conn.query_iter(
            "SELECT COUNT(*) FROM system.temporary_tables"
        )
        temp_table_count = list(rows)[0].values()[0]
        assert temp_table_count == 0, (
            f"temp_table_count after close = {temp_table_count}"
        )
