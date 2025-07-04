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
    context.client = client
    context.cursor = client.cursor()


@when("Create a test table")
def _(context):
    context.cursor.execute("DROP TABLE IF EXISTS test")
    context.cursor.execute(
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


@then("Select params binding")
def _(context):
    context.cursor.execute("SELECT ?, ?, ?, ?, ?", (3, False, 4, "55", None))
    row = context.cursor.fetchone()
    assert row.values() == (3, False, 4, "55", None), f"output: {row.values()}"

    # Test with named parameters
    context.cursor.execute(
        "SELECT :a, :b, :c, :d, :e",
        {"a": 3, "b": False, "c": 4, "d": "55", "e": None},
    )
    row = context.cursor.fetchone()
    assert row.values() == (3, False, 4, "55", None), f"output: {row.values()}"

    context.cursor.execute("SELECT ?", 4)
    row = context.cursor.fetchone()
    assert row.values() == (4,), f"output: {row.values()}"

    # Test with positional parameters again
    context.cursor.execute("SELECT ?, ?, ?, ?, ?", (3, False, 4, "55", None))
    row = context.cursor.fetchone()
    assert row.values() == (3, False, 4, "55", None), f"output: {row.values()}"


@then("Select string {input} should be equal to {output}")
def _(context, input, output):
    context.cursor.execute(f"SELECT '{input}'")
    row = context.cursor.fetchone()

    # getitem
    assert output == row[0], f"output: {output}"

    # iter
    val = next(row)
    assert val == output, f"val: {val}"


@then("Select types should be expected native types")
async def _(context):
    # Binary
    context.cursor.execute("select to_binary('xyz')")
    row = context.cursor.fetchone()
    expected = (b"xyz",)
    assert row.values() == expected, f"Binary: {row.values()}"

    # Interval
    context.cursor.execute("select to_interval('1 days')")
    row = context.cursor.fetchone()
    expected = (timedelta(1),)
    assert row.values() == expected, f"Interval: {row.values()}"

    # Decimal
    context.cursor.execute("SELECT 15.7563::Decimal(8,4), 2.0+3.0")
    row = context.cursor.fetchone()
    expected = (Decimal("15.7563"), Decimal("5.0"))
    assert row.values() == expected, f"Decimal: {row.values()}"

    # Array
    context.cursor.execute("select [10::Decimal(15,2), 1.1+2.3]")
    row = context.cursor.fetchone()
    expected = [Decimal("10.00"), Decimal("3.40")]
    assert row.values() == expected, f"Array: {row.values()}"

    # Map
    context.cursor.execute("select {'xx':to_date('2020-01-01')}")
    row = context.cursor.fetchone()
    expected = {"xx": date(2020, 1, 1)}
    assert row.values() == expected, f"Map: {row.values()}"

    # Tuple
    context.cursor.execute("select (10, '20', to_datetime('2024-04-16 12:34:56.789'))")
    row = context.cursor.fetchone()
    expected = (
        10,
        "20",
        datetime(2024, 4, 16, 12, 34, 56, 789000),
    )
    assert row.values() == expected, f"Tuple: {row.values()}"


@then("Select numbers should iterate all rows")
def _(context):
    context.cursor.execute("SELECT number FROM numbers(5)")

    rows = context.cursor.fetchmany(3)
    ret = [row[0] for row in rows]
    expected = [0, 1, 2]
    assert ret == expected, f"ret: {ret}"

    rows = context.cursor.fetchmany(3)
    ret = [row[0] for row in rows]
    expected = [3, 4]
    assert ret == expected, f"ret: {ret}"


@then("Insert and Select should be equal")
def _(context):
    context.cursor.execute(
        r"""
        INSERT INTO test VALUES
            (-1, 1, 1.0, '\'', NULL, '2011-03-06', '2011-03-06 06:20:00'),
            (-2, 2, 2.0, '"', '', '2012-05-31', '2012-05-31 11:20:00'),
            (-3, 3, 3.0, '\\', 'NULL', '2016-04-04', '2016-04-04 11:30:00')
        """
    )
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', "", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]

    # fetchall
    context.cursor.execute("SELECT * FROM test")
    rows = context.cursor.fetchall()
    ret = [row.values() for row in rows]
    assert ret == expected, f"ret: {ret}"

    desc = context.cursor.description
    assert desc is not None

    # fetchmany
    context.cursor.execute("SELECT * FROM test")
    rows = context.cursor.fetchmany(3)
    ret = [row.values() for row in rows]
    assert ret == expected, f"ret: {ret}"

    # iter
    context.cursor.execute("SELECT * FROM test")
    ret = [row.values() for row in context.cursor]
    assert ret == expected, f"ret: {ret}"


@then("Stream load and Select should be equal")
def _(context):
    values = [
        [-1, 1, 1.0, "'", None, "2011-03-06", "2011-03-06T06:20:00Z"],
        (-2, "2", 2.0, '"', "", "2012-05-31", "2012-05-31T11:20:00Z"),
        ["-3", 3, 3.0, "\\", "NULL", "2016-04-04", "2016-04-04T11:30:00Z"],
    ]
    count = context.cursor.executemany("INSERT INTO test VALUES", values)
    assert count == 3, f"count: {count}"

    context.cursor.execute("SELECT * FROM test")
    rows = context.cursor.fetchall()
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


@then("Temp table should work with cluster")
def _(context):
    cursor = context.client.cursor()
    for i in range(10):
        cursor.execute(f"create or replace temp table temp_{i}(a int)")
        cursor.execute(f"INSERT INTO temp_{i} VALUES (1),({i})")
        cursor.execute(f"SELECT * FROM temp_{i}")
        rows = cursor.fetchall()
        ret = [row.values() for row in rows]
        expected = [(1,), (i,)]
        assert ret == expected, f"ret: {ret}"
        if i == 1:
            cursor.execute(f"DROP TABLE temp_{i}")

    # use cursor which is stickied to the node
    cursor.execute(f"SELECT COUNT(*) FROM system.temporary_tables")
    rows = cursor.fetchall()
    temp_table_count = list(rows)[0].values()[0]
    assert temp_table_count == 9, f"temp_table_count before close = {temp_table_count}"

    cursor.close()

    # check 3 nodes behind nginx
    for _ in range(3):
        context.cursor.execute(f"SELECT COUNT(*) FROM system.temporary_tables")
        rows = context.cursor.fetchall()
        temp_table_count = list(rows)[0].values()[0]
        assert temp_table_count == 0, (
            f"temp_table_count after close = {temp_table_count}"
        )


@then("Load file and Select should be equal")
def _(context):
    print("SKIP")
