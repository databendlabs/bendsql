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

DB_VERSION = os.getenv("DB_VERSION")
if DB_VERSION is not None:
    DB_VERSION = tuple(map(int, DB_VERSION.split(".")))
else:
    DB_VERSION = (100, 0, 0)

DRIVER_VERSION = os.getenv("DRIVER_VERSION")
if DRIVER_VERSION is not None:
    DRIVER_VERSION = tuple(map(int, DRIVER_VERSION.split(".")))
else:
    DRIVER_VERSION = (100, 0, 0)


@given("A new Databend Driver Client")
def _(context):
    dsn = os.getenv(
        "TEST_DATABEND_DSN",
        "databend://root:root@localhost:8000/?sslmode=disable",
    )
    client = databend_driver.BlockingDatabendClient(dsn)
    context.conn = client.get_conn()
    context.client = client


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
def _(context):
    # Binary
    row = context.conn.query_row("select to_binary('xyz')")
    assert row.values() == (b"xyz",), f"Binary: {row.values()}"

    # Interval
    row = context.conn.query_row("select to_interval('1 microseconds')")
    assert row.values() == (timedelta(microseconds=1),), f"Interval: {row.values()}"

    # Decimal
    row = context.conn.query_row("SELECT 15.7563::Decimal(8,4), 2.0+3.0", params=[8, 4])
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
    ret = [row.values()[0] for row in rows]
    expected = [0, 1, 2, 3, 4]
    assert ret == expected, f"ret: {ret}"


@then("Insert and Select should be equal")
def _(context):
    context.conn.exec(
        r"""
        INSERT INTO test VALUES
            (-1, 1, 1.0, '\'', NULL, '2011-03-06', '2011-03-06 06:20:00'),
            (-2, 2, 2.0, '"', '', '2012-05-31', '2012-05-31 11:20:00'),
            (-3, 3, 3.0, '\\', 'NULL', '2016-04-04', '2016-04-04 11:30:00')
        """
    )
    rows = context.conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', "", date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


@then("Stream load and Select should be equal")
def _(context):
    values = [
        ["-1", "1", "1.0", "'", "\\N", "2011-03-06", "2011-03-06T06:20:00Z"],
        ["-2", "2", "2.0", '"', "", "2012-05-31", "2012-05-31T11:20:00Z"],
        ["-3", "3", "3.0", "\\", "NULL", "2016-04-04", "2016-04-04T11:30:00Z"],
    ]
    progress = context.conn.stream_load("INSERT INTO test VALUES", values)
    assert progress.write_rows == 3, f"progress.write_rows: {progress.write_rows}"

    rows = context.conn.query_iter("SELECT * FROM test")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"ret: {ret}"


def test_load_file(context, load_method):
    if DRIVER_VERSION >= (0, 28, 3) and DB_VERSION >= (1, 2, 792):
        context.conn.exec("CREATE OR REPLACE DATABASE db1")
        context.conn.exec("use db1")
    context.conn.exec(
        """
        CREATE OR REPLACE TABLE test1 (
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
    progress = context.conn.load_file(
        "INSERT INTO test1 VALUES FROM @_databend_load file_format = (type=csv)",
        "tests/data/test.csv",
    )
    assert progress.write_rows == 3, (
        f"{load_method} progress.write_rows: {progress.write_rows}"
    )

    rows = context.conn.query_iter("SELECT * FROM test1")
    ret = [row.values() for row in rows]
    expected = [
        (-1, 1, 1.0, "'", None, date(2011, 3, 6), datetime(2011, 3, 6, 6, 20)),
        (-2, 2, 2.0, '"', None, date(2012, 5, 31), datetime(2012, 5, 31, 11, 20)),
        (-3, 3, 3.0, "\\", "NULL", date(2016, 4, 4), datetime(2016, 4, 4, 11, 30)),
    ]
    assert ret == expected, f"{load_method} ret: {ret}"


@then("Load file with Stage and Select should be equal")
def _(context):
    test_load_file(context, "stage")


@then("Load file with Streaming and Select should be equal")
def _(context):
    test_load_file(context, "streaming")


@then("Temp table should work with cluster")
def _(context):
    conn = context.client.get_conn()
    for i in range(10):
        conn.exec(f"create or replace temp table temp_{i}(a int)")
        conn.exec(f"INSERT INTO temp_{i} VALUES (1),({i})")
        rows = conn.query_iter(f"SELECT * FROM temp_{i}")
        ret = [row.values() for row in rows]
        expected = [(1,), (i,)]
        assert ret == expected, f"ret: {ret}, expected: {expected}"
        if i == 1:
            conn.exec(f"DROP TABLE temp_{i}")

    # use conn which is stickied to the node
    rows = conn.query_iter("SELECT COUNT(*) FROM system.temporary_tables")
    temp_table_count = list(rows)[0].values()[0]
    assert temp_table_count == 9, f"temp_table_count before close = {temp_table_count}"

    conn.close()

    # check 3 nodes behind nginx
    for _ in range(3):
        rows = context.conn.query_iter("SELECT COUNT(*) FROM system.temporary_tables")
        temp_table_count = list(rows)[0].values()[0]
        assert temp_table_count == 0, (
            f"temp_table_count after close = {temp_table_count}"
        )


@then("last_query_id should return query ID after execution")
def _(context):
    if DRIVER_VERSION < (0, 28, 3):
        return

    # Initially no query ID
    assert context.conn.last_query_id() is None, "Initially should have no query ID"

    # Execute a query
    context.conn.query_row("SELECT 1")

    # Should have a query ID now
    query_id1 = context.conn.last_query_id()
    assert query_id1 is not None, "Should have a query ID after query execution"
    assert isinstance(query_id1, str), "Query ID should be a string"
    assert len(query_id1) > 0, "Query ID should not be empty"

    # Execute another query
    context.conn.query_row("SELECT 2")

    # Should have a different query ID
    query_id2 = context.conn.last_query_id()
    assert query_id2 is not None, "Should have a query ID after second query execution"
    assert isinstance(query_id2, str), "Query ID should be a string"
    assert query_id1 != query_id2, "Query IDs should be different"

    # Test with queryIter
    rows = context.conn.query_iter("SELECT number FROM numbers(3)")
    list(rows)  # Consume the iterator
    query_id3 = context.conn.last_query_id()
    assert query_id3 is not None, "Should have a query ID after queryIter execution"
    assert query_id2 != query_id3, "Query IDs should be different"

    # Test with exec
    context.conn.exec("SELECT 42")
    query_id4 = context.conn.last_query_id()
    assert query_id4 is not None, "Should have a query ID after exec execution"
    assert query_id3 != query_id4, "Query IDs should be different"


@then("killQuery should return error for non-existent query ID")
def _(context):
    if DRIVER_VERSION < (0, 28, 3):
        return

    # Test API signature
    assert hasattr(context.conn, "kill_query"), "kill_query should be a method"
    assert callable(getattr(context.conn, "kill_query")), (
        "kill_query should be callable"
    )

    # Test killing non-existent query with valid UUID format
    non_existent_query_id = "12345678-1234-1234-1234-123456789012"

    try:
        context.conn.kill_query(non_existent_query_id)
        assert False, (
            "kill_query should have raised an exception for non-existent query ID"
        )
    except Exception as err:
        # Should get an error for non-existent query
        assert isinstance(err, Exception), "Should raise an Exception"
        assert isinstance(err.args[0], str) and len(err.args[0]) > 0, (
            "Should return meaningful error message"
        )
        print(f"Expected error for non-existent query: {err}")
