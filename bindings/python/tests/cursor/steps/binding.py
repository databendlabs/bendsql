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
from datetime import datetime, date
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


@then("Select string {input} should be equal to {output}")
def _(context, input, output):
    context.cursor.execute(f"SELECT '{input}'")
    row = context.cursor.fetch_one()
    assert output == row[0], f"output: {output}"


@then("Select types should be expected native types")
async def _(context):
    # Binary
    context.cursor.execute("select to_binary('xyz')")
    row = context.cursor.fetch_one()
    assert row[0] == b"xyz", f"Binary: {row.values()}"

    # Decimal
    context.cursor.execute("SELECT 15.7563::Decimal(8,4), 2.0+3.0")
    row = context.cursor.fetch_one()
    assert row.values() == (
        Decimal("15.7563"),
        Decimal("5.0"),
    ), f"Decimal: {row.values()}"

    # Array
    context.cursor.execute("select [10::Decimal(15,2), 1.1+2.3]")
    row = context.cursor.fetch_one()
    assert row.values() == (
        [Decimal("10.00"), Decimal("3.40")],
    ), f"Array: {row.values()}"

    # Map
    context.cursor.execute("select {'xx':to_date('2020-01-01')}")
    row = context.cursor.fetch_one()
    assert row.values() == ({"xx": date(2020, 1, 1)},), f"Map: {row.values()}"

    # Tuple
    context.cursor.execute("select (10, '20', to_datetime('2024-04-16 12:34:56.789'))")
    row = context.cursor.fetch_one()
    assert row.values() == (
        (10, "20", datetime(2024, 4, 16, 12, 34, 56, 789000)),
    ), f"Tuple: {row.values()}"


@then("Select numbers should iterate all rows")
def _(context):
    context.cursor.execute("SELECT number FROM numbers(5)")
    rows = context.cursor.fetch_all()
    ret = []
    for row in rows:
        ret.append(row[0])
    expected = [0, 1, 2, 3, 4]
    assert ret == expected, f"ret: {ret}"


@then("Insert and Select should be equal")
def _(context):
    context.cursor.exec(
        """
        INSERT INTO test VALUES
            (-1, 1, 1.0, '1', '1', '2011-03-06', '2011-03-06 06:20:00'),
            (-2, 2, 2.0, '2', '2', '2012-05-31', '2012-05-31 11:20:00'),
            (-3, 3, 3.0, '3', '2', '2016-04-04', '2016-04-04 11:30:00')
        """
    )
    context.cursor.execute("SELECT * FROM test")
    rows = context.cursor.fetch_all()
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
        [-1, 1, 1.0, "1", "1", "2011-03-06", "2011-03-06T06:20:00Z"],
        [-2, 2, 2.0, "2", "2", "2012-05-31", "2012-05-31T11:20:00Z"],
        [-3, 3, 3.0, "3", "2", "2016-04-04", "2016-04-04T11:30:00Z"],
    ]
    count = context.cursor.executemany("INSERT INTO test VALUES", values)
    assert count == 3, f"count: {count}"

    context.cursor.execute("SELECT * FROM test")
    rows = context.cursor.fetch_all()
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
    print("SKIP")