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

import importlib.util
import os
import sys
import tempfile
from importlib import metadata
from pathlib import Path
from unittest.mock import patch

from behave import given, then, when


LOCAL_MODULE_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "package"
    / "databend_driver"
    / "local.py"
)

MIN_DATABEND_VERSION = (1, 2, 895)


def load_local_module():
    spec = importlib.util.spec_from_file_location("databend_driver.local", LOCAL_MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def parse_version(version: str) -> tuple[int, ...]:
    parts = []
    for chunk in version.split("."):
        digits = []
        for char in chunk:
            if char.isdigit():
                digits.append(char)
            else:
                break
        if not digits:
            break
        parts.append(int("".join(digits)))
    return tuple(parts)


def require_real_embedded():
    if sys.version_info < (3, 12):
        raise AssertionError(
            "local integration tests require Python 3.12+, because "
            "databend>=1.2.895 currently only publishes cp312 wheels"
        )

    try:
        import databend  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise AssertionError(
            "local integration tests require real `databend` and `pyarrow` packages installed"
        ) from exc

    try:
        version = metadata.version("databend")
    except metadata.PackageNotFoundError as exc:
        raise AssertionError("databend package metadata is not available") from exc

    if parse_version(version) < MIN_DATABEND_VERSION:
        raise AssertionError(
            f"local integration tests require databend >= 1.2.895, found {version}"
        )


@given("Real local embedded dependencies are available")
def _(context):
    require_real_embedded()
    context.local = load_local_module()
    context.tmpdirs = []


@when("A new local embedded connection is created")
def _(context):
    tmpdir = tempfile.TemporaryDirectory(prefix="bendsql-local-")
    context.tmpdirs.append(tmpdir)
    context.tmpdir = tmpdir.name
    context.conn = context.local.connect_local(context.tmpdir)


@when("A new local memory connection is created")
def _(context):
    context.conn = context.local.connect(":memory:")


@when("A new local tenant connection is created")
def _(context):
    tmpdir = tempfile.TemporaryDirectory(prefix="bendsql-tenant-")
    context.tmpdirs.append(tmpdir)
    context.tmpdir = tmpdir.name
    context.conn = context.local.connect_local(context.tmpdir, tenant="default")


@when("A parquet file is registered in local mode")
def _(context):
    import pyarrow as pa
    import pyarrow.parquet as pq

    tmpdir = tempfile.TemporaryDirectory(prefix="bendsql-register-")
    context.tmpdirs.append(tmpdir)
    context.tmpdir = tmpdir.name
    parquet_path = Path(context.tmpdir) / "books.parquet"
    pq.write_table(
        pa.table({"id": [1, 2], "name": ["databend", "bendsql"]}),
        parquet_path,
    )

    context.conn = context.local.connect_local(context.tmpdir)
    context.conn.register("books", parquet_path, format="parquet")


@when("A new local dsn connection is created")
def _(context):
    tmpdir = tempfile.TemporaryDirectory(prefix="bendsql-dsn-")
    context.tmpdirs.append(tmpdir)
    context.tmpdir = tmpdir.name
    context.local.connect("databend+local:///tmp/demo")
    context.conn = context.local.connect(
        f"databend+local:///{Path(context.tmpdir).as_posix().lstrip('/')}"
    )


@when("A new local tenant dsn connection is created")
def _(context):
    tmpdir = tempfile.TemporaryDirectory(prefix="bendsql-dsn-tenant-")
    context.tmpdirs.append(tmpdir)
    context.tmpdir = tmpdir.name
    dsn = f"databend+local:///{Path(context.tmpdir).as_posix().lstrip('/')}?tenant=test_tenant"
    context.conn = context.local.connect(dsn)


@then("Local select 1 should equal 1")
def _(context):
    assert context.conn.sql("select 1").fetchone() == (1,)


@then("Local numbers aggregate should match expected values")
def _(context):
    assert context.conn.sql("select sum(number), 'a' from numbers(101)").fetchone() == (
        5050,
        "a",
    )


@then("Local explicit memory dsn should parse as memory mode")
def _(context):
    database, data_path, tenant = context.local._parse_local_target(
        "databend+local:///:memory:", None, None
    )
    assert database == ":memory:"
    assert data_path == ":memory:"
    assert tenant is None


@then("Local execute should create and populate a table")
def _(context):
    context.conn.execute("create or replace table t(a int)")
    context.conn.exec("insert into t values (1), (2), (3)")
    assert context.conn.query_row("select sum(a) from t").values() == (6,)


@then("Local tenant connection should use the configured data path")
def _(context):
    assert str(context.conn._impl._data_path) == str(Path(context.tmpdir).resolve())
    assert context.conn.query_row("select 1").values() == (1,)


@then("Local parquet query should return expected rows")
def _(context):
    assert context.conn.query_row("select count(*) from books").values() == (2,)
    assert context.conn.query_row("select max(name), min(name) from books").values() == (
        "databend",
        "bendsql",
    )


@then("Local dsn connection should execute queries")
def _(context):
    assert context.conn.query_row("select 1").values() == (1,)


@then("Local tenant dsn connection should execute queries")
def _(context):
    assert str(context.conn._impl._data_path) == str(Path(context.tmpdir).resolve())
    assert context.conn.query_row("select 11111").values() == (11111,)


@then("Local import error should mention Python 3.12 requirement")
def _(context):
    local = load_local_module()
    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "databend":
            raise ImportError("missing databend")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with patch.object(local, "_python_version_tuple", return_value=(3, 11)):
            try:
                local.connect(":memory:")
            except ImportError as exc:
                message = str(exc)
            else:
                raise AssertionError("expected ImportError for missing databend")

    assert "databend-driver with the `local` extra" in message
    assert "Python 3.12+" in message


@then("Local blocking query api should behave like expected")
def _(context):
    context.conn.exec("create or replace table t(a int)")
    context.conn.exec("insert into t values (1), (2), (3)")
    assert context.conn.query_row("SELECT 1, 'x', TRUE").values() == (1, "x", True)
    assert [row.values() for row in context.conn.query_iter("SELECT * FROM t")] == [
        (1,),
        (2,),
        (3,),
    ]
    assert [row.values() for row in context.conn.query_all("SELECT * FROM t")] == [
        (1,),
        (2,),
        (3,),
    ]
    assert context.conn.execute("SELECT 1, 2") is None
    assert context.conn.exec("SELECT 1, 2") is None
    assert context.conn.last_query_id() is None


@then("Local parameter formatting should behave like expected")
def _(context):
    row = context.conn.query_row("SELECT ?, ?, ?", params=(1, "abc", False))
    assert row.values() == (1, "abc", False)
    formatted = context.conn.format_sql(
        "SELECT :a, :b, :c", {"a": 1, "b": "x", "c": True}
    )
    assert formatted == "SELECT 1, 'x', TRUE"
