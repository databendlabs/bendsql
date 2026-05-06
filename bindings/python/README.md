# databend-driver

Databend Python Client

[![image](https://img.shields.io/pypi/v/databend-driver.svg)](https://pypi.org/project/databend-driver)
![License](https://img.shields.io/pypi/l/databend-driver.svg)
[![image](https://img.shields.io/pypi/pyversions/databend-driver.svg)](https://pypi.org/project/databend-driver)

## Usage

### Local Embedded Connection

The local embedded mode runs a full Databend engine in-process without any
server. It is useful for local analytics, testing, and offline workflows.

Install the `local` extra to pull in the embedded engine:

```bash
pip install "databend-driver[local]"
```

The embedded dependency currently requires Python 3.12 or later.

```python
from databend_driver import connect

# Persistent state stored under ./local-state
conn = connect("databend+local:///./local-state")
conn.exec("CREATE TABLE books(id INT, title STRING)")
conn.exec("INSERT INTO books VALUES (1, 'Databend')")

row = conn.query_row("SELECT title FROM books ORDER BY id LIMIT 1")
print(row.values())  # ('Databend',)

rows = [row.values() for row in conn.query_iter("SELECT * FROM books ORDER BY id")]
```

Supported local targets:

- `connect(":memory:")` — temporary in-memory instance (discarded on close)
- `connect("databend+local:///:memory:")` — explicit in-memory instance
- `connect("databend+local:///./local-state")` — persistent state under `./local-state`
- `connect("databend+local:///./local-state?tenant=default")` — persistent state with an explicit tenant
- `connect("databend+local:///./local-state?database=mydb")` — open a specific database

You can also use `connect_local()` directly for more control:

```python
from databend_driver import connect_local

conn = connect_local(database=":memory:")
conn = connect_local(data_path="./local-state", tenant="default")
```

If the optional `databend` package is not installed, `connect()` raises an
`ImportError` with guidance about enabling the `local` extra and the Python
version requirement.

For remote Databend, the same `connect()` entrypoint accepts standard DSNs:

```python
from databend_driver import connect

conn = connect("databend://root:@localhost:8000/?sslmode=disable")
row = conn.query_row("SELECT 1")
```

#### Relation API

The local connection exposes an embedded-specific relation API for working
with query results as DataFrames or Arrow tables:

```python
relation = conn.sql("SELECT * FROM books")

df = relation.df()       # pandas DataFrame
pl = relation.pl()       # polars DataFrame
tbl = relation.arrow()   # pyarrow Table

rows = relation.fetchall()   # list[tuple]
row = relation.fetchone()    # tuple | None
```

#### Registering External Data

You can register files or in-memory data as virtual tables:

```python
# Register a Parquet file
conn.register("sales", "./data/sales.parquet")
conn.sql("SELECT * FROM sales LIMIT 10").df()

# Register a CSV file
conn.register("events", "./data/events.csv")

# Register a pandas or polars DataFrame
import pandas as pd
df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
conn.register("users", df)

# Shorthand: register a DataFrame and return a relation immediately
relation = conn.from_df(df)

# Read helpers (register and return relation in one call)
relation = conn.read_parquet("./data/sales.parquet")
relation = conn.read_csv("./data/events.csv")
relation = conn.read_json("./data/logs.ndjson")
relation = conn.read_text("./data/raw.txt")
```

### PEP 249 Cursor Object

```python
from databend_driver import BlockingDatabendClient

client = BlockingDatabendClient('databend://root:root@localhost:8000/?sslmode=disable')
cursor = client.cursor()

cursor.execute(
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
cursor.execute("INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?)", (1, 1, 1.0, 'hello', 'world', '2021-01-01', '2021-01-01 00:00:00'))
cursor.execute("SELECT * FROM test")
rows = cursor.fetchall()
for row in rows:
    print(row.values())
cursor.close()
```

### Blocking Connection Object

```python
from databend_driver import BlockingDatabendClient

client = BlockingDatabendClient('databend://root:root@localhost:8000/?sslmode=disable')
conn = client.get_conn()
conn.exec(
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
rows = conn.query_iter("SELECT * FROM test")
for row in rows:
    print(row.values())
conn.close()
```

### Asyncio Connection Object

```python
import asyncio
from databend_driver import AsyncDatabendClient

async def main():
    client = AsyncDatabendClient('databend://root:root@localhost:8000/?sslmode=disable')
    conn = await client.get_conn()
    await conn.exec(
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
    rows = await conn.query_iter("SELECT * FROM test")
    async for row in rows:
        print(row.values())
    await conn.close()

asyncio.run(main())
```

### Parameter bindings

```python
# Positional parameters using ?
row = await context.conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))

# Named parameters using :name
row = await context.conn.query_row(
    "SELECT :a, :b, :c, :d", {"a": 3, "b": False, "c": 4, "d": "55"}
)

# Single value (no tuple needed)
row = await context.conn.query_row("SELECT ?", 3)

# Keyword argument form
row = await context.conn.query_row("SELECT ?, ?, ?, ?", params=(3, False, 4, "55"))
```

Named parameters use token-aware matching, so `:a` will not corrupt `:ab`.
For local embedded connections, passing a mismatched number of `?` placeholders
and values raises a `ValueError` immediately.

### Query ID tracking and query management

```python
# Get the last executed query ID
query_id = conn.last_query_id()
print(f"Last query ID: {query_id}")

# Execute a query and get its ID
await conn.query_row("SELECT 1")
query_id = conn.last_query_id()
print(f"Query ID: {query_id}")

# Kill a running query (if needed)
try:
    await conn.kill_query("some-query-id")
    print("Query killed successfully")
except Exception as e:
    print(f"Failed to kill query: {e}")
```

## Type Mapping

[Databend Types](https://docs.databend.com/sql/sql-reference/data-types/)

### General Data Types

| Databend    | Python               |
| ----------- | -------------------- |
| `BOOLEAN`   | `bool`               |
| `TINYINT`   | `int`                |
| `SMALLINT`  | `int`                |
| `INT`       | `int`                |
| `BIGINT`    | `int`                |
| `FLOAT`     | `float`              |
| `DOUBLE`    | `float`              |
| `DECIMAL`   | `decimal.Decimal`    |
| `DATE`      | `datetime.date`      |
| `TIMESTAMP` | `datetime.datetime`  |
| `INTERVAL`  | `datetime.timedelta` |
| `VARCHAR`   | `str`                |
| `BINARY`    | `bytes`              |

### Semi-Structured Data Types

| Databend    | Python        |
| ----------- | ------------- |
| `ARRAY`     | `list`        |
| `TUPLE`     | `tuple`       |
| `MAP`       | `dict`        |
| `VARIANT`   | `str`         |
| `BITMAP`    | `str`         |
| `GEOMETRY`  | `str / bytes` |
| `GEOGRAPHY` | `str / bytes` |

Note: `VARIANT` is a json encoded string. Example:

```sql
CREATE TABLE example (
    data VARIANT
);
INSERT INTO example VALUES ('{"a": 1, "b": "hello"}');
```

```python
row = await conn.query_row("SELECT * FROM example limit 1;")
data = row.values()[0]
value = json.loads(data)
print(value)
```

`GEOMETRY` and `GEOGRAPHY` follow the current `geometry_output_format` setting. Text formats such as `GeoJSON` or `WKT` return `str`; binary formats such as `WKB` or `EWKB` return `bytes`.

For example:

```python
row = await conn.query_row("settings(geometry_output_format='WKB') SELECT st_point(60, 37)")
assert isinstance(row.values()[0], bytes)
```

## APIs

### Exception Classes (PEP 249 Compliant)

The driver provides a complete set of exception classes that follow the PEP 249 standard for database interfaces:

```python
# Base exceptions
class Warning(Exception): ...
class Error(Exception): ...

# Interface errors
class InterfaceError(Error): ...

# Database errors
class DatabaseError(Error): ...

# Specific database error types
class DataError(DatabaseError): ...
class OperationalError(DatabaseError): ...
class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...
```

These exceptions are automatically mapped from Databend error codes to appropriate PEP 249 exception types based on the nature of the error.

Note: `stream_load` and `load_file` support an optional `method` parameter, it accepts two string values:
- stage: Data is first uploaded to a temporary stage and then loaded. This is the default behavior.
- streaming: Data is directly streamed to the Databend server.

### AsyncDatabendClient

```python
class AsyncDatabendClient:
    def __init__(self, dsn: str): ...
    async def get_conn(self) -> AsyncDatabendConnection: ...
```

### AsyncDatabendConnection

```python
class AsyncDatabendConnection:
    async def info(self) -> ConnectionInfo: ...
    async def version(self) -> str: ...
    async def close(self) -> None: ...
    def last_query_id(self) -> str | None: ...
    async def kill_query(self, query_id: str) -> None: ...
    async def exec(self, sql: str, params: list[string] | tuple[string] | any = None) -> int: ...
    async def query_row(self, sql: str, params: list[string] | tuple[string] | any = None) -> Row: ...
    async def query_iter(self, sql: str, params: list[string] | tuple[string] | any = None) -> RowIterator: ...
    async def stream_load(self, sql: str, data: list[list[str]], method: str = None) -> ServerStats: ...
    async def load_file(self, sql: str, file: str, method: str = None) -> ServerStats: ...
```

### BlockingDatabendClient

```python
class BlockingDatabendClient:
    def __init__(self, dsn: str): ...
    def get_conn(self) -> BlockingDatabendConnection: ...
    def cursor(self) -> BlockingDatabendCursor: ...
```

### BlockingDatabendConnection

```python
class BlockingDatabendConnection:
    def info(self) -> ConnectionInfo: ...
    def version(self) -> str: ...
    def close(self) -> None: ...
    def last_query_id(self) -> str | None: ...
    def kill_query(self, query_id: str) -> None: ...
    def exec(self, sql: str, params: list[string] | tuple[string] | any = None) -> int: ...
    def query_row(self, sql: str, params: list[string] | tuple[string] | any = None) -> Row: ...
    def query_iter(self, sql: str, params: list[string] | tuple[string] | any = None) -> RowIterator: ...
    def stream_load(self, sql: str, data: list[list[str]], method: str = None) -> ServerStats: ...
    def load_file(self, sql: str, file: str, method: str = None, format_option: dict = None, copy_options: dict = None) -> ServerStats: ...
```

### BlockingDatabendCursor

```python
class BlockingDatabendCursor:
    @property
    def description(self) -> list[tuple[str, str, int | None, int | None, int | None, int | None, bool | None]] | None: ...
    @property
    def rowcount(self) -> int: ...
    def close(self) -> None: ...
    def execute(self, operation: str, params: list[string] | tuple[string] = None) -> None | int: ...
    def executemany(self, operation: str, params: list[string] | tuple[string] = None, values: list[list[string] | tuple[string]]) -> None | int: ...
    def fetchone(self) -> Row | None: ...
    def fetchmany(self, size: int = 1) -> list[Row]: ...
    def fetchall(self) -> list[Row]: ...

    # Optional DB API Extensions
    def next(self) -> Row: ...
    def __next__(self) -> Row: ...
    def __iter__(self) -> BlockingDatabendCursor: ...
```

### Row

```python
class Row:
    def values(self) -> tuple: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Row: ...
    def __next__(self) -> any: ...
    def __dict__(self) -> dict: ...
    def __getitem__(self, key: int | str) -> any: ...
```

### RowIterator

```python
class RowIterator:
    def schema(self) -> Schema: ...

    def __iter__(self) -> RowIterator: ...
    def __next__(self) -> Row: ...

    def __aiter__(self) -> RowIterator: ...
    async def __anext__(self) -> Row: ...
```

### Field

```python
class Field:
    @property
    def name(self) -> str: ...
    @property
    def data_type(self) -> str: ...
```

### Schema

```python
class Schema:
    def fields(self) -> list[Field]: ...
```

### ServerStats

```python
class ServerStats:
    @property
    def total_rows(self) -> int: ...
    @property
    def total_bytes(self) -> int: ...
    @property
    def read_rows(self) -> int: ...
    @property
    def read_bytes(self) -> int: ...
    @property
    def write_rows(self) -> int: ...
    @property
    def write_bytes(self) -> int: ...
    @property
    def running_time_ms(self) -> float: ...
```

### ConnectionInfo

```python
class ConnectionInfo:
    @property
    def handler(self) -> str: ...
    @property
    def host(self) -> str: ...
    @property
    def port(self) -> int: ...
    @property
    def user(self) -> str: ...
    @property
    def database(self) -> str | None: ...
    @property
    def warehouse(self) -> str | None: ...
```

### connect_local

```python
def connect_local(
    database: str = ":memory:",
    *,
    data_path: str | None = None,
    tenant: str | None = None,
) -> LocalConnection: ...
```

### LocalConnection

```python
class LocalConnection:
    def sql(self, query: str) -> LocalRelation: ...
    def table(self, name: str) -> LocalRelation: ...
    def format_sql(self, sql: str, params: Any = None) -> str: ...
    def execute(self, query: str, params: Any = None) -> None: ...
    def exec(self, sql: str, params: Any = None) -> None: ...
    def query_row(self, sql: str, params: Any = None) -> LocalRow | None: ...
    def query_all(self, sql: str, params: Any = None) -> list[LocalRow]: ...
    def query_iter(self, sql: str, params: Any = None) -> LocalRowIterator: ...
    def close(self) -> None: ...
    def last_query_id(self) -> None: ...  # always None for local mode
    def kill_query(self, query_id: str) -> None: ...  # raises NotImplementedError
    def register(
        self,
        name: str,
        source: Any,           # path str/Path, pandas/polars DataFrame, or pyarrow Table
        *,
        format: str | None = None,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> LocalConnection: ...
    def from_df(self, source: Any, *, name: str | None = None) -> LocalRelation: ...
    def read_parquet(
        self, path: str | Path, *, pattern: str | None = None,
        connection: str | None = None, name: str | None = None,
    ) -> LocalRelation: ...
    def read_csv(
        self, path: str | Path, *, pattern: str | None = None,
        connection: str | None = None, name: str | None = None,
    ) -> LocalRelation: ...
    def read_json(
        self, path: str | Path, *, pattern: str | None = None,
        connection: str | None = None, name: str | None = None,
    ) -> LocalRelation: ...
    def read_text(
        self, path: str | Path, *, pattern: str | None = None,
        connection: str | None = None, name: str | None = None,
    ) -> LocalRelation: ...
```

### LocalRelation

```python
class LocalRelation:
    def df(self) -> Any: ...          # pandas DataFrame
    def pl(self) -> Any: ...          # polars DataFrame
    def arrow(self) -> Any: ...       # pyarrow Table
    def fetchall(self) -> list[tuple]: ...
    def fetchone(self) -> tuple | None: ...
```

### LocalRow

```python
class LocalRow:
    def values(self) -> tuple[Any, ...]: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> LocalRow: ...
    def __next__(self) -> Any: ...
    def __getitem__(self, key: int) -> Any: ...
```

### LocalRowIterator

```python
class LocalRowIterator:
    def schema(self) -> Any: ...  # not yet implemented for local mode
    def close(self) -> None: ...
    def __iter__(self) -> LocalRowIterator: ...
    def __next__(self) -> LocalRow: ...
```

## Development

```
cd tests
make up
```

```shell
cd bindings/python
uv python install 3.12
uv venv --python 3.12
uv sync --extra local
source .venv/bin/activate
maturin develop

behave tests/asyncio
behave tests/blocking
behave tests/cursor
behave tests/local
```
