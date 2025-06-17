# databend-driver

Databend Python Client

[![image](https://img.shields.io/pypi/v/databend-driver.svg)](https://pypi.org/project/databend-driver)
![License](https://img.shields.io/pypi/l/databend-driver.svg)
[![image](https://img.shields.io/pypi/pyversions/databend-driver.svg)](https://pypi.org/project/databend-driver)

## Usage

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

asyncio.run(main())
```

### Parameter bindings

```python
# Test with positional parameters
row = await context.conn.query_row("SELECT ?, ?, ?, ?", (3, False, 4, "55"))
row = await context.conn.query_row(
    "SELECT :a, :b, :c, :d", {"a": 3, "b": False, "c": 4, "d": "55"}
)
row = await context.conn.query_row(
    "SELECT ?", 3
)
row = await context.conn.query_row("SELECT ?, ?, ?, ?", params = (3, False, 4, "55"))
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

| Databend    | Python  |
| ----------- | ------- |
| `ARRAY`     | `list`  |
| `TUPLE`     | `tuple` |
| `MAP`       | `dict`  |
| `VARIANT`   | `str`   |
| `BITMAP`    | `str`   |
| `GEOMETRY`  | `str`   |
| `GEOGRAPHY` | `str`   |

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

## APIs

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
    async def exec(self, sql: str, params: list[string] | tuple[string] | any = None) -> int: ...
    async def query_row(self, sql: str, params: list[string] | tuple[string] | any = None) -> Row: ...
    async def query_iter(self, sql: str, params: list[string] | tuple[string] | any = None) -> RowIterator: ...
    async def stream_load(self, sql: str, data: list[list[str]]) -> ServerStats: ...
    async def load_file(self, sql: str, file: str, format_option: dict, copy_options: dict = None) -> ServerStats: ...
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
    def exec(self, sql: str, params: list[string] | tuple[string] | any = None) -> int: ...
    def query_row(self, sql: str, params: list[string] | tuple[string] | any = None) -> Row: ...
    def query_iter(self, sql: str, params: list[string] | tuple[string] | any = None) -> RowIterator: ...
    def stream_load(self, sql: str, data: list[list[str]]) -> ServerStats: ...
    def load_file(self, sql: str, file: str, format_option: dict, copy_options: dict = None) -> ServerStats: ...
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

## Development

```
cd tests
make up
```

```shell
cd bindings/python
uv sync
source .venv/bin/activate
maturin develop --uv

behave tests/asyncio
behave tests/blocking
behave tests/cursor
```
