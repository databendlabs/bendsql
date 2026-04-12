from __future__ import annotations

import uuid
from importlib import import_module
from pathlib import Path
from tempfile import mkdtemp
from typing import Any
from urllib.parse import parse_qs, urlparse


def _load_embedded_module():
    try:
        import databend as embedded
    except ImportError as exc:
        version_hint = ""
        if _python_version_tuple() < (3, 12):
            version_hint = (
                f" Current interpreter is Python {_python_version_str()}, but the "
                "embedded dependency currently requires Python 3.12+."
            )
        raise ImportError(
            "Local embedded mode requires the optional `databend` package. "
            "Install databend-driver with the `local` extra or provide the "
            "internal databend binding in the environment."
            + version_hint
        ) from exc
    return embedded


def _normalize_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())


def _random_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class LocalRelation:
    def __init__(self, relation: Any):
        self._relation = relation

    def __repr__(self) -> str:
        return repr(self._relation)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._relation, name)

    def df(self):
        return self._relation.to_pandas()

    def pl(self):
        return self._relation.to_polars()

    def arrow(self):
        return self._relation.to_arrow_table()

    def fetchall(self) -> list[tuple[Any, ...]]:
        table = self.arrow()
        columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
        return [
            tuple(column[row_index] for column in columns)
            for row_index in range(table.num_rows)
        ]

    def fetchone(self) -> tuple[Any, ...] | None:
        rows = self.fetchall()
        return rows[0] if rows else None


class LocalRow:
    def __init__(self, values: tuple[Any, ...]):
        self._values = values
        self._idx = 0

    def values(self) -> tuple[Any, ...]:
        return self._values

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> LocalRow:
        return self

    def __next__(self) -> Any:
        if self._idx >= len(self._values):
            raise StopIteration("Columns exhausted")
        value = self._values[self._idx]
        self._idx += 1
        return value

    def __getitem__(self, key: int) -> Any:
        if not isinstance(key, int):
            raise TypeError("key must be an integer")
        return self._values[key]

    def __repr__(self) -> str:
        return repr(self._values)


class LocalRowIterator:
    def __init__(self, rows: list[LocalRow]):
        self._rows = rows
        self._idx = 0

    def schema(self):
        raise NotImplementedError("schema() is not available for local embedded queries yet.")

    def close(self) -> None:
        self._idx = len(self._rows)

    def __iter__(self) -> LocalRowIterator:
        return self

    def __next__(self) -> LocalRow:
        if self._idx >= len(self._rows):
            raise StopIteration("Rows exhausted")
        row = self._rows[self._idx]
        self._idx += 1
        return row


class LocalConnection:
    def __init__(self, impl: Any):
        self._impl = impl

    def __repr__(self) -> str:
        return repr(self._impl)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._impl, name)

    def sql(self, query: str) -> LocalRelation:
        return LocalRelation(self._impl.sql(query))

    def format_sql(self, sql: str, params: Any = None) -> str:
        if params is None:
            return sql

        if isinstance(params, dict):
            rendered = sql
            for key, value in params.items():
                rendered = rendered.replace(f":{key}", _sql_literal(value))
            return rendered

        if not isinstance(params, (list, tuple)):
            params = [params]

        rendered = sql
        for value in params:
            rendered = rendered.replace("?", _sql_literal(value), 1)
        return rendered

    def execute(self, query: str, params: Any = None) -> None:
        statement = self.format_sql(query, params)
        self._impl.sql(statement).collect()

    def exec(self, sql: str, params: Any = None) -> None:
        self.execute(sql, params)

    def query_row(self, sql: str, params: Any = None) -> LocalRow | None:
        statement = self.format_sql(sql, params)
        row = self.sql(statement).fetchone()
        if row is None:
            return None
        return LocalRow(tuple(row))

    def query_all(self, sql: str, params: Any = None) -> list[LocalRow]:
        statement = self.format_sql(sql, params)
        return [LocalRow(tuple(row)) for row in self.sql(statement).fetchall()]

    def query_iter(self, sql: str, params: Any = None) -> LocalRowIterator:
        return LocalRowIterator(self.query_all(sql, params))

    def close(self) -> None:
        if hasattr(self._impl, "close"):
            self._impl.close()

    def last_query_id(self) -> None:
        return None

    def kill_query(self, query_id: str) -> None:
        raise NotImplementedError(
            "kill_query() is not supported for local embedded mode."
        )

    def table(self, name: str) -> LocalRelation:
        return self.sql(f"SELECT * FROM {name}")

    def register(
        self,
        name: str,
        source: Any,
        *,
        format: str | None = None,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> LocalConnection:
        if isinstance(source, (str, Path)):
            source_path = str(source)
            source_format = (format or Path(source_path).suffix.lstrip(".")).lower()
            if source_format in {"parquet", "pq"}:
                self._impl.register_parquet(
                    name, source_path, pattern=pattern, connection=connection
                )
            elif source_format in {"csv"}:
                self._impl.register_csv(
                    name, source_path, pattern=pattern, connection=connection
                )
            elif source_format in {"json", "ndjson"}:
                self._impl.register_ndjson(
                    name, source_path, pattern=pattern, connection=connection
                )
            elif source_format in {"txt", "text", "tsv"}:
                self._impl.register_text(
                    name, source_path, pattern=pattern, connection=connection
                )
            else:
                raise ValueError(
                    f"Unsupported format for {source_path!r}. "
                    "Use format= explicitly or pass pandas/polars/pyarrow data."
                )
            return self

        parquet_path = self._materialize_relation_source(name, source)
        self._impl.register_parquet(name, parquet_path, pattern=pattern, connection=connection)
        return self

    def from_df(self, source: Any, *, name: str | None = None) -> LocalRelation:
        target = name or _random_name("df")
        self.register(target, source)
        return self.table(target)

    def read_parquet(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> LocalRelation:
        target = name or _random_name("parquet")
        self._impl.register_parquet(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def read_csv(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> LocalRelation:
        target = name or _random_name("csv")
        self._impl.register_csv(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def read_json(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> LocalRelation:
        target = name or _random_name("json")
        self._impl.register_ndjson(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def read_text(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> LocalRelation:
        target = name or _random_name("text")
        self._impl.register_text(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def _materialize_relation_source(self, name: str, source: Any) -> str:
        table = self._to_arrow_table(source)
        temp_dir = self._data_path() / "python" / "registered"
        temp_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = temp_dir / f"{name}_{uuid.uuid4().hex}.parquet"

        import pyarrow.parquet as pq

        pq.write_table(table, parquet_path)
        return _normalize_path(parquet_path)

    @staticmethod
    def _to_arrow_table(source: Any):
        if hasattr(source, "schema") and hasattr(source, "to_pydict"):
            return source

        if hasattr(source, "to_arrow"):
            return source.to_arrow()

        if hasattr(source, "to_pandas"):
            source = source.to_pandas()

        try:
            import pyarrow as pa

            return pa.Table.from_pandas(source, preserve_index=False)
        except Exception as exc:
            raise TypeError(
                "Unsupported source type. Expected path, pandas.DataFrame, "
                "polars.DataFrame, or pyarrow.Table."
            ) from exc

    def _data_path(self) -> Path:
        value = getattr(self._impl, "_data_path", None)
        if value is None:
            return Path(".databend").resolve()
        return Path(value).expanduser().resolve()


def connect_local(
    database: str = ":memory:",
    *,
    data_path: str | None = None,
    tenant: str | None = None,
) -> LocalConnection:
    embedded = _load_embedded_module()
    memory_target = database == ":memory:"
    explicit_data_path = None if memory_target and data_path == ":memory:" else data_path

    if tenant is None and hasattr(embedded, "connect"):
        if explicit_data_path is not None:
            return LocalConnection(
                embedded.connect(database=database, data_path=explicit_data_path)
            )
        if memory_target:
            conn = LocalConnection(embedded.connect(data_path=mkdtemp(prefix="databend-embedded-")))
            conn._ephemeral = True
            return conn
        return LocalConnection(embedded.connect(data_path=database))

    target_path = explicit_data_path or (".databend" if memory_target else database)
    return LocalConnection(embedded.SessionContext(tenant, data_path=target_path))

def connect(target: str = ":memory:", **kwargs: Any):
    if _is_local_target(target):
        database, data_path, tenant = _parse_local_target(
            target,
            kwargs.get("data_path"),
            kwargs.get("tenant"),
        )
        return connect_local(database=database, data_path=data_path, tenant=tenant)

    package = import_module("databend_driver")
    client = package.BlockingDatabendClient(target)
    return client.get_conn()


def _is_local_target(target: str) -> bool:
    return target == ":memory:" or target.startswith("databend+local://")


def _parse_local_target(
    target: str, explicit_data_path: str | None, explicit_tenant: str | None
) -> tuple[str, str | None, str | None]:
    if target == ":memory:":
        return ":memory:", explicit_data_path, explicit_tenant

    parsed = urlparse(target)
    database = ":memory:"
    query = parse_qs(parsed.query)
    tenant = explicit_tenant

    if explicit_data_path is not None:
        data_path = explicit_data_path
    elif "data_path" in query and query["data_path"]:
        data_path = query["data_path"][0]
    else:
        raw_path = parsed.path or ""
        if raw_path == "/:memory:":
            raw_path = ":memory:"
        data_path = raw_path if raw_path not in {"", "/"} else None

    if "database" in query and query["database"]:
        database = query["database"][0]
    elif data_path is not None:
        database = data_path

    if tenant is None and "tenant" in query and query["tenant"]:
        tenant = query["tenant"][0]

    return database, data_path, tenant


def _python_version_tuple() -> tuple[int, int]:
    import sys

    return sys.version_info[:2]


def _python_version_str() -> str:
    major, minor = _python_version_tuple()
    return f"{major}.{minor}"


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    raise TypeError(
        f"Invalid parameter type for {value!r}, expected str, bool, int, float or None"
    )


__all__ = [
    "LocalConnection",
    "LocalRelation",
    "LocalRow",
    "LocalRowIterator",
    "connect",
    "connect_local",
]
