# databend-driver

Databend Node.js Client

[![image](https://img.shields.io/npm/v/databend-driver.svg)](https://www.npmjs.com/package/databend-driver)
![License](https://img.shields.io/npm/l/databend-driver.svg)
[![image](https://img.shields.io/npm/types/databend-driver.svg)](https://www.npmjs.com/package/databend-driver)

## Usage

```javascript
const { Client } = require("databend-driver");

const client = new Client(
  "databend://root:root@localhost:8000/?sslmode=disable",
);
const conn = await client.getConn();

await conn.exec(`CREATE TABLE test (
	i64 Int64,
	u64 UInt64,
	f64 Float64,
	s   String,
	s2  String,
	d   Date,
	t   DateTime
);`);

// get rows of value array
const rows = await conn.queryIter("SELECT * FROM test");
let row = await rows.next();
while (row) {
  console.log(row.values());
  row = await rows.next();
}

// get rows of map
const rows = await conn.queryIter("SELECT * FROM test");
let row = await rows.next();
while (row) {
  console.log(row.data());
  row = await rows.next();
}

// iter rows
const rows = await conn.queryIter("SELECT * FROM test");
for await (const row of rows) {
  console.log(row.values());
}

// pipe rows
import { Transform } from "node:stream";
import { finished, pipeline } from "node:stream/promises";

const rows = await conn.queryIter("SELECT * FROM test");
const stream = rows.stream();
const transformer = new Transform({
  readableObjectMode: true,
  writableObjectMode: true,
  transform(row, _, callback) {
    console.log(row.data());
  },
});
await pipeline(stream, transformer);
await finished(stream);

await conn.close();
```

### Query ID tracking

```javascript
await conn.queryRow("SELECT 1");
console.log(conn.lastQueryId()); // Synchronous accessor; no await needed.
```

For remote REST connections, `conn.lastQueryId()` exposes the connection's latest
published query ID, not just the last successful query. **After a SQL error, it now
returns the failed query's ID instead of retaining the previous query's ID.**

| Situation                                                                                                                | `lastQueryId()`                                                      |
| ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| No query has been submitted                                                                                              | `null`                                                               |
| Submission has started, before the first response                                                                        | The driver-generated ID, published before sending the request        |
| The first query response is decoded, whether successful or a SQL error                                                   | The server's response ID, even if a gateway changed the generated ID |
| Failure proven to be before sending, such as local validation, request construction, or connect-only failures            | The previous ID, or `null` if there was none                         |
| HTTP/body/decode errors, timeouts, or interruptions after a request may have been sent, without a decoded query response | The generated ID is retained; execution state is unknown             |
| Pagination or result processing fails after the first response was decoded                                               | The response ID is retained                                          |

Retries reuse the generated ID. A later connect failure does not roll it back if
an earlier attempt may have reached the server. Having an ID does not prove that
the server accepted, completed, or cancelled the query.

This is connection-wide state: concurrent submissions and first responses can
replace it, so do not use it to correlate individual concurrent queries. A rollback
neither overwrites a newer ID nor restores another submission known to be unsent.
FlightSQL connections always return `null`.

## Type Mapping

[Databend Types](https://docs.databend.com/sql/sql-reference/data-types/)

### General Data Types

| Databend       | Node.js   |
| -------------- | --------- |
| `BOOLEAN`      | `Boolean` |
| `TINYINT`      | `Number`  |
| `SMALLINT`     | `Number`  |
| `INT`          | `Number`  |
| `BIGINT`       | `BigInt`  |
| `FLOAT`        | `Number`  |
| `DOUBLE`       | `Number`  |
| `DECIMAL`      | `String`  |
| `DATE`         | `Date`    |
| `TIMESTAMP`    | `Date`    |
| `TIMESTAMP_TZ` | `Date`    |
| `VARCHAR`      | `String`  |
| `BINARY`       | `Buffer`  |

### Semi-Structured Data Types

| Databend    | Node.js           |
| ----------- | ----------------- |
| `ARRAY`     | `Array`           |
| `TUPLE`     | `Array`           |
| `MAP`       | `Object`          |
| `VARIANT`   | `String / Object` |
| `BITMAP`    | `String`          |
| `GEOMETRY`  | `String / Buffer` |
| `GEOGRAPHY` | `String / Buffer` |

Note: `VARIANT` is a json encoded string. Example:

```sql
CREATE TABLE example (
    data VARIANT
);
INSERT INTO example VALUES ('{"a": 1, "b": "hello"}');
```

```javascript
const row = await conn.queryRow("SELECT * FROM example limit 1;");
const data = row.values()[0];
const value = JSON.parse(data);
console.log(value);
```

`GEOMETRY` and `GEOGRAPHY` follow the current `geometry_output_format` setting. Text formats such as `GeoJSON` or `WKT` return `String`; binary formats such as `WKB` or `EWKB` return `Buffer`.

For example:

```javascript
const row = await conn.queryRow(
  "settings(geometry_output_format='WKB') SELECT st_point(60, 37)",
);
console.log(Buffer.isBuffer(row.values()[0]));
```

We also provide a helper function to convert `VARIANT` to `Object`:

```javascript
const row = await conn.queryRow("SELECT * FROM example limit 1;");
row.setOpts({ variantAsObject: true });
console.log(row.data());
```

Parameter binding

```javascript
const row = await this.conn.queryRow(
  "SELECT $1, $2, $3, $4",
  (params = [3, false, 4, "55"]),
);
const row = await this.conn.queryRow(
  "SELECT :a, :b, :c, :d",
  (params = { a: 3, b: false, c: 4, d: "55" }),
);
const row = await this.conn.queryRow("SELECT ?, ?, ?, ?", [3, false, 4, "55"]);
```

## Development

```shell
cd bindings/nodejs
pnpm install
pnpm run build:debug
pnpm run test
```
