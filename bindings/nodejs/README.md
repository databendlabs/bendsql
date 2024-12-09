# databend-driver

## Build

```shell
cd bindings/nodejs
pnpm install
pnpm run build
```

## Usage

```javascript
const { Client } = require("databend-driver");

const client = new Client(
  "databend+http://root:root@localhost:8000/?sslmode=disable",
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
```

## Type Mapping

[Databend Types](https://docs.databend.com/sql/sql-reference/data-types/)

### General Data Types

| Databend    | Node.js   |
| ----------- | --------- |
| `BOOLEAN`   | `Boolean` |
| `TINYINT`   | `Number`  |
| `SMALLINT`  | `Number`  |
| `INT`       | `Number`  |
| `BIGINT`    | `BigInt`  |
| `FLOAT`     | `Number`  |
| `DOUBLE`    | `Number`  |
| `DECIMAL`   | `String`  |
| `DATE`      | `Date`    |
| `TIMESTAMP` | `Date`    |
| `VARCHAR`   | `String`  |
| `BINARY`    | `Buffer`  |

### Semi-Structured Data Types

| Databend    | Node.js           |
| ----------- | ----------------- |
| `ARRAY`     | `Array`           |
| `TUPLE`     | `Array`           |
| `MAP`       | `Object`          |
| `VARIANT`   | `String / Object` |
| `BITMAP`    | `String`          |
| `GEOMETRY`  | `String`          |
| `GEOGRAPHY` | `String`          |

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

We also provide a helper function to convert `VARIANT` to `Object`:

```javascript
const row = await conn.queryRow("SELECT * FROM example limit 1;");
row.setOpts({ variantAsObject: true });
console.log(row.data());
```

## Development

```shell
cd bindings/nodejs
pnpm install
pnpm run build:debug
pnpm run test
```
