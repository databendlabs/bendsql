/*
 * Copyright 2021 Datafuse Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { Transform } = require("node:stream");
const { finished, pipeline } = require("node:stream/promises");

const assert = require("assert");
const { Given, When, Then } = require("@cucumber/cucumber");

const { Client } = require("../index.js");

const dsn = process.env.TEST_DATABEND_DSN
  ? process.env.TEST_DATABEND_DSN
  : "databend://root:@localhost:8000/default?sslmode=disable";

Given("A new Databend Driver Client", async function () {
  this.client = new Client(dsn);
  const conn = await this.client.getConn();
  if (!conn) {
    assert.fail("No connection returned");
  }
  this.conn = conn;
});

Then("Select string {string} should be equal to {string}", async function (input, output) {
  const row = await this.conn.queryRow(`SELECT '${input}'`);
  const value = row.values()[0];
  assert.equal(output, value);
});

Then("Select params binding", async function () {
  {
    const row = await this.conn.queryRow("SELECT $1, $2, $3, $4", (params = [3, false, 4, "55"]));
    assert.deepEqual(row.values(), [3, false, 4, "55"]);
  }

  {
    const row = await this.conn.queryRow("SELECT :a, :b, :c, :d", (params = { a: 3, b: false, c: 4, d: "55" }));
    assert.deepEqual(row.values(), [3, false, 4, "55"]);
  }

  {
    const row = await this.conn.queryRow("SELECT ?, ?, ?, ?", [3, false, 4, "55"]);
    assert.deepEqual(row.values(), [3, false, 4, "55"]);
  }
});

Then("Select types should be expected native types", async function () {
  // BOOLEAN
  {
    const row = await this.conn.queryRow("SELECT true, false");
    assert.deepEqual(row.values(), [true, false]);
  }

  // TINYINT
  {
    const row = await this.conn.queryRow("SELECT 1::TINYINT, 2::TINYINT");
    assert.deepEqual(row.values(), [1, 2]);
  }

  // SMALLINT
  {
    const row = await this.conn.queryRow("SELECT 1::SMALLINT, 2::SMALLINT");
    assert.deepEqual(row.values(), [1, 2]);
  }

  // INT
  {
    const row = await this.conn.queryRow("SELECT 1::INT, 2::INT");
    assert.deepEqual(row.values(), [1, 2]);
  }

  // BIGINT
  {
    const row = await this.conn.queryRow("SELECT 14294967295::BIGINT, 1::BIGINT");
    assert.deepEqual(row.values(), [14294967295n, 1n]);
  }

  // FLOAT
  {
    const row = await this.conn.queryRow("SELECT ?::FLOAT, ?::FLOAT", (params = [1.11, 2.22]));
    assert.deepEqual(
      row.values().map((v) => v.toFixed(2)),
      [1.11, 2.22],
    );
  }

  // DOUBLE
  {
    const row = await this.conn.queryRow("SELECT 1.11::DOUBLE, 2.22::DOUBLE");
    assert.deepEqual(
      row.values().map((v) => v.toFixed(2)),
      [1.11, 2.22],
    );
  }

  // Decimal
  {
    const row = await this.conn.queryRow(`SELECT 15.7563::Decimal(8,4), 2.0+3.0`);
    assert.deepEqual(row.values(), ["15.7563", "5.0"]);
  }

  // DATE
  {
    const row = await this.conn.queryRow("SELECT to_date('2020-01-01'), to_date('2020-01-02')");
    assert.deepEqual(row.values(), [new Date("2020-01-01"), new Date("2020-01-02")]);
  }

  // TIMESTAMP
  {
    const row = await this.conn.queryRow(
      "SELECT to_datetime('2020-01-01 12:34:56.789'), to_datetime('2020-01-02 12:34:56.789')",
    );
    assert.deepEqual(row.values(), [new Date("2020-01-01T12:34:56.789Z"), new Date("2020-01-02T12:34:56.789Z")]);
  }

  // VARCHAR
  {
    const row = await this.conn.queryRow("SELECT 'xyz', 'abc'");
    assert.deepEqual(row.values(), ["xyz", "abc"]);
  }

  // BINARY
  {
    const row = await this.conn.queryRow("select to_binary('xyz')");
    assert.deepEqual(row.values(), [Buffer.from("xyz")]);
  }

  // ARRAY
  {
    const row = await this.conn.queryRow(`SELECT [10::Decimal(15,2), 1.1+2.3]`);
    assert.deepEqual(row.values(), [["10.00", "3.40"]]);
  }

  // TUPLE
  {
    const row = await this.conn.queryRow(`SELECT (10, '20', to_datetime('2024-04-16 12:34:56.789'))`);
    assert.deepEqual(row.values(), [[10, "20", new Date("2024-04-16T12:34:56.789Z")]]);
  }

  // MAP
  {
    const row = await this.conn.queryRow(`SELECT {'xx':to_date('2020-01-01')}`);
    assert.deepEqual(row.values(), [{ xx: new Date("2020-01-01") }]);
  }

  // Variant as String
  {
    const value =
      '{"customer_id": 123, "order_id": 1001, "items": [{"name": "Shoes", "price": 59.99}, {"name": "T-shirt", "price": 19.99}]}';
    const row = await this.conn.queryRow(`SELECT parse_json('${value}')`);
    assert.deepEqual(
      row.values()[0],
      '{"customer_id":123,"items":[{"name":"Shoes","price":59.99},{"name":"T-shirt","price":19.99}],"order_id":1001}',
    );
  }

  // Variant as Object
  {
    const value =
      '{"customer_id": 123, "order_id": 1001, "items": [{"name": "Shoes", "price": 59.99}, {"name": "T-shirt", "price": 19.99}]}';
    const row = await this.conn.queryRow(`SELECT parse_json('${value}')`);
    row.setOpts({ variantAsObject: true });
    assert.deepEqual(row.values()[0], {
      customer_id: 123,
      order_id: 1001,
      items: [
        { name: "Shoes", price: 59.99 },
        { name: "T-shirt", price: 19.99 },
      ],
    });
  }

  // Variant as param
  {
    const value = [3, "15", { aa: 3 }];
    const row = await this.conn.queryRow(`SELECT ?, ?, ?`, (params = value));
    row.setOpts({ variantAsObject: true });
    assert.deepEqual(row.values(), [
      3,
      "15",
      {
        aa: 3,
      },
    ]);
  }
});

Then("Select numbers should iterate all rows", async function () {
  // iter
  {
    let rows = await this.conn.queryIter("SELECT number FROM numbers(5)");
    let ret = [];
    let row = await rows.next();
    while (row) {
      ret.push(row.values()[0]);
      row = await rows.next();
    }
    const expected = [0, 1, 2, 3, 4];
    assert.deepEqual(ret, expected);
  }

  // iter as async iterator
  {
    let rows = await this.conn.queryIter("SELECT number FROM numbers(5)");
    let ret = [];
    for await (const row of rows) {
      ret.push(row.values()[0]);
    }
    const expected = [0, 1, 2, 3, 4];
    assert.deepEqual(ret, expected);
  }

  // async iter return with field names
  {
    let rows = await this.conn.queryIter("SELECT number as n FROM numbers(5)");
    let ret = [];
    for await (const row of rows) {
      ret.push(row.data());
    }
    const expected = [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }];
    assert.deepEqual(ret, expected);
  }

  // pipeline transform rows as stream
  {
    let rows = await this.conn.queryIter("SELECT number FROM numbers(5)");
    const ret = [];
    const stream = rows.stream();
    const transformer = new Transform({
      readableObjectMode: true,
      writableObjectMode: true,
      transform(row, _, callback) {
        ret.push(row.values()[0]);
        callback();
      },
    });
    await pipeline(stream, transformer);
    await finished(stream);
    const expected = [0, 1, 2, 3, 4];
    assert.deepEqual(ret, expected);
  }
});

When("Create a test table", async function () {
  await this.conn.exec("DROP TABLE IF EXISTS test");
  await this.conn.exec(`CREATE TABLE test (
		i64 Int64,
		u64 UInt64,
		f64 Float64,
		s   String,
		s2  String,
		d   Date,
		t   DateTime
    );`);
});

Then("Insert and Select should be equal", async function () {
  await this.conn.exec(`INSERT INTO test VALUES
    (-1, 1, 1.0, '1', '1', '2011-03-06', '2011-03-06 06:20:00'),
    (-2, 2, 2.0, '2', '2', '2012-05-31', '2012-05-31 11:20:00'),
    (-3, 3, 3.0, '3', '2', '2016-04-04', '2016-04-04 11:30:00')`);
  const rows = await this.conn.queryIter("SELECT * FROM test");
  const ret = [];
  for await (const row of rows) {
    ret.push(row.values());
  }
  const expected = [
    [-1, 1, 1.0, "1", "1", new Date("2011-03-06"), new Date("2011-03-06T06:20:00Z")],
    [-2, 2, 2.0, "2", "2", new Date("2012-05-31"), new Date("2012-05-31T11:20:00Z")],
    [-3, 3, 3.0, "3", "2", new Date("2016-04-04"), new Date("2016-04-04T11:30:00Z")],
  ];
  assert.deepEqual(ret, expected);
});

Then("Stream load and Select should be equal", async function () {
  const values = [
    ["-1", "1", "1.0", "1", "1", "2011-03-06", "2011-03-06T06:20:00Z"],
    ["-2", "2", "2.0", "2", "2", "2012-05-31", "2012-05-31T11:20:00Z"],
    ["-3", "3", "3.0", "3", "2", "2016-04-04", "2016-04-04T11:30:00Z"],
  ];
  const progress = await this.conn.streamLoad(`INSERT INTO test VALUES`, values);
  assert.equal(progress.writeRows, 3);
  assert.equal(progress.writeBytes, 187);

  const rows = await this.conn.queryIter("SELECT * FROM test");
  const ret = [];
  for await (const row of rows) {
    ret.push(row.values());
  }
  const expected = [
    [-1, 1, 1.0, "1", "1", new Date("2011-03-06"), new Date("2011-03-06T06:20:00Z")],
    [-2, 2, 2.0, "2", "2", new Date("2012-05-31"), new Date("2012-05-31T11:20:00Z")],
    [-3, 3, 3.0, "3", "2", new Date("2016-04-04"), new Date("2016-04-04T11:30:00Z")],
  ];
  assert.deepEqual(ret, expected);
});

Then("Load file and Select should be equal", async function () {
  const progress = await this.conn.loadFile(`INSERT INTO test VALUES`, "tests/data/test.csv", { type: "CSV" });
  assert.equal(progress.writeRows, 3);
  assert.equal(progress.writeBytes, 187);

  const rows = await this.conn.queryIter("SELECT * FROM test");
  const ret = [];
  for await (const row of rows) {
    ret.push(row.values());
  }
  const expected = [
    [-1, 1, 1.0, "1", "1", new Date("2011-03-06"), new Date("2011-03-06T06:20:00Z")],
    [-2, 2, 2.0, "2", "2", new Date("2012-05-31"), new Date("2012-05-31T11:20:00Z")],
    [-3, 3, 3.0, "3", "2", new Date("2016-04-04"), new Date("2016-04-04T11:30:00Z")],
  ];
  assert.deepEqual(ret, expected);
});

Then("Temp table should work with cluster", async function () {
  await this.conn.exec(`create or replace temp table temp_1(a int)`);
  await this.conn.exec(`INSERT INTO temp_1 VALUES (1),(2)`);

  const rows = await this.conn.queryIter("SELECT * FROM temp_1");
  const ret = [];
  for await (const row of rows) {
    ret.push(row.values());
  }
  assert.deepEqual(ret, [[1], [2]]);
  await this.conn.exec(`drop table temp_1`);
});
