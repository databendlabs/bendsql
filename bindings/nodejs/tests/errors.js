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

const assert = require("node:assert/strict");
const { once } = require("node:events");
const { createServer } = require("node:http");
const { Writable } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const { Then } = require("@cucumber/cucumber");
const { Client } = require("../index.js");

const progress = { rows: 0, bytes: 0 };
const permissionDenied = { code: 1063, message: "Permission denied" };
const queryId = "mock-query-id";

function queryResponse(fields) {
  return {
    id: queryId,
    schema: [{ name: "col", type: "String" }],
    data: [],
    state: "Succeeded",
    stats: {
      scan_progress: progress,
      write_progress: progress,
      result_progress: progress,
      running_time_ms: 0,
    },
    ...fields,
  };
}

async function withServer(respond, run) {
  const server = createServer((req, res) => {
    req.resume();
    const response = respond(req);
    if (!response) {
      req.socket.destroy();
      return;
    }
    const [status, body] = response;
    res.writeHead(status, { "Content-Type": "application/json", Connection: "close" });
    res.end(JSON.stringify(body));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = server.address().port;
  try {
    await run(`databend://root:@127.0.0.1:${port}/default?sslmode=disable&presign=off&retry_delay_secs=0`);
  } finally {
    await new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
}

function checkErrorDetails(message, fields) {
  return (error) => {
    assert(error instanceof Error);
    assert.equal(error.name, "Error");
    assert.equal(error.code, "GenericFailure");
    assert.match(error.message, message);
    assert.equal(String(error), `Error: ${error.message}`);
    for (const [key, value] of Object.entries(fields)) {
      if (value === undefined) {
        assert.equal(Object.hasOwn(error, key), false, key);
      } else {
        assert.equal(error[key], value, key);
      }
    }
    return true;
  };
}

Then("connection, SQL, and transport failures expose structured error properties", async function () {
  await withServer(
    () => [400, { error: { code: 1003, message: "Unknown database" } }],
    async (dsn) => {
      await assert.rejects(
        new Client(dsn).getConn(),
        checkErrorDetails(/^APIError: \[request_kind=login\]: BadRequest:\(400 Bad Request\)\[1003\]Unknown database/, {
          errorCode: 1003,
          httpStatus: 400,
          requestKind: "login",
        }),
      );
    },
  );

  await withServer(
    () => [200, queryResponse({ state: "Failed", error: permissionDenied })],
    async (dsn) => {
      const conn = await new Client(`${dsn}&login=disable`).getConn();
      try {
        await assert.rejects(
          conn.exec("select 1"),
          checkErrorDetails(/^APIError: QueryFailed: \[1063\]Permission denied/, {
            errorCode: 1063,
            httpStatus: 200,
          }),
        );
      } finally {
        await conn.close();
      }
    },
  );

  await withServer(
    () => undefined,
    async (dsn) => {
      await assert.rejects(
        new Client(`${dsn}&retry_count=1`).getConn(),
        checkErrorDetails(/^APIError: \[request_kind=login\]: reqwest::Error:/, {
          errorCode: undefined,
          httpStatus: undefined,
          requestKind: "login",
          retryTimes: 0,
        }),
      );
    },
  );

  let submittedQueryId;
  await withServer(
    (req) => {
      submittedQueryId = req.headers["x-databend-query-id"];
      return undefined;
    },
    async (dsn) => {
      const conn = await new Client(`${dsn}&login=disable&retry_count=2`).getConn();
      try {
        await assert.rejects(conn.exec("select 1"), (error) => {
          assert(submittedQueryId);
          return checkErrorDetails(/^APIError: \[request_kind=query\/start query_id=/, {
            errorCode: undefined,
            httpStatus: undefined,
            requestKind: "query/start",
            queryId: submittedQueryId,
            retryTimes: 1,
          })(error);
        });
      } finally {
        await conn.close();
      }
    },
  );
});

Then("both row iterators and the result stream report page failures as errors", async function () {
  await withServer(
    () => [200, queryResponse({ data: [["one"]] })],
    async (dsn) => {
      const conn = await new Client(`${dsn}&login=disable`).getConn();
      try {
        const rows = await conn.queryIter("select 1");
        assert.deepEqual((await rows.next()).values(), ["one"]);
        assert.equal(await rows.next(), null);

        const rowsWithStats = await conn.queryIterExt("select 1");
        assert((await rowsWithStats.next()).stats);
        assert.deepEqual((await rowsWithStats.next()).row.values(), ["one"]);
        assert.equal(await rowsWithStats.next(), null);
      } finally {
        await conn.close();
      }
    },
  );

  await withServer(
    (req) => {
      if (req.url === "/v1/query" && req.method === "POST") {
        return [200, queryResponse({ state: "Running", data: [["one"], ["two"]], next_uri: "/page/2" })];
      }
      if (req.url === "/page/2") {
        return [200, queryResponse({ state: "Failed", error: permissionDenied })];
      }
      return [404, {}];
    },
    async (dsn) => {
      const conn = await new Client(`${dsn}&login=disable`).getConn();
      const checkFailure = checkErrorDetails(/^APIError: QueryFailed: \[1063\]Permission denied/, {
        errorCode: 1063,
        httpStatus: 200,
      });
      try {
        const rows = await conn.queryIter("select 1");
        assert.deepEqual((await rows.next()).values(), ["one"]);
        await assert.rejects(rows.next(), checkFailure);

        const rowsWithStats = await conn.queryIterExt("select 1");
        assert((await rowsWithStats.next()).stats);
        assert.deepEqual((await rowsWithStats.next()).row.values(), ["one"]);
        await assert.rejects(rowsWithStats.next(), checkFailure);

        const iterated = [];
        const iterable = await conn.queryIter("select 1");
        await assert.rejects(async () => {
          for await (const row of iterable) {
            iterated.push(row.values());
          }
        }, checkFailure);
        assert.deepEqual(iterated, [["one"]]);

        const stream = (await conn.queryIter("select 1")).stream();
        const streamed = [];
        const sink = new Writable({
          objectMode: true,
          write(row, _encoding, callback) {
            streamed.push(row.values());
            callback();
          },
        });
        await assert.rejects(pipeline(stream, sink), checkFailure);
        assert.deepEqual(streamed, [["one"]]);
        assert.equal(stream.destroyed, true);
      } finally {
        await conn.close();
      }
    },
  );
});
