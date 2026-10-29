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

// Worker for bindings/tests/cloud_dsn.py; loads the locally built native addon.
// Kept outside cucumber's tests/*.js glob and the old-driver compatibility suite.
const fs = require("node:fs");
const { Client } = require("../../index.js");

function requireCondition(condition) {
  if (!condition) throw new Error("Cloud DSN assertion failed");
}

async function main() {
  const test = JSON.parse(fs.readFileSync(0, "utf8"));
  try {
    const client = new Client(test.dsn);
    for (let connection = 0; connection < 2; connection++) {
      const conn = await client.getConn();
      try {
        for (let query = 0; query < 2; query++) {
          const rows = await conn.queryAll("SELECT 1");
          const values = rows.map((row) => row.values()[0]);
          requireCondition(JSON.stringify(values) === JSON.stringify(test.expected));
        }
      } finally {
        await conn.close();
      }
    }
  } catch (error) {
    requireCondition(test.error && String(error).includes(test.error));
    const tokens = new URL(test.dsn).searchParams.getAll("cloud_api_token");
    requireCondition(tokens.every((token) => !String(error).includes(token)));
    console.log("PASS");
    return;
  }
  requireCondition(!test.error);
  console.log("PASS");
}

main().catch(() => {
  console.error("FAIL: Node Cloud DSN assertion (diagnostics withheld)");
  process.exitCode = 1;
});
