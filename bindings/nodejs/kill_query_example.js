#!/usr/bin/env node

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

/*
 * Example: Using killQuery() API to cancel running queries
 *
 * This demonstrates the new killQuery() method combined with lastQueryId()
 */

const { Client } = require("./index.js");

async function main() {
  const dsn = process.env.DATABEND_DSN || "databend://root:@localhost:8000/default?sslmode=disable";

  console.log("🔗 Connecting to Databend...");
  const client = new Client(dsn);
  const conn = await client.getConn();

  console.log("\n🔪 Testing Kill Query functionality:");

  // 1. Test killing non-existent query (this should return an error)
  console.log("\n1. Test killing non-existent query:");
  const nonExistentId = "12345678-1234-1234-1234-123456789012";
  try {
    await conn.killQuery(nonExistentId);
    console.log("   ❌ Unexpected: kill succeeded for non-existent query");
  } catch (err) {
    console.log("   ✅ Expected error:", err.message);
  }

  // 3. Demonstrate real-world usage pattern
  console.log("\n3. Real-world usage pattern for long-running queries:");
  console.log("   ```javascript");
  console.log("   // Start a potentially long-running query");
  console.log('   const queryPromise = conn.queryIter("SELECT * FROM huge_table");');
  console.log("   const queryId = conn.lastQueryId();");
  console.log("   ");
  console.log("   // Set up a timeout or user cancellation handler");
  console.log("   const timeoutId = setTimeout(async () => {");
  console.log("     try {");
  console.log("       await conn.killQuery(queryId);");
  console.log('       console.log("Query killed due to timeout");');
  console.log("     } catch (err) {");
  console.log('       console.log("Query already completed");');
  console.log("     }");
  console.log("   }, 30000); // 30 second timeout");
  console.log("   ");
  console.log("   try {");
  console.log("     const rows = await queryPromise;");
  console.log("     clearTimeout(timeoutId);");
  console.log("     // Process results");
  console.log("   } catch (err) {");
  console.log('     if (err.message.includes("killed")) {');
  console.log('       console.log("Query was cancelled");');
  console.log("     } else {");
  console.log("       throw err;");
  console.log("     }");
  console.log("   }");
  console.log("   ```");

  await conn.close();
  console.log("\n✅ Kill query example completed!");
  console.log("\n💡 Usage notes:");
  console.log("   - killQuery() sends POST /v1/query/{queryId}/kill to Databend server");
  console.log("   - Useful for canceling long-running queries");
  console.log("   - Can be combined with lastQueryId() for immediate cancellation");
  console.log("   - Safe to call on completed or non-existent queries");
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main };
