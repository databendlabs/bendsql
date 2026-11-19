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

const fs = require("fs");

let files = ["generated.js", "index.d.ts"];

for (path of files) {
  let data = fs.readFileSync(path, "utf8");
  fs.writeFileSync(
    path,
    `/*
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

` + data,
  );
}

// index.js yields every result from next() except the null EOF sentinel.
const declarationsPath = "index.d.ts";
const declarations = fs.readFileSync(declarationsPath, "utf8");
if (!declarations.includes("export interface RowIterator {")) {
  const asyncIteratorDeclaration = `type RowIteratorItem = Exclude<Awaited<ReturnType<RowIterator['next']>>, null>

export interface RowIterator {
  [Symbol.asyncIterator](): AsyncGenerator<RowIteratorItem, void, unknown>
}

`;
  fs.writeFileSync(declarationsPath, declarations.replace(/^export /m, `${asyncIteratorDeclaration}$&`));
}
