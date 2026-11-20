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

import type { Readable } from "node:stream";

// index.js exports Client at runtime; the other names are available only as types.
export { Client } from "./native";
export type {
  Connection,
  ConnectionInfo,
  Field,
  Row,
  RowIterator,
  RowIteratorExt,
  RowOrStats,
  Schema,
  ServerStats,
  ValueOptions,
} from "./native";

// Augment the native type so queryIter() sees the methods installed by index.js.
declare module "./native" {
  interface RowIterator {
    [Symbol.asyncIterator](): AsyncGenerator<Exclude<Awaited<ReturnType<RowIterator["next"]>>, null>, void, unknown>;
    /** Return an object-mode readable stream for the query result. */
    stream(): Readable;
  }
}
