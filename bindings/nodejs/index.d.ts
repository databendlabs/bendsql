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

/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export class ValueOptions {
  variantAsObject: boolean
}
export class Client {
  /** Create a new databend client with a given DSN. */
  constructor(dsn: string, opts?: ValueOptions | undefined | null)
  /** Get a connection from the client. */
  getConn(): Promise<Connection>
}
export class Connection {
  /** Get the connection information. */
  info(): Promise<ConnectionInfo>
  /** Get the databend version. */
  version(): Promise<string>
  /** Execute a SQL query, return the number of affected rows. */
  exec(sql: string): Promise<number>
  /** Execute a SQL query, and only return the first row. */
  queryRow(sql: string): Promise<Row | null>
  /** Execute a SQL query and fetch all data into the result */
  queryAll(sql: string): Promise<Array<Row>>
  /** Execute a SQL query, and return all rows. */
  queryIter(sql: string): Promise<RowIterator>
  /** Execute a SQL query, and return all rows with schema and stats. */
  queryIterExt(sql: string): Promise<RowIteratorExt>
  /**
   * Load data with stage attachment.
   * The SQL can be `INSERT INTO tbl VALUES` or `REPLACE INTO tbl VALUES`.
   */
  streamLoad(sql: string, data: Array<Array<string>>): Promise<ServerStats>
}
export class ConnectionInfo {
  get handler(): string
  get host(): string
  get port(): number
  get user(): string
  get database(): string | null
  get warehouse(): string | null
}
export class Schema {
  fields(): Array<Field>
}
export class Field {
  get name(): string
  get dataType(): string
}
export class RowIterator {
  /** Get Schema for rows. */
  schema(): Schema
  /**
   * Fetch next row.
   * Returns `None` if there are no more rows.
   */
  next(): Promise<Error | Row | null>
  read(): Promise<Error | Row | null>
}
export class RowIteratorExt {
  /**
   * Fetch next row or stats.
   * Returns `None` if there are no more rows.
   */
  next(): Promise<Error | RowOrStats | null>
  schema(): Schema
}
/** Must contain either row or stats. */
export class RowOrStats {
  get row(): Row | null
  get stats(): ServerStats | null
}
export class Row {
  setOpts(opts: ValueOptions): void
  values(): Array<any>
  data(): Record<string, any>
}
export class ServerStats {
  get totalRows(): bigint
  get totalBytes(): bigint
  get readRows(): bigint
  get readBytes(): bigint
  get writeRows(): bigint
  get writeBytes(): bigint
  get runningTimeMs(): number
}
