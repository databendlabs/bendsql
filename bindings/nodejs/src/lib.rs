// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_use]
extern crate napi_derive;

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use napi::{bindgen_prelude::*, Env};
use once_cell::sync::Lazy;
use tokio_stream::StreamExt;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

#[napi]
#[derive(Clone, Debug, Default)]
pub struct ValueOptions {
    pub variant_as_object: bool,
}

#[napi]
impl FromNapiValue for ValueOptions {
    unsafe fn from_napi_value(env: sys::napi_env, val: sys::napi_value) -> Result<Self> {
        let mut opts = ValueOptions::default();
        let obj = Object::from_napi_value(env, val)?;
        if let Some(val) = obj.get("variantAsObject")? {
            opts.variant_as_object = val;
        }
        Ok(opts)
    }
}

#[napi]
pub struct Client {
    inner: databend_driver::Client,
    opts: ValueOptions,
}

#[napi]
impl Client {
    /// Create a new databend client with a given DSN.
    #[napi(constructor)]
    pub fn new(dsn: String, opts: Option<ValueOptions>) -> Self {
        let name = format!("databend-driver-nodejs/{}", VERSION.as_str());
        let client = databend_driver::Client::new(dsn).with_name(name);
        Self {
            inner: client,
            opts: opts.unwrap_or_default(),
        }
    }

    /// Get a connection from the client.
    #[napi]
    pub async fn get_conn(&self) -> Result<Connection> {
        let conn = self.inner.get_conn().await.map_err(format_napi_error)?;
        Ok(Connection::new(conn, self.opts.clone()))
    }
}

#[napi]
pub struct Connection {
    inner: databend_driver::Connection,
    opts: ValueOptions,
}

pub type Params = serde_json::Value;

impl Connection {
    pub fn new(inner: databend_driver::Connection, opts: ValueOptions) -> Self {
        Self { inner, opts }
    }
}

#[napi]
impl Connection {
    /// Get the connection information.
    #[napi]
    pub async fn info(&self) -> ConnectionInfo {
        ConnectionInfo(self.inner.info().await)
    }

    /// Get the databend version.
    #[napi]
    pub async fn version(&self) -> Result<String> {
        self.inner.version().await.map_err(format_napi_error)
    }

    #[napi]
    pub fn format_sql(&self, sql: String, params: Option<Params>) -> Result<String> {
        Ok(self.inner.format_sql(&sql, params))
    }

    /// Execute a SQL query, return the number of affected rows.
    #[napi]
    pub async fn exec(&self, sql: String, params: Option<Params>) -> Result<i64> {
        self.inner
            .exec(&sql, params)
            .await
            .map_err(format_napi_error)
    }

    /// Execute a SQL query, and only return the first row.
    #[napi]
    pub async fn query_row(&self, sql: String, params: Option<Params>) -> Result<Option<Row>> {
        let ret = self
            .inner
            .query_row(&sql, params)
            .await
            .map_err(format_napi_error)?;
        let row = ret.map(|r| Row::new(r, self.opts.clone()));
        Ok(row)
    }

    /// Execute a SQL query and fetch all data into the result
    #[napi]
    pub async fn query_all(&self, sql: String, params: Option<Params>) -> Result<Vec<Row>> {
        Ok(self
            .inner
            .query_all(&sql, params)
            .await
            .map_err(format_napi_error)?
            .into_iter()
            .map(|r| Row::new(r, self.opts.clone()))
            .collect())
    }

    /// Execute a SQL query, and return all rows.
    #[napi]
    pub async fn query_iter(&self, sql: String, params: Option<Params>) -> Result<RowIterator> {
        let iterator = self
            .inner
            .query_iter(&sql, params)
            .await
            .map_err(format_napi_error)?;
        Ok(RowIterator::new(iterator, self.opts.clone()))
    }

    /// Execute a SQL query, and return all rows with schema and stats.
    #[napi]
    pub async fn query_iter_ext(
        &self,
        sql: String,
        params: Option<Params>,
    ) -> Result<RowIteratorExt> {
        let iterator = self
            .inner
            .query_iter_ext(&sql, params)
            .await
            .map_err(format_napi_error)?;
        Ok(RowIteratorExt::new(iterator, self.opts.clone()))
    }

    /// Load data with stage attachment.
    /// The SQL can be `INSERT INTO tbl VALUES` or `REPLACE INTO tbl VALUES`.
    #[napi]
    pub async fn stream_load(&self, sql: String, data: Vec<Vec<&str>>) -> Result<ServerStats> {
        let ss = self
            .inner
            .stream_load(&sql, data)
            .await
            .map_err(format_napi_error)?;
        Ok(ServerStats(ss))
    }

    /// Load file with stage attachment.
    /// The SQL can be `INSERT INTO tbl VALUES` or `REPLACE INTO tbl VALUES`.
    #[napi]
    pub async fn load_file(
        &self,
        sql: String,
        file: String,
        format_options: Option<BTreeMap<String, String>>,
        copy_options: Option<BTreeMap<String, String>>,
    ) -> Result<ServerStats> {
        let format_options = match format_options {
            None => None,
            Some(ref opts) => Some(opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()),
        };
        let copy_options = match copy_options {
            None => None,
            Some(ref opts) => Some(opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()),
        };
        let ss = self
            .inner
            .load_file(&sql, Path::new(&file), format_options, copy_options)
            .await
            .map_err(format_napi_error)?;
        Ok(ServerStats(ss))
    }
}

#[napi]
pub struct ConnectionInfo(databend_driver::ConnectionInfo);

#[napi]
impl ConnectionInfo {
    #[napi(getter)]
    pub fn handler(&self) -> String {
        self.0.handler.to_string()
    }

    #[napi(getter)]
    pub fn host(&self) -> String {
        self.0.host.to_string()
    }

    #[napi(getter)]
    pub fn port(&self) -> u16 {
        self.0.port
    }

    #[napi(getter)]
    pub fn user(&self) -> String {
        self.0.user.to_string()
    }

    #[napi(getter)]
    pub fn database(&self) -> Option<String> {
        self.0.database.clone()
    }

    #[napi(getter)]
    pub fn warehouse(&self) -> Option<String> {
        self.0.warehouse.clone()
    }
}

pub struct Value<'v> {
    inner: &'v databend_driver::Value,
    opts: &'v ValueOptions,
}

impl<'v> Value<'v> {
    pub fn new(inner: &'v databend_driver::Value, opts: &'v ValueOptions) -> Self {
        Self { inner, opts }
    }
}

impl<'v> ToNapiValue for Value<'v> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let ctx = Env::from(env);
        match val.inner {
            databend_driver::Value::Null => Null::to_napi_value(env, Null),
            databend_driver::Value::EmptyArray => {
                let arr = ctx.create_array(0)?;
                Array::to_napi_value(env, arr)
            }
            databend_driver::Value::EmptyMap => {
                let obj = ctx.create_object()?;
                Object::to_napi_value(env, obj)
            }
            databend_driver::Value::Boolean(b) => bool::to_napi_value(env, *b),
            databend_driver::Value::Binary(b) => {
                Buffer::to_napi_value(env, Buffer::from(b.as_slice()))
            }
            databend_driver::Value::String(s) => String::to_napi_value(env, s.to_string()),
            databend_driver::Value::Number(n) => {
                NumberValue::to_napi_value(env, NumberValue(n.clone()))
            }
            databend_driver::Value::Timestamp(_) => {
                let inner = val.inner.clone();
                let v = NaiveDateTime::try_from(inner).map_err(format_napi_error)?;
                NaiveDateTime::to_napi_value(env, v)
            }
            databend_driver::Value::Date(_) => {
                let inner = val.inner.clone();
                let v = NaiveDate::try_from(inner).map_err(format_napi_error)?;
                NaiveDateTime::to_napi_value(
                    env,
                    NaiveDateTime::new(v, NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
                )
            }
            databend_driver::Value::Array(inner) => {
                let mut arr = ctx.create_array(inner.len() as u32)?;
                for (i, v) in inner.into_iter().enumerate() {
                    arr.set(i as u32, Value::new(v, val.opts))?;
                }
                Array::to_napi_value(env, arr)
            }
            databend_driver::Value::Map(inner) => {
                let mut obj = ctx.create_object()?;
                for (k, v) in inner.into_iter() {
                    obj.set(k.to_string(), Value::new(v, val.opts))?;
                }
                Object::to_napi_value(env, obj)
            }
            databend_driver::Value::Tuple(inner) => {
                let mut arr = ctx.create_array(inner.len() as u32)?;
                for (i, v) in inner.into_iter().enumerate() {
                    arr.set(i as u32, Value::new(v, val.opts))?;
                }
                Array::to_napi_value(env, arr)
            }
            databend_driver::Value::Bitmap(s) => String::to_napi_value(env, s.to_string()),
            databend_driver::Value::Variant(s) => {
                if val.opts.variant_as_object {
                    let val: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| Error::from_reason(format!("parse variant error: {}", e)))?;
                    serde_json::Value::to_napi_value(env, val)
                } else {
                    String::to_napi_value(env, s.to_string())
                }
            }
            databend_driver::Value::Geometry(s) => String::to_napi_value(env, s.to_string()),
            databend_driver::Value::Interval(s) => String::to_napi_value(env, s.to_string()),
            databend_driver::Value::Geography(s) => String::to_napi_value(env, s.to_string()),
        }
    }
}

pub struct NumberValue(databend_driver::NumberValue);

impl ToNapiValue for NumberValue {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        match val.0 {
            databend_driver::NumberValue::Int8(i) => i8::to_napi_value(env, i),
            databend_driver::NumberValue::Int16(i) => i16::to_napi_value(env, i),
            databend_driver::NumberValue::Int32(i) => i32::to_napi_value(env, i),
            databend_driver::NumberValue::Int64(i) => i64::to_napi_value(env, i),
            databend_driver::NumberValue::UInt8(i) => u8::to_napi_value(env, i),
            databend_driver::NumberValue::UInt16(i) => u16::to_napi_value(env, i),
            databend_driver::NumberValue::UInt32(i) => u32::to_napi_value(env, i),
            databend_driver::NumberValue::UInt64(i) => u64::to_napi_value(env, i),
            databend_driver::NumberValue::Float32(i) => f32::to_napi_value(env, i),
            databend_driver::NumberValue::Float64(i) => f64::to_napi_value(env, i),
            databend_driver::NumberValue::Decimal128(_, _) => {
                String::to_napi_value(env, val.0.to_string())
            }
            databend_driver::NumberValue::Decimal256(_, _) => {
                String::to_napi_value(env, val.0.to_string())
            }
        }
    }
}

#[napi]
pub struct Schema(databend_driver::SchemaRef);

#[napi]
impl Schema {
    #[napi]
    pub fn fields(&self) -> Vec<Field> {
        self.0.fields().iter().map(|f| Field(f.clone())).collect()
    }
}

#[napi]
pub struct Field(databend_driver::Field);

#[napi]
impl Field {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.0.name.to_string()
    }

    #[napi(getter)]
    pub fn data_type(&self) -> String {
        self.0.data_type.to_string()
    }
}

#[napi]
pub struct RowIterator {
    inner: databend_driver::RowIterator,
    opts: ValueOptions,
}

impl RowIterator {
    pub fn new(inner: databend_driver::RowIterator, opts: ValueOptions) -> Self {
        Self { inner, opts }
    }
}

#[napi]
impl RowIterator {
    /// Get Schema for rows.
    #[napi]
    pub fn schema(&self) -> Schema {
        Schema(self.inner.schema().clone())
    }

    /// Fetch next row.
    /// Returns `None` if there are no more rows.
    #[napi]
    #[allow(clippy::missing_safety_doc)]
    pub async unsafe fn next(&mut self) -> Option<Result<Row>> {
        self.inner.next().await.map(|row| {
            row.map(|r| Row::new(r, self.opts.clone()))
                .map_err(format_napi_error)
        })
    }

    /// Return a Readable Stream for the query result.
    /// Should be used with `ObjectMode` set to `true`.
    #[napi(ts_return_type = "import('stream').Readable")]
    pub fn stream(&self) -> () {
        unreachable!()
    }
}

#[napi]
pub struct RowIteratorExt {
    inner: databend_driver::RowStatsIterator,
    opts: ValueOptions,
}

impl RowIteratorExt {
    pub fn new(inner: databend_driver::RowStatsIterator, opts: ValueOptions) -> Self {
        Self { inner, opts }
    }
}

#[napi]
impl RowIteratorExt {
    #[napi]
    pub fn schema(&self) -> Schema {
        Schema(self.inner.schema().clone())
    }

    /// Fetch next row or stats.
    /// Returns `None` if there are no more rows.
    #[napi]
    #[allow(clippy::missing_safety_doc)]
    pub async unsafe fn next(&mut self) -> Option<Result<RowOrStats>> {
        match self.inner.next().await {
            None => None,
            Some(r0) => match r0 {
                Ok(r1) => match r1 {
                    databend_driver::RowWithStats::Row(r) => Some(Ok(RowOrStats {
                        row: Some(Row::new(r, self.opts.clone())),
                        stats: None,
                    })),
                    databend_driver::RowWithStats::Stats(ss) => Some(Ok(RowOrStats {
                        row: None,
                        stats: Some(ServerStats(ss)),
                    })),
                },
                Err(e) => Some(Err(format_napi_error(e))),
            },
        }
    }
}

/// Must contain either row or stats.
#[napi]
pub struct RowOrStats {
    row: Option<Row>,
    stats: Option<ServerStats>,
}

#[napi]
impl RowOrStats {
    #[napi(getter)]
    pub fn row(&self) -> Option<Row> {
        self.row.clone()
    }

    #[napi(getter)]
    pub fn stats(&self) -> Option<ServerStats> {
        self.stats.clone()
    }
}

#[napi]
#[derive(Clone)]
pub struct Row {
    inner: databend_driver::Row,
    opts: ValueOptions,
}

impl Row {
    pub fn new(inner: databend_driver::Row, opts: ValueOptions) -> Self {
        Self { inner, opts }
    }
}

#[napi]
impl Row {
    #[napi]
    pub fn set_opts(&mut self, opts: ValueOptions) {
        self.opts = opts;
    }

    #[napi]
    pub fn values(&self) -> Vec<Value> {
        self.inner
            .values()
            .iter()
            .map(|v| Value::new(v, &self.opts))
            .collect()
    }

    #[napi]
    pub fn data(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        let schema = self.inner.schema();
        for (name, value) in schema
            .fields()
            .iter()
            .map(|f| f.name.to_string())
            .zip(self.inner.values().iter())
        {
            map.insert(name.clone(), Value::new(value, &self.opts));
        }
        map
    }
}

#[napi]
#[derive(Clone)]
pub struct ServerStats(databend_driver::ServerStats);

#[napi]
impl ServerStats {
    #[napi(getter)]
    pub fn total_rows(&self) -> usize {
        self.0.total_rows
    }

    #[napi(getter)]
    pub fn total_bytes(&self) -> usize {
        self.0.total_bytes
    }

    #[napi(getter)]
    pub fn read_rows(&self) -> usize {
        self.0.read_rows
    }

    #[napi(getter)]
    pub fn read_bytes(&self) -> usize {
        self.0.read_bytes
    }

    #[napi(getter)]
    pub fn write_rows(&self) -> usize {
        self.0.write_rows
    }

    #[napi(getter)]
    pub fn write_bytes(&self) -> usize {
        self.0.write_bytes
    }

    #[napi(getter)]
    pub fn spill_file_nums(&self) -> usize {
        self.0.spill_file_nums
    }

    #[napi(getter)]
    pub fn running_time_ms(&self) -> f64 {
        self.0.running_time_ms
    }
}

fn format_napi_error(err: databend_driver::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
