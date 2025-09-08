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

use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;
use url::Url;

use crate::conn::IConnection;
#[cfg(feature = "flight-sql")]
use crate::flight_sql::FlightSQLConnection;
use crate::ConnectionInfo;
use crate::Params;

use databend_client::PresignedResponse;
use databend_driver_core::error::{Error, Result};
use databend_driver_core::raw_rows::{RawRow, RawRowIterator};
use databend_driver_core::rows::{Row, RowIterator, RowStatsIterator, ServerStats};
use databend_driver_core::value::Value;

use crate::rest_api::RestAPIConnection;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LoadMethod {
    Stage,
    Streaming,
}

impl FromStr for LoadMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "stage" => Ok(LoadMethod::Stage),
            "streaming" => Ok(LoadMethod::Streaming),
            _ => Err(Error::BadArgument(format!("invalid load method: {s}"))),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    dsn: String,
    name: String,
}

use crate::conn::Reader;

pub struct Connection {
    inner: Box<dyn IConnection>,
}

impl Client {
    pub fn new(dsn: String) -> Self {
        let name = format!("databend-driver-rust/{}", VERSION.as_str());
        Self { dsn, name }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub async fn get_conn(&self) -> Result<Connection> {
        let u = Url::parse(&self.dsn)?;
        match u.scheme() {
            "databend" | "databend+http" | "databend+https" => {
                let conn = RestAPIConnection::try_create(&self.dsn, self.name.clone()).await?;
                Ok(Connection {
                    inner: Box::new(conn),
                })
            }
            #[cfg(feature = "flight-sql")]
            "databend+flight" | "databend+grpc" => {
                let conn = FlightSQLConnection::try_create(&self.dsn, self.name.clone()).await?;
                Ok(Connection {
                    inner: Box::new(conn),
                })
            }
            _ => Err(Error::Parsing(format!(
                "Unsupported scheme: {}",
                u.scheme()
            ))),
        }
    }
}

impl Connection {
    pub fn inner(&self) -> &dyn IConnection {
        self.inner.as_ref()
    }

    pub async fn info(&self) -> ConnectionInfo {
        self.inner.info().await
    }
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    pub fn last_query_id(&self) -> Option<String> {
        self.inner.last_query_id()
    }

    pub async fn version(&self) -> Result<String> {
        self.inner.version().await
    }

    pub fn format_sql<P: Into<Params> + Send>(&self, sql: &str, params: P) -> String {
        let params = params.into();
        params.replace(sql)
    }

    pub async fn kill_query(&self, query_id: &str) -> Result<()> {
        self.inner.kill_query(query_id).await
    }

    pub fn query(&self, sql: &str) -> QueryBuilder<'_> {
        QueryBuilder::new(self, sql)
    }

    pub fn exec(&self, sql: &str) -> ExecBuilder<'_> {
        ExecBuilder::new(self, sql)
    }

    pub async fn query_iter(&self, sql: &str) -> Result<RowIterator> {
        QueryBuilder::new(self, sql).iter().await
    }

    pub async fn query_iter_ext(&self, sql: &str) -> Result<RowStatsIterator> {
        QueryBuilder::new(self, sql).iter_ext().await
    }

    pub async fn query_row(&self, sql: &str) -> Result<Option<Row>> {
        QueryBuilder::new(self, sql).one().await
    }

    pub async fn query_all(&self, sql: &str) -> Result<Vec<Row>> {
        QueryBuilder::new(self, sql).all().await
    }

    // raw data response query, only for test
    pub async fn query_raw_iter(&self, sql: &str) -> Result<RawRowIterator> {
        self.inner.query_raw_iter(sql).await
    }

    // raw data response query, only for test
    pub async fn query_raw_all(&self, sql: &str) -> Result<Vec<RawRow>> {
        self.inner.query_raw_all(sql).await
    }

    /// Get presigned url for a given operation and stage location.
    /// The operation can be "UPLOAD" or "DOWNLOAD".
    pub async fn get_presigned_url(
        &self,
        operation: &str,
        stage: &str,
    ) -> Result<PresignedResponse> {
        self.inner.get_presigned_url(operation, stage).await
    }

    pub async fn upload_to_stage(&self, stage: &str, data: Reader, size: u64) -> Result<()> {
        self.inner.upload_to_stage(stage, data, size).await
    }

    pub async fn load_data(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        method: LoadMethod,
    ) -> Result<ServerStats> {
        self.inner.load_data(sql, data, size, method).await
    }

    pub async fn load_file(&self, sql: &str, fp: &Path, method: LoadMethod) -> Result<ServerStats> {
        self.inner.load_file(sql, fp, method).await
    }

    pub async fn load_file_with_options(
        &self,
        sql: &str,
        fp: &Path,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        self.inner
            .load_file_with_options(sql, fp, file_format_options, copy_options)
            .await
    }

    pub async fn stream_load(
        &self,
        sql: &str,
        data: Vec<Vec<&str>>,
        method: LoadMethod,
    ) -> Result<ServerStats> {
        self.inner.stream_load(sql, data, method).await
    }

    // PUT file://<path_to_file>/<filename> internalStage|externalStage
    pub async fn put_files(&self, local_file: &str, stage: &str) -> Result<RowStatsIterator> {
        self.inner.put_files(local_file, stage).await
    }

    pub async fn get_files(&self, stage: &str, local_file: &str) -> Result<RowStatsIterator> {
        self.inner.get_files(stage, local_file).await
    }

    // ORM Methods
    pub fn query_as<T>(&self, sql: &str) -> ORMQueryBuilder<'_, T>
    where
        T: TryFrom<Row> + RowORM,
        T::Error: std::fmt::Display,
    {
        ORMQueryBuilder::new(self, sql)
    }

    pub async fn insert<T>(&self, table_name: &str) -> Result<InsertCursor<'_, T>>
    where
        T: Clone + RowORM,
    {
        Ok(InsertCursor::new(self, table_name.to_string()))
    }
}

pub struct QueryCursor<T> {
    iter: RowIterator,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> QueryCursor<T>
where
    T: TryFrom<Row>,
    T::Error: std::fmt::Display,
{
    fn new(iter: RowIterator) -> Self {
        Self {
            iter,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn fetch(&mut self) -> Result<Option<T>> {
        use tokio_stream::StreamExt;
        match self.iter.next().await {
            Some(row) => {
                let row = row?;
                let typed_row = T::try_from(row).map_err(|e| Error::Parsing(e.to_string()))?;
                Ok(Some(typed_row))
            }
            None => Ok(None),
        }
    }

    pub async fn next(&mut self) -> Result<Option<T>> {
        self.fetch().await
    }

    pub async fn fetch_all(self) -> Result<Vec<T>> {
        self.iter.try_collect().await
    }
}

pub struct InsertCursor<'a, T> {
    connection: &'a Connection,
    table_name: String,
    rows: Vec<T>,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> InsertCursor<'a, T>
where
    T: Clone + RowORM,
{
    fn new(connection: &'a Connection, table_name: String) -> Self {
        Self {
            connection,
            table_name,
            rows: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn write(&mut self, row: &T) -> Result<()> {
        self.rows.push(row.clone());
        Ok(())
    }

    pub async fn end(self) -> Result<i64> {
        if self.rows.is_empty() {
            return Ok(0);
        }
        let connection = self.connection;
        // Generate field names and values for INSERT (exclude skip_serializing)
        let field_names = T::insert_field_names();
        let field_list = field_names.join(", ");
        let placeholder_list = (0..field_names.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table_name, field_list, placeholder_list
        );

        let mut total_inserted = 0;
        for row in &self.rows {
            let values = row.to_values();
            let param_strings: Vec<String> =
                values.into_iter().map(|v| v.to_sql_string()).collect();
            let params = Params::QuestionParams(param_strings);
            let inserted = connection.exec(&sql).bind(params).await?;
            total_inserted += inserted;
        }

        Ok(total_inserted)
    }
}

// Helper function to replace ?fields placeholder for queries
fn replace_query_fields_placeholder(sql: &str, field_names: &[&str]) -> String {
    let fields = field_names.join(", ");
    sql.replace("?fields", &fields)
}

// Helper function to replace ?fields placeholder for inserts
#[allow(dead_code)]
fn replace_insert_fields_placeholder(sql: &str, field_names: &[&str]) -> String {
    let fields = field_names.join(", ");
    sql.replace("?fields", &fields)
}

// ORM Query Builder
pub struct ORMQueryBuilder<'a, T> {
    connection: &'a Connection,
    sql: String,
    params: Option<Params>,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> ORMQueryBuilder<'a, T>
where
    T: TryFrom<Row> + RowORM,
    T::Error: std::fmt::Display,
{
    fn new(connection: &'a Connection, sql: &str) -> Self {
        Self {
            connection,
            sql: sql.to_string(),
            params: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn bind<P: Into<Params> + Send>(mut self, params: P) -> Self {
        self.params = Some(params.into());
        self
    }

    pub async fn execute(self) -> Result<QueryCursor<T>> {
        let sql_with_fields = replace_query_fields_placeholder(&self.sql, &T::query_field_names());
        let final_sql = if let Some(params) = self.params {
            params.replace(&sql_with_fields)
        } else {
            sql_with_fields
        };
        let row_iter = self.connection.inner.query_iter(&final_sql).await?;
        Ok(QueryCursor::new(row_iter))
    }
}

impl<'a, T> std::future::IntoFuture for ORMQueryBuilder<'a, T>
where
    T: TryFrom<Row> + RowORM + Send + 'a,
    T::Error: std::fmt::Display,
{
    type Output = Result<QueryCursor<T>>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

// Builder pattern for query operations
pub struct QueryBuilder<'a> {
    connection: &'a Connection,
    sql: String,
    params: Option<Params>,
}

impl<'a> QueryBuilder<'a> {
    fn new(connection: &'a Connection, sql: &str) -> Self {
        Self {
            connection,
            sql: sql.to_string(),
            params: None,
        }
    }

    pub fn bind<P: Into<Params> + Send>(mut self, params: P) -> Self {
        self.params = Some(params.into());
        self
    }

    pub async fn iter(self) -> Result<RowIterator> {
        let sql = self.get_final_sql();
        self.connection.inner.query_iter(&sql).await
    }

    pub async fn iter_ext(self) -> Result<RowStatsIterator> {
        let sql = self.get_final_sql();
        self.connection.inner.query_iter_ext(&sql).await
    }

    pub async fn one(self) -> Result<Option<Row>> {
        let sql = self.get_final_sql();
        self.connection.inner.query_row(&sql).await
    }

    pub async fn all(self) -> Result<Vec<Row>> {
        let sql = self.get_final_sql();
        self.connection.inner.query_all(&sql).await
    }

    pub async fn cursor_as<T>(self) -> Result<QueryCursor<T>>
    where
        T: TryFrom<Row> + RowORM,
        T::Error: std::fmt::Display,
    {
        let sql_with_fields = replace_query_fields_placeholder(&self.sql, &T::query_field_names());
        let final_sql = if let Some(params) = self.params {
            params.replace(&sql_with_fields)
        } else {
            sql_with_fields
        };
        let row_iter = self.connection.inner.query_iter(&final_sql).await?;
        Ok(QueryCursor::new(row_iter))
    }

    fn get_final_sql(&self) -> String {
        match &self.params {
            Some(params) => params.replace(&self.sql),
            None => self.sql.clone(),
        }
    }
}

// Builder pattern for execution operations
pub struct ExecBuilder<'a> {
    connection: &'a Connection,
    sql: String,
    params: Option<Params>,
}

impl<'a> ExecBuilder<'a> {
    fn new(connection: &'a Connection, sql: &str) -> Self {
        Self {
            connection,
            sql: sql.to_string(),
            params: None,
        }
    }

    pub fn bind<P: Into<Params> + Send>(mut self, params: P) -> Self {
        self.params = Some(params.into());
        self
    }

    pub async fn execute(self) -> Result<i64> {
        let sql = match self.params {
            Some(params) => params.replace(&self.sql),
            None => self.sql,
        };
        self.connection.inner.exec(&sql).await
    }
}

impl<'a> std::future::IntoFuture for ExecBuilder<'a> {
    type Output = Result<i64>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

// Add trait bounds for ORM functionality
pub trait RowORM: TryFrom<Row> + Clone {
    fn field_names() -> Vec<&'static str>; // For backward compatibility
    fn query_field_names() -> Vec<&'static str>; // For SELECT queries (exclude skip_deserializing)
    fn insert_field_names() -> Vec<&'static str>; // For INSERT statements (exclude skip_serializing)
    fn to_values(&self) -> Vec<Value>;
}
