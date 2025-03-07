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

use std::collections::BTreeMap;
use std::path::Path;

use once_cell::sync::Lazy;
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

use crate::rest_api::RestAPIConnection;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

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

    pub async fn exec<P: Into<Params> + Send>(&self, sql: &str, params: P) -> Result<i64> {
        let params = params.into();
        self.inner.exec(&params.replace(sql)).await
    }
    pub async fn query_iter<P: Into<Params> + Send>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<RowIterator> {
        let params = params.into();
        self.inner.query_iter(&params.replace(sql)).await
    }

    pub async fn query_iter_ext<P: Into<Params> + Send>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<RowStatsIterator> {
        let params = params.into();
        self.inner.query_iter_ext(&params.replace(sql)).await
    }

    pub async fn query_row<P: Into<Params> + Send>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Option<Row>> {
        let params = params.into();
        self.inner.query_row(&params.replace(sql)).await
    }

    pub async fn query_all<P: Into<Params> + Send>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<Row>> {
        let params = params.into();
        self.inner.query_all(&params.replace(sql)).await
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
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        self.inner
            .load_data(sql, data, size, file_format_options, copy_options)
            .await
    }

    pub async fn load_file(
        &self,
        sql: &str,
        fp: &Path,
        format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        self.inner
            .load_file(sql, fp, format_options, copy_options)
            .await
    }

    pub async fn stream_load(&self, sql: &str, data: Vec<Vec<&str>>) -> Result<ServerStats> {
        self.inner.stream_load(sql, data).await
    }

    // PUT file://<path_to_file>/<filename> internalStage|externalStage
    pub async fn put_files(&self, local_file: &str, stage: &str) -> Result<RowStatsIterator> {
        self.inner.put_files(local_file, stage).await
    }

    pub async fn get_files(&self, stage: &str, local_file: &str) -> Result<RowStatsIterator> {
        self.inner.get_files(stage, local_file).await
    }
}
