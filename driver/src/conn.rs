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

use async_trait::async_trait;
use dyn_clone::DynClone;
use tokio::io::AsyncRead;
use url::Url;

#[cfg(feature = "flight-sql")]
use crate::flight_sql::FlightSQLConnection;

use databend_client::presign::PresignedResponse;
use databend_sql::error::{Error, Result};
use databend_sql::rows::{QueryProgress, Row, RowIterator, RowProgressIterator};
use databend_sql::schema::Schema;

use crate::rest_api::RestAPIConnection;

pub struct Client {
    dsn: String,
}

impl<'c> Client {
    pub fn new(dsn: String) -> Self {
        Self { dsn }
    }

    pub async fn get_conn(&self) -> Result<Box<dyn Connection>> {
        let u = Url::parse(&self.dsn)?;
        match u.scheme() {
            "databend" | "databend+http" | "databend+https" => {
                let conn = RestAPIConnection::try_create(&self.dsn).await?;
                Ok(Box::new(conn))
            }
            #[cfg(feature = "flight-sql")]
            "databend+flight" | "databend+grpc" => {
                let conn = FlightSQLConnection::try_create(&self.dsn).await?;
                Ok(Box::new(conn))
            }
            _ => Err(Error::Parsing(format!(
                "Unsupported scheme: {}",
                u.scheme()
            ))),
        }
    }
}

pub struct ConnectionInfo {
    pub handler: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: Option<String>,
    pub warehouse: Option<String>,
}

pub type Reader = Box<dyn AsyncRead + Send + Sync + Unpin + 'static>;

#[async_trait]
pub trait Connection: DynClone + Send + Sync {
    async fn info(&self) -> ConnectionInfo;

    async fn version(&self) -> Result<String> {
        let row = self.query_row("SELECT version()").await?;
        let version = match row {
            Some(row) => {
                let (version,): (String,) = row.try_into().map_err(Error::Parsing)?;
                version
            }
            None => "".to_string(),
        };
        Ok(version)
    }

    async fn exec(&self, sql: &str) -> Result<i64>;
    async fn query_row(&self, sql: &str) -> Result<Option<Row>>;
    async fn query_iter(&self, sql: &str) -> Result<RowIterator>;
    async fn query_iter_ext(&self, sql: &str) -> Result<(Schema, RowProgressIterator)>;

    async fn upload_to_stage(&self, stage_location: &str, data: Reader, size: u64) -> Result<()>;

    async fn get_presigned_url(&self, stage_location: &str) -> Result<PresignedResponse> {
        let sql = format!("PRESIGN {}", stage_location);
        let row = self.query_row(&sql).await?.ok_or(Error::InvalidResponse(
            "Empty response from server for presigned request".to_string(),
        ))?;
        let (_, headers, url): (String, String, String) = row.try_into().map_err(Error::Parsing)?;
        let headers: BTreeMap<String, String> = serde_json::from_str(&headers)?;
        Ok(PresignedResponse { headers, url })
    }

    async fn stream_load(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<QueryProgress> {
        Err(Error::Protocol(
            "STREAM LOAD only available in HTTP API".to_owned(),
        ))
    }

    async fn put_files(&self, local_file: &str, stage_path: &str) -> Result<(Schema, RowIterator)> {
        Err(Error::Protocol(
            "PUT statement only available in HTTP API".to_owned(),
        ))
    }

    async fn get_files(&self, stage_path: &str, local_file: &str) -> Result<(Schema, RowIterator)> {
        Err(Error::Protocol(
            "GET statement only available in HTTP API".to_owned(),
        ))
    }
}
dyn_clone::clone_trait_object!(Connection);
