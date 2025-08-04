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

use async_trait::async_trait;
use log::info;
use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_stream::Stream;

use crate::client::LoadMethod;
use crate::conn::{ConnectionInfo, IConnection, Reader};
use databend_client::APIClient;
use databend_client::Pages;
use databend_driver_core::error::{Error, Result};
use databend_driver_core::raw_rows::{RawRow, RawRowIterator, RawRowWithStats};
use databend_driver_core::rows::{Row, RowIterator, RowStatsIterator, RowWithStats, ServerStats};
use databend_driver_core::schema::{Schema, SchemaRef};

const LOAD_PLACEHOLDER: &str = "@_databend_load";

#[derive(Clone)]
pub struct RestAPIConnection {
    client: Arc<APIClient>,
}

impl RestAPIConnection {
    fn gen_temp_stage_location(&self) -> Result<String> {
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| Error::IO("Failed to get current timestamp".to_string()))?;
        Ok(format!("@~/client/load/{now}"))
    }

    async fn load_data_with_stage(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
    ) -> Result<ServerStats> {
        let location = self.gen_temp_stage_location()?;
        self.upload_to_stage(&location, data, size).await?;
        if self.client.capability().streaming_load {
            let sql = sql.replace(LOAD_PLACEHOLDER, &location);
            let page = self.client.query_all(&sql).await?;
            Ok(ServerStats::from(page.stats))
        } else {
            let file_format_options = Self::default_file_format_options();
            let copy_options = Self::default_copy_options();
            let stats = self
                .client
                .insert_with_stage(sql, &location, file_format_options, copy_options)
                .await?;
            Ok(ServerStats::from(stats))
        }
    }

    async fn load_data_with_streaming(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
    ) -> Result<ServerStats> {
        let start = Instant::now();
        let response = self
            .client
            .streaming_load(sql, data, "<no_filename>")
            .await?;
        Ok(ServerStats {
            total_rows: 0,
            total_bytes: 0,
            read_rows: response.stats.rows,
            read_bytes: size as usize,
            write_rows: response.stats.rows,
            write_bytes: response.stats.bytes,
            running_time_ms: start.elapsed().as_millis() as f64,
            spill_file_nums: 0,
            spill_bytes: 0,
        })
    }
    async fn load_data_with_options(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        let location = self.gen_temp_stage_location()?;
        let file_format_options =
            file_format_options.unwrap_or_else(Self::default_file_format_options);
        let copy_options = copy_options.unwrap_or_else(Self::default_copy_options);
        self.upload_to_stage(&location, Box::new(data), size)
            .await?;
        let stats = self
            .client
            .insert_with_stage(sql, &location, file_format_options, copy_options)
            .await?;
        Ok(ServerStats::from(stats))
    }
}

#[async_trait]
impl IConnection for RestAPIConnection {
    async fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            handler: "RestAPI".to_string(),
            host: self.client.host().to_string(),
            port: self.client.port(),
            user: self.client.username(),
            catalog: self.client.current_catalog(),
            database: self.client.current_database(),
            warehouse: self.client.current_warehouse(),
        }
    }

    fn last_query_id(&self) -> Option<String> {
        self.client.last_query_id()
    }

    async fn close(&self) -> Result<()> {
        self.client.close().await;
        Ok(())
    }

    async fn exec(&self, sql: &str) -> Result<i64> {
        info!("exec: {}", sql);
        let page = self.client.query_all(sql).await?;
        Ok(page.stats.progresses.write_progress.rows as i64)
    }

    async fn kill_query(&self, query_id: &str) -> Result<()> {
        Ok(self.client.kill_query(query_id).await?)
    }

    async fn query_iter(&self, sql: &str) -> Result<RowIterator> {
        info!("query iter: {}", sql);
        let rows_with_progress = self.query_iter_ext(sql).await?;
        let rows = rows_with_progress.filter_rows().await;
        Ok(rows)
    }

    async fn query_iter_ext(&self, sql: &str) -> Result<RowStatsIterator> {
        info!("query iter ext: {}", sql);
        let pages = self.client.start_query(sql, true).await?;
        let (schema, rows) = RestAPIRows::<RowWithStats>::from_pages(pages).await?;
        Ok(RowStatsIterator::new(Arc::new(schema), Box::pin(rows)))
    }

    // raw data response query, only for test
    async fn query_raw_iter(&self, sql: &str) -> Result<RawRowIterator> {
        info!("query raw iter: {}", sql);
        let pages = self.client.start_query(sql, true).await?;
        let (schema, rows) = RestAPIRows::<RawRowWithStats>::from_pages(pages).await?;
        Ok(RawRowIterator::new(Arc::new(schema), Box::pin(rows)))
    }

    async fn upload_to_stage(&self, stage: &str, data: Reader, size: u64) -> Result<()> {
        self.client.upload_to_stage(stage, data, size).await?;
        Ok(())
    }

    async fn load_data(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        method: LoadMethod,
    ) -> Result<ServerStats> {
        let sql = sql.trim_end();
        let sql = sql.trim_end_matches(';');
        info!("load data: {}, size: {}, method: {method:?}", sql, size);
        let sql_low = sql.to_lowercase();
        let has_place_holder = sql_low.contains(LOAD_PLACEHOLDER);
        let sql = match (self.client.capability().streaming_load, has_place_holder) {
            (false, false) => {
                // todo: deprecate this later
                return self
                    .load_data_with_options(sql, data, size, None, None)
                    .await;
            }
            (false, true) => return Err(Error::BadArgument(
                "Please upgrade your server to >= 1.2.781 to support insert from @_databend_load"
                    .to_string(),
            )),
            (true, false) => {
                format!("{sql} from @_databend_load file_format=(type=csv)")
            }
            (true, true) => sql.to_string(),
        };

        match method {
            LoadMethod::Streaming => self.load_data_with_streaming(&sql, data, size).await,
            LoadMethod::Stage => self.load_data_with_stage(&sql, data, size).await,
        }
    }

    async fn load_file(&self, sql: &str, fp: &Path, method: LoadMethod) -> Result<ServerStats> {
        info!("load file: {}, file: {:?}", sql, fp,);
        let file = File::open(fp).await?;
        let metadata = file.metadata().await?;
        let size = metadata.len();
        let data = BufReader::new(file);
        self.load_data(sql, Box::new(data), size, method).await
    }

    async fn load_file_with_options(
        &self,
        sql: &str,
        fp: &Path,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        let file = File::open(fp).await?;
        let metadata = file.metadata().await?;
        let size = metadata.len();
        let data = BufReader::new(file);
        self.load_data_with_options(sql, Box::new(data), size, file_format_options, copy_options)
            .await
    }

    async fn stream_load(
        &self,
        sql: &str,
        data: Vec<Vec<&str>>,
        method: LoadMethod,
    ) -> Result<ServerStats> {
        info!("stream load: {}, length: {:?}", sql, data.len());
        let mut wtr = csv::WriterBuilder::new().from_writer(vec![]);
        for row in data {
            wtr.write_record(row)
                .map_err(|e| Error::BadArgument(e.to_string()))?;
        }
        let bytes = wtr.into_inner().map_err(|e| Error::IO(e.to_string()))?;
        let size = bytes.len() as u64;
        let reader = Box::new(std::io::Cursor::new(bytes));
        let stats = if self.client.capability().streaming_load {
            let sql = format!("{sql} from @_databend_load file_format = (type = csv)");
            self.load_data(&sql, reader, size, method).await?
        } else {
            self.load_data_with_options(sql, reader, size, None, None)
                .await?
        };
        Ok(stats)
    }
}

impl<'o> RestAPIConnection {
    pub async fn try_create(dsn: &str, name: String) -> Result<Self> {
        let client = APIClient::new(dsn, Some(name)).await?;
        Ok(Self { client })
    }

    fn default_file_format_options() -> BTreeMap<&'o str, &'o str> {
        vec![
            ("type", "CSV"),
            ("field_delimiter", ","),
            ("record_delimiter", "\n"),
            ("skip_header", "0"),
        ]
        .into_iter()
        .collect()
    }

    fn default_copy_options() -> BTreeMap<&'o str, &'o str> {
        vec![("purge", "true")].into_iter().collect()
    }
}

pub struct RestAPIRows<T> {
    pages: Pages,

    schema: SchemaRef,
    data: VecDeque<Vec<Option<String>>>,
    stats: Option<ServerStats>,

    _phantom: std::marker::PhantomData<T>,
}

impl<T> RestAPIRows<T> {
    async fn from_pages(pages: Pages) -> Result<(Schema, Self)> {
        let (pages, schema) = pages.wait_for_schema(true).await?;
        let schema: Schema = schema.try_into()?;
        let rows = Self {
            pages,
            schema: Arc::new(schema.clone()),
            data: Default::default(),
            stats: None,
            _phantom: PhantomData,
        };
        Ok((schema, rows))
    }
}

impl<T: FromRowStats + std::marker::Unpin> Stream for RestAPIRows<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(ss) = self.stats.take() {
            return Poll::Ready(Some(Ok(T::from_stats(ss))));
        }
        // Skip to fetch next page if there is only one row left in buffer.
        // Therefore, we could guarantee the `/final` called before the last row.
        if self.data.len() > 1 {
            if let Some(row) = self.data.pop_front() {
                let row = T::try_from_row(row, self.schema.clone())?;
                return Poll::Ready(Some(Ok(row)));
            }
        }

        match Pin::new(&mut self.pages).poll_next(cx) {
            Poll::Ready(Some(Ok(page))) => {
                if self.schema.fields().is_empty() {
                    self.schema = Arc::new(page.schema.try_into()?);
                }
                let mut new_data = page.data.into();
                self.data.append(&mut new_data);
                Poll::Ready(Some(Ok(T::from_stats(page.stats.into()))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => match self.data.pop_front() {
                Some(row) => {
                    let row = T::try_from_row(row, self.schema.clone())?;
                    Poll::Ready(Some(Ok(row)))
                }
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

trait FromRowStats: Send + Sync + Clone {
    fn from_stats(stats: ServerStats) -> Self;
    fn try_from_row(row: Vec<Option<String>>, schema: SchemaRef) -> Result<Self>;
}

impl FromRowStats for RowWithStats {
    fn from_stats(stats: ServerStats) -> Self {
        RowWithStats::Stats(stats)
    }

    fn try_from_row(row: Vec<Option<String>>, schema: SchemaRef) -> Result<Self> {
        Ok(RowWithStats::Row(Row::try_from((schema, row))?))
    }
}

impl FromRowStats for RawRowWithStats {
    fn from_stats(stats: ServerStats) -> Self {
        RawRowWithStats::Stats(stats)
    }

    fn try_from_row(row: Vec<Option<String>>, schema: SchemaRef) -> Result<Self> {
        let rows = Row::try_from((schema, row.clone()))?;
        Ok(RawRowWithStats::Row(RawRow::new(rows, row)))
    }
}
