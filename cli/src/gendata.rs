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

use crate::{ast::GenType, session::Session};
use anyhow::anyhow;
use anyhow::Result;
use databend_driver::DataType;
use databend_driver::Field;
use databend_driver::NumberDataType;
use databend_driver::NumberValue;
use databend_driver::Row;
use databend_driver::RowStatsIterator;
use databend_driver::RowWithStats;
use databend_driver::Schema;

#[cfg(not(any(
    all(target_arch = "x86_64", target_os = "linux"),
    all(target_arch = "aarch64", target_os = "macos")
)))]
impl Session {
    pub(crate) async fn gendata(
        &self,
        _t: GenType,
        _scale: f32,
        _drop_override: bool,
    ) -> Result<RowStatsIterator> {
        Err(anyhow!("gendata is not supported on this platform"))
    }
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
impl Session {
    pub(crate) async fn gendata(
        &self,
        t: GenType,
        scale: f32,
        drop_override: bool,
    ) -> Result<RowStatsIterator> {
        use std::sync::Arc;

        use databend_driver::Value;
        use duckdb::params;
        use duckdb::Connection;
        use tempfile::tempdir;
        use tokio::fs::File;
        use tokio::io::BufReader;

        let temp_dir = tempdir()?;
        // use duckdb to generate tpch/tpcds data in memory and upload it via upload api
        let conn = Connection::open_in_memory().map_err(|err| anyhow!("{}", err))?;
        match t {
            GenType::TPCH => {
                conn.execute("install tpch;", params![]).unwrap();
                conn.execute("load tpch;", params![]).unwrap();
                conn.execute(&format!("CALL DBGEN(sf = {});", scale), params![])
                    .unwrap();
            }
            GenType::TPCDS => {
                conn.execute("install tpcds;", params![]).unwrap();
                conn.execute("load tpcds;", params![]).unwrap();
                conn.execute(&format!("CALL DSDGEN(sf = {});", scale), params![])
                    .unwrap();
            }
        }

        conn.execute(
            &format!(
                "EXPORT DATABASE '{}/' (FORMAT PARQUET);",
                temp_dir.path().display()
            ),
            params![],
        )
        .unwrap();

        let mut results = vec![];
        let schema = Arc::new(gendata_schema());

        let mut entries: Vec<_> = std::fs::read_dir(&temp_dir)?.collect();
        entries.sort_by_key(|e| e.as_ref().unwrap().path());

        for f in entries {
            let f = f?;
            let path = f.path();

            // Skip if the path is a directory or if it does not end with .parquet
            if path.is_dir() || path.extension().is_none_or(|ext| ext != "parquet") {
                continue;
            }
            let table_name = path.file_stem().unwrap().to_str().unwrap().to_string();

            let file = File::open(&path).await?;
            let metadata = file.metadata().await.unwrap();
            let data = BufReader::new(file);
            let size = metadata.len();

            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
            let stage = format!("@~/client/load/{}", now);
            self.conn
                .upload_to_stage(&stage, Box::new(data), size)
                .await?;

            let create = if drop_override {
                "CREATE OR REPLACE"
            } else {
                "CREATE"
            };

            let _ = self
                .conn
                .exec(
                    &format!("{create} TABLE {table_name} as SELECT * FROM '{stage}' limit 0",),
                    (),
                )
                .await?;

            let _ = self
                .conn
                .exec(&format!(
                    "COPY INTO {table_name} FROM (SELECT * FROM '{stage}')  force = true purge = true",
                ),())
                .await?;

            results.push(Ok(RowWithStats::Row(Row::from_vec(
                schema.clone(),
                vec![
                    Value::String(table_name),
                    Value::String("OK".to_string()),
                    Value::Number(NumberValue::UInt64(size)),
                ],
            ))));
        }

        Ok(RowStatsIterator::new(
            schema,
            Box::pin(tokio_stream::iter(results)),
        ))
    }
}

pub fn gendata_schema() -> Schema {
    Schema::from_vec(vec![
        Field {
            name: "table".to_string(),
            data_type: DataType::String,
        },
        Field {
            name: "status".to_string(),
            data_type: DataType::String,
        },
        Field {
            name: "size".to_string(),
            data_type: DataType::Number(NumberDataType::UInt64),
        },
    ])
}
