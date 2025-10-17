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
use databend_driver::RowStatsIterator;
use databend_driver::Schema;
use databend_driver::{NumberValue, Row, RowWithStats, Value};
use std::process::Command;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::BufReader;

impl Session {
    pub(crate) async fn gendata(
        &self,
        t: GenType,
        scale: f32,
        drop_override: bool,
    ) -> Result<RowStatsIterator> {
        // Check if duckdb is available
        let duckdb_check = Command::new("duckdb").arg("--version").output();
        if duckdb_check.is_err() {
            return Err(anyhow!(
                "DuckDB is not installed. Please install it first by running: !install duckdb"
            ));
        }

        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("gendata.db");
        let export_path = temp_dir.path().join("export");
        std::fs::create_dir_all(&export_path)?;

        // Create DuckDB commands based on type
        let commands = match t {
            GenType::TPCH => vec![
                "install tpch;".to_string(),
                "load tpch;".to_string(),
                format!("CALL DBGEN(sf = {});", scale),
                format!(
                    "EXPORT DATABASE '{}' (FORMAT PARQUET);",
                    export_path.display()
                ),
            ],
            GenType::TPCDS => vec![
                "install tpcds;".to_string(),
                "load tpcds;".to_string(),
                format!("CALL DSDGEN(sf = {});", scale),
                format!(
                    "EXPORT DATABASE '{}' (FORMAT PARQUET);",
                    export_path.display()
                ),
            ],
        };

        // Execute DuckDB commands
        for command in commands {
            let output = Command::new("duckdb")
                .arg(db_path.to_str().unwrap())
                .arg("-c")
                .arg(&command)
                .output()
                .map_err(|e| anyhow!("Failed to execute DuckDB command '{}': {}", command, e))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!("DuckDB command '{}' failed: {}", command, stderr));
            }
        }

        let mut results = vec![];
        let schema = Arc::new(gendata_schema());

        // Process exported parquet files
        let mut entries: Vec<_> = std::fs::read_dir(&export_path)?.collect();
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
            let stage = format!("@~/client/load/{now}");
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
                .exec(&format!(
                    "{create} TABLE {table_name} as SELECT * FROM '{stage}' limit 0",
                ))
                .await?;

            let _ = self
                .conn
                .exec(&format!(
                    "COPY INTO {table_name} FROM (SELECT * FROM '{stage}')  force = true purge = true",
                ))
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

fn gendata_schema() -> Schema {
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
