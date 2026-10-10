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

use std::collections::VecDeque;
use std::fmt::{self, Write};

use databend_driver::{Row, Schema, ServerStats, Value};
use serde::Serialize;

const MAX_ROWS: usize = 30;
const MAX_COLUMNS: usize = 64;
const MAX_PREVIEW_BYTES: usize = 16 * 1024;
const MAX_RECORDS: usize = 20;
const MAX_MEMORY_BYTES: usize = 256 * 1024;
pub const MAX_TEXT_BYTES: usize = 8 * 1024;

/// A UTF-8 safe formatter that bounds retained text and stops formatting early.
struct BoundedText {
    text: String,
    limit: usize,
    truncated: bool,
}

impl Write for BoundedText {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.truncated {
            return Err(fmt::Error);
        }
        let remaining = self.limit.saturating_sub(self.text.len());
        let mut end = text.len().min(remaining);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        self.text.push_str(&text[..end]);
        self.truncated = end < text.len();
        if self.truncated {
            Err(fmt::Error)
        } else {
            Ok(())
        }
    }
}

pub fn bounded_text(value: impl fmt::Display, limit: usize) -> (String, bool) {
    let mut writer = BoundedText {
        text: String::new(),
        limit,
        truncated: false,
    };
    let _ = write!(writer, "{value}");
    (writer.text, writer.truncated)
}

#[derive(Default, Debug, Serialize)]
pub struct QueryPreview {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<Option<String>>>,
    pub fetched_rows: usize,
    pub total_rows: Option<usize>,
    pub complete: bool,
    pub cells_truncated: bool,
    pub schema_truncated: bool,
    #[serde(skip)]
    bytes: usize,
    #[serde(skip)]
    stopped: bool,
}

impl QueryPreview {
    pub fn new(schema: &Schema) -> Self {
        let mut preview = Self {
            schema_truncated: schema.fields().len() > MAX_COLUMNS,
            ..Self::default()
        };
        for field in schema.fields().iter().take(MAX_COLUMNS) {
            let (name, name_cut) = bounded_text(&field.name, 256);
            let (ty, type_cut) = bounded_text(&field.data_type, 256);
            preview.schema_truncated |= name_cut || type_cut;
            if preview.bytes + name.len() + ty.len() > 4096 {
                preview.schema_truncated = true;
                break;
            }
            preview.bytes += name.len() + ty.len();
            preview.columns.push((name, ty));
        }
        preview
    }

    pub fn observe(&mut self, row: &Row) {
        self.fetched_rows += 1;
        if self.stopped || self.rows.len() >= MAX_ROWS {
            return;
        }
        let mut cells = Vec::new();
        let mut bytes = 0;
        let mut truncated = false;
        for value in row.values().iter().take(self.columns.len()) {
            if matches!(value, Value::Null) {
                cells.push(None);
                bytes += 4;
            } else {
                let (text, cut) = match value {
                    Value::Binary(bytes) => {
                        let mut text = String::new();
                        for byte in bytes.iter().take(256) {
                            let _ = write!(text, "{byte:02X}");
                        }
                        (text, bytes.len() > 256)
                    }
                    _ => bounded_text(value, 512),
                };
                bytes += text.len() + 8;
                truncated |= cut;
                cells.push(Some(text));
            }
        }
        if self.bytes + bytes > MAX_PREVIEW_BYTES {
            self.stopped = true;
            return;
        }
        self.bytes += bytes;
        self.cells_truncated |= truncated;
        self.rows.push(cells);
    }

    pub fn finish(&mut self, success: bool) {
        self.total_rows = success.then_some(self.fetched_rows);
        self.complete = success
            && self.rows.len() == self.fetched_rows
            && !self.cells_truncated
            && !self.schema_truncated;
    }
}

#[derive(Debug, Serialize)]
pub struct QueryRecord {
    pub id: String,
    pub sql: String,
    pub sql_truncated: bool,
    pub query_id: Option<String>,
    pub database: Option<String>,
    pub warehouse: Option<String>,
    pub status: String,
    pub cancel_request_sent: bool,
    pub elapsed_ms: u128,
    pub preview: QueryPreview,
    pub stats: Option<serde_json::Value>,
    pub error: Option<String>,
}

impl QueryRecord {
    pub fn new(sql: &str) -> Self {
        let (sql, sql_truncated) = bounded_text(sql, MAX_TEXT_BYTES);
        Self {
            id: String::new(),
            sql,
            sql_truncated,
            query_id: None,
            database: None,
            warehouse: None,
            status: "failed".into(),
            cancel_request_sent: false,
            elapsed_ms: 0,
            preview: QueryPreview::default(),
            stats: None,
            error: None,
        }
    }

    pub fn set_stats(&mut self, stats: &ServerStats) {
        self.stats = Some(serde_json::json!({
            "read_rows": stats.read_rows,
            "read_bytes": stats.read_bytes,
            "write_rows": stats.write_rows,
            "write_bytes": stats.write_bytes,
            "server_running_time_ms": stats.running_time_ms,
            "spill_bytes": stats.spill_bytes,
        }));
    }
}

#[derive(Default)]
pub struct Memory {
    records: VecDeque<(QueryRecord, usize)>,
    bytes: usize,
    next_id: usize,
}

impl Memory {
    pub fn push(&mut self, mut record: QueryRecord) -> String {
        self.next_id += 1;
        record.id = format!("Q{}", self.next_id);
        let id = record.id.clone();
        let bytes = serde_json::to_vec(&record)
            .expect("query record is serializable")
            .len();
        if bytes > MAX_MEMORY_BYTES {
            return id;
        }
        while self.records.len() >= MAX_RECORDS || self.bytes + bytes > MAX_MEMORY_BYTES {
            if let Some((_, size)) = self.records.pop_front() {
                self.bytes -= size;
            }
        }
        self.bytes += bytes;
        self.records.push_back((record, bytes));
        id
    }

    pub fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
        // Do not reuse IDs: an old reference must not silently refer to a new query.
    }

    pub fn summary(&self) -> String {
        self.records
            .iter()
            .map(|(r, _)| {
                format!(
                    "{}: {} | cached {}/{} rows | complete={} | {}\n",
                    r.id,
                    r.status,
                    r.preview.rows.len(),
                    r.preview
                        .total_rows
                        .map_or("unknown".into(), |n| n.to_string()),
                    r.preview.complete,
                    bounded_text(r.sql.replace('\n', " "), 160).0
                )
            })
            .collect()
    }

    /// Select recent records and exact Q<n> references, without cutting JSON.
    pub fn context(&self, question: &str, budget: usize) -> String {
        let references: Vec<&str> = question
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|s| s.starts_with('Q') && s[1..].chars().all(|c| c.is_ascii_digit()))
            .collect();
        let mut selected = Vec::new();
        let mut remaining = budget.saturating_sub(2);
        for referenced in [true, false] {
            for (record, _) in self.records.iter().rev() {
                if references.contains(&record.id.as_str()) != referenced {
                    continue;
                }
                let mut value = serde_json::to_value(record).expect("query record is serializable");
                let mut json = value.to_string();
                // A large preview must not hide the latest SQL/status/error entirely.
                if json.len() >= remaining && (referenced || selected.is_empty()) {
                    value["preview"]["rows"] = serde_json::json!([]);
                    value["preview"]["complete"] = false.into();
                    value["context_preview_omitted"] = true.into();
                    json = value.to_string();
                    if json.len() >= remaining {
                        value["preview"]["columns"] = serde_json::json!([]);
                        value["preview"]["schema_truncated"] = true.into();
                        let text_budget =
                            remaining.saturating_sub(1024).saturating_div(12).min(512);
                        value["sql"] = bounded_text(&record.sql, text_budget).0.into();
                        value["sql_truncated"] = true.into();
                        value["error"] = record
                            .error
                            .as_ref()
                            .map(|e| bounded_text(e, text_budget).0)
                            .into();
                        json = value.to_string();
                    }
                }
                if json.len() < remaining {
                    remaining -= json.len() + 1;
                    selected.push(json);
                }
            }
        }
        format!("[{}]", selected.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_is_bounded_and_utf8_safe() {
        assert_eq!(bounded_text("中文abc", 4), ("中".into(), true));
        assert_eq!(bounded_text("abc", 3), ("abc".into(), false));
    }

    fn schema() -> Schema {
        Schema::from_vec(vec![
            databend_driver::Field {
                name: "a".into(),
                data_type: databend_driver::DataType::String,
            },
            databend_driver::Field {
                name: "b".into(),
                data_type: databend_driver::DataType::String,
            },
        ])
    }

    #[test]
    fn preview_distinguishes_null_and_string_and_bounds_rows() {
        let mut preview = QueryPreview::new(&schema());
        let row = Row::new(
            std::sync::Arc::new(schema()),
            vec![Value::Null, Value::String("NULL".into())],
        );
        for _ in 0..100 {
            preview.observe(&row);
        }
        preview.finish(true);
        assert_eq!(preview.rows.len(), MAX_ROWS);
        assert_eq!(preview.total_rows, Some(100));
        assert!(!preview.complete);
        assert_eq!(preview.rows[0], vec![None, Some("NULL".into())]);
    }

    #[test]
    fn failed_and_truncated_previews_are_not_complete() {
        let mut preview = QueryPreview::new(&schema());
        preview.finish(false);
        assert_eq!(preview.total_rows, None);
        assert!(!preview.complete);
        preview.observe(&Row::new(
            Default::default(),
            vec![Value::String("x".repeat(1000))],
        ));
        preview.finish(true);
        assert!(preview.cells_truncated);
        assert!(!preview.complete);
    }

    #[test]
    fn preview_and_serialized_memory_have_byte_budgets() {
        let mut preview = QueryPreview::new(&schema());
        let row = Row::new(
            std::sync::Arc::new(schema()),
            vec![Value::String("x".repeat(512)); 2],
        );
        for _ in 0..100 {
            preview.observe(&row);
        }
        assert!(preview.bytes <= MAX_PREVIEW_BYTES);
        assert!(preview.rows.len() < MAX_ROWS);
        let mut memory = Memory::default();
        for _ in 0..100 {
            let mut record = QueryRecord::new(&"\u{0000}".repeat(MAX_TEXT_BYTES));
            record.error = Some("\u{0000}".repeat(4096));
            memory.push(record);
        }
        assert!(memory.bytes <= MAX_MEMORY_BYTES);
        assert!(memory.records.len() < MAX_RECORDS);
        let context = memory.context("last query", 2048);
        assert!(context.len() <= 2048);
        let records: serde_json::Value = serde_json::from_str(&context).unwrap();
        assert_eq!(records[0]["id"], "Q100");
        assert_eq!(records[0]["sql_truncated"], true);
    }

    #[test]
    fn memory_eviction_references_and_clear() {
        let mut memory = Memory::default();
        for n in 0..25 {
            memory.push(QueryRecord::new(&format!("SELECT {n}")));
        }
        assert_eq!(memory.records.len(), MAX_RECORDS);
        assert!(!memory.summary().contains("Q1:"));
        let context = memory.context("compare Q6 with Q25", 600);
        assert!(context.len() <= 600);
        let parsed: serde_json::Value = serde_json::from_str(&context).unwrap();
        assert!(parsed.as_array().unwrap().iter().any(|v| v["id"] == "Q25"));
        memory.clear();
        assert_eq!(memory.context("Q6", 1000), "[]");
        assert_eq!(memory.push(QueryRecord::new("SELECT 1")), "Q26");
    }
}
