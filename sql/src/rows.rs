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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use serde::Deserialize;
use tokio_stream::{Stream, StreamExt};

use crate::error::{Error, Result};
use crate::value::Value;
use arrow::record_batch::RecordBatch;
use databend_client::schema::SchemaRef;
use databend_client::ResultFormatSettings;
use jiff::tz::TimeZone;

#[derive(Clone, Debug)]
pub enum RowWithStats {
    Row(Row),
    Stats(ServerStats),
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct ServerStats {
    #[serde(default)]
    pub total_rows: usize,
    #[serde(default)]
    pub total_bytes: usize,

    #[serde(default)]
    pub read_rows: usize,
    #[serde(default)]
    pub read_bytes: usize,

    #[serde(default)]
    pub write_rows: usize,
    #[serde(default)]
    pub write_bytes: usize,

    #[serde(default)]
    pub running_time_ms: f64,

    #[serde(default)]
    pub spill_file_nums: usize,

    #[serde(default)]
    pub spill_bytes: usize,
}

impl ServerStats {
    pub fn normalize(&mut self) {
        if self.total_rows == 0 {
            self.total_rows = self.read_rows;
        }
        if self.total_bytes == 0 {
            self.total_bytes = self.read_bytes;
        }
    }

    pub fn merge(&mut self, other: &ServerStats) {
        self.total_rows += other.total_rows;
        self.total_bytes += other.total_bytes;
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.write_rows += other.write_rows;
        self.write_bytes += other.write_bytes;
        self.running_time_ms += other.running_time_ms;
        self.spill_file_nums += other.spill_file_nums;
        self.spill_bytes += other.spill_bytes;
    }
}

impl From<databend_client::QueryStats> for ServerStats {
    fn from(stats: databend_client::QueryStats) -> Self {
        let mut p = Self {
            total_rows: 0,
            total_bytes: 0,
            read_rows: stats.progresses.scan_progress.rows,
            read_bytes: stats.progresses.scan_progress.bytes,
            write_rows: stats.progresses.write_progress.rows,
            write_bytes: stats.progresses.write_progress.bytes,
            spill_file_nums: stats.progresses.spill_progress.file_nums,
            spill_bytes: stats.progresses.spill_progress.bytes,
            running_time_ms: stats.running_time_ms,
        };
        if let Some(total) = stats.progresses.total_scan {
            p.total_rows = total.rows;
            p.total_bytes = total.bytes;
        }
        p
    }
}

#[derive(Clone, Debug, Default)]
pub struct Row {
    schema: SchemaRef,
    values: Vec<Value>,
}

impl Row {
    pub fn new(schema: SchemaRef, values: Vec<Value>) -> Self {
        Self { schema, values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn from_vec(schema: SchemaRef, values: Vec<Value>) -> Self {
        Self { schema, values }
    }
}

impl TryFrom<(SchemaRef, Vec<Option<String>>, &TimeZone)> for Row {
    type Error = Error;

    fn try_from((schema, data, tz): (SchemaRef, Vec<Option<String>>, &TimeZone)) -> Result<Self> {
        let mut values: Vec<Value> = Vec::with_capacity(data.len());
        for (field, val) in schema.fields().iter().zip(data.into_iter()) {
            values.push(Value::try_from((&field.data_type, val, tz))?);
        }
        Ok(Self::new(schema, values))
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[derive(Clone, Debug)]
pub struct Rows {
    rows: Vec<Row>,
}

impl Rows {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    // pub fn schema(&self) -> SchemaRef {
    //     self.schema.clone()
    // }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl TryFrom<(RecordBatch, ResultFormatSettings)> for Rows {
    type Error = Error;
    fn try_from((batch, settings): (RecordBatch, ResultFormatSettings)) -> Result<Self> {
        let batch_schema = batch.schema();
        let schema = SchemaRef::new(batch_schema.clone().try_into()?);
        let mut rows: Vec<Row> = Vec::new();
        for i in 0..batch.num_rows() {
            let mut values: Vec<Value> = Vec::new();
            for j in 0..batch_schema.fields().len() {
                let v = batch.column(j);
                let field = batch_schema.field(j);
                let value = Value::try_from((field, v, i, &settings))?;
                values.push(value);
            }
            rows.push(Row::new(schema.clone(), values));
        }
        Ok(Self::new(rows))
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

// This macro implements TryFrom for tuple of types
macro_rules! impl_tuple_from_row {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> TryFrom<Row> for ($($Ti,)+)
        where
            $($Ti: TryFrom<Value>),+
        {
            type Error = String;
            fn try_from(row: Row) -> Result<Self, String> {
                // It is not possible yet to get the number of metavariable repetitions
                // ref: https://github.com/rust-lang/lang-team/issues/28#issue-644523674
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);

                if expected_len != row.len() {
                    return Err(format!("row size mismatch: expected {} columns, got {}", expected_len, row.len()));
                }
                let mut vals_iter = row.into_iter().enumerate();

                Ok((
                    $(
                        {
                            let (col_ix, col_value) = vals_iter
                                .next()
                                .unwrap(); // vals_iter size is checked before this code is reached,
                                           // so it is safe to unwrap
                            let t = col_value.get_type();
                            $Ti::try_from(col_value)
                                .map_err(|_| format!("failed converting column {} from type({:?}) to type({})", col_ix, t, std::any::type_name::<$Ti>()))?
                        }
                    ,)+
                ))
            }
        }
    }
}

// Implement FromRow for tuples of size up to 16
impl_tuple_from_row!(T1);
impl_tuple_from_row!(T1, T2);
impl_tuple_from_row!(T1, T2, T3);
impl_tuple_from_row!(T1, T2, T3, T4);
impl_tuple_from_row!(T1, T2, T3, T4, T5);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

pub struct RowIterator {
    schema: SchemaRef,
    it: Option<Pin<Box<dyn Stream<Item = Result<Row>> + Send>>>,
}

impl RowIterator {
    pub fn new(schema: SchemaRef, it: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>) -> Self {
        Self {
            schema,
            it: Some(it),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub async fn try_collect<T>(mut self) -> Result<Vec<T>>
    where
        T: TryFrom<Row>,
        T::Error: std::fmt::Display,
    {
        if let Some(it) = &mut self.it {
            let mut ret = Vec::new();
            while let Some(row) = it.next().await {
                let v = T::try_from(row?).map_err(|e| Error::Parsing(e.to_string()))?;
                ret.push(v)
            }
            Ok(ret)
        } else {
            Err(Error::BadArgument("RowIterator already closed".to_owned()))
        }
    }

    pub fn close(&mut self) {
        self.it = None;
    }
}

impl Stream for RowIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(it) = self.it.as_mut() {
            Pin::new(it).poll_next(cx)
        } else {
            Poll::Ready(Some(Err(Error::BadArgument(
                "RowIterator already closed".to_owned(),
            ))))
        }
    }
}

pub struct RowStatsIterator {
    schema: SchemaRef,
    it: Option<Pin<Box<dyn Stream<Item = Result<RowWithStats>> + Send>>>,
}

impl RowStatsIterator {
    pub fn new(
        schema: SchemaRef,
        it: Pin<Box<dyn Stream<Item = Result<RowWithStats>> + Send>>,
    ) -> Self {
        Self {
            schema,
            it: Some(it),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub async fn filter_rows(self) -> Result<RowIterator> {
        if let Some(it) = self.it {
            let it = it.filter_map(|r| match r {
                Ok(RowWithStats::Row(r)) => Some(Ok(r)),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            });
            Ok(RowIterator::new(self.schema, Box::pin(it)))
        } else {
            Err(Error::BadArgument("RowIterator already closed".to_owned()))
        }
    }

    pub fn close(&mut self) {
        self.it = None;
    }
}

impl Stream for RowStatsIterator {
    type Item = Result<RowWithStats>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(it) = self.it.as_mut() {
            Pin::new(it).poll_next(cx)
        } else {
            Poll::Ready(Some(Err(Error::BadArgument(
                "RowStatsIterator already closed".to_owned(),
            ))))
        }
    }
}
