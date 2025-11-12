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

use chrono_tz::Tz;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio_stream::{Stream, StreamExt};

use crate::error::Error;
use crate::error::Result;
use crate::rows::Row;
use crate::rows::ServerStats;
use crate::schema::SchemaRef;
use crate::value::Value;

#[derive(Clone, Debug)]
pub enum RawRowWithStats {
    Row(RawRow),
    Stats(ServerStats),
}

#[derive(Clone, Debug, Default)]
pub struct RawRow {
    pub row: Row,
    pub raw_row: Vec<Option<String>>,
}

impl RawRow {
    pub fn new(row: Row, raw_row: Vec<Option<String>>) -> Self {
        Self { row, raw_row }
    }

    pub fn len(&self) -> usize {
        self.raw_row.len()
    }

    pub fn is_empty(&self) -> bool {
        self.raw_row.is_empty()
    }

    pub fn values(&self) -> &[Option<String>] {
        &self.raw_row
    }

    pub fn schema(&self) -> SchemaRef {
        self.row.schema()
    }
}

impl TryFrom<(SchemaRef, Vec<Option<String>>, Tz)> for RawRow {
    type Error = Error;

    fn try_from((schema, data, tz): (SchemaRef, Vec<Option<String>>, Tz)) -> Result<Self> {
        let mut values: Vec<Value> = Vec::with_capacity(data.len());
        for (field, val) in schema.fields().iter().zip(data.clone().into_iter()) {
            values.push(Value::try_from((&field.data_type, val, tz))?);
        }

        let row = Row::new(schema, values);
        Ok(RawRow::new(row, data))
    }
}

impl From<Row> for RawRow {
    fn from(row: Row) -> Self {
        let mut raw_row: Vec<Option<String>> = Vec::with_capacity(row.values().len());
        for val in row.values() {
            raw_row.push(Some(val.to_string()));
        }
        RawRow::new(row, raw_row)
    }
}

impl IntoIterator for RawRow {
    type Item = Option<String>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.raw_row.into_iter()
    }
}

#[derive(Clone, Debug)]
pub struct RawRows {
    rows: Vec<RawRow>,
}

impl RawRows {
    pub fn new(rows: Vec<RawRow>) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> &[RawRow] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl IntoIterator for RawRows {
    type Item = RawRow;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

pub struct RawRowIterator {
    schema: SchemaRef,
    it: Pin<Box<dyn Stream<Item = Result<RawRow>> + Send>>,
}

impl RawRowIterator {
    pub fn new(
        schema: SchemaRef,
        it: Pin<Box<dyn Stream<Item = Result<RawRowWithStats>> + Send>>,
    ) -> Self {
        let it = it.filter_map(|r| match r {
            Ok(RawRowWithStats::Row(r)) => Some(Ok(r)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
        Self {
            schema,
            it: Box::pin(it),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RawRowIterator {
    type Item = Result<RawRow>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.it).poll_next(cx)
    }
}
