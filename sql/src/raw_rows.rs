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

use tokio_stream::{Stream, StreamExt};

use crate::error::Result;
use crate::rows::ServerStats;
use crate::schema::SchemaRef;

#[derive(Clone, Debug)]
pub enum RawRowWithStats {
    Row(RawRow),
    Stats(ServerStats),
}

#[derive(Clone, Debug, Default)]
pub struct RawRow {
    pub schema: SchemaRef,
    pub values: Vec<Option<String>>,
}

impl RawRow {
    pub fn new(schema: SchemaRef, values: Vec<Option<String>>) -> Self {
        Self { schema, values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Option<String>] {
        &self.values
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn from_vec(schema: SchemaRef, values: Vec<Option<String>>) -> Self {
        Self { schema, values }
    }
}

impl From<(SchemaRef, Vec<Option<String>>)> for RawRow {
    fn from(value: (SchemaRef, Vec<Option<String>>)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl IntoIterator for RawRow {
    type Item = Option<String>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
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
