// Copyright 2023 Datafuse Labs.
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

use anyhow::{Error, Result};

use crate::schema::DataType;
use crate::value::Value;

#[derive(Debug, Default)]
pub struct Row(Vec<Value>);

impl TryFrom<(Vec<DataType>, Vec<String>)> for Row {
    type Error = Error;

    fn try_from((schema, data): (Vec<DataType>, Vec<String>)) -> Result<Self> {
        let mut row: Vec<Value> = Vec::new();
        for (i, value) in data.into_iter().enumerate() {
            row.push(Value::try_from((schema[i].clone(), value))?);
        }
        Ok(Self(row))
    }
}

impl Row {
    /// Allows converting Row into tuple of rust types or custom struct deriving FromRow
    pub fn into_typed<RowT: FromRow>(self) -> Result<RowT> {
        RowT::from_row(self)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// This trait defines a way to convert Row value into some rust type
pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self>;
}
