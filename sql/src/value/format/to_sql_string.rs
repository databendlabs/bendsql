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

use crate::_macro_internal::Value;
use crate::value::base::{DAYS_FROM_CE, TIMESTAMP_FORMAT, TIMESTAMP_TIMEZONE_FORMAT};
use chrono::NaiveDate;

impl Value {
    // for now only used in ORM to fmt values to insert,
    // for Params, rust use Param::as_sql_string, and py/js bindings are handled in binding code
    pub fn to_sql_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) => {
                if *b {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            Value::String(s) => format!("'{}'", s),
            Value::Number(n) => n.to_string(),
            Value::Timestamp(dt) => {
                format!("'{}'", dt.format(TIMESTAMP_FORMAT))
            }
            Value::TimestampTz(dt) => {
                let formatted = dt.format(TIMESTAMP_TIMEZONE_FORMAT);
                format!("'{formatted}'")
            }
            Value::Date(d) => {
                let date = NaiveDate::from_num_days_from_ce_opt(*d + DAYS_FROM_CE).unwrap();
                format!("'{}'", date.format("%Y-%m-%d"))
            }
            Value::Binary(b) => format!("'{}'", hex::encode(b)),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_sql_string()).collect();
                format!("[{}]", items.join(", "))
            }
            Value::Map(map) => {
                let items: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k.to_sql_string(), v.to_sql_string()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
            Value::Tuple(tuple) => {
                let items: Vec<String> = tuple.iter().map(|v| v.to_sql_string()).collect();
                format!("({})", items.join(", "))
            }
            Value::Bitmap(b) => format!("'{}'", b),
            Value::Variant(v) => format!("'{}'", v),
            Value::Geometry(g) => format!("'{}'", g),
            Value::Geography(g) => format!("'{}'", g),
            Value::Interval(i) => format!("'{}'", i),
            Value::Vector(v) => {
                let items: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                format!("[{}]", items.join(", "))
            }
            Value::EmptyArray => "[]".to_string(),
            Value::EmptyMap => "{}".to_string(),
        }
    }
}
