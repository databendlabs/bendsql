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
use crate::value::base::{NumberValue, DAYS_FROM_CE, TIMESTAMP_FORMAT, TIMESTAMP_TIMEZONE_FORMAT};
use chrono::NaiveDate;

impl Value {
    /// Convert a Value to a serde_json::Value for use in Params.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::EmptyArray => serde_json::json!([]),
            Value::EmptyMap => serde_json::json!({}),
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Number(n) => match n {
                NumberValue::Int8(v) => serde_json::json!(v),
                NumberValue::Int16(v) => serde_json::json!(v),
                NumberValue::Int32(v) => serde_json::json!(v),
                NumberValue::Int64(v) => serde_json::json!(v),
                NumberValue::UInt8(v) => serde_json::json!(v),
                NumberValue::UInt16(v) => serde_json::json!(v),
                NumberValue::UInt32(v) => serde_json::json!(v),
                NumberValue::UInt64(v) => serde_json::json!(v),
                NumberValue::Float32(v) => serde_json::json!(v),
                NumberValue::Float64(v) => serde_json::json!(v),
                NumberValue::Decimal64(v, _) => serde_json::Value::String(v.to_string()),
                NumberValue::Decimal128(v, _) => serde_json::Value::String(v.to_string()),
                NumberValue::Decimal256(v, _) => serde_json::Value::String(v.to_string()),
            },
            Value::Timestamp(dt) => {
                serde_json::Value::String(dt.strftime(TIMESTAMP_FORMAT).to_string())
            }
            Value::TimestampTz(dt) => {
                serde_json::Value::String(dt.strftime(TIMESTAMP_TIMEZONE_FORMAT).to_string())
            }
            Value::Date(d) => {
                let date = NaiveDate::from_num_days_from_ce_opt(*d + DAYS_FROM_CE).unwrap();
                serde_json::Value::String(date.format("%Y-%m-%d").to_string())
            }
            Value::Binary(b) => serde_json::Value::String(hex::encode(b)),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.to_json_value()).collect())
            }
            Value::Map(map) => {
                let obj: serde_json::Map<String, serde_json::Value> = map
                    .iter()
                    .map(|(k, v)| {
                        let key = match k {
                            Value::String(s) => s.clone(),
                            other => format!("{:?}", other),
                        };
                        (key, v.to_json_value())
                    })
                    .collect();
                serde_json::Value::Object(obj)
            }
            Value::Tuple(tuple) => {
                serde_json::Value::Array(tuple.iter().map(|v| v.to_json_value()).collect())
            }
            Value::Bitmap(b) => serde_json::Value::String(b.clone()),
            Value::Variant(v) => serde_json::Value::String(v.clone()),
            Value::Geometry(g) => serde_json::Value::String(g.clone()),
            Value::Geography(g) => serde_json::Value::String(g.clone()),
            Value::Interval(i) => serde_json::Value::String(i.clone()),
            Value::Vector(v) => {
                serde_json::Value::Array(v.iter().map(|f| serde_json::json!(f)).collect())
            }
        }
    }

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
                format!("'{}'", dt.strftime(TIMESTAMP_FORMAT))
            }
            Value::TimestampTz(dt) => {
                let formatted = dt.strftime(TIMESTAMP_TIMEZONE_FORMAT);
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
