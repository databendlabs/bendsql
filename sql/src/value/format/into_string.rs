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

use crate::_macro_internal::{Error, Value};
use crate::error::ConvertError;
use crate::value::base::{DAYS_FROM_CE, TIMESTAMP_FORMAT};
use crate::value::format::display::{display_decimal_128, display_decimal_256};
use crate::value::NumberValue;
use chrono::NaiveDate;

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(val: Value) -> crate::_macro_internal::Result<Self> {
        match val {
            Value::String(s) => Ok(s),
            Value::Bitmap(s) => Ok(s),
            Value::Number(NumberValue::Decimal128(v, s)) => Ok(display_decimal_128(v, s.scale)),
            Value::Number(NumberValue::Decimal256(v, s)) => Ok(display_decimal_256(v, s.scale)),
            Value::Geometry(s) => Ok(s),
            Value::Geography(s) => Ok(s),
            Value::Interval(s) => Ok(s),
            Value::Variant(s) => Ok(s),
            Value::Date(d) => {
                let date =
                    NaiveDate::from_num_days_from_ce_opt(d + DAYS_FROM_CE).ok_or_else(|| {
                        ConvertError::new("date", format!("invalid date value: {}", d))
                    })?;
                Ok(date.format("%Y-%m-%d").to_string())
            }
            Value::Timestamp(dt) => Ok(dt.format(TIMESTAMP_FORMAT).to_string()),
            _ => Err(ConvertError::new("string", format!("{val:?}")).into()),
        }
    }
}
