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
use crate::value::NumberValue;
use chrono::NaiveDate;
use ethnum::i256;
use std::fmt::Write;

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.display_value(f, true)
    }
}

impl std::fmt::Display for NumberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberValue::Int8(i) => write!(f, "{i}"),
            NumberValue::Int16(i) => write!(f, "{i}"),
            NumberValue::Int32(i) => write!(f, "{i}"),
            NumberValue::Int64(i) => write!(f, "{i}"),
            NumberValue::UInt8(i) => write!(f, "{i}"),
            NumberValue::UInt16(i) => write!(f, "{i}"),
            NumberValue::UInt32(i) => write!(f, "{i}"),
            NumberValue::UInt64(i) => write!(f, "{i}"),
            NumberValue::Float32(i) => write!(f, "{i}"),
            NumberValue::Float64(i) => write!(f, "{i}"),
            NumberValue::Decimal128(v, s) => write!(f, "{}", display_decimal_128(*v, s.scale)),
            NumberValue::Decimal256(v, s) => write!(f, "{}", display_decimal_256(*v, s.scale)),
        }
    }
}

impl Value {
    // Used as output of cli
    // Compatible with Databend, strings inside nested types are quoted.
    pub fn display_value(&self, f: &mut std::fmt::Formatter<'_>, raw: bool) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::EmptyArray => write!(f, "[]"),
            Value::EmptyMap => write!(f, "{{}}"),
            Value::Boolean(b) => {
                if *b {
                    write!(f, "true")
                } else {
                    write!(f, "false")
                }
            }
            Value::Number(n) => write!(f, "{n}"),
            Value::Binary(s) => write!(f, "{}", hex::encode_upper(s)),
            Value::String(s)
            | Value::Bitmap(s)
            | Value::Variant(s)
            | Value::Interval(s)
            | Value::Geometry(s)
            | Value::Geography(s) => {
                if raw {
                    write!(f, "{s}")
                } else {
                    write!(f, "'{s}'")
                }
            }
            Value::Timestamp(dt) => {
                let formatted = dt.strftime(TIMESTAMP_FORMAT);
                if raw {
                    write!(f, "{formatted}")
                } else {
                    write!(f, "'{formatted}'")
                }
            }
            Value::TimestampTz(dt) => {
                let formatted = dt.strftime(TIMESTAMP_TIMEZONE_FORMAT);
                if raw {
                    write!(f, "{formatted}")
                } else {
                    write!(f, "'{formatted}'")
                }
            }
            Value::Date(i) => {
                let days = i + DAYS_FROM_CE;
                let d = NaiveDate::from_num_days_from_ce_opt(days).unwrap_or_default();
                if raw {
                    write!(f, "{d}")
                } else {
                    write!(f, "'{d}'")
                }
            }
            Value::Array(vals) => {
                write!(f, "[")?;
                for (i, val) in vals.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    val.display_value(f, false)?;
                }
                write!(f, "]")?;
                Ok(())
            }
            Value::Map(kvs) => {
                write!(f, "{{")?;
                for (i, (key, val)) in kvs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    key.display_value(f, false)?;
                    write!(f, ":")?;
                    val.display_value(f, false)?;
                }
                write!(f, "}}")?;
                Ok(())
            }
            Value::Tuple(vals) => {
                write!(f, "(")?;
                for (i, val) in vals.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    val.display_value(f, false)?;
                }
                write!(f, ")")?;
                Ok(())
            }
            Value::Vector(vals) => {
                write!(f, "[")?;
                for (i, val) in vals.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{val}")?;
                }
                write!(f, "]")?;
                Ok(())
            }
        }
    }
}

pub fn display_decimal_128(num: i128, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{num}").unwrap();
    } else {
        let pow_scale = 10_i128.pow(scale as u32);
        if num >= 0 {
            write!(
                buf,
                "{}.{:0>width$}",
                num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        } else {
            write!(
                buf,
                "-{}.{:0>width$}",
                -num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        }
    }
    buf
}

pub fn display_decimal_256(num: i256, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{}", num).unwrap();
    } else {
        let pow_scale = i256::from(10).pow(scale as u32);
        // -1/10 = 0
        if !num.is_negative() {
            write!(
                buf,
                "{}.{:0>width$}",
                num / pow_scale,
                (num % pow_scale).wrapping_abs(),
                width = scale as usize
            )
        } else {
            write!(
                buf,
                "-{}.{:0>width$}",
                -num / pow_scale,
                (num % pow_scale).wrapping_abs(),
                width = scale as usize
            )
        }
        .expect("display_decimal_256 should not fail");
    }
    String::from_utf8_lossy(buf.as_bytes()).to_string()
}
