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

use std::fmt::Write;

use arrow_buffer::i256;
use chrono::{DateTime, NaiveDate};
use hex;

use super::{NumberValue, Value, DAYS_FROM_CE, TIMESTAMP_FORMAT, TIMESTAMP_TIMEZONE_FORMAT};

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

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        encode_value(f, self, true)
    }
}

// Used as output of cli
// Compatible with Databend, strings inside nested types are quoted.
fn encode_value(f: &mut std::fmt::Formatter<'_>, val: &Value, raw: bool) -> std::fmt::Result {
    match val {
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
        Value::Timestamp(micros, _tz) => {
            let (mut secs, mut nanos) = (*micros / 1_000_000, (*micros % 1_000_000) * 1_000);
            if nanos < 0 {
                secs -= 1;
                nanos += 1_000_000_000;
            }
            let t = DateTime::from_timestamp(secs, nanos as _).unwrap_or_default();
            let t = t.naive_utc();
            if raw {
                write!(f, "{}", t.format(TIMESTAMP_FORMAT))
            } else {
                write!(f, "'{}'", t.format(TIMESTAMP_FORMAT))
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
                encode_value(f, val, false)?;
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
                encode_value(f, key, false)?;
                write!(f, ":")?;
                encode_value(f, val, false)?;
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
                encode_value(f, val, false)?;
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
        Value::TimestampTz(dt) => {
            let formatted = dt.format(TIMESTAMP_TIMEZONE_FORMAT);
            if raw {
                write!(f, "{formatted}")
            } else {
                write!(f, "'{formatted}'")
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
        write!(buf, "{num}").unwrap();
    } else {
        let pow_scale = i256::from_i128(10i128).wrapping_pow(scale as u32);
        let width = scale as usize;
        // -1/10 = 0
        let (int_part, neg) = if num >= i256::ZERO {
            (num / pow_scale, "")
        } else {
            (-num / pow_scale, "-")
        };
        let frac_part = (num % pow_scale).wrapping_abs();

        match frac_part.to_i128() {
            Some(frac_part) => {
                write!(buf, "{neg}{int_part}.{frac_part:0>width$}").unwrap();
            }
            None => {
                // fractional part is too big for display,
                // split it into two parts.
                let pow = i256::from_i128(10i128).wrapping_pow(38);
                let frac_high_part = frac_part / pow;
                let frac_low_part = frac_part % pow;
                let frac_width = (scale - 38) as usize;

                write!(
                    buf,
                    "{neg}{int_part}.{:0>frac_width$}{}",
                    frac_high_part.to_i128().unwrap(),
                    frac_low_part.to_i128().unwrap(),
                )
                .unwrap();
            }
        }
    }
    buf
}

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
            Value::Timestamp(ts, tz) => {
                // TODO: use ts directly?
                let dt = DateTime::from_timestamp_micros(*ts).unwrap();
                let dt = dt.with_timezone(tz);
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
