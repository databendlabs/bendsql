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
use crate::value::format::display::{display_decimal_128, display_decimal_256};
use crate::value::NumberValue;
use chrono::NaiveDate;
use hex;
use lexical_core::{ToLexical, ToLexicalWithOptions, WriteFloatOptions};

pub struct FormatOptions {
    pub true_string: &'static [u8],
    pub false_string: &'static [u8],
    pub float_options: WriteFloatOptions,
}

// pub static CLI_OPTIONS: FormatOptions = FormatOptions {
//     true_string: b"true",
//     false_string: b"false",
//     float_options: WriteFloatOptionsBuilder::new()
//         .nan_string(Some(b"NaN"))
//         .nan_string(Some(b"inf"))
//         .trim_floats(true)
//         .build_unchecked(),
// };

impl Value {
    pub fn to_string_with_options(&self, format_options: &FormatOptions) -> String {
        let mut buf = Vec::<u8>::with_capacity(100);
        self.write_with_option(&mut buf, true, format_options);
        // todo: optimize for directly string
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn write_with_option(&self, bytes: &mut Vec<u8>, raw: bool, format_options: &FormatOptions) {
        match self {
            Value::Null => bytes.extend_from_slice("NULL".as_bytes()),
            Value::EmptyArray => bytes.extend_from_slice("[]".as_bytes()),
            Value::EmptyMap => bytes.extend_from_slice("{}".as_bytes()),
            Value::Boolean(b) => {
                if *b {
                    bytes.extend_from_slice(format_options.true_string)
                } else {
                    bytes.extend_from_slice(format_options.false_string)
                }
            }
            Value::Number(n) => match n {
                NumberValue::Int8(v) => Self::write_int(bytes, *v),
                NumberValue::Int16(v) => Self::write_int(bytes, *v),
                NumberValue::Int32(v) => Self::write_int(bytes, *v),
                NumberValue::Int64(v) => Self::write_int(bytes, *v),
                NumberValue::UInt8(v) => Self::write_int(bytes, *v),
                NumberValue::UInt16(v) => Self::write_int(bytes, *v),
                NumberValue::UInt32(v) => Self::write_int(bytes, *v),
                NumberValue::UInt64(v) => Self::write_int(bytes, *v),
                NumberValue::Float32(v) => Self::write_float(bytes, *v, format_options),
                NumberValue::Float64(v) => Self::write_float(bytes, *v, format_options),
                NumberValue::Decimal128(v, size) => {
                    let s = display_decimal_128(*v, size.scale);
                    Self::write_string(bytes, &s, true);
                }
                NumberValue::Decimal256(v, size) => {
                    let s = display_decimal_256(*v, size.scale);
                    Self::write_string(bytes, &s, true);
                }
            },
            Value::Binary(s) => bytes.extend_from_slice(hex::encode_upper(s).as_bytes()),
            Value::String(s)
            | Value::Bitmap(s)
            | Value::Variant(s)
            | Value::Interval(s)
            | Value::Geometry(s)
            | Value::Geography(s) => {
                Self::write_string(bytes, s, raw);
            }
            Value::Timestamp(dt) => {
                let s = format!("{}", dt.format(TIMESTAMP_FORMAT));
                Self::write_string(bytes, &s, raw);
            }
            Value::TimestampTz(dt) => {
                let formatted = dt.format(TIMESTAMP_TIMEZONE_FORMAT);
                let s = format!("{}", formatted);
                Self::write_string(bytes, &s, raw);
            }
            Value::Date(i) => {
                let days = i + DAYS_FROM_CE;
                let d = NaiveDate::from_num_days_from_ce_opt(days).unwrap_or_default();
                let s = format!("{}", d);
                Self::write_string(bytes, &s, raw);
            }
            Value::Array(vals) => {
                bytes.push(b'[');
                for (i, val) in vals.iter().enumerate() {
                    if i > 0 {
                        bytes.push(b',');
                    }
                    val.write_with_option(bytes, false, format_options);
                }
                bytes.push(b']');
            }
            Value::Map(kvs) => {
                bytes.push(b'{');
                for (i, (key, val)) in kvs.iter().enumerate() {
                    if i > 0 {
                        bytes.push(b',');
                    }
                    key.write_with_option(bytes, false, format_options);
                    bytes.push(b':');
                    val.write_with_option(bytes, false, format_options);
                }
                bytes.push(b'}');
            }
            Value::Tuple(vals) => {
                bytes.push(b'(');
                for (i, val) in vals.iter().enumerate() {
                    if i > 0 {
                        bytes.push(b',');
                    }
                    val.write_with_option(bytes, false, format_options);
                }
                bytes.push(b')');
            }
            Value::Vector(vals) => {
                bytes.push(b'[');
                for (i, val) in vals.iter().enumerate() {
                    if i > 0 {
                        bytes.push(b',');
                    }
                    Self::write_float(bytes, *val, format_options);
                }
                bytes.push(b']');
            }
        }
    }

    fn write_string(bytes: &mut Vec<u8>, string: &String, raw: bool) {
        if !raw {
            bytes.push(b'\'');
        }
        bytes.extend_from_slice(string.as_bytes());
        if !raw {
            bytes.push(b'\'');
        }
    }

    fn write_float<T: ToLexicalWithOptions<Options = WriteFloatOptions>>(
        out_buf: &mut Vec<u8>,
        v: T,
        options: &FormatOptions,
    ) {
        out_buf.reserve(T::FORMATTED_SIZE_DECIMAL);
        let len0 = out_buf.len();
        unsafe {
            let slice = std::slice::from_raw_parts_mut(
                out_buf.as_mut_ptr().add(len0),
                out_buf.capacity() - len0,
            );
            let len = v
                .to_lexical_with_options::<{ lexical_core::format::STANDARD }>(
                    slice,
                    &options.float_options,
                )
                .len();
            out_buf.set_len(len0 + len);
        }
    }

    fn write_int<T: ToLexical>(out_buf: &mut Vec<u8>, v: T) {
        out_buf.reserve(T::FORMATTED_SIZE_DECIMAL);
        let len0 = out_buf.len();
        unsafe {
            let slice = std::slice::from_raw_parts_mut(
                out_buf.as_mut_ptr().add(len0),
                out_buf.capacity() - len0,
            );
            let len = v.to_lexical(slice).len();
            out_buf.set_len(len0 + len);
        }
    }
}
