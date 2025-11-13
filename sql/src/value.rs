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

use crate::cursor_ext::{
    collect_binary_number, collect_number, BufferReadStringExt, ReadBytesExt, ReadCheckPointExt,
    ReadNumberExt,
};
use crate::error::{ConvertError, Error, Result};
use arrow_buffer::i256;
use chrono::{DateTime, Datelike, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use databend_client::schema::{DataType, DecimalDataType, DecimalSize, NumberDataType};
use geozero::wkb::FromWkb;
use geozero::wkb::WkbDialect;
use geozero::wkt::Ewkt;
use hex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write};
use std::hash::Hash;
use std::io::BufRead;
use std::io::Cursor;

use {
    arrow_array::{
        Array as ArrowArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        Decimal256Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, StringArray,
        StringViewArray, StructArray, TimestampMicrosecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    arrow_schema::{DataType as ArrowDataType, Field as ArrowField, TimeUnit},
    databend_client::schema::{
        ARROW_EXT_TYPE_BITMAP, ARROW_EXT_TYPE_EMPTY_ARRAY, ARROW_EXT_TYPE_EMPTY_MAP,
        ARROW_EXT_TYPE_GEOGRAPHY, ARROW_EXT_TYPE_GEOMETRY, ARROW_EXT_TYPE_INTERVAL,
        ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE, ARROW_EXT_TYPE_VARIANT, ARROW_EXT_TYPE_VECTOR,
        EXTENSION_KEY,
    },
    jsonb::RawJsonb,
    std::sync::Arc,
};

// Thu 1970-01-01 is R.D. 719163
const DAYS_FROM_CE: i32 = 719_163;
const NULL_VALUE: &str = "NULL";
const TRUE_VALUE: &str = "1";
const FALSE_VALUE: &str = "0";
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
const TIMESTAMP_TIMEZONE_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f %z";

#[derive(Clone, Debug, PartialEq)]
pub enum NumberValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal128(i128, DecimalSize),
    Decimal256(i256, DecimalSize),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean(bool),
    Binary(Vec<u8>),
    String(String),
    Number(NumberValue),
    /// Microseconds from 1970-01-01 00:00:00 UTC
    Timestamp(i64, Tz),
    TimestampTz(DateTime<FixedOffset>),
    Date(i32),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Tuple(Vec<Value>),
    Bitmap(String),
    Variant(String),
    Geometry(String),
    Geography(String),
    Interval(String),
    Vector(Vec<f32>),
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::EmptyArray => DataType::EmptyArray,
            Self::EmptyMap => DataType::EmptyMap,
            Self::Boolean(_) => DataType::Boolean,
            Self::Binary(_) => DataType::Binary,
            Self::String(_) => DataType::String,
            Self::Number(n) => match n {
                NumberValue::Int8(_) => DataType::Number(NumberDataType::Int8),
                NumberValue::Int16(_) => DataType::Number(NumberDataType::Int16),
                NumberValue::Int32(_) => DataType::Number(NumberDataType::Int32),
                NumberValue::Int64(_) => DataType::Number(NumberDataType::Int64),
                NumberValue::UInt8(_) => DataType::Number(NumberDataType::UInt8),
                NumberValue::UInt16(_) => DataType::Number(NumberDataType::UInt16),
                NumberValue::UInt32(_) => DataType::Number(NumberDataType::UInt32),
                NumberValue::UInt64(_) => DataType::Number(NumberDataType::UInt64),
                NumberValue::Float32(_) => DataType::Number(NumberDataType::Float32),
                NumberValue::Float64(_) => DataType::Number(NumberDataType::Float64),
                NumberValue::Decimal128(_, s) => DataType::Decimal(DecimalDataType::Decimal128(*s)),
                NumberValue::Decimal256(_, s) => DataType::Decimal(DecimalDataType::Decimal256(*s)),
            },
            Self::Timestamp(_, _) => DataType::Timestamp,
            Self::TimestampTz(_) => DataType::TimestampTz,

            Self::Date(_) => DataType::Date,
            Self::Interval(_) => DataType::Interval,
            Self::Array(vals) => {
                if vals.is_empty() {
                    DataType::EmptyArray
                } else {
                    DataType::Array(Box::new(vals[0].get_type()))
                }
            }
            Self::Map(kvs) => {
                if kvs.is_empty() {
                    DataType::EmptyMap
                } else {
                    let inner_ty = DataType::Tuple(vec![kvs[0].0.get_type(), kvs[0].1.get_type()]);
                    DataType::Map(Box::new(inner_ty))
                }
            }
            Self::Tuple(vals) => {
                let inner_tys = vals.iter().map(|v| v.get_type()).collect::<Vec<_>>();
                DataType::Tuple(inner_tys)
            }
            Self::Bitmap(_) => DataType::Bitmap,
            Self::Variant(_) => DataType::Variant,
            Self::Geometry(_) => DataType::Geometry,
            Self::Geography(_) => DataType::Geography,
            Self::Vector(v) => DataType::Vector(v.len() as u64),
        }
    }
}

impl TryFrom<(&DataType, Option<String>, Tz)> for Value {
    type Error = Error;

    fn try_from((t, v, tz): (&DataType, Option<String>, Tz)) -> Result<Self> {
        match v {
            Some(v) => Self::try_from((t, v, tz)),
            None => match t {
                DataType::Null => Ok(Self::Null),
                DataType::Nullable(_) => Ok(Self::Null),
                _ => Err(Error::InvalidResponse(
                    "NULL value for non-nullable field".to_string(),
                )),
            },
        }
    }
}

impl TryFrom<(&DataType, String, Tz)> for Value {
    type Error = Error;

    fn try_from((t, v, tz): (&DataType, String, Tz)) -> Result<Self> {
        match t {
            DataType::Null => Ok(Self::Null),
            DataType::EmptyArray => Ok(Self::EmptyArray),
            DataType::EmptyMap => Ok(Self::EmptyMap),
            DataType::Boolean => Ok(Self::Boolean(v == "1")),
            DataType::Binary => Ok(Self::Binary(hex::decode(v)?)),
            DataType::String => Ok(Self::String(v)),
            DataType::Number(NumberDataType::Int8) => {
                Ok(Self::Number(NumberValue::Int8(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int16) => {
                Ok(Self::Number(NumberValue::Int16(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int32) => {
                Ok(Self::Number(NumberValue::Int32(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int64) => {
                Ok(Self::Number(NumberValue::Int64(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt8) => {
                Ok(Self::Number(NumberValue::UInt8(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt16) => {
                Ok(Self::Number(NumberValue::UInt16(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt32) => {
                Ok(Self::Number(NumberValue::UInt32(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt64) => {
                Ok(Self::Number(NumberValue::UInt64(v.parse()?)))
            }
            DataType::Number(NumberDataType::Float32) => {
                Ok(Self::Number(NumberValue::Float32(v.parse()?)))
            }
            DataType::Number(NumberDataType::Float64) => {
                Ok(Self::Number(NumberValue::Float64(v.parse()?)))
            }
            DataType::Decimal(DecimalDataType::Decimal128(size)) => {
                let d = parse_decimal(v.as_str(), *size)?;
                Ok(Self::Number(d))
            }
            DataType::Decimal(DecimalDataType::Decimal256(size)) => {
                let d = parse_decimal(v.as_str(), *size)?;
                Ok(Self::Number(d))
            }
            DataType::Timestamp => {
                let naive_dt = NaiveDateTime::parse_from_str(v.as_str(), "%Y-%m-%d %H:%M:%S%.6f")?;
                let dt_with_tz = match tz.from_local_datetime(&naive_dt) {
                    LocalResult::Single(dt) => dt,
                    LocalResult::None => {
                        return Err(Error::Parsing(format!(
                            "time {v} not exists in timezone {tz}"
                        )))
                    }
                    LocalResult::Ambiguous(dt1, _dt2) => dt1,
                };
                let ts = dt_with_tz.timestamp_micros();
                Ok(Self::Timestamp(ts, tz))
            }
            DataType::TimestampTz => {
                let t =
                    DateTime::<FixedOffset>::parse_from_str(v.as_str(), TIMESTAMP_TIMEZONE_FORMAT)?;
                Ok(Self::TimestampTz(t))
            }
            DataType::Date => Ok(Self::Date(
                NaiveDate::parse_from_str(v.as_str(), "%Y-%m-%d")?.num_days_from_ce()
                    - DAYS_FROM_CE,
            )),
            DataType::Bitmap => Ok(Self::Bitmap(v)),
            DataType::Variant => Ok(Self::Variant(v)),
            DataType::Geometry => Ok(Self::Geometry(v)),
            DataType::Geography => Ok(Self::Geography(v)),
            DataType::Interval => Ok(Self::Interval(v)),
            DataType::Array(_) | DataType::Map(_) | DataType::Tuple(_) | DataType::Vector(_) => {
                let mut reader = Cursor::new(v.as_str());
                let decoder = ValueDecoder {};
                decoder.read_field(t, &mut reader)
            }
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::String => Ok(Self::String(v.to_string())),
                _ => {
                    // not string type, try to check if it is NULL
                    // for compatible with old version server
                    if v == NULL_VALUE {
                        Ok(Self::Null)
                    } else {
                        Self::try_from((inner.as_ref(), v, tz))
                    }
                }
            },
        }
    }
}

impl TryFrom<(&ArrowField, &Arc<dyn ArrowArray>, usize, Tz)> for Value {
    type Error = Error;
    fn try_from(
        (field, array, seq, ltz): (&ArrowField, &Arc<dyn ArrowArray>, usize, Tz),
    ) -> std::result::Result<Self, Self::Error> {
        if let Some(extend_type) = field.metadata().get(EXTENSION_KEY) {
            return match extend_type.as_str() {
                ARROW_EXT_TYPE_EMPTY_ARRAY => Ok(Value::EmptyArray),
                ARROW_EXT_TYPE_EMPTY_MAP => Ok(Value::EmptyMap),
                ARROW_EXT_TYPE_VARIANT => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => {
                            Ok(Value::Variant(RawJsonb::new(array.value(seq)).to_string()))
                        }
                        None => Err(ConvertError::new("variant", format!("{array:?}")).into()),
                    }
                }
                ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match array.as_any().downcast_ref::<Decimal128Array>() {
                        Some(array) => {
                            let v = array.value(seq);
                            let ts = v as u64 as i64;
                            let offset = (v >> 64) as i32;

                            let secs = ts / 1_000_000;
                            let nanos = ((ts % 1_000_000) * 1000) as u32;
                            let dt = match DateTime::from_timestamp(secs, nanos) {
                                Some(t) => {
                                    let off = FixedOffset::east_opt(offset).ok_or_else(|| {
                                        Error::Parsing("invalid offset".to_string())
                                    })?;
                                    t.with_timezone(&off)
                                }
                                None => {
                                    return Err(ConvertError::new("Datetime", format!("{v}")).into())
                                }
                            };
                            Ok(Value::TimestampTz(dt))
                        }
                        None => Err(ConvertError::new("Interval", format!("{array:?}")).into()),
                    }
                }
                ARROW_EXT_TYPE_INTERVAL => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match array.as_any().downcast_ref::<Decimal128Array>() {
                        Some(array) => {
                            let res = months_days_micros(array.value(seq));
                            Ok(Value::Interval(
                                Interval {
                                    months: res.months(),
                                    days: res.days(),
                                    micros: res.microseconds(),
                                }
                                .to_string(),
                            ))
                        }
                        None => Err(ConvertError::new("Interval", format!("{array:?}")).into()),
                    }
                }
                ARROW_EXT_TYPE_BITMAP => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => {
                            let rb = roaring::RoaringTreemap::deserialize_from(array.value(seq))
                                .expect("failed to deserialize bitmap");
                            let raw = rb.into_iter().collect::<Vec<_>>();
                            let s = itertools::join(raw.iter(), ",");
                            Ok(Value::Bitmap(s))
                        }
                        None => Err(ConvertError::new("bitmap", format!("{array:?}")).into()),
                    }
                }
                ARROW_EXT_TYPE_GEOMETRY => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => {
                            let wkt = parse_geometry(array.value(seq))?;
                            Ok(Value::Geometry(wkt))
                        }
                        None => Err(ConvertError::new("geometry", format!("{array:?}")).into()),
                    }
                }
                ARROW_EXT_TYPE_GEOGRAPHY => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => {
                            let wkt = parse_geometry(array.value(seq))?;
                            Ok(Value::Geography(wkt))
                        }
                        None => Err(ConvertError::new("geography", format!("{array:?}")).into()),
                    }
                }
                ARROW_EXT_TYPE_VECTOR => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    match field.data_type() {
                        ArrowDataType::FixedSizeList(_, dimension) => {
                            match array
                                .as_any()
                                .downcast_ref::<arrow_array::FixedSizeListArray>()
                            {
                                Some(inner_array) => {
                                    match inner_array
                                        .value(seq)
                                        .as_any()
                                        .downcast_ref::<Float32Array>()
                                    {
                                        Some(inner_array) => {
                                            let dimension = *dimension as usize;
                                            let mut values = Vec::with_capacity(dimension);
                                            for i in 0..dimension {
                                                let value = inner_array.value(i);
                                                values.push(value);
                                            }
                                            Ok(Value::Vector(values))
                                        }
                                        None => Err(ConvertError::new(
                                            "vector float32",
                                            format!("{inner_array:?}"),
                                        )
                                        .into()),
                                    }
                                }
                                None => {
                                    Err(ConvertError::new("vector", format!("{array:?}")).into())
                                }
                            }
                        }
                        arrow_type => Err(ConvertError::new(
                            "vector",
                            format!("Unsupported Arrow type: {arrow_type:?}"),
                        )
                        .into()),
                    }
                }
                _ => Err(ConvertError::new(
                    "extension",
                    format!("Unsupported extension datatype for arrow field: {field:?}"),
                )
                .into()),
            };
        }

        if field.is_nullable() && array.is_null(seq) {
            return Ok(Value::Null);
        }
        match field.data_type() {
            ArrowDataType::Null => Ok(Value::Null),
            ArrowDataType::Boolean => match array.as_any().downcast_ref::<BooleanArray>() {
                Some(array) => Ok(Value::Boolean(array.value(seq))),
                None => Err(ConvertError::new("bool", format!("{array:?}")).into()),
            },
            ArrowDataType::Int8 => match array.as_any().downcast_ref::<Int8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int8(array.value(seq)))),
                None => Err(ConvertError::new("int8", format!("{array:?}")).into()),
            },
            ArrowDataType::Int16 => match array.as_any().downcast_ref::<Int16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int16(array.value(seq)))),
                None => Err(ConvertError::new("int16", format!("{array:?}")).into()),
            },
            ArrowDataType::Int32 => match array.as_any().downcast_ref::<Int32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int32(array.value(seq)))),
                None => Err(ConvertError::new("int64", format!("{array:?}")).into()),
            },
            ArrowDataType::Int64 => match array.as_any().downcast_ref::<Int64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int64(array.value(seq)))),
                None => Err(ConvertError::new("int64", format!("{array:?}")).into()),
            },
            ArrowDataType::UInt8 => match array.as_any().downcast_ref::<UInt8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt8(array.value(seq)))),
                None => Err(ConvertError::new("uint8", format!("{array:?}")).into()),
            },
            ArrowDataType::UInt16 => match array.as_any().downcast_ref::<UInt16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt16(array.value(seq)))),
                None => Err(ConvertError::new("uint16", format!("{array:?}")).into()),
            },
            ArrowDataType::UInt32 => match array.as_any().downcast_ref::<UInt32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt32(array.value(seq)))),
                None => Err(ConvertError::new("uint32", format!("{array:?}")).into()),
            },
            ArrowDataType::UInt64 => match array.as_any().downcast_ref::<UInt64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt64(array.value(seq)))),
                None => Err(ConvertError::new("uint64", format!("{array:?}")).into()),
            },
            ArrowDataType::Float32 => match array.as_any().downcast_ref::<Float32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float32(array.value(seq)))),
                None => Err(ConvertError::new("float32", format!("{array:?}")).into()),
            },
            ArrowDataType::Float64 => match array.as_any().downcast_ref::<Float64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float64(array.value(seq)))),
                None => Err(ConvertError::new("float64", format!("{array:?}")).into()),
            },

            ArrowDataType::Decimal128(p, s) => {
                match array.as_any().downcast_ref::<Decimal128Array>() {
                    Some(array) => Ok(Value::Number(NumberValue::Decimal128(
                        array.value(seq),
                        DecimalSize {
                            precision: *p,
                            scale: *s as u8,
                        },
                    ))),
                    None => Err(ConvertError::new("Decimal128", format!("{array:?}")).into()),
                }
            }
            ArrowDataType::Decimal256(p, s) => {
                match array.as_any().downcast_ref::<Decimal256Array>() {
                    Some(array) => Ok(Value::Number(NumberValue::Decimal256(
                        array.value(seq),
                        DecimalSize {
                            precision: *p,
                            scale: *s as u8,
                        },
                    ))),
                    None => Err(ConvertError::new("Decimal256", format!("{array:?}")).into()),
                }
            }

            ArrowDataType::Binary => match array.as_any().downcast_ref::<BinaryArray>() {
                Some(array) => Ok(Value::Binary(array.value(seq).to_vec())),
                None => Err(ConvertError::new("binary", format!("{array:?}")).into()),
            },
            ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
                match array.as_any().downcast_ref::<LargeBinaryArray>() {
                    Some(array) => Ok(Value::Binary(array.value(seq).to_vec())),
                    None => Err(ConvertError::new("large binary", format!("{array:?}")).into()),
                }
            }
            ArrowDataType::Utf8 => match array.as_any().downcast_ref::<StringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("string", format!("{array:?}")).into()),
            },
            ArrowDataType::LargeUtf8 => match array.as_any().downcast_ref::<LargeStringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("large string", format!("{array:?}")).into()),
            },
            ArrowDataType::Utf8View => match array.as_any().downcast_ref::<StringViewArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("string view", format!("{array:?}")).into()),
            },
            // we only support timestamp in microsecond in databend
            ArrowDataType::Timestamp(unit, tz) => {
                match array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    Some(array) => {
                        if unit != &TimeUnit::Microsecond {
                            return Err(ConvertError::new("timestamp", format!("{array:?}"))
                                .with_message(format!(
                                    "unsupported timestamp unit: {unit:?}, only support microsecond"
                                ))
                                .into());
                        }
                        let ts = array.value(seq);
                        match tz {
                            None => Ok(Value::Timestamp(ts, ltz)),
                            Some(tz) => Err(ConvertError::new("timestamp", format!("{array:?}"))
                                .with_message(format!("non-UTC timezone not supported: {tz:?}"))
                                .into()),
                        }
                    }
                    None => Err(ConvertError::new("timestamp", format!("{array:?}")).into()),
                }
            }
            ArrowDataType::Date32 => match array.as_any().downcast_ref::<Date32Array>() {
                Some(array) => Ok(Value::Date(array.value(seq))),
                None => Err(ConvertError::new("date", format!("{array:?}")).into()),
            },
            ArrowDataType::List(f) => match array.as_any().downcast_ref::<ListArray>() {
                Some(array) => {
                    let inner_array = unsafe { array.value_unchecked(seq) };
                    let mut values = Vec::with_capacity(inner_array.len());
                    for i in 0..inner_array.len() {
                        let value = Value::try_from((f.as_ref(), &inner_array, i, ltz))?;
                        values.push(value);
                    }
                    Ok(Value::Array(values))
                }
                None => Err(ConvertError::new("list", format!("{array:?}")).into()),
            },
            ArrowDataType::LargeList(f) => match array.as_any().downcast_ref::<LargeListArray>() {
                Some(array) => {
                    let inner_array = unsafe { array.value_unchecked(seq) };
                    let mut values = Vec::with_capacity(inner_array.len());
                    for i in 0..inner_array.len() {
                        let value = Value::try_from((f.as_ref(), &inner_array, i, ltz))?;
                        values.push(value);
                    }
                    Ok(Value::Array(values))
                }
                None => Err(ConvertError::new("large list", format!("{array:?}")).into()),
            },
            ArrowDataType::Map(f, _) => match array.as_any().downcast_ref::<MapArray>() {
                Some(array) => {
                    if let ArrowDataType::Struct(fs) = f.data_type() {
                        let inner_array = unsafe { array.value_unchecked(seq) };
                        let mut values = Vec::with_capacity(inner_array.len());
                        for i in 0..inner_array.len() {
                            let key =
                                Value::try_from((fs[0].as_ref(), inner_array.column(0), i, ltz))?;
                            let val =
                                Value::try_from((fs[1].as_ref(), inner_array.column(1), i, ltz))?;
                            values.push((key, val));
                        }
                        Ok(Value::Map(values))
                    } else {
                        Err(
                            ConvertError::new("invalid map inner type", format!("{array:?}"))
                                .into(),
                        )
                    }
                }
                None => Err(ConvertError::new("map", format!("{array:?}")).into()),
            },
            ArrowDataType::Struct(fs) => match array.as_any().downcast_ref::<StructArray>() {
                Some(array) => {
                    let mut values = Vec::with_capacity(array.len());
                    for (f, inner_array) in fs.iter().zip(array.columns().iter()) {
                        let value = Value::try_from((f.as_ref(), inner_array, seq, ltz))?;
                        values.push(value);
                    }
                    Ok(Value::Tuple(values))
                }
                None => Err(ConvertError::new("struct", format!("{array:?}")).into()),
            },
            _ => Err(ConvertError::new("unsupported data type", format!("{array:?}")).into()),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
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
            Value::Timestamp(ts, tz) => {
                let dt = DateTime::from_timestamp_micros(ts).ok_or_else(|| {
                    ConvertError::new("timestamp", format!("invalid timestamp: {}", ts))
                })?;
                let dt = dt.with_timezone(&tz);
                Ok(dt.format(TIMESTAMP_FORMAT).to_string())
            }
            _ => Err(ConvertError::new("string", format!("{val:?}")).into()),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Boolean(b) => Ok(b),
            Value::Number(n) => Ok(n != NumberValue::Int8(0)),
            _ => Err(ConvertError::new("bool", format!("{val:?}")).into()),
        }
    }
}

// This macro implements TryFrom for NumberValue
macro_rules! impl_try_from_number_value {
    ($($t:ty),*) => {
        $(
            impl TryFrom<Value> for $t {
                type Error = Error;
                fn try_from(val: Value) -> Result<Self> {
                    match val {
                        Value::Number(NumberValue::Int8(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int16(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int64(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt8(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt16(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt64(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Float32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Float64(i)) => Ok(i as $t),
                        Value::Date(i) => Ok(i as $t),
                        Value::Timestamp(i, _) => Ok(i as $t),
                        _ => Err(ConvertError::new("number", format!("{:?}", val)).into()),
                    }
                }
            }
        )*
    };
}

impl_try_from_number_value!(u8);
impl_try_from_number_value!(u16);
impl_try_from_number_value!(u32);
impl_try_from_number_value!(u64);
impl_try_from_number_value!(i8);
impl_try_from_number_value!(i16);
impl_try_from_number_value!(i32);
impl_try_from_number_value!(i64);
impl_try_from_number_value!(f32);
impl_try_from_number_value!(f64);

impl TryFrom<Value> for NaiveDateTime {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Timestamp(i, _tz) => {
                let secs = i / 1_000_000;
                let nanos = ((i % 1_000_000) * 1000) as u32;
                match DateTime::from_timestamp(secs, nanos) {
                    Some(t) => Ok(t.naive_utc()),
                    None => Err(ConvertError::new("NaiveDateTime", format!("{val}")).into()),
                }
            }
            _ => Err(ConvertError::new("NaiveDateTime", format!("{val}")).into()),
        }
    }
}

impl TryFrom<Value> for DateTime<Tz> {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Timestamp(i, tz) => {
                let secs = i / 1_000_000;
                let nanos = ((i % 1_000_000) * 1000) as u32;
                match DateTime::from_timestamp(secs, nanos) {
                    Some(t) => Ok(tz.from_utc_datetime(&t.naive_utc())),
                    None => Err(ConvertError::new("Datetime", format!("{val}")).into()),
                }
            }
            _ => Err(ConvertError::new("DateTime", format!("{val}")).into()),
        }
    }
}

impl TryFrom<Value> for NaiveDate {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Date(i) => {
                let days = i + DAYS_FROM_CE;
                match NaiveDate::from_num_days_from_ce_opt(days) {
                    Some(d) => Ok(d),
                    None => Err(ConvertError::new("NaiveDate", "".to_string()).into()),
                }
            }
            _ => Err(ConvertError::new("NaiveDate", format!("{val}")).into()),
        }
    }
}

impl<V> TryFrom<Value> for Vec<V>
where
    V: TryFrom<Value, Error = Error>,
{
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Binary(vals) => vals
                .into_iter()
                .map(|v| V::try_from(Value::Number(NumberValue::UInt8(v))))
                .collect(),
            Value::Array(vals) => vals.into_iter().map(V::try_from).collect(),
            Value::EmptyArray => Ok(vec![]),
            _ => Err(ConvertError::new("Vec", format!("{val}")).into()),
        }
    }
}

impl<K, V> TryFrom<Value> for HashMap<K, V>
where
    K: TryFrom<Value, Error = Error> + Eq + Hash,
    V: TryFrom<Value, Error = Error>,
{
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Map(kvs) => {
                let mut map = HashMap::new();
                for (k, v) in kvs {
                    let k = K::try_from(k)?;
                    let v = V::try_from(v)?;
                    map.insert(k, v);
                }
                Ok(map)
            }
            Value::EmptyMap => Ok(HashMap::new()),
            _ => Err(ConvertError::new("HashMap", format!("{val}")).into()),
        }
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

// This macro implements TryFrom for tuple of types
macro_rules! impl_tuple_from_value {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> TryFrom<Value> for ($($Ti,)+)
        where
            $($Ti: TryFrom<Value>),+
        {
            type Error = String;
            fn try_from(val: Value) -> Result<Self, String> {
                // It is not possible yet to get the number of metavariable repetitions
                // ref: https://github.com/rust-lang/lang-team/issues/28#issue-644523674
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);

                match val {
                    Value::Tuple(vals) => {
                        if expected_len != vals.len() {
                            return Err(format!("value tuple size mismatch: expected {} columns, got {}", expected_len, vals.len()));
                        }
                        let mut vals_iter = vals.into_iter().enumerate();

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
                    _ => Err(format!("expected tuple, got {:?}", val)),
                }
            }
        }
    }
}

// Implement From Value for tuples of size up to 16
impl_tuple_from_value!(T1);
impl_tuple_from_value!(T1, T2);
impl_tuple_from_value!(T1, T2, T3);
impl_tuple_from_value!(T1, T2, T3, T4);
impl_tuple_from_value!(T1, T2, T3, T4, T5);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
impl_tuple_from_value!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17);
impl_tuple_from_value!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18
);
impl_tuple_from_value!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19
);
impl_tuple_from_value!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20
);
impl_tuple_from_value!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21
);
impl_tuple_from_value!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21,
    T22
);

// This macro implements TryFrom to Option for Nullable column
macro_rules! impl_try_from_to_option {
    ($($t:ty),*) => {
        $(
            impl TryFrom<Value> for Option<$t> {
                type Error = Error;
                fn try_from(val: Value) -> Result<Self> {
                    match val {
                        Value::Null => Ok(None),
                        _ => {
                            let inner: $t = val.try_into()?;
                            Ok(Some(inner))
                        },
                    }

                }
            }
        )*
    };
}

impl_try_from_to_option!(String);
impl_try_from_to_option!(bool);
impl_try_from_to_option!(u8);
impl_try_from_to_option!(u16);
impl_try_from_to_option!(u32);
impl_try_from_to_option!(u64);
impl_try_from_to_option!(i8);
impl_try_from_to_option!(i16);
impl_try_from_to_option!(i32);
impl_try_from_to_option!(i64);
impl_try_from_to_option!(f32);
impl_try_from_to_option!(f64);
impl_try_from_to_option!(NaiveDateTime);
impl_try_from_to_option!(NaiveDate);

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

// Compatible with Databend, inner values of nested types are quoted.
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

/// assume text is from
/// used only for expr, so put more weight on readability
pub fn parse_decimal(text: &str, size: DecimalSize) -> Result<NumberValue> {
    let mut start = 0;
    let bytes = text.as_bytes();
    let mut is_negative = false;

    // Check if the number is negative
    if bytes[start] == b'-' {
        is_negative = true;
        start += 1;
    }

    while start < text.len() && bytes[start] == b'0' {
        start += 1
    }
    let text = &text[start..];
    let point_pos = text.find('.');
    let e_pos = text.find(|c| ['E', 'e'].contains(&c));
    let (i_part, f_part, e_part) = match (point_pos, e_pos) {
        (Some(p1), Some(p2)) => (&text[..p1], &text[(p1 + 1)..p2], Some(&text[(p2 + 1)..])),
        (Some(p), None) => (&text[..p], &text[(p + 1)..], None),
        (None, Some(p)) => (&text[..p], "", Some(&text[(p + 1)..])),
        (None, None) => (text, "", None),
    };
    let exp = match e_part {
        Some(s) => s.parse::<i32>()?,
        None => 0,
    };
    if i_part.len() as i32 + exp > 76 {
        Err(ConvertError::new("decimal", format!("{text:?}")).into())
    } else {
        let mut digits = Vec::with_capacity(76);
        digits.extend_from_slice(i_part.as_bytes());
        digits.extend_from_slice(f_part.as_bytes());
        if digits.is_empty() {
            digits.push(b'0')
        }
        let scale = f_part.len() as i32 - exp;
        if scale < 0 {
            // e.g 123.1e3
            for _ in 0..(-scale) {
                digits.push(b'0')
            }
        };

        let precision = std::cmp::min(digits.len(), 76);
        let digits = unsafe { std::str::from_utf8_unchecked(&digits[..precision]) };

        let result = if size.precision > 38 {
            NumberValue::Decimal256(i256::from_string(digits).unwrap(), size)
        } else {
            NumberValue::Decimal128(digits.parse::<i128>()?, size)
        };

        // If the number was negative, negate the result
        if is_negative {
            match result {
                NumberValue::Decimal256(val, size) => Ok(NumberValue::Decimal256(-val, size)),
                NumberValue::Decimal128(val, size) => Ok(NumberValue::Decimal128(-val, size)),
                _ => Ok(result),
            }
        } else {
            Ok(result)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buffer = [0u8; 70];
        let len = IntervalToStringCast::format(*self, &mut buffer);
        write!(f, "{}", String::from_utf8_lossy(&buffer[..len]))
    }
}

struct IntervalToStringCast;

impl IntervalToStringCast {
    fn format_signed_number(value: i64, buffer: &mut [u8], length: &mut usize) {
        let s = value.to_string();
        let bytes = s.as_bytes();
        buffer[*length..*length + bytes.len()].copy_from_slice(bytes);
        *length += bytes.len();
    }

    fn format_two_digits(value: i64, buffer: &mut [u8], length: &mut usize) {
        let s = format!("{:02}", value.abs());
        let bytes = s.as_bytes();
        buffer[*length..*length + bytes.len()].copy_from_slice(bytes);
        *length += bytes.len();
    }

    fn format_interval_value(value: i32, buffer: &mut [u8], length: &mut usize, name: &str) {
        if value == 0 {
            return;
        }
        if *length != 0 {
            buffer[*length] = b' ';
            *length += 1;
        }
        Self::format_signed_number(value as i64, buffer, length);
        let name_bytes = name.as_bytes();
        buffer[*length..*length + name_bytes.len()].copy_from_slice(name_bytes);
        *length += name_bytes.len();
        if value != 1 && value != -1 {
            buffer[*length] = b's';
            *length += 1;
        }
    }

    fn format_micros(mut micros: i64, buffer: &mut [u8], length: &mut usize) {
        if micros < 0 {
            micros = -micros;
        }
        let s = format!("{micros:06}");
        let bytes = s.as_bytes();
        buffer[*length..*length + bytes.len()].copy_from_slice(bytes);
        *length += bytes.len();

        while *length > 0 && buffer[*length - 1] == b'0' {
            *length -= 1;
        }
    }

    pub fn format(interval: Interval, buffer: &mut [u8]) -> usize {
        let mut length = 0;
        if interval.months != 0 {
            let years = interval.months / 12;
            let months = interval.months - years * 12;
            Self::format_interval_value(years, buffer, &mut length, " year");
            Self::format_interval_value(months, buffer, &mut length, " month");
        }
        if interval.days != 0 {
            Self::format_interval_value(interval.days, buffer, &mut length, " day");
        }
        if interval.micros != 0 {
            if length != 0 {
                buffer[length] = b' ';
                length += 1;
            }
            let mut micros = interval.micros;
            if micros < 0 {
                buffer[length] = b'-';
                length += 1;
                micros = -micros;
            }
            let hour = micros / MINROS_PER_HOUR;
            micros -= hour * MINROS_PER_HOUR;
            let min = micros / MICROS_PER_MINUTE;
            micros -= min * MICROS_PER_MINUTE;
            let sec = micros / MICROS_PER_SEC;
            micros -= sec * MICROS_PER_SEC;

            Self::format_signed_number(hour, buffer, &mut length);
            buffer[length] = b':';
            length += 1;
            Self::format_two_digits(min, buffer, &mut length);
            buffer[length] = b':';
            length += 1;
            Self::format_two_digits(sec, buffer, &mut length);
            if micros != 0 {
                buffer[length] = b'.';
                length += 1;
                Self::format_micros(micros, buffer, &mut length);
            }
        } else if length == 0 {
            buffer[..8].copy_from_slice(b"00:00:00");
            return 8;
        }
        length
    }
}

impl Interval {
    pub fn from_string(str: &str) -> Result<Self> {
        Self::from_cstring(str.as_bytes())
    }

    pub fn from_cstring(str: &[u8]) -> Result<Self> {
        let mut result = Interval::default();
        let mut pos = 0;
        let len = str.len();
        let mut found_any = false;

        if len == 0 {
            return Err(Error::BadArgument("Empty string".to_string()));
        }
        match str[pos] {
            b'@' => {
                pos += 1;
            }
            b'P' | b'p' => {
                return Err(Error::BadArgument(
                    "Posix intervals not supported yet".to_string(),
                ));
            }
            _ => {}
        }

        while pos < len {
            match str[pos] {
                b' ' | b'\t' | b'\n' => {
                    pos += 1;
                    continue;
                }
                b'0'..=b'9' => {
                    let (number, fraction, next_pos) = parse_number(&str[pos..])?;
                    pos += next_pos;
                    let (specifier, next_pos) = parse_identifier(&str[pos..]);

                    pos += next_pos;
                    let _ = apply_specifier(&mut result, number, fraction, &specifier);
                    found_any = true;
                }
                b'-' => {
                    pos += 1;
                    let (number, fraction, next_pos) = parse_number(&str[pos..])?;
                    let number = -number;
                    let fraction = -fraction;

                    pos += next_pos;

                    let (specifier, next_pos) = parse_identifier(&str[pos..]);

                    pos += next_pos;
                    let _ = apply_specifier(&mut result, number, fraction, &specifier);
                    found_any = true;
                }
                b'a' | b'A' => {
                    if len - pos < 3
                        || str[pos + 1] != b'g' && str[pos + 1] != b'G'
                        || str[pos + 2] != b'o' && str[pos + 2] != b'O'
                    {
                        return Err(Error::BadArgument("Invalid 'ago' specifier".to_string()));
                    }
                    pos += 3;
                    while pos < len {
                        match str[pos] {
                            b' ' | b'\t' | b'\n' => {
                                pos += 1;
                            }
                            _ => {
                                return Err(Error::BadArgument(
                                    "Trailing characters after 'ago'".to_string(),
                                ));
                            }
                        }
                    }
                    result.months = -result.months;
                    result.days = -result.days;
                    result.micros = -result.micros;
                    return Ok(result);
                }
                _ => {
                    return Err(Error::BadArgument(format!(
                        "Unexpected character at position {pos}"
                    )));
                }
            }
        }

        if !found_any {
            return Err(Error::BadArgument(
                "No interval specifiers found".to_string(),
            ));
        }
        Ok(result)
    }
}

fn parse_number(bytes: &[u8]) -> Result<(i64, i64, usize)> {
    let mut number: i64 = 0;
    let mut fraction: i64 = 0;
    let mut pos = 0;

    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        number = number
            .checked_mul(10)
            .ok_or(Error::BadArgument("Number too large".to_string()))?
            + (bytes[pos] - b'0') as i64;
        pos += 1;
    }

    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let mut mult: i64 = 100000;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            if mult > 0 {
                fraction += (bytes[pos] - b'0') as i64 * mult;
            }
            mult /= 10;
            pos += 1;
        }
    }
    if pos < bytes.len() && bytes[pos] == b':' {
        // parse time format HH:MM:SS[.FFFFFF]
        let time_bytes = &bytes[pos..];
        let mut time_pos = 0;
        let mut total_micros: i64 = number * 60 * 60 * MICROS_PER_SEC;
        let mut colon_count = 0;

        while colon_count < 2 && time_bytes.len() > time_pos {
            let (minute, _, next_pos) = parse_time_part(&time_bytes[time_pos..])?;
            let minute_micros = minute * 60 * MICROS_PER_SEC;
            total_micros += minute_micros;
            time_pos += next_pos;

            if time_bytes.len() > time_pos && time_bytes[time_pos] == b':' {
                time_pos += 1;
                colon_count += 1;
            } else {
                break;
            }
        }
        if time_bytes.len() > time_pos {
            let (seconds, micros, next_pos) = parse_time_part_with_macros(&time_bytes[time_pos..])?;
            total_micros += seconds * MICROS_PER_SEC + micros;
            time_pos += next_pos;
        }
        return Ok((total_micros, 0, pos + time_pos));
    }

    if pos == 0 {
        return Err(Error::BadArgument("Expected number".to_string()));
    }

    Ok((number, fraction, pos))
}

fn parse_time_part(bytes: &[u8]) -> Result<(i64, i64, usize)> {
    let mut number: i64 = 0;
    let mut pos = 0;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        number = number
            .checked_mul(10)
            .ok_or(Error::BadArgument("Number too large".to_string()))?
            + (bytes[pos] - b'0') as i64;
        pos += 1;
    }
    Ok((number, 0, pos))
}

fn parse_time_part_with_macros(bytes: &[u8]) -> Result<(i64, i64, usize)> {
    let mut number: i64 = 0;
    let mut fraction: i64 = 0;
    let mut pos = 0;

    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        number = number
            .checked_mul(10)
            .ok_or(Error::BadArgument("Number too large".to_string()))?
            + (bytes[pos] - b'0') as i64;
        pos += 1;
    }

    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let mut mult: i64 = 100000;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            if mult > 0 {
                fraction += (bytes[pos] - b'0') as i64 * mult;
            }
            mult /= 10;
            pos += 1;
        }
    }

    Ok((number, fraction, pos))
}

fn parse_identifier(s: &[u8]) -> (String, usize) {
    let mut pos = 0;
    while pos < s.len() && (s[pos] == b' ' || s[pos] == b'\t' || s[pos] == b'\n') {
        pos += 1;
    }
    let start_pos = pos;
    while pos < s.len() && (s[pos].is_ascii_alphabetic()) {
        pos += 1;
    }

    if pos == start_pos {
        return ("".to_string(), pos);
    }

    let identifier = String::from_utf8_lossy(&s[start_pos..pos]).to_string();
    (identifier, pos)
}

#[derive(Debug, PartialEq, Eq)]
enum DatePartSpecifier {
    Millennium,
    Century,
    Decade,
    Year,
    Quarter,
    Month,
    Day,
    Week,
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
}

fn try_get_date_part_specifier(specifier_str: &str) -> Result<DatePartSpecifier> {
    match specifier_str.to_lowercase().as_str() {
        "millennium" | "millennia" => Ok(DatePartSpecifier::Millennium),
        "century" | "centuries" => Ok(DatePartSpecifier::Century),
        "decade" | "decades" => Ok(DatePartSpecifier::Decade),
        "year" | "years" | "y" => Ok(DatePartSpecifier::Year),
        "quarter" | "quarters" => Ok(DatePartSpecifier::Quarter),
        "month" | "months" | "mon" => Ok(DatePartSpecifier::Month),
        "day" | "days" | "d" => Ok(DatePartSpecifier::Day),
        "week" | "weeks" | "w" => Ok(DatePartSpecifier::Week),
        "microsecond" | "microseconds" | "us" => Ok(DatePartSpecifier::Microseconds),
        "millisecond" | "milliseconds" | "ms" => Ok(DatePartSpecifier::Milliseconds),
        "second" | "seconds" | "s" => Ok(DatePartSpecifier::Second),
        "minute" | "minutes" | "m" => Ok(DatePartSpecifier::Minute),
        "hour" | "hours" | "h" => Ok(DatePartSpecifier::Hour),
        _ => Err(Error::BadArgument(format!(
            "Invalid date part specifier: {specifier_str}"
        ))),
    }
}

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_MSEC: i64 = 1_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SEC;
const MINROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const DAYS_PER_WEEK: i32 = 7;
const MONTHS_PER_QUARTER: i32 = 3;
const MONTHS_PER_YEAR: i32 = 12;
const MONTHS_PER_DECADE: i32 = 120;
const MONTHS_PER_CENTURY: i32 = 1200;
const MONTHS_PER_MILLENNIUM: i32 = 12000;

fn apply_specifier(
    result: &mut Interval,
    number: i64,
    fraction: i64,
    specifier_str: &str,
) -> Result<()> {
    if specifier_str.is_empty() {
        result.micros = result
            .micros
            .checked_add(number)
            .ok_or(Error::BadArgument("Overflow".to_string()))?;
        result.micros = result
            .micros
            .checked_add(fraction)
            .ok_or(Error::BadArgument("Overflow".to_string()))?;
        return Ok(());
    }

    let specifier = try_get_date_part_specifier(specifier_str)?;
    match specifier {
        DatePartSpecifier::Millennium => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_MILLENNIUM as i64)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Century => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_CENTURY as i64)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Decade => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_DECADE as i64)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Year => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_YEAR as i64)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Quarter => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_QUARTER as i64)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Month => {
            result.months = result
                .months
                .checked_add(
                    number
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Day => {
            result.days = result
                .days
                .checked_add(
                    number
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Week => {
            result.days = result
                .days
                .checked_add(
                    number
                        .checked_mul(DAYS_PER_WEEK as i64)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Microseconds => {
            result.micros = result
                .micros
                .checked_add(number)
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Milliseconds => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_MSEC)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Second => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_SEC)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Minute => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_MINUTE)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
        DatePartSpecifier::Hour => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MINROS_PER_HOUR)
                        .ok_or(Error::BadArgument("Overflow".to_string()))?,
                )
                .ok_or(Error::BadArgument("Overflow".to_string()))?;
        }
    }
    Ok(())
}

pub fn parse_geometry(raw_data: &[u8]) -> Result<String> {
    let mut data = Cursor::new(raw_data);
    let wkt = Ewkt::from_wkb(&mut data, WkbDialect::Ewkb)?;
    Ok(wkt.0)
}

struct ValueDecoder {}

impl ValueDecoder {
    fn read_field<R: AsRef<[u8]>>(&self, ty: &DataType, reader: &mut Cursor<R>) -> Result<Value> {
        match ty {
            DataType::Null => self.read_null(reader),
            DataType::EmptyArray => self.read_empty_array(reader),
            DataType::EmptyMap => self.read_empty_map(reader),
            DataType::Boolean => self.read_bool(reader),
            DataType::Number(NumberDataType::Int8) => self.read_int8(reader),
            DataType::Number(NumberDataType::Int16) => self.read_int16(reader),
            DataType::Number(NumberDataType::Int32) => self.read_int32(reader),
            DataType::Number(NumberDataType::Int64) => self.read_int64(reader),
            DataType::Number(NumberDataType::UInt8) => self.read_uint8(reader),
            DataType::Number(NumberDataType::UInt16) => self.read_uint16(reader),
            DataType::Number(NumberDataType::UInt32) => self.read_uint32(reader),
            DataType::Number(NumberDataType::UInt64) => self.read_uint64(reader),
            DataType::Number(NumberDataType::Float32) => self.read_float32(reader),
            DataType::Number(NumberDataType::Float64) => self.read_float64(reader),
            DataType::Decimal(DecimalDataType::Decimal128(size)) => self.read_decimal(size, reader),
            DataType::Decimal(DecimalDataType::Decimal256(size)) => self.read_decimal(size, reader),
            DataType::String => self.read_string(reader),
            DataType::Binary => self.read_binary(reader),
            DataType::Timestamp => self.read_timestamp(reader),
            DataType::TimestampTz => self.read_timestamp_tz(reader),
            DataType::Date => self.read_date(reader),
            DataType::Bitmap => self.read_bitmap(reader),
            DataType::Variant => self.read_variant(reader),
            DataType::Geometry => self.read_geometry(reader),
            DataType::Interval => self.read_interval(reader),
            DataType::Geography => self.read_geography(reader),
            DataType::Array(inner_ty) => self.read_array(inner_ty.as_ref(), reader),
            DataType::Map(inner_ty) => self.read_map(inner_ty.as_ref(), reader),
            DataType::Tuple(inner_tys) => self.read_tuple(inner_tys.as_ref(), reader),
            DataType::Vector(dimension) => self.read_vector(*dimension as usize, reader),
            DataType::Nullable(inner_ty) => self.read_nullable(inner_ty.as_ref(), reader),
        }
    }

    fn match_bytes<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: &[u8]) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_bytes(bs) {
            true
        } else {
            reader.rollback(pos);
            false
        }
    }

    fn read_null<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        if self.match_bytes(reader, NULL_VALUE.as_bytes()) {
            Ok(Value::Null)
        } else {
            let buf = reader.fill_buf()?;
            Err(ConvertError::new("null", String::from_utf8_lossy(buf).to_string()).into())
        }
    }

    fn read_bool<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        if self.match_bytes(reader, TRUE_VALUE.as_bytes()) {
            Ok(Value::Boolean(true))
        } else if self.match_bytes(reader, FALSE_VALUE.as_bytes()) {
            Ok(Value::Boolean(false))
        } else {
            let buf = reader.fill_buf()?;
            Err(ConvertError::new("boolean", String::from_utf8_lossy(buf).to_string()).into())
        }
    }

    fn read_int8<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: i8 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::Int8(v)))
    }

    fn read_int16<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: i16 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::Int16(v)))
    }

    fn read_int32<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: i32 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::Int32(v)))
    }

    fn read_int64<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: i64 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::Int64(v)))
    }

    fn read_uint8<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: u8 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::UInt8(v)))
    }

    fn read_uint16<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: u16 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::UInt16(v)))
    }

    fn read_uint32<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: u32 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::UInt32(v)))
    }

    fn read_uint64<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: u64 = reader.read_int_text()?;
        Ok(Value::Number(NumberValue::UInt64(v)))
    }

    fn read_float32<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: f32 = reader.read_float_text()?;
        Ok(Value::Number(NumberValue::Float32(v)))
    }

    fn read_float64<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let v: f64 = reader.read_float_text()?;
        Ok(Value::Number(NumberValue::Float64(v)))
    }

    fn read_decimal<R: AsRef<[u8]>>(
        &self,
        size: &DecimalSize,
        reader: &mut Cursor<R>,
    ) -> Result<Value> {
        let buf = reader.fill_buf()?;
        // parser decimal need fractional part.
        // 10.00 and 10 is different value.
        let (n_in, _) = collect_number(buf);
        let v = unsafe { std::str::from_utf8_unchecked(&buf[..n_in]) };
        let d = parse_decimal(v, *size)?;
        reader.consume(n_in);
        Ok(Value::Number(d))
    }

    fn read_string<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::String(unsafe { String::from_utf8_unchecked(buf) }))
    }

    fn read_binary<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let buf = reader.fill_buf()?;
        let n = collect_binary_number(buf);
        let v = buf[..n].to_vec();
        reader.consume(n);
        Ok(Value::Binary(hex::decode(v)?))
    }

    fn read_date<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        let v = unsafe { std::str::from_utf8_unchecked(&buf) };
        let days = NaiveDate::parse_from_str(v, "%Y-%m-%d")?.num_days_from_ce() - DAYS_FROM_CE;
        Ok(Value::Date(days))
    }

    fn read_timestamp<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        let v = unsafe { std::str::from_utf8_unchecked(&buf) };
        let ts = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.6f")?
            .and_utc()
            .timestamp_micros();
        Ok(Value::Timestamp(ts, Tz::UTC))
    }

    fn read_interval<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::Interval(unsafe { String::from_utf8_unchecked(buf) }))
    }

    fn read_timestamp_tz<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        let v = unsafe { std::str::from_utf8_unchecked(&buf) };
        let t = DateTime::<FixedOffset>::parse_from_str(v, TIMESTAMP_TIMEZONE_FORMAT)?;
        Ok(Value::TimestampTz(t))
    }

    fn read_bitmap<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::Bitmap(unsafe { String::from_utf8_unchecked(buf) }))
    }

    fn read_variant<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::Variant(unsafe { String::from_utf8_unchecked(buf) }))
    }

    fn read_geometry<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::Geometry(unsafe { String::from_utf8_unchecked(buf) }))
    }

    fn read_geography<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::Geography(unsafe {
            String::from_utf8_unchecked(buf)
        }))
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        ty: &DataType,
        reader: &mut Cursor<R>,
    ) -> Result<Value> {
        match self.read_null(reader) {
            Ok(val) => Ok(val),
            Err(_) => self.read_field(ty, reader),
        }
    }

    fn read_empty_array<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        reader.must_ignore_byte(b'[')?;
        reader.must_ignore_byte(b']')?;
        Ok(Value::EmptyArray)
    }

    fn read_empty_map<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        reader.must_ignore_byte(b'{')?;
        reader.must_ignore_byte(b'}')?;
        Ok(Value::EmptyArray)
    }

    fn read_array<R: AsRef<[u8]>>(&self, ty: &DataType, reader: &mut Cursor<R>) -> Result<Value> {
        let mut vals = Vec::new();
        reader.must_ignore_byte(b'[')?;
        for idx in 0.. {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            let val = self.read_field(ty, reader)?;
            vals.push(val);
        }
        Ok(Value::Array(vals))
    }

    fn read_vector<R: AsRef<[u8]>>(
        &self,
        dimension: usize,
        reader: &mut Cursor<R>,
    ) -> Result<Value> {
        let mut vals = Vec::with_capacity(dimension);
        reader.must_ignore_byte(b'[')?;
        for idx in 0..dimension {
            let _ = reader.ignore_white_spaces();
            if idx > 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            let val: f32 = reader.read_float_text()?;
            vals.push(val);
        }
        reader.must_ignore_byte(b']')?;
        Ok(Value::Vector(vals))
    }

    fn read_map<R: AsRef<[u8]>>(&self, ty: &DataType, reader: &mut Cursor<R>) -> Result<Value> {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        let mut kvs = Vec::new();
        reader.must_ignore_byte(b'{')?;
        match ty {
            DataType::Tuple(inner_tys) => {
                for idx in 0.. {
                    let _ = reader.ignore_white_spaces();
                    if reader.ignore_byte(b'}') {
                        break;
                    }
                    if idx != 0 {
                        reader.must_ignore_byte(b',')?;
                    }
                    let _ = reader.ignore_white_spaces();
                    let key = self.read_field(&inner_tys[KEY], reader)?;
                    let _ = reader.ignore_white_spaces();
                    reader.must_ignore_byte(b':')?;
                    let _ = reader.ignore_white_spaces();
                    let val = self.read_field(&inner_tys[VALUE], reader)?;
                    kvs.push((key, val));
                }
                Ok(Value::Map(kvs))
            }
            _ => unreachable!(),
        }
    }

    fn read_tuple<R: AsRef<[u8]>>(
        &self,
        tys: &[DataType],
        reader: &mut Cursor<R>,
    ) -> Result<Value> {
        let mut vals = Vec::new();
        reader.must_ignore_byte(b'(')?;
        for (idx, ty) in tys.iter().enumerate() {
            let _ = reader.ignore_white_spaces();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            let val = self.read_field(ty, reader)?;
            vals.push(val);
        }
        reader.must_ignore_byte(b')')?;
        Ok(Value::Tuple(vals))
    }
}

/// The in-memory representation of the MonthDayMicros variant of the "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct months_days_micros(pub i128);

/// Mask for extracting the lower 64 bits (microseconds).
pub const MICROS_MASK: i128 = 0xFFFFFFFFFFFFFFFF;
/// Mask for extracting the middle 32 bits (days or months).
pub const DAYS_MONTHS_MASK: i128 = 0xFFFFFFFF;

impl months_days_micros {
    /// A new [`months_days_micros`].
    pub fn new(months: i32, days: i32, microseconds: i64) -> Self {
        let months_bits = (months as i128) << 96;
        // converting to u32 before i128 ensures we’re working with the raw, unsigned bit pattern of the i32 value,
        // preventing unwanted sign extension when that value is later used within the i128.
        let days_bits = ((days as u32) as i128) << 64;
        let micros_bits = (microseconds as u64) as i128;

        Self(months_bits | days_bits | micros_bits)
    }

    #[inline]
    pub fn months(&self) -> i32 {
        // Decoding logic
        ((self.0 >> 96) & DAYS_MONTHS_MASK) as i32
    }

    #[inline]
    pub fn days(&self) -> i32 {
        ((self.0 >> 64) & DAYS_MONTHS_MASK) as i32
    }

    #[inline]
    pub fn microseconds(&self) -> i64 {
        (self.0 & MICROS_MASK) as i64
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct timestamp_tz(pub i128);

// From implementations for basic types to Value
impl From<&String> for Value {
    fn from(s: &String) -> Self {
        Value::String(s.clone())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<&bool> for Value {
    fn from(b: &bool) -> Self {
        Value::Boolean(*b)
    }
}

impl From<u8> for Value {
    fn from(n: u8) -> Self {
        Value::Number(NumberValue::UInt8(n))
    }
}

impl From<&u8> for Value {
    fn from(n: &u8) -> Self {
        Value::Number(NumberValue::UInt8(*n))
    }
}

impl From<u16> for Value {
    fn from(n: u16) -> Self {
        Value::Number(NumberValue::UInt16(n))
    }
}

impl From<&u16> for Value {
    fn from(n: &u16) -> Self {
        Value::Number(NumberValue::UInt16(*n))
    }
}

impl From<u32> for Value {
    fn from(n: u32) -> Self {
        Value::Number(NumberValue::UInt32(n))
    }
}

impl From<&u32> for Value {
    fn from(n: &u32) -> Self {
        Value::Number(NumberValue::UInt32(*n))
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::Number(NumberValue::UInt64(n))
    }
}

impl From<&u64> for Value {
    fn from(n: &u64) -> Self {
        Value::Number(NumberValue::UInt64(*n))
    }
}

impl From<i8> for Value {
    fn from(n: i8) -> Self {
        Value::Number(NumberValue::Int8(n))
    }
}

impl From<&i8> for Value {
    fn from(n: &i8) -> Self {
        Value::Number(NumberValue::Int8(*n))
    }
}

impl From<i16> for Value {
    fn from(n: i16) -> Self {
        Value::Number(NumberValue::Int16(n))
    }
}

impl From<&i16> for Value {
    fn from(n: &i16) -> Self {
        Value::Number(NumberValue::Int16(*n))
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Number(NumberValue::Int32(n))
    }
}

impl From<&i32> for Value {
    fn from(n: &i32) -> Self {
        Value::Number(NumberValue::Int32(*n))
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Number(NumberValue::Int64(n))
    }
}

impl From<&i64> for Value {
    fn from(n: &i64) -> Self {
        Value::Number(NumberValue::Int64(*n))
    }
}

impl From<f32> for Value {
    fn from(n: f32) -> Self {
        Value::Number(NumberValue::Float32(n))
    }
}

impl From<&f32> for Value {
    fn from(n: &f32) -> Self {
        Value::Number(NumberValue::Float32(*n))
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Number(NumberValue::Float64(n))
    }
}

impl From<NaiveDate> for Value {
    fn from(date: NaiveDate) -> Self {
        let days = date.num_days_from_ce() - DAYS_FROM_CE;
        Value::Date(days)
    }
}

impl From<&NaiveDate> for Value {
    fn from(date: &NaiveDate) -> Self {
        let days = date.num_days_from_ce() - DAYS_FROM_CE;
        Value::Date(days)
    }
}

impl From<NaiveDateTime> for Value {
    fn from(dt: NaiveDateTime) -> Self {
        let timestamp_micros = dt.and_utc().timestamp_micros();
        Value::Timestamp(timestamp_micros, Tz::UTC)
    }
}

impl From<&NaiveDateTime> for Value {
    fn from(dt: &NaiveDateTime) -> Self {
        let timestamp_micros = dt.and_utc().timestamp_micros();
        Value::Timestamp(timestamp_micros, Tz::UTC)
    }
}

impl From<&f64> for Value {
    fn from(n: &f64) -> Self {
        Value::Number(NumberValue::Float64(*n))
    }
}

// Implement conversion from Value to SQL string for parameter system
impl Value {
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

// 30% faster lexical_core::write to tmp buf and extend_from_slice
#[inline]
pub fn extend_lexical<N: lexical_core::ToLexical>(n: N, out_buf: &mut Vec<u8>) {
    out_buf.reserve(N::FORMATTED_SIZE_DECIMAL);
    let len0 = out_buf.len();
    unsafe {
        let slice = std::slice::from_raw_parts_mut(
            out_buf.as_mut_ptr().add(len0),
            out_buf.capacity() - len0,
        );
        let len = lexical_core::write(n, slice).len();
        out_buf.set_len(len0 + len);
    }
}

#[derive(Clone)]
pub struct OutputCommonSettings {
    pub true_bytes: Vec<u8>,
    pub false_bytes: Vec<u8>,
    pub null_bytes: Vec<u8>,
    pub nan_bytes: Vec<u8>,
    pub inf_bytes: Vec<u8>,
    // pub binary_format: BinaryFormat,
    // pub geometry_format: GeometryDataType,
}

pub trait PrimitiveWithFormat {
    fn write_field(self, buf: &mut Vec<u8>, settings: &OutputCommonSettings);
}

macro_rules! impl_float {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, buf: &mut Vec<u8>, settings: &OutputCommonSettings) {
                match self {
                    $ty::INFINITY => buf.extend_from_slice(&settings.inf_bytes),
                    $ty::NEG_INFINITY => {
                        buf.push(b'-');
                        buf.extend_from_slice(&settings.inf_bytes);
                    }
                    _ => {
                        if self.is_nan() {
                            buf.extend_from_slice(&settings.nan_bytes);
                        } else {
                            extend_lexical(self, buf);
                        }
                    }
                }
            }
        }
    };
}

macro_rules! impl_int {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, out_buf: &mut Vec<u8>, _settings: &OutputCommonSettings) {
                extend_lexical(self, out_buf);
            }
        }
    };
}

impl_int!(i8);
impl_int!(i16);
impl_int!(i32);
impl_int!(i64);
impl_int!(u8);
impl_int!(u16);
impl_int!(u32);
impl_int!(u64);
impl_float!(f32);
impl_float!(f64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_string_basic_positive() {
        let interval = Interval::from_string("0:00:00.000001").unwrap();
        assert_eq!(interval.micros, 1);
    }
}
