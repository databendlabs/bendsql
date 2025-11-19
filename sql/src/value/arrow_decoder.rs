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

use std::sync::Arc;

use super::{Interval, NumberValue, Value};
use crate::error::{ConvertError, Error};
use crate::value::geo::convert_geometry;
use arrow_array::{
    Array as ArrowArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StringViewArray,
    StructArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use chrono::{FixedOffset, LocalResult, TimeZone};
use databend_client::schema::{
    DecimalSize, ARROW_EXT_TYPE_BITMAP, ARROW_EXT_TYPE_EMPTY_ARRAY, ARROW_EXT_TYPE_EMPTY_MAP,
    ARROW_EXT_TYPE_GEOGRAPHY, ARROW_EXT_TYPE_GEOMETRY, ARROW_EXT_TYPE_INTERVAL,
    ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE, ARROW_EXT_TYPE_VARIANT, ARROW_EXT_TYPE_VECTOR,
    EXTENSION_KEY,
};
use databend_client::ResultFormatSettings;
use ethnum::i256;
use jsonb::RawJsonb;

/// The in-memory representation of the MonthDayMicros variant of the "Interval" logical type.
#[allow(non_camel_case_types)]
#[repr(C)]
struct months_days_micros(pub i128);

/// Mask for extracting the lower 64 bits (microseconds).
const MICROS_MASK: i128 = 0xFFFFFFFFFFFFFFFF;
/// Mask for extracting the middle 32 bits (days or months).
const DAYS_MONTHS_MASK: i128 = 0xFFFFFFFF;

impl months_days_micros {
    #[inline]
    pub fn months(&self) -> i32 {
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

impl
    TryFrom<(
        &ArrowField,
        &Arc<dyn ArrowArray>,
        usize,
        ResultFormatSettings,
    )> for Value
{
    type Error = Error;
    fn try_from(
        (field, array, seq, settings): (
            &ArrowField,
            &Arc<dyn ArrowArray>,
            usize,
            ResultFormatSettings,
        ),
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
                            let unix_ts = v as u64 as i64;
                            let offset = (v >> 64) as i32;
                            let offset = FixedOffset::east_opt(offset)
                                .ok_or_else(|| Error::Parsing("invalid offset".to_string()))?;
                            let dt =
                                offset.timestamp_micros(unix_ts).single().ok_or_else(|| {
                                    Error::Parsing(format!(
                                        "Invalid timestamp_micros {unix_ts} for offset {offset}"
                                    ))
                                })?;
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
                            let value = convert_geometry(
                                array.value(seq),
                                settings.geometry_output_format,
                            )?;
                            Ok(Value::Geometry(value))
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
                            let value = convert_geometry(
                                array.value(seq),
                                settings.geometry_output_format,
                            )?;
                            Ok(Value::Geography(value))
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
                    Some(array) => {
                        let v = array.value(seq);
                        let v = i256::from_le_bytes(v.to_le_bytes());
                        Ok(Value::Number(NumberValue::Decimal256(
                            v,
                            DecimalSize {
                                precision: *p,
                                scale: *s as u8,
                            },
                        )))
                    }
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
                            None => {
                                let ltz = settings.timezone;
                                let dt = match ltz.timestamp_micros(ts) {
                                    LocalResult::Single(dt) => dt,
                                    LocalResult::None => {
                                        return Err(Error::Parsing(format!(
                                            "time {ts} not exists in timezone {ltz}"
                                        )))
                                    }
                                    LocalResult::Ambiguous(dt1, _dt2) => dt1,
                                };
                                Ok(Value::Timestamp(dt))
                            }
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
                        let value = Value::try_from((f.as_ref(), &inner_array, i, settings))?;
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
                        let value = Value::try_from((f.as_ref(), &inner_array, i, settings))?;
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
                            let key = Value::try_from((
                                fs[0].as_ref(),
                                inner_array.column(0),
                                i,
                                settings,
                            ))?;
                            let val = Value::try_from((
                                fs[1].as_ref(),
                                inner_array.column(1),
                                i,
                                settings,
                            ))?;
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
                        let value = Value::try_from((f.as_ref(), inner_array, seq, settings))?;
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
