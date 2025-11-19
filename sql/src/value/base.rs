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

use databend_client::schema::{DataType, DecimalDataType, DecimalSize, NumberDataType};
use ethnum::i256;
use jiff::Zoned;

// Thu 1970-01-01 is R.D. 719163
pub(crate) const DAYS_FROM_CE: i32 = 719_163;
pub(crate) const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
pub(crate) const TIMESTAMP_TIMEZONE_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f %z";

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
    Timestamp(Zoned),
    TimestampTz(Zoned),
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
            Self::Timestamp(_) => DataType::Timestamp,
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
