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

use crate::_macro_internal::Error;
use crate::cursor_ext::{
    collect_binary_number, collect_number, BufferReadStringExt, ReadBytesExt, ReadCheckPointExt,
    ReadNumberExt,
};
use crate::error::{ConvertError, Result};
use arrow_buffer::i256;
use chrono::{DateTime, Datelike, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use databend_client::schema::{DataType, DecimalDataType, DecimalSize, NumberDataType};
use hex;
use std::io::{BufRead, Cursor};

use super::{NumberValue, Value, DAYS_FROM_CE, TIMESTAMP_TIMEZONE_FORMAT};

const NULL_VALUE: &str = "NULL";
const TRUE_VALUE: &str = "1";
const FALSE_VALUE: &str = "0";

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
            DataType::Timestamp => parse_timestamp(v.as_str(), tz),
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
                let decoder = ValueDecoder { timezone: tz };
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

struct ValueDecoder {
    pub timezone: Tz,
}

impl ValueDecoder {
    pub(super) fn read_field<R: AsRef<[u8]>>(
        &self,
        ty: &DataType,
        reader: &mut Cursor<R>,
    ) -> Result<Value> {
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
        parse_timestamp(v, self.timezone)
    }

    fn read_timestamp_tz<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        let v = unsafe { std::str::from_utf8_unchecked(&buf) };
        let t = DateTime::<FixedOffset>::parse_from_str(v, TIMESTAMP_TIMEZONE_FORMAT)?;
        Ok(Value::TimestampTz(t))
    }

    fn read_interval<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<Value> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
        Ok(Value::Interval(unsafe { String::from_utf8_unchecked(buf) }))
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

fn parse_timestamp(ts_string: &str, tz: Tz) -> Result<Value> {
    let naive_dt = NaiveDateTime::parse_from_str(ts_string, "%Y-%m-%d %H:%M:%S%.6f")?;
    let dt_with_tz = match tz.from_local_datetime(&naive_dt) {
        LocalResult::Single(dt) => dt,
        LocalResult::None => {
            return Err(Error::Parsing(format!(
                "time {ts_string} not exists in timezone {tz}"
            )))
        }
        LocalResult::Ambiguous(dt1, _dt2) => dt1,
    };
    let ts = dt_with_tz.timestamp_micros();
    Ok(Value::Timestamp(ts, tz))
}

fn parse_decimal(text: &str, size: DecimalSize) -> Result<NumberValue> {
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
