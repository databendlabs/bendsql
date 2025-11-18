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

use std::collections::HashMap;
use std::hash::Hash;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;

use crate::error::{ConvertError, Error, Result};

use super::{NumberValue, Value, DAYS_FROM_CE};

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
            Value::Timestamp(i, _tz) => match DateTime::from_timestamp_micros(i) {
                Some(t) => Ok(t.naive_utc()),
                None => Err(ConvertError::new("NaiveDateTime", format!("{val}")).into()),
            },
            _ => Err(ConvertError::new("NaiveDateTime", format!("{val}")).into()),
        }
    }
}

impl TryFrom<Value> for DateTime<Tz> {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Timestamp(i, tz) => match DateTime::from_timestamp_micros(i) {
                Some(t) => Ok(tz.from_utc_datetime(&t.naive_utc())),
                None => Err(ConvertError::new("Datetime", format!("{val}")).into()),
            },
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
