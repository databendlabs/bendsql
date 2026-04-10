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
use std::fmt::Debug;

use databend_common_ast::parser::Dialect;

pub trait Param: Debug {
    fn as_json_value(&self) -> serde_json::Value;

    fn as_sql_string(&self) -> String {
        json_value_to_sql_string(&self.as_json_value())
    }
}

#[derive(Debug)]
pub enum Params {
    // ?, ?
    QuestionParams(Vec<serde_json::Value>),
    // :name, :age
    NamedParams(HashMap<String, serde_json::Value>),
}

impl Default for Params {
    fn default() -> Self {
        Params::QuestionParams(vec![])
    }
}

/// Convert a `serde_json::Value` to a SQL string representation for client-side binding.
pub fn json_value_to_sql_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{s}'"),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_value_to_sql_string).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(map) => {
            let mut s = String::from("'{");
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&format!("\"{k}\": {}", json_value_to_sql_string(v)));
            }
            s.push_str("}'::JSON");
            s
        }
    }
}

impl Params {
    pub fn len(&self) -> usize {
        match self {
            Params::QuestionParams(vec) => vec.len(),
            Params::NamedParams(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // index based from 1
    pub fn get_by_index(&self, index: usize) -> Option<&serde_json::Value> {
        if index == 0 {
            return None;
        }
        match self {
            Params::QuestionParams(vec) => vec.get(index - 1),
            _ => None,
        }
    }

    pub fn get_by_name(&self, name: &str) -> Option<&serde_json::Value> {
        match self {
            Params::NamedParams(map) => map.get(name),
            _ => None,
        }
    }

    pub fn merge(&mut self, other: Params) {
        match (self, other) {
            (Params::QuestionParams(vec1), Params::QuestionParams(vec2)) => {
                vec1.extend(vec2);
            }
            (Params::NamedParams(map1), Params::NamedParams(map2)) => {
                map1.extend(map2);
            }
            _ => panic!("Cannot merge QuestionParams with NamedParams"),
        }
    }

    /// Convert params to a JSON value suitable for server-side parameter binding.
    /// `QuestionParams` → `Value::Array`, `NamedParams` → `Value::Object`.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Params::QuestionParams(vec) => serde_json::Value::Array(vec.clone()),
            Params::NamedParams(map) => {
                let obj: serde_json::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                serde_json::Value::Object(obj)
            }
        }
    }

    pub fn replace(&self, sql: &str) -> String {
        if !self.is_empty() {
            let tokens = databend_common_ast::parser::tokenize_sql(sql).unwrap();
            if let Ok((stmt, _)) =
                databend_common_ast::parser::parse_sql(&tokens, Dialect::PostgreSQL)
            {
                let mut v = super::placeholder::PlaceholderVisitor::new();
                return v.replace_sql(self, &stmt, sql);
            }
        }
        sql.to_string()
    }
}

// Implement Param for numeric types that fit in serde_json::Number
macro_rules! impl_param_for_json_number {
    ($($t:ty)*) => ($(
        impl Param for $t {
            fn as_json_value(&self) -> serde_json::Value {
                serde_json::json!(self)
            }
        }
    )*)
}

impl_param_for_json_number! { i8 i16 i32 i64 isize u8 u16 u32 u64 usize f32 f64 }

// i128/u128 cannot be represented in JSON numbers, store as string
impl Param for i128 {
    fn as_json_value(&self) -> serde_json::Value {
        // If it fits in i64, use a number; otherwise use a string to avoid precision loss
        if *self >= i128::from(i64::MIN) && *self <= i128::from(i64::MAX) {
            serde_json::json!(*self as i64)
        } else {
            serde_json::Value::String(self.to_string())
        }
    }
}

impl Param for u128 {
    fn as_json_value(&self) -> serde_json::Value {
        // If it fits in u64, use a number; otherwise use a string to avoid precision loss
        if *self <= u128::from(u64::MAX) {
            serde_json::json!(*self as u64)
        } else {
            serde_json::Value::String(self.to_string())
        }
    }
}

impl Param for bool {
    fn as_json_value(&self) -> serde_json::Value {
        serde_json::Value::Bool(*self)
    }
}

impl Param for String {
    fn as_json_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.clone())
    }
}

impl Param for &str {
    fn as_json_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.to_string())
    }
}

impl Param for () {
    fn as_json_value(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

impl<T> Param for Option<T>
where
    T: Param,
{
    fn as_json_value(&self) -> serde_json::Value {
        match self {
            Some(s) => s.as_json_value(),
            None => serde_json::Value::Null,
        }
    }
}

impl Param for serde_json::Value {
    fn as_json_value(&self) -> serde_json::Value {
        self.clone()
    }
}

/// let name = d;
/// let age = 4;
/// params!{a => 1, b => 2, c =>  name }  ---> generate Params::NamedParams{"a" : 1, "b": 2, "c": "d"}
/// params!{ name, age } ---> generate Params::QuestionParams{ vec!["d", 4] }
#[macro_export]
macro_rules! params {
    // Handle named parameters
    () => {
        $crate::Params::default()
    };
    ($($key:ident => $value:expr),* $(,)?) => {
        $crate::Params::NamedParams({
            let mut map = HashMap::new();

            $(
                map.insert(stringify!($key).to_string(), $crate::Param::as_json_value(&$value));
            )*
            map
        })
    };
    // Handle positional parameters
    ($($value:expr),* $(,)?) => {
        $crate::Params::QuestionParams(vec![
            $(
                $crate::Param::as_json_value(&$value),
            )*
        ])
    };
}

impl From<()> for Params {
    fn from(_: ()) -> Self {
        Params::default()
    }
}

// impl From Tuple(A, B, C, D....) for Params where A, B, C, D: Param
macro_rules! impl_from_tuple_for_params {
    // empty tuple
    () => {};

    // recursive impl
    ($head:ident, $($tail:ident),*) => {
	#[allow(non_snake_case)]
        impl<$head: Param, $($tail: Param),*> From<($head, $($tail),*)> for Params {
            fn from(tuple: ($head, $($tail),*)) -> Self {
                let (h, $($tail),*) = tuple;
                let mut params = Params::QuestionParams(vec![h.as_json_value()]);
                $(params.merge(Params::QuestionParams(vec![$tail.as_json_value()]));)*
                params
            }
        }

        impl_from_tuple_for_params!($($tail),*);
    };

    // single element tuple
    ($last:ident) => {
        impl<$last: Param> From<($last,)> for Params {
            fn from(tuple: ($last,)) -> Self {
                Params::QuestionParams(vec![tuple.0.as_json_value()])
            }
        }
    };
}

impl_from_tuple_for_params! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10 }

impl From<Option<serde_json::Value>> for Params {
    fn from(value: Option<serde_json::Value>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Params::default(),
        }
    }
}

impl From<serde_json::Value> for Params {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Array(arr) => Params::QuestionParams(arr),
            serde_json::Value::Object(obj) => Params::NamedParams(obj.into_iter().collect()),
            other => Params::QuestionParams(vec![other]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_params() {
        // Test named parameters
        {
            let name = "d";
            let age = 4;
            let params = params! {a => 1, b => age, c => name};
            match params {
                Params::NamedParams(map) => {
                    assert_eq!(map.get("a").unwrap(), &serde_json::json!(1));
                    assert_eq!(map.get("b").unwrap(), &serde_json::json!(4));
                    assert_eq!(map.get("c").unwrap(), &serde_json::json!("d"));
                }
                _ => panic!("Expected NamedParams"),
            }
            let params = params! {};
            assert!(params.is_empty());
        }

        // Test positional parameters
        {
            let name = "d";
            let age = 4;
            let params = params! {name, age, 33u64};
            match params {
                Params::QuestionParams(vec) => {
                    assert_eq!(
                        vec,
                        vec![
                            serde_json::json!("d"),
                            serde_json::json!(4),
                            serde_json::json!(33u64)
                        ]
                    );
                }
                _ => panic!("Expected QuestionParams"),
            }
        }

        // Test into params for tuple
        {
            let params: Params = (1, "44", 2, 3, "55", "66").into();
            match params {
                Params::QuestionParams(vec) => {
                    assert_eq!(
                        vec,
                        vec![
                            serde_json::json!(1),
                            serde_json::json!("44"),
                            serde_json::json!(2),
                            serde_json::json!(3),
                            serde_json::json!("55"),
                            serde_json::json!("66"),
                        ]
                    );
                }
                _ => panic!("Expected QuestionParams"),
            }
        }

        // Test Option<T>
        {
            let params: Params = (Some(1), None::<()>, Some("44"), None::<()>).into();
            match params {
                Params::QuestionParams(vec) => assert_eq!(
                    vec,
                    vec![
                        serde_json::json!(1),
                        serde_json::Value::Null,
                        serde_json::json!("44"),
                        serde_json::Value::Null,
                    ]
                ),
                _ => panic!("Expected QuestionParams"),
            }
        }

        // Test into params for serde_json
        {
            let params: Params = serde_json::json!({
            "a": 1,
            "b": "44",
            "c": 2,
            "d": 3,
            "e": "55",
            "f": "66",
            })
            .into();
            match params {
                Params::NamedParams(map) => {
                    assert_eq!(map.get("a").unwrap(), &serde_json::json!(1));
                    assert_eq!(map.get("b").unwrap(), &serde_json::json!("44"));
                    assert_eq!(map.get("c").unwrap(), &serde_json::json!(2));
                    assert_eq!(map.get("d").unwrap(), &serde_json::json!(3));
                    assert_eq!(map.get("e").unwrap(), &serde_json::json!("55"));
                    assert_eq!(map.get("f").unwrap(), &serde_json::json!("66"));
                }
                _ => panic!("Expected NamedParams"),
            }
        }

        // Test into params for serde_json::Value::Array
        {
            let params: Params =
                serde_json::json!([1, "44", 2, serde_json::json!({"a" : 1}), "55", "66"]).into();
            match params {
                Params::QuestionParams(vec) => {
                    assert_eq!(
                        vec,
                        vec![
                            serde_json::json!(1),
                            serde_json::json!("44"),
                            serde_json::json!(2),
                            serde_json::json!({"a": 1}),
                            serde_json::json!("55"),
                            serde_json::json!("66"),
                        ]
                    );
                }
                _ => panic!("Expected QuestionParams"),
            }
        }
    }

    #[test]
    fn test_to_json_value() {
        // Test positional params
        let params = params! {1, "hello", 9.99};
        let json = params.to_json_value();
        assert_eq!(json, serde_json::json!([1, "hello", 9.99]));

        // Test named params
        let params = params! {a => 1, b => "hello", c => true};
        let json = params.to_json_value();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.get("a").unwrap(), &serde_json::json!(1));
        assert_eq!(obj.get("b").unwrap(), &serde_json::json!("hello"));
        assert_eq!(obj.get("c").unwrap(), &serde_json::json!(true));

        // Test NULL
        let params = params! {()};
        let json = params.to_json_value();
        assert_eq!(json, serde_json::json!([null]));

        // Test Option<T>
        let params: Params = (Some(42), None::<()>, Some("world")).into();
        let json = params.to_json_value();
        assert_eq!(json, serde_json::json!([42, null, "world"]));

        // Test lowercase bool (from serde_json::Value::Bool)
        let params: Params = serde_json::json!([true, false]).into();
        let json = params.to_json_value();
        assert_eq!(json, serde_json::json!([true, false]));

        // Test large u64 above i64::MAX
        let big: u64 = u64::MAX;
        let params: Params = (big,).into();
        let json = params.to_json_value();
        assert_eq!(json, serde_json::json!([big]));
    }

    #[test]
    fn test_replace() {
        let params = params! {1, "44", 2, 3, "55", "66"};
        let sql =
            "SELECT * FROM table WHERE a = ? AND '?' = cj AND b = ? AND c = ? AND d = ? AND e = ? AND f = ?";
        let replaced_sql = params.replace(sql);
        assert_eq!(replaced_sql, "SELECT * FROM table WHERE a = 1 AND '?' = cj AND b = '44' AND c = 2 AND d = 3 AND e = '55' AND f = '66'");

        let params = params! {a => 1, b => "44", c => 2, d => 3, e => "55", f => "66"};

        {
            let sql = "SELECT * FROM table WHERE a = :a AND '?' = cj AND b = :b AND c = :c AND d = :d AND e = :e AND f = :f";
            let replaced_sql = params.replace(sql);
            assert_eq!(replaced_sql, "SELECT * FROM table WHERE a = 1 AND '?' = cj AND b = '44' AND c = 2 AND d = 3 AND e = '55' AND f = '66'");
        }

        {
            let sql = "SELECT b = :b, a = :a FROM table WHERE a = :a AND '?' = cj AND b = :b AND c = :c AND d = :d AND e = :e AND f = :f";
            let replaced_sql = params.replace(sql);
            assert_eq!(replaced_sql, "SELECT b = '44', a = 1 FROM table WHERE a = 1 AND '?' = cj AND b = '44' AND c = 2 AND d = 3 AND e = '55' AND f = '66'");
        }

        {
            let params = params! {1, "44", 2, 3, "55", "66"};
            let sql = "SELECT $3, $2, $1 FROM table WHERE a = $1 AND '?' = cj AND b = $2 AND c = $3 AND d = $4 AND e = $5 AND f = $6";
            let replaced_sql = params.replace(sql);
            assert_eq!(replaced_sql, "SELECT 2, '44', 1 FROM table WHERE a = 1 AND '?' = cj AND b = '44' AND c = 2 AND d = 3 AND e = '55' AND f = '66'");
        }
    }
}
