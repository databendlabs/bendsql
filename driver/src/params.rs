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
    fn as_sql_string(&self) -> String;
}

#[derive(Debug)]
pub enum Params {
    // ?, ?
    QuestionParams(Vec<String>),
    // :name, :age
    NamedParams(HashMap<String, String>),
}

impl Default for Params {
    fn default() -> Self {
        Params::QuestionParams(vec![])
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
    pub fn get_by_index(&self, index: usize) -> Option<&String> {
        if index == 0 {
            return None;
        }
        match self {
            Params::QuestionParams(vec) => vec.get(index - 1),
            _ => None,
        }
    }

    pub fn get_by_name(&self, name: &str) -> Option<&String> {
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

    pub fn replace(&self, sql: &str) -> String {
        if !self.is_empty() {
            if let Ok((stmt, _)) = databend_common_ast::parser::parse_sql(sql, Dialect::PostgreSQL)
            {
                let mut sql = sql.to_string();
                let mut positions = Vec::new();

                for token in tokens {
                    match token.kind {
                        databend_common_ast::parser::token::TokenKind::Placeholder => {
                            positions.push(token.span);
                        }
                        _ => {}
                    }
                }
                let size = positions.len();
                for (index, r) in positions.iter().rev().enumerate() {
                    if let Some(param) = self.get_by_index(size - index) {
                        let start = r.start as usize;
                        let end = r.end as usize;
                        sql.replace_range(start..end, param);
                    }
                }
                return sql;
            }
        }
        return sql.to_string();
    }
}

// impl param for all integer types and string types
macro_rules! impl_param_for_integer {
    ($($t:ty)*) => ($(
        impl Param for $t {
            fn as_sql_string(&self) -> String {
                self.to_string()
            }
        }
    )*)
}

impl_param_for_integer! { i8 i16 i32 i64 f32 f64 i128 isize u8 u16 u32 u64 u128 usize }

// Implement Param for String
impl Param for bool {
    fn as_sql_string(&self) -> String {
        if *self {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }
    }
}

// Implement Param for String
impl Param for String {
    fn as_sql_string(&self) -> String {
        format!("'{}'", self)
    }
}

// Implement Param for &str
impl Param for &str {
    fn as_sql_string(&self) -> String {
        format!("'{}'", self)
    }
}

impl Param for serde_json::Value {
    fn as_sql_string(&self) -> String {
        match self {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => format!("'{}'", s),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Array(values) => {
                let mut s = String::from("[");
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(&v.as_sql_string());
                }
                s.push_str("]");
                s
            }
            serde_json::Value::Object(map) => {
                let mut s = String::from("'{");
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(&format!("\"{}\": {}", k, v.as_sql_string()));
                }
                s.push_str("}'::JSON");
                s
            }
        }
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
	Params::default()
    };
    ($($key:ident => $value:expr),* $(,)?) => {
        Params::NamedParams({
            let mut map = HashMap::new();
            $(
                map.insert(stringify!($key).to_string(), $value.as_sql_string());
            )*
            map
        })
    };
    // Handle positional parameters
    ($($value:expr),* $(,)?) => {
        Params::QuestionParams(vec![
            $(
                $value.as_sql_string(),
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
                let mut params = Params::QuestionParams(vec![h.as_sql_string()]);
                $(params.merge(Params::QuestionParams(vec![$tail.as_sql_string()]));)*
                params
            }
        }

        impl_from_tuple_for_params!($($tail),*);
    };

    // single element tuple
    ($last:ident) => {
        impl<$last: Param> From<($last,)> for Params {
            fn from(tuple: ($last,)) -> Self {
                Params::QuestionParams(vec![tuple.0.as_sql_string()])
            }
        }
    };
}

impl_from_tuple_for_params! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10 }

impl From<Option<serde_json::Value>> for Params {
    fn from(value: Option<serde_json::Value>) -> Self {
        match value {
            Some(serde_json::Value::Array(obj)) => {
                let mut array = Vec::new();
                for v in obj {
                    array.push(v.as_sql_string());
                }
                Params::QuestionParams(array)
            }
            None => Params::default(),
            _ => unimplemented!("json value must be an array as params"),
        }
    }
}

impl From<serde_json::Value> for Params {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Array(obj) => {
                let mut array = Vec::new();
                for v in obj {
                    array.push(v.as_sql_string());
                }
                Params::QuestionParams(array)
            }
            serde_json::Value::Object(obj) => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v.as_sql_string());
                }
                Params::NamedParams(map)
            }
            other => Params::QuestionParams(vec![other.as_sql_string()]),
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
                    assert_eq!(map.get("a").unwrap(), "1");
                    assert_eq!(map.get("b").unwrap(), "4");
                    assert_eq!(map.get("c").unwrap(), "'d'");
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
                    assert_eq!(vec, vec!["'d'", "4", "33"]);
                }
                _ => panic!("Expected QuestionParams"),
            }
        }

        // Test into params for tuple
        {
            let params: Params = (1, "44", 2, 3, "55", "66").into();
            match params {
                Params::QuestionParams(vec) => {
                    assert_eq!(vec, vec!["1", "'44'", "2", "3", "'55'", "'66'"]);
                }
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
                    assert_eq!(map.get("a").unwrap(), "1");
                    assert_eq!(map.get("b").unwrap(), "'44'");
                    assert_eq!(map.get("c").unwrap(), "2");
                    assert_eq!(map.get("d").unwrap(), "3");
                    assert_eq!(map.get("e").unwrap(), "'55'");
                    assert_eq!(map.get("f").unwrap(), "'66'");
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
                        vec!["1", "'44'", "2", "'{\"a\": 1}'::JSON", "'55'", "'66'"]
                    );
                }
                _ => panic!("Expected QuestionParams"),
            }
        }
    }

    #[test]
    fn test_replace() {
        let params = params! {1, "44", 2, 3, "55", "66"};
        let sql =
            "SELECT * FROM table WHERE a = ? AND '?' = cj AND b = ? AND c = ? AND d = ? AND e = ? AND f = ?";
        let replaced_sql = params.replace(sql);
        assert_eq!(replaced_sql, "SELECT * FROM table WHERE a = 1 AND '?' = cj AND b = '44' AND c = 2 AND d = 3 AND e = '55' AND f = '66'");
    }
}
