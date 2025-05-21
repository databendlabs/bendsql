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

use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_driver::Param;
use databend_driver::Params;
use pyo3::exceptions::PyAttributeError;
use pyo3::types::PyTuple;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};

#[ctor::ctor]
pub(crate) static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap();

/// Utility to collect rust futures with GIL released
pub(crate) fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    py.allow_threads(|| RUNTIME.block_on(f))
}

pub(crate) fn to_sql_params(v: Option<Bound<PyAny>>) -> Params {
    match v {
        Some(v) => {
            if let Ok(v) = v.downcast::<PyDict>() {
                let mut params = HashMap::new();
                for (k, v) in v.iter() {
                    let k = k.extract::<String>().unwrap();
                    let v = to_sql_string(v).unwrap();
                    params.insert(k, v);
                }
                Params::NamedParams(params)
            } else if let Ok(v) = v.downcast::<PyList>() {
                let mut params = vec![];
                for v in v.iter() {
                    let v = to_sql_string(v).unwrap();
                    params.push(v);
                }
                Params::QuestionParams(params)
            } else if let Ok(v) = v.downcast::<PyTuple>() {
                let mut params = vec![];
                for v in v.iter() {
                    let v = to_sql_string(v).unwrap();
                    params.push(v);
                }
                Params::QuestionParams(params)
            } else {
                Params::QuestionParams(vec![to_sql_string(v).unwrap()])
            }
        }
        None => Params::default(),
    }
}

fn to_sql_string(v: Bound<PyAny>) -> PyResult<String> {
    match v.downcast::<PyAny>() {
        Ok(v) => {
            if let Ok(v) = v.extract::<String>() {
                Ok(v.as_sql_string())
            } else if let Ok(v) = v.extract::<bool>() {
                Ok(v.as_sql_string())
            } else if let Ok(v) = v.extract::<i64>() {
                Ok(v.as_sql_string())
            } else if let Ok(v) = v.extract::<f64>() {
                Ok(v.as_sql_string())
            } else {
                Err(PyAttributeError::new_err(format!(
                    "Invalid parameter type for: {:?}, expected str, bool, int or float",
                    v
                )))
            }
        }
        Err(e) => Err(e.into()),
    }
}

pub(super) fn options_as_ref(
    format_options: &Option<BTreeMap<String, String>>,
) -> Option<BTreeMap<&str, &str>> {
    format_options
        .as_ref()
        .map(|opts| opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect())
}
