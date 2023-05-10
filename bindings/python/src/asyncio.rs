// Copyright 2023 Datafuse Labs.
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

use databend_client;
use databend_driver::rest_api::RestAPIConnection;
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use databend_driver::Connection;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use std::fs::Metadata;
use pyo3::pyobject_native_type;

use crate::{build_rest_api_client};
use crate::format_pyerr;

/// `AsyncDatabendDriver` is the entry for all public async API
#[pyclass(module = "databend_python")]
pub struct AsyncDatabendDriver(RestAPIConnection);

#[pymethods]
impl AsyncDatabendDriver {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: &str) -> PyResult<Self> {
        Ok(AsyncDatabendDriver(build_rest_api_client(dsn)?))
    }

    /// exec
    pub fn exec<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res = this.exec(&sql).await.unwrap();
            Ok(res)
        })
    }

    // pub fn query_row<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
    //     let this = self.0.clone();
    //     future_into_py(py, async move {
    //         let row = this.query_row(&sql).await.unwrap();
    //         let row = row.unwrap();
    //         let res = row.clone().try_into().unwrap();
    //         // let py_res: PyObject = Python::with_gil(|py| PyObject::new(py, &res).into());
    //         Ok(res)
    //     },
    //     )
    // }
}