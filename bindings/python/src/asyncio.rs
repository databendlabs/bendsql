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
use std::path::Path;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{
    types::{ConnectionInfo, DriverError, Row, RowIterator, ServerStats, VERSION},
    utils::to_sql_params,
};

#[pyclass(module = "databend_driver")]
pub struct AsyncDatabendClient(databend_driver::Client);

#[pymethods]
impl AsyncDatabendClient {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: String) -> PyResult<Self> {
        let name = format!("databend-driver-python/{}", VERSION.as_str());
        let client = databend_driver::Client::new(dsn).with_name(name);
        Ok(Self(client))
    }

    pub fn get_conn<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let conn = this.get_conn().await.map_err(DriverError::new)?;
            Ok(AsyncDatabendConnection(Arc::new(conn)))
        })
    }
}

#[pyclass(module = "databend_driver")]
pub struct AsyncDatabendConnection(Arc<databend_driver::Connection>);

#[pymethods]
impl AsyncDatabendConnection {
    pub fn info<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let info = this.info().await;
            Ok(ConnectionInfo::new(info))
        })
    }

    pub fn version<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let version = this.version().await.map_err(DriverError::new)?;
            Ok(version)
        })
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn format_sql(
        &self,
        _py: Python,
        sql: String,
        params: Option<Bound<PyAny>>,
    ) -> PyResult<String> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        Ok(this.format_sql(&sql, params))
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn exec<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        params: Option<Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        future_into_py(py, async move {
            let res = this.exec(&sql, params).await.map_err(DriverError::new)?;
            Ok(res)
        })
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn query_row<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        params: Option<Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        future_into_py(py, async move {
            let row = this
                .query_row(&sql, params)
                .await
                .map_err(DriverError::new)?;
            Ok(row.map(Row::new))
        })
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn query_all<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        params: Option<Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        future_into_py(py, async move {
            let rows: Vec<Row> = this
                .query_all(&sql, params)
                .await
                .map_err(DriverError::new)?
                .into_iter()
                .map(Row::new)
                .collect();
            Ok(rows)
        })
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn query_iter<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        params: Option<Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        let params = to_sql_params(params);

        future_into_py(py, async move {
            let streamer = this
                .query_iter(&sql, params)
                .await
                .map_err(DriverError::new)?;
            Ok(RowIterator::new(streamer))
        })
    }

    pub fn stream_load<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        data: Vec<Vec<String>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let data = data
                .iter()
                .map(|v| v.iter().map(|s| s.as_ref()).collect())
                .collect();
            let ss = this
                .stream_load(&sql, data)
                .await
                .map_err(DriverError::new)?;
            Ok(ServerStats::new(ss))
        })
    }

    #[pyo3(signature = (sql, fp, format_options, copy_options=None))]
    pub fn load_file<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        fp: String,
        format_options: Option<BTreeMap<String, String>>,
        copy_options: Option<BTreeMap<String, String>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let format_options = match format_options {
                None => None,
                Some(ref opts) => {
                    Some(opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect())
                }
            };
            let copy_options = match copy_options {
                None => None,
                Some(ref opts) => {
                    Some(opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect())
                }
            };
            let ss = this
                .load_file(&sql, Path::new(&fp), format_options, copy_options)
                .await
                .map_err(DriverError::new)?;
            Ok(ServerStats::new(ss))
        })
    }
}
