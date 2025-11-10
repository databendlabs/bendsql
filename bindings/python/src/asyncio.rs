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

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::{
    types::{ConnectionInfo, DriverError, Row, RowIterator, ServerStats, VERSION},
    utils::to_sql_params,
};
use databend_driver::LoadMethod;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

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

    pub fn last_query_id(&self) -> Option<String> {
        self.0.last_query_id()
    }

    pub fn kill_query<'p>(
        &'p self,
        py: Python<'p>,
        query_id: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            this.kill_query(&query_id).await.map_err(DriverError::new)?;
            Ok(())
        })
    }

    pub fn close<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            this.close().await.map_err(DriverError::new)?;
            Ok(())
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
            let res = if params.is_empty() {
                this.exec(&sql).await.map_err(DriverError::new)?
            } else {
                this.exec(&sql)
                    .bind(params)
                    .await
                    .map_err(DriverError::new)?
            };
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
            let row = if params.is_empty() {
                this.query_row(&sql).await.map_err(DriverError::new)?
            } else {
                this.query(&sql)
                    .bind(params)
                    .one()
                    .await
                    .map_err(DriverError::new)?
            };
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
            let rows: Vec<Row> = {
                let core_rows = if params.is_empty() {
                    this.query_all(&sql).await.map_err(DriverError::new)?
                } else {
                    this.query(&sql)
                        .bind(params)
                        .all()
                        .await
                        .map_err(DriverError::new)?
                };
                core_rows.into_iter().map(Row::new).collect()
            };
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
            let streamer = if params.is_empty() {
                this.query_iter(&sql).await.map_err(DriverError::new)?
            } else {
                this.query(&sql)
                    .bind(params)
                    .iter()
                    .await
                    .map_err(DriverError::new)?
            };
            Ok(RowIterator::new(streamer))
        })
    }

    #[pyo3(signature = (sql, data, method=None))]
    pub fn stream_load<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        data: Vec<Vec<String>>,
        method: Option<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let load_method = LoadMethod::from_str(&method.unwrap_or_else(|| "stage".to_string()))
                .map_err(DriverError::new)?;
            let data = data
                .iter()
                .map(|v| v.iter().map(|s| s.as_ref()).collect())
                .collect();
            let ss = this
                .stream_load(&sql, data, load_method)
                .await
                .map_err(DriverError::new)?;
            Ok(ServerStats::new(ss))
        })
    }

    #[pyo3(signature = (sql, fp, method=None))]
    pub fn load_file<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        fp: String,
        method: Option<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.0.clone();
        let load_method = LoadMethod::from_str(&method.unwrap_or_else(|| "stage".to_string()))
            .map_err(DriverError::new)?;
        future_into_py(py, async move {
            let ss = this
                .load_file(&sql, Path::new(&fp), load_method)
                .await
                .map_err(DriverError::new)?;
            Ok(ServerStats::new(ss))
        })
    }
}
