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
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use crate::types::{ConnectionInfo, DriverError, Row, RowIterator, ServerStats, VERSION};
use crate::utils::{options_as_ref, to_sql_params, wait_for_future};
use databend_driver::{LoadMethod, RowWithStats, SchemaRef};
use pyo3::exceptions::{PyAttributeError, PyException, PyStopIteration};
use pyo3::types::{PyList, PyTuple};
use pyo3::{prelude::*, IntoPyObjectExt};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendClient(databend_driver::Client);

#[pymethods]
impl BlockingDatabendClient {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: String) -> PyResult<Self> {
        let name = format!("databend-driver-python/{}", VERSION.as_str());
        let client = databend_driver::Client::new(dsn).with_name(name);
        Ok(Self(client))
    }

    pub fn get_conn(&self, py: Python) -> PyResult<BlockingDatabendConnection> {
        let this = self.0.clone();
        let conn = wait_for_future(py, async move {
            this.get_conn().await.map_err(DriverError::new)
        })?;
        Ok(BlockingDatabendConnection(Arc::new(conn)))
    }

    pub fn cursor(&self, py: Python) -> PyResult<BlockingDatabendCursor> {
        let this = self.0.clone();
        let conn = wait_for_future(py, async move {
            this.get_conn().await.map_err(DriverError::new)
        })?;
        Ok(BlockingDatabendCursor::new(conn))
    }
}

#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendConnection(Arc<databend_driver::Connection>);

#[pymethods]
impl BlockingDatabendConnection {
    pub fn info(&self, py: Python) -> PyResult<ConnectionInfo> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move { this.info().await });
        Ok(ConnectionInfo::new(ret))
    }

    pub fn version(&self, py: Python) -> PyResult<String> {
        let this = self.0.clone();
        let ret = wait_for_future(
            py,
            async move { this.version().await.map_err(DriverError::new) },
        )?;
        Ok(ret)
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
    pub fn exec(&self, py: Python, sql: String, params: Option<Bound<PyAny>>) -> PyResult<i64> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        let ret = wait_for_future(py, async move {
            if params.is_empty() {
                this.exec(&sql).await.map_err(DriverError::new)
            } else {
                this.exec(&sql).bind(params).await.map_err(DriverError::new)
            }
        })?;
        Ok(ret)
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn query_row(
        &self,
        py: Python,
        sql: String,
        params: Option<Bound<PyAny>>,
    ) -> PyResult<Option<Row>> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        let ret = wait_for_future(py, async move {
            if params.is_empty() {
                this.query_row(&sql).await.map_err(DriverError::new)
            } else {
                this.query(&sql)
                    .bind(params)
                    .one()
                    .await
                    .map_err(DriverError::new)
            }
        })?;
        Ok(ret.map(Row::new))
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn query_all(
        &self,
        py: Python,
        sql: String,
        params: Option<Bound<PyAny>>,
    ) -> PyResult<Vec<Row>> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        let rows = wait_for_future(py, async move {
            if params.is_empty() {
                this.query_all(&sql).await.map_err(DriverError::new)
            } else {
                this.query(&sql)
                    .bind(params)
                    .all()
                    .await
                    .map_err(DriverError::new)
            }
        })?;
        Ok(rows.into_iter().map(Row::new).collect())
    }

    #[pyo3(signature = (sql, params=None))]
    pub fn query_iter(
        &self,
        py: Python,
        sql: String,
        params: Option<Bound<PyAny>>,
    ) -> PyResult<RowIterator> {
        let this = self.0.clone();
        let params = to_sql_params(params);
        let it = wait_for_future(py, async {
            if params.is_empty() {
                this.query_iter(&sql).await.map_err(DriverError::new)
            } else {
                this.query(&sql)
                    .bind(params)
                    .iter()
                    .await
                    .map_err(DriverError::new)
            }
        })?;
        Ok(RowIterator::new(it))
    }

    #[pyo3(signature = (sql, data, method=None))]
    pub fn stream_load(
        &self,
        py: Python,
        sql: String,
        data: Vec<Vec<String>>,
        method: Option<String>,
    ) -> PyResult<ServerStats> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            let load_method = LoadMethod::from_str(&method.unwrap_or_else(|| "stage".to_string()))
                .map_err(DriverError::new)?;
            let data = data
                .iter()
                .map(|v| v.iter().map(|s| s.as_ref()).collect())
                .collect();
            this.stream_load(&sql, data, load_method)
                .await
                .map_err(DriverError::new)
        })?;
        Ok(ServerStats::new(ret))
    }

    #[pyo3(signature = (sql, fp, method=None, format_options=None, copy_options=None))]
    pub fn load_file<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        fp: String,
        method: Option<String>,
        format_options: Option<BTreeMap<String, String>>,
        copy_options: Option<BTreeMap<String, String>>,
    ) -> PyResult<ServerStats> {
        let this = self.0.clone();
        let ret = if format_options.is_some() {
            wait_for_future(py, async move {
                let format_options = options_as_ref(&format_options);
                let copy_options = options_as_ref(&copy_options);
                this.load_file_with_options(&sql, Path::new(&fp), format_options, copy_options)
                    .await
                    .map_err(DriverError::new)
            })?
        } else {
            let load_method = LoadMethod::from_str(&method.unwrap_or_else(|| "stage".to_string()))
                .map_err(DriverError::new)?;
            wait_for_future(py, async move {
                this.load_file(&sql, Path::new(&fp), load_method)
                    .await
                    .map_err(DriverError::new)
            })?
        };
        Ok(ServerStats::new(ret))
    }

    pub fn last_query_id(&self) -> Option<String> {
        self.0.last_query_id()
    }

    pub fn kill_query(&self, py: Python, query_id: String) -> PyResult<()> {
        let this = self.0.clone();
        wait_for_future(py, async move {
            this.kill_query(&query_id).await.map_err(DriverError::new)
        })?;
        Ok(())
    }

    pub fn close(&mut self, py: Python) -> PyResult<()> {
        wait_for_future(
            py,
            async move { self.0.close().await.map_err(DriverError::new) },
        )?;
        Ok(())
    }
}

/// BlockingDatabendCursor is an object that follows PEP 249
/// https://peps.python.org/pep-0249/#cursor-objects
#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendCursor {
    conn: Arc<databend_driver::Connection>,
    rows: Option<Arc<Mutex<databend_driver::RowIterator>>>,
    stats: Arc<RwLock<Option<databend_driver::ServerStats>>>,
    rowcount_mode: RowcountMode,
    // buffer is used to store only the first row after execute
    buffer: Vec<Row>,
    schema: Option<SchemaRef>,
    closed: bool,
}

impl BlockingDatabendCursor {
    fn new(conn: databend_driver::Connection) -> Self {
        Self {
            conn: Arc::new(conn),
            rows: None,
            stats: Arc::new(RwLock::new(None)),
            rowcount_mode: RowcountMode::Unknown,
            buffer: Vec::new(),
            schema: None,
            closed: false,
        }
    }
}

impl BlockingDatabendCursor {
    fn reset(&mut self) {
        self.rows = None;
        *self.stats.write().unwrap() = None;
        self.rowcount_mode = RowcountMode::Unknown;
        self.buffer.clear();
        self.schema = None;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum RowcountMode {
    #[default]
    Unknown,
    Affected,
}

impl RowcountMode {
    fn rowcount(self, stats: Option<&databend_driver::ServerStats>) -> i64 {
        match self {
            RowcountMode::Affected => stats.map(|stats| stats.write_rows as i64).unwrap_or(-1),
            RowcountMode::Unknown => -1,
        }
    }
}

fn capture_stats(
    rows: databend_driver::RowStatsIterator,
    stats: Arc<RwLock<Option<databend_driver::ServerStats>>>,
) -> databend_driver::RowIterator {
    let schema = rows.schema();
    let rows = rows.filter_map(move |result| match result {
        Ok(RowWithStats::Row(row)) => Some(Ok(row)),
        Ok(RowWithStats::Stats(value)) => {
            *stats.write().unwrap() = Some(value);
            None
        }
        Err(error) => Some(Err(error)),
    });
    databend_driver::RowIterator::new(schema, Box::pin(rows))
}

#[pymethods]
impl BlockingDatabendCursor {
    #[getter]
    pub fn description<'p>(&'p self, py: Python<'p>) -> PyResult<PyObject> {
        if let Some(ref schema) = self.schema {
            let mut fields = vec![];
            for field in schema.fields() {
                let field = (
                    field.name.clone(),          // name
                    field.data_type.to_string(), // type_code
                    None::<i64>,                 // display_size
                    None::<i64>,                 // internal_size
                    None::<i64>,                 // precision
                    None::<i64>,                 // scale
                    None::<bool>,                // null_ok
                );
                fields.push(field.into_pyobject(py)?);
            }
            PyList::new(py, fields)?.into_py_any(py)
        } else {
            Ok(py.None())
        }
    }

    fn set_schema(&mut self, py: Python) {
        if let Some(ref rows) = self.rows {
            let schema = wait_for_future(py, async move {
                let rows = rows.lock().await;
                rows.schema()
            });
            self.schema = Some(schema)
        }
    }

    #[getter]
    pub fn rowcount(&self, _py: Python) -> i64 {
        self.rowcount_mode
            .rowcount(self.stats.read().unwrap().as_ref())
    }

    #[getter]
    pub fn stats(&self) -> Option<ServerStats> {
        self.stats.read().unwrap().clone().map(ServerStats::new)
    }

    pub fn close(&mut self, py: Python) -> PyResult<()> {
        self.closed = true;
        self.reset();
        wait_for_future(py, async move {
            self.conn.close().await.map_err(DriverError::new)
        })?;
        Ok(())
    }

    /// Only `INSERT` and `REPLACE` statements are supported if parameters provided.
    /// Parameters will be translated into CSV format, and then loaded as stage attachment.
    #[pyo3(signature = (operation, params=None, values=None))]
    pub fn execute<'p>(
        &'p mut self,
        py: Python<'p>,
        operation: String,
        params: Option<Bound<'p, PyAny>>,
        values: Option<Bound<'p, PyAny>>,
    ) -> PyResult<PyObject> {
        if self.closed {
            return Err(PyException::new_err(
                "BlockingDatabendCursor already closed",
            ));
        }
        if let Some(values) = values {
            return self.executemany(py, operation, [values].to_vec());
        }

        self.reset();
        let conn = self.conn.clone();
        let cursor_stats = self.stats.clone();
        // fetch first row after execute
        // then we could finish the query directly if there's no result
        let params = to_sql_params(params);
        let (rowcount_mode, first, rows) = wait_for_future(py, async move {
            let query = conn.query(&operation);
            let rowcount_mode = if query.is_dml() {
                RowcountMode::Affected
            } else {
                RowcountMode::Unknown
            };
            let rows = if params.is_empty() {
                query.iter_ext().await?
            } else {
                query.bind(params).iter_ext().await?
            };
            let mut rows = capture_stats(rows, cursor_stats);
            let first = rows.next().await.transpose()?;
            Ok::<_, databend_driver::Error>((rowcount_mode, first, rows))
        })
        .map_err(DriverError::new)?;
        self.rowcount_mode = rowcount_mode;
        if let Some(first) = first {
            self.buffer.push(Row::new(first));
        }
        self.rows = Some(Arc::new(Mutex::new(rows)));
        self.set_schema(py);
        Ok(py.None())
    }

    /// Only `INSERT` and `REPLACE` statements are supported.
    /// Parameters will be translated into CSV format, and then loaded as stage attachment.
    pub fn executemany<'p>(
        &'p mut self,
        py: Python<'p>,
        sql: String,
        seq_of_parameters: Vec<Bound<'p, PyAny>>,
    ) -> PyResult<PyObject> {
        self.reset();
        self.rowcount_mode = RowcountMode::Affected;
        let conn = self.conn.clone();
        if let Some(param) = seq_of_parameters.first() {
            if param.downcast::<PyList>().is_ok() || param.downcast::<PyTuple>().is_ok() {
                let strings = to_csv_strings(seq_of_parameters)?;
                let strs = strings
                    .iter()
                    .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let stats = wait_for_future(py, async move {
                    conn.stream_load(&sql, strs, LoadMethod::Stage)
                        .await
                        .map_err(DriverError::new)
                })?;
                let result = stats.write_rows.into_pyobject(py)?;
                *self.stats.write().unwrap() = Some(stats);
                return Ok(result.into());
            } else {
                return Err(PyAttributeError::new_err(
                    "Invalid parameter type, expected list or tuple",
                ));
            }
        }
        Ok(py.None())
    }

    pub fn fetchone(&mut self, py: Python) -> PyResult<Option<Row>> {
        if let Some(row) = self.buffer.pop() {
            return Ok(Some(row));
        }
        match self.rows {
            Some(ref rows) => {
                match wait_for_future(py, async move { rows.lock().await.next().await }) {
                    Some(row) => Ok(Some(Row::new(row.map_err(DriverError::new)?))),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    #[pyo3(signature = (size=1))]
    pub fn fetchmany(&mut self, py: Python, size: Option<usize>) -> PyResult<Vec<Row>> {
        let mut result = self.buffer.drain(..).collect::<Vec<_>>();
        if let Some(ref rows) = self.rows {
            let size = size.unwrap_or(1);
            while result.len() < size {
                let row = wait_for_future(py, async move {
                    let mut rows = rows.lock().await;
                    rows.next().await.transpose().map_err(DriverError::new)
                })?;
                if let Some(row) = row {
                    result.push(Row::new(row));
                } else {
                    break;
                }
            }
        }
        Ok(result)
    }

    pub fn fetchall(&mut self, py: Python) -> PyResult<Vec<Row>> {
        let mut result = self.buffer.drain(..).collect::<Vec<_>>();
        match self.rows.take() {
            Some(rows) => {
                let fetched = wait_for_future(py, async move {
                    let mut rows = rows.lock().await;
                    let mut result = Vec::new();
                    while let Some(row) = rows.next().await {
                        result.push(row);
                    }
                    result
                });
                for row in fetched {
                    result.push(Row::new(row.map_err(DriverError::new)?));
                }
                Ok(result)
            }
            None => Ok(vec![]),
        }
    }

    // Optional DB API Extensions

    pub fn next(&mut self, py: Python) -> PyResult<Row> {
        self.__next__(py)
    }

    pub fn __next__(&mut self, py: Python) -> PyResult<Row> {
        match self.fetchone(py)? {
            Some(row) => Ok(row),
            None => Err(PyStopIteration::new_err("Rows exhausted")),
        }
    }
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

fn to_csv_strings(parameters: Vec<Bound<'_, PyAny>>) -> PyResult<Vec<Vec<String>>> {
    let mut rows = Vec::with_capacity(parameters.len());
    for row in parameters {
        let iter = row.try_iter()?;
        let row = iter
            .map(|v| match v {
                Ok(v) => to_csv_field(v),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}

fn to_csv_field(v: Bound<PyAny>) -> PyResult<String> {
    if v.is_none() {
        return Ok("".to_string());
    }
    match v.downcast::<PyAny>() {
        Ok(v) => {
            if let Ok(v) = v.extract::<String>() {
                Ok(v)
            } else if let Ok(v) = v.extract::<bool>() {
                Ok(v.to_string())
            } else if let Ok(v) = v.extract::<i64>() {
                Ok(v.to_string())
            } else if let Ok(v) = v.extract::<f64>() {
                Ok(v.to_string())
            } else {
                // Try to convert dict-like objects to JSON string
                if let Ok(dict) = v.downcast::<pyo3::types::PyDict>() {
                    // Convert Python dict to JSON string
                    let json_string = Python::with_gil(|py| {
                        let json_module = py.import("json")?;
                        let dumps = json_module.getattr("dumps")?;
                        let result = dumps.call1((dict,))?;
                        result.extract::<String>()
                    })?;
                    Ok(json_string)
                } else {
                    // Try to convert any object to string using its __str__ method
                    match v.call_method0("__str__") {
                        Ok(str_obj) => {
                            if let Ok(s) = str_obj.extract::<String>() {
                                Ok(s)
                            } else {
                                Err(PyAttributeError::new_err(format!(
                                    "Failed to convert object to string: {v:?}"
                                )))
                            }
                        }
                        Err(_) => {
                            Err(PyAttributeError::new_err(format!(
                                "Invalid parameter type for: {v:?}, expected str, bool, int, float, dict, or object with __str__ method"
                            )))
                        }
                    }
                }
            }
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use databend_driver::{RowStatsIterator, Schema, ServerStats};

    #[tokio::test]
    async fn test_capture_stats_filters_stats_and_keeps_latest_snapshot() {
        let schema = Arc::new(Schema::default());
        let items = vec![
            Ok(RowWithStats::Stats(ServerStats {
                write_rows: 1,
                ..Default::default()
            })),
            Ok(RowWithStats::Row(databend_driver::Row::new(
                schema.clone(),
                vec![],
            ))),
            Ok(RowWithStats::Stats(ServerStats {
                write_rows: 2,
                ..Default::default()
            })),
        ];
        let rows = RowStatsIterator::new(schema, Box::pin(tokio_stream::iter(items)));
        let stats = Arc::new(RwLock::new(None));
        let mut rows = capture_stats(rows, stats.clone());

        assert!(rows.next().await.transpose().unwrap().is_some());
        assert!(rows.next().await.transpose().unwrap().is_none());
        assert_eq!(stats.read().unwrap().as_ref().unwrap().write_rows, 2);
    }

    #[test]
    fn test_rowcount_mode() {
        let zero = ServerStats {
            write_rows: 0,
            ..Default::default()
        };
        let positive = ServerStats {
            write_rows: 3,
            ..Default::default()
        };

        assert_eq!(RowcountMode::Unknown.rowcount(Some(&zero)), -1);
        assert_eq!(RowcountMode::Affected.rowcount(None), -1);
        assert_eq!(RowcountMode::Affected.rowcount(Some(&zero)), 0);
        assert_eq!(RowcountMode::Affected.rowcount(Some(&positive)), 3);
    }
}
