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

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

use crate::types::{ConnectionInfo, DriverError, Row, RowIterator, ServerStats, VERSION};
use crate::utils::wait_for_future;

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
pub struct BlockingDatabendConnection(Arc<Box<dyn databend_driver::Connection>>);

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

    pub fn exec(&self, py: Python, sql: String) -> PyResult<i64> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            this.exec(&sql).await.map_err(DriverError::new)
        })?;
        Ok(ret)
    }

    pub fn query_row(&self, py: Python, sql: String) -> PyResult<Option<Row>> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            this.query_row(&sql).await.map_err(DriverError::new)
        })?;
        Ok(ret.map(Row::new))
    }

    pub fn query_all(&self, py: Python, sql: String) -> PyResult<Vec<Row>> {
        let this = self.0.clone();
        let rows = wait_for_future(py, async move {
            this.query_all(&sql).await.map_err(DriverError::new)
        })?;
        Ok(rows.into_iter().map(Row::new).collect())
    }

    pub fn query_iter(&self, py: Python, sql: String) -> PyResult<RowIterator> {
        let this = self.0.clone();
        let it = wait_for_future(py, async {
            this.query_iter(&sql).await.map_err(DriverError::new)
        })?;
        Ok(RowIterator::new(it))
    }

    pub fn stream_load(
        &self,
        py: Python,
        sql: String,
        data: Vec<Vec<String>>,
    ) -> PyResult<ServerStats> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            let data = data
                .iter()
                .map(|v| v.iter().map(|s| s.as_ref()).collect())
                .collect();
            this.stream_load(&sql, data).await.map_err(DriverError::new)
        })?;
        Ok(ServerStats::new(ret))
    }

    #[pyo3(signature = (sql, fp, format_options=None, copy_options=None))]
    pub fn load_file<'p>(
        &'p self,
        py: Python<'p>,
        sql: String,
        fp: String,
        format_options: Option<BTreeMap<String, String>>,
        copy_options: Option<BTreeMap<String, String>>,
    ) -> PyResult<ServerStats> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
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
            this.load_file(&sql, Path::new(&fp), format_options, copy_options)
                .await
                .map_err(DriverError::new)
        })?;
        Ok(ServerStats::new(ret))
    }
}

/// BlockingDatabendCursor is an object that follows PEP 249
/// https://peps.python.org/pep-0249/#cursor-objects
#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendCursor {
    conn: Arc<Box<dyn databend_driver::Connection>>,
    rows: Option<Arc<Mutex<databend_driver::RowIterator>>>,
    buffer: Vec<Row>,
}

impl BlockingDatabendCursor {
    fn new(conn: Box<dyn databend_driver::Connection>) -> Self {
        Self {
            conn: Arc::new(conn),
            rows: None,
            buffer: Vec::new(),
        }
    }
}

#[pymethods]
impl BlockingDatabendCursor {
    fn reset(&mut self) {
        self.rows = None;
        self.buffer.clear();
    }

    pub fn close(&mut self, py: Python) -> PyResult<()> {
        self.reset();
        wait_for_future(py, async move {
            self.conn.close().await.map_err(DriverError::new)
        })?;
        Ok(())
    }

    #[pyo3(signature = (operation, parameters=None))]
    pub fn execute<'p>(
        &'p mut self,
        py: Python<'p>,
        operation: String,
        parameters: Option<Vec<Bound<'p, PyAny>>>,
    ) -> PyResult<PyObject> {
        self.reset();
        let conn = self.conn.clone();
        if let Some(parameters) = parameters {
            if let Some(first) = parameters.first() {
                if let Ok(_) = first.downcast::<PyList>() {
                    let data = parameters
                        .iter()
                        .map(|item| {
                            if let Ok(l) = item.downcast::<PyList>() {
                                Ok(l.iter()
                                    .map(|v| {
                                        if let Ok(v) = v.extract::<String>() {
                                            Ok(v)
                                        } else {
                                            Err(PyException::new_err("Invalid parameter type"))
                                        }
                                    })
                                    .collect::<Result<Vec<_>, _>>()?)
                            } else {
                                Err(PyException::new_err("Invalid parameter type"))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut wtr = csv::WriterBuilder::new().from_writer(vec![]);
                    for row in data {
                        wtr.write_record(row)
                            .map_err(|e| PyException::new_err(e.to_string()))?;
                    }
                    let bytes = wtr
                        .into_inner()
                        .map_err(|e| PyException::new_err(e.to_string()))?;
                    let size = bytes.len() as u64;
                    let reader = Box::new(std::io::Cursor::new(bytes));
                    let stats = wait_for_future(py, async move {
                        conn.load_data(&operation, reader, size, None, None)
                            .await
                            .map_err(DriverError::new)
                    })?;
                    let result = stats.write_rows.into_pyobject(py)?;
                    return Ok(result.into());
                } else {
                    return Err(PyException::new_err("Invalid parameter type"));
                }
            }
        }
        let (first, rows) = wait_for_future(py, async move {
            let mut rows = conn.query_iter(&operation).await?;
            let first = rows.next().await.transpose()?;
            Ok::<_, databend_driver::Error>((first, rows))
        })
        .map_err(DriverError::new)?;
        if let Some(first) = first {
            self.buffer.push(Row::new(first));
        }
        self.rows = Some(Arc::new(Mutex::new(rows)));
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

    pub fn fetchall(&mut self, py: Python) -> PyResult<Vec<Row>> {
        match self.rows.take() {
            Some(rows) => {
                let result = wait_for_future(py, async move {
                    let mut rows = rows.lock().await;
                    let mut result = Vec::new();
                    while let Some(row) = rows.next().await {
                        result.push(row);
                    }
                    result
                });
                let rows = result
                    .into_iter()
                    .map(|res| res.map(Row::new))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DriverError::new)?;
                Ok(rows)
            }
            None => Ok(vec![]),
        }
    }
}
