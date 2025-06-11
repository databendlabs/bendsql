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

use crate::error::Result;
use crate::response::QueryResponse;
use crate::{APIClient, QueryStats, SchemaField};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_stream::{Stream, StreamExt};

#[derive(Default)]
pub struct Page {
    pub schema: Vec<SchemaField>,
    pub data: Vec<Vec<Option<String>>>,
    pub stats: QueryStats,
}

impl Page {
    pub fn from_response(response: QueryResponse) -> Self {
        Self {
            schema: response.schema,
            data: response.data,
            stats: response.stats,
        }
    }

    pub fn update(&mut self, p: Page) {
        self.schema = p.schema;
        if self.data.is_empty() {
            self.data = p.data
        } else {
            self.data.extend_from_slice(&p.data);
        }
        self.stats = p.stats;
    }

    pub fn affected_rows(&self) -> Result<i64, Box<dyn std::error::Error>> {
        if self.schema.is_empty() {
            return Ok(0);
        }

        let first_field = &self.schema[0];

        if !first_field.name.contains("number of rows") {
            return Ok(0);
        }

        if self.data.is_empty() || self.data[0].is_empty() {
            return Ok(0);
        }

        match &self.data[0][0] {
            Some(value_str) => self.parse_row_count_string(value_str),
            None => Ok(0),
        }
    }

    fn parse_row_count_string(&self, value_str: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let trimmed = value_str.trim();

        if trimmed.is_empty() {
            return Ok(0);
        }

        if let Ok(count) = trimmed.parse::<i64>() {
            return Ok(count);
        }

        if let Ok(count) = serde_json::from_str::<i64>(trimmed) {
            return Ok(count);
        }

        let unquoted = trimmed.trim_matches('"');
        if let Ok(count) = unquoted.parse::<i64>() {
            return Ok(count);
        }

        Err(format!("failed to parse affected rows from: '{}'", value_str).into())
    }

    ///the schema can be `number of rows inserted`, `number of rows deleted`, `number of rows updated` when sql start with  `insert`, `delete`, `update`
    pub fn has_affected_rows(&self) -> bool {
        !self.schema.is_empty() && self.schema[0].name.contains("number of rows")
    }
}

type PageFut = Pin<Box<dyn Future<Output = Result<QueryResponse>> + Send>>;

pub struct Pages {
    query_id: String,
    client: Arc<APIClient>,
    first_page: Option<Page>,
    need_progress: bool,

    next_page_future: Option<PageFut>,
    node_id: Option<String>,
    next_uri: Option<String>,
}

impl Pages {
    pub fn new(client: Arc<APIClient>, first_response: QueryResponse, need_progress: bool) -> Self {
        let mut s = Self {
            query_id: first_response.id.clone(),
            need_progress,
            client,
            next_page_future: None,
            node_id: first_response.node_id.clone(),
            first_page: None,
            next_uri: first_response.next_uri.clone(),
        };
        let first_page = Page::from_response(first_response);
        s.first_page = Some(first_page);
        s
    }

    pub fn add_back(&mut self, page: Page) {
        self.first_page = Some(page);
    }

    pub async fn wait_for_schema(
        mut self,
        need_progress: bool,
    ) -> Result<(Self, Vec<SchemaField>)> {
        while let Some(page) = self.next().await {
            let page = page?;
            if !page.schema.is_empty()
                || !page.data.is_empty()
                || (need_progress && page.stats.progresses.has_progress())
            {
                let schema = page.schema.clone();
                self.add_back(page);
                return Ok((self, schema));
            }
        }
        Ok((self, vec![]))
    }
}

impl Stream for Pages {
    type Item = Result<Page>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(p) = mem::take(&mut self.first_page) {
            return Poll::Ready(Some(Ok(p)));
        };
        match self.next_page_future {
            Some(ref mut next_page) => match Pin::new(next_page).poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    self.next_uri = resp.next_uri.clone();
                    self.next_page_future = None;
                    if resp.data.is_empty() && !self.need_progress {
                        self.poll_next(cx)
                    } else {
                        Poll::Ready(Some(Ok(Page::from_response(resp))))
                    }
                }
                Poll::Ready(Err(e)) => {
                    self.next_page_future = None;
                    self.next_uri = None;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            None => match self.next_uri {
                Some(ref next_uri) => {
                    let client = self.client.clone();
                    let next_uri = next_uri.clone();
                    let query_id = self.query_id.clone();
                    let node_id = self.node_id.clone();
                    self.next_page_future = Some(Box::pin(async move {
                        client.query_page(&query_id, &next_uri, &node_id).await
                    }));
                    self.poll_next(cx)
                }
                None => Poll::Ready(None),
            },
        }
    }
}
