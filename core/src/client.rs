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

use crate::auth::{AccessTokenAuth, AccessTokenFileAuth, Auth, BasicAuth};
use crate::capability::Capability;
use crate::client_mgr::{GLOBAL_CLIENT_MANAGER, GLOBAL_RUNTIME};
use crate::error_code::{need_refresh_token, ResponseWithErrorCode};
use crate::global_cookie_store::GlobalCookieStore;
use crate::login::{
    LoginRequest, LoginResponseResult, RefreshResponse, RefreshSessionTokenRequest,
    SessionTokenInfo,
};
use crate::presign::{presign_upload_to_stage, PresignMode, PresignedResponse, Reader};
use crate::response::LoadResponse;
use crate::stage::StageLocation;
use crate::{
    error::{Error, Result},
    request::{PaginationConfig, QueryRequest, StageAttachmentConfig},
    response::QueryResponse,
    session::SessionState,
    QueryStats,
};
use crate::{Page, Pages};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use base64::engine::general_purpose::URL_SAFE;
use base64::Engine;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use percent_encoding::percent_decode_str;
use reqwest::cookie::CookieStore;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use reqwest::{Body, Client as HttpClient, Request, RequestBuilder, Response, StatusCode};
use semver::Version;
use serde::{de, Deserialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio_retry::strategy::jitter;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use url::Url;

const HEADER_QUERY_ID: &str = "X-DATABEND-QUERY-ID";
const HEADER_TENANT: &str = "X-DATABEND-TENANT";
const HEADER_STICKY_NODE: &str = "X-DATABEND-STICKY-NODE";
const HEADER_WAREHOUSE: &str = "X-DATABEND-WAREHOUSE";
const HEADER_STAGE_NAME: &str = "X-DATABEND-STAGE-NAME";
const HEADER_ROUTE_HINT: &str = "X-DATABEND-ROUTE-HINT";
const TXN_STATE_ACTIVE: &str = "Active";
const HEADER_SQL: &str = "X-DATABEND-SQL";
const HEADER_QUERY_CONTEXT: &str = "X-DATABEND-QUERY-CONTEXT";
const HEADER_SESSION_ID: &str = "X-DATABEND-SESSION-ID";
const CONTENT_TYPE_ARROW: &str = "application/vnd.apache.arrow.stream";
const CONTENT_TYPE_ARROW_OR_JSON: &str = "application/vnd.apache.arrow.stream";

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

#[derive(Clone)]
pub(crate) struct QueryState {
    pub node_id: String,
    pub last_access_time: Arc<Mutex<Instant>>,
    pub timeout_secs: u64,
}

impl QueryState {
    pub fn need_heartbeat(&self, now: Instant) -> bool {
        let t = *self.last_access_time.lock();
        now.duration_since(t).as_secs() > self.timeout_secs / 2
    }
}

pub struct APIClient {
    pub(crate) session_id: String,
    cli: HttpClient,
    scheme: String,
    host: String,
    port: u16,

    endpoint: Url,

    auth: Arc<dyn Auth>,

    tenant: Option<String>,
    warehouse: Mutex<Option<String>>,
    session_state: Mutex<SessionState>,
    route_hint: RouteHintGenerator,

    disable_login: bool,
    body_format: String,
    disable_session_token: bool,
    session_token_info: Option<Arc<Mutex<(SessionTokenInfo, Instant)>>>,

    closed: AtomicBool,

    server_version: Option<Version>,

    wait_time_secs: Option<i64>,
    max_rows_in_buffer: Option<i64>,
    max_rows_per_page: Option<i64>,

    connect_timeout: Duration,
    page_request_timeout: Duration,

    tls_ca_file: Option<String>,

    presign: Mutex<PresignMode>,
    last_node_id: Mutex<Option<String>>,
    last_query_id: Mutex<Option<String>>,

    capability: Capability,

    queries_need_heartbeat: Mutex<HashMap<String, QueryState>>,
}

impl Drop for APIClient {
    fn drop(&mut self) {
        self.close_with_spawn()
    }
}

impl APIClient {
    pub async fn new(dsn: &str, name: Option<String>) -> Result<Arc<Self>> {
        let mut client = Self::from_dsn(dsn).await?;
        client.build_client(name).await?;
        if !client.disable_login {
            client.login().await?;
        }
        if client.session_id.is_empty() {
            client.session_id = format!("no_login_{}", uuid::Uuid::new_v4());
        }
        let client = Arc::new(client);
        client.check_presign().await?;
        GLOBAL_CLIENT_MANAGER.register_client(client.clone()).await;
        Ok(client)
    }

    pub fn capability(&self) -> &Capability {
        &self.capability
    }

    fn set_presign_mode(&self, mode: PresignMode) {
        *self.presign.lock() = mode
    }
    fn get_presign_mode(&self) -> PresignMode {
        *self.presign.lock()
    }

    async fn from_dsn(dsn: &str) -> Result<Self> {
        let u = Url::parse(dsn)?;
        let mut client = Self::default();
        if let Some(host) = u.host_str() {
            client.host = host.to_string();
        }

        if u.username() != "" {
            let password = u.password().unwrap_or_default();
            let password = percent_decode_str(password).decode_utf8()?;
            client.auth = Arc::new(BasicAuth::new(u.username(), password));
        }

        let mut session_state = SessionState::default();
        session_state.set_database(u.path().trim_start_matches('/'));

        let mut scheme = "https";
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "wait_time_secs" => {
                    client.wait_time_secs = Some(v.parse()?);
                }
                "max_rows_in_buffer" => {
                    client.max_rows_in_buffer = Some(v.parse()?);
                }
                "max_rows_per_page" => {
                    client.max_rows_per_page = Some(v.parse()?);
                }
                "connect_timeout" => client.connect_timeout = Duration::from_secs(v.parse()?),
                "page_request_timeout_secs" => {
                    client.page_request_timeout = {
                        let secs: u64 = v.parse()?;
                        Duration::from_secs(secs)
                    };
                }
                "presign" => {
                    let presign_mode = match v.as_ref() {
                        "auto" => PresignMode::Auto,
                        "detect" => PresignMode::Detect,
                        "on" => PresignMode::On,
                        "off" => PresignMode::Off,
                        _ => {
                            return Err(Error::BadArgument(format!(
                            "Invalid value for presign: {v}, should be one of auto/detect/on/off"
                        )))
                        }
                    };
                    client.set_presign_mode(presign_mode);
                }
                "tenant" => {
                    client.tenant = Some(v.to_string());
                }
                "warehouse" => {
                    client.warehouse = Mutex::new(Some(v.to_string()));
                }
                "role" => session_state.set_role(v),
                "sslmode" => match v.as_ref() {
                    "disable" => scheme = "http",
                    "require" | "enable" => scheme = "https",
                    _ => {
                        return Err(Error::BadArgument(format!(
                            "Invalid value for sslmode: {v}"
                        )))
                    }
                },
                "tls_ca_file" => {
                    client.tls_ca_file = Some(v.to_string());
                }
                "access_token" => {
                    client.auth = Arc::new(AccessTokenAuth::new(v));
                }
                "access_token_file" => {
                    client.auth = Arc::new(AccessTokenFileAuth::new(v));
                }
                "login" => {
                    client.disable_login = match v.as_ref() {
                        "disable" => true,
                        "enable" => false,
                        _ => {
                            return Err(Error::BadArgument(format!("Invalid value for login: {v}")))
                        }
                    }
                }
                "session_token" => {
                    client.disable_session_token = match v.as_ref() {
                        "disable" => true,
                        "enable" => false,
                        _ => {
                            return Err(Error::BadArgument(format!(
                                "Invalid value for session_token: {v}"
                            )))
                        }
                    }
                }
                "body_format" => {
                    let v = v.to_string().to_lowercase();
                    match v.as_str() {
                        "json" | "arrow" => client.body_format = v.to_string(),
                        _ => {
                            return Err(Error::BadArgument(format!(
                                "Invalid value for body_format: {v}"
                            )))
                        }
                    }
                }
                _ => {
                    session_state.set(k, v);
                }
            }
        }
        client.port = match u.port() {
            Some(p) => p,
            None => match scheme {
                "http" => 80,
                "https" => 443,
                _ => unreachable!(),
            },
        };
        client.scheme = scheme.to_string();
        client.endpoint = Url::parse(&format!("{}://{}:{}", scheme, client.host, client.port))?;
        client.session_state = Mutex::new(session_state);

        Ok(client)
    }

    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn scheme(&self) -> &str {
        self.scheme.as_str()
    }

    async fn build_client(&mut self, name: Option<String>) -> Result<()> {
        let ua = match name {
            Some(n) => n,
            None => format!("databend-client-rust/{}", VERSION.as_str()),
        };
        let cookie_provider = GlobalCookieStore::new();
        let cookie = HeaderValue::from_str("cookie_enabled=true").unwrap();
        let mut initial_cookies = [&cookie].into_iter();
        cookie_provider.set_cookies(&mut initial_cookies, &Url::parse("https://a.com").unwrap());
        let mut cli_builder = HttpClient::builder()
            .user_agent(ua)
            .cookie_provider(Arc::new(cookie_provider))
            .pool_idle_timeout(Duration::from_secs(1));
        #[cfg(any(feature = "rustls", feature = "native-tls"))]
        if self.scheme == "https" {
            if let Some(ref ca_file) = self.tls_ca_file {
                let cert_pem = tokio::fs::read(ca_file).await?;
                let cert = reqwest::Certificate::from_pem(&cert_pem)?;
                cli_builder = cli_builder.add_root_certificate(cert);
            }
        }
        self.cli = cli_builder.build()?;
        Ok(())
    }

    async fn check_presign(self: &Arc<Self>) -> Result<()> {
        let mode = match self.get_presign_mode() {
            PresignMode::Auto => {
                if self.host.ends_with(".databend.com") || self.host.ends_with(".databend.cn") {
                    PresignMode::On
                } else {
                    PresignMode::Off
                }
            }
            PresignMode::Detect => match self.get_presigned_upload_url("@~/.bendsql/check").await {
                Ok(_) => PresignMode::On,
                Err(e) => {
                    warn!("presign mode off with error detected: {e}");
                    PresignMode::Off
                }
            },
            mode => mode,
        };
        self.set_presign_mode(mode);
        Ok(())
    }

    pub fn current_warehouse(&self) -> Option<String> {
        let guard = self.warehouse.lock();
        guard.clone()
    }

    pub fn current_catalog(&self) -> Option<String> {
        let guard = self.session_state.lock();
        guard.catalog.clone()
    }

    pub fn current_database(&self) -> Option<String> {
        let guard = self.session_state.lock();
        guard.database.clone()
    }

    pub fn set_warehouse(&self, warehouse: impl Into<String>) {
        let mut guard = self.warehouse.lock();
        *guard = Some(warehouse.into());
    }

    pub fn set_database(&self, database: impl Into<String>) {
        let mut guard = self.session_state.lock();
        guard.set_database(database);
    }

    pub fn set_role(&self, role: impl Into<String>) {
        let mut guard = self.session_state.lock();
        guard.set_role(role);
    }

    pub fn set_session(&self, key: impl Into<String>, value: impl Into<String>) {
        let mut guard = self.session_state.lock();
        guard.set(key, value);
    }

    pub async fn current_role(&self) -> Option<String> {
        let guard = self.session_state.lock();
        guard.role.clone()
    }

    fn in_active_transaction(&self) -> bool {
        let guard = self.session_state.lock();
        guard
            .txn_state
            .as_ref()
            .map(|s| s.eq_ignore_ascii_case(TXN_STATE_ACTIVE))
            .unwrap_or(false)
    }

    pub fn username(&self) -> String {
        self.auth.username()
    }

    fn gen_query_id(&self) -> String {
        uuid::Uuid::now_v7().simple().to_string()
    }

    async fn handle_session(&self, session: &Option<SessionState>) {
        let session = match session {
            Some(session) => session,
            None => return,
        };

        // save the updated session state from the server side
        {
            let mut session_state = self.session_state.lock();
            *session_state = session.clone();
        }

        // process warehouse changed via session settings
        if let Some(settings) = session.settings.as_ref() {
            if let Some(v) = settings.get("warehouse") {
                let mut warehouse = self.warehouse.lock();
                *warehouse = Some(v.clone());
            }
        }
    }

    pub fn set_last_node_id(&self, node_id: String) {
        *self.last_node_id.lock() = Some(node_id)
    }

    pub fn set_last_query_id(&self, query_id: Option<String>) {
        *self.last_query_id.lock() = query_id
    }

    pub fn last_query_id(&self) -> Option<String> {
        self.last_query_id.lock().clone()
    }

    fn last_node_id(&self) -> Option<String> {
        self.last_node_id.lock().clone()
    }

    fn handle_warnings(&self, resp: &QueryResponse) {
        if let Some(warnings) = &resp.warnings {
            for w in warnings {
                warn!(target: "server_warnings", "server warning: {w}");
            }
        }
    }

    pub async fn start_query(self: &Arc<Self>, sql: &str, need_progress: bool) -> Result<Pages> {
        info!("start query: {sql}");
        let (resp, batches) = self.start_query_inner(sql, None, false).await?;
        Pages::new(self.clone(), resp, batches, need_progress)
    }

    pub fn finalize_query(self: &Arc<Self>, query_id: &str) {
        let mut mgr = self.queries_need_heartbeat.lock();
        if let Some(state) = mgr.remove(query_id) {
            let self_cloned = self.clone();
            let query_id = query_id.to_owned();
            GLOBAL_RUNTIME.spawn(async move {
                if let Err(e) = self_cloned
                    .end_query(&query_id, "final", Some(state.node_id.as_str()))
                    .await
                {
                    error!("failed to final query {query_id}: {e}");
                }
            });
        }
    }

    fn wrap_auth_or_session_token(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        if let Some(info) = &self.session_token_info {
            let info = info.lock();
            Ok(builder.bearer_auth(info.0.session_token.clone()))
        } else {
            self.auth.wrap(builder)
        }
    }

    async fn start_query_inner(
        &self,
        sql: &str,
        stage_attachment_config: Option<StageAttachmentConfig<'_>>,
        force_json_body: bool,
    ) -> Result<(QueryResponse, Vec<RecordBatch>)> {
        if !self.in_active_transaction() {
            self.route_hint.next();
        }
        let endpoint = self.endpoint.join("v1/query")?;

        // body
        let session_state = self.session_state();
        let need_sticky = session_state.need_sticky.unwrap_or(false);
        let req = QueryRequest::new(sql)
            .with_pagination(self.make_pagination())
            .with_session(Some(session_state))
            .with_stage_attachment(stage_attachment_config);

        // headers
        let query_id = self.gen_query_id();
        let mut headers = self.make_headers(Some(&query_id))?;
        if self.capability.arrow_data && self.body_format == "arrow" && !force_json_body {
            debug!("accept arrow data");
            headers.insert(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_ARROW_OR_JSON));
        }

        if need_sticky {
            if let Some(node_id) = self.last_node_id() {
                headers.insert(HEADER_STICKY_NODE, node_id.parse()?);
            }
        }
        let mut builder = self.cli.post(endpoint.clone()).json(&req);
        builder = self.wrap_auth_or_session_token(builder)?;
        let request = builder.headers(headers.clone()).build()?;
        let response = self.query_request_helper(request, true, true).await?;
        self.handle_page(response, true).await
    }

    fn is_arrow_data(response: &Response) -> bool {
        if let Some(typ) = response.headers().get(CONTENT_TYPE) {
            if let Ok(t) = typ.to_str() {
                return t == CONTENT_TYPE_ARROW;
            }
        }
        false
    }

    async fn handle_page(
        &self,
        response: Response,
        is_first: bool,
    ) -> Result<(QueryResponse, Vec<RecordBatch>)> {
        let status = response.status();
        if status != 200 {
            return Err(Error::response_error(status, &response.bytes().await?));
        }
        let is_arrow_data = Self::is_arrow_data(&response);
        if is_first {
            if let Some(route_hint) = response.headers().get(HEADER_ROUTE_HINT) {
                self.route_hint.set(route_hint.to_str().unwrap_or_default());
            }
        }
        let mut body = response.bytes().await?;
        let mut batches = vec![];
        if is_arrow_data {
            if is_first {
                debug!("received arrow data");
            }
            let cursor = std::io::Cursor::new(body.as_ref());
            let reader = StreamReader::try_new(cursor, None)
                .map_err(|e| Error::Decode(format!("failed to decode arrow stream: {e}")))?;
            let schema = reader.schema();
            let json_body = if let Some(json_resp) = schema.metadata.get("response_header") {
                bytes::Bytes::copy_from_slice(json_resp.as_bytes())
            } else {
                return Err(Error::Decode(
                    "missing response_header metadata in arrow payload".to_string(),
                ));
            };
            for batch in reader {
                let batch = batch
                    .map_err(|e| Error::Decode(format!("failed to decode arrow batch: {e}")))?;
                batches.push(batch);
            }
            body = json_body
        };
        let resp: QueryResponse = json_from_slice(&body).map_err(|e| {
            if let Error::Logic(status, ec) = &e {
                if *status == 404 {
                    return Error::QueryNotFound(ec.message.clone());
                }
            }
            e
        })?;
        self.handle_session(&resp.session).await;
        if let Some(err) = &resp.error {
            return Err(Error::QueryFailed(err.clone()));
        }
        if is_first {
            self.handle_warnings(&resp);
            self.set_last_query_id(Some(resp.id.clone()));
            if let Some(node_id) = &resp.node_id {
                self.set_last_node_id(node_id.clone());
            }
        }
        Ok((resp, batches))
    }

    pub async fn query_page(
        &self,
        query_id: &str,
        next_uri: &str,
        node_id: &Option<String>,
    ) -> Result<(QueryResponse, Vec<RecordBatch>)> {
        info!("query page: {next_uri}");
        let endpoint = self.endpoint.join(next_uri)?;
        let mut headers = self.make_headers(Some(query_id))?;
        if self.capability.arrow_data && self.body_format == "arrow" {
            headers.insert(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_ARROW_OR_JSON));
        }
        let mut builder = self.cli.get(endpoint.clone());
        builder = self
            .wrap_auth_or_session_token(builder)?
            .headers(headers.clone())
            .timeout(self.page_request_timeout);
        if let Some(node_id) = node_id {
            builder = builder.header(HEADER_STICKY_NODE, node_id)
        }
        let request = builder.build()?;

        let response = self.query_request_helper(request, false, true).await?;
        self.handle_page(response, false).await
    }

    pub async fn kill_query(&self, query_id: &str) -> Result<()> {
        self.end_query(query_id, "kill", None).await
    }

    pub async fn final_query(&self, query_id: &str, node_id: Option<&str>) -> Result<()> {
        self.end_query(query_id, "final", node_id).await
    }

    pub async fn end_query(
        &self,
        query_id: &str,
        method: &str,
        node_id: Option<&str>,
    ) -> Result<()> {
        let uri = format!("/v1/query/{query_id}/{method}");
        let endpoint = self.endpoint.join(&uri)?;
        let headers = self.make_headers(Some(query_id))?;

        info!("{method} query: {uri}");

        let mut builder = self.cli.post(endpoint);
        if let Some(node_id) = node_id {
            builder = builder.header(HEADER_STICKY_NODE, node_id)
        }
        builder = self.wrap_auth_or_session_token(builder)?;
        let resp = builder.headers(headers.clone()).send().await?;
        if resp.status() != 200 {
            return Err(Error::response_error(resp.status(), &resp.bytes().await?)
                .with_context(&format!("{method} query")));
        }
        Ok(())
    }

    pub async fn query_all(self: &Arc<Self>, sql: &str) -> Result<Page> {
        self.query_all_inner(sql, false).await
    }

    pub async fn query_all_inner(
        self: &Arc<Self>,
        sql: &str,
        force_json_body: bool,
    ) -> Result<Page> {
        let (resp, batches) = self.start_query_inner(sql, None, force_json_body).await?;
        let mut pages = Pages::new(self.clone(), resp, batches, false)?;
        let mut all = Page::default();
        while let Some(page) = pages.next().await {
            all.update(page?);
        }
        Ok(all)
    }

    fn session_state(&self) -> SessionState {
        self.session_state.lock().clone()
    }

    fn make_pagination(&self) -> Option<PaginationConfig> {
        if self.wait_time_secs.is_none()
            && self.max_rows_in_buffer.is_none()
            && self.max_rows_per_page.is_none()
        {
            return None;
        }
        let mut pagination = PaginationConfig {
            wait_time_secs: None,
            max_rows_in_buffer: None,
            max_rows_per_page: None,
        };
        if let Some(wait_time_secs) = self.wait_time_secs {
            pagination.wait_time_secs = Some(wait_time_secs);
        }
        if let Some(max_rows_in_buffer) = self.max_rows_in_buffer {
            pagination.max_rows_in_buffer = Some(max_rows_in_buffer);
        }
        if let Some(max_rows_per_page) = self.max_rows_per_page {
            pagination.max_rows_per_page = Some(max_rows_per_page);
        }
        Some(pagination)
    }

    fn make_headers(&self, query_id: Option<&str>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(tenant) = &self.tenant {
            headers.insert(HEADER_TENANT, tenant.parse()?);
        }
        let warehouse = self.warehouse.lock().clone();
        if let Some(warehouse) = warehouse {
            headers.insert(HEADER_WAREHOUSE, warehouse.parse()?);
        }
        let route_hint = self.route_hint.current();
        headers.insert(HEADER_ROUTE_HINT, route_hint.parse()?);
        if let Some(query_id) = query_id {
            headers.insert(HEADER_QUERY_ID, query_id.parse()?);
        }
        Ok(headers)
    }

    pub async fn insert_with_stage(
        self: &Arc<Self>,
        sql: &str,
        stage: &str,
        file_format_options: BTreeMap<&str, &str>,
        copy_options: BTreeMap<&str, &str>,
    ) -> Result<QueryStats> {
        info!("insert with stage: {sql}, format: {file_format_options:?}, copy: {copy_options:?}");
        let stage_attachment = Some(StageAttachmentConfig {
            location: stage,
            file_format_options: Some(file_format_options),
            copy_options: Some(copy_options),
        });
        let (resp, batches) = self.start_query_inner(sql, stage_attachment, true).await?;
        let mut pages = Pages::new(self.clone(), resp, batches, false)?;
        let mut all = Page::default();
        while let Some(page) = pages.next().await {
            all.update(page?);
        }
        Ok(all.stats)
    }

    async fn get_presigned_upload_url(self: &Arc<Self>, stage: &str) -> Result<PresignedResponse> {
        info!("get presigned upload url: {stage}");
        let sql = format!("PRESIGN UPLOAD {stage}");
        let resp = self.query_all_inner(&sql, true).await?;
        if resp.data.len() != 1 {
            return Err(Error::Decode(
                "Empty response from server for presigned request".to_string(),
            ));
        }
        if resp.data[0].len() != 3 {
            return Err(Error::Decode(
                "Invalid response from server for presigned request".to_string(),
            ));
        }
        // resp.data[0]: [ "PUT", "{\"host\":\"s3.us-east-2.amazonaws.com\"}", "https://s3.us-east-2.amazonaws.com/query-storage-xxxxx/tnxxxxx/stage/user/xxxx/xxx?" ]
        let method = resp.data[0][0].clone().unwrap_or_default();
        if method != "PUT" {
            return Err(Error::Decode(format!(
                "Invalid method for presigned upload request: {method}"
            )));
        }
        let headers: BTreeMap<String, String> =
            serde_json::from_str(resp.data[0][1].clone().unwrap_or("{}".to_string()).as_str())?;
        let url = resp.data[0][2].clone().unwrap_or_default();
        Ok(PresignedResponse {
            method,
            headers,
            url,
        })
    }

    pub async fn upload_to_stage(
        self: &Arc<Self>,
        stage: &str,
        data: Reader,
        size: u64,
    ) -> Result<()> {
        match self.get_presign_mode() {
            PresignMode::Off => self.upload_to_stage_with_stream(stage, data, size).await,
            PresignMode::On => {
                let presigned = self.get_presigned_upload_url(stage).await?;
                presign_upload_to_stage(presigned, data, size).await
            }
            PresignMode::Auto => {
                unreachable!("PresignMode::Auto should be handled during client initialization")
            }
            PresignMode::Detect => {
                unreachable!("PresignMode::Detect should be handled during client initialization")
            }
        }
    }

    /// Upload data to stage with stream api, should not be used directly, use `upload_to_stage` instead.
    async fn upload_to_stage_with_stream(
        &self,
        stage: &str,
        data: Reader,
        size: u64,
    ) -> Result<()> {
        info!("upload to stage with stream: {stage}, size: {size}");
        if let Some(info) = self.need_pre_refresh_session().await {
            self.refresh_session_token(info).await?;
        }
        let endpoint = self.endpoint.join("v1/upload_to_stage")?;
        let location = StageLocation::try_from(stage)?;
        let query_id = self.gen_query_id();
        let mut headers = self.make_headers(Some(&query_id))?;
        headers.insert(HEADER_STAGE_NAME, location.name.parse()?);
        let stream = Body::wrap_stream(ReaderStream::new(data));
        let part = Part::stream_with_length(stream, size).file_name(location.path);
        let form = Form::new().part("upload", part);
        let mut builder = self.cli.put(endpoint.clone());
        builder = self.wrap_auth_or_session_token(builder)?;
        let resp = builder.headers(headers).multipart(form).send().await?;
        let status = resp.status();
        if status != 200 {
            return Err(
                Error::response_error(status, &resp.bytes().await?).with_context("upload_to_stage")
            );
        }
        Ok(())
    }

    // use base64 encode whenever possible for safety
    // but also accept raw JSON for test/debug/one-shot operations
    pub fn decode_json_header<T>(key: &str, value: &str) -> Result<T, String>
    where
        T: de::DeserializeOwned,
    {
        if value.starts_with("{") {
            serde_json::from_slice(value.as_bytes())
                .map_err(|e| format!("Invalid value {value} for {key} JSON decode error: {e}",))?
        } else {
            let json = URL_SAFE.decode(value).map_err(|e| {
                format!(
                    "Invalid value {} for {key}, base64 decode error: {}",
                    value, e
                )
            })?;
            serde_json::from_slice(&json).map_err(|e| {
                format!(
                    "Invalid value {value} for {key}, JSON value {},  decode error: {e}",
                    String::from_utf8_lossy(&json)
                )
            })
        }
    }

    pub async fn streaming_load(
        &self,
        sql: &str,
        data: Reader,
        file_name: &str,
    ) -> Result<LoadResponse> {
        let body = Body::wrap_stream(ReaderStream::new(data));
        let part = Part::stream(body).file_name(file_name.to_string());
        let endpoint = self.endpoint.join("v1/streaming_load")?;
        let mut builder = self.cli.put(endpoint.clone());
        builder = self.wrap_auth_or_session_token(builder)?;
        let query_id = self.gen_query_id();
        let mut headers = self.make_headers(Some(&query_id))?;
        headers.insert(HEADER_SQL, sql.parse()?);
        let session = serde_json::to_string(&*self.session_state.lock())
            .expect("serialize session state should not fail");
        headers.insert(HEADER_QUERY_CONTEXT, session.parse()?);
        let form = Form::new().part("upload", part);
        let resp = builder.headers(headers).multipart(form).send().await?;
        let status = resp.status();
        if let Some(value) = resp.headers().get(HEADER_QUERY_CONTEXT) {
            match Self::decode_json_header::<SessionState>(
                HEADER_QUERY_CONTEXT,
                value.to_str().unwrap(),
            ) {
                Ok(session) => *self.session_state.lock() = session,
                Err(e) => {
                    error!("Error decoding session state when streaming load: {e}");
                }
            }
        };
        if status != 200 {
            return Err(
                Error::response_error(status, &resp.bytes().await?).with_context("streaming_load")
            );
        }
        let resp = resp.json::<LoadResponse>().await?;
        Ok(resp)
    }

    async fn login(&mut self) -> Result<()> {
        let endpoint = self.endpoint.join("/v1/session/login")?;
        let headers = self.make_headers(None)?;
        let body = LoginRequest::from(&*self.session_state.lock());
        let mut builder = self.cli.post(endpoint.clone()).json(&body);
        if self.disable_session_token {
            builder = builder.query(&[("disable_session_token", true)]);
        }
        let builder = self.auth.wrap(builder)?;
        let request = builder
            .headers(headers.clone())
            .timeout(self.connect_timeout)
            .build()?;
        let response = self.query_request_helper(request, true, false).await;
        let response = match response {
            Ok(r) => r,
            Err(e) if e.status_code() == Some(StatusCode::NOT_FOUND) => {
                info!("login return 404, skip login on the old version server");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if let Some(v) = response.headers().get(HEADER_SESSION_ID) {
            if let Ok(s) = v.to_str() {
                self.session_id = s.to_string();
            }
        }

        let body = response.bytes().await?;
        let response = json_from_slice(&body)?;
        match response {
            LoginResponseResult::Err { error } => return Err(Error::AuthFailure(error)),
            LoginResponseResult::Ok(info) => {
                let server_version = info
                    .version
                    .parse()
                    .map_err(|e| Error::Decode(format!("invalid server version: {e}")))?;
                self.capability = Capability::from_server_version(&server_version);
                self.server_version = Some(server_version.clone());
                let session_id = self.session_id.as_str();
                if let Some(tokens) = info.tokens {
                    info!(
                        "[session {session_id}] login success with session token version = {server_version}",
                    );
                    self.session_token_info = Some(Arc::new(Mutex::new((tokens, Instant::now()))))
                } else {
                    info!("[session {session_id}] login success, version = {server_version}");
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn try_heartbeat(&self) -> Result<()> {
        let endpoint = self.endpoint.join("/v1/session/heartbeat")?;
        let queries = self.queries_need_heartbeat.lock().clone();
        let mut node_to_queries = HashMap::<String, Vec<String>>::new();
        let now = Instant::now();

        let mut query_ids = Vec::new();
        for (qid, state) in queries {
            if state.need_heartbeat(now) {
                query_ids.push(qid.to_string());
                if let Some(arr) = node_to_queries.get_mut(&state.node_id) {
                    arr.push(qid);
                } else {
                    node_to_queries.insert(state.node_id, vec![qid]);
                }
            }
        }

        if node_to_queries.is_empty() && !self.session_state.lock().need_sticky.unwrap_or_default()
        {
            return Ok(());
        }

        let body = json!({
           "node_to_queries": node_to_queries
        });
        let builder = self.cli.post(endpoint.clone()).json(&body);
        let request = self.wrap_auth_or_session_token(builder)?.build()?;
        let response = self.query_request_helper(request, true, false).await?;
        let json: Value = response.json().await?;
        let session_id = self.session_id.as_str();
        info!("[session {session_id}] heartbeat request={body}, response={json}");
        if let Some(queries_to_remove) = json.get("queries_to_remove") {
            if let Some(arr) = queries_to_remove.as_array() {
                if !arr.is_empty() {
                    let mut queries = self.queries_need_heartbeat.lock();
                    for q in arr {
                        if let Some(q) = q.as_str() {
                            queries.remove(q);
                        }
                    }
                }
            }
        }
        let now = Instant::now();
        let mut queries = self.queries_need_heartbeat.lock();
        for qid in query_ids {
            if let Some(state) = queries.get_mut(&qid) {
                *state.last_access_time.lock() = now;
            }
        }
        Ok(())
    }

    fn build_log_out_request(&self) -> Result<Request> {
        let endpoint = self.endpoint.join("/v1/session/logout")?;

        let session_state = self.session_state();
        let need_sticky = session_state.need_sticky.unwrap_or(false);
        let mut headers = self.make_headers(None)?;
        if need_sticky {
            if let Some(node_id) = self.last_node_id() {
                headers.insert(HEADER_STICKY_NODE, node_id.parse()?);
            }
        }
        let builder = self.cli.post(endpoint.clone()).headers(headers.clone());

        let builder = self.wrap_auth_or_session_token(builder)?;
        let req = builder.build()?;
        Ok(req)
    }

    pub(crate) fn need_logout(&self) -> bool {
        self.session_token_info.is_some()
            || self.session_state.lock().need_keep_alive.unwrap_or(false)
    }

    async fn refresh_session_token(
        &self,
        self_login_info: Arc<parking_lot::Mutex<(SessionTokenInfo, Instant)>>,
    ) -> Result<()> {
        let (session_token_info, _) = { self_login_info.lock().clone() };
        let endpoint = self.endpoint.join("/v1/session/refresh")?;
        let body = RefreshSessionTokenRequest {
            session_token: session_token_info.session_token.clone(),
        };
        let headers = self.make_headers(None)?;
        let request = self
            .cli
            .post(endpoint.clone())
            .json(&body)
            .headers(headers.clone())
            .bearer_auth(session_token_info.refresh_token.clone())
            .timeout(self.connect_timeout)
            .build()?;

        // avoid recursively call request_helper
        for i in 0..3 {
            let req = request.try_clone().expect("request not cloneable");
            match self.cli.execute(req).await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.bytes().await?;
                    if status == StatusCode::OK {
                        let response = json_from_slice(&body)?;
                        return match response {
                            RefreshResponse::Err { error } => Err(Error::AuthFailure(error)),
                            RefreshResponse::Ok(info) => {
                                *self_login_info.lock() = (info, Instant::now());
                                Ok(())
                            }
                        };
                    }
                    if status != StatusCode::SERVICE_UNAVAILABLE || i >= 2 {
                        return Err(Error::response_error(status, &body));
                    }
                }
                Err(err) => {
                    if !(err.is_timeout() || err.is_connect()) || i > 2 {
                        return Err(Error::Request(err.to_string()));
                    }
                }
            };
            sleep(jitter(Duration::from_secs(10))).await;
        }
        Ok(())
    }

    async fn need_pre_refresh_session(&self) -> Option<Arc<Mutex<(SessionTokenInfo, Instant)>>> {
        if let Some(info) = &self.session_token_info {
            let (start, ttl) = {
                let guard = info.lock();
                (guard.1, guard.0.session_token_ttl_in_secs)
            };
            if Instant::now() > start + Duration::from_secs(ttl) {
                return Some(info.clone());
            }
        }
        None
    }

    /// return Ok if and only if status code is 200.
    ///
    /// retry on
    ///   - network errors
    ///   - (optional) 503
    ///
    /// refresh databend token or reload jwt token if needed.
    async fn query_request_helper(
        &self,
        mut request: Request,
        retry_if_503: bool,
        refresh_if_401: bool,
    ) -> std::result::Result<Response, Error> {
        let mut refreshed = false;
        let mut retries = 0;
        loop {
            let req = request.try_clone().expect("request not cloneable");
            let (err, retry): (Error, bool) = match self.cli.execute(req).await {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::OK {
                        return Ok(response);
                    }
                    let body = response.bytes().await?;
                    if retry_if_503 && status == StatusCode::SERVICE_UNAVAILABLE {
                        // waiting for server to start
                        (Error::response_error(status, &body), true)
                    } else {
                        let resp = serde_json::from_slice::<ResponseWithErrorCode>(&body);
                        match resp {
                            Ok(r) => {
                                let e = r.error;
                                if status == StatusCode::UNAUTHORIZED {
                                    request.headers_mut().remove(reqwest::header::AUTHORIZATION);
                                    if let Some(session_token_info) = &self.session_token_info {
                                        info!(
                                            "will retry {} after refresh token on auth error {}",
                                            request.url(),
                                            e
                                        );
                                        let retry = if need_refresh_token(e.code)
                                            && !refreshed
                                            && refresh_if_401
                                        {
                                            self.refresh_session_token(session_token_info.clone())
                                                .await?;
                                            refreshed = true;
                                            true
                                        } else {
                                            false
                                        };
                                        (Error::AuthFailure(e), retry)
                                    } else if self.auth.can_reload() {
                                        info!(
                                            "will retry {} after reload token on auth error {}",
                                            request.url(),
                                            e
                                        );
                                        let builder = RequestBuilder::from_parts(
                                            HttpClient::new(),
                                            request.try_clone().unwrap(),
                                        );
                                        let builder = self.auth.wrap(builder)?;
                                        request = builder.build()?;
                                        (Error::AuthFailure(e), true)
                                    } else {
                                        (Error::AuthFailure(e), false)
                                    }
                                } else {
                                    (Error::Logic(status, e), false)
                                }
                            }
                            Err(_) => (
                                Error::Response {
                                    status,
                                    msg: String::from_utf8_lossy(&body).to_string(),
                                },
                                false,
                            ),
                        }
                    }
                }
                Err(err) => (
                    Error::Request(err.to_string()),
                    err.is_timeout() || err.is_connect() || err.is_request(),
                ),
            };
            if !retry {
                return Err(err.with_context(&format!("{} {}", request.method(), request.url())));
            }
            match &err {
                Error::AuthFailure(_) => {
                    if refreshed {
                        retries = 0;
                    } else if retries == 2 {
                        return Err(err.with_context(&format!(
                            "{} {} after 3 retries",
                            request.method(),
                            request.url()
                        )));
                    }
                }
                _ => {
                    if retries == 2 {
                        return Err(err.with_context(&format!(
                            "{} {} after 3 reties",
                            request.method(),
                            request.url()
                        )));
                    }
                    retries += 1;
                    info!(
                        "will retry {} the {retries}th times on error {}",
                        request.url(),
                        err
                    );
                }
            }
            warn!("will retry after 10 seconds");
            sleep(jitter(Duration::from_secs(10))).await;
        }
    }

    pub async fn logout(http_client: HttpClient, request: Request, session_id: &str) {
        if let Err(err) = http_client.execute(request).await {
            error!("[session {session_id}] logout request failed: {err}");
        } else {
            info!("[session {session_id}] logout success");
        };
    }

    pub async fn close(&self) {
        let session_id = &self.session_id;
        info!("[session {session_id}] try closing now");
        if self
            .closed
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            GLOBAL_CLIENT_MANAGER.unregister_client(&self.session_id);
            if self.need_logout() {
                let cli = self.cli.clone();
                let req = self
                    .build_log_out_request()
                    .expect("failed to build logout request");
                Self::logout(cli, req, &self.session_id).await;
            }
        }
    }
    pub fn close_with_spawn(&self) {
        let session_id = &self.session_id;
        info!("[session {session_id}]: try closing with spawn");
        if self
            .closed
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            GLOBAL_CLIENT_MANAGER.unregister_client(&self.session_id);
            if self.need_logout() {
                let cli = self.cli.clone();
                let req = self
                    .build_log_out_request()
                    .expect("failed to build logout request");
                let session_id = self.session_id.clone();
                GLOBAL_RUNTIME.spawn(async move {
                    Self::logout(cli, req, session_id.as_str()).await;
                });
            }
        }
    }

    pub(crate) fn register_query_for_heartbeat(&self, query_id: &str, state: QueryState) {
        let mut queries = self.queries_need_heartbeat.lock();
        queries.insert(query_id.to_string(), state);
    }
}

fn json_from_slice<'a, T>(body: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    serde_json::from_slice::<T>(body).map_err(|e| {
        Error::Decode(format!(
            "fail to decode JSON response: {e}, body: {}",
            String::from_utf8_lossy(body)
        ))
    })
}

impl Default for APIClient {
    fn default() -> Self {
        Self {
            session_id: Default::default(),
            cli: HttpClient::new(),
            scheme: "http".to_string(),
            endpoint: Url::parse("http://localhost:8080").unwrap(),
            host: "localhost".to_string(),
            port: 8000,
            tenant: None,
            warehouse: Mutex::new(None),
            auth: Arc::new(BasicAuth::new("root", "")) as Arc<dyn Auth>,
            session_state: Mutex::new(SessionState::default()),
            wait_time_secs: None,
            max_rows_in_buffer: None,
            max_rows_per_page: None,
            connect_timeout: Duration::from_secs(10),
            page_request_timeout: Duration::from_secs(30),
            tls_ca_file: None,
            presign: Mutex::new(PresignMode::Auto),
            route_hint: RouteHintGenerator::new(),
            last_node_id: Default::default(),
            disable_session_token: true,
            disable_login: false,
            body_format: "json".to_string(),
            session_token_info: None,
            closed: AtomicBool::new(false),
            last_query_id: Default::default(),
            server_version: None,
            capability: Default::default(),
            queries_need_heartbeat: Default::default(),
        }
    }
}

struct RouteHintGenerator {
    nonce: AtomicU64,
    current: std::sync::Mutex<String>,
}

impl RouteHintGenerator {
    fn new() -> Self {
        let gen = Self {
            nonce: AtomicU64::new(0),
            current: std::sync::Mutex::new("".to_string()),
        };
        gen.next();
        gen
    }

    fn current(&self) -> String {
        let guard = self.current.lock().unwrap();
        guard.clone()
    }

    fn set(&self, hint: &str) {
        let mut guard = self.current.lock().unwrap();
        *guard = hint.to_string();
    }

    fn next(&self) -> String {
        let nonce = self.nonce.fetch_add(1, Ordering::AcqRel);
        let uuid = uuid::Uuid::new_v4();
        let current = format!("rh:{uuid}:{nonce:06}");
        let mut guard = self.current.lock().unwrap();
        guard.clone_from(&current);
        current
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn parse_dsn() -> Result<()> {
        let dsn = "databend://username:password@app.databend.com/test?wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&warehouse=wh&sslmode=disable";
        let client = APIClient::from_dsn(dsn).await?;
        assert_eq!(client.host, "app.databend.com");
        assert_eq!(client.endpoint, Url::parse("http://app.databend.com:80")?);
        assert_eq!(client.wait_time_secs, Some(10));
        assert_eq!(client.max_rows_in_buffer, Some(5000000));
        assert_eq!(client.max_rows_per_page, Some(10000));
        assert_eq!(client.tenant, None);
        assert_eq!(
            *client.warehouse.try_lock().unwrap(),
            Some("wh".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_encoded_password() -> Result<()> {
        let dsn = "databend://username:3a%40SC(nYE1k%3D%7B%7BR@localhost";
        let client = APIClient::from_dsn(dsn).await?;
        assert_eq!(client.host(), "localhost");
        assert_eq!(client.port(), 443);
        Ok(())
    }

    #[tokio::test]
    async fn parse_special_chars_password() -> Result<()> {
        let dsn = "databend://username:3a@SC(nYE1k={{R@localhost:8000";
        let client = APIClient::from_dsn(dsn).await?;
        assert_eq!(client.host(), "localhost");
        assert_eq!(client.port(), 8000);
        Ok(())
    }
}
