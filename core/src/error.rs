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

use crate::error_code::ErrorCode;
use reqwest::StatusCode;
use std::error::Error as StdError;

#[derive(Debug, Clone)]
pub enum RequestKind {
    QueryStart,
    QueryPage,
    QueryKill,
    QueryFinal,
    UploadToStage,
    StreamingLoad,
    Login,
    Heartbeat,
    SessionRefresh,
    Other(String),
}

impl RequestKind {
    fn as_str(&self) -> &str {
        match self {
            Self::QueryStart => "query/start",
            Self::QueryPage => "query/page",
            Self::QueryKill => "query/kill",
            Self::QueryFinal => "query/final",
            Self::UploadToStage => "upload_to_stage",
            Self::StreamingLoad => "streaming_load",
            Self::Login => "login",
            Self::Heartbeat => "heartbeat",
            Self::SessionRefresh => "session/refresh",
            Self::Other(v) => v.as_str(),
        }
    }
}

impl std::fmt::Display for RequestKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for RequestKind {
    fn from(value: &str) -> Self {
        match value {
            "query/start" => Self::QueryStart,
            "query/page" => Self::QueryPage,
            "query/kill" => Self::QueryKill,
            "query/final" => Self::QueryFinal,
            "upload_to_stage" => Self::UploadToStage,
            "streaming_load" => Self::StreamingLoad,
            "login" => Self::Login,
            "heartbeat" => Self::Heartbeat,
            "session/refresh" => Self::SessionRefresh,
            other => Self::Other(other.to_string()),
        }
    }
}

impl From<String> for RequestKind {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

#[derive(Debug)]
pub enum Error {
    WithContext {
        inner: Box<Error>,
        request_kind: Option<RequestKind>,
        query_id: Option<String>,
        retry_times: Option<u32>,
    },

    /// errors detected before sending request.
    /// e.g. invalid DSN, header value, stage name.
    BadArgument(String),
    /// errors when
    /// 1. accessing local file and presign_url
    /// 2. From(std::io::Error)
    IO(String),

    /// send request error
    Request(String),

    /// http handler return 200, but body is invalid
    /// 1. failed to decode body to Utf8 or JSON
    /// 2. failed to decode result data
    Decode(String),

    /// http handler return 200, but query failed (.error != null)
    QueryFailed(ErrorCode),

    /// http handler return non-200, with JSON body of type QueryError.
    Logic(StatusCode, ErrorCode),

    /// other non-200 response
    Response {
        status: StatusCode,
        msg: String,
    },

    /// the following are more detail type of Logic
    ///
    /// possible reasons:
    ///  - expired: if you have not polled the next_page_uri for too long, the session will be expired, you'll get a 404
    ///    on accessing this next page uri.
    ///  - routed to another server
    ///  - server restarted
    ///
    /// TODO: try to distinguish them
    QueryNotFound(String),
    AuthFailure(ErrorCode),
}

impl Error {
    pub fn response_error(status: StatusCode, body: &[u8]) -> Self {
        Self::Response {
            status,
            msg: String::from_utf8_lossy(body).to_string(),
        }
    }

    pub fn with_context(self, request_kind: impl Into<RequestKind>) -> Self {
        Self::WithContext {
            inner: Box::new(self),
            request_kind: Some(request_kind.into()),
            query_id: None,
            retry_times: None,
        }
    }

    pub fn with_query_id(self, query_id: impl Into<String>) -> Self {
        match self {
            Self::WithContext {
                inner,
                request_kind,
                retry_times,
                ..
            } => Self::WithContext {
                inner,
                request_kind,
                query_id: Some(query_id.into()),
                retry_times,
            },
            other => Self::WithContext {
                inner: Box::new(other),
                request_kind: None,
                query_id: Some(query_id.into()),
                retry_times: None,
            },
        }
    }

    pub fn with_retry_times(self, retry_times: u32) -> Self {
        match self {
            Self::WithContext {
                inner,
                request_kind,
                query_id,
                ..
            } => Self::WithContext {
                inner,
                request_kind,
                query_id,
                retry_times: Some(retry_times),
            },
            other => Self::WithContext {
                inner: Box::new(other),
                request_kind: None,
                query_id: None,
                retry_times: Some(retry_times),
            },
        }
    }

    pub fn status_code(&self) -> Option<StatusCode> {
        match self {
            Self::Logic(status, ..) => Some(*status),
            Self::Response { status, .. } => Some(*status),
            Self::WithContext { inner, .. } => inner.status_code(),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(msg) => write!(f, "DecodeError: {msg}"),
            Self::BadArgument(msg) => write!(f, "BadArgument: {msg}"),
            Self::Request(msg) => write!(f, "{msg}"),
            Self::Response { msg, status } => write!(f, "ResponseError: ({status}){msg}"),
            Self::IO(msg) => write!(f, "IOError: {msg}"),
            Self::Logic(status_code, ec) => write!(f, "BadRequest:({status_code}){ec}"),
            Self::QueryNotFound(msg) => write!(f, "QueryNotFound: {msg}"),
            Self::QueryFailed(ec) => write!(f, "QueryFailed: {ec}"),
            Self::AuthFailure(ec) => write!(f, "AuthFailure: {ec}"),
            Self::WithContext {
                inner,
                request_kind,
                query_id,
                retry_times,
            } => {
                write!(f, "[")?;
                if let Some(v) = request_kind {
                    write!(f, "request_kind={v}")?;
                }
                if let Some(v) = query_id {
                    write!(f, " query_id={v}")?;
                }
                if let Some(v) = retry_times {
                    if *v > 1 {
                        write!(f, " retry_times={v}")?;
                    }
                }
                write!(f, "]: {inner}")
            }
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = core::result::Result<T, E>;

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::Decode(e.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::Decode(e.to_string())
    }
}

/// only used in make_headers
impl From<reqwest::header::InvalidHeaderValue> for Error {
    fn from(e: reqwest::header::InvalidHeaderValue) -> Self {
        Error::BadArgument(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Decode(e.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        let e = e.without_url();
        let source = e
            .source()
            .map(|s| format!(", source={}", s))
            .unwrap_or_default();
        Error::Request(format!("reqwest::Error: {}{}", e, source))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Decode(e.to_string())
    }
}
