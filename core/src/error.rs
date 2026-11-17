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

const SSLMODE_DISABLE_HINT: &str = "the server may be using plain HTTP; if TLS is not enabled on the server, set sslmode=disable in the DSN";

pub(crate) fn suggests_sslmode_disable(e: &reqwest::Error) -> bool {
    let mut current = e.source();
    while let Some(source) = current {
        if is_plain_http_tls_error(&source.to_string()) {
            return true;
        }
        current = source.source();
    }
    false
}

/// Whether a TLS error message indicates that the peer answered with plain HTTP.
fn is_plain_http_tls_error(message: &str) -> bool {
    let rustls_plain_http =
        message.contains("InvalidContentType") && message.contains("received corrupt message");
    // OpenSSL reads the "HTTP/1.1 ..." response bytes as a TLS record header.
    // Depending on the version, it rejects the version field ("HTTP" -> 0x5454)
    // or the length field ("P/" -> 0x502F) first, and the function name in the
    // message changes across versions:
    // - 1.1.1 to 3.1: `ssl3_get_record:wrong version number`
    // - 3.2 to 3.5.5 and 3.6.0 to 3.6.1: `tls_get_more_records:packet length too long`
    // - 3.5.6, 3.6.2 and later: `tls_validate_record_header:wrong version number`
    let openssl_plain_http = message.contains("SSL routines")
        && (message.contains(":wrong version number")
            || message.contains(":packet length too long"));
    // Secure Transport (native-tls on macOS).
    let secure_transport_plain_http = message == "record overflow";
    rustls_plain_http || openssl_plain_http || secure_transport_plain_http
}

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
        let add_sslmode_hint = suggests_sslmode_disable(&e);
        let mut source_chain = String::new();
        let mut current = e.source();
        if current.is_some() {
            source_chain.push_str(", source_chain=");
        }
        let mut first = true;
        while let Some(source) = current {
            if !first {
                source_chain.push_str(" -> ");
            }
            first = false;
            source_chain.push_str(&source.to_string());
            current = source.source();
        }
        let hint = if add_sslmode_hint {
            format!(", hint={SSLMODE_DISABLE_HINT}")
        } else {
            String::new()
        };
        Error::Request(format!("reqwest::Error: {}{}{}", e, source_chain, hint))
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

#[cfg(test)]
mod tests {
    use super::is_plain_http_tls_error;

    #[test]
    fn detects_plain_http_tls_errors() {
        let messages = [
            // rustls
            "received corrupt message of type InvalidContentType",
            // OpenSSL 1.1.1
            "error:1408F10B:SSL routines:ssl3_get_record:wrong version number:ssl/record/ssl3_record.c:332:",
            // OpenSSL 3.0
            "error:0A00010B:SSL routines:ssl3_get_record:wrong version number:../ssl/record/ssl3_record.c:354:",
            // OpenSSL 3.2 to 3.5.5, 3.6.0 to 3.6.1
            "error:0A0000C6:SSL routines:tls_get_more_records:packet length too long:../ssl/record/methods/tls_common.c:660:, error:0A000139:SSL routines::record layer failure:../ssl/record/rec_layer_s3.c:698:",
            // OpenSSL 3.5.6, 3.6.2 and later
            "error:0A00010B:SSL routines:tls_validate_record_header:wrong version number:ssl/record/methods/tlsany_meth.c:78:",
            // Secure Transport
            "record overflow",
        ];
        for message in messages {
            assert!(is_plain_http_tls_error(message), "{message}");
        }
    }

    #[test]
    fn ignores_other_tls_errors() {
        let messages = [
            "error:0A000086:SSL routines:tls_post_process_server_certificate:certificate verify failed:../ssl/statem/statem_clnt.c:2102:",
            "invalid peer certificate: UnknownIssuer",
            "connection refused",
        ];
        for message in messages {
            assert!(!is_plain_http_tls_error(message), "{message}");
        }
    }
}
