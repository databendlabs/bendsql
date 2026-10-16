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

use log::LevelFilter;
use std::io::BufWriter;
use std::io::Write;
use std::str::FromStr;

use anyhow::Result;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;

const MAX_LOG_FILES: usize = 10;
static AI_ACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn protect_ai_payloads() {
    AI_ACTIVE.store(true, std::sync::atomic::Ordering::SeqCst);
}

fn log_message<'a>(target: &str, message: &'a str, ai_active: bool) -> std::borrow::Cow<'a, str> {
    use once_cell::sync::Lazy;
    use regex::Regex;
    if !ai_active {
        return std::borrow::Cow::Borrowed(message);
    }
    // Hide query/payload values, not diagnostic records. SQL-only and batch
    // logs never enter this path. Non-payload authentication errors stay useful.
    if target.starts_with("databend_") {
        for prefix in ["start query: ", "insert with stage: "] {
            if let Some(at) = message.find(prefix) {
                return std::borrow::Cow::Owned(format!(
                    "{}[SQL payload omitted]",
                    &message[..at + prefix.len()]
                ));
            }
        }
    }
    static ASSIGNMENT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?ix)\b(password|secret_access_key|access_key_id|aws_secret_key|api_key|api_token|client_secret|refresh_token|access_token|session_token|token|secret|authorization)\b(["']?\s*[:=]\s*)("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|(?:(?:Bearer|Basic)\s+)?[^\s,\]}&]+)"#).unwrap()
    });
    let mut output = ASSIGNMENT
        .replace_all(message, |cap: &regex::Captures<'_>| {
            let value = &cap[3];
            let replacement = if value.starts_with('"') {
                "\"<redacted>\""
            } else if value.starts_with('\'') {
                "'<redacted>'"
            } else {
                "<redacted>"
            };
            format!("{}{}{replacement}", &cap[1], &cap[2])
        })
        .into_owned();
    static PAYLOAD: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)\b(request|response|body|payload|params|result|messages|prompt|content|data|rows)\b["']?\s*[:=]\s*"#).unwrap()
    });
    static USERINFO: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)([a-z][a-z0-9+.-]*://)[^/\s@]+@").unwrap());
    output = USERINFO
        .replace_all(&output, "${1}<redacted>@")
        .into_owned();
    let mut cursor = 0;
    let mut protected = String::new();
    while let Some(found) = PAYLOAD.find_at(&output, cursor) {
        protected.push_str(&output[cursor..found.end()]);
        let value = &output[found.end()..];
        if !value.starts_with(['{', '[', '"', '\'']) {
            cursor = found.end();
            continue;
        }
        let end = payload_end(value);
        protected.push_str("\"<redacted>\"");
        cursor = found.end() + end;
    }
    protected.push_str(&output[cursor..]);
    output = protected;
    std::borrow::Cow::Owned(output)
}

fn payload_end(value: &str) -> usize {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut escape = false;
    let mut depth: usize = 0;
    for (i, &byte) in bytes.iter().enumerate() {
        if let Some(delimiter) = quote {
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == delimiter {
                quote = None;
                if depth == 0 {
                    return i + 1;
                }
            }
        } else {
            match byte {
                b'"' | b'\'' => quote = Some(byte),
                b'{' | b'[' => depth += 1,
                b'}' | b']' if depth > 0 => {
                    depth -= 1;
                    if depth == 0 {
                        return i + 1;
                    }
                }
                b',' | b' ' | b'\n' | b'\r' if depth == 0 => return i,
                _ => {}
            }
        }
    }
    value.len()
}

#[allow(dyn_drop)]
pub async fn init_logging(
    dir: &str,
    level: &str,
) -> Result<Vec<Box<dyn Drop + Send + Sync + 'static>>> {
    let mut guards: Vec<Box<dyn Drop + Send + Sync + 'static>> = Vec::new();
    let mut logger = fern::Dispatch::new();

    let rolling = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("bendsql.log")
        .max_log_files(MAX_LOG_FILES)
        .build(dir)?;
    let (non_blocking, flush_guard) = tracing_appender::non_blocking(rolling);
    let buffered_non_blocking = BufWriter::with_capacity(64 * 1024 * 1024, non_blocking);

    guards.push(Box::new(flush_guard));
    let dispatch_file = fern::Dispatch::new()
        .format(|out, message, record| {
            let message = message.to_string();
            let text = log_message(
                record.target(),
                &message,
                AI_ACTIVE.load(std::sync::atomic::Ordering::SeqCst),
            );
            out.finish(format_args!(
                "[{}] - {} - [{}] {}",
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                record.level(),
                record.target(),
                text
            ))
        })
        .level(LevelFilter::from_str(level)?)
        .chain(Box::new(buffered_non_blocking) as Box<dyn Write + Send>);
    logger = logger.chain(dispatch_file);

    let dispatch_stderr = fern::Dispatch::new()
        .level(LevelFilter::Warn)
        .filter(|metadata| metadata.target() == "server_warnings")
        .format(|out, message, _| {
            out.finish(format_args!(
                "\x1B[{}m{}\x1B[0m",
                fern::colors::Color::Yellow.to_fg_str(),
                message
            ))
        })
        .chain(std::io::stderr());
    logger = logger.chain(dispatch_stderr);

    if logger.apply().is_err() {
        eprintln!("logger has already been set");
        return Ok(Vec::new());
    }

    Ok(guards)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_and_batch_logging_are_byte_for_byte_unchanged_without_ai() {
        for message in [
            "start query: SELECT token FROM t",
            "connection failed; authorization token refresh",
            "password='original'",
            "CREATE STAGE s",
            r#"response={"token":"original"}"#,
        ] {
            assert_eq!(
                log_message("databend_client::client", message, false),
                message
            );
        }
    }
    #[test]
    fn ai_redaction_preserves_diagnostics_and_hides_only_values() {
        let status = "token refresh failed: connection refused (401), authorization expired";
        assert_eq!(log_message("databend_client::client", status, true), status);
        assert_eq!(
            log_message(
                "databend_client::client",
                "start query: SELECT secret FROM vault",
                true
            ),
            "start query: [SQL payload omitted]"
        );
        let log = r#"[session abc] heartbeat request={"sql":"private-query"}, response={"rows":["private-result"]}, status=503; token='private-token'"#;
        let redacted = log_message("databend_client::client", log, true);
        for structure in [
            "[session abc]",
            "heartbeat request=",
            ", response=",
            "status=503",
            "token=",
        ] {
            assert!(redacted.contains(structure));
        }
        for secret in ["private-query", "private-result", "private-token"] {
            assert!(!redacted.contains(secret));
        }
        let auth = log_message(
            "provider",
            "Authorization: Bearer private-key, status=401 uri=https://user:private@host/path",
            true,
        );
        assert!(!auth.contains("private"));
        assert!(
            auth.contains("Authorization:")
                && auth.contains("status=401")
                && auth.contains("@host/path")
        );
        assert_eq!(
            log_message(
                "databend_client",
                "request failed: connection refused; response=503",
                true
            ),
            "request failed: connection refused; response=503"
        );
    }
}
