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

use std::io::BufRead;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

use crate::ast::quote_string_in_box_display;
use crate::ast::QueryKind;
use crate::config::ExpandMode;
use crate::config::Settings;
use crate::config::TimeOption;
use crate::display::INTERRUPTED_MESSAGE;
use crate::display::{format_write_progress, ChunkDisplay, FormatDisplay};
use crate::helper::CliHelper;
use crate::web::start_server;
use crate::VERSION;
use anyhow::anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use chrono::NaiveDateTime;
use databend_common_ast::parser::all_reserved_keywords;
use databend_common_ast::parser::token::TokenKind;
use databend_common_ast::parser::token::Tokenizer;
use databend_driver::{Client, Connection, LoadMethod, ServerStats, TryFromRow};
use log::error;
use once_cell::sync::Lazy;
use rustyline::config::Builder;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{CompletionType, Editor};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::fs::{remove_file, File};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_stream::StreamExt;

static PROMPT_SQL: &str = "select name, 'f' as type from system.functions union all select name, 'd' as type from system.databases union all select name, 't' as type from system.tables union all select name, 'c' as type from system.columns limit 10000";

static VERSION_SHORT: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    let sha = option_env!("VERGEN_GIT_SHA").unwrap_or("dev");
    match option_env!("BENDSQL_BUILD_INFO") {
        Some(info) => format!("{version}-{info}"),
        None => format!("{version}-{sha}"),
    }
});

pub struct Session {
    client: Client,
    pub conn: Connection,
    is_repl: bool,

    settings: Settings,
    query: String,

    server_handle: Option<JoinHandle<std::io::Result<()>>>,
    server_addr: Option<String>,

    keywords: Option<Arc<sled::Db>>,
    interrupted: Arc<AtomicBool>,
}

impl Session {
    pub async fn try_new(dsn: String, settings: Settings, is_repl: bool) -> Result<Self> {
        let client = Client::new(dsn).with_name(format!("bendsql/{}", VERSION_SHORT.as_str()));
        let conn = client.get_conn().await?;
        let info = conn.info().await;
        let mut keywords: Option<Arc<sled::Db>> = None;

        if is_repl {
            println!("Welcome to BendSQL {}.", VERSION.as_str());
            match info.warehouse {
                Some(ref warehouse) => {
                    println!(
                        "Connecting to {}:{} with warehouse {} as user {}",
                        info.host, info.port, warehouse, info.user
                    );
                }
                None => {
                    println!(
                        "Connecting to {}:{} as user {}.",
                        info.host, info.port, info.user
                    );
                }
            }
            let version = match conn.version().await {
                Ok(version) => version,
                Err(err) => {
                    match err {
                        databend_driver::Error::Api(databend_client::Error::AuthFailure(_)) => {
                            return Err(err.into());
                        }
                        databend_driver::Error::Arrow(arrow::error::ArrowError::IpcError(
                            ref ipc_err,
                        )) => {
                            if ipc_err.contains("Unauthenticated")
                                || ipc_err.contains("Connection refused")
                            {
                                return Err(err.into());
                            }
                        }
                        databend_driver::Error::Api(databend_client::Error::Request(
                            ref resp_err,
                        )) => {
                            if resp_err.contains("error sending request for url") {
                                return Err(err.into());
                            }
                        }
                        _ => {}
                    }
                    "".to_string()
                }
            };
            println!("Connected to {version}");

            let config = sled::Config::new().temporary(true);
            let db = config.open()?;
            // ast keywords
            {
                let mut keywords = all_reserved_keywords();
                keywords.push("GENDATA".to_string());
                let mut batch = sled::Batch::default();
                for word in keywords {
                    batch.insert(word.to_ascii_lowercase().as_str(), "k")
                }
                db.apply_batch(batch)?;
            }
            // server keywords
            if !settings.no_auto_complete {
                let rows = conn.query_iter(PROMPT_SQL, ()).await;
                match rows {
                    Ok(mut rows) => {
                        let mut count = 0;
                        let mut batch = sled::Batch::default();
                        while let Some(Ok(row)) = rows.next().await {
                            let (w, t): (String, String) = row.try_into().unwrap();
                            batch.insert(w.as_str(), t.as_str());
                            count += 1;
                            if count % 1000 == 0 {
                                db.apply_batch(batch)?;
                                batch = sled::Batch::default();
                            }
                        }
                        db.apply_batch(batch)?;
                        println!("Loaded {} auto complete keywords from server.", db.len());
                    }
                    Err(e) => {
                        eprintln!("WARN: loading auto complete keywords failed: {e}");
                    }
                }
            }
            keywords = Some(Arc::new(db));
        }

        let mut server_handle = None;
        let mut server_addr = None;
        if is_repl {
            let listener =
                TcpListener::bind(format!("{}:{}", settings.bind_address, settings.bind_port))
                    .unwrap();
            let addr = listener.local_addr().unwrap();
            let handle = tokio::spawn(async move { start_server(listener).await });
            println!("Started web server at {addr}");
            server_addr = Some(addr.to_string());
            server_handle = Some(handle);
        };

        let interrupted = Arc::new(AtomicBool::new(false));
        let interrupted_clone = interrupted.clone();

        if is_repl {
            println!();

            // Register the Ctrl+C handler
            ctrlc::set_handler(move || {
                interrupted_clone.store(true, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");
        }

        Ok(Self {
            client,
            conn,
            is_repl,
            settings,
            query: String::new(),
            keywords,
            server_handle,
            server_addr,
            interrupted,
        })
    }

    async fn prompt(&self) -> String {
        if !self.query.trim().is_empty() {
            "> ".to_owned()
        } else {
            let info = self.conn.info().await;
            let mut prompt = self.settings.prompt.clone();
            prompt = prompt.replace("{host}", &info.host);
            prompt = prompt.replace("{user}", &info.user);
            prompt = prompt.replace("{port}", &info.port.to_string());
            if let Some(catalog) = &info.catalog {
                prompt = prompt.replace("{catalog}", catalog);
            } else {
                prompt = prompt.replace("{catalog}", "default");
            }
            if let Some(database) = &info.database {
                prompt = prompt.replace("{database}", database);
            } else {
                prompt = prompt.replace("{database}", "default");
            }
            if let Some(warehouse) = &info.warehouse {
                prompt = prompt.replace("{warehouse}", &format!("({warehouse})"));
            } else {
                prompt = prompt.replace("{warehouse}", &format!("{}:{}", info.host, info.port));
            }
            format!("{} ", prompt.trim_end())
        }
    }

    pub async fn check(&mut self) -> Result<()> {
        // bendsql version
        {
            println!("BendSQL {}", VERSION.as_str());
        }

        // basic connection info
        {
            let info = self.conn.info().await;
            println!(
                "Checking Databend Query server via {} at {}:{} as user {}.",
                info.handler, info.host, info.port, info.user
            );
            if let Some(warehouse) = &info.warehouse {
                println!("Using Databend Cloud warehouse: {warehouse}");
            }
            if let Some(database) = &info.database {
                println!("Current database: {database}");
            } else {
                println!("Current database: default");
            }
        }

        // server version
        {
            let version = self.conn.version().await.unwrap_or_default();
            println!("Server version: {version}");
        }

        #[derive(TryFromRow)]
        struct LicenseInfo {
            license_issuer: String,
            license_type: String,
            organization: String,
            issued_at: NaiveDateTime,
            expire_at: NaiveDateTime,
            available_time_until_expiry: String,
            features: String,
        }

        // license info
        match self.conn.query_iter("call admin$license_info()", ()).await {
            Ok(mut rows) => {
                let row = rows.next().await.unwrap()?;
                let linfo: LicenseInfo = row
                    .try_into()
                    .map_err(|e| anyhow!("parse license info failed: {e}"))?;
                if chrono::Utc::now().naive_utc() > linfo.expire_at {
                    eprintln!("-> WARN: License expired at {}", linfo.expire_at);
                } else {
                    println!(
                        "License({}) issued by [{}] for [{}]",
                        linfo.license_type, linfo.license_issuer, linfo.organization,
                    );
                    println!("  Issued at: {}", linfo.issued_at);
                    println!("  Expire at: {}", linfo.expire_at);
                    println!("  Features: {}", linfo.features);
                    println!(
                        "  Available time until expiry: {}",
                        linfo.available_time_until_expiry
                    );
                }
            }
            Err(_) => {
                eprintln!("-> WARN: License not available, only community features enabled.");
            }
        }

        // backend storage
        {
            let stage_file = "@~/bendsql/.check";
            match self.conn.get_presigned_url("UPLOAD", stage_file).await {
                Err(_) => {
                    eprintln!("-> WARN: Backend storage dose not support presigned url.");
                    eprintln!("         Loading data from local file may not work as expected.");
                    eprintln!("         Be aware of data transfer cost with arg `presign=off`.");
                }
                Ok(resp) => {
                    let now_utc = chrono::Utc::now();
                    let data = now_utc.to_rfc3339().as_bytes().to_vec();
                    let size = data.len() as u64;
                    let reader = Box::new(std::io::Cursor::new(data));
                    match self.conn.upload_to_stage(stage_file, reader, size).await {
                        Err(e) => {
                            eprintln!("-> ERR: Backend storage upload not working as expected.");
                            eprintln!("        {e}");
                        }
                        Ok(()) => {
                            let u = url::Url::parse(&resp.url)?;
                            let host = u.host_str().unwrap_or("unknown");
                            println!("Backend storage OK: {host}");
                        }
                    };
                }
            }
        }

        Ok(())
    }

    pub async fn handle_repl(&mut self) {
        let config = Builder::new()
            .completion_prompt_limit(10)
            .completion_type(CompletionType::List)
            .build();
        let mut rl = Editor::<CliHelper, DefaultHistory>::with_config(config).unwrap();

        rl.set_helper(Some(CliHelper::new(self.keywords.clone())));
        rl.load_history(&get_history_path()).ok();

        'F: loop {
            match rl.readline(&self.prompt().await) {
                Ok(line) => {
                    let queries = self.append_query(&line);
                    for query in queries {
                        if !query.starts_with('!') {
                            let _ = rl.add_history_entry(format!(
                                "{}{}",
                                query, self.settings.sql_delimiter
                            ));
                        }

                        match self.handle_query(true, &query).await {
                            Ok(None) => {
                                break 'F;
                            }
                            Ok(Some(_)) => {}
                            Err(e) => {
                                if e.to_string().contains("Unauthenticated") {
                                    if let Err(e) = self.reconnect().await {
                                        eprintln!("reconnect error: {e}");
                                    } else if let Err(e) = self.handle_query(true, &query).await {
                                        eprintln!("error: {e}");
                                    }
                                } else {
                                    eprintln!("error: {e}");
                                    if e.to_string().contains(INTERRUPTED_MESSAGE) {
                                        if let Some(query_id) = self.conn.last_query_id() {
                                            println!("killing query: {query_id}");
                                            let _ = self.conn.kill_query(&query_id).await;
                                        }
                                    }
                                    self.query.clear();
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => match e {
                    ReadlineError::Io(err) => {
                        eprintln!("io err: {err}");
                    }
                    ReadlineError::Interrupted => {
                        self.query.clear();
                        println!("^C");
                    }
                    ReadlineError::Eof => {
                        break;
                    }
                    #[cfg(unix)]
                    ReadlineError::Errno(err) => {
                        error!("Unix err: {err}");
                        break;
                    }
                    #[cfg(windows)]
                    ReadlineError::SystemError(err) => {
                        error!("Windows err: {err}");
                        break;
                    }
                    _ => {}
                },
            }
        }
        // save history first to avoid loss data.
        let _ = rl.save_history(&get_history_path());
        if let Err(e) = self.conn.close().await {
            println!("got error when closing session: {e}");
        }
        println!("Bye~");
    }

    pub async fn handle_reader<R: BufRead>(&mut self, r: R) -> Result<()> {
        let start = Instant::now();
        let mut lines = r.lines();
        let mut stats: Option<ServerStats> = None;
        loop {
            match lines.next() {
                Some(Ok(line)) => {
                    let queries = self.append_query(&line);
                    for query in queries {
                        stats = self.handle_query(false, &query).await?;
                    }
                }
                Some(Err(e)) => {
                    return Err(anyhow!("read lines err: {e}"));
                }
                None => break,
            }
        }

        // if the last query is not finished with `;`, we need to execute it.
        let query = self.query.trim().to_owned();
        if !query.is_empty() {
            self.query.clear();
            stats = self.handle_query(false, &query).await?;
        }
        match self.settings.time {
            None => {}
            Some(TimeOption::Local) => {
                println!("{:.3}", start.elapsed().as_secs_f64());
            }
            Some(TimeOption::Server) => {
                let server_time_ms = match stats {
                    None => 0.0,
                    Some(ss) => ss.running_time_ms,
                };
                println!("{:.3}", server_time_ms / 1000.0);
            }
        }
        self.conn.close().await.ok();
        Ok(())
    }

    pub fn append_query(&mut self, line: &str) -> Vec<String> {
        if line.is_empty() {
            return vec![];
        }

        if self.query.is_empty()
            && (line.starts_with('!')
                || line == "exit"
                || line == "quit"
                || line.to_uppercase().starts_with("PUT"))
        {
            return vec![line.to_owned()];
        }

        if !self.settings.multi_line {
            if line.starts_with("--") {
                return vec![];
            } else {
                return vec![line.to_owned()];
            }
        }

        // consume self.query and get the result
        let mut queries = Vec::new();

        if !self.query.is_empty() {
            self.query.push('\n');
        }
        self.query.push_str(line);
        let mut err = String::new();
        let delimiter = self.settings.sql_delimiter;

        'Parser: loop {
            let mut is_valid = true;
            let tokenizer = Tokenizer::new(&self.query);
            let mut previous_token_backslash = false;
            for token in tokenizer {
                match token {
                    Ok(token) => {
                        // SQL end with `;` or `\G` in repl
                        let is_end_query = token.text() == delimiter.to_string();
                        let is_slash_g = self.is_repl
                            && (previous_token_backslash
                                && token.kind == TokenKind::Ident
                                && token.text() == "G")
                            || (token.text().ends_with("\\G"));

                        if is_end_query || is_slash_g {
                            // push to current and continue the tokenizer
                            let (sql, remain) = self.query.split_at(token.span.end as usize);
                            if is_valid && !sql.is_empty() && sql.trim() != delimiter.to_string() {
                                let sql = sql.trim_end_matches(delimiter);
                                queries.push(sql.to_string());
                            }
                            self.query = remain.to_string();
                            continue 'Parser;
                        }
                        previous_token_backslash = matches!(token.kind, TokenKind::Backslash);
                    }
                    Err(e) => {
                        // ignore current query if have invalid token.
                        is_valid = false;
                        err = e.to_string();
                        continue;
                    }
                }
            }
            break;
        }

        if self.query.is_empty() && queries.is_empty() && !err.is_empty() {
            eprintln!("Parser '{line}' failed\nwith error '{err}'");
        }
        queries
    }

    #[async_recursion]
    pub async fn handle_query(
        &mut self,
        is_repl: bool,
        raw_query: &str,
    ) -> Result<Option<ServerStats>> {
        let mut query = raw_query
            .trim_end_matches(self.settings.sql_delimiter)
            .trim();
        let mut expand = None;
        self.interrupted.store(false, Ordering::SeqCst);

        if is_repl {
            if query.starts_with('!') {
                return self.handle_commands(raw_query).await;
            }
            if query == "exit" || query == "quit" {
                return Ok(None);
            }
            if query.ends_with("\\G") {
                query = query.trim_end_matches("\\G");
                expand = Some(ExpandMode::On);
            }
        }

        let start = Instant::now();
        let kind = QueryKind::from(query);
        match kind {
            QueryKind::AlterUserPassword => {
                // When changing the current user's password,
                // exit the client and login again with the new password.
                let _ = self.conn.exec(query, ()).await?;
                Ok(None)
            }
            other => {
                let quote_string = !if self.settings.quote_string {
                    false
                } else {
                    quote_string_in_box_display(query)
                };

                let data = match other {
                    QueryKind::Put(l, r) => self.conn.put_files(&l, &r).await?,
                    QueryKind::Get(l, r) => self.conn.get_files(&l, &r).await?,
                    QueryKind::GenData(t, s, o) => self.gendata(t, s, o).await?,
                    _ => self.conn.query_iter_ext(query, ()).await?,
                };

                let mut displayer = FormatDisplay::new(
                    &self.settings,
                    query,
                    quote_string,
                    start,
                    data,
                    self.interrupted.clone(),
                    self.server_addr.clone(),
                );
                let stats = displayer.display(expand).await?;
                Ok(Some(stats))
            }
        }
    }

    #[async_recursion]
    pub async fn handle_commands(&mut self, query: &str) -> Result<Option<ServerStats>> {
        match query {
            "!exit" | "!quit" => {
                return Ok(None);
            }
            "!configs" => {
                println!("{:#?}", self.settings);
            }
            other => {
                if other.starts_with("!set") {
                    let query = query[4..].split_whitespace().collect::<Vec<_>>();
                    if query.len() == 3 {
                        if query[1] != "=" {
                            return Err(anyhow!(
                                "Control command error, must be syntax of `!set cmd_name = cmd_value`."
                            ));
                        }
                        self.settings.inject_ctrl_cmd(query[0], query[2])?;
                    } else if query.len() == 2 {
                        self.settings.inject_ctrl_cmd(query[0], query[1])?;
                    } else {
                        return Err(anyhow!(
                            "Control command error, must be syntax of `!set cmd_name = cmd_value` or `!set cmd_name cmd_value`."
                        ));
                    }
                } else if other.starts_with("!source") {
                    let query = query[7..].trim();
                    let path = Path::new(query);
                    if !path.exists() {
                        return Err(anyhow!("File not found: {query}"));
                    }
                    let file = std::fs::File::open(path)?;
                    let reader = std::io::BufReader::new(file);
                    self.handle_reader(reader).await?;
                } else {
                    return Err(anyhow!("Unknown commands: {other}"));
                }
            }
        }
        Ok(Some(ServerStats::default()))
    }

    pub async fn stream_load_stdin(&mut self, query: &str, method: LoadMethod) -> Result<()> {
        let dir = std::env::temp_dir();
        // TODO:(everpcpc) write by chunks
        let mut lines = std::io::stdin().lock().lines();
        let now = chrono::Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            anyhow!("Failed to get timestamp, please check your system time is correct and retry.")
        })?;
        let tmp_file = dir.join(format!("bendsql_{now}"));
        {
            let mut file = File::create(&tmp_file).await?;
            loop {
                match lines.next() {
                    Some(Ok(line)) => {
                        file.write_all(line.as_bytes()).await?;
                        file.write_all(b"\n").await?;
                    }
                    Some(Err(e)) => {
                        return Err(anyhow!("stream load stdin err: {e}"));
                    }
                    None => break,
                }
            }
            file.flush().await?;
        }
        self.stream_load_file(query, &tmp_file, method).await?;
        remove_file(tmp_file).await?;
        Ok(())
    }

    pub async fn stream_load_file(
        &mut self,
        query: &str,
        file_path: &Path,
        method: LoadMethod,
    ) -> Result<()> {
        let start = Instant::now();
        let file = File::open(file_path).await?;
        let metadata = file.metadata().await?;

        let ss = self
            .conn
            .load_data(query, Box::new(file), metadata.len(), method)
            .await?;

        // TODO:(everpcpc) show progress
        if self.settings.show_progress {
            eprintln!(
                "==> stream loaded {}:\n    {}",
                file_path.display(),
                format_write_progress(&ss, start.elapsed().as_secs_f64())
            );
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.conn = self.client.get_conn().await?;
        if self.is_repl {
            let info = self.conn.info().await;
            eprintln!(
                "reconnecting to {}:{} as user {}.",
                info.host, info.port, info.user
            );
            let version = self.conn.version().await.unwrap_or_default();
            eprintln!("connected to {version}");
            eprintln!();
        }
        Ok(())
    }
}

fn get_history_path() -> String {
    format!(
        "{}/.bendsql_history",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    )
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
    }
}
