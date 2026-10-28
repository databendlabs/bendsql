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

#![allow(clippy::upper_case_acronyms)]

mod agent;
mod args;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod argv_mask;
mod ast;
mod config;
mod display;
mod gendata;
mod helper;
mod session;
mod sql_parser;
mod trace;
mod web;

use std::io::{stdin, IsTerminal};

use anyhow::{anyhow, Context, Result};
use clap::{parser::ValueSource, ArgAction, CommandFactory, FromArgMatches, Parser};
use databend_client::{is_cloud_webapi_host, SensitiveString};
use log::info;
use once_cell::sync::Lazy;

use crate::config::ClapLoadMethod;
use crate::{
    args::ConnectionArgs,
    config::{Config, OutputFormat, OutputQuoteStyle, Settings, TimeOption},
};

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    let sha = option_env!("VERGEN_GIT_SHA").unwrap_or("dev");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("");
    match option_env!("BENDSQL_BUILD_INFO") {
        Some(info) => format!("{version}-{info}"),
        None => format!("{version}-{sha}({timestamp})"),
    }
});

#[derive(Debug, Parser, PartialEq)]
#[command(version = VERSION.as_str())]
// disable default help flag since it would conflict with --host
#[command(author, about, disable_help_flag = true)]
struct Args {
    #[clap(long, help = "Print help information")]
    help: bool,

    #[clap(
        long,
        help = "Using FlightSQL, ignored by native DSNs and unsupported with Cloud WebAPI"
    )]
    flight: bool,

    #[clap(long, help = "Enable TLS, ignored when --dsn is set")]
    tls: Option<bool>,

    #[clap(
        short = 'h',
        long,
        help = "Databend Server host, Default: 127.0.0.1, ignored when --dsn is set"
    )]
    host: Option<String>,

    #[clap(
        short = 'P',
        long,
        help = "Databend Server port, Default: 8000, ignored when --dsn is set"
    )]
    port: Option<u16>,

    #[clap(
        short = 'u',
        long,
        help = "Default: root, overrides SQL username in DSN; ignored for Cloud WebAPI"
    )]
    user: Option<String>,

    #[clap(
        short = 'p',
        long,
        env = "BENDSQL_PASSWORD",
        hide_env_values = true,
        help = "Password, overrides SQL password in DSN; ignored for Cloud WebAPI"
    )]
    password: Option<SensitiveString>,

    #[clap(
        long,
        help = "Private key file for key-pair authentication, overrides password in DSN"
    )]
    private_key_file: Option<String>,

    #[clap(
        long,
        help = "Personal Cloud API token (or BENDSQL_CLOUD_API_TOKEN); ignores username/password"
    )]
    cloud_api_token: Option<SensitiveString>,

    #[clap(long, num_args = 0..=1, require_equals = true, value_name = "ORG_SLUG",
        help = "Select Cloud WebAPI; use --org=slug or read the organization from config/BENDSQL_ORG")]
    org: Option<Option<String>>,

    #[clap(long, help = "Passphrase file for encrypted PKCS#8 private key")]
    private_key_passphrase_file: Option<String>,

    #[clap(short = 'r', long, help = "Downgrade role name, overrides role in DSN")]
    role: Option<String>,

    #[clap(short = 'D', long, help = "Database name, overrides database in DSN")]
    database: Option<String>,

    #[clap(long, value_parser = parse_key_val::<String, String>, help = "Settings, overrides settings in DSN")]
    set: Vec<(String, String)>,

    #[clap(
        long,
        env = "BENDSQL_DSN",
        hide_env_values = true,
        help = "Data source name; official API hosts select Cloud WebAPI"
    )]
    dsn: Option<SensitiveString>,

    #[clap(short = 'n', long, help = "Force non-interactive mode")]
    non_interactive: bool,

    #[clap(
        long,
        value_enum,
        conflicts_with_all = ["non_interactive", "query", "check", "data", "time"],
        help = "Initial interactive mode: sql (default), smart, or agent"
    )]
    mode: Option<agent::InteractionMode>,

    #[clap(
        long,
        conflicts_with_all = ["non_interactive", "query", "check", "data", "time"],
        help = "Select a built-in CLI (claude/codex/pi), a configured backend, or env for BENDSQL_AGENT_* settings"
    )]
    backend: Option<String>,

    #[clap(
        short = 'A',
        long,
        help = "Disable loading tables and fields for auto-completion, which offers a quicker start"
    )]
    no_auto_complete: bool,

    #[clap(long, help = "Check for server status and exit")]
    check: bool,

    #[clap(
        long,
        help = "Enable web UI interface (⚠️  SECURITY RISK: Allows SQL execution from any browser that can access this port)"
    )]
    ui: bool,

    #[clap(
        long,
        help = "Web UI port, Default: 8084, ignored when --ui is not set"
    )]
    bind_port: Option<u16>,

    #[clap(long, require_equals = true, help = "Query to execute")]
    query: Option<String>,

    #[clap(short = 'd', long, help = "Data to load, @file or @- for stdin")]
    data: Option<String>,

    #[clap(long, value_enum, default_value_t = ClapLoadMethod::Stage, help = "method to load data to table")]
    load_method: ClapLoadMethod,

    #[clap(short = 'o', long, help = "Output format")]
    output: Option<OutputFormat>,

    #[clap(
        long,
        help = "Output quote style, applies to `csv` and `tsv` output formats"
    )]
    quote_style: Option<OutputQuoteStyle>,

    #[clap(
        long,
        help = "Show progress for query execution in stderr, only works with output format `table` and `null`."
    )]
    progress: bool,

    #[clap(
        long,
        help = "Show stats after query execution in stderr, only works with non-interactive mode."
    )]
    stats: bool,

    #[clap(
        short = 'q',
        long = "query-id",
        help = "Show the last query ID after each statement."
    )]
    qid: bool,

    #[clap(
        long,
        action = ArgAction::Set,
        num_args = 0..=1, require_equals = true, default_missing_value = "server",
        help = "Only show execution time without results, will implicitly set output format to `null`."
    )]
    time: Option<TimeOption>,

    #[clap(short = 'l', default_value = "info", long)]
    log_level: String,
}

impl Args {
    fn select_explicit_cloud(
        &mut self,
        matches: &clap::ArgMatches,
        config: &mut Config,
    ) -> Result<bool> {
        let cloud_dsn = self
            .dsn
            .as_ref()
            .and_then(|dsn| url::Url::parse(dsn.inner()).ok())
            .is_some_and(|dsn| dsn.host_str().is_some_and(is_cloud_webapi_host));
        if self.org.is_none() && !cloud_dsn {
            return Ok(false);
        }
        let selector = if self.org.is_some() {
            "--org"
        } else {
            "Cloud WebAPI DSN"
        };
        // CLI flags are deliberate choices; env/config are overridable defaults.
        // Do not use clap conflicts_with: that would also reject env-sourced DSN
        // and password values that explicit Cloud selection must supersede.
        if self.org.is_some()
            && !cloud_dsn
            && matches.value_source("dsn") == Some(ValueSource::CommandLine)
        {
            return Err(anyhow!(
                "--org conflicts with the Gateway/private target in --dsn"
            ));
        }
        for name in ["private_key_file", "private_key_passphrase_file", "flight"] {
            if matches.value_source(name) == Some(ValueSource::CommandLine) {
                return Err(anyhow!(
                    "{selector} cannot be combined with --{}",
                    name.replace('_', "-")
                ));
            }
        }
        for (key, _) in &self.set {
            if ConnectionArgs::is_native_auth_option(key) {
                return Err(anyhow!("{selector} cannot be combined with --set {key}"));
            }
        }
        if !cloud_dsn {
            self.dsn = None;
        }
        self.user = None;
        self.password = None;
        let connection = &mut config.connection;
        connection.host = None;
        connection.port = None;
        connection.user = None;
        connection.tls = None;
        connection.args.retain(|key, _| {
            !ConnectionArgs::is_native_auth_option(key) && key != "sslmode" && key != "tenant"
        });
        Ok(true)
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(
    s: &str,
) -> std::result::Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut config = Config::load();

    let matches = Args::command().get_matches();
    let mut args = Args::from_arg_matches(&matches)?;
    // Hide command-line credentials from `ps` / `/proc/<pid>/cmdline`.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    argv_mask::mask_credentials(Args::command());
    let mut cmd = Args::command();
    if args.help {
        cmd.print_help()?;
        return Ok(());
    }
    if (args.mode.is_some() || args.backend.is_some())
        && (!stdin().is_terminal() || !std::io::stdout().is_terminal())
    {
        return Err(anyhow!(
            "--mode/--backend require an interactive terminal (stdin and stdout)"
        ));
    }

    let explicit_cloud = args.select_explicit_cloud(&matches, &mut config)?;

    let mut conn_args = match args.dsn {
        Some(ref dsn) => {
            if args.host.is_some() {
                eprintln!("warning: --host is ignored when --dsn is set");
            }
            if args.port.is_some() {
                eprintln!("warning: --port is ignored when --dsn is set");
            }
            if !args.set.is_empty() {
                eprintln!("warning: --set is ignored when --dsn is set");
            }
            if let Some(tls) = args.tls {
                if tls {
                    eprintln!("warning: --tls is ignored when --dsn is set");
                }
            } else if let Some(tls) = config.connection.tls {
                if tls {
                    eprintln!("warning: --tls is ignored when --dsn is set")
                }
            }

            if args.flight {
                eprintln!("warning: --flight is ignored when --dsn is set");
            }
            ConnectionArgs::from_dsn(dsn.inner())?
        }
        None => {
            let host = args
                .host
                .clone()
                .or_else(|| config.connection.host.clone())
                .unwrap_or_else(|| "localhost".to_string());
            let port = args.port.or(config.connection.port).or(Some(8000));

            let user = args
                .user
                .clone()
                .or_else(|| config.connection.user.clone())
                .unwrap_or_else(|| "root".to_string());
            let password = args
                .password
                .clone()
                .unwrap_or_else(|| SensitiveString::from(""));

            ConnectionArgs {
                host,
                port,
                user,
                password,
                database: config.connection.database.clone(),
                flight: args.flight,
                dsn_scheme: None,
                args: config.connection.args.clone(),
            }
        }
    };

    let implicit_cloud_tls = args.dsn.is_none()
        && args.tls.is_none()
        && config.connection.tls.is_none()
        && !config.connection.args.contains_key("sslmode")
        && !args.set.iter().any(|(key, _)| key == "sslmode");
    let implicit_cloud_host =
        args.dsn.is_none() && args.host.is_none() && config.connection.host.is_none();
    let implicit_cloud_port =
        args.dsn.is_none() && args.port.is_none() && config.connection.port.is_none();
    let explicit_userinfo =
        args.user.is_some() || args.password.is_some() || config.connection.user.is_some();

    // Override connection args with command line options
    {
        if args.database.is_some() {
            conn_args.database.clone_from(&args.database);
        }

        // override only if args.dsn is none
        if args.dsn.is_none() {
            if let Some(tls) = args.tls {
                if !tls {
                    conn_args
                        .args
                        .insert("sslmode".to_string(), "disable".to_string());
                }
            } else if let Some(tls) = config.connection.tls {
                if !tls {
                    conn_args
                        .args
                        .insert("sslmode".to_string(), "disable".to_string());
                }
            } else if config.connection.tls.is_none() {
                // means arg.tls is none and not config in config.toml
                conn_args
                    .args
                    .insert("sslmode".to_string(), "disable".to_string());
            }

            // override args if specified in command line
            for (k, v) in args.set {
                conn_args.args.insert(k, v);
            }
        }

        // override user if specified in command line
        if let Some(user) = args.user {
            conn_args.user = user;
        }
        if let Some(password) = args.password {
            conn_args.password = password;
        }

        // override role if specified in command line
        if let Some(role) = args.role {
            conn_args.args.insert("role".to_string(), role);
        }

        // override private key file if specified in command line
        if let Some(private_key_file) = args.private_key_file {
            conn_args
                .args
                .insert("private_key_file".to_string(), private_key_file);
            if args.private_key_passphrase_file.is_none() {
                conn_args.args.remove("private_key_passphrase_file");
            }
        }
        if let Some(private_key_passphrase_file) = args.private_key_passphrase_file {
            conn_args.args.insert(
                "private_key_passphrase_file".to_string(),
                private_key_passphrase_file,
            );
        }
    }

    let environment_cloud_token = if args.cloud_api_token.is_none()
        && !conn_args.args.contains_key("cloud_api_token")
        && (explicit_cloud
            || conn_args.accepts_environment_cloud_token(args.dsn.is_some(), explicit_userinfo))
    {
        match std::env::var("BENDSQL_CLOUD_API_TOKEN") {
            Ok(token) => Some(token.into()),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(anyhow!("BENDSQL_CLOUD_API_TOKEN must be UTF-8"))
            }
        }
    } else {
        None
    };
    conn_args.apply_cloud_api_token(args.cloud_api_token, environment_cloud_token);
    let uses_cloud_api_token = conn_args.args.contains_key("cloud_api_token");
    if explicit_cloud && !uses_cloud_api_token {
        return Err(anyhow!(
            "Cloud WebAPI requires cloud_api_token in DSN/config, --cloud-api-token or BENDSQL_CLOUD_API_TOKEN"
        ));
    }
    let environment_org = if uses_cloud_api_token
        && args.org.as_ref().and_then(Option::as_ref).is_none()
        && !conn_args.args.contains_key("org_slug")
    {
        match std::env::var("BENDSQL_ORG") {
            Ok(org) => Some(org).filter(|org| !org.is_empty()),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(anyhow!("BENDSQL_ORG must be UTF-8"))
            }
        }
    } else {
        None
    };
    conn_args.apply_org(args.org.flatten(), environment_org);
    if explicit_cloud && !conn_args.args.contains_key("org_slug") {
        return Err(anyhow!(
            "Cloud WebAPI requires --org=<slug>, org_slug in DSN/config or BENDSQL_ORG"
        ));
    }
    conn_args.cloud_defaults(implicit_cloud_host, implicit_cloud_port, implicit_cloud_tls);
    // Validate after all credential sources are merged, before network access or
    // publishing the DSN to the unauthenticated web/Python runner.
    conn_args.validate_cloud_options(args.ui)?;
    let user = conn_args.user.clone();
    let dsn = conn_args.get_dsn()?;
    let mut settings = Settings::default();
    let is_terminal = stdin().is_terminal();
    let is_repl = is_terminal
        && !args.non_interactive
        && !args.check
        && args.query.is_none()
        && args.data.is_none()
        && args.time.is_none();
    if is_repl {
        settings.display_pretty_sql = true;
        settings.show_progress = true;
        settings.show_stats = true;
        settings.output_format = OutputFormat::Table;
    } else {
        settings.output_format = OutputFormat::TSV;
    }

    if let Some(bind_port) = args.bind_port {
        settings.bind_port = bind_port;
    }
    settings.merge_config(&config);

    // Set UI flag from command line
    settings.enable_ui = args.ui;

    if args.no_auto_complete {
        settings.no_auto_complete = true;
    }
    if let Some(output) = args.output {
        settings.output_format = output;
    }
    if let Some(quote_style) = args.quote_style {
        settings.quote_style = quote_style
    }
    if args.progress {
        settings.show_progress = true;
    }
    if args.stats {
        settings.show_stats = true;
    }
    if args.qid {
        settings.show_query_id = true;
    }
    if args.time.is_some() {
        settings.output_format = OutputFormat::Null;
    }
    settings.time = args.time;

    let log_dir = format!(
        "{}/.bendsql",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    );

    std::fs::create_dir_all(&log_dir)
        .with_context(|| format!("failed to create log directory {log_dir}"))?;

    // Respect the user's log level in SQL and batch sessions. Payload protection
    // is activated separately when AI is explicitly enabled.
    let _guards = trace::init_logging(&log_dir, &args.log_level).await?;
    info!("-> bendsql version: {}", VERSION.as_str());

    let initial_mode = args.mode.unwrap_or_default();
    let mut session = match session::Session::try_new(dsn, settings, is_repl, initial_mode).await {
        Ok(session) => session,
        Err(err) => {
            // Exit client if user login failed.
            if uses_cloud_api_token {
                return Err(err);
            }
            if let Some(error) = err.downcast_ref::<databend_driver::Error>() {
                match error {
                    databend_driver::Error::Api(databend_client::Error::AuthFailure(_)) => {
                        println!("Authenticate failed wrong password user {user}");
                        return Ok(());
                    }
                    databend_driver::Error::Arrow(arrow::error::ArrowError::IpcError(ipc_err))
                        if ipc_err.contains("Unauthenticated") =>
                    {
                        println!("Authenticate failed wrong password user {user}");
                        return Ok(());
                    }
                    _ => {}
                }
            }
            return Err(err);
        }
    };

    if args.check {
        session.check().await?;
        return Ok(());
    }

    if is_repl {
        return session
            .handle_repl(initial_mode, config.agent, args.backend)
            .await;
    }

    match args.query {
        None => {
            if args.non_interactive {
                return Err(anyhow!("no query specified"));
            }
            session.handle_reader(stdin().lock()).await?;
        }
        Some(query) => match args.data {
            None => {
                session.handle_reader(std::io::Cursor::new(query)).await?;
            }
            Some(data) => {
                let load_method = args.load_method.into();
                if data.starts_with('@') {
                    match data.strip_prefix('@') {
                        Some("-") => session.stream_load_stdin(&query, load_method).await?,
                        Some(fname) => {
                            let path = std::path::Path::new(fname);
                            if !path.exists() {
                                return Err(anyhow!("file not found: {fname}"));
                            }
                            session.stream_load_file(&query, path, load_method).await?
                        }
                        None => {
                            return Err(anyhow!("invalid data input: {data}"));
                        }
                    }
                } else {
                    // TODO: should we allow passing data directly?
                    return Err(anyhow!("invalid data input: {data}"));
                }
            }
        },
    }
    Ok(())
}

#[cfg(test)]
mod agent_args_tests {
    use super::*;

    #[test]
    fn interactive_options_do_not_require_a_flag_and_conflict_with_batch_modes() {
        let args = Args::try_parse_from(["bendsql"]).unwrap();
        assert_eq!(args.mode.unwrap_or_default(), agent::InteractionMode::Sql);
        assert!(Args::try_parse_from(["bendsql", "--agent"]).is_err());
        for flag in [
            "--non-interactive",
            "--check",
            "--time",
            "--query=SELECT 1",
            "--data=@file",
        ] {
            assert!(Args::try_parse_from(["bendsql", "--mode", "sql", flag]).is_err());
            assert!(Args::try_parse_from(["bendsql", "--backend", "api", flag]).is_err());
        }
        assert!(Args::try_parse_from(["bendsql", "--output", "tsv"]).is_ok());
        assert_eq!(
            Args::try_parse_from(["bendsql", "--backend", "api"])
                .unwrap()
                .backend
                .as_deref(),
            Some("api")
        );
        assert!(Args::try_parse_from(["bendsql", "--mode", "sql"]).is_ok());
        assert!(Args::try_parse_from(["bendsql", "--mode", "unknown"]).is_err());
        for mode in ["smart", "sql", "agent"] {
            let args = Args::try_parse_from(["bendsql", "--mode", mode]).unwrap();
            assert_eq!(args.mode.unwrap().name(), mode);
        }
    }
}
