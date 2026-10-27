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

use super::*;

#[test]
fn cloud_token_precedence_and_ignored_password() {
    let mut args = ConnectionArgs::from_dsn("databend://cloudapp:%FF@api.databend.com/default?warehouse=wh&cloud_api_token=dsn-secret&org_slug=org-example").unwrap();
    args.apply_cloud_api_token(None, Some("env-secret".into()));
    assert_eq!(args.args["cloud_api_token"], "dsn-secret");
    args.apply_cloud_api_token(Some("cli-secret".into()), Some("env-secret".into()));
    assert_eq!(args.args["cloud_api_token"], "cli-secret");
    assert!(!format!("{args:?}").contains("cli-secret"));
    let dsn = url::Url::parse(&args.get_dsn().unwrap()).unwrap();
    assert_eq!(dsn.username(), "");
    assert!(dsn.password().is_none());
    assert_eq!(
        dsn.query_pairs()
            .find(|(key, _)| key == "cloud_api_token")
            .unwrap()
            .1,
        "cli-secret"
    );

    let mut args =
        ConnectionArgs::from_dsn("databend://cloudapp:old-password@host/default").unwrap();
    args.apply_cloud_api_token(Some("cli-secret".into()), Some("env-secret".into()));
    let dsn = args.get_dsn().unwrap();
    assert!(!dsn.contains("old-password"));
    assert!(!dsn.contains("cloudapp"));
    assert!(dsn.contains("cloud_api_token=cli-secret"));
}

#[test]
fn cloud_token_rejects_duplicate_dsn_credentials() {
    let err = ConnectionArgs::from_dsn(
        "databend://host?cloud_api_token=first-secret&%63loud_api_token=second-secret",
    )
    .unwrap_err();
    assert!(err.to_string().contains("only once"));
    assert!(!err.to_string().contains("first-secret"));
    assert!(!err.to_string().contains("second-secret"));
}

#[test]
fn cloud_webapi_defaults_and_org_precedence() {
    let mut args = ConnectionArgs {
        host: "localhost".into(),
        port: Some(8000),
        user: "root".into(),
        password: "old-password".into(),
        ..Default::default()
    };
    args.args.insert("sslmode".into(), "disable".into());
    args.apply_cloud_api_token(Some("secret".into()), None);
    args.apply_org(None, Some("env-org".into()));
    args.apply_org(None, Some("ignored-env-org".into()));
    assert_eq!(args.args["org_slug"], "env-org");
    args.apply_org(Some("cli-org".into()), Some("ignored".into()));
    args.cloud_defaults(true, true, true);
    let dsn = url::Url::parse(&args.get_dsn().unwrap()).unwrap();
    assert_eq!(dsn.host_str(), Some("api.databend.com"));
    assert_eq!(dsn.port(), Some(443));
    assert!(dsn.username().is_empty());
    assert!(dsn.password().is_none());
    let pairs: BTreeMap<_, _> = dsn.query_pairs().collect();
    assert_eq!(pairs["org_slug"], "cli-org");
    assert!(!pairs.contains_key("sslmode"));

    let mut args = ConnectionArgs::from_dsn("databend://api.databend.cn:8000/default?cloud_api_token=secret&org_slug=dsn-org&sslmode=disable").unwrap();
    args.apply_org(None, Some("env-org".into()));
    args.cloud_defaults(false, false, false);
    assert_eq!(args.host, "api.databend.cn");
    assert_eq!(args.port, Some(8000));
    assert_eq!(args.args["org_slug"], "dsn-org");
    assert_eq!(args.args["sslmode"], "disable");

    assert!(
        ConnectionArgs::from_dsn("databend://api.databend.com?org_slug=first&org_slug=second")
            .is_err()
    );
}

#[test]
fn environment_token_is_optional_and_does_not_override_explicit_connections() {
    let mut args = ConnectionArgs::default();
    assert!(args.accepts_environment_cloud_token(false, false));
    assert!(!args.accepts_environment_cloud_token(true, false));
    assert!(!args.accepts_environment_cloud_token(false, true));
    args.apply_cloud_api_token(None, Some("".into()));
    assert!(!args.args.contains_key("cloud_api_token"));
    args.apply_cloud_api_token(Some("".into()), Some("env-token".into()));
    assert_eq!(args.args["cloud_api_token"], "");
    for key in [
        "cloud_api_token",
        "access_token",
        "access_token_file",
        "private_key_file",
        "private_key_passphrase_file",
    ] {
        let mut args = ConnectionArgs::default();
        args.args.insert(key.into(), "value".into());
        assert!(!args.accepts_environment_cloud_token(false, false), "{key}");
    }
}

#[test]
fn explicit_config_defaults_remain_authoritative_in_cloud_mode() {
    use crate::config::ConnectionConfig;
    let implicit: ConnectionConfig = toml::from_str("").unwrap();
    assert!(implicit.host.is_none() && implicit.port.is_none() && implicit.user.is_none());
    for config in [
        "",
        "host = 'localhost'",
        "port = 8000",
        "user = 'root'",
        "host = 'localhost'\nport = 8000\nuser = 'root'",
    ] {
        let connection: ConnectionConfig = toml::from_str(config).unwrap();
        let mut args = ConnectionArgs {
            host: connection
                .host
                .clone()
                .unwrap_or_else(|| "localhost".into()),
            port: connection.port.or(Some(8000)),
            user: connection.user.clone().unwrap_or_else(|| "root".into()),
            ..Default::default()
        };
        args.apply_cloud_api_token(Some("fake-token".into()), None);
        args.cloud_defaults(connection.host.is_none(), connection.port.is_none(), true);
        if connection.host.is_some() {
            assert_eq!(args.host, "localhost");
        } else {
            assert_eq!(args.host, "api.databend.com");
        }
        if connection.port.is_some() {
            assert_eq!(args.port, Some(8000));
        } else {
            assert_eq!(args.port, Some(443));
        }
        assert_eq!(
            connection.user.is_some(),
            config.lines().any(|line| line.starts_with("user"))
        );
        assert_eq!(args.user, "root");
    }
}

#[test]
fn explicitly_empty_connection_config_is_not_treated_as_absent() {
    let connection: crate::config::ConnectionConfig =
        toml::from_str("host = ''\nport = 0\nuser = ''").unwrap();
    assert_eq!(connection.host.as_deref(), Some(""));
    assert_eq!(connection.port, Some(0));
    assert_eq!(connection.user.as_deref(), Some(""));
}

#[test]
fn cloud_token_rejects_flight_sql() {
    for scheme in ["databend+flight", "databend+grpc"] {
        for source in ["dsn", "cli", "environment"] {
            let mut dsn = format!("{scheme}://host/default");
            if source == "dsn" {
                dsn.push_str("?cloud_api_token=flight-secret");
            }
            let err = match ConnectionArgs::from_dsn(&dsn) {
                Ok(mut args) => {
                    if source == "cli" {
                        args.apply_cloud_api_token(Some("flight-secret".into()), None);
                    } else if source == "environment" {
                        args.apply_cloud_api_token(None, Some("flight-secret".into()));
                    }
                    args.get_dsn().unwrap_err()
                }
                Err(err) => err,
            };
            assert!(
                err.to_string().contains("not supported with FlightSQL"),
                "{scheme}, {source}: {err}"
            );
            assert!(!err.to_string().contains("flight-secret"));
        }
    }
}

#[test]
fn cloud_ui_rejects_all_token_sources() {
    let dsn = ConnectionArgs::from_dsn(
        "databend://api.databend.com/default?%63loud_api_token=ui-secret&org_slug=org-example",
    )
    .unwrap();
    let mut cli = ConnectionArgs::default();
    cli.apply_cloud_api_token(Some("ui-secret".into()), None);
    let mut environment = ConnectionArgs::default();
    environment.apply_cloud_api_token(None, Some("ui-secret".into()));
    // Config and --set populate the same connection argument map.
    let mut config = ConnectionArgs::default();
    config
        .args
        .insert("cloud_api_token".into(), "ui-secret".into());
    for args in [dsn, cli, environment, config] {
        assert!(args.validate_cloud_options(false).is_ok());
        let err = args.validate_cloud_options(true).unwrap_err().to_string();
        assert!(err.contains("--ui is not supported with cloud_api_token"));
        assert!(!err.contains("ui-secret"));
    }
}

#[test]
fn cloud_http_dsn_is_rejected_for_all_token_sources() {
    for source in ["dsn", "cli", "environment"] {
        for sslmode in ["", "&sslmode=require"] {
            let mut dsn =
                format!("databend+http://api.databend.com/default?org_slug=org-example{sslmode}");
            if source == "dsn" {
                dsn.push_str("&cloud_api_token=http-secret");
            }
            let mut args = ConnectionArgs::from_dsn(&dsn).unwrap();
            if source == "cli" {
                args.apply_cloud_api_token(Some("http-secret".into()), None);
            } else if source == "environment" {
                args.apply_cloud_api_token(None, Some("http-secret".into()));
            }
            assert_eq!(args.dsn_scheme.as_deref(), Some("databend+http"));
            args.cloud_defaults(false, false, false);
            let err = args.get_dsn().unwrap_err().to_string();
            assert!(err.contains("requires HTTPS"), "{source}: {err}");
            assert!(!err.contains("http-secret"));
        }
    }
}

#[test]
fn cloud_restrictions_do_not_change_private_connections() {
    let args = ConnectionArgs::from_dsn(
        "databend+http://user:password@localhost:8000/default?sslmode=disable",
    )
    .unwrap();
    assert!(args.validate_cloud_options(true).is_ok());
    // Preserve the existing non-Cloud CLI serialization behavior.
    assert_eq!(
        args.get_dsn().unwrap(),
        "databend://user:password@localhost:8000/default?sslmode=disable"
    );
}
