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

// Resolver tests own credential/option matrices; keep DSN serialization here.

#[test]
fn cloud_token_precedence_and_ignored_password() {
    let mut args = ConnectionArgs::from_dsn("databend://cloudapp:%FF@api.databend.com/default?warehouse=wh&cloud_api_token=dsn-secret&org=org-example").unwrap();
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

    let mut args = ConnectionArgs::from_dsn(
        "databend://cloudapp:old-password@api.databend.com/default?org=org-example",
    )
    .unwrap();
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
fn empty_environment_token_is_unset_but_explicit_empty_token_is_an_error() {
    let mut args = ConnectionArgs::default();
    args.apply_cloud_api_token(None, Some("".into()));
    assert!(!args.args.contains_key("cloud_api_token"));
    args.apply_cloud_api_token(Some("".into()), Some("env-token".into()));
    assert_eq!(args.args["cloud_api_token"], "");
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
