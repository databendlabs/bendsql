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
    let mut args = ConnectionArgs::from_dsn("databend://cloudapp:%FF@tnexample.gw.aws-us-east-2.default.databend.com/default?warehouse=wh&cloud_api_token=dsn-secret").unwrap();
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
    args.apply_cloud_api_token(None, Some("env-secret".into()));
    let dsn = args.get_dsn().unwrap();
    assert!(!dsn.contains("old-password"));
    assert!(!dsn.contains("cloudapp"));
    assert!(dsn.contains("cloud_api_token=env-secret"));
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
fn cloud_token_rejects_flight_sql() {
    for scheme in ["databend+flight", "databend+grpc"] {
        let dsn = format!("{scheme}://host/default?cloud_api_token=secret");
        let err = match ConnectionArgs::from_dsn(&dsn) {
            Ok(args) => args.get_dsn().unwrap_err(),
            Err(err) => err,
        };
        assert!(err.to_string().contains("not supported with FlightSQL"));
        assert!(!err.to_string().contains("secret"));
    }
}
