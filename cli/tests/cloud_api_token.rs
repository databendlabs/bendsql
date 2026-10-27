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

use std::process::Command;

const TOKEN: &str = "fake-cloud-review-secret";
const CLOUD_DSN: &str = "databend://api.databend.com/default?org_slug=org-example";
const HTTP_DSN: &str = "databend+http://api.databend.com/default?org_slug=org-example";

fn rejects(args: &[&str], environment: &[(&str, &str)], config: Option<&str>, expected: &str) {
    let home = tempfile::tempdir().unwrap();
    let config_dir = home.path().join(".bendsql");
    if let Some(config) = config {
        std::fs::create_dir(&config_dir).unwrap();
        std::fs::write(config_dir.join("config.toml"), config).unwrap();
    }
    let output = Command::new(env!("CARGO_BIN_EXE_bendsql"))
        .env_clear()
        .env("HOME", home.path())
        // Prevent external requests even if a future regression bypasses the
        // validation. A failed guard must not send the fake token to Cloud.
        .env("HTTPS_PROXY", "http://127.0.0.1:0")
        .env("HTTP_PROXY", "http://127.0.0.1:0")
        .env("ALL_PROXY", "http://127.0.0.1:0")
        .envs(environment.iter().copied())
        .args(["--non-interactive", "--query=SELECT 1"])
        .args(args)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "unexpected success: {text}");
    assert!(text.contains(expected), "unexpected diagnostic: {text}");
    assert!(!text.contains(TOKEN), "token exposed in diagnostic");
    // Validation must precede logging, connection setup and UI startup.
    if config.is_some() {
        assert_eq!(std::fs::read_dir(config_dir).unwrap().count(), 1);
    } else {
        assert!(!config_dir.exists());
    }
}

#[test]
fn ui_rejects_cloud_tokens_from_every_cli_source() {
    let dsn = format!("{CLOUD_DSN}&%63loud_api_token={TOKEN}");
    let token_flag = format!("--cloud-api-token={TOKEN}");
    let token_setting = format!("cloud_api_token={TOKEN}");
    let config =
        format!("[connection.args]\ncloud_api_token = '{TOKEN}'\norg_slug = 'org-example'\n");
    let expected = "--ui is not supported with cloud_api_token";
    rejects(&["--ui", "--dsn", &dsn], &[], None, expected);
    rejects(
        &["--ui", &token_flag, "--org=org-example"],
        &[],
        None,
        expected,
    );
    rejects(
        &["--ui"],
        &[
            ("BENDSQL_CLOUD_API_TOKEN", TOKEN),
            ("BENDSQL_ORG", "org-example"),
        ],
        None,
        expected,
    );
    rejects(
        &["--ui", "--set", &token_setting, "--org=org-example"],
        &[],
        None,
        expected,
    );
    rejects(&["--ui"], &[], Some(&config), expected);
    rejects(&["--ui"], &[("BENDSQL_DSN", &dsn)], None, expected);
}

#[test]
fn explicit_org_rejects_competing_command_line_choices_without_exposing_values() {
    rejects(
        &[
            "--org=org-example",
            "--dsn",
            &format!("databend://root:{TOKEN}@localhost:8000"),
        ],
        &[],
        None,
        "--org conflicts with the Gateway/private target in --dsn",
    );
    for (option, value) in [
        ("--private-key-file", Some("missing.pem")),
        ("--private-key-passphrase-file", Some("missing.txt")),
        ("--flight", None),
    ] {
        let mut flags = vec!["--org=org-example", option];
        if let Some(value) = value {
            flags.push(value);
        }
        rejects(
            &flags,
            &[("BENDSQL_CLOUD_API_TOKEN", TOKEN)],
            None,
            &format!("--org cannot be combined with {option}"),
        );
    }
    for key in [
        "access_token",
        "access_token_file",
        "private_key_file",
        "private_key_passphrase_file",
    ] {
        rejects(
            &["--org=org-example", "--set", &format!("{key}={TOKEN}")],
            &[],
            None,
            &format!("--org cannot be combined with --set {key}"),
        );
    }
}

#[test]
fn explicit_org_overrides_environment_and_saved_native_credentials() {
    let config = "[connection]\nhost='localhost'\nport=8000\nuser='root'\ntls=false\n[connection.args]\naccess_token_file='missing'\nprivate_key_file='missing'\nsslmode='disable'\ntenant='tn-private'\n";
    for dsn in ["databend://root:pw@localhost:8000", "not a DSN"] {
        rejects(
            &["--org=org-example", "--ui"],
            &[
                ("BENDSQL_DSN", dsn),
                ("BENDSQL_PASSWORD", "native-password"),
                ("BENDSQL_CLOUD_API_TOKEN", TOKEN),
            ],
            Some(config),
            "--ui is not supported with cloud_api_token",
        );
    }
}

#[test]
fn bare_org_reads_environment_without_consuming_another_flag() {
    rejects(
        &["--org", "--ui"],
        &[
            ("BENDSQL_CLOUD_API_TOKEN", TOKEN),
            ("BENDSQL_ORG", "org-example"),
            ("BENDSQL_DSN", "not a DSN"),
            ("BENDSQL_PASSWORD", "native-password"),
        ],
        None,
        "--ui is not supported with cloud_api_token",
    );
    rejects(
        &["--org"],
        &[("BENDSQL_CLOUD_API_TOKEN", TOKEN)],
        None,
        "Cloud WebAPI requires --org=<slug>, org_slug in DSN/config or BENDSQL_ORG",
    );
}

#[test]
fn explicit_org_without_a_cloud_token_never_falls_back_to_native_auth() {
    let config = "[connection]\nuser='root'\n[connection.args]\naccess_token_file='missing'\n";
    for token in ["", "not-set"] {
        let environment = if token.is_empty() {
            vec![("BENDSQL_CLOUD_API_TOKEN", "")]
        } else {
            vec![]
        };
        rejects(
            &["--org=org-example"],
            &environment,
            Some(config),
            "Cloud WebAPI requires cloud_api_token in DSN/config, --cloud-api-token or BENDSQL_CLOUD_API_TOKEN",
        );
    }
}

#[test]
fn cloud_dsn_selects_account_auth_and_ignores_sql_userinfo() {
    for host in ["api.databend.com", "api.databend.cn", "API.DATABEND.COM"] {
        let dsn = format!("databend://root:%FF@{host}/default");
        rejects(
            &[
                "--dsn",
                &dsn,
                "--org=org-example",
                "--ui",
                "--user=root",
                "--password",
                TOKEN,
            ],
            &[("BENDSQL_CLOUD_API_TOKEN", TOKEN)],
            None,
            "--ui is not supported with cloud_api_token",
        );
        rejects(
            &["--dsn", &dsn, "--ui"],
            &[
                ("BENDSQL_CLOUD_API_TOKEN", TOKEN),
                ("BENDSQL_ORG", "org-example"),
            ],
            None,
            "--ui is not supported with cloud_api_token",
        );
        rejects(
            &["--dsn", &dsn],
            &[],
            None,
            "Cloud WebAPI requires cloud_api_token",
        );
        for option in [
            "access_token=other",
            "access_token_file=missing",
            "private_key_file=missing",
        ] {
            let bad = format!("{dsn}?org_slug=org-example&{option}");
            rejects(
                &["--dsn", &bad],
                &[("BENDSQL_CLOUD_API_TOKEN", TOKEN)],
                None,
                "cloud_api_token cannot be combined with Access Token or Key Pair",
            );
        }
        let bad = format!("{dsn}?org_slug=org-example&tenant=tn-other");
        rejects(
            &["--dsn", &bad],
            &[("BENDSQL_CLOUD_API_TOKEN", TOKEN)],
            None,
            "not a separate tenant parameter",
        );
    }
    rejects(
        &[
            "--org=org-example",
            "--dsn",
            "databend://api.databend.com.evil.test/default",
        ],
        &[],
        None,
        "--org conflicts with the Gateway/private target in --dsn",
    );
}

#[test]
fn http_dsn_cannot_be_normalized_away_before_auth_selection() {
    let token_flag = format!("--cloud-api-token={TOKEN}");
    for sslmode in ["", "&sslmode=require"] {
        let dsn = format!("{HTTP_DSN}{sslmode}");
        let token_dsn = format!("{dsn}&cloud_api_token={TOKEN}");
        let expected = "requires HTTPS; databend+http is not supported";
        rejects(&["--dsn", &token_dsn], &[], None, expected);
        rejects(&["--dsn", &dsn, &token_flag], &[], None, expected);
        rejects(&[&token_flag], &[("BENDSQL_DSN", &dsn)], None, expected);
    }
}

#[test]
fn grpc_dsn_cannot_be_normalized_away_before_auth_selection() {
    let dsn = "databend+grpc://api.databend.com/default?org_slug=org-example";
    let token_flag = format!("--cloud-api-token={TOKEN}");
    let expected = "not supported with FlightSQL";
    rejects(&["--dsn", dsn, &token_flag], &[], None, expected);
    rejects(&[&token_flag], &[("BENDSQL_DSN", dsn)], None, expected);
}
