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

// Resolver unit tests cover option matrices. These check real process/env/UI
// boundaries and ensure rejection precedes logging or connection setup.

const TOKEN: &str = "fake-cloud-review-secret";
const CLOUD_DSN: &str = "databend://api.databend.com/default?org=org-example";
const HTTP_DSN: &str = "databend+http://api.databend.com/default?org=org-example";

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
fn cloud_ui_rejected_for_flag_dsn_and_environment_credentials() {
    let dsn = format!("{CLOUD_DSN}&cloud_api_token={TOKEN}");
    rejects(&["--ui", "--dsn", &dsn], &[], None, "--ui is not supported");
    rejects(
        &["--ui", "--org", "org-example", "--cloud-api-token", TOKEN],
        &[],
        None,
        "--ui is not supported",
    );
    rejects(
        &["--ui"],
        &[
            ("BENDSQL_DSN", CLOUD_DSN),
            ("BENDSQL_CLOUD_API_TOKEN", TOKEN),
        ],
        None,
        "--ui is not supported",
    );
}

#[test]
fn org_requires_a_value() {
    for flags in [vec!["--org"], vec!["--org", "--ui"]] {
        rejects(&flags, &[], None, "a value is required for '--org <ORG>'");
    }
}

#[test]
fn cloud_without_token_does_not_fall_back_to_native() {
    for token in [None, Some("")] {
        let env = token
            .map(|value| vec![("BENDSQL_CLOUD_API_TOKEN", value)])
            .unwrap_or_default();
        rejects(
            &["--org", "org-example"],
            &env,
            None,
            "Cloud mode requires cloud_api_token",
        );
    }
}

#[test]
fn cloud_settings_are_not_private_config_or_set_options() {
    for key in ["org", "org_slug", "cloud_api_token"] {
        let config = format!("[connection.args]\n{key}='{TOKEN}'\n");
        rejects(
            &[],
            &[],
            Some(&config),
            "Cloud options are not supported in config [connection]",
        );
        rejects(
            &["--set", &format!("{key}={TOKEN}")],
            &[],
            None,
            "is not supported; use --org",
        );
    }
}

#[test]
fn explicit_http_and_grpc_are_not_normalized_away() {
    let http = format!("{HTTP_DSN}&cloud_api_token={TOKEN}");
    rejects(&["--dsn", &http], &[], None, "requires HTTPS");
    let grpc = format!("databend+grpc://api.databend.com?org=example&cloud_api_token={TOKEN}");
    rejects(&["--dsn", &grpc], &[], None, "not supported with FlightSQL");
}

#[test]
fn official_host_without_org_does_not_use_environment_token() {
    rejects(
        &[],
        &[
            ("BENDSQL_DSN", "databend://api.databend.com"),
            ("BENDSQL_CLOUD_API_TOKEN", TOKEN),
        ],
        None,
        "requires org",
    );
}
