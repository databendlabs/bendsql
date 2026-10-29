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
use clap::Parser;

const CLOUD: &str = "databend://api.databend.com/default?org=example";

fn resolve(
    flags: &[&str],
    dsn: Option<&str>,
    token: Option<&str>,
    config: &str,
) -> Result<ResolvedConnection> {
    let cli =
        Args::try_parse_from(std::iter::once("bendsql").chain(flags.iter().copied())).unwrap();
    let env = ConnectionEnvironment {
        dsn: dsn.map(Into::into),
        cloud_api_token: token.map(Into::into),
        ..Default::default()
    };
    resolve_connection(
        &cli,
        &env,
        &toml::from_str::<ConnectionConfig>(config).unwrap(),
    )
}

#[test]
fn target_selection_table() {
    for (flags, env_dsn, config, host, cloud) in [
        (vec![], None, "", "localhost", false),
        (vec!["--host", "private.example"], None, "", "private.example", false),
        (vec![], None, "host='private.example'", "private.example", false),
        (vec![], Some("databend://u:pw@private.example:8000?sslmode=disable"), "", "private.example", false),
        (vec!["--org", "example"], None, "", "api.databend.com", true),
        (vec!["--org", "example", "--host", "api.databend.cn"], None, "", "api.databend.cn", true),
        (vec!["--dsn", CLOUD], None, "", "api.databend.com", true),
        (vec![], Some(CLOUD), "host='private.example'\nport=8000\nuser='root'\ntls=false", "api.databend.com", true),
        (vec!["--org", "example"], Some("not a dsn"), "host='localhost'\nport=8000\nuser='root'\ntls=false\n[args]\naccess_token_file='missing'", "api.databend.com", true),
    ] {
        let resolved = resolve(&flags, env_dsn, Some("env-token"), config).unwrap();
        assert_eq!(resolved.args.host, host, "{flags:?}");
        assert_eq!(matches!(resolved.target, Target::Cloud { .. }), cloud);
        assert_eq!(resolved.args.args.contains_key("cloud_api_token"), cloud);
        if cloud { assert!(!resolved.args.args.contains_key("sslmode")); }
    }
}

#[test]
fn conflict_table() {
    for (flags, config, message) in [
        (vec!["--org", ""], "", "valid Organization ID"),
        (vec!["--cloud-api-token", "token"], "", "requires org"),
        (
            vec!["--dsn", "databend://api.databend.com?cloud_api_token=token"],
            "",
            "requires org",
        ),
        (vec!["--host", "api.databend.com"], "", "requires org"),
        (vec!["--org", "example", "--user", "root"], "", "--user"),
        (
            vec!["--org", "example", "--password", "pw"],
            "",
            "--password",
        ),
        (
            vec!["--org", "example", "--private-key-file", "missing"],
            "",
            "--private-key-file",
        ),
        (
            vec![
                "--org",
                "example",
                "--private-key-passphrase-file",
                "missing",
            ],
            "",
            "--private-key-passphrase-file",
        ),
        (vec!["--org", "example", "--flight"], "", "--flight"),
        (
            vec!["--org", "example", "--host", "private.example"],
            "",
            "official",
        ),
        (vec!["--org", "example", "--port", "8000"], "", "port 443"),
        (vec!["--org", "example", "--tls=false"], "", "HTTPS"),
        (
            vec!["--dsn", CLOUD, "--set", "sslmode=disable"],
            "",
            "HTTPS",
        ),
        (vec![], "host='api.databend.com'", "requires org"),
        (
            vec!["--org", "example", "--dsn", "databend://localhost:8000"],
            "",
            "conflicts",
        ),
        (
            vec!["--dsn", "databend+http://api.databend.com?org=example"],
            "",
            "requires HTTPS",
        ),
        (
            vec![
                "--dsn",
                "databend+http://api.databend.com?org=example&sslmode=require",
            ],
            "",
            "requires HTTPS",
        ),
        (
            vec!["--dsn", "databend+flight://api.databend.com?org=example"],
            "",
            "not supported with FlightSQL",
        ),
        (
            vec!["--dsn", "databend://api.databend.com:8443?org=example"],
            "",
            "port 443",
        ),
        (
            vec![
                "--dsn",
                "databend://api.databend.com?org=example&sslmode=disable",
            ],
            "",
            "requires HTTPS",
        ),
        (
            vec![
                "--dsn",
                "databend://api.databend.com?org=example&access_token=other",
            ],
            "",
            "cannot be combined",
        ),
        (
            vec!["--org", "example", "--set", "tenant=private"],
            "",
            "cannot be combined",
        ),
        (vec!["--org", "example", "--ui"], "", "--ui"),
        (vec![], "[args]\norg='example'", "config [connection]"),
        (
            vec![],
            "[args]\ncloud_api_token='token'",
            "config [connection]",
        ),
        (vec!["--set", "org=example"], "", "--set org"),
    ] {
        let error = resolve(&flags, None, Some("env-token"), config)
            .err()
            .expect("must fail")
            .to_string();
        assert!(error.contains(message), "{flags:?}: {error}");
        assert!(!error.contains("env-token"));
    }
}

#[test]
fn token_precedence_and_required_credentials() {
    for (flags, env, expected) in [
        (
            vec![
                "--dsn",
                "databend://api.databend.com?org=example&cloud_api_token=dsn-token",
                "--cloud-api-token",
                "cli-token",
            ],
            Some("env-token"),
            "cli-token",
        ),
        (
            vec![
                "--dsn",
                "databend://api.databend.com?org=example&cloud_api_token=dsn-token",
            ],
            Some("env-token"),
            "dsn-token",
        ),
        (vec!["--org", "example"], Some("env-token"), "env-token"),
    ] {
        let resolved = resolve(&flags, None, env, "").unwrap();
        assert_eq!(resolved.args.args["cloud_api_token"], expected);
        assert!(!format!("{:?}", resolved.args).contains(expected));
    }
    for env in [None, Some("")] {
        assert!(resolve(&["--org", "example"], None, env, "").is_err());
    }
    assert!(resolve(
        &["--org", "example", "--cloud-api-token", ""],
        None,
        Some("env-token"),
        ""
    )
    .is_err());
}

#[test]
fn warnings_name_sources_not_values_and_inputs_are_unchanged() {
    let cli = Args::try_parse_from(["bendsql", "--org", "example"]).unwrap();
    let config: ConnectionConfig = toml::from_str("host='private-secret.example'\nuser='private-user'\n[args]\naccess_token_file='secret-path'").unwrap();
    let env = ConnectionEnvironment {
        dsn: Some("private-secret-dsn".into()),
        password: Some("private-secret-password".into()),
        cloud_api_token: Some("cloud-secret-token".into()),
    };
    let resolved = resolve_connection(&cli, &env, &config).unwrap();
    assert_eq!(resolved.warnings.len(), 3);
    assert!(resolved
        .warnings
        .join(" ")
        .contains("BENDSQL_DSN ignored when --org is set"));
    assert!(!resolved.warnings.join(" ").contains("secret"));
    assert_eq!(config.host.as_deref(), Some("private-secret.example"));
    assert!(config.args.contains_key("access_token_file"));
}

#[test]
fn native_credentials_preserve_cli_environment_dsn_precedence() {
    for (flags, password, expected_user, expected_password) in [
        (vec![], None, "dsn-user", "dsn-password"),
        (vec![], Some("env-password"), "dsn-user", "env-password"),
        (
            vec!["--user", "cli-user", "--password", "cli-password"],
            Some("env-password"),
            "cli-user",
            "cli-password",
        ),
    ] {
        let cli = Args::try_parse_from(std::iter::once("bendsql").chain(flags)).unwrap();
        let env = ConnectionEnvironment {
            dsn: Some(
                "databend://dsn-user:dsn-password@private.example:8000?sslmode=disable".into(),
            ),
            password: password.map(Into::into),
            cloud_api_token: Some("ignored-cloud-token".into()),
        };
        let resolved = resolve_connection(&cli, &env, &ConnectionConfig::default()).unwrap();
        assert_eq!(resolved.target, Target::Native);
        assert_eq!(resolved.args.user, expected_user);
        assert_eq!(resolved.args.password.inner(), expected_password);
    }
    let resolved = resolve(
        &["--set", "access_token=native-token"],
        None,
        Some("ignored-cloud-token"),
        "",
    )
    .unwrap();
    assert_eq!(resolved.args.args["access_token"], "native-token");
    let resolved = resolve(
        &["--user", "key-user", "--private-key-file", "key.pem"],
        None,
        Some("ignored-cloud-token"),
        "",
    )
    .unwrap();
    assert_eq!(resolved.args.args["private_key_file"], "key.pem");
}

#[cfg(unix)]
#[test]
fn unused_non_utf8_environment_credentials_are_ignored() {
    use std::os::unix::ffi::OsStringExt;
    let cli = Args::try_parse_from(["bendsql", "--host", "private.example"]).unwrap();
    let mut env = ConnectionEnvironment {
        cloud_api_token: Some(OsString::from_vec(vec![255])),
        ..Default::default()
    };
    assert_eq!(
        resolve_connection(&cli, &env, &ConnectionConfig::default())
            .unwrap()
            .target,
        Target::Native
    );
    env.dsn = Some(OsString::from_vec(vec![255]));
    env.password = Some(OsString::from_vec(vec![255]));
    let cli = Args::try_parse_from([
        "bendsql",
        "--org",
        "example",
        "--cloud-api-token",
        "cli-token",
    ])
    .unwrap();
    assert!(resolve_connection(&cli, &env, &ConnectionConfig::default()).is_ok());
}
