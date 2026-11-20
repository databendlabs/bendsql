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

#![cfg(unix)]

use std::process::Command;

#[test]
fn gendata_provides_manual_guidance_when_duckdb_is_unavailable() {
    let home = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bendsql"))
        .env_clear()
        .env("HOME", home.path())
        .env("PATH", home.path())
        .args([
            "--non-interactive",
            "--dsn",
            "databend://root@127.0.0.1:1/default?sslmode=disable&login=disable&presign=off&retry_count=1",
            "--query=gendata(tpch, scale = 1, override = 0)",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("DuckDB CLI is not available"), "{stderr}");
    assert!(
        stderr.contains("https://duckdb.org/docs/stable/installation/"),
        "{stderr}"
    );
}
