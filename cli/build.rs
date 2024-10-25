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

use std::{env, error::Error, process::Command};
use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    EmitBuilder::builder()
        .fail_on_error()
        .build_timestamp()
        .git_sha(true)
        .emit()
        .unwrap_or_else(|_| {
            let info = match env::var("BENDSQL_BUILD_INFO") {
                Ok(info) => info,
                Err(_) => "unknown".to_string(),
            };
            println!("cargo:rustc-env=BENDSQL_BUILD_INFO={}", info);
        });

    if env::var("BUILD_FRONTEND").is_ok() {
        println!("cargo:warning=Start to build frontend dir via env BUILD_FRONTEND.");
        let cwd = env::current_dir().expect("Failed to get current directory");
        println!("cargo:warning=Current Dir {:?}.", cwd.display());

        env::set_current_dir("../frontend").expect("Failed to change directory to ../frontend");
        // Clean old frontend directory
        let _ = Command::new("rm")
            .arg("-rf")
            .arg("../cli/frontend")
            .status()
            .expect("Failed to remove old frontend directory");

        // Mkdir new dir
        let _ = Command::new("mkdir")
            .arg("-p")
            .arg("../cli/frontend")
            .status()
            .expect("Failed to create frontend directory");

        let _ = Command::new("yarn")
            .arg("config")
            .arg("set")
            .arg("network-timeout")
            .arg("600000")
            .status()
            .expect("Failed to set Yarn network timeout");

        let _ = Command::new("yarn")
            .arg("install")
            .status()
            .expect("Yarn install failed");

        let _ = Command::new("yarn")
            .arg("build")
            .status()
            .expect("Yarn build failed");

        // 移动构建结果
        let _ = Command::new("mv")
            .arg("build")
            .arg("../cli/frontend/")
            .status()
            .expect("Failed to move build directory");

        env::set_current_dir(cwd).unwrap();
    }
    Ok(())
}
