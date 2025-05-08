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

use std::{env, error::Error};
use vergen_gix::BuildBuilder;
use vergen_gix::Emitter;
use vergen_gix::GixBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    let builder = BuildBuilder::default().build_timestamp(true).build()?;
    let gix_builder = GixBuilder::default().sha(false).build()?;

    Emitter::new()
        .fail_on_error()
        .add_instructions(&builder)?
        .add_instructions(&gix_builder)?
        .emit()
        .unwrap_or_else(|_| {
            let info = env::var("BENDSQL_BUILD_INFO").unwrap_or_else(|_| "unknown".to_string());
            println!("cargo:rustc-env=BENDSQL_BUILD_INFO={}", info);
        });

    Ok(())
}
