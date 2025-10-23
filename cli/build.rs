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
use vergen_gix::{BuildBuilder, Emitter, GixBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    let gix = GixBuilder::default().sha(false).build();
    let build = BuildBuilder::default().build_timestamp(true).build()?;

    let mut emitter = Emitter::default();

    if let Ok(gix) = gix {
        emitter.add_instructions(&gix)?;
    }

    emitter.add_instructions(&build)?;
    emitter.emit().unwrap_or_else(|_| {
        let info = env::var("BENDSQL_BUILD_INFO").unwrap_or_else(|_| "unknown".to_string());
        println!("cargo:rustc-env=BENDSQL_BUILD_INFO={info}");
    });

    Ok(())
}
