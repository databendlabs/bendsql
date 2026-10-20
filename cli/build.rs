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

use std::{env, error::Error, path::Path};
use vergen_gix::{BuildBuilder, Emitter, GixBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=skills");
    let skills = Path::new(&env::var("CARGO_MANIFEST_DIR")?).join("skills");
    let mut files = Vec::new();
    collect_skills(&skills, &skills, &mut files)?;
    files.sort();
    let mut source = String::from("const EMBEDDED_SKILLS: &[(&str, &[u8])] = &[\n");
    for (name, path) in files {
        source.push_str(&format!("({name:?}, include_bytes!({path:?})),\n"));
    }
    source.push_str("];\n");
    std::fs::write(
        Path::new(&env::var("OUT_DIR")?).join("embedded_skills.rs"),
        source,
    )?;
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

fn collect_skills(
    root: &Path,
    path: &Path,
    files: &mut Vec<(String, String)>,
) -> Result<(), Box<dyn Error>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err("Embedded skills must not contain symlinks".into());
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path)? {
            collect_skills(root, &entry?.path(), files)?;
        }
    } else if metadata.is_file() {
        let name = path
            .strip_prefix(root)?
            .to_str()
            .ok_or("Skill path must be UTF-8")?
            .replace('\\', "/");
        let absolute = path.to_str().ok_or("Skill path must be UTF-8")?.to_owned();
        files.push((name, absolute));
    } else {
        return Err("Embedded skill resources must be regular files".into());
    }
    Ok(())
}
