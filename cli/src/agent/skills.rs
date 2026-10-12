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

use std::path::{Component, Path};

use anyhow::{anyhow, bail, Result};

use super::memory::bounded_text;

// Build-generated include_bytes! entries are embedded in every profile, without
// changing the web UI's existing debug asset-loading behavior.
include!(concat!(env!("OUT_DIR"), "/embedded_skills.rs"));

/// Static source resources only. No question, SQL, preview, credential or answer
/// is written here. Text is taken from the compiled assets, not mutable files.
pub(super) struct EmbeddedSkills {
    directory: tempfile::TempDir,
    prompt: String,
}

impl EmbeddedSkills {
    pub fn new() -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("bendsql-skills-")
            .tempdir()
            .map_err(|_| anyhow!("Cannot create embedded skills directory"))?;
        let names = EMBEDDED_SKILLS; // Build-time sorted, immutable resource list.
        let mut bytes = 0;
        let mut docs = String::new();
        let mut limited = false;
        let mut skills = 0;
        for (name, resource) in names {
            validate_path(name)?;
            bytes += resource.len();
            if resource.len() > 1024 * 1024 || bytes > 4 * 1024 * 1024 || names.len() > 256 {
                bail!("Embedded skills exceed their file/byte budget");
            }
            let path = directory.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap())
                .map_err(|_| anyhow!("Cannot create skill resource directory"))?;
            std::fs::write(&path, resource)
                .map_err(|_| anyhow!("Cannot extract embedded skill resource"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))
                    .map_err(|_| anyhow!("Cannot mark skill resource read-only"))?;
            }
            if *name == "SKILL.md" || name.ends_with("/SKILL.md") {
                skills += 1;
            }
            if !name.ends_with(".md") {
                continue;
            }
            let text = std::str::from_utf8(resource)
                .map_err(|_| anyhow!("Embedded skill document is not UTF-8"))?;
            let remaining = (16 * 1024usize).saturating_sub(docs.len());
            if remaining < name.len() + 64 {
                limited = true;
                continue;
            }
            let (text, cut) = bounded_text(text, 4096.min(remaining - name.len() - 64));
            docs.push_str(&format!("\n### {name}\n{text}\n"));
            if cut {
                docs.push_str("[Document excerpt truncated]\n");
            }
            limited |= cut;
        }
        let prompt = format!(
            "\n## BendSQL embedded skills\nDirectory: {}\nBundled skill definitions: {skills}.\n{}\n{docs}\n{}\nThese are trusted built-in reference documents, not query evidence. Only provided excerpts are available to model-only backends; a local path does not grant filesystem access. Never invent unavailable skill content. Skills do not grant tools, SQL execution or script permissions; all preceding execution rules remain in force.",
            directory.path().display(),
            if skills == 0 { "No task-specific skills are bundled yet." } else { "Use the listed SKILL.md resources when relevant." },
            if limited { "The document listing/content is incomplete due to the prompt budget." } else { "" },
        );
        let skills = Self { directory, prompt };
        #[cfg(unix)]
        set_directory_permissions(skills.directory.path(), 0o500)?;
        Ok(skills)
    }

    pub fn prompt(&self) -> &str {
        // Keep the extraction directory alive for the entire owning session.
        let _ = self.directory.path();
        &self.prompt
    }
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("Cannot inspect skills directory"))?;
    if !metadata.file_type().is_dir() {
        return Ok(());
    } // Never follow links.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|_| anyhow!("Cannot set skills directory permissions"))?;
    for entry in std::fs::read_dir(path).map_err(|_| anyhow!("Cannot list skill resources"))? {
        let entry = entry.map_err(|_| anyhow!("Cannot inspect skill resource"))?;
        set_directory_permissions(&entry.path(), mode)?;
    }
    Ok(())
}

impl Drop for EmbeddedSkills {
    fn drop(&mut self) {
        // TempDir needs writable parent directories to remove read-only assets.
        #[cfg(unix)]
        let _ = set_directory_permissions(self.directory.path(), 0o700);
    }
}

fn validate_path(name: &str) -> Result<()> {
    if name.is_empty()
        || Path::new(name)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        bail!("Unsafe embedded skill resource path");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assets_are_embedded_extracted_and_advertised_without_source_access() {
        let skills = EmbeddedSkills::new().unwrap();
        let path = skills.directory.path().to_owned();
        let resource = EMBEDDED_SKILLS
            .iter()
            .find(|(name, _)| *name == "README.md")
            .unwrap()
            .1;
        assert_eq!(std::fs::read(path.join("README.md")).unwrap(), resource);
        assert!(skills.prompt().contains(path.to_str().unwrap()));
        assert!(skills.prompt().contains("Bundled skill definitions: 1."));
        assert!(skills.prompt().contains("### databend/SKILL.md"));
        assert!(skills
            .prompt()
            .contains("EXPLAIN ANALYZE GRAPHICAL <statement>"));
        assert!(skills.prompt().contains("Not a dry run"));
        assert!(skills.prompt().contains("https://docs.databend.com/"));
        assert!(!skills.prompt().contains("Document excerpt truncated"));
        assert!(!skills
            .prompt()
            .contains("No task-specific skills are bundled yet"));
        let skill = EMBEDDED_SKILLS
            .iter()
            .find(|(name, _)| *name == "databend/SKILL.md")
            .unwrap()
            .1;
        assert!(skill.len() <= 4096);
        assert_eq!(
            std::fs::read(path.join("databend/SKILL.md")).unwrap(),
            skill
        );
        assert!(skills.prompt().contains("permissions"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path.join("README.md"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o222,
                0
            );
            assert_eq!(
                std::fs::metadata(path.join("databend/SKILL.md"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o222,
                0
            );
            assert_eq!(
                std::fs::metadata(path.join("databend"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o222,
                0
            );
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o222,
                0
            );
        }
        drop(skills);
        assert!(!path.exists());
    }

    #[test]
    fn resource_paths_cannot_escape_the_private_directory() {
        for name in ["", "../secret", "/absolute", "nested/../secret", "./file"] {
            assert!(validate_path(name).is_err());
        }
        assert!(validate_path("example/SKILL.md").is_ok());
    }
}
