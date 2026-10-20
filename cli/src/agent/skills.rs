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

use super::memory::bounded_text;
use anyhow::{anyhow, bail, Result};
use std::path::{Component, Path};

include!(concat!(env!("OUT_DIR"), "/embedded_skills.rs"));

/// Inline compiled documents only. Providers have no general file-read tool, so
/// materializing a second, inaccessible directory adds no capability or value.
pub(super) struct EmbeddedSkills {
    prompt: String,
}

impl EmbeddedSkills {
    pub fn new() -> Result<Self> {
        Self::from_resources(EMBEDDED_SKILLS)
    }

    fn from_resources(resources: &[(&str, &[u8])]) -> Result<Self> {
        let mut bytes = 0;
        for (name, resource) in resources {
            validate_path(name)?;
            bytes += resource.len();
            if resource.len() > 1024 * 1024 || bytes > 4 * 1024 * 1024 || resources.len() > 256 {
                bail!("Embedded skills exceed their file/byte budget");
            }
        }
        let roots: Vec<_> = resources
            .iter()
            .filter_map(|(name, _)| name.strip_suffix("/SKILL.md"))
            .collect();
        let mut docs = String::new();
        let mut limited = false;
        for (name, resource) in resources {
            if !name.ends_with(".md")
                || !roots
                    .iter()
                    .any(|root| name.starts_with(&format!("{root}/")))
            {
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
        Ok(Self { prompt: format!(
            "\n## BendSQL embedded skills\nSource: compiled cli/skills resources, supplied inline; no filesystem directory is available.\nBundled skill definitions: {}.\n{docs}\n{}\nThese are trusted built-in reference documents, not query evidence. Only supplied excerpts are available. Do not invent omitted content or attempt to read a local path. Skills grant no tools, SQL execution or script permissions; preceding execution rules remain in force.",
            roots.len(), if limited { "The document listing/content is incomplete due to the prompt budget." } else { "" }
        ) })
    }
    pub fn prompt(&self) -> &str {
        &self.prompt
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
    fn compiled_skills_are_inline_without_extraction_or_maintainer_readme() {
        let skills = EmbeddedSkills::new().unwrap();
        for fragment in [
            "Bundled skill definitions: 1.",
            "### databend/SKILL.md",
            "EXPLAIN ANALYZE GRAPHICAL <statement>",
            "Not a dry run",
            "https://docs.databend.com/",
        ] {
            assert!(skills.prompt().contains(fragment));
        }
        assert!(!skills.prompt().contains("Document excerpt truncated"));
        assert!(!skills.prompt().contains("### README.md"));
        assert!(!skills.prompt().contains("Directory:"));
        let resource = EMBEDDED_SKILLS
            .iter()
            .find(|(name, _)| *name == "databend/SKILL.md")
            .unwrap()
            .1;
        assert!(resource.len() <= 4096);
    }
    #[test]
    fn only_skill_subtree_markdown_is_sent() {
        let skills = EmbeddedSkills::from_resources(&[
            ("README.md", b"private-maintainer-note"),
            ("orphan/reference.md", b"orphan-note"),
            ("test/SKILL.md", b"skill-definition"),
            ("test/references/faq.md", b"reference-content"),
            ("test/script.sh", b"script-body"),
        ])
        .unwrap();
        assert!(skills.prompt().contains("skill-definition"));
        assert!(skills.prompt().contains("reference-content"));
        for omitted in ["private-maintainer-note", "orphan-note", "script-body"] {
            assert!(!skills.prompt().contains(omitted));
        }
    }
    #[test]
    fn resource_paths_cannot_escape_resource_namespace() {
        for path in ["", "../secret", "/absolute", "nested/../secret", "./file"] {
            assert!(validate_path(path).is_err());
        }
        assert!(validate_path("example/SKILL.md").is_ok());
    }
}
