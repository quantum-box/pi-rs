//! Resource snapshots for native SDK hosts that own discovery and permissions.
use std::path::{Component, Path, PathBuf};

use pi_rs::{Error, Result, SessionBuilder};

use crate::prompt_resources::{
    ContextReadGrants, ContextResources, ExplicitPromptResourceFactory, PromptReadGrants,
    PromptResources,
};
use crate::skill_resources::{SkillReadGrants, SkillReader, SkillResources};
use crate::template_resources::{PromptTemplate, TemplateReadGrants};
use crate::theme_resources::{ThemeReadGrants, ThemeSnapshot};

/// Files and directories the host authorizes for one load. Paths must be absolute
/// without parent components. Grants include symlink targets.
#[derive(Clone, Debug, Default)]
pub struct ResourceReadGrants {
    pub prompts: PromptReadGrants,
    pub prompt_files: Vec<PathBuf>,
    pub context: ContextReadGrants,
    pub skills: SkillReadGrants,
    pub skill_directories: Vec<PathBuf>,
    pub templates: TemplateReadGrants,
    pub template_directories: Vec<PathBuf>,
    pub themes: ThemeReadGrants,
}

/// Load the file resources used by a native session, without global discovery.
#[derive(Clone, Debug, Default)]
pub struct ResourceFactory {
    pub prompts: ExplicitPromptResourceFactory,
}

/// An owned snapshot. A later load never updates the snapshot or any session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceSnapshot {
    pub prompts: PromptResources,
    pub context: ContextResources,
    pub skills: SkillResources,
    pub templates: Vec<PromptTemplate>,
    pub themes: ThemeSnapshot,
}

impl ResourceFactory {
    /// Read resources using the supplied grants. Run this off the UI thread.
    /// A read or validation error returns no snapshot. Skill diagnostics are
    /// retained by the skill loader.
    pub fn load(&self, grants: &ResourceReadGrants) -> Result<ResourceSnapshot> {
        if grants.context.directories.len() > 128 || grants.prompt_files.len() > 128 {
            return Err(Error::Config(
                "too many context directories or prompt files".into(),
            ));
        }
        // The focused loaders have their own path contracts. Validate all paths
        // here before any loader can enumerate a directory or open a file.
        for path in grants
            .prompts
            .project
            .iter()
            .chain(grants.prompts.global.iter())
            .chain(&grants.prompt_files)
            .chain(&grants.context.directories)
            .chain(&grants.skills.files)
            .chain(&grants.skill_directories)
            .chain(&grants.templates.files)
            .chain(&grants.template_directories)
            .chain(&grants.themes.files)
        {
            if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
                return Err(Error::Config(format!(
                    "resource grant must be absolute without parent components: {}",
                    path.display()
                )));
            }
        }
        Ok(ResourceSnapshot {
            prompts: self.prompts.load(&grants.prompts, &grants.prompt_files)?,
            context: grants.context.load()?,
            skills: grants
                .skills
                .load_with_directories(&grants.skill_directories)?,
            templates: grants
                .templates
                .load_with_directories(&grants.template_directories)?,
            themes: grants.themes.load()?,
        })
    }
}

impl ResourceSnapshot {
    /// Compose the configured prompt using Pi's resource section order.
    /// The host supplies its default preamble (including any tool and docs
    /// sections) and selects the reader available in its tool loadout. Pass
    /// `None` to omit the skill list when no reader is available.
    pub fn system_prompt(
        &self,
        default: &str,
        cwd: &Path,
        skill_reader: Option<SkillReader>,
    ) -> Result<String> {
        let mut prompt = self
            .prompts
            .system
            .as_ref()
            .map(|resource| resource.text.as_str())
            .filter(|text| !text.is_empty())
            .unwrap_or(default)
            .to_owned();
        let addendum = self
            .prompts
            .append
            .iter()
            .filter(|resource| resource.source.is_some() || !resource.text.is_empty())
            .map(|resource| resource.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        let context = self.context.render();
        let skills = match skill_reader {
            Some(reader) => self.skills.render(reader)?,
            None => String::new(),
        };
        for (name, content) in [
            ("addendum", addendum.as_str()),
            ("project_context", context.as_str()),
            ("skills", skills.trim_start_matches('\n')),
        ] {
            if !content.is_empty() {
                push_section(&mut prompt, name, content);
            }
        }
        push_section(
            &mut prompt,
            "cwd",
            &cwd.to_string_lossy().replace('\\', "/"),
        );
        Ok(prompt)
    }

    /// Set the configured system prompt on a host-owned builder. The host
    /// handles template invocation, theme selection and extension registration.
    pub fn apply(
        &self,
        builder: SessionBuilder,
        default: &str,
        cwd: &Path,
        skill_reader: Option<SkillReader>,
    ) -> Result<SessionBuilder> {
        Ok(builder.system_prompt(self.system_prompt(default, cwd, skill_reader)?))
    }
}

fn push_section(prompt: &mut String, name: &str, content: &str) {
    if !prompt.is_empty() {
        prompt.push_str("\n\n");
    }
    prompt.push_str(&format!("<{name}>\n{content}\n</{name}>"));
}
