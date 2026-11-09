//! Context files, discoverable skills, and non-executable prompt templates.
use super::Cli;
use pi_rs::{Error, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Skill {
    pub path: PathBuf,
    pub description: String,
    pub automatic: bool,
}
#[derive(Clone)]
pub struct Prompt {
    pub body: String,
    pub description: String,
}
#[derive(Default)]
pub struct Resources {
    pub instructions: String,
    pub system: Option<String>,
    pub append_system: String,
    pub skills: BTreeMap<String, Skill>,
    pub prompts: BTreeMap<String, Prompt>,
    pub warnings: Vec<String>,
    pub packages: super::packages::PackageResources,
    pub extensions: Vec<PathBuf>,
    pub themes: Vec<PathBuf>,
}
#[derive(Deserialize, Default)]
struct Frontmatter {
    name: Option<String>,
    description: Option<String>,
    #[serde(default, rename = "disable-model-invocation")]
    manual: bool,
}
impl Resources {
    pub fn load(args: &Cli) -> Result<Self> {
        let mut resources = Self {
            packages: super::packages::resources(args)?,
            ..Self::default()
        };
        let agent = crate::config::agent_dir();
        let workspace = args.workspace.canonicalize()?;
        let mut directories = workspace
            .ancestors()
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        directories.reverse();
        directories.insert(0, agent.clone());
        let mut seen = std::collections::BTreeSet::new();
        for directory in directories.into_iter().filter(|_| !args.no_context_files) {
            // Explicit --project continues to use the core's project-scoped loader.
            if args.project.is_some() && directory.starts_with(&workspace) {
                continue;
            }
            let override_file = directory.join("AGENTS.override.md");
            let paths = if override_file.is_file() {
                vec![override_file]
            } else {
                vec![directory.join("AGENTS.md"), directory.join("CLAUDE.md")]
            };
            for path in paths {
                if path.is_file() && seen.insert(path.canonicalize()?) {
                    resources.instructions.push_str(&format!(
                        "\n\n# Context {}\n{}",
                        path.display(),
                        read(&path)?
                    ));
                }
            }
        }
        use super::resource_paths::{self, Kind};
        let explicit_skills: Vec<_> = args
            .skill
            .iter()
            .filter_map(|path| {
                let path = resource_path(&workspace, path);
                (args.project.is_none() || path.exists()).then_some(path)
            })
            .collect();
        let skill_roots = resource_paths::resolve(
            args,
            Kind::Skill,
            &resources.packages.skills,
            &explicit_skills,
            &mut resources.warnings,
        )?;
        let prompt_roots = resource_paths::resolve(
            args,
            Kind::Prompt,
            &resources.packages.prompts,
            &args.prompt_template,
            &mut resources.warnings,
        )?;
        let explicit_extensions: Vec<_> = args
            .extensions
            .iter()
            .filter(|entry| !resource_paths::named_extension(entry))
            .map(PathBuf::from)
            .collect();
        resources.extensions = resource_paths::resolve(
            args,
            Kind::Extension,
            &resources.packages.extensions,
            &explicit_extensions,
            &mut resources.warnings,
        )?;
        resources.themes = resource_paths::resolve(
            args,
            Kind::Theme,
            &resources.packages.themes,
            &args.themes,
            &mut resources.warnings,
        )?;
        let mut config_directories = vec![agent.clone()];
        if args.trusted {
            config_directories.push(workspace.join(".pi"));
        }
        for directory in config_directories {
            let system = directory.join("SYSTEM.md");
            if system.is_file() {
                resources.system = Some(read(&system)?);
            }
            let append = directory.join("APPEND_SYSTEM.md");
            if append.is_file() {
                resources.append_system = read(&append)?;
            }
        }
        for root in skill_roots {
            let mut files = Vec::new();
            if root.is_file() {
                files.push(root);
            } else if root.is_dir() {
                for entry in ignore::WalkBuilder::new(root)
                    .hidden(false)
                    .git_ignore(false)
                    .follow_links(true)
                    .build()
                    .flatten()
                {
                    if entry.file_type().is_some_and(|kind| kind.is_file())
                        && entry.file_name() == "SKILL.md"
                    {
                        files.push(entry.into_path());
                    }
                }
            }
            files.sort();
            for path in files {
                match parse_skill(&path) {
                    Ok((name, skill)) => {
                        if let std::collections::btree_map::Entry::Vacant(entry) =
                            resources.skills.entry(name.clone())
                        {
                            entry.insert(skill);
                        } else {
                            resources
                                .warnings
                                .push(format!("Duplicate skill {name} at {}", path.display()));
                        }
                    }
                    Err(error) => resources.warnings.push(error.to_string()),
                }
            }
        }
        for root in prompt_roots {
            let files = if root.is_file() {
                vec![root]
            } else {
                std::fs::read_dir(root)
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
                    .collect()
            };
            for path in files {
                let Some(name) = path.file_stem().and_then(|name| name.to_str()) else {
                    continue;
                };
                let text = read(&path)?;
                let (metadata, body) = frontmatter(&text)?;
                resources.prompts.entry(name.into()).or_insert(Prompt {
                    description: metadata.description.unwrap_or_else(|| {
                        body.lines()
                            .find(|line| !line.trim().is_empty())
                            .unwrap_or("")
                            .into()
                    }),
                    body,
                });
            }
        }
        if resources.instructions.len() > 256 * 1024 {
            return Err(Error::Config("context files exceed size limit".into()));
        }
        Ok(resources)
    }
    pub fn system_prompt(
        &self,
        base: &str,
        custom: bool,
        tools: &[pi_rs::ToolSpec],
        workspace: &Path,
    ) -> String {
        let mut text = if custom {
            base
        } else {
            self.system.as_deref().unwrap_or(base)
        }
        .to_owned();
        if !custom && self.system.is_none() {
            text.push_str("\n\n<tools>\n");
            if tools.is_empty() {
                text.push_str("(none)\n");
            }
            for tool in tools {
                text.push_str(&format!("- {}: {}\n", tool.name, tool.description));
            }
            text.push_str("Additional tools may be available.\n</tools>\n\n<rules>\n");
            if tools.iter().any(|tool| tool.name == "read") {
                text.push_str("- Use read to inspect files before editing them.\n");
            }
            if tools.iter().any(|tool| tool.name == "edit") {
                text.push_str("- Use edit for targeted changes. Batch changes to one file in a single call.\n- Each edits[].oldText must exactly and uniquely match the original file. Keep matches short and non-overlapping; merge nearby changes without including large unchanged regions.\n");
            }
            if tools.iter().any(|tool| tool.name == "write") {
                text.push_str("- Use write only for new files or complete rewrites.\n");
            }
            text.push_str("- Be concise in your responses.\n- Show file paths clearly when working with files.\n</rules>");
        }
        if !self.append_system.is_empty() {
            text.push_str(&format!(
                "\n\n<addendum>\n{}\n</addendum>",
                self.append_system
            ));
        }
        if !self.instructions.is_empty() {
            text.push_str(&format!(
                "\n\n<project_context>\n{}\n</project_context>",
                self.instructions
            ));
        }
        let reader = ["read", "bash", "powershell"]
            .into_iter()
            .find(|name| tools.iter().any(|tool| tool.name == *name));
        let automatic = self
            .skills
            .iter()
            .filter(|(_, skill)| skill.automatic)
            .collect::<Vec<_>>();
        if let Some(reader) = reader
            && !automatic.is_empty()
        {
            text.push_str(&format!("\n\n<skills>\nThe following skills provide specialized instructions for specific tasks.\nUse {reader} to load a skill's file when the task matches its description.\nResolve relative paths against the skill's directory (parent of SKILL.md). Use absolute paths in tool calls.\n\n<available_skills>\n"));
            for (name, skill) in automatic {
                text.push_str(&format!("  <skill>\n    <name>{}</name>\n    <description>{}</description>\n    <location>{}</location>\n  </skill>\n", xml(name), xml(&skill.description), xml(&skill.path.to_string_lossy())));
            }
            text.push_str("</available_skills>\n</skills>");
        }
        text.push_str(&format!("\n\n<cwd>\n{}\n</cwd>", workspace.display()));
        text
    }
    pub fn expand(&self, input: &str) -> Result<Option<String>> {
        let (name, arguments) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
        if let Some(name) = name.strip_prefix("/skill:") {
            let skill = self
                .skills
                .get(name)
                .ok_or_else(|| Error::Config(format!("skill {name:?} not found")))?;
            return Ok(Some(format!(
                "Skill directory: {}\n{}\n\n{}",
                skill.path.parent().unwrap_or(Path::new(".")).display(),
                read(&skill.path)?,
                arguments
            )));
        }
        let Some(prompt) = name
            .strip_prefix('/')
            .and_then(|name| self.prompts.get(name))
        else {
            return Ok(None);
        };
        Ok(Some(expand_arguments(&prompt.body, arguments)?))
    }
}
fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn resource_path(workspace: &Path, path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
    {
        return PathBuf::from(home).join(rest);
    }
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    }
}
fn parse_skill(path: &Path) -> Result<(String, Skill)> {
    let text = read(path)?;
    let (metadata, _) = frontmatter(&text)?;
    let name = metadata
        .name
        .or_else(|| path.parent()?.file_name()?.to_str().map(str::to_owned))
        .ok_or_else(|| Error::Config(format!("{} has no skill name", path.display())))?;
    let description = metadata
        .description
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::Config(format!("{} has no skill description", path.display())))?;
    Ok((
        name,
        Skill {
            path: path.canonicalize()?,
            description,
            automatic: !metadata.manual,
        },
    ))
}
fn frontmatter(text: &str) -> Result<(Frontmatter, String)> {
    let text = text.trim_start_matches('\u{feff}');
    if !text.starts_with("---\n") && !text.starts_with("---\r\n") {
        return Ok((Frontmatter::default(), text.into()));
    }
    let mut lines = text.lines();
    lines.next();
    let mut yaml = String::new();
    for line in lines.by_ref() {
        if line == "---" {
            return Ok((
                serde_yaml::from_str(&yaml).map_err(|error| Error::Config(error.to_string()))?,
                lines.collect::<Vec<_>>().join("\n"),
            ));
        }
        yaml.push_str(line);
        yaml.push('\n');
    }
    Err(Error::Config("unterminated resource frontmatter".into()))
}
pub fn prompt_argument(workspace: &Path, argument: &str) -> Result<String> {
    let path = resource_path(workspace, argument);
    if path.is_file() {
        read(&path)
    } else {
        Ok(argument.into())
    }
}
fn read(path: &Path) -> Result<String> {
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(64 * 1024 + 1)
        .read_to_string(&mut text)?;
    if text.len() > 64 * 1024 {
        return Err(Error::Config(format!(
            "{} exceeds resource size limit",
            path.display()
        )));
    }
    Ok(text)
}
fn expand_arguments(body: &str, arguments: &str) -> Result<String> {
    let args = shell_words::split(arguments).map_err(|error| Error::Config(error.to_string()))?;
    let all = args.join(" ");
    let expression =
        regex::Regex::new(r"\$\{([^}]+)\}|\$ARGUMENTS\b|\$@|\$(\d+)").expect("argument expression");
    Ok(expression
        .replace_all(body, |captures: &regex::Captures<'_>| {
            let token = captures.get(0).unwrap().as_str();
            let key = captures
                .get(1)
                .or_else(|| captures.get(2))
                .map(|value| value.as_str())
                .unwrap_or(token.trim_start_matches('$'));
            let (key, fallback) = key.split_once(":-").unwrap_or((key, ""));
            let value = if key == "@" || key == "ARGUMENTS" {
                all.clone()
            } else if let Some(slice) = key.strip_prefix("@:") {
                let (start, length) = slice.split_once(':').unwrap_or((slice, ""));
                let start = start.parse::<usize>().unwrap_or(1).saturating_sub(1);
                let length = length.parse::<usize>().unwrap_or(usize::MAX);
                args.iter()
                    .skip(start)
                    .take(length)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                key.parse::<usize>()
                    .ok()
                    .and_then(|index| index.checked_sub(1))
                    .and_then(|index| args.get(index))
                    .cloned()
                    .unwrap_or_default()
            };
            if value.is_empty() {
                fallback.into()
            } else {
                value
            }
        })
        .into_owned())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prompt_arguments_accept_text_and_workspace_relative_files() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("system.md"), "File instruction").unwrap();
        assert_eq!(
            prompt_argument(directory.path(), "system.md").unwrap(),
            "File instruction"
        );
        assert_eq!(
            prompt_argument(directory.path(), "A literal instruction.").unwrap(),
            "A literal instruction."
        );
    }
    #[test]
    fn prompt_uses_available_readers_and_preserves_custom_precedence() {
        let mut resources = Resources::default();
        resources.skills.insert(
            "demo&skill".into(),
            Skill {
                path: "/tmp/SKILL.md".into(),
                description: "specific <task>".into(),
                automatic: true,
            },
        );
        let no_reader = resources.system_prompt("default", false, &[], Path::new("/workspace"));
        assert!(!no_reader.contains("available_skills"));
        let tools = [pi_rs::ToolSpec {
            name: "read".into(),
            description: "Read a file".into(),
            parameters: serde_json::json!({}),
        }];
        let prompt = resources.system_prompt("default", false, &tools, Path::new("/workspace"));
        assert!(prompt.contains("<tools>"));
        assert!(prompt.contains("demo&amp;skill"));
        assert!(prompt.contains("specific &lt;task&gt;"));
        resources.system = Some("project system".into());
        let custom =
            resources.system_prompt("explicit system", true, &tools, Path::new("/workspace"));
        assert!(custom.starts_with("explicit system"));
        assert!(!custom.contains("project system"));
        assert!(!custom.contains("<rules>"));
        assert!(custom.contains("<skills>"));
    }
    #[test]
    fn disabled_discovery_keeps_explicit_resource_paths() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path();
        std::fs::create_dir_all(workspace.join(".pi/skills/discovered")).unwrap();
        std::fs::create_dir_all(workspace.join(".pi/prompts")).unwrap();
        std::fs::write(workspace.join("AGENTS.md"), "excluded context marker").unwrap();
        std::fs::write(
            workspace.join(".pi/skills/discovered/SKILL.md"),
            "---\nname: discovered\ndescription: discovered skill\n---\nHidden",
        )
        .unwrap();
        std::fs::write(workspace.join(".pi/prompts/discovered.md"), "Hidden").unwrap();
        std::fs::write(
            workspace.join("explicit-skill.md"),
            "---\nname: explicit\ndescription: explicit skill\n---\nVisible",
        )
        .unwrap();
        std::fs::write(workspace.join("explicit-prompt.md"), "Visible $1").unwrap();
        let mut args = Cli::try_parse_from([
            "pi",
            "--no-skills",
            "--no-prompt-templates",
            "--no-context-files",
            "--skill",
            "explicit-skill.md",
            "--prompt-template",
            "explicit-prompt.md",
        ])
        .unwrap();
        args.workspace = workspace.to_path_buf();
        args.trusted = true;
        let resources = Resources::load(&args).unwrap();
        assert!(resources.instructions.is_empty());
        assert_eq!(
            resources.skills.keys().collect::<Vec<_>>(),
            vec!["explicit"]
        );
        assert_eq!(
            resources.prompts.keys().collect::<Vec<_>>(),
            vec!["explicit-prompt"]
        );
        args.no_skills = false;
        args.no_prompt_templates = false;
        args.no_context_files = false;
        let resources = Resources::load(&args).unwrap();
        assert!(resources.instructions.contains("excluded context marker"));
        assert!(resources.skills.contains_key("discovered"));
        assert!(resources.prompts.contains_key("discovered"));
    }
    #[test]
    fn templates_preserve_quoted_arguments_defaults_and_slices() {
        assert_eq!(
            expand_arguments(
                "$1|$2|$@|${3:-default}|${@:2:1}",
                "\"API compatibility\" tests"
            )
            .unwrap(),
            "API compatibility|tests|API compatibility tests|default|tests"
        );
    }
    #[test]
    fn frontmatter_reads_multiline_yaml_without_exposing_it_as_prompt() {
        let (metadata, body) = frontmatter(
            "---\nname: test\ndescription: >\n  first line\n  second line\n---\nInstructions",
        )
        .unwrap();
        assert_eq!(
            metadata.description.as_deref(),
            Some("first line second line\n")
        );
        assert_eq!(body, "Instructions");
    }
}
