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
        let mut resources = Self::default();
        let agent = crate::config::agent_dir();
        let workspace = args.workspace.canonicalize()?;
        let mut directories = workspace
            .ancestors()
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        directories.reverse();
        directories.insert(0, agent.clone());
        let mut seen = std::collections::BTreeSet::new();
        for directory in directories {
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
        let mut skill_roots = vec![agent.join("skills")];
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            skill_roots.push(PathBuf::from(home).join(".agents/skills"));
        }
        let mut prompt_roots = vec![agent.join("prompts")];
        let mut config_directories = vec![agent.clone()];
        if args.trusted {
            skill_roots.push(workspace.join(".pi/skills"));
            for directory in workspace.ancestors() {
                skill_roots.push(directory.join(".agents/skills"));
                if directory.join(".git").exists() {
                    break;
                }
            }
            prompt_roots.push(workspace.join(".pi/prompts"));
            config_directories.push(workspace.join(".pi"));
        }
        skill_roots.extend(
            args.settings
                .strings("skills")
                .into_iter()
                .map(|path| resource_path(&workspace, &path)),
        );
        prompt_roots.extend(
            args.settings
                .strings("prompts")
                .into_iter()
                .map(|path| resource_path(&workspace, &path)),
        );
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
    pub fn system_prompt(&self, base: &str) -> String {
        let mut text = self.system.as_deref().unwrap_or(base).to_owned();
        text.push_str(&self.append_system);
        text.push_str(&self.instructions);
        let automatic = self
            .skills
            .iter()
            .filter(|(_, skill)| skill.automatic)
            .collect::<Vec<_>>();
        if !automatic.is_empty() {
            text.push_str("\n\nAvailable skills. Read a matching SKILL.md before following its instructions.\n");
            for (name, skill) in automatic {
                text.push_str(&format!(
                    "- {name}: {} ({})\n",
                    skill.description,
                    skill.path.display()
                ));
            }
        }
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
