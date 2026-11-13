//! Context files, discoverable skills, and non-executable prompt templates.
use super::Cli;
use pi_rs::{Error, Result};
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
    pub path: PathBuf,
    pub body: String,
    pub description: String,
}
#[derive(Clone, Default)]
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
#[derive(Default)]
struct Frontmatter {
    name: Option<String>,
    description: Option<String>,
    manual: bool,
}
impl Resources {
    pub fn load_with_discovered(
        args: &Cli,
        discovered: &pi_rs::DiscoveredResources,
    ) -> Result<Self> {
        let mut resources = Self::load(args)?;
        if discovered.skill_paths.is_empty()
            && discovered.prompt_paths.is_empty()
            && discovered.theme_paths.is_empty()
        {
            return Ok(resources);
        }
        let mut augmented = args.clone();
        augmented.no_skills = true;
        augmented.no_prompt_templates = true;
        augmented.no_themes = true;
        augmented.no_extensions = true;
        augmented.no_context_files = true;
        augmented.extensions.clear();
        augmented.skill = discovered
            .skill_paths
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        augmented.prompt_template = discovered.prompt_paths.clone();
        augmented.themes = discovered.theme_paths.clone();
        let additions = match Self::load(&augmented) {
            Ok(additional) => vec![additional],
            Err(_) => {
                // A resource can disappear or become unreadable between the
                // extension callback and the host loading it. Keep the other
                // extensions' resources available in that case.
                let mut base = augmented.clone();
                base.skill.clear();
                base.prompt_template.clear();
                base.themes.clear();
                let mut loaded = Vec::new();
                for (kind, paths) in [
                    (0, &discovered.skill_paths),
                    (1, &discovered.prompt_paths),
                    (2, &discovered.theme_paths),
                ] {
                    for path in paths {
                        let mut one = base.clone();
                        match kind {
                            0 => one.skill.push(path.to_string_lossy().into_owned()),
                            1 => one.prompt_template.push(path.clone()),
                            _ => one.themes.push(path.clone()),
                        }
                        match Self::load(&one) {
                            Ok(additional) => loaded.push(additional),
                            Err(error) => resources.warnings.push(format!(
                                "Could not load discovered resource {}: {error}",
                                path.display()
                            )),
                        }
                    }
                }
                loaded
            }
        };
        for additional in additions {
            for (name, skill) in additional.skills {
                match resources.skills.entry(name.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(skill);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if !same_resource_file(&entry.get().path, &skill.path) =>
                    {
                        resources.warnings.push(resource_collision(
                            "skill",
                            &name,
                            &entry.get().path,
                            &skill.path,
                        ));
                    }
                    _ => {}
                }
            }
            for (name, prompt) in additional.prompts {
                match resources.prompts.entry(name.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(prompt);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if !same_resource_file(&entry.get().path, &prompt.path) =>
                    {
                        resources.warnings.push(resource_collision(
                            "prompt",
                            &name,
                            &entry.get().path,
                            &prompt.path,
                        ));
                    }
                    _ => {}
                }
            }
            for theme in additional.themes {
                if !resources.themes.contains(&theme) {
                    resources.themes.push(theme);
                }
            }
            resources.warnings.extend(additional.warnings);
        }
        Ok(resources)
    }

    pub fn load(args: &Cli) -> Result<Self> {
        let mut resources = Self {
            packages: super::packages::resources(args)?,
            ..Self::default()
        };
        let agent = crate::config::agent_dir();
        let workspace = args.workspace.canonicalize()?;
        if !args.no_context_files {
            let mut directories: Vec<_> = workspace.ancestors().collect();
            directories.reverse();
            let shadowed = if args.project.is_none() {
                shadowed_context(&workspace)
            } else {
                None
            };
            let mut seen = std::collections::BTreeSet::new();
            for (index, directory) in std::iter::once(agent.as_path())
                .chain(directories)
                .enumerate()
            {
                // Explicit --project continues to use the core's project-scoped loader.
                if args.project.is_some() && directory.starts_with(&workspace) {
                    continue;
                }
                if let Some((path, resolved, text)) =
                    context_file(directory, &mut resources.warnings)
                    && (index == 0 || shadowed.as_ref() != Some(&path))
                    && seen.insert(resolved)
                {
                    resources
                        .instructions
                        .push_str(&format!("\n\n# Context {}\n{text}", path.display()));
                }
            }
        }
        use super::resource_paths::{self, Kind};
        let explicit_skills: Vec<_> = args
            .skill
            .iter()
            .filter_map(|path| resource_paths::skill_path(args, path))
            .collect();
        let transient = super::packages::transient_resources(args)?;
        let explicit_skills: Vec<_> = explicit_skills
            .into_iter()
            .chain(transient.skills)
            .collect();
        let explicit_prompts: Vec<_> = args
            .prompt_template
            .iter()
            .cloned()
            .chain(transient.prompts)
            .collect();
        let explicit_themes: Vec<_> = args
            .themes
            .iter()
            .cloned()
            .chain(transient.themes)
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
            &explicit_prompts,
            &mut resources.warnings,
        )?;
        let explicit_extensions: Vec<_> = args
            .extensions
            .iter()
            .filter(|entry| {
                !resource_paths::named_extension(entry)
                    && !super::packages::is_package_source(entry, &workspace)
            })
            .map(PathBuf::from)
            .chain(transient.extensions)
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
            &explicit_themes,
            &mut resources.warnings,
        )?;
        let project_config = workspace.join(".pi");
        let discover_prompt = |name: &str| {
            let project = project_config.join(name);
            let user = agent.join(name);
            (args.trusted && project.is_file())
                .then_some(project)
                .or_else(|| user.is_file().then_some(user))
        };
        resources.system = if let Some(argument) = &args.system_prompt {
            Some(prompt_argument(&workspace, &argument.to_string_lossy())?)
        } else {
            discover_prompt("SYSTEM.md")
                .as_deref()
                .map(read_without_bom)
                .transpose()?
        }
        .filter(|text| !text.is_empty());
        resources.append_system = if args.append_system_prompt.is_empty() {
            discover_prompt("APPEND_SYSTEM.md")
                .as_deref()
                .map(read_without_bom)
                .transpose()?
                .unwrap_or_default()
        } else {
            args.append_system_prompt
                .iter()
                .filter(|argument| !argument.is_empty())
                .map(|argument| prompt_argument(&workspace, argument))
                .collect::<Result<Vec<_>>>()?
                .join("\n\n")
        };
        for path in skill_roots {
            match parse_skill(&path) {
                Ok((name, skill)) => {
                    validate_skill(&name, &skill, &mut resources.warnings);
                    match resources.skills.entry(name.clone()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(skill);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if !same_resource_file(&entry.get().path, &skill.path) =>
                        {
                            resources.warnings.push(resource_collision(
                                "skill",
                                &name,
                                &entry.get().path,
                                &skill.path,
                            ));
                        }
                        _ => {}
                    }
                }
                Err(error) if path.file_name().is_some_and(|name| name == "SKILL.md") => {
                    resources
                        .warnings
                        .push(format!("{}: {error}", path.display()));
                }
                Err(_) => {}
            }
        }
        for root in prompt_roots {
            let mut files = if root.is_file() {
                if root
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(".md"))
                {
                    vec![root]
                } else {
                    Vec::new()
                }
            } else {
                std::fs::read_dir(root)
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.is_file() && path.extension().is_some_and(|ext| ext == "md")
                    })
                    .collect()
            };
            files.sort();
            for path in files {
                let Some(name) = path.file_stem().and_then(|name| name.to_str()) else {
                    continue;
                };
                match parse_prompt(&path) {
                    Ok(prompt) => match resources.prompts.entry(name.into()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(prompt);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if !same_resource_file(&entry.get().path, &prompt.path) =>
                        {
                            resources.warnings.push(resource_collision(
                                "prompt",
                                name,
                                &entry.get().path,
                                &prompt.path,
                            ));
                        }
                        _ => {}
                    },
                    Err(error) => resources
                        .warnings
                        .push(format!("{}: {error}", path.display())),
                }
            }
        }
        if resources.instructions.len() > 256 * 1024 {
            return Err(Error::Config("context files exceed size limit".into()));
        }
        Ok(resources)
    }
    pub fn system_prompt(&self, base: &str, tools: &[pi_rs::ToolSpec], workspace: &Path) -> String {
        let mut text = self.system.as_deref().unwrap_or(base).to_owned();
        if self.system.is_none() {
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
    pub fn expand(&self, input: &str) -> Option<String> {
        if let Some(command) = input.strip_prefix("/skill:") {
            let (name, arguments) = command.split_once(' ').unwrap_or((command, ""));
            if let Some(skill) = self.skills.get(name) {
                match read_without_bom(&skill.path).and_then(|text| frontmatter(&text)) {
                    Ok((_, body)) => {
                        let location = skill.path.display();
                        let directory = skill.path.parent().unwrap_or(Path::new(".")).display();
                        let body = body.trim_matches(is_prompt_space);
                        let mut text = format!(
                            "<skill name=\"{name}\" location=\"{location}\">\nReferences are relative to {directory}.\n\n{body}\n</skill>"
                        );
                        let arguments = arguments.trim_matches(is_prompt_space);
                        if !arguments.is_empty() {
                            text.push_str("\n\n");
                            text.push_str(arguments);
                        }
                        return Some(text);
                    }
                    Err(error) => {
                        eprintln!("Could not expand skill {}: {error}", skill.path.display())
                    }
                }
            }
        }
        let (name, arguments) = input.split_once(is_prompt_space).unwrap_or((input, ""));
        let prompt = name
            .strip_prefix('/')
            .and_then(|name| self.prompts.get(name))?;
        Some(expand_arguments(&prompt.body, arguments))
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
// Pi's collision diagnostics identify both the resource in use and the one
// ignored. Treat aliases to the same file as one resource.
fn same_resource_file(left: &Path, right: &Path) -> bool {
    left == right || matches!((left.canonicalize(), right.canonicalize()), (Ok(a), Ok(b)) if a == b)
}

fn resource_collision(kind: &str, name: &str, winner: &Path, loser: &Path) -> String {
    format!(
        "Duplicate {kind} {name} at {}; using {}",
        loser.display(),
        winner.display()
    )
}

fn validate_skill(name: &str, skill: &Skill, warnings: &mut Vec<String>) {
    let mut warning = |message: &str| warnings.push(format!("{}: {message}", skill.path.display()));
    let name_length = name.encode_utf16().count();
    if name_length > 64 {
        warning(&format!("name exceeds 64 characters ({name_length})"));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        warning("name contains invalid characters (must be lowercase a-z, 0-9, hyphens only)");
    }
    if name.starts_with('-') || name.ends_with('-') {
        warning("name must not start or end with a hyphen");
    }
    if name.contains("--") {
        warning("name must not contain consecutive hyphens");
    }
    let description_length = skill.description.encode_utf16().count();
    if description_length > 1024 {
        warning(&format!(
            "description exceeds 1024 characters ({description_length})"
        ));
    }
}

fn parse_skill(path: &Path) -> Result<(String, Skill)> {
    let text = read(path)?;
    let (metadata, _) = frontmatter(&text)?;
    let name = metadata
        .name
        .filter(|name| !name.is_empty())
        .or_else(|| path.parent()?.file_name()?.to_str().map(str::to_owned))
        .ok_or_else(|| Error::Config("skill name is required".into()))?;
    let description = metadata
        .description
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::Config("description is required".into()))?;
    Ok((
        name,
        Skill {
            path: path.canonicalize()?,
            description,
            automatic: !metadata.manual,
        },
    ))
}
fn parse_prompt(path: &Path) -> Result<Prompt> {
    let (metadata, body) = frontmatter(&read(path)?)?;
    let description = metadata
        .description
        .filter(|description| !description.is_empty())
        .unwrap_or_else(|| {
            let first = body
                .lines()
                .find(|line| !line.trim().is_empty())
                .unwrap_or_default();
            let mut characters = first.chars();
            let mut description: String = characters.by_ref().take(60).collect();
            if characters.next().is_some() {
                description.push_str("...");
            }
            description
        });
    Ok(Prompt {
        path: path.to_owned(),
        description,
        body,
    })
}

fn frontmatter(text: &str) -> Result<(Frontmatter, String)> {
    let normalized = text
        .trim_start_matches('\u{feff}')
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let Some(end) = normalized
        .strip_prefix("---")
        .and_then(|rest| rest.find("\n---"))
        .map(|index| index + 3)
    else {
        return Ok((Frontmatter::default(), normalized));
    };
    let yaml = normalized.get(4..=end).unwrap_or_default();
    let value: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| Error::Config(error.to_string()))?;
    let metadata = Frontmatter {
        name: value
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        description: value
            .get("description")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        manual: value
            .get("disable-model-invocation")
            .and_then(|v| v.as_bool())
            == Some(true),
    };
    Ok((metadata, normalized[end + 4..].trim().into()))
}
fn context_file(
    directory: &Path,
    warnings: &mut Vec<String>,
) -> Option<(PathBuf, PathBuf, String)> {
    for filename in [
        "AGENTS.override.md",
        "AGENTS.md",
        "AGENTS.MD",
        "CLAUDE.md",
        "CLAUDE.MD",
    ] {
        let path = directory.join(filename);
        if !path.is_file() {
            continue;
        }
        match path
            .canonicalize()
            .map_err(Error::from)
            .and_then(|resolved| read_without_bom(&path).map(|text| (resolved, text)))
        {
            Ok((resolved, text)) => return Some((path, resolved, text)),
            Err(error) => warnings.push(format!(
                "Warning: Could not read {}: {error}",
                path.display()
            )),
        }
    }
    None
}

fn shadowed_context(workspace: &Path) -> Option<PathBuf> {
    let worktree = workspace
        .ancestors()
        .find(|directory| directory.join(".git").exists())?;
    let git_file = worktree.join(".git");
    if !git_file.is_file() {
        return None;
    }
    let link = read(&git_file).ok()?;
    let git_dir = worktree.join(link.trim().strip_prefix("gitdir: ")?.trim());
    if !git_dir.join("HEAD").is_file() {
        return None;
    }
    let common = read(&git_dir.join("commondir")).ok()?;
    let common = git_dir.join(common.trim()).canonicalize().ok()?;
    let main = common.parent()?;
    if main == worktree
        || !worktree.starts_with(main)
        || main.join(".git").canonicalize().ok()? != common
    {
        return None;
    }
    let (path, _, _) = context_file(worktree, &mut Vec::new())?;
    Some(main.join(path.file_name()?))
}

fn prompt_argument(workspace: &Path, argument: &str) -> Result<String> {
    let path = resource_path(workspace, argument);
    if path.is_file() {
        read_without_bom(&path)
    } else {
        Ok(argument.into())
    }
}
fn read_without_bom(path: &Path) -> Result<String> {
    let bytes = read_bytes(path)?;
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&bytes);
    Ok(String::from_utf8_lossy(bytes).into_owned())
}
fn read(path: &Path) -> Result<String> {
    String::from_utf8(read_bytes(path)?)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error).into())
}
fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err(Error::Config(format!(
            "{} exceeds resource size limit",
            path.display()
        )));
    }
    Ok(bytes)
}
pub(super) fn skill_invocation(text: &str) -> Option<(&str, &str, Option<&str>)> {
    static BLOCK: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let captures = BLOCK
        .get_or_init(|| {
            regex::Regex::new(
                r#"(?s)\A<skill name="([^"]+)" location="[^"]+">\n(.*?)\n</skill>(?:\n\n(.+))?\z"#,
            )
            .expect("Pi skill invocation block")
        })
        .captures(text)?;
    let message = captures
        .get(3)
        .map(|text| text.as_str().trim_matches(is_prompt_space))
        .filter(|text| !text.is_empty());
    Some((
        captures.get(1)?.as_str(),
        captures.get(2)?.as_str(),
        message,
    ))
}

fn is_prompt_space(character: char) -> bool {
    (character.is_whitespace() && character != '\u{0085}') || character == '\u{feff}'
}

fn parse_arguments(arguments: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut has_token = false;
    let mut quote = None;
    for character in arguments.chars() {
        if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                current.push(character);
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
            has_token = true;
        } else if is_prompt_space(character) {
            if std::mem::take(&mut has_token) {
                args.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
            has_token = true;
        }
    }
    if has_token {
        args.push(current);
    }
    args
}

fn expand_arguments(body: &str, arguments: &str) -> String {
    static PLACEHOLDER: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let expression = PLACEHOLDER.get_or_init(|| {
        regex::Regex::new(r"\$\{([0-9]+|ARGUMENTS|@):-([^}]*)\}|\$\{@:([0-9]+)(?::([0-9]+))?\}|\$(ARGUMENTS|@|[0-9]+)")
            .expect("Pi argument placeholders")
    });
    let args = parse_arguments(arguments);
    let all = args.join(" ");
    let value = |key: &str| match key {
        "@" | "ARGUMENTS" => all.as_str(),
        _ => key
            .parse::<usize>()
            .ok()
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| args.get(index))
            .map_or("", String::as_str),
    };
    expression
        .replace_all(body, |captures: &regex::Captures<'_>| {
            if let Some(target) = captures.get(1) {
                let found = value(target.as_str());
                return if found.is_empty() {
                    captures[2].to_owned()
                } else {
                    found.to_owned()
                };
            }
            if let Some(start) = captures.get(3) {
                let start = start
                    .as_str()
                    .parse::<usize>()
                    .unwrap_or(usize::MAX)
                    .saturating_sub(1)
                    .min(args.len());
                let length = captures
                    .get(4)
                    .and_then(|length| length.as_str().parse::<usize>().ok())
                    .unwrap_or(usize::MAX);
                return args[start..start.saturating_add(length).min(args.len())].join(" ");
            }
            value(&captures[5]).to_owned()
        })
        .into_owned()
}
#[cfg(test)]
mod tests {
    #[test]
    fn discovered_resources_remain_available_when_auto_discovery_is_disabled() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let skill = directory.path().join("skills/example/SKILL.md");
        let prompt = directory.path().join("prompts/greet.md");
        let theme = directory.path().join("themes/example.json");
        let original = directory.path().join("existing/greet.md");
        for path in [&skill, &prompt, &theme, &original] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        }
        std::fs::write(
            &skill,
            "---\nname: example\ndescription: Example skill\n---\nSkill instructions",
        )
        .unwrap();
        std::fs::write(&prompt, "Say hello to $ARGUMENTS").unwrap();
        std::fs::write(&original, "Existing prompt: $ARGUMENTS").unwrap();
        std::fs::write(&theme, "{}").unwrap();
        let mut args =
            super::Cli::parse_from(["pi", "--no-skills", "--no-prompt-templates", "--no-themes"]);
        args.workspace = directory.path().into();
        let mut discovered = pi_rs::DiscoveredResources {
            skill_paths: vec![skill],
            prompt_paths: vec![prompt],
            theme_paths: vec![theme.clone()],
        };
        let resources = super::Resources::load_with_discovered(&args, &discovered).unwrap();
        assert!(resources.skills.contains_key("example"));
        assert!(resources.expand("/greet world").unwrap().contains("world"));
        assert!(resources.themes.contains(&theme.canonicalize().unwrap()));
        args.prompt_template.push(original);
        let resources = super::Resources::load_with_discovered(&args, &discovered).unwrap();
        assert!(
            resources
                .expand("/greet world")
                .unwrap()
                .contains("Existing prompt: world")
        );
        let missing = directory.path().join("missing/SKILL.md");
        discovered.skill_paths.push(missing.clone());
        let resources = super::Resources::load_with_discovered(&args, &discovered).unwrap();
        assert!(resources.skills.contains_key("example"));
        assert!(
            resources
                .expand("/greet world")
                .unwrap()
                .contains("Existing prompt: world")
        );
        assert!(resources.themes.contains(&theme.canonicalize().unwrap()));
        assert!(
            resources
                .warnings
                .iter()
                .any(|warning| warning.contains(&missing.display().to_string()))
        );
    }
    use super::*;
    #[test]
    fn skill_discovery_honors_declared_roots_hidden_paths_and_local_ignores() {
        use super::super::resource_paths::skill_files;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        for path in [
            "declared/SKILL.md",
            "declared/child/SKILL.md",
            "group/second/SKILL.md",
            "group/nested-legacy.md",
            "git-hidden/SKILL.md",
            "ignore-hidden/SKILL.md",
            "fd-hidden/SKILL.md",
            ".private/SKILL.md",
            "node_modules/vendor/SKILL.md",
            "legacy.md",
        ] {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "---\ndescription: fixture\n---\nSkill content").unwrap();
        }
        std::fs::write(root.join(".gitignore"), "git-hidden/\n").unwrap();
        std::fs::write(root.join(".ignore"), "ignore-hidden/\n").unwrap();
        std::fs::write(root.join(".fdignore"), "fd-hidden/\n").unwrap();
        assert_eq!(
            skill_files(root),
            [
                root.join("declared/SKILL.md"),
                root.join("group/second/SKILL.md"),
                root.join("legacy.md"),
            ],
        );
        let declared = root.join("declared");
        assert_eq!(skill_files(&declared), [declared.join("SKILL.md")]);
    }

    #[test]
    fn duplicate_resources_report_winner_and_ignored_path_for_explicit_and_discovered() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let skill_a = root.join("first/SKILL.md");
        let skill_b = root.join("second/SKILL.md");
        let prompt_a = root.join("first/greet.md");
        let prompt_b = root.join("second/greet.md");
        for path in [&skill_a, &skill_b, &prompt_a, &prompt_b] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        }
        std::fs::write(
            &skill_a,
            "---\nname: shared\ndescription: first\n---\nFirst",
        )
        .unwrap();
        std::fs::write(
            &skill_b,
            "---\nname: shared\ndescription: second\n---\nSecond",
        )
        .unwrap();
        std::fs::write(&prompt_a, "first prompt").unwrap();
        std::fs::write(&prompt_b, "second prompt").unwrap();
        let mut args = Cli::parse_from([
            "pi",
            "--no-skills",
            "--no-prompt-templates",
            "--no-themes",
            "--no-extensions",
            "--no-context-files",
        ]);
        args.workspace = root.into();
        args.skill = vec![
            skill_a.to_string_lossy().into_owned(),
            skill_b.to_string_lossy().into_owned(),
        ];
        args.prompt_template = vec![prompt_a.clone(), prompt_b.clone()];
        let resources = Resources::load(&args).unwrap();
        let skill_a = skill_a.canonicalize().unwrap();
        let skill_b = skill_b.canonicalize().unwrap();
        let skill_warning = format!(
            "Duplicate skill shared at {}; using {}",
            skill_b.display(),
            skill_a.display()
        );
        let prompt_warning = format!(
            "Duplicate prompt greet at {}; using {}",
            prompt_b.display(),
            prompt_a.display()
        );
        assert!(
            resources.warnings.contains(&skill_warning),
            "{:?}",
            resources.warnings
        );
        assert!(
            resources.warnings.contains(&prompt_warning),
            "{:?}",
            resources.warnings
        );
        assert_eq!(resources.skills["shared"].path, skill_a);
        assert_eq!(resources.prompts["greet"].body, "first prompt");

        args.skill.truncate(1);
        args.prompt_template.truncate(1);
        let discovered = pi_rs::DiscoveredResources {
            skill_paths: vec![skill_b.clone()],
            prompt_paths: vec![prompt_b.clone()],
            theme_paths: vec![],
        };
        let resources = Resources::load_with_discovered(&args, &discovered).unwrap();
        assert!(
            resources.warnings.contains(&skill_warning),
            "{:?}",
            resources.warnings
        );
        assert!(
            resources.warnings.contains(&prompt_warning),
            "{:?}",
            resources.warnings
        );
        assert_eq!(resources.skills["shared"].path, skill_a);
        assert_eq!(resources.prompts["greet"].body, "first prompt");
    }

    #[cfg(unix)]
    #[test]
    fn prompt_symlink_alias_does_not_report_a_collision() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first");
        let second = directory.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        std::fs::write(first.join("greet.md"), "hello").unwrap();
        std::os::unix::fs::symlink(first.join("greet.md"), second.join("greet.md")).unwrap();
        let mut args = Cli::parse_from([
            "pi",
            "--no-skills",
            "--no-prompt-templates",
            "--no-themes",
            "--no-extensions",
            "--no-context-files",
        ]);
        args.workspace = directory.path().into();
        args.prompt_template = vec![first, second];
        let resources = Resources::load(&args).unwrap();
        assert_eq!(resources.prompts["greet"].body, "hello");
        assert!(
            !resources
                .warnings
                .iter()
                .any(|warning| warning.contains("Duplicate prompt greet"))
        );
    }

    #[test]
    fn skill_metadata_warnings_preserve_loadable_skills() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("SKILL.md");
        let description = "x".repeat(1025);
        std::fs::write(
            &file,
            format!("---\nname: -Bad--name\ndescription: {description}\n---\nBody"),
        )
        .unwrap();
        let (name, skill) = parse_skill(&file).unwrap();
        assert_eq!(name, "-Bad--name");
        let mut warnings = Vec::new();
        validate_skill(&name, &skill, &mut warnings);
        assert_eq!(warnings.len(), 4);
        assert!(
            warnings
                .iter()
                .any(|message| message.contains("exceeds 1024"))
        );
        assert!(warnings.iter().all(|message| message.contains("SKILL.md")));
    }

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
    fn nested_worktree_shadows_only_the_matching_main_repo_context_file() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let main = root.join("main");
        let worktree = main.join("nested");
        let workspace = worktree.join("src");
        let git = main.join(".git/worktrees/nested");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&git).unwrap();
        std::fs::write(worktree.join(".git"), "gitdir: ../.git/worktrees/nested\n").unwrap();
        std::fs::write(git.join("HEAD"), "ref: refs/heads/fixture\n").unwrap();
        std::fs::write(git.join("commondir"), "../..\n").unwrap();
        std::fs::write(root.join("AGENTS.md"), "outer fixture").unwrap();
        std::fs::write(main.join("AGENTS.md"), "main fixture").unwrap();
        std::fs::write(worktree.join("AGENTS.md"), "worktree fixture").unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = workspace;
        let resources = Resources::load(&args).unwrap();
        assert!(resources.instructions.contains("outer fixture"));
        assert!(resources.instructions.contains("worktree fixture"));
        assert!(!resources.instructions.contains("main fixture"));
        std::fs::rename(worktree.join("AGENTS.md"), worktree.join("CLAUDE.md")).unwrap();
        let resources = Resources::load(&args).unwrap();
        assert!(resources.instructions.contains("main fixture"));
        assert!(resources.instructions.contains("worktree fixture"));
    }

    #[test]
    fn context_uses_one_readable_file_per_directory_in_pi_order() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let project = root.join("project");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(root.join("AGENTS.MD"), "\u{feff}parent fixture").unwrap();
        std::fs::write(project.join("AGENTS.md"), "primary fixture").unwrap();
        std::fs::write(project.join("CLAUDE.md"), "fallback fixture").unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = project.clone();
        let resources = Resources::load(&args).unwrap();
        let parent = resources.instructions.find("parent fixture").unwrap();
        let child = resources.instructions.find("primary fixture").unwrap();
        assert!(parent < child);
        assert!(!resources.instructions.contains("\u{feff}parent fixture"));
        assert!(!resources.instructions.contains("fallback fixture"));
        std::fs::write(
            project.join("AGENTS.override.md"),
            vec![b'x'; 64 * 1024 + 1],
        )
        .unwrap();
        let resources = Resources::load(&args).unwrap();
        assert!(resources.instructions.contains("primary fixture"));
        assert!(
            resources
                .warnings
                .iter()
                .any(|warning| warning.contains("AGENTS.override.md"))
        );
        std::fs::write(project.join("AGENTS.override.md"), "override fixture").unwrap();
        let resources = Resources::load(&args).unwrap();
        assert!(resources.instructions.contains("override fixture"));
        assert!(!resources.instructions.contains("primary fixture"));
    }

    #[test]
    fn explicit_prompt_sources_replace_discovery_and_strip_file_boms() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path();
        std::fs::create_dir(workspace.join(".pi")).unwrap();
        std::fs::write(workspace.join(".pi/SYSTEM.md"), "\u{feff}discovered base").unwrap();
        std::fs::write(
            workspace.join(".pi/APPEND_SYSTEM.md"),
            "\u{feff}discovered addendum",
        )
        .unwrap();
        std::fs::write(workspace.join("extra.md"), "\u{feff}second").unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = workspace.to_path_buf();
        args.trusted = true;
        let resources = Resources::load(&args).unwrap();
        assert_eq!(resources.system.as_deref(), Some("discovered base"));
        assert_eq!(resources.append_system, "discovered addendum");
        std::fs::write(workspace.join(".pi/SYSTEM.md"), "x".repeat(65_537)).unwrap();
        std::fs::write(workspace.join(".pi/APPEND_SYSTEM.md"), "x".repeat(65_537)).unwrap();
        args.system_prompt = Some("explicit base".into());
        args.append_system_prompt = vec!["first".into(), "".into(), "extra.md".into()];
        let resources = Resources::load(&args).unwrap();
        assert_eq!(resources.system.as_deref(), Some("explicit base"));
        assert_eq!(resources.append_system, "first\n\nsecond");
    }

    #[test]
    fn explicit_prompt_files_only_load_markdown() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("note.txt"), "text prompt").unwrap();
        std::fs::write(root.join("review.md"), "markdown prompt").unwrap();
        let mut args = Cli::try_parse_from([
            "pi",
            "--no-skills",
            "--no-prompt-templates",
            "--prompt-template",
            "note.txt",
            "--prompt-template",
            "review.md",
        ])
        .unwrap();
        args.workspace = root.to_path_buf();
        let resources = Resources::load(&args).unwrap();
        assert!(resources.prompts.contains_key("review"));
        assert!(!resources.prompts.contains_key("note"));
    }

    #[cfg(unix)]
    #[test]
    fn explicit_prompt_alias_uses_selected_name_and_extension() {
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("content.txt"), "text target").unwrap();
        std::fs::write(root.join("content.md"), "markdown target").unwrap();
        symlink("content.txt", root.join("alias.md")).unwrap();
        symlink("content.md", root.join("alias.txt")).unwrap();
        symlink("content.txt", root.join("second.md")).unwrap();
        let mut args = Cli::try_parse_from([
            "pi",
            "--no-skills",
            "--no-prompt-templates",
            "--prompt-template",
            "alias.txt",
            "--prompt-template",
            "alias.md",
            "--prompt-template",
            "second.md",
            "--prompt-template",
            "content.md",
        ])
        .unwrap();
        args.workspace = root.to_path_buf();
        let resources = Resources::load(&args).unwrap();
        assert_eq!(
            resources
                .prompts
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["alias"]
        );
        assert_eq!(resources.prompts["alias"].body, "text target");
        args.prompt_template = vec!["content.md".into(), "alias.txt".into()];
        let resources = Resources::load(&args).unwrap();
        assert_eq!(resources.prompts["content"].body, "markdown target");
    }

    #[cfg(unix)]
    #[test]
    fn explicit_prompt_alias_deduplicates_before_suffix_filtering() {
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("content.txt"), "fixture").unwrap();
        symlink("content.txt", root.join("alias.md")).unwrap();
        let mut args = Cli::try_parse_from(["pi", "--no-skills", "--no-prompt-templates"]).unwrap();
        args.workspace = root.to_path_buf();
        args.prompt_template = vec!["content.txt".into(), "alias.md".into()];
        assert!(Resources::load(&args).unwrap().prompts.is_empty());
        args.prompt_template.reverse();
        assert_eq!(
            Resources::load(&args).unwrap().prompts["alias"].body,
            "fixture"
        );
    }

    #[test]
    fn discovered_prompt_precedes_explicit_prompt_with_same_name() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir_all(root.join(".pi/prompts")).unwrap();
        std::fs::create_dir(root.join("extra")).unwrap();
        std::fs::write(root.join(".pi/prompts/review.md"), "discovered").unwrap();
        std::fs::write(root.join("extra/review.md"), "explicit").unwrap();
        let mut args =
            Cli::try_parse_from(["pi", "--no-skills", "--prompt-template", "extra/review.md"])
                .unwrap();
        args.workspace = root.to_path_buf();
        args.trusted = true;
        args.settings = Default::default();
        let resources = Resources::load(&args).unwrap();
        assert_eq!(resources.prompts["review"].body, "discovered");
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
        let no_reader = resources.system_prompt("default", &[], Path::new("/workspace"));
        assert!(!no_reader.contains("available_skills"));
        let tools = [pi_rs::ToolSpec {
            name: "read".into(),
            description: "Read a file".into(),
            parameters: serde_json::json!({}),
        }];
        let prompt = resources.system_prompt("default", &tools, Path::new("/workspace"));
        assert!(prompt.contains("<tools>"));
        assert!(prompt.contains("demo&amp;skill"));
        assert!(prompt.contains("specific &lt;task&gt;"));
        resources.system = Some("explicit system".into());
        let custom = resources.system_prompt("default", &tools, Path::new("/workspace"));
        assert!(custom.starts_with("explicit system"));
        assert!(!custom.contains("project system"));
        assert!(!custom.contains("<rules>"));
        assert!(custom.contains("<skills>"));
    }
    #[test]
    fn malformed_prompt_warns_without_hiding_other_prompts() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path();
        std::fs::write(workspace.join("broken.md"), "---\ndescription: [\n---\nBad").unwrap();
        let description = "日".repeat(61);
        std::fs::write(
            workspace.join("valid.md"),
            format!("---\n---\n{description}"),
        )
        .unwrap();
        let mut args = Cli::try_parse_from([
            "pi",
            "--no-skills",
            "--no-prompt-templates",
            "--prompt-template",
            ".",
        ])
        .unwrap();
        args.workspace = workspace.to_owned();
        let resources = Resources::load(&args).unwrap();
        assert_eq!(resources.prompts.keys().collect::<Vec<_>>(), ["valid"]);
        assert_eq!(
            resources.prompts["valid"].description,
            format!("{}...", "日".repeat(60))
        );
        assert!(
            resources
                .warnings
                .iter()
                .any(|warning| warning.contains("broken.md"))
        );
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
    fn explicit_skill_expansion_sends_body_and_optional_user_text() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("SKILL.md");
        std::fs::write(&path, "\u{feff}---\r\nname: fixture\r\ndescription: hidden metadata\r\n---\r\n  skill body  \r\n").unwrap();
        let (name, skill) = parse_skill(&path).unwrap();
        let location = skill.path.display();
        let base = skill.path.parent().unwrap().display();
        let block = format!(
            "<skill name=\"fixture\" location=\"{location}\">\nReferences are relative to {base}.\n\nskill body\n</skill>"
        );
        let resources = Resources {
            skills: BTreeMap::from([(name, skill)]),
            ..Resources::default()
        };
        assert_eq!(resources.expand("/skill:fixture  "), Some(block.clone()));
        assert_eq!(
            resources.expand("/skill:fixture  do this \n"),
            Some(format!("{block}\n\ndo this"))
        );
        assert_eq!(resources.expand("/skill:missing"), None);
    }

    #[test]
    fn templates_preserve_quoted_arguments_defaults_and_slices() {
        assert_eq!(
            expand_arguments(
                "$1|$2|$@|${3:-default}|${@:2:1}",
                "\"API compatibility\" tests"
            ),
            "API compatibility|tests|API compatibility tests|default|tests"
        );
        assert_eq!(
            expand_arguments(
                "$1|$2|$3|$4|$5",
                r#"C:\work\file "" "two words" 'unfinished quote"#,
            ),
            r"C:\work\file||two words|unfinished quote|"
        );
        assert_eq!(
            expand_arguments(
                "$1|$ARGUMENTSsuffix|${2:-$1}|${1}|${bad:-literal}|${@:0:1}|${@:1:0}",
                "'$@'",
            ),
            "$@|$@suffix|$1|${1}|${bad:-literal}|$@|"
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

    #[test]
    fn skill_frontmatter_ignores_wrong_field_types() {
        let directory = tempfile::tempdir().unwrap();
        let skill_dir = directory.path().join("fallback-name");
        std::fs::create_dir(&skill_dir).unwrap();
        let path = skill_dir.join("SKILL.md");
        std::fs::write(
            &path,
            "---\nname: [invalid]\ndescription: A usable skill\ndisable-model-invocation: 'true'\n---\nBody",
        )
        .unwrap();
        let (name, skill) = parse_skill(&path).unwrap();
        assert_eq!(name, "fallback-name");
        assert!(skill.automatic);

        std::fs::write(
            &path,
            "---\nname: 42\ndescription: A usable skill\n---\nBody",
        )
        .unwrap();
        assert_eq!(parse_skill(&path).unwrap().0, "fallback-name");
        std::fs::write(
            &path,
            "---\ndescription: A usable skill\ndisable-model-invocation: true\n---\nBody",
        )
        .unwrap();
        assert!(!parse_skill(&path).unwrap().1.automatic);
        std::fs::write(&path, "---\ndescription: [invalid]\n---\nBody").unwrap();
        assert!(
            parse_skill(&path)
                .err()
                .unwrap()
                .to_string()
                .contains("description is required")
        );
    }
}
