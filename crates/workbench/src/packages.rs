//! Native Rust package installation and resource discovery.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use clap::Subcommand;
use pi_rs::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::{Cli, config, output};

#[derive(Clone, Subcommand)]
pub enum PackageCommand {
    Install {
        source: String,
        #[arg(short = 'l', long)]
        local: bool,
    },
    Remove {
        source: String,
        #[arg(short = 'l', long)]
        local: bool,
    },
    Update {
        source: Option<String>,
    },
    List,
    InitExtension {
        path: PathBuf,
        #[arg(long)]
        dev_path: Option<PathBuf>,
    },
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PackageResources {
    #[serde(default)]
    pub extensions: Vec<PathBuf>,
    #[serde(default)]
    pub skills: Vec<PathBuf>,
    #[serde(default)]
    pub prompts: Vec<PathBuf>,
    #[serde(default)]
    pub themes: Vec<PathBuf>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Installed {
    source: String,
    root: PathBuf,
    version: Option<String>,
    resources: PackageResources,
}
struct Declaration {
    source: String,
    value: Value,
}

pub async fn command(args: &Cli, command: PackageCommand) -> Result<()> {
    match command {
        PackageCommand::Install { source, local } => {
            let source = normalize(&source, &args.workspace)?;
            install(&source).await?;
            let settings = if local {
                args.workspace.join(".pi/settings.json")
            } else {
                config::agent_dir().join("settings.json")
            };
            config::update_object(&settings, |document| {
                let packages = document
                    .entry("packages")
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .ok_or_else(|| Error::Config("packages must be an array".into()))?;
                if !packages
                    .iter()
                    .any(|entry| source_field(entry) == Some(source.as_str()))
                {
                    packages.push(json!(source));
                }
                Ok(())
            })?;
            output(json!({"type":"package_installed","source":source}));
        }
        PackageCommand::Remove { source, local } => {
            let target = normalize(&source, &args.workspace)?;
            let settings = if local {
                args.workspace.join(".pi/settings.json")
            } else {
                config::agent_dir().join("settings.json")
            };
            config::update_object(&settings, |document| {
                let Some(packages) = document.get_mut("packages").and_then(Value::as_array_mut)
                else {
                    return Err(Error::Config("package is not configured".into()));
                };
                let previous = packages.len();
                packages.retain(|entry| source_field(entry) != Some(target.as_str()));
                if packages.len() == previous {
                    return Err(Error::Config(
                        "package is not configured in this scope".into(),
                    ));
                }
                Ok(())
            })?;
            output(json!({"type":"package_removed","source":target}));
        }
        PackageCommand::Update { source } => {
            let requested = source
                .map(|source| normalize(&source, &args.workspace))
                .transpose()?;
            let declarations = declarations(args)?;
            if let Some(source) = &requested
                && !declarations
                    .iter()
                    .any(|declaration| &declaration.source == source)
            {
                return Err(Error::Config("package is not configured".into()));
            }
            for declaration in declarations {
                if requested
                    .as_ref()
                    .is_none_or(|requested| requested == &declaration.source)
                {
                    install(&declaration.source).await?;
                    output(json!({"type":"package_updated","source":declaration.source}));
                }
            }
        }
        PackageCommand::List => {
            let index = index()?;
            for declaration in declarations(args)? {
                let installed = index
                    .get(&key(&declaration.source))
                    .cloned()
                    .map(serde_json::from_value::<Installed>)
                    .transpose()?;
                output(
                    json!({"type":"package","source":declaration.source,"installed":installed.is_some(),"version":installed.as_ref().and_then(|package| package.version.as_ref()),"root":installed.map(|package| package.root)}),
                );
            }
        }
        PackageCommand::InitExtension { path, dev_path } => scaffold(&path, dev_path.as_deref())?,
    }
    Ok(())
}

pub async fn prepare(args: &Cli) -> Result<()> {
    let mut installed = index()?;
    for declaration in declarations(args)? {
        if !installed.contains_key(&key(&declaration.source)) {
            eprintln!("Preparing native package {}", declaration.source);
            match install(&declaration.source).await {
                Ok(package) => {
                    installed.insert(key(&declaration.source), serde_json::to_value(package)?);
                }
                Err(error) => eprintln!("Package {}: {error}", declaration.source),
            }
        }
    }
    Ok(())
}

pub fn resources(args: &Cli) -> Result<PackageResources> {
    let installed = index()?;
    let mut result = PackageResources::default();
    for declaration in declarations(args)? {
        let Some(package) = installed.get(&key(&declaration.source)) else {
            continue;
        };
        let package: Installed = serde_json::from_value(package.clone())?;
        for (name, paths, target) in [
            (
                "extensions",
                &package.resources.extensions,
                &mut result.extensions,
            ),
            ("skills", &package.resources.skills, &mut result.skills),
            ("prompts", &package.resources.prompts, &mut result.prompts),
            ("themes", &package.resources.themes, &mut result.themes),
        ] {
            let filters = declaration
                .value
                .get(name)
                .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
                .transpose()?;
            if let Some(filters) = &filters {
                validate_patterns(filters)?;
            }
            for path in paths {
                if filters
                    .as_ref()
                    .is_none_or(|filters| selected(path, &package.root, filters))
                    && !target.contains(path)
                {
                    target.push(path.clone());
                }
            }
        }
    }
    Ok(result)
}
fn validate_patterns(patterns: &[String]) -> Result<()> {
    for pattern in patterns
        .iter()
        .filter(|pattern| !pattern.starts_with(['+', '-']))
    {
        let pattern = pattern
            .strip_prefix('!')
            .unwrap_or(pattern)
            .replace('\\', "/");
        globset::GlobBuilder::new(&pattern)
            .literal_separator(true)
            .backslash_escape(false)
            .build()
            .map_err(|error| {
                Error::Config(format!("invalid resource pattern {pattern:?}: {error}"))
            })?;
    }
    Ok(())
}
fn selected(path: &Path, root: &Path, filters: &[String]) -> bool {
    select_resource(path, root, filters, false)
}
fn manifest_selected(path: &Path, root: &Path, filters: &[String]) -> bool {
    select_resource(path, root, filters, true)
}
fn select_resource(path: &Path, root: &Path, filters: &[String], directory_roots: bool) -> bool {
    if filters.is_empty() {
        return false;
    }
    let plain: Vec<_> = filters
        .iter()
        .filter(|filter| !filter.starts_with(['!', '+', '-']))
        .collect();
    let mut included = plain.is_empty()
        || plain
            .iter()
            .any(|pattern| resource_matches(path, root, pattern, false, directory_roots));
    if filters
        .iter()
        .filter_map(|filter| filter.strip_prefix('!'))
        .any(|pattern| resource_matches(path, root, pattern, false, directory_roots))
    {
        included = false;
    }
    if filters
        .iter()
        .filter_map(|filter| filter.strip_prefix('+'))
        .any(|pattern| resource_matches(path, root, pattern, true, directory_roots))
    {
        included = true;
    }
    if filters
        .iter()
        .filter_map(|filter| filter.strip_prefix('-'))
        .any(|pattern| resource_matches(path, root, pattern, true, directory_roots))
    {
        included = false;
    }
    included
}
fn resource_matches(
    path: &Path,
    root: &Path,
    pattern: &str,
    exact: bool,
    directory_roots: bool,
) -> bool {
    let normalize = |path: &Path| path.to_string_lossy().replace('\\', "/");
    let pattern = pattern.replace('\\', "/");
    let pattern = pattern.trim_start_matches("./").to_owned();
    let mut candidates = vec![
        normalize(path),
        normalize(path.strip_prefix(root).unwrap_or(path)),
    ];
    if path.file_name().is_some_and(|name| name == "SKILL.md")
        && let Some(parent) = path.parent()
    {
        candidates.push(normalize(parent));
        candidates.push(normalize(parent.strip_prefix(root).unwrap_or(parent)));
        if !exact && let Some(name) = parent.file_name() {
            candidates.push(name.to_string_lossy().into_owned());
        }
    }
    if exact {
        return candidates.iter().any(|candidate| candidate == &pattern);
    }
    if let Some(name) = path.file_name() {
        candidates.push(name.to_string_lossy().into_owned());
    }
    let Ok(glob) = globset::GlobBuilder::new(&pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
    else {
        return false;
    };
    let matcher = glob.compile_matcher();
    candidates.iter().any(|candidate| {
        matcher.is_match(candidate)
            || (directory_roots
                && !pattern.contains(['*', '?', '[', '{'])
                && candidate.starts_with(&format!("{pattern}/")))
    })
}

fn source_field(value: &Value) -> Option<&str> {
    value.as_str().or_else(|| value.get("source")?.as_str())
}
fn declarations(args: &Cli) -> Result<Vec<Declaration>> {
    let mut result = BTreeMap::new();
    let mut files = vec![(
        config::agent_dir().join("settings.json"),
        config::agent_dir(),
    )];
    if args.trusted {
        files.push((
            args.workspace.join(".pi/settings.json"),
            args.workspace.join(".pi"),
        ));
    }
    for (file, base) in files {
        let document = config::read_object(&file)?;
        if let Some(packages) = document.get("packages") {
            for value in packages
                .as_array()
                .ok_or_else(|| Error::Config("packages must be an array".into()))?
            {
                let source = source_field(value)
                    .ok_or_else(|| Error::Config("package needs a source".into()))?;
                let source = normalize(source, &base)?;
                result.insert(
                    source.clone(),
                    Declaration {
                        source,
                        value: value.clone(),
                    },
                );
            }
        }
    }
    Ok(result.into_values().collect())
}
fn normalize(source: &str, base: &Path) -> Result<String> {
    if let Some(source) = source.strip_prefix("git:") {
        let source = if source.contains("://") || source.starts_with("git@") {
            source.to_owned()
        } else {
            format!("https://{source}")
        };
        return Ok(format!("git:{source}"));
    }
    if source.starts_with("https://") || source.starts_with("ssh://") || source.starts_with("git@")
    {
        return Ok(format!("git:{source}"));
    }
    if source.starts_with("cargo:") {
        return Ok(source.into());
    }
    if source.starts_with("npm:") {
        return Err(Error::Config(
            "native packages use Cargo, git, or local paths".into(),
        ));
    }
    let path = if let Some(path) = source.strip_prefix("~/") {
        PathBuf::from(
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .ok_or_else(|| Error::Config("home directory unavailable".into()))?,
        )
        .join(path)
    } else {
        base.join(source)
    };
    Ok(path.canonicalize()?.to_string_lossy().into_owned())
}
fn key(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}
fn index() -> Result<serde_json::Map<String, Value>> {
    Ok(
        config::read_object(&config::agent_dir().join("packages/index.json"))?
            .as_object()
            .expect("object")
            .clone(),
    )
}

async fn install(source: &str) -> Result<Installed> {
    let directory = config::agent_dir().join("packages").join(key(source));
    std::fs::create_dir_all(&directory)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("install.lock"))?;
    let _lock = tokio::task::spawn_blocking(move || {
        lock.lock()?;
        Ok::<_, std::io::Error>(lock)
    })
    .await
    .map_err(|error| Error::Config(error.to_string()))??;
    let mut root = PathBuf::from(source);
    let mut installed_binaries = Vec::new();
    if let Some(specification) = source.strip_prefix("cargo:") {
        let (name, version) = specification
            .rsplit_once('@')
            .map_or((specification, None), |(name, version)| {
                (name, Some(version))
            });
        let mut command = tokio::process::Command::new("cargo");
        command
            .args(["install", name, "--locked", "--root"])
            .arg(&directory);
        if let Some(version) = version {
            command.args(["--version", version]);
        }
        run(command).await?;
        root = directory.clone();
        installed_binaries = executable_files(&directory.join("bin"))?;
    } else if let Some(specification) = source.strip_prefix("git:") {
        let (url, reference) = specification
            .split_once('#')
            .unwrap_or((specification, "HEAD"));
        if let Ok(parsed) = reqwest::Url::parse(url)
            && ((parsed.scheme() != "ssh" && !parsed.username().is_empty())
                || parsed.password().is_some())
        {
            return Err(Error::Config(
                "use a credential helper for git package authentication".into(),
            ));
        }
        root = directory.join("source");
        if !root.exists() {
            let mut command = tokio::process::Command::new("git");
            command
                .args(["clone", "--no-checkout", "--depth", "1", "--", url])
                .arg(&root);
            run(command).await?;
        }
        let mut fetch = tokio::process::Command::new("git");
        fetch
            .arg("-C")
            .arg(&root)
            .args(["fetch", "--depth", "1", "origin", reference]);
        run(fetch).await?;
        let mut checkout = tokio::process::Command::new("git");
        checkout
            .arg("-C")
            .arg(&root)
            .args(["reset", "--hard", "FETCH_HEAD"]);
        run(checkout).await?;
    }
    if root.is_file() {
        installed_binaries.push(root.clone());
        root = root.parent().unwrap_or(Path::new(".")).into();
    }
    let mut manifest = if root.join("pi-package.json").is_file() {
        config::read_object(&root.join("pi-package.json"))?
    } else {
        json!({})
    };
    let mut version = manifest
        .get("version")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if root.join("Cargo.toml").is_file() && !source.starts_with("cargo:") {
        let mut metadata = tokio::process::Command::new("cargo");
        metadata
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(root.join("Cargo.toml"));
        let metadata: Value = serde_json::from_slice(&run(metadata).await?)?;
        if let Some(package) = metadata["packages"].as_array().and_then(|packages| {
            packages.iter().find(|package| {
                package["manifest_path"]
                    .as_str()
                    .is_some_and(|path| Path::new(path) == root.join("Cargo.toml"))
            })
        }) {
            if !root.join("pi-package.json").is_file() {
                manifest = json!({"pi":package["metadata"]["pi"]});
            }
            version = package["version"].as_str().map(str::to_owned);
        }
        let mut build = tokio::process::Command::new("cargo");
        build
            .args([
                "build",
                "--release",
                "--message-format=json",
                "--manifest-path",
            ])
            .arg(root.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(directory.join("target"));
        let artifacts = run(build).await?;
        for line in artifacts.split(|byte| *byte == b'\n') {
            if let Ok(artifact) = serde_json::from_slice::<Value>(line)
                && artifact["reason"] == "compiler-artifact"
                && let Some(executable) = artifact["executable"].as_str()
            {
                installed_binaries.push(executable.into());
            }
        }
    }
    let pi = manifest
        .get("pi")
        .filter(|value| value.is_object())
        .unwrap_or(&manifest);
    if installed_binaries.is_empty() {
        installed_binaries = executable_files(&root.join("extensions"))?;
    }
    let extensions = if let Some(names) = pi.get("extensions") {
        let names: Vec<String> = serde_json::from_value(names.clone())?;
        for name in &names {
            let path = root.join(name);
            if path.is_file() && !installed_binaries.contains(&path) {
                installed_binaries.push(path);
            }
        }
        installed_binaries
            .into_iter()
            .filter(|path| {
                names.iter().any(|name| {
                    path.file_stem().is_some_and(|file| file == name.as_str())
                        || manifest_selected(path, &root, std::slice::from_ref(name))
                })
            })
            .collect()
    } else if !installed_binaries.is_empty() {
        installed_binaries
    } else {
        executable_files(&root.join("extensions"))?
    };
    let mut resources = PackageResources {
        extensions,
        ..PackageResources::default()
    };
    for (name, target, suffix) in [
        ("skills", &mut resources.skills, "SKILL.md"),
        ("prompts", &mut resources.prompts, ".md"),
        ("themes", &mut resources.themes, ".json"),
    ] {
        let patterns = pi
            .get(name)
            .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
            .transpose()?
            .unwrap_or_else(|| vec![name.into()]);
        validate_patterns(&patterns)?;
        for entry in ignore::WalkBuilder::new(&root)
            .hidden(false)
            .git_ignore(false)
            .filter_entry(|entry| {
                !matches!(
                    entry.file_name().to_str(),
                    Some(".git" | "target" | "node_modules")
                )
            })
            .build()
            .flatten()
        {
            if entry.file_type().is_some_and(|kind| kind.is_file())
                && entry.file_name().to_string_lossy().ends_with(suffix)
                && manifest_selected(entry.path(), &root, &patterns)
            {
                target.push(entry.into_path());
            }
        }
    }
    for executable in &resources.extensions {
        pi_rs_extensions::ExecutableExtension::load(executable.to_string_lossy().as_ref())?;
    }
    let package = Installed {
        source: source.into(),
        root,
        version,
        resources,
    };
    config::update_object(&config::agent_dir().join("packages/index.json"), |index| {
        index.insert(key(source), serde_json::to_value(&package)?);
        Ok(())
    })?;
    Ok(package)
}
fn executable_files(path: &Path) -> Result<Vec<PathBuf>> {
    if !path.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if path.metadata()?.permissions().mode() & 0o111 == 0 {
                continue;
            }
        }
        #[cfg(windows)]
        if path.extension().is_none_or(|extension| extension != "exe") {
            continue;
        }
        files.push(path);
    }
    Ok(files)
}
async fn capture(mut stream: impl AsyncRead + Unpin, limit: usize) -> Result<(Vec<u8>, bool)> {
    let mut output = Vec::new();
    let mut overflow = false;
    let mut bytes = [0; 8192];
    loop {
        let count = stream.read(&mut bytes).await?;
        if count == 0 {
            break;
        }
        let available = limit.saturating_sub(output.len());
        overflow |= count > available;
        output.extend_from_slice(&bytes[..count.min(available)]);
    }
    Ok((output, overflow))
}
async fn run(mut command: tokio::process::Command) -> Result<Vec<u8>> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut wrapped = process_wrap::tokio::CommandWrap::from(command);
    #[cfg(unix)]
    wrapped.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    wrapped.wrap(process_wrap::tokio::JobObject);
    wrapped.wrap(process_wrap::tokio::KillOnDrop);
    let mut child = wrapped.spawn()?;
    let stdout = child.stdout().take().expect("stdout");
    let stderr = child.stderr().take().expect("stderr");
    let (status, (stdout, overflow), (stderr, _)) =
        tokio::time::timeout(Duration::from_secs(1200), async {
            tokio::try_join!(
                async { child.wait().await.map_err(Error::from) },
                capture(stdout, 8 * 1024 * 1024),
                capture(stderr, 64 * 1024)
            )
        })
        .await
        .map_err(|_| Error::Config("package command timed out".into()))??;
    if !status.success() {
        return Err(Error::Config(format!(
            "package command failed ({status}): {}",
            String::from_utf8_lossy(&stderr)
        )));
    }
    if overflow {
        return Err(Error::Config("package command output exceeds limit".into()));
    }
    Ok(stdout)
}
fn scaffold(path: &Path, dev_path: Option<&Path>) -> Result<()> {
    if path.exists() {
        return Err(Error::Config("extension destination already exists".into()));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| {
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
        })
        .ok_or_else(|| Error::Config("extension directory needs a Cargo-compatible name".into()))?;
    let dependency = |crate_name: &str| -> Result<String> {
        if let Some(root) = dev_path {
            let directory = if crate_name == "pi-rs-core" {
                "core"
            } else {
                "extensions"
            };
            Ok(format!(
                "{{ path = {} }}",
                serde_json::to_string(&root.canonicalize()?.join("crates").join(directory))?
            ))
        } else {
            Ok("{ git = \"https://github.com/quantum-box/pi-rs\", branch = \"main\" }".into())
        }
    };
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let staging = tempfile::tempdir_in(parent)?;
    std::fs::create_dir(staging.path().join("src"))?;
    std::fs::write(
        staging.path().join("Cargo.toml"),
        format!(
            "[package]\nname = {name:?}\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[package.metadata.pi]\nextensions = [{name:?}]\n\n[dependencies]\npi-rs-core = {}\npi-rs-extensions = {}\n",
            dependency("pi-rs-core")?,
            dependency("pi-rs-extensions")?
        ),
    )?;
    let source = include_str!("../../extensions/examples/project_notes.rs")
        .lines()
        .filter(|line| !line.starts_with("//! "))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("ProjectNotes", "NativeExtension")
        .replace("project-notes", name);
    std::fs::write(staging.path().join("src/main.rs"), source)?;
    std::fs::rename(staging.path(), path)?;
    output(json!({"type":"extension_created","path":path}));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filters_include_exclude_and_empty() {
        let root = Path::new("/package");
        let path = root.join("skills/demo/SKILL.md");
        let filters = |items: &[&str]| {
            items
                .iter()
                .map(|item| (*item).to_owned())
                .collect::<Vec<_>>()
        };
        assert!(manifest_selected(&path, root, &filters(&["skills"])));
        assert!(selected(&path, root, &filters(&["!skills/other"])));
        assert!(!selected(&path, root, &[]));
        assert!(!selected(
            &path,
            root,
            &filters(&["skills/**", "!skills/demo"])
        ));
        assert!(selected(
            &path,
            root,
            &filters(&["skills/**", "!skills/demo", "+skills/demo"])
        ));
    }
    #[test]
    fn exact_filter_overrides_do_not_expand_globs_or_depend_on_order() {
        let root = Path::new("/package");
        let path = root.join("prompts/nested/demo.md");
        let patterns = |items: &[&str]| {
            items
                .iter()
                .map(|item| (*item).to_owned())
                .collect::<Vec<_>>()
        };
        assert!(selected(&path, root, &patterns(&["+missing.md"])));
        assert!(selected(
            &path,
            root,
            &patterns(&["!**", "+prompts/nested/demo.md"])
        ));
        assert!(!selected(&path, root, &patterns(&["!**", "+demo.md"])));
        assert!(!selected(
            &path,
            root,
            &patterns(&["!**", "+prompts/**/*.md"])
        ));
        assert!(!selected(
            &path,
            root,
            &patterns(&["-prompts/nested/demo.md", "+prompts/nested/demo.md"])
        ));
        assert!(!selected(&path, root, &patterns(&["prompts/*.md"])));
        assert!(selected(&path, root, &patterns(&["prompts/**/*.md"])));
    }
    #[test]
    fn scaffold_is_buildable_shape_and_refuses_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("native-demo");
        scaffold(&target, None).unwrap();
        let manifest = std::fs::read_to_string(target.join("Cargo.toml")).unwrap();
        assert!(manifest.contains("[package.metadata.pi]"));
        let source = std::fs::read_to_string(target.join("src/main.rs")).unwrap();
        assert!(source.contains("struct NativeExtension"));
        assert!(source.contains("native-demo"));
        assert!(!source.contains("project_notes"));
        assert!(scaffold(&target, None).is_err());
    }
}
