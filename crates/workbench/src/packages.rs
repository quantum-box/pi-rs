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
        /// Reconcile every configured native package.
        #[arg(long = "extensions", conflicts_with = "source")]
        update_extensions: bool,
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
    inherited: Option<Value>,
    transient: bool,
}

pub async fn command(args: &Cli, command: PackageCommand) -> Result<()> {
    match command {
        PackageCommand::Install { source, local } => {
            let source = normalize(&source, &args.workspace)?;
            install(&source, args.offline).await?;
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
                let base = settings.parent().expect("settings directory");
                let mut entry = packages
                    .iter()
                    .rev()
                    .find(|entry| matches_source(entry, &source, base))
                    .cloned()
                    .unwrap_or_else(|| json!(source));
                if entry.is_object() {
                    entry["source"] = json!(source);
                } else {
                    entry = json!(source);
                }
                packages.retain(|entry| !matches_source(entry, &source, base));
                packages.push(entry);
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
                let base = settings.parent().expect("settings directory");
                packages.retain(|entry| !matches_source(entry, &target, base));
                if packages.len() == previous {
                    return Err(Error::Config(
                        "package is not configured in this scope".into(),
                    ));
                }
                Ok(())
            })?;
            output(json!({"type":"package_removed","source":target}));
        }
        PackageCommand::Update { source, .. } => {
            let requested = source
                .map(|source| normalize(&source, &args.workspace))
                .transpose()?;
            let sources = configured_update_sources(args)?;
            if let Some(source) = &requested
                && !sources
                    .iter()
                    .any(|entry| source_identity(entry) == source_identity(source))
            {
                return Err(Error::Config("package is not configured".into()));
            }
            for source in sources {
                if requested
                    .as_ref()
                    .is_none_or(|requested| source_identity(requested) == source_identity(&source))
                {
                    install(&source, args.offline).await?;
                    output(json!({"type":"package_updated","source":source}));
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
        let cached = installed.contains_key(&key(&declaration.source));
        if args.offline && remote(&declaration.source) {
            if !cached {
                eprintln!("Package {}: skipped in offline mode", declaration.source);
            }
            continue;
        }
        if declaration.transient || !cached {
            eprintln!("Preparing native package {}", declaration.source);
            match install(&declaration.source, args.offline).await {
                Ok(package) => {
                    installed.insert(key(&declaration.source), serde_json::to_value(package)?);
                }
                Err(error) if declaration.transient && !cached => return Err(error),
                Err(error) => eprintln!("Package {}: {error}", declaration.source),
            }
        }
    }
    Ok(())
}

pub fn resources(args: &Cli) -> Result<PackageResources> {
    resources_for(args, false)
}
pub fn transient_resources(args: &Cli) -> Result<PackageResources> {
    resources_for(args, true)
}
fn resources_for(args: &Cli, transient: bool) -> Result<PackageResources> {
    let installed = index()?;
    let mut result = PackageResources::default();
    for declaration in declarations(args)?
        .into_iter()
        .filter(|entry| entry.transient == transient)
    {
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
            let filter = ResourceFilter::new(&declaration.value, name)?;
            let inherited = declaration
                .inherited
                .as_ref()
                .map(|value| ResourceFilter::new(value, name))
                .transpose()?;
            for path in paths {
                let base_enabled = inherited
                    .as_ref()
                    .is_some_and(|filter| filter.includes(path, &package.root, false));
                if filter.includes(path, &package.root, base_enabled) && !target.contains(path) {
                    target.push(path.clone());
                }
            }
        }
    }
    Ok(result)
}
struct ResourceFilter {
    patterns: Option<Vec<String>>,
    autoload: bool,
}
impl ResourceFilter {
    fn new(value: &Value, resource: &str) -> Result<Self> {
        let patterns = value
            .get(resource)
            .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
            .transpose()?;
        if let Some(patterns) = &patterns {
            validate_patterns(patterns)?;
        }
        Ok(Self {
            patterns,
            autoload: value["autoload"] != false,
        })
    }
    fn includes(&self, path: &Path, root: &Path, inherited: bool) -> bool {
        if self.autoload {
            return self
                .patterns
                .as_ref()
                .is_none_or(|patterns| selected(path, root, patterns));
        }
        self.patterns
            .iter()
            .flatten()
            .fold(inherited, |enabled, pattern| {
                let prefix = pattern.chars().next();
                let override_pattern = matches!(prefix, Some('+' | '-' | '!'));
                let target = if override_pattern {
                    &pattern[1..]
                } else {
                    pattern.as_str()
                };
                if resource_matches(path, root, target, matches!(prefix, Some('+' | '-')), false) {
                    !matches!(prefix, Some('-' | '!'))
                } else {
                    enabled
                }
            })
    }
}

pub(crate) fn validate_patterns(patterns: &[String]) -> Result<()> {
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
pub(crate) fn selected(path: &Path, root: &Path, filters: &[String]) -> bool {
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
pub(crate) fn resource_matches(
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

// Updates reconcile each configured revision, including a user declaration
// shadowed by a project declaration. Transient CLI packages are not persisted
// and are refreshed by prepare instead.
fn configured_update_sources(args: &Cli) -> Result<Vec<String>> {
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
    update_sources(&files)
}

fn update_sources(files: &[(PathBuf, PathBuf)]) -> Result<Vec<String>> {
    let mut sources = Vec::new();
    for (file, base) in files {
        let document = config::read_object(file)?;
        if let Some(packages) = document.get("packages") {
            for value in packages
                .as_array()
                .ok_or_else(|| Error::Config("packages must be an array".into()))?
            {
                let source = source_field(value)
                    .ok_or_else(|| Error::Config("package needs a source".into()))?;
                let source = normalize(source, base)?;
                if !sources.contains(&source) {
                    sources.push(source);
                }
            }
        }
    }
    Ok(sources)
}
fn declarations(args: &Cli) -> Result<Vec<Declaration>> {
    let mut result = BTreeMap::new();
    let mut global = BTreeMap::new();
    let mut files = vec![(
        config::agent_dir().join("settings.json"),
        config::agent_dir(),
        false,
    )];
    if args.trusted {
        files.push((
            args.workspace.join(".pi/settings.json"),
            args.workspace.join(".pi"),
            true,
        ));
    }
    for (file, base, project) in files {
        let document = config::read_object(&file)?;
        if let Some(packages) = document.get("packages") {
            for value in packages
                .as_array()
                .ok_or_else(|| Error::Config("packages must be an array".into()))?
            {
                let source = source_field(value)
                    .ok_or_else(|| Error::Config("package needs a source".into()))?;
                let source = normalize(source, &base)?;
                let identity = source_identity(&source);
                if !project {
                    global.insert(identity.clone(), value.clone());
                }
                let inherited = if project && value["autoload"] == false {
                    global.get(&identity).cloned()
                } else {
                    None
                };
                result.insert(
                    identity,
                    Declaration {
                        source,
                        value: value.clone(),
                        inherited,
                        transient: false,
                    },
                );
            }
        }
    }
    for entry in args
        .extensions
        .iter()
        .filter(|entry| is_package_source(entry, &args.workspace))
    {
        let source = normalize(entry, &args.workspace)?;
        result.insert(
            source_identity(&source),
            Declaration {
                value: json!(source),
                source,
                inherited: None,
                transient: true,
            },
        );
    }
    Ok(result.into_values().collect())
}
pub fn is_package_source(source: &str, workspace: &Path) -> bool {
    !super::resource_paths::named_extension(source)
        && normalize(source, workspace)
            .is_ok_and(|source| remote(&source) || Path::new(&source).is_dir())
}

fn git_transport(source: &str) -> bool {
    ["https://", "http://", "ssh://", "git://", "git@"]
        .iter()
        .any(|prefix| source.starts_with(prefix))
}

fn normalize(source: &str, base: &Path) -> Result<String> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(Error::Config("package source is empty".into()));
    }
    if git_transport(trimmed) {
        return Ok(format!("git:{trimmed}"));
    }
    if let Some(source) = trimmed.strip_prefix("git:") {
        let source = source.trim();
        if source.is_empty() {
            return Err(Error::Config("git package source is empty".into()));
        }
        let source = if source.contains("://") || git_transport(source) {
            source.to_owned()
        } else {
            format!("https://{source}")
        };
        return Ok(format!("git:{source}"));
    }
    if trimmed.starts_with("cargo:") {
        return Ok(trimmed.into());
    }
    if trimmed.starts_with("npm:") {
        return Err(Error::Config(
            "native packages use Cargo, git, or local paths".into(),
        ));
    }
    let path = if trimmed.starts_with("file:") {
        let invalid = || Error::Config("invalid local package file URL".into());
        let url = reqwest::Url::parse(trimmed).map_err(|_| invalid())?;
        if url.query().is_some() || url.fragment().is_some() {
            return Err(invalid());
        }
        url.to_file_path().map_err(|_| invalid())?
    } else if source == "~" || source.starts_with("~/") {
        let path = source.strip_prefix("~/").unwrap_or("");
        PathBuf::from(
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .ok_or_else(|| Error::Config("home directory unavailable".into()))?,
        )
        .join(path)
    } else {
        base.join(source)
    };
    Ok(resolve_source_path(&path)?.to_string_lossy().into_owned())
}
fn resolve_source_path(path: &Path) -> Result<PathBuf> {
    let mut prefix = std::path::absolute(path)?;
    let mut suffix = Vec::new();
    loop {
        match prefix.canonicalize() {
            Ok(mut resolved) => {
                for component in suffix.into_iter().rev() {
                    if component == std::ffi::OsStr::new("..") {
                        resolved.pop();
                    } else if component != std::ffi::OsStr::new(".") {
                        resolved.push(component);
                    }
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = prefix
                    .components()
                    .next_back()
                    .ok_or(Error::Io(error))?
                    .as_os_str()
                    .to_os_string();
                suffix.push(component);
                if !prefix.pop() {
                    return Err(Error::Config(
                        "package source has no existing parent".into(),
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}
fn source_identity(source: &str) -> String {
    if let Some(package) = source.strip_prefix("cargo:") {
        return format!("cargo:{}", package.split('@').next().unwrap_or(package));
    }
    if let Some(source) = source.strip_prefix("git:") {
        let (url, _) = git_reference(source);
        let repository = if url.contains("://") {
            reqwest::Url::parse(url).ok()
        } else {
            url.strip_prefix("git@")
                .and_then(|source| source.split_once(':'))
                .and_then(|(host, path)| {
                    reqwest::Url::parse(&format!("ssh://git@{host}/{path}")).ok()
                })
        };
        if let Some(repository) = repository
            && let Some(host) = repository.host_str()
        {
            let path = repository.path().trim_matches('/');
            let path = path.strip_suffix(".git").unwrap_or(path);
            let port = repository.port().filter(|port| {
                !matches!(
                    (repository.scheme(), port),
                    ("ssh", 22) | ("http", 80) | ("https", 443) | ("git", 9418)
                )
            });
            let authority = port.map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"));
            return format!("git:{authority}/{path}");
        }
        let url = url.trim_end_matches('/');
        return format!("git:{}", url.strip_suffix(".git").unwrap_or(url));
    }
    source.into()
}
fn matches_source(entry: &Value, source: &str, base: &Path) -> bool {
    source_field(entry)
        .and_then(|entry| normalize(entry, base).ok())
        .is_some_and(|entry| source_identity(&entry) == source_identity(source))
}
fn git_reference(source: &str) -> (&str, &str) {
    if let Some(parts) = source.split_once('#') {
        return parts;
    }
    let path_start = if let Some((scheme, rest)) = source.split_once("://") {
        scheme.len() + 3 + rest.find('/').unwrap_or(rest.len())
    } else {
        source.find(':').map_or(0, |index| index + 1)
    };
    match source.rfind('@').filter(|index| *index >= path_start) {
        Some(index) => (&source[..index], &source[index + 1..]),
        None => (source, "HEAD"),
    }
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

fn remote(source: &str) -> bool {
    source.starts_with("cargo:") || source.starts_with("git:")
}

async fn install(source: &str, offline: bool) -> Result<Installed> {
    if offline && remote(source) {
        return Err(Error::Config(format!(
            "cannot install package in offline mode: {source}"
        )));
    }
    if !remote(source) && !Path::new(source).exists() {
        return Err(Error::Config(format!(
            "local package source does not exist: {source}"
        )));
    }
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
    let generation = tempfile::Builder::new()
        .prefix("install-")
        .tempdir_in(&directory)?;
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
            .args([
                "install",
                name,
                "--locked",
                "--force",
                "--message-format=json",
                "--root",
            ])
            .arg(generation.path().join("cargo"));
        if let Some(version) = version {
            command.args(["--version", version]);
        }
        let artifacts = run(command).await?;
        root = artifacts
            .split(|byte| *byte == b'\n')
            .filter_map(|line| {
                let artifact: Value = serde_json::from_slice(line).ok()?;
                if artifact["reason"] != "compiler-artifact" || !artifact["executable"].is_string()
                {
                    return None;
                }
                Path::new(artifact["manifest_path"].as_str()?)
                    .parent()
                    .map(Path::to_path_buf)
            })
            .next_back()
            .ok_or_else(|| {
                Error::Config("cargo did not report the installed package source".into())
            })?;
        installed_binaries = executable_files(&generation.path().join("cargo/bin"))?;
    } else if let Some(specification) = source.strip_prefix("git:") {
        let (url, reference) = git_reference(specification);
        if let Ok(parsed) = reqwest::Url::parse(url)
            && ((parsed.scheme() != "ssh" && !parsed.username().is_empty())
                || parsed.password().is_some())
        {
            return Err(Error::Config(
                "use a credential helper for git package authentication".into(),
            ));
        }
        root = generation.path().join("source");
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
            .args(["fetch", "--depth", "1", "--", "origin", reference]);
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
    if root.join("Cargo.toml").is_file() {
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
        let expected_manifest = root.join("Cargo.toml").canonicalize()?;
        if let Some(package) = metadata["packages"].as_array().and_then(|packages| {
            packages.iter().find(|package| {
                package["manifest_path"].as_str().is_some_and(|path| {
                    Path::new(path)
                        .canonicalize()
                        .is_ok_and(|path| path == expected_manifest)
                })
            })
        }) {
            if !root.join("pi-package.json").is_file() {
                manifest = json!({"pi":package["metadata"]["pi"]});
            }
            version = package["version"].as_str().map(str::to_owned);
        }
        if !source.starts_with("cargo:") {
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
            if offline {
                build.arg("--offline");
            }
            let artifacts = run(build).await?;
            for line in artifacts.split(|byte| *byte == b'\n') {
                if let Ok(artifact) = serde_json::from_slice::<Value>(line)
                    && artifact["reason"] == "compiler-artifact"
                    && let Some(executable) = artifact["executable"].as_str()
                {
                    let source = Path::new(executable);
                    let binaries = generation.path().join("bin");
                    std::fs::create_dir_all(&binaries)?;
                    let destination = binaries.join(source.file_name().ok_or_else(|| {
                        Error::Config("compiled executable has no filename".into())
                    })?);
                    std::fs::copy(source, &destination)?;
                    installed_binaries.push(destination);
                }
            }
        }
    }
    if source.starts_with("cargo:") {
        root = snapshot_registry_source(&root, &directory)?;
    }

    let pi = manifest
        .get("pi")
        .filter(|value| value.is_object())
        .unwrap_or(&manifest);
    let resources = collect_package_resources(&root, pi, installed_binaries)?;
    for executable in &resources.extensions {
        pi_rs_extensions::ExecutableExtension::load(executable.to_string_lossy().as_ref())?;
    }
    let retain_generation = root.starts_with(generation.path())
        || resources
            .extensions
            .iter()
            .any(|path| path.starts_with(generation.path()));
    let package = Installed {
        source: source.into(),
        root,
        version,
        resources,
    };
    if retain_generation {
        let _ = generation.keep();
    }
    config::update_object(&config::agent_dir().join("packages/index.json"), |index| {
        index.insert(key(source), serde_json::to_value(&package)?);
        Ok(())
    })?;
    Ok(package)
}

fn collect_package_resources(
    root: &Path,
    pi: &Value,
    mut installed_binaries: Vec<PathBuf>,
) -> Result<PackageResources> {
    let explicit = ["extensions", "skills", "prompts", "themes"]
        .iter()
        .any(|name| pi.get(name).is_some());
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
                        || manifest_selected(path, root, std::slice::from_ref(name))
                })
            })
            .collect()
    } else if explicit {
        Vec::new()
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
            .unwrap_or_else(|| {
                if explicit {
                    Vec::new()
                } else {
                    vec![name.into()]
                }
            });
        validate_patterns(&patterns)?;
        if name == "prompts" {
            *target = collect_package_prompts(root, &patterns);
            continue;
        }
        for entry in ignore::WalkBuilder::new(root)
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
                && manifest_selected(entry.path(), root, &patterns)
            {
                target.push(entry.into_path());
            }
        }
    }
    Ok(resources)
}

fn collect_package_prompts(root: &Path, patterns: &[String]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let overrides: Vec<String> = patterns
        .iter()
        .filter(|p| p.starts_with(['!', '+', '-']))
        .cloned()
        .collect();
    let plain: Vec<_> = patterns
        .iter()
        .filter(|p| !p.starts_with(['!', '+', '-']))
        .collect();
    // Override-only manifests historically selected from the package contents.
    // Do not follow symlinks while gathering that implicit candidate set.
    let implicit = plain.is_empty() && !overrides.is_empty();
    let sources: Vec<(PathBuf, bool)> = if implicit {
        vec![(root.to_path_buf(), false)]
    } else {
        plain
            .iter()
            .flat_map(|pattern| {
                let sources = if pattern.contains(['*', '?', '[', '{']) {
                    expand_prompt_glob(root, pattern)
                } else {
                    vec![root.join(pattern)]
                };
                sources.into_iter().map(|source| (source, true))
            })
            .collect()
    };
    for (source, follow_links) in sources {
        if source.is_file() {
            paths.push(source);
        } else if source.is_dir() {
            // A selected directory is the ignore root, including when it is a symlink.
            for entry in ignore::WalkBuilder::new(&source)
                .hidden(true)
                .parents(false)
                .git_ignore(true)
                .require_git(false)
                .git_global(false)
                .git_exclude(false)
                .add_custom_ignore_filename(".fdignore")
                .follow_links(follow_links)
                .filter_entry(|entry| entry.file_name() != "node_modules")
                .build()
                .flatten()
            {
                if entry.file_type().is_some_and(|kind| kind.is_file())
                    && entry.file_name().to_string_lossy().ends_with(".md")
                {
                    paths.push(entry.into_path());
                }
            }
        }
    }
    // Exact force-includes may select a file outside the plain source entries.
    for path in overrides.iter().filter_map(|p| p.strip_prefix('+')) {
        let path = root.join(path);
        if path.is_file() {
            paths.push(path);
        }
    }
    if !overrides.is_empty() {
        paths.retain(|path| manifest_selected(path, root, &overrides));
    }
    let mut seen = std::collections::HashSet::new();
    paths.retain(|path| seen.insert(path.clone()));
    paths
}

fn expand_prompt_glob(root: &Path, pattern: &str) -> Vec<PathBuf> {
    let pattern = pattern.trim_start_matches("./");
    let Ok(glob) = globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
    else {
        return Vec::new();
    };
    let matcher = glob.compile_matcher();
    let mut base = root.to_path_buf();
    for component in Path::new(pattern).components() {
        let part = component.as_os_str();
        if part.to_string_lossy().contains(['*', '?', '[', '{']) {
            break;
        }
        if part.to_string_lossy().starts_with('.') && part != "." {
            return Vec::new();
        }
        base.push(part);
    }
    let mut matches: Vec<_> = ignore::WalkBuilder::new(base)
        .hidden(true)
        .parents(false)
        .ignore(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .follow_links(false)
        .build()
        .flatten()
        .filter(|entry| {
            entry.path().strip_prefix(root).is_ok_and(|path| {
                path.components()
                    .all(|c| !c.as_os_str().to_string_lossy().starts_with('.'))
                    && matcher.is_match(path)
            })
        })
        .map(|entry| entry.into_path())
        .collect();
    matches.sort();
    matches
}
fn snapshot_registry_source(source: &Path, cache: &Path) -> Result<PathBuf> {
    use std::io::{Read, Write};
    let bundles = cache.join("sources");
    std::fs::create_dir_all(&bundles)?;
    let staging = tempfile::tempdir_in(&bundles)?;
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    let mut count = 0_usize;
    for entry in ignore::WalkBuilder::new(source)
        .hidden(false)
        .parents(false)
        .ignore(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .sort_by_file_path(|a, b| a.cmp(b))
        .filter_entry(|entry| !matches!(entry.file_name().to_str(), Some(".git" | "target")))
        .build()
    {
        let entry = entry.map_err(|error| Error::Config(error.to_string()))?;
        let relative = entry
            .path()
            .strip_prefix(source)
            .map_err(|error| Error::Config(error.to_string()))?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let destination = staging.path().join(relative);
        let metadata = entry.path().symlink_metadata()?;
        if metadata.is_dir() {
            std::fs::create_dir_all(destination)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(Error::Config(
                "published package contains a non-regular resource".into(),
            ));
        }
        count += 1;
        size = size.saturating_add(metadata.len());
        if count > 20_000 || size > 128 * 1024 * 1024 {
            return Err(Error::Config(
                "published package resources exceed the bundle limit".into(),
            ));
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        digest.update(relative.as_os_str().as_encoded_bytes());
        digest.update([0]);
        digest.update(metadata.len().to_le_bytes());
        let mut input = std::fs::File::open(entry.path())?;
        let mut output = std::fs::File::create(&destination)?;
        let mut buffer = [0; 64 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            digest.update(&buffer[..read]);
        }
        std::fs::set_permissions(&destination, metadata.permissions())?;
    }
    let destination = bundles.join(format!("{:x}", digest.finalize()));
    if !destination.exists() {
        std::fs::rename(staging.path(), &destination)?;
    }
    Ok(destination)
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
    #[cfg(unix)]
    #[test]
    fn package_prompts_follow_file_and_directory_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("package");
        let external = directory.path().join("external");
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        std::fs::create_dir_all(&external).unwrap();
        std::fs::write(external.join("file.md"), "file").unwrap();
        std::fs::write(external.join("ignored.md"), "ignored").unwrap();
        std::fs::write(external.join(".ignore"), "ignored.md\n").unwrap();
        symlink(external.join("file.md"), root.join("prompts/linked.md")).unwrap();
        symlink(&external, root.join("prompts/linked-dir")).unwrap();
        symlink(external.join("missing"), root.join("prompts/broken.md")).unwrap();
        symlink(root.join("prompts"), external.join("cycle")).unwrap();

        let mut prompts = collect_package_resources(&root, &json!({}), Vec::new())
            .unwrap()
            .prompts;
        prompts.sort();
        assert_eq!(
            prompts,
            vec![
                root.join("prompts/linked-dir/file.md"),
                root.join("prompts/linked.md"),
            ]
        );
        let selected = collect_package_resources(
            &root,
            &json!({"prompts": ["prompts/linked.md"]}),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(selected.prompts, vec![root.join("prompts/linked.md")]);
    }
    #[cfg(unix)]
    #[test]
    fn package_prompt_selection_bounds_symlink_traversal_and_globs() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("package");
        let external = temp.path().join("external");
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        std::fs::create_dir_all(&external).unwrap();
        std::fs::write(root.join("prompts/visible.md"), "visible").unwrap();
        std::fs::write(external.join("linked.md"), "linked").unwrap();
        symlink(&external, root.join("prompts/link")).unwrap();
        symlink("/", root.join("unrelated")).unwrap();

        let discover = |patterns: serde_json::Value| {
            collect_package_resources(&root, &json!({"prompts": patterns}), Vec::new())
                .unwrap()
                .prompts
        };
        assert!(discover(json!([])).is_empty());
        assert_eq!(
            discover(json!(["!prompts/missing.md"])),
            vec![root.join("prompts/visible.md")]
        );
        assert_eq!(
            discover(json!(["prompts/visible.md"])),
            vec![root.join("prompts/visible.md")]
        );
        assert_eq!(
            discover(json!(["prompts/**/*.md"])),
            vec![root.join("prompts/visible.md")]
        );
        assert_eq!(
            discover(json!(["prompts/link/*.md"])),
            vec![root.join("prompts/link/linked.md")]
        );
        assert_eq!(
            discover(json!(["./prompts/link/*.md"])),
            vec![root.join("prompts/link/linked.md")]
        );
        assert_eq!(
            discover(json!(["prompts/link"])),
            vec![root.join("prompts/link/linked.md")]
        );
    }

    #[test]
    fn package_prompt_manifest_preserves_source_order_and_sorts_each_glob() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for directory in ["z", "a"] {
            std::fs::create_dir(root.join(directory)).unwrap();
            std::fs::write(root.join(directory).join("review.md"), "review").unwrap();
        }
        std::fs::write(root.join("z/another.md"), "another").unwrap();
        let prompts = collect_package_resources(
            root,
            &json!({"prompts": ["z/review.md", "a/review.md", "z/*.md"]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert_eq!(
            prompts,
            vec![
                root.join("z/review.md"),
                root.join("a/review.md"),
                root.join("z/another.md")
            ]
        );
        let globbed =
            collect_package_resources(root, &json!({"prompts": ["./z/*.md"]}), Vec::new())
                .unwrap()
                .prompts;
        assert_eq!(
            globbed,
            vec![root.join("z/another.md"), root.join("z/review.md")]
        );
    }

    #[test]
    fn package_prompt_manifest_override_only_and_force_include() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir(root.join("prompts")).unwrap();
        for name in ["public.md", "private.md", "extra.md"] {
            std::fs::write(root.join("prompts").join(name), "fixture").unwrap();
        }
        let discover = |patterns| {
            collect_package_resources(root, &json!({"prompts": patterns}), Vec::new())
                .unwrap()
                .prompts
        };
        let mut excluded = discover(json!(["!prompts/private.md"]));
        excluded.sort();
        assert_eq!(
            excluded,
            vec![
                root.join("prompts/extra.md"),
                root.join("prompts/public.md")
            ]
        );
        assert_eq!(
            discover(json!(["prompts/public.md", "+prompts/extra.md"])),
            vec![
                root.join("prompts/public.md"),
                root.join("prompts/extra.md")
            ]
        );
        assert_eq!(
            discover(json!([
                "prompts/public.md",
                "+prompts/extra.md",
                "-prompts/extra.md"
            ])),
            vec![root.join("prompts/public.md")]
        );
    }

    #[test]
    fn package_prompt_globs_expand_brackets_and_braces() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir(root.join("prompts")).unwrap();
        for name in ["a.md", "b.md", "review.md", "deploy.md"] {
            std::fs::write(root.join("prompts").join(name), "fixture").unwrap();
        }
        std::fs::create_dir(root.join("prompts/a")).unwrap();
        std::fs::write(root.join("prompts/a/nested.md"), "fixture").unwrap();
        let discover = |pattern| {
            collect_package_resources(root, &json!({"prompts": [pattern]}), Vec::new())
                .unwrap()
                .prompts
        };
        assert_eq!(
            discover("./prompts/[ab].md"),
            vec![root.join("prompts/a.md"), root.join("prompts/b.md")]
        );
        assert_eq!(
            discover("prompts/{review,deploy}.md"),
            vec![
                root.join("prompts/deploy.md"),
                root.join("prompts/review.md")
            ]
        );
        assert_eq!(
            discover("./prompts/[ab]/*.md"),
            vec![root.join("prompts/a/nested.md")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn package_prompt_symlink_targets_respect_local_ignores_and_hidden_entries() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("package");
        let external = temp.path().join("external");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(external.join(".hidden-dir")).unwrap();
        for name in [
            "visible.md",
            "git.md",
            "fd.md",
            ".hidden.md",
            ".hidden-dir/nested.md",
        ] {
            std::fs::write(external.join(name), "fixture").unwrap();
        }
        std::fs::write(external.join(".gitignore"), "git.md\n").unwrap();
        std::fs::write(external.join(".fdignore"), "fd.md\n").unwrap();
        symlink(&external, root.join("prompts")).unwrap();
        for manifest in [json!({}), json!({"prompts": ["prompts"]})] {
            let prompts = collect_package_resources(&root, &manifest, Vec::new())
                .unwrap()
                .prompts;
            assert_eq!(prompts, vec![root.join("prompts/visible.md")]);
        }
        let explicit = collect_package_resources(
            &root,
            &json!({"prompts": ["prompts/.hidden.md", "prompts/git.md"]}),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            explicit.prompts,
            vec![root.join("prompts/.hidden.md"), root.join("prompts/git.md")]
        );
    }
    #[test]
    fn explicit_manifest_excludes_undeclared_resource_types() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        for name in ["prompts", "themes", "skills/demo"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        for name in [
            "prompts/example.md",
            "themes/example.json",
            "skills/demo/SKILL.md",
        ] {
            std::fs::write(root.join(name), "fixture").unwrap();
        }
        let binary = root.join("native-extension");
        std::fs::write(&binary, "fixture").unwrap();
        let discover =
            |manifest| collect_package_resources(root, &manifest, vec![binary.clone()]).unwrap();

        let explicit = discover(json!({"prompts": ["prompts"]}));
        assert_eq!(explicit.prompts, vec![root.join("prompts/example.md")]);
        assert!(explicit.extensions.is_empty());
        assert!(explicit.skills.is_empty());
        assert!(explicit.themes.is_empty());

        let empty = discover(json!({"prompts": []}));
        assert!(empty.prompts.is_empty() && empty.skills.is_empty() && empty.extensions.is_empty());
        let metadata_only = discover(json!({"changelogPath": "NEWS.md"}));
        assert_eq!(metadata_only.extensions, vec![binary]);
        assert_eq!(metadata_only.skills.len(), 1);
        assert_eq!(metadata_only.prompts.len(), 1);
        assert_eq!(metadata_only.themes.len(), 1);
    }
    #[test]
    fn update_reconciles_shadowed_revisions_without_transient_sources() {
        let root = tempfile::tempdir().unwrap();
        let user = root.path().join("user.json");
        let project = root.path().join("project.json");
        std::fs::write(
            &user,
            json!({"packages":["git:https://example.com/team/pkg.git#v1", "cargo:other@1"]})
                .to_string(),
        )
        .unwrap();
        std::fs::write(&project, json!({"packages":[{"source":"git:https://example.com/team/pkg.git#v2"}, "cargo:other@1"]}).to_string()).unwrap();
        let files = vec![(user, root.path().into()), (project, root.path().into())];
        let sources = update_sources(&files).unwrap();
        assert_eq!(sources.len(), 3);
        let package = sources
            .iter()
            .filter(|source| source_identity(source) == "git:example.com/team/pkg")
            .collect::<Vec<_>>();
        assert_eq!(package.len(), 2);
        assert!(package.iter().any(|source| source.ends_with("#v1")));
        assert!(package.iter().any(|source| source.ends_with("#v2")));
    }
    #[test]
    fn package_sources_normalize_file_urls_and_git_transports() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("native package");
        std::fs::create_dir(&package).unwrap();
        let url = reqwest::Url::from_file_path(&package).unwrap();
        assert!(url.as_str().contains("%20"));
        assert_eq!(
            normalize(url.as_str(), root.path()).unwrap(),
            normalize("native package", root.path()).unwrap()
        );
        assert!(is_package_source(url.as_str(), root.path()));
        let git = [
            " https://github.com/example/demo.git#v1 ",
            "http://github.com:80/example/demo.git",
            "git://github.com:9418/example/demo.git",
            "git: ssh://git@github.com:22/example/demo@main ",
            "git@github.com:example/demo",
        ];
        for source in git {
            assert_eq!(
                source_identity(&normalize(source, root.path()).unwrap()),
                "git:github.com/example/demo"
            );
        }
    }

    #[test]
    fn git_revisions_preserve_transport_credentials_and_branch_slashes() {
        for (source, url, reference) in [
            (
                "https://github.com/org/repo@v1",
                "https://github.com/org/repo",
                "v1",
            ),
            (
                "https://host/repo@feature/new",
                "https://host/repo",
                "feature/new",
            ),
            ("ssh://git@host/repo", "ssh://git@host/repo", "HEAD"),
            ("git@host:org/repo@v1", "git@host:org/repo", "v1"),
            ("git@host:org/repo", "git@host:org/repo", "HEAD"),
            (
                "https://user@host/repo#old",
                "https://user@host/repo",
                "old",
            ),
        ] {
            assert_eq!(git_reference(source), (url, reference));
        }
    }
    #[test]
    fn registry_bundles_survive_source_removal_and_keep_previous_versions() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("registry");
        let cache = root.path().join("cache");
        std::fs::create_dir_all(source.join("prompts")).unwrap();
        std::fs::write(source.join("prompts/check.md"), "first").unwrap();
        std::fs::write(source.join(".metadata"), "included").unwrap();
        let first = snapshot_registry_source(&source, &cache).unwrap();
        assert_eq!(first, snapshot_registry_source(&source, &cache).unwrap());
        std::fs::write(source.join("prompts/check.md"), "second").unwrap();
        let second = snapshot_registry_source(&source, &cache).unwrap();
        assert_ne!(first, second);
        std::fs::remove_dir_all(source).unwrap();
        assert_eq!(
            std::fs::read_to_string(first.join("prompts/check.md")).unwrap(),
            "first"
        );
        assert_eq!(
            std::fs::read_to_string(second.join("prompts/check.md")).unwrap(),
            "second"
        );
        assert_eq!(
            std::fs::read_to_string(second.join(".metadata")).unwrap(),
            "included"
        );
    }

    #[test]
    fn identities_ignore_revisions_and_missing_paths_keep_their_identity() {
        assert_eq!(
            source_identity("cargo:example@1.2.3"),
            source_identity("cargo:example@2")
        );
        assert_eq!(
            source_identity("git:https://user@host/repo.git@main"),
            source_identity("git:https://user@host/repo#feature/next")
        );
        let github = source_identity("git:https://github.com/team/repo.git@main");
        for source in [
            "git:git@github.com:team/repo.git#feature/next",
            "git:ssh://git@github.com:22/team/repo@stable",
            "git:https://user@GITHUB.COM:443/team/repo/",
        ] {
            assert_eq!(source_identity(source), github);
        }
        assert_ne!(
            source_identity("git:ssh://git@github.com:2222/team/repo"),
            github
        );
        assert_ne!(source_identity("git:https://github.com/Team/repo"), github);
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let missing = root.join("deleted");
        assert_eq!(
            normalize("deleted/../deleted", &root).unwrap(),
            missing.to_string_lossy()
        );
        assert!(matches_source(
            &json!({"source":"./deleted", "prompts":[]}),
            &missing.to_string_lossy(),
            &root
        ));
    }

    #[test]
    fn disabled_autoload_applies_only_explicit_ordered_deltas() {
        let root = Path::new("/package");
        let path = root.join("prompts/keep.md");
        let empty = ResourceFilter::new(&json!({"autoload":false}), "prompts").unwrap();
        assert!(!empty.includes(&path, root, false));
        assert!(empty.includes(&path, root, true));
        let filter = ResourceFilter::new(&json!({"autoload":false,"prompts":["prompts/*.md","-prompts/keep.md","+prompts/keep.md"]}), "prompts").unwrap();
        assert!(filter.includes(&path, root, false));
        assert!(!filter.includes(&root.join("other.md"), root, false));
        let filter = ResourceFilter::new(
            &json!({"autoload":false,"prompts":["!prompts/*.md"]}),
            "prompts",
        )
        .unwrap();
        assert!(!filter.includes(&path, root, true));
        assert!(filter.includes(&root.join("other.md"), root, true));
        let normal = ResourceFilter::new(&json!({"prompts":[]}), "prompts").unwrap();
        assert!(!normal.includes(&path, root, true));
    }

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
