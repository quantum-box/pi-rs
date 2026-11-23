//! Native Rust package installation and resource discovery.
use pi_rs_workbench::process_cleanup::{Children, owns_kill_target};
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

#[path = "package_diagnostic.rs"]
mod package_diagnostic;

#[derive(Clone, Subcommand)]
pub enum PackageCommand {
    Install {
        source: String,
        #[arg(short = 'l', long)]
        local: bool,
    },
    #[command(visible_alias = "uninstall")]
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
    scope_order: u8,
}

pub async fn command(args: &Cli, command: PackageCommand) -> Result<()> {
    match command {
        PackageCommand::Install { source, local } => {
            let source = normalize(&source, &args.workspace)?;
            install(&source, args.offline, None).await?;
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
            output(json!({"type":"package_installed","source":display_source(&source)}));
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
            output(json!({"type":"package_removed","source":display_source(&target)}));
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
            // Like Pi, offline update validates the requested source but performs no work.
            if args.offline {
                return Ok(());
            }
            for source in sources {
                if requested
                    .as_ref()
                    .is_none_or(|requested| source_identity(requested) == source_identity(&source))
                {
                    install(&source, args.offline, None).await?;
                    output(json!({"type":"package_updated","source":display_source(&source)}));
                }
            }
        }
        PackageCommand::List => {
            let index = index()?;
            for (declaration, filtered) in configured_packages(args)? {
                let installed = index
                    .get(&key(&declaration.source))
                    .cloned()
                    .map(serde_json::from_value::<Installed>)
                    .transpose()?;
                output(
                    json!({"type":"package","source":display_source(&declaration.source),"scope":if declaration.scope_order == 1 { "user" } else { "project" },"filtered":filtered,"installed":installed.is_some(),"version":installed.as_ref().and_then(|package| package.version.as_ref()),"root":installed.map(|package| package.root)}),
                );
            }
        }
        PackageCommand::InitExtension { path, dev_path } => scaffold(&path, dev_path.as_deref())?,
    }
    Ok(())
}

pub async fn prepare(args: &Cli) -> Result<()> {
    prepare_with_cleanup(args, None).await
}

pub(super) async fn prepare_with_cleanup(args: &Cli, cleanup: Option<&Children>) -> Result<()> {
    let mut installed = index()?;
    let mut plan = resource_declarations(args, false)?;
    plan.extend(resource_declarations(args, true)?);
    for declaration in plan {
        let is_remote = remote(&declaration.source);
        let cached = if is_remote {
            installed.contains_key(&key(&declaration.source))
        } else {
            installed
                .get(&key(&declaration.source))
                .is_some_and(|value| {
                    serde_json::from_value::<Installed>(value.clone())
                        .map(|package| local_source_matches_cache(&package))
                        .unwrap_or(true)
                })
        };
        if args.offline && is_remote {
            if !cached {
                eprintln!(
                    "Package {}: skipped in offline mode",
                    display_source(&declaration.source)
                );
            }
            continue;
        }
        if declaration.transient
            && cached
            && let Some(value) = installed.get(&key(&declaration.source))
            && cached_git_pin_matches(&declaration.source, value, cleanup).await
        {
            continue;
        }
        if declaration.transient || !cached {
            eprintln!(
                "Preparing native package {}",
                display_source(&declaration.source)
            );
            match install(&declaration.source, args.offline, cleanup).await {
                Ok(package) => {
                    installed.insert(key(&declaration.source), serde_json::to_value(package)?);
                }
                // Standalone share requires a complete runtime. Keep strict
                // validation for explicitly requested native files and directories.
                Err(error)
                    if cleanup.is_some() || (!is_remote && declaration.transient && !cached) =>
                {
                    return Err(error);
                }
                Err(error) => eprintln!("Package {}: {error}", display_source(&declaration.source)),
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
    resolve_package_resources(args, transient, false)
}

#[derive(PartialEq, Eq)]
enum ResourceLocation {
    WithinPackage(PathBuf),
    BuiltExecutable(std::ffi::OsString),
    External(PathBuf),
}
#[derive(Default)]
struct ResourceSelections {
    entries: Vec<(PathBuf, bool, String, ResourceLocation)>,
}
impl ResourceSelections {
    fn insert(&mut self, path: &Path, enabled: bool, identity: &str, root: &Path, extension: bool) {
        let relative = match path.strip_prefix(root) {
            Ok(relative) => ResourceLocation::WithinPackage(relative.to_path_buf()),
            Err(_) if extension => ResourceLocation::BuiltExecutable(
                path.file_name().unwrap_or(path.as_os_str()).to_os_string(),
            ),
            Err(_) => ResourceLocation::External(path.to_path_buf()),
        };
        if !self
            .entries
            .iter()
            .any(|(existing, _, prior_identity, prior_relative)| {
                existing == path || (prior_identity == identity && prior_relative == &relative)
            })
        {
            self.entries
                .push((path.to_path_buf(), enabled, identity.to_owned(), relative));
        }
    }
    fn enabled(self) -> Vec<PathBuf> {
        self.entries
            .into_iter()
            .filter_map(|(path, enabled, _, _)| enabled.then_some(path))
            .collect()
    }
}

fn resolve_package_resources(
    args: &Cli,
    transient: bool,
    startup: bool,
) -> Result<PackageResources> {
    let installed = index()?;
    let mut extensions = ResourceSelections::default();
    let mut skills = ResourceSelections::default();
    let mut prompts = ResourceSelections::default();
    let mut themes = ResourceSelections::default();
    for declaration in resource_declarations(args, transient)? {
        let package = if startup {
            startup_package(&declaration, &installed)?
        } else {
            installed
                .get(&key(&declaration.source))
                .map(|value| serde_json::from_value::<Installed>(value.clone()))
                .transpose()?
                .and_then(revalidate_cached_package)
        };
        let Some(package) = package else { continue };
        let local = !remote(&declaration.source);
        // Single-file native sources retain their existing handling; Pi loads
        // such files directly and does not apply directory package filters.
        let legacy = local && Path::new(&declaration.source).is_file();
        if legacy {
            let identity = source_identity(&declaration.source);
            extensions.insert(
                Path::new(&declaration.source),
                true,
                &identity,
                &package.root,
                true,
            );
            continue;
        }
        let pi_package =
            !legacy && local_startup_manifest(&package.root).is_ok_and(|(_, upstream)| upstream);
        let identity = source_identity(&declaration.source);
        for (name, paths, target) in [
            ("extensions", &package.resources.extensions, &mut extensions),
            ("skills", &package.resources.skills, &mut skills),
            ("prompts", &package.resources.prompts, &mut prompts),
            ("themes", &package.resources.themes, &mut themes),
        ] {
            if startup && name != "themes" && paths.is_empty() && !pi_package {
                continue;
            }
            let requested = declaration.value.get(name).and_then(Value::as_array);
            let autoload = declaration.value["autoload"] != false;
            let diagnostic = if pi_package
                && name != "themes"
                && (autoload || requested.is_some_and(|patterns| !patterns.is_empty()))
            {
                let kind = match name {
                    "extensions" => package_diagnostic::Kind::Extension,
                    "skills" => package_diagnostic::Kind::Skill,
                    "prompts" => package_diagnostic::Kind::Prompt,
                    _ => unreachable!(),
                };
                match package_diagnostic::candidates(&package.root, kind) {
                    Ok(candidates) => candidates,
                    Err(_) if local => break,
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            if name != "themes"
                && paths.is_empty()
                && (diagnostic.as_ref().is_some_and(Vec::is_empty)
                    || (startup && diagnostic.is_none()))
            {
                continue;
            }
            let filter = match ResourceFilter::new(&declaration.value, name) {
                Ok(filter) => filter,
                Err(_) if local && pi_package => break,
                Err(error) => return Err(error),
            };
            if let Some(candidates) = diagnostic {
                match filter.states(&candidates, &package.root, false) {
                    Ok(_) => {}
                    Err(_) if local => break,
                    Err(error) => return Err(error),
                }
            }
            if startup && name != "themes" && paths.is_empty() {
                continue;
            }
            let states = match filter.states(paths, &package.root, legacy) {
                Ok(states) => states,
                Err(_) if local => break,
                Err(error) => return Err(error),
            };
            for (path, state) in paths.iter().zip(states) {
                if let Some(enabled) = state {
                    target.insert(
                        path,
                        enabled,
                        &identity,
                        &package.root,
                        name == "extensions",
                    );
                }
            }
        }
    }
    Ok(PackageResources {
        extensions: extensions.enabled(),
        skills: skills.enabled(),
        prompts: prompts.enabled(),
        themes: themes.enabled(),
    })
}

fn local_source_matches_cache(package: &Installed) -> bool {
    let source = Path::new(&package.source);
    if source.is_file() {
        source.parent().is_some_and(|parent| {
            dunce::simplified(&package.root) == dunce::simplified(parent)
                && package
                    .resources
                    .extensions
                    .iter()
                    .any(|path| dunce::simplified(path) == dunce::simplified(source))
        })
    } else {
        source.is_dir() && dunce::simplified(&package.root) == dunce::simplified(source)
    }
}

fn revalidate_cached_package(mut package: Installed) -> Option<Installed> {
    if !remote(&package.source) {
        if !local_source_matches_cache(&package) {
            return None;
        }
        let source = Path::new(&package.source);
        if source.is_file() {
            package.root = source.parent()?.to_path_buf();
            package.resources = PackageResources {
                extensions: vec![source.to_path_buf()],
                ..PackageResources::default()
            };
            return Some(package);
        }
    }
    if !package.root.is_dir() {
        return None;
    }
    let (manifest, pi_package) = local_startup_manifest(&package.root).ok()?;
    let pi = manifest_pi(&manifest).ok()?;
    package.resources.themes = collect_package_themes(&package.root, pi).ok()?;
    if has_resource_fields(pi) {
        if pi_package {
            package.resources.extensions.clear();
            package.resources.skills.clear();
            package.resources.prompts.clear();
        } else {
            let extensions = resource_patterns(pi, "extensions").ok()?;
            package
                .resources
                .extensions
                .retain(|path| extension_selected(path, &package.root, &extensions));
            let skills = resource_patterns(pi, "skills").ok()?;
            package
                .resources
                .skills
                .retain(|path| manifest_selected(path, &package.root, &skills));
            let prompts = resource_patterns(pi, "prompts").ok()?;
            let selected = collect_package_prompts(&package.root, &prompts);
            package
                .resources
                .prompts
                .retain(|path| selected.contains(path));
        }
    }
    Some(package)
}
/// Resolve themes for the startup picker without preparing packages or executing
/// their extensions. Unindexed local sources can be inspected directly; missing
/// remote packages remain unavailable until normal package preparation.
pub(super) fn startup_themes(args: &Cli, transient: bool) -> Result<Vec<PathBuf>> {
    Ok(resolve_package_resources(args, transient, true)?.themes)
}

fn startup_package(
    declaration: &Declaration,
    installed: &serde_json::Map<String, Value>,
) -> Result<Option<Installed>> {
    let local = !remote(&declaration.source);
    let inspect_prior = local
        && declaration.inherited.is_none()
        && ["extensions", "skills", "prompts"].iter().any(|name| {
            declaration
                .value
                .get(name)
                .and_then(Value::as_array)
                .is_some_and(|patterns| {
                    patterns.iter().any(|pattern| {
                        pattern.as_str().is_some_and(|pattern| {
                            let pattern = pattern.strip_prefix(['!', '+', '-']).unwrap_or(pattern);
                            let pattern = pattern
                                .strip_prefix("./")
                                .or_else(|| pattern.strip_prefix(".\\"))
                                .unwrap_or(pattern);
                            pattern.starts_with("file://")
                                && pi_rs_file_url::to_file_path(pattern).is_err()
                        })
                    })
                })
        });
    let package = if !remote(&declaration.source) && !Path::new(&declaration.source).is_file() {
        let root = PathBuf::from(&declaration.source);
        if !root.is_dir() {
            return Ok(None);
        }
        let manifest = match local_startup_manifest(&root) {
            Ok((manifest, _)) => manifest,
            Err(_) => return Ok(None),
        };
        let Ok(pi) = manifest_pi(&manifest) else {
            return Ok(None);
        };
        let themes = match collect_package_themes(&root, pi) {
            Ok(themes) => themes,
            Err(_) => return Ok(None),
        };
        let resources = if inspect_prior {
            let cached = installed
                .get(&key(&declaration.source))
                .and_then(|value| serde_json::from_value::<Installed>(value.clone()).ok())
                .and_then(revalidate_cached_package);
            let mut resources = match cached {
                Some(package) => package.resources,
                None => collect_package_resources(&root, pi, Vec::new()).unwrap_or_default(),
            };
            resources.themes = themes;
            resources
        } else {
            PackageResources {
                themes,
                ..PackageResources::default()
            }
        };
        Installed {
            source: declaration.source.clone(),
            root,
            version: None,
            resources,
        }
    } else if let Some(value) = installed.get(&key(&declaration.source)) {
        let package = serde_json::from_value::<Installed>(value.clone())?;
        let Some(package) = revalidate_cached_package(package) else {
            return Ok(None);
        };
        package
    } else {
        return Ok(None);
    };
    Ok(Some(package))
}

fn collect_package_themes(root: &Path, pi: &Value) -> Result<Vec<PathBuf>> {
    let explicit = ["extensions", "skills", "prompts", "themes"]
        .iter()
        .any(|name| pi.get(name).is_some());
    let patterns = pi
        .get("themes")
        .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
        .transpose()?
        .unwrap_or_else(|| {
            if explicit {
                Vec::new()
            } else {
                vec!["themes".into()]
            }
        });
    validate_patterns(&patterns)?;
    if patterns.is_empty() {
        return Ok(Vec::new());
    }
    let mut themes = Vec::new();
    // Explicit files are authoritative: ignore files and symlink traversal
    // rules for directory scans must not hide a named manifest entry.
    for pattern in &patterns {
        let candidate = pattern.strip_prefix('+').unwrap_or(pattern);
        if pattern.starts_with(['!', '-']) || candidate.contains(['*', '?', '[', '{']) {
            continue;
        }
        let path = root.join(candidate.replace('\\', "/"));
        if path.is_file()
            && path.extension().is_some_and(|ext| ext == "json")
            && manifest_selected(&path, root, &patterns)
        {
            themes.push(path);
        }
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
            && entry.file_name().to_string_lossy().ends_with(".json")
            && manifest_selected(entry.path(), root, &patterns)
        {
            themes.push(entry.into_path());
        }
    }
    let mut seen = std::collections::HashSet::new();
    themes.retain(|path| seen.insert(dunce::simplified(path).to_path_buf()));
    Ok(themes)
}

const MAX_PACKAGE_MANIFEST_BYTES: u64 = 1024 * 1024;

// Open nonblocking before checking the opened descriptor, so a FIFO or device
// substituted between path inspection and open cannot hang the startup UI.
fn read_optional_manifest(root: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    read_optional_manifest_after_probe(root, name, || {})
}

fn read_optional_manifest_after_probe(
    root: &Path,
    name: &str,
    after_probe: impl FnOnce(),
) -> Result<Option<Vec<u8>>> {
    let path = root.join(name);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::Config(format!("{}: {error}", path.display()))),
    }
    after_probe();
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options
        .open(&path)
        .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
    let metadata = file
        .metadata()
        .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
    if !metadata.is_file() {
        return Err(Error::Config(format!(
            "{} must be a regular file",
            path.display()
        )));
    }
    if metadata.len() > MAX_PACKAGE_MANIFEST_BYTES {
        return Err(Error::Config(format!(
            "{} exceeds the package manifest size limit",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut std::io::Read::take(file, MAX_PACKAGE_MANIFEST_BYTES + 1),
        &mut bytes,
    )
    .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
    if bytes.len() as u64 > MAX_PACKAGE_MANIFEST_BYTES {
        return Err(Error::Config(format!(
            "{} exceeds the package manifest size limit",
            path.display()
        )));
    }
    Ok(Some(bytes))
}

// Mirror installation precedence without running Cargo or loading executables.
fn local_startup_manifest(root: &Path) -> Result<(Value, bool)> {
    let pi_bytes = read_optional_manifest(root, "pi-package.json")?;
    let cargo_bytes = read_optional_manifest(root, "Cargo.toml")?;
    let cargo_manifest = if let Some(bytes) = cargo_bytes {
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| Error::Config(format!("invalid Cargo manifest: {error}")))?;
        let manifest: toml::Value = toml::from_str(text)
            .map_err(|error| Error::Config(format!("invalid Cargo manifest: {error}")))?;
        let metadata = manifest
            .get("package")
            .and_then(|v| v.get("metadata"))
            .and_then(|v| v.get("pi"));
        if let Some(metadata) = metadata {
            validate_pi(&serde_json::to_value(metadata)?)?;
        }
        Some(manifest)
    } else {
        None
    };
    // Validate a competing Pi package manifest even when a higher-priority
    // native or Cargo manifest supplies the selected resources.
    let package_manifest = read_optional_pi_package_json(root)?;
    if let Some(bytes) = pi_bytes {
        let manifest = parse_manifest_object(&root.join("pi-package.json"), &bytes, false)?;
        manifest_pi(&manifest)?;
        return Ok((manifest, false));
    }
    if let Some(metadata) = cargo_manifest.as_ref().and_then(|manifest| {
        manifest
            .get("package")
            .and_then(|value| value.get("metadata"))
            .and_then(|value| value.get("pi"))
    }) {
        return Ok((json!({"pi": metadata}), false));
    }
    let pi_package = package_manifest.is_some();
    Ok((package_manifest.unwrap_or_else(|| json!({})), pi_package))
}

// A present manifest is authoritative, including broken links or devices.
fn read_optional_pi_package_json(root: &Path) -> Result<Option<Value>> {
    read_optional_manifest(root, "package.json")?
        .map(|bytes| parse_pi_package_json(&root.join("package.json"), &bytes))
        .transpose()
}

fn parse_manifest_object(path: &Path, bytes: &[u8], strip_bom: bool) -> Result<Value> {
    let input = if strip_bom {
        bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes)
    } else {
        bytes
    };
    let value: Value = serde_json::from_slice(input)
        .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
    if !value.is_object() {
        return Err(Error::Config(format!(
            "{} must contain an object",
            path.display()
        )));
    }
    Ok(value)
}

fn has_resource_fields(pi: &Value) -> bool {
    ["extensions", "skills", "prompts", "themes"]
        .iter()
        .any(|name| pi.get(name).is_some())
}

fn resource_patterns(pi: &Value, name: &str) -> Result<Vec<String>> {
    let patterns = pi
        .get(name)
        .map(|value| serde_json::from_value::<Vec<String>>(value.clone()))
        .transpose()?
        .unwrap_or_default();
    validate_patterns(&patterns)?;
    Ok(patterns)
}

fn validate_pi(pi: &Value) -> Result<()> {
    if !pi.is_object() {
        return Err(Error::Config("pi must contain an object".into()));
    }
    for name in ["extensions", "skills", "prompts", "themes"] {
        resource_patterns(pi, name)?;
    }
    Ok(())
}

fn manifest_pi(manifest: &Value) -> Result<&Value> {
    let pi = manifest.get("pi").unwrap_or(manifest);
    validate_pi(pi)?;
    Ok(pi)
}

// Pi packages describe resources in package.json's `pi` object. Never run
// package scripts while reading it, and accept Pi's UTF-8 BOM handling.
fn parse_pi_package_json(path: &Path, bytes: &[u8]) -> Result<Value> {
    let value = parse_manifest_object(path, bytes, true)?;
    let pi = value.get("pi");
    if let Some(pi) = pi {
        validate_pi(pi)?;
    }
    let explicit = pi.is_some_and(has_resource_fields);
    let theme_only = if explicit {
        json!({"extensions": [], "skills": [], "prompts": [],
            "themes": pi.and_then(|pi| pi.get("themes")).cloned().unwrap_or_else(|| json!([]))})
    } else {
        json!({})
    };
    // Treat an explicit Pi resource manifest as authoritative before scanning
    // native executables; only theme entries are supported from this format.
    Ok(json!({"pi": theme_only, "version": value.get("version")}))
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
            validate_resource_patterns(patterns, true)?;
        }
        Ok(Self {
            patterns,
            autoload: value["autoload"] != false,
        })
    }
    // Local and layered packages retain the previous behavior until their
    // separately caught errors and evaluation order can be reproduced.
    fn includes_legacy(&self, path: &Path, root: &Path, inherited: bool) -> bool {
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
                let target = if matches!(prefix, Some('+' | '-' | '!')) {
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

    // Delta filters write only matched paths. Computing from both initial
    // states distinguishes an untouched path from an explicit enable/disable.
    fn states(&self, paths: &[PathBuf], root: &Path, legacy: bool) -> Result<Vec<Option<bool>>> {
        let run = |initial: bool| -> Result<Vec<bool>> {
            if legacy {
                Ok(paths
                    .iter()
                    .map(|path| self.includes_legacy(path, root, initial))
                    .collect())
            } else {
                self.apply(paths, root, &vec![initial; paths.len()])
            }
        };
        let disabled = run(false)?;
        if self.autoload {
            return Ok(disabled.into_iter().map(Some).collect());
        }
        let enabled = run(true)?;
        Ok(disabled
            .into_iter()
            .zip(enabled)
            .map(|(a, b)| (a == b).then_some(a))
            .collect())
    }

    #[cfg(test)]
    fn includes(&self, path: &Path, root: &Path, inherited: bool) -> Result<bool> {
        Ok(self.apply(&[path.to_path_buf()], root, &[inherited])?[0])
    }

    fn apply(&self, paths: &[PathBuf], root: &Path, inherited: &[bool]) -> Result<Vec<bool>> {
        let Some(patterns) = &self.patterns else {
            return Ok(if self.autoload {
                vec![true; paths.len()]
            } else {
                inherited.to_vec()
            });
        };
        let matches = |path: &Path, pattern: &str, exact: bool| -> Result<bool> {
            let normalized = if exact {
                pattern
                    .strip_prefix("./")
                    .or_else(|| pattern.strip_prefix(".\\"))
                    .unwrap_or(pattern)
            } else {
                pattern
            };
            if normalized.starts_with("file://") {
                pi_rs_file_url::to_file_path(normalized).map_err(Error::Config)?;
            }
            Ok(resource_matches(path, root, pattern, exact, false))
        };
        if !self.autoload {
            let mut enabled = inherited.to_vec();
            for pattern in patterns {
                let prefix = pattern.chars().next();
                let override_pattern = matches!(prefix, Some('+' | '-' | '!'));
                let target = if override_pattern {
                    &pattern[1..]
                } else {
                    pattern.as_str()
                };
                for (path, enabled) in paths.iter().zip(&mut enabled) {
                    if matches(path, target, matches!(prefix, Some('+' | '-')))? {
                        *enabled = !matches!(prefix, Some('-' | '!'));
                    }
                }
            }
            return Ok(enabled);
        }
        if patterns.is_empty() {
            return Ok(vec![false; paths.len()]);
        }
        let any = |path: &Path, prefix: Option<char>, exact: bool| -> Result<bool> {
            for pattern in patterns {
                let target = match prefix {
                    Some(prefix) => match pattern.strip_prefix(prefix) {
                        Some(target) => target,
                        None => continue,
                    },
                    None if !pattern.starts_with(['!', '+', '-']) => pattern.as_str(),
                    None => continue,
                };
                if matches(path, target, exact)? {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let has_plain = patterns.iter().any(|p| !p.starts_with(['!', '+', '-']));
        let mut enabled = vec![!has_plain; paths.len()];
        if has_plain {
            for (path, enabled) in paths.iter().zip(&mut enabled) {
                *enabled = any(path, None, false)?;
            }
        }
        for (prefix, exact, current, replacement) in [
            ('!', false, true, false),
            ('+', true, false, true),
            ('-', true, true, false),
        ] {
            for (path, enabled) in paths.iter().zip(&mut enabled) {
                if *enabled == current && any(path, Some(prefix), exact)? {
                    *enabled = replacement;
                }
            }
        }
        Ok(enabled)
    }
}

pub(crate) fn validate_patterns(patterns: &[String]) -> Result<()> {
    validate_resource_patterns(patterns, false)
}

fn validate_resource_patterns(patterns: &[String], settings_filter: bool) -> Result<()> {
    for pattern in patterns
        .iter()
        .filter(|pattern| !pattern.starts_with(['+', '-']))
    {
        let pattern = pattern
            .strip_prefix('!')
            .unwrap_or(pattern)
            .replace('\\', "/");
        if settings_filter && pattern.starts_with('#') {
            continue;
        }
        let target = if settings_filter {
            pattern.trim_start_matches('!')
        } else {
            &pattern
        };
        resource_glob(target, settings_filter).map_err(|error| {
            Error::Config(format!("invalid resource pattern {pattern:?}: {error}"))
        })?;
    }
    Ok(())
}

fn resource_glob(
    pattern: &str,
    allow_unclosed_class: bool,
) -> std::result::Result<globset::Glob, globset::Error> {
    globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .allow_unclosed_class(allow_unclosed_class)
        .build()
}

pub(crate) fn selected(path: &Path, root: &Path, filters: &[String]) -> bool {
    select_resource(path, root, filters, false)
}
fn manifest_selected(path: &Path, root: &Path, filters: &[String]) -> bool {
    select_resource(path, root, filters, true)
}
fn select_resource(path: &Path, root: &Path, filters: &[String], directory_roots: bool) -> bool {
    select_resource_with(filters, |pattern, exact| {
        resource_matches(path, root, pattern, exact, directory_roots)
    })
}
fn extension_selected(path: &Path, root: &Path, filters: &[String]) -> bool {
    select_resource_with(filters, |pattern, exact| {
        resource_matches(path, root, pattern, exact, true)
            || path.file_stem().is_some_and(|stem| stem == pattern)
    })
}
fn select_resource_with(filters: &[String], matches: impl Fn(&str, bool) -> bool) -> bool {
    if filters.is_empty() {
        return false;
    }
    let plain: Vec<_> = filters
        .iter()
        .filter(|filter| !filter.starts_with(['!', '+', '-']))
        .collect();
    let mut included = plain.is_empty() || plain.iter().any(|pattern| matches(pattern, false));
    if filters
        .iter()
        .filter_map(|filter| filter.strip_prefix('!'))
        .any(|pattern| matches(pattern, false))
    {
        included = false;
    }
    if filters
        .iter()
        .filter_map(|filter| filter.strip_prefix('+'))
        .any(|pattern| matches(pattern, true))
    {
        included = true;
    }
    if filters
        .iter()
        .filter_map(|filter| filter.strip_prefix('-'))
        .any(|pattern| matches(pattern, true))
    {
        included = false;
    }
    included
}

// Match Node's os.homedir(): the platform-specific environment variable takes
// precedence even when empty; otherwise query the current account.
fn filter_home_dir() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        if let Some(home) = std::env::var_os("HOME") {
            return Some(home.into());
        }
        let mut size = 1024;
        loop {
            let mut buffer = vec![0u8; size];
            let mut entry = std::mem::MaybeUninit::<libc::passwd>::uninit();
            let mut result = std::ptr::null_mut();
            // getpwuid_r owns no returned memory: pw_dir points into buffer.
            let status = unsafe {
                libc::getpwuid_r(
                    libc::geteuid(),
                    entry.as_mut_ptr(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    &mut result,
                )
            };
            if status == libc::ERANGE && size < 1024 * 1024 {
                size *= 2;
                continue;
            }
            if status != 0 || result.is_null() {
                return None;
            }
            let dir = unsafe { std::ffi::CStr::from_ptr((*result).pw_dir) };
            return Some(std::ffi::OsString::from_vec(dir.to_bytes().to_vec()).into());
        }
    }
    #[cfg(windows)]
    {
        if let Some(home) = std::env::var_os("USERPROFILE") {
            return Some(home.into());
        }
        // On Windows std uses GetUserProfileDirectoryW as its fallback.
        #[allow(deprecated)]
        std::env::home_dir()
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

fn normalize_filter_home(home: &Path, suffix: &str) -> PathBuf {
    // Node's path.join does not let a later absolute component replace home.
    let joined = if home.as_os_str().is_empty() {
        suffix.to_owned()
    } else {
        format!("{}/{suffix}", home.to_string_lossy())
    };
    normalize_filter_path(Path::new(&joined))
}

fn normalize_filter_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(result.components().next_back(), Some(Component::Normal(_))) {
                    result.pop();
                } else if !result.has_root() {
                    result.push("..");
                }
            }
            component => result.push(component.as_os_str()),
        }
    }
    result
}

pub(crate) fn resource_matches(
    path: &Path,
    root: &Path,
    pattern: &str,
    exact: bool,
    directory_roots: bool,
) -> bool {
    let path = dunce::simplified(path);
    let root = dunce::simplified(root);
    let normalize = |path: &Path| path.to_string_lossy().replace('\\', "/");
    // Upstream removes one leading ./ or .\ from exact overrides before
    // expanding a tilde or converting a file URL.
    let pattern = if exact {
        pattern
            .strip_prefix("./")
            .or_else(|| pattern.strip_prefix(".\\"))
            .unwrap_or(pattern)
    } else {
        pattern
    };
    #[cfg(windows)]
    let windows_pattern = pi_rs_workbench::windows_shell_path(pattern);
    #[cfg(windows)]
    let pattern = windows_pattern.as_str();
    // Pi normalizes home-relative paths and file URLs before matching both
    // ordinary patterns and exact +/- overrides.
    let expanded = if pattern == "~"
        || pattern.starts_with("~/")
        || (cfg!(windows) && pattern.starts_with("~\\"))
    {
        filter_home_dir().map(|home| {
            if pattern == "~" {
                home
            } else {
                normalize_filter_home(&home, &pattern[2..])
            }
        })
    } else if pattern.starts_with("file://") {
        match pi_rs_file_url::to_file_path(pattern) {
            Ok(path) => Some(path),
            Err(_) => return false,
        }
    } else {
        None
    };
    let pattern = expanded
        .as_deref()
        .map(normalize)
        .unwrap_or_else(|| pattern.replace('\\', "/"));
    let (pattern, negated) = if !exact && !directory_roots {
        if pattern.starts_with('#') {
            return false;
        }
        let target = pattern.trim_start_matches('!');
        (target, (pattern.len() - target.len()) % 2 == 1)
    } else {
        (pattern.as_str(), false)
    };
    let pattern = if exact {
        pattern
    } else {
        pattern.trim_start_matches("./")
    };
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
        return candidates.iter().any(|candidate| candidate == pattern);
    }
    if let Some(name) = path.file_name() {
        candidates.push(name.to_string_lossy().into_owned());
    }
    let positive_extglob = package_diagnostic::extglob_regex(pattern).flatten();
    let negative_extglob = if directory_roots {
        None
    } else {
        package_diagnostic::negative_extglob_regex(pattern)
    };
    if positive_extglob.is_some() || negative_extglob.is_some() {
        // The bounded translators match ASCII paths without hidden components.
        // Keep using the native matcher for other paths or a backtracking error.
        let mut matched = false;
        candidates.retain(|candidate| {
            if !candidate.is_ascii() || candidate.split('/').any(|part| part.starts_with('.')) {
                return true;
            }
            let extglob_match = if let Some(regex) = &negative_extglob {
                regex.is_match(candidate).ok()
            } else {
                positive_extglob
                    .as_ref()
                    .map(|regex| regex.is_match(candidate))
            };
            if let Some(extglob_match) = extglob_match {
                matched |= extglob_match != negated;
                false
            } else {
                true
            }
        });
        if matched || candidates.is_empty() {
            return matched;
        }
    }
    let Ok(glob) = resource_glob(pattern, !directory_roots) else {
        return false;
    };
    let matcher = glob.compile_matcher();
    candidates.iter().any(|candidate| {
        // Pi passes each spelling to minimatch separately, including when
        // that glob is negated.
        let matches = matcher.is_match(candidate)
            && (directory_roots || matches_hidden_filter_segments(candidate, pattern));
        (matches != negated)
            || (directory_roots
                && !pattern.contains(['*', '?', '[', '{'])
                && candidate.starts_with(&format!("{pattern}/")))
    })
}

// Minimatch requires a leading dot in the pattern segment that matches a
// hidden component. A globstar can skip zero segments or consume visible ones.
fn matches_hidden_filter_segments(candidate: &str, pattern: &str) -> bool {
    if !candidate.split('/').any(|segment| segment.starts_with('.')) {
        return true;
    }
    if pattern.contains('{') {
        // Keep the existing glob match for brace combinations outside the
        // bounded expansion. A leading dot can come from any alternative.
        return package_diagnostic::expand_diagnostic_braces(pattern).is_none_or(|variants| {
            variants
                .iter()
                .any(|variant| matches_hidden_filter_segments(candidate, variant))
        });
    }
    let components: Vec<_> = candidate.split('/').collect();
    let mut matched = vec![false; components.len() + 1];
    matched[0] = true;
    for segment in pattern.split('/') {
        if segment == "**" {
            for (index, component) in components.iter().enumerate() {
                matched[index + 1] |= matched[index] && !component.starts_with('.');
            }
            continue;
        }
        let Ok(glob) = resource_glob(segment, true) else {
            return false;
        };
        let matcher = glob.compile_matcher();
        let allows_dot = segment.starts_with('.') || segment.starts_with("[.]");
        for (index, component) in components.iter().enumerate().rev() {
            matched[index + 1] = matched[index]
                && (allows_dot || !component.starts_with('.'))
                && matcher.is_match(component);
        }
        matched[0] = false;
    }
    matched[components.len()]
}

fn source_field(value: &Value) -> Option<&str> {
    value.as_str().or_else(|| value.get("source")?.as_str())
}

// Git URL userinfo is rejected before invoking Git, but declarations can
// still contain it. Never print it while reporting or listing a package.
fn display_source(source: &str) -> String {
    let Some(git) = source.strip_prefix("git:") else {
        return source.to_owned();
    };
    let Some(scheme_end) = git.find("://") else {
        return source.to_owned();
    };
    let start = "git:".len() + scheme_end + 3;
    let end = source[start..]
        .find(['/', '?', '#'])
        .map_or(source.len(), |i| start + i);
    let Some(at) = source[start..end].rfind('@') else {
        return source.to_owned();
    };
    format!("{}[redacted]{}", &source[..start], &source[start + at..])
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
    update_sources_with_settings(&files, Some(&args.settings))
}

#[cfg(test)]
fn update_sources(files: &[(PathBuf, PathBuf)]) -> Result<Vec<String>> {
    update_sources_with_settings(files, None)
}

fn update_sources_with_settings(
    files: &[(PathBuf, PathBuf)],
    settings: Option<&config::Settings>,
) -> Result<Vec<String>> {
    let mut sources = Vec::new();
    for (file, base) in files {
        let document = match settings.and_then(|settings| settings.stored_layer(base)) {
            Some(snapshot) => snapshot.clone(),
            None => config::read_object(file)?,
        };
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
fn configured_packages(args: &Cli) -> Result<Vec<(Declaration, bool)>> {
    let mut files = vec![(
        config::agent_dir().join("settings.json"),
        config::agent_dir(),
        1,
    )];
    if args.trusted {
        files.push((
            args.workspace.join(".pi/settings.json"),
            args.workspace.join(".pi"),
            0,
        ));
    }
    let mut result = Vec::new();
    for (file, base, scope_order) in files {
        let document = match args.settings.stored_layer(&base) {
            Some(snapshot) => snapshot.clone(),
            None => config::read_object(&file)?,
        };
        if let Some(packages) = document.get("packages") {
            for value in packages
                .as_array()
                .ok_or_else(|| Error::Config("packages must be an array".into()))?
            {
                let source = source_field(value)
                    .ok_or_else(|| Error::Config("package needs a source".into()))?;
                result.push((
                    Declaration {
                        source: normalize(source, &base)?,
                        value: value.clone(),
                        inherited: None,
                        transient: false,
                        scope_order,
                    },
                    value.is_object(),
                ));
            }
        }
    }
    Ok(result)
}

fn declarations(args: &Cli) -> Result<Vec<Declaration>> {
    let mut result = BTreeMap::new();
    let mut global = BTreeMap::new();
    let mut order = BTreeMap::new();
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
        let document = match args.settings.stored_layer(&base) {
            Some(snapshot) => snapshot.clone(),
            None => config::read_object(&file)?,
        };
        if let Some(packages) = document.get("packages") {
            for (position, value) in packages
                .as_array()
                .ok_or_else(|| Error::Config("packages must be an array".into()))?
                .iter()
                .enumerate()
            {
                let source = source_field(value)
                    .ok_or_else(|| Error::Config("package needs a source".into()))?;
                let source = normalize(source, &base)?;
                let identity = source_identity(&source);
                let scope = if project { 0 } else { 1 };
                // User duplicates keep the first value; project duplicates replace
                // that value while retaining its original position.
                let same_scope = order
                    .get(&identity)
                    .is_some_and(|(seen_scope, _)| *seen_scope == scope);
                if same_scope && !project {
                    continue;
                }
                if !project {
                    global.insert(identity.clone(), value.clone());
                }
                let inherited = if project && value["autoload"] == false {
                    global.get(&identity).cloned()
                } else {
                    None
                };
                if !same_scope {
                    order.insert(identity.clone(), (scope, position));
                }
                result.insert(
                    identity,
                    Declaration {
                        source,
                        value: value.clone(),
                        inherited,
                        transient: false,
                        scope_order: scope,
                    },
                );
            }
        }
    }
    for (position, entry) in args
        .extensions
        .iter()
        .enumerate()
        .filter(|(_, entry)| is_package_source(entry, &args.workspace))
    {
        let source = normalize(entry, &args.workspace)?;
        let identity = source_identity(&source);
        if order.get(&identity).is_some_and(|(scope, _)| *scope == 2) {
            continue;
        }
        order.insert(identity.clone(), (2, position));
        result.insert(
            identity,
            Declaration {
                value: json!(source),
                source,
                inherited: None,
                transient: true,
                scope_order: 2,
            },
        );
    }
    let mut entries: Vec<_> = result.into_iter().collect();
    entries.sort_by_key(|(identity, _)| order[identity]);
    Ok(entries
        .into_iter()
        .map(|(_, declaration)| declaration)
        .collect())
}
// Resource resolution retains project deltas and each corresponding user
// declaration separately. Other package commands continue to use declarations().
fn resource_declarations(args: &Cli, transient: bool) -> Result<Vec<Declaration>> {
    if transient {
        return Ok(declarations(args)?
            .into_iter()
            .filter(|entry| entry.transient)
            .collect());
    }
    let agent = config::agent_dir();
    let project = args.workspace.join(".pi");
    let read = |base: &Path| -> Result<Vec<Declaration>> {
        let settings = match args.settings.stored_layer(base) {
            Some(snapshot) => snapshot.clone(),
            None => config::read_object(&base.join("settings.json"))?,
        };
        let Some(packages) = settings.get("packages") else {
            return Ok(Vec::new());
        };
        packages
            .as_array()
            .ok_or_else(|| Error::Config("packages must be an array".into()))?
            .iter()
            .map(|value| {
                let source = source_field(value)
                    .ok_or_else(|| Error::Config("package needs a source".into()))?;
                Ok(Declaration {
                    source: normalize(source, base)?,
                    value: value.clone(),
                    inherited: None,
                    transient: false,
                    scope_order: 0,
                })
            })
            .collect()
    };
    let transient_identities: std::collections::BTreeSet<_> = declarations(args)?
        .into_iter()
        .filter(|entry| entry.transient)
        .map(|entry| source_identity(&entry.source))
        .collect();
    let users: Vec<_> = read(&agent)?
        .into_iter()
        .filter(|entry| !transient_identities.contains(&source_identity(&entry.source)))
        .collect();
    let projects = if args.trusted {
        read(&project)?
            .into_iter()
            .filter(|entry| !transient_identities.contains(&source_identity(&entry.source)))
            .collect()
    } else {
        Vec::new()
    };
    let cached = index()?;
    let mut plan: Vec<Declaration> = Vec::new();
    let mut projects_by_identity = BTreeMap::new();
    for mut entry in projects {
        let identity = source_identity(&entry.source);
        if entry.value["autoload"] == false {
            let first = users
                .iter()
                .find(|user| source_identity(&user.source) == identity);
            let available = users.iter().find(|user| {
                source_identity(&user.source) == identity
                    && cached
                        .get(&key(&user.source))
                        .and_then(|value| serde_json::from_value::<Installed>(value.clone()).ok())
                        .and_then(revalidate_cached_package)
                        .is_some()
            });
            if let Some(user) = available.or(first) {
                entry.source = user.source.clone();
            }
        }
        if let Some(index) = projects_by_identity.get(&identity) {
            plan[*index] = entry;
        } else {
            projects_by_identity.insert(identity, plan.len());
            plan.push(entry);
        }
    }
    let mut user_only = std::collections::BTreeSet::new();
    for mut entry in users {
        let identity = source_identity(&entry.source);
        if let Some(index) = projects_by_identity.get(&identity) {
            if plan[*index].value["autoload"] != false {
                continue;
            }
        } else if !user_only.insert(identity) {
            continue;
        }
        entry.scope_order = 1;
        plan.push(entry);
    }
    Ok(plan)
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
    #[cfg(windows)]
    let windows_path = pi_rs_workbench::windows_shell_path(trimmed);
    #[cfg(windows)]
    let trimmed = windows_path.as_str();
    let path = if trimmed.starts_with("file://") {
        pi_rs_file_url::to_file_path(trimmed).map_err(Error::Config)?
    } else if trimmed == "~"
        || trimmed.starts_with("~/")
        || (cfg!(windows) && trimmed.starts_with("~\\"))
    {
        // Package source resolution uses HOME when nonempty, then os.homedir().
        let home = std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .map(PathBuf::from)
            .or_else(filter_home_dir)
            .ok_or_else(|| Error::Config("home directory unavailable".into()))?;
        let suffix = if trimmed == "~" { "" } else { &trimmed[2..] };
        let expanded = normalize_filter_home(&home, suffix);
        if expanded.is_absolute() {
            expanded
        } else {
            base.join(expanded)
        }
    } else {
        base.join(trimmed)
    };
    Ok(resolve_source_path(&path)?.to_string_lossy().into_owned())
}
fn resolve_source_path(path: &Path) -> Result<PathBuf> {
    // Pi resolves dot segments before asking the filesystem to compare
    // package paths. A symlink followed by ".." therefore never redirects
    // the package to the parent of the symlink target.
    let mut prefix = normalize_filter_path(&std::path::absolute(path)?);
    let mut suffix = Vec::new();
    loop {
        match prefix.canonicalize() {
            Ok(mut resolved) => {
                resolved.extend(suffix.into_iter().rev());
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
fn git_path_start(source: &str) -> usize {
    if let Some((scheme, rest)) = source.split_once("://") {
        scheme.len() + 3 + rest.find('/').unwrap_or(rest.len())
    } else {
        source.find(':').map_or(0, |index| index + 1)
    }
}
fn ambiguous_git_reference(source: &str) -> bool {
    !source.contains('#')
        && source[git_path_start(source)..]
            .bytes()
            .filter(|byte| *byte == b'@')
            .count()
            > 1
}
fn git_reference(source: &str) -> (&str, &str) {
    if let Some(parts) = source.split_once('#') {
        return parts;
    }
    // Keep the established native interpretation for ambiguous sources.
    // `#ref` disambiguates repositories and refs containing `@`.
    match source
        .rfind('@')
        .filter(|index| *index >= git_path_start(source))
    {
        Some(index) => (&source[..index], &source[index + 1..]),
        None => (source, "HEAD"),
    }
}
fn is_git_commit_prefix(reference: &str) -> bool {
    (7..=64).contains(&reference.len()) && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn cached_git_pin_matches(source: &str, cache: &Value, cleanup: Option<&Children>) -> bool {
    let Some(specification) = source.strip_prefix("git:") else {
        return false;
    };
    let (_, reference) = git_reference(specification);
    if !is_git_commit_prefix(reference) {
        return false;
    }
    let Ok(package) = serde_json::from_value::<Installed>(cache.clone()) else {
        return false;
    };
    if package.source != source || !package.root.join(".git").is_dir() {
        return false;
    }
    resolve_git_commit(&package.root, "HEAD", source, cleanup)
        .await
        .is_some_and(|commit| commit.starts_with(&reference.to_ascii_lowercase()))
}

async fn resolve_git_commit(
    root: &Path,
    reference: &str,
    source: &str,
    cleanup: Option<&Children>,
) -> Option<String> {
    let mut command = tokio::process::Command::new("git");
    command
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "--verify", "--end-of-options"])
        .arg(format!("{reference}^{{commit}}"));
    let output = run(command, source, cleanup).await.ok()?;
    let commit = std::str::from_utf8(&output).ok()?.trim();
    (matches!(commit.len(), 40 | 64) && commit.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| commit.to_ascii_lowercase())
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

async fn install(source: &str, offline: bool, cleanup: Option<&Children>) -> Result<Installed> {
    if offline && remote(source) {
        return Err(Error::Config(format!(
            "cannot install package in offline mode: {}",
            display_source(source)
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
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(Error::Io(error)),
        }
    }
    let _lock = lock;
    if !remote(source) && Path::new(source).is_file() {
        let executable = PathBuf::from(source);
        pi_rs_extensions::ExecutableExtension::load_async(&executable).await?;
        let root = executable.parent().unwrap_or(Path::new(".")).to_path_buf();
        let package = Installed {
            source: source.into(),
            root,
            version: None,
            resources: PackageResources {
                extensions: vec![executable],
                ..PackageResources::default()
            },
        };
        config::update_object(&config::agent_dir().join("packages/index.json"), |index| {
            index.insert(key(source), serde_json::to_value(&package)?);
            Ok(())
        })?;
        return Ok(package);
    }
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
        let artifacts = run(command, source, cleanup).await?;
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
        if ambiguous_git_reference(specification) {
            eprintln!(
                "Ambiguous Git package source: using the last @ as the ref separator for compatibility; use #ref to disambiguate"
            );
        }
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
        let abbreviated = is_git_commit_prefix(reference) && !matches!(reference.len(), 40 | 64);
        let mut command = tokio::process::Command::new("git");
        command.args(["clone", "--no-checkout"]);
        if !abbreviated {
            command.args(["--depth", "1"]);
        }
        command.args(["--", url]).arg(&root);
        run(command, source, cleanup).await?;

        let mut commit = None;
        if abbreviated {
            let prefix = reference.to_ascii_lowercase();
            commit = resolve_git_commit(&root, &prefix, source, cleanup)
                .await
                .filter(|commit| commit.starts_with(&prefix));
        }
        let target = if let Some(commit) = commit.as_deref() {
            commit
        } else {
            let mut fetch = tokio::process::Command::new("git");
            fetch
                .arg("-C")
                .arg(&root)
                .args(["fetch", "--depth", "1", "--", "origin", reference]);
            run(fetch, source, cleanup).await?;
            "FETCH_HEAD"
        };
        let mut checkout = tokio::process::Command::new("git");
        checkout
            .arg("-C")
            .arg(&root)
            .args(["reset", "--hard", target]);
        run(checkout, source, cleanup).await?;
    }
    if root.is_file() {
        installed_binaries.push(root.clone());
        root = root.parent().unwrap_or(Path::new(".")).into();
    }
    // Check both files before executable discovery or local/Git Cargo builds.
    // Registry `cargo install` above has already built the crate in order to
    // locate its source; it cannot be prevalidated through this path.
    let pi_bytes = read_optional_manifest(&root, "pi-package.json")?;
    let cargo_bytes = read_optional_manifest(&root, "Cargo.toml")?;
    // Validate relevant manifests before the local/Git build below.
    let (mut manifest, _) = local_startup_manifest(&root)?;
    let has_pi_manifest = pi_bytes.is_some();
    let has_cargo_manifest = cargo_bytes.is_some();
    manifest_pi(&manifest)?;
    let mut version = manifest
        .get("version")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if has_cargo_manifest {
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
        let metadata: Value = serde_json::from_slice(&run(metadata, source, cleanup).await?)?;
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
            if !has_pi_manifest
                && let Some(pi) = package
                    .get("metadata")
                    .and_then(|metadata| metadata.get("pi"))
            {
                manifest = json!({"pi":pi});
                manifest_pi(&manifest)?;
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
            let artifacts = run(build, source, cleanup).await?;
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

    let pi = manifest_pi(&manifest)?;
    let resources = collect_package_resources(&root, pi, installed_binaries)?;
    for executable in &resources.extensions {
        pi_rs_extensions::ExecutableExtension::load_async(executable).await?;
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
            .filter(|path| extension_selected(path, root, &names))
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
        if name == "themes" {
            *target = collect_package_themes(root, pi)?;
            continue;
        }
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
    let sources: Vec<PathBuf> = if implicit {
        paths.extend(legacy_prompt_candidates(root, None, false));
        Vec::new()
    } else {
        plain
            .iter()
            .flat_map(|pattern| {
                let pattern = pattern.replace('\\', "/");
                if pattern.contains(['*', '?', '[', '{']) {
                    if !pattern.trim_start_matches("./").contains('/') {
                        let mut legacy = legacy_prompt_candidates(root, None, true);
                        legacy.retain(|path| resource_matches(path, root, &pattern, false, true));
                        legacy.sort();
                        legacy
                    } else {
                        expand_prompt_glob(root, &pattern)
                    }
                } else {
                    let mut sources = vec![root.join(&pattern)];
                    if pattern.ends_with(".md") && !pattern.trim_start_matches("./").contains('/') {
                        let mut legacy = legacy_prompt_candidates(root, None, true);
                        legacy.retain(|path| resource_matches(path, root, &pattern, false, true));
                        legacy.sort();
                        sources.extend(legacy);
                    }
                    sources
                }
            })
            .collect()
    };
    for source in sources {
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
                .follow_links(true)
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
        let path = root.join(path.replace('\\', "/"));
        paths.extend(legacy_prompt_candidates(root, Some(&path), false));
    }
    if !overrides.is_empty() {
        paths.retain(|path| manifest_selected(path, root, &overrides));
    }
    let mut seen = std::collections::HashSet::new();
    paths.retain(|path| seen.insert(dunce::simplified(path).to_path_buf()));
    paths
}

// Match package-wide candidate rules without traversing directory links.
// An exact target prunes traversal to its ancestors and cannot escape the root.
fn legacy_prompt_candidates(
    root: &Path,
    target: Option<&Path>,
    include_file_links: bool,
) -> Vec<PathBuf> {
    if let Some(target) = target {
        let Ok(relative) = target.strip_prefix(root) else {
            return Vec::new();
        };
        if relative
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Vec::new();
        }
    }
    let target_for_filter = target.map(Path::to_path_buf);
    ignore::WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(false)
        .follow_links(false)
        .filter_entry(move |entry| {
            !matches!(
                entry.file_name().to_str(),
                Some(".git" | "target" | "node_modules")
            ) && target_for_filter
                .as_ref()
                .is_none_or(|target| target.starts_with(entry.path()))
        })
        .build()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_some_and(|kind| {
                kind.is_file()
                    || (include_file_links && kind.is_symlink() && entry.path().is_file())
            }) && entry.file_name().to_string_lossy().ends_with(".md")
                && target.is_none_or(|target| entry.path() == target)
        })
        .map(|entry| entry.into_path())
        .collect()
}

fn expand_prompt_glob(root: &Path, pattern: &str) -> Vec<PathBuf> {
    let Some(patterns) = expand_prompt_braces(pattern) else {
        return Vec::new();
    };
    let mut paths: Vec<_> = patterns
        .iter()
        .flat_map(|pattern| expand_prompt_glob_variant(root, pattern))
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

// Expand alternatives before splitting into path components for traversal pruning.
fn expand_prompt_braces(pattern: &str) -> Option<Vec<String>> {
    let mut start = None;
    let mut depth = 0;
    let mut in_class = false;
    let mut separators = Vec::new();
    for (index, ch) in pattern.char_indices() {
        match ch {
            '[' if !in_class => in_class = true,
            ']' if in_class => in_class = false,
            '{' if !in_class => {
                if depth == 0 {
                    start = Some(index);
                }
                depth += 1;
            }
            ',' if !in_class && depth == 1 => separators.push(index),
            '}' if !in_class && depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    let start = start?;
                    let mut variants = Vec::new();
                    let mut begin = start + 1;
                    for end in separators.into_iter().chain(std::iter::once(index)) {
                        let next = format!(
                            "{}{}{}",
                            &pattern[..start],
                            &pattern[begin..end],
                            &pattern[index + 1..]
                        );
                        variants.extend(expand_prompt_braces(&next)?);
                        if variants.len() > 256 {
                            return None;
                        }
                        begin = end + 1;
                    }
                    return Some(variants);
                }
            }
            _ => {}
        }
    }
    Some(vec![pattern.to_owned()])
}

fn expand_prompt_glob_variant(root: &Path, pattern: &str) -> Vec<PathBuf> {
    let root = dunce::simplified(root);
    let input = dunce::simplified(Path::new(pattern));
    let mut literal_prefix = if input.is_absolute() {
        PathBuf::new()
    } else {
        root.to_path_buf()
    };
    let mut pattern_path = if input.is_absolute() {
        PathBuf::new()
    } else {
        root.to_path_buf()
    };
    let mut saw_wildcard = false;
    for component in input.components() {
        if matches!(component, std::path::Component::CurDir) {
            continue;
        }
        if matches!(component, std::path::Component::ParentDir) {
            // Resolving a parent after a wildcard would widen the search unpredictably.
            if saw_wildcard || !pattern_path.pop() {
                return Vec::new();
            }
            if literal_prefix.starts_with(&pattern_path) {
                literal_prefix = pattern_path.clone();
            }
        } else {
            saw_wildcard |= component
                .as_os_str()
                .to_string_lossy()
                .contains(['*', '?', '[', '{']);
            pattern_path.push(component.as_os_str());
        }
    }
    let literal_count = literal_prefix
        .components()
        .filter(|c| matches!(c, std::path::Component::Normal(_)))
        .count();
    let suffix = pattern_path
        .strip_prefix(&literal_prefix)
        .unwrap_or(&pattern_path)
        .to_string_lossy()
        .replace('\\', "/");
    let mut normalized = globset::escape(&literal_prefix.to_string_lossy().replace('\\', "/"));
    if !normalized.is_empty() && !normalized.ends_with('/') && !suffix.is_empty() {
        normalized.push('/');
    }
    normalized.push_str(&suffix);
    let Ok(glob) = globset::GlobBuilder::new(&normalized)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
    else {
        return Vec::new();
    };
    let matcher = glob.compile_matcher();
    let mut base = PathBuf::new();
    let mut normal_count = 0;
    for component in pattern_path.components() {
        let part = component.as_os_str();
        let literal =
            matches!(component, std::path::Component::Normal(_)) && normal_count < literal_count;
        if !literal && part.to_string_lossy().contains(['*', '?', '[', '{']) {
            break;
        }
        if matches!(component, std::path::Component::Normal(_)) {
            normal_count += 1;
        }
        base.push(part);
    }
    // A wildcard directly under a filesystem root must not cause a root-wide scan.
    if base.parent().is_none() {
        if root.starts_with(&base) {
            base = root.to_path_buf();
        } else {
            return Vec::new();
        }
    }
    let Some(prefix) = PromptGlobPrefix::new(&pattern_path, literal_count) else {
        return Vec::new();
    };
    let depth = prefix.max_depth(&base);
    let mut matches: Vec<_> = ignore::WalkBuilder::new(base)
        .max_depth(depth)
        .hidden(true)
        .parents(false)
        .ignore(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .follow_links(false)
        .filter_entry(move |entry| entry.depth() == 0 || prefix.can_match(entry.path()))
        .build()
        .flatten()
        .filter(|entry| {
            let entry_components: Vec<_> = entry.path().components().collect();
            let root_components: Vec<_> = root.components().collect();
            let common = entry_components.iter().zip(&root_components).take_while(|(a, b)| a == b).count();
            entry_components[common..].iter().all(|c| {
                !matches!(c, std::path::Component::Normal(name) if name.to_string_lossy().starts_with('.'))
            }) && matcher.is_match(entry.path())
        })
        .map(|entry| entry.into_path())
        .collect();
    matches.sort();
    matches
}

// A component NFA lets the walker discard subtrees that cannot match, including
// patterns with **, without traversing every descendant of the fixed prefix.
struct PromptGlobPrefix {
    components: Vec<Option<globset::GlobMatcher>>,
}

impl PromptGlobPrefix {
    fn new(pattern: &Path, literal_count: usize) -> Option<Self> {
        let mut components = Vec::new();
        for component in pattern.components() {
            if let std::path::Component::Normal(name) = component {
                let name = name.to_string_lossy();
                if components.len() >= literal_count && name == "**" {
                    components.push(None);
                } else {
                    let escaped;
                    let name = if components.len() < literal_count {
                        escaped = globset::escape(&name);
                        escaped.as_str()
                    } else {
                        name.as_ref()
                    };
                    let glob = globset::GlobBuilder::new(name)
                        .literal_separator(true)
                        .backslash_escape(false)
                        .build()
                        .ok()?;
                    components.push(Some(glob.compile_matcher()));
                }
            }
        }
        Some(Self { components })
    }

    fn max_depth(&self, base: &Path) -> Option<usize> {
        if self.components.iter().any(Option::is_none) {
            None
        } else {
            let count = base
                .components()
                .filter(|c| matches!(c, std::path::Component::Normal(_)))
                .count();
            Some(self.components.len().saturating_sub(count))
        }
    }

    fn can_match(&self, path: &Path) -> bool {
        let mut states = vec![false; self.components.len() + 1];
        states[0] = true;
        for component in path.components().filter_map(|c| match c {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        }) {
            self.expand_globstars(&mut states);
            let mut next = vec![false; states.len()];
            for (index, matcher) in self.components.iter().enumerate() {
                if states[index] {
                    match matcher {
                        None => next[index] = true,
                        Some(matcher) if matcher.is_match(component) => next[index + 1] = true,
                        _ => {}
                    }
                }
            }
            states = next;
        }
        self.expand_globstars(&mut states);
        states.iter().any(|state| *state)
    }

    fn expand_globstars(&self, states: &mut [bool]) {
        for (index, matcher) in self.components.iter().enumerate() {
            if states[index] && matcher.is_none() {
                states[index + 1] = true;
            }
        }
    }
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
fn package_command_failed(source: &str, status: &str, stderr: &[u8], git: bool) -> Error {
    let stderr = String::from_utf8_lossy(stderr);
    let hint = if git && git_needs_credentials(&stderr) {
        "\nGit cannot ask for a password or passphrase while pi runs. Set up a Git credential helper or an SSH agent."
    } else {
        ""
    };
    // Subprocesses can echo the whole URL or just part of its userinfo.
    // Keep status but never expose their stderr for such sources.
    if display_source(source) != source {
        Error::Config(format!(
            "package command failed ({status}); subprocess output redacted{hint}"
        ))
    } else {
        Error::Config(format!("package command failed ({status}): {stderr}{hint}"))
    }
}

fn git_needs_credentials(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    [
        "terminal prompts disabled",
        "could not read username",
        "could not read password",
        "permission denied (publickey",
        "host key verification failed",
        "passphrase",
    ]
    .iter()
    .any(|message| stderr.contains(message))
}

// Tokio's kill-on-drop only terminates the direct child on Unix. Keep the
// process-wrap child so cancellation also signals its still-live group.
struct PackageChildGuard<'a> {
    child: Option<Box<dyn process_wrap::tokio::ChildWrapper>>,
    cleanup: Option<&'a Children>,
}

impl Drop for PackageChildGuard<'_> {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take()
            && owns_kill_target(child.as_ref())
        {
            if let Some(cleanup) = self.cleanup {
                cleanup.adopt(child);
            } else {
                let _ = child.start_kill();
            }
        }
    }
}

async fn run(
    mut command: tokio::process::Command,
    source: &str,
    cleanup: Option<&Children>,
) -> Result<Vec<u8>> {
    // Package commands are captured in both the terminal UI and native CLI.
    // Git and ssh therefore have no prompt the caller can answer.
    let git = command.as_std().get_program() == "git";
    if git {
        command.env("GIT_TERMINAL_PROMPT", "0");
        if std::env::var_os("GIT_SSH_COMMAND").is_none() {
            command.env("GIT_SSH_COMMAND", "ssh -o BatchMode=yes");
        }
    }
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
    let mut process = PackageChildGuard {
        child: Some(wrapped.spawn()?),
        cleanup,
    };
    let child = process.child.as_mut().expect("owned child");
    let stdout = child.stdout().take().expect("stdout");
    let stderr = child.stderr().take().expect("stderr");
    let (status, (stdout, overflow), (stderr, _)) =
        tokio::time::timeout(Duration::from_secs(1200), async {
            // Keep the group leader unreaped until all inherited output pipes
            // close. Its reserved PID prevents the process group ID being
            // reused while a descendant still holds either pipe.
            let (stdout, stderr) =
                tokio::try_join!(capture(stdout, 8 * 1024 * 1024), capture(stderr, 64 * 1024))?;
            let status = child.wait().await.map_err(Error::from)?;
            Ok::<_, Error>((status, stdout, stderr))
        })
        .await
        .map_err(|_| Error::Config("package command timed out".into()))??;
    process.child.take();
    if !status.success() {
        return Err(package_command_failed(
            source,
            &status.to_string(),
            &stderr,
            git,
        ));
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
    fn worker_package_discovery_keeps_recovered_scope_after_disk_repair() {
        use clap::Parser;
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        std::fs::create_dir_all(project.join(".pi")).unwrap();
        std::fs::write(
            project.join(".pi/settings.json"),
            json!({"packages":["git:https://example.com/new.git"]}).to_string(),
        )
        .unwrap();
        let mut args = Cli::parse_from(["pi"]);
        args.workspace = project.clone();
        args.trusted = true;
        for (worker, diagnostics) in [
            (true, vec!["previous scope could not be read"]),
            (false, vec![]),
        ] {
            args.worker_settings_stdin = worker;
            args.settings = serde_json::from_value(json!({
            "value":{},
            "resource_layers":[[config::agent_dir(),{}],[project.join(".pi"),{"packages":["git:https://example.com/old.git"]}]],
            "diagnostics":diagnostics,
            "live_auto_retry":null,
            "live_auto_compaction":null
        })).unwrap();
            let entries = declarations(&args).unwrap();
            assert_eq!(entries.len(), 1);
            assert!(
                entries[0].source.contains("old.git"),
                "{}",
                entries[0].source
            );
            let listing = configured_packages(&args).unwrap();
            assert_eq!(listing.len(), 1);
            assert!(
                listing[0].0.source.contains("old.git"),
                "{}",
                listing[0].0.source
            );
            let layered = resource_declarations(&args, false).unwrap();
            assert_eq!(layered.len(), 1);
            assert!(
                layered[0].source.contains("old.git"),
                "{}",
                layered[0].source
            );
            let sources = configured_update_sources(&args).unwrap();
            assert_eq!(sources.len(), 1);
            assert!(sources[0].contains("old.git"), "{sources:?}");
        }
    }

    // A private test subprocess: the child owns a connection until it exits,
    // letting the test observe that exact process without relying on PID reuse.
    #[test]
    fn local_package_filter_error_stops_later_resource_types() {
        const CHILD: &str = "PI_RS_LOCAL_FILTER_TEST_DIR";
        let Some(directory) = std::env::var_os(CHILD) else {
            let temporary = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "packages::tests::local_package_filter_error_stops_later_resource_types",
                    "--nocapture",
                ])
                .env(CHILD, temporary.path())
                .env("PI_RS_HOME", temporary.path().join("agent"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        };
        use clap::Parser;
        let directory = PathBuf::from(directory);
        let agent = config::agent_dir();
        std::fs::create_dir_all(agent.join("packages")).unwrap();
        let mut index = serde_json::Map::new();
        let mut sources = Vec::new();
        for name in ["first", "second"] {
            let root = directory.join(name);
            for subdir in ["skills/demo", "prompts", "themes"] {
                std::fs::create_dir_all(root.join(subdir)).unwrap();
            }
            std::fs::write(root.join("skills/demo/SKILL.md"), "skill").unwrap();
            std::fs::write(root.join("prompts/demo.md"), "prompt").unwrap();
            std::fs::write(root.join("themes/demo.json"), "{}").unwrap();
            let source = normalize(&root.to_string_lossy(), &directory).unwrap();
            let installed = Installed {
                source: source.clone(),
                root: PathBuf::from(&source),
                version: None,
                resources: collect_package_resources(Path::new(&source), &json!({}), Vec::new())
                    .unwrap(),
            };
            index.insert(key(&source), serde_json::to_value(installed).unwrap());
            sources.push(source);
        }
        let settings = json!({"packages": [
            {"source": sources[0], "prompts": ["file:///bad%2Fpath"]},
            {"source": sources[1]}
        ]});
        std::fs::write(agent.join("settings.json"), settings.to_string()).unwrap();
        let mut args = Cli::parse_from(["pi", "--no-trust"]);
        args.workspace = directory.clone();
        // Before package preparation, theme discovery still honors earlier failures.
        let themes = startup_themes(&args, false).unwrap();
        assert_eq!(themes.len(), 1);
        assert!(themes[0].starts_with(&sources[1]));
        std::fs::write(directory.join("first/package.json"), "{}").unwrap();
        assert_eq!(startup_themes(&args, false).unwrap().len(), 1);
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(index.clone()).to_string(),
        )
        .unwrap();
        let resources = resources_for(&args, false).unwrap();
        assert_eq!(resources.skills.len(), 2);
        assert_eq!(resources.prompts.len(), 1);
        assert_eq!(resources.themes.len(), 1);
        assert!(resources.themes[0].starts_with(&sources[1]));
        assert_eq!(startup_themes(&args, false).unwrap(), themes);
        // Pi-only prompts affect diagnostics without becoming native resources.
        std::fs::write(
            directory.join("first/package.json"),
            json!({"pi":{
                "prompts":["./prompts/*.md"], "themes":["themes"]
            }})
            .to_string(),
        )
        .unwrap();
        let resources = resources_for(&args, false).unwrap();
        assert_eq!(resources.prompts.len(), 1);
        assert_eq!(resources.themes.len(), 1);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 1);
        std::fs::remove_file(agent.join("packages/index.json")).unwrap();
        assert_eq!(startup_themes(&args, false).unwrap().len(), 1);
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(index.clone()).to_string(),
        )
        .unwrap();
        // Empty Pi candidates do not evaluate an unrelated invalid settings glob.
        std::fs::remove_file(directory.join("first/prompts/demo.md")).unwrap();
        std::fs::write(
            directory.join("first/package.json"),
            json!({"pi":{"prompts":[],"themes":["themes"]}}).to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[
                {"source":sources[0],"prompts":["["]}, {"source":sources[1]}
            ]})
            .to_string(),
        )
        .unwrap();
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
        assert_eq!(resources_for(&args, false).unwrap().themes.len(), 2);
        let empty_remote = "git:https://example.com/empty/pkg#rev";
        let mut empty_index = index.clone();
        let mut empty_package: Installed =
            serde_json::from_value(empty_index[&key(&sources[0])].clone()).unwrap();
        empty_package.source = empty_remote.into();
        empty_index.insert(
            key(empty_remote),
            serde_json::to_value(empty_package).unwrap(),
        );
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(empty_index).to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[{"source":empty_remote,"prompts":["["]}]}).to_string(),
        )
        .unwrap();
        assert_eq!(resources_for(&args, false).unwrap().themes.len(), 1);
        std::fs::write(directory.join("first/prompts/demo.md"), "prompt").unwrap();
        // Manifest overrides are evaluated before settings filters. Remote
        // errors propagate, while local package errors stop only that package.
        std::fs::write(
            directory.join("first/package.json"),
            json!({"pi":{
                "prompts":["prompts/demo.md","!**","+file:///bad%2Fpath"], "themes":["themes"]
            }})
            .to_string(),
        )
        .unwrap();
        let remote = "git:https://example.com/diagnostic/pkg#rev";
        let mut remote_index = index.clone();
        let mut remote_package: Installed =
            serde_json::from_value(remote_index[&key(&sources[0])].clone()).unwrap();
        remote_package.source = remote.into();
        remote_index.insert(key(remote), serde_json::to_value(remote_package).unwrap());
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(remote_index).to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[{"source":remote,"prompts":["["]}]}).to_string(),
        )
        .unwrap();
        assert!(
            resources_for(&args, false)
                .err()
                .unwrap()
                .to_string()
                .contains("encoded")
        );
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[{"source":sources[0],"prompts":["["]}, {"source":sources[1]}]})
                .to_string(),
        )
        .unwrap();
        assert_eq!(resources_for(&args, false).unwrap().themes.len(), 1);
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(index.clone()).to_string(),
        )
        .unwrap();
        std::fs::remove_file(directory.join("first/package.json")).unwrap();
        // An error while evaluating themes preserves earlier prompt resources.
        let settings = json!({"packages": [
            {"source": sources[0], "themes": ["file:///bad%2Fpath"]},
            {"source": sources[1]}
        ]});
        std::fs::write(agent.join("settings.json"), settings.to_string()).unwrap();
        let resources = resources_for(&args, false).unwrap();
        assert_eq!(resources.prompts.len(), 2);
        assert_eq!(resources.themes.len(), 1);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 1);
        // Startup must not validate unrelated filters when no file URL is involved.
        let settings = json!({"packages": [
            {"source": sources[0], "prompts": ["["]}, {"source": sources[1]}
        ]});
        std::fs::write(agent.join("settings.json"), settings.to_string()).unwrap();
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
        // Project deltas run before separate user declarations and reserve
        // explicitly disabled paths while leaving others to user defaults.
        std::fs::create_dir_all(directory.join(".pi")).unwrap();
        std::fs::write(
            directory.join(".pi/settings.json"),
            json!({"packages":[
                {"source":sources[0],"autoload":false,"prompts":["-prompts/demo.md"]}
            ]})
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[
                {"source":sources[0]}, {"source":sources[1]}
            ]})
            .to_string(),
        )
        .unwrap();
        args.trusted = true;
        assert_eq!(resources_for(&args, false).unwrap().prompts.len(), 1);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
        // An error stops only the project declaration. The user declaration
        // still contributes prompts and themes, after prior project skills.
        std::fs::write(directory.join(".pi/settings.json"), json!({"packages":[
            {"source":sources[0],"autoload":false,"skills":["+skills/demo"],"prompts":["file:///bad%2Fpath"]}
        ]}).to_string()).unwrap();
        let resources = resources_for(&args, false).unwrap();
        assert_eq!(resources.skills.len(), 2);
        assert_eq!(resources.prompts.len(), 2);
        assert_eq!(resources.themes.len(), 2);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
        // Both user declarations survive a project delta. The first user
        // decision, including an explicit disable, takes precedence.
        std::fs::write(
            directory.join(".pi/settings.json"),
            json!({"packages":[
                {"source":sources[0],"autoload":false}
            ]})
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[
                {"source":sources[0],"prompts":["!prompts/*.md"]},
                {"source":sources[0],"prompts":["+prompts/demo.md"]},
                {"source":sources[1]}
            ]})
            .to_string(),
        )
        .unwrap();
        assert_eq!(resources_for(&args, false).unwrap().prompts.len(), 1);
        // A project delta resolves against the first user source even when
        // its own revision differs.
        std::fs::write(
            directory.join(".pi/settings.json"),
            json!({"packages":[
                {"source":"git:https://example.com/org/repo#project","autoload":false}
            ]})
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[
                {"source":"git:https://example.com/org/repo#user"},
                {"source":"git:https://example.com/org/repo#other"}
            ]})
            .to_string(),
        )
        .unwrap();
        let plan = resource_declarations(&args, false).unwrap();
        assert_eq!(plan.len(), 3);
        assert!(plan[0].source.ends_with("#user"));
        assert!(plan[1].source.ends_with("#user"));
        assert!(plan[2].source.ends_with("#other"));
        // Different cached checkouts must not bypass the first checkout's
        // project exclusion when Pi resolves them to one managed Git path.
        let saved_index = std::fs::read(agent.join("packages/index.json")).unwrap();
        let mut revision_index: serde_json::Map<String, Value> =
            serde_json::from_slice(&saved_index).unwrap();
        for (source, root) in [
            ("git:https://example.com/org/repo#user", &sources[0]),
            ("git:https://example.com/org/repo#other", &sources[1]),
        ] {
            let resources =
                collect_package_resources(Path::new(root), &json!({}), Vec::new()).unwrap();
            revision_index.insert(
                key(source),
                serde_json::to_value(Installed {
                    source: source.into(),
                    root: PathBuf::from(root),
                    version: None,
                    resources,
                })
                .unwrap(),
            );
        }
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(revision_index).to_string(),
        )
        .unwrap();
        std::fs::write(directory.join(".pi/settings.json"), json!({"packages":[
            {"source":"git:https://example.com/org/repo#project","autoload":false,"prompts":["-prompts/demo.md"]}
        ]}).to_string()).unwrap();
        assert!(resources_for(&args, false).unwrap().prompts.is_empty());
        // If the first source is unavailable, the later source still works.
        let mut later_only: serde_json::Map<String, Value> =
            serde_json::from_slice(&std::fs::read(agent.join("packages/index.json")).unwrap())
                .unwrap();
        later_only.remove(&key("git:https://example.com/org/repo#user"));
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(later_only).to_string(),
        )
        .unwrap();
        assert!(resources_for(&args, false).unwrap().prompts.is_empty());
        args.offline = true;
        assert!(resources_for(&args, false).unwrap().prompts.is_empty());
        args.offline = false;
        let mut broken: serde_json::Map<String, Value> =
            serde_json::from_slice(&std::fs::read(agent.join("packages/index.json")).unwrap())
                .unwrap();
        let unavailable = Installed {
            source: "git:https://example.com/org/repo#user".into(),
            root: directory.join("deleted-checkout"),
            version: None,
            resources: PackageResources::default(),
        };
        broken.insert(
            key(&unavailable.source),
            serde_json::to_value(unavailable).unwrap(),
        );
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(broken).to_string(),
        )
        .unwrap();
        assert!(resources_for(&args, false).unwrap().prompts.is_empty());
        assert_eq!(startup_themes(&args, false).unwrap().len(), 1);
        std::fs::write(agent.join("packages/index.json"), saved_index).unwrap();
        // Temporary sources replace configured declarations with the same
        // identity, including when their revisions differ.
        args.extensions
            .push("git:https://example.com/org/repo#cli".into());
        assert!(resource_declarations(&args, false).unwrap().is_empty());
        let transient = resource_declarations(&args, true).unwrap();
        assert_eq!(transient.len(), 1);
        assert!(transient[0].source.ends_with("#cli"));
        args.extensions.clear();
        args.trusted = false;
        args.extensions.push(sources[0].clone());
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[{"source":sources[0]}]}).to_string(),
        )
        .unwrap();
        assert!(resource_declarations(&args, false).unwrap().is_empty());
        assert_eq!(resource_declarations(&args, true).unwrap().len(), 1);
        args.extensions.clear();
        args.trusted = false;
        // A prior matching pattern short-circuits the invalid URL.
        let settings = json!({"packages": [
            {"source": sources[0], "prompts": ["**", "file:///bad%2Fpath"]},
            {"source": sources[1]}
        ]});
        std::fs::write(agent.join("settings.json"), settings.to_string()).unwrap();
        assert_eq!(resources_for(&args, false).unwrap().themes.len(), 2);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
        let settings = json!({"packages": [
            {"source": sources[0], "prompts": ["file:///bad%2Fpath"]},
            {"source": sources[1]}
        ]});
        std::fs::write(agent.join("settings.json"), settings.to_string()).unwrap();
        // Both paths use cached candidates, even when a file has been deleted.
        std::fs::remove_file(directory.join("first/prompts/demo.md")).unwrap();
        assert_eq!(resources_for(&args, false).unwrap().themes.len(), 1);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 1);
        // A file added after installation is not a candidate until preparation.
        index.get_mut(&key(&sources[0])).unwrap()["resources"]["prompts"] = json!([]);
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(index).to_string(),
        )
        .unwrap();
        std::fs::write(directory.join("first/prompts/demo.md"), "prompt").unwrap();
        assert_eq!(resources_for(&args, false).unwrap().themes.len(), 2);
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
        // A file source ignores its parent's manifest, resources and filters.
        let file_root = directory.join("file-source");
        std::fs::create_dir_all(file_root.join("themes")).unwrap();
        std::fs::write(file_root.join("themes/demo.json"), "{}").unwrap();
        std::fs::write(
            file_root.join("package.json"),
            json!({"pi":{"themes":["themes"]}}).to_string(),
        )
        .unwrap();
        let file = file_root.join("extension");
        std::fs::write(&file, "fixture").unwrap();
        let source = normalize(&file.to_string_lossy(), &directory).unwrap();
        let root = normalize(&file_root.to_string_lossy(), &directory).unwrap();
        let installed = Installed {
            source: source.clone(),
            root: PathBuf::from(&root),
            version: None,
            resources: PackageResources {
                themes: vec![PathBuf::from(&root).join("themes/demo.json")],
                ..PackageResources::default()
            },
        };
        let mut file_index = serde_json::Map::new();
        file_index.insert(key(&source), serde_json::to_value(installed).unwrap());
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(file_index).to_string(),
        )
        .unwrap();
        std::fs::write(
            agent.join("settings.json"),
            json!({"packages":[{"source":source,"autoload":false,"extensions":["file:///bad%2Fpath"],"themes":["-file:///bad%2Fpath"]}]}).to_string(),
        )
        .unwrap();
        // Old caches may contain only the parent's theme and never validated
        // the selected file; they cannot activate it until re-prepared.
        assert!(resources_for(&args, false).unwrap().extensions.is_empty());
        assert!(startup_themes(&args, false).unwrap().is_empty());
        let validated = Installed {
            source: source.clone(),
            root: PathBuf::from(&root),
            version: None,
            resources: PackageResources {
                extensions: vec![PathBuf::from(&source)],
                ..PackageResources::default()
            },
        };
        let mut file_index = serde_json::Map::new();
        file_index.insert(key(&source), serde_json::to_value(validated).unwrap());
        std::fs::write(
            agent.join("packages/index.json"),
            Value::Object(file_index).to_string(),
        )
        .unwrap();
        let file_resources = resources_for(&args, false).unwrap();
        assert_eq!(file_resources.extensions, vec![PathBuf::from(&source)]);
        assert!(file_resources.themes.is_empty());
        assert!(startup_themes(&args, false).unwrap().is_empty());
        // Replacing the file with a directory does not load the old parent.
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        assert!(resources_for(&args, false).unwrap().extensions.is_empty());
        std::fs::write(agent.join("settings.json"), settings.to_string()).unwrap();
        // Without a cache and without candidates the URL is never evaluated.
        std::fs::remove_file(agent.join("packages/index.json")).unwrap();
        std::fs::remove_file(directory.join("first/prompts/demo.md")).unwrap();
        assert_eq!(startup_themes(&args, false).unwrap().len(), 2);
    }

    #[test]
    fn selection_deduplicates_external_binaries_by_package_identity() {
        let mut entries = ResourceSelections::default();
        let first = Path::new("/cache/one/bin/tool");
        let second = Path::new("/cache/two/bin/tool");
        entries.insert(first, false, "cargo:example", Path::new("/src/one"), true);
        entries.insert(second, true, "cargo:example", Path::new("/src/two"), true);
        assert!(entries.enabled().is_empty());
    }

    #[test]
    fn local_package_cache_requires_matching_source_kind_and_validated_file() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("source");
        std::fs::write(&file, "fixture").unwrap();
        let source = normalize(&file.to_string_lossy(), directory.path()).unwrap();
        let mut package = Installed {
            source: source.clone(),
            root: Path::new(&source).parent().unwrap().to_path_buf(),
            version: None,
            resources: PackageResources::default(),
        };
        assert!(!local_source_matches_cache(&package));
        package.resources.extensions.push(PathBuf::from(&source));
        assert!(local_source_matches_cache(&package));
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        assert!(!local_source_matches_cache(&package));
        package.root = PathBuf::from(&source);
        assert!(local_source_matches_cache(&package));
        std::fs::remove_dir(&file).unwrap();
        std::fs::write(&file, "fixture").unwrap();
        assert!(!local_source_matches_cache(&package));
    }

    #[cfg(unix)]
    #[test]
    fn single_file_install_ignores_parent_manifests_and_resources() {
        use std::os::unix::fs::PermissionsExt;
        const CHILD: &str = "PI_RS_SINGLE_FILE_TEST_DIR";
        let Some(directory) = std::env::var_os(CHILD) else {
            let temporary = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "packages::tests::single_file_install_ignores_parent_manifests_and_resources",
                    "--nocapture",
                ])
                .env(CHILD, temporary.path())
                .env("PI_RS_HOME", temporary.path().join("agent"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        };
        let directory = PathBuf::from(directory);
        let root = directory.join("package");
        std::fs::create_dir_all(root.join("themes")).unwrap();
        std::fs::write(root.join("themes/sibling.json"), "{}").unwrap();
        std::fs::write(root.join("pi-package.json"), "not json").unwrap();
        std::fs::write(root.join("Cargo.toml"), "not toml").unwrap();
        let file = root.join("extension");
        std::fs::write(&file, r#"#!/bin/sh
cat >/dev/null
touch "$PI_RS_HOME/validated"
printf '%s\n' '{"version":1,"response":{"Ok":{"id":"single","dependencies":[],"commands":[],"tools":[]}}}'
"#).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        let source = normalize(&file.to_string_lossy(), &directory).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let package = runtime.block_on(install(&source, false, None)).unwrap();
        assert_eq!(package.resources.extensions, vec![PathBuf::from(&source)]);
        assert!(package.resources.themes.is_empty());
        assert!(package.resources.prompts.is_empty());
        assert!(package.resources.skills.is_empty());
        let mut old_cache = package.clone();
        old_cache.resources.extensions.clear();
        old_cache
            .resources
            .themes
            .push(root.join("themes/sibling.json"));
        let restored = revalidate_cached_package(package).unwrap();
        assert_eq!(restored.resources.extensions, vec![PathBuf::from(&source)]);
        assert!(restored.resources.themes.is_empty());

        use clap::Parser;
        std::fs::write(
            config::agent_dir().join("settings.json"),
            json!({"packages":[source]}).to_string(),
        )
        .unwrap();
        let mut args = Cli::parse_from(["pi", "--no-trust"]);
        args.workspace = directory.clone();
        std::fs::remove_file(config::agent_dir().join("validated")).unwrap();
        config::update_object(&config::agent_dir().join("packages/index.json"), |index| {
            index.insert(key(&source), serde_json::to_value(old_cache).unwrap());
            Ok(())
        })
        .unwrap();
        runtime.block_on(prepare(&args)).unwrap();
        assert!(config::agent_dir().join("validated").exists());
        assert_eq!(
            resources_for(&args, false).unwrap().extensions,
            vec![PathBuf::from(&source)]
        );
        std::fs::remove_file(config::agent_dir().join("validated")).unwrap();
        // A file replaced by a directory must be prepared as a new package.
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir_all(file.join("themes")).unwrap();
        std::fs::write(file.join("themes/new.json"), "{}").unwrap();
        runtime.block_on(prepare(&args)).unwrap();
        let resources = resources_for(&args, false).unwrap();
        assert!(resources.extensions.is_empty());
        assert_eq!(resources.themes.len(), 1);
        // A directory replaced by an invalid executable cannot reuse its cache.
        std::fs::remove_dir_all(&file).unwrap();
        std::fs::write(&file, "#!/bin/sh\nexit 1\n").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        runtime.block_on(prepare(&args)).unwrap();
        assert!(resources_for(&args, false).unwrap().extensions.is_empty());
        // The next valid executable is revalidated before becoming available.
        std::fs::write(&file, r#"#!/bin/sh
cat >/dev/null
touch "$PI_RS_HOME/validated"
printf '%s\n' '{"version":1,"response":{"Ok":{"id":"single","dependencies":[],"commands":[],"tools":[]}}}'
"#).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        runtime.block_on(prepare(&args)).unwrap();
        assert!(config::agent_dir().join("validated").exists());
        let resources = resources_for(&args, false).unwrap();
        assert_eq!(resources.extensions, vec![PathBuf::from(&source)]);
        assert!(resources.themes.is_empty());
    }

    #[test]
    fn package_process_fixture() {
        use std::io::{Read, Write};
        let Ok(role) = std::env::var("PI_PACKAGE_TEST_ROLE") else {
            return;
        };
        let address = std::env::var("PI_PACKAGE_TEST_ADDRESS").unwrap();
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(90)))
            .unwrap();
        writeln!(stream, "{role} {}", std::process::id()).unwrap();
        if role == "parent" {
            let mut descendant = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "packages::tests::package_process_fixture",
                    "--nocapture",
                ])
                .env("PI_PACKAGE_TEST_ROLE", "child")
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            // Reap the descendant if it finishes while this parent is alive.
            // The parent-exits-first case deliberately ends this waiter early.
            std::thread::spawn(move || {
                let _ = descendant.wait();
            });
        }
        let mut byte = [0];
        // The test releases the parent, or closes the connection on cleanup.
        let _ = stream.read(&mut byte);
    }

    #[cfg(any(unix, windows))]
    async fn check_package_process_tree(parent_exits: bool) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "packages::tests::package_process_fixture",
                "--nocapture",
            ])
            .env("PI_PACKAGE_TEST_ROLE", "parent")
            .env(
                "PI_PACKAGE_TEST_ADDRESS",
                listener.local_addr().unwrap().to_string(),
            );
        let task = tokio::spawn(run(command, "test", None));
        let _cleanup = AbortOnDrop(task.abort_handle());
        let (mut parent, parent_pid, mut child, child_pid) =
            tokio::time::timeout(Duration::from_secs(45), async {
                let mut parent = None;
                let mut child = None;
                for _ in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    let (role, pid) = line.split_once(' ').unwrap();
                    let pid: u32 = pid.trim().parse().unwrap();
                    match role {
                        "parent" => parent = Some((reader.into_inner(), pid)),
                        "child" => child = Some((reader.into_inner(), pid)),
                        _ => panic!("unexpected fixture role"),
                    }
                }
                let (child, pid) = child.unwrap();
                let (parent, parent_pid) = parent.unwrap();
                (parent, parent_pid, child, pid)
            })
            .await
            .expect("package process tree did not become ready");

        #[cfg(windows)]
        let (child_handle, parent_handle) = {
            use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_TIMEOUT};
            use windows_sys::Win32::System::Threading::{
                OpenProcess, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, TerminateProcess,
                WaitForSingleObject,
            };
            struct Handle(HANDLE);
            impl Drop for Handle {
                fn drop(&mut self) {
                    // SAFETY: this handle refers to the original child, even if its PID is reused.
                    unsafe {
                        if WaitForSingleObject(self.0, 0) == WAIT_TIMEOUT {
                            TerminateProcess(self.0, 1);
                        }
                        CloseHandle(self.0);
                    }
                }
            }
            // SAFETY: open the PID reported through the child's own connection.
            let handle =
                unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, child_pid) };
            assert!(
                !handle.is_null(),
                "cannot open child: {}",
                std::io::Error::last_os_error()
            );
            let handle = Handle(handle);
            // SAFETY: handle remains owned until the end of this test.
            assert_eq!(unsafe { WaitForSingleObject(handle.0, 0) }, WAIT_TIMEOUT);
            // SAFETY: parent reported its own PID while blocked on its connection.
            let parent =
                unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, parent_pid) };
            assert!(
                !parent.is_null(),
                "cannot open parent: {}",
                std::io::Error::last_os_error()
            );
            let parent = Handle(parent);
            assert_eq!(unsafe { WaitForSingleObject(parent.0, 0) }, WAIT_TIMEOUT);
            (handle, parent)
        };
        #[cfg(unix)]
        {
            // SAFETY: signal zero only checks that the reporting process exists.
            assert_eq!(unsafe { libc::kill(child_pid as i32, 0) }, 0);
        }
        if parent_exits {
            parent.write_all(&[1]).await.unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(15), parent.read(&mut byte))
                    .await
                    .expect("parent did not exit")
                    .unwrap(),
                0
            );
            #[cfg(unix)]
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    // WNOWAIT observes the original direct child without releasing its PID.
                    // SAFETY: zeroed siginfo is valid for waitid and the pointer is writable.
                    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
                    let result = unsafe {
                        libc::waitid(
                            libc::P_PID,
                            parent_pid as libc::id_t,
                            &mut info,
                            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                        )
                    };
                    assert_eq!(
                        result,
                        0,
                        "parent was reaped while its descendant held a pipe: {}",
                        std::io::Error::last_os_error()
                    );
                    // SAFETY: waitid initialized siginfo; zero means the child has not exited yet.
                    if unsafe { info.si_pid() } == parent_pid as libc::pid_t {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("parent process did not exit");
            #[cfg(windows)]
            {
                use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
                use windows_sys::Win32::System::Threading::WaitForSingleObject;
                // SAFETY: retained handle identifies the original parent process.
                assert_eq!(
                    unsafe { WaitForSingleObject(parent_handle.0, 15000) },
                    WAIT_OBJECT_0,
                    "original parent did not exit"
                );
            }
            // Give an erroneously concurrent wait a chance to reap before cancellation.
            tokio::time::sleep(Duration::from_millis(250)).await;
            #[cfg(unix)]
            {
                // SAFETY: observe the still-owned child without reaping it.
                let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
                let result = unsafe {
                    libc::waitid(
                        libc::P_PID,
                        parent_pid as libc::id_t,
                        &mut info,
                        libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                    )
                };
                assert_eq!(result, 0, "parent ownership was lost before cancellation");
                // SAFETY: successful waitid initialized siginfo.
                assert_eq!(unsafe { info.si_pid() }, parent_pid as libc::pid_t);
            }
        }
        task.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        let mut byte = [0];
        let connection = tokio::time::timeout(Duration::from_secs(15), child.read(&mut byte))
            .await
            .expect("child connection remained open after cancellation");
        match connection {
            Ok(0) => {}
            // Windows can reset a socket when TerminateJobObject kills its owner.
            // The original process handle below must still signal termination.
            #[cfg(windows)]
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("unexpected child connection result: {other:?}"),
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
            use windows_sys::Win32::System::Threading::WaitForSingleObject;
            // SAFETY: the original child handle remains valid and is not PID-based.
            assert_eq!(
                unsafe { WaitForSingleObject(child_handle.0, 5000) },
                WAIT_OBJECT_0,
                "the original child process survived cancellation"
            );
        }
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn cancelling_package_command_terminates_its_running_process_group() {
        check_package_process_tree(false).await;
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn cancelling_package_command_after_parent_exit_terminates_pipe_holder() {
        check_package_process_tree(true).await;
    }
    #[test]
    fn resource_filters_expand_home_and_file_urls() {
        let root = std::env::temp_dir().join("pi filter root");
        let path = root.join("prompts").join("sample prompt.md");
        let url = reqwest::Url::from_file_path(&path).unwrap().to_string();
        assert!(resource_matches(&path, &root, &url, true, false));
        assert!(!selected(&path, &root, &[format!("-{url}")]));
        let filter =
            ResourceFilter::new(&json!({"prompts": [format!("-{url}")]}), "prompts").unwrap();
        assert!(!filter.includes(&path, &root, true).unwrap());
        let filter = ResourceFilter::new(
            &json!({"autoload": false, "prompts": [format!("+{url}")]}),
            "prompts",
        )
        .unwrap();
        assert!(filter.includes(&path, &root, false).unwrap());

        if let Some(home) = filter_home_dir() {
            let path = home.join("pi-filter-test").join("prompt.md");
            assert!(resource_matches(
                &path,
                &root,
                "~/pi-filter-test/prompt.md",
                true,
                false
            ));
            assert!(resource_matches(
                &path,
                &root,
                "~/pi-filter-test/*.md",
                false,
                false
            ));
            assert!(!selected(
                &path,
                &root,
                &["-~/pi-filter-test/prompt.md".into()]
            ));
        }
    }

    #[test]
    fn resource_filter_home_paths_normalize_components() {
        let home = filter_home_dir().expect("account home");
        let path = normalize_filter_home(&home, "prompts/review.md");
        let root = path.parent().unwrap();
        for pattern in [
            "~/prompts/../prompts/./review.md",
            "~/prompts/../prompts/*.md",
        ] {
            assert!(
                resource_matches(&path, root, pattern, false, false),
                "{pattern}"
            );
        }
        assert!(!selected(
            &path,
            root,
            &["-~/prompts/../prompts/./review.md".into()]
        ));
        assert!(selected(
            &path,
            root,
            &["!*.md".into(), "+~/prompts/../prompts/review.md".into()]
        ));
        assert_eq!(
            normalize_filter_home(Path::new(""), "a/../b"),
            PathBuf::from("b")
        );
    }

    #[test]
    fn exact_resource_overrides_expand_after_one_relative_prefix() {
        let home = filter_home_dir().expect("account home");
        let home_path = normalize_filter_home(&home, "pi-filter-test/prompt.md");
        let root = std::env::temp_dir();
        let url_path = root.join("pi-filter-test").join("prompt space.md");
        let url = reqwest::Url::from_file_path(&url_path).unwrap().to_string();

        for (path, pattern) in [
            (&home_path, "~/pi-filter-test/prompt.md".to_owned()),
            (&url_path, url),
        ] {
            for prefix in ["./", ".\\"] {
                let target = format!("{prefix}{pattern}");
                assert!(!selected(path, &root, &[format!("-{target}")]));
                assert!(selected(
                    path,
                    &root,
                    &["!*.md".into(), format!("+{target}")]
                ));
                let filter = ResourceFilter::new(
                    &json!({"autoload": false, "prompts": [format!("+{target}")]}),
                    "prompts",
                )
                .unwrap();
                assert!(filter.includes(path, &root, false).unwrap());
            }
            // Upstream removes exactly one prefix, not an arbitrary number.
            assert!(selected(path, &root, &[format!("-././{pattern}")]));
        }
    }

    #[test]
    fn resource_filter_home_uses_platform_environment_and_account_fallback() {
        const CHILD: &str = "PI_RS_FILTER_HOME_CHILD";
        if let Ok(mode) = std::env::var(CHILD) {
            let home = filter_home_dir().expect("account home");
            let decoy = PathBuf::from("pi-filter-wrong-platform-home");
            if mode == "fallback" {
                assert_ne!(home, decoy);
                assert!(!home.as_os_str().is_empty());
            } else if mode == "empty" {
                assert!(home.as_os_str().is_empty());
            } else {
                assert_eq!(home, PathBuf::from("pi-filter-home"));
            }
            let path = normalize_filter_home(&home, "prompts/review.md");
            assert!(!selected(&path, &home, &["-~/prompts/review.md".into()]));
            return;
        }
        for mode in ["fallback", "environment", "empty"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.args(["--exact", "packages::tests::resource_filter_home_uses_platform_environment_and_account_fallback", "--nocapture"])
                .env(CHILD, mode);
            #[cfg(unix)]
            {
                command.env("USERPROFILE", "pi-filter-wrong-platform-home");
                if mode == "fallback" {
                    command.env_remove("HOME");
                } else {
                    command.env(
                        "HOME",
                        if mode == "empty" {
                            ""
                        } else {
                            "pi-filter-home"
                        },
                    );
                }
            }
            #[cfg(windows)]
            {
                command.env("HOME", "pi-filter-wrong-platform-home");
                if mode == "fallback" {
                    command.env_remove("USERPROFILE");
                } else {
                    command.env(
                        "USERPROFILE",
                        if mode == "empty" {
                            ""
                        } else {
                            "pi-filter-home"
                        },
                    );
                }
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
    }

    #[test]
    fn resource_filter_file_urls_validate_before_matching() {
        let root = std::env::temp_dir();
        let path = root.join("prompts").join("review.md");
        let base = reqwest::Url::from_directory_path(&root).unwrap();
        let invalid = format!("{base}prompts%2Freview.md");
        assert!(!resource_matches(&path, &root, &invalid, true, false));
        assert!(!resource_matches(&path, &root, &invalid, false, false));
        assert!(selected(&path, &root, &[format!("-{invalid}")]));
    }

    #[test]
    fn package_filters_match_bounded_positive_extglobs() {
        let root = Path::new("package");
        let candidates = [
            "review",
            "triage",
            "audit",
            "review-quick",
            "@(review|triage)",
        ]
        .map(|name| root.join(format!("prompts/{name}.md")));
        let filter = ResourceFilter::new(
            &json!({"prompts":["prompts/@(review|triage).md", "!@(triage|audit).md", "+prompts/audit.md"]}),
            "prompts",
        ).unwrap();

        assert_eq!(
            filter.states(&candidates, root, false).unwrap(),
            [
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(false)
            ]
        );

        let delta = ResourceFilter::new(
            &json!({"autoload":false,"prompts":["prompts/review?(-quick).md"]}),
            "prompts",
        )
        .unwrap();
        assert_eq!(
            delta.states(&candidates, root, false).unwrap(),
            [Some(true), None, None, Some(true), None]
        );
    }

    #[test]
    fn package_settings_filters_match_a_bounded_negative_extglob() {
        let root = Path::new("package");
        let candidates = ["review", "triage", "audit", "review-quick", ".hidden"]
            .map(|name| root.join(format!("prompts/{name}.md")));

        for (patterns, expected) in [
            (
                vec!["prompts/!(review|triage).md"],
                [false, false, true, true, false],
            ),
            (
                vec!["!prompts/!(review|triage).md"],
                [true, true, false, false, true],
            ),
            (
                vec!["prompts/!(review|triage).md", "+prompts/review.md"],
                [true, false, true, true, false],
            ),
            (vec!["!!prompts/!(review|triage).md"], [false; 5]),
        ] {
            let filter = ResourceFilter::new(&json!({"prompts": patterns}), "prompts").unwrap();
            assert_eq!(
                filter.states(&candidates, root, false).unwrap(),
                expected.map(Some),
                "{patterns:?}"
            );
        }
    }

    #[test]
    fn package_filter_reports_invalid_file_urls_when_evaluated() {
        let root = std::env::temp_dir();
        let path = root.join("prompts/review.md");
        let invalid = format!(
            "{}prompts%2Freview.md",
            reqwest::Url::from_directory_path(&root).unwrap()
        );
        for prefix in ["", "!", "+", "-", "+./", "-./"] {
            let filters = if prefix.starts_with('+') {
                vec!["no-match".to_owned(), format!("{prefix}{invalid}")]
            } else {
                vec![format!("{prefix}{invalid}")]
            };
            let filter = ResourceFilter::new(&json!({"prompts": filters}), "prompts").unwrap();
            assert!(
                filter
                    .includes(&path, &root, false)
                    .unwrap_err()
                    .to_string()
                    .contains("encoded"),
                "{prefix}"
            );
        }
        let filter = ResourceFilter::new(
            &json!({"autoload": false, "prompts": [format!("-{invalid}")]}),
            "prompts",
        )
        .unwrap();
        assert!(filter.includes(&path, &root, true).is_err());
        let filter = ResourceFilter::new(&json!({"prompts": ["**", invalid]}), "prompts").unwrap();
        assert!(filter.includes(&path, &root, false).unwrap());
    }

    #[test]
    fn package_filters_evaluate_all_candidates_per_stage() {
        let root = std::env::temp_dir();
        let base = reqwest::Url::from_directory_path(&root).unwrap();
        let paths = vec![root.join("a.md"), root.join("b.md")];
        let filter = ResourceFilter::new(
            &json!({"prompts": ["a.md", format!("{base}bad%ZZ"), format!("-{base}bad%2Fpath")]}),
            "prompts",
        )
        .unwrap();
        let error = filter.apply(&paths, &root, &[false, false]).unwrap_err();
        assert!(error.to_string().contains("URI malformed"));
    }

    #[cfg(windows)]
    #[test]
    fn resource_filters_accept_windows_home_and_unicode_unc() {
        let home = filter_home_dir().unwrap();
        let path = normalize_filter_home(&home, r"prompts\review.md");
        assert!(resource_matches(
            &path,
            &home,
            r"~\prompts\*.md",
            false,
            false
        ));
        assert!(!selected(&path, &home, &[r"-~\prompts\review.md".into()]));
        let unc = Path::new(r"\\☃\share\review.md");
        let url = "file://xn--n3h/share/review.md";
        assert!(resource_matches(
            unc,
            Path::new(r"\\☃\share"),
            url,
            true,
            false
        ));
        assert!(!selected(
            unc,
            Path::new(r"\\☃\share"),
            &[format!("-{url}")]
        ));
    }

    #[test]
    fn extension_manifest_applies_includes_and_exclusions_together() {
        let root = Path::new("/package");
        let excluded = root.join("extensions/unexpected");
        let allowed = root.join("extensions/allowed");
        let filters = vec!["extensions".into(), "!extensions/unexpected".into()];
        assert!(!extension_selected(&excluded, root, &filters));
        assert!(extension_selected(&allowed, root, &filters));
        let outside_binary = Path::new("/cache/bin/demo.exe");
        assert!(extension_selected(outside_binary, root, &["demo".into()]));
        assert!(!extension_selected(
            outside_binary,
            root,
            &["demo".into(), "!demo".into()]
        ));
        assert!(extension_selected(
            &excluded,
            root,
            &[
                "extensions".into(),
                "!extensions/unexpected".into(),
                "+extensions/unexpected".into()
            ]
        ));
        assert!(!extension_selected(
            &excluded,
            root,
            &["extensions".into(), "-extensions/unexpected".into()]
        ));
    }
    #[test]
    fn absent_manifest_keeps_default_discovery_and_oversized_manifests_fail() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir_all(root.join("themes")).unwrap();
        let theme = root.join("themes/default.json");
        std::fs::write(&theme, "{}").unwrap();
        assert!(read_optional_pi_package_json(root).unwrap().is_none());
        assert_eq!(
            collect_package_themes(root, &local_startup_manifest(root).unwrap().0).unwrap(),
            [theme]
        );
        for name in ["package.json", "pi-package.json", "Cargo.toml"] {
            let path = root.join(name);
            std::fs::write(&path, vec![b'x'; MAX_PACKAGE_MANIFEST_BYTES as usize + 1]).unwrap();
            let error = local_startup_manifest(root).unwrap_err().to_string();
            assert!(error.contains("size limit"), "{name}: {error}");
            std::fs::remove_file(path).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn fifo_manifests_are_rejected_before_a_deadline() {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;
        for name in ["package.json", "pi-package.json", "Cargo.toml"] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().to_path_buf();
            let path = root.join(name);
            let name_bytes = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(name_bytes.as_ptr(), 0o600) }, 0);
            let (sender, receiver) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                let _ = sender.send(local_startup_manifest(&root));
            });
            let result = receiver.recv_timeout(Duration::from_secs(5));
            if result.is_err() {
                // Release a regressed blocking FIFO open so the test fails cleanly.
                let _writer = std::fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&path);
            }
            let result = result.expect("manifest inspection blocked on FIFO");
            assert!(result.is_err(), "FIFO {name} must be rejected");
            worker.join().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn manifest_replaced_with_fifo_after_probe_is_rejected() {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;
        for manifest_name in ["package.json", "pi-package.json", "Cargo.toml"] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().to_path_buf();
            let path = root.join(manifest_name);
            std::fs::write(&path, "{}").unwrap();
            let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
            let (sender, receiver) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                let result = read_optional_manifest_after_probe(&root, manifest_name, || {
                    std::fs::remove_file(root.join(manifest_name)).unwrap();
                    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                });
                let _ = sender.send(result);
            });
            let result = receiver.recv_timeout(Duration::from_secs(5));
            if result.is_err() {
                let _writer = std::fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&path);
            }
            let error = result
                .expect("replacement FIFO blocked manifest inspection")
                .unwrap_err();
            assert!(error.to_string().contains("regular file"), "{error}");
            worker.join().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn broken_pi_manifest_symlink_is_not_treated_as_absent() {
        let directory = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(
            directory.path().join("missing"),
            directory.path().join("package.json"),
        )
        .unwrap();
        assert!(read_optional_pi_package_json(directory.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn pi_manifest_does_not_hide_unusable_cargo_manifest() {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("pi-package.json"), r#"{"pi":{"themes":[]}}"#).unwrap();
        let cargo = root.join("Cargo.toml");
        std::os::unix::fs::symlink(root.join("missing"), &cargo).unwrap();
        assert!(local_startup_manifest(root).is_err());
        std::fs::remove_file(&cargo).unwrap();
        let name = std::ffi::CString::new(cargo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let root = root.to_path_buf();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _ = sender.send(local_startup_manifest(&root));
        });
        let result = receiver.recv_timeout(Duration::from_secs(5));
        if result.is_err() {
            let _writer = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&cargo);
        }
        assert!(
            result
                .expect("Cargo FIFO blocked manifest inspection")
                .is_err()
        );
        worker.join().unwrap();
    }

    #[test]
    fn explicitly_named_theme_file_bypasses_ignore_rules() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir_all(root.join("assets")).unwrap();
        let theme = root.join("assets/ignored.json");
        std::fs::write(&theme, "{}").unwrap();
        std::fs::write(root.join(".ignore"), "assets/\n").unwrap();
        let manifest = json!({"themes":["assets/ignored.json"]});
        assert_eq!(
            collect_package_themes(root, &manifest).unwrap().as_slice(),
            std::slice::from_ref(&theme)
        );
        assert_eq!(
            collect_package_resources(root, &manifest, Vec::new())
                .unwrap()
                .themes,
            [theme]
        );
        let excluded = json!({"themes":["assets/ignored.json", "!assets/ignored.json"]});
        assert!(collect_package_themes(root, &excluded).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn explicitly_named_theme_files_follow_file_and_directory_symlinks() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("package");
        let external = directory.path().join("external");
        std::fs::create_dir_all(root.join("assets")).unwrap();
        std::fs::create_dir_all(&external).unwrap();
        std::fs::write(external.join("theme.json"), "{}").unwrap();
        symlink(
            external.join("theme.json"),
            root.join("assets/file-link.json"),
        )
        .unwrap();
        symlink(&external, root.join("assets/dir-link")).unwrap();
        let files = vec![
            root.join("assets/file-link.json"),
            root.join("assets/dir-link/theme.json"),
        ];
        let manifest = json!({"themes":["assets/file-link.json", "assets/dir-link/theme.json"]});
        assert_eq!(collect_package_themes(&root, &manifest).unwrap(), files);
        assert_eq!(
            collect_package_resources(&root, &manifest, Vec::new())
                .unwrap()
                .themes,
            files
        );
    }

    #[test]
    fn local_package_sources_trim_surrounding_whitespace() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("native package")).unwrap();
        let expected = normalize("native package", directory.path()).unwrap();
        assert_eq!(
            normalize(" \tnative package\n", directory.path()).unwrap(),
            expected
        );
        assert!(is_package_source(" native package ", directory.path()));
        assert!(matches_source(
            &json!(" native package "),
            &expected,
            directory.path()
        ));
        assert_eq!(
            normalize(" ~/pi-package ", directory.path()).unwrap(),
            normalize("~/pi-package", directory.path()).unwrap()
        );
    }

    #[test]
    fn package_source_home_fallback_and_relative_base() {
        const CHILD: &str = "PI_RS_PACKAGE_HOME_CHILD";
        if let Some(case) = std::env::var_os(CHILD) {
            let root = std::env::current_dir()
                .unwrap()
                .join("package-home-test-base");
            let actual = normalize("~/child", &root).unwrap();
            let expected_home = if case == "relative" {
                root.join("relative-home")
            } else if (case == "empty" && cfg!(unix)) || (case != "relative" && cfg!(windows)) {
                root.clone()
            } else {
                filter_home_dir().unwrap()
            };
            assert_eq!(
                actual,
                resolve_source_path(&expected_home.join("child"))
                    .unwrap()
                    .to_string_lossy()
            );
            assert_eq!(
                normalize("~", &root).unwrap(),
                resolve_source_path(&expected_home)
                    .unwrap()
                    .to_string_lossy()
            );
            let doubled = normalize("~//child", &root).unwrap();
            let expected_doubled = if case == "empty" && cfg!(unix) {
                resolve_source_path(Path::new("/child"))
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            } else {
                actual.clone()
            };
            assert_eq!(doubled, expected_doubled);
            #[cfg(windows)]
            {
                assert_eq!(normalize(r"~\child", &root).unwrap(), actual);
                assert_eq!(normalize(r"~\\child", &root).unwrap(), actual);
            }
            return;
        }
        for case in ["relative", "empty", "missing"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "packages::tests::package_source_home_fallback_and_relative_base",
                    "--nocapture",
                ])
                .env(CHILD, case);
            #[cfg(windows)]
            command.env(
                "USERPROFILE",
                std::env::current_dir()
                    .unwrap()
                    .join("package-home-test-base"),
            );
            match case {
                "relative" => {
                    command.env("HOME", "relative-home");
                }
                "empty" => {
                    command.env("HOME", "");
                }
                _ => {
                    command.env_remove("HOME");
                }
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
    }

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
    fn package_prompt_overrides_respect_legacy_candidates_and_markdown_type() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for directory in ["prompts", "prompts/pruned", "node_modules"] {
            std::fs::create_dir(root.join(directory)).unwrap();
        }
        for file in [
            "prompts/visible.md",
            "prompts/.hidden.md",
            "prompts/git.md",
            "prompts/private.md",
            "prompts/pruned/secret.md",
            "prompts/not-prompt.txt",
            "node_modules/module.md",
        ] {
            std::fs::write(root.join(file), "fixture").unwrap();
        }
        std::fs::write(root.join(".gitignore"), "prompts/git.md\n").unwrap();
        std::fs::write(
            root.join(".ignore"),
            "prompts/private.md\nprompts/pruned/\n",
        )
        .unwrap();
        let discover = |patterns| {
            collect_package_resources(root, &json!({"prompts": patterns}), Vec::new())
                .unwrap()
                .prompts
        };
        let mut implicit = discover(json!(["!prompts/visible.md"]));
        implicit.sort();
        assert_eq!(
            implicit,
            vec![root.join("prompts/.hidden.md"), root.join("prompts/git.md")]
        );
        assert_eq!(
            discover(json!([
                "prompts/visible.md",
                "+prompts/private.md",
                "+prompts/pruned/secret.md",
                "+node_modules/module.md",
                "+prompts/not-prompt.txt"
            ])),
            vec![root.join("prompts/visible.md")]
        );
        assert_eq!(
            discover(json!(["prompts/not-prompt.txt"])),
            vec![root.join("prompts/not-prompt.txt")]
        );
        assert_eq!(
            discover(json!(["prompts/*.txt"])),
            vec![root.join("prompts/not-prompt.txt")]
        );
        assert!(!discover(json!(["prompts"])).contains(&root.join("prompts/not-prompt.txt")));
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

    #[test]
    fn package_prompt_glob_expands_cross_component_braces_under_literal_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("pkg[1]");
        for directory in ["prompts/team/a", "prompts/team/b/nested", "prompts/team/c"] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
        }
        std::fs::write(root.join("prompts/team/a/review.md"), "a").unwrap();
        std::fs::write(root.join("prompts/team/b/nested/review.md"), "b").unwrap();
        std::fs::write(root.join("prompts/team/c/review.md"), "c").unwrap();
        let prompts = collect_package_resources(
            &root,
            &json!({"prompts": ["prompts/{team/a,team/b}/**/*.md"]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert_eq!(
            prompts,
            vec![
                root.join("prompts/team/a/review.md"),
                root.join("prompts/team/b/nested/review.md")
            ]
        );
        let simple = collect_package_resources(
            &root,
            &json!({"prompts": ["prompts/team/a/*.md"]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert_eq!(simple, vec![root.join("prompts/team/a/review.md")]);
    }

    #[test]
    fn package_prompt_globs_match_explicit_external_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("package");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        std::fs::create_dir_all(outside.join("prompts")).unwrap();
        std::fs::write(root.join("prompts/review.md"), "fixture").unwrap();
        std::fs::write(root.join("prompts/.hidden.md"), "hidden").unwrap();
        std::fs::write(outside.join("prompts/outside.md"), "outside").unwrap();
        std::fs::write(outside.join("prompts/.hidden.md"), "hidden").unwrap();
        let discover = |pattern: String| {
            collect_package_resources(&root, &json!({"prompts": [pattern]}), Vec::new())
                .unwrap()
                .prompts
        };
        let absolute = format!("{}/prompts/*.md", root.display());
        assert_eq!(
            discover(absolute.clone()),
            vec![root.join("prompts/review.md")]
        );
        assert_eq!(
            discover(absolute.replace('/', "\\")),
            vec![root.join("prompts/review.md")]
        );
        assert_eq!(
            discover(format!("{}/*/prompts/*.md", temp.path().display())),
            vec![
                outside.join("prompts/outside.md"),
                root.join("prompts/review.md")
            ]
        );
        assert_eq!(
            discover(format!("{}/prompts/*.md", outside.display())),
            vec![outside.join("prompts/outside.md")]
        );
        assert_eq!(
            discover("../outside/prompts/*.md".into()),
            vec![outside.join("prompts/outside.md")]
        );
        assert!(discover("*/../outside/prompts/*.md".into()).is_empty());
    }

    #[test]
    fn package_prompt_glob_prunes_unrelated_subtrees_and_limits_depth() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("users");
        let root = base.join("owner");
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        let deep = base.join("other/unrelated/a/b/c/prompts");
        std::fs::create_dir_all(&deep).unwrap();
        std::fs::write(deep.join("unexpected.md"), "fixture").unwrap();
        std::fs::write(root.join("prompts/visible.md"), "fixture").unwrap();
        let pattern = base.join("*/prompts/*.md");
        let prefix = PromptGlobPrefix::new(&pattern, 0).unwrap();
        assert_eq!(prefix.max_depth(&base), Some(3));
        assert!(prefix.can_match(&base.join("other")));
        assert!(!prefix.can_match(&base.join("other/unrelated")));
        assert!(!prefix.can_match(&deep));
        let prompts = collect_package_resources(
            &root,
            &json!({"prompts": [pattern.to_string_lossy()]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert_eq!(prompts, vec![root.join("prompts/visible.md")]);
        let recursive = PromptGlobPrefix::new(&base.join("*/**/prompts/*.md"), 0).unwrap();
        assert_eq!(recursive.max_depth(&base), None);
        assert!(recursive.can_match(&deep));
    }

    #[cfg(windows)]
    #[test]
    fn package_prompt_absolute_glob_matches_verbatim_package_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("package");
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        std::fs::write(root.join("prompts/review.md"), "fixture").unwrap();
        let canonical = root.canonicalize().unwrap();
        let ordinary = dunce::simplified(&canonical);
        let pattern = format!("{}/prompts/*.md", ordinary.display());
        let prompts =
            collect_package_resources(&canonical, &json!({"prompts": [pattern]}), Vec::new())
                .unwrap()
                .prompts;
        assert_eq!(prompts.len(), 1);
        assert_eq!(
            prompts[0].canonicalize().unwrap(),
            canonical.join("prompts/review.md")
        );
        let excluded = collect_package_resources(
            &canonical,
            &json!({"prompts": [pattern, "!prompts/review.md"]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert!(excluded.is_empty());
        let pattern = format!("{}/prompts/*.md", ordinary.display());
        let excluded = collect_package_resources(
            &canonical,
            &json!({"prompts": [pattern, "-prompts/review.md"]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert!(excluded.is_empty());
        let pattern = format!("{}/prompts/*.md", ordinary.display());
        let included = collect_package_resources(
            &canonical,
            &json!({"prompts": [pattern, "!prompts/review.md", "+prompts/review.md"]}),
            Vec::new(),
        )
        .unwrap()
        .prompts;
        assert_eq!(included.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn package_prompt_legacy_basename_and_backslash_patterns_are_bounded() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("package");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.join("prompts/nested")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        for file in [
            "root.md",
            "prompts/nested/review.md",
            "prompts/nested/.hidden.md",
            "prompts/nested/ignored.md",
        ] {
            std::fs::write(root.join(file), "fixture").unwrap();
        }
        std::fs::write(root.join(".ignore"), "ignored.md\n").unwrap();
        std::fs::write(outside.join("external.md"), "outside").unwrap();
        symlink(&outside, root.join("prompts/link")).unwrap();
        symlink("/", root.join("unrelated.md")).unwrap();
        let discover = |patterns| {
            collect_package_resources(&root, &json!({"prompts": patterns}), Vec::new())
                .unwrap()
                .prompts
        };
        let mut basename = discover(json!(["*.md"]));
        basename.sort();
        assert_eq!(
            basename,
            vec![
                root.join("prompts/nested/.hidden.md"),
                root.join("prompts/nested/review.md"),
                root.join("root.md")
            ]
        );
        assert_eq!(
            discover(json!([r"prompts\nested\*.md"])),
            vec![
                root.join("prompts/nested/ignored.md"),
                root.join("prompts/nested/review.md")
            ]
        );
        assert_eq!(
            discover(json!([r"prompts\nested\review.md"])),
            vec![root.join("prompts/nested/review.md")]
        );
        assert_eq!(
            discover(json!(["root.md", r"+prompts\nested\review.md"])),
            vec![root.join("root.md"), root.join("prompts/nested/review.md")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn package_prompt_literal_basename_matches_nested_legacy_candidates() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir(root.join("prompts")).unwrap();
        std::fs::create_dir(root.join("private")).unwrap();
        std::fs::write(root.join("review.md"), "root").unwrap();
        std::fs::write(root.join("prompts/review.md"), "nested").unwrap();
        std::fs::write(root.join("private/review.md"), "ignored").unwrap();
        std::fs::write(root.join(".ignore"), "private/\n").unwrap();
        symlink("/", root.join("escape")).unwrap();
        let discover = || {
            collect_package_resources(root, &json!({"prompts": ["review.md"]}), Vec::new())
                .unwrap()
                .prompts
        };
        assert_eq!(
            discover(),
            vec![root.join("review.md"), root.join("prompts/review.md")]
        );
        std::fs::remove_file(root.join("review.md")).unwrap();
        assert_eq!(discover(), vec![root.join("prompts/review.md")]);
    }

    #[cfg(unix)]
    #[test]
    fn package_prompt_basename_glob_uses_only_unignored_files() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir(root.join("archive.md")).unwrap();
        std::fs::write(root.join("archive.md/inside.md"), "inside").unwrap();
        symlink("/", root.join("archive.md/escape")).unwrap();
        std::fs::write(root.join("private.md"), "private").unwrap();
        std::fs::write(root.join("visible.md"), "visible").unwrap();
        std::fs::write(root.join(".ignore"), "private.md\n").unwrap();
        let prompts = collect_package_resources(root, &json!({"prompts": ["*.md"]}), Vec::new())
            .unwrap()
            .prompts;
        assert_eq!(
            prompts,
            vec![root.join("archive.md/inside.md"), root.join("visible.md")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn package_prompt_basename_glob_includes_unignored_symlink_files() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::write(root.join("content.txt"), "prompt content").unwrap();
        std::fs::write(root.join(".ignore"), "ignored.md\n").unwrap();
        symlink("content.txt", root.join("alias.md")).unwrap();
        symlink("content.txt", root.join("ignored.md")).unwrap();
        symlink("missing", root.join("broken.md")).unwrap();
        symlink("/", root.join("directory.md")).unwrap();
        let prompts = collect_package_resources(root, &json!({"prompts": ["*.md"]}), Vec::new())
            .unwrap()
            .prompts;
        assert_eq!(prompts, vec![root.join("alias.md")]);
        assert_eq!(
            std::fs::read_to_string(&prompts[0]).unwrap(),
            "prompt content"
        );
    }

    #[cfg(unix)]
    #[test]
    fn package_prompt_path_glob_collects_selected_symlink_directory() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("package");
        let external = temp.path().join("external");
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        std::fs::create_dir_all(&external).unwrap();
        std::fs::write(external.join("review.md"), "fixture").unwrap();
        symlink(&external, root.join("prompts/linked")).unwrap();
        let prompts =
            collect_package_resources(&root, &json!({"prompts": ["prompts/*"]}), Vec::new())
                .unwrap()
                .prompts;
        assert_eq!(prompts, vec![root.join("prompts/linked/review.md")]);
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
    #[cfg(windows)]
    #[test]
    fn package_sources_and_filters_accept_windows_shell_drive_paths() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("native package");
        let prompts = root.join("prompts");
        std::fs::create_dir_all(&prompts).unwrap();
        let prompt = prompts.join("review.md");
        std::fs::write(&prompt, "review").unwrap();
        let root = dunce::simplified(&root);
        let mut components = root.components();
        let Some(std::path::Component::Prefix(prefix)) = components.next() else {
            panic!("fixture must have a drive prefix: {root:?}");
        };
        let std::path::Prefix::Disk(drive) = prefix.kind() else {
            panic!("fixture must use a disk: {root:?}");
        };
        let suffix = components
            .as_path()
            .display()
            .to_string()
            .replace('\\', "/");
        let drive = char::from(drive).to_ascii_lowercase();
        let expected = normalize(root.to_str().unwrap(), directory.path()).unwrap();
        for prefix in ["", "mnt/", "CYGDRIVE/"] {
            let source = format!("/{prefix}{drive}{suffix}");
            assert_eq!(normalize(&source, directory.path()).unwrap(), expected);
            assert!(resource_matches(
                &prompt,
                root,
                &format!("{source}/prompts/*.md"),
                false,
                false
            ));
            assert!(resource_matches(
                &prompt,
                root,
                &format!("{source}/prompts/review.md"),
                true,
                false
            ));
        }
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
    fn local_package_file_urls_match_node_path_validation() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("native package");
        std::fs::create_dir(&package).unwrap();
        let url = reqwest::Url::from_directory_path(&package).unwrap();
        let expected = normalize("native package", root.path()).unwrap();
        assert_eq!(
            normalize(&format!("{url}?ignored=%2F#%FF"), root.path()).unwrap(),
            expected
        );
        assert!(matches_source(
            &json!(format!("{url}?ignored#fragment")),
            &expected,
            root.path()
        ));
        for (tail, error) in [
            ("a%2Fb", "encoded"),
            ("a%ZZ", "URI malformed"),
            ("a%FF", "URI malformed"),
        ] {
            assert!(
                normalize(&format!("{url}{tail}"), root.path())
                    .unwrap_err()
                    .to_string()
                    .contains(error)
            );
        }
        #[cfg(windows)]
        assert!(normalize(&format!("{url}a%5Cb"), root.path()).is_err());
        #[cfg(unix)]
        assert_eq!(
            normalize("file:local", root.path()).unwrap(),
            resolve_source_path(&root.path().join("file:local"))
                .unwrap()
                .to_string_lossy()
        );
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
            (
                "git@host:org/repo@feature@v2",
                "git@host:org/repo@feature",
                "v2",
            ),
            (
                "ssh://git@host/org/repo#feature@v2",
                "ssh://git@host/org/repo",
                "feature@v2",
            ),
            (
                "https://host/org/repo#feature@v2",
                "https://host/org/repo",
                "feature@v2",
            ),
            (
                "https://user:secret@host/org/repo#feature@v2",
                "https://user:secret@host/org/repo",
                "feature@v2",
            ),
            (
                "https://host/org@team/repo@stable",
                "https://host/org@team/repo",
                "stable",
            ),
            (
                "https://host/org@team/repo#stable",
                "https://host/org@team/repo",
                "stable",
            ),
            ("git@host:org/repo", "git@host:org/repo", "HEAD"),
            (
                "https://user@host/repo#old",
                "https://user@host/repo",
                "old",
            ),
        ] {
            assert_eq!(git_reference(source), (url, reference));
        }
        assert_eq!(
            source_identity("git:ssh://git@host/org/repo#feature@v2"),
            source_identity("git:https://host/org/repo@main")
        );
        assert!(matches_source(
            &json!({"source":"git:https://host/org/repo#feature@v2", "prompts":["+prompts/keep.md"]}),
            "git:ssh://git@host/org/repo@main",
            Path::new("."),
        ));
        assert!(!matches_source(
            &json!({"source":"git:https://host/org/repo#feature@v2"}),
            "git:https://host/org/other@main",
            Path::new("."),
        ));
        assert_eq!(
            source_identity("git:https://host/org@team/repo@stable"),
            source_identity("git:https://host/org@team/repo#stable")
        );
        assert!(matches_source(
            &json!({"source":"git:https://host/org@team/repo@stable", "prompts":["+prompts/keep.md"]}),
            "git:https://host/org@team/repo#stable",
            Path::new(".")
        ));
        assert!(ambiguous_git_reference("https://host/org@team/repo@stable"));
        assert!(!ambiguous_git_reference(
            "https://host/org@team/repo#stable"
        ));
        assert!(!ambiguous_git_reference("ssh://git@host/org/repo@stable"));
    }

    #[test]
    fn git_source_display_redacts_url_userinfo_without_changing_identity() {
        for source in [
            "git:https://private-user:topsecret@example.invalid/org/repo@stable",
            "git:ssh://private-user@example.invalid/org/repo#feature@v2",
            "git:https://private-user:topsecret@example.invalid?query@value",
        ] {
            let displayed = display_source(source);
            assert!(!displayed.contains("private-user"), "{displayed}");
            assert!(!displayed.contains("topsecret"), "{displayed}");
            assert!(
                displayed.contains("[redacted]@example.invalid"),
                "{displayed}"
            );
        }
        assert_eq!(
            display_source("git:https://example.invalid/org/repo@stable"),
            "git:https://example.invalid/org/repo@stable"
        );
        assert_eq!(
            source_identity("git:https://private-user:topsecret@example.invalid/org/repo@stable"),
            source_identity("git:https://example.invalid/org/repo#feature")
        );
        let error = package_command_failed(
            "git:ssh://private-user@example.invalid/org/repo",
            "exit status: 1",
            b"private-user@example.invalid: Permission denied (publickey).",
            true,
        )
        .to_string();
        assert!(!error.contains("private-user"), "{error}");
        assert!(
            error.contains("exit status: 1") && error.contains("redacted"),
            "{error}"
        );
        assert!(
            error.contains("credential helper or an SSH agent"),
            "{error}"
        );
        let error = package_command_failed(
            "git:https://example.invalid/org/repo",
            "exit status: 1",
            b"useful error",
            true,
        )
        .to_string();
        assert!(error.contains("useful error"), "{error}");
        assert!(!error.contains("credential helper"), "{error}");
    }

    #[tokio::test]
    async fn git_userinfo_keeps_safe_offline_diagnostic() {
        let source = "git:ssh://git@example.invalid/org/repo@stable";
        let error = match install(source, true, None).await {
            Ok(_) => panic!("offline install unexpectedly succeeded"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("cannot install package in offline mode"),
            "{error}"
        );
        assert!(error.contains("[redacted]@example.invalid"), "{error}");
        assert!(!error.contains("ssh://git@"), "{error}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn captured_git_package_commands_disable_prompts() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let git = root.path().join("git");
        std::fs::write(
            &git,
            "#!/bin/sh\nprintf '%s\\n%s' \"$GIT_TERMINAL_PROMPT\" \"$GIT_SSH_COMMAND\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut command = tokio::process::Command::new("git");
        command
            .current_dir(root.path())
            .env("PATH", root.path())
            .env("GIT_TERMINAL_PROMPT", "1");
        let output = run(command, "git:https://example.invalid/org/repo", None)
            .await
            .unwrap();
        let expected_ssh =
            std::env::var_os("GIT_SSH_COMMAND").unwrap_or_else(|| "ssh -o BatchMode=yes".into());
        let mut expected = b"0\n".to_vec();
        expected.extend_from_slice(expected_ssh.as_encoded_bytes());
        assert_eq!(output, expected);
    }

    #[tokio::test]
    async fn git_userinfo_hides_real_subprocess_stderr() {
        let mut command = if cfg!(windows) {
            let mut command = tokio::process::Command::new("cmd");
            command.args(["/C", "echo private-user 1>&2 & exit /b 7"]);
            command
        } else {
            let mut command = tokio::process::Command::new("sh");
            command.args(["-c", "printf private-user >&2; exit 7"]);
            command
        };
        command.current_dir(std::env::temp_dir());
        let error = run(
            command,
            "git:ssh://private-user@example.invalid/org/repo",
            None,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("subprocess output redacted"), "{error}");
        assert!(!error.contains("private-user"), "{error}");
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
        assert!(!empty.includes(&path, root, false).unwrap());
        assert!(empty.includes(&path, root, true).unwrap());
        let filter = ResourceFilter::new(&json!({"autoload":false,"prompts":["prompts/*.md","-prompts/keep.md","+prompts/keep.md"]}), "prompts").unwrap();
        assert!(filter.includes(&path, root, false).unwrap());
        assert!(
            !filter
                .includes(&root.join("other.md"), root, false)
                .unwrap()
        );
        let filter = ResourceFilter::new(
            &json!({"autoload":false,"prompts":["!prompts/*.md"]}),
            "prompts",
        )
        .unwrap();
        assert!(!filter.includes(&path, root, true).unwrap());
        assert!(filter.includes(&root.join("other.md"), root, true).unwrap());
        let normal = ResourceFilter::new(&json!({"prompts":[]}), "prompts").unwrap();
        assert!(!normal.includes(&path, root, true).unwrap());
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
    fn package_filter_globs_require_explicit_hidden_components() {
        let root = Path::new("/package");
        for (path, pattern, expected) in [
            (".hidden.md", "*.md", false),
            (".hidden.md", "?hidden.md", false),
            (".hidden.md", "[a.]hidden.md", false),
            (".hidden.md", "[.]hidden.md", true),
            (".hidden.md", "{.hidden,visible}.md", true),
            (".hidden.md", ".*.md", true),
            (".hidden.md", "+./.hidden.md", true),
            ("visible.md", "*.md", true),
            ("prompts/open/sub.md", "prompts/**/*.md", true),
            ("prompts/.nested/buried.md", "prompts/**/*.md", false),
            ("prompts/.nested/buried.md", "prompts/.*/buried.md", true),
            ("prompts/.nested/buried.md", "**/buried.md", true),
            ("prompts/.nested/buried.md", "prompts/**/.nested/*.md", true),
            ("prompts/.nested/buried.md", "prompts/**/.other/*.md", false),
            (".hidden.md", "!*.md", true),
            ("skills/.hidden/SKILL.md", "skills/*", false),
            ("skills/.hidden/SKILL.md", "skills/.*", true),
            ("skills/.hidden/SKILL.md", "+skills/.hidden", true),
        ] {
            let filter = ResourceFilter::new(&json!({"prompts": [pattern]}), "prompts").unwrap();
            assert_eq!(
                filter.includes(&root.join(path), root, false).unwrap(),
                expected,
                "{pattern} against {path}"
            );
        }
    }

    #[test]
    fn hidden_package_candidate_reaches_later_filter_diagnostics() {
        let root = std::env::temp_dir();
        let path = root.join(".hidden.md");
        let base = reqwest::Url::from_directory_path(&root).unwrap();
        let invalid = format!("{base}bad%2Fpath");
        let ordinary =
            ResourceFilter::new(&json!({"prompts": ["*.md", invalid]}), "prompts").unwrap();
        assert!(ordinary.includes(&path, &root, false).is_err());
        let explicit =
            ResourceFilter::new(&json!({"prompts": [".*.md", invalid]}), "prompts").unwrap();
        assert!(explicit.includes(&path, &root, false).unwrap());
    }

    #[test]
    fn settings_filters_accept_literal_brackets_and_ignore_comments() {
        let root = Path::new("/package");
        for (name, patterns, expected) in [
            ("draft[.md", json!(["draft[.md"]), true),
            (".draft[.md", json!([".draft[.md"]), true),
            ("draft[.md", json!(["draft?.md"]), true),
            ("#literal.md", json!(["#literal.md"]), false),
            (
                "#literal.md",
                json!(["no-match", "+prompts/#literal.md"]),
                true,
            ),
            ("visible.md", json!(["#ignore {", "*.md"]), true),
        ] {
            let filter = ResourceFilter::new(&json!({"prompts": patterns}), "prompts").unwrap();
            assert_eq!(
                filter
                    .includes(&root.join("prompts").join(name), root, false)
                    .unwrap(),
                expected,
                "{name} with {patterns}"
            );
        }
        assert!(validate_patterns(&["draft[.md".into()]).is_err());
    }

    #[test]
    fn package_filters_apply_negated_globs_to_each_candidate_spelling() {
        let root = Path::new("/package");
        let paths =
            ["visible.md", "draft[.md", "#literal.md"].map(|name| root.join("prompts").join(name));
        for (pattern, expected) in [
            ("!!visible.md", [false, false, false]),
            ("!!@(visible|other).md", [false, false, false]),
            ("!!!@(visible|other).md", [false, true, true]),
            ("!!**/visible.md", [true, false, false]),
            ("!!!**/visible.md", [false, true, true]),
            ("!!!!**/visible.md", [true, false, false]),
            ("!!**/draft[.md", [false, true, false]),
            ("!!", [false, false, false]),
            ("!!!", [true, true, true]),
        ] {
            let filter = ResourceFilter::new(&json!({"prompts": [pattern]}), "prompts").unwrap();
            assert_eq!(
                filter.apply(&paths, root, &[false; 3]).unwrap(),
                expected,
                "{pattern}"
            );
        }
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

    #[cfg(unix)]
    #[test]
    fn package_sources_resolve_parent_segments_before_symlinks() {
        let directory = tempfile::tempdir().unwrap();
        let work = directory.path().join("work");
        let package = work.join("package");
        let external = directory.path().join("external");
        let via_symlink = external.join("package");
        let target = external.join("target");
        for path in [&package, &via_symlink, &target] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::os::unix::fs::symlink(&target, work.join("link")).unwrap();

        let resolved = normalize("./link/../package", &work).unwrap();
        assert_eq!(Path::new(&resolved), package.canonicalize().unwrap());
        let resolved = normalize("./link/../missing/package", &work).unwrap();
        assert_eq!(
            Path::new(&resolved),
            work.canonicalize().unwrap().join("missing/package")
        );
        let resolved = normalize("./link", &work).unwrap();
        assert_eq!(Path::new(&resolved), target.canonicalize().unwrap());
    }
}
