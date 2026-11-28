//! Shared read-only package manifest parsing for the CLI and SDK.
use std::fs::Metadata;
use std::path::Path;

use pi_rs::{Error, Result};
use serde_json::{Value, json};

pub const MAX_PACKAGE_MANIFEST_BYTES: u64 = 1024 * 1024;

// Open nonblocking before checking the opened descriptor, so a FIFO or device
// substituted between path inspection and open cannot hang the startup UI.
pub fn read_optional_manifest(root: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    read_optional_manifest_after_probe(root, name, || {})
}

pub fn read_optional_manifest_after_probe(
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
pub fn local_startup_manifest(root: &Path) -> Result<(Value, bool)> {
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
        let name = cargo_manifest
            .as_ref()
            .and_then(|manifest| manifest.get("package"))
            .and_then(|package| package.get("name"));
        return Ok((json!({"pi": metadata, "name": name}), false));
    }
    let pi_package = package_manifest.is_some();
    Ok((package_manifest.unwrap_or_else(|| json!({})), pi_package))
}

// A present manifest is authoritative, including broken links or devices.
pub fn read_optional_pi_package_json(root: &Path) -> Result<Option<Value>> {
    read_optional_manifest(root, "package.json")?
        .map(|bytes| parse_pi_package_json(&root.join("package.json"), &bytes))
        .transpose()
}

pub fn parse_manifest_object(path: &Path, bytes: &[u8], strip_bom: bool) -> Result<Value> {
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

pub fn has_resource_fields(pi: &Value) -> bool {
    ["extensions", "skills", "prompts", "themes"]
        .iter()
        .any(|name| pi.get(name).is_some())
}

pub fn resource_patterns(pi: &Value, name: &str) -> Result<Vec<String>> {
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

pub fn manifest_pi(manifest: &Value) -> Result<&Value> {
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
    let mut theme_only = if explicit {
        json!({"extensions": [], "skills": [], "prompts": [],
            "themes": pi.and_then(|pi| pi.get("themes")).cloned().unwrap_or_else(|| json!([]))})
    } else {
        json!({})
    };
    if let Some(path) = pi.and_then(|pi| pi["changelogPath"].as_str()) {
        theme_only["changelogPath"] = json!(path);
    }
    // Treat an explicit Pi resource manifest as authoritative before scanning
    // native executables; only theme entries are supported from this format.
    Ok(json!({"pi": theme_only, "version": value.get("version"), "name": value.get("name")}))
}

pub fn validate_patterns(patterns: &[String]) -> Result<()> {
    validate_resource_patterns(patterns, false)
}

pub fn validate_resource_patterns(patterns: &[String], settings_filter: bool) -> Result<()> {
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

pub fn resource_glob(
    pattern: &str,
    allow_unclosed_class: bool,
) -> std::result::Result<globset::Glob, globset::Error> {
    globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .allow_unclosed_class(allow_unclosed_class)
        .build()
}

/// Match the native extension name in addition to its relative path.
pub fn extension_stem_matches(path: &Path, pattern: &str) -> bool {
    path.file_stem().is_some_and(|stem| stem == pattern)
}

/// A skill can be selected by either its index file or its containing directory.
pub fn skill_directory(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|_| path.file_name().is_some_and(|name| name == "SKILL.md"))
}

/// Test a regular file using the native platform's executable convention.
pub fn is_native_executable(_path: &Path, metadata: &Metadata) -> bool {
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        _path.extension().is_some_and(|ext| ext == "exe")
    }
}

/// Apply native include, exclude, force-include and force-exclude precedence.
pub fn select_resource_with(filters: &[String], matches: impl Fn(&str, bool) -> bool) -> bool {
    if filters.is_empty() {
        return false;
    }
    let mut plain = filters
        .iter()
        .filter(|filter| !filter.starts_with(['!', '+', '-']))
        .peekable();
    let mut included = plain.peek().is_none() || plain.any(|pattern| matches(pattern, false));
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
