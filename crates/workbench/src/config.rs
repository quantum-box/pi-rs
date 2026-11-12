//! User and trusted-project settings with non-destructive updates.
use pi_rs::{Error, Result};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::{Path, PathBuf};

pub fn agent_dir() -> PathBuf {
    pi_rs_workbench::agent_dir()
}

#[derive(Clone, Default)]
pub struct Settings {
    value: Value,
    resource_layers: Vec<(PathBuf, Value)>,
    // Live controls override merged settings only for this running session.
    live_auto_retry: Option<bool>,
    live_auto_compaction: Option<bool>,
}
impl Settings {
    pub fn live_controls(&self) -> (Option<bool>, Option<bool>) {
        (self.live_auto_compaction, self.live_auto_retry)
    }
    pub fn set_user_values(&mut self, values: &[(String, Value)]) -> Result<()> {
        self.set_user_values_at(&agent_dir().join("settings.json"), values)
    }
    fn set_user_values_at(&mut self, path: &Path, values: &[(String, Value)]) -> Result<()> {
        update_object(path, |settings| {
            for (key, value) in values {
                settings.insert(key.clone(), value.clone());
            }
            validate_settings(&Value::Object(settings.clone()), path)
        })?;
        if !self.value.is_object() {
            self.value = Value::Object(Map::new());
        }
        for (key, value) in values {
            self.value
                .as_object_mut()
                .expect("settings object")
                .insert(key.clone(), value.clone());
            if let Some((_, layer)) = self.resource_layers.first_mut() {
                layer
                    .as_object_mut()
                    .expect("settings layer")
                    .insert(key.clone(), value.clone());
            }
        }
        self.refresh_effective();
        Ok(())
    }
    fn refresh_effective(&mut self) {
        let mut layers = self.resource_layers.iter();
        if let Some((_, user)) = layers.next() {
            let mut value = user.clone();
            for (_, project) in layers {
                merge(&mut value, project.clone());
            }
            self.value = value;
        }
    }
    pub fn load(workspace: &Path, trusted: bool) -> Result<Self> {
        let user_path = agent_dir().join("settings.json");
        let mut value = read_object(&user_path)?;
        validate_settings(&value, &user_path)?;
        let mut resource_layers = vec![(agent_dir(), value.clone())];
        if trusted {
            let project_path = workspace.join(".pi/settings.json");
            let mut project = read_object(&project_path)?;
            let user_only = project.as_object_mut().expect("object");
            user_only.remove("defaultProjectTrust");
            user_only.remove("cacheWarming");
            validate_settings(&project, &project_path)?;
            resource_layers.push((workspace.join(".pi"), project.clone()));
            merge(&mut value, project);
        }
        Ok(Self {
            value,
            resource_layers,
            ..Self::default()
        })
    }
    pub fn string(&self, key: &str) -> Option<&str> {
        self.value.get(key)?.as_str()
    }
    pub fn thinking_for_model(&self, provider: &str, model: &str) -> Option<&str> {
        self.value
            .get("modelThinkingLevels")
            .and_then(|levels| levels.get(format!("{provider}/{model}")))
            .and_then(Value::as_str)
            .or_else(|| self.thinking_without_qualified_override(model))
    }
    pub fn thinking_without_qualified_override(&self, model: &str) -> Option<&str> {
        self.value
            .get("modelThinkingLevels")
            .and_then(|levels| levels.get(model))
            .and_then(Value::as_str)
            .or_else(|| self.string("defaultThinkingLevel"))
    }
    pub fn boolean(&self, key: &str) -> Option<bool> {
        self.value.get(key)?.as_bool()
    }
    pub fn http_idle_timeout_ms(&self) -> u64 {
        match self.value.get("httpIdleTimeoutMs") {
            Some(Value::String(value)) if value == "disabled" => 0,
            Some(Value::Number(value)) => value
                .as_f64()
                .unwrap_or(300_000.0)
                .clamp(0.0, 2_147_483_647.0) as u64,
            _ => 300_000,
        }
    }
    pub fn http_idle_timeout(&self) -> Option<std::time::Duration> {
        let value = self.http_idle_timeout_ms();
        (value > 0).then(|| std::time::Duration::from_millis(value))
    }
    pub fn strings(&self, key: &str) -> Vec<String> {
        self.value
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    }
    pub fn resource_layers(&self, key: &str) -> Result<Vec<(PathBuf, Vec<String>)>> {
        let layers = if self.resource_layers.is_empty() {
            vec![(agent_dir(), self.value.clone())]
        } else {
            self.resource_layers.clone()
        };
        layers
            .into_iter()
            .map(|(base, value)| {
                let entries = value
                    .get(key)
                    .map(|value| {
                        serde_json::from_value::<Vec<String>>(value.clone()).map_err(|error| {
                            Error::Config(format!(
                                "{}: invalid {key}: {error}",
                                base.join("settings.json").display()
                            ))
                        })
                    })
                    .transpose()?
                    .unwrap_or_default();
                Ok((base, entries))
            })
            .collect()
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn retry_settings(&self) -> pi_rs::RetrySettings {
        let value = &self.value["retry"];
        let defaults = pi_rs::RetrySettings::pi_defaults();
        let number = |key: &str, default| {
            value[key]
                .as_u64()
                .or_else(|| value[key].as_f64().map(|v| v as u64))
                .unwrap_or(default)
        };
        pi_rs::RetrySettings {
            enabled: self
                .live_auto_retry
                .unwrap_or_else(|| value["enabled"].as_bool().unwrap_or(defaults.enabled)),
            max_retries: number("maxRetries", u64::from(defaults.max_retries))
                .min(u64::from(u32::MAX)) as u32,
            base_delay_ms: number("baseDelayMs", defaults.base_delay_ms),
            max_agent_delay_ms: number("maxAgentDelayMs", defaults.max_agent_delay_ms),
        }
    }
    pub fn set_auto_retry_enabled(&mut self, enabled: bool) -> Result<()> {
        self.set_user_member("retry", "enabled", Value::Bool(enabled))?;
        self.live_auto_retry = Some(enabled);
        Ok(())
    }
    pub fn auto_compaction_enabled(&self) -> bool {
        self.live_auto_compaction.unwrap_or_else(|| {
            self.value["compaction"]["enabled"]
                .as_bool()
                .unwrap_or(true)
        })
    }
    pub fn compaction_tokens(&self, provider: &str, model: &str) -> pi_rs::CompactionTokens {
        let compaction = &self.value["compaction"];
        let overrides = &compaction["modelOverrides"][format!("{provider}/{model}")];
        let token_value = |name: &str| {
            overrides
                .get(name)
                .or_else(|| compaction.get(name))
                .and_then(|value| {
                    // JSON Schema accepts 42.0 as an integer and bounds it before this conversion.
                    value
                        .as_u64()
                        .or_else(|| value.as_f64().map(|number| number as u64))
                })
        };
        let defaults = pi_rs::CompactionTokens::pi_defaults();
        pi_rs::CompactionTokens {
            reserve_tokens: token_value("reserveTokens").or(defaults.reserve_tokens),
            keep_recent_tokens: token_value("keepRecentTokens").or(defaults.keep_recent_tokens),
        }
    }
    pub fn set_auto_compaction_enabled(&mut self, enabled: bool) -> Result<()> {
        self.set_user_member("compaction", "enabled", Value::Bool(enabled))?;
        self.live_auto_compaction = Some(enabled);
        Ok(())
    }
    pub fn autocomplete_max_visible(&self) -> usize {
        self.value["autocompleteMaxVisible"]
            .as_f64()
            .map_or(5, |count| count as usize)
    }
    pub fn enable_skill_commands(&self) -> bool {
        self.boolean("enableSkillCommands").unwrap_or(true)
    }
    pub fn terminal_progress(&self) -> bool {
        self.value["terminal"]["showTerminalProgress"]
            .as_bool()
            .unwrap_or(false)
    }
    pub fn resize_model_images(&self) -> bool {
        self.value["images"]["autoResize"].as_bool().unwrap_or(true)
    }
    pub fn block_model_images(&self) -> bool {
        self.value["images"]["blockImages"]
            .as_bool()
            .unwrap_or(false)
    }
    pub fn show_terminal_images(&self) -> bool {
        self.value["terminal"]["showImages"]
            .as_bool()
            .unwrap_or(true)
    }
    pub fn terminal_image_width(&self) -> usize {
        self.value["terminal"]["imageWidthCells"]
            .as_f64()
            .map_or(60, |width| width.min(f64::from(u16::MAX)) as usize)
    }
    pub fn mermaid_mode(&self) -> &str {
        self.value["markdown"]["mermaid"]
            .as_str()
            .unwrap_or("streaming")
    }

    pub fn clear_terminal_on_shrink(&self) -> bool {
        self.value["terminal"]["clearOnShrink"] == true
    }

    pub fn show_hardware_cursor(&self) -> bool {
        self.boolean("showHardwareCursor").unwrap_or_else(|| {
            std::env::var_os("PI_HARDWARE_CURSOR").is_some_and(|value| value == "1")
        })
    }

    pub fn output_padding(&self) -> usize {
        usize::from(self.value["outputPad"].as_f64() != Some(0.0))
    }

    pub fn set_user_member(&mut self, key: &str, member: &str, value: Value) -> Result<()> {
        self.set_user_member_at(&agent_dir().join("settings.json"), key, member, value)
    }
    fn set_user_member_at(
        &mut self,
        path: &Path,
        key: &str,
        member: &str,
        value: Value,
    ) -> Result<()> {
        let update = |settings: &mut Map<String, Value>| -> Result<()> {
            settings
                .entry(key)
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .ok_or_else(|| Error::Config(format!("{key} must be an object")))?
                .insert(member.into(), value.clone());
            Ok(())
        };
        update_object(path, |settings| {
            update(settings)?;
            validate_settings(&Value::Object(settings.clone()), path)
        })?;
        if !self.value.is_object() {
            self.value = Value::Object(Map::new());
        }
        update(self.value.as_object_mut().expect("settings object"))?;
        if let Some((_, layer)) = self.resource_layers.first_mut() {
            update(layer.as_object_mut().expect("settings layer"))?;
        }
        self.refresh_effective();
        Ok(())
    }

    pub fn remove_user_member(&mut self, key: &str, member: &str) -> Result<()> {
        let path = agent_dir().join("settings.json");
        update_object(&path, |settings| {
            if let Some(object) = settings.get_mut(key).and_then(Value::as_object_mut) {
                object.remove(member);
                if object.is_empty() {
                    settings.remove(key);
                }
            }
            validate_settings(&Value::Object(settings.clone()), &path)
        })?;
        if let Some((_, user)) = self.resource_layers.first_mut()
            && let Some(settings) = user.as_object_mut()
            && let Some(object) = settings.get_mut(key).and_then(Value::as_object_mut)
        {
            object.remove(member);
            if object.is_empty() {
                settings.remove(key);
            }
        }
        let inherited = self
            .resource_layers
            .iter()
            .skip(1)
            .rev()
            .find_map(|(_, layer)| layer.get(key)?.get(member).cloned());
        if let Some(object) = self.value.get_mut(key).and_then(Value::as_object_mut) {
            if let Some(value) = inherited {
                object.insert(member.into(), value);
            } else {
                object.remove(member);
            }
        }
        if self
            .value
            .get(key)
            .and_then(Value::as_object)
            .is_some_and(Map::is_empty)
            && let Some(settings) = self.value.as_object_mut()
        {
            settings.remove(key);
        }
        Ok(())
    }
    pub fn set_user(&mut self, key: &str, value: Value) -> Result<()> {
        self.set_user_at(&agent_dir().join("settings.json"), key, value)
    }
    fn set_user_at(&mut self, path: &Path, key: &str, value: Value) -> Result<()> {
        std::fs::create_dir_all(path.parent().expect("agent directory"))?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))?;
        lock.lock()?;
        let mut settings = read_object(path)?;
        settings
            .as_object_mut()
            .expect("validated settings object")
            .insert(key.into(), value.clone());
        validate_settings(&settings, path)?;
        write_json(path, &settings)?;
        if !self.value.is_object() {
            self.value = Value::Object(Map::new());
        }
        self.value
            .as_object_mut()
            .expect("settings object")
            .insert(key.into(), value.clone());
        if let Some((_, layer)) = self.resource_layers.first_mut() {
            layer
                .as_object_mut()
                .expect("settings layer")
                .insert(key.into(), value);
        }
        self.refresh_effective();
        Ok(())
    }
}

/// Validate implemented settings while retaining extension-owned and future keys.
pub fn validate_settings(value: &Value, path: &Path) -> Result<()> {
    static SCHEMA: std::sync::LazyLock<jsonschema::Validator> = std::sync::LazyLock::new(|| {
        let schema: Value = serde_json::from_str(include_str!("../assets/settings.schema.json"))
            .expect("bundled settings schema");
        jsonschema::validator_for(&schema).expect("valid settings schema")
    });
    if let Some(error) = SCHEMA.iter_errors(value).next() {
        return Err(Error::Config(format!(
            "{}{}: {}",
            path.display(),
            error.instance_path,
            error
        )));
    }
    Ok(())
}

pub fn project_trust(workspace: &Path, explicit: Option<bool>) -> Result<Option<bool>> {
    if explicit.is_some() {
        return Ok(explicit);
    }
    let workspace = workspace.canonicalize()?;
    let decisions = read_object(&agent_dir().join("trust.json"))?;
    for directory in workspace.ancestors() {
        if let Some(decision) = decisions
            .get(directory.to_string_lossy().as_ref())
            .and_then(Value::as_bool)
        {
            return Ok(Some(decision));
        }
    }
    let settings = read_object(&agent_dir().join("settings.json"))?;
    match settings["defaultProjectTrust"].as_str() {
        Some("always") => Ok(Some(true)),
        Some("never") => Ok(Some(false)),
        _ if ![
            "settings.json",
            "mcp.json",
            "extensions",
            "skills",
            "prompts",
            "themes",
            "SYSTEM.md",
            "APPEND_SYSTEM.md",
        ]
        .iter()
        .any(|name| workspace.join(".pi").join(name).exists()) =>
        {
            Ok(Some(true))
        }
        _ => Ok(None),
    }
}

pub fn save_trust(workspace: &Path, decision: bool) -> Result<()> {
    let path = agent_dir().join("trust.json");
    std::fs::create_dir_all(agent_dir())?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))?;
    lock.lock()?;
    let mut value = read_object(&path)?;
    value.as_object_mut().expect("object").insert(
        workspace.canonicalize()?.to_string_lossy().into_owned(),
        Value::Bool(decision),
    );
    write_json(&path, &value)
}

pub fn update_object(
    path: &Path,
    update: impl FnOnce(&mut Map<String, Value>) -> Result<()>,
) -> Result<()> {
    let shared = if path.file_name().is_some_and(|name| name == "auth.json") {
        Some(super::auth_lock::acquire(path)?)
    } else {
        None
    };
    update_object_locked(path, shared.as_ref(), update)
}

pub(super) fn update_object_locked(
    path: &Path,
    shared: Option<&super::auth_lock::Guard>,
    update: impl FnOnce(&mut Map<String, Value>) -> Result<()>,
) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))?;
    lock.lock()?;
    let mut value = read_object_unlocked(path)?;
    update(value.as_object_mut().expect("object"))?;
    if let Some(guard) = shared {
        guard.check()?;
    }
    write_json(path, &value)
}

pub fn read_object(path: &Path) -> Result<Value> {
    if path.file_name().is_none_or(|name| name != "auth.json") {
        return read_object_unlocked(path);
    }
    match super::auth_lock::acquire(path) {
        Ok(guard) => {
            let value = read_object_unlocked(path)?;
            guard.check()?;
            Ok(value)
        }
        Err(error) => read_readonly_auth(path, error),
    }
}

pub(super) async fn read_auth_object(path: &Path) -> Result<Value> {
    match super::auth_lock::acquire_async(path).await {
        Ok(guard) => {
            let value = read_object_unlocked(path)?;
            guard.check()?;
            Ok(value)
        }
        Err(error) => read_readonly_auth(path, error),
    }
}

fn same_auth_metadata(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    if a.len() != b.len() || a.modified().ok() != b.modified().ok() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev()
            && a.ino() == b.ino()
            && a.ctime() == b.ctime()
            && a.ctime_nsec() == b.ctime_nsec()
    }
    #[cfg(not(unix))]
    {
        a.created().ok() == b.created().ok()
    }
}

// If lock creation is denied, use checked snapshots. Any observed lock or
// concurrent change fails closed; do not infer access from Unix mode bits.
pub(super) fn read_readonly_auth(path: &Path, error: Error) -> Result<Value> {
    let denied = matches!(&error, Error::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied || e.raw_os_error() == Some(libc::EROFS));
    if !denied {
        return Err(error);
    }
    let mut lock = path.as_os_str().to_os_string();
    lock.push(".lock");
    let lock = std::path::PathBuf::from(lock);
    let unlocked = || match std::fs::symlink_metadata(&lock) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(Error::Config(
            "auth.json is locked; cannot read a read-only store safely".into(),
        )),
        Err(e) => Err(Error::Io(e)),
    };
    let snapshot = || -> Result<Option<(Vec<u8>, std::fs::Metadata)>> {
        match std::fs::File::open(path) {
            Ok(mut file) => {
                use std::io::Read;
                let before = file.metadata()?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                let after = file.metadata()?;
                if !same_auth_metadata(&before, &after) {
                    return Err(Error::Config("auth.json changed while reading".into()));
                }
                Ok(Some((bytes, after)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    };
    unlocked()?;
    let first = snapshot()?;
    unlocked()?;
    let second = snapshot()?;
    unlocked()?;
    let same = match (&first, &second) {
        (None, None) => true,
        (Some((a, ma)), Some((b, mb))) => a == b && same_auth_metadata(ma, mb),
        _ => false,
    };
    if !same {
        return Err(Error::Config("auth.json changed while reading".into()));
    }
    // A writer could finish and remove its lock after the second read; compare
    // the path again after the final lock check before accepting the snapshot.
    let current = match std::fs::metadata(path) {
        Ok(value) => Some(value),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    if !match (&second, current.as_ref()) {
        (None, None) => true,
        (Some((_, before)), Some(after)) => same_auth_metadata(before, after),
        _ => false,
    } {
        return Err(Error::Config("auth.json changed while reading".into()));
    }
    match second {
        None => Ok(Value::Object(Map::new())),
        Some((bytes, _)) => {
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
            if !value.is_object() {
                return Err(Error::Config(format!(
                    "{} must contain an object",
                    path.display()
                )));
            }
            Ok(value)
        }
    }
}

pub(super) fn read_object_unlocked(path: &Path) -> Result<Value> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
            if !value.is_object() {
                return Err(Error::Config(format!(
                    "{} must contain an object",
                    path.display()
                )));
            }
            Ok(value)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Value::Object(Map::new())),
        Err(error) => Err(error.into()),
    }
}

pub fn write_json(path: &Path, value: &Value) -> Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    writeln!(file)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn merge(base: &mut Value, overlay: Value) {
    if let (Value::Object(base), Value::Object(overlay)) = (base, overlay) {
        for (key, value) in overlay {
            if let Some(prior) = base.get_mut(&key) {
                if key == "defaultTools"
                    && let (Some(base_tools), Some(project_tools)) =
                        (prior.as_array_mut(), value.as_array())
                    && project_tools.iter().all(|entry| {
                        entry
                            .as_str()
                            .is_some_and(|entry| entry.starts_with(['+', '-']))
                    })
                {
                    base_tools.extend(project_tools.iter().cloned());
                    continue;
                }
                if prior.is_object() && value.is_object() {
                    merge(prior, value);
                    continue;
                }
                if matches!(
                    key.as_str(),
                    "extensions" | "skills" | "prompts" | "themes" | "packages"
                ) && prior.is_array()
                    && value.is_array()
                {
                    let list = prior.as_array_mut().expect("array");
                    for entry in value.as_array().expect("array") {
                        if !list.contains(entry) {
                            list.push(entry.clone());
                        }
                    }
                    continue;
                }
            }
            base.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn auth_read_waits_asynchronously_until_lock_is_released() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, r#"{"provider":{"type":"api_key","key":"fixture"}}"#).unwrap();
        let guard = super::super::auth_lock::acquire(&path).unwrap();
        let reader_path = path.clone();
        let reader = tokio::spawn(async move { super::read_auth_object(&reader_path).await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!reader.is_finished());
        drop(guard);
        assert_eq!(reader.await.unwrap().unwrap()["provider"]["key"], "fixture");
    }

    #[cfg(unix)]
    #[test]
    fn denied_lock_allows_checked_snapshot_with_owner_writable_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, r#"{"provider":{"type":"api_key","key":"fixture"}}"#).unwrap();
        let denied = pi_rs::Error::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(
            super::read_readonly_auth(&path, denied).unwrap()["provider"]["key"],
            "fixture"
        );
    }
    #[cfg(unix)]
    #[test]
    fn readonly_auth_snapshot_rejects_active_pi_lock() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, r#"{"provider":{"type":"api_key","key":"fixture"}}"#).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let denied =
            || pi_rs::Error::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(
            super::read_readonly_auth(&path, denied()).unwrap()["provider"]["key"],
            "fixture"
        );
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::create_dir(dir.path().join("auth.json.lock")).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        assert!(super::read_readonly_auth(&path, denied()).is_err());
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    #[test]
    fn global_updates_preserve_project_precedence_in_memory_and_on_disk() {
        use super::*;
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        let user = json!({"theme":"dark", "compaction":{"enabled":true,"reserveTokens":100}, "defaultModel":"old"});
        let project = json!({"theme":"project", "compaction":{"enabled":false}, "defaultModel":"project-model"});
        write_json(&path, &user).unwrap();
        let mut effective = user.clone();
        merge(&mut effective, project.clone());
        let mut settings = Settings {
            value: effective,
            resource_layers: vec![
                (dir.path().to_owned(), user),
                (dir.path().join("project"), project),
            ],
            ..Settings::default()
        };

        settings
            .set_user_at(&path, "theme", json!("light"))
            .unwrap();
        assert_eq!(settings.string("theme"), Some("project"));
        settings
            .set_user_member_at(&path, "compaction", "enabled", json!(true))
            .unwrap();
        assert!(!settings.auto_compaction_enabled());
        settings
            .set_user_member_at(&path, "compaction", "reserveTokens", json!(200))
            .unwrap();
        settings
            .set_user_values_at(
                &path,
                &[
                    ("defaultModel".into(), json!("new")),
                    ("defaultProvider".into(), json!("ollama")),
                ],
            )
            .unwrap();

        assert_eq!(settings.string("theme"), Some("project"));
        assert!(!settings.auto_compaction_enabled());
        assert_eq!(settings.value()["compaction"]["reserveTokens"], 200);
        assert_eq!(settings.string("defaultModel"), Some("project-model"));
        assert_eq!(settings.string("defaultProvider"), Some("ollama"));
        let saved = read_object(&path).unwrap();
        assert_eq!(saved["theme"], "light");
        assert_eq!(
            saved["compaction"],
            json!({"enabled":true,"reserveTokens":200})
        );
        assert_eq!(saved["defaultModel"], "new");
        let mut reloaded = saved;
        merge(&mut reloaded, settings.resource_layers[1].1.clone());
        assert_eq!(settings.value(), &reloaded);
    }

    #[test]
    fn default_tools_project_modifiers_extend_user_selection() {
        use super::*;
        use serde_json::json;
        let mut settings = json!({"defaultTools": ["read", "bash", "+codemode"]});
        merge(
            &mut settings,
            json!({"defaultTools": ["-codemode", "+grep"]}),
        );
        assert_eq!(
            settings["defaultTools"],
            json!(["read", "bash", "+codemode", "-codemode", "+grep"])
        );
        use clap::Parser;
        let mut args = super::super::Cli::try_parse_from(["pi"]).unwrap();
        args.settings = Settings {
            value: settings.clone(),
            resource_layers: vec![],
            ..Settings::default()
        };
        let selection = super::super::tool_selection::Selection::new(&args).unwrap();
        assert!(selection.includes("read"));
        assert!(selection.includes("bash"));
        assert!(selection.includes("grep"));
        assert!(!selection.includes("codemode"));
        assert!(!selection.includes("write"));
        merge(&mut settings, json!({"defaultTools": ["write"]}));
        assert_eq!(settings["defaultTools"], json!(["write"]));
        merge(&mut settings, json!({"defaultTools": []}));
        assert_eq!(settings["defaultTools"], json!(["write"]));
    }
    #[test]
    fn http_idle_timeout_has_pi_defaults_and_supports_disabling() {
        let mut settings = super::Settings::default();
        assert_eq!(settings.http_idle_timeout_ms(), 300_000);
        settings.value = serde_json::json!({"httpIdleTimeoutMs": 42.9});
        assert_eq!(settings.http_idle_timeout_ms(), 42);
        settings.value = serde_json::json!({"httpIdleTimeoutMs": "disabled"});
        assert!(settings.http_idle_timeout().is_none());
        settings.value = serde_json::json!({"httpIdleTimeoutMs": 0});
        assert!(settings.http_idle_timeout().is_none());
        assert!(
            super::validate_settings(
                &serde_json::json!({"httpIdleTimeoutMs": -1}),
                std::path::Path::new("settings.json")
            )
            .is_err()
        );
    }
    use super::*;
    use serde_json::json;
    #[test]
    fn resources_keep_scope_paths_filters_and_project_precedence() {
        use super::super::resource_paths::{self, Kind};
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let user = directory.path().join("agent");
        let workspace = directory.path().join("work");
        let project = workspace.join(".pi");
        for base in [&user, &project] {
            std::fs::create_dir_all(base.join("templates")).unwrap();
            std::fs::create_dir_all(base.join("prompts")).unwrap();
            std::fs::write(base.join("templates/item.md"), "template").unwrap();
            std::fs::write(base.join("templates/skip.md"), "excluded").unwrap();
            std::fs::write(base.join("prompts/common.md"), "auto").unwrap();
        }
        let mut args = super::super::Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = workspace.clone();
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![
                (
                    user.clone(),
                    json!({"prompts":["templates", "!**/skip.md"], "extensions":["-builtin:mcp"]}),
                ),
                (
                    project.clone(),
                    json!({"prompts":["templates", "!**/skip.md"], "extensions":["+builtin:mcp"]}),
                ),
            ],
            ..Settings::default()
        };
        let mut warnings = Vec::new();
        let paths = resource_paths::resolve(&args, Kind::Prompt, &[], &[], &mut warnings).unwrap();
        assert_eq!(
            paths[0],
            project.join("templates/item.md").canonicalize().unwrap()
        );
        assert!(paths.contains(&user.join("templates/item.md").canonicalize().unwrap()));
        assert!(paths.iter().all(|path| !path.ends_with("skip.md")));
        #[cfg(unix)]
        {
            let alias = directory.path().join("work-alias");
            std::os::unix::fs::symlink(&workspace, &alias).unwrap();
            args.workspace = alias;
            let aliased =
                resource_paths::resolve(&args, Kind::Prompt, &[], &[], &mut warnings).unwrap();
            assert!(aliased.iter().all(|path| !path.ends_with("skip.md")));
            args.workspace = workspace.clone();
        }
        assert!(resource_paths::builtin_enabled(&args, "builtin:mcp").unwrap());
        args.no_extensions = true;
        assert!(!resource_paths::builtin_enabled(&args, "builtin:mcp").unwrap());
        args.extensions.push("builtin:mcp".into());
        assert!(resource_paths::builtin_enabled(&args, "builtin:mcp").unwrap());
        args.no_prompt_templates = true;
        let explicit = user.join("templates/skip.md");
        assert_eq!(
            resource_paths::resolve(
                &args,
                Kind::Prompt,
                &[],
                std::slice::from_ref(&explicit),
                &mut warnings
            )
            .unwrap(),
            vec![explicit]
        );
        assert!(warnings.is_empty());
        args.settings.resource_layers[0].1["prompts"] = json!("invalid array");
        let error = args
            .settings
            .resource_layers("prompts")
            .unwrap_err()
            .to_string();
        assert!(error.contains("settings.json") && error.contains("prompts"));
    }

    #[test]
    fn automatic_prompts_are_visible_top_level_files_respecting_local_ignores() {
        use super::super::resource_paths::{self, Kind};
        use clap::Parser;
        let dir = tempfile::tempdir().unwrap();
        let user = dir.path().join("agent");
        let workspace = dir.path().join("work");
        let project = workspace.join(".pi");
        for base in [&user, &project] {
            let prompts = base.join("prompts");
            std::fs::create_dir_all(prompts.join("nested")).unwrap();
            for name in ["visible.md", ".hidden.md", "ignored.md", "nested/child.md"] {
                std::fs::write(prompts.join(name), "prompt").unwrap();
            }
            std::fs::write(prompts.join(".fdignore"), "ignored.md\n").unwrap();
        }
        let mut args = super::super::Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = workspace;
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![(user.clone(), json!({})), (project.clone(), json!({}))],
            ..Settings::default()
        };
        let mut warnings = Vec::new();
        let paths = resource_paths::resolve(&args, Kind::Prompt, &[], &[], &mut warnings).unwrap();
        assert_eq!(
            paths,
            vec![
                project.join("prompts/visible.md").canonicalize().unwrap(),
                user.join("prompts/visible.md").canonicalize().unwrap()
            ]
        );
        // Explicit files remain loadable even when automatic discovery skips them.
        let explicit = project.join("prompts/nested/child.md");
        let paths = resource_paths::resolve(
            &args,
            Kind::Prompt,
            &[],
            std::slice::from_ref(&explicit),
            &mut warnings,
        )
        .unwrap();
        assert!(paths.contains(&explicit));
        assert!(warnings.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn configured_and_packaged_prompts_deduplicate_before_suffix_filtering() {
        use super::super::resource_paths::{self, Kind};
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("work");
        let project = workspace.join(".pi");
        std::fs::create_dir_all(&project).unwrap();
        let txt = project.join("content.txt");
        let md = project.join("alias.md");
        std::fs::write(&txt, "fixture").unwrap();
        symlink("content.txt", &md).unwrap();
        let mut args = super::super::Cli::try_parse_from(["pi", "--no-skills"]).unwrap();
        args.workspace = workspace;
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![(
                project.clone(),
                json!({"prompts": ["content.txt", "alias.md"]}),
            )],
            ..Settings::default()
        };
        assert!(
            super::super::resources::Resources::load(&args)
                .unwrap()
                .prompts
                .is_empty()
        );
        args.settings.resource_layers[0].1 = json!({"prompts": ["alias.md", "content.txt"]});
        assert_eq!(
            super::super::resources::Resources::load(&args)
                .unwrap()
                .prompts["alias"]
                .body,
            "fixture"
        );
        args.settings.resource_layers[0].1 = json!({});
        for (paths, expected) in [
            (vec![txt.clone(), md.clone()], txt.clone()),
            (vec![md.clone(), txt.clone()], md.clone()),
        ] {
            let mut warnings = Vec::new();
            let resolved =
                resource_paths::resolve(&args, Kind::Prompt, &paths, &[], &mut warnings).unwrap();
            assert_eq!(resolved, vec![expected]);
        }
    }

    #[cfg(unix)]
    #[test]
    fn prompt_symlink_names_and_overrides_use_discovered_paths() {
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("work");
        let project = workspace.join(".pi");
        std::fs::create_dir_all(project.join("prompts")).unwrap();
        std::fs::write(dir.path().join("template.md"), "Review $1").unwrap();
        let link = project.join("prompts/review.md");
        symlink(dir.path().join("template.md"), &link).unwrap();
        let mut args = super::super::Cli::try_parse_from(["pi", "--no-skills"]).unwrap();
        args.workspace = workspace;
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![
                (dir.path().join("agent"), json!({})),
                (project.clone(), json!({})),
            ],
            ..Settings::default()
        };
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert_eq!(
            resources.prompts["review"].path,
            project.canonicalize().unwrap().join("prompts/review.md")
        );
        assert_eq!(
            resources.expand("/review code").as_deref(),
            Some("Review code")
        );
        assert!(!resources.prompts.contains_key("template"));
        let mut warnings = Vec::new();
        let paths = super::super::resource_paths::resolve(
            &args,
            super::super::resource_paths::Kind::Prompt,
            &[],
            &[link.clone(), dir.path().join("template.md")],
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            paths,
            vec![project.canonicalize().unwrap().join("prompts/review.md")]
        );
        args.settings.resource_layers[1].1 = json!({"prompts": ["-prompts/review.md"]});
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert!(!resources.prompts.contains_key("review"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_prompt_directories_apply_recursive_ignore_and_preserve_symlinks() {
        use super::super::resource_paths::{self, Kind};
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("work/.pi");
        let root = project.join("custom");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::create_dir_all(root.join("target")).unwrap();
        std::fs::create_dir_all(root.join("node_modules")).unwrap();
        for file in [
            "visible.md",
            ".hidden.md",
            "ignored.md",
            "nested/child.md",
            "nested/skip.md",
            "target/build.md",
            "node_modules/skip.md",
        ] {
            std::fs::write(root.join(file), "prompt").unwrap();
        }
        std::fs::write(root.join(".gitignore"), "ignored.md\n").unwrap();
        std::fs::write(root.join("nested/.fdignore"), "skip.md\n").unwrap();
        symlink(dir.path().join("target.md"), root.join("alias.md")).unwrap();
        std::fs::write(dir.path().join("target.md"), "target").unwrap();
        let mut args = super::super::Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = dir.path().join("work");
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![
                (dir.path().join("agent"), json!({})),
                (
                    project,
                    json!({"prompts": ["custom", "-custom/visible.md"]}),
                ),
            ],
            ..Settings::default()
        };
        let mut warnings = Vec::new();
        let paths = resource_paths::resolve(&args, Kind::Prompt, &[], &[], &mut warnings).unwrap();
        let root = root.canonicalize().unwrap();
        assert_eq!(
            paths,
            vec![
                root.join("alias.md"),
                root.join("nested/child.md"),
                root.join("target/build.md")
            ]
        );
        assert!(warnings.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn prompt_directory_aliases_and_configured_alias_deduplication() {
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("work");
        let project = workspace.join(".pi");
        let custom = project.join("custom");
        std::fs::create_dir_all(&custom).unwrap();
        let target = dir.path().join("template.md");
        std::fs::write(&target, "Shared $1").unwrap();
        symlink(&target, custom.join("audit.md")).unwrap();
        symlink(&target, custom.join("review.md")).unwrap();
        let mut args =
            super::super::Cli::try_parse_from(["pi", "--no-skills", "--no-prompt-templates"])
                .unwrap();
        args.workspace = workspace;
        args.trusted = true;
        args.prompt_template.push(custom.clone());
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![
                (dir.path().join("agent"), json!({})),
                (project, json!({"prompts": ["custom", "-custom/audit.md"]})),
            ],
            ..Settings::default()
        };
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert_eq!(
            resources.expand("/audit one").as_deref(),
            Some("Shared one")
        );
        assert_eq!(
            resources.expand("/review two").as_deref(),
            Some("Shared two")
        );
        args.prompt_template = vec![custom.join("audit.md"), custom.join("review.md")];
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert!(resources.prompts.contains_key("audit"));
        assert!(!resources.prompts.contains_key("review"));
        args.prompt_template.clear();
        args.no_prompt_templates = false;
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert!(!resources.prompts.contains_key("audit"));
        assert!(!resources.prompts.contains_key("review"));
        args.settings.resource_layers[1].1 = json!({"prompts": ["custom"]});
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert!(resources.prompts.contains_key("audit"));
        assert!(!resources.prompts.contains_key("review"));
    }

    #[cfg(unix)]
    #[test]
    fn automatic_prompt_aliases_deduplicate_before_disabled_filtering() {
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("work/.pi");
        let prompts = project.join("prompts");
        std::fs::create_dir_all(&prompts).unwrap();
        let target = dir.path().join("template.md");
        std::fs::write(&target, "Shared").unwrap();
        symlink(&target, prompts.join("audit.md")).unwrap();
        symlink(&target, prompts.join("review.md")).unwrap();
        let mut args = super::super::Cli::try_parse_from(["pi", "--no-skills"]).unwrap();
        args.workspace = dir.path().join("work");
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![(dir.path().join("agent"), json!({})), (project, json!({}))],
            ..Settings::default()
        };
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert!(resources.prompts.contains_key("audit"));
        assert!(!resources.prompts.contains_key("review"));
        args.settings.resource_layers[1].1 = json!({"prompts": ["-prompts/audit.md"]});
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert!(!resources.prompts.contains_key("audit"));
        assert!(!resources.prompts.contains_key("review"));
    }

    #[cfg(unix)]
    #[test]
    fn discovered_prompts_precede_explicit_prompt_files() {
        use clap::Parser;
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("work");
        let project = workspace.join(".pi");
        let prompts = project.join("prompts");
        let extra = workspace.join("extra");
        std::fs::create_dir_all(&prompts).unwrap();
        std::fs::create_dir_all(&extra).unwrap();
        std::fs::write(prompts.join("review.md"), "discovered $1").unwrap();
        std::fs::write(extra.join("review.md"), "explicit $1").unwrap();
        symlink(prompts.join("review.md"), extra.join("audit.md")).unwrap();
        let mut args = super::super::Cli::try_parse_from(["pi", "--no-skills"]).unwrap();
        args.workspace = workspace;
        args.trusted = true;
        args.settings = Settings {
            value: json!({}),
            resource_layers: vec![(dir.path().join("agent"), json!({})), (project, json!({}))],
            ..Settings::default()
        };
        args.prompt_template = vec![extra.join("review.md"), extra.join("audit.md")];
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert_eq!(
            resources.expand("/review code").as_deref(),
            Some("discovered code")
        );
        assert!(!resources.prompts.contains_key("audit"));
        assert!(
            resources
                .warnings
                .iter()
                .any(|warning| warning.contains("Duplicate prompt review"))
        );
        args.no_prompt_templates = true;
        let resources = super::super::resources::Resources::load(&args).unwrap();
        assert_eq!(
            resources.expand("/review code").as_deref(),
            Some("explicit code")
        );
        assert_eq!(
            resources.expand("/audit code").as_deref(),
            Some("discovered code")
        );
    }

    #[test]
    fn project_settings_merge_nested_objects_and_resource_lists() {
        let mut base = json!({"theme":"dark","compaction":{"enabled":true,"reserveTokens":100},"skills":["first"]});
        merge(
            &mut base,
            json!({"theme":"light","compaction":{"reserveTokens":200},"skills":["first","second"]}),
        );
        assert_eq!(base["theme"], "light");
        assert_eq!(
            base["compaction"],
            json!({"enabled":true,"reserveTokens":200})
        );
        assert_eq!(base["skills"], json!(["first", "second"]));
    }
    #[test]
    fn compaction_tokens_resolve_each_model_override_independently() {
        let settings = Settings {
            value: json!({"compaction":{"reserveTokens":200,"keepRecentTokens":300,"modelOverrides":{
                "ollama/first":{"reserveTokens":50},"ollama/second":{"keepRecentTokens":0}
            }}}),
            ..Settings::default()
        };
        assert_eq!(
            settings.compaction_tokens("ollama", "first"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(50),
                keep_recent_tokens: Some(300)
            }
        );
        assert_eq!(
            settings.compaction_tokens("ollama", "second"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(200),
                keep_recent_tokens: Some(0)
            }
        );
        assert_eq!(
            settings.compaction_tokens("ollama", "other"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(200),
                keep_recent_tokens: Some(300)
            }
        );
        assert_eq!(
            Settings::default().compaction_tokens("ollama", "first"),
            pi_rs::CompactionTokens::pi_defaults()
        );
        validate_settings(settings.value(), Path::new("settings.json")).unwrap();
    }

    #[test]
    fn missing_compaction_token_fields_use_pi_defaults_without_overwriting_zero() {
        let settings = Settings {
            value: json!({"compaction":{"reserveTokens":0,"modelOverrides":{"ollama/test":{"keepRecentTokens":0}}}}),
            ..Settings::default()
        };
        assert_eq!(
            settings.compaction_tokens("ollama", "test"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(0),
                keep_recent_tokens: Some(0)
            }
        );
        assert_eq!(
            settings.compaction_tokens("ollama", "other"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(0),
                keep_recent_tokens: Some(20_000)
            }
        );
        let only_keep = Settings {
            value: json!({"compaction":{"keepRecentTokens":17}}),
            ..Settings::default()
        };
        assert_eq!(
            only_keep.compaction_tokens("ollama", "test"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(16_384),
                keep_recent_tokens: Some(17)
            }
        );
    }

    #[test]
    fn compaction_tokens_accept_integral_json_numbers() {
        let settings = Settings {
            value: json!({"compaction":{"reserveTokens":0.0,"keepRecentTokens":42.0}}),
            ..Settings::default()
        };
        validate_settings(settings.value(), Path::new("settings.json")).unwrap();
        assert_eq!(
            settings.compaction_tokens("ollama", "test"),
            pi_rs::CompactionTokens {
                reserve_tokens: Some(0),
                keep_recent_tokens: Some(42)
            }
        );
    }

    #[test]
    fn retry_settings_preserve_defaults_and_accept_zero_delay() {
        let defaults = Settings::default().retry_settings();
        assert!(defaults.enabled);
        assert_eq!(defaults.max_retries, 3);
        assert_eq!(defaults.base_delay_ms, 2000);
        assert_eq!(defaults.max_agent_delay_ms, 60000);
        let settings = Settings {
            value: json!({"retry":{"enabled":false,"maxRetries":0,"baseDelayMs":0.0,"maxAgentDelayMs":42.0,"provider":{"custom":true}}}),
            ..Settings::default()
        };
        validate_settings(settings.value(), Path::new("settings.json")).unwrap();
        let retry = settings.retry_settings();
        assert!(!retry.enabled);
        assert_eq!(retry.max_retries, 0);
        assert_eq!(retry.base_delay_ms, 0);
        assert_eq!(retry.max_agent_delay_ms, 42);
    }

    #[test]
    fn model_thinking_defaults_prefer_the_provider_key_then_global_default() {
        let settings = Settings {
            value: json!({"defaultThinkingLevel":"low", "modelThinkingLevels":{"ollama/a":"high", "a":"medium", "b":"max"}}),
            ..Settings::default()
        };
        assert_eq!(settings.thinking_for_model("ollama", "a"), Some("high"));
        assert_eq!(settings.thinking_for_model("ollama", "b"), Some("max"));
        assert_eq!(settings.thinking_for_model("ollama", "c"), Some("low"));
        validate_settings(settings.value(), Path::new("settings.json")).unwrap();
    }

    #[test]
    fn settings_validation_identifies_bad_values_and_keeps_extension_keys() {
        let path = Path::new("settings.json");
        validate_settings(&json!({"codemode":{"mode":"only","inlineBudget":0},"myExtension":{"option":true},"defaultThinkingLevel":null}), path).unwrap();
        for invalid in [
            json!({"codemode":{"mode":"invalid"}}),
            json!({"codemode":{"inlineBudget":-1}}),
            json!({"hideThinkingBlock":"false"}),
            json!({"compaction": false}),
            json!({"retry":{"enabled":"false"}}),
            json!({"retry":{"maxRetries":-1}}),
            json!({"retry":{"baseDelayMs":1.5}}),
            json!({"retry":{"maxAgentDelayMs":9007199254740992_u64}}),
            json!({"compaction":{"reserveTokens":-1}}),
            json!({"compaction":{"keepRecentTokens":1.5}}),
            json!({"compaction":{"reserveTokens":9007199254740992_u64}}),
            json!({"compaction":{"modelOverrides":{"ollama/test":false}}}),
            json!({"compaction":{"modelOverrides":{"ollama/test":{"reserveTokens":"3"}}}}),
            json!({"compaction": {"enabled": "false"}}),
            json!({"packages":[{"extensions":[]}]}),
            json!({"modelThinkingLevels":{"model":"unknown"}}),
            json!({"skills":[false]}),
        ] {
            let error = validate_settings(&invalid, path).unwrap_err().to_string();
            assert!(error.contains("settings.json/"), "{error}");
        }
    }

    #[test]
    fn atomic_setting_write_roundtrips_without_temporary_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config/settings.json");
        write_json(&path, &json!({"custom":{"keep":true}})).unwrap();
        assert_eq!(read_object(&path).unwrap()["custom"]["keep"], true);
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
    }
}
