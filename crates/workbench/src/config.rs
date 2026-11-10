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
}
impl Settings {
    pub fn load(workspace: &Path, trusted: bool) -> Result<Self> {
        let user_path = agent_dir().join("settings.json");
        let mut value = read_object(&user_path)?;
        validate_settings(&value, &user_path)?;
        let mut resource_layers = vec![(agent_dir(), value.clone())];
        if trusted {
            let project_path = workspace.join(".pi/settings.json");
            let mut project = read_object(&project_path)?;
            project
                .as_object_mut()
                .expect("object")
                .remove("defaultProjectTrust");
            validate_settings(&project, &project_path)?;
            resource_layers.push((workspace.join(".pi"), project.clone()));
            merge(&mut value, project);
        }
        Ok(Self {
            value,
            resource_layers,
        })
    }
    pub fn string(&self, key: &str) -> Option<&str> {
        self.value.get(key)?.as_str()
    }
    pub fn thinking_for_model(&self, provider: &str, model: &str) -> Option<&str> {
        let levels = self.value.get("modelThinkingLevels");
        levels
            .and_then(|levels| levels.get(format!("{provider}/{model}")))
            .or_else(|| levels.and_then(|levels| levels.get(model)))
            .and_then(Value::as_str)
            .or_else(|| self.string("defaultThinkingLevel"))
    }
    pub fn boolean(&self, key: &str) -> Option<bool> {
        self.value.get(key)?.as_bool()
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
            enabled: value["enabled"].as_bool().unwrap_or(defaults.enabled),
            max_retries: number("maxRetries", u64::from(defaults.max_retries))
                .min(u64::from(u32::MAX)) as u32,
            base_delay_ms: number("baseDelayMs", defaults.base_delay_ms),
            max_agent_delay_ms: number("maxAgentDelayMs", defaults.max_agent_delay_ms),
        }
    }
    pub fn set_auto_retry_enabled(&mut self, enabled: bool) -> Result<()> {
        let path = agent_dir().join("settings.json");
        let update = |settings: &mut Map<String, Value>| -> Result<()> {
            settings
                .entry("retry")
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
                .ok_or_else(|| Error::Config("retry must be an object".into()))?
                .insert("enabled".into(), Value::Bool(enabled));
            Ok(())
        };
        update_object(&path, |settings| {
            update(settings)?;
            validate_settings(&Value::Object(settings.clone()), &path)
        })?;
        if !self.value.is_object() {
            self.value = serde_json::json!({});
        }
        update(self.value.as_object_mut().expect("settings object"))?;
        if let Some((_, layer)) = self.resource_layers.first_mut() {
            update(layer.as_object_mut().expect("settings layer"))?;
        }
        Ok(())
    }
    pub fn auto_compaction_enabled(&self) -> bool {
        self.value["compaction"]["enabled"]
            .as_bool()
            .unwrap_or(true)
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
        let path = agent_dir().join("settings.json");
        let update = |settings: &mut Map<String, Value>| -> Result<()> {
            let compaction = settings
                .entry("compaction")
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .ok_or_else(|| Error::Config("compaction must be an object".into()))?;
            compaction.insert("enabled".into(), Value::Bool(enabled));
            Ok(())
        };
        update_object(&path, |settings| {
            update(settings)?;
            validate_settings(&Value::Object(settings.clone()), &path)
        })?;
        if !self.value.is_object() {
            self.value = Value::Object(Map::new());
        }
        update(self.value.as_object_mut().expect("settings object"))?;
        if let Some((_, layer)) = self.resource_layers.first_mut() {
            update(layer.as_object_mut().expect("settings layer"))?;
        }
        Ok(())
    }
    pub fn set_user(&mut self, key: &str, value: Value) -> Result<()> {
        let path = agent_dir().join("settings.json");
        std::fs::create_dir_all(path.parent().expect("agent directory"))?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))?;
        lock.lock()?;
        let mut settings = read_object(&path)?;
        settings
            .as_object_mut()
            .expect("validated settings object")
            .insert(key.into(), value.clone());
        validate_settings(&settings, &path)?;
        write_json(&path, &settings)?;
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
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))?;
    lock.lock()?;
    let mut value = read_object(path)?;
    update(value.as_object_mut().expect("object"))?;
    write_json(path, &value)
}

pub fn read_object(path: &Path) -> Result<Value> {
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
            vec![explicit.canonicalize().unwrap()]
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
