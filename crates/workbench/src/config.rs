//! User and trusted-project settings with non-destructive updates.
use pi_rs::{Error, Result};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::{Path, PathBuf};

pub fn agent_dir() -> PathBuf {
    std::env::var_os("PI_RS_HOME")
        .or_else(|| std::env::var_os("PI_CODING_AGENT_DIR"))
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(|home| PathBuf::from(home).join(".pi-rs"))
        })
        .unwrap_or_else(|| std::env::temp_dir().join("pi-rs"))
}

#[derive(Clone, Default)]
pub struct Settings {
    value: Value,
}
impl Settings {
    pub fn load(workspace: &Path, trusted: bool) -> Result<Self> {
        let mut value = read_object(&agent_dir().join("settings.json"))?;
        if trusted {
            let mut project = read_object(&workspace.join(".pi/settings.json"))?;
            project
                .as_object_mut()
                .expect("object")
                .remove("defaultProjectTrust");
            merge(&mut value, project);
        }
        Ok(Self { value })
    }
    pub fn string(&self, key: &str) -> Option<&str> {
        self.value.get(key)?.as_str()
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
    pub fn value(&self) -> &Value {
        &self.value
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
        write_json(&path, &settings)?;
        if !self.value.is_object() {
            self.value = Value::Object(Map::new());
        }
        self.value
            .as_object_mut()
            .expect("settings object")
            .insert(key.into(), value);
        Ok(())
    }
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
