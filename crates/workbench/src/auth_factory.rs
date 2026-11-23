//! Read-only credential loading for a native host that owns endpoint policy.
use std::path::{Component, PathBuf};
use std::sync::LazyLock;

use pi_rs::{Error, HostAuthBuilder, HostAuthSnapshot, HostCredential, HostCredentialKind, Result};
use regex::Regex;
use serde_json::Value;

pub(crate) const MAX_API_KEY_BYTES: usize = 4096;

/// The exact provider and endpoint the host authorizes to use a saved key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthRoute {
    pub provider: String,
    pub endpoint: String,
}

/// Access to one explicitly granted Pi-format credential file and a set of
/// routes. The file path is absolute and symlinks are followed.
#[derive(Clone, Debug)]
pub struct AuthFileReadGrants {
    pub file: PathBuf,
    pub routes: Vec<AuthRoute>,
}

impl AuthFileReadGrants {
    /// Load a fresh snapshot from a bounded UTF-8 JSON file. A missing file,
    /// absent key, unsupported credential, or mismatched saved endpoint leaves
    /// the route unconfigured. Endpointless keys are bound only to the routes
    /// supplied here. This performs no network I/O, shell execution, OAuth,
    /// or process-environment lookup. Run the file read off the UI thread.
    pub fn load(&self) -> Result<HostAuthSnapshot> {
        self.load_with_variables(|_, _| None)
    }

    /// Load saved keys with an explicit source for variables absent from a saved
    /// entry. The host can grant variable names separately for each route.
    /// Variables are resolved while taking the snapshot.
    pub fn load_with_variables(
        &self,
        mut lookup: impl FnMut(&AuthRoute, &str) -> Option<String>,
    ) -> Result<HostAuthSnapshot> {
        if !self.file.is_absolute()
            || self
                .file
                .components()
                .any(|part| part == Component::ParentDir)
        {
            return Err(Error::Config(
                "auth file grant must be absolute without parent components".into(),
            ));
        }
        if self
            .routes
            .iter()
            .any(|route| route.provider.trim().is_empty() || route.endpoint.trim().is_empty())
        {
            return Err(Error::Config(
                "auth route provider or endpoint is empty".into(),
            ));
        }
        let mut snapshot = HostAuthBuilder::new();
        if self.routes.is_empty() {
            return Ok(snapshot.build());
        }
        let Some(text) = crate::prompt_resources::read_optional_resource(&self.file, "auth file")?
        else {
            return Ok(snapshot.build());
        };
        let credentials: serde_json::Map<String, Value> = serde_json::from_str(&text)
            .map_err(|_| Error::Config("invalid auth file: expected a JSON object".into()))?;
        for route in &self.routes {
            if let Some(saved) = credentials.get(&route.provider)
                && let Some(key) = api_key(saved, &route.endpoint, |name| lookup(route, name))?
            {
                snapshot.register(
                    &route.provider,
                    &route.endpoint,
                    HostCredential::new(HostCredentialKind::ApiKey, key)?,
                )?;
            }
        }
        Ok(snapshot.build())
    }
}

fn api_key(
    saved: &Value,
    endpoint: &str,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<Option<String>> {
    if saved["type"] != "api_key" {
        return Ok(None);
    }
    if let Some(value) = saved.get("endpoint")
        && value.as_str() != Some(endpoint)
    {
        return Ok(None);
    }
    let Some(key) = saved.get("key") else {
        return Ok(None);
    };
    let key = key
        .as_str()
        .ok_or_else(|| Error::Config("auth key must be a string".into()))?;
    let resolved = if saved["literal"] == true {
        Some(key.to_owned())
    } else {
        expand_config_value(key, MAX_API_KEY_BYTES, |name| {
            saved["env"]
                .get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .or_else(|| lookup(name))
        })?
    };
    if resolved.as_deref().is_some_and(|key| !valid_api_key(key)) {
        return Err(Error::Config("invalid saved API key".into()));
    }
    Ok(resolved)
}

#[doc(hidden)]
pub fn valid_api_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_API_KEY_BYTES
        && key.bytes().all(|byte| byte.is_ascii_graphic())
}

/// Expand Pi-style non-command configuration values with the caller's lookup.
#[doc(hidden)]
pub fn expand_config_value(
    value: &str,
    max_len: usize,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<Option<String>> {
    if value.starts_with('!') {
        return Ok(None);
    }
    static NAME: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*").expect("environment name"));
    let mut expanded = String::new();
    let mut append = |chunk: &str| -> Result<()> {
        if chunk.len() > max_len - expanded.len() {
            return Err(Error::Config(
                "expanded configuration value exceeds limit".into(),
            ));
        }
        expanded.push_str(chunk);
        Ok(())
    };
    let last_brace = value.rfind('}').unwrap_or_default();
    let mut cursor = 0;
    while let Some(offset) = value[cursor..].find('$') {
        let dollar = cursor + offset;
        append(&value[cursor..dollar])?;
        cursor = dollar + 1;
        let tail = &value[cursor..];
        let name = match tail.as_bytes().first() {
            Some(b'$' | b'!') => {
                append(&tail[..1])?;
                cursor += 1;
                continue;
            }
            Some(b'{') if cursor < last_brace => {
                let close = tail.find('}').expect("closing brace remains");
                cursor += close + 1;
                let name = &tail[1..close];
                if NAME
                    .find(name)
                    .is_none_or(|found| found.end() != name.len())
                {
                    append(&value[dollar..cursor])?;
                    continue;
                }
                name
            }
            _ => {
                let Some(name) = NAME.find(tail) else {
                    append("$")?;
                    continue;
                };
                cursor += name.end();
                name.as_str()
            }
        };
        let Some(replacement) = lookup(name).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        append(&replacement)?;
    }
    append(&value[cursor..])?;
    Ok(Some(expanded))
}
