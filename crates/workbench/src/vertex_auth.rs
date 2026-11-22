//! Application Default Credentials for the official Google Vertex AI endpoint.
use pi_rs::{Error, Result};
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::time::Duration;

type Environment = Map<String, Value>;

#[derive(Clone)]
pub(super) struct Adc {
    pub endpoint: String,
    credentials_path: Option<PathBuf>,
}

impl Adc {
    pub fn configured(env: Option<&Environment>) -> Option<Self> {
        let mut read = |name: &str| std::env::var(name).ok();
        Self::configured_with(env, &mut read, |configured| {
            configured
                .map(PathBuf::from)
                .or_else(default_credentials_path)
                .is_some_and(|path| path.is_file())
        })
    }

    fn configured_with(
        env: Option<&Environment>,
        read: &mut impl FnMut(&str) -> Option<String>,
        file_exists: impl FnOnce(Option<&str>) -> bool,
    ) -> Option<Self> {
        let mut get = |name: &str| {
            env.and_then(|vars| vars.get(name))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| read(name))
                .filter(|value| !value.is_empty())
        };
        let project = get("GOOGLE_CLOUD_PROJECT").or_else(|| get("GCLOUD_PROJECT"))?;
        let location = get("GOOGLE_CLOUD_LOCATION")?;
        let credentials_path = get("GOOGLE_APPLICATION_CREDENTIALS");
        let endpoint = endpoint(&project, &location)?;
        file_exists(credentials_path.as_deref()).then(|| Self {
            endpoint,
            credentials_path: credentials_path.map(PathBuf::from),
        })
    }

    pub fn quota_project(&self) -> Option<String> {
        let path = self
            .credentials_path
            .clone()
            .or_else(default_credentials_path)?;
        if std::fs::metadata(&path).ok()?.len() > 128 * 1024 {
            return None;
        }
        let data: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
        let project = data["quota_project_id"].as_str()?;
        identifier(project).then(|| project.to_owned())
    }

    pub async fn access(&self) -> Result<String> {
        let executable = if cfg!(windows) {
            "gcloud.cmd"
        } else {
            "gcloud"
        };
        let mut command = tokio::process::Command::new(executable);
        command
            .args([
                "auth",
                "application-default",
                "print-access-token",
                "--quiet",
            ])
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        if let Some(path) = &self.credentials_path {
            command.env("GOOGLE_APPLICATION_CREDENTIALS", path);
        }
        let result = tokio::time::timeout(Duration::from_secs(25), command.output())
            .await
            .map_err(|_| Error::Config("Google Vertex AI credential lookup timed out".into()))?
            .map_err(|_| Error::Config("Google Vertex AI ADC requires the gcloud CLI".into()))?;
        if !result.status.success() {
            return Err(Error::Config(
                "gcloud could not resolve Google Vertex AI Application Default Credentials".into(),
            ));
        }
        std::str::from_utf8(&result.stdout)
            .ok()
            .map(str::trim)
            .filter(|token| super::model_auth::valid_key(token))
            .map(str::to_owned)
            .ok_or_else(|| Error::Config("gcloud returned an invalid Google access token".into()))
    }
}

fn default_credentials_path() -> Option<PathBuf> {
    #[cfg(windows)]
    let config = std::env::var_os("APPDATA").map(PathBuf::from);
    #[cfg(not(windows))]
    let config = std::env::home_dir().map(|home| home.join(".config"));
    std::env::var_os("CLOUDSDK_CONFIG")
        .map(PathBuf::from)
        .or_else(|| config.map(|config| config.join("gcloud")))
        .map(|base| base.join("application_default_credentials.json"))
}

fn identifier(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 63
        && id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && id.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn endpoint(project: &str, location: &str) -> Option<String> {
    if !identifier(project) || !identifier(location) {
        return None;
    }
    let host = if location == "global" {
        "aiplatform.googleapis.com".to_owned()
    } else {
        format!("{location}-aiplatform.googleapis.com")
    };
    Some(format!(
        "https://{host}/v1/projects/{project}/locations/{location}"
    ))
}

pub(super) fn validate_identifier(value: &str, field: &str) -> Result<()> {
    if identifier(value) {
        Ok(())
    } else {
        Err(Error::Config(format!("Enter a valid Google Cloud {field}")))
    }
}

pub(super) fn save_login(
    project: &str,
    location: &str,
    credentials_path: Option<&std::path::Path>,
) -> Result<()> {
    save_login_at(
        &super::config::agent_dir().join("auth.json"),
        project,
        location,
        credentials_path,
        |env| Adc::configured(Some(env)).is_some(),
    )
}

fn save_login_at(
    path: &std::path::Path,
    project: &str,
    location: &str,
    credentials_path: Option<&std::path::Path>,
    adc_available: impl FnOnce(&Environment) -> bool,
) -> Result<()> {
    validate_identifier(project, "project ID")?;
    validate_identifier(location, "location")?;
    let mut env = Environment::from_iter([
        ("GOOGLE_CLOUD_PROJECT".into(), project.into()),
        ("GOOGLE_CLOUD_LOCATION".into(), location.into()),
    ]);
    if let Some(path) = credentials_path {
        let path = std::path::absolute(path)?;
        let path = path.to_str().filter(|_| path.is_file()).ok_or_else(|| {
            Error::Config("Enter the path to an existing service account credentials file".into())
        })?;
        env.insert("GOOGLE_APPLICATION_CREDENTIALS".into(), path.into());
    } else if !adc_available(&env) {
        return Err(Error::Config(
            "No Application Default Credentials file found. Run gcloud auth application-default login or set GOOGLE_APPLICATION_CREDENTIALS.".into(),
        ));
    }
    super::config::update_object(path, |data| {
        data.insert(
            "google-vertex".into(),
            serde_json::json!({"type": "api_key", "env": env}),
        );
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_saves_a_pi_compatible_adc_entry_without_a_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let fixture = dir.path().join("service-account.json");
        std::fs::write(&fixture, "{}").unwrap();

        save_login_at(&path, "project-id", "us-central1", Some(&fixture), |_| {
            false
        })
        .unwrap();
        let saved = super::super::config::read_object(&path).unwrap();
        let entry = saved["google-vertex"].as_object().unwrap();
        assert_eq!(entry["type"], "api_key");
        assert!(!entry.contains_key("key"));
        let adc = Adc::configured(entry["env"].as_object()).unwrap();
        assert_eq!(
            adc.endpoint,
            "https://us-central1-aiplatform.googleapis.com/v1/projects/project-id/locations/us-central1"
        );
        assert_eq!(adc.credentials_path, Some(fixture));

        assert!(save_login_at(&path, "invalid/id", "global", None, |_| true).is_err());
        assert!(save_login_at(&path, "project-id", "global", None, |_| false).is_err());
        assert!(
            save_login_at(
                &path,
                "project-id",
                "global",
                Some(&dir.path().join("missing.json")),
                |_| true,
            )
            .is_err()
        );
        assert_eq!(super::super::config::read_object(&path).unwrap(), saved);
        save_login_at(&path, "project-id", "global", None, |_| true).unwrap();
        let saved = super::super::config::read_object(&path).unwrap();
        assert_eq!(
            saved["google-vertex"]["env"]["GOOGLE_CLOUD_LOCATION"],
            "global"
        );
        assert!(
            saved["google-vertex"]["env"]
                .get("GOOGLE_APPLICATION_CREDENTIALS")
                .is_none()
        );
    }

    #[test]
    fn adc_resolves_regional_and_global_projects_with_saved_precedence() {
        let saved = serde_json::json!({
            "GOOGLE_CLOUD_PROJECT":"my-project",
            "GOOGLE_APPLICATION_CREDENTIALS":"/fixture/service-account.json",
        });
        let mut vars = |name: &str| match name {
            "GOOGLE_CLOUD_PROJECT" => Some("ambient-project".to_owned()),
            "GOOGLE_CLOUD_LOCATION" => Some("us-central1".to_owned()),
            _ => None,
        };
        let adc = Adc::configured_with(saved.as_object(), &mut vars, |path| {
            path == Some("/fixture/service-account.json")
        })
        .unwrap();
        assert_eq!(
            adc.endpoint,
            "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1"
        );
        assert_eq!(
            endpoint("12345", "global").as_deref(),
            Some("https://aiplatform.googleapis.com/v1/projects/12345/locations/global")
        );
        assert!(endpoint("parent/other", "global").is_none());
        assert!(endpoint("my-project", "elsewhere.example").is_none());
        assert!(Adc::configured_with(saved.as_object(), &mut vars, |_| false).is_none());
    }
}
