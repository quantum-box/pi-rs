//! Correlated extension interactions between the worker and its frontend.
use pi_rs::{CancellationToken, ExtensionResult, ExtensionUi, UiRequest, UiResponse};
use pi_rs_workbench::theme_resources::{ExtensionThemeSnapshot, extension_theme_snapshot};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Default)]
pub struct Broker {
    pending: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<UiResponse>>>>,
    themes: Option<Mutex<ExtensionResult<ExtensionThemeSnapshot>>>,
}
struct Pending {
    id: Uuid,
    requests: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<UiResponse>>>>,
    completed: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.requests
            .lock()
            .expect("extension UI requests")
            .remove(&self.id);
        if !self.completed {
            super::output(serde_json::json!({"type":"extension_ui_cancelled","id":self.id}));
        }
    }
}
impl Broker {
    pub fn new(terminal_managed: bool) -> Self {
        Self {
            themes: terminal_managed.then(|| Mutex::new(Ok(ExtensionThemeSnapshot::default()))),
            ..Self::default()
        }
    }

    pub fn refresh_themes(&self, paths: &[std::path::PathBuf]) {
        if let Some(themes) = &self.themes {
            let snapshot = extension_theme_snapshot(paths).map_err(|error| error.to_string());
            *themes.lock().expect("extension theme catalog") = snapshot;
        }
    }

    fn theme_response(&self, request: &UiRequest) -> ExtensionResult<Option<UiResponse>> {
        let get = |snapshot: &ExtensionThemeSnapshot| match request {
            UiRequest::GetAllThemes => UiResponse::Themes(snapshot.catalog().to_vec()),
            UiRequest::GetTheme { name } => snapshot
                .palette(name)
                .cloned()
                .map_or(UiResponse::Cancelled, UiResponse::Theme),
            _ => unreachable!("theme request"),
        };
        match (&self.themes, request) {
            (None, UiRequest::GetAllThemes) => Ok(Some(UiResponse::Themes(vec![]))),
            (None, UiRequest::GetTheme { .. } | UiRequest::GetActiveTheme) => {
                Ok(Some(UiResponse::Cancelled))
            }
            (None, UiRequest::SetTheme { .. }) => {
                Ok(Some(UiResponse::ThemeChanged(pi_rs::UiThemeChange {
                    success: false,
                    error: Some("UI not available".into()),
                })))
            }
            (Some(_), UiRequest::GetTheme { name }) if name == "system" => Ok(None),
            (Some(themes), UiRequest::GetAllThemes | UiRequest::GetTheme { .. }) => themes
                .lock()
                .expect("extension theme snapshot")
                .as_ref()
                .map(|snapshot| Some(get(snapshot)))
                .map_err(Clone::clone),
            _ => Ok(None),
        }
    }

    pub fn resolve(&self, id: Uuid, value: UiResponse) {
        if let Some(sender) = self
            .pending
            .lock()
            .expect("extension UI requests")
            .remove(&id)
        {
            let _ = sender.send(value);
        }
    }
}
#[async_trait::async_trait]
impl ExtensionUi for Broker {
    async fn request(
        &self,
        request: UiRequest,
        cancellation: CancellationToken,
    ) -> ExtensionResult<UiResponse> {
        request.validate()?;
        if matches!(
            request,
            UiRequest::GetAllThemes
                | UiRequest::GetTheme { .. }
                | UiRequest::GetActiveTheme
                | UiRequest::SetTheme { .. }
        ) && cancellation.is_cancelled()
        {
            return Err("extension UI cancelled".into());
        }
        if let Some(response) = self.theme_response(&request)? {
            return if request.accepts_response(&response) {
                Ok(response)
            } else {
                Err("extension theme response exceeds the UI response limit".into())
            };
        }
        let id = Uuid::new_v4();
        let response = if request.needs_response() {
            let (sender, receiver) = oneshot::channel();
            let mut requests = self.pending.lock().expect("extension UI requests");
            if requests.len() >= 16 {
                return Err("too many pending extension UI requests".into());
            }
            requests.insert(id, sender);
            Some(receiver)
        } else {
            None
        };
        let mut guard = Pending {
            id,
            requests: self.pending.clone(),
            completed: !request.needs_response(),
        };
        let mut event = serde_json::to_value(&request).map_err(|error| error.to_string())?;
        event["type"] = serde_json::json!("extension_ui_request");
        event["id"] = serde_json::json!(id);
        super::output(event);
        let Some(response) = response else {
            return Ok(UiResponse::Cancelled);
        };
        let deadline = request
            .timeout()
            .map(|millis| {
                tokio::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(millis))
                    .ok_or_else(|| "extension UI timeout is too large".to_owned())
            })
            .transpose()?;
        let expiry = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        let value = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err("extension UI cancelled".into()),
            _ = expiry => return Ok(UiResponse::Cancelled),
            response = response => response.map_err(|_| "extension UI response was lost")?,
        };
        guard.completed = true;
        if !request.accepts_response(&value) {
            return Err("invalid extension UI response".into());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn theme_queries_use_the_worker_snapshot_without_forwarding_to_a_client() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first.json");
        let invalid = directory.path().join("invalid.json");
        let later = directory.path().join("later.json");
        let replaced = directory.path().join("replaced.json");
        let shadow = directory.path().join("embedded.json");
        let mut theme: serde_json::Value =
            serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
        theme["colors"]["accent"] = serde_json::json!("#010203");
        std::fs::write(&shadow, theme.to_string()).unwrap();
        theme["name"] = serde_json::json!("fixture");
        theme["vars"]["fixtureColor"] = serde_json::json!("#123456");
        theme["colors"]["accent"] = serde_json::json!("fixtureColor");
        theme["colors"]["text"] = serde_json::json!("");
        theme["colors"]["border"] = serde_json::json!(39);
        std::fs::write(&first, format!("\u{feff}{theme}")).unwrap();
        theme["colors"]["accent"] = serde_json::json!("#789abc");
        std::fs::write(&later, theme.to_string()).unwrap();
        std::fs::write(&invalid, "invalid theme").unwrap();
        std::fs::create_dir(&replaced).unwrap();
        theme["name"] = serde_json::json!("unresolved");
        std::fs::write(replaced.join("child.json"), theme.to_string()).unwrap();

        let paths = [invalid, first.clone(), later.clone(), replaced, shadow];
        let terminal = Broker::new(true);
        terminal.refresh_themes(&paths);
        std::fs::remove_file(&first).unwrap();
        let themes = terminal
            .request(UiRequest::GetAllThemes, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(themes).unwrap(),
            serde_json::json!([{"name":"system"},{"name":"dark"},{"name":"fixture","path":first},{"name":"light"}])
        );
        let request = |name: &str| UiRequest::GetTheme { name: name.into() };
        let UiResponse::Theme(fixture) = terminal
            .request(request("fixture"), CancellationToken::new())
            .await
            .unwrap()
        else {
            panic!("missing file palette")
        };
        assert_eq!(fixture.path.as_deref(), Some(first.as_path()));
        assert_eq!(
            fixture.colors["accent"],
            pi_rs::UiThemeColor::Rgb {
                r: 18,
                g: 52,
                b: 86
            }
        );
        assert_eq!(fixture.colors["text"], pi_rs::UiThemeColor::Reset);
        assert_eq!(
            fixture.colors["border"],
            pi_rs::UiThemeColor::Indexed { index: 39 }
        );
        assert!(fixture.dim.is_empty());

        let UiResponse::Theme(dark) = terminal
            .request(request("dark"), CancellationToken::new())
            .await
            .unwrap()
        else {
            panic!("missing embedded palette")
        };
        let rendered =
            crate::ui::theme::Theme::parse(include_str!("../assets/themes/dark.json"), None)
                .unwrap()
                .extension_palette();
        assert_eq!(dark, rendered);
        for name in ["Fixture", "../fixture", "light/dark", "unresolved"] {
            assert_eq!(
                terminal
                    .request(request(name), CancellationToken::new())
                    .await
                    .unwrap(),
                UiResponse::Cancelled
            );
        }
        terminal.refresh_themes(&paths);
        let UiResponse::Theme(fixture) = terminal
            .request(request("fixture"), CancellationToken::new())
            .await
            .unwrap()
        else {
            panic!("missing refreshed palette")
        };
        assert_eq!(fixture.path.as_deref(), Some(later.as_path()));
        assert_eq!(
            fixture.colors["accent"],
            pi_rs::UiThemeColor::Rgb {
                r: 120,
                g: 154,
                b: 188
            }
        );

        assert!(terminal.pending.lock().unwrap().is_empty());

        let rpc = Broker::default();
        rpc.refresh_themes(&paths);
        assert_eq!(
            rpc.request(UiRequest::GetAllThemes, CancellationToken::new())
                .await
                .unwrap(),
            UiResponse::Themes(vec![])
        );
        for name in ["fixture", "system", "dark"] {
            assert_eq!(
                rpc.request(request(name), CancellationToken::new())
                    .await
                    .unwrap(),
                UiResponse::Cancelled
            );
        }
        assert_eq!(
            rpc.request(UiRequest::GetActiveTheme, CancellationToken::new())
                .await
                .unwrap(),
            UiResponse::Cancelled
        );
        assert_eq!(
            rpc.request(
                UiRequest::SetTheme {
                    theme: pi_rs::UiThemeSelection::Name("dark".into())
                },
                CancellationToken::new(),
            )
            .await
            .unwrap(),
            UiResponse::ThemeChanged(pi_rs::UiThemeChange {
                success: false,
                error: Some("UI not available".into()),
            })
        );
        assert!(rpc.pending.lock().unwrap().is_empty());
    }

    // The default macOS filesystem rejects non-UTF-8 filenames.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn theme_catalog_omits_paths_with_non_utf8_filenames() {
        use std::os::unix::ffi::OsStrExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join(std::ffi::OsStr::from_bytes(b"custom-\xff.json"));
        let mut theme: serde_json::Value =
            serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
        theme["name"] = serde_json::json!("custom");
        std::fs::write(&path, theme.to_string()).unwrap();

        let terminal = Broker::new(true);
        terminal.refresh_themes(&[path]);
        let theme_list = terminal
            .request(UiRequest::GetAllThemes, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(theme_list).unwrap()[1],
            serde_json::json!({"name":"custom"}),
        );
    }

    #[tokio::test]
    async fn replies_are_correlated_and_cancelled_dialogs_release_their_slot() {
        let broker = Arc::new(Broker::default());
        let task_broker = broker.clone();
        let task = tokio::spawn(async move {
            task_broker
                .request(
                    UiRequest::Confirm {
                        title: "Confirm".into(),
                        message: "Continue?".into(),
                        timeout: None,
                    },
                    CancellationToken::new(),
                )
                .await
        });
        tokio::task::yield_now().await;
        let id = *broker.pending.lock().unwrap().keys().next().unwrap();
        broker.resolve(id, UiResponse::Confirmed(true));
        assert_eq!(task.await.unwrap().unwrap(), UiResponse::Confirmed(true));
        assert!(broker.pending.lock().unwrap().is_empty());
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let task_broker = broker.clone();
        let task = tokio::spawn(async move {
            task_broker
                .request(
                    UiRequest::Input {
                        title: "Text".into(),
                        placeholder: None,
                        initial_value: None,
                        timeout: None,
                    },
                    token,
                )
                .await
        });
        tokio::task::yield_now().await;
        cancellation.cancel();
        assert!(task.await.unwrap().is_err());
        assert!(broker.pending.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    #[tokio::test]
    async fn dialog_timeout_returns_cancelled_and_releases_the_request() {
        let broker = Broker::default();
        let result = broker
            .request(
                UiRequest::Select {
                    title: "Select".into(),
                    options: vec!["one".into()],
                    timeout: Some(1),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result, UiResponse::Cancelled);
        assert!(broker.pending.lock().unwrap().is_empty());
    }
}
