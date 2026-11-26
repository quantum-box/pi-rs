//! Correlated extension interactions between the worker and its frontend.
use pi_rs::{
    CancellationToken, ExtensionResult, ExtensionUi, HeaderRenderContext, HeaderRenderer,
    RenderedExtensionRows, UiRequest, UiResponse, UiThemeInfo,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Default)]
pub struct Broker {
    pending: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<UiResponse>>>>,
    themes: Option<Mutex<ExtensionResult<Vec<UiThemeInfo>>>>,
    header: Mutex<Option<MountedHeader>>,
    header_render: tokio::sync::Mutex<()>,
}
struct MountedHeader {
    id: Uuid,
    owner: String,
    renderer: Arc<dyn HeaderRenderer>,
    cancellation: CancellationToken,
}

impl Drop for MountedHeader {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
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
            themes: terminal_managed.then(|| Mutex::new(Ok(Vec::new()))),
            ..Self::default()
        }
    }

    pub fn refresh_themes(&self, paths: &[std::path::PathBuf]) {
        if let Some(themes) = &self.themes {
            let snapshot = pi_rs_workbench::theme_resources::extension_theme_catalog(paths)
                .map_err(|error| error.to_string());
            *themes.lock().expect("extension theme catalog") = snapshot;
        }
    }

    pub async fn render_header(
        &self,
        id: Uuid,
        context: &HeaderRenderContext,
    ) -> Option<RenderedExtensionRows> {
        let _slot = self.header_render.lock().await;
        let (renderer, cancellation) = {
            let header = self.header.lock().expect("extension header");
            let header = header.as_ref().filter(|header| header.id == id)?;
            (header.renderer.clone(), header.cancellation.clone())
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            rows = tokio::time::timeout(Duration::from_millis(500), renderer.render(context)) => {
                rows.ok()?.ok()?.filter(RenderedExtensionRows::within_header_limits)
            }
        }
    }

    pub async fn clear_header_renderer(&self) {
        {
            let mut header = self.header.lock().expect("extension header");
            if header.take().is_some() {
                super::output(json!({"type":"header_renderer_changed","renderer":null}));
            }
        }
        // Finish dropping the in-flight callback before extension shutdown.
        let _slot = self.header_render.lock().await;
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
    fn set_header_renderer(
        &self,
        owner: &str,
        renderer: Option<Arc<dyn HeaderRenderer>>,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        let mut header = self.header.lock().expect("extension header");
        if cancellation.is_cancelled() {
            return Err("extension UI cancelled".into());
        }
        if self.themes.is_none() {
            return Ok(());
        }
        *header = renderer.map(|renderer| MountedHeader {
            id: Uuid::new_v4(),
            owner: owner.into(),
            renderer,
            cancellation: CancellationToken::new(),
        });
        let renderer = header.as_ref().map(|header| header.id);
        super::output(json!({"type":"header_renderer_changed","renderer":renderer}));
        Ok(())
    }

    fn invalidate_header_renderer(
        &self,
        owner: &str,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        let header = self.header.lock().expect("extension header");
        if cancellation.is_cancelled() {
            return Err("extension UI cancelled".into());
        }
        if let Some(header) = header.as_ref().filter(|header| header.owner == owner) {
            super::output(json!({"type":"header_renderer_invalidated","renderer":header.id}));
        }
        Ok(())
    }

    async fn request(
        &self,
        request: UiRequest,
        cancellation: CancellationToken,
    ) -> ExtensionResult<UiResponse> {
        request.validate()?;
        if matches!(
            request,
            UiRequest::SetHeaderRenderer { .. } | UiRequest::InvalidateHeaderRenderer
        ) {
            return Err(
                "dynamic headers require an extension presentation lifecycle callback".into(),
            );
        }
        if matches!(request, UiRequest::GetAllThemes) {
            if cancellation.is_cancelled() {
                return Err("extension UI cancelled".into());
            }
            let themes = self.themes.as_ref().map_or_else(
                || Ok(Vec::new()),
                |themes| themes.lock().expect("extension theme catalog").clone(),
            )?;
            let response = UiResponse::Themes(themes);
            return if request.accepts_response(&response) {
                Ok(response)
            } else {
                Err("extension theme catalog exceeds the UI response limit".into())
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
        if matches!(request, UiRequest::SetHeader { .. }) {
            let mut header = self.header.lock().expect("extension header");
            if cancellation.is_cancelled() {
                return Err("extension UI cancelled".into());
            }
            header.take();
            super::output(event);
        } else {
            super::output(event);
        }
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
    async fn dynamic_header_lifecycle_owns_and_cancels_the_mounted_renderer() {
        struct Renderer(Arc<tokio::sync::Notify>);

        #[async_trait::async_trait]
        impl HeaderRenderer for Renderer {
            async fn render(
                &self,
                _: &HeaderRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.0.notify_one();
                std::future::pending().await
            }
        }

        struct Hook(Arc<tokio::sync::Notify>);

        #[async_trait::async_trait]
        impl pi_rs::Extension for Hook {
            fn id(&self) -> &str {
                "header-test"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }
            fn handles_session_lifecycle(&self) -> bool {
                true
            }
            fn header_renderer(&self) -> Option<Arc<dyn HeaderRenderer>> {
                Some(Arc::new(Renderer(self.0.clone())))
            }
            async fn lifecycle_with_host(
                &self,
                _: &pi_rs::LifecycleEvent,
                _: Option<&pi_rs::ExtensionState>,
                host: Arc<dyn pi_rs::ToolHost>,
            ) -> ExtensionResult<()> {
                host.ui(UiRequest::SetHeaderRenderer { enabled: true })
                    .await?;
                Ok(())
            }
        }

        let ui = Arc::new(Broker::new(true));
        let entered = Arc::new(tokio::sync::Notify::new());
        let mut registry =
            pi_rs::Registry::compose(vec![Arc::new(Hook(entered.clone()))], &Default::default())
                .unwrap();
        registry.set_ui(ui.clone());
        let mount = || async {
            pi_rs::notify_extension_lifecycle_filtered_with_ui(
                &registry,
                pi_rs::LifecycleEvent::SessionStart {
                    reason: pi_rs::SessionStartReason::Startup,
                    previous_session_file: None,
                },
                &Default::default(),
                Duration::from_secs(1),
                &mut |_| panic!("unexpected lifecycle error"),
                |_| true,
            )
            .await
        };
        // The worker calls the same snapshot presentation path on session start.
        mount().await;
        let header = || {
            let lock = ui.header.lock().unwrap();
            let header = lock.as_ref().unwrap();
            assert_eq!(header.owner, "header-test");
            (header.id, Arc::downgrade(&header.renderer))
        };
        let (first, renderer) = header();
        let context = HeaderRenderContext {
            width: 72,
            theme: crate::ui::theme::Theme::system().extension_render_theme(),
        };
        let (rendered, ()) = tokio::join!(ui.render_header(first, &context), async {
            entered.notified().await;
            mount().await;
        });
        assert!(rendered.is_none());
        assert!(renderer.upgrade().is_none());
        let (second, renderer) = header();
        assert_ne!(first, second);
        assert!(ui.render_header(first, &context).await.is_none());

        ui.request(
            UiRequest::SetHeader {
                lines: Some(vec![]),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(renderer.upgrade().is_none());
        assert!(ui.header.lock().unwrap().is_none());
        mount().await;
        let (_, renderer) = header();
        ui.clear_header_renderer().await;
        assert!(renderer.upgrade().is_none());
    }

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
        std::fs::write(&shadow, theme.to_string()).unwrap();
        theme["name"] = serde_json::json!("fixture");
        std::fs::write(&first, format!("\u{feff}{theme}")).unwrap();
        std::fs::write(&later, theme.to_string()).unwrap();
        std::fs::write(&invalid, "invalid theme").unwrap();
        std::fs::create_dir(&replaced).unwrap();
        theme["name"] = serde_json::json!("unresolved");
        std::fs::write(replaced.join("child.json"), theme.to_string()).unwrap();

        let paths = [invalid, first.clone(), later, replaced, shadow];
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
        assert!(terminal.pending.lock().unwrap().is_empty());

        let rpc = Broker::default();
        rpc.refresh_themes(&paths);
        assert_eq!(
            rpc.request(UiRequest::GetAllThemes, CancellationToken::new())
                .await
                .unwrap(),
            UiResponse::Themes(vec![])
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
