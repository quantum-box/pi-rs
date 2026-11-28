//! Correlated extension interactions between the worker and its frontend.
use futures_util::FutureExt;
use pi_rs::{
    CancellationToken, ExtensionResult, ExtensionUi, FooterRenderContext, FooterRenderer,
    HeaderRenderContext, HeaderRenderer, RenderedExtensionRows, UiRequest, UiResponse,
    UiThemePalette, UiThemeSelection,
};
use pi_rs_workbench::theme_resources::{ExtensionThemeSnapshot, extension_theme_snapshot};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

type PendingRequests = Arc<Mutex<BTreeMap<Uuid, PendingResponse>>>;

struct PendingResponse {
    sender: oneshot::Sender<UiResponse>,
    theme_claim: Option<ThemeClaim>,
}

enum ThemeClaim {
    Unclaimed {
        callback: CancellationToken,
        requester: Option<CancellationToken>,
        selection: PreparedTheme,
    },
    Claimed,
}

pub(super) fn theme_unavailable() -> UiResponse {
    UiResponse::ThemeChanged(pi_rs::UiThemeChange {
        success: false,
        error: Some("UI not available".into()),
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum PreparedTheme {
    System,
    Named {
        palette: UiThemePalette,
        // Keep the native watch path even when the public palette cannot expose it.
        path: Option<OsString>,
    },
    Palette {
        palette: UiThemePalette,
    },
    Unavailable {
        error: String,
    },
}

#[derive(Default)]
pub struct Broker {
    pending: PendingRequests,
    themes: Option<Mutex<ExtensionResult<ExtensionThemeSnapshot>>>,
    chrome: [ChromeSlot; 2],
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ChromeSection {
    Header,
    Footer,
}

impl ChromeSection {
    pub(super) const ALL: [Self; 2] = [Self::Header, Self::Footer];
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "section", content = "layout", rename_all = "snake_case")]
pub(super) enum ChromeRenderContext {
    Header(HeaderRenderContext),
    Footer(FooterRenderContext),
}

impl ChromeRenderContext {
    pub(super) fn section(&self) -> ChromeSection {
        match self {
            Self::Header(_) => ChromeSection::Header,
            Self::Footer(_) => ChromeSection::Footer,
        }
    }
}

#[derive(Clone)]
enum ChromeRenderer {
    Header(Arc<dyn HeaderRenderer>),
    Footer(Arc<dyn FooterRenderer>),
}

impl ChromeRenderer {
    async fn render_with_cancellation(
        &self,
        context: &ChromeRenderContext,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<Option<RenderedExtensionRows>> {
        match (self, context) {
            (Self::Header(renderer), ChromeRenderContext::Header(context)) => {
                renderer
                    .render_with_cancellation(context, cancellation)
                    .await
            }
            (Self::Footer(renderer), ChromeRenderContext::Footer(context)) => {
                renderer
                    .render_with_cancellation(context, cancellation)
                    .await
            }
            _ => Err("extension renderer does not match the requested section".into()),
        }
    }
}

#[derive(Default)]
struct ChromeSlot {
    mounted: Mutex<Option<MountedChrome>>,
    rendering: Arc<tokio::sync::Mutex<()>>,
}

struct MountedChrome {
    id: Uuid,
    owner: String,
    renderer: ChromeRenderer,
    cancellation: CancellationToken,
}

impl Drop for MountedChrome {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct Pending {
    id: Uuid,
    requests: PendingRequests,
    completed: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        let removed = self
            .requests
            .lock()
            .expect("extension UI requests")
            .remove(&self.id);
        // A claim already authorizes the frontend to finish the mutation.
        if !self.completed
            && !matches!(
                removed.and_then(|request| request.theme_claim),
                Some(ThemeClaim::Claimed)
            )
        {
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

    fn slot(&self, section: ChromeSection) -> &ChromeSlot {
        &self.chrome[section as usize]
    }

    fn mount(
        &self,
        section: ChromeSection,
        owner: &str,
        renderer: Option<ChromeRenderer>,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        let mut mounted = self.slot(section).mounted.lock().expect("extension chrome");
        if cancellation.is_cancelled() {
            return Err("extension UI cancelled".into());
        }
        if self.themes.is_none() {
            return Ok(());
        }
        *mounted = renderer.map(|renderer| MountedChrome {
            id: Uuid::new_v4(),
            owner: owner.into(),
            renderer,
            cancellation: CancellationToken::new(),
        });
        let renderer = mounted.as_ref().map(|mount| mount.id);
        super::output(
            json!({"type":"chrome_renderer_changed","section":section,"renderer":renderer}),
        );
        Ok(())
    }

    fn invalidate(
        &self,
        section: ChromeSection,
        owner: &str,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        let mounted = self.slot(section).mounted.lock().expect("extension chrome");
        if cancellation.is_cancelled() {
            return Err("extension UI cancelled".into());
        }
        if let Some(mount) = mounted.as_ref().filter(|mount| mount.owner == owner) {
            super::output(
                json!({"type":"chrome_renderer_invalidated","section":section,"renderer":mount.id}),
            );
        }
        Ok(())
    }

    pub fn admit_chrome_render(
        &self,
        id: Uuid,
        context: ChromeRenderContext,
    ) -> Option<impl Future<Output = Option<RenderedExtensionRows>> + Send + use<>> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let section = self.slot(context.section());
        let slot = section.rendering.clone().try_lock_owned().ok()?;
        let (renderer, cancellation) = {
            let mounted = section.mounted.lock().expect("extension chrome");
            let mount = mounted.as_ref().filter(|mount| mount.id == id)?;
            (mount.renderer.clone(), mount.cancellation.child_token())
        };
        Some(async move {
            let _slot = slot;
            let rows = {
                let render = async {
                    if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
                        return Ok(None);
                    }
                    renderer
                        .render_with_cancellation(&context, &cancellation)
                        .await
                };
                let render = AssertUnwindSafe(render).catch_unwind();
                tokio::pin!(render);
                let result = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => None,
                    _ = tokio::time::sleep_until(deadline) => None,
                    rows = &mut render => Some(rows),
                };
                match result {
                    Some(Ok(rows)) => rows.ok().flatten().filter(|rows| {
                        !cancellation.is_cancelled()
                            && tokio::time::Instant::now() < deadline
                            && rows.within_chrome_limits()
                    }),
                    Some(Err(_panic)) => {
                        cancellation.cancel();
                        None
                    }
                    None => {
                        cancellation.cancel();
                        // Keep the slot until any background work has been reaped.
                        let _ = render.await;
                        None
                    }
                }
            };
            drop(renderer);
            rows
        })
    }

    pub async fn clear_chrome_renderers(&self) {
        for section in ChromeSection::ALL {
            let mut mounted = self.slot(section).mounted.lock().expect("extension chrome");
            if mounted.take().is_some() {
                super::output(
                    json!({"type":"chrome_renderer_changed","section":section,"renderer":null}),
                );
            }
        }
        // Finish render cleanup before extension shutdown.
        for slot in &self.chrome {
            let _rendering = slot.rendering.lock().await;
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
            (None, UiRequest::SetTheme { .. }) => Ok(Some(theme_unavailable())),
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

    fn prepare_theme(&self, selection: &UiThemeSelection) -> PreparedTheme {
        let name = match selection {
            UiThemeSelection::Palette(palette) => {
                return PreparedTheme::Palette {
                    palette: palette.clone(),
                };
            }
            UiThemeSelection::Name(name) if name == "system" => return PreparedTheme::System,
            UiThemeSelection::Name(name) => name,
        };
        let snapshot = self
            .themes
            .as_ref()
            .expect("terminal theme snapshot")
            .lock()
            .expect("extension theme snapshot");
        match snapshot.as_ref() {
            Ok(snapshot) => match snapshot.palette(name) {
                Some(palette) => PreparedTheme::Named {
                    palette: palette.clone(),
                    path: snapshot.path(name).map(|path| path.as_os_str().to_owned()),
                },
                None => PreparedTheme::Unavailable {
                    error: format!("theme {name:?} was not found"),
                },
            },
            Err(error) => PreparedTheme::Unavailable {
                error: error.clone(),
            },
        }
    }

    pub(super) fn claim_theme(&self, id: Uuid) -> Option<PreparedTheme> {
        let mut requests = self.pending.lock().expect("extension UI requests");
        let claim = &mut requests.get_mut(&id)?.theme_claim;
        // Check both the callback and the executable transport before authorizing the mutation.
        // Cancellation after this point can stop the waiter while the frontend finishes.
        if matches!(claim, Some(ThemeClaim::Unclaimed { callback, requester, .. })
            if !callback.is_cancelled() && !requester.as_ref().is_some_and(CancellationToken::is_cancelled))
            && let Some(ThemeClaim::Unclaimed { selection, .. }) =
                claim.replace(ThemeClaim::Claimed)
        {
            return Some(selection);
        }
        None
    }

    pub fn resolve(&self, id: Uuid, value: UiResponse) {
        let mut requests = self.pending.lock().expect("extension UI requests");
        if requests.get(&id).is_some_and(|request| {
            !matches!(request.theme_claim, Some(ThemeClaim::Unclaimed { .. }))
        }) && let Some(request) = requests.remove(&id)
        {
            drop(requests);
            let _ = request.sender.send(value);
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
        self.mount(
            ChromeSection::Header,
            owner,
            renderer.map(ChromeRenderer::Header),
            cancellation,
        )
    }

    fn invalidate_header_renderer(
        &self,
        owner: &str,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        self.invalidate(ChromeSection::Header, owner, cancellation)
    }

    fn set_footer_renderer(
        &self,
        owner: &str,
        renderer: Option<Arc<dyn FooterRenderer>>,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        self.mount(
            ChromeSection::Footer,
            owner,
            renderer.map(ChromeRenderer::Footer),
            cancellation,
        )
    }

    fn invalidate_footer_renderer(
        &self,
        owner: &str,
        cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        self.invalidate(ChromeSection::Footer, owner, cancellation)
    }

    async fn request(
        &self,
        request: UiRequest,
        cancellation: CancellationToken,
    ) -> ExtensionResult<UiResponse> {
        request.validate()?;
        if self.themes.is_none()
            && matches!(
                request,
                UiRequest::SetEditorComponent { .. } | UiRequest::GetEditorComponent
            )
        {
            return Ok(UiResponse::Cancelled);
        }
        if matches!(
            request,
            UiRequest::SetHeaderRenderer { .. }
                | UiRequest::InvalidateHeaderRenderer
                | UiRequest::SetFooterRenderer { .. }
                | UiRequest::InvalidateFooterRenderer
        ) {
            return Err(
                "dynamic headers and footers require an extension presentation lifecycle callback"
                    .into(),
            );
        }
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
            let theme_claim = match &request {
                UiRequest::SetTheme { theme } => Some(ThemeClaim::Unclaimed {
                    callback: cancellation.clone(),
                    requester: pi_rs::ui_requester_cancellation(),
                    selection: self.prepare_theme(theme),
                }),
                _ => None,
            };
            let (sender, receiver) = oneshot::channel();
            let mut requests = self.pending.lock().expect("extension UI requests");
            if requests.len() >= 16 {
                return Err("too many pending extension UI requests".into());
            }
            requests.insert(
                id,
                PendingResponse {
                    sender,
                    theme_claim,
                },
            );
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
        let static_section = match request {
            UiRequest::SetHeader { .. } => Some(ChromeSection::Header),
            UiRequest::SetFooter { .. } => Some(ChromeSection::Footer),
            _ => None,
        };
        if let Some(section) = static_section {
            let mut mounted = self.slot(section).mounted.lock().expect("extension chrome");
            if cancellation.is_cancelled() {
                return Err("extension UI cancelled".into());
            }
            mounted.take();
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
    async fn editor_component_requests_are_local_noops_in_headless_mode() {
        let broker = Broker::default();
        for request in [
            UiRequest::SetEditorComponent {
                component: Some(pi_rs::UiEditorComponent::default()),
            },
            UiRequest::GetEditorComponent,
        ] {
            assert_eq!(
                broker
                    .request(request, CancellationToken::new())
                    .await
                    .unwrap(),
                UiResponse::Cancelled
            );
        }
        assert!(broker.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn theme_claim_checks_requester_before_waiter_is_polled() {
        let broker = Broker::new(true);
        let callback = CancellationToken::new();
        let requester = CancellationToken::new();
        let request = broker.request(
            UiRequest::SetTheme {
                theme: UiThemeSelection::Name("light".into()),
            },
            callback.clone(),
        );
        let mut request = std::pin::pin!(pi_rs::with_ui_requester_cancellation(
            requester.clone(),
            request
        ));
        assert!(futures_util::poll!(&mut request).is_pending());
        let id = *broker.pending.lock().unwrap().keys().next().unwrap();
        requester.cancel();
        assert!(!callback.is_cancelled());
        assert!(broker.claim_theme(id).is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_eof_retracts_pending_theme_mutation() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let extension_path = dir.path().join("theme-exit");
        std::fs::write(&extension_path, r#"#!/usr/bin/env python3
import json, pathlib, sys, time
if '--pi-rs-extension-v1' in sys.argv:
    json.load(sys.stdin)
    descriptor = {'id':'theme-exit','dependencies':[],'tools':[],'commands':[],'lifecycle_hook':True,'session_lifecycle_hook':True,'lifecycle_host_calls':True}
    print(json.dumps({'version':1,'response':{'Ok':descriptor}}))
else:
    json.loads(sys.stdin.readline())
    print(json.dumps({'type':'ui','request':{'method':'setTheme','theme':'light'}}), flush=True)
    release = pathlib.Path(__file__).with_name('exit-now')
    while not release.exists():
        time.sleep(0.005)
"#).unwrap();
        std::fs::set_permissions(&extension_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let broker = Arc::new(Broker::new(true));
        broker.refresh_themes(&[]);
        let mut registry = pi_rs::Registry::default();
        registry.set_ui(broker.clone());
        registry
            .register(Arc::new(
                pi_rs_extensions::ExecutableExtension::load(extension_path).unwrap(),
            ))
            .unwrap();

        let mut error = None;
        let states = pi_rs::ExtensionStates::default();
        let mut observe = |notification: pi_rs::Notification<'_>| {
            if let pi_rs::Notification::ExtensionLifecycleError { message, .. } = notification {
                error = Some(message.to_owned());
            }
        };
        let lifecycle = pi_rs::notify_extension_lifecycle_filtered_with_ui(
            &registry,
            pi_rs::LifecycleEvent::SessionStart {
                reason: pi_rs::SessionStartReason::Startup,
                previous_session_file: None,
            },
            &states,
            Duration::from_secs(10),
            &mut observe,
            |_| true,
        );
        let ((), id) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(lifecycle, async {
                let id = loop {
                    if let Some(id) = broker.pending.lock().unwrap().keys().next().copied() {
                        break id;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                };
                std::fs::write(dir.path().join("exit-now"), []).unwrap();
                id
            })
        })
        .await
        .expect("EOF must release the waiting UI callback before its lifecycle deadline");
        assert_eq!(
            error.as_deref(),
            Some("native extension disconnected during UI request")
        );
        assert!(broker.claim_theme(id).is_none());
        assert!(broker.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn theme_claim_is_the_mutation_authorization_point() {
        let broker = Broker::new(true);
        let pending_id = || *broker.pending.lock().unwrap().keys().next().unwrap();
        let selection = || UiRequest::SetTheme {
            theme: pi_rs::UiThemeSelection::Name("light".into()),
        };
        let success = UiResponse::ThemeChanged(pi_rs::UiThemeChange {
            success: true,
            error: None,
        });

        let token = CancellationToken::new();
        let mut request = std::pin::pin!(broker.request(selection(), token.clone()));
        assert!(futures_util::poll!(&mut request).is_pending());
        let id = pending_id();
        token.cancel();
        // The cancellation token wins even before the waiter is polled again.
        assert!(broker.claim_theme(id).is_none());
        assert!(request.await.is_err());
        assert!(broker.claim_theme(id).is_none());

        let token = CancellationToken::new();
        let mut request = std::pin::pin!(broker.request(selection(), token.clone()));
        assert!(futures_util::poll!(&mut request).is_pending());
        let id = pending_id();
        broker.resolve(id, success.clone());
        assert!(futures_util::poll!(&mut request).is_pending());
        assert!(broker.claim_theme(id).is_some());
        assert!(broker.claim_theme(id).is_none());
        // The authorization stays valid if the caller stops waiting after the claim.
        token.cancel();
        assert!(request.await.is_err());
        broker.resolve(id, success.clone());
        assert!(broker.pending.lock().unwrap().is_empty());

        let mut request = std::pin::pin!(broker.request(selection(), CancellationToken::new()));
        assert!(futures_util::poll!(&mut request).is_pending());
        let id = pending_id();
        assert!(broker.claim_theme(id).is_some());
        broker.resolve(id, success.clone());
        assert_eq!(request.await.unwrap(), success);
    }

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
            let lock = ui.slot(ChromeSection::Header).mounted.lock().unwrap();
            let header = lock.as_ref().unwrap();
            assert_eq!(header.owner, "header-test");
            let ChromeRenderer::Header(renderer) = &header.renderer else {
                panic!("expected a header")
            };
            (header.id, Arc::downgrade(renderer))
        };
        let (first, renderer) = header();
        let context = ChromeRenderContext::Header(HeaderRenderContext {
            width: 72,
            theme: crate::ui::theme::Theme::system().extension_render_theme(),
        });
        let render = ui.admit_chrome_render(first, context.clone()).unwrap();
        let (rendered, ()) = tokio::join!(render, async {
            entered.notified().await;
            mount().await;
            let (replacement, _) = header();
            assert!(
                ui.admit_chrome_render(replacement, context.clone())
                    .is_none()
            );
        });
        assert!(rendered.is_none());
        assert!(renderer.upgrade().is_none());
        let (second, renderer) = header();
        assert_ne!(first, second);
        assert!(ui.admit_chrome_render(first, context).is_none());

        ui.request(
            UiRequest::SetHeader {
                lines: Some(vec![]),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(renderer.upgrade().is_none());
        assert!(
            ui.slot(ChromeSection::Header)
                .mounted
                .lock()
                .unwrap()
                .is_none()
        );
        mount().await;
        let (_, renderer) = header();
        ui.clear_chrome_renderers().await;
        assert!(renderer.upgrade().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_chrome_deadline_includes_scheduling_and_clear_waits_for_cleanup() {
        #[derive(Default)]
        struct Renderer {
            entered: tokio::sync::Notify,
            cancelled: tokio::sync::Notify,
            finish_cleanup: tokio::sync::Notify,
        }

        impl Renderer {
            async fn run(
                &self,
                cancellation: &CancellationToken,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.entered.notify_one();
                cancellation.cancelled().await;
                self.cancelled.notify_one();
                self.finish_cleanup.notified().await;
                Ok(Some(RenderedExtensionRows::default()))
            }
        }

        #[async_trait::async_trait]
        impl HeaderRenderer for Renderer {
            async fn render(
                &self,
                _: &HeaderRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                panic!("the broker must pass a cancellation token");
            }

            async fn render_with_cancellation(
                &self,
                _: &HeaderRenderContext,
                cancellation: &CancellationToken,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.run(cancellation).await
            }
        }

        #[async_trait::async_trait]
        impl FooterRenderer for Renderer {
            async fn render(
                &self,
                _: &FooterRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                panic!("the broker must pass a cancellation token");
            }

            async fn render_with_cancellation(
                &self,
                _: &FooterRenderContext,
                cancellation: &CancellationToken,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.run(cancellation).await
            }
        }

        for section in ChromeSection::ALL {
            let ui = Broker::new(true);
            let renderer = Arc::new(Renderer::default());
            let theme = crate::ui::theme::Theme::system().extension_render_theme();
            let context = match section {
                ChromeSection::Header => {
                    ChromeRenderContext::Header(HeaderRenderContext { width: 72, theme })
                }
                ChromeSection::Footer => ChromeRenderContext::Footer(FooterRenderContext {
                    width: 72,
                    theme,
                    data: Default::default(),
                }),
            };
            let mount = || {
                let token = CancellationToken::new();
                match section {
                    ChromeSection::Header => {
                        ui.set_header_renderer("test", Some(renderer.clone()), &token)
                    }
                    ChromeSection::Footer => {
                        ui.set_footer_renderer("test", Some(renderer.clone()), &token)
                    }
                }
                .unwrap();
                ui.slot(section)
                    .mounted
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .id
            };

            let id = mount();
            let delayed = ui.admit_chrome_render(id, context.clone()).unwrap();
            assert!(ui.admit_chrome_render(id, context.clone()).is_none());
            tokio::time::advance(Duration::from_millis(500)).await;
            assert!(delayed.await.is_none());
            assert!(futures_util::poll!(std::pin::pin!(renderer.entered.notified())).is_pending());

            for expires in [true, false] {
                let id = mount();
                let running = tokio::spawn(ui.admit_chrome_render(id, context.clone()).unwrap());
                renderer.entered.notified().await;
                let mut clear = std::pin::pin!(ui.clear_chrome_renderers());
                if expires {
                    tokio::time::advance(Duration::from_millis(500)).await;
                } else {
                    assert!(futures_util::poll!(&mut clear).is_pending());
                }
                renderer.cancelled.notified().await;
                assert!(futures_util::poll!(&mut clear).is_pending());
                assert!(!running.is_finished());
                renderer.finish_cleanup.notify_one();
                clear.await;
                assert!(running.await.unwrap().is_none());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_chrome_panic_returns_fallback_and_allows_replacement() {
        struct Renderer {
            should_panic: bool,
            entered: Option<Arc<tokio::sync::Notify>>,
            request_token: Arc<Mutex<Option<CancellationToken>>>,
        }

        impl Renderer {
            async fn run(
                &self,
                cancellation: &CancellationToken,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                *self.request_token.lock().unwrap() = Some(cancellation.clone());
                if let Some(entered) = &self.entered {
                    entered.notify_one();
                    cancellation.cancelled().await;
                }
                assert!(!self.should_panic, "test renderer panic");
                Ok(Some(RenderedExtensionRows::default()))
            }
        }

        #[async_trait::async_trait]
        impl HeaderRenderer for Renderer {
            async fn render(
                &self,
                _: &HeaderRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                panic!("the broker must pass a cancellation token");
            }

            async fn render_with_cancellation(
                &self,
                _: &HeaderRenderContext,
                cancellation: &CancellationToken,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.run(cancellation).await
            }
        }

        #[async_trait::async_trait]
        impl FooterRenderer for Renderer {
            async fn render(
                &self,
                _: &FooterRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                panic!("the broker must pass a cancellation token");
            }

            async fn render_with_cancellation(
                &self,
                _: &FooterRenderContext,
                cancellation: &CancellationToken,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.run(cancellation).await
            }
        }

        for section in ChromeSection::ALL {
            let ui = Broker::new(true);
            let theme = crate::ui::theme::Theme::system().extension_render_theme();
            let context = match section {
                ChromeSection::Header => {
                    ChromeRenderContext::Header(HeaderRenderContext { width: 72, theme })
                }
                ChromeSection::Footer => ChromeRenderContext::Footer(FooterRenderContext {
                    width: 72,
                    theme,
                    data: Default::default(),
                }),
            };
            let request_token = Arc::new(Mutex::new(None));
            let mount = |should_panic, entered| {
                let renderer = Arc::new(Renderer {
                    should_panic,
                    entered,
                    request_token: request_token.clone(),
                });
                let renderer = match section {
                    ChromeSection::Header => ChromeRenderer::Header(renderer),
                    ChromeSection::Footer => ChromeRenderer::Footer(renderer),
                };
                ui.mount(section, "test", Some(renderer), &CancellationToken::new())
                    .unwrap();
                ui.slot(section)
                    .mounted
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .id
            };
            let render = |id| tokio::spawn(ui.admit_chrome_render(id, context.clone()).unwrap());

            for entered in [None, Some(Arc::new(tokio::sync::Notify::new()))] {
                let running = render(mount(true, entered.clone()));
                let replacement = if let Some(entered) = entered {
                    entered.notified().await;
                    Some(mount(false, None))
                } else {
                    None
                };
                assert!(running.await.unwrap().is_none());
                assert!(
                    request_token
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .is_cancelled()
                );
                let replacement = replacement.unwrap_or_else(|| mount(false, None));
                assert_eq!(
                    render(replacement).await.unwrap(),
                    Some(RenderedExtensionRows::default())
                );
            }
        }
    }

    #[tokio::test]
    async fn dynamic_footer_disposes_only_its_mount_on_static_replacement() {
        struct Footer(Arc<tokio::sync::Notify>);

        #[async_trait::async_trait]
        impl FooterRenderer for Footer {
            async fn render(
                &self,
                _: &FooterRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                self.0.notify_one();
                std::future::pending().await
            }
        }

        struct Header;
        #[async_trait::async_trait]
        impl HeaderRenderer for Header {
            async fn render(
                &self,
                _: &HeaderRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                Ok(Some(RenderedExtensionRows::default()))
            }
        }

        let ui = Broker::new(true);
        let entered = Arc::new(tokio::sync::Notify::new());
        let header: Arc<dyn HeaderRenderer> = Arc::new(Header);
        let footer: Arc<dyn FooterRenderer> = Arc::new(Footer(entered.clone()));
        let header_reference = Arc::downgrade(&header);
        let footer_reference = Arc::downgrade(&footer);
        let token = CancellationToken::new();
        ui.set_header_renderer("header", Some(header), &token)
            .unwrap();
        ui.set_footer_renderer("footer", Some(footer), &token)
            .unwrap();
        let mount = ui
            .slot(ChromeSection::Footer)
            .mounted
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .id;
        let context = ChromeRenderContext::Footer(FooterRenderContext {
            width: 40,
            theme: crate::ui::theme::Theme::system().extension_render_theme(),
            data: Default::default(),
        });
        let (rows, ()) = tokio::join!(ui.admit_chrome_render(mount, context).unwrap(), async {
            entered.notified().await;
            ui.request(UiRequest::SetFooter { lines: None }, token.clone())
                .await
                .unwrap();
        });
        assert!(rows.is_none());
        assert!(footer_reference.upgrade().is_none());
        assert!(header_reference.upgrade().is_some());
        ui.clear_chrome_renderers().await;
        assert!(header_reference.upgrade().is_none());
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
        let paths = [path.clone()];
        terminal.refresh_themes(&paths);
        let theme_list = terminal
            .request(UiRequest::GetAllThemes, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(theme_list).unwrap()[1],
            serde_json::json!({"name":"custom"}),
        );
        let PreparedTheme::Named {
            palette,
            path: watch,
        } = terminal.prepare_theme(&UiThemeSelection::Name("custom".into()))
        else {
            panic!("expected the named theme to retain its native watch path");
        };
        let selected = crate::ui::theme::Theme::from_named_palette(palette, watch.map(Into::into));
        assert_eq!(selected.path.as_deref(), Some(path.as_path()));
        assert!(selected.extension_palette().path.is_none());
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
        assert!(broker.claim_theme(id).is_none());
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
