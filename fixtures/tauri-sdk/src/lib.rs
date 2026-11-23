//! Compile fixture for Tauri's managed state and command registration.
//! A real application supplies its own model, runtime, and Tauri context.
use std::sync::Arc;

use pi_rs_core::{AgentSession, Model, SessionBuilder, SessionControl};
use tokio::sync::{Mutex, watch};

pub struct AppState {
    session: Arc<Mutex<Option<AgentSession>>>,
    control: SessionControl,
    closing: watch::Sender<bool>,
    close_started: std::sync::atomic::AtomicBool,
    close_result: watch::Sender<Option<Result<(), String>>>,
    #[cfg(test)]
    shutdown_abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    #[cfg(test)]
    admissions: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    startup_gate: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    startup_release: tokio::sync::Notify,
}

struct CloseCompletion {
    sender: watch::Sender<Option<Result<(), String>>>,
    completed: bool,
}

impl CloseCompletion {
    fn complete(mut self, result: Result<(), String>) {
        self.completed = true;
        self.sender.send_replace(Some(result));
    }
}

impl Drop for CloseCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.sender
                .send_replace(Some(Err("shutdown task did not complete".into())));
        }
    }
}

/// Commands can be composed with application commands in one invoke handler.
pub mod commands {
    use super::*;

    #[tauri::command]
    pub async fn prompt(state: tauri::State<'_, AppState>, text: String) -> Result<String, String> {
        let mut closing = state.closing.subscribe();
        #[cfg(test)]
        state
            .admissions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let closed = || "session is closing".to_owned();
        let mut session = tokio::select! {
            biased;
            _ = closing.wait_for(|closing| *closing) => return Err(closed()),
            session = state.session.lock() => session,
        };
        let session = session.as_mut().ok_or_else(closed)?;
        let started = std::sync::atomic::AtomicBool::new(false);
        let run = async {
            #[cfg(test)]
            {
                let gate = state.startup_gate.lock().unwrap().take();
                if let Some(entered) = gate {
                    entered.send(()).unwrap();
                    state.startup_release.notified().await;
                }
            }
            session.prompt(text).await
        };
        tokio::pin!(run);
        let run = std::future::poll_fn(|cx| {
            started.store(true, std::sync::atomic::Ordering::SeqCst);
            run.as_mut().poll(cx)
        });
        tokio::pin!(run);
        tokio::select! {
            biased;
            _ = async { let _ = closing.wait_for(|closing| *closing).await; } => {
                if !started.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(closed());
                }
                #[cfg(test)]
                state.startup_release.notify_one();
                // Shutdown is inherited by runs registered during startup.
                // Await the prompt so its SDK cleanup still runs.
                let _ = run.await;
                Err(closed())
            }
            result = &mut run => result.map_err(|error| error.to_string()),
        }
    }

    #[tauri::command]
    pub fn abort(state: tauri::State<'_, AppState>) {
        state.control.abort();
    }

    #[tauri::command]
    pub async fn close(state: tauri::State<'_, AppState>) -> Result<(), String> {
        let mut result = state.close_result.subscribe();
        if !state
            .close_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            state.control.request_shutdown();
            state.closing.send_replace(true);
            let session = state.session.clone();
            let worker_session = session.clone();
            let completion = CloseCompletion {
                sender: state.close_result.clone(),
                completed: false,
            };
            let worker = tauri::async_runtime::spawn(async move {
                let mut owned = worker_session.lock().await.take();
                let outcome = match owned.as_mut() {
                    Some(session) => session.close().await.map_err(|error| error.to_string()),
                    None => Ok(()),
                };
                drop(owned);
                outcome
            });
            #[cfg(test)]
            {
                *state.shutdown_abort.lock().unwrap() = Some(worker.inner().abort_handle());
            }
            tauri::async_runtime::spawn(async move {
                let outcome = match worker.await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        drop(session.lock().await.take());
                        Err("shutdown task did not complete".to_owned())
                    }
                };
                completion.complete(outcome);
            });
        }
        let completed = result
            .wait_for(|outcome| outcome.is_some())
            .await
            .map_err(|_| "shutdown task ended without a result".to_owned())?;
        completed
            .as_ref()
            .expect("completed shutdown result")
            .clone()
    }
}

/// Create state for a Tauri builder without installing an invoke handler.
pub fn session_state(model: Arc<dyn Model>, model_name: &str) -> pi_rs_core::Result<AppState> {
    let session = SessionBuilder::new(model, model_name).in_memory()?;
    Ok(session_state_from_session(session))
}

/// Wrap a host-constructed session without exposing its configuration to IPC.
/// The host chooses the journal, model, resources and extensions before handing
/// ownership to the fixture's prompt, abort and close commands.
pub fn session_state_from_session(session: AgentSession) -> AppState {
    let control = session.controls();
    let (closing, _) = watch::channel(false);
    let (close_result, _) = watch::channel(None);
    AppState {
        session: Arc::new(Mutex::new(Some(session))),
        control,
        closing,
        close_started: std::sync::atomic::AtomicBool::new(false),
        close_result,
        #[cfg(test)]
        shutdown_abort: std::sync::Mutex::new(None),
        #[cfg(test)]
        admissions: std::sync::atomic::AtomicUsize::new(0),
        #[cfg(test)]
        startup_gate: std::sync::Mutex::new(None),
        #[cfg(test)]
        startup_release: tokio::sync::Notify::new(),
    }
}

/// Install the fixture's state and a handler containing its three commands.
/// This replaces any prior invoke handler; use `session_state` and register all
/// public commands in one handler when composing with application commands.
pub fn configure<R: tauri::Runtime>(
    builder: tauri::Builder<R>,
    model: Arc<dyn Model>,
    model_name: &str,
) -> pi_rs_core::Result<tauri::Builder<R>> {
    Ok(builder
        .manage(session_state(model, model_name)?)
        .invoke_handler(tauri::generate_handler![
            commands::prompt,
            commands::abort,
            commands::close
        ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs_core::{ModelEvent, ModelRequest, ModelStream};
    use tauri::Manager;

    struct Fixture;
    struct ImmediateModel(std::sync::atomic::AtomicUsize);
    #[async_trait::async_trait]
    impl Model for ImmediateModel {
        async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Fixture.stream(request).await
        }
    }
    struct StalledModel(std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>);
    #[async_trait::async_trait]
    impl Model for StalledModel {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            self.0.lock().unwrap().take().unwrap().send(()).unwrap();
            std::future::pending().await
        }
    }
    #[async_trait::async_trait]
    impl Model for Fixture {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("fixture answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }

    fn request(command: &str, body: serde_json::Value) -> tauri::webview::InvokeRequest {
        tauri::webview::InvokeRequest {
            cmd: command.into(),
            callback: tauri::ipc::CallbackFn(0),
            error: tauri::ipc::CallbackFn(1),
            url: if cfg!(windows) {
                "http://tauri.localhost"
            } else {
                "tauri://localhost"
            }
            .parse()
            .unwrap(),
            body: tauri::ipc::InvokeBody::Json(body),
            headers: Default::default(),
            invoke_key: tauri::test::INVOKE_KEY.into(),
        }
    }

    #[test]
    fn mock_ipc_reuses_managed_session_and_dispatches_commands() {
        let app = configure(tauri::test::mock_builder(), Arc::new(Fixture), "fixture")
            .unwrap()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .unwrap();
        for text in ["first", "second"] {
            let response = tauri::test::get_ipc_response(
                &webview,
                request("prompt", serde_json::json!({"text":text})),
            )
            .unwrap();
            assert_eq!(response.deserialize::<String>().unwrap(), "fixture answer");
        }
        assert!(
            tauri::test::get_ipc_response(&webview, request("abort", serde_json::json!({})))
                .is_ok()
        );
        tauri::async_runtime::block_on(async {
            let state = app.state::<AppState>();
            let session = state.session.lock().await;
            assert_eq!(session.as_ref().unwrap().messages().unwrap().len(), 4);
        });
        assert!(
            tauri::test::get_ipc_response(&webview, request("close", serde_json::json!({})))
                .is_ok()
        );
        assert!(
            tauri::test::get_ipc_response(
                &webview,
                request("prompt", serde_json::json!({"text":"after close"}))
            )
            .is_err()
        );
    }

    #[test]
    fn mock_ipc_close_aborts_active_and_queued_prompts() {
        use std::time::Duration;
        let (entered, started) = std::sync::mpsc::channel();
        let app = configure(
            tauri::test::mock_builder(),
            Arc::new(StalledModel(std::sync::Mutex::new(Some(entered)))),
            "fixture",
        )
        .unwrap()
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .unwrap();
        let mut events = tauri::async_runtime::block_on(async {
            app.state::<AppState>()
                .session
                .lock()
                .await
                .as_ref()
                .unwrap()
                .subscribe()
        });
        let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .unwrap();
        let prompt_view = webview.clone();
        let (prompt_sender, prompt_result) = std::sync::mpsc::channel();
        let prompt_thread = std::thread::spawn(move || {
            let response = tauri::test::get_ipc_response(
                &prompt_view,
                request("prompt", serde_json::json!({"text":"stall"})),
            );
            prompt_sender.send(response.is_err()).unwrap();
        });
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("prompt did not start");
        let queued_view = webview.clone();
        let (queued_sender, queued_result) = std::sync::mpsc::channel();
        let queued_thread = std::thread::spawn(move || {
            let response = tauri::test::get_ipc_response(
                &queued_view,
                request("prompt", serde_json::json!({"text":"queued"})),
            );
            queued_sender.send(response.is_err()).unwrap();
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while app
            .state::<AppState>()
            .admissions
            .load(std::sync::atomic::Ordering::SeqCst)
            < 2
        {
            assert!(
                std::time::Instant::now() < deadline,
                "second prompt was not admitted"
            );
            std::thread::yield_now();
        }
        let close_view = webview.clone();
        let (close_sender, close_result) = std::sync::mpsc::channel();
        let close_thread = std::thread::spawn(move || {
            let response =
                tauri::test::get_ipc_response(&close_view, request("close", serde_json::json!({})));
            close_sender.send(response.is_ok()).unwrap();
        });
        assert!(
            close_result
                .recv_timeout(Duration::from_secs(5))
                .expect("close blocked on prompt")
        );
        assert!(
            prompt_result
                .recv_timeout(Duration::from_secs(5))
                .expect("prompt was not aborted")
        );
        assert!(
            queued_result
                .recv_timeout(Duration::from_secs(5))
                .expect("queued prompt was not rejected")
        );
        close_thread.join().unwrap();
        prompt_thread.join().unwrap();
        queued_thread.join().unwrap();
        let mut ended = false;
        while let Ok(event) = events.try_recv() {
            if matches!(event, pi_rs_core::SessionEvent::RunEnded { .. }) {
                ended = true;
            }
        }
        assert!(ended, "cancelled run must emit its end event before close");
        assert!(
            tauri::test::get_ipc_response(
                &webview,
                request("prompt", serde_json::json!({"text":"after close"}))
            )
            .is_err()
        );
    }

    #[test]
    fn mock_ipc_close_cancels_prompt_before_run_registration() {
        use std::time::Duration;
        let model = Arc::new(ImmediateModel(std::sync::atomic::AtomicUsize::new(0)));
        let app = configure(tauri::test::mock_builder(), model.clone(), "fixture")
            .unwrap()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let (entered, started) = std::sync::mpsc::channel();
        *app.state::<AppState>().startup_gate.lock().unwrap() = Some(entered);
        let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .unwrap();
        let prompt_view = webview.clone();
        let (sender, result) = std::sync::mpsc::channel();
        let prompt_thread = std::thread::spawn(move || {
            let response = tauri::test::get_ipc_response(
                &prompt_view,
                request("prompt", serde_json::json!({"text":"starting"})),
            );
            sender.send(response.is_err()).unwrap();
        });
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("prompt did not acquire session");
        let close_view = webview.clone();
        let (sender, close_result) = std::sync::mpsc::channel();
        let close_thread = std::thread::spawn(move || {
            let response =
                tauri::test::get_ipc_response(&close_view, request("close", serde_json::json!({})));
            sender.send(response.is_ok()).unwrap();
        });
        assert!(
            close_result
                .recv_timeout(Duration::from_secs(5))
                .expect("close blocked during startup")
        );
        assert!(
            result
                .recv_timeout(Duration::from_secs(5))
                .expect("startup was not cancelled")
        );
        assert_eq!(model.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        prompt_thread.join().unwrap();
        close_thread.join().unwrap();
    }
}

#[cfg(test)]
mod close_cancellation_test {
    use super::*;
    use pi_rs_core::{
        Extension, ExtensionResult, ExtensionState, LifecycleEvent, ModelEvent, ModelRequest,
        ModelStream,
    };
    use tauri::Manager;
    struct Fixture;
    #[async_trait::async_trait]
    impl Model for Fixture {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            )])))
        }
    }
    struct BlockingShutdown {
        entered: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
        release: Arc<tokio::sync::Notify>,
        completed: Arc<std::sync::atomic::AtomicBool>,
    }
    #[async_trait::async_trait]
    impl Extension for BlockingShutdown {
        fn id(&self) -> &str {
            "blocking-shutdown"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            if matches!(event, LifecycleEvent::SessionShutdown { .. }) {
                self.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                self.release.notified().await;
                self.completed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(())
        }
    }
    #[test]
    fn cancelling_close_caller_does_not_cancel_shutdown_hook() {
        let (sender, entered) = std::sync::mpsc::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let extension = Arc::new(BlockingShutdown {
            entered: std::sync::Mutex::new(Some(sender)),
            release: release.clone(),
            completed: completed.clone(),
        });
        let mut session = SessionBuilder::new(Arc::new(Fixture), "fixture")
            .extension(extension)
            .unwrap()
            .in_memory()
            .unwrap();
        tauri::async_runtime::block_on(session.start()).unwrap();
        let app = tauri::test::mock_builder()
            .manage(session_state_from_session(session))
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let handle = app.handle().clone();
        let task = tauri::async_runtime::spawn(async move {
            commands::close(handle.state::<AppState>()).await
        });
        entered
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        task.abort();
        release.notify_one();
        let result = tauri::async_runtime::block_on(commands::close(app.state::<AppState>()));
        assert!(result.is_ok());
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }
    struct PanickingShutdown;
    #[async_trait::async_trait]
    impl Extension for PanickingShutdown {
        fn id(&self) -> &str {
            "panic-shutdown"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            assert!(
                !matches!(event, LifecycleEvent::SessionShutdown { .. }),
                "test shutdown panic"
            );
            Ok(())
        }
    }
    #[test]
    fn panicking_shutdown_reports_same_error_to_later_callers() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("pi-tauri-panic-{}-{unique}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let mut session = SessionBuilder::new(Arc::new(Fixture), "fixture")
            .extension(Arc::new(PanickingShutdown))
            .unwrap()
            .open(&path)
            .unwrap();
        tauri::async_runtime::block_on(session.start()).unwrap();
        let app = tauri::test::mock_builder()
            .manage(session_state_from_session(session))
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let first = tauri::async_runtime::block_on(commands::close(app.state::<AppState>()));
        let second = tauri::async_runtime::block_on(commands::close(app.state::<AppState>()));
        assert_eq!(first, Err("shutdown task did not complete".to_owned()));
        assert_eq!(second, first);
        let reopened = SessionBuilder::new(Arc::new(Fixture), "fixture")
            .open(&path)
            .unwrap();
        drop(reopened);
        drop(app);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn aborted_shutdown_worker_releases_waiting_session() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("pi-tauri-abort-{}-{unique}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let session = SessionBuilder::new(Arc::new(Fixture), "fixture")
            .open(&path)
            .unwrap();
        let app = tauri::test::mock_builder()
            .manage(session_state_from_session(session))
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        let state = app.state::<AppState>();
        let guard = tauri::async_runtime::block_on(state.session.lock());
        let handle = app.handle().clone();
        let caller = tauri::async_runtime::spawn(async move {
            commands::close(handle.state::<AppState>()).await
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let abort = loop {
            if let Some(abort) = state.shutdown_abort.lock().unwrap().take() {
                break abort;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shutdown worker was not started"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        abort.abort();
        drop(guard);
        let result = tauri::async_runtime::block_on(caller).unwrap();
        assert_eq!(result, Err("shutdown task did not complete".to_owned()));
        let reopened = SessionBuilder::new(Arc::new(Fixture), "fixture")
            .open(&path)
            .unwrap();
        drop(reopened);
        drop(app);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
