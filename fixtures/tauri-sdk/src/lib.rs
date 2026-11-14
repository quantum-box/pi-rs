//! Compile fixture for Tauri's managed state and command registration.
//! A real application supplies its own model, runtime, and Tauri context.
use std::sync::Arc;

use pi_rs_core::{AgentSession, Model, SessionBuilder, SessionControl};
use tokio::sync::{Mutex, watch};

pub struct AppState {
    session: Mutex<AgentSession>,
    control: SessionControl,
    closing: watch::Sender<bool>,
    #[cfg(test)]
    admissions: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    startup_gate: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    startup_release: tokio::sync::Notify,
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
        state.control.request_shutdown();
        state.closing.send_replace(true);
        state
            .session
            .lock()
            .await
            .close()
            .await
            .map_err(|error| error.to_string())
    }
}

/// Create state for a Tauri builder without installing an invoke handler.
pub fn session_state(model: Arc<dyn Model>, model_name: &str) -> pi_rs_core::Result<AppState> {
    let session = SessionBuilder::new(model, model_name).in_memory()?;
    let control = session.controls();
    let (closing, _) = watch::channel(false);
    Ok(AppState {
        session: Mutex::new(session),
        control,
        closing,
        #[cfg(test)]
        admissions: std::sync::atomic::AtomicUsize::new(0),
        #[cfg(test)]
        startup_gate: std::sync::Mutex::new(None),
        #[cfg(test)]
        startup_release: tokio::sync::Notify::new(),
    })
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
            assert_eq!(session.messages().unwrap().len(), 4);
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
            app.state::<AppState>().session.lock().await.subscribe()
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
