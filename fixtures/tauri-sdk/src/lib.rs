//! Compile fixture for Tauri's managed state and command registration.
//! A real application supplies its own model, runtime, and Tauri context.
use std::sync::Arc;

use pi_rs_core::{AgentSession, Model, SessionBuilder, SessionControl};
use tokio::sync::Mutex;

struct AppState {
    session: Mutex<AgentSession>,
    control: SessionControl,
}

#[tauri::command]
async fn prompt(state: tauri::State<'_, AppState>, text: String) -> Result<String, String> {
    state
        .session
        .lock()
        .await
        .prompt(text)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn abort(state: tauri::State<'_, AppState>) {
    state.control.abort();
}

#[tauri::command]
async fn close(state: tauri::State<'_, AppState>) -> Result<(), String> {
    state.control.abort();
    state
        .session
        .lock()
        .await
        .close()
        .await
        .map_err(|error| error.to_string())
}

/// Register a single in-memory session and its commands on a host-supplied builder.
/// This installs the builder's invoke handler, replacing any existing handler.
/// Applications with other commands must register them in the same handler.
pub fn configure<R: tauri::Runtime>(
    builder: tauri::Builder<R>,
    model: Arc<dyn Model>,
    model_name: &str,
) -> pi_rs_core::Result<tauri::Builder<R>> {
    let session = SessionBuilder::new(model, model_name).in_memory()?;
    let control = session.controls();
    Ok(builder
        .manage(AppState {
            session: Mutex::new(session),
            control,
        })
        .invoke_handler(tauri::generate_handler![prompt, abort, close]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs_core::{ModelEvent, ModelRequest, ModelStream};
    use tauri::Manager;

    struct Fixture;
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
    fn mock_ipc_close_aborts_a_stalled_prompt() {
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
        close_thread.join().unwrap();
        prompt_thread.join().unwrap();
        assert!(
            tauri::test::get_ipc_response(
                &webview,
                request("prompt", serde_json::json!({"text":"after close"}))
            )
            .is_err()
        );
    }
}
