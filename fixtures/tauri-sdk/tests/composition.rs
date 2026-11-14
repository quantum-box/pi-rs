//! Compile and dispatch an application command alongside the SDK commands.
use pi_rs_core::{Model, ModelEvent, ModelRequest, ModelStream};
use std::sync::Arc;

struct Fixture;
#[async_trait::async_trait]
impl Model for Fixture {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        Ok(Box::pin(futures_util::stream::iter([
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])))
    }
}

#[tauri::command]
fn app_status() -> &'static str {
    "ready"
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
fn application_and_sdk_commands_share_one_handler() {
    let state = pi_rs_tauri_sdk_fixture::session_state(Arc::new(Fixture), "fixture").unwrap();
    let app = tauri::test::mock_builder()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            app_status,
            pi_rs_tauri_sdk_fixture::commands::prompt,
            pi_rs_tauri_sdk_fixture::commands::abort,
            pi_rs_tauri_sdk_fixture::commands::close,
        ])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .unwrap();
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
        .build()
        .unwrap();
    let status =
        tauri::test::get_ipc_response(&webview, request("app_status", serde_json::json!({})))
            .unwrap();
    assert_eq!(status.deserialize::<String>().unwrap(), "ready");
    let answer = tauri::test::get_ipc_response(
        &webview,
        request("prompt", serde_json::json!({"text":"hello"})),
    )
    .unwrap();
    assert_eq!(answer.deserialize::<String>().unwrap(), "answer");
    assert!(
        tauri::test::get_ipc_response(&webview, request("close", serde_json::json!({}))).is_ok()
    );
}
