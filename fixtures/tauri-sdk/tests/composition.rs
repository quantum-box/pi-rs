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

#[test]
fn host_configured_persistent_session_uses_existing_commands() {
    use pi_rs_core::{Message, SessionBuilder};
    use std::sync::Mutex;
    struct CapturingModel(Arc<Mutex<Vec<ModelRequest>>>);
    #[async_trait::async_trait]
    impl Model for CapturingModel {
        async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            let response = Fixture.stream(request.clone()).await;
            self.0.lock().unwrap().push(request);
            response
        }
    }
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("pi-tauri-fixture-{}-{unique}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("session.jsonl");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(CapturingModel(requests.clone()));
    let session = SessionBuilder::new(model.clone(), "fixture")
        .system_prompt("host-approved prompt")
        .open(&path)
        .unwrap();
    let state = pi_rs_tauri_sdk_fixture::session_state_from_session(session);
    let app = tauri::test::mock_builder()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            pi_rs_tauri_sdk_fixture::commands::prompt,
            pi_rs_tauri_sdk_fixture::commands::abort,
            pi_rs_tauri_sdk_fixture::commands::close,
        ])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .unwrap();
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
        .build()
        .unwrap();
    let answer = tauri::test::get_ipc_response(
        &webview,
        request("prompt", serde_json::json!({"text":"hello"})),
    )
    .unwrap();
    assert_eq!(answer.deserialize::<String>().unwrap(), "answer");
    assert!(requests.lock().unwrap()[0].messages.iter().any(|message| matches!(message, Message::System { content } if content == "host-approved prompt")));
    assert!(
        tauri::test::get_ipc_response(&webview, request("close", serde_json::json!({}))).is_ok()
    );
    assert!(
        tauri::test::get_ipc_response(&webview, request("close", serde_json::json!({}))).is_ok()
    );
    let reopened = SessionBuilder::new(model, "fixture").open(&path).unwrap();
    let messages = reopened.messages().unwrap();
    assert!(
        messages
            .iter()
            .any(|message| matches!(message, Message::User { content } if content == "hello"))
    );
    assert!(messages.iter().any(
        |message| matches!(message, Message::Assistant { content, .. } if content == "answer")
    ));
    drop(reopened);
    assert!(
        tauri::test::get_ipc_response(
            &webview,
            request("prompt", serde_json::json!({"text":"late"}))
        )
        .is_err()
    );
    drop(webview);
    drop(app);
    std::fs::remove_dir_all(dir).unwrap();
}
