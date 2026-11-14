//! The native state pattern used by the Tauri guide; no Tauri runtime required.
use std::sync::Arc;

use pi_rs::{AgentSession, Model, Result, SessionBuilder, SessionControl, ollama::Ollama};
use tokio::sync::Mutex;

struct AppState {
    session: Mutex<AgentSession>,
    control: SessionControl,
}

impl AppState {
    fn new(model: Arc<dyn Model>, model_name: &str) -> Result<Self> {
        let session = SessionBuilder::new(model, model_name).in_memory()?;
        let control = session.controls();
        Ok(Self {
            session: Mutex::new(session),
            control,
        })
    }

    async fn prompt(&self, text: String) -> Result<String> {
        self.session.lock().await.prompt(text).await
    }

    fn abort(&self) {
        self.control.abort();
    }

    async fn close(&self) -> Result<()> {
        self.session.lock().await.close().await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let model = Arc::new(Ollama::new(
        "http://127.0.0.1:11434/v1/chat/completions",
        None,
    )?);
    let state = AppState::new(model, "qwen2.5:7b")?;
    println!("{}", state.prompt("Say hello.".into()).await?);
    state.abort();
    state.close().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs::{ModelEvent, ModelRequest, ModelStream};

    struct Fixture;
    #[async_trait::async_trait]
    impl Model for Fixture {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("fixture answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }

    #[tokio::test]
    async fn host_state_is_shareable_and_preserves_session_across_prompts() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AppState>();
        let state = AppState::new(Arc::new(Fixture), "fixture").unwrap();
        assert_eq!(
            state.prompt("first".into()).await.unwrap(),
            "fixture answer"
        );
        assert_eq!(
            state.prompt("second".into()).await.unwrap(),
            "fixture answer"
        );
        let session = state.session.lock().await;
        assert_eq!(session.messages().unwrap().len(), 4);
        drop(session);
        state.close().await.unwrap();
    }
}
