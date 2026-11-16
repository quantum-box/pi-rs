//! The native state pattern used by the Tauri guide; no Tauri runtime required.
use std::sync::Arc;

use pi_rs::{
    AgentSession, Model, ModelEvent, ModelRequest, ModelStream, Result, SessionBuilder,
    SessionControl,
};
use tokio::sync::Mutex;

struct AppState {
    session: Mutex<AgentSession>,
    control: SessionControl,
}

// A deterministic stand-in; use a real provider client in an application.
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
        self.control.request_shutdown();
        self.session.lock().await.close().await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let state = AppState::new(Arc::new(Fixture), "fixture")?;
    println!("{}", state.prompt("Say hello.".into()).await?);
    state.abort();
    state.close().await
}

#[cfg(test)]
mod tests {
    use super::*;
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
    struct Stalled(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Model for Stalled {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            self.0.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn closing_requests_shutdown_before_waiting_for_the_session_lock() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(AppState::new(Arc::new(Stalled(entered.clone())), "fixture").unwrap());
        let active = {
            let state = state.clone();
            tokio::spawn(async move { state.prompt("waiting".into()).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), state.close())
            .await
            .unwrap()
            .unwrap();
        assert!(active.await.unwrap().is_err());
    }
}
