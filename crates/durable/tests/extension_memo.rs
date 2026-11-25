use async_trait::async_trait;
use pi_rs_core::{Error, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    extension::Memo,
    recovery::UncertainPolicy,
    runtime::{Harness, Options},
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

struct UnusedModel;
#[async_trait]
impl Model for UnusedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        panic!("model must not be invoked")
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(UnusedModel),
        tools: vec![],
        restart_limit: 2,
        max_turns: 2,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}

#[tokio::test]
async fn memo_is_scoped_atomic_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let task = Uuid::new_v4();
    {
        let h = Harness::open_paused(&path, options()).unwrap();
        let memo = h.extension_memo(task).unwrap();
        let other = h.extension_memo(Uuid::new_v4()).unwrap();
        let (a, b) = tokio::join!(
            memo.get_or_insert("key", json!(1)),
            memo.get_or_insert("key", json!(2))
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(other.get("key").await.unwrap(), None);
    }
    let h = Harness::open_paused(&path, options()).unwrap();
    let memo = h.extension_memo(task).unwrap();
    assert_eq!(memo.get("key").await.unwrap(), Some(json!(1)));
    assert_eq!(memo.get_or_insert("key", json!(9)).await.unwrap(), json!(1));
}

#[tokio::test]
async fn memo_crash_child() {
    let Ok(path) = std::env::var("PI_MEMO_CRASH_DB") else {
        return;
    };
    let h = Harness::open_paused(path, options()).unwrap();
    let memo = h.extension_memo(Uuid::nil()).unwrap();
    assert_eq!(
        memo.get_or_insert("committed", json!(42)).await.unwrap(),
        json!(42)
    );
    std::process::abort();
}

#[tokio::test]
async fn committed_memo_survives_process_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "memo_crash_child", "--nocapture"])
        .env("PI_MEMO_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(!status.success());
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.extension_memo(Uuid::nil())
            .unwrap()
            .get("committed")
            .await
            .unwrap(),
        Some(json!(42))
    );
}
