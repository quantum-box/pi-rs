//! Windows-only checks for the unconnected experimental per-account mutex.
//! Tests use a random name, never the user's fixed production mutex name.
#![cfg(windows)]
#![allow(dead_code)]

#[path = "../src/mcp_shared_gate.rs"]
mod mcp_shared_gate;

#[tokio::test]
async fn mutex_serializes_tasks_and_cancelled_waiter_exits_while_first_is_held() {
    use mcp_shared_gate::windows_mutex::{WorkerHooks, acquire_named_for_test};
    use std::time::Duration;
    let name = format!("Global\\pi-rs-mcp-test-{}", uuid::Uuid::new_v4());
    let first = tokio::time::timeout(
        Duration::from_secs(10),
        acquire_named_for_test(name.clone(), None),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        !first.abandoned,
        "recovery is not implemented in this prototype"
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
    let waiter_name = name.clone();
    let waiting = tokio::spawn(async move {
        acquire_named_for_test(
            waiter_name,
            Some(WorkerHooks {
                wait_started: started_tx,
                exited: exited_tx,
            }),
        )
        .await
    });
    // The hook fires only after WaitForSingleObject returns WAIT_TIMEOUT,
    // establishing that the worker actually waited while first was held.
    tokio::time::timeout(Duration::from_secs(10), started_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(!waiting.is_finished());
    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    // Join notification proves the worker exited, with first still held.
    assert!(
        tokio::time::timeout(Duration::from_secs(10), exited_rx)
            .await
            .unwrap()
            .unwrap()
    );
    drop(first);
    let next = tokio::time::timeout(Duration::from_secs(10), acquire_named_for_test(name, None))
        .await
        .unwrap()
        .unwrap();
    assert!(!next.abandoned, "unexpected abandoned mutex");
}
