#![cfg(unix)]

use durable_store::owner::{OwnedStore, OwnerError};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
use uuid::Uuid;

#[tokio::test]
async fn serializes_same_operation_but_not_other_operations() {
    let dir = tempfile::tempdir().unwrap();
    let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
    let id = Uuid::from_u128(1);
    let first = owner.summary_guard(id).await;
    let other = tokio::time::timeout(
        Duration::from_secs(1),
        owner.summary_guard(Uuid::from_u128(2)),
    )
    .await
    .unwrap();
    let (tx, mut rx) = oneshot::channel();
    let (started, ready) = oneshot::channel();
    let clone = owner.clone();
    let waiter = tokio::spawn(async move {
        started.send(()).unwrap();
        let _guard = clone.summary_guard(id).await;
        tx.send(()).unwrap();
    });
    ready.await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut rx)
        .await
        .is_err());
    let cancelled_owner = owner.clone();
    let cancelled_waiter = tokio::spawn(async move { cancelled_owner.summary_guard(id).await });
    tokio::task::yield_now().await;
    cancelled_waiter.abort();
    assert!(cancelled_waiter.await.is_err());
    drop(first);
    tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .unwrap()
        .unwrap();
    waiter.await.unwrap();
    drop(other);
}

#[tokio::test]
async fn cancellation_releases_guard_and_owner_for_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let owner = Arc::new(OwnedStore::open(&path).unwrap());
    let id = Uuid::from_u128(1);
    let (tx, rx) = oneshot::channel();
    let clone = owner.clone();
    let task = tokio::spawn(async move {
        let _guard = clone.summary_guard(id).await;
        tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    rx.await.unwrap();
    assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let guard = tokio::time::timeout(Duration::from_secs(1), owner.summary_guard(id))
        .await
        .unwrap();
    drop(owner);
    assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
    drop(guard);
    let reopened = Arc::new(OwnedStore::open(&path).unwrap());
    let _fresh = tokio::time::timeout(Duration::from_secs(1), reopened.summary_guard(id))
        .await
        .unwrap();
}
