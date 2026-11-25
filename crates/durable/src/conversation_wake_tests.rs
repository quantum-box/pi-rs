use super::*;
use crate::task::{Checkpoint, Task, TaskOwner};
use crate::task_store::{Save, TaskStore};
use durable_store::{Mutation, Store};
use serde_json::json;

fn conversation() -> Conversation {
    Harness::empty_conversation(
        Uuid::new_v4(),
        Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 1,
        },
    )
}
fn save(store: &Store, c: &Conversation) {
    store
        .commit(
            &[],
            &[Mutation::Put {
                key: Harness::key(c.id),
                value: serde_json::to_vec(c).unwrap(),
            }],
        )
        .unwrap();
}
fn wake(store: &Store, c: &Conversation, now: i64) -> ConversationWake {
    conversation_wake(store, c.id, now).unwrap()
}
#[test]
fn persisted_phases_deadlines_signals_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let mut c = conversation();
    save(&store, &c);
    assert_eq!(wake(&store, &c, 0), ConversationWake::Parked { at: None });
    c.phase = Phase::ModelReady { turns: 0 };
    save(&store, &c);
    assert_eq!(wake(&store, &c, 0), ConversationWake::Ready);
    c.phase = Phase::RetryWait { turns: 0 };
    c.retry_state.deadline_ms = Some(100);
    save(&store, &c);
    assert_eq!(
        wake(&store, &c, 99),
        ConversationWake::Parked { at: Some(100) }
    );
    assert_eq!(wake(&store, &c, 100), ConversationWake::Ready);
    c.retry_state.deadline_ms = None;
    save(&store, &c);
    assert_eq!(wake(&store, &c, 100), ConversationWake::Unknown);
    c.phase = Phase::ToolGateSleeping {
        turns: 0,
        calls: vec![],
        index: 0,
        operation_id: Uuid::new_v4(),
        until_millis: Some(200),
        signals: vec!["go".into()],
    };
    save(&store, &c);
    assert_eq!(
        wake(&store, &c, 199),
        ConversationWake::Parked { at: Some(200) }
    );
    assert_eq!(wake(&store, &c, 200), ConversationWake::Ready);
    let signal =
        crate::conversation_signal::Signal::new(Uuid::new_v4(), c.id, "go".into(), json!(1));
    crate::conversation_signal::Inbox::new(&store)
        .send_at(&signal, 100)
        .unwrap();
    let seq = store.commit_sequence().unwrap();
    assert_eq!(wake(&store, &c, 100), ConversationWake::Ready);
    assert_eq!(wake(&store, &c, 100), ConversationWake::Ready);
    assert_eq!(store.commit_sequence().unwrap(), seq);
    c.phase = Phase::ToolGateSleeping {
        turns: 0,
        calls: vec![],
        index: 0,
        operation_id: Uuid::new_v4(),
        until_millis: None,
        signals: vec!["other".into()],
    };
    save(&store, &c);
    assert_eq!(wake(&store, &c, 100), ConversationWake::Parked { at: None });
    c.phase = Phase::FailedIdle;
    save(&store, &c);
    assert_eq!(wake(&store, &c, 100), ConversationWake::Parked { at: None });
    c.phase = Phase::ModelRunning {
        turns: 0,
        request_id: Uuid::new_v4(),
        restarts: 0,
    };
    save(&store, &c);
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(wake(&store, &c, 100), ConversationWake::Unknown);
    c.abort_requested = true;
    save(&store, &c);
    assert_eq!(wake(&store, &c, 100), ConversationWake::Unknown);
}

#[test]
fn child_waits_require_all_terminal_and_validate_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db")).unwrap();
    let mut c = conversation();
    save(&store, &c);
    let id = Uuid::new_v4();
    let mut child = Task::new(
        id,
        TaskOwner::Conversation(c.id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        1,
    )
    .unwrap();
    let tasks = TaskStore::new(&store);
    tasks.save(&[Save::Create(&child)]).unwrap();
    c.phase = Phase::HookTasksWaiting {
        children: vec![id],
        turns: None,
    };
    save(&store, &c);
    assert_eq!(wake(&store, &c, 0), ConversationWake::Parked { at: None });
    child.start().unwrap();
    child.succeed(json!(null)).unwrap();
    let mut saved = tasks.load(id).unwrap().unwrap();
    saved.task = child;
    tasks.save(&[Save::Update(&saved)]).unwrap();
    assert_eq!(wake(&store, &c, 0), ConversationWake::Ready);
    let other_id = Uuid::new_v4();
    let other = Task::new(
        other_id,
        TaskOwner::Conversation(c.id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        1,
    )
    .unwrap();
    tasks.save(&[Save::Create(&other)]).unwrap();
    c.phase = Phase::YieldTasksWaiting {
        turns: 0,
        answer: "a".into(),
        children: vec![id, other_id],
    };
    save(&store, &c);
    assert_eq!(wake(&store, &c, 0), ConversationWake::Parked { at: None });
    c.phase = Phase::YieldTasksWaiting {
        turns: 0,
        answer: "a".into(),
        children: vec![id, Uuid::new_v4()],
    };
    save(&store, &c);
    assert!(conversation_wake(&store, c.id, 0).is_err());
}

#[tokio::test]
async fn runtime_wake_combines_deadlines_and_preserves_unknown_states() {
    struct ModelStub;
    #[async_trait::async_trait]
    impl Model for ModelStub {
        async fn stream(
            &self,
            _: pi_rs_core::ModelRequest,
        ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
            unreachable!("wake inspection does not call the model")
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let owner = RuntimeOwner::open(
        dir.path().join("db"),
        Options {
            model: Arc::new(ModelStub),
            tools: vec![],
            restart_limit: 3,
            max_turns: 5,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let store = owner.scheduler.harness.owner.store();
    let mut sleeping = conversation();
    sleeping.phase = Phase::RetryWait { turns: 0 };
    sleeping.retry_state.deadline_ms = Some(100);
    save(store, &sleeping);
    let mut uncertain = conversation();
    uncertain.abort_requested = true;
    save(store, &uncertain);
    let snapshot = owner.wake_snapshot(99).unwrap();
    assert!(!snapshot.active);
    assert_eq!(snapshot.at, Some(100));
    assert_eq!(snapshot.unknown_conversations, vec![uncertain.id]);
    let due = owner.wake_snapshot(100).unwrap();
    assert!(due.active);
    assert_eq!(due.at, None);
    assert_eq!(due.unknown_conversations, vec![uncertain.id]);
    owner.shutdown().await.unwrap();
}

struct WakeModelStub;
#[async_trait::async_trait]
impl Model for WakeModelStub {
    async fn stream(
        &self,
        _: pi_rs_core::ModelRequest,
    ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
        unreachable!()
    }
}
fn wake_owner(path: &std::path::Path) -> RuntimeOwner {
    RuntimeOwner::open(
        path,
        Options {
            model: Arc::new(WakeModelStub),
            tools: vec![],
            restart_limit: 3,
            max_turns: 5,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        },
        Default::default(),
        Default::default(),
    )
    .unwrap()
}
async fn next_wake(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::wake_delivery::Wake>,
) -> crate::wake_delivery::Wake {
    tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn subscription_initial_changes_unknown_and_shutdown() {
    use crate::wake_delivery::Wake;
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let store = owner.scheduler.harness.owner.store();
    let mut uncertain = conversation();
    uncertain.abort_requested = true;
    save(store, &uncertain);
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(99));
    let now = clock.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let subscription = owner
        .subscribe_wake(
            move || now.load(std::sync::atomic::Ordering::SeqCst),
            move |w| {
                tx.send(w).unwrap();
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: true,
            at: None
        }
    );
    uncertain.abort_requested = false;
    uncertain.phase = Phase::RetryWait { turns: 0 };
    uncertain.retry_state.deadline_ms = Some(100);
    save(store, &uncertain);
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: false,
            at: Some(100)
        }
    );
    clock.store(100, std::sync::atomic::Ordering::SeqCst);
    subscription.kick();
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: true,
            at: None
        }
    );
    subscription.shutdown();
    uncertain.phase = Phase::ModelReady { turns: 0 };
    save(store, &uncertain);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_err()
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscription_callback_failure_kick_and_reentrant_shutdown() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let slot: Arc<std::sync::Mutex<Option<WakeSubscription>>> =
        Arc::new(std::sync::Mutex::new(None));
    let callback_slot = slot.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (errors_tx, mut errors_rx) = tokio::sync::mpsc::unbounded_channel();
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let subscription = owner
        .subscribe_wake(
            || 0,
            move |_| {
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("failed".into());
                }
                let slot = callback_slot.lock().unwrap();
                let handle = slot.as_ref().unwrap();
                handle.kick();
                handle.shutdown();
                done_tx.send(()).unwrap();
                Ok(())
            },
            move |error| {
                errors_tx.send(error).unwrap();
            },
        )
        .unwrap();
    *slot.lock().unwrap() = Some(subscription);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), errors_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        WakeSubscriptionError::Delivery(crate::wake_delivery::DeliveryError::Rejected(
            "failed".into()
        ))
    );
    slot.lock().unwrap().as_ref().unwrap().kick();
    tokio::time::timeout(std::time::Duration::from_secs(5), done_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshot_failure_is_conservatively_active_and_recovers() {
    use crate::wake_delivery::Wake;
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let clock_fail = fail.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (errors_tx, mut errors_rx) = tokio::sync::mpsc::unbounded_channel();
    let sub = owner
        .subscribe_wake(
            move || {
                assert!(
                    !clock_fail.load(std::sync::atomic::Ordering::SeqCst),
                    "clock failure"
                );
                0
            },
            move |w| {
                tx.send(w).unwrap();
                Ok(())
            },
            move |e| {
                errors_tx.send(e).unwrap();
            },
        )
        .unwrap();
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: false,
            at: None
        }
    );
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    sub.kick();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), errors_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        WakeSubscriptionError::Snapshot(_)
    ));
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: true,
            at: None
        }
    );
    fail.store(false, std::sync::atomic::Ordering::SeqCst);
    sub.kick();
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: false,
            at: None
        }
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn owner_shutdown_stops_subscription_even_if_handle_survives() {
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sub = owner
        .subscribe_wake(
            || 0,
            move |w| {
                tx.send(w).unwrap();
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    next_wake(&mut rx).await;
    owner.shutdown().await.unwrap();
    sub.kick();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn subscription_includes_persisted_task_deadline() {
    use crate::{task::WaitFor, wake_delivery::Wake};
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _sub = owner
        .subscribe_wake(
            || 0,
            move |w| {
                tx.send(w).unwrap();
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: false,
            at: None
        }
    );
    let mut task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(Uuid::new_v4()),
        Checkpoint {
            phase: "sleep".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("missing".into(), true);
    task.start().unwrap();
    task.wait(WaitFor::DeadlineMillis(4_000_000_000_000))
        .unwrap();
    TaskStore::new(owner.scheduler.harness.owner.store())
        .save(&[Save::Create(&task)])
        .unwrap();
    assert_eq!(
        next_wake(&mut rx).await,
        Wake {
            active: false,
            at: Some(4_000_000_000_000)
        }
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn owner_drop_and_initial_error_do_not_start_delivery() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let sub = owner
        .subscribe_wake(
            || 0,
            move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    drop(owner);
    sub.kick();
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(sub);

    let owner = wake_owner(&dir.path().join("other"));
    let count = calls.clone();
    assert!(
        owner
            .subscribe_wake(
                || panic!("initial clock failure"),
                move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_| {}
            )
            .is_err()
    );
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_one_subscription_leaves_other_subscriptions_running() {
    use crate::wake_delivery::Wake;
    let dir = tempfile::tempdir().unwrap();
    let owner = wake_owner(&dir.path().join("db"));
    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
    let first = owner
        .subscribe_wake(
            || 0,
            move |w| {
                let _ = tx1.send(w);
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    let second = owner
        .subscribe_wake(
            || 0,
            move |w| {
                let _ = tx2.send(w);
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    next_wake(&mut rx1).await;
    next_wake(&mut rx2).await;
    drop(first);
    let mut c = conversation();
    c.phase = Phase::RetryWait { turns: 0 };
    c.retry_state.deadline_ms = Some(100);
    save(owner.scheduler.harness.owner.store(), &c);
    assert_eq!(
        next_wake(&mut rx2).await,
        Wake {
            active: false,
            at: Some(100)
        }
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), rx1.recv())
            .await
            .unwrap()
            .is_none()
    );
    drop(second);
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_wake_retries_overlapping_conversation_commits() {
    struct ModelStub;
    #[async_trait::async_trait]
    impl Model for ModelStub {
        async fn stream(
            &self,
            _: pi_rs_core::ModelRequest,
        ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
            unreachable!()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let owner = RuntimeOwner::open(
        dir.path().join("db"),
        Options {
            model: Arc::new(ModelStub),
            tools: vec![],
            restart_limit: 3,
            max_turns: 5,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let mut first = conversation();
    let mut second = conversation();
    first.phase = Phase::RetryWait { turns: 0 };
    second.phase = Phase::RetryWait { turns: 0 };
    first.retry_state.deadline_ms = Some(100);
    second.retry_state.deadline_ms = Some(300);
    let store_owner = owner.scheduler.harness.owner.clone();
    let write = |first: &Conversation, second: &Conversation| {
        vec![
            Mutation::Put {
                key: Harness::key(first.id),
                value: serde_json::to_vec(first).unwrap(),
            },
            Mutation::Put {
                key: Harness::key(second.id),
                value: serde_json::to_vec(second).unwrap(),
            },
        ]
    };
    let a = write(&first, &second);
    first.retry_state.deadline_ms = Some(300);
    second.retry_state.deadline_ms = Some(200);
    let b = write(&first, &second);
    store_owner.store().commit(&[], &a).unwrap();
    let (trigger, wait_for_trigger) = std::sync::mpsc::sync_channel::<()>(1);
    let (committed, wait_for_commit) = std::sync::mpsc::sync_channel::<()>(1);
    let timeout = std::time::Duration::from_secs(5);
    let writer = std::thread::spawn(move || {
        wait_for_trigger.recv_timeout(timeout).unwrap();
        store_owner.store().commit(&[], &b).unwrap();
        committed.send(()).unwrap();
    });
    let driver = owner.driver.weak().upgrade().unwrap();
    let mut reads = 0;
    let snapshot = runtime_wake_snapshot_with_observer(
        &owner.scheduler.harness.owner,
        &owner.scheduler.worker_activity(),
        &driver,
        0,
        |candidate| {
            reads += 1;
            if reads == 1 {
                // The first read observed A. Publish B before its sequence
                // check so the result can only be accepted after a retry.
                assert_eq!(candidate.as_ref().unwrap().at, Some(100));
                trigger.send(()).unwrap();
                wait_for_commit.recv_timeout(timeout).unwrap();
            }
        },
    )
    .unwrap();
    writer.join().unwrap();
    assert!(reads >= 2, "overlapping commit must cause a retry");
    assert_eq!(snapshot.at, Some(200));
    assert!(!snapshot.active);
    assert!(snapshot.unknown_conversations.is_empty());
    assert_eq!(owner.wake_snapshot(0).unwrap(), snapshot);
    owner.shutdown().await.unwrap();
}
