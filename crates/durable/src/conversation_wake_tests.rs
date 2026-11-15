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
    let running = Arc::new(AtomicBool::new(true));
    let writer_running = running.clone();
    let writer = std::thread::spawn(move || {
        while writer_running.load(Ordering::Acquire) {
            store_owner.store().commit(&[], &b).unwrap();
            store_owner.store().commit(&[], &a).unwrap();
        }
    });
    let mut inconsistent = false;
    for _ in 0..100 {
        if let Ok(snapshot) = owner.wake_snapshot(0) {
            inconsistent |= !matches!(snapshot.at, Some(100 | 200))
                || snapshot.active
                || !snapshot.unknown_conversations.is_empty();
        }
    }
    running.store(false, Ordering::Release);
    writer.join().unwrap();
    assert!(!inconsistent);
    assert!(matches!(
        owner.wake_snapshot(0).unwrap().at,
        Some(100 | 200)
    ));
    owner.shutdown().await.unwrap();
}
