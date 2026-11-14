use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Harness, InputMode, Options, Scheduler, Status},
    subagent::ReportedInput,
};
use std::{process::Command, sync::Arc};
use uuid::Uuid;

struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "fake".into(),
        system_prompt: "child".into(),
        max_output_tokens: 100,
    }
}

#[test]
fn crash_worker() {
    let Ok(path) = std::env::var("SUBAGENT_CRASH_DB") else {
        return;
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h.create_conversation(config()).unwrap();
    let (link, submission) = h
        .submit_subagent(
            parent,
            "reader",
            config(),
            "request",
            "hello".into(),
            InputMode::FollowUp,
        )
        .unwrap();
    std::fs::write(
        format!("{path}.ids"),
        format!("{parent} {} {submission}", link.child),
    )
    .unwrap();
    // Kill this process without running destructors or closing SQLite.
    Command::new("kill")
        .args(["-KILL", &std::process::id().to_string()])
        .status()
        .unwrap();
    panic!("SIGKILL did not terminate the worker");
}

#[tokio::test]
async fn process_death_reopens_same_child_and_submission_and_runs_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_worker"])
        .env("SUBAGENT_CRASH_DB", &path)
        .status()
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    #[cfg(not(unix))]
    assert!(!status.success());
    let ids = std::fs::read_to_string(format!("{}.ids", path.display())).unwrap();
    let ids: Vec<Uuid> = ids.split_whitespace().map(|s| s.parse().unwrap()).collect();
    let mut s = Scheduler::open(&path, options()).unwrap();
    let (link, submission) = s
        .submit_subagent(
            ids[0],
            "reader",
            config(),
            "request",
            "changed".into(),
            InputMode::Steer,
        )
        .unwrap();
    assert_eq!((link.child, submission), (ids[1], ids[2]));
    s.wait(link.child).await.unwrap();
    let child = s.conversation(link.child).unwrap();
    assert_eq!(child.submissions.len(), 1);
    assert_eq!(child.submissions[0].content, "hello");
    assert!(matches!(child.submissions[0].status, Status::Done { .. }));
    assert!(
        s.ensure_subagent(
            ids[0],
            "reader",
            Config {
                model: "other".into(),
                ..config()
            }
        )
        .is_err()
    );
    let other = s.create_conversation(config()).unwrap();
    assert_ne!(
        s.ensure_subagent(other, "reader", config()).unwrap().child,
        link.child
    );
    s.shutdown().await.unwrap();
}

#[test]
fn interrupted_reservation_finishes_creation_after_reopen() {
    use durable_store::{Check, Expected, Mutation, Store};
    use pi_rs_durable::subagent::Link;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let parent = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.create_conversation(config()).unwrap()
    };
    let link = Link {
        parent,
        key: "reader".into(),
        child: Uuid::new_v4(),
        config: config(),
    };
    {
        let db = Store::open(&path).unwrap();
        let key = format!("runtime/subagent/{parent}/reader").into_bytes();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&link).unwrap(),
            }],
        )
        .unwrap();
    }
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.subagent(parent, "reader").unwrap(), Some(link.clone()));
    assert!(h.conversation(link.child).is_err());
    assert_eq!(h.ensure_subagent(parent, "reader", config()).unwrap(), link);
    assert_eq!(h.ensure_subagent(parent, "reader", config()).unwrap(), link);
    assert_eq!(h.conversation(link.child).unwrap().id, link.child);
}

#[test]
fn reporter_crash_worker() {
    let Ok(path) = std::env::var("REPORTER_CRASH_DB") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h.create_conversation(config()).unwrap();
    let (link, first) = h
        .submit_subagent_reported(
            parent,
            "reader",
            config(),
            ReportedInput {
                history: false,
                request_id: "one".into(),
                content: "first".into(),
                mode: InputMode::FollowUp,
            },
        )
        .unwrap();
    let (_, second) = h
        .submit_subagent_reported(
            parent,
            "reader",
            config(),
            ReportedInput {
                history: false,
                request_id: "two".into(),
                content: "steer".into(),
                mode: InputMode::Steer,
            },
        )
        .unwrap();
    runtime
        .block_on(h.resume_conversation(link.child, &pi_rs_core::CancellationToken::new()))
        .unwrap();
    assert_eq!(
        h.conversation(link.child).unwrap().answer_id(first),
        h.conversation(link.child).unwrap().answer_id(second)
    );
    std::fs::write(format!("{path}.ids"), format!("{parent} {}", link.child)).unwrap();
    Command::new("kill")
        .args(["-KILL", &std::process::id().to_string()])
        .status()
        .unwrap();
    panic!("SIGKILL did not terminate worker");
}

#[tokio::test]
async fn crash_after_answer_reports_shared_answer_once_and_retries_receipt() {
    use durable_store::{Check, Expected, Mutation, Store};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "reporter_crash_worker"])
        .env("REPORTER_CRASH_DB", &path)
        .status()
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    let ids = std::fs::read_to_string(format!("{}.ids", path.display())).unwrap();
    let ids: Vec<Uuid> = ids.split_whitespace().map(|s| s.parse().unwrap()).collect();
    let scheduler = Scheduler::open(&path, options()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let parent = scheduler.conversation(ids[0]).unwrap();
            if parent
                .submissions
                .iter()
                .any(|s| matches!(s.status, Status::Done { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    scheduler.shutdown().await.unwrap();
    // Simulate a crash after parent admission but before reporter receipt.
    let db = Store::open(&path).unwrap();
    let records = db.list(b"runtime/subagent-report/").unwrap();
    assert_eq!(records.len(), 2);
    for (key, entry) in records {
        let mut record: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        record["delivered"] = false.into();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&record).unwrap(),
            }],
        )
        .unwrap();
    }
    drop(db);
    let scheduler = Scheduler::open(&path, options()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let db = Store::open(&path).unwrap();
            let records = db.list(b"runtime/subagent-report/").unwrap();
            if records.iter().all(|(_, e)| {
                serde_json::from_slice::<serde_json::Value>(&e.value).unwrap()["delivered"] == true
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let parent = scheduler.conversation(ids[0]).unwrap();
    assert_eq!(parent.submissions.len(), 1);
    assert!(
        parent.submissions[0]
            .content
            .contains("[subagent reader answered, no reply needed] answer")
    );
    assert!(
        scheduler.reporter_error().is_none(),
        "{:?}",
        scheduler.reporter_error()
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_fork_is_optional_and_requires_settled_parent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h.create_conversation(config()).unwrap();
    h.submit(parent, Some("input".into()), "original".into())
        .unwrap();
    assert!(
        h.ensure_subagent_with_history(parent, "fork", config(), true)
            .is_err()
    );
    h.resume_conversation(parent, &pi_rs_core::CancellationToken::new())
        .await
        .unwrap();
    let history = serde_json::to_value(&h.conversation(parent).unwrap().messages).unwrap();
    let fork = h
        .ensure_subagent_with_history(parent, "fork", config(), true)
        .unwrap();
    let empty = h.ensure_subagent(parent, "empty", config()).unwrap();
    assert_eq!(
        serde_json::to_value(&h.conversation(fork.child).unwrap().messages).unwrap(),
        history
    );
    assert!(h.conversation(fork.child).unwrap().submissions.is_empty());
    assert!(h.conversation(empty.child).unwrap().messages.is_empty());
    h.reset(parent, None).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.ensure_subagent_with_history(parent, "fork", config(), true)
            .unwrap(),
        fork
    );
    assert_eq!(
        serde_json::to_value(&h.conversation(fork.child).unwrap().messages).unwrap(),
        history
    );
    assert!(
        h.ensure_subagent_with_history(parent, "fork", config(), false)
            .is_err()
    );
}

#[tokio::test]
async fn restart_dispatches_persisted_reporter_without_child_admission() {
    use durable_store::{Check, Expected, Mutation, Store};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (parent, link) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let link = h.ensure_subagent(parent, "reader", config()).unwrap();
        (parent, link)
    };
    let db = Store::open(&path).unwrap();
    let key = format!("runtime/subagent-report/{}/request", link.child).into_bytes();
    let record = serde_json::json!({"link": link, "request_id":"request", "content":"recover me", "mode":"FollowUp", "delivered":false});
    db.commit(
        &[Check {
            key: key.clone(),
            expected: Expected::Missing,
        }],
        &[Mutation::Put {
            key,
            value: serde_json::to_vec(&record).unwrap(),
        }],
    )
    .unwrap();
    drop(db);
    let s = Scheduler::open(&path, options()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if s.conversation(parent)
                .unwrap()
                .submissions
                .iter()
                .any(|x| matches!(x.status, Status::Done { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(s.conversation(link.child).unwrap().submissions.len(), 1);
    assert_eq!(s.conversation(parent).unwrap().submissions.len(), 1);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_reservation_restores_snapshot_even_if_parent_changes() {
    use durable_store::{Mutation, Store};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (parent, link, original) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        h.submit(parent, None, "original".into()).unwrap();
        h.resume_conversation(parent, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        let original = serde_json::to_value(&h.conversation(parent).unwrap().messages).unwrap();
        let link = h
            .ensure_subagent_with_history(parent, "reader", config(), true)
            .unwrap();
        h.reset(parent, None).unwrap();
        (parent, link, original)
    };
    // Recreate the exact reservation-only state of a crash before child creation.
    let db = Store::open(&path).unwrap();
    let mut prefixes = vec![
        format!("runtime/conversation/{}", link.child).into_bytes(),
        format!("runtime/fork/{}/", link.child).into_bytes(),
    ];
    let mut observer = b"durable/observer/".to_vec();
    observer.extend_from_slice(link.child.as_bytes());
    observer.push(b'/');
    prefixes.push(observer);
    let mutations: Vec<_> = prefixes
        .iter()
        .flat_map(|prefix| db.list(prefix).unwrap())
        .map(|(key, _)| Mutation::Delete { key })
        .collect();
    db.commit(&[], &mutations).unwrap();
    drop(db);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.ensure_subagent_with_history(parent, "reader", config(), true)
            .unwrap(),
        link
    );
    assert_eq!(
        serde_json::to_value(&h.conversation(link.child).unwrap().messages).unwrap(),
        original
    );
    assert!(h.conversation(parent).unwrap().messages.is_empty());
}

#[tokio::test]
async fn dropping_idle_scheduler_releases_reporter_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(Scheduler::open(&path, options()).unwrap());
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if Harness::open_paused(&path, options()).is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
