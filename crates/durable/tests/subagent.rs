use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Harness, InputMode, Options, Scheduler, Status},
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
