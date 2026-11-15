use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, ExtensionState, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolReply, ToolSpec,
};
use pi_rs_durable::{
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Options, RegisteredTool, Scheduler},
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let done = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(if done { "done" } else { "" }.into())),
            Ok(ModelEvent::Complete {
                calls: if done {
                    vec![]
                } else {
                    vec![ToolCall {
                        id: "call".into(),
                        name: "parent".into(),
                        arguments: "{}".into(),
                    }]
                },
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }
}
fn log(root: &Path, name: &str) {
    use std::io::Write;
    writeln!(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(name))
            .unwrap(),
        "effect"
    )
    .unwrap();
}
fn count(root: &Path, name: &str) -> usize {
    std::fs::read_to_string(root.join(name))
        .unwrap_or_default()
        .lines()
        .count()
}
struct Parent {
    root: PathBuf,
    stage: String,
    version: Option<u32>,
}
#[async_trait]
impl KeyedTool for Parent {
    fn spec(&self) -> ToolSpec {
        let mut spec = spec("parent");
        if self.stage == "changed-spec" {
            spec.description = "changed".into();
        }
        spec
    }
    fn unsafe_park_version(&self) -> Option<u32> {
        self.version
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if let Some(wake) = host.pending_wake() {
            log(&self.root, "wake-effects");
            if self.stage == "wake" {
                std::fs::write(self.root.join("wake-running"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            let child = host
                .call_keyed_unpolled_with_id("child", "child", json!({}))
                .await
                .unwrap();
            assert_eq!(wake.checkpoint, json!({"child":child.operation_id}));
            assert_eq!(wake.ended, vec![child.operation_id]);
            return match child.outcome {
                Outcome::Succeeded(value) => Outcome::Succeeded(format!("resumed:{value}")).into(),
                other => Outcome::Failed(format!("unexpected child: {other:?}")).into(),
            };
        }
        log(&self.root, "initial-effects");
        let child = host
            .call_keyed_unpolled_with_id("child", "child", json!({}))
            .await
            .unwrap();
        assert!(matches!(child.outcome, Outcome::Pending { .. }));
        if self.stage == "before-park" {
            std::fs::write(self.root.join("before-park"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        if self.stage == "without-checkpoint" {
            host.suspend_pending().unwrap();
        } else {
            host.suspend_pending_with_checkpoint(json!({"child":child.operation_id}))
                .unwrap();
        }
        Outcome::Succeeded("discarded".into()).into()
    }
    async fn invoke_controlled(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> (ToolReply, pi_rs_durable::reset::ToolControl) {
        let reply = self.invoke(arguments, context, state, host).await;
        let control = pi_rs_durable::reset::ToolControl {
            terminate: self.stage == "with-control",
            ..Default::default()
        };
        (reply, control)
    }
}
struct Child(PathBuf);
#[async_trait]
impl Tool for Child {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        log(&self.0, "child-effects");
        Outcome::Pending {
            reference: "remote".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        std::fs::write(self.0.join("poll-entered"), b"ready").unwrap();
        Some(if self.0.join("finish").exists() {
            Outcome::Succeeded("result".into())
        } else {
            Outcome::Pending {
                reference: "remote".into(),
            }
        })
    }
}
fn options(root: &Path, stage: &str, version: Option<u32>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(Parent {
                root: root.into(),
                stage: stage.into(),
                version,
            }))
            .register(ReplayPolicy::Unsafe),
            RegisteredTool {
                tool: Arc::new(Child(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timeout");
}
async fn wait(s: &mut Scheduler, id: Uuid) {
    tokio::time::timeout(Duration::from_secs(15), s.wait(id))
        .await
        .unwrap()
        .unwrap();
}
struct Kill(std::process::Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn(root: &Path, stage: &str) -> Kill {
    Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "unsafe_park_kill_worker"])
            .env("PI_UNSAFE_PARK_DIR", root)
            .env("PI_UNSAFE_PARK_STAGE", stage)
            .spawn()
            .unwrap(),
    )
}
fn kill(child: &mut Kill) {
    child.0.kill().unwrap();
    child.0.wait().unwrap();
}
fn id(root: &Path) -> Uuid {
    std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn unsafe_park_kill_worker() {
    let Some(root) = std::env::var_os("PI_UNSAFE_PARK_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let stage = std::env::var("PI_UNSAFE_PARK_STAGE").unwrap();
    let mut scheduler = Scheduler::open(root.join("db"), options(&root, &stage, Some(1))).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn parked_unsafe_parent_resumes_once_after_process_death_with_new_attempt() {
    use pi_rs_durable::observer::Cursor;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut child = spawn(root, "park");
    until(|| root.join("poll-entered").exists()).await;
    kill(&mut child);
    let id = id(root);
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let mut opts = options(root, "finish", Some(1));
    // A committed park is not an interrupted invocation.
    opts.restart_limit = 1;
    let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content,.. } if content=="resumed:result"))
    );
    assert_eq!(
        (
            count(root, "initial-effects"),
            count(root, "wake-effects"),
            count(root, "child-effects")
        ),
        (1, 1, 1)
    );
    let attempts = scheduler
        .tool_attempts(id, Cursor(0), std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .records;
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].value["attempt"]["operation_id"],
        attempts[1].value["attempt"]["operation_id"]
    );
    assert_ne!(
        attempts[0].value["attempt"]["attempt_id"],
        attempts[1].value["attempt"]["attempt_id"]
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsafe_running_crashes_before_park_or_after_wake_never_replay_effects() {
    for stage in ["before-park", "wake"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut child = spawn(root, stage);
        if stage == "wake" {
            until(|| root.join("poll-entered").exists()).await;
            std::fs::write(root.join("finish"), b"ready").unwrap();
            until(|| root.join("wake-running").exists()).await;
        } else {
            until(|| root.join("before-park").exists()).await;
        }
        kill(&mut child);
        let id = id(root);
        let mut scheduler =
            Scheduler::open(root.join("db"), options(root, "finish", Some(1))).unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_some());
        assert!(!scheduler.conversation(id).unwrap().nested_suspended());
        assert_eq!(count(root, "initial-effects"), 1);
        assert_eq!(count(root, "wake-effects"), usize::from(stage == "wake"));
        assert_eq!(count(root, "child-effects"), 1);
        scheduler.shutdown().await.unwrap();
        let mut scheduler =
            Scheduler::open(root.join("db"), options(root, "finish", Some(1))).unwrap();
        wait(&mut scheduler, id).await;
        assert_eq!(count(root, "initial-effects"), 1);
        assert_eq!(count(root, "wake-effects"), usize::from(stage == "wake"));
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_park_abort_or_definition_change_fences_resume() {
    for mode in ["abort", "version", "spec", "policy", "unregistered"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut child = spawn(root, "park");
        until(|| root.join("poll-entered").exists()).await;
        kill(&mut child);
        let id = id(root);
        if mode != "abort" {
            std::fs::write(root.join("finish"), b"ready").unwrap();
        }
        let version = match mode {
            "version" => Some(2),
            "unregistered" => None,
            _ => Some(1),
        };
        let stage = if mode == "spec" {
            "changed-spec"
        } else {
            "finish"
        };
        let mut opts = options(root, stage, version);
        if mode == "policy" {
            opts.tools[0].replay = ReplayPolicy::Safe;
        }
        let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
        if mode == "abort" {
            scheduler.abort(id).unwrap();
        } else {
            let root_op = scheduler.blocked(id).unwrap().unwrap().0;
            until(|| {
                scheduler
                    .nested_pending_jobs(id, root_op)
                    .is_ok_and(|jobs| jobs.is_empty())
            })
            .await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(count(root, "initial-effects"), 1);
        assert_eq!(count(root, "wake-effects"), 0);
        assert_eq!(count(root, "child-effects"), 1);
        assert!(scheduler.conversation(id).unwrap().nested_suspended());
        scheduler.shutdown().await.unwrap();
        std::fs::write(root.join("finish"), b"ready").unwrap();
        let mut reopened =
            Scheduler::open(root.join("db"), options(root, "finish", Some(1))).unwrap();
        wait(&mut reopened, id).await;
        if mode == "abort" {
            assert!(reopened.conversation(id).unwrap().abort_requested);
            assert_eq!(count(root, "wake-effects"), 0);
        } else {
            assert!(reopened.blocked(id).unwrap().is_none(), "{mode}");
            assert_eq!(count(root, "wake-effects"), 1);
        }
        assert_eq!(count(root, "initial-effects"), 1);
        assert_eq!(count(root, "child-effects"), 1);
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_park_requires_opt_in_checkpoint_and_no_control() {
    for (stage, version) in [
        ("finish", None),
        ("finish", Some(0)),
        ("without-checkpoint", Some(1)),
        ("with-control", Some(1)),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut scheduler =
            Scheduler::open(root.join("db"), options(root, stage, version)).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        until(|| scheduler.blocked(id).unwrap().is_some()).await;
        assert!(
            !scheduler.conversation(id).unwrap().nested_suspended(),
            "{stage}"
        );
        std::fs::write(root.join("finish"), b"ready").unwrap();
        scheduler.schedule(id);
        wait(&mut scheduler, id).await;
        assert_eq!(
            (
                count(root, "initial-effects"),
                count(root, "wake-effects"),
                count(root, "child-effects")
            ),
            (1, 0, 1),
            "{stage}"
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn interrupted_result_policy_never_replays_unsafe_prepark_effect() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut child = spawn(root, "before-park");
    until(|| root.join("before-park").exists()).await;
    kill(&mut child);
    let id = id(root);
    let mut opts = options(root, "finish", Some(1));
    opts.uncertain_policy = UncertainPolicy::InterruptedResult;
    let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
    wait(&mut scheduler, id).await;
    assert!(!scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        (
            count(root, "initial-effects"),
            count(root, "wake-effects"),
            count(root, "child-effects")
        ),
        (1, 0, 1)
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_after_unsafe_wake_started_never_replays_it_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut scheduler = Scheduler::open(root.join("db"), options(root, "wake", Some(1))).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| root.join("poll-entered").exists()).await;
    std::fs::write(root.join("finish"), b"ready").unwrap();
    until(|| root.join("wake-running").exists()).await;
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        (
            count(root, "initial-effects"),
            count(root, "wake-effects"),
            count(root, "child-effects")
        ),
        (1, 1, 1)
    );
    scheduler.shutdown().await.unwrap();
    let mut reopened = Scheduler::open(root.join("db"), options(root, "finish", Some(1))).unwrap();
    wait(&mut reopened, id).await;
    assert_eq!(
        (
            count(root, "initial-effects"),
            count(root, "wake-effects"),
            count(root, "child-effects")
        ),
        (1, 1, 1)
    );
    reopened.shutdown().await.unwrap();
}
