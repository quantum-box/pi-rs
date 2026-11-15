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
    identity: String,
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
    fn unsafe_park_identity(&self) -> Option<String> {
        Some(self.identity.clone())
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
            if self.stage == "pause-wake" {
                std::fs::write(self.root.join("wake-running"), b"ready").unwrap();
                while !self.root.join("release-wake").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            if self.stage == "wake" {
                std::fs::write(self.root.join("wake-running"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            let child = host
                .call_keyed_unpolled_with_id("child", "child", json!({}))
                .await
                .unwrap();
            assert_eq!(wake.checkpoint, json!({"child":child.operation_id}));
            if self.root.join("two-children").exists() {
                let second = host
                    .call_keyed_unpolled_with_id("child2", "child2", json!({}))
                    .await
                    .unwrap();
                assert!(wake.ended.contains(&child.operation_id));
                assert!(wake.ended.contains(&second.operation_id));
            } else {
                assert_eq!(wake.ended, vec![child.operation_id]);
            }
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
        if self.root.join("two-children").exists() {
            let second = host
                .call_keyed_unpolled_with_id("child2", "child2", json!({}))
                .await
                .unwrap();
            assert!(matches!(second.outcome, Outcome::Pending { .. }));
        }
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
        match self.stage.as_str() {
            "return-failed" => Outcome::Failed("parent explicit failure".into()).into(),
            "return-skipped" => Outcome::Skipped("parent explicit skip".into()).into(),
            _ => Outcome::Succeeded("discarded".into()).into(),
        }
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
struct SecondChild(PathBuf);
#[async_trait]
impl Tool for SecondChild {
    fn spec(&self) -> ToolSpec {
        spec("child2")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "remote2".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        Some(if self.0.join("finish").exists() {
            Outcome::Succeeded("result2".into())
        } else {
            Outcome::Pending {
                reference: "remote2".into(),
            }
        })
    }
}
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
        log(&self.0, "child-polls");
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
    options_custom(
        root,
        stage,
        version,
        "test-parent/tenant-a",
        Some("test-backend:child:v1"),
        ReplayPolicy::Unsafe,
    )
}
fn options_custom(
    root: &Path,
    stage: &str,
    version: Option<u32>,
    parent_id: &str,
    child_id: Option<&str>,
    policy: ReplayPolicy,
) -> Options {
    let parent = Arc::new(KeyedToolAdapter::new(Parent {
        root: root.into(),
        stage: stage.into(),
        version,
        identity: parent_id.into(),
    }))
    .register(policy);
    pi_rs_durable::nested_identity::register(&parent.tool, Some(parent_id.into())).unwrap();
    let child: Arc<dyn Tool> = Arc::new(Child(root.into()));
    pi_rs_durable::nested_identity::register(&child, child_id.map(str::to_owned)).unwrap();
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent,
            RegisteredTool {
                tool: child,
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

async fn start_parked(root: &Path, opts: Options) -> (Scheduler, Uuid, Uuid) {
    let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| {
        scheduler
            .conversation(id)
            .is_ok_and(|c| c.nested_suspended())
            && root.join("poll-entered").exists()
    })
    .await;
    let root_op = scheduler.blocked(id).unwrap().unwrap().0;
    (scheduler, id, root_op)
}

#[tokio::test]
async fn reopen_rejects_same_spec_different_parent_or_child_tenant_for_safe_and_unsafe() {
    for mode in ["unsafe-parent", "unsafe-child", "safe-child"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let policy = if mode == "safe-child" {
            ReplayPolicy::Safe
        } else {
            ReplayPolicy::Unsafe
        };
        let (scheduler, id, _) = start_parked(
            root,
            options_custom(
                root,
                "finish",
                Some(1),
                "parent/tenant-a",
                Some("child/tenant-a"),
                policy,
            ),
        )
        .await;
        scheduler.shutdown().await.unwrap();
        let before = count(root, "child-polls");
        std::fs::write(root.join("finish"), b"ready").unwrap();
        let parent_id = if mode == "unsafe-parent" {
            "parent/tenant-b"
        } else {
            "parent/tenant-a"
        };
        let child_id = if mode.ends_with("child") {
            "child/tenant-b"
        } else {
            "child/tenant-a"
        };
        let scheduler = Scheduler::open(
            root.join("db"),
            options_custom(root, "finish", Some(1), parent_id, Some(child_id), policy),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            count(root, "child-polls"),
            before,
            "{mode}: wrong backend was polled"
        );
        assert_eq!(count(root, "wake-effects"), 0, "{mode}");
        assert!(
            scheduler.conversation(id).unwrap().nested_suspended(),
            "{mode}"
        );
        scheduler.shutdown().await.unwrap();
        let mut scheduler = Scheduler::open(
            root.join("db"),
            options_custom(
                root,
                "finish",
                Some(1),
                "parent/tenant-a",
                Some("child/tenant-a"),
                policy,
            ),
        )
        .unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none(), "{mode}");
        assert_eq!(
            (
                count(root, "initial-effects"),
                count(root, "wake-effects"),
                count(root, "child-effects")
            ),
            (1, 1, 1),
            "{mode}"
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn safe_legacy_suspension_does_not_poll_after_parent_becomes_unsafe() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (scheduler, id, _) = start_parked(
        root,
        options_custom(
            root,
            "finish",
            Some(1),
            "parent/tenant-a",
            None,
            ReplayPolicy::Safe,
        ),
    )
    .await;
    scheduler.shutdown().await.unwrap();
    let before = count(root, "child-polls");
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let scheduler = Scheduler::open(
        root.join("db"),
        options_custom(
            root,
            "finish",
            Some(1),
            "parent/tenant-a",
            None,
            ReplayPolicy::Unsafe,
        ),
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(count(root, "child-polls"), before);
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_pending_safe_polls_but_unsafe_requires_verified_binding() {
    use durable_store::{Check, Expected, Mutation, Store};
    for policy in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (scheduler, id, root_op) = start_parked(
            root,
            options_custom(
                root,
                "finish",
                Some(1),
                "parent/tenant-a",
                Some("child/tenant-a"),
                policy,
            ),
        )
        .await;
        let job = scheduler
            .nested_pending_jobs(id, root_op)
            .unwrap()
            .pop()
            .unwrap();
        scheduler.shutdown().await.unwrap();
        let store = Store::open(root.join("db")).unwrap();
        let (key, entry) = store
            .list(format!("nested/{root_op}/").as_bytes())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let mut record: Value = serde_json::from_slice(&entry.value).unwrap();
        record.as_object_mut().unwrap().remove("durable_identity");
        store
            .commit(
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
        drop(store);
        let before = count(root, "child-polls");
        std::fs::write(root.join("finish"), b"ready").unwrap();
        let mut scheduler = Scheduler::open(
            root.join("db"),
            options_custom(
                root,
                "finish",
                Some(1),
                "parent/tenant-a",
                Some("child/tenant-a"),
                policy,
            ),
        )
        .unwrap();
        if policy == ReplayPolicy::Safe {
            wait(&mut scheduler, id).await;
            assert!(scheduler.blocked(id).unwrap().is_none());
            assert!(count(root, "child-polls") > before);
            scheduler.shutdown().await.unwrap();
            continue;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            count(root, "child-polls"),
            before,
            "legacy backend was polled"
        );
        assert!(
            scheduler
                .bind_nested_pending_identity(
                    id,
                    root_op,
                    job.operation_id,
                    "wrong",
                    "child/tenant-a"
                )
                .is_err()
        );
        assert!(
            scheduler
                .bind_nested_pending_identity(
                    id,
                    root_op,
                    job.operation_id,
                    &job.reference,
                    "child/tenant-b"
                )
                .is_err()
        );
        scheduler
            .bind_nested_pending_identity(
                id,
                root_op,
                job.operation_id,
                &job.reference,
                "child/tenant-a",
            )
            .unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none());
        assert_eq!(
            (
                count(root, "initial-effects"),
                count(root, "wake-effects"),
                count(root, "child-effects")
            ),
            (1, 1, 1)
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn old_unsafe_proof_without_parent_identity_stays_blocked_after_child_bind() {
    use durable_store::{Check, Expected, Mutation, Store};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (scheduler, id, root_op) = start_parked(
        root,
        options_custom(
            root,
            "finish",
            Some(1),
            "parent/tenant-a",
            Some("child/tenant-a"),
            ReplayPolicy::Unsafe,
        ),
    )
    .await;
    let job = scheduler
        .nested_pending_jobs(id, root_op)
        .unwrap()
        .pop()
        .unwrap();
    scheduler.shutdown().await.unwrap();
    let store = Store::open(root.join("db")).unwrap();
    let entries = store.list(b"").unwrap();
    let mut changed_proof = false;
    let mut changed_child = false;
    for (key, entry) in entries {
        let Ok(mut value) = serde_json::from_slice::<Value>(&entry.value) else {
            continue;
        };
        let mut changed = false;
        if let Some(identity) = value
            .pointer_mut("/nested_resume/unsafe_park/identity")
            .and_then(Value::as_object_mut)
        {
            changed_proof = identity.remove("identity").is_some();
            changed = changed_proof;
        }
        if key.starts_with(format!("nested/{root_op}/").as_bytes()) {
            changed_child = value
                .as_object_mut()
                .unwrap()
                .remove("durable_identity")
                .is_some();
            changed |= changed_child;
        }
        if changed {
            store
                .commit(
                    &[Check {
                        key: key.clone(),
                        expected: Expected::Version(entry.version),
                    }],
                    &[Mutation::Put {
                        key,
                        value: serde_json::to_vec(&value).unwrap(),
                    }],
                )
                .unwrap();
        }
    }
    assert!(changed_proof && changed_child);
    drop(store);
    let before = count(root, "child-polls");
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let mut scheduler = Scheduler::open(
        root.join("db"),
        options_custom(
            root,
            "finish",
            Some(1),
            "parent/tenant-a",
            Some("child/tenant-a"),
            ReplayPolicy::Unsafe,
        ),
    )
    .unwrap();
    scheduler
        .bind_nested_pending_identity(
            id,
            root_op,
            job.operation_id,
            &job.reference,
            "child/tenant-a",
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(count(root, "child-polls"), before);
    assert_eq!(count(root, "wake-effects"), 0);
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    scheduler.shutdown().await.unwrap();
}

#[derive(Clone)]
struct MutableSettings(Arc<std::sync::Mutex<(Option<u32>, Option<String>)>>);
struct MutableParent {
    inner: Parent,
    settings: MutableSettings,
}
#[async_trait]
impl KeyedTool for MutableParent {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }
    fn unsafe_park_version(&self) -> Option<u32> {
        self.settings.0.lock().unwrap().0
    }
    fn unsafe_park_identity(&self) -> Option<String> {
        self.settings.0.lock().unwrap().1.clone()
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        self.inner.invoke(args, context, state, host).await
    }
}
fn mutable_options(
    root: &Path,
    stage: &str,
) -> (
    Options,
    pi_rs_durable::nested_identity::IdentityHandle,
    pi_rs_durable::nested_identity::IdentityHandle,
    MutableSettings,
) {
    let settings = MutableSettings(Arc::new(std::sync::Mutex::new((
        Some(1),
        Some("parent/tenant-a".into()),
    ))));
    let parent = Arc::new(KeyedToolAdapter::new(MutableParent {
        inner: Parent {
            root: root.into(),
            stage: stage.into(),
            version: Some(1),
            identity: "parent/tenant-a".into(),
        },
        settings: settings.clone(),
    }))
    .register(ReplayPolicy::Unsafe);
    let parent_handle =
        pi_rs_durable::nested_identity::register(&parent.tool, Some("parent/tenant-a".into()))
            .unwrap();
    let child: Arc<dyn Tool> = Arc::new(Child(root.into()));
    let child_handle =
        pi_rs_durable::nested_identity::register(&child, Some("child/tenant-a".into())).unwrap();
    (
        Options {
            model: Arc::new(ModelStub),
            tools: vec![
                parent,
                RegisteredTool {
                    tool: child,
                    replay: ReplayPolicy::Unsafe,
                },
            ],
            restart_limit: 8,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        parent_handle,
        child_handle,
        settings,
    )
}
async fn update_when_idle(
    handle: &pi_rs_durable::nested_identity::IdentityHandle,
    identity: &str,
    apply: impl Fn(),
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handle.update(Some(identity.into()), &apply).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn same_arc_live_identity_version_and_optout_changes_are_fenced() {
    for mode in [
        "parent-version",
        "parent-optout",
        "parent-tenant",
        "child-tenant",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (opts, parent_handle, child_handle, settings) = mutable_options(root, "finish");
        let (mut scheduler, id, _) = start_parked(root, opts).await;
        let config = settings.0.clone();
        match mode {
            "parent-version" => {
                update_when_idle(&parent_handle, "parent/tenant-a", move || {
                    config.lock().unwrap().0 = Some(2)
                })
                .await
            }
            "parent-optout" => {
                update_when_idle(&parent_handle, "parent/tenant-a", move || {
                    config.lock().unwrap().0 = None
                })
                .await
            }
            "parent-tenant" => {
                update_when_idle(&parent_handle, "parent/tenant-b", move || {
                    config.lock().unwrap().1 = Some("parent/tenant-b".into())
                })
                .await
            }
            _ => update_when_idle(&child_handle, "child/tenant-b", || {}).await,
        }
        let before = count(root, "child-polls");
        std::fs::write(root.join("finish"), b"ready").unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            count(root, "child-polls"),
            before,
            "{mode}: wrong backend was polled"
        );
        assert_eq!(count(root, "wake-effects"), 0, "{mode}");
        assert!(scheduler.conversation(id).unwrap().nested_suspended());
        let config = settings.0.clone();
        update_when_idle(&parent_handle, "parent/tenant-a", move || {
            *config.lock().unwrap() = (Some(1), Some("parent/tenant-a".into()))
        })
        .await;
        update_when_idle(&child_handle, "child/tenant-a", || {}).await;
        scheduler.schedule(id);
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none(), "{mode}");
        assert_eq!(
            (
                count(root, "initial-effects"),
                count(root, "wake-effects"),
                count(root, "child-effects")
            ),
            (1, 1, 1),
            "{mode}"
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_wake_holds_all_child_and_parent_leases_during_handler() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("two-children"), b"yes").unwrap();
    let (mut opts, parent, child, _) = mutable_options(root, "pause-wake");
    let second: Arc<dyn Tool> = Arc::new(SecondChild(root.into()));
    let second_handle =
        pi_rs_durable::nested_identity::register(&second, Some("child2/tenant-a".into())).unwrap();
    opts.tools.push(RegisteredTool {
        tool: second,
        replay: ReplayPolicy::Unsafe,
    });
    let (mut scheduler, id, _) = start_parked(root, opts).await;
    std::fs::write(root.join("finish"), b"ready").unwrap();
    until(|| root.join("wake-running").exists()).await;
    for (handle, identity) in [
        (&parent, "parent/tenant-b"),
        (&child, "child/tenant-b"),
        (&second_handle, "child2/tenant-b"),
    ] {
        assert!(
            handle
                .update(Some(identity.into()), || panic!("lease allowed update"))
                .await
                .is_err()
        );
    }
    std::fs::write(root.join("release-wake"), b"ready").unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        second_handle
            .update(Some("child2/tenant-a".into()), || {})
            .await
            .is_ok()
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_or_skipped_after_suspend_preserves_safe_and_unsafe_contracts() {
    for policy in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
        for stage in ["return-failed", "return-skipped"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let mut scheduler = Scheduler::open(
                root.join("db"),
                options_custom(
                    root,
                    stage,
                    Some(1),
                    "parent/tenant-a",
                    Some("child/tenant-a"),
                    policy,
                ),
            )
            .unwrap();
            let id = scheduler
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: "".into(),
                    max_output_tokens: 100,
                })
                .unwrap();
            scheduler.submit(id, None, "work".into()).unwrap();
            until(|| scheduler.blocked(id).unwrap().is_some()).await;
            let (_, reason) = scheduler.blocked(id).unwrap().unwrap();
            if policy == ReplayPolicy::Safe {
                assert!(scheduler.conversation(id).unwrap().nested_suspended());
                std::fs::write(root.join("finish"), b"ready").unwrap();
                wait(&mut scheduler, id).await;
                assert!(scheduler.blocked(id).unwrap().is_none());
                assert_eq!(count(root, "wake-effects"), 1);
                scheduler.shutdown().await.unwrap();
                continue;
            }
            assert!(
                reason.contains(if stage == "return-failed" {
                    "parent explicit failure"
                } else {
                    "parent explicit skip"
                }),
                "{reason}"
            );
            assert!(!scheduler.conversation(id).unwrap().nested_suspended());
            std::fs::write(root.join("finish"), b"ready").unwrap();
            scheduler.schedule(id);
            wait(&mut scheduler, id).await;
            assert_eq!(count(root, "wake-effects"), 0);
            scheduler.shutdown().await.unwrap();
        }
    }
}

struct ReentrantParent {
    inner: Parent,
    handle: Arc<std::sync::Mutex<Option<pi_rs_durable::nested_identity::IdentityHandle>>>,
}
#[async_trait]
impl KeyedTool for ReentrantParent {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }
    fn unsafe_park_version(&self) -> Option<u32> {
        Some(1)
    }
    fn unsafe_park_identity(&self) -> Option<String> {
        Some("parent/tenant-a".into())
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let handle = self.handle.lock().unwrap().clone().unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            handle.update(Some("parent/tenant-b".into()), || panic!("busy update ran")),
        )
        .await
        .expect("parent identity update hung");
        assert!(result.is_err());
        log(&self.inner.root, "parent-busy");
        self.inner.invoke(args, context, state, host).await
    }
}
struct ReentrantChild {
    root: PathBuf,
    handle: Arc<std::sync::Mutex<Option<pi_rs_durable::nested_identity::IdentityHandle>>>,
}

#[tokio::test]
async fn initial_unsafe_parent_waits_for_busy_identity_and_rejects_invalidated_identity() {
    for invalidated in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (opts, parent, _, _) = mutable_options(root, "finish");
        if invalidated {
            let handle = parent.clone();
            assert!(
                tokio::spawn(async move {
                    handle
                        .update(Some("parent/tenant-a".into()), || {
                            panic!("interrupted update")
                        })
                        .await
                })
                .await
                .is_err()
            );
        }
        let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let mut writer = None;
        let mut release = None;
        if !invalidated {
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let handle = parent.clone();
            writer = Some(std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(handle.update(Some("parent/tenant-a".into()), || {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }))
            }));
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            release = Some(release_tx);
        }
        scheduler.submit(id, None, "work".into()).unwrap();
        if invalidated {
            until(|| scheduler.blocked(id).unwrap().is_some()).await;
            assert!(
                scheduler
                    .blocked(id)
                    .unwrap()
                    .unwrap()
                    .1
                    .contains("identity unavailable")
            );
        } else {
            until(|| scheduler.conversation(id).unwrap().tool_attempt().is_some()).await;
            assert_eq!(count(root, "initial-effects"), 0);
            release.unwrap().send(()).unwrap();
            assert!(writer.unwrap().join().unwrap().is_ok());
            until(|| root.join("poll-entered").exists()).await;
        }
        assert_eq!(count(root, "initial-effects"), usize::from(!invalidated));
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn safe_unbound_child_poll_holds_registered_identity_lease() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let slot = Arc::new(std::sync::Mutex::new(None));
    let parent = Arc::new(KeyedToolAdapter::new(Parent {
        root: root.into(),
        stage: "finish".into(),
        version: Some(1),
        identity: "parent/tenant-a".into(),
    }))
    .register(ReplayPolicy::Safe);
    let child: Arc<dyn Tool> = Arc::new(ReentrantChild {
        root: root.into(),
        handle: slot.clone(),
    });
    *slot.lock().unwrap() = Some(pi_rs_durable::nested_identity::register(&child, None).unwrap());
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent,
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| count(root, "child-busy") > 0).await;
    std::fs::write(root.join("finish"), b"ready").unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    scheduler.shutdown().await.unwrap();
}
#[async_trait]
impl Tool for ReentrantChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        let handle = self.handle.lock().unwrap().clone().unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            handle.update(Some("child/tenant-b".into()), || panic!("busy update ran")),
        )
        .await
        .expect("child identity update hung");
        assert!(result.is_err());
        log(&self.root, "child-invoke-busy");
        Outcome::Pending {
            reference: "remote".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        let handle = self.handle.lock().unwrap().clone().unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            handle.update(Some("child/tenant-b".into()), || panic!("busy update ran")),
        )
        .await
        .expect("poll identity update hung");
        assert!(result.is_err());
        log(&self.root, "child-busy");
        Some(if self.root.join("finish").exists() {
            Outcome::Succeeded("result".into())
        } else {
            Outcome::Pending {
                reference: "remote".into(),
            }
        })
    }
}
#[tokio::test]
async fn identity_update_from_parent_and_poll_returns_busy_without_hanging() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let parent_slot = Arc::new(std::sync::Mutex::new(None));
    let child_slot = Arc::new(std::sync::Mutex::new(None));
    let parent = Arc::new(KeyedToolAdapter::new(ReentrantParent {
        inner: Parent {
            root: root.into(),
            stage: "finish".into(),
            version: Some(1),
            identity: "parent/tenant-a".into(),
        },
        handle: parent_slot.clone(),
    }))
    .register(ReplayPolicy::Unsafe);
    *parent_slot.lock().unwrap() = Some(
        pi_rs_durable::nested_identity::register(&parent.tool, Some("parent/tenant-a".into()))
            .unwrap(),
    );
    let child: Arc<dyn Tool> = Arc::new(ReentrantChild {
        root: root.into(),
        handle: child_slot.clone(),
    });
    *child_slot.lock().unwrap() = Some(
        pi_rs_durable::nested_identity::register(&child, Some("child/tenant-a".into())).unwrap(),
    );
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent,
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(root.join("db"), opts).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| count(root, "child-busy") > 0).await;
    std::fs::write(root.join("finish"), b"ready").unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert_eq!(count(root, "parent-busy"), 2);
    assert_eq!(count(root, "child-invoke-busy"), 1);
    assert!(count(root, "child-busy") >= 1);
    scheduler.shutdown().await.unwrap();
}
