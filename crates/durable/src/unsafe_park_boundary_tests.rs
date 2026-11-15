use super::*;
use crate::keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter};
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    ExtensionState, ModelEvent, ModelRequest, ModelStream, Tool, ToolContext, ToolReply,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

struct Checkpoint {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
type Checkpoints = std::collections::HashMap<(Uuid, &'static str), Arc<Checkpoint>>;
static CHECKPOINTS: std::sync::OnceLock<std::sync::Mutex<Checkpoints>> = std::sync::OnceLock::new();
fn install(id: Uuid, stage: &'static str) -> Arc<Checkpoint> {
    let point = Arc::new(Checkpoint {
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    CHECKPOINTS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert((id, stage), point.clone());
    point
}
pub(super) async fn checkpoint(id: Uuid, stage: &'static str) {
    let point = CHECKPOINTS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(id, stage));
    if let Some(point) = point {
        point.reached.notify_one();
        point.release.notified().await;
    }
}

struct BoundaryModel;
#[async_trait::async_trait]
impl pi_rs_core::Model for BoundaryModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
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
fn append(root: &Path, name: &str) {
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
struct BoundaryParent {
    root: PathBuf,
    version: u32,
    changed_spec: bool,
}
#[async_trait::async_trait]
impl KeyedTool for BoundaryParent {
    fn spec(&self) -> ToolSpec {
        let mut s = spec("parent");
        if self.changed_spec {
            s.description = "changed".into();
        }
        s
    }
    fn unsafe_park_identity(&self) -> Option<String> {
        Some("test-parent/tenant-a".into())
    }
    fn unsafe_park_version(&self) -> Option<u32> {
        Some(self.version)
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if let Some(wake) = host.pending_wake() {
            append(&self.root, "wake-effects");
            if std::env::var("PI_UNSAFE_BOUNDARY_STAGE").ok().as_deref() == Some("handler-started")
            {
                std::fs::write(self.root.join("boundary"), b"handler-started").unwrap();
                std::future::pending::<()>().await;
            }
            let call = host
                .call_keyed_unpolled_with_id("child", "child", json!({}))
                .await
                .unwrap();
            assert_eq!(wake.checkpoint, json!({"child":call.operation_id}));
            assert_eq!(wake.ended, vec![call.operation_id]);
            return match call.outcome {
                Outcome::Succeeded(v) => Outcome::Succeeded(format!("resumed:{v}")).into(),
                _ => Outcome::Failed("unexpected child".into()).into(),
            };
        }
        append(&self.root, "initial-effects");
        let call = host
            .call_keyed_unpolled_with_id("child", "child", json!({}))
            .await
            .unwrap();
        assert!(matches!(call.outcome, Outcome::Pending { .. }));
        host.suspend_pending_with_checkpoint(json!({"child":call.operation_id}))
            .unwrap();
        Outcome::Succeeded("park".into()).into()
    }
}
struct BoundaryChild(PathBuf);
#[async_trait::async_trait]
impl Tool for BoundaryChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        append(&self.0, "child-effects");
        Outcome::Pending {
            reference: "remote".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        std::fs::write(self.0.join("poll-entered"), b"ready").unwrap();
        Some(if self.0.join("release").exists() {
            Outcome::Succeeded("result".into())
        } else {
            Outcome::Pending {
                reference: "remote".into(),
            }
        })
    }
}
fn options(root: &Path, version: u32, changed_spec: bool, policy: ReplayPolicy) -> Options {
    options_with_handles(root, version, changed_spec, policy).0
}
fn options_with_handles(
    root: &Path,
    version: u32,
    changed_spec: bool,
    policy: ReplayPolicy,
) -> (
    Options,
    crate::nested_identity::IdentityHandle,
    crate::nested_identity::IdentityHandle,
) {
    let parent = Arc::new(KeyedToolAdapter::new(BoundaryParent {
        root: root.into(),
        version,
        changed_spec,
    }))
    .register(policy);
    let parent_handle =
        crate::nested_identity::register(&parent.tool, Some("test-parent/tenant-a".into()))
            .unwrap();
    let child: Arc<dyn Tool> = Arc::new(BoundaryChild(root.into()));
    let child_handle =
        crate::nested_identity::register(&child, Some("test-backend:child:v1".into())).unwrap();
    (
        Options {
            model: Arc::new(BoundaryModel),
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
    )
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("boundary timeout");
}
async fn wait(scheduler: &mut Scheduler, id: Uuid) {
    tokio::time::timeout(Duration::from_secs(20), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn busy_identity_does_not_claim_and_leases_survive_until_commit() {
    for busy_parent in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (opts, parent, child) = options_with_handles(root, 1, false, ReplayPolicy::Unsafe);
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
        let before_claim = install(id, "before-claim");
        let busy = install(id, "lease-busy");
        let result = install(id, "before-result-commit");
        std::fs::write(root.join("release"), b"ready").unwrap();
        tokio::time::timeout(Duration::from_secs(20), before_claim.reached.notified())
            .await
            .unwrap();
        let parked_attempt = scheduler.conversation(id).unwrap().tool_attempt();
        let target = if busy_parent {
            parent.clone()
        } else {
            child.clone()
        };
        let identity = if busy_parent {
            "test-parent/tenant-a"
        } else {
            "test-backend:child:v1"
        };
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(target.update(Some(identity.into()), || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }))
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        before_claim.release.notify_one();
        tokio::time::timeout(Duration::from_secs(20), busy.reached.notified())
            .await
            .unwrap();
        assert_eq!(
            scheduler.conversation(id).unwrap().tool_attempt(),
            parked_attempt
        );
        assert_eq!(count(root, "wake-effects"), 0);
        release_tx.send(()).unwrap();
        assert!(writer.join().unwrap().is_ok());
        busy.release.notify_one();
        tokio::time::timeout(Duration::from_secs(20), result.reached.notified())
            .await
            .unwrap();
        assert_eq!(count(root, "wake-effects"), 1);
        assert!(
            parent
                .update(Some("changed".into()), || panic!("parent lease lost"))
                .await
                .is_err()
        );
        assert!(
            child
                .update(Some("changed".into()), || panic!("child lease lost"))
                .await
                .is_err()
        );
        result.release.notify_one();
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none());
        scheduler.shutdown().await.unwrap();
    }
}
struct Kill(std::process::Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn stop_process(root: &Path, stage: &str) -> Uuid {
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::unsafe_park_boundary_tests::unsafe_boundary_worker",
            ])
            .env("PI_UNSAFE_BOUNDARY_DIR", root)
            .env("PI_UNSAFE_BOUNDARY_STAGE", stage)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("poll-entered").exists()).await;
    std::fs::write(root.join("release"), b"ready").unwrap();
    until(|| root.join("boundary").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap()
}
#[tokio::test]
async fn unsafe_boundary_worker() {
    let Some(root) = std::env::var_os("PI_UNSAFE_BOUNDARY_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut scheduler = Scheduler::open(
        root.join("db"),
        options(&root, 1, false, ReplayPolicy::Unsafe),
    )
    .unwrap();
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
async fn killed_wake_claim_and_handler_boundaries_obey_attempt_fence() {
    use crate::observer::Cursor;
    for stage in ["wake-saved", "attempt-claimed", "handler-started"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let id = stop_process(root, stage).await;
        let mut scheduler = Scheduler::open(
            root.join("db"),
            options(root, 1, false, ReplayPolicy::Unsafe),
        )
        .unwrap();
        wait(&mut scheduler, id).await;
        let conversation = scheduler.conversation(id).unwrap();
        if stage == "wake-saved" {
            assert!(scheduler.blocked(id).unwrap().is_none());
            assert!(
                conversation
                    .messages
                    .iter()
                    .any(|m| matches!(m,Message::Tool {content,..} if content=="resumed:result"))
            );
            assert_eq!(count(root, "wake-effects"), 1);
        } else {
            assert!(scheduler.blocked(id).unwrap().is_some(), "{stage}");
            assert!(!conversation.nested_suspended(), "{stage}");
            assert_eq!(
                count(root, "wake-effects"),
                usize::from(stage == "handler-started")
            );
        }
        assert_eq!(count(root, "initial-effects"), 1);
        assert_eq!(count(root, "child-effects"), 1);
        let attempts = scheduler
            .tool_attempts(id, Cursor(0), std::num::NonZeroUsize::new(10).unwrap())
            .unwrap()
            .records;
        assert_eq!(attempts.len(), 2, "{stage}");
        assert_ne!(
            attempts[0].value["attempt"]["attempt_id"],
            attempts[1].value["attempt"]["attempt_id"]
        );
        if stage != "handler-started" {
            let at_boundary: Value =
                serde_json::from_slice(&std::fs::read(root.join("boundary")).unwrap()).unwrap();
            let expected = if stage == "wake-saved" {
                &attempts[0]
            } else {
                &attempts[1]
            };
            assert_eq!(at_boundary, expected.value["attempt"], "{stage}");
        }
        scheduler.shutdown().await.unwrap();
        let mut scheduler = Scheduler::open(
            root.join("db"),
            options(root, 1, false, ReplayPolicy::Unsafe),
        )
        .unwrap();
        wait(&mut scheduler, id).await;
        assert_eq!(count(root, "initial-effects"), 1);
        assert_eq!(
            count(root, "wake-effects"),
            usize::from(stage != "attempt-claimed")
        );
        scheduler.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn killed_wake_and_claim_respect_abort_and_definition_fences() {
    for stage in ["wake-saved", "attempt-claimed"] {
        for mode in ["abort", "version", "spec", "policy"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let id = stop_process(root, stage).await;
            if mode == "abort" {
                let mut harness = Harness::open_paused(
                    root.join("db"),
                    options(root, 1, false, ReplayPolicy::Unsafe),
                )
                .unwrap();
                harness.request_abort(id).unwrap();
                drop(harness);
            }
            let (version, changed, policy) = match mode {
                "version" => (2, false, ReplayPolicy::Unsafe),
                "spec" => (1, true, ReplayPolicy::Unsafe),
                "policy" => (1, false, ReplayPolicy::Safe),
                _ => (1, false, ReplayPolicy::Unsafe),
            };
            let scheduler =
                Scheduler::open(root.join("db"), options(root, version, changed, policy)).unwrap();
            tokio::time::sleep(Duration::from_millis(400)).await;
            assert_eq!(count(root, "initial-effects"), 1, "{mode}");
            assert_eq!(count(root, "child-effects"), 1, "{mode}");
            assert_eq!(count(root, "wake-effects"), 0, "{mode}");
            assert!(scheduler.blocked(id).unwrap().is_some(), "{mode}");
            let attempts = scheduler
                .tool_attempts(
                    id,
                    crate::observer::Cursor(0),
                    std::num::NonZeroUsize::new(10).unwrap(),
                )
                .unwrap()
                .records;
            assert_eq!(
                attempts.len(),
                if stage == "wake-saved" { 1 } else { 2 },
                "{stage}/{mode}: unexpected resumed claim"
            );
            scheduler.shutdown().await.unwrap();
        }
    }
}
