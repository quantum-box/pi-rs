use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Model, ModelEvent, ModelRequest, ModelStream, Outcome, ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{
        Config, IndependentCompactionStatus, Options, RegisteredTool, Scheduler,
        SummaryProviderIdentity,
    },
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

struct ModelStub {
    issue_tool: AtomicBool,
    issue_multiple: AtomicBool,
    summary_started: Semaphore,
    summary_release: Semaphore,
    requests: Mutex<Vec<ModelRequest>>,
}
impl Default for ModelStub {
    fn default() -> Self {
        Self {
            issue_tool: false.into(),
            issue_multiple: false.into(),
            summary_started: Semaphore::new(0),
            summary_release: Semaphore::new(0),
            requests: Mutex::new(Vec::new()),
        }
    }
}
#[async_trait::async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests.lock().unwrap().push(request);
        let calls = if self.issue_tool.swap(false, Ordering::SeqCst) {
            let mut calls = vec![ToolCall {
                id: "tool-call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }];
            if self.issue_multiple.swap(false, Ordering::SeqCst) {
                calls.push(ToolCall {
                    id: "second-call".into(),
                    name: "job".into(),
                    arguments: "{}".into(),
                });
            }
            calls
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
    async fn stream_for_summary(
        &self,
        _: Uuid,
        _: Uuid,
        _: ModelRequest,
    ) -> pi_rs_core::Result<ModelStream> {
        self.summary_started.add_permits(1);
        self.summary_release.acquire().await.unwrap().forget();
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("tool checkpoint".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
struct ToolState {
    started: Semaphore,
    release: Semaphore,
    calls: AtomicUsize,
    handoff: AtomicBool,
}
impl Default for ToolState {
    fn default() -> Self {
        Self {
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            calls: AtomicUsize::new(0),
            handoff: false.into(),
        }
    }
}
struct GatedTool(Arc<ToolState>);
#[async_trait::async_trait]
impl pi_rs_durable::keyed_tool::KeyedTool for GatedTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> (pi_rs_core::ToolReply, pi_rs_durable::reset::ToolControl) {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.started.add_permits(1);
        self.0.release.acquire().await.unwrap().forget();
        (
            Outcome::Succeeded("tool result".into()).into(),
            pi_rs_durable::reset::ToolControl {
                handoff: self
                    .0
                    .handoff
                    .load(Ordering::SeqCst)
                    .then(|| "tool handoff".into()),
                ..Default::default()
            },
        )
    }
}
#[derive(Default)]
struct PendingTool {
    calls: AtomicUsize,
}
#[async_trait::async_trait]
impl pi_rs_core::Tool for PendingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "external-job".into(),
        }
    }
}
fn provider() -> SummaryProviderIdentity {
    SummaryProviderIdentity {
        model: "tool:model:v1".into(),
        settlement: "tool:ledger:v1".into(),
    }
}
fn options(model: Arc<ModelStub>, tool: RegisteredTool) -> Options {
    Options {
        model,
        tools: vec![tool],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
fn active_registration(state: Arc<ToolState>) -> RegisteredTool {
    Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(GatedTool(
        state,
    )))
    .register(ReplayPolicy::Unsafe)
}
fn pending_registration(tool: Arc<PendingTool>) -> RegisteredTool {
    RegisteredTool {
        tool,
        replay: ReplayPolicy::Unsafe,
    }
}
async fn setup(
    path: &std::path::Path,
    model: Arc<ModelStub>,
    tool: RegisteredTool,
) -> (Scheduler, Uuid) {
    let mut scheduler = Scheduler::open_with_summary_recovery(
        path,
        options(model.clone(), tool),
        Default::default(),
        None,
        provider(),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    for input in ["first", "second"] {
        scheduler.submit(id, None, input.into()).unwrap();
        scheduler.wait(id).await.unwrap();
    }
    model.issue_tool.store(true, Ordering::SeqCst);
    scheduler.submit(id, None, "third".into()).unwrap();
    (scheduler, id)
}
async fn request_summary(s: &mut Scheduler, id: Uuid, model: &ModelStub) -> Uuid {
    let op = Uuid::new_v4();
    assert!(s.request_busy_independent_compaction(id, op, 0).unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        model.summary_started.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    model.summary_release.add_permits(1);
    assert_eq!(
        s.wait_independent_compaction(op).await.unwrap(),
        IndependentCompactionStatus::Running
    );
    op
}
async fn active_setup(
    path: &std::path::Path,
    model: Arc<ModelStub>,
    state: Arc<ToolState>,
) -> (Scheduler, Uuid, Uuid, Uuid) {
    let (mut s, id) = setup(path, model.clone(), active_registration(state.clone())).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), state.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let attempt = s
        .conversation(id)
        .unwrap()
        .tool_attempt()
        .unwrap()
        .operation_id;
    let op = request_summary(&mut s, id, &model).await;
    (s, id, op, attempt)
}
#[tokio::test]
async fn summary_during_running_tool_keeps_result_and_inbox_order() {
    use pi_rs_durable::state::InputMode;
    for mode in [InputMode::Steer, InputMode::FollowUp] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ModelStub::default());
        let tool = Arc::new(ToolState::default());
        let (mut s, id, op, _) =
            active_setup(&dir.path().join("db"), model.clone(), tool.clone()).await;
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        s.submit_with_mode(id, None, "queued".into(), mode).unwrap();
        tool.release.add_permits(1);
        s.wait(id).await.unwrap();
        assert_eq!(
            s.independent_compaction_status(op).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        {
            let requests = model.requests.lock().unwrap();
            assert!(requests[3].messages.iter().any(|m| matches!(m, pi_rs_core::Message::Tool { call_id, content } if call_id == "tool-call" && content == "tool result")));
            assert!(requests[3].messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("tool checkpoint"))));
            assert_eq!(
                requests[3].messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")
                ),
                mode == InputMode::Steer
            );
            if mode == InputMode::FollowUp {
                assert!(requests[4].messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")
                ));
            }
        }
        s.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn tool_handoff_stales_running_summary_without_repeating_effect() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelStub::default());
    let tool = Arc::new(ToolState::default());
    tool.handoff.store(true, Ordering::SeqCst);
    let (mut s, id, op, _) = active_setup(&dir.path().join("db"), model, tool.clone()).await;
    tool.release.add_permits(1);
    s.wait(id).await.unwrap();
    assert_eq!(
        s.independent_compaction_status(op).unwrap(),
        IndependentCompactionStatus::Stale
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert!(
        s.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "tool handoff")
        )
    );
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn abort_with_running_tool_keeps_summary_until_explicit_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelStub::default());
    let tool = Arc::new(ToolState::default());
    let (mut s, id, op, attempt) = active_setup(&dir.path().join("db"), model, tool.clone()).await;
    s.submit(id, None, "withdraw me".into()).unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), s.abort_and_wait(id))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("abort cleanup remains pending"));
    assert_eq!(
        s.independent_compaction_status(op).unwrap(),
        IndependentCompactionStatus::Running
    );
    assert_eq!(s.blocked(id).unwrap().unwrap().0, attempt);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.reconcile(id, attempt, Outcome::Succeeded("verified result".into()))
        .unwrap();
    s.wait(id).await.unwrap();
    assert_eq!(
        s.independent_compaction_status(op).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = s.conversation(id).unwrap();
    assert_eq!(c.messages.iter().filter(|m| matches!(m, pi_rs_core::Message::Tool { call_id, content } if call_id == "tool-call" && content == "verified result")).count(), 1);
    assert!(
        !c.messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "withdraw me")
        )
    );
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn interrupted_unsafe_tool_reopens_blocked_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(ModelStub::default());
    let tool = Arc::new(ToolState::default());
    let (s, id, op, attempt) = active_setup(&path, model.clone(), tool.clone()).await;
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open_with_summary_recovery(
        &path,
        options(model, active_registration(tool.clone())),
        Default::default(),
        None,
        provider(),
    )
    .unwrap();
    assert_eq!(s.blocked(id).unwrap().unwrap().0, attempt);
    assert!(
        s.request_busy_independent_compaction(id, Uuid::new_v4(), 0)
            .is_err()
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.reconcile(id, attempt, Outcome::Succeeded("verified result".into()))
        .unwrap();
    s.wait(id).await.unwrap();
    assert_eq!(
        s.independent_compaction_status(op).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn parked_pending_tool_recovers_and_settles_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(ModelStub::default());
    let tool = Arc::new(PendingTool::default());
    let (mut s, id) = setup(&path, model.clone(), pending_registration(tool.clone())).await;
    s.wait(id).await.unwrap();
    let attempt = s
        .conversation(id)
        .unwrap()
        .tool_attempt()
        .unwrap()
        .operation_id;
    let op = request_summary(&mut s, id, &model).await;
    s.submit(id, None, "queued".into()).unwrap();
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open_with_summary_recovery(
        &path,
        options(model, pending_registration(tool.clone())),
        Default::default(),
        None,
        provider(),
    )
    .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.reconcile(id, attempt, Outcome::Succeeded("pending result".into()))
        .unwrap();
    s.wait(id).await.unwrap();
    assert_eq!(
        s.independent_compaction_status(op).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = s.conversation(id).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("tool checkpoint"))).unwrap();
    let result = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "pending result")).unwrap();
    let queued = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
        .unwrap();
    assert!(summary < result && result < queued);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn tool_finishes_before_summary_and_result_is_retained() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelStub::default());
    let tool = Arc::new(ToolState::default());
    let (mut s, id) = setup(
        &dir.path().join("db"),
        model.clone(),
        active_registration(tool.clone()),
    )
    .await;
    tool.started.acquire().await.unwrap().forget();
    let op = Uuid::new_v4();
    assert!(s.request_busy_independent_compaction(id, op, 0).unwrap());
    model.summary_started.acquire().await.unwrap().forget();
    tool.release.add_permits(1);
    s.wait(id).await.unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        s.wait_independent_compaction(op).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = s.conversation(id).unwrap();
    assert_eq!(c.messages.iter().filter(|m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "tool result")).count(), 1);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn running_tool_reset_order_fences_summary() {
    for prior in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ModelStub::default());
        let tool = Arc::new(ToolState::default());
        let (mut s, id) = setup(
            &dir.path().join("db"),
            model.clone(),
            active_registration(tool.clone()),
        )
        .await;
        tool.started.acquire().await.unwrap().forget();
        let op = Uuid::new_v4();
        assert!(s.request_busy_independent_compaction(id, op, 0).unwrap());
        model.summary_started.acquire().await.unwrap().forget();
        if prior {
            s.reset(id, Some("before".into())).unwrap();
        }
        model.summary_release.add_permits(1);
        assert_eq!(
            s.wait_independent_compaction(op).await.unwrap(),
            IndependentCompactionStatus::Running
        );
        if !prior {
            s.reset(id, Some("after".into())).unwrap();
        }
        tool.release.add_permits(1);
        s.wait(id).await.unwrap();
        assert_eq!(
            s.independent_compaction_status(op).unwrap(),
            if prior {
                IndependentCompactionStatus::Stale
            } else {
                IndependentCompactionStatus::Applied
            }
        );
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        s.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn multiple_tool_calls_remain_ineligible() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelStub::default());
    let tool = Arc::new(ToolState::default());
    model.issue_multiple.store(true, Ordering::SeqCst);
    let (mut s, id) = setup(
        &dir.path().join("db"),
        model,
        active_registration(tool.clone()),
    )
    .await;
    tool.started.acquire().await.unwrap().forget();
    assert!(
        s.request_busy_independent_compaction(id, Uuid::new_v4(), 0)
            .is_err()
    );
    tool.release.add_permits(2);
    s.wait(id).await.unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 2);
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn running_tool_kill_child() {
    let Ok(dir) = std::env::var("PI_RUNNING_TOOL_SUMMARY_KILL_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (s, id, op, attempt) = active_setup(
        &dir.join("db"),
        Arc::new(ModelStub::default()),
        Arc::new(ToolState::default()),
    )
    .await;
    std::fs::write(dir.join("ready"), format!("{id} {op} {attempt}")).unwrap();
    let _keep_owner = s;
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_unsafe_tool_requires_reconciliation_before_summary_placement() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("running_tool_kill_child")
        .arg("--nocapture")
        .env("PI_RUNNING_TOOL_SUMMARY_KILL_DIR", dir.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let ready = dir.path().join("ready");
    let found = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(&ready) {
                break value;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before ready: {status}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if found.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let ids = found.unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let ids: Vec<Uuid> = ids
        .split_whitespace()
        .map(|s| Uuid::parse_str(s).unwrap())
        .collect();
    let tool = Arc::new(ToolState::default());
    let model = Arc::new(ModelStub::default());
    let mut s = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        options(model.clone(), active_registration(tool.clone())),
        Default::default(),
        None,
        provider(),
    )
    .unwrap();
    assert_eq!(s.blocked(ids[0]).unwrap().unwrap().0, ids[2]);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert!(model.summary_started.try_acquire().is_err());
    s.reconcile(
        ids[0],
        ids[2],
        Outcome::Succeeded("confirmed after kill".into()),
    )
    .unwrap();
    s.wait(ids[0]).await.unwrap();
    assert_eq!(
        s.independent_compaction_status(ids[1]).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(s.conversation(ids[0]).unwrap().messages.iter().filter(|m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "confirmed after kill")).count(), 1);
    s.shutdown().await.unwrap();
}
