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
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

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
struct Parent(Arc<AtomicUsize>);
#[async_trait]
impl KeyedTool for Parent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        match host
            .call_keyed_unpolled("child-key", "child", json!({}))
            .await
        {
            Outcome::Pending { .. } => {
                host.suspend_pending().unwrap();
                Outcome::Succeeded("discarded".into()).into()
            }
            outcome => outcome.into(),
        }
    }
}
struct Child(Arc<AtomicUsize>);
#[async_trait]
impl Tool for Child {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "external-job".into(),
        }
    }
}
fn options(parent: Arc<KeyedToolAdapter<Parent>>, child: Arc<Child>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
async fn wait(s: &mut Scheduler, id: uuid::Uuid) {
    tokio::time::timeout(Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn suspend_releases_worker_and_reopens_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(Child(cc.clone()));
    let mut s = Scheduler::open(&path, options(parent.clone(), child.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait(&mut s, id).await;
    assert!(s.conversation(id).unwrap().nested_suspended());
    let root = s.blocked(id).unwrap().unwrap().0;
    let job = s.nested_pending_jobs(id, root).unwrap().pop().unwrap();
    // A second scheduled execution completes while the child remains pending.
    s.schedule(id);
    wait(&mut s, id).await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert_eq!(cc.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(parent, child)).unwrap();
    wait(&mut s, id).await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert!(s.conversation(id).unwrap().nested_suspended());
    s.reconcile_nested_pending(
        id,
        root,
        job.operation_id,
        &job.reference,
        Outcome::Succeeded("child-result".into()),
    )
    .unwrap();
    wait(&mut s, id).await;
    assert!(s.blocked(id).unwrap().is_none());
    assert_eq!(pc.load(Ordering::SeqCst), 2);
    assert_eq!(cc.load(Ordering::SeqCst), 1);
    assert!(
        s.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "child-result"))
    );
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn abort_while_suspended_survives_reopen_and_fences_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(Child(cc.clone()));
    let mut s = Scheduler::open(&path, options(parent.clone(), child.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait(&mut s, id).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    let job = s.nested_pending_jobs(id, root).unwrap().pop().unwrap();
    s.abort(id).unwrap();
    wait(&mut s, id).await;
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(parent, child)).unwrap();
    wait(&mut s, id).await;
    assert!(s.conversation(id).unwrap().abort_requested);
    assert!(s.conversation(id).unwrap().nested_suspended());
    s.cancel_nested_pending(
        id,
        root,
        job.operation_id,
        &job.reference,
        "confirmed cancelled".into(),
    )
    .unwrap();
    wait(&mut s, id).await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert_eq!(cc.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}
