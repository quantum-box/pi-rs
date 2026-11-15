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

struct MultiParent(Arc<AtomicUsize>);
#[async_trait]
impl KeyedTool for MultiParent {
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
        let a = host
            .call_keyed_unpolled("a", "child", json!({"job":"a"}))
            .await;
        let b = host
            .call_keyed_unpolled("b", "child", json!({"job":"b"}))
            .await;
        if matches!(a, Outcome::Pending { .. }) || matches!(b, Outcome::Pending { .. }) {
            host.suspend_pending().unwrap();
            Outcome::Succeeded("discarded".into()).into()
        } else {
            match (a, b) {
                (Outcome::Succeeded(a), Outcome::Succeeded(b)) => {
                    Outcome::Succeeded(format!("{a},{b}")).into()
                }
                _ => Outcome::Failed("unexpected child outcome".into()).into(),
            }
        }
    }
}
struct AutoChild {
    invoked: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    ready_a: Arc<std::sync::atomic::AtomicBool>,
    ready_b: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl Tool for AutoChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        self.invoked.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: args["job"].as_str().unwrap().into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        let ready = if reference == "a" {
            &self.ready_a
        } else {
            &self.ready_b
        };
        Some(if ready.load(Ordering::SeqCst) {
            Outcome::Succeeded(reference.into())
        } else {
            Outcome::Pending {
                reference: reference.into(),
            }
        })
    }
}
fn multi_options(parent: Arc<KeyedToolAdapter<MultiParent>>, child: Arc<AutoChild>) -> Options {
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
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition did not become true");
}
#[tokio::test]
async fn multiple_children_are_polled_and_parent_wakes_automatically() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let a = Arc::new(AtomicBool::new(false));
    let b = Arc::new(AtomicBool::new(false));
    let parent = Arc::new(KeyedToolAdapter::new(MultiParent(pc.clone())));
    let child = Arc::new(AutoChild {
        invoked: cc.clone(),
        polls: polls.clone(),
        ready_a: a.clone(),
        ready_b: b.clone(),
    });
    let mut s = Scheduler::open(dir.path().join("db"), multi_options(parent, child)).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    until(|| polls.load(Ordering::SeqCst) >= 2).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    assert!(s.conversation(id).unwrap().nested_suspended());
    assert_eq!(s.nested_pending_jobs(id, root).unwrap().len(), 2);
    a.store(true, Ordering::SeqCst);
    until(|| {
        s.nested_pending_jobs(id, root)
            .is_ok_and(|jobs| jobs.len() == 1)
    })
    .await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    b.store(true, Ordering::SeqCst);
    wait(&mut s, id).await;
    assert!(s.blocked(id).unwrap().is_none());
    assert_eq!(cc.load(Ordering::SeqCst), 2);
    assert_eq!(pc.load(Ordering::SeqCst), 2);
    assert!(
        s.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "a,b"))
    );
    s.shutdown().await.unwrap();
}

struct HangingChild {
    started: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}
struct ActivePoll(Arc<AtomicUsize>);
impl Drop for ActivePoll {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Tool for HangingChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "hang".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActivePoll(self.active.clone());
        self.started.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
fn hanging_options(parent: Arc<KeyedToolAdapter<Parent>>, child: Arc<HangingChild>) -> Options {
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
#[tokio::test]
async fn abort_cancels_and_joins_live_poll_without_replay_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(HangingChild {
        started: started.clone(),
        active: active.clone(),
    });
    let mut s = Scheduler::open(&path, hanging_options(parent.clone(), child.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    until(|| started.load(Ordering::SeqCst) == 1).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    s.abort(id).unwrap();
    wait(&mut s, id).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(s.nested_pending_jobs(id, root).unwrap().len(), 1);
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, hanging_options(parent, child)).unwrap();
    wait(&mut s, id).await;
    assert!(s.conversation(id).unwrap().abort_requested);
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn selected_extension_prevents_automatic_poll_and_replay_across_reopen() {
    use pi_rs_durable::extension::{Extension, Registry};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(HangingChild {
        started: started.clone(),
        active,
    });
    let mut extensions = Registry::default();
    extensions
        .install(Extension {
            name: "selected".into(),
            ..Default::default()
        })
        .unwrap();
    let mut s = Scheduler::open_with_extensions(
        &path,
        hanging_options(parent.clone(), child.clone()),
        extensions.clone(),
    )
    .unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.select_extensions(id, vec!["selected".into()]).unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait(&mut s, id).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    assert!(!s.conversation(id).unwrap().nested_suspended());
    assert_eq!(started.load(Ordering::SeqCst), 0);
    let job = s.nested_pending_jobs(id, root).unwrap().pop().unwrap();
    s.shutdown().await.unwrap();
    let mut s =
        Scheduler::open_with_extensions(&path, hanging_options(parent, child), extensions).unwrap();
    wait(&mut s, id).await;
    s.reconcile_nested_pending(
        id,
        root,
        job.operation_id,
        &job.reference,
        Outcome::Succeeded("done".into()),
    )
    .unwrap();
    wait(&mut s, id).await;
    assert!(s.blocked(id).unwrap().is_some());
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert_eq!(started.load(Ordering::SeqCst), 0);
    s.shutdown().await.unwrap();
}
