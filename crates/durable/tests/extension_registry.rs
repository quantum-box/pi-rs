use async_trait::async_trait;
use pi_rs_durable::extension::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MemoryMemo(Mutex<std::collections::HashMap<String, Value>>);
#[async_trait]
impl Memo for MemoryMemo {
    async fn get(&self, key: &str) -> Result<Option<Value>, String> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    async fn get_or_insert(&self, key: &str, value: Value) -> Result<Value, String> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .entry(key.into())
            .or_insert(value)
            .clone())
    }
}
struct Text(&'static str, &'static str);
#[async_trait]
impl Section for Text {
    fn key(&self) -> &str {
        self.0
    }
    async fn render(&self, _: &Context<'_>) -> Result<Option<String>, String> {
        Ok(Some(self.1.into()))
    }
}
struct Rewrite(i64);
#[async_trait]
impl Hook for Rewrite {
    async fn before_tool(&self, call: &ToolCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        let n = ctx.memo.get_or_insert("deadline", json!(self.0)).await?;
        Ok(BeforeTool::Rewrite(json!(
            call.arguments.as_i64().unwrap() + n.as_i64().unwrap()
        )))
    }
    async fn after_tool(
        &self,
        _: &ToolCall,
        result: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Ok(Some(json!(result.as_i64().unwrap() + self.0)))
    }
}
struct Fails;
#[async_trait]
impl Hook for Fails {
    async fn before_tool(&self, _: &ToolCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        panic!("broken guard")
    }
    async fn after_tool(
        &self,
        _: &ToolCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Err("broken observer".into())
    }
}
fn ext(name: &str) -> Extension {
    Extension {
        name: name.into(),
        ..Default::default()
    }
}
fn handler() -> Arc<dyn pi_rs_durable::task_scheduler::Handler> {
    Arc::new(|_: &pi_rs_durable::task::Task| {
        Ok(pi_rs_durable::task_scheduler::Step::Succeed(json!(null)))
    })
}

#[tokio::test]
async fn replacement_is_atomic_ordered_and_snapshots_keep_old_code() {
    let mut r = Registry::default();
    let mut a = ext("a");
    a.sections.push(Arc::new(Text("one", "old")));
    a.tasks.push(("one".into(), handler()));
    r.install(a).unwrap();
    let mut b = ext("b");
    b.sections.push(Arc::new(Text("two", "second")));
    b.sections.push(Arc::new(Text("one", "override")));
    b.tasks.push(("two".into(), handler()));
    r.install(b).unwrap();
    let old = r.snapshot().resolve(None);
    let mut a = ext("a");
    a.sections.push(Arc::new(Text("one", "new")));
    a.tasks.push(("two".into(), handler()));
    assert!(matches!(r.install(a), Err(RegistryError::TaskConflict(_))));
    assert_eq!(r.snapshot().names(), vec!["a", "b"]);
    let mut b = ext("b");
    b.sections.push(Arc::new(Text("one", "reloaded")));
    r.install(b).unwrap();
    let memo = MemoryMemo::default();
    let snapshot = json!({});
    let ctx = Context {
        snapshot: &snapshot,
        memo: &memo,
    };
    assert_eq!(
        old.prompt(&ctx, None).await.0,
        "<one>\noverride\n</one>\n\n<two>\nsecond\n</two>"
    );
    assert_eq!(
        r.snapshot()
            .resolve(None)
            .prompt(&ctx, Some("last"))
            .await
            .0,
        "<one>\nreloaded\n</one>\n\n<instructions>\nlast\n</instructions>"
    );
    r.uninstall("b");
    assert!(
        r.snapshot()
            .resolve(Some(&["missing".into(), "a".into()]))
            .task("one")
            .is_some()
    );
}

#[tokio::test]
async fn hooks_chain_and_fail_closed_while_after_errors_are_reported() {
    let mut r = Registry::default();
    let mut a = ext("a");
    a.hooks.push(Arc::new(Rewrite(2)));
    r.install(a).unwrap();
    let mut b = ext("b");
    b.hooks.push(Arc::new(Fails));
    b.hooks.push(Arc::new(Rewrite(100)));
    r.install(b).unwrap();
    let memo = MemoryMemo::default();
    let snapshot = json!({});
    let ctx = Context {
        snapshot: &snapshot,
        memo: &memo,
    };
    let call = ToolCall {
        name: "bash".into(),
        arguments: json!(1),
    };
    let (call, decision, reports) = r.snapshot().resolve(None).before_tool(call, &ctx).await;
    assert_eq!(call.arguments, json!(3));
    assert!(matches!(decision, BeforeTool::Block(_)));
    assert_eq!(reports[0].extension, "b");
    let (result, reports) = r
        .snapshot()
        .resolve(None)
        .after_tool(&call, json!(1), &ctx)
        .await;
    assert_eq!(result, json!(103));
    assert_eq!(reports[0].error, "broken observer");
    assert_eq!(
        memo.get_or_insert("deadline", json!(999)).await.unwrap(),
        json!(2)
    );
}

#[test]
fn validates_sections_and_reserved_tasks() {
    let mut r = Registry::default();
    for key in ["instructions", "Bad", "", "a.b"] {
        let mut a = ext("a");
        a.sections.push(Arc::new(Text(key, "x")));
        assert!(matches!(
            r.install(a),
            Err(RegistryError::InvalidSection(_))
        ));
    }
    let mut a = ext("a");
    a.sections.push(Arc::new(Text("same", "x")));
    a.sections.push(Arc::new(Text("same", "y")));
    assert!(matches!(r.install(a), Err(RegistryError::Duplicate { .. })));
    let mut a = ext("a");
    a.tasks.push(("pi.tool".into(), handler()));
    assert!(matches!(r.install(a), Err(RegistryError::TaskConflict(_))));
}

struct Gate;
#[async_trait]
impl Hook for Gate {
    async fn before_tool(&self, _: &ToolCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        Ok(BeforeTool::Sleep(Sleep {
            signals: vec!["approval".into()],
            until_millis: None,
        }))
    }
    async fn generation(
        &self,
        event: GenerationEvent,
        value: &Value,
        _: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        Ok(match event {
            GenerationEvent::BeforeRequest => {
                GenerationDecision::Replace(json!(value.as_i64().unwrap() + 1))
            }
            GenerationEvent::OnYield => GenerationDecision::Continue(json!("continue")),
            GenerationEvent::AfterRun => GenerationDecision::Tasks(vec![HookTask {
                name: "review".into(),
                input: json!(null),
            }]),
            _ => GenerationDecision::Pass,
        })
    }
}
#[tokio::test]
async fn gate_stops_chain_and_generation_decisions_are_returned() {
    let mut r = Registry::default();
    let mut a = ext("gate");
    a.hooks.push(Arc::new(Gate));
    a.hooks.push(Arc::new(Rewrite(99)));
    r.install(a).unwrap();
    let memo = MemoryMemo::default();
    let snapshot = json!({});
    let ctx = Context {
        snapshot: &snapshot,
        memo: &memo,
    };
    let resolved = r.snapshot().resolve(None);
    let (call, decision, _) = resolved
        .before_tool(
            ToolCall {
                name: "x".into(),
                arguments: json!(1),
            },
            &ctx,
        )
        .await;
    assert_eq!(call.arguments, json!(1));
    assert!(matches!(decision, BeforeTool::Sleep(_)));
    assert_eq!(memo.get("deadline").await.unwrap(), None);
    assert_eq!(
        resolved
            .generation(GenerationEvent::BeforeRequest, json!(1), &ctx)
            .await
            .0,
        json!(2)
    );
    assert_eq!(
        resolved
            .generation(GenerationEvent::OnYield, json!(null), &ctx)
            .await
            .1,
        Some(json!("continue"))
    );
    assert_eq!(
        resolved
            .generation(GenerationEvent::AfterRun, json!(null), &ctx)
            .await
            .2[0]
            .name,
        "review"
    );
}

struct YieldTasks;
#[async_trait]
impl Hook for YieldTasks {
    async fn generation(
        &self,
        event: GenerationEvent,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event == GenerationEvent::OnYield {
            Ok(GenerationDecision::Tasks(vec![HookTask {
                name: "review".into(),
                input: json!(null),
            }]))
        } else {
            Ok(GenerationDecision::Pass)
        }
    }
}
#[tokio::test]
async fn first_yield_task_decision_precedes_later_continuation() {
    let mut registry = Registry::default();
    let mut extension = ext("review");
    extension.hooks.push(Arc::new(YieldTasks));
    extension.hooks.push(Arc::new(Gate));
    registry.install(extension).unwrap();
    let memo = MemoryMemo::default();
    let snapshot = json!({});
    let ctx = Context {
        snapshot: &snapshot,
        memo: &memo,
    };
    let (_, continuation, tasks, reports) = registry
        .snapshot()
        .resolve(None)
        .generation(GenerationEvent::OnYield, json!(null), &ctx)
        .await;
    assert_eq!(continuation, None);
    assert_eq!(tasks.len(), 1);
    assert!(reports.is_empty());
}
