#![cfg(feature = "coding-tools")]

use async_trait::async_trait;
use pi_rs_core::{CancellationToken, Invocation, Outcome, ToolCall, ToolHost, ToolSpec};
use pi_rs_durable::{
    coding_tools,
    recovery::{ReplayPolicy, ToolRecovery, UncertainPolicy, recover_tool},
    runtime::RegisteredTool,
    tool,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

struct Host;
#[async_trait]
impl ToolHost for Host {
    fn tools(&self) -> Vec<ToolSpec> {
        vec![]
    }
    async fn call(&self, _: &str, _: Value) -> Outcome {
        Outcome::Skipped("unused".into())
    }
}

async fn call(tools: &[RegisteredTool], name: &str, args: Value) -> Outcome {
    let registered = tools.iter().find(|t| t.tool.spec().name == name).unwrap();
    let invocation = Invocation {
        operation_id: uuid::Uuid::new_v4(),
        call: ToolCall {
            id: "call".into(),
            name: name.into(),
            arguments: args.to_string(),
        },
    };
    let cancel = CancellationToken::new();
    let prepared = tool::prepare(
        registered.tool.as_ref(),
        &invocation,
        &cancel,
        Duration::from_secs(5),
        jsonschema::is_valid,
    )
    .await
    .unwrap();
    tool::execute(
        registered.tool.as_ref(),
        &invocation,
        prepared,
        None,
        Arc::new(Host),
        &cancel,
        Duration::from_secs(5),
    )
    .await
    .outcome
}

#[tokio::test]
async fn coding_tools_execute_through_durable_adapter() {
    let dir = tempfile::tempdir().unwrap();
    let tools = coding_tools::local(dir.path()).unwrap();
    let mut names: Vec<_> = tools.iter().map(|t| t.tool.spec().name).collect();
    names.sort();
    assert_eq!(names, ["bash", "edit", "read", "write"]);
    assert!(tools.iter().all(|t| t.replay == ReplayPolicy::Unsafe));
    assert!(matches!(
        call(
            &tools,
            "write",
            json!({"path":"sub/file.txt","content":"one\ntwo\n"})
        )
        .await,
        Outcome::Succeeded(_)
    ));
    let read = call(
        &tools,
        "read",
        json!({"path":"sub/file.txt","offset":2,"limit":1}),
    )
    .await;
    assert!(format!("{read:?}").contains("two"));
    let edited = call(&tools, "edit", json!({"path":"sub/file.txt","edits":[{"oldText":"one","newText":"ONE"},{"oldText":"two","newText":"TWO"}]})).await;
    assert!(
        matches!(
            edited,
            Outcome::Succeeded(_)
                | Outcome::SucceededWithDetails { .. }
                | Outcome::Structured { .. }
        ),
        "{edited:?}"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("sub/file.txt")).unwrap(),
        "ONE\nTWO\n"
    );
    let failed = call(
        &tools,
        "edit",
        json!({"path":"sub/file.txt","edits":[{"oldText":"missing","newText":"x"}]}),
    )
    .await;
    assert!(matches!(failed, Outcome::Failed(_)));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("sub/file.txt")).unwrap(),
        "ONE\nTWO\n"
    );
    let bash = call(&tools, "bash", json!({"command":"printf durable"})).await;
    assert!(format!("{bash:?}").contains("durable"), "{bash:?}");
    let failed = call(&tools, "bash", json!({"command":"exit 7"})).await;
    assert!(matches!(failed, Outcome::Failed(_)), "{failed:?}");
}

#[test]
fn interrupted_builtins_are_never_automatically_replayed() {
    let dir = tempfile::tempdir().unwrap();
    for tool in coding_tools::local(dir.path()).unwrap() {
        assert_eq!(
            recover_tool(
                tool.replay,
                Some(tool.replay),
                0,
                3,
                UncertainPolicy::InterruptedResult
            ),
            ToolRecovery::Interrupted
        );
        assert_eq!(
            recover_tool(
                tool.replay,
                Some(tool.replay),
                0,
                3,
                UncertainPolicy::RequireReconciliation
            ),
            ToolRecovery::Reconcile
        );
        assert_ne!(
            recover_tool(
                tool.replay,
                Some(ReplayPolicy::Safe),
                0,
                3,
                UncertainPolicy::InterruptedResult
            ),
            ToolRecovery::Replay
        );
    }
}
