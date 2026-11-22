use futures_util::stream;
use pi_rs::*;
use pi_rs_workbench::coding::CodingTools;
use serde_json::json;
use std::sync::{Arc, Mutex};

struct Script(Mutex<Vec<ModelRequest>>);
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let calls = if requests.len() == 1 {
            [
                ("ls-float", "ls", json!({"limit": 1.0})),
                ("ls-zero", "ls", json!({"limit": 0})),
                ("ls-negative", "ls", json!({"limit": -2})),
                ("ls-fraction", "ls", json!({"limit": 1.5})),
                ("ls-fraction-two", "ls", json!({"limit": 2.99})),
                ("ls-exact", "ls", json!({"limit": 3})),
                ("ls-huge", "ls", json!({"limit": 1e100})),
                ("ls-missing", "ls", json!({"path": "missing"})),
                (
                    "grep-missing",
                    "grep",
                    json!({"path": "missing", "pattern": "match"}),
                ),
                ("read-offset", "read", json!({"path": "a", "offset": 9})),
                ("find-float", "find", json!({"pattern":"*", "limit": 1.0})),
                ("find-zero", "find", json!({"pattern":"*", "limit": 0})),
                ("find-negative", "find", json!({"pattern":"*", "limit": -1})),
                (
                    "find-fraction",
                    "find",
                    json!({"pattern":"*", "limit": 1.5}),
                ),
                ("find-two", "find", json!({"pattern":"*", "limit": 2.99})),
                ("find-exact", "find", json!({"pattern":"*", "limit": 3})),
                (
                    "find-large",
                    "find",
                    json!({"pattern":"*", "limit": 18446744073709551615u64}),
                ),
                ("find-exp", "find", json!({"pattern":"*", "limit": 1e21})),
                (
                    "find-missing-fraction",
                    "find",
                    json!({"pattern":"*", "path":"missing", "limit":1.5}),
                ),
                (
                    "find-missing-negative",
                    "find",
                    json!({"pattern":"*", "path":"missing", "limit":-1}),
                ),
                (
                    "find-missing-large",
                    "find",
                    json!({"pattern":"*", "path":"missing", "limit":1e21}),
                ),
            ]
            .into_iter()
            .chain(cfg!(unix).then(|| {
                (
                    "bash-timeout",
                    "bash",
                    json!({"command": "sleep 4", "timeout": 0.25}),
                )
            }))
            .map(|(id, name, args)| ToolCall {
                id: id.into(),
                name: name.into(),
                arguments: args.to_string(),
            })
            .collect()
        } else {
            vec![]
        };
        Ok(Box::pin(stream::iter(vec![Ok(ModelEvent::Complete {
            calls,
            usage: None,
        })])))
    }
}

#[tokio::test]
async fn local_numeric_limits_pass_dispatch_and_match_pi_output() {
    let root = tempfile::tempdir().unwrap();
    for name in ["a", "b", "c"] {
        std::fs::write(root.path().join(name), "").unwrap();
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CodingTools::local(root.path()).unwrap()))
        .unwrap();
    let model = Script(Mutex::new(vec![]));
    let agent = Agent {
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    agent
        .run(
            &mut session,
            Some("search".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: Notification<'_>| {},
        )
        .await
        .unwrap();
    let tool_results: std::collections::HashMap<_, _> = session
        .operations()
        .into_iter()
        .map(|(invocation, state)| (invocation.call.id, state))
        .collect();
    for call_id in ["ls-missing", "grep-missing", "read-offset", "find-exp"]
        .into_iter()
        .chain(cfg!(unix).then_some("bash-timeout"))
    {
        assert!(
            matches!(
                &tool_results[call_id],
                OperationState::Finished(Outcome::TextResult {
                    details: Some(details),
                    is_error: true,
                    ..
                }) if *details == json!({})
            ),
            "{call_id}: {:?}",
            tool_results[call_id]
        );
    }
    let requests = model.0.lock().unwrap();
    let replies: std::collections::HashMap<_, _> = requests[1]
        .messages
        .iter()
        .filter_map(|m| match m {
            Message::Tool {
                call_id,
                content,
                is_error,
                ..
            } => Some((call_id.as_str(), (content.as_str(), *is_error))),
            _ => None,
        })
        .collect();
    for id in ["ls-float", "ls-zero", "ls-negative", "ls-fraction"] {
        assert_eq!(
            replies[id],
            (
                "a\n\n[1 entries limit reached. Use limit=2 for more]",
                false
            )
        );
    }
    assert_eq!(
        replies["ls-fraction-two"],
        (
            "a\nb\n\n[2 entries limit reached. Use limit=4 for more]",
            false
        )
    );
    assert_eq!(replies["ls-exact"], ("a\nb\nc", false));
    assert_eq!(replies["ls-huge"], ("a\nb\nc", false));
    let missing = format!(
        "Path not found: {}",
        root.path()
            .canonicalize()
            .unwrap()
            .join("missing")
            .display()
    );
    assert_eq!(replies["ls-missing"], (missing.as_str(), true));
    assert_eq!(replies["grep-missing"], (missing.as_str(), true));
    #[cfg(unix)]
    assert_eq!(
        replies["bash-timeout"],
        ("Command timed out after 0.25 seconds", true)
    );
    assert_eq!(
        replies["read-offset"],
        ("Offset 9 is beyond end of file (1 lines total)", true)
    );
    for id in ["find-float", "find-zero", "find-negative", "find-fraction"] {
        assert_eq!(
            replies[id],
            (
                "a\n\n[1 results limit reached. Use limit=2 for more, or refine pattern]",
                false
            )
        );
    }
    assert_eq!(
        replies["find-two"],
        (
            "a\nb\n\n[2 results limit reached. Use limit=4 for more, or refine pattern]",
            false
        )
    );
    assert_eq!(replies["find-exact"], ("a\nb\nc", false));
    assert_eq!(
        replies["find-large"],
        (
            "error: invalid value '18446744073709552000' for '--max-results <count>': number too large to fit in target type\n\nFor more information, try '--help'.",
            true
        )
    );
    assert_eq!(
        replies["find-missing-fraction"],
        replies["find-missing-negative"]
    );
    assert!(
        replies["find-missing-fraction"]
            .0
            .contains("is not a directory.")
    );
    assert!(replies["find-missing-fraction"].1);
    assert_eq!(replies["find-missing-large"], replies["find-exp"]);
    assert_eq!(
        replies["find-exp"],
        (
            "error: invalid value '1e+21' for '--max-results <count>': invalid digit found in string\n\nFor more information, try '--help'.",
            true
        )
    );
}
