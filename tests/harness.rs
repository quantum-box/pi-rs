use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures_util::stream;
use pi_rs::*;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::mpsc;

#[derive(Default)]
struct Script {
    replies: Mutex<VecDeque<Vec<ModelEvent>>>,
    requests: Mutex<Vec<ModelRequest>>,
    concurrent_start: Option<tokio::sync::Barrier>,
}

impl Script {
    fn new(replies: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let request_number = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request);
            requests.len()
        };
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected model request");
        if request_number <= 2
            && let Some(barrier) = &self.concurrent_start
        {
            barrier.wait().await;
        }
        Ok(Box::pin(stream::iter(reply.into_iter().map(Ok))))
    }
}

struct Fixture {
    called: AtomicUsize,
    hold: bool,
    effects: Mutex<BTreeMap<uuid::Uuid, String>>,
}

#[async_trait::async_trait]
impl Tool for Fixture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "lookup".into(),
            description: "Fixture lookup".into(),
            parameters: json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}),
        }
    }

    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        self.called.fetch_add(1, Ordering::SeqCst);
        let effect = format!("found {}", args["id"].as_str().unwrap());
        self.effects
            .lock()
            .unwrap()
            .insert(context.operation_id, effect.clone());
        if self.hold {
            std::future::pending::<()>().await;
        }
        Outcome::Succeeded(effect)
    }
}

struct Tools(Arc<Fixture>);
impl Extension for Tools {
    fn id(&self) -> &str {
        "test-tools"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![self.0.clone()]
    }
}

struct Policy;
#[async_trait::async_trait]
impl Extension for Policy {
    fn id(&self) -> &str {
        "test-policy"
    }

    async fn before_tool(
        &self,
        _: &str,
        args: &Value,
        _: &ToolContext,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        Ok(if args["id"] == "denied" {
            Decision::Deny("host denied".into())
        } else {
            Decision::Allow
        })
    }
}

fn agent<'a>(model: &'a dyn Model, registry: &'a Registry) -> Agent<'a> {
    Agent {
        model,
        model_name: "fixture",
        registry,
        system_prompt: "fixture prompt",
        limits: Limits::default(),
    }
}

fn tool_reply(args: &[&str]) -> Vec<ModelEvent> {
    vec![ModelEvent::Complete {
        calls: args
            .iter()
            .enumerate()
            .map(|(index, arguments)| ToolCall {
                id: format!("call-{index}"),
                name: "lookup".into(),
                arguments: (*arguments).into(),
            })
            .collect(),
        usage: Some(Usage {
            input_tokens: 20,
            output_tokens: 8,
        }),
    }]
}

fn answer(value: &str) -> Vec<ModelEvent> {
    vec![
        ModelEvent::Text(value.into()),
        ModelEvent::Complete {
            calls: vec![],
            usage: Some(Usage {
                input_tokens: 20,
                output_tokens: 8,
            }),
        },
    ]
}

fn ignore(_: Notification<'_>) {}

#[tokio::test]
async fn validation_policy_and_queued_input_keep_complete_tool_groups() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let model = Script::new(vec![
        tool_reply(&[
            "{bad",
            r#"{"id":1}"#,
            r#"{"id":"denied"}"#,
            r#"{"id":"42"}"#,
        ]),
        answer("done"),
    ]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Policy)).unwrap();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let (sender, mut inputs) = mpsc::channel(4);
    let mut observer = |event: Notification<'_>| {
        if matches!(
            event,
            Notification::Committed(Record {
                event: Event::ToolStarted { .. },
                ..
            })
        ) {
            sender
                .try_send("steer after the tool batch".into())
                .unwrap();
        }
    };
    let agent = agent(&model, &registry);
    assert_eq!(
        agent
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut observer
            )
            .await
            .unwrap(),
        "done"
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .operations()
            .iter()
            .filter(|(_, state)| matches!(state, OperationState::Finished(Outcome::Skipped(_))))
            .count(),
        3
    );
    let requests = model.requests.lock().unwrap();
    let messages = &requests[1].messages;
    assert!(
        matches!(&messages[2], Message::Assistant { calls, .. } if calls.len() == 4 && calls[0].arguments == "{}")
    );
    assert!(matches!(&messages[6], Message::Tool { content, .. } if content == "found 42"));
    assert!(
        matches!(&messages[7], Message::User { content } if content == "steer after the tool batch")
    );
}

#[tokio::test]
async fn interrupted_dispatch_is_never_replayed_and_can_be_reconciled() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("session");
    let mut session = Session::open(&path).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"first"}"#, r#"{"id":"second"}"#]),
        answer("recovered"),
    ]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(4);
    let cancel = CancellationToken::new();

    // Drop the run after the tool begins, with no opportunity to record a result.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            agent.run(
                &mut session,
                Some("lookup".into()),
                &mut inputs,
                &cancel,
                &mut ignore,
            )
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert_eq!(session.operations()[0].1, OperationState::Started);
    assert_eq!(session.operations()[1].1, OperationState::Planned);
    drop(session);

    let mut session = Session::open(&path).unwrap();
    let err = agent
        .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
        .await
        .unwrap_err();
    let Error::Reconciliation(operation_id) = err else {
        panic!("{err}")
    };
    assert!(matches!(
        session.operations()[0].1,
        OperationState::Finished(Outcome::Unknown(_))
    ));
    assert!(matches!(
        session.operations()[1].1,
        OperationState::Finished(Outcome::Skipped(_))
    ));
    assert!(session.checkout(1).is_err());
    let confirmed = fixture
        .effects
        .lock()
        .unwrap()
        .get(&operation_id)
        .unwrap()
        .clone();
    session
        .reconcile(operation_id, Outcome::Succeeded(confirmed))
        .unwrap();
    let result = agent
        .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
        .await
        .unwrap();
    assert_eq!(result, "recovered");
    assert_eq!(
        agent
            .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
            .await
            .unwrap(),
        result
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert_eq!(model.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancellation_after_dispatch_leaves_unknown_result() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":"42"}"#])]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let observer = &mut |event: Notification<'_>| {
        if matches!(
            event,
            Notification::Committed(Record {
                event: Event::ToolStarted { .. },
                ..
            })
        ) {
            let signal = signal.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                signal.cancel();
            });
        }
    };
    let error = agent
        .run(
            &mut session,
            Some("lookup".into()),
            &mut inputs,
            &cancel,
            observer,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Reconciliation(_)));
    assert!(matches!(
        session.operations()[0].1,
        OperationState::Finished(Outcome::Unknown(_))
    ));
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn independent_sessions_run_together_and_checkout_restores_context() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("one");
    let mut first = Session::open(&path).unwrap();
    assert!(Session::open(&path).is_err());
    let mut second = Session::open(dir.path().join("two")).unwrap();
    let mut model = Script::new(vec![
        answer("one"),
        answer("two"),
        answer("continued"),
        answer("fork"),
    ]);
    model.concurrent_start = Some(tokio::sync::Barrier::new(2));
    let registry = Registry::default();
    let agent = agent(&model, &registry);
    let (_, mut queue1) = mpsc::channel(1);
    let (_, mut queue2) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let mut observer_one = ignore;
    let mut observer_two = ignore;
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            agent.run(
                &mut first,
                Some("private original".into()),
                &mut queue1,
                &cancel,
                &mut observer_one
            ),
            agent.run(
                &mut second,
                Some("other session".into()),
                &mut queue2,
                &cancel,
                &mut observer_two
            ),
        )
    })
    .await
    .expect("both independent model requests should overlap");
    assert_eq!(a.unwrap(), "one");
    assert_eq!(b.unwrap(), "two");
    let original = first.head();
    agent
        .compact(
            &mut first,
            Compaction {
                summary: "key fact 42".into(),
                keep_turns: 0,
            },
            &mut ignore,
        )
        .unwrap();
    drop(first);
    let mut first = Session::open(&path).unwrap();
    agent
        .run(
            &mut first,
            Some("use the summary".into()),
            &mut queue1,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    first.checkout(original).unwrap();
    drop(first);
    let mut first = Session::open(&path).unwrap();
    assert_eq!(first.head(), original);
    agent
        .run(
            &mut first,
            Some("follow original".into()),
            &mut queue1,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    let compacted = serde_json::to_string(&requests[2]).unwrap();
    let forked = serde_json::to_string(&requests[3]).unwrap();
    assert!(compacted.contains("key fact 42") && !compacted.contains("private original"));
    assert!(forked.contains("private original") && !forked.contains("key fact 42"));
    assert!(!compacted.contains("other session") && !forked.contains("other session"));
}

#[tokio::test]
async fn incomplete_stream_commits_neither_partial_reply_nor_tool_and_recovers_torn_tail() {
    use std::io::Write;
    let dir = tempdir().unwrap();
    let path = dir.path().join("session");
    let mut session = Session::open(&path).unwrap();
    let model = Script::new(vec![
        vec![ModelEvent::Text("partial text".into())],
        answer("after reconnect"),
    ]);
    let registry = Registry::default();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    assert!(matches!(
        agent
            .run(
                &mut session,
                Some("original".into()),
                &mut inputs,
                &cancel,
                &mut ignore
            )
            .await,
        Err(Error::Model(_))
    ));
    assert!(
        !session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Assistant { .. }))
    );
    let head = session.head();
    drop(session);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{\"partial\":")
        .unwrap();
    let mut session = Session::open(&path).unwrap();
    assert_eq!(session.head(), head);
    assert_eq!(
        agent
            .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
            .await
            .unwrap(),
        "after reconnect"
    );
    drop(session);
    assert!(Session::open(&path).is_ok());
}

#[test]
fn project_instructions_are_ordered_and_skills_are_explicit() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join(".pi/skills/first")).unwrap();
    std::fs::create_dir(root.join("nested")).unwrap();
    std::fs::write(root.join("AGENTS.md"), "root instructions").unwrap();
    std::fs::write(root.join("nested/AGENTS.md"), "nested instructions").unwrap();
    std::fs::write(root.join(".pi/skills/first/SKILL.md"), "skill text").unwrap();
    let out = project_context(root, root.join("nested"), &["first".into()]).unwrap();
    assert!(out.find("root instructions").unwrap() < out.find("nested instructions").unwrap());
    assert!(out.ends_with("skill text"));
    assert!(
        !project_context(root, root, &[])
            .unwrap()
            .contains("skill text")
    );
    assert!(project_context(root, root, &["../first".into()]).is_err());
}

#[tokio::test]
async fn model_summary_uses_no_tools_and_preserves_the_prior_branch() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let model = Script::new(vec![
        answer("initial answer"),
        answer("key fact 42"),
        answer("42"),
    ]);
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    agent
        .run(
            &mut session,
            Some("private original".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    agent
        .summarize(&mut session, 0, &cancel, &mut ignore)
        .await
        .unwrap();
    agent
        .run(
            &mut session,
            Some("What was the key fact?".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    assert!(requests[1].tools.is_empty());
    assert!(requests[1].messages.iter().any(
        |message| matches!(message, Message::User { content } if content == "private original")
    ));
    assert!(requests[2].messages.iter().any(
        |message| matches!(message, Message::User { content } if content.contains("key fact 42"))
    ));
    assert!(!requests[2].messages.iter().any(
        |message| matches!(message, Message::User { content } if content == "private original")
    ));
    assert!(session.records().iter().any(
        |record| matches!(&record.event, Event::User { content } if content == "private original")
    ));
    assert_eq!(fixture.called.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ollama_sse_requires_end_marker_even_after_finish_reason() {
    use futures_util::StreamExt;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        server.local_addr().unwrap()
    );
    let serve = std::thread::spawn(move || {
        let (mut socket, _) = server.accept().unwrap();
        let mut reader = BufReader::new(&socket);
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" || header.is_empty() {
                break;
            }
        }
        let body = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    });
    let model = ollama::Ollama::new(&endpoint, None).unwrap();
    let mut stream = model
        .stream(ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 32,
        })
        .await
        .unwrap();
    assert!(
        matches!(stream.next().await.unwrap().unwrap(), ModelEvent::Text(value) if value == "partial")
    );
    assert!(matches!(stream.next().await.unwrap(), Err(Error::Model(_))));
    assert!(stream.next().await.is_none());
    serve.join().unwrap();
}

#[tokio::test]
async fn tool_timeout_blocks_until_reconciliation() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":"42"}"#])]);
    let mut agent = agent(&model, &registry);
    agent.limits.tool_timeout = Duration::from_millis(20);
    let (_, mut inputs) = mpsc::channel(1);
    let error = agent
        .run(
            &mut session,
            Some("lookup".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Reconciliation(_)));
    assert!(
        matches!(&session.operations()[0].1, OperationState::Finished(Outcome::Unknown(reason)) if reason == "tool timed out")
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
}
