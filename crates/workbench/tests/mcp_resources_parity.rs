use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use pi_rs::{CancellationToken, Extension, Outcome, ToolContext};
use pi_rs_workbench::mcp::{McpExtension, ServerConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

struct PageModel(Mutex<Vec<pi_rs::ModelRequest>>);
#[async_trait::async_trait]
impl pi_rs::Model for PageModel {
    async fn stream(&self, request: pi_rs::ModelRequest) -> pi_rs::Result<pi_rs::ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let calls = if requests.len() == 1 {
            vec![pi_rs::ToolCall {
                id: "page".into(),
                name: "mcp__fixture__list_resources_page".into(),
                arguments: json!({"cursor":"oversized"}).to_string(),
            }]
        } else {
            vec![]
        };
        Ok(Box::pin(stream::iter(vec![Ok(
            pi_rs::ModelEvent::Complete { calls, usage: None },
        )])))
    }
}

#[tokio::test]
async fn http_mcp_resources_and_tool_retries_match_pi() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
    let server_calls = calls.clone();
    let initialize_attempts = Arc::new(AtomicUsize::new(0));
    let server_initialize_attempts = initialize_attempts.clone();
    let tool_requests = Arc::new(Mutex::new(Vec::new()));
    let server_tool_requests = tool_requests.clone();
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let calls = server_calls.clone();
            let attempts = server_initialize_attempts.clone();
            let tool_requests = server_tool_requests.clone();
            tokio::spawn(async move {
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                if socket.read_line(&mut line).await.is_err() {
                    return;
                }
                let templates_unimplemented = line.starts_with("POST /without-templates ");
                let mut length = 0;
                let mut session = String::new();
                loop {
                    line.clear();
                    if socket.read_line(&mut line).await.is_err()
                        || line == "\r\n"
                        || line.is_empty()
                    {
                        break;
                    }
                    if let Some((header, value)) = line.split_once(':') {
                        if header.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        } else if header.eq_ignore_ascii_case("mcp-session-id") {
                            session = value.trim().to_owned();
                        }
                    }
                }
                let mut bytes = vec![0; length];
                if socket.read_exact(&mut bytes).await.is_err() {
                    return;
                }
                let request: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                let method = request["method"].as_str().unwrap_or_default();
                let cursor = request["params"]["cursor"].as_str();
                let scenario = request["params"]["arguments"]["scenario"].as_str();
                let mut session_header = String::new();
                let mut status = None;
                if method.starts_with("resources/") || method == "tools/call" {
                    let mut calls = calls.lock().unwrap();
                    let first = !calls
                        .iter()
                        .any(|(name, value)| name == method && value.as_deref() == cursor);
                    calls.push((method.to_owned(), cursor.map(str::to_owned)));
                    status = match (method, cursor, first) {
                        ("resources/list", Some("retry-408"), true) => Some("408 Request Timeout"),
                        ("resources/list", Some("retry-429"), true) => {
                            Some("429 Too Many Requests")
                        }
                        ("resources/templates/list", Some("retry-503"), true) => {
                            Some("503 Unavailable")
                        }
                        ("resources/list", Some("never-retry-501"), _) => {
                            Some("501 Not Implemented")
                        }
                        ("resources/list", Some("always-503"), _) => Some("503 Unavailable"),
                        ("tools/call", _, _) => {
                            tool_requests
                                .lock()
                                .unwrap()
                                .push((session.clone(), request["params"].clone()));
                            match (scenario, session.as_str()) {
                                (Some("always-expired"), _) | (Some("recover"), "3") => {
                                    Some("404 Not Found")
                                }
                                (Some("recover"), _) => None,
                                _ => Some("503 Unavailable"),
                            }
                        }
                        _ => None,
                    };
                }
                if method == "initialize" {
                    let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                    status = match attempt {
                        1 => Some("503 Unavailable"),
                        2 => Some("429 Too Many Requests"),
                        _ => {
                            session_header = format!("Mcp-Session-Id: {attempt}\r\n");
                            None
                        }
                    };
                }
                if let Some(status) = status {
                    socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                    return;
                }
                if templates_unimplemented && request["method"] == "resources/templates/list" {
                    let (code, message) = if request["params"]["cursor"] == "fault" {
                        (-32603, "fixture internal error")
                    } else {
                        (-32601, "resource templates are unavailable")
                    };
                    let body = json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":code,"message":message}}).to_string();
                    let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
                    return;
                }
                let result = match request["method"].as_str() {
                    Some("initialize") => Some(
                        json!({"protocolVersion":request["params"]["protocolVersion"],"capabilities":{"resources":{},"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}),
                    ),
                    Some("tools/list") => Some(
                        json!({"tools":[{"name":"write_fixture","inputSchema":{"type":"object"}}]}),
                    ),
                    Some("tools/call") => {
                        Some(json!({"content":[{"type":"text","text":"confirmed"}]}))
                    }
                    Some("resources/list")
                        if matches!(
                            request["params"]["cursor"].as_str(),
                            Some("  opaque \t" | "")
                        ) =>
                    {
                        Some(json!({"resources":[],"nextCursor":request["params"]["cursor"]}))
                    }
                    Some("resources/templates/list")
                        if matches!(
                            request["params"]["cursor"].as_str(),
                            Some("  opaque \t" | "")
                        ) =>
                    {
                        Some(
                            json!({"resourceTemplates":[],"nextCursor":request["params"]["cursor"]}),
                        )
                    }
                    Some("resources/list") if request["params"]["cursor"] == "oversized" => Some(
                        json!({"resources":[{"uri":"file:///large","name":"large","description":"猫\n".repeat(300_000)}],"nextCursor":"after-large"}),
                    ),
                    Some("resources/list") if request["params"]["cursor"] == "next" => {
                        Some(json!({"resources":[
                            {"uri":"file:///second.txt","name":"second","mimeType":"text/plain","_meta":{"secret":"discard"},"icons":[{"src":"https://example.invalid/icon"}]},
                        {"uri":"file:///panel.html","name":"panel","mimeType":"text/html; PROFILE = \"mcp-app\""},
                        {"uri":"file:///extension.html","name":"prefix-match","mimeType":"text/html;profile=mcp-app-extra"}
                        ]}))
                    }
                    Some("resources/list") => Some(json!({"resources":[
                        {"uri":"file:///first.txt","name":"first","description":"readable"},
                        {"uri":"ui://fixture/panel","name":"ui"},
                        {"uri":"file:///plain.html","name":"plain","mimeType":"text/html;profile=standard"}
                    ],"nextCursor":"next"})),
                    Some("resources/templates/list") if request["params"]["cursor"] == "next" => {
                        Some(json!({"resourceTemplates":[
                            {"uriTemplate":"file:///items/{id}","name":"item","mimeType":"text/plain","_meta":{"private":true},"icons":[{"src":"https://example.invalid/icon"}]},
                        {"uriTemplate":"file:///app/{id}","name":"app","mimeType":"text/html;profile=mcp-app"},
                        {"uriTemplate":"file:///extension/{id}","name":"prefix-match","mimeType":"text/html;profile=mcp-app-extra"}
                        ]}))
                    }
                    Some("resources/templates/list") => Some(
                        json!({"resourceTemplates":[{"uriTemplate":"ui://fixture/{id}","name":"ui"}],"nextCursor":"next"}),
                    ),
                    _ => None,
                };
                let (status, body) = match result {
                    Some(result) => (
                        "200 OK",
                        json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string(),
                    ),
                    None => ("202 Accepted", String::new()),
                };
                let _ = socket.write_all(format!("HTTP/1.1 {status}\r\n{session_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
            });
        }
    });
    let directory = tempfile::tempdir().unwrap();
    let config: ServerConfig =
        serde_json::from_value(json!({"url":format!("http://{address}/mcp"),"oauth":false}))
            .unwrap();
    let extension = Arc::new(
        McpExtension::connect("fixture", &config, directory.path())
            .await
            .unwrap(),
    );
    assert_eq!(initialize_attempts.load(Ordering::Relaxed), 3);
    let tools = extension.tools();
    for (suffix, key, names) in [
        (
            "list_resources",
            "resources",
            vec!["first", "plain", "second"],
        ),
        ("list_resource_templates", "resourceTemplates", vec!["item"]),
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.spec().name == format!("mcp__fixture__{suffix}"))
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            tool.invoke(
                json!({}),
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: CancellationToken::new(),
                },
            ),
        )
        .await
        .unwrap();
        let Outcome::Structured { data, .. } = result else {
            panic!("unexpected outcome {result:?}")
        };
        let entries = data[key].as_array().unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            names
        );
        assert!(
            entries
                .iter()
                .all(|entry| entry.get("icons").is_none() && entry.get("_meta").is_none())
        );
    }
    for (suffix, key, first, second) in [
        (
            "list_resources_page",
            "resources",
            vec!["first", "plain"],
            vec!["second"],
        ),
        (
            "list_resource_templates_page",
            "resourceTemplates",
            vec![],
            vec!["item"],
        ),
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.spec().name == format!("mcp__fixture__{suffix}"))
            .unwrap();
        let before = calls.lock().unwrap().len();
        for (arguments, expected, next) in [
            (json!({}), first, Some("next")),
            (json!({"cursor":"next"}), second, None),
        ] {
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                tool.invoke(
                    arguments,
                    ToolContext {
                        operation_id: uuid::Uuid::new_v4(),
                        cancellation: CancellationToken::new(),
                    },
                ),
            )
            .await
            .unwrap();
            let Outcome::Structured { data, .. } = result else {
                panic!("unexpected page {result:?}")
            };
            assert_eq!(data["server"], "fixture");
            assert_eq!(data["nextCursor"].as_str(), next);
            let entries = data[key].as_array().unwrap();
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry["name"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(entries.iter().all(|entry| entry["server"] == "fixture"
                && entry.get("icons").is_none()
                && entry.get("_meta").is_none()));
        }
        {
            let logged = calls.lock().unwrap();
            assert_eq!(logged.len(), before + 2);
            assert_eq!(logged[before].1, None);
            assert_eq!(logged[before + 1].1.as_deref(), Some("next"));
        }
        for cursor in ["  opaque \t", ""] {
            let result = tool
                .invoke(
                    json!({"cursor":cursor}),
                    ToolContext {
                        operation_id: uuid::Uuid::new_v4(),
                        cancellation: CancellationToken::new(),
                    },
                )
                .await;
            let Outcome::Structured { data, .. } = result else {
                panic!("unexpected cursor page {result:?}")
            };
            assert_eq!(data["nextCursor"], cursor);
            assert_eq!(
                calls.lock().unwrap().last().unwrap().1.as_deref(),
                Some(cursor)
            );
        }
        let before_invalid = calls.lock().unwrap().len();
        let invalid = tool
            .invoke(
                json!({"cursor":42}),
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await;
        assert!(
            matches!(invalid, Outcome::Failed(message) if message.contains("cursor must be a string"))
        );
        assert_eq!(calls.lock().unwrap().len(), before_invalid);
    }
    for (tool, cursor, attempts, succeeds) in [
        ("list_resources_page", "retry-408", 2, true),
        ("list_resources_page", "retry-429", 2, true),
        ("list_resource_templates_page", "retry-503", 2, true),
        ("list_resources_page", "never-retry-501", 1, false),
        ("list_resources_page", "always-503", 2, false),
        ("write_fixture", "", 1, false),
    ] {
        let tool = tools
            .iter()
            .find(|configured| configured.spec().name == format!("mcp__fixture__{tool}"))
            .unwrap();
        let arguments = if cursor.is_empty() {
            json!({})
        } else {
            json!({"cursor":cursor})
        };
        let before = calls.lock().unwrap().len();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            tool.invoke(
                arguments,
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: CancellationToken::new(),
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            matches!(outcome, Outcome::Structured { .. }),
            succeeds,
            "{cursor}: {outcome:?}"
        );
        assert_eq!(
            calls.lock().unwrap().len() - before,
            attempts,
            "{cursor}: {outcome:?}"
        );
    }

    let hidden_config: ServerConfig = serde_json::from_value(json!({"url":format!("http://{address}/mcp"),"oauth":false,"exposure":"direct","toolExposure":{"list_resources":"hidden"}})).unwrap();
    let hidden = McpExtension::connect("hidden", &hidden_config, directory.path())
        .await
        .unwrap();
    let names: Vec<_> = hidden.tools().iter().map(|tool| tool.spec().name).collect();
    assert!(
        !names
            .iter()
            .any(|name| name.starts_with("mcp__hidden__list_resources"))
    );
    assert!(names.contains(&"mcp__hidden__list_resource_templates_page".to_owned()));
    hidden.close().await;

    // Exercise the actual agent output limit, then recover and decode the page.
    let mut registry = pi_rs::Registry::default();
    registry.register(extension.clone()).unwrap();
    let model = PageModel(Mutex::new(Vec::new()));
    let agent = pi_rs::Agent {
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: pi_rs::Limits::default(),
    };
    let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    agent
        .run(
            &mut session,
            Some("list".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: pi_rs::Notification<'_>| {},
        )
        .await
        .unwrap();
    let id = {
        let requests = model.0.lock().unwrap();
        let content = requests[1]
            .messages
            .iter()
            .find_map(|message| match message {
                pi_rs::Message::Tool {
                    call_id,
                    content,
                    is_error,
                    ..
                } if call_id == "page" && !is_error => Some(content),
                _ => None,
            })
            .expect("agent must retain the page handle");
        let result: Value = serde_json::from_str(content).unwrap();
        let envelope = &result["data"];
        assert_eq!(envelope["nextCursor"], "after-large");
        assert!(content.len() < 1024 * 1024);
        envelope["outputId"].as_str().unwrap().to_owned()
    };
    let page_tool = tools
        .iter()
        .find(|tool| tool.spec().name == "mcp__fixture__list_resources_page")
        .unwrap();
    let mut offset = 0;
    let mut page = String::new();
    loop {
        let result = page_tool
            .invoke(
                json!({"outputId": id, "offset": offset}),
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await;
        let Outcome::Structured { data, .. } = result else {
            panic!("bad chunk: {result:?}")
        };
        page.push_str(data["text"].as_str().unwrap());
        match data["nextOffset"].as_u64() {
            Some(next) => offset = next,
            None => break,
        }
    }
    let page: Value = serde_json::from_str(&page).unwrap();
    assert_eq!(page["nextCursor"], "after-large");
    assert_eq!(page["resources"][0]["description"], "猫\n".repeat(300_000));

    let writer = tools
        .iter()
        .find(|tool| tool.spec().name == "mcp__fixture__write_fixture")
        .unwrap();
    assert_eq!(initialize_attempts.load(Ordering::Relaxed), 4);
    for (scenario, sessions, succeeds) in [
        ("recover", ["3", "5"], true),
        ("always-expired", ["5", "6"], false),
    ] {
        let before = tool_requests.lock().unwrap().len();
        let operation_id = uuid::Uuid::new_v4();
        let arguments = json!({"scenario": scenario});
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            writer.invoke(
                arguments.clone(),
                ToolContext {
                    operation_id,
                    cancellation: CancellationToken::new(),
                },
            ),
        )
        .await
        .unwrap();
        if succeeds {
            let Outcome::Structured { data, .. } = outcome else {
                panic!("MCP session did not recover: {outcome:?}");
            };
            assert_eq!(data["content"][0]["text"], "confirmed");
        } else {
            assert!(matches!(outcome, Outcome::Unknown(_)), "{outcome:?}");
        }
        let requests = tool_requests.lock().unwrap();
        assert_eq!(requests.len() - before, sessions.len());
        for ((session, params), expected_session) in requests[before..].iter().zip(sessions) {
            assert_eq!(session, expected_session);
            assert_eq!(params["name"], "write_fixture");
            assert_eq!(params["arguments"], arguments);
            assert_eq!(params["_meta"]["operationId"], operation_id.to_string());
        }
    }
    assert_eq!(initialize_attempts.load(Ordering::Relaxed), 6);

    let legacy_config: ServerConfig = serde_json::from_value(json!({
        "url":format!("http://{address}/without-templates"),
        "oauth":false
    }))
    .unwrap();
    let legacy = McpExtension::connect("legacy", &legacy_config, directory.path())
        .await
        .unwrap();
    for (name, arguments, expected) in [
        (
            "mcp__legacy__list_resource_templates",
            json!({}),
            json!({"resourceTemplates":[]}),
        ),
        (
            "mcp__legacy__list_resource_templates_page",
            json!({"cursor":"stale"}),
            json!({"server":"legacy","resourceTemplates":[]}),
        ),
    ] {
        let tool = legacy
            .tools()
            .into_iter()
            .find(|tool| tool.spec().name == name)
            .unwrap();
        let context = || ToolContext {
            operation_id: uuid::Uuid::new_v4(),
            cancellation: CancellationToken::new(),
        };
        let result = tool.invoke(arguments, context()).await;
        let Outcome::Structured { data, .. } = result else {
            panic!("an unimplemented template method should return an empty result: {result:?}");
        };
        assert_eq!(data, expected);
        if name.ends_with("_page") {
            let error = tool.invoke(json!({"cursor":"fault"}), context()).await;
            assert!(
                matches!(&error, Outcome::Unknown(message) if message.contains("fixture internal error")),
                "expected the existing MCP error outcome, got {error:?}"
            );
        }
    }
    legacy.close().await;
    extension.close().await;
    server.abort();
}
