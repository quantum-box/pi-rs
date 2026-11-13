use std::{
    sync::{Arc, Mutex},
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
async fn resource_listings_filter_mcp_apps_and_presentation_metadata_across_pages() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
    let server_calls = calls.clone();
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let calls = server_calls.clone();
            tokio::spawn(async move {
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                if socket.read_line(&mut line).await.is_err() {
                    return;
                }
                let mut length = 0;
                loop {
                    line.clear();
                    if socket.read_line(&mut line).await.is_err()
                        || line == "\r\n"
                        || line.is_empty()
                    {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut bytes = vec![0; length];
                if socket.read_exact(&mut bytes).await.is_err() {
                    return;
                }
                let request: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                if let Some(method) = request["method"]
                    .as_str()
                    .filter(|method| method.starts_with("resources/"))
                {
                    calls.lock().unwrap().push((
                        method.to_owned(),
                        request["params"]["cursor"].as_str().map(str::to_owned),
                    ));
                }
                let result = match request["method"].as_str() {
                    Some("initialize") => Some(
                        json!({"protocolVersion":request["params"]["protocolVersion"],"capabilities":{"resources":{}},"serverInfo":{"name":"fixture","version":"1"}}),
                    ),
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
                let _ = socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
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
    extension.close().await;
    server.abort();
}
