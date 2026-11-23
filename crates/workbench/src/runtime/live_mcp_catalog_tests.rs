use super::*;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

async fn reply_to_mcp(
    socket: TcpStream,
    initialized: Arc<std::sync::Mutex<Vec<String>>>,
    hide_old_tool: Arc<std::sync::atomic::AtomicBool>,
) -> std::io::Result<()> {
    let mut socket = BufReader::new(socket);
    let mut line = String::new();
    if socket.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let path = line
        .split_ascii_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    let mut length = 0;
    loop {
        line.clear();
        if socket.read_line(&mut line).await? == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap();
        }
    }
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await?;
    let request: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    let result = match request["method"].as_str() {
        Some("initialize") => {
            initialized.lock().unwrap().push(path.clone());
            Some(json!({
                "protocolVersion":request["params"]["protocolVersion"],
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"fixture","version":"1"}
            }))
        }
        Some("tools/list") => {
            let mut tools = vec![json!({"name":"shared","inputSchema":{"type":"object"}})];
            if path != "/old" || !hide_old_tool.load(std::sync::atomic::Ordering::SeqCst) {
                tools.push(
                    json!({"name":path.trim_start_matches('/'),"inputSchema":{"type":"object"}}),
                );
            }
            Some(json!({"tools":tools}))
        }
        Some("tools/call") => {
            initialized.lock().unwrap().push(format!("{path}:call"));
            Some(json!({"content":[]}))
        }
        _ => None,
    };
    let (status, body) = match result {
        Some(result) => (
            "200 OK",
            json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string(),
        ),
        None => ("202 Accepted", String::new()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    socket.write_all(response.as_bytes()).await
}

#[tokio::test]
async fn live_mcp_catalog_connects_replaces_and_retires_declared_tools() {
    struct Owner;
    impl pi_rs::Extension for Owner {
        fn id(&self) -> &str {
            "fixture-owner"
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let initialized = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = initialized.clone();
    let hide_old = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hide = hide_old.clone();
    let peer = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let calls = calls.clone();
            let hide = hide.clone();
            tokio::spawn(async move {
                let _ = reply_to_mcp(socket, calls, hide).await;
            });
        }
    });

    let workspace = tempfile::tempdir().unwrap();
    let mut args = Cli::try_parse_from(["pi", "--offline", "--no-trust"]).unwrap();
    args.workspace = workspace.path().into();
    let catalog = LiveMcpCatalog {
        args,
        builtin_tool_search: false,
        resources: Default::default(),
        configured_namespaces: Default::default(),
        state: std::sync::Mutex::new(McpCatalogState {
            servers: Vec::new(),
            declarations: Default::default(),
            installed_ids: Default::default(),
        }),
        refresh: tokio::sync::Mutex::new(()),
        closing: Default::default(),
    };
    let mut registry = Registry::default();
    registry.register(Arc::new(Owner)).unwrap();
    let server = |name: &str| pi_rs::McpServerDeclaration {
        name: "sample".into(),
        config: json!({"url":format!("{url}/{name}"),"oauth":false,"exposure":"direct"}),
    };

    use pi_rs::RegistryRefresher as _;
    let scenario = async {
        assert!(catalog.refresh(&registry).await.unwrap().is_none());
        registry
            .register_mcp_server("fixture-owner", server("old"))
            .unwrap();
        let updated = catalog.refresh(&registry).await.unwrap().unwrap().registry;
        assert!(updated.has_tool("mcp__sample__old"));
        let old_connection = catalog.servers()[0].clone();
        assert!(updated.has_tool("mcp__sample__shared"));
        let reused = catalog.refresh(&updated).await.unwrap().unwrap();
        assert!(reused.registry.has_tool("mcp__sample__old"));

        hide_old.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut updated = catalog
            .refresh_for_dispatch(&reused.registry)
            .await
            .unwrap()
            .unwrap()
            .registry;
        assert!(!updated.has_tool("mcp__sample__old"));
        assert!(updated.has_tool("mcp__sample__shared"));

        updated
            .register_mcp_server("fixture-owner", server("new"))
            .unwrap();
        let dispatch = catalog
            .refresh_for_dispatch(&updated)
            .await
            .unwrap()
            .unwrap();
        assert!(!dispatch.registry.has_tool("mcp__sample__shared"));
        assert!(old_connection.is_retired());
        let original_tool = old_connection
            .tools()
            .into_iter()
            .find(|tool| tool.spec().name == "mcp__sample__shared")
            .unwrap();
        let outcome = original_tool
            .invoke(
                json!({}),
                pi_rs::ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await;
        assert!(outcome.is_error());
        assert_eq!(*initialized.lock().unwrap(), ["/old"]);
        let mut updated = catalog.refresh(&updated).await.unwrap().unwrap().registry;
        assert!(!updated.has_tool("mcp__sample__old"));
        assert!(updated.has_tool("mcp__sample__new"));
        assert!(updated.has_tool("mcp__sample__shared"));
        assert!(old_connection.is_retired());

        assert!(updated.unregister_mcp_server("fixture-owner", "sample"));
        let removed = catalog.refresh(&updated).await.unwrap().unwrap();
        assert!(!removed.registry.has_tool("mcp__sample__new"));
        assert!(catalog.servers().is_empty());
        // The caller may still hold a stale registry from a previous turn.
        let stale = catalog.refresh(&updated).await.unwrap().unwrap();
        assert!(!stale.registry.has_tool("mcp__sample__new"));
        assert_eq!(*initialized.lock().unwrap(), ["/old", "/new"]);
        catalog.close().await;
    };
    let result = tokio::time::timeout(Duration::from_secs(15), scenario).await;
    peer.abort();
    result.unwrap();
}
