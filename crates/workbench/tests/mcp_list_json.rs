use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn list_json_reports_servers_tools_sources_and_failures() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reject_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reject_address = reject_listener.local_addr().unwrap();
    let reject = tokio::spawn(async move {
        while let Ok((mut socket, _)) = reject_listener.accept().await {
            tokio::spawn(async move {
                let mut bytes = [0; 4096];
                let _ = socket.read(&mut bytes).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            });
        }
    });
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
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
                let result = match request["method"].as_str() {
                    Some("initialize") => Some(
                        json!({"protocolVersion":request["params"]["protocolVersion"],"capabilities":{"tools":{},"resources":{}},"serverInfo":{"name":"fixture","version":"1"}}),
                    ),
                    Some("tools/list") => Some(
                        json!({"tools":[{"name":"Public","inputSchema":{"type":"object"}},{"name":"Private","inputSchema":{"type":"object"}}]}),
                    ),
                    _ => None,
                };
                let (status, content_type, body) = match result {
                    Some(result) => {
                        let reply = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
                        if request["method"] == "tools/list" {
                            let notification = json!({
                                "jsonrpc":"2.0","method":"notifications/message",
                                "params":{"level":"warning","logger":"test","data":"fixture log\nsecond line"}
                            });
                            (
                                "200 OK",
                                "text/event-stream",
                                format!("data: {notification}\n\ndata: {reply}\n\n"),
                            )
                        } else {
                            ("200 OK", "application/json", reply.to_string())
                        }
                    }
                    None => ("202 Accepted", "application/json", String::new()),
                };
                let _ = socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
            });
        }
    });
    std::fs::write(
        home.join("mcp.json"),
        json!({"mcpServers":{
            "fixture":{"url":format!("http://{address}/mcp"),"oauth":false},
            "disabled":{"command":"ignored","args":["--token","private-argument"],"enabled":false},
            "failed":{"url":format!("http://{reject_address}/mcp?token=private-query"),"timeout":1,"oauth":false}
        }})
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        project.join("mcp.json"),
        json!({"mcpServers":{
            "fixture":{"toolExposure":{"P*":"direct","*":"deferred","Private":"hidden"}},
            "local":{"command":"ignored","enabled":false}
        }})
        .to_string(),
    )
    .unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env_remove("HTTP_PROXY")
            .env_remove("http_proxy")
            .env_remove("HTTPS_PROXY")
            .env_remove("https_proxy")
            .env_remove("ALL_PROXY")
            .env_remove("all_proxy")
            .args(["--trust", "mcp", "list", "--json"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    server.abort();
    reject.abort();
    assert!(!output.status.success());
    let data: Value =
        serde_json::from_slice(&output.stdout).expect("stdout must be one JSON document");
    assert_eq!(data["errors"], json!([]));
    let reports = data["servers"].as_array().unwrap();
    assert_eq!(
        reports
            .iter()
            .map(|server| server["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["fixture", "disabled", "failed", "local"]
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("private-argument"));
    assert!(!stdout.contains("private-query"));
    assert!(!stdout.contains("fixture log"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture log"));
    let log = std::fs::read_to_string(home.join("mcp.log")).unwrap();
    assert!(log.contains("[fixture] warning test: fixture log\n    second line\n"));
    let find = |name: &str| {
        reports
            .iter()
            .find(|server| server["name"] == name)
            .unwrap()
    };
    let fixture = find("fixture");
    assert_eq!(fixture["state"], "connected");
    assert_eq!(fixture["scope"], "global");
    assert_eq!(
        fixture["source"],
        home.join("mcp.json").to_string_lossy().as_ref()
    );
    assert_eq!(
        fixture["override"],
        std::fs::canonicalize(project.join("mcp.json"))
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    assert_eq!(fixture["tools"], json!(["Public", "Private"]));
    assert_eq!(
        fixture["toolExposure"],
        json!({"Public":"direct","Private":"hidden"})
    );
    assert_eq!(fixture["resources"], true);
    assert_eq!(find("disabled")["state"], "disabled");
    assert_eq!(find("local")["scope"], "project");
    assert_eq!(find("failed")["state"], "failed");
    assert!(find("failed")["error"].is_string());
}

#[test]
fn list_json_reports_invalid_entries_and_untrusted_project() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(
        home.join("mcp.json"),
        json!({"mcpServers":{
            "invalid":{"command":"ignored","enabled":"yes"},
            "valid":{"command":"ignored","enabled":false},
            "a-b":{"command":"ignored","enabled":"yes"},
            "a_b":{"command":"ignored","enabled":false}
        }})
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        project.join("mcp.json"),
        json!({"mcpServers":{"local":{"command":"ignored","enabled":false}}}).to_string(),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--no-trust", "mcp", "list", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["servers"].as_array().unwrap().len(), 2);
    assert_eq!(data["servers"][0]["name"], "valid");
    assert_eq!(data["servers"][1]["name"], "a_b");
    assert_eq!(data["errors"].as_array().unwrap().len(), 2);
    assert!(data["note"].as_str().unwrap().contains("not trusted"));
}

#[test]
fn list_json_rejects_non_object_mcp_servers() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("mcp.json"), r#"{"mcpServers":[]}"#).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args(["--no-trust", "mcp", "list", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["servers"], json!([]));
    assert!(
        data["errors"][0]
            .as_str()
            .unwrap()
            .contains("mcpServers must be an object")
    );
}

#[test]
fn list_json_keeps_other_source_when_a_file_is_malformed() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .args(["--trust", "mcp", "list", "--json"])
            .output()
            .unwrap()
    };
    std::fs::write(
        home.join("mcp.json"),
        json!({"mcpServers":{"global":{"command":"ignored","enabled":false}}}).to_string(),
    )
    .unwrap();
    std::fs::write(project.join("mcp.json"), "{not-json").unwrap();
    let output = run();
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["servers"][0]["name"], "global");
    assert!(data["errors"][0].as_str().unwrap().contains(".pi"));
    assert!(data["errors"][0].as_str().unwrap().contains("mcp.json"));
    std::fs::write(home.join("mcp.json"), "{not-json").unwrap();
    std::fs::write(
        project.join("mcp.json"),
        json!({"mcpServers":{"local":{"command":"ignored","enabled":false}}}).to_string(),
    )
    .unwrap();
    let output = run();
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["servers"][0]["name"], "local");
    assert!(data["errors"][0].as_str().unwrap().contains("home"));
}

#[test]
fn list_json_keeps_global_on_invalid_project_replacement_and_redacts_values() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(home.join("mcp.json"), json!({"mcpServers":{
        "zeta":{"url":"https://username:password@example.invalid/private-path?key=query-secret#fragment-secret","enabled":false},
        "same":{"command":"global-command","enabled":false},
        "same2":{"command":"global-two","enabled":false},
        "replaced":{"command":"ignored","enabled":"invalid"},
        "a-b":{"command":"ignored","url":"https://example.invalid","enabled":false},
        "a_b":{"command":"ignored","enabled":false},
        "b-c":{"command":"ignored","url":"https://example.invalid","enabled":true},
        "b_c":{"command":"ignored","enabled":false},
        "bad":{"command":"ignored","enabled":"private-invalid-value"}
    }}).to_string()).unwrap();
    std::fs::write(
        project.join("mcp.json"),
        json!({"mcpServers":{
            "same":{"command":"project-command","enabled":"invalid"},
            "same2":null,
            "replaced":{"command":"replacement","enabled":false},
            "local":{"command":"ignored","enabled":false}
        }})
        .to_string(),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--trust", "mcp", "list", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for secret in [
        "username",
        "password",
        "private-path",
        "query-secret",
        "fragment-secret",
        "private-invalid-value",
    ] {
        assert!(!stdout.contains(secret));
    }
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    let servers = data["servers"].as_array().unwrap();
    assert_eq!(
        servers
            .iter()
            .map(|server| server["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["zeta", "same", "same2", "a_b", "b_c", "replaced", "local"]
    );
    assert_eq!(servers[1]["scope"], "global");
    assert_eq!(servers[1]["transport"], "global-command");
    assert_eq!(servers[2]["transport"], "global-two");
    assert_eq!(servers[5]["scope"], "project");
    assert_eq!(data["errors"].as_array().unwrap().len(), 6);
}

#[test]
fn list_json_collision_identifies_project_source() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(
        home.join("mcp.json"),
        json!({"mcpServers":{"a-b":{"command":"ignored","enabled":false}}}).to_string(),
    )
    .unwrap();
    std::fs::write(
        project.join("mcp.json"),
        json!({"mcpServers":{"a_b":{"command":"ignored","enabled":false}}}).to_string(),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--trust", "mcp", "list", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["servers"][0]["name"], "a-b");
    let error = data["errors"][0].as_str().unwrap();
    assert!(error.contains(".pi") && error.contains("mcp.json") && error.contains("a_b"));
}

#[test]
fn list_json_reports_invalid_name_from_each_source() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let data = json!({"mcpServers":{"bad name":{"command":"ignored","enabled":false}}}).to_string();
    std::fs::write(home.join("mcp.json"), &data).unwrap();
    std::fs::write(project.join("mcp.json"), &data).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--trust", "mcp", "list", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    let errors = data["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 2);
    assert!(errors[0].as_str().unwrap().contains("home"));
    assert!(errors[1].as_str().unwrap().contains(".pi"));
}

#[cfg(target_os = "linux")]
#[test]
fn list_json_fails_when_stdout_cannot_be_written() {
    let root = tempfile::tempdir().unwrap();
    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args(["--no-trust", "mcp", "list", "--json"])
        .stdout(full)
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn list_reports_invalid_auto_enable_codemode_in_each_trusted_scope() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let global_config = home.join("mcp.json");
    let project_config = project.join("mcp.json");
    std::fs::write(
        &global_config,
        r#"{"autoEnableCodemode":"private-value","mcpServers":{"global":{"command":"unused","enabled":false}}}"#,
    )
    .unwrap();
    std::fs::write(
        &project_config,
        r#"{"autoEnableCodemode":null,"mcpServers":{"local":{"command":"unused","enabled":false}}}"#,
    )
    .unwrap();

    let run = |trust, json_output| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .args([trust, "mcp", "list"]);
        if json_output {
            command.arg("--json");
        }
        command.output().unwrap()
    };
    let output = run("--trust", true);
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    let issue = |path: &std::path::Path| {
        assert!(!path.as_os_str().is_empty());
        format!("{}: autoEnableCodemode must be a boolean", path.display())
    };
    let errors = data["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0], issue(&global_config));
    assert!(
        errors[1]
            .as_str()
            .unwrap()
            .ends_with(&issue(std::path::Path::new(".pi/mcp.json")))
    );
    assert_eq!(data["servers"][0]["name"], "global");
    assert_eq!(data["servers"][1]["name"], "local");
    assert!(!data.to_string().contains("private-value"));

    let output = run("--no-trust", true);
    assert!(!output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["errors"], json!([issue(&global_config)]));
    assert_eq!(data["servers"].as_array().unwrap().len(), 1);

    let output = run("--trust", false);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&issue(&global_config)), "{stderr}");
    assert!(
        stderr.contains(&issue(std::path::Path::new(".pi/mcp.json"))),
        "{stderr}"
    );
    assert!(!stderr.contains("private-value"), "{stderr}");
}

#[test]
fn text_list_reports_bad_configuration_and_continues_with_the_other_scope() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let user_file = home.join("mcp.json");
    let project_file = project.join("mcp.json");

    for (bad_file, good_file, invalid, diagnostic) in [
        (
            &user_file,
            &project_file,
            r#"{"mcpServers":[]}"#,
            "mcpServers must be an object",
        ),
        (
            &project_file,
            &user_file,
            r#"{"mcpServers":"private-value""#,
            "could not read or parse MCP configuration",
        ),
    ] {
        std::fs::write(bad_file, invalid).unwrap();
        std::fs::write(
            good_file,
            r#"{"mcpServers":{"retired":{"command":"unused","enabled":false}}}"#,
        )
        .unwrap();
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .args(["--trust", "--offline", "mcp", "list"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        let expected = format!("mcp.json: {diagnostic}");
        assert!(stderr.contains(&expected), "{stderr}");
        assert!(!stderr.contains("private-value"), "{stderr}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("retired") && stdout.contains("disabled"),
            "{stdout}"
        );
    }
}
