use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

use pi_rs::{
    Agent, CancellationToken, Limits, MemoryStore, Notification, Registry, Session, ollama::Ollama,
};
use serde_json::{Value, json};

#[test]
fn offline_package_update_is_a_noop_but_checks_requested_source() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let settings = home.join("settings.json");
    let contents = json!({"packages": ["git:https://example.com/team/package.git"]}).to_string();
    std::fs::write(&settings, &contents).unwrap();

    for source in [None, Some("git:https://example.com/team/package.git")] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command.current_dir(root.path()).env("PI_RS_HOME", &home);
        command.args(["--offline", "update"]);
        if let Some(source) = source {
            command.arg(source);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        assert_eq!(std::fs::read_to_string(&settings).unwrap(), contents);
        assert!(!home.join("packages").exists());
    }

    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args([
            "--offline",
            "update",
            "git:https://example.com/team/missing.git",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("package is not configured"));
}

#[test]
fn explicit_prompt_directory_loads_immediate_files_in_rpc() {
    let root = tempfile::tempdir().unwrap();
    let prompts = root.path().join("custom");
    std::fs::create_dir_all(prompts.join("nested")).unwrap();
    std::fs::write(prompts.join("nested/deep.md"), "Nested").unwrap();
    std::fs::write(prompts.join("top.md"), "Top level").unwrap();
    std::fs::write(prompts.join(".hidden.md"), "Hidden").unwrap();
    std::fs::write(prompts.join("ignored.md"), "Ignored").unwrap();
    std::fs::write(prompts.join(".gitignore"), "ignored.md\n").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("top.md", prompts.join("audit.md")).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--no-prompt-templates",
            "--prompt-template",
        ])
        .arg(&prompts)
        .args(["--mode", "rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    writeln!(input, "{}", json!({"id":"commands", "type":"get_commands"})).unwrap();
    input.flush().unwrap();
    let response = loop {
        let event = receiver.recv_timeout(Duration::from_secs(30)).unwrap();
        if event["id"] == "commands" {
            break event;
        }
    };
    let commands = response["data"]["commands"]
        .as_array()
        .expect("command list");
    for name in ["top", ".hidden", "ignored"] {
        assert!(
            commands
                .iter()
                .any(|command| command["name"] == name && command["source"] == "prompt"),
            "{response}"
        );
    }
    assert!(
        !commands.iter().any(|command| command["name"] == "deep"),
        "{response}"
    );
    #[cfg(unix)]
    assert!(
        commands
            .iter()
            .any(|command| command["name"] == "audit" && command["source"] == "prompt"),
        "{response}"
    );
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

// Some hosted runners open idle connections to local listeners. Read each
// connection independently so one idle peer cannot block the real request.
fn accept_proxy_request(listener: TcpListener) -> (TcpStream, BufReader<TcpStream>, String) {
    // Windows duplicates may not retain the original listener’s nonblocking mode.
    // Without this, accept can block after a reader has received the request.
    listener.set_nonblocking(true).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "proxy request not received before deadline"
        );
        if let Ok(request) = receiver.try_recv() {
            return request;
        }
        match listener.accept() {
            Ok((socket, _)) => {
                let sender = sender.clone();
                std::thread::spawn(move || {
                    // Accepted sockets can inherit O_NONBLOCK on macOS.
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    let mut reader = BufReader::new(socket.try_clone().unwrap());
                    let mut first = String::new();
                    if reader.read_line(&mut first).is_ok() && !first.is_empty() {
                        let _ = sender.send((socket, reader, first));
                    }
                });
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("proxy request not received: {error}"),
        }
    }
}

#[test]
fn chatgpt_plan_provider_streams_through_the_cli() {
    responses_provider_streams_through_the_cli("openai-chatgpt");
}

#[test]
fn openai_api_key_provider_streams_through_the_cli() {
    responses_provider_streams_through_the_cli("openai");
}

#[test]
fn xai_provider_streams_through_the_cli() {
    responses_provider_streams_through_the_cli("xai");
}

#[test]
fn responses_model_sampling_uses_pi_thinking_level_and_overrides() {
    for (level, temperature, top_k) in [("high", 0.2, 40), ("off", 0.7, 10)] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        std::fs::write(root.path().join("models.json"), json!({"providers":{"openai":{
            "baseUrl":base,
            "models":[{"id":"fixture-model","reasoning":true,
                "thinkingLevelMap":{"high":"strong"},
                "samplingParams":{"temperature":0.7,"top_p":0.9,"top_k":10,"parallel_tool_calls":false,"max_tool_calls":3,"tool_choice":"required"},
                "samplingParamsByThinkingLevel":{"high":{"temperature":0.5,"top_k":40}}}],
            "modelOverrides":{"fixture-model":{"samplingParamsByThinkingLevel":{"high":{"temperature":0.2}}}}
        }}}).to_string()).unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = accept_proxy_request(listener);
            assert!(first.starts_with("POST /v1/responses "), "{first}");
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["temperature"], temperature);
            assert_eq!(body["top_p"], 0.9);
            assert_eq!(body["top_k"], top_k);
            assert_eq!(body["parallel_tool_calls"], false);
            assert_eq!(body["max_tool_calls"], 3);
            assert_eq!(body["tool_choice"], "required");
            if level == "high" {
                assert_eq!(body["reasoning"]["effort"], "strong");
            }
            let events = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--no-tools",
                "--offline",
                "--provider",
                "openai",
                "--model",
                "fixture-model",
                "--api-key",
                "fixture",
                "--thinking",
                level,
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        server.join().unwrap();
    }
}

#[test]
fn unrelated_provider_loads_legacy_responses_sampling_configuration() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("models.json"),
        json!({"providers":{
            "openai":{"models":[{"id":"legacy","samplingParams":{"stream":false}}]},
            "ollama":{"models":[{"id":"local"}]}
        }})
        .to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--no-mcp",
            "--offline",
            "--provider",
            "ollama",
            "--endpoint",
            "http://127.0.0.1:1/v1/chat/completions",
            "--list-models",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("local"));
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--no-tools",
            "--offline",
            "--provider",
            "openai",
            "--model",
            "legacy",
            "--api-key",
            "fixture",
            "--endpoint",
            "http://127.0.0.1:1/v1/responses",
            "--print",
            "hello",
        ])
        .output()
        .unwrap();
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        error.contains("openai/legacy") && error.contains("stream"),
        "{error}"
    );
}

#[test]
fn kimi_coding_models_send_their_specific_reasoning_options() {
    for (model, deepseek) in [("k3.1-pro", false), ("kimi-for-coding-highspeed", true)] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = accept_proxy_request(listener);
            assert!(first.starts_with("POST /v1/chat/completions "), "{first}");
            let mut length = 0;
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let line = line.to_ascii_lowercase();
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                headers.push_str(&line);
            }
            assert!(headers.contains("authorization: bearer fixture"));
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["model"], model);
            assert!(body["max_tokens"].is_number());
            assert!(body.get("max_completion_tokens").is_none());
            if deepseek {
                assert_eq!(body["thinking"]["type"], "enabled");
                assert!(body.get("reasoning_effort").is_none());
            } else {
                assert_eq!(body["reasoning_effort"], "high");
                assert!(body.get("thinking").is_none());
            }
            let events = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--no-tools",
                "--offline",
                "--provider",
                "kimi-coding",
                "--model",
                model,
                "--thinking",
                "high",
                "--api-key",
                "fixture",
                "--endpoint",
                &endpoint,
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{model}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
        server.join().unwrap();
    }
}

#[test]
fn qwen_token_plans_use_model_specific_thinking_and_tool_format() {
    for (provider, model, level, effort) in [
        ("qwen-token-plan", "deepseek-v3.2", "high", None),
        ("qwen-token-plan-cn", "glm-5.2", "high", Some("high")),
        (
            "qwen-token-plan-individual",
            "qwen3.8-max",
            "medium",
            Some("medium"),
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = loop {
                let (mut socket, reader, first) =
                    accept_proxy_request(listener.try_clone().unwrap());
                if first.starts_with("GET ") {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    continue;
                }
                break (socket, reader, first);
            };
            assert!(first.starts_with("POST /v1/chat/completions "), "{first}");
            let mut length = 0;
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let line = line.to_ascii_lowercase();
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
                headers.push_str(&line);
            }
            assert!(headers.contains("authorization: bearer fixture"));
            let mut bytes = vec![0; length];
            reader.read_exact(&mut bytes).unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["model"], model);
            assert_eq!(body["enable_thinking"], true);
            assert_eq!(body.get("reasoning_effort").and_then(Value::as_str), effort);
            assert!(body.get("store").is_none());
            assert_eq!(body["messages"][0]["role"], "system");
            assert_eq!(body["tools"][0]["function"]["strict"], false);
            let events = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--offline",
                "--tools",
                "read",
                "--provider",
                provider,
                "--model",
                model,
                "--thinking",
                level,
                "--api-key",
                "fixture",
                "--endpoint",
                &endpoint,
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{provider}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
        server.join().unwrap();
    }
}

#[test]
fn xiaomi_providers_send_thinking_and_bearer_credentials() {
    for provider in [
        "xiaomi",
        "xiaomi-token-plan-cn",
        "xiaomi-token-plan-ams",
        "xiaomi-token-plan-sgp",
    ] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = accept_proxy_request(listener);
            assert!(first.starts_with("POST /v1/chat/completions "), "{first}");
            let mut length = 0;
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let line = line.to_ascii_lowercase();
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                headers.push_str(&line);
            }
            assert!(headers.contains("authorization: bearer fixture"));
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["model"], "mimo-v2.6-pro");
            assert_eq!(body["thinking"]["type"], "enabled");
            assert!(body.get("reasoning_effort").is_none());
            assert!(body["max_completion_tokens"].is_number());
            assert!(body.get("max_tokens").is_none());
            let events = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--no-tools",
                "--offline",
                "--provider",
                provider,
                "--model",
                "mimo-v2.6-pro",
                "--thinking",
                "high",
                "--api-key",
                "fixture",
                "--endpoint",
                &endpoint,
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{provider}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
        server.join().unwrap();
    }
}

#[test]
fn vercel_gateway_streams_messages_with_api_key() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        assert!(first.starts_with("POST /v1/messages "), "{first}");
        let mut length = 0;
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            let line = line.to_ascii_lowercase();
            if let Some(value) = line.strip_prefix("content-length:") {
                length = value.trim().parse().unwrap();
            }
            headers.push_str(&line);
        }
        assert!(headers.contains("x-api-key: fixture"));
        assert!(!headers.contains("authorization: bearer"));
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["model"], "anthropic/claude-sonnet-4.6");
        assert_eq!(body["thinking"]["type"], "adaptive");
        let events = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"anthropic/claude-sonnet-4.6\"}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"fixture answer\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
    });
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--no-tools",
            "--offline",
            "--provider",
            "vercel-ai-gateway",
            "--model",
            "anthropic/claude-sonnet-4.6",
            "--thinking",
            "high",
            "--api-key",
            "fixture",
            "--endpoint",
            &endpoint,
            "--print",
            "hello",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
    server.join().unwrap();
}

#[test]
fn minimax_regional_providers_use_the_messages_adapter() {
    for provider in ["minimax", "minimax-cn"] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!(
            "http://{}/anthropic/v1/messages",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = accept_proxy_request(listener);
            assert!(first.starts_with("POST /anthropic/v1/messages "), "{first}");
            let mut length = 0;
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let line = line.to_ascii_lowercase();
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                headers.push_str(&line);
            }
            assert!(headers.contains("authorization: bearer fixture"));
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["model"], "MiniMax-M3");
            assert_eq!(body["stream"], true);
            assert_eq!(body["thinking"]["type"], "adaptive");
            assert!(body.get("output_config").is_none());
            let events = concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"MiniMax-M3\"}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"fixture answer\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--no-tools",
                "--offline",
                "--provider",
                provider,
                "--model",
                "MiniMax-M3",
                "--thinking",
                "high",
                "--api-key",
                "fixture",
                "--endpoint",
                &endpoint,
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{provider}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
        server.join().unwrap();
    }
}

fn responses_provider_streams_through_the_cli(provider: &str) {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        assert!(first.starts_with("POST /v1/responses "), "{first}");
        let mut length = None;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = Some(value.trim().parse::<usize>().unwrap());
            }
        }
        let mut body = vec![0; length.unwrap()];
        reader.read_exact(&mut body).unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["model"], "fixture-model");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        let events = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}",events.len()).unwrap();
    });
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--no-tools",
            "--offline",
            "--provider",
            provider,
            "--model",
            "fixture-model",
            "--api-key",
            "fixture",
            "--endpoint",
            &endpoint,
            "--print",
            "hello",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
    server.join().unwrap();
}

#[test]
fn chatgpt_account_switch_preserves_separate_saved_sessions() {
    let root = tempfile::tempdir().unwrap();
    let credential = |id: &str| json!({"client_id":id,"subject":format!("user-{id}"),"email":null,"id_token":"id","access_token":format!("access-{id}"),"refresh_token":format!("refresh-{id}"),"expires_at":4_000_000_000u64,"scopes":["chatgpt.tokens.use.direct"]});
    std::fs::write(root.path().join("chatgpt-registrations.json"),json!({"active":"one","accounts":[{"client_id":"one","subject":"user-one","email":null},{"client_id":"two","subject":"user-two","email":null}]}).to_string()).unwrap();
    std::fs::write(
        root.path().join("chatgpt-sessions.json"),
        json!({"one":credential("one"),"two":credential("two")}).to_string(),
    )
    .unwrap();
    std::fs::write(
        root.path().join("auth.json"),
        json!({"openai-chatgpt":{"type":"oauth","credential":credential("one")}}).to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args(["login", "openai-chatgpt", "--account", "two"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("access-two"));
    let auth: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("auth.json")).unwrap()).unwrap();
    assert_eq!(auth["openai-chatgpt"]["credential"]["client_id"], "two");
    let store: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("chatgpt-sessions.json")).unwrap())
            .unwrap();
    assert_eq!(store["one"]["client_id"], "one");
    assert_eq!(store["two"]["client_id"], "two");
}

#[cfg(unix)]
#[test]
fn print_and_json_prompts_are_reported_as_interactive_input() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("input.py");
    std::fs::write(
        &extension,
        r##"#!/usr/bin/env python3
import json, os, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'input-fixture', 'dependencies':[], 'commands':[], 'tools':[], 'input_hook':True}
elif request['type'] == 'input':
    with open(os.path.join(os.environ['PI_RS_HOME'], 'input-events.jsonl'), 'a') as log:
        log.write(json.dumps(request['event']) + '\n')
    result = {'action':'handled'}
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##,
    )
    .unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    for mode in ["print", "json"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--offline",
                "--extension",
            ])
            .arg(&extension)
            .stdin(Stdio::null());
        if mode == "print" {
            command.arg("--print");
        } else {
            command.args(["--mode", "json"]);
        }
        let output = command.arg("hello").output().unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let events = std::fs::read_to_string(root.path().join("input-events.jsonl")).unwrap();
    let events: Vec<Value> = events
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 2);
    for event in events {
        assert_eq!(event["source"], "interactive");
        assert_eq!(event["text"], "hello");
        assert!(event.get("streamingBehavior").is_none());
    }
}

#[cfg(unix)]
#[test]
fn extension_command_can_trigger_model_in_print_and_json_modes() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("trigger.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'trigger', 'dependencies':[], 'commands':['trigger'], 'tools':[], 'command_effects':True}
elif request['type'] == 'command_effects':
    result = {'actions':[{'type':'trigger_message','message':{'customType':'hint','content':'same input','display':False}}]}
elif request['type'] in ('context', 'before_request'):
    result = request['request']
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (endpoint, server) = model_fixture();
    for mode in ["--print", "--mode"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--endpoint",
                &endpoint,
                "--extension",
            ])
            .arg(&extension)
            .arg(mode);
        if mode == "--mode" {
            command.arg("json");
        }
        let output = command.arg("/trigger").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("fixture answer"));
    }
    server.join().unwrap();

    let (endpoint, server) = model_fixture_with_finish(None, "length");
    for mode in ["print", "json"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--endpoint",
                &endpoint,
                "--extension",
            ])
            .arg(&extension);
        if mode == "print" {
            command.arg("--print");
        } else {
            command.args(["--mode", "json"]);
        }
        let output = command
            .args(["extension", "trigger", "trigger"])
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        if mode == "print" {
            assert_eq!(output.status.code(), Some(1));
            assert!(String::from_utf8_lossy(&output.stderr).contains("Response was truncated"));
            assert!(stdout.contains("fixture answer"));
        } else {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let events: Vec<Value> = stdout
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(events[0]["type"], "session");
            assert!(events.iter().any(|event| event["type"] == "agent_start"));
            assert!(events.iter().any(|event| event["type"] == "agent_end"));
        }
    }
    server.join().unwrap();

    let (endpoint, server) = model_fixture();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--endpoint",
            &endpoint,
            "--extension",
        ])
        .arg(&extension)
        .args(["--mode", "rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    for id in ["first", "second"] {
        writeln!(
            input,
            "{}",
            json!({"id": id, "type": "prompt", "message": "/trigger"})
        )
        .unwrap();
        input.flush().unwrap();
        let mut started = false;
        loop {
            let event = receive.recv_timeout(Duration::from_secs(60)).unwrap();
            if event["type"] == "response" && event["id"] == id {
                assert_eq!(event["data"]["disposition"], "started", "{event}");
                started = true;
            }
            if event["type"] == "agent_end" {
                assert!(started, "turn ended before prompt was acknowledged");
                break;
            }
        }
    }
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    server.join().unwrap();
}

#[cfg(unix)]
#[test]
fn executable_extension_can_add_custom_model_context_without_display() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("context.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'context', 'dependencies':[], 'commands':['context'], 'tools':[], 'command_effects':True}
elif request['type'] == 'command_effects':
    result = {'actions':[{'type':'message','message':{'customType':'hint','content':'model context','display':False,'details':{'private':'metadata'}}}]}
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let session = root.path().join("session.jsonl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .arg("--session")
        .arg(&session)
        .args([
            "--mode",
            "rpc",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--extension",
        ])
        .arg(&extension)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let mut until = |predicate: &dyn Fn(&Value) -> bool| loop {
        let event = receive
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|error| {
                let _ = child.kill();
                panic!("RPC event timed out: {error}");
            });
        if predicate(&event) {
            break event;
        }
    };
    writeln!(
        input,
        "{}",
        json!({"id":"context", "type":"prompt", "message":"/context"})
    )
    .unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "context");
    assert_eq!(response["data"]["disposition"], "handled", "{response}");
    writeln!(input, "{}", json!({"id":"entries", "type":"get_entries"})).unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "entries");
    let entry = response["data"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["type"] == "custom_message")
        .unwrap();
    assert_eq!(entry["customType"], "hint");
    assert_eq!(entry["display"], false);
    assert_eq!(entry["details"]["private"], "metadata");
    writeln!(
        input,
        "{}",
        json!({"id":"messages", "type":"get_messages", "includeNative":true})
    )
    .unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "messages");
    assert_eq!(response["data"]["messages"][0]["role"], "custom");
    assert_eq!(response["data"]["messages"][0]["content"], "model context");
    assert_eq!(response["data"]["messages"][0]["display"], false);
    assert_eq!(
        response["data"]["nativeMessages"][0]["content"],
        "model context"
    );
    assert!(
        !response["data"]["nativeMessages"]
            .to_string()
            .contains("metadata")
    );
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reopened = Session::snapshot(&session).unwrap();
    assert!(
        matches!(reopened.current_messages().last(), Some(pi_rs::Message::User { content }) if content == "model context")
    );
}

#[cfg(unix)]
#[test]
fn executable_extension_commands_persist_custom_entries_without_model_messages() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("bookmark.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'bookmark', 'dependencies':[], 'commands':['bookmark'], 'tools':[], 'command_entries':True}
elif request['type'] == 'command_output':
    result = {'entries':[{'customType':'bookmark', 'data':{'note':request['arguments']}}]}
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let no_model = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .env_remove("ANTHROPIC_API_KEY")
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--provider",
            "anthropic",
            "--extension",
        ])
        .arg(&extension)
        .args(["extension", "bookmark", "bookmark", "offline"])
        .output()
        .unwrap();
    assert!(
        no_model.status.success(),
        "{}",
        String::from_utf8_lossy(&no_model.stderr)
    );
    let session = root.path().join("session.jsonl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .arg("--session")
        .arg(&session)
        .args([
            "--mode",
            "rpc",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--extension",
        ])
        .arg(&extension)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let mut until = |predicate: &dyn Fn(&Value) -> bool| loop {
        let event = receive
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|error| {
                let _ = child.kill();
                panic!("RPC event timed out: {error}");
            });
        if predicate(&event) {
            break event;
        }
    };
    writeln!(
        input,
        "{}",
        json!({"id":"bookmark", "type":"prompt", "message":"/bookmark remember this"})
    )
    .unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "bookmark");
    assert_eq!(response["success"], true, "{response}");
    assert_eq!(response["data"]["disposition"], "handled");
    writeln!(input, "{}", json!({"id":"entries", "type":"get_entries"})).unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "entries");
    assert!(
        response["data"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["type"] == "custom"
                && entry["customType"] == "bookmark"
                && entry["data"]["note"] == "remember this")
    );
    writeln!(input, "{}", json!({"id":"messages", "type":"get_messages"})).unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "messages");
    assert!(response["data"]["messages"].as_array().unwrap().is_empty());
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reopened = Session::snapshot(&session).unwrap();
    assert!(reopened.branch().iter().any(|record| matches!(&record.event, pi_rs::Event::CustomEntry { entry } if entry.custom_type == "bookmark" && entry.data.as_ref().unwrap()["note"] == "remember this")));
    assert!(reopened.current_messages().is_empty());
}

#[cfg(unix)]
#[test]
fn rpc_streaming_shell_operations_deliver_output_and_cancel() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("settings.json"),
        r#"{"shellCommandPrefix":"export PI_PREFIX_EXAMPLE=1"}"#,
    )
    .unwrap();
    let extension = root.path().join("stream.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, sys, time
if '--pi-rs-extension-bash-v1' in sys.argv:
    request = json.loads(sys.stdin.readline())
    assert request['event']['command'].startswith('export PI_PREFIX_EXAMPLE=1\nstream ')
    print(json.dumps({'type':'output', 'data':list('日本'.encode())}), flush=True)
    time.sleep(30 if request['event']['command'].endswith('stream wait') else 0.25)
    print(json.dumps({'type':'output', 'data':list(' done'.encode())}), flush=True)
    print(json.dumps({'type':'complete', 'response':{'Ok':7}}), flush=True)
else:
    request = json.load(sys.stdin)
    if request['type'] == 'describe':
        result = {'id':'stream', 'dependencies':[], 'commands':[], 'tools':[], 'user_bash_operations_hook':True}
    elif request['type'] == 'user_bash_operations':
        result = request['event']['command'].startswith('stream')
    else:
        result = None
    print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--mode",
            "rpc",
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--extension",
        ])
        .arg(&extension)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let mut until = |predicate: &dyn Fn(&Value) -> bool| {
        loop {
            let event = receive
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|error| {
                    let _ = child.kill();
                    panic!("RPC event timed out: {error}");
                });
            if predicate(&event) {
                break event;
            }
        }
    };
    writeln!(
        input,
        "{}",
        json!({"type":"bash", "id":"stream", "command":"stream ok", "excludeFromContext":true})
    )
    .unwrap();
    input.flush().unwrap();
    let update = until(&|event| event["type"] == "bash_execution_update");
    assert_eq!(update["output"], "日本");
    let response = until(&|event| event["type"] == "response" && event["id"] == "stream");
    assert_eq!(response["data"]["output"], "日本 done");
    assert_eq!(response["data"]["exitCode"], 7);
    writeln!(
        input,
        "{}",
        json!({"type":"get_entries", "id":"entries", "includeNative":true})
    )
    .unwrap();
    input.flush().unwrap();
    let entries = until(&|event| event["type"] == "response" && event["id"] == "entries");
    let entry = entries["data"]["nativeEntries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["event"]["type"] == "bash_execution")
        .unwrap();
    assert_eq!(entry["event"]["command"], "stream ok");
    assert_eq!(entry["event"]["excludeFromContext"], true);
    writeln!(
        input,
        "{}",
        json!({"type":"bash", "id":"cancelled", "command":"stream wait"})
    )
    .unwrap();
    input.flush().unwrap();
    until(&|event| event["type"] == "bash_execution_update");
    writeln!(input, "{}", json!({"type":"abort_bash", "id":"abort"})).unwrap();
    input.flush().unwrap();
    let response = until(&|event| event["type"] == "response" && event["id"] == "cancelled");
    assert_eq!(response["data"]["cancelled"], true);
    assert_eq!(response["data"]["output"], "日本");
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[cfg(unix)]
#[test]
fn rpc_notifies_native_extensions_of_actual_model_and_thinking_changes() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("models.json"),
        json!({"providers":{"ollama":{"models":[
            {"id":"alpha", "reasoning":true}, {"id":"beta", "reasoning":true}
        ]}}})
        .to_string(),
    )
    .unwrap();
    let extension = directory.path().join("selection.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'selection', 'dependencies':[], 'commands':[], 'tools':[], 'lifecycle_hook':True, 'model_selection_hook':True}
elif request['type'] == 'lifecycle':
    with open(os.path.join(os.environ['PI_RS_HOME'], 'events.jsonl'), 'a') as log:
        log.write(json.dumps(request['event']) + '\n')
    result = None
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", directory.path())
        .args([
            "--mode",
            "rpc",
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--model",
            "alpha",
            "--thinking",
            "off",
            "--extension",
        ])
        .arg(&extension)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    for request in [
        json!({"id":"think", "type":"set_thinking_level", "level":"low"}),
        json!({"id":"same-think", "type":"set_thinking_level", "level":"low"}),
        json!({"id":"model", "type":"set_model", "provider":"ollama", "modelId":"beta"}),
        json!({"id":"same-model", "type":"set_model", "provider":"ollama", "modelId":"beta"}),
    ] {
        writeln!(input, "{request}").unwrap();
        input.flush().unwrap();
        loop {
            let response = receive
                .recv_timeout(Duration::from_secs(20))
                .unwrap_or_else(|error| {
                    let _ = child.kill();
                    panic!("RPC event timed out: {error}");
                });
            if response["type"] == "response" && response["id"] == request["id"] {
                assert_eq!(response["success"], true, "{response}");
                break;
            }
        }
    }
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let events: Vec<Value> = std::fs::read_to_string(directory.path().join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(
        events[0],
        json!({"type":"thinking_level_select", "level":"low", "previousLevel":"off"})
    );
    assert_eq!(events[1]["type"], "model_select");
    assert_eq!(events[1]["previousModel"]["id"], "alpha");
    assert_eq!(events[1]["model"]["id"], "beta");
    assert_eq!(events[1]["source"], "set");
}

#[cfg(unix)]
#[test]
fn rpc_user_bash_extension_can_replace_execution_and_pass_through() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let extension = directory.path().join("shell_extension.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id': 'shell-hook', 'dependencies': [], 'commands': [], 'tools': [], 'user_bash_hook': True}
elif request['type'] == 'user_bash':
    event = request['event']
    if event['command'].startswith('touch '):
        assert event['excludeFromContext'] and event['cwd']
        result = {'output': 'intercepted', 'exitCode': 7}
    else:
        result = None
else:
    result = None
print(json.dumps({'version': 1, 'response': {'Ok': result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let marker = directory.path().join("should-not-exist");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", directory.path())
        .args([
            "--mode",
            "rpc",
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--extension",
        ])
        .arg(&extension)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (events, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if events
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    for (id, command, excluded, expected, exit) in [
        (
            "replace",
            format!("touch {}", marker.display()),
            true,
            "intercepted",
            7,
        ),
        ("pass", "printf passthrough".into(), false, "passthrough", 0),
    ] {
        writeln!(
            input,
            "{}",
            json!({"type":"bash", "id":id, "command":command, "excludeFromContext":excluded})
        )
        .unwrap();
        input.flush().unwrap();
        loop {
            let event = receive.recv_timeout(Duration::from_secs(15)).unwrap();
            if event["type"] == "response" && event["id"] == id {
                assert_eq!(event["success"], true, "{event}");
                assert_eq!(event["data"]["output"], expected);
                assert_eq!(event["data"]["exitCode"], exit);
                break;
            }
        }
    }
    assert!(!marker.exists());
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn rpc_live_toggles_override_project_settings_for_the_session() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(root.path().join(".pi")).unwrap();
    std::fs::write(
        home.join("models.json"),
        r#"{"providers":{"openai":{"models":[{"id":"second"}]}}}"#,
    )
    .unwrap();
    std::fs::write(
        home.join("settings.json"),
        r#"{"compaction":{"enabled":false},"retry":{"enabled":false}}"#,
    )
    .unwrap();
    std::fs::write(
        root.path().join(".pi/settings.json"),
        r#"{"compaction":{"enabled":true},"retry":{"enabled":true}}"#,
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("OPENAI_API_KEY", "test-key")
        .args([
            "--trust",
            "--no-mcp",
            "--offline",
            "--model",
            "fixture",
            "--no-session",
            "--context-window",
            "32",
            "--max-output-tokens",
            "128",
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    {
        let mut request = |id: &str, kind: &str, enabled: Option<bool>| -> Value {
            let mut command = json!({"id":id, "type":kind});
            if let Some(enabled) = enabled {
                command["enabled"] = json!(enabled);
            }
            writeln!(input, "{command}").unwrap();
            input.flush().unwrap();
            loop {
                let reply = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
                if reply["id"] == id && reply["type"] == "response" {
                    assert_eq!(reply["success"], true, "{reply}");
                    break reply;
                }
            }
        };
        assert_eq!(
            request("initial", "get_state", None)["data"]["autoCompactionEnabled"],
            true
        );
        for enabled in [false, true, false] {
            request("compact", "set_auto_compaction", Some(enabled));
            assert_eq!(
                request("state", "get_state", None)["data"]["autoCompactionEnabled"],
                enabled
            );
            request("retry", "set_auto_retry", Some(enabled));
            assert_eq!(
                request("after-retry", "get_state", None)["data"]["autoCompactionEnabled"],
                enabled
            );
            let saved: Value =
                serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap())
                    .unwrap();
            assert_eq!(saved["compaction"]["enabled"], enabled);
            assert_eq!(saved["retry"]["enabled"], enabled);
        }
    }
    let command =
        json!({"id":"switch", "type":"set_model", "provider":"openai", "modelId":"second"});
    writeln!(input, "{command}").unwrap();
    input.flush().unwrap();
    loop {
        let reply = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
        if reply["id"] == "switch" && reply["type"] == "response" {
            assert_eq!(reply["success"], true, "{reply}");
            break;
        }
    }
    writeln!(
        input,
        "{}",
        json!({"id":"prompt", "type":"prompt", "message":"test worker restart"})
    )
    .unwrap();
    input.flush().unwrap();
    let mut saw_disabled = false;
    loop {
        let event = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
        saw_disabled |= event
            .to_string()
            .contains("automatic compaction is disabled");
        if event["type"] == "agent_settled"
            || (event["id"] == "prompt" && event["success"] == false)
        {
            assert!(
                saw_disabled,
                "replacement worker ignored disabled compaction: {event}"
            );
            break;
        }
    }
    writeln!(input, "{}", json!({"id":"stop", "type":"shutdown"})).unwrap();

    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    // A new session resolves the still-unchanged trusted project settings.
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args([
            "--trust",
            "--no-mcp",
            "--offline",
            "--model",
            "fixture",
            "--no-session",
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut fresh = output;
    fresh
        .stdin
        .take()
        .unwrap()
        .write_all(
            b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n",
        )
        .unwrap();
    let result = fresh.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let state = String::from_utf8(result.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["id"] == "state")
        .unwrap();
    assert_eq!(state["data"]["autoCompactionEnabled"], true);
}

#[test]
fn rpc_replacement_worker_reads_external_compaction_edit_with_retry_override() {
    rpc_replacement_compaction_edit_case(true);
}

#[test]
fn rpc_replacement_worker_reads_external_edit_without_live_overrides() {
    rpc_replacement_compaction_edit_case(false);
}

fn rpc_replacement_compaction_edit_case(live_retry: bool) {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(root.path().join(".pi")).unwrap();
    std::fs::write(
        root.path().join(".pi/settings.json"),
        r#"{"compaction":{"enabled":true},"retry":{"enabled":true}}"#,
    )
    .unwrap();
    std::fs::write(
        home.join("models.json"),
        r#"{"providers":{"openai":{"models":[{"id":"second"}]}}}"#,
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("OPENAI_API_KEY", "test-key")
        .args([
            "--trust",
            "--no-mcp",
            "--offline",
            "--model",
            "fixture",
            "--no-session",
            "--context-window",
            "32",
            "--max-output-tokens",
            "128",
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    for command in [
        if live_retry {
            json!({"id":"startup","type":"set_auto_retry","enabled":false})
        } else {
            json!({"id":"startup","type":"get_commands"})
        },
        json!({"id":"switch","type":"set_model","provider":"openai","modelId":"second"}),
    ] {
        writeln!(input, "{command}").unwrap();
        input.flush().unwrap();
        loop {
            let event = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
            if event["id"] == command["id"] && event["type"] == "response" {
                assert_eq!(event["success"], true, "{event}");
                break;
            }
        }
        if command["id"] == "startup" {
            std::fs::write(
                root.path().join(".pi/settings.json"),
                r#"{"compaction":{"enabled":false},"retry":{"enabled":true}}"#,
            )
            .unwrap();
        }
    }
    writeln!(
        input,
        "{}",
        json!({"id":"prompt","type":"prompt","message":"test"})
    )
    .unwrap();
    input.flush().unwrap();
    let mut disabled = false;
    loop {
        let event = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
        disabled |= event
            .to_string()
            .contains("automatic compaction is disabled");
        if event["type"] == "agent_settled"
            || (event["id"] == "prompt" && event["success"] == false)
        {
            break;
        }
    }
    assert!(
        disabled,
        "replacement worker ignored externally disabled compaction"
    );
    writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn rpc_replacement_worker_keeps_retry_disabled() {
    rpc_replacement_retry_case(true);
}

#[test]
fn rpc_replacement_worker_reads_external_retry_edit_with_compaction_override() {
    rpc_replacement_retry_case(false);
}

fn rpc_replacement_retry_case(live_retry: bool) {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(root.path().join(".pi")).unwrap();
    std::fs::write(
        root.path().join(".pi/settings.json"),
        r#"{"retry":{"enabled":true,"maxRetries":1,"baseDelayMs":0}}"#,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    std::fs::write(
        home.join("models.json"),
        json!({"providers":{"openai":{"baseUrl":endpoint,"apiKey":"test-key","models":[{"id":"second"}]}}}).to_string(),
    )
    .unwrap();
    listener.set_nonblocking(true).unwrap();
    let (stop, stopping) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut count = 0;
        while stopping.try_recv().is_err() {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let mut reader = BufReader::new(socket.try_clone().unwrap());
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        continue;
                    }
                    if line.starts_with("POST ") {
                        count += 1;
                    }
                    let body = r#"{"error":{"message":"unavailable"}}"#;
                    let _ = write!(
                        socket,
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(error) => panic!("fixture: {error}"),
            }
        }
        count
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("OPENAI_API_KEY", "test-key")
        .args([
            "--trust",
            "--no-mcp",
            "--offline",
            "--model",
            "fixture",
            "--no-session",
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    for command in [
        if live_retry {
            json!({"id":"retry","type":"set_auto_retry","enabled":false})
        } else {
            json!({"id":"compaction","type":"set_auto_compaction","enabled":false})
        },
        json!({"id":"switch","type":"set_model","provider":"openai","modelId":"second"}),
    ] {
        writeln!(input, "{command}").unwrap();
        input.flush().unwrap();
        loop {
            let event = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
            if event["id"] == command["id"] && event["type"] == "response" {
                assert_eq!(event["success"], true, "{event}");
                break;
            }
        }
        if !live_retry && command["id"] == "compaction" {
            std::fs::write(
                root.path().join(".pi/settings.json"),
                r#"{"retry":{"enabled":false,"maxRetries":1,"baseDelayMs":0}}"#,
            )
            .unwrap();
        }
    }
    writeln!(
        input,
        "{}",
        json!({"id":"prompt","type":"prompt","message":"test"})
    )
    .unwrap();
    input.flush().unwrap();
    let mut retry_event = false;
    loop {
        let event = receiver.recv_timeout(Duration::from_secs(15)).unwrap();
        retry_event |= event.to_string().contains("auto_retry_start");
        if event["type"] == "agent_settled" {
            break;
        }
    }
    assert!(
        !retry_event,
        "replacement worker retried despite the live override"
    );
    writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
    drop(input);
    let result = child.wait_with_output().unwrap();
    reader.join().unwrap();
    stop.send(()).unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(server.join().unwrap(), 1);
}

#[test]
fn rpc_correlates_commands_and_rejects_unknown_commands() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .args(["--mode", "rpc", "--no-session"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for request in [
        r#"{"id":"state","type":"get_state"}"#,
        r#"{"id":"new","type":"new_session"}"#,
        r#"{"id":"unknown","type":"unsupported"}"#,
        r#"{"id":"stats","type":"get_session_stats"}"#,
        r#"{"id":"native","type":"get_state","includeNative":true}"#,
    ] {
        writeln!(stdin, "{request}").unwrap();
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let replies: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(replies.len(), 5);
    assert_eq!(replies[0]["id"], "state");
    assert_eq!(replies[0]["data"]["isStreaming"], false);
    assert!(replies[0]["data"].get("sessionFile").is_none());
    assert!(replies[0]["data"].get("sessionName").is_none());
    assert_eq!(replies[1]["success"], true);
    assert_eq!(replies[2]["id"], "unknown");
    assert_eq!(replies[2]["success"], false);
    assert_eq!(replies[3]["id"], "stats");
    assert!(replies[3]["data"].get("sessionFile").is_none());
    assert_eq!(replies[4]["id"], "native");
    assert!(replies[4]["data"]["sessionFile"].is_string());
}

#[test]
fn rpc_model_discovery_does_not_block_other_commands() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (accepted, ready) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        loop {
            let (mut socket, mut reader, first) =
                accept_proxy_request(listener.try_clone().unwrap());
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            if first.starts_with("GET /api/ps ") {
                socket
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                continue;
            }
            assert!(first.starts_with("GET /v1/models "), "{first}");
            accepted.send(()).unwrap();
            let _ = gate.recv_timeout(Duration::from_secs(30));
            let body = br#"{"data":[{"id":"fixture-model"}]}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            socket.write_all(body).unwrap();
            break;
        }
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .args([
            "--workspace",
            root.path().to_str().unwrap(),
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--model",
            "fixture-model",
            "--endpoint",
            &endpoint,
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (events, received) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if events
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    writeln!(
        input,
        "{}",
        json!({"type":"get_available_models","id":"models"})
    )
    .unwrap();
    ready.recv_timeout(Duration::from_secs(30)).unwrap();
    writeln!(input, "{}", json!({"type":"get_state","id":"state"})).unwrap();
    let event = received.recv_timeout(Duration::from_secs(10));
    drop(input);
    // Allow the child to observe EOF while discovery is still held open.
    std::thread::sleep(Duration::from_millis(150));
    let _ = release.send(());
    let event = event.expect("state request blocked behind model discovery");
    assert_eq!(event["id"], "state", "{event}");
    assert_eq!(event["success"], true);
    let models = received.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(models["id"], "models", "{models}");
    assert_eq!(models["success"], true, "{models}");
    assert!(
        models["data"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["id"] == "fixture-model")
    );
    let status = child.wait().unwrap();
    reader.join().unwrap();
    server.join().unwrap();
    assert!(status.success());
}

#[test]
fn rpc_workspace_switch_defers_thinking_levels_and_orders_session_commands() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let launch = root.path().join("launch");
    let target = root.path().join("target");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(launch.join(".pi")).unwrap();
    std::fs::create_dir_all(target.join(".pi")).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    std::fs::write(
        home.join("models.json"),
        json!({"providers":{"ollama":{"baseUrl":base,"models":[{"id":"alpha","reasoning":false},{"id":"dynamic","reasoning":true}]}}}).to_string(),
    )
    .unwrap();
    std::fs::write(
        launch.join(".pi/settings.json"),
        json!({"defaultProvider":"ollama","defaultModel":"alpha"}).to_string(),
    )
    .unwrap();
    std::fs::write(
        target.join(".pi/settings.json"),
        json!({"enabledModels":["ollama/dynamic"]}).to_string(),
    )
    .unwrap();
    let target_session = root.path().join("target.jsonl");
    Session::open(&target_session)
        .unwrap()
        .set_working_directory(&target)
        .unwrap();
    let (accepted, ready) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        assert!(first.starts_with("GET /v1/models "), "{first}");
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        accepted.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(10)).unwrap();
        let body = br#"{"data":[{"id":"dynamic"}]}"#;
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        socket.write_all(body).unwrap();
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", &home)
        .arg("--workspace")
        .arg(&launch)
        .arg("--session")
        .arg(root.path().join("launch.jsonl"))
        .args(["--mode", "rpc", "--offline", "--no-mcp", "--approve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(child.stdout.take().unwrap())
            .lines()
            .map_while(Result::ok)
        {
            if send
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
        child.wait_with_output().unwrap()
    });
    writeln!(
        input,
        "{}",
        json!({"id":"switch","type":"switch_session","sessionPath":target_session})
    )
    .unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    writeln!(input, "{}", json!({"id":"state","type":"get_state"})).unwrap();
    writeln!(
        input,
        "{}",
        json!({"id":"levels","type":"get_available_thinking_levels"})
    )
    .unwrap();
    assert!(receive.recv_timeout(Duration::from_millis(150)).is_err());
    writeln!(input, "{}", json!({"id":"new","type":"new_session"})).unwrap();
    writeln!(input, "{}", json!({"id":"state2","type":"get_state"})).unwrap();
    release.send(()).unwrap();
    let mut replies = Vec::new();
    while replies.len() < 5 {
        let event = receive.recv_timeout(Duration::from_secs(10)).unwrap();
        if ["switch", "state", "levels", "new", "state2"]
            .contains(&event["id"].as_str().unwrap_or_default())
        {
            replies.push(event);
        }
    }
    assert_eq!(replies[0]["id"], "switch");
    assert_eq!(replies[0]["success"], true);
    assert_eq!(replies[1]["id"], "state");
    assert_eq!(replies[1]["data"]["model"]["id"], "dynamic");
    assert_eq!(replies[2]["id"], "levels");
    assert!(replies[2]["data"]["levels"].as_array().unwrap().len() > 1);
    assert_eq!(replies[3]["id"], "new");
    assert_eq!(replies[3]["success"], true);
    assert_eq!(replies[4]["id"], "state2");
    assert_eq!(replies[4]["data"]["model"]["id"], "dynamic");
    writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
    drop(input);
    let output = reader.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.join().unwrap();
}

#[test]
fn rpc_abort_and_shutdown_cancel_pending_switch_and_queued_prompt() {
    for control in ["abort", "shutdown"] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let launch = root.path().join("launch");
        let target = root.path().join("target");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(launch.join(".pi")).unwrap();
        std::fs::create_dir_all(target.join(".pi")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        std::fs::write(home.join("models.json"), json!({"providers":{"ollama":{"baseUrl":base,"models":[{"id":"alpha","reasoning":false}]}}}).to_string()).unwrap();
        std::fs::write(
            launch.join(".pi/settings.json"),
            json!({"defaultProvider":"ollama","defaultModel":"alpha"}).to_string(),
        )
        .unwrap();
        std::fs::write(
            target.join(".pi/settings.json"),
            json!({"enabledModels":["ollama/dynamic"]}).to_string(),
        )
        .unwrap();
        let path = root.path().join("target.jsonl");
        Session::open(&path)
            .unwrap()
            .set_working_directory(&target)
            .unwrap();
        let (accepted, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, _) = accept_proxy_request(listener);
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            accepted.send(()).unwrap();
            let _ = gate.recv_timeout(Duration::from_secs(10));
            let body = br#"{"data":[{"id":"dynamic"}]}"#;
            let _ = write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(body);
        });
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", &home)
            .arg("--workspace")
            .arg(&launch)
            .arg("--session")
            .arg(root.path().join("launch.jsonl"))
            .args(["--mode", "rpc", "--offline", "--no-mcp", "--approve"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(child.stdout.take().unwrap())
                .lines()
                .map_while(Result::ok)
            {
                if send
                    .send(serde_json::from_str::<Value>(&line).unwrap())
                    .is_err()
                {
                    break;
                }
            }
            child.wait_with_output().unwrap()
        });
        writeln!(
            input,
            "{}",
            json!({"id":"switch","type":"switch_session","sessionPath":path})
        )
        .unwrap();
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        writeln!(
            input,
            "{}",
            json!({"id":"prompt","type":"prompt","message":"must not run"})
        )
        .unwrap();
        writeln!(input, "{}", json!({"id":"control","type":control})).unwrap();
        let mut replies = Vec::new();
        while replies.len() < 3 {
            let event = receive
                .recv_timeout(Duration::from_secs(3))
                .expect("control blocked by discovery");
            if event.get("id").is_some() {
                replies.push(event);
            }
        }
        assert_eq!(replies[0]["id"], "switch");
        assert_eq!(replies[0]["data"]["cancelled"], true);
        assert_eq!(replies[1]["id"], "prompt");
        assert_eq!(replies[1]["success"], false);
        assert_eq!(replies[2]["id"], "control");
        assert_eq!(replies[2]["success"], true);
        if control == "abort" {
            writeln!(input, "{}", json!({"id":"state","type":"get_state"})).unwrap();
            let state = receive.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(state["id"], "state");
            assert_eq!(state["data"]["model"]["id"], "alpha");
            writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
        }
        drop(input);
        let _ = release.send(());
        let output = reader.join().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        server.join().unwrap();
    }
}

#[test]
fn rpc_new_session_recreates_cli_and_workspace_defaults() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let launch = root.path().join("launch");
    let target = root.path().join("target");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("models.json"), json!({"providers":{"ollama":{"baseUrl":"https://example.invalid/v1", "models":[{"id":"alpha","reasoning":true},{"id":"beta","reasoning":true}]}}}).to_string()).unwrap();
    for (workspace, model, thinking) in [(&launch, "alpha", "low"), (&target, "beta", "high")] {
        std::fs::create_dir_all(workspace.join(".pi")).unwrap();
        std::fs::write(workspace.join(".pi/settings.json"), json!({"defaultProvider":"ollama","defaultModel":model,"defaultThinkingLevel":thinking,"enabledModels":[format!("ollama/{model}:{thinking}")]}).to_string()).unwrap();
    }
    let target_session = root.path().join("target.jsonl");
    let mut session = Session::open(&target_session).unwrap();
    session.set_working_directory(&target).unwrap();
    drop(session);
    for explicit in [true, false] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .env("PI_RS_HOME", &home)
            .arg("--workspace")
            .arg(&launch)
            .arg("--session")
            .arg(root.path().join(if explicit {
                "explicit.jsonl"
            } else {
                "workspace.jsonl"
            }))
            .args(["--mode", "rpc", "--offline", "--no-mcp", "--approve"]);
        if explicit {
            command.args(["--model", "alpha", "--thinking", "low"]);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut requests = Vec::new();
        if !explicit {
            requests
                .push(json!({"id":"switch","type":"switch_session","sessionPath":target_session}));
        }
        requests.push(json!({"id":"model","type":"set_model","provider":"ollama","modelId":if explicit { "beta" } else { "alpha" }}));
        requests.push(json!({"id":"thinking","type":"set_thinking_level","level":if explicit { "high" } else { "low" }}));
        requests.push(json!({"id":"new","type":"new_session"}));
        requests.push(json!({"id":"state","type":"get_state"}));
        child
            .stdin
            .take()
            .unwrap()
            .write_all(
                requests
                    .iter()
                    .map(|request| format!("{request}\n"))
                    .collect::<String>()
                    .as_bytes(),
            )
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let responses: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for id in if explicit {
            vec!["model", "thinking", "new"]
        } else {
            vec!["switch", "model", "thinking", "new"]
        } {
            let response = responses.iter().find(|value| value["id"] == id).unwrap();
            assert_eq!(response["success"], true, "{response}");
        }
        let state = responses
            .iter()
            .find(|value| value["id"] == "state")
            .unwrap();
        assert_eq!(
            state["data"]["model"]["id"],
            if explicit { "alpha" } else { "beta" },
            "{state}"
        );
        assert_eq!(
            state["data"]["thinkingLevel"],
            if explicit { "low" } else { "high" },
            "{state}"
        );
    }
}

#[test]
fn scoped_thinking_applies_to_new_sessions_and_resumes_restore_branch_thinking() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("settings.json"),
        json!({
            "defaultProvider":"ollama", "defaultModel":"alpha",
            "defaultThinkingLevel":"medium",
            "enabledModels":["ollama/alpha:high", "ollama/beta:high"]
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        home.join("models.json"),
        json!({"providers":{"ollama":{
            "baseUrl":"https://example.invalid/v1", "models":[
                {"id":"alpha", "reasoning":true}, {"id":"beta", "reasoning":true},
                {"id":"gamma", "reasoning":true}
            ]
        }}})
        .to_string(),
    )
    .unwrap();
    for (index, arguments, saved_model, has_message, expected_model, expected_thinking) in [
        (0, vec![], "alpha", false, "alpha", "high"),
        (
            1,
            vec!["--model", "alpha"],
            "alpha",
            false,
            "alpha",
            "medium",
        ),
        (2, vec!["--model", "beta"], "alpha", false, "beta", "medium"),
        (4, vec![], "gamma", false, "alpha", "high"),
        (
            3,
            vec!["--model", "beta", "--thinking", "minimal"],
            "alpha",
            false,
            "beta",
            "minimal",
        ),
        (5, vec![], "alpha", true, "alpha", "low"),
        (6, vec!["--model", "alpha"], "alpha", true, "alpha", "low"),
        (7, vec!["--model", "beta"], "alpha", true, "beta", "low"),
        (
            8,
            vec!["--model", "beta", "--thinking", "minimal"],
            "alpha",
            true,
            "beta",
            "minimal",
        ),
        (9, vec![], "gamma", true, "gamma", "low"),
        (
            10,
            vec!["--provider", "ollama", "--model", "beta"],
            "alpha",
            true,
            "beta",
            "low",
        ),
    ] {
        let path = directory.path().join(format!("session-{index}.jsonl"));
        let mut session = Session::open(&path).unwrap();
        session
            .select_provider_model(None, saved_model, Some("low"))
            .unwrap();
        if has_message {
            session
                .append_user_with_images("existing conversation".into(), Vec::new(), None)
                .unwrap();
        }
        drop(session);
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", &home)
            .args(["--session"])
            .arg(&path)
            .args(["--workspace"])
            .arg(directory.path())
            .args(["--mode", "rpc", "--offline", "--no-mcp", "--no-trust"])
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let state: Value =
            serde_json::from_slice(output.stdout.split(|byte| *byte == b'\n').next().unwrap())
                .unwrap();
        assert_eq!(state["data"]["model"]["id"], expected_model, "{state}");
        assert_eq!(state["data"]["thinkingLevel"], expected_thinking, "{state}");
    }
}

#[test]
fn rpc_worker_preserves_imported_selection_and_cli_provenance() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("settings.json"),
        json!({"defaultProvider":"ollama","defaultModel":"physical"}).to_string(),
    )
    .unwrap();
    std::fs::write(home.join("models.json"), json!({"providers":{"ollama":{"baseUrl":"https://example.invalid/v1","models":[{"id":"physical","reasoning":true}]}}}).to_string()).unwrap();
    let source = root.path().join("pi.jsonl");
    let entries = [
        json!({"type":"session","version":3}),
        json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
        json!({"type":"message","id":"user","parentId":"change","message":{"role":"user","content":"question"}}),
        json!({"type":"message","id":"assistant","parentId":"user","message":{"role":"assistant","provider":"ollama","model":"physical","content":[]}}),
    ];
    std::fs::write(
        &source,
        entries
            .iter()
            .map(|entry| format!("{entry}\n"))
            .collect::<String>(),
    )
    .unwrap();
    for (label, flags, expected) in [
        ("implicit", vec![], vec!["virtual"]),
        ("thinking", vec!["--thinking", "high"], vec!["virtual"]),
        (
            "model",
            vec!["--model", "physical"],
            vec!["virtual", "physical"],
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        std::fs::write(home.join("models.json"), json!({"providers":{"ollama":{"baseUrl":base,"models":[{"id":"physical","reasoning":true}]}}}).to_string()).unwrap();
        let server = std::thread::spawn(move || {
            loop {
                let (mut socket, mut reader, first) =
                    accept_proxy_request(listener.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                if !first.starts_with("POST /v1/chat/completions ") {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    continue;
                }
                let events = concat!(
                    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                );
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
                break;
            }
        });
        let native = root.path().join(format!("{label}.jsonl"));
        let imported = Command::new(env!("CARGO_BIN_EXE_pi-rs"))
            .env("PI_RS_HOME", &home)
            .arg("--session")
            .arg(&native)
            .arg("--workspace")
            .arg(root.path())
            .args(["--offline", "--no-mcp", "--no-trust", "import"])
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            imported.status.success(),
            "{}",
            String::from_utf8_lossy(&imported.stderr)
        );
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", &home)
            .arg("--session")
            .arg(&native)
            .arg("--workspace")
            .arg(root.path())
            .args(["--mode", "rpc", "--offline", "--no-mcp", "--no-trust"])
            .args(flags)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(child.stdout.take().unwrap())
                .lines()
                .map_while(Result::ok)
            {
                if send
                    .send(serde_json::from_str::<Value>(&line).unwrap())
                    .is_err()
                {
                    break;
                }
            }
            child.wait_with_output().unwrap()
        });
        writeln!(
            input,
            "{}",
            json!({"id":"prompt","type":"prompt","message":"continue"})
        )
        .unwrap();
        loop {
            let event = receive.recv_timeout(Duration::from_secs(15)).unwrap();
            if event["type"] == "agent_settled" {
                assert_eq!(event["aborted"], false, "{event}");
                break;
            }
        }
        writeln!(input, "{}", json!({"id":"entries","type":"get_entries"})).unwrap();
        let projected = loop {
            let event = receive.recv_timeout(Duration::from_secs(15)).unwrap();
            if event["type"] == "response" && event["id"] == "entries" {
                break event;
            }
        };
        let changes: Vec<_> = projected["data"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["type"] == "model_change")
            .filter_map(|entry| entry["modelId"].as_str())
            .collect();
        assert_eq!(changes, expected, "{label}: {projected}");
        if label == "thinking" {
            assert!(
                projected["data"]["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["type"] == "thinking_level_change"
                        && entry["thinkingLevel"] == "high"),
                "{projected}"
            );
        }
        writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
        drop(input);
        let output = reader.join().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        server.join().unwrap();
    }
}

#[test]
fn omitted_user_with_empty_failed_assistant_is_a_resumed_session() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("settings.json"), json!({"defaultProvider":"ollama", "defaultModel":"alpha", "enabledModels":["ollama/alpha:high"]}).to_string()).unwrap();
    std::fs::write(home.join("models.json"), json!({"providers":{"ollama":{"baseUrl":"https://example.invalid/v1", "models":[{"id":"alpha", "reasoning":true}]}}}).to_string()).unwrap();
    for (index, reason, omit_assistant, expected) in [
        (0, "error", false, "low"),
        (1, "aborted", false, "low"),
        (2, "error", true, "high"),
    ] {
        let source = directory.path().join(format!("source-{index}.jsonl"));
        let native = directory.path().join(format!("native-{index}.jsonl"));
        let mut entries = vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"m","parentId":null,"provider":"ollama","modelId":"alpha"}),
            json!({"type":"thinking_level_change","id":"t","parentId":"m","thinkingLevel":"low"}),
            json!({"type":"message","id":"u","parentId":"t","message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","provider":"ollama","model":"alpha","stopReason":reason,"content":[]}}),
            json!({"type":"context_edit","id":"eu","parentId":"a","targetId":"u","replacement":null}),
        ];
        if omit_assistant {
            entries.push(json!({"type":"context_edit","id":"ea","parentId":"eu","targetId":"a","replacement":null}));
        }
        std::fs::write(
            &source,
            entries
                .iter()
                .map(|entry| format!("{entry}\n"))
                .collect::<String>(),
        )
        .unwrap();
        let imported = Command::new(env!("CARGO_BIN_EXE_pi-rs"))
            .env("PI_RS_HOME", &home)
            .arg("--session")
            .arg(&native)
            .arg("--workspace")
            .arg(directory.path())
            .args(["--offline", "--no-mcp", "--no-trust", "import"])
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            imported.status.success(),
            "{}",
            String::from_utf8_lossy(&imported.stderr)
        );
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", &home)
            .arg("--session")
            .arg(&native)
            .arg("--workspace")
            .arg(directory.path())
            .args(["--mode", "rpc", "--offline", "--no-mcp", "--no-trust"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let state: Value =
            serde_json::from_slice(output.stdout.split(|byte| *byte == b'\n').next().unwrap())
                .unwrap();
        assert_eq!(state["data"]["thinkingLevel"], expected, "{state}");
    }
}

#[test]
fn invalid_thinking_levels_warn_and_keep_the_last_valid_selection() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("settings.json"),
        r#"{"defaultThinkingLevel":"medium"}"#,
    )
    .unwrap();
    for (arguments, expected, warnings) in [
        (vec!["--thinking", "invalid"], "medium", 1),
        (vec!["--thinking", "--invalid"], "medium", 1),
        (vec!["--thinking", "low", "--thinking", "invalid"], "low", 1),
        (
            vec!["--thinking", "invalid", "--thinking", "high"],
            "high",
            1,
        ),
        (vec!["--thinking", "low", "--thinking", "high"], "high", 0),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .env("PI_RS_HOME", directory.path())
            .args([
                "--mode",
                "rpc",
                "--no-session",
                "--no-mcp",
                "--offline",
                "--model",
                "test-thinking-fixture",
            ])
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        child.stdin.take().unwrap().write_all(
            b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n",
        ).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let response: Value =
            serde_json::from_slice(output.stdout.split(|byte| *byte == b'\n').next().unwrap())
                .unwrap();
        assert_eq!(response["data"]["thinkingLevel"], expected);
        assert_eq!(
            String::from_utf8_lossy(&output.stderr)
                .matches("Warning: Invalid thinking level")
                .count(),
            warnings
        );
    }
}

#[test]
fn native_notes_extension_is_saved_and_restored() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.jsonl");
    for (command, text) in [("set", "Review parser"), ("append", "Test Unicode")] {
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .arg("--session")
            .arg(&path)
            .args(["--extension", "notes", "extension", "notes", command, text])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let session = Session::open(path).unwrap();
    assert_eq!(
        session.extension_states()["notes"].data["text"],
        "Review parser\nTest Unicode"
    );
}

#[test]
fn print_and_json_modes_accept_positional_prompts() {
    let (endpoint, server) = model_fixture();
    for mode in ["--print", "--mode"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command.args(["--no-session", "--endpoint", &endpoint, mode]);
        if mode == "--mode" {
            command.arg("json");
        }
        let output = command.arg("same input").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        if mode == "--print" {
            assert_eq!(stdout, "fixture answer\n");
        } else {
            let records: Vec<Value> = stdout
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(records[0]["type"], "session");
            assert_eq!(records[0]["version"], 3);
            let updates: Vec<_> = records
                .iter()
                .filter_map(|event| event.get("assistantMessageEvent"))
                .filter(|update| {
                    matches!(
                        update["type"].as_str(),
                        Some("thinking_delta" | "text_delta")
                    )
                })
                .collect();
            assert_eq!(updates.len(), 2);
            assert_eq!(updates[0]["type"], "thinking_delta");
            assert_eq!(updates[1]["type"], "text_delta");
            assert!(
                records
                    .iter()
                    .any(|event| event["type"] == "agent_end" && event["success"] == true)
            );
            assert!(!records.iter().any(|record| record["type"] == "answer"));
        }
    }
    server.join().unwrap();
}

#[test]
fn json_print_mode_emits_only_agent_events_for_multiple_prompts() {
    let root = tempfile::tempdir().unwrap();
    let (endpoint, server) = model_fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--endpoint",
            &endpoint,
            "--mode",
            "json",
            "same input",
            "second",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events[0]["type"], "session");
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "agent_end")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "agent_settled")
            .count(),
        2
    );
    assert!(!events.iter().any(|event| event["type"] == "answer"));
    server.join().unwrap();
}

#[test]
fn continue_reopens_the_latest_saved_session() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path())
        .args([
            "--extension",
            "notes",
            "extension",
            "notes",
            "set",
            "remember this",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path())
        .args(["--continue", "--trace", "inspect"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("remember this")
    );
}

fn model_fixture() -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_effort(None)
}

fn model_fixture_with_effort(
    effort: Option<&'static str>,
) -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_finish(effort, "stop")
}

fn model_fixture_with_finish(
    effort: Option<&'static str>,
    finish: &'static str,
) -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_config(effort, finish, None)
}

fn model_fixture_with_config(
    effort: Option<&'static str>,
    finish: &'static str,
    expected_token: Option<&'static str>,
) -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_gate(effort, finish, expected_token, None)
}

fn model_fixture_with_gate(
    effort: Option<&'static str>,
    finish: &'static str,
    expected_token: Option<&'static str>,
    mut release_first: Option<std::sync::mpsc::Receiver<()>>,
) -> (String, std::thread::JoinHandle<()>) {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        server.local_addr().unwrap()
    );
    server.set_nonblocking(true).unwrap();
    let handle = std::thread::spawn(move || {
        let mut completed = 0;
        let deadline = Instant::now() + Duration::from_secs(180);
        while completed < 2 {
            let (mut socket, _) = match server.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "received {completed} of 2 model requests"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("model fixture accept failed: {error}"),
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&socket);
            let mut content_length = 0;
            let mut authorization = None;
            let mut metadata_request = false;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line.is_empty() || line == "\r\n" {
                    break;
                }
                metadata_request |= line.starts_with("GET /api/ps ");
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
                if line.to_ascii_lowercase().starts_with("authorization:") {
                    authorization = Some(line.split_once(':').unwrap().1.trim().to_owned());
                }
            }
            if let Some(token) = expected_token {
                assert_eq!(authorization, Some(format!("Bearer {token}")));
            }
            if metadata_request {
                write!(
                    socket,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                continue;
            }
            completed += 1;
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            if let Some(effort) = effort {
                assert_eq!(request["reasoning_effort"], effort);
            }
            assert!(
                request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|message| message["content"] == "same input")
            );
            let response = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"private-reasoning-sentinel\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"fixture-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"fixture-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":19,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":7}}}\n\n",
                "data: [DONE]\n\n",
            ).replace("\"finish_reason\":\"stop\"", &format!("\"finish_reason\":\"{finish}\""));
            if let Some(release) = release_first.take() {
                release
                    .recv_timeout(Duration::from_secs(15))
                    .expect("release first response");
            }
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{response}", response.len()).unwrap();
        }
    });
    (endpoint, handle)
}

#[test]
fn token_limit_preserves_partial_text_and_its_finish_reason() {
    let (endpoint, server) = model_fixture_with_finish(None, "length");
    let directory = tempfile::tempdir().unwrap();
    for index in 0..2 {
        let path = directory.path().join(format!("{index}.jsonl"));
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", directory.path().join("home"))
            .args([
                "--endpoint",
                &endpoint,
                "--print",
                "--no-tools",
                "--session",
            ])
            .arg(&path)
            .arg("same input")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            "Response was truncated: the model reached its output token limit.\n"
        );
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "fixture answer\n"
        );
        let session = Session::open(path).unwrap();
        assert!(session.records().iter().any(|record| matches!(&record.event, pi_rs::Event::ModelFinished { metadata, .. } if metadata.finish_reason.as_deref() == Some("length"))));
    }
    server.join().unwrap();
}

#[test]
fn thinking_override_reaches_provider_and_restores_from_session() {
    let (endpoint, server) = model_fixture_with_effort(Some("none"));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.jsonl");
    for first in [true, false] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .env("PI_RS_HOME", directory.path().join("home"))
            .arg("--session")
            .arg(&path)
            .args(["--endpoint", &endpoint]);
        if first {
            command.args(["--thinking", "off"]);
        }
        let output = command.args(["run", "same input"]).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        Session::open(path)
            .unwrap()
            .model_selection()
            .unwrap()
            .1
            .as_deref(),
        Some("off")
    );
    server.join().unwrap();
}

#[tokio::test]
async fn cli_and_library_use_the_same_stream_and_do_not_emit_auth() {
    let (endpoint, server) = model_fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    let output = Command::new(env!("CARGO_BIN_EXE_pi-rs"))
        .arg("--session")
        .arg(&path)
        .arg("--workspace")
        .arg(dir.path())
        .args(["--endpoint", &endpoint, "run", "same input"])
        .env("PI_RS_MODEL_TOKEN", "private-test-token")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("private-test-token"));
    assert!(!stdout.contains("private-reasoning-sentinel"));
    assert!(
        !std::fs::read_to_string(path)
            .unwrap()
            .contains("private-test-token")
    );
    let events: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let stages: Vec<&str> = events
        .iter()
        .filter(|event| event["type"] == "model_progress")
        .filter_map(|event| event["stage"].as_str())
        .collect();
    assert_eq!(stages, ["waiting_for_model", "thinking", "generating"]);
    let answer = events
        .iter()
        .find(|event| event["type"] == "answer")
        .unwrap()["content"]
        .as_str()
        .unwrap();
    let metadata = events
        .iter()
        .find(|event| event["event"] == "model_finished")
        .unwrap();
    assert_eq!(metadata["metadata"]["cached_input_tokens"], 7);
    assert_eq!(metadata["usage"]["kind"], "measured");

    let model = Ollama::new(&endpoint, None).unwrap();
    let registry = Registry::default();
    let agent = Agent {
        model: &model,
        model_name: "qwen2.5:7b",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    let result = agent
        .run(
            &mut session,
            Some("same input".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: Notification<'_>| {},
        )
        .await
        .unwrap();
    assert_eq!(answer, result);
    server.join().unwrap();
}

#[test]
fn plan_mode_changes_are_branch_local_and_can_be_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    let run = |arguments: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi-rs"))
            .arg("--session")
            .arg(&path)
            .arg("--workspace")
            .arg(dir.path())
            .args(arguments)
            .output()
            .unwrap()
    };
    assert!(
        run(&["extension", "plan-mode", "start", "review before editing"])
            .status
            .success()
    );
    let active = Session::open(&path).unwrap().head();
    assert!(run(&["extension", "plan-mode", "accept"]).status.success());
    assert_eq!(
        Session::open(&path).unwrap().extension_states()["plan-mode"].data["mode"],
        "executing"
    );
    assert!(run(&["checkout", &active.to_string()]).status.success());
    assert_eq!(
        Session::open(&path).unwrap().extension_states()["plan-mode"].data["mode"],
        "planning"
    );
    assert!(run(&["extension", "plan-mode", "discard"]).status.success());
    assert_eq!(
        Session::open(&path).unwrap().extension_states()["plan-mode"].data["mode"],
        "normal"
    );
}

#[test]
fn persistent_worker_accepts_multiple_prompts_and_releases_the_journal() {
    let (endpoint, server) = model_fixture();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("persistent.jsonl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .arg("--session")
        .arg(&path)
        .args(["--endpoint", &endpoint, "--trace", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if let Ok(value) = serde_json::from_str::<Value>(&line) {
                let _ = sender.send(value);
            }
        }
    });
    for _ in 0..2 {
        writeln!(
            input,
            "{}",
            serde_json::json!({"type":"prompt","content":"same input"})
        )
        .unwrap();
        input.flush().unwrap();
        let mut events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let event = receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|error| {
                    panic!("worker did not settle: {error}; events: {events:?}")
                });
            let settled = event["type"] == "agent_settled";
            events.push(event);
            if settled {
                assert_eq!(events.last().unwrap()["success"], true, "{events:?}");
                break;
            }
        }
        assert!(
            events.iter().any(|event| event["type"] == "answer"),
            "{events:?}"
        );
        assert!(child.try_wait().unwrap().is_none());
        drop(Session::open(&path).unwrap());
    }
    writeln!(input, "{}", serde_json::json!({"type":"shutdown"})).unwrap();
    drop(input);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("persistent worker did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(child.wait().unwrap().success());
    reader.join().unwrap();
    server.join().unwrap();
}

#[test]
fn api_key_override_reaches_direct_and_rpc_requests_without_persistence() {
    let token = "override-test-secret";
    let (endpoint, server) = model_fixture_with_config(None, "stop", Some(token));
    let directory = tempfile::tempdir().unwrap();
    let session = directory.path().join("direct.jsonl");
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .env("PI_RS_MODEL_TOKEN", "unused-environment-token")
        .arg("--session")
        .arg(&session)
        .args([
            "--endpoint",
            &endpoint,
            "--api-key",
            token,
            "--print",
            "same input",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    assert!(!std::fs::read_to_string(session).unwrap().contains(token));
    let mut rpc = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .env("PI_RS_MODEL_TOKEN", "unused-environment-token")
        .args([
            "--no-session",
            "--endpoint",
            &endpoint,
            "--api-key",
            token,
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = rpc.stdin.take().unwrap();
    writeln!(input, "{{\"type\":\"prompt\",\"message\":\"same input\"}}").unwrap();
    input.flush().unwrap();
    let stdout = rpc.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            assert!(!line.contains(token));
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut events = Vec::new();
    loop {
        let event = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = rpc.kill();
                let _ = rpc.wait();
                panic!("RPC did not settle: {error}; events: {events:?}");
            });
        let settled = event["type"] == "agent_settled";
        events.push(event);
        if settled {
            let end = events
                .iter()
                .find(|event| event["type"] == "agent_end")
                .expect("agent end precedes settlement");
            assert_eq!(end["success"], true, "{events:?}");
            assert_eq!(end["messages"][0]["role"], "user");
            assert_eq!(end["messages"][1]["role"], "assistant");
            let update = events
                .iter()
                .find(|event| event["assistantMessageEvent"]["type"] == "text_delta")
                .expect("streamed text update");
            assert_eq!(update["assistantMessageEvent"]["contentIndex"], 1);
            assert_eq!(update["message"]["content"][0]["type"], "thinking");
            assert_eq!(update["message"]["content"][1]["text"], "fixture answer");
            assert_eq!(
                update["message"],
                update["assistantMessageEvent"]["partial"]
            );
            let entries: Vec<_> = events
                .iter()
                .filter(|event| event["type"] == "entry_appended")
                .map(|event| &event["entry"])
                .collect();
            let user = entries
                .iter()
                .position(|entry| entry["message"]["role"] == "user")
                .expect("user entry appended");
            let assistant = entries
                .iter()
                .position(|entry| entry["message"]["role"] == "assistant")
                .expect("assistant entry appended");
            assert!(assistant > user);
            for pair in entries[user..=assistant].windows(2) {
                assert_eq!(pair[1]["parentId"], pair[0]["id"]);
            }
            assert_eq!(events.last().unwrap()["aborted"], false);
            break;
        }
    }
    writeln!(
        input,
        "{{\"type\":\"get_messages\",\"id\":\"messages\",\"includeNative\":true}}"
    )
    .unwrap();
    input.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let messages = loop {
        let event = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = rpc.kill();
                let _ = rpc.wait();
                panic!("RPC transcript did not arrive: {error}");
            });
        if event["type"] == "response" && event["id"] == "messages" {
            break event["data"].clone();
        }
    };
    assert_eq!(
        messages["messages"][1]["content"][1]["text"],
        "fixture answer"
    );
    assert_eq!(messages["messages"][1]["usage"]["cacheRead"], 7);
    assert_eq!(messages["nativeMessages"][1]["content"], "fixture answer");
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    input.flush().unwrap();
    drop(input);
    let output = rpc.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    server.join().unwrap();
}

#[test]
fn rpc_controls_apply_while_idle_and_streaming() {
    let root = tempfile::tempdir().unwrap();
    let journal = root.path().join("session.jsonl");
    let (release, gate) = std::sync::mpsc::channel();
    let (endpoint, fixture) = model_fixture_with_gate(None, "stop", None, Some(gate));
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path().join("home"))
        .args(["--workspace"])
        .arg(root.path())
        .args(["--session"])
        .arg(&journal)
        .args([
            "--no-trust",
            "--no-mcp",
            "--endpoint",
            &endpoint,
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (events, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if events
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let mut send = |request: Value| {
        writeln!(input, "{request}").unwrap();
        input.flush().unwrap();
    };
    let mut until = |matches: &dyn Fn(&Value) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let event = receive
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|error| {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("RPC event timed out: {error}");
                });
            if matches(&event) {
                break event;
            }
        }
    };
    send(serde_json::json!({"type":"steer","id":"idle-steer","message":"queued while idle"}));
    let updated = until(&|event| event["type"] == "queue_update" || event["id"] == "idle-steer");
    assert_eq!(
        updated["steering"],
        serde_json::json!(["queued while idle"])
    );
    let queued = until(&|event| event["type"] == "response" && event["id"] == "idle-steer");
    assert_eq!(queued["data"]["disposition"], "queued");
    send(serde_json::json!({"type":"get_state","id":"idle-state"}));
    let state = until(&|event| event["id"] == "idle-state");
    assert_eq!(state["data"]["pendingMessageCount"], 1);
    assert_eq!(state["data"]["isStreaming"], false);
    send(serde_json::json!({"type":"clear_queue","id":"idle-clear"}));
    let cleared = until(&|event| event["id"] == "idle-clear");
    assert_eq!(
        cleared["data"]["steering"],
        serde_json::json!(["queued while idle"])
    );
    send(serde_json::json!({"type":"prompt","message":"same input"}));
    until(&|event| event["record"]["event"]["type"] == "model_requested");
    send(serde_json::json!({"type":"set_session_name","id":"rename","name":"  Active name  "}));
    let renamed = until(&|event| event["type"] == "session_info_changed");
    assert_eq!(renamed["name"], "Active name");
    assert_eq!(until(&|event| event["id"] == "rename")["success"], true);
    send(serde_json::json!({"type":"get_state","id":"renamed-state"}));
    let renamed_state = until(&|event| event["id"] == "renamed-state");
    assert_eq!(renamed_state["data"]["sessionName"], "Active name");
    assert_eq!(renamed_state["data"]["isStreaming"], true);
    send(serde_json::json!({"type":"bash","id":"shell","command":"echo parallel-shell"}));
    let shell = until(&|event| event["type"] == "response" && event["id"] == "shell");
    // Windows prevents reading a journal locked by the running agent.
    send(serde_json::json!({"type":"get_entries","id":"before-settlement","includeNative":true}));
    let before_settlement =
        until(&|event| event["type"] == "response" && event["id"] == "before-settlement");
    release.send(()).unwrap();
    until(&|event| event["type"] == "agent_settled");
    let after_settlement = std::fs::read_to_string(&journal).unwrap();
    assert_eq!(
        Session::snapshot(&journal).unwrap().name(),
        Some("Active name")
    );
    send(serde_json::json!({"type":"prompt","message":"same input"}));
    until(&|event| event["type"] == "agent_settled");
    send(serde_json::json!({"type":"shutdown"}));
    drop(input);
    let exit = child.wait_with_output().unwrap();
    reader.join().unwrap();
    fixture.join().unwrap();
    assert!(
        exit.status.success(),
        "{}",
        String::from_utf8_lossy(&exit.stderr)
    );
    assert_eq!(shell["success"], true, "{shell:?}");
    assert_eq!(
        shell["data"]["output"].as_str().unwrap().trim(),
        "parallel-shell"
    );
    assert_eq!(before_settlement["success"], true);
    assert!(
        !before_settlement["data"]["nativeEntries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["event"]["type"] == "bash_execution")
    );
    assert!(after_settlement.contains("parallel-shell"));
}

#[test]
fn export_flag_replaces_html_and_preserves_source() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("session.jsonl");
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    session
        .append_user_with_images(
            "export <marker>\n\n## Export heading".into(),
            Vec::new(),
            None,
        )
        .unwrap();
    session.export_jsonl(&input).unwrap();
    let before = std::fs::read(&input).unwrap();
    let export = |destination: Option<&std::path::Path>| {
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", directory.path().join("home"))
            .current_dir(directory.path())
            .arg("--export")
            .arg(&input)
            .args(destination)
            .output()
            .unwrap()
    };
    let output = export(None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Exported to: pi-session-session.html\n"
    );
    let document =
        std::fs::read_to_string(directory.path().join("pi-session-session.html")).unwrap();
    assert!(document.contains("export &lt;marker&gt;"));
    assert!(document.contains("<h2>Export heading</h2>"));
    let destination = directory.path().join("transcript.jsonl");
    std::fs::write(&destination, "old").unwrap();
    assert!(export(Some(&destination)).status.success());
    assert!(
        std::fs::read_to_string(destination)
            .unwrap()
            .starts_with("<!doctype html>")
    );
    assert!(!export(Some(&input)).status.success());
    assert_eq!(std::fs::read(input).unwrap(), before);
}

#[test]
fn http_idle_timeout_resets_on_data_and_can_be_disabled() {
    for (setting, intervals, header_delay, succeeds) in [
        (json!(500), vec![150, 150, 150, 150, 150, 150], 0, true),
        (json!(500), vec![900], 0, false),
        (json!(500), vec![], 900, false),
        (json!("disabled"), vec![900], 0, true),
    ] {
        let scenario = format!("{setting} {intervals:?} header={header_delay}");
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("settings.json"),
            json!({"httpIdleTimeoutMs":setting,"retry":{"enabled":false}}).to_string(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let mut socket = loop {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut size = 0;
                let mut method = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if method.is_empty() {
                        method = line.clone();
                    }
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        size = value.trim().parse().unwrap();
                    }
                }
                if method.starts_with("GET ") {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    continue;
                }
                let mut request = vec![0; size];
                reader.read_exact(&mut request).unwrap();
                break socket;
            };
            std::thread::sleep(Duration::from_millis(header_delay));
            if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").is_err() { return; }
            let send = |socket: &mut std::net::TcpStream, text: &str, finish: Option<&str>| {
                let event = json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":finish}]});
                write!(socket, "data: {event}\n\n").and_then(|()| socket.flush())
            };
            if send(&mut socket, "a", None).is_err() {
                return;
            }
            for interval in intervals {
                std::thread::sleep(Duration::from_millis(interval));
                if send(&mut socket, "b", None).is_err() {
                    return;
                }
            }
            if send(&mut socket, "", Some("stop")).is_ok() {
                let _ = socket.write_all(b"data: [DONE]\n\n");
            }
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--offline",
                "--model",
                "test-fixture",
                "--endpoint",
                &endpoint,
                "--model-timeout-secs",
                "4",
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            succeeds,
            "{scenario}: stdout: {} stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if succeeds {
            assert!(String::from_utf8_lossy(&output.stdout).starts_with("ab"));
        } else {
            assert!(String::from_utf8_lossy(&output.stderr).contains("HTTP idle timeout"));
        }
        server.join().unwrap();
    }
}

#[test]
fn http_idle_timeout_applies_to_native_provider_streams() {
    for (provider, endpoint_path) in [
        ("anthropic", "/v1/messages"),
        ("google", "/v1beta"),
        ("radius", "/v1/messages"),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("settings.json"),
            json!({"httpIdleTimeoutMs":300,"retry":{"enabled":false}}).to_string(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}{}", listener.local_addr().unwrap(), endpoint_path);
        let server = std::thread::spawn(move || {
            loop {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut size = 0;
                let mut method = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if method.is_empty() {
                        method = line.clone();
                    }
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        size = value.trim().parse().unwrap();
                    }
                }
                if method.starts_with("GET ") {
                    let _ = socket.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    continue;
                }
                let mut request = vec![0; size];
                reader.read_exact(&mut request).unwrap();
                let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: heartbeat\n\n");
                let _ = socket.flush();
                std::thread::sleep(Duration::from_millis(650));
                break;
            }
        });
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--offline",
                "--provider",
                provider,
                "--model",
                "fixture-model",
                "--api-key",
                "fixture",
                "--endpoint",
                &endpoint,
                "--model-timeout-secs",
                "4",
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "{provider}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("HTTP idle timeout"),
            "{provider}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        server.join().unwrap();
    }
}

#[test]
fn model_requests_use_configured_http_proxy_and_environment_overrides() {
    for (provider, path, from_env) in [
        ("ollama", "/v1/chat/completions", false),
        ("anthropic", "/v1/messages", false),
        ("google", "/v1beta", true),
        ("radius", "/v1/messages", true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let proxy = format!("http://user:pass@{}", listener.local_addr().unwrap());
        let setting = if from_env {
            "http://127.0.0.1:1"
        } else {
            &proxy
        };
        std::fs::write(
            root.path().join("settings.json"),
            json!({"httpProxy":setting,"retry":{"enabled":false}}).to_string(),
        )
        .unwrap();
        let (server_tx, server_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                assert!(
                    Instant::now() < deadline,
                    "proxy did not receive a model request"
                );
                let (mut socket, mut reader, first) =
                    accept_proxy_request(listener.try_clone().unwrap());
                let mut headers = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    headers.push_str(&line.to_ascii_lowercase());
                }
                if first.starts_with("GET ") {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    continue;
                }
                socket.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                let _ = server_tx.send((first, headers));
                return;
            }
        });
        let target = format!("http://127.0.0.1:1{path}");
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .env("PI_RS_HOME", root.path())
            .env_remove("http_proxy")
            .env_remove("HTTP_PROXY")
            .env_remove("https_proxy")
            .env_remove("HTTPS_PROXY")
            .env_remove("no_proxy")
            .env_remove("NO_PROXY")
            .args([
                "--no-session",
                "--no-mcp",
                "--offline",
                "--provider",
                provider,
                "--model",
                "fixture-model",
                "--api-key",
                "fixture",
                "--endpoint",
                &target,
                "--model-timeout-secs",
                "20",
                "--print",
                "hello",
            ]);
        if from_env {
            command.env("http_proxy", &proxy);
        }
        let stderr = tempfile::NamedTempFile::new().unwrap();
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr.reopen().unwrap()))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(45);
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        if child.try_wait().unwrap().is_none() {
            let _ = child.kill();
            panic!(
                "{provider}: CLI timed out; stderr: {}",
                String::from_utf8_lossy(&std::fs::read(stderr.path()).unwrap())
            );
        }
        let status = child.wait().unwrap();
        let stderr = std::fs::read(stderr.path()).unwrap();
        let (request, headers) = server_rx
            .recv_timeout(Duration::from_secs(75))
            .unwrap_or_else(|error| {
                panic!(
                    "{provider}: proxy server did not finish ({error}); status: {}; stderr: {}",
                    status,
                    String::from_utf8_lossy(&stderr)
                )
            });
        assert!(
            request.starts_with("POST http://127.0.0.1:1/"),
            "{provider}: {request}"
        );
        assert!(
            headers.contains("proxy-authorization: basic dxnlcjpwyxnz"),
            "{provider}: {headers}"
        );
        assert!(!String::from_utf8_lossy(&stderr).contains("user:pass"));
    }
}

#[test]
fn no_proxy_bypasses_configured_proxy_for_matching_endpoint_and_port() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("settings.json"),
        json!({"httpProxy":"http://127.0.0.1:1","retry":{"enabled":false}}).to_string(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let target = listener.local_addr().unwrap();
    let endpoint = format!("http://{target}/v1/chat/completions");
    let (server_tx, server_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        loop {
            let (mut socket, mut reader, first) =
                accept_proxy_request(listener.try_clone().unwrap());
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            if first.starts_with("GET ") {
                socket
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                continue;
            }
            socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let _ = server_tx.send(first);
            return;
        }
    });
    let stderr = tempfile::NamedTempFile::new().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .env_remove("http_proxy")
        .env_remove("HTTP_PROXY")
        .env_remove("https_proxy")
        .env_remove("HTTPS_PROXY")
        .env("no_proxy", target.to_string())
        .args([
            "--no-session",
            "--no-mcp",
            "--offline",
            "--model",
            "fixture-model",
            "--endpoint",
            &endpoint,
            "--model-timeout-secs",
            "4",
            "--print",
            "hello",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr.reopen().unwrap()))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if child.try_wait().unwrap().is_none() {
        let _ = child.kill();
        panic!(
            "CLI timed out; stderr: {}",
            String::from_utf8_lossy(&std::fs::read(stderr.path()).unwrap())
        );
    }
    let status = child.wait().unwrap();
    let request = server_rx
        .recv_timeout(Duration::from_secs(75))
        .unwrap_or_else(|error| {
            panic!(
                "direct server did not finish ({error}); status: {status}; stderr: {}",
                String::from_utf8_lossy(&std::fs::read(stderr.path()).unwrap())
            )
        });
    assert!(
        request.starts_with("POST /v1/chat/completions "),
        "{request}"
    );
}

#[test]
fn model_listing_uses_proxy_before_a_model_request() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    std::fs::write(
        root.path().join("settings.json"),
        json!({"httpProxy":format!("http://{}", listener.local_addr().unwrap())}).to_string(),
    )
    .unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        let body = br#"{"data":[]}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
        socket.write_all(body).unwrap();
        first
    });
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .env_remove("http_proxy")
        .env_remove("HTTP_PROXY")
        .env_remove("https_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("no_proxy")
        .env_remove("NO_PROXY")
        .args([
            "--no-mcp",
            "--offline",
            "--provider",
            "ollama",
            "--endpoint",
            "http://127.0.0.1:1/v1/chat/completions",
            "--list-models",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        server
            .join()
            .unwrap()
            .starts_with("GET http://127.0.0.1:1/v1/models ")
    );
}

#[test]
fn radius_auth_refresh_uses_proxy_from_settings() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    std::fs::write(
        root.path().join("settings.json"),
        json!({"httpProxy":format!("http://{}", listener.local_addr().unwrap())}).to_string(),
    )
    .unwrap();
    std::fs::write(root.path().join("auth.json"), json!({"radius":{"type":"oauth","gateway":"http://127.0.0.1:1/","access":"old-token","refresh":"fixture-refresh","expires":0}}).to_string()).unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        let response =
            br#"{"access_token":"new-token","refresh_token":"new-refresh","expires_in":3600}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).unwrap();
        socket.write_all(response).unwrap();
        (first, body)
    });
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .env("PI_RADIUS_GATEWAY", "http://127.0.0.1:1")
        .env_remove("http_proxy")
        .env_remove("HTTP_PROXY")
        .env_remove("https_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("no_proxy")
        .env_remove("NO_PROXY")
        .args(["--provider", "radius", "auth", "print-bearer-token"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "new-token");
    let (request, body) = server.join().unwrap();
    assert!(request.starts_with("POST http://127.0.0.1:1/v1/oauth/token "));
    assert!(
        String::from_utf8(body)
            .unwrap()
            .contains("refresh_token=fixture-refresh")
    );
}

#[test]
fn mcp_provider_authenticated_connection_uses_configured_proxy() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    std::fs::write(
        root.path().join("settings.json"),
        json!({"httpProxy":format!("http://{}", listener.local_addr().unwrap())}).to_string(),
    )
    .unwrap();
    std::fs::write(root.path().join("mcp.json"), json!({"mcpServers":{"fixture":{"url":"http://127.0.0.1:1/mcp","auth":{"provider":"anthropic"},"oauth":false}}}).to_string()).unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            headers.push_str(&line.to_ascii_lowercase());
        }
        socket
            .write_all(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        (first, headers)
    });
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .env("ANTHROPIC_API_KEY", "fixture-token")
        .env_remove("http_proxy")
        .env_remove("HTTP_PROXY")
        .env_remove("https_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("no_proxy")
        .env_remove("NO_PROXY")
        .args(["mcp", "list"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let (request, headers) = server.join().unwrap_or_else(|_| {
        panic!(
            "MCP proxy fixture failed; status: {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(
        request.starts_with("POST http://127.0.0.1:1/mcp "),
        "{request}"
    );
    assert!(
        headers.contains("authorization: bearer fixture-token"),
        "{headers}"
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-token"));
}

#[test]
fn default_model_and_provider_are_read_as_separate_fields() {
    for (settings, provider, model) in [
        (
            json!({"defaultProvider":"ollama", "defaultModel":"anthropic/fixture"}),
            "ollama",
            "anthropic/fixture",
        ),
        (
            json!({"defaultProvider":"anthropic", "defaultModel":"anthropic/fixture"}),
            "anthropic",
            "anthropic/fixture",
        ),
        (
            json!({"defaultModel":"anthropic/legacy-model"}),
            "anthropic",
            "legacy-model",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("settings.json"), settings.to_string()).unwrap();
        let mut process = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .env("ANTHROPIC_API_KEY", "fixture")
            .args(["--mode", "rpc", "--no-session", "--no-mcp", "--offline"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        process.stdin.take().unwrap().write_all(b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"new\",\"type\":\"new_session\"}\n{\"id\":\"after\",\"type\":\"get_state\"}\n").unwrap();
        let output = process.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let events: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let state = events.iter().find(|event| event["id"] == "state").unwrap();
        assert_eq!(state["data"]["model"]["provider"], provider);
        assert_eq!(state["data"]["model"]["id"], model);
        let after = events.iter().find(|event| event["id"] == "after").unwrap();
        assert_eq!(after["data"]["model"]["provider"], provider);
        assert_eq!(after["data"]["model"]["id"], model);
    }
}

#[test]
fn provider_flag_without_model_survives_workspace_refresh_and_new_session() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("models.json"), json!({"providers":{
        "ollama":{"baseUrl":"https://example.invalid/v1","models":[{"id":"alpha"}]},
        "relay":{"baseUrl":"https://example.invalid/v1","api":"openai-completions","models":[{"id":"beta"}]}
    }}).to_string()).unwrap();
    std::fs::write(root.path().join("settings.json"), json!({"defaultProvider":"ollama","defaultModel":"alpha","enabledModels":["ollama/alpha","relay/beta"]}).to_string()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .args([
            "--mode",
            "rpc",
            "--no-session",
            "--no-mcp",
            "--offline",
            "--provider",
            "relay",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"{\"id\":\"before\",\"type\":\"get_state\"}\n{\"id\":\"new\",\"type\":\"new_session\"}\n{\"id\":\"after\",\"type\":\"get_state\"}\n").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for id in ["before", "after"] {
        let state = events.iter().find(|event| event["id"] == id).unwrap();
        assert_eq!(state["data"]["model"]["provider"], "relay", "{state}");
        assert_eq!(state["data"]["model"]["id"], "beta", "{state}");
    }
}

#[cfg(unix)]
#[test]
fn kimi_auth_commands_wait_for_refresh_after_sigint() {
    for command in ["check", "print-bearer-token"] {
        let root = tempfile::tempdir().unwrap();
        let auth = root.path().join("auth.json");
        std::fs::write(&auth, json!({"kimi-coding":{"type":"oauth","access":"old","refresh":"old-refresh","expires":0}}).to_string()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut bytes = [0; 4096];
            assert!(socket.read(&mut bytes).unwrap() > 0);
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let body = r#"{"access_token":"new","refresh_token":"rotated","expires_in":3600}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let barrier = tempfile::NamedTempFile::new().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_WORKER_REFRESH_BARRIER", barrier.path())
            .env("PI_RS_HOME", root.path())
            .env("KIMI_CODE_OAUTH_HOST", host)
            .args(["--provider", "kimi-coding", "auth", command])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(matches!(
            barrier.as_file().try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            child.try_wait().unwrap().is_none(),
            "{command} exited during refresh"
        );
        release_tx.send(()).unwrap();
        let output = child.wait_with_output().unwrap();
        server.join().unwrap();
        assert_eq!(
            output.status.code(),
            Some(130),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stored: Value = serde_json::from_slice(&std::fs::read(&auth).unwrap()).unwrap();
        assert_eq!(stored["kimi-coding"]["refresh"], "rotated");
        barrier.as_file().try_lock().unwrap();
        barrier.as_file().unlock().unwrap();
    }
}

#[cfg(unix)]
#[test]
fn auth_check_reads_readonly_store_and_environment_keys() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    let auth = home.join("auth.json");
    std::fs::write(&auth, json!({"kimi-coding":{"type":"oauth","access":"cached","refresh":"refresh","expires":9_000_000_000_000_u64}}).to_string()).unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o555)).unwrap();
    let kimi = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--provider", "kimi-coding", "auth", "check", "--json"])
        .output()
        .unwrap();
    let env_key = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("OPENAI_API_KEY", "fixture-key")
        .args(["--provider", "openai", "auth", "check", "--json"])
        .output()
        .unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(
        &auth,
        json!({"kimi-coding":{"type":"oauth","access":"expired","refresh":"refresh","expires":0}})
            .to_string(),
    )
    .unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o555)).unwrap();
    let expired = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--provider", "kimi-coding", "auth", "check", "--json"])
        .output()
        .unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::remove_file(&auth).unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o555)).unwrap();
    let missing = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("OPENAI_API_KEY", "fixture-key")
        .args(["--provider", "openai", "auth", "check", "--json"])
        .output()
        .unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(expired.status.code(), Some(2));
    assert!(missing.status.success());
    assert!(
        kimi.status.success(),
        "{}",
        String::from_utf8_lossy(&kimi.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&kimi.stdout).unwrap()["status"],
        "ready"
    );
    assert!(
        env_key.status.success(),
        "{}",
        String::from_utf8_lossy(&env_key.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&env_key.stdout).unwrap()["status"],
        "ready"
    );
}

#[cfg(unix)]
#[test]
fn cancelled_chatgpt_login_and_logout_do_not_wait_for_held_refresh_lock() {
    for action in ["login", "logout"] {
        let root = tempfile::tempdir().unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.path().join("chatgpt-refresh.lock"))
            .unwrap();
        lock.lock().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .args([action, "openai-chatgpt"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));
        assert!(child.try_wait().unwrap().is_none());
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let status = child.try_wait().unwrap();
        lock.unlock().unwrap();
        if status.is_none() {
            let _ = child.kill();
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            status.is_some(),
            "{action} did not cancel while the lock was held"
        );
        assert_eq!(
            output.status.code(),
            Some(130),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(unix)]
#[test]
fn sigint_interrupts_waiting_for_auth_json_lock() {
    for arguments in [
        vec!["--provider", "kimi-coding", "auth", "check"],
        vec!["login", "openai-chatgpt"],
    ] {
        let root = tempfile::tempdir().unwrap();
        let lock = root.path().join("auth.json.lock");
        std::fs::create_dir(&lock).unwrap();
        std::fs::write(root.path().join("auth.json"), "{}").unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));
        assert!(child.try_wait().unwrap().is_none());
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let status = child.try_wait().unwrap();
        if status.is_none() {
            let _ = child.kill();
        }
        let output = child.wait_with_output().unwrap();
        assert!(status.is_some(), "auth lock wait ignored SIGINT");
        assert_eq!(
            output.status.code(),
            Some(130),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(lock.exists());
    }
}

#[cfg(unix)]
#[test]
fn sigint_cancels_running_package_install() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let marker = root.path().join("git-started");
    let git = bin.join("git");
    std::fs::write(
        &git,
        "#!/bin/sh\nprintf ready > \"$PI_TEST_GIT_STARTED\"\nexec /bin/sleep 30\n",
    )
    .unwrap();
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    )))
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .env("PATH", path)
        .env("PI_TEST_GIT_STARTED", &marker)
        .args(["install", "git:https://example.invalid/pkg.git"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !marker.exists() && child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if !marker.exists() {
        let _ = child.kill();
        let output = child.wait_with_output().unwrap();
        panic!(
            "package did not start: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    let deadline = Instant::now() + Duration::from_secs(4);
    while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = child.try_wait().unwrap();
    if status.is_none() {
        let _ = child.kill();
    }
    let output = child.wait_with_output().unwrap();
    assert!(status.is_some(), "package ignored SIGINT");
    assert_eq!(
        output.status.code(),
        Some(130),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[test]
fn auth_check_reads_root_owned_0755_directory_when_unprivileged() {
    use std::os::unix::{fs::PermissionsExt, process::CommandExt};
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(home.join("auth.json"), json!({"kimi-coding":{"type":"oauth","access":"cached","refresh":"refresh","expires":9_000_000_000_000_u64}}).to_string()).unwrap();
    let binary = root.path().join("pi-test");
    std::fs::copy(env!("CARGO_BIN_EXE_pi"), &binary).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = Command::new(&binary)
        .uid(65534)
        .gid(65534)
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args([
            "--provider",
            "kimi-coding",
            "auth",
            "check",
            "--json",
            "--no-refresh",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["status"],
        "ready"
    );
}

#[cfg(unix)]
#[test]
fn sigint_cancels_package_install_waiting_for_package_lock() {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let source = "git:https://example.invalid/pkg.git";
    let directory = root
        .path()
        .join("packages")
        .join(format!("{:x}", Sha256::digest(source.as_bytes())));
    std::fs::create_dir_all(&directory).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("install.lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args(["install", source])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_secs(2));
    assert!(child.try_wait().unwrap().is_none());
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = child.try_wait().unwrap();
    lock.unlock().unwrap();
    if status.is_none() {
        let _ = child.kill();
    }
    let output = child.wait_with_output().unwrap();
    assert!(status.is_some(), "package lock wait ignored SIGINT");
    assert_eq!(
        output.status.code(),
        Some(130),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[test]
fn sigint_interrupts_managed_model_discovery() {
    for mode in [vec!["--mode", "rpc"], vec!["--models", "ollama/*", "serve"]] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut bytes = [0; 4096];
            assert!(socket.read(&mut bytes).unwrap() > 0);
            started_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .env_remove("HTTP_PROXY")
            .env_remove("http_proxy")
            .env_remove("ALL_PROXY")
            .env_remove("all_proxy")
            .args(["--no-mcp", "--provider", "ollama", "--endpoint", &endpoint])
            .args(&mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        started_rx
            .recv_timeout(Duration::from_secs(15))
            .unwrap_or_else(|e| panic!("{mode:?}: {e}"));
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let status = child.try_wait().unwrap();
        let _ = release_tx.send(());
        if status.is_none() {
            let _ = child.kill();
        }
        let output = child.wait_with_output().unwrap();
        server.join().unwrap();
        assert!(status.is_some(), "model discovery ignored SIGINT");
        assert_eq!(
            output.status.code(),
            Some(130),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(unix)]
#[test]
fn sigint_interrupts_open_rpc_and_piped_stdin() {
    for args in [vec!["--mode", "rpc"], vec!["--print"]] {
        let root = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .args(["--no-mcp", "--offline", "--model", "fixture"])
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_secs(5));
        assert!(child.try_wait().unwrap().is_none());
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let status = child.try_wait().unwrap();
        if status.is_none() {
            let _ = child.kill();
        }
        let output = child.wait_with_output().unwrap();
        assert!(status.is_some(), "{args:?} ignored SIGINT");
        assert_eq!(
            output.status.code(),
            Some(130),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(unix)]
#[test]
fn radius_refresh_survives_sigint_and_persists_rotated_token() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let gateway = format!("http://{}/", listener.local_addr().unwrap());
    let auth = root.path().join("auth.json");
    std::fs::write(&auth, json!({"radius":{"type":"oauth","gateway":gateway,"access":"old","refresh":"old-refresh","expires":0}}).to_string()).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut buffer = [0; 4096];
        assert!(socket.read(&mut buffer).unwrap() > 0);
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        let body = r#"{"access_token":"new","refresh_token":"rotated","expires_in":3600}"#;
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .env("PI_RADIUS_GATEWAY", &gateway)
        .args(["--provider", "radius", "auth", "print-bearer-token"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    std::thread::sleep(Duration::from_millis(200));
    assert!(child.try_wait().unwrap().is_none());
    release_tx.send(()).unwrap();
    let output = child.wait_with_output().unwrap();
    server.join().unwrap();
    assert_eq!(
        output.status.code(),
        Some(130),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stored: Value = serde_json::from_slice(&std::fs::read(auth).unwrap()).unwrap();
    assert_eq!(stored["radius"]["refresh"], "rotated");
}

#[cfg(unix)]
#[test]
fn sigint_interrupts_rpc_session_transition_hook() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("slow-transition.py");
    let marker = root.path().join("transition-started");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys, time
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'slow-transition', 'dependencies':[], 'commands':[], 'tools':[], 'session_transition_hook':True}
elif request['type'] == 'before_session_transition':
    with open(os.path.join(os.environ['PI_RS_HOME'], 'transition-started'), 'w') as f:
        f.write(str(os.getpid()))
    time.sleep(30)
    result = False
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--mode",
            "rpc",
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--offline",
            "--model",
            "fixture",
            "--extension",
        ])
        .arg(extension)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{}", json!({"type":"new_session", "id":"test"})).unwrap();
    stdin.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !marker.exists() && child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if !marker.exists() {
        let _ = child.kill();
        drop(stdin);
        let output = child.wait_with_output().unwrap();
        panic!(
            "transition hook did not start: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    let deadline = Instant::now() + Duration::from_secs(4);
    while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = child.try_wait().unwrap();
    if status.is_none() {
        let _ = child.kill();
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(status.is_some(), "RPC transition wait ignored SIGINT");
    let extension_pid: i32 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while unsafe { libc::kill(extension_pid, 0) } == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_ne!(
        unsafe { libc::kill(extension_pid, 0) },
        0,
        "transition extension survived shutdown"
    );
    assert_eq!(
        output.status.code(),
        Some(130),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
