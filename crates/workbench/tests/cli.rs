use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

use pi_rs::{
    Agent, CancellationToken, Limits, MemoryStore, Notification, Registry, Session, ollama::Ollama,
};
use serde_json::{Value, json};

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
        r#"{"id":"stop","type":"shutdown"}"#,
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
    assert_eq!(replies.len(), 6);
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
    assert_eq!(replies[5]["id"], "stop");
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
        let (mut socket, mut reader, first) = accept_proxy_request(listener);
        assert!(first.starts_with("GET "), "{first}");
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        accepted.send(()).unwrap();
        let _ = gate.recv_timeout(Duration::from_secs(30));
        let body = br#"{"data":[{"id":"fixture-model"}]}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
        socket.write_all(body).unwrap();
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
    assert_eq!(models["success"], true);
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
            assert!(
                records
                    .iter()
                    .any(|record| record["type"] == "answer"
                        && record["content"] == "fixture answer")
            );
        }
    }
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
        process.stdin.take().unwrap().write_all(b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n").unwrap();
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
    }
}
