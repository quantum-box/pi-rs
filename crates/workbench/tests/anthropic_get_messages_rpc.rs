use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[test]
fn anthropic_blocks_survive_get_messages() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/anthropic/v1/messages",
        listener.local_addr().unwrap()
    );
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("waiting for Anthropic request: {e}"),
            }
        };
        // Accepted sockets can inherit the listener's nonblocking mode.
        // Switch before cloning so request reads wait for the client.
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("POST /anthropic/v1/messages "), "{line}");
        let mut length = 0;
        loop {
            line.clear();
            assert_ne!(reader.read_line(&mut line).unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        let request: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["model"], "MiniMax-M3");
        assert_eq!(request["stream"], true);
        let events = [
            json!({"type":"message_start","message":{"id":"msg_fixture","model":"MiniMax-M3","usage":{"input_tokens":1,"output_tokens":0}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"first thought"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig0"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"second thought"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"signature_delta","signature":"sig1"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"redacted_thinking","data":"opaque-secret-fixture"}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"content_block_start","index":3,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":3,"delta":{"type":"text_delta","text":"first answer"}}),
            json!({"type":"content_block_stop","index":3}),
            json!({"type":"content_block_start","index":4,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":4,"delta":{"type":"text_delta","text":"second answer"}}),
            json!({"type":"content_block_stop","index":4}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}),
            json!({"type":"message_stop"}),
        ];
        let sse: String = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}", sse.len()).unwrap();
    });

    let stderr_path = root.path().join("stderr");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .args([
            "--offline",
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--no-tools",
            "--provider",
            "minimax",
            "--model",
            "MiniMax-M3",
            "--api-key",
            "fixture",
            "--endpoint",
            &endpoint,
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if send.send(line).is_err() {
                break;
            }
        }
    });
    writeln!(
        input,
        "{}",
        json!({"type":"prompt","id":"run","message":"hello"})
    )
    .unwrap();
    input.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(70);
    let mut events = Vec::new();
    loop {
        let line = receive
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "waiting for agent_end: {error}; events: {events:?}; stderr: {}",
                    std::fs::read_to_string(&stderr_path).unwrap_or_default()
                );
            })
            .unwrap();
        let event: Value = serde_json::from_str(&line).unwrap();
        let done = event["type"] == "agent_end";
        events.push(event);
        if done {
            break;
        }
    }
    writeln!(input, "{}", json!({"type":"get_messages","id":"snapshot"})).unwrap();
    input.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut intervening = Vec::new();
    let response = loop {
        let line = receive
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "waiting for get_messages response: {error}; events: {intervening:?}; stderr: {}",
                    std::fs::read_to_string(&stderr_path).unwrap_or_default()
                );
            })
            .unwrap();
        let event: Value = serde_json::from_str(&line).unwrap();
        if event["type"] == "response" && event["id"] == "snapshot" {
            break event;
        }
        intervening.push(event);
    };
    assert_eq!(response["success"], true, "{response:?}");
    let message = response["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "assistant")
        .expect("assistant snapshot");
    assert_eq!(
        message["content"],
        json!([
            {"type":"thinking","thinking":"first thought"},
            {"type":"thinking","thinking":"second thought"},
            {"type":"thinking","thinking":"[Reasoning redacted]","redacted":true},
            {"type":"text","text":"first answer"},
            {"type":"text","text":"second answer"}
        ])
    );
    let snapshot_redacted = message["content"][2].clone();
    let visible = serde_json::to_string(&response).unwrap();
    assert!(!visible.contains("opaque-secret-fixture"));
    assert!(!visible.contains("sig0") && !visible.contains("sig1"));
    writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
    drop(input);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("RPC shutdown timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    reader.join().unwrap();
    server.join().unwrap();
    let stderr = std::fs::read_to_string(&stderr_path).unwrap();
    assert!(status.success(), "{stderr}");
    assert!(!stderr.contains("opaque-secret-fixture"));
    assert!(
        events
            .iter()
            .all(|event| event.get("redactedSignature").is_none())
    );
    assert!(
        !serde_json::to_string(
            &events
                .iter()
                .filter(|event| event["type"] == "message_update")
                .collect::<Vec<_>>()
        )
        .unwrap()
        .contains("opaque-secret-fixture")
    );
    assert_eq!(events.last().unwrap()["success"], true, "{events:?}");
    let redacted_start = events
        .iter()
        .filter_map(|event| event.get("assistantMessageEvent"))
        .find(|event| event["type"] == "thinking_start" && event["contentIndex"] == 2)
        .expect("redacted thinking start");
    assert_eq!(
        redacted_start["partial"]["content"][2],
        json!({"type":"thinking","thinking":"[Reasoning redacted]","redacted":true})
    );
    assert!(
        events.iter().any(|event| event["type"] == "response"
            && event["id"] == "run"
            && event["success"] == true),
        "{events:?}"
    );
    let updates: Vec<_> = events
        .iter()
        .filter_map(|event| event.get("assistantMessageEvent"))
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some(
                    "thinking_start"
                        | "thinking_delta"
                        | "thinking_end"
                        | "text_start"
                        | "text_delta"
                        | "text_end"
                )
            )
        })
        .map(|event| {
            (
                event["type"].as_str().unwrap(),
                event["contentIndex"].as_u64().unwrap(),
                event
                    .get("delta")
                    .or_else(|| event.get("content"))
                    .and_then(Value::as_str),
            )
        })
        .collect();
    assert_eq!(
        updates,
        [
            ("thinking_start", 0, None),
            ("thinking_delta", 0, Some("first thought")),
            ("thinking_end", 0, Some("first thought")),
            ("thinking_start", 1, None),
            ("thinking_delta", 1, Some("second thought")),
            ("thinking_end", 1, Some("second thought")),
            ("thinking_start", 2, None),
            ("thinking_end", 2, Some("[Reasoning redacted]")),
            ("text_start", 3, None),
            ("text_delta", 3, Some("first answer")),
            ("text_end", 3, Some("first answer")),
            ("text_start", 4, None),
            ("text_delta", 4, Some("second answer")),
            ("text_end", 4, Some("second answer")),
        ],
        "{events:?}"
    );
    let message = events
        .iter()
        .find(|event| event["type"] == "message_end" && event["message"]["role"] == "assistant")
        .expect("assistant message_end");
    assert_eq!(message["message"]["content"][2], snapshot_redacted);
    assert_eq!(
        message["message"]["content"],
        json!([
            {"type":"thinking","thinking":"first thought"},
            {"type":"thinking","thinking":"second thought"},
            {"type":"thinking","thinking":"[Reasoning redacted]","redacted":true},
            {"type":"text","text":"first answer"},
            {"type":"text","text":"second answer"}
        ])
    );
}
