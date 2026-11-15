use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[test]
fn unsigned_anthropic_blocks_survive_snapshot_and_reopen() {
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
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
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
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"first answer"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"second answer"}}),
            json!({"type":"content_block_stop","index":1}),
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
    let session_path = root.path().join("session.jsonl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .args([
            "--offline",
            "--session",
            session_path.to_str().unwrap(),
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
    let expected = json!([
        {"type":"text","text":"first answer"},
        {"type":"text","text":"second answer"}
    ]);
    assert_eq!(message["content"], expected);
    writeln!(input, "{}", json!({"type":"get_entries","id":"entries"})).unwrap();
    input.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let entries = loop {
        let line = receive
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("waiting for get_entries")
            .unwrap();
        let event: Value = serde_json::from_str(&line).unwrap();
        if event["type"] == "response" && event["id"] == "entries" {
            break event;
        }
    };
    assert_eq!(entries["success"], true, "{entries:?}");
    let assistant_entry = entries["data"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["message"]["role"] == "assistant")
        .expect("assistant entry");
    assert_eq!(assistant_entry["message"], *message);
    let appended = events
        .iter()
        .find(|event| {
            event["type"] == "entry_appended" && event["entry"]["message"]["role"] == "assistant"
        })
        .expect("live assistant entry");
    assert_eq!(appended["entry"], *assistant_entry);
    let live = events
        .iter()
        .find(|event| event["type"] == "message_end" && event["message"]["role"] == "assistant")
        .expect("live assistant");
    assert_eq!(live["message"]["content"], expected);
    let stream: Vec<_> = events
        .iter()
        .filter_map(|event| event.get("assistantMessageEvent"))
        .filter_map(|event| match event["type"].as_str() {
            Some(kind @ ("text_start" | "text_delta" | "text_end")) => {
                Some((kind, event["contentIndex"].as_u64().unwrap()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        stream,
        [
            ("text_start", 0),
            ("text_delta", 0),
            ("text_end", 0),
            ("text_start", 1),
            ("text_delta", 1),
            ("text_end", 1)
        ]
    );
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
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(stderr_path).unwrap()
    );
    assert_eq!(events.last().unwrap()["success"], true, "{events:?}");
    let mut reopened = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .args([
            "--offline",
            "--no-mcp",
            "--no-trust",
            "--no-tools",
            "--session",
            session_path.to_str().unwrap(),
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
        .stderr(Stdio::from(
            std::fs::File::create(root.path().join("reopen-stderr")).unwrap(),
        ))
        .spawn()
        .unwrap();
    let mut reopen_input = reopened.stdin.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let output = reopened.stdout.take().unwrap();
    let reopen_reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    writeln!(
        reopen_input,
        "{}",
        json!({"type":"get_messages","id":"reopened","includeNative":true})
    )
    .unwrap();
    reopen_input.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let response = loop {
        let line = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = reopened.kill();
                let _ = reopened.wait();
                panic!(
                    "reopened snapshot timed out: {error}; stderr: {}",
                    std::fs::read_to_string(root.path().join("reopen-stderr")).unwrap_or_default()
                );
            })
            .unwrap();
        let event: Value = serde_json::from_str(&line).unwrap();
        if event["type"] == "response" && event["id"] == "reopened" {
            break event;
        }
    };
    assert_eq!(response["success"], true, "{response:?}");
    let assistant = response["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "assistant")
        .unwrap();
    assert_eq!(assistant["content"], expected);
    let native = response["data"]["nativeMessages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "assistant")
        .unwrap();
    assert!(
        native.get("replay").is_none() || native["replay"].is_null(),
        "{native:?}"
    );
    writeln!(reopen_input, "{}", json!({"type":"shutdown"})).unwrap();
    drop(reopen_input);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = reopened.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if Instant::now() >= deadline {
            let _ = reopened.kill();
            let _ = reopened.wait();
            panic!("reopened RPC shutdown timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    reopen_reader.join().unwrap();
}
