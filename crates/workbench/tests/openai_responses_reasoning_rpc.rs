use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[test]
fn responses_reasoning_item_survives_rpc_stream_and_finalization() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("waiting for Responses request: {e}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("POST /v1/responses "), "{line}");
        let mut length = 0;
        loop {
            line.clear();
            assert_ne!(reader.read_line(&mut line).unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        let request: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["model"], "fixture-model");
        assert_eq!(request["stream"], true);
        let events = [
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}),
            json!({"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"msg_1","role":"assistant","content":[]}}),
            json!({"type":"response.output_text.delta","output_index":1,"delta":"Answer"}),
            json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"draft "}),
            json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"thought"}),
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"Final summary"}]}}),
            json!({"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"Answer"}]}}),
            json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"Final summary"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer"}]}]}}),
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
            "openai",
            "--model",
            "fixture-model",
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
    let deadline = Instant::now() + Duration::from_secs(20);
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
    assert!(
        !events.iter().any(|event| matches!(
            event["type"].as_str(),
            Some(
                "text_item_start"
                    | "text_item_delta"
                    | "text_item_end"
                    | "reasoning_item_start"
                    | "reasoning_item_delta"
                    | "reasoning_item_end"
                    | "reasoning_delta"
            )
        )),
        "{events:?}"
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
        .collect();
    let selected: Vec<_> = updates
        .iter()
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some(
                    "thinking_start"
                        | "thinking_delta"
                        | "thinking_end"
                        | "text_start"
                        | "text_delta"
                )
            )
        })
        .map(|event| {
            (
                event["type"].as_str().unwrap(),
                event["contentIndex"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        selected,
        [
            ("thinking_start", 0),
            ("text_start", 1),
            ("text_delta", 1),
            ("thinking_delta", 0),
            ("thinking_delta", 0),
            ("thinking_end", 0)
        ],
        "{updates:?}"
    );
    let deltas: Vec<_> = updates
        .iter()
        .filter(|event| event["type"] == "thinking_delta")
        .map(|event| event["delta"].as_str().unwrap())
        .collect();
    assert_eq!(deltas, ["draft ", "thought"]);
    let end = updates
        .iter()
        .find(|event| event["type"] == "thinking_end")
        .unwrap();
    assert_eq!(end["content"], "Final summary");
    let message = events
        .iter()
        .find(|event| event["type"] == "message_end" && event["message"]["role"] == "assistant")
        .expect("assistant message_end");
    assert_eq!(
        message["message"]["content"],
        json!([{"type":"thinking","thinking":"Final summary"},{"type":"text","text":"Answer"}])
    );
}
