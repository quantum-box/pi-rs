use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn read_request(socket: &std::net::TcpStream) -> Value {
    let mut reader = BufReader::new(socket.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("POST "));
    let mut length = 0;
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
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
    serde_json::from_slice(&body).unwrap()
}

fn launch(
    root: &std::path::Path,
    session: &std::path::Path,
    endpoint: &str,
) -> (Child, ChildStdin, mpsc::Receiver<Value>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root)
        .env("PI_RS_HOME", root.join("home"))
        .args([
            "--offline",
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
            endpoint,
            "--mode",
            "rpc",
            "--session",
        ])
        .arg(session)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            let Ok(value) = serde_json::from_str(&line) else {
                continue;
            };
            if send.send(value).is_err() {
                break;
            }
        }
    });
    (child, input, receive)
}

fn send(input: &mut ChildStdin, value: Value) {
    writeln!(input, "{value}").unwrap();
    input.flush().unwrap();
}
fn until(receiver: &mpsc::Receiver<Value>, predicate: impl Fn(&Value) -> bool) -> Value {
    loop {
        let event = receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("timed out waiting for RPC event");
        if predicate(&event) {
            return event;
        }
    }
}
fn assistant_messages(response: &Value) -> Vec<&Value> {
    response["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .collect()
}

#[test]
fn aborted_partial_survives_rpc_snapshot_and_reopen_without_entering_model_context() {
    let root = tempfile::tempdir().unwrap();
    let session = root.path().join("session.jsonl");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let (release, released) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        let _ = read_request(&first);
        write!(
            first,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        for value in [
            json!({"type":"message_start","message":{"id":"first","model":"MiniMax-M3","usage":{}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial sentinel"}}),
        ] {
            write!(first, "data: {value}\n\n").unwrap();
        }
        first.flush().unwrap();
        released.recv_timeout(Duration::from_secs(30)).unwrap();
        drop(first);
        let (mut second, _) = listener.accept().unwrap();
        let request = read_request(&second);
        assert!(
            !request.to_string().contains("partial sentinel")
                && !request.to_string().contains("edited sentinel"),
            "cancelled partial entered model context"
        );
        let events = [
            json!({"type":"message_start","message":{"id":"second","model":"MiniMax-M3","usage":{"input_tokens":1,"output_tokens":0}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"finished"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
            json!({"type":"message_stop"}),
        ];
        let body: String = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        write!(second, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let (mut child, mut input, events) = launch(root.path(), &session, &endpoint);
    send(
        &mut input,
        json!({"type":"prompt","id":"first","message":"first question"}),
    );
    until(&events, |event| {
        event["type"] == "message_update"
            && event["assistantMessageEvent"]["delta"] == "partial sentinel"
    });
    send(&mut input, json!({"type":"abort","id":"abort"}));
    let live = until(&events, |event| {
        event["type"] == "message_end" && event["message"]["role"] == "assistant"
    });
    assert_eq!(live["message"]["stopReason"], "aborted");
    assert_eq!(live["message"]["content"][0]["text"], "partial sentinel");
    until(&events, |event| {
        event["type"] == "response" && event["id"] == "abort"
    });
    release.send(()).unwrap();
    send(&mut input, json!({"type":"get_messages","id":"snapshot"}));
    let response = until(&events, |event| {
        event["type"] == "response" && event["id"] == "snapshot"
    });
    let messages = assistant_messages(&response);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["stopReason"], "aborted");
    assert_eq!(messages[0]["content"][0]["text"], "partial sentinel");
    send(&mut input, json!({"type":"shutdown"}));
    drop(input);
    assert!(child.wait().unwrap().success());

    let (mut child, mut input, events) = launch(root.path(), &session, &endpoint);
    send(&mut input, json!({"type":"get_messages","id":"reopened"}));
    let response = until(&events, |event| {
        event["type"] == "response" && event["id"] == "reopened"
    });
    let messages = assistant_messages(&response);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"][0]["text"], "partial sentinel");
    assert_eq!(messages[0]["stopReason"], "aborted");
    send(&mut input, json!({"type":"get_entries","id":"entries"}));
    let entries = until(&events, |event| {
        event["type"] == "response" && event["id"] == "entries"
    });
    let partial_entry = entries["data"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["type"] == "message" && entry["message"]["stopReason"] == "aborted")
        .expect("aborted entry");
    let target = partial_entry["id"].as_str().unwrap().to_owned();
    assert_eq!(
        partial_entry["message"]["content"][0]["text"],
        "partial sentinel"
    );
    send(&mut input, json!({"type":"get_tree","id":"tree"}));
    let tree = until(&events, |event| {
        event["type"] == "response" && event["id"] == "tree"
    });
    assert!(tree["data"]["tree"].to_string().contains(&target));
    send(
        &mut input,
        json!({"type":"edit_context","id":"edit","targetId":target,"replacement":{"content":"edited sentinel"}}),
    );
    let edit = until(&events, |event| {
        event["type"] == "response" && event["id"] == "edit"
    });
    assert_eq!(edit["success"], true, "{edit}");
    send(&mut input, json!({"type":"get_messages","id":"edited"}));
    let edited = until(&events, |event| {
        event["type"] == "response" && event["id"] == "edited"
    });
    assert_eq!(
        assistant_messages(&edited)[0]["content"][0]["text"],
        "edited sentinel"
    );
    send(
        &mut input,
        json!({"type":"prompt","id":"second","message":"second question"}),
    );
    until(&events, |event| event["type"] == "agent_end");
    send(&mut input, json!({"type":"get_messages","id":"after"}));
    let response = until(&events, |event| {
        event["type"] == "response" && event["id"] == "after"
    });
    let messages = assistant_messages(&response);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["content"][0]["text"], "edited sentinel");
    assert_eq!(messages[1]["content"][0]["text"], "finished");
    send(
        &mut input,
        json!({"type":"edit_context","id":"delete","targetId":target,"replacement":null}),
    );
    let deleted = until(&events, |event| {
        event["type"] == "response" && event["id"] == "delete"
    });
    assert_eq!(deleted["success"], true, "{deleted}");
    send(
        &mut input,
        json!({"type":"get_messages","id":"after-delete"}),
    );
    let after_delete = until(&events, |event| {
        event["type"] == "response" && event["id"] == "after-delete"
    });
    let messages = assistant_messages(&after_delete);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"][0]["text"], "finished");
    send(&mut input, json!({"type":"shutdown"}));
    drop(input);
    assert!(child.wait().unwrap().success());
    server.join().unwrap();
}
