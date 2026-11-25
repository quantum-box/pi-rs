#![cfg(feature = "text-cli")]

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    process::{Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

#[test]
fn persists_text_conversation_and_rejects_a_changed_provider_route() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let (finish_first_tx, finish_first_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut bodies = Vec::new();
        for (index, answer) in [
            "first answer",
            "second answer",
            "third answer",
            "partial before abort",
        ]
        .into_iter()
        .enumerate()
        {
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "provider request timed out");
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept provider request: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&mut socket);
            let mut length = 0;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            bodies.push(serde_json::from_slice::<Value>(&body).unwrap());
            let delta = serde_json::json!({
                "type": "response.output_text.delta", "output_index": 0, "delta": answer
            });
            let completed = serde_json::json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "output": [{
                        "type": "message", "role": "assistant",
                        "content": [{"type": "output_text", "text": answer}]
                    }],
                    "usage": {"input_tokens": 1, "output_tokens": 1}
                }
            });
            let partial = format!("data: {delta}\n\n");
            let final_event = format!("data: {completed}\n\n");
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{partial}",
                partial.len() + final_event.len(),
            ).unwrap();
            if index != 1 {
                socket.flush().unwrap();
                finish_first_rx
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
            let result = socket.write_all(final_event.as_bytes());
            if index != 3 {
                result.unwrap();
            }
        }
        bodies
    });

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("durable.sqlite");
    let command = |url: &str, args: &[&str]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi-rs-durable"));
        command
            .env("OPENAI_API_KEY", "fixture-key")
            .arg("--db")
            .arg(&db)
            .args(["--endpoint", url])
            .args(args);
        command
    };
    let mut first_run = command(&endpoint, &["--model", "fixture", "first request"])
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(first_run.stdout.take().unwrap());
    let mut first_frames = Vec::new();
    let mut saw_progress = false;
    loop {
        let mut line = String::new();
        if stdout.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let frame: Value = serde_json::from_str(&line).unwrap();
        if frame["type"] == "progress" && !saw_progress {
            assert_eq!(frame["text"], "first answer");
            saw_progress = true;
            finish_first_tx.send(()).unwrap();
        }
        first_frames.push(frame);
    }
    let output = first_run.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(saw_progress);
    let id = first_frames[0]["id"].as_str().unwrap();
    assert_eq!(first_frames.last().unwrap()["text"], "first answer");
    let wrong_route = command(
        "http://127.0.0.1:1/v1/responses",
        &["--conversation", id, "wrong route"],
    )
    .arg("--json")
    .output()
    .unwrap();
    assert!(!wrong_route.status.success());
    assert!(String::from_utf8_lossy(&wrong_route.stderr).contains("different provider route"));
    let second = frames(
        command(&endpoint, &["--conversation", id, "second request"])
            .arg("--json")
            .output()
            .unwrap(),
    );
    assert_eq!(second[0]["id"], id);
    assert_eq!(second.last().unwrap()["text"], "second answer");

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(stdin, r#"{{"type":"get_state","id":"status"}}"#).unwrap();
    writeln!(stdin, r#"{{"type":"get_messages","id":"history"}}"#).unwrap();
    writeln!(
        stdin,
        r#"{{"type":"prompt","id":"run","message":"third request"}}"#
    )
    .unwrap();
    drop(stdin);
    let stdout = BufReader::new(rpc.stdout.take().unwrap());
    let mut saw_rpc_progress = false;
    let rpc_frames: Vec<Value> = stdout
        .lines()
        .map(|line| {
            let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
            if frame["assistantMessageEvent"]["type"] == "text_delta" && !saw_rpc_progress {
                assert_eq!(frame["assistantMessageEvent"]["delta"], "third answer");
                saw_rpc_progress = true;
                finish_first_tx.send(()).unwrap();
            }
            frame
        })
        .collect();
    let output = rpc.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(saw_rpc_progress);
    assert_eq!(rpc_frames[0]["id"], "status");
    assert_eq!(rpc_frames[0]["data"]["sessionId"], id);
    assert_eq!(rpc_frames[1]["id"], "history");
    assert_eq!(
        rpc_frames[1]["data"]["messages"].as_array().unwrap().len(),
        4
    );
    assert_eq!(rpc_frames[2]["id"], "run");
    assert_eq!(rpc_frames[2]["success"], true);
    assert_eq!(rpc_frames[2]["data"]["disposition"], "started");
    let end = rpc_frames.last().unwrap();
    assert_eq!(end["type"], "agent_end");
    assert_eq!(end["success"], true);
    assert_eq!(end["messages"][1]["content"][0]["text"], "third answer");

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(
        stdin,
        r#"{{"type":"prompt","id":"cancel-test","message":"cancel this request"}}"#
    )
    .unwrap();
    let stdout = BufReader::new(rpc.stdout.take().unwrap());
    let mut sent_abort = false;
    let mut ended = false;
    for line in stdout.lines() {
        let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
        if frame["assistantMessageEvent"]["type"] == "text_delta" && !sent_abort {
            assert_eq!(
                frame["assistantMessageEvent"]["delta"],
                "partial before abort"
            );
            sent_abort = true;
            writeln!(stdin, r#"{{"type":"abort","id":"cancel"}}"#).unwrap();
        } else if frame["id"] == "cancel" {
            assert_eq!(frame["success"], true);
            finish_first_tx.send(()).unwrap();
        } else if frame["type"] == "agent_end" {
            assert_eq!(frame["success"], false);
            ended = true;
            break;
        }
    }
    drop(stdin);
    let output = rpc.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(sent_abort && ended);

    let bodies = worker.join().unwrap();
    assert_eq!(bodies[0]["model"], "fixture");
    let input = bodies[1]["input"].to_string();
    assert!(input.contains("first request"));
    assert!(input.contains("first answer"));
    assert!(input.contains("second request"));
    assert!(!input.contains("wrong route"));
    let third = bodies[2]["input"].to_string();
    assert!(third.contains("second answer"));
    assert!(third.contains("third request"));
}

fn frames(output: Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
