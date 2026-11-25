#![cfg(feature = "text-cli")]

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

#[test]
fn persists_text_conversation_and_rejects_a_changed_provider_route() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let mut bodies = Vec::new();
        for answer in ["first answer", "second answer"] {
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
            let stream = format!("data: {delta}\n\ndata: {completed}\n\n");
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{stream}",
                stream.len(),
            ).unwrap();
        }
        bodies
    });

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("durable.sqlite");
    let run = |url: &str, args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi-rs-durable"))
            .env("OPENAI_API_KEY", "fixture-key")
            .arg("--db")
            .arg(&db)
            .args(["--endpoint", url, "--json"])
            .args(args)
            .output()
            .unwrap()
    };

    let first = frames(run(&endpoint, &["--model", "fixture", "first request"]));
    let id = first[0]["id"].as_str().unwrap();
    assert_eq!(first.last().unwrap()["text"], "first answer");
    let wrong_route = run(
        "http://127.0.0.1:1/v1/responses",
        &["--conversation", id, "wrong route"],
    );
    assert!(!wrong_route.status.success());
    assert!(String::from_utf8_lossy(&wrong_route.stderr).contains("different provider route"));
    let second = frames(run(&endpoint, &["--conversation", id, "second request"]));
    assert_eq!(second[0]["id"], id);
    assert_eq!(second.last().unwrap()["text"], "second answer");

    let bodies = worker.join().unwrap();
    assert_eq!(bodies[0]["model"], "fixture");
    let input = bodies[1]["input"].to_string();
    assert!(input.contains("first request"));
    assert!(input.contains("first answer"));
    assert!(input.contains("second request"));
    assert!(!input.contains("wrong route"));
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
