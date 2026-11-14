use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn configured_thinking_controls_wire_without_models_api() {
    for (offline, adaptive, mapped, level, expected) in [
        (true, false, false, "high", "budgeted"),
        (false, false, false, "minimal", "budgeted"),
        (true, true, false, "minimal", "low"),
        (true, true, true, "minimal", "medium"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut model = json!({"id":"fixture-model","reasoning":true});
        if adaptive {
            model["compat"] = json!({"forceAdaptiveThinking":true});
        }
        if mapped {
            model["thinkingLevelMap"] = json!({"minimal":"medium"});
        }
        std::fs::write(
            root.path().join("models.json"),
            json!({"providers":{"anthropic":{"models":[model]}}}).to_string(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let mut socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(error) => panic!("accept model request: {error}"),
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let request = line.clone();
                let mut length = 0;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        length = value.trim().parse().unwrap();
                    }
                }
                if request.starts_with("GET ") {
                    socket.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    continue;
                }
                assert!(request.starts_with("POST /v1/messages "), "{request}");
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                if expected == "budgeted" {
                    assert_eq!(body["thinking"]["type"], "enabled");
                    assert!(
                        body["thinking"]["budget_tokens"]
                            .as_u64()
                            .is_some_and(|value| value > 0)
                    );
                } else {
                    assert_eq!(body["thinking"]["type"], "adaptive");
                    assert_eq!(body["output_config"]["effort"], expected);
                }
                let events = concat!(
                    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"fixture-model\"}}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n"
                );
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
                break;
            }
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--no-tools",
                "--provider",
                "anthropic",
                "--model",
                "fixture-model",
                "--thinking",
                level,
                "--api-key",
                "fixture",
                "--endpoint",
                &endpoint,
            ]);
        if offline {
            command.arg("--offline");
        }
        let mut child = command
            .args(["--print", "hello"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("model request timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        server.join().unwrap();
        assert!(status.success(), "configured model request failed");
    }
}
