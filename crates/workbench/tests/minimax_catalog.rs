use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn minimax_m27_catalog_and_budgeted_messages_work_offline() {
    for (provider, model, input_price) in [
        ("minimax", "MiniMax-M2.7", 0.3),
        ("minimax", "MiniMax-M2.7-highspeed", 0.6),
        ("minimax-cn", "MiniMax-M2.7", 0.3137),
        ("minimax-cn", "MiniMax-M2.7-highspeed", 0.6275),
    ] {
        let root = tempfile::tempdir().unwrap();
        let listing = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .args([
                "--offline",
                "--no-mcp",
                "--provider",
                provider,
                "--api-key",
                "fixture",
                "--mode",
                "json",
                "--list-models",
                model,
            ])
            .output()
            .unwrap();
        assert!(
            listing.status.success(),
            "{}",
            String::from_utf8_lossy(&listing.stderr)
        );
        let rows: Vec<Value> = String::from_utf8(listing.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let row = rows
            .iter()
            .find(|row| row["model"] == model)
            .expect("catalog model");
        assert_eq!(row["provider"], provider);
        assert_eq!(row["contextWindow"], 204800);
        assert_eq!(row["maxTokens"], 131072);
        assert_eq!(row["reasoning"], true);
        assert_eq!(row["images"], false);
        assert_eq!(row["cost"]["input"], input_price);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!(
            "http://{}/anthropic/v1/messages",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(error) => panic!("accept model request: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert!(line.starts_with("POST /anthropic/v1/messages "), "{line}");
            let mut headers = String::new();
            let mut length = 0;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some((key, value)) = line.split_once(':')
                    && key.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap();
                }
                headers.push_str(&line.to_ascii_lowercase());
            }
            assert!(headers.contains("authorization: bearer fixture"));
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["model"], model);
            assert_eq!(body["thinking"]["type"], "enabled");
            assert!(
                body["thinking"]["budget_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
            let events = concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"fixture\"}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
        });
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
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
        assert!(status.success(), "{provider}/{model} failed");
    }
}
