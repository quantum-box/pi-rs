use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn read_rpc_model_fixture_request(
    socket: &TcpStream,
    after_setup: impl FnOnce(&TcpStream) -> std::io::Result<()>,
) -> std::io::Result<(String, Vec<u8>)> {
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(Duration::from_secs(3)))?;
    after_setup(socket)?;
    let mut reader = BufReader::new(socket.try_clone()?);
    let mut request = String::new();
    if reader.read_line(&mut request)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing request",
        ));
    }
    let mut length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 || line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value
                .trim()
                .parse()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok((request, body))
}

#[test]
fn rpc_discovered_reasoning_without_levels_reaches_worker_wire() {
    use std::sync::mpsc;
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    std::fs::write(
        root.path().join("models.json"),
        json!({"providers":{"ollama":{"compat":{"supportsThinkingTokenBudget":true}}}}).to_string(),
    )
    .unwrap();
    std::fs::write(
        root.path().join("settings.json"),
        json!({"thinkingBudgets":{"low":1500}}).to_string(),
    )
    .unwrap();
    let (send, receive) = mpsc::channel();
    let stopping = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server_stopping = stopping.clone();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut workers = Vec::new();
        while Instant::now() < deadline
            && !server_stopping.load(std::sync::atomic::Ordering::Relaxed)
        {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    let send = send.clone();
                    workers.push(std::thread::spawn(move || {
                        let Ok((request, bytes)) = read_rpc_model_fixture_request(&socket, |_| Ok(())) else { return; };
                        let (body, content_type) = if request.starts_with("POST /api/show ") {
                            (r#"{"capabilities":["thinking"]}"#.to_owned(), "application/json")
                        } else if request.starts_with("GET /api/ps ") {
                            (r#"{"models":[]}"#.to_owned(), "application/json")
                        } else if request.starts_with("GET /v1/models ") {
                            (r#"{"data":[{"id":"dynamic"}]}"#.to_owned(), "application/json")
                        } else if request.starts_with("POST /v1/chat/completions ") {
                            let body: Value = serde_json::from_slice(&bytes).unwrap();
                            let _ = send.send(body);
                            ("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_owned(), "text/event-stream")
                        } else { panic!("unexpected request {request}") };
                        let _ = write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(error) => panic!("{error}"),
            }
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let mut child = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", root.path())
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--offline",
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--max-output-tokens",
                "4096",
                "--model",
                "dynamic",
                "--endpoint",
                &endpoint,
                "--mode",
                "rpc",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut input = child.0.stdin.take().unwrap();
    let output = child.0.stdout.take().unwrap();
    let (events, replies) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && events.send(value).is_err()
            {
                break;
            }
        }
    });
    writeln!(
        input,
        "{}",
        json!({"id":"thinking","type":"set_thinking_level","level":"low"})
    )
    .unwrap();
    loop {
        let event = replies
            .recv_timeout(Duration::from_secs(10))
            .expect("thinking response");
        if event["id"] == "thinking" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    writeln!(
        input,
        "{}",
        json!({"id":"prompt","type":"prompt","message":"hello"})
    )
    .unwrap();
    let body = receive
        .recv_timeout(Duration::from_secs(10))
        .expect("model request");
    assert_eq!(body["thinking_token_budget"], 1500);
    stopping.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(input);
    drop(child);
    drop(replies);
    reader.join().unwrap();
    server.join().unwrap();
}
