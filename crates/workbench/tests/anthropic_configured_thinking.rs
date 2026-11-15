use serde_json::{Value, json};
use std::io::{BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

#[derive(Debug)]
enum FixtureEvent {
    Accepted,
    Reading,
    RequestLine,
    BodyReading,
    Finished,
}

fn disconnected(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NotConnected
    )
}

fn fixture_write(result: std::io::Result<()>, stopping: &AtomicBool) -> bool {
    match result {
        Ok(()) => true,
        Err(_) if stopping.load(Ordering::Acquire) => false,
        Err(error) => panic!("write fixture response: {error}"),
    }
}

// Discovery clients can abandon a fully sent GET before reading the response.
// A model POST still uses fixture_write so response failures remain visible.
fn fixture_discovery_write(result: std::io::Result<()>, stopping: &AtomicBool) -> bool {
    match result {
        Ok(()) => true,
        Err(error) if stopping.load(Ordering::Acquire) || disconnected(&error) => false,
        Err(error) => panic!("write discovery response: {error}"),
    }
}

// Poll blocking sockets so shutdown is bounded even where shutdown on a
// duplicated socket does not interrupt another thread's in-flight read.
const SOCKET_POLL: Duration = Duration::from_millis(100);
const REQUEST_WAIT: Duration = Duration::from_secs(10);

fn retryable_read(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::Interrupted
    )
}

fn fixture_line(reader: &mut BufReader<TcpStream>, stopping: &AtomicBool) -> Option<String> {
    let deadline = Instant::now() + REQUEST_WAIT;
    let mut bytes = Vec::new();
    loop {
        if stopping.load(Ordering::Acquire) {
            return None;
        }
        let mut byte = [0];
        match reader.read(&mut byte) {
            Ok(0) => return None,
            Ok(_) => {
                bytes.push(byte[0]);
                assert!(bytes.len() <= 64 * 1024, "fixture request line too long");
                if byte[0] == b'\n' {
                    return Some(String::from_utf8(bytes).expect("request line must be UTF-8"));
                }
            }
            Err(error) if disconnected(&error) => return None,
            Err(error) if retryable_read(&error) && Instant::now() < deadline => {}
            Err(error) if retryable_read(&error) => return None,
            Err(error) => panic!("read fixture line: {error}"),
        }
    }
}

fn fixture_body(
    reader: &mut BufReader<TcpStream>,
    bytes: &mut [u8],
    stopping: &AtomicBool,
) -> bool {
    let deadline = Instant::now() + REQUEST_WAIT;
    let mut offset = 0;
    while offset < bytes.len() {
        if stopping.load(Ordering::Acquire) {
            return false;
        }
        match reader.read(&mut bytes[offset..]) {
            Ok(0) => return false,
            Ok(count) => offset += count,
            Err(error) if disconnected(&error) => return false,
            Err(error) if retryable_read(&error) && Instant::now() < deadline => {}
            Err(error) if retryable_read(&error) => return false,
            Err(error) => panic!("read request body: {error}"),
        }
    }
    true
}

// Handle each accepted connection independently so an idle connection cannot
// prevent the actual HTTP request from reaching its assertions.
fn serve_connection(
    mut socket: TcpStream,
    discovery: i32,
    expected: &str,
    level: &str,
    stopping: &AtomicBool,
    progress: Option<mpsc::Sender<FixtureEvent>>,
) -> bool {
    // Windows inherits the listener's nonblocking mode on accept.
    // The buffered HTTP reader below relies on blocking reads.
    socket.set_nonblocking(false).unwrap();
    socket.set_read_timeout(Some(SOCKET_POLL)).unwrap();
    socket.set_write_timeout(Some(SOCKET_POLL)).unwrap();
    let mut reader = BufReader::new(socket.try_clone().unwrap());
    if let Some(progress) = &progress {
        let _ = progress.send(FixtureEvent::Reading);
    }
    let Some(request) = fixture_line(&mut reader, stopping) else {
        return false;
    };
    if let Some(progress) = &progress {
        let _ = progress.send(FixtureEvent::RequestLine);
    }
    let mut length = 0;
    loop {
        let Some(line) = fixture_line(&mut reader, stopping) else {
            return false;
        };
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap();
        }
    }
    if request.starts_with("GET ") {
        if discovery != 0 {
            let capabilities = if discovery == 3 {
                json!({"thinking":{"supported":true,"types":{"adaptive":{"supported":true},"disabled":{"supported":true}}},"effort":{"low":{"supported":true},"high":{"supported":true}}})
            } else if discovery == 4 {
                json!({})
            } else {
                json!({"thinking":{"supported":false}})
            };
            let model = json!({"id":"fixture-model","max_tokens":4096,"max_input_tokens":200000,"capabilities":capabilities});
            let body = if request.contains("/models?") {
                json!({"data":[model],"has_more":false}).to_string()
            } else {
                model.to_string()
            };
            if !fixture_discovery_write(
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ),
                stopping,
            ) {
                return false;
            }
        } else if !fixture_discovery_write(
            socket.write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ),
            stopping,
        ) {
            return false;
        }
        return false;
    }
    assert!(request.starts_with("POST /v1/messages "), "{request}");
    let mut bytes = vec![0; length];
    if let Some(progress) = &progress {
        let _ = progress.send(FixtureEvent::BodyReading);
    }
    if !fixture_body(&mut reader, &mut bytes, stopping) {
        return false;
    }
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    if discovery != 0 {
        assert_eq!(body["max_tokens"], 4096);
    }
    if expected == "budgeted" {
        assert_eq!(body["thinking"]["type"], "enabled");
        assert!(
            body["thinking"]["budget_tokens"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
        if level == "low" {
            assert_eq!(body["thinking"]["budget_tokens"], 2048);
        }
    } else if expected == "disabled" {
        assert!(body.get("thinking").is_none());
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
    fixture_write(
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}",
            events.len()
        ),
        stopping,
    )
}

fn serve_fixture(
    listener: TcpListener,
    discovery: i32,
    expected: &str,
    level: &str,
    accepted: Option<mpsc::Sender<FixtureEvent>>,
    timeout: Duration,
    max_connections: usize,
) {
    let deadline = Instant::now() + timeout;
    let (sender, receiver) = mpsc::channel();
    let mut workers: Vec<(TcpStream, std::thread::JoinHandle<()>)> = Vec::new();
    let mut completed = false;
    let mut failure = None;
    let mut accept_error = None;
    let stopping = Arc::new(AtomicBool::new(false));
    loop {
        while let Ok(result) = receiver.try_recv() {
            match result {
                Ok(true) => completed = true,
                Ok(false) => {}
                Err(error) => failure = Some(error),
            }
        }
        if completed || failure.is_some() || Instant::now() >= deadline {
            break;
        }
        // Reclaim finished readers and their control sockets before accepting
        // another connection: the limit applies to concurrent workers.
        let mut index = 0;
        while index < workers.len() {
            if workers[index].1.is_finished() {
                let (_, worker) = workers.swap_remove(index);
                worker.join().unwrap();
            } else {
                index += 1;
            }
        }
        if workers.len() < max_connections {
            match listener.accept() {
                Ok((socket, _)) => {
                    let control = socket.try_clone().unwrap();
                    let sender = sender.clone();
                    let stopping = stopping.clone();
                    let progress = accepted.clone();
                    let finished = accepted.clone();
                    let expected = expected.to_owned();
                    let level = level.to_owned();
                    let worker = std::thread::spawn(move || {
                        let result = std::panic::catch_unwind(|| {
                            serve_connection(
                                socket, discovery, &expected, &level, &stopping, progress,
                            )
                        });
                        let _ = sender.send(result);
                        if let Some(finished) = finished {
                            let _ = finished.send(FixtureEvent::Finished);
                        }
                    });
                    workers.push((control, worker));
                    if let Some(accepted) = &accepted {
                        let _ = accepted.send(FixtureEvent::Accepted);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    accept_error = Some(error);
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    // Interrupt idle readers before joining, including on timeout or assertion failure.
    stopping.store(true, Ordering::Release);
    for (socket, _) in &workers {
        let _ = socket.shutdown(Shutdown::Both);
    }
    for (_, worker) in workers {
        worker.join().unwrap();
    }
    while let Ok(result) = receiver.try_recv() {
        if let Err(error) = result {
            failure = Some(error);
        }
    }
    if let Some(error) = failure {
        std::panic::resume_unwind(error);
    }
    if let Some(error) = accept_error {
        panic!("accept model request: {error}");
    }
    assert!(
        completed,
        "model request was not received before fixture deadline (max {max_connections} connections)"
    );
}

#[test]
fn configured_thinking_controls_wire_without_models_api() {
    for (offline, adaptive, mapped, level, expected, discovery) in [
        (true, false, false, "high", "budgeted", 0),
        (false, false, false, "minimal", "budgeted", 0),
        (true, false, true, "low", "budgeted", 0),
        (false, false, true, "low", "budgeted", 1),
        (false, false, false, "high", "disabled", 2),
        (false, false, false, "low", "budgeted", 4),
        (false, false, false, "minimal", "low", 3),
        (true, true, false, "minimal", "low", 0),
        (true, true, true, "minimal", "medium", 0),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut model = json!({"id":"fixture-model","reasoning":true});
        if adaptive {
            model["compat"] = json!({"forceAdaptiveThinking":true});
        }
        if mapped {
            model["thinkingLevelMap"] = if adaptive {
                json!({"minimal":"medium"})
            } else {
                json!({"low":"high"})
            };
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
            serve_fixture(
                listener,
                discovery,
                expected,
                level,
                None,
                Duration::from_secs(20),
                32,
            )
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
                "--max-output-tokens",
                "8192",
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

#[test]
fn idle_connection_does_not_block_following_model_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted, received) = mpsc::channel();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            Some(accepted),
            Duration::from_secs(15),
            32,
        )
    });
    let idle = TcpStream::connect(address).unwrap();
    received.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut partial = TcpStream::connect(address).unwrap();
    partial.write_all(b"GET /v1/").unwrap();
    received.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut incomplete_headers = TcpStream::connect(address).unwrap();
    incomplete_headers
        .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\n")
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match received
            .recv_timeout(until.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            FixtureEvent::RequestLine => break,
            FixtureEvent::Accepted
            | FixtureEvent::Reading
            | FixtureEvent::BodyReading
            | FixtureEvent::Finished => {}
        }
    }
    let mut incomplete_body = TcpStream::connect(address).unwrap();
    incomplete_body.write_all(b"POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{\"partial\":").unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match received
            .recv_timeout(until.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            FixtureEvent::BodyReading => break,
            FixtureEvent::Accepted
            | FixtureEvent::Reading
            | FixtureEvent::RequestLine
            | FixtureEvent::Finished => {}
        }
    }
    let mut request = TcpStream::connect(address).unwrap();
    request
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let body = r#"{"thinking":{"type":"enabled","budget_tokens":1000}}"#;
    write!(
        request,
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("message_stop"));
    server.join().unwrap();
    drop((idle, partial, incomplete_headers, incomplete_body));
}

#[test]
fn fixture_limits_connections_and_finishes_at_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted, received) = mpsc::channel();
    let start = Instant::now();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            Some(accepted),
            Duration::from_secs(2),
            1,
        )
    });
    let idle = TcpStream::connect(address).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    let mut accepted_seen = false;
    let mut reading_seen = false;
    while !accepted_seen || !reading_seen {
        match received
            .recv_timeout(until.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            FixtureEvent::Accepted => accepted_seen = true,
            FixtureEvent::Reading => reading_seen = true,
            other => panic!("unexpected fixture event: {other:?}"),
        }
    }
    let _queued = TcpStream::connect(address).unwrap();
    assert!(
        server.join().is_err(),
        "fixture must fail when no model request arrives"
    );
    assert!(
        received
            .try_iter()
            .all(|event| !matches!(event, FixtureEvent::Accepted)),
        "only one connection may be accepted"
    );
    assert!(start.elapsed() < Duration::from_secs(8));
    drop(idle);
}

#[test]
fn fixture_rejects_complete_invalid_model_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            None,
            Duration::from_secs(5),
            4,
        )
    });
    let mut request = TcpStream::connect(address).unwrap();
    let body = r#"{"thinking":{"type":"disabled"}}"#;
    write!(
        request,
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    assert!(
        server.join().is_err(),
        "a complete request with invalid thinking must fail its assertion"
    );
}

#[test]
fn early_header_disconnect_does_not_block_following_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (progress, received) = mpsc::channel();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            Some(progress),
            Duration::from_secs(15),
            8,
        )
    });
    for headers in [
        b"GET /v1/models HTTP/1.1\r\nHost: partial".as_slice(),
        b"GET /v1/models HTTP/1.1\r\nHost: complete\r\n".as_slice(),
    ] {
        let mut disconnected = TcpStream::connect(address).unwrap();
        disconnected.write_all(headers).unwrap();
        drop(disconnected);
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            match received
                .recv_timeout(until.saturating_duration_since(Instant::now()))
                .unwrap()
            {
                FixtureEvent::Finished => break,
                FixtureEvent::Accepted
                | FixtureEvent::Reading
                | FixtureEvent::RequestLine
                | FixtureEvent::BodyReading => {}
            }
        }
    }
    let mut request = TcpStream::connect(address).unwrap();
    request
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let body = r#"{"thinking":{"type":"enabled","budget_tokens":1000}}"#;
    write!(
        request,
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("message_stop"));
    server.join().unwrap();
}

#[test]
fn disconnected_discovery_response_does_not_mask_other_write_errors() {
    let stopping = AtomicBool::new(false);
    for kind in [
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::ConnectionReset,
    ] {
        assert!(!fixture_discovery_write(
            Err(std::io::Error::from(kind)),
            &stopping
        ));
        assert!(
            std::panic::catch_unwind(|| fixture_write(Err(std::io::Error::from(kind)), &stopping))
                .is_err()
        );
    }
    assert!(
        std::panic::catch_unwind(|| fixture_discovery_write(
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            &stopping
        ))
        .is_err()
    );
}

#[test]
fn disconnected_discovery_client_does_not_block_model_request() {
    for discovery in [0, 1] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (progress, received) = mpsc::channel();
        let server = std::thread::spawn(move || {
            serve_fixture(
                listener,
                discovery,
                "budgeted",
                "high",
                Some(progress),
                Duration::from_secs(15),
                8,
            )
        });
        let mut client = TcpStream::connect(address).unwrap();
        client
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let _ = client.shutdown(Shutdown::Both);
        drop(client);
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            match received
                .recv_timeout(until.saturating_duration_since(Instant::now()))
                .unwrap()
            {
                FixtureEvent::Finished => break,
                FixtureEvent::Accepted
                | FixtureEvent::Reading
                | FixtureEvent::RequestLine
                | FixtureEvent::BodyReading => {}
            }
        }
        let mut request = TcpStream::connect(address).unwrap();
        request
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let body = r#"{"max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":1000}}"#;
        write!(
            request,
            "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut response = String::new();
        request.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("message_stop"));
        server.join().unwrap();
    }
}

#[test]
fn fragmented_request_survives_read_poll_timeouts() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            None,
            Duration::from_secs(10),
            4,
        )
    });
    let mut request = TcpStream::connect(address).unwrap();
    request
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    request.write_all(b"POST /v1/mes").unwrap();
    std::thread::sleep(SOCKET_POLL * 3);
    let body = r#"{"thinking":{"type":"enabled","budget_tokens":1000}}"#;
    write!(
        request,
        "sages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        &body[..10]
    )
    .unwrap();
    std::thread::sleep(SOCKET_POLL * 3);
    request.write_all(&body.as_bytes()[10..]).unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("message_stop"));
    server.join().unwrap();
}

#[test]
fn timed_out_partial_body_does_not_stop_following_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (progress, received) = mpsc::channel();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            Some(progress),
            Duration::from_secs(25),
            4,
        )
    });
    let mut partial = TcpStream::connect(address).unwrap();
    let start = Instant::now();
    partial.write_all(b"POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{\"partial\":").unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !matches!(
        received
            .recv_timeout(until.saturating_duration_since(Instant::now()))
            .unwrap(),
        FixtureEvent::BodyReading
    ) {}
    let until = Instant::now() + REQUEST_WAIT + Duration::from_secs(5);
    while !matches!(
        received
            .recv_timeout(until.saturating_duration_since(Instant::now()))
            .unwrap(),
        FixtureEvent::Finished
    ) {}
    assert!(start.elapsed() >= REQUEST_WAIT);
    let mut request = TcpStream::connect(address).unwrap();
    request
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let body = r#"{"thinking":{"type":"enabled","budget_tokens":1000}}"#;
    write!(
        request,
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("message_stop"));
    server.join().unwrap();
    drop(partial);
}

#[test]
fn completed_discovery_connections_release_worker_slots() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (progress, received) = mpsc::channel();
    let server = std::thread::spawn(move || {
        serve_fixture(
            listener,
            0,
            "budgeted",
            "high",
            Some(progress),
            Duration::from_secs(15),
            2,
        )
    });
    let idle = TcpStream::connect(address).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !matches!(
        received
            .recv_timeout(until.saturating_duration_since(Instant::now()))
            .unwrap(),
        FixtureEvent::Reading
    ) {}
    for _ in 0..6 {
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 403 Forbidden"), "{response}");
    }
    let mut request = TcpStream::connect(address).unwrap();
    request
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let body = r#"{"thinking":{"type":"enabled","budget_tokens":1000}}"#;
    write!(
        request,
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("message_stop"));
    server.join().unwrap();
    drop(idle);
}
