use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn receive(receiver: &mpsc::Receiver<Value>, predicate: impl Fn(&Value) -> bool) -> Value {
    loop {
        let value = receiver
            .recv_timeout(Duration::from_secs(20))
            .expect("worker response timed out");
        if predicate(&value) {
            return value;
        }
    }
}

#[test]
fn share_context_tracks_idle_and_inflight_worker() {
    let root = tempfile::tempdir().unwrap();
    let prompt = root.path().join("system.txt");
    std::fs::write(&prompt, "Unique active system prompt").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (received, started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.is_empty() || line == "\r\n" {
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
        received.send(()).unwrap();
        released.recv_timeout(Duration::from_secs(20)).unwrap();
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .args([
            "--offline",
            "--no-mcp",
            "--no-trust",
            "--no-extensions",
            "--no-skills",
            "--no-context-files",
            "--trace",
            "--endpoint",
            &endpoint,
            "--tools",
            "read",
            "--system-prompt",
        ])
        .arg(&prompt)
        .arg("--session")
        .arg(root.path().join("session.jsonl"))
        .arg("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = KillOnDrop(child);
    let mut input = child.0.stdin.take().unwrap();
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(event) = serde_json::from_str::<Value>(&line) {
                let _ = tx.send(event);
            }
        }
    });
    writeln!(
        input,
        "{}",
        json!({"type":"get_share_context", "id":"idle", "session":root.path().join("session.jsonl")})
    )
    .unwrap();
    input.flush().unwrap();
    let idle = receive(&rx, |event| {
        event["id"] == "idle" && event["command"] == "get_share_context"
    });
    assert_eq!(idle["success"], true, "{idle}");
    assert!(
        idle["data"]["context"]["systemPrompt"]
            .as_str()
            .unwrap()
            .contains("Unique active system prompt")
    );
    assert!(
        idle["data"]["context"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "read")
    );
    let mut external = pi_rs::Session::open(root.path().join("session.jsonl")).unwrap();
    external
        .append_user_with_images("edited while idle".into(), vec![], None)
        .unwrap();
    let new_head = external.head();
    drop(external);
    writeln!(input, "{}", json!({"type":"get_share_context", "id":"refreshed", "session":root.path().join("session.jsonl")})).unwrap();
    input.flush().unwrap();
    let refreshed = receive(&rx, |event| {
        event["id"] == "refreshed" && event["command"] == "get_share_context"
    });
    assert_eq!(refreshed["data"]["head"], new_head);
    assert_eq!(refreshed["data"]["context"], idle["data"]["context"]);
    writeln!(input, "{}", json!({"type":"prompt","content":"hello"})).unwrap();
    input.flush().unwrap();
    let request = receive(&rx, |event| event["type"] == "request");
    started.recv_timeout(Duration::from_secs(20)).unwrap();
    writeln!(
        input,
        "{}",
        json!({"type":"get_share_context","id":"active", "session":root.path().join("session.jsonl")})
    )
    .unwrap();
    input.flush().unwrap();
    let active = receive(&rx, |event| {
        event["id"] == "active" && event["command"] == "get_share_context"
    });
    assert_eq!(active["success"], true, "{active}");
    let system = request["request"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "system")
        .unwrap();
    assert_eq!(active["data"]["context"]["systemPrompt"], system["content"]);
    assert_eq!(
        active["data"]["context"]["tools"],
        request["request"]["tools"]
    );
    release.send(()).unwrap();
    receive(&rx, |event| event["type"] == "agent_settled");
    writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
    input.flush().unwrap();
    server.join().unwrap();
}

#[cfg(unix)]
#[test]
fn terminal_shares_idle_and_during_model_request() {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::time::Instant;
    let root = tempfile::tempdir().unwrap();
    let prompt = root.path().join("system.txt");
    std::fs::write(&prompt, "Terminal share prompt").unwrap();
    let model_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let model_endpoint = format!(
        "http://{}/v1/chat/completions",
        model_listener.local_addr().unwrap()
    );
    let share_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let share_endpoint = format!(
        "http://{}/v1/artifacts",
        share_listener.local_addr().unwrap()
    );
    let (share_tx, share_rx) = mpsc::channel();
    let shares = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut socket, _) = share_listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut size = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line.is_empty() || line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        size = value.trim().parse().unwrap();
                    }
                }
            }
            let mut bytes = vec![0; size];
            reader.read_exact(&mut bytes).unwrap();
            let body = r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            let entries: Vec<Value> = bytes
                .split(|b| *b == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice(line).unwrap())
                .collect();
            share_tx.send(entries).unwrap();
        }
    });
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let model = std::thread::spawn(move || {
        let (mut socket, _) = model_listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.is_empty() || line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
            }
        }
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes).unwrap();
        started_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(20)).unwrap();
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut size,
            )
        },
        0
    );
    let mut master = unsafe { std::fs::File::from_raw_fd(master_fd) };
    let slave = unsafe { std::fs::File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .env("TERM", "xterm-256color")
        .env("RADIUS_API_KEY", "fixture")
        .env("PI_RS_RADIUS_SHARE_URL", &share_endpoint)
        .args([
            "--offline",
            "--no-mcp",
            "--no-trust",
            "--no-extensions",
            "--no-skills",
            "--no-context-files",
            "--tui-mode",
            "regular",
            "--endpoint",
            &model_endpoint,
            "--tools",
            "read",
            "--system-prompt",
        ])
        .arg(&prompt)
        .arg("--session")
        .arg(root.path().join("session.jsonl"))
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave))
        .spawn()
        .unwrap();
    let _child = KillOnDrop(child);
    let wait_output = |master: &mut std::fs::File, wanted: &str| {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut output = Vec::new();
        let mut buffer = [0; 8192];
        while Instant::now() < deadline {
            match master.read(&mut buffer) {
                Ok(n) => output.extend_from_slice(&buffer[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("terminal read: {error}"),
            }
            if String::from_utf8_lossy(&output).contains(wanted) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!(
            "terminal did not show {wanted}: {}",
            String::from_utf8_lossy(&output)
        );
    };
    wait_output(&mut master, "Enter to send");
    master.write_all(b"/share\r").unwrap();
    let idle = share_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    let idle_context = idle.last().unwrap();
    assert_eq!(idle_context["customType"], "pi.share");
    assert!(
        idle_context["data"]["systemPrompt"]
            .as_str()
            .unwrap()
            .contains("Terminal share prompt")
    );
    assert!(
        idle_context["data"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "read")
    );
    wait_output(&mut master, "Share URL:");
    master.write_all(b"hello\r").unwrap();
    started_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    master.write_all(b"/share\r").unwrap();
    let active = share_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    assert_eq!(active.last().unwrap()["customType"], "pi.share");
    assert_eq!(active.last().unwrap()["data"], idle_context["data"]);
    release_tx.send(()).unwrap();
    shares.join().unwrap();
    model.join().unwrap();
}
