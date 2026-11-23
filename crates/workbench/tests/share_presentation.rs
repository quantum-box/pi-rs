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

// Native Ollama probing can precede the actual chat request. Reply to it
// immediately so the in-flight share is synchronized with the model stream.
fn accept_model_request(listener: TcpListener) -> (std::net::TcpStream, Value) {
    for _ in 0..8 {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.is_empty() || line == "\r\n" {
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
        if request_line.starts_with("GET /api/ps ") || request_line.starts_with("POST /api/show ") {
            let response = r#"{"models":[]}"#;
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
            continue;
        }
        assert!(
            request_line.starts_with("POST /v1/chat/completions "),
            "unexpected model request: {request_line:?}"
        );
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["stream"], true, "not a streaming chat request: {body}");
        assert!(
            body["messages"]
                .as_array()
                .is_some_and(|messages| !messages.is_empty()),
            "missing model messages: {body}"
        );
        return (stream, body);
    }
    panic!("model chat request did not follow the probes");
}

#[test]
fn standalone_radius_share_includes_live_metadata_without_changing_journal() {
    for checkpoint in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let prompt = root.path().join("system.txt");
        std::fs::write(&prompt, "Unique standalone share prompt").unwrap();
        if checkpoint {
            let initial = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
            let mut records = initial.records().to_vec();
            records.push(pi_rs::Record {
                timestamp: None,
                run_id: None,
                version: records[0].version,
                id: 2,
                parent: Some(1),
                event: pi_rs::Event::SystemCheckpoint {
                    content: "Checkpoint prompt".into(),
                    sections: Vec::new(),
                    tools_added: Vec::new(),
                    tools_removed: Vec::new(),
                },
            });
            let mut session = pi_rs::Session::from_records(records).unwrap();
            session
                .append_user_with_images("hello".into(), Vec::new(), None)
                .unwrap();
            session.export_jsonl(&path).unwrap();
        } else {
            let mut session = pi_rs::Session::open(&path).unwrap();
            session
                .append_user_with_images("hello".into(), Vec::new(), None)
                .unwrap();
        }
        let original = std::fs::read(&path).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/artifacts", listener.local_addr().unwrap());
        let (sender, receiver) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut length = 0;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
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
            let response = r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
            sender.send(body).unwrap();
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("RADIUS_API_KEY", "fixture")
            .env("PI_RS_RADIUS_SHARE_URL", endpoint)
            .env_remove("GEMINI_API_KEY")
            .env_remove("GOOGLE_API_KEY")
            .env_remove("PI_RS_MODEL_TOKEN")
            .args([
                "--offline",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--tools",
                "read",
                "--system-prompt",
            ])
            .arg(&prompt)
            .arg("--session")
            .arg(&path);
        command.args([
            "--no-trust",
            "--endpoint",
            "http://127.0.0.1:1/v1/chat/completions",
        ]);
        let mut child = command
            .arg("share")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("standalone share timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let body = receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        server.join().unwrap();
        let entries: Vec<Value> = body
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        let metadata = entries
            .iter()
            .find(|entry| entry["customType"] == "pi.share");
        let metadata = metadata.unwrap();
        assert_eq!(entries.last(), Some(metadata));
        assert_eq!(metadata["parentId"], entries[entries.len() - 2]["id"]);
        assert!(
            metadata["data"]["systemPrompt"]
                .as_str()
                .unwrap()
                .contains(if checkpoint {
                    "Checkpoint prompt"
                } else {
                    "Unique standalone share prompt"
                })
        );
        assert!(
            metadata["data"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "read" && tool["parameters"].is_object())
        );
        if checkpoint {
            assert_eq!(metadata["data"]["tools"].as_array().unwrap().len(), 1);
        }
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

#[test]
fn standalone_share_requires_successful_relevant_mcp_discovery() {
    for case in [
        "invalid-map",
        "invalid-server",
        "missing-command",
        "disabled-config",
        "disabled-extension",
    ] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir(&home).unwrap();
        let path = root.path().join("session.jsonl");
        drop(pi_rs::Session::open(&path).unwrap());
        let original = std::fs::read(&path).unwrap();
        let missing = root.path().join("missing-mcp-command");
        let server = match case {
            "invalid-map" => json!({"mcpServers": []}),
            "invalid-server" => json!({"mcpServers": {"fixture": {"command": 42}}}),
            "disabled-config" => {
                json!({"mcpServers": {"fixture": {"command": missing, "enabled": false}}})
            }
            _ => json!({"mcpServers": {"fixture": {"command": missing}}}),
        };
        std::fs::write(home.join("mcp.json"), serde_json::to_vec(&server).unwrap()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/v1/artifacts", listener.local_addr().unwrap());
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("RADIUS_API_KEY", "fixture")
            .env("PI_RS_RADIUS_SHARE_URL", endpoint)
            .args([
                "--offline",
                "--no-trust",
                "--no-skills",
                "--no-context-files",
                "--provider",
                "ollama",
                "--model",
                "fixture",
                "--endpoint",
                "http://127.0.0.1:9",
            ])
            .arg("--session")
            .arg(&path);
        if case == "disabled-extension" {
            command.args(["--disable-extension", "mcp-fixture"]);
        }
        let child = command
            .arg("share")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut child = KillOnDrop(child);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut uploaded = None;
        loop {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut reader = BufReader::new(socket.try_clone().unwrap());
                    let mut length = 0;
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
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
                    let response = r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#;
                    write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
                    uploaded = Some(body);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("Radius accept failed: {error}"),
            }
            if child.0.try_wait().unwrap().is_some() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "share timed out for {case}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = child.0.wait().unwrap();
        let mut stderr = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        let disabled = case.starts_with("disabled-");
        assert_eq!(status.success(), disabled, "{case}: {stderr}");
        if disabled {
            let body = uploaded.expect("disabled server should not block sharing");
            assert!(
                body.windows(b"pi.share".len())
                    .any(|part| part == b"pi.share")
            );
        } else {
            assert!(stderr.contains("MCP"), "{case}: {stderr}");
            assert!(uploaded.is_none(), "{case} uploaded an incomplete context");
        }
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

#[test]
fn standalone_radius_share_rejects_unavailable_context_before_connecting() {
    for pending_trust in [false, true] {
        let root = tempfile::tempdir().unwrap();
        if pending_trust {
            std::fs::create_dir_all(root.path().join(".agents/skills/example")).unwrap();
        }
        let path = root.path().join("session.jsonl");
        drop(pi_rs::Session::open(&path).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/v1/artifacts", listener.local_addr().unwrap());
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("RADIUS_API_KEY", "fixture")
            .env("PI_RS_RADIUS_SHARE_URL", endpoint)
            .env_remove("GEMINI_API_KEY")
            .env_remove("GOOGLE_API_KEY")
            .env_remove("PI_RS_MODEL_TOKEN")
            .args([
                "--offline",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
            ])
            .arg("--session")
            .arg(&path);
        if pending_trust {
            command.args(["--endpoint", "http://127.0.0.1:1/v1/chat/completions"]);
        } else {
            command.args([
                "--no-trust",
                "--provider",
                "google",
                "--model",
                "gemini-2.5-flash",
            ]);
        }
        let mut child = command
            .arg("share")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("standalone share did not finish");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success(), "unavailable context was shared");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let expected = if pending_trust {
            "project trust is pending"
        } else {
            "GEMINI_API_KEY"
        };
        assert!(stderr.contains(expected), "{stderr}");
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "Radius was contacted"
        );
    }
}

#[cfg(unix)]
#[test]
fn standalone_share_does_not_upload_after_extension_breaks_context_reconstruction() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("session.jsonl");
    let prompt = root.path().join("prompt.txt");
    let marker = root.path().join("hook-ran");
    let extension = root.path().join("break-resources.py");
    std::fs::write(&prompt, "original prompt").unwrap();
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys
r = json.load(sys.stdin)
if r['type'] == 'describe':
    response = {'Ok': {'id':'break-resources','dependencies':[],'commands':[],'tools':[],'resource_discovery_hook':True}}
elif r['type'] == 'discover_resources':
    with open(os.environ['SHARE_PROMPT'], 'w') as f: f.write('x' * (64 * 1024 + 1))
    with open(os.environ['SHARE_MARKER'], 'w') as f: f.write('ran')
    response = {'Ok': {'skillPaths':[],'promptPaths':[],'themePaths':[]}}
else:
    response = {'Ok': None}
print(json.dumps({'version':1,'response':response}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let session = pi_rs::Session::open(&path).unwrap();
    drop(session);
    let before = std::fs::read(&path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/artifacts", listener.local_addr().unwrap());
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .env("RADIUS_API_KEY", "fixture")
        .env("PI_RS_RADIUS_SHARE_URL", endpoint)
        .env("SHARE_PROMPT", &prompt)
        .env("SHARE_MARKER", &marker)
        .args([
            "--offline",
            "--no-mcp",
            "--no-trust",
            "--no-skills",
            "--no-context-files",
            "--endpoint",
            "http://127.0.0.1:1/v1/chat/completions",
            "--system-prompt",
        ])
        .arg(&prompt)
        .arg("--extension")
        .arg(&extension)
        .arg("--session")
        .arg(&path)
        .arg("share")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("standalone share did not finish");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        marker.exists(),
        "resource discovery hook did not run: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "failed reconstruction was shared");
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "Radius was contacted"
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
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
        let (mut stream, _request) = accept_model_request(listener);
        received.send(()).unwrap();
        released.recv_timeout(Duration::from_secs(20)).unwrap();
        let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
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
    let settled = receive(&rx, |event| {
        assert_ne!(event["type"], "error", "model request error: {event}");
        event["type"] == "agent_settled"
    });
    assert_eq!(settled["success"], true, "model request failed: {settled}");
    server.join().unwrap();
    writeln!(input, "{}", json!({"type":"shutdown"})).unwrap();
    input.flush().unwrap();
}

#[cfg(unix)]
#[test]
fn terminal_shares_idle_and_during_model_request() {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::PermissionsExt;
    use std::time::Instant;
    let root = tempfile::tempdir().unwrap();
    let prompt = root.path().join("system.txt");
    std::fs::write(&prompt, "Terminal share prompt").unwrap();
    let extension = root.path().join("context.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, sys
r = json.load(sys.stdin)
if r['type'] == 'describe':
    result = {'id':'share-context-fixture','dependencies':[],'commands':[],'tools':[],'context_with_system_hook':True}
elif r['type'] == 'context_with_system':
    result = r['messages']
    for message in result:
        if message['role'] == 'system': message['content'] += ' fixture-context-hook'
elif r['type'] == 'before_request':
    result = r['request']
else:
    result = None
print(json.dumps({'version':1,'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
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
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    size = value.trim().parse().unwrap();
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
        let (mut socket, request) = accept_model_request(model_listener);
        started_tx.send(request).unwrap();
        release_rx.recv_timeout(Duration::from_secs(20)).unwrap();
        let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
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
                std::ptr::addr_of_mut!(size),
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
        .arg("--extension")
        .arg(&extension)
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
    let receive_share = |master: &mut std::fs::File| {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut output = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            if let Ok(value) = share_rx.try_recv() {
                return value;
            }
            if let Ok(n) = master.read(&mut buffer) {
                output.extend_from_slice(&buffer[..n]);
            }
            assert!(
                Instant::now() < deadline,
                "share timed out: {}",
                String::from_utf8_lossy(&output)
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    };
    let idle = receive_share(&mut master);
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
    let request = started_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    assert!(
        request["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "system"
                && message["content"]
                    .as_str()
                    .is_some_and(|text| text.contains("fixture-context-hook")))
    );
    master.write_all(b"/share\r").unwrap();
    let active = receive_share(&mut master);
    assert_eq!(active.last().unwrap()["customType"], "pi.share");
    assert_eq!(active.last().unwrap()["data"], idle_context["data"]);
    release_tx.send(()).unwrap();
    wait_output(&mut master, "done");
    shares.join().unwrap();
    model.join().unwrap();
}

#[cfg(unix)]
#[test]
fn terminal_radius_share_succeeds_without_selected_model_credentials() {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::time::Instant;
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/artifacts", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut headers = String::new();
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.is_empty() || line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
            headers.push_str(&line.to_ascii_lowercase());
        }
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes).unwrap();
        let body = r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        tx.send((headers, bytes)).unwrap();
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
                std::ptr::addr_of_mut!(size),
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
        .env("RADIUS_API_KEY", "radius-fixture")
        .env("PI_RS_RADIUS_SHARE_URL", endpoint)
        .env_remove("GEMINI_API_KEY")
        .env_remove("PI_RS_MODEL_TOKEN")
        .args([
            "--offline",
            "--no-mcp",
            "--no-trust",
            "--no-extensions",
            "--no-skills",
            "--tui-mode",
            "regular",
            "--provider",
            "google",
            "--model",
            "gemini-2.5-flash",
        ])
        .arg("--session")
        .arg(root.path().join("session.jsonl"))
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave))
        .spawn()
        .unwrap();
    let _child = KillOnDrop(child);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        if let Ok(n) = master.read(&mut buffer) {
            output.extend_from_slice(&buffer[..n]);
        }
        if String::from_utf8_lossy(&output).contains("Enter to send") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminal startup: {}",
            String::from_utf8_lossy(&output)
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    master.write_all(b"/share\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let (headers, bytes) = loop {
        if let Ok(value) = rx.try_recv() {
            break value;
        }
        if let Ok(n) = master.read(&mut buffer) {
            output.extend_from_slice(&buffer[..n]);
        }
        assert!(
            Instant::now() < deadline,
            "share timed out: {}",
            String::from_utf8_lossy(&output)
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(headers.contains("authorization: bearer radius-fixture"));
    let entries: Vec<Value> = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert!(
        !entries
            .iter()
            .any(|entry| entry["customType"] == "pi.share")
    );
    server.join().unwrap();
}

#[test]
fn standalone_share_rejects_incomplete_journal_without_repair_or_upload() {
    for incomplete in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = if incomplete {
            root.path().join("session.jsonl")
        } else {
            root.path().join("missing/session.jsonl")
        };
        let original = if incomplete {
            drop(pi_rs::Session::open(&path).unwrap());
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"{\"incomplete\":")
                .unwrap();
            Some(std::fs::read(&path).unwrap())
        } else {
            None
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/v1/artifacts", listener.local_addr().unwrap());
        let child = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("RADIUS_API_KEY", "fixture")
            .env("PI_RS_RADIUS_SHARE_URL", endpoint)
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--provider",
                "ollama",
                "--model",
                "fixture",
                "--endpoint",
                "http://127.0.0.1:9",
            ])
            .arg("--session")
            .arg(&path)
            .arg("share")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut child = KillOnDrop(child);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while child.0.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                let _ = child.0.kill();
                let _ = child.0.wait();
                panic!("standalone share did not finish");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = child.0.wait().unwrap();
        let mut stderr = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(!status.success(), "incomplete journal was shared");
        if let Some(original) = original {
            assert!(stderr.contains("journal is incomplete"));
            assert_eq!(std::fs::read(&path).unwrap(), original);
        } else {
            assert!(
                !path.parent().unwrap().exists(),
                "missing session directory was created"
            );
        }
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "Radius was contacted"
        );
    }
}

#[cfg(unix)]
#[test]
fn standalone_share_interrupt_reaps_preparation_process_groups() {
    use std::os::unix::fs::PermissionsExt;

    for source in ["mcp", "mcp-credential", "extension", "package"] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir(&home).unwrap();
        let path = root.path().join("session.jsonl");
        drop(pi_rs::Session::open(&path).unwrap());
        let before = std::fs::read(&path).unwrap();
        let upload = TcpListener::bind("127.0.0.1:0").unwrap();
        upload.set_nonblocking(true).unwrap();
        let fixture = TcpListener::bind("127.0.0.1:0").unwrap();
        fixture.set_nonblocking(true).unwrap();
        let program = root.path().join("hold-preparation");
        std::fs::write(
            &program,
            r#"#!/usr/bin/env python3
import os, socket, subprocess, time
host, port = os.environ['PI_SHARE_FIXTURE'].split(':')
peer = socket.create_connection((host, int(port)))
subprocess.Popen(['sleep', '120'], pass_fds=(peer.fileno(),))
peer.sendall(f'{os.getpid()}\n'.encode())
time.sleep(120)
"#,
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();

        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env(
                "PI_SHARE_FIXTURE",
                fixture.local_addr().unwrap().to_string(),
            )
            .env("RADIUS_API_KEY", "fixture")
            .env(
                "PI_RS_RADIUS_SHARE_URL",
                format!("http://{}/v1/artifacts", upload.local_addr().unwrap()),
            )
            .args([
                "--offline",
                "--no-trust",
                "--provider",
                "ollama",
                "--model",
                "fixture",
            ])
            .arg("--session")
            .arg(&path);
        match source {
            "mcp" => {
                let config = json!({"mcpServers": {"fixture": {"command":program}}});
                std::fs::write(home.join("mcp.json"), config.to_string()).unwrap();
            }
            "mcp-credential" => {
                command.env("PI_SHARE_HOLD_PROGRAM", &program);
                let config = json!({
                    "mcpServers": {"fixture": {
                        "command": "unused-after-credential-cancel",
                        "env": {"TOKEN": r#"!exec "$PI_SHARE_HOLD_PROGRAM""#}
                    }}
                });
                std::fs::write(home.join("mcp.json"), config.to_string()).unwrap();
            }
            "extension" => {
                command.arg("--no-mcp").arg("--extension").arg(&program);
            }
            "package" => {
                let project = root.path().join("fixture-package");
                std::fs::create_dir(&project).unwrap();
                std::fs::write(
                    project.join("Cargo.toml"),
                    "[package]\nname='fixture-package'\nversion='0.1.0'\n",
                )
                .unwrap();
                std::fs::rename(program, root.path().join("cargo")).unwrap();
                let process_path = std::iter::once(root.path().to_path_buf())
                    .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap()))
                    .collect::<Vec<_>>();
                command.env("PATH", std::env::join_paths(process_path).unwrap());
                command.arg("--no-mcp").arg("--extension").arg(project);
            }
            _ => unreachable!(),
        }
        let mut child = KillOnDrop(
            command
                .arg("share")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut peer = loop {
            match fixture.accept() {
                Ok((peer, _)) => break BufReader::new(peer),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < deadline =>
                {
                    if let Some(status) = child.0.try_wait().unwrap() {
                        let mut stderr = String::new();
                        child
                            .0
                            .stderr
                            .take()
                            .unwrap()
                            .read_to_string(&mut stderr)
                            .unwrap();
                        panic!("{source}: pi exited early ({status}): {stderr}");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("{source}: subprocess did not start: {error}"),
            }
        };
        peer.get_ref().set_nonblocking(false).unwrap();
        peer.get_ref()
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut pid = String::new();
        peer.read_line(&mut pid).unwrap();
        let pid: libc::pid_t = pid.trim().parse().unwrap();

        assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGINT) }, 0);
        let mut byte = [0];
        assert_eq!(
            peer.read(&mut byte).unwrap(),
            0,
            "{source}: inherited socket is still held"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{source}: interrupted share did not exit"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        let mut stderr = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert_eq!(status.code(), Some(130), "{source}: {stderr}");
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "{source}: subprocess was not reaped"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "{source}"
        );
        assert_eq!(std::fs::read(path).unwrap(), before);
        assert!(
            matches!(upload.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "{source}: interrupted share contacted Radius"
        );
    }
}
