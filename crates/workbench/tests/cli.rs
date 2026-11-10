use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

use pi_rs::{
    Agent, CancellationToken, Limits, MemoryStore, Notification, Registry, Session, ollama::Ollama,
};
use serde_json::Value;

#[test]
fn rpc_correlates_commands_and_rejects_unknown_commands() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .args(["--mode", "rpc", "--no-session"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for request in [
        r#"{"id":"state","type":"get_state"}"#,
        r#"{"id":"new","type":"new_session"}"#,
        r#"{"id":"unknown","type":"unsupported"}"#,
        r#"{"id":"stop","type":"shutdown"}"#,
    ] {
        writeln!(stdin, "{request}").unwrap();
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let replies: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(replies.len(), 4);
    assert_eq!(replies[0]["id"], "state");
    assert_eq!(replies[0]["data"]["isStreaming"], false);
    assert_eq!(replies[1]["success"], true);
    assert_eq!(replies[2]["id"], "unknown");
    assert_eq!(replies[2]["success"], false);
    assert_eq!(replies[3]["id"], "stop");
}

#[test]
fn native_notes_extension_is_saved_and_restored() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.jsonl");
    for (command, text) in [("set", "Review parser"), ("append", "Test Unicode")] {
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .arg("--session")
            .arg(&path)
            .args(["--extension", "notes", "extension", "notes", command, text])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let session = Session::open(path).unwrap();
    assert_eq!(
        session.extension_states()["notes"].data["text"],
        "Review parser\nTest Unicode"
    );
}

#[test]
fn print_and_json_modes_accept_positional_prompts() {
    let (endpoint, server) = model_fixture();
    for mode in ["--print", "--mode"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command.args(["--no-session", "--endpoint", &endpoint, mode]);
        if mode == "--mode" {
            command.arg("json");
        }
        let output = command.arg("same input").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        if mode == "--print" {
            assert_eq!(stdout, "fixture answer\n");
        } else {
            let records: Vec<Value> = stdout
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert!(
                records
                    .iter()
                    .any(|record| record["type"] == "answer"
                        && record["content"] == "fixture answer")
            );
        }
    }
    server.join().unwrap();
}

#[test]
fn continue_reopens_the_latest_saved_session() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path())
        .args([
            "--extension",
            "notes",
            "extension",
            "notes",
            "set",
            "remember this",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path())
        .args(["--continue", "--trace", "inspect"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("remember this")
    );
}

fn model_fixture() -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_effort(None)
}

fn model_fixture_with_effort(
    effort: Option<&'static str>,
) -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_finish(effort, "stop")
}

fn model_fixture_with_finish(
    effort: Option<&'static str>,
    finish: &'static str,
) -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_config(effort, finish, None)
}

fn model_fixture_with_config(
    effort: Option<&'static str>,
    finish: &'static str,
    expected_token: Option<&'static str>,
) -> (String, std::thread::JoinHandle<()>) {
    model_fixture_with_gate(effort, finish, expected_token, None)
}

fn model_fixture_with_gate(
    effort: Option<&'static str>,
    finish: &'static str,
    expected_token: Option<&'static str>,
    mut release_first: Option<std::sync::mpsc::Receiver<()>>,
) -> (String, std::thread::JoinHandle<()>) {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        server.local_addr().unwrap()
    );
    server.set_nonblocking(true).unwrap();
    let handle = std::thread::spawn(move || {
        let mut completed = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        while completed < 2 {
            let (mut socket, _) = match server.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "received {completed} of 2 model requests"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("model fixture accept failed: {error}"),
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&socket);
            let mut content_length = 0;
            let mut authorization = None;
            let mut metadata_request = false;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line.is_empty() || line == "\r\n" {
                    break;
                }
                metadata_request |= line.starts_with("GET /api/ps ");
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
                if line.to_ascii_lowercase().starts_with("authorization:") {
                    authorization = Some(line.split_once(':').unwrap().1.trim().to_owned());
                }
            }
            if let Some(token) = expected_token {
                assert_eq!(authorization, Some(format!("Bearer {token}")));
            }
            if metadata_request {
                write!(
                    socket,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                continue;
            }
            completed += 1;
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            if let Some(effort) = effort {
                assert_eq!(request["reasoning_effort"], effort);
            }
            assert!(
                request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|message| message["content"] == "same input")
            );
            let response = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"private-reasoning-sentinel\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"fixture-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"fixture-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":19,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":7}}}\n\n",
                "data: [DONE]\n\n",
            ).replace("\"finish_reason\":\"stop\"", &format!("\"finish_reason\":\"{finish}\""));
            if let Some(release) = release_first.take() {
                release
                    .recv_timeout(Duration::from_secs(15))
                    .expect("release first response");
            }
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{response}", response.len()).unwrap();
        }
    });
    (endpoint, handle)
}

#[test]
fn token_limit_preserves_partial_text_and_its_finish_reason() {
    let (endpoint, server) = model_fixture_with_finish(None, "length");
    let directory = tempfile::tempdir().unwrap();
    for index in 0..2 {
        let path = directory.path().join(format!("{index}.jsonl"));
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .env("PI_RS_HOME", directory.path().join("home"))
            .args([
                "--endpoint",
                &endpoint,
                "--print",
                "--no-tools",
                "--session",
            ])
            .arg(&path)
            .arg("same input")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "fixture answer\n"
        );
        let session = Session::open(path).unwrap();
        assert!(session.records().iter().any(|record| matches!(&record.event, pi_rs::Event::ModelFinished { metadata, .. } if metadata.finish_reason.as_deref() == Some("length"))));
    }
    server.join().unwrap();
}

#[test]
fn thinking_override_reaches_provider_and_restores_from_session() {
    let (endpoint, server) = model_fixture_with_effort(Some("none"));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.jsonl");
    for first in [true, false] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .env("PI_RS_HOME", directory.path().join("home"))
            .arg("--session")
            .arg(&path)
            .args(["--endpoint", &endpoint]);
        if first {
            command.args(["--thinking", "off"]);
        }
        let output = command.args(["run", "same input"]).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        Session::open(path)
            .unwrap()
            .model_selection()
            .unwrap()
            .1
            .as_deref(),
        Some("off")
    );
    server.join().unwrap();
}

#[tokio::test]
async fn cli_and_library_use_the_same_stream_and_do_not_emit_auth() {
    let (endpoint, server) = model_fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    let output = Command::new(env!("CARGO_BIN_EXE_pi-rs"))
        .arg("--session")
        .arg(&path)
        .arg("--workspace")
        .arg(dir.path())
        .args(["--endpoint", &endpoint, "run", "same input"])
        .env("PI_RS_MODEL_TOKEN", "private-test-token")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("private-test-token"));
    assert!(!stdout.contains("private-reasoning-sentinel"));
    assert!(
        !std::fs::read_to_string(path)
            .unwrap()
            .contains("private-test-token")
    );
    let events: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let stages: Vec<&str> = events
        .iter()
        .filter(|event| event["type"] == "model_progress")
        .filter_map(|event| event["stage"].as_str())
        .collect();
    assert_eq!(stages, ["waiting_for_model", "thinking", "generating"]);
    let answer = events
        .iter()
        .find(|event| event["type"] == "answer")
        .unwrap()["content"]
        .as_str()
        .unwrap();
    let metadata = events
        .iter()
        .find(|event| event["event"] == "model_finished")
        .unwrap();
    assert_eq!(metadata["metadata"]["cached_input_tokens"], 7);
    assert_eq!(metadata["usage"]["kind"], "measured");

    let model = Ollama::new(&endpoint, None).unwrap();
    let registry = Registry::default();
    let agent = Agent {
        model: &model,
        model_name: "qwen2.5:7b",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    let result = agent
        .run(
            &mut session,
            Some("same input".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: Notification<'_>| {},
        )
        .await
        .unwrap();
    assert_eq!(answer, result);
    server.join().unwrap();
}

#[test]
fn plan_mode_changes_are_branch_local_and_can_be_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    let run = |arguments: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi-rs"))
            .arg("--session")
            .arg(&path)
            .arg("--workspace")
            .arg(dir.path())
            .args(arguments)
            .output()
            .unwrap()
    };
    assert!(
        run(&["extension", "plan-mode", "start", "review before editing"])
            .status
            .success()
    );
    let active = Session::open(&path).unwrap().head();
    assert!(run(&["extension", "plan-mode", "accept"]).status.success());
    assert_eq!(
        Session::open(&path).unwrap().extension_states()["plan-mode"].data["mode"],
        "executing"
    );
    assert!(run(&["checkout", &active.to_string()]).status.success());
    assert_eq!(
        Session::open(&path).unwrap().extension_states()["plan-mode"].data["mode"],
        "planning"
    );
    assert!(run(&["extension", "plan-mode", "discard"]).status.success());
    assert_eq!(
        Session::open(&path).unwrap().extension_states()["plan-mode"].data["mode"],
        "normal"
    );
}

#[test]
fn persistent_worker_accepts_multiple_prompts_and_releases_the_journal() {
    let (endpoint, server) = model_fixture();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("persistent.jsonl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .arg("--session")
        .arg(&path)
        .args(["--endpoint", &endpoint, "--trace", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if let Ok(value) = serde_json::from_str::<Value>(&line) {
                let _ = sender.send(value);
            }
        }
    });
    for _ in 0..2 {
        writeln!(
            input,
            "{}",
            serde_json::json!({"type":"prompt","content":"same input"})
        )
        .unwrap();
        input.flush().unwrap();
        let mut events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let event = receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|error| {
                    panic!("worker did not settle: {error}; events: {events:?}")
                });
            let settled = event["type"] == "agent_settled";
            events.push(event);
            if settled {
                assert_eq!(events.last().unwrap()["success"], true, "{events:?}");
                break;
            }
        }
        assert!(
            events.iter().any(|event| event["type"] == "answer"),
            "{events:?}"
        );
        assert!(child.try_wait().unwrap().is_none());
        drop(Session::open(&path).unwrap());
    }
    writeln!(input, "{}", serde_json::json!({"type":"shutdown"})).unwrap();
    drop(input);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("persistent worker did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(child.wait().unwrap().success());
    reader.join().unwrap();
    server.join().unwrap();
}

#[test]
fn api_key_override_reaches_direct_and_rpc_requests_without_persistence() {
    let token = "override-test-secret";
    let (endpoint, server) = model_fixture_with_config(None, "stop", Some(token));
    let directory = tempfile::tempdir().unwrap();
    let session = directory.path().join("direct.jsonl");
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .env("PI_RS_MODEL_TOKEN", "unused-environment-token")
        .arg("--session")
        .arg(&session)
        .args([
            "--endpoint",
            &endpoint,
            "--api-key",
            token,
            "--print",
            "same input",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    assert!(!std::fs::read_to_string(session).unwrap().contains(token));
    let mut rpc = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .env("PI_RS_MODEL_TOKEN", "unused-environment-token")
        .args([
            "--no-session",
            "--endpoint",
            &endpoint,
            "--api-key",
            token,
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = rpc.stdin.take().unwrap();
    writeln!(input, "{{\"type\":\"prompt\",\"message\":\"same input\"}}").unwrap();
    input.flush().unwrap();
    let stdout = rpc.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            assert!(!line.contains(token));
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut events = Vec::new();
    loop {
        let event = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = rpc.kill();
                let _ = rpc.wait();
                panic!("RPC did not settle: {error}; events: {events:?}");
            });
        let settled = event["type"] == "agent_settled";
        events.push(event);
        if settled {
            let end = events
                .iter()
                .find(|event| event["type"] == "agent_end")
                .expect("agent end precedes settlement");
            assert_eq!(end["success"], true, "{events:?}");
            assert_eq!(end["messages"][0]["role"], "user");
            assert_eq!(end["messages"][1]["role"], "assistant");
            let update = events
                .iter()
                .find(|event| event["assistantMessageEvent"]["type"] == "text_delta")
                .expect("streamed text update");
            assert_eq!(update["assistantMessageEvent"]["contentIndex"], 1);
            assert_eq!(update["message"]["content"][0]["type"], "thinking");
            assert_eq!(update["message"]["content"][1]["text"], "fixture answer");
            assert_eq!(
                update["message"],
                update["assistantMessageEvent"]["partial"]
            );
            let entries: Vec<_> = events
                .iter()
                .filter(|event| event["type"] == "entry_appended")
                .map(|event| &event["entry"])
                .collect();
            let user = entries
                .iter()
                .position(|entry| entry["message"]["role"] == "user")
                .expect("user entry appended");
            let assistant = entries
                .iter()
                .position(|entry| entry["message"]["role"] == "assistant")
                .expect("assistant entry appended");
            assert!(assistant > user);
            for pair in entries[user..=assistant].windows(2) {
                assert_eq!(pair[1]["parentId"], pair[0]["id"]);
            }
            assert_eq!(events.last().unwrap()["aborted"], false);
            break;
        }
    }
    writeln!(
        input,
        "{{\"type\":\"get_messages\",\"id\":\"messages\",\"includeNative\":true}}"
    )
    .unwrap();
    input.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let messages = loop {
        let event = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| {
                let _ = rpc.kill();
                let _ = rpc.wait();
                panic!("RPC transcript did not arrive: {error}");
            });
        if event["type"] == "response" && event["id"] == "messages" {
            break event["data"].clone();
        }
    };
    assert_eq!(
        messages["messages"][1]["content"][1]["text"],
        "fixture answer"
    );
    assert_eq!(messages["messages"][1]["usage"]["cacheRead"], 7);
    assert_eq!(messages["nativeMessages"][1]["content"], "fixture answer");
    writeln!(input, "{{\"type\":\"shutdown\"}}").unwrap();
    input.flush().unwrap();
    drop(input);
    let output = rpc.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    server.join().unwrap();
}

#[test]
fn rpc_controls_apply_while_idle_and_streaming() {
    let root = tempfile::tempdir().unwrap();
    let journal = root.path().join("session.jsonl");
    let (release, gate) = std::sync::mpsc::channel();
    let (endpoint, fixture) = model_fixture_with_gate(None, "stop", None, Some(gate));
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path().join("home"))
        .args(["--workspace"])
        .arg(root.path())
        .args(["--session"])
        .arg(&journal)
        .args([
            "--no-trust",
            "--no-mcp",
            "--endpoint",
            &endpoint,
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let (events, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if events
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let mut send = |request: Value| {
        writeln!(input, "{request}").unwrap();
        input.flush().unwrap();
    };
    let mut until = |matches: &dyn Fn(&Value) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let event = receive
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|error| {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("RPC event timed out: {error}");
                });
            if matches(&event) {
                break event;
            }
        }
    };
    send(serde_json::json!({"type":"steer","id":"idle-steer","message":"queued while idle"}));
    let updated = until(&|event| event["type"] == "queue_update" || event["id"] == "idle-steer");
    assert_eq!(
        updated["steering"],
        serde_json::json!(["queued while idle"])
    );
    let queued = until(&|event| event["type"] == "response" && event["id"] == "idle-steer");
    assert_eq!(queued["data"]["disposition"], "queued");
    send(serde_json::json!({"type":"get_state","id":"idle-state"}));
    let state = until(&|event| event["id"] == "idle-state");
    assert_eq!(state["data"]["pendingMessageCount"], 1);
    assert_eq!(state["data"]["isStreaming"], false);
    send(serde_json::json!({"type":"clear_queue","id":"idle-clear"}));
    let cleared = until(&|event| event["id"] == "idle-clear");
    assert_eq!(
        cleared["data"]["steering"],
        serde_json::json!(["queued while idle"])
    );
    send(serde_json::json!({"type":"prompt","message":"same input"}));
    until(&|event| event["record"]["event"]["type"] == "model_requested");
    send(serde_json::json!({"type":"bash","id":"shell","command":"echo parallel-shell"}));
    let shell = until(&|event| event["type"] == "response" && event["id"] == "shell");
    // Windows prevents reading a journal locked by the running agent.
    send(serde_json::json!({"type":"get_entries","id":"before-settlement","includeNative":true}));
    let before_settlement =
        until(&|event| event["type"] == "response" && event["id"] == "before-settlement");
    release.send(()).unwrap();
    until(&|event| event["type"] == "agent_settled");
    let after_settlement = std::fs::read_to_string(&journal).unwrap();
    send(serde_json::json!({"type":"prompt","message":"same input"}));
    until(&|event| event["type"] == "agent_settled");
    send(serde_json::json!({"type":"shutdown"}));
    drop(input);
    let exit = child.wait_with_output().unwrap();
    reader.join().unwrap();
    fixture.join().unwrap();
    assert!(
        exit.status.success(),
        "{}",
        String::from_utf8_lossy(&exit.stderr)
    );
    assert_eq!(shell["success"], true, "{shell:?}");
    assert_eq!(
        shell["data"]["output"].as_str().unwrap().trim(),
        "parallel-shell"
    );
    assert_eq!(before_settlement["success"], true);
    assert!(
        !before_settlement["data"]["nativeEntries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["event"]["type"] == "bash_execution")
    );
    assert!(after_settlement.contains("parallel-shell"));
}

#[test]
fn export_flag_preserves_source_and_refuses_overwrite() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("session.jsonl");
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    session
        .append_user_with_images("export <marker>".into(), Vec::new(), None)
        .unwrap();
    session.export_jsonl(&input).unwrap();
    let before = std::fs::read(&input).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .arg("--export")
        .arg(&input)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        std::fs::read_to_string(input.with_extension("html"))
            .unwrap()
            .contains("export &lt;marker&gt;")
    );
    let again = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", directory.path().join("home"))
        .arg("--export")
        .arg(&input)
        .output()
        .unwrap();
    assert!(!again.status.success());
    assert_eq!(std::fs::read(input).unwrap(), before);
}
