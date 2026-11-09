use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::time::Duration;

use pi_rs::{
    Agent, CancellationToken, Limits, MemoryStore, Notification, Registry, Session, ollama::Ollama,
};
use serde_json::Value;

fn model_fixture() -> (String, std::thread::JoinHandle<()>) {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        server.local_addr().unwrap()
    );
    let handle = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut socket, _) = server.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&socket);
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line.is_empty() || line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert!(
                request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|message| message["content"] == "same input")
            );
            let response = concat!(
                "data: {\"id\":\"fixture-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"fixture-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":19,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":7}}}\n\n",
                "data: [DONE]\n\n",
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{response}", response.len()).unwrap();
        }
    });
    (endpoint, handle)
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
    assert!(
        !std::fs::read_to_string(path)
            .unwrap()
            .contains("private-test-token")
    );
    let events: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
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
