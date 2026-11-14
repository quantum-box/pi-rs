use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

#[test]
fn rpc_replaces_invalid_utf8_and_continues_after_parse_errors() {
    let root = tempfile::tempdir().unwrap();
    // Files avoid filling a pipe while waiting for the process to exit.
    let stdout_path = root.path().join("stdout");
    let stderr_path = root.path().join("stderr");
    let stdout = std::fs::File::create(&stdout_path).unwrap();
    let stderr = std::fs::File::create(&stderr_path).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .args([
            "--offline",
            "--no-session",
            "--no-mcp",
            "--no-trust",
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input
        .write_all(b"{\"type\":\"get_state\",\"id\":\"bad\xffid\"}\r\n")
        .unwrap();
    input.write_all(b"\xff\n").unwrap();
    input
        .write_all("{\"type\":\"get_state\",\"id\":\"a\u{2028}b\"}\n".as_bytes())
        .unwrap();
    input
        .write_all(b"{\"type\":\"get_state\",\"id\":\"last\"}\r")
        .unwrap();
    drop(input);
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("RPC process did not exit after stdin closed");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "{}",
        String::from_utf8_lossy(&std::fs::read(stderr_path).unwrap())
    );
    let responses: Vec<Value> = String::from_utf8(std::fs::read(stdout_path).unwrap())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(responses.len(), 4, "{responses:?}");
    assert_eq!(responses[0]["id"], "bad�id");
    assert_eq!(responses[0]["success"], true);
    assert_eq!(responses[1]["command"], "parse");
    assert_eq!(responses[1]["success"], false);
    assert_eq!(responses[2]["id"], "a\u{2028}b");
    assert_eq!(responses[2]["success"], true);
    assert_eq!(responses[3]["id"], "last");
    assert_eq!(responses[3]["success"], true);
}
