use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

#[test]
fn rpc_replaces_invalid_utf8_and_continues_after_parse_errors() {
    let root = tempfile::tempdir().unwrap();
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
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let responses: Vec<Value> = String::from_utf8(output.stdout)
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
