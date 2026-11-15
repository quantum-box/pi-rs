use serde_json::json;
use std::process::Command;

#[test]
fn login_selects_trusted_project_definition_and_restricts_local() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(home.join("mcp.json"), json!({"mcpServers":{"service":{"url":"https://global.example/mcp","auth":{"provider":"fixture"}}}}).to_string()).unwrap();
    std::fs::write(project.join("mcp.json"), json!({"mcpServers":{"service":{"url":"https://project.example/mcp","headers":{"Authorization":"Bearer fixture"}}}}).to_string()).unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .args(args)
            .output()
            .unwrap()
    };
    for args in [
        vec!["--trust", "mcp", "login", "service"],
        vec!["--trust", "mcp", "login", "service", "--local"],
    ] {
        let output = run(&args);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("remove the MCP Authorization header")
        );
    }
    let output = run(&["--no-trust", "mcp", "login", "service"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("uses credentials from provider"));
    let output = run(&["--no-trust", "mcp", "login", "service", "--local"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("not configured in the trusted project")
    );
    std::fs::write(
        project.join("mcp.json"),
        json!({"mcpServers":{"service":{"enabled":false}}}).to_string(),
    )
    .unwrap();
    let output = run(&["--trust", "mcp", "login", "service", "--local"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("not configured in the trusted project")
    );
}

#[test]
fn list_waits_for_login_configuration_finalization() {
    let root = tempfile::tempdir().unwrap();
    let gate = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.path().join("mcp-config-gate.lock"))
        .unwrap();
    gate.lock().unwrap();
    let config = root.path().join("mcp.json");
    std::fs::write(
        &config,
        json!({"mcpServers":{"service":{"url":"https://example.invalid/mcp","enabled":true}}})
            .to_string(),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .args(["mcp", "list", "--json"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(
        child.try_wait().unwrap().is_none(),
        "list did not wait for finalization"
    );
    std::fs::write(
        &config,
        json!({"mcpServers":{"service":{"url":"https://example.invalid/mcp","enabled":false}}})
            .to_string(),
    )
    .unwrap();
    gate.unlock().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("list did not finish after finalization");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["servers"][0]["state"], "disabled");
}

#[test]
fn login_deadline_includes_wait_for_initial_configuration_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let gate = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.path().join("mcp-config-gate.lock"))
        .unwrap();
    gate.lock().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", root.path())
        .args(["mcp", "login", "service", "--timeout", "0.1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("login did not time out waiting for its initial snapshot");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("MCP login timed out"));
}
