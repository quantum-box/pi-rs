#[cfg(unix)]
#[test]
fn malformed_project_override_never_starts_disabled_server() {
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(workspace.join(".pi")).unwrap();
    let script = root.path().join("server.sh");
    let sentinel = root.path().join("executed");
    std::fs::write(
        &script,
        format!("#!/bin/sh\ntouch '{}'\n", sentinel.display()),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        home.join("mcp.json"),
        serde_json::json!({"mcpServers":{"fixture":{"command":script,"enabled":false}}})
            .to_string(),
    )
    .unwrap();
    let project = workspace.join(".pi/mcp.json");
    let invalid = r#"{"mcpServers":{"fixture":{"enabled":true,"commmand":"unexpected"}}}"#;
    std::fs::write(&project, invalid).unwrap();
    let run = |trust: &str| {
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(&workspace)
            .env("PI_RS_HOME", &home)
            .args(["--offline", trust, "mcp", "list"])
            .output()
            .unwrap()
    };
    let trusted = run("--trust");
    assert!(!trusted.status.success());
    assert!(String::from_utf8_lossy(&trusted.stderr).contains("commmand"));
    assert!(!sentinel.exists());
    assert_eq!(std::fs::read_to_string(&project).unwrap(), invalid);
    let untrusted = run("--no-trust");
    assert!(
        untrusted.status.success(),
        "{}",
        String::from_utf8_lossy(&untrusted.stderr)
    );
    assert!(!sentinel.exists());
}
