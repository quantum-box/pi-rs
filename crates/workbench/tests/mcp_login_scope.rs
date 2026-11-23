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
    std::fs::remove_file(project.join("mcp.json")).unwrap();
    let output = run(&["--trust", "mcp", "login", "service"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("uses credentials from provider"));
    let output = run(&["--trust", "mcp", "login", "service", "--local"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("not configured in the trusted project")
    );
}

#[test]
fn login_respects_a_trusted_project_oauth_disable() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&project).unwrap();
    let global_contents =
        json!({"mcpServers": {"fixture": {"url": "https://example.invalid/mcp", "oauth": true}}})
            .to_string();
    let project_contents = json!({"mcpServers": {"fixture": {"oauth": false}}}).to_string();
    std::fs::write(home.join("mcp.json"), &global_contents).unwrap();
    std::fs::write(project.join("mcp.json"), &project_contents).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--trust", "mcp", "login", "fixture"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("MCP OAuth is disabled"));
    assert_eq!(
        std::fs::read_to_string(home.join("mcp.json")).unwrap(),
        global_contents
    );
    assert_eq!(
        std::fs::read_to_string(project.join("mcp.json")).unwrap(),
        project_contents
    );
    assert!(!home.join("mcp-auth").exists());
}
