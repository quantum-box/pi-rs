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
