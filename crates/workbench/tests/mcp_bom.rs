use std::process::Command;

use serde_json::Value;

#[test]
fn mcp_config_with_utf8_bom_loads_and_can_be_edited() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(workspace.join(".pi")).unwrap();
    let global = home.join("mcp.json");
    let project = workspace.join(".pi/mcp.json");
    std::fs::write(&global, "\u{feff}{\"mcpServers\":{\"global\":{\"command\":\"unused\",\"enabled\":false}},\"custom\":\"keep\"}").unwrap();
    std::fs::write(
        &project,
        "\u{feff}{\"mcpServers\":{\"project\":{\"command\":\"unused\",\"enabled\":false}}}",
    )
    .unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(&workspace)
            .env("PI_RS_HOME", &home)
            .args(["--offline", "--trust", "mcp"])
            .args(args)
            .output()
            .unwrap()
    };
    let list = run(&["list"]);
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let output = String::from_utf8_lossy(&list.stdout);
    assert!(
        output.contains("global") && output.contains("project"),
        "{output}"
    );
    for args in [
        vec!["add", "extra", "--url", "https://example.invalid/mcp"],
        vec![
            "add",
            "project-extra",
            "--local",
            "--url",
            "https://example.invalid/mcp",
        ],
    ] {
        let output = run(&args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let global_json: Value = serde_json::from_slice(&std::fs::read(global).unwrap()).unwrap();
    let project_json: Value = serde_json::from_slice(&std::fs::read(project).unwrap()).unwrap();
    assert_eq!(global_json["custom"], "keep");
    assert_eq!(global_json["mcpServers"]["global"]["enabled"], false);
    assert_eq!(
        global_json["mcpServers"]["extra"]["url"],
        "https://example.invalid/mcp"
    );
    assert_eq!(project_json["mcpServers"]["project"]["enabled"], false);
    assert_eq!(
        project_json["mcpServers"]["project-extra"]["url"],
        "https://example.invalid/mcp"
    );
}

#[test]
fn malformed_bom_config_is_not_overwritten() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("mcp.json");
    let invalid = "\u{feff}{\"mcpServers\":";
    std::fs::write(&path, invalid).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path())
        .args([
            "--offline",
            "--no-trust",
            "mcp",
            "add",
            "new",
            "--url",
            "https://example.invalid/mcp",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(std::fs::read_to_string(path).unwrap(), invalid);
}
