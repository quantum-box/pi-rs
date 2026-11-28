use std::process::Command;

#[test]
fn explicit_agent_dir_selects_profile_before_listing_and_subcommands() {
    let root = tempfile::tempdir().unwrap();
    for (directory, model) in [
        ("default", "default-only"),
        ("first", "first-only"),
        ("other", "profile-only"),
        ("profile space", "url-only"),
    ] {
        let directory = root.path().join(directory);
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(
            directory.join("models.json"),
            serde_json::json!({"providers":{"fixture":{
                "api":"openai-completions", "baseUrl":"http://127.0.0.1:9/v1",
                "apiKey":"fixture-key", "models":[{"id":model}]
            }}})
            .to_string(),
        )
        .unwrap();
    }
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("HOME", root.path())
            .env("USERPROFILE", root.path())
            .env("PI_RS_HOME", root.path().join("default"))
            .env("PI_CODING_AGENT_DIR", root.path().join("first"))
            .args(args)
            .output()
            .unwrap()
    };
    let output = run(&["--offline", "--list-models"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("default-only"));
    assert!(!stdout.contains("first-only"));
    let delimited = run(&["--offline", "--list-models", "--", "--agent-dir", "other"]);
    assert!(
        delimited.status.success(),
        "{}",
        String::from_utf8_lossy(&delimited.stderr)
    );
    let stdout = String::from_utf8_lossy(&delimited.stdout);
    assert!(stdout.contains("default-only"));
    assert!(!stdout.contains("profile-only"));
    for args in [
        vec!["--offline", "--list-models", "--agent-dir", "other"],
        vec!["--offline", "--list-models", "--agent-dir", "~//other"],
        vec![
            "--agent-dir",
            "first",
            "--offline",
            "--list-models",
            "--agent-dir",
            "other/../other",
        ],
    ] {
        let output = run(&args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("profile-only"), "{stdout}");
        assert!(!stdout.contains("default-only") && !stdout.contains("first-only"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("Unknown option"));
    }
    let file_url = reqwest::Url::from_directory_path(root.path().join("profile space"))
        .unwrap()
        .to_string();
    let output = run(&["--offline", "--list-models", "--agent-dir", &file_url]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("url-only"));
    let invalid = run(&["--agent-dir", "file:///bad%2Fpath", "--list-models"]);
    assert!(!invalid.status.success());
    assert!(!String::from_utf8_lossy(&invalid.stdout).contains("default-only"));
    let output = run(&[
        "--offline",
        "--provider",
        "fixture",
        "auth",
        "check",
        "--json",
        "--no-refresh",
        "--agent-dir",
        "other",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("fixture-key"));
    let missing = run(&["--agent-dir"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--agent-dir requires a directory"));
    let attached = run(&["--agent-dir=other", "--list-models"]);
    assert!(!attached.status.success());
    assert!(attached.stdout.is_empty());
}
