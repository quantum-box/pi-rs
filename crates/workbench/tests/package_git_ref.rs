//! Git ref disambiguation and legacy package identity with a local repository.
use std::path::Path;
use std::process::Command;

fn git(directory: &Path, args: &[&str]) -> String {
    let result = Command::new("git")
        .current_dir(directory)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
        ])
        .args(args)
        .output()
        .expect("run git");
    assert!(
        result.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

#[test]
fn installs_explicit_ref_and_preserves_legacy_at_sign_repository() {
    let directory = tempfile::tempdir().unwrap();
    let repo = directory.path().join("org@team/repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::create_dir(repo.join("prompts")).unwrap();
    std::fs::write(repo.join("prompts/default.md"), "default").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "default"]);
    git(&repo, &["checkout", "--quiet", "-b", "feature@v2"]);
    std::fs::write(repo.join("prompts/selected.md"), "selected").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "selected"]);
    git(&repo, &["branch", "stable"]);
    let expected = git(&repo, &["rev-parse", "HEAD"]);
    let home = directory.path().join("agent");
    let url = reqwest::Url::from_directory_path(&repo).unwrap();
    let source = format!("git:{}#feature@v2", url.as_str().trim_end_matches('/'));
    let result = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("PI_OFFLINE")
        .args(["--no-trust", "install", &source])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("packages/index.json")).unwrap()).unwrap();
    let installed = index.as_object().unwrap().values().next().unwrap();
    let root = Path::new(installed["root"].as_str().unwrap());
    assert_eq!(git(root, &["rev-parse", "HEAD"]), expected);
    assert_eq!(
        std::fs::read_to_string(root.join("prompts/selected.md")).unwrap(),
        "selected"
    );
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    assert!(
        settings["packages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &source)
    );
    let mut settings = settings;
    settings["packages"] =
        serde_json::json!([{ "source": source, "prompts": ["+prompts/selected.md"] }]);
    std::fs::write(
        home.join("settings.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let legacy_source = format!("git:{}@stable", url.as_str().trim_end_matches('/'));
    let legacy = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("PI_OFFLINE")
        .args(["--no-trust", "install", &legacy_source])
        .output()
        .unwrap();
    assert!(
        legacy.status.success(),
        "{}",
        String::from_utf8_lossy(&legacy.stderr)
    );
    assert!(String::from_utf8_lossy(&legacy.stderr).contains("Ambiguous Git package source"));
    let index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("packages/index.json")).unwrap()).unwrap();
    let installed = index
        .as_object()
        .unwrap()
        .values()
        .find(|entry| entry["source"] == legacy_source)
        .unwrap();
    let root = Path::new(installed["root"].as_str().unwrap());
    assert_eq!(git(root, &["rev-parse", "HEAD"]), expected);
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    let packages = settings["packages"].as_array().unwrap();
    assert_eq!(packages.len(), 1, "same repository retains one declaration");
    assert_eq!(packages[0]["source"], legacy_source);
    assert_eq!(
        packages[0]["prompts"],
        serde_json::json!(["+prompts/selected.md"])
    );
}

#[test]
fn configured_git_userinfo_is_not_exposed_in_logs_or_list() {
    let directory = tempfile::tempdir().unwrap();
    let agent = directory.path().join("agent");
    std::fs::create_dir(&agent).unwrap();
    let source = "git:https://private-user:topsecret@example.invalid/org/repo@stable";
    std::fs::write(
        agent.join("settings.json"),
        serde_json::json!({"packages":[source]}).to_string(),
    )
    .unwrap();
    for offline in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(directory.path())
            .env("PI_RS_HOME", &agent)
            .env_remove("PI_OFFLINE")
            .env_remove("GEMINI_API_KEY")
            .env_remove("GOOGLE_API_KEY")
            .env_remove("PI_RS_MODEL_TOKEN")
            .args([
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--provider",
                "google",
                "--model",
                "gemini-2.5-flash",
            ]);
        if offline {
            command.arg("--offline");
        }
        let output = command.args(["--print", "hello"]).output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(if offline {
                "skipped in offline mode"
            } else {
                "Preparing native package"
            }),
            "{stderr}"
        );
        if !offline {
            assert!(stderr.contains("use a credential helper"), "{stderr}");
        }
        assert!(
            !stderr.contains("private-user")
                && !stderr.contains("topsecret")
                && !stderr.contains(source),
            "{stderr}"
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("topsecret"));
    }
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &agent)
        .args(["--no-trust", "list"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[redacted]@example.invalid"), "{stdout}");
    assert!(
        !stdout.contains("private-user") && !stdout.contains("topsecret"),
        "{stdout}"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(&std::fs::read(agent.join("settings.json")).unwrap()).unwrap();
    assert_eq!(stored["packages"][0], source);

    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &agent)
        .args([
            "--offline",
            "--no-trust",
            "install",
            "git:ssh://git@example.invalid/org/repo@stable",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("cannot install package in offline mode"),
        "{stderr}"
    );
    assert!(stderr.contains("[redacted]@example.invalid"), "{stderr}");
    assert!(!stderr.contains("ssh://git@"), "{stderr}");
}

#[test]
fn malformed_remote_package_filter_is_reported_during_startup() {
    let directory = tempfile::tempdir().unwrap();
    let repo = directory.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::create_dir(repo.join("prompts")).unwrap();
    std::fs::write(repo.join("prompts/review.md"), "review").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "fixture"]);
    let home = directory.path().join("agent");
    let url = reqwest::Url::from_directory_path(&repo).unwrap();
    let source = format!("git:{}", url.as_str().trim_end_matches('/'));
    let install = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("PI_OFFLINE")
        .args(["--no-trust", "install", &source])
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({
            "packages": [{"source": source, "prompts": ["file:///bad%2Fpath"]}]
        })
        .to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("PI_OFFLINE", "1")
        .args(["--no-trust", "--no-mcp", "--print", "hello"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("encoded"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn invalid_remote_filters_follow_package_settings_order() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("agent");
    let mut sources = Vec::new();
    for name in ["z", "a"] {
        let repo = directory.path().join(name);
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        std::fs::create_dir(repo.join("prompts")).unwrap();
        std::fs::write(repo.join("prompts/review.md"), "review").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let url = reqwest::Url::from_directory_path(&repo).unwrap();
        let source = format!("git:{}", url.as_str().trim_end_matches('/'));
        let install = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(directory.path())
            .env("PI_RS_HOME", &home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env_remove("PI_OFFLINE")
            .args(["--no-trust", "install", &source])
            .output()
            .unwrap();
        assert!(
            install.status.success(),
            "{}",
            String::from_utf8_lossy(&install.stderr)
        );
        sources.push(source);
    }
    let listed = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .args([
            "--no-trust",
            "-e",
            &sources[0],
            "-e",
            &sources[1],
            "-e",
            &sources[0],
            "list",
        ])
        .output()
        .unwrap();
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let lines: Vec<serde_json::Value> = String::from_utf8(listed.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["source"], sources[0]);
    assert_eq!(lines[1]["source"], sources[1]);
    for reverse in [false, true] {
        let z = serde_json::json!({"source":sources[0], "prompts":["file:///bad%2Fpath"]});
        let a = serde_json::json!({"source":sources[1], "prompts":["file:///bad%ZZ"]});
        let packages = if reverse { vec![a, z] } else { vec![z, a] };
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages": packages}).to_string(),
        )
        .unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(directory.path())
            .env("PI_RS_HOME", &home)
            .env("PI_OFFLINE", "1")
            .args(["--no-trust", "--no-mcp", "--print", "hello"])
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success());
        assert!(
            stderr.contains(if reverse { "URI malformed" } else { "encoded" }),
            "{stderr}"
        );
    }
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({"packages":[
            {"source":sources[0],"prompts":["file:///bad%2Fpath"]},
            {"source":sources[1],"prompts":["file:///bad%ZZ"]},
            {"source":sources[0],"prompts":["file:///bad%ZZ"]}
        ]})
        .to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("PI_OFFLINE", "1")
        .args(["--no-trust", "--no-mcp", "--print", "hello"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("encoded"), "{stderr}");
    let project = directory.path().join(".pi");
    std::fs::create_dir(&project).unwrap();
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({"packages":[{"source":sources[1],"prompts":["file:///bad%ZZ"]}]})
            .to_string(),
    )
    .unwrap();
    std::fs::write(
        project.join("settings.json"),
        serde_json::json!({"packages":[{"source":sources[0],"prompts":["file:///bad%2Fpath"]}]})
            .to_string(),
    )
    .unwrap();
    let listed = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .args(["--trust", "list"])
        .output()
        .unwrap();
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let lines: Vec<serde_json::Value> = String::from_utf8(listed.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[0]["source"], sources[1]);
    assert_eq!(lines[1]["source"], sources[0]);
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("PI_OFFLINE", "1")
        .args(["--trust", "--no-mcp", "--print", "hello"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("encoded"), "{stderr}");
    std::fs::write(
        project.join("settings.json"),
        serde_json::json!({"packages":[
            {"source":sources[0],"prompts":["file:///bad%ZZ"]},
            {"source":sources[1],"prompts":["file:///bad%ZZ"]},
            {"source":sources[0],"prompts":["file:///bad%2Fpath"]}
        ]})
        .to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("PI_OFFLINE", "1")
        .args(["--trust", "--no-mcp", "--print", "hello"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("encoded"), "{stderr}");
}
