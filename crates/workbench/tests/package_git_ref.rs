//! Git ref disambiguation and legacy package identity with a local repository.
use std::path::Path;
use std::process::Command;

#[test]
fn list_keeps_configured_entries_and_excludes_transient_sources() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("agent");
    let project = directory.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let source = "git:https://example.invalid/repo#v1";
    let other = "git:https://example.invalid/other";
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({"packages":[source, {"source":source,"prompts":[]}]}).to_string(),
    )
    .unwrap();
    std::fs::write(
        project.join("settings.json"),
        serde_json::json!({"packages":[source]}).to_string(),
    )
    .unwrap();
    for (trust, expected) in [("--no-trust", 2), ("--trust", 3)] {
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(directory.path())
            .env("PI_RS_HOME", &home)
            .args([trust, "-e", other, "list"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let lines: Vec<serde_json::Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), expected);
        assert!(lines.iter().all(|line| line["source"] == source));
        assert_eq!(lines[0]["scope"], "user");
        assert_eq!(lines[0]["filtered"], false);
        assert_eq!(lines[1]["filtered"], true);
        if expected == 3 {
            assert_eq!(lines[2]["scope"], "project");
        }
    }
}

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
fn unavailable_transient_git_package_warns_and_keeps_rpc_available() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::Stdio;

    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("agent");
    std::fs::create_dir(&home).unwrap();
    let repo = "https://example.invalid/acme/missing";
    let source = format!("git:{repo}");
    let missing = reqwest::Url::from_directory_path(directory.path().join("missing")).unwrap();
    let command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(directory.path())
            .env("PI_RS_HOME", &home)
            .env_remove("PI_OFFLINE")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", directory.path().join("gitconfig"))
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", format!("url.{missing}.insteadOf"))
            .env("GIT_CONFIG_VALUE_0", repo)
            .arg("--no-trust");
        command
    };
    let mut child = command()
        .args([
            "--no-mcp",
            "--no-extensions",
            "--no-skills",
            "--no-session",
            "--mode",
            "rpc",
            "-e",
            &source,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            if send.send(line).is_err() {
                break;
            }
        }
    });
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"id":"package-startup","type":"get_commands"})
    )
    .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut warned = false;
    let response = loop {
        let Ok(line) =
            receive.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        else {
            let _ = child.kill();
            break None;
        };
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        if value["id"] == "package-startup" {
            break Some(value);
        }
        warned |= value["message"].as_str().is_some_and(|message| {
            message.contains(&format!("Package {source}:"))
                && message.contains("package command failed")
        });
    };
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(output.status.success(), "{response:?}");
    assert!(
        warned,
        "missing package warning before RPC response: {response:?}"
    );
    assert!(response.is_some_and(|value| value["success"] == true));

    let explicit = command().args(["install", &source]).output().unwrap();
    assert!(!explicit.status.success());
    assert!(String::from_utf8_lossy(&explicit.stderr).contains("package command failed"));
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
    assert_eq!(
        lines
            .iter()
            .map(|line| line["source"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![sources[1].as_str(), sources[0].as_str()]
    );
    assert_eq!(
        lines
            .iter()
            .map(|line| line["scope"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["user", "project"]
    );
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

#[test]
fn project_delta_prepares_user_revision_when_cache_is_empty() {
    let directory = tempfile::tempdir().unwrap();
    let repo = directory.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::create_dir(repo.join("prompts")).unwrap();
    std::fs::write(repo.join("prompts/demo.md"), "fixture").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "fixture"]);
    git(&repo, &["branch", "user-revision"]);
    git(&repo, &["branch", "project-revision"]);
    git(&repo, &["branch", "other-revision"]);
    let home = directory.path().join("agent");
    let project = directory.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let url = reqwest::Url::from_directory_path(&repo).unwrap();
    let base = url.as_str().trim_end_matches('/');
    let user = format!("git:{base}#user-revision");
    let local = format!("git:{base}#project-revision");
    let other = format!("git:{base}#other-revision");
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({"packages":[{"source":user},{"source":other}]}).to_string(),
    )
    .unwrap();
    std::fs::write(project.join("settings.json"), serde_json::json!({"packages":[{"source":local,"autoload":false,"prompts":["+prompts/demo.md"]}]}).to_string()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("PI_OFFLINE")
        .env_remove("GEMINI_API_KEY")
        .env_remove("GOOGLE_API_KEY")
        .env_remove("PI_RS_MODEL_TOKEN")
        .args([
            "--trust",
            "--no-mcp",
            "--provider",
            "google",
            "--model",
            "gemini-2.5-flash",
            "--print",
            "hello",
        ])
        .output()
        .unwrap();
    let index: serde_json::Value = serde_json::from_slice(
        &std::fs::read(home.join("packages/index.json"))
            .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stderr))),
    )
    .unwrap();
    let sources: Vec<_> = index
        .as_object()
        .unwrap()
        .values()
        .filter_map(|entry| entry["source"].as_str())
        .collect();
    assert!(sources.contains(&user.as_str()), "{sources:?}");
    assert!(!sources.contains(&local.as_str()), "{sources:?}");
    assert!(sources.contains(&other.as_str()), "{sources:?}");

    // A missing first revision must not prevent the next source from being prepared.
    std::fs::remove_file(home.join("packages/index.json")).unwrap();
    let missing = format!("git:{base}#missing-revision");
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({"packages":[{"source":missing},{"source":other}]}).to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(directory.path())
        .env("PI_RS_HOME", &home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("PI_OFFLINE")
        .env_remove("GEMINI_API_KEY")
        .env_remove("GOOGLE_API_KEY")
        .env_remove("PI_RS_MODEL_TOKEN")
        .args([
            "--trust",
            "--no-mcp",
            "--provider",
            "google",
            "--model",
            "gemini-2.5-flash",
            "--print",
            "hello",
        ])
        .output()
        .unwrap();
    let index: serde_json::Value = serde_json::from_slice(
        &std::fs::read(home.join("packages/index.json"))
            .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stderr))),
    )
    .unwrap();
    let sources: Vec<_> = index
        .as_object()
        .unwrap()
        .values()
        .filter_map(|entry| entry["source"].as_str())
        .collect();
    assert!(sources.contains(&other.as_str()), "{sources:?}");
    assert!(!sources.contains(&missing.as_str()), "{sources:?}");
}

#[test]
fn a_transient_git_commit_pin_reuses_its_checkout_without_fetching() {
    let directory = tempfile::tempdir().unwrap();
    let repo = directory.path().join("repo");
    let prompts = repo.join("prompts");
    std::fs::create_dir_all(&prompts).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(prompts.join("older.md"), "older").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "older"]);
    let full = git(&repo, &["rev-parse", "HEAD"]);
    let abbreviation = &full[..9];
    std::fs::write(prompts.join("newer.md"), "newer").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "newer"]);

    let home = directory.path().join("agent");
    let url = reqwest::Url::from_directory_path(&repo).unwrap();
    let source = format!("git:{}#{abbreviation}", url.as_str().trim_end_matches('/'));
    let command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(directory.path())
            .env("PI_RS_HOME", &home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", directory.path().join("gitconfig"))
            .env_remove("PI_OFFLINE")
            .env_remove("GEMINI_API_KEY")
            .env_remove("GOOGLE_API_KEY")
            .env_remove("PI_RS_MODEL_TOKEN")
            .args(["--no-trust", "--no-mcp", "--no-extensions", "--no-skills"]);
        command
    };
    let output = command().args(["install", &source]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let index = std::fs::read(home.join("packages/index.json")).unwrap();
    let package: serde_json::Value = serde_json::from_slice(&index).unwrap();
    let root = Path::new(
        package.as_object().unwrap().values().next().unwrap()["root"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(git(root, &["rev-parse", "HEAD"]), full);
    assert!(root.join("prompts/older.md").is_file());
    assert!(!root.join("prompts/newer.md").exists());

    let trace = directory.path().join("trace.log");
    let _output = command()
        .env("GIT_TRACE", &trace)
        .args([
            "-e",
            &source,
            "--provider",
            "google",
            "--model",
            "gemini-2.5-flash",
            "--print",
            "hello",
        ])
        .output()
        .unwrap();
    let trace = std::fs::read_to_string(trace).unwrap();
    assert!(trace.contains("rev-parse"), "{trace}");
    assert!(
        !trace.contains("git clone") && !trace.contains("git fetch"),
        "{trace}"
    );
    assert_eq!(
        std::fs::read(home.join("packages/index.json")).unwrap(),
        index
    );
}
