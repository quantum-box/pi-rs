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
