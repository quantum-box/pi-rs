use std::process::Command;

#[test]
fn local_file_url_install_and_remove_share_path_identity() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("agent");
    let package = directory.path().join("local package");
    std::fs::create_dir(&package).unwrap();
    let url = reqwest::Url::from_directory_path(&package).unwrap();
    let source = format!("{url}?ignored=%2F#%FF");
    let run = |arguments: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(directory.path())
            .env("PI_RS_HOME", &home)
            .args(arguments)
            .output()
            .unwrap()
    };
    let result = run(&["--no-trust", "install", &source]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["packages"].as_array().unwrap().len(), 1);
    let result = run(&["--no-trust", "remove", "local package"]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let result = run(&["--no-trust", "install", &format!("{url}bad%2Fpath")]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("encoded"));
}
