use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    let home = tempfile::tempdir().unwrap();
    Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn version_takes_precedence_over_help_in_either_order() {
    let version = run(&["--version"]);
    assert!(version.status.success());
    for args in [["--help", "--version"], ["--version", "--help"]] {
        let output = run(&args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert_eq!(output.stdout, version.stdout, "{args:?}");
        assert_eq!(output.stderr, version.stderr, "{args:?}");
    }
}

#[test]
fn parse_errors_take_precedence_over_metadata_in_either_order() {
    for args in [
        ["--help", "--mode", "invalid"],
        ["--mode", "invalid", "--help"],
        ["--version", "--tui-mode", "invalid"],
        ["--tui-mode", "invalid", "--version"],
        ["--help", "--tools", "read,+bash"],
        ["--tools", "read,+bash", "--help"],
        ["-ptread,+bash", "--help", "--offline"],
    ] {
        let output = run(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
        assert!(!output.stderr.is_empty(), "{args:?}: {output:?}");
    }
}

#[test]
fn export_runs_before_help_but_version_runs_before_export() {
    let export = run(&["--export", "missing-session.jsonl", "--help"]);
    assert!(!export.status.success(), "{export:?}");
    assert!(export.stdout.is_empty());
    let version = run(&["--version"]);
    let output = run(&["--export", "missing-session.jsonl", "--help", "--version"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, version.stdout);
}

#[test]
fn export_with_help_preserves_explicit_output_even_when_it_names_a_subcommand() {
    let home = tempfile::tempdir().unwrap();
    let input = home.path().join("session.jsonl");
    let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
    session
        .append_user_with_images("export marker".into(), Vec::new(), None)
        .unwrap();
    session.export_jsonl(&input).unwrap();
    for destination in ["report.html", "run"] {
        for order in 0..3 {
            let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
            command
                .current_dir(home.path())
                .env("PI_RS_HOME", home.path())
                .arg("--export")
                .arg(&input);
            match order {
                0 => {
                    command.args([destination, "--help"]);
                }
                1 => {
                    command.args(["--help", destination]);
                }
                _ => {
                    command.args(["--help", "--mode", "json", destination]);
                }
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "{destination} {order}: {output:?}");
            let document = std::fs::read_to_string(home.path().join(destination))
                .unwrap_or_else(|error| panic!("{destination} {order}: {error}; {output:?}"));
            assert!(document.contains("export marker"));
            std::fs::remove_file(home.path().join(destination)).unwrap();
        }
    }
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg("--export")
        .arg(&input)
        .args(["--help", "--", "--version"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(
        std::fs::read_to_string(home.path().join("--version"))
            .unwrap()
            .contains("export marker")
    );
    let invalid = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg("--export")
        .arg(&input)
        .args(["--max-requests", "bad", "--help", "--", "--out"])
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(!home.path().join("--out").exists());
    let trusted = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .args(["--trust", "--export"])
        .arg(&input)
        .args(["--no-trust", "--help", "--", "--trusted-output"])
        .output()
        .unwrap();
    assert!(trusted.status.success(), "{trusted:?}");
    assert!(
        std::fs::read_to_string(home.path().join("--trusted-output"))
            .unwrap()
            .contains("export marker")
    );
    let ordinary = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .args(["--trust", "--export"])
        .arg(&input)
        .args(["--no-trust", "--help", "ordinary.html"])
        .output()
        .unwrap();
    assert!(ordinary.status.success(), "{ordinary:?}");
    assert!(
        std::fs::read_to_string(home.path().join("ordinary.html"))
            .unwrap()
            .contains("export marker")
    );
    for before_export in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(home.path())
            .env("PI_RS_HOME", home.path());
        if before_export {
            command.arg("prompt.html");
        }
        command.arg("--export").arg(&input).arg("--help");
        if !before_export {
            command.args(["--print", "prompt.html"]);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "{before_export}: {output:?}");
        assert!(
            std::fs::read_to_string(home.path().join("prompt.html"))
                .unwrap()
                .contains("export marker")
        );
        std::fs::remove_file(home.path().join("prompt.html")).unwrap();
    }
    let first = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg("first.html")
        .arg("--export")
        .arg(&input)
        .args(["second.html", "--help"])
        .output()
        .unwrap();
    assert!(first.status.success(), "{first:?}");
    assert!(home.path().join("first.html").exists());
    assert!(!home.path().join("second.html").exists());
    let hyphenated = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg("--export")
        .arg(&input)
        .args(["--help", "--print", "---output"])
        .output()
        .unwrap();
    assert!(hyphenated.status.success(), "{hyphenated:?}");
    assert!(
        std::fs::read_to_string(home.path().join("---output"))
            .unwrap()
            .contains("export marker")
    );
    std::fs::write(home.path().join("attachment"), "keep").unwrap();
    let attachment = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg("--export")
        .arg(&input)
        .args(["@attachment", "--help", "attachment-output.html"])
        .output()
        .unwrap();
    assert!(attachment.status.success(), "{attachment:?}");
    assert!(
        std::fs::read_to_string(home.path().join("attachment-output.html"))
            .unwrap()
            .contains("export marker")
    );
    assert_eq!(
        std::fs::read_to_string(home.path().join("attachment")).unwrap(),
        "keep"
    );
    for args in [
        &["--help", "@attachment", "later.html"][..],
        &["@attachment", "@attachment", "--help", "later.html"][..],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(home.path())
            .env("PI_RS_HOME", home.path())
            .arg("--export")
            .arg(&input)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert!(
            std::fs::read_to_string(home.path().join("later.html"))
                .unwrap()
                .contains("export marker")
        );
        assert_eq!(
            std::fs::read_to_string(home.path().join("attachment")).unwrap(),
            "keep"
        );
        std::fs::remove_file(home.path().join("later.html")).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn attached_non_utf8_export_input_is_not_mistaken_for_help() {
    use std::os::unix::ffi::OsStringExt;
    let home = tempfile::tempdir().unwrap();
    let name = std::ffi::OsString::from_vec(b"session-\xff.jsonl".to_vec());
    let input = home.path().join(name);
    let mut argument = std::ffi::OsString::from("--export=");
    argument.push(&input);
    let plain = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg(&argument)
        .arg("--help")
        .output()
        .unwrap();
    assert!(!plain.status.success(), "{plain:?}");
    assert!(plain.stdout.is_empty());
    let output = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(home.path())
        .env("PI_RS_HOME", home.path())
        .arg(argument)
        .args(["--help", "--", "--out"])
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    assert!(!home.path().join("--out").exists());
}

#[test]
fn name_consumes_a_following_help_token_before_version() {
    let version = run(&["--version"]);
    let output = run(&["--name", "--help", "--version"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, version.stdout);
    assert!(output.stderr.is_empty());
}

#[test]
fn pi_value_options_consume_metadata_and_delimiters() {
    let help = run(&["--provider", "--version", "--help"]);
    assert!(help.status.success(), "{help:?}");
    assert!(String::from_utf8_lossy(&help.stdout).contains("Usage:"));
    let version = run(&["--version"]);
    let output = run(&["--help", "--thinking", "--", "--version"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, version.stdout);
    assert!(String::from_utf8_lossy(&output.stderr).contains("Invalid thinking level \"--\""));
}

#[cfg(unix)]
#[test]
fn extension_help_survives_clustered_help_and_help_as_a_value() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let extension = home.path().join("fixture.sh");
    std::fs::write(&extension, r##"#!/bin/sh
printf '%s\n' '{"version":1,"response":{"Ok":{"id":"metadata-fixture","dependencies":[],"commands":[],"tools":[],"flags":[{"name":"fixture-switch","default":false}]}}}'
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    for args in [&["-ph"][..], &["--name", "--help", "--help"][..]] {
        let output = Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(home.path())
            .env("PI_RS_HOME", home.path())
            .args(["--no-trust", "--extension"])
            .arg(&extension)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("--fixture-switch"),
            "{args:?}: {output:?}"
        );
    }
}

#[test]
fn thinking_warning_precedes_metadata_but_does_not_prevent_it() {
    for metadata in ["--help", "--version"] {
        let output = run(&[metadata, "--thinking", "not-a-level"]);
        assert!(output.status.success(), "{output:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("Invalid thinking level"));
        assert!(!output.stdout.is_empty());
    }
}

#[test]
fn option_values_and_native_subcommands_keep_their_ownership() {
    let value = run(&["--thinking", "--help", "--version"]);
    assert!(value.status.success(), "{value:?}");
    assert!(String::from_utf8_lossy(&value.stderr).contains("Invalid thinking level"));
    let version = run(&["--version"]);
    assert_eq!(value.stdout, version.stdout);

    let native_help = run(&["mcp", "--help"]);
    assert!(native_help.status.success(), "{native_help:?}");
    assert!(String::from_utf8_lossy(&native_help.stdout).contains("Usage:"));
    assert_ne!(native_help.stdout, version.stdout);
}
