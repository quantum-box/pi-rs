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

fn write_export_session(path: &std::path::Path, marker: &str) {
    let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
    session
        .append_user_with_images(marker.into(), Vec::new(), None)
        .unwrap();
    session.export_jsonl(path).unwrap();
}

#[test]
fn repeated_exports_select_last_input_and_first_message_without_touching_inputs() {
    for help in [false, true] {
        for output_position in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let a = dir.path().join("a.jsonl");
            let b = dir.path().join("b.jsonl");
            write_export_session(&a, "first marker");
            write_export_session(&b, "second marker");
            let a_before = std::fs::read(&a).unwrap();
            let b_before = std::fs::read(&b).unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
            command
                .current_dir(dir.path())
                .env("PI_RS_HOME", dir.path());
            if output_position == 1 {
                command.arg("result.html");
            }
            command.arg("--export").arg(&a);
            if output_position == 2 {
                command.arg("result.html");
            }
            if help {
                command.arg("--help");
            }
            command.arg("--export").arg(&b);
            if output_position == 3 {
                command.arg("result.html");
            }
            let result = command.output().unwrap();
            assert!(
                result.status.success(),
                "help={help} position={output_position}: {result:?}"
            );
            let dest = if output_position == 0 {
                dir.path().join("pi-session-b.html")
            } else {
                dir.path().join("result.html")
            };
            let html = std::fs::read_to_string(dest).unwrap();
            assert!(html.contains("second marker") && !html.contains("first marker"));
            assert_eq!(std::fs::read(&a).unwrap(), a_before);
            assert_eq!(std::fs::read(&b).unwrap(), b_before);
        }
    }
}

#[test]
fn export_validation_precedence_and_delimiter_preserve_files() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.jsonl");
    let b = dir.path().join("b.jsonl");
    write_export_session(&a, "A");
    write_export_session(&b, "B");
    let before_a = std::fs::read(&a).unwrap();
    let before = std::fs::read(&b).unwrap();
    for extra in [&["--max-requests", "bad"][..], &["--mode", "bad"][..]] {
        for help in [false, true] {
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_pi"));
            cmd.current_dir(dir.path())
                .env("PI_RS_HOME", dir.path())
                .arg("--export")
                .arg(&a)
                .arg("--export")
                .arg(&b)
                .args(extra);
            if help {
                cmd.arg("--help");
            }
            let result = cmd.args(["--", "--output"]).output().unwrap();
            assert!(!result.status.success(), "{result:?}");
            assert!(!dir.path().join("--output").exists());
            assert_eq!(std::fs::read(&b).unwrap(), before);
            assert_eq!(std::fs::read(&a).unwrap(), before_a);
        }
    }
    let result = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(dir.path())
        .env("PI_RS_HOME", dir.path())
        .arg("--export")
        .arg(&a)
        .arg("--export")
        .arg(&b)
        .args(["--help", "--version"])
        .output()
        .unwrap();
    assert!(result.status.success(), "{result:?}");
    assert!(!dir.path().join("pi-session-b.html").exists());
    assert_eq!(std::fs::read(&a).unwrap(), before_a);
    assert_eq!(std::fs::read(&b).unwrap(), before);
    let result = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(dir.path())
        .env("PI_RS_HOME", dir.path())
        .arg("--export")
        .arg(&a)
        .arg("--export")
        .arg(&b)
        .args(["--help", "@attachment", "--", "--output", "ignored"])
        .output()
        .unwrap();
    assert!(result.status.success(), "{result:?}");
    assert!(
        std::fs::read_to_string(dir.path().join("--output"))
            .unwrap()
            .contains('B')
    );
    assert!(!dir.path().join("ignored").exists());
    assert_eq!(std::fs::read(&b).unwrap(), before);
}

#[cfg(unix)]
#[test]
fn repeated_exports_preserve_non_utf8_paths() {
    use std::os::unix::ffi::OsStringExt;
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.jsonl");
    let b = dir
        .path()
        .join(std::ffi::OsString::from_vec(b"input-\xff.jsonl".to_vec()));
    let out = dir
        .path()
        .join(std::ffi::OsString::from_vec(b"output-\xfe.html".to_vec()));
    write_export_session(&a, "old marker");
    let before = std::fs::read(&a).unwrap();
    let mut attached = std::ffi::OsString::from("--export=");
    attached.push(&b);
    let result = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(dir.path())
        .env("PI_RS_HOME", dir.path())
        .arg("--export")
        .arg(&a)
        .arg(attached)
        .arg("--help")
        .arg(&out)
        .output()
        .unwrap();
    assert!(!result.status.success(), "{result:?}");
    assert!(!out.exists());
    assert_eq!(std::fs::read(&a).unwrap(), before);
}

#[test]
fn export_ownership_respects_native_commands_and_option_values() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.jsonl");
    write_export_session(&source, "ownership marker");
    for help in [false, true] {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pi"));
        cmd.current_dir(dir.path())
            .env("PI_RS_HOME", dir.path())
            .args(["--name", "--version", "--no-tools", "--export"])
            .arg(&source)
            .args(["@attachment", "--print", "---out"]);
        if help {
            cmd.arg("--help");
        }
        let result = cmd.output().unwrap();
        assert!(result.status.success(), "{result:?}");
        assert!(
            std::fs::read_to_string(dir.path().join("---out"))
                .unwrap()
                .contains("ownership marker")
        );
        std::fs::remove_file(dir.path().join("---out")).unwrap();
    }
    let native = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(dir.path())
        .env("PI_RS_HOME", dir.path())
        .args(["mcp", "--help", "--export"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&native.stdout).contains("Usage: pi mcp"),
        "{native:?}"
    );
    assert!(!dir.path().join("pi-session-source.html").exists());
}

#[test]
fn export_rejects_output_alias_of_selected_session() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.jsonl");
    let last = dir.path().join("last.jsonl");
    write_export_session(&first, "first");
    write_export_session(&last, "last");
    let before = std::fs::read(&last).unwrap();
    let first_before = std::fs::read(&first).unwrap();
    #[cfg(unix)]
    let alias = {
        let alias = dir.path().join("source-link");
        std::os::unix::fs::symlink(&last, &alias).unwrap();
        alias
    };
    #[cfg(unix)]
    let outputs = vec![last.clone(), alias];
    #[cfg(not(unix))]
    let outputs = vec![last.clone()];
    for output in outputs {
        for help in [false, true] {
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_pi"));
            cmd.current_dir(dir.path())
                .env("PI_RS_HOME", dir.path())
                .arg("--export")
                .arg(&first)
                .arg("--export")
                .arg(&last);
            if help {
                cmd.arg("--help");
            }
            let result = cmd.arg(&output).output().unwrap();
            assert!(!result.status.success(), "{result:?}");
            assert_eq!(std::fs::read(&last).unwrap(), before);
            assert_eq!(std::fs::read(&first).unwrap(), first_before);
        }
    }
}

#[test]
fn missing_or_empty_last_export_never_falls_back_to_earlier_input() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.jsonl");
    write_export_session(&first, "sentinel");
    let before = std::fs::read(&first).unwrap();
    for help in [false, true] {
        for last in [
            &["--export"][..],
            &["--export="][..],
            &["--export", "missing.jsonl"][..],
        ] {
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_pi"));
            cmd.current_dir(dir.path())
                .env("PI_RS_HOME", dir.path())
                .arg("--export")
                .arg(&first);
            if help {
                cmd.arg("--help");
            }
            let result = cmd.args(last).output().unwrap();
            assert!(!result.status.success(), "{help} {last:?}: {result:?}");
            assert_eq!(std::fs::read(&first).unwrap(), before);
            assert!(!dir.path().join("pi-session-first.html").exists());
        }
    }
}

#[test]
fn repeated_export_destination_cannot_overwrite_any_earlier_input() {
    for help in [false, true] {
        for destination in [
            "default",
            "explicit",
            "output-symlink",
            "input-symlink",
            "home-alias",
            "hard-link",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let earlier = dir.path().join("pi-session-b.html");
            let selected = dir.path().join("b.jsonl");
            write_export_session(&earlier, "earlier sentinel");
            write_export_session(&selected, "selected sentinel");
            let earlier_before = std::fs::read(&earlier).unwrap();
            let selected_before = std::fs::read(&selected).unwrap();
            let alias = dir.path().join("alias");
            #[cfg(unix)]
            if matches!(destination, "output-symlink" | "input-symlink") {
                std::os::unix::fs::symlink(&earlier, &alias).unwrap();
            }
            #[cfg(unix)]
            if destination == "hard-link" {
                std::fs::hard_link(&earlier, &alias).unwrap();
            }
            #[cfg(not(unix))]
            if matches!(
                destination,
                "output-symlink" | "input-symlink" | "hard-link"
            ) {
                continue;
            }
            let home_alias = std::path::PathBuf::from("~/pi-session-b.html");
            let previous = match destination {
                "input-symlink" => &alias,
                "home-alias" => &home_alias,
                _ => &earlier,
            };
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_pi"));
            cmd.current_dir(dir.path())
                .env("PI_RS_HOME", dir.path())
                .env("HOME", dir.path())
                .env("USERPROFILE", dir.path())
                .arg("--export")
                .arg(previous)
                .arg("--export")
                .arg(&selected);
            if help {
                cmd.arg("--help");
            }
            match destination {
                "explicit" | "input-symlink" | "home-alias" => {
                    cmd.arg("./pi-session-b.html");
                }
                "output-symlink" | "hard-link" => {
                    cmd.arg(&alias);
                }
                _ => {}
            }
            let result = cmd.output().unwrap();
            assert!(!result.status.success(), "{help} {destination}: {result:?}");
            assert_eq!(std::fs::read(&earlier).unwrap(), earlier_before);
            assert_eq!(std::fs::read(&selected).unwrap(), selected_before);
        }
    }
}
