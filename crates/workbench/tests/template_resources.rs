use pi_rs_workbench::template_resources::TemplateReadGrants;

#[test]
fn templates_are_explicit_snapshots_with_pi_metadata() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("review.md");
    std::fs::write(&path, "\u{feff}---\r\ndescription: Review this\r\nargument-hint: '<file>'\r\n---\r\n  Inspect $1.  \r\n").unwrap();
    let grants = TemplateReadGrants {
        files: vec![path.clone()],
    };
    let snapshot = grants.load().unwrap();
    assert_eq!(snapshot[0].name, "review");
    assert_eq!(snapshot[0].description, "Review this");
    assert_eq!(snapshot[0].argument_hint.as_deref(), Some("<file>"));
    assert_eq!(snapshot[0].body, "Inspect $1.");
    assert_eq!(snapshot[0].path, path);
    std::fs::write(&path, "Next\nbody").unwrap();
    assert_eq!(grants.load().unwrap()[0].description, "Next");
    assert_eq!(snapshot[0].body, "Inspect $1.");
    assert!(TemplateReadGrants::default().load().unwrap().is_empty());
}

#[test]
fn invalid_or_duplicate_files_fail_without_partial_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("test.md");
    std::fs::write(&file, "valid").unwrap();
    assert!(
        TemplateReadGrants {
            files: vec![file.clone(), file.clone()]
        }
        .load()
        .is_err()
    );
    assert!(
        TemplateReadGrants {
            files: vec![file.clone(), root.path().join("missing.md")]
        }
        .load()
        .is_err()
    );
    std::fs::write(&file, "---\ndescription: [\n---\nbody").unwrap();
    assert!(TemplateReadGrants { files: vec![file] }.load().is_err());
}

#[test]
fn fallback_description_truncates_in_utf16_units() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("long.md");
    std::fs::write(&file, format!("{}more", "a".repeat(61))).unwrap();
    let templates = TemplateReadGrants { files: vec![file] }.load().unwrap();
    assert_eq!(templates[0].description, format!("{}...", "a".repeat(60)));
}

#[test]
fn invalid_utf8_and_oversized_template_fail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("test.md");
    let grants = TemplateReadGrants {
        files: vec![path.clone()],
    };
    std::fs::write(&path, [0xff]).unwrap();
    assert!(grants.load().is_err());
    std::fs::write(&path, vec![b'x'; 256 * 1024 + 1]).unwrap();
    assert!(grants.load().is_err());
}

#[cfg(unix)]
#[test]
fn fifo_template_is_rejected_without_waiting_for_a_writer() {
    use std::os::unix::ffi::OsStrExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("pipe.md");
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    assert!(TemplateReadGrants { files: vec![path] }.load().is_err());
}
