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

#[test]
fn directory_grants_load_only_sorted_direct_markdown_files() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("prompts");
    std::fs::create_dir_all(directory.join("nested")).unwrap();
    for (name, text) in [
        ("z.md", "last"),
        (".md", "empty name"),
        (".hidden.md", "hidden"),
        ("a.md", "first"),
        ("skip.txt", "skip"),
        ("nested/inner.md", "nested"),
    ] {
        std::fs::write(directory.join(name), text).unwrap();
    }
    let explicit = root.path().join("explicit.md");
    std::fs::write(&explicit, "explicit").unwrap();
    let grants = TemplateReadGrants {
        files: vec![explicit],
    };
    let result = grants
        .load_with_directories(std::slice::from_ref(&directory))
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["explicit", ".hidden", "", "a", "z"]
    );
    let dot_md = TemplateReadGrants {
        files: vec![directory.join(".md")],
    }
    .load()
    .unwrap();
    assert_eq!(
        (dot_md[0].name.as_str(), dot_md[0].body.as_str()),
        ("", "empty name")
    );
    assert!(
        grants
            .load_with_directories(&[directory, root.path().join("missing")])
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn directory_grants_follow_file_symlinks_and_skip_broken_links() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("prompts");
    std::fs::create_dir(&directory).unwrap();
    let target = root.path().join("target");
    std::fs::write(&target, "target text").unwrap();
    std::os::unix::fs::symlink(&target, directory.join("linked.md")).unwrap();
    std::os::unix::fs::symlink(root.path().join("missing"), directory.join("broken.md")).unwrap();
    let result = TemplateReadGrants::default()
        .load_with_directories(&[directory.clone()])
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].path, directory.join("linked.md"));
    assert_eq!(result[0].body, "target text");
}

#[test]
fn directory_grants_reject_duplicate_names_and_too_many_paths() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("prompts");
    std::fs::create_dir(&directory).unwrap();
    let file = directory.join("one.md");
    std::fs::write(&file, "text").unwrap();
    assert!(
        TemplateReadGrants { files: vec![file] }
            .load_with_directories(&[directory.clone()])
            .is_err()
    );
    assert!(
        TemplateReadGrants::default()
            .load_with_directories(&vec![directory; 129])
            .is_err()
    );
}

#[test]
fn directory_grants_enforce_template_count_at_the_boundary() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("prompts");
    std::fs::create_dir(&directory).unwrap();
    for index in 0..128 {
        std::fs::write(directory.join(format!("{index}.md")), "text").unwrap();
    }
    let grants = TemplateReadGrants::default();
    assert_eq!(
        grants
            .load_with_directories(std::slice::from_ref(&directory))
            .unwrap()
            .len(),
        128
    );
    std::fs::write(directory.join("extra.md"), "text").unwrap();
    assert!(grants.load_with_directories(&[directory]).is_err());
}
