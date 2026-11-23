use pi_rs_workbench::skill_resources::SkillReadGrants;

fn write_skill(path: &std::path::Path, description: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, format!("---\ndescription: {description}\n---\nbody")).unwrap();
}

#[test]
fn directory_discovery_respects_roots_ignores_hidden_and_node_modules() {
    let root = tempfile::tempdir().unwrap();
    let skills = root.path().join("skills");
    write_skill(&skills.join("legacy.md"), "legacy");
    write_skill(&skills.join("one/SKILL.md"), "one");
    write_skill(&skills.join("one/child/SKILL.md"), "suppressed");
    write_skill(&skills.join("two/SKILL.md"), "two");
    write_skill(&skills.join("ignored/SKILL.md"), "ignored");
    write_skill(&skills.join(".hidden/SKILL.md"), "hidden");
    write_skill(&skills.join("node_modules/x/SKILL.md"), "vendor");
    std::fs::write(skills.join(".gitignore"), "ignored/\n").unwrap();
    let result = SkillReadGrants::default()
        .load_with_directories(std::slice::from_ref(&skills))
        .unwrap();
    assert_eq!(
        result
            .skills
            .iter()
            .map(|skill| skill.description.as_str())
            .collect::<Vec<_>>(),
        ["legacy", "one", "two"]
    );
    let relative = SkillReadGrants::default()
        .load_with_directories_and_base(&["skills".into()], root.path())
        .unwrap();
    assert_eq!(relative.skills, result.skills);
    assert!(
        SkillReadGrants::default()
            .load_with_directories(&["skills".into()])
            .is_err()
    );
}

#[test]
fn declared_root_suppresses_legacy_and_nested_files() {
    let root = tempfile::tempdir().unwrap();
    write_skill(&root.path().join("SKILL.md"), "root");
    write_skill(&root.path().join("legacy.md"), "legacy");
    write_skill(&root.path().join("nested/SKILL.md"), "nested");
    let result = SkillReadGrants::default()
        .load_with_directories(&[root.path().to_owned()])
        .unwrap();
    assert_eq!(result.skills.len(), 1);
    assert_eq!(result.skills[0].description, "root");
}

#[cfg(unix)]
#[test]
fn directory_discovery_retains_granted_symlink_paths() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    write_skill(&target.join("SKILL.md"), "linked");
    let source = root.path().join("skills");
    std::fs::create_dir(&source).unwrap();
    std::os::unix::fs::symlink(&target, source.join("alias")).unwrap();
    let result = SkillReadGrants::default()
        .load_with_directories(std::slice::from_ref(&source))
        .unwrap();
    assert_eq!(result.skills.len(), 1);
    assert_eq!(result.skills[0].path, source.join("alias/SKILL.md"));
}

#[test]
fn negated_hidden_entry_remains_hidden_and_ignore_errors_fail() {
    let root = tempfile::tempdir().unwrap();
    write_skill(&root.path().join(".hidden/SKILL.md"), "hidden");
    write_skill(&root.path().join("visible/SKILL.md"), "visible");
    std::fs::write(root.path().join(".ignore"), "!.hidden/\n").unwrap();
    let grants = SkillReadGrants::default();
    let skills = grants
        .load_with_directories(&[root.path().to_owned()])
        .unwrap();
    assert_eq!(skills.skills.len(), 1);
    assert_eq!(skills.skills[0].description, "visible");
    std::fs::write(root.path().join(".ignore"), vec![b'x'; 256 * 1024 + 1]).unwrap();
    assert!(
        grants
            .load_with_directories(&[root.path().to_owned()])
            .is_err()
    );
    std::fs::write(root.path().join(".ignore"), "[z-a]\n").unwrap();
    assert!(
        grants
            .load_with_directories(&[root.path().to_owned()])
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn declared_root_does_not_read_descendant_ignore_fifo() {
    use std::os::unix::ffi::OsStrExt;
    let root = tempfile::tempdir().unwrap();
    write_skill(&root.path().join("SKILL.md"), "root");
    let child = root.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let fifo = std::ffi::CString::new(child.join(".ignore").as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let grants = SkillReadGrants::default();
    let result = grants
        .load_with_directories(&[root.path().to_owned()])
        .unwrap();
    assert_eq!(result.skills.len(), 1);
    assert_eq!(result.skills[0].description, "root");
    std::fs::remove_file(root.path().join("SKILL.md")).unwrap();
    assert!(
        grants
            .load_with_directories(&[root.path().to_owned()])
            .is_err()
    );
}

#[test]
fn nested_ignore_basename_is_anchored_to_its_directory() {
    let root = tempfile::tempdir().unwrap();
    write_skill(&root.path().join("sub/foo/SKILL.md"), "ignored");
    write_skill(&root.path().join("sub/nested/foo/SKILL.md"), "kept");
    write_skill(&root.path().join("sub/  # notes/SKILL.md"), "comment");
    write_skill(&root.path().join("sub/bar/SKILL.md"), "reallowed");
    std::fs::write(
        root.path().join("sub/.ignore"),
        "foo\n  # notes\nbar\n!bar\n",
    )
    .unwrap();
    let result = SkillReadGrants::default()
        .load_with_directories(&[root.path().to_owned()])
        .unwrap();
    assert_eq!(
        result
            .skills
            .iter()
            .map(|skill| skill.description.as_str())
            .collect::<Vec<_>>(),
        ["comment", "reallowed", "kept"]
    );
}
