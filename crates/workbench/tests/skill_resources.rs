use pi_rs_workbench::skill_resources::{SkillReadGrants, SkillReader};

#[test]
fn loads_explicit_skill_snapshot_and_renders_only_visible_skills() {
    let root = tempfile::tempdir().unwrap();
    let visible = root.path().join("review/SKILL.md");
    let hidden = root.path().join("private/SKILL.md");
    for path in [&visible, &hidden] {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    }
    std::fs::write(
        &visible,
        "\u{feff}---\r\nname: review\r\ndescription: 'Read <a> & \"b\"'\r\n---\r\nBody",
    )
    .unwrap();
    std::fs::write(&hidden, "---\nname: private\ndescription: Explicit only\ndisable-model-invocation: true\n---\nSecret body").unwrap();
    let grants = SkillReadGrants {
        files: vec![visible.clone(), hidden.clone()],
    };
    let snapshot = grants.load().unwrap();
    assert_eq!(snapshot.skills.len(), 2);
    assert_eq!(snapshot.skills[0].name, "review");
    assert!(snapshot.skills[1].disable_model_invocation);
    let rendered = snapshot.render(SkillReader::Read).unwrap();
    assert!(rendered.contains("Use the read tool to load a skill's file"));
    assert!(rendered.contains("Read &lt;a&gt; &amp; &quot;b&quot;"));
    assert!(rendered.contains(&visible.display().to_string()));
    assert!(!rendered.contains("private"));
    assert!(
        snapshot
            .render(SkillReader::Bash)
            .unwrap()
            .contains("Use bash")
    );
    assert!(
        snapshot
            .render(SkillReader::Indirect)
            .unwrap()
            .contains("Load a skill's file")
    );
    std::fs::write(&visible, "---\ndescription: Changed\n---\nBody").unwrap();
    assert_eq!(grants.load().unwrap().skills[0].description, "Changed");
    assert_eq!(snapshot.skills[0].description, "Read <a> & \"b\"");
    assert!(
        SkillReadGrants::default()
            .load()
            .unwrap()
            .render(SkillReader::Read)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn reports_pi_validation_and_rejects_ambiguous_or_unreadable_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("a/SKILL.md");
    let second = root.path().join("b/SKILL.md");
    let missing_description = root.path().join("c/SKILL.md");
    for path in [&first, &second, &missing_description] {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    }
    std::fs::write(
        &first,
        "---\nname: Bad--name\ndescription: hello\n---\nbody",
    )
    .unwrap();
    std::fs::write(&second, "---\nname: second\ndescription: hello\n---\nbody").unwrap();
    std::fs::write(
        &missing_description,
        "---\nname: Missing--description\n---\nbody",
    )
    .unwrap();
    let grants = SkillReadGrants {
        files: vec![first.clone(), second.clone(), missing_description],
    };
    let result = grants.load().unwrap();
    assert_eq!(result.skills.len(), 2);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("invalid characters"))
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("consecutive hyphens"))
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message == "description is required")
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.path.ends_with("c/SKILL.md")
                && diagnostic.message.contains("consecutive hyphens"))
    );
    std::fs::write(
        &second,
        "---\nname: Bad--name\ndescription: duplicate\n---\nbody",
    )
    .unwrap();
    assert!(grants.load().is_err());
    std::fs::write(&second, [0xff]).unwrap();
    assert!(grants.load().is_err());
    assert!(
        SkillReadGrants {
            files: vec![root.path().join("missing.md")]
        }
        .load()
        .is_err()
    );
}

#[test]
fn relative_paths_require_explicit_absolute_base_and_resolve_before_rendering() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("review/SKILL.md");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "---\ndescription: Test\n---\nbody").unwrap();
    let grants = SkillReadGrants {
        files: vec!["review/./SKILL.md".into()],
    };
    assert!(grants.load().is_err());
    assert!(
        grants
            .load_with_base(std::path::Path::new("relative"))
            .is_err()
    );
    let loaded = grants.load_with_base(root.path()).unwrap();
    assert_eq!(loaded.skills[0].path, path);
    assert!(
        loaded
            .render(SkillReader::Read)
            .unwrap()
            .contains(&path.display().to_string())
    );
}

#[test]
fn errors_identify_bad_and_duplicate_skill_paths() {
    let root = tempfile::tempdir().unwrap();
    let one = root.path().join("one/SKILL.md");
    let two = root.path().join("two/SKILL.md");
    for path in [&one, &two] {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    }
    std::fs::write(&one, "---\nname: same\ndescription: first\n---\nbody").unwrap();
    std::fs::write(&two, "---\nname: same\ndescription: second\n---\nbody").unwrap();
    let grants = SkillReadGrants {
        files: vec![one.clone(), two.clone()],
    };
    let message = grants.load().unwrap_err().to_string();
    assert!(
        message.contains("same")
            && message.contains(&one.display().to_string())
            && message.contains(&two.display().to_string())
    );
    std::fs::write(&two, "---\ndescription: [\n---\nbody").unwrap();
    assert!(
        grants
            .load()
            .unwrap_err()
            .to_string()
            .contains(&two.display().to_string())
    );
}

#[cfg(unix)]
#[test]
fn skill_paths_reject_parent_components_and_non_utf8_directories() {
    use std::os::unix::ffi::OsStringExt;
    let root = tempfile::tempdir().unwrap();
    let allowed = root.path().join("allowed");
    let outside = root.path().join("outside/child");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(
        allowed.join("SKILL.md"),
        "---\ndescription: wrong\n---\nbody",
    )
    .unwrap();
    std::fs::write(
        outside.parent().unwrap().join("SKILL.md"),
        "---\ndescription: symlink target\n---\nbody",
    )
    .unwrap();
    std::os::unix::fs::symlink(&outside, allowed.join("link")).unwrap();
    let ambiguous = allowed.join("link/../SKILL.md");
    assert!(
        SkillReadGrants {
            files: vec![ambiguous]
        }
        .load()
        .is_err()
    );
    let non_utf8 = root
        .path()
        .join(std::ffi::OsString::from_vec(b"invalid-\xff".to_vec()));
    let path = non_utf8.join("SKILL.md");
    let error = SkillReadGrants {
        files: vec![path.clone()],
    }
    .load()
    .unwrap_err();
    assert!(error.to_string().contains("UTF-8"));
    let manual = pi_rs_workbench::skill_resources::SkillResources {
        skills: vec![pi_rs_workbench::skill_resources::SkillResource {
            name: "test".into(),
            description: "Test".into(),
            path,
            disable_model_invocation: false,
        }],
        diagnostics: vec![],
    };
    assert!(manual.render(SkillReader::Read).is_err());
}
