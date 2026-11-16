use std::fs;

use pi_rs_workbench::prompt_resources::{PromptReadGrants, PromptResourceFactory};

#[test]
fn grants_precedence_and_snapshot_reload_are_explicit() {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join("project");
    let global = temp.path().join("global");
    fs::create_dir_all(project.join(".pi")).unwrap();
    fs::create_dir_all(&global).unwrap();
    fs::write(global.join("SYSTEM.md"), "global").unwrap();
    fs::write(global.join("APPEND_SYSTEM.md"), "global append").unwrap();
    fs::write(project.join(".pi/SYSTEM.md"), "\u{feff}project").unwrap();
    let factory = PromptResourceFactory::default();
    assert_eq!(
        factory
            .load(&PromptReadGrants::default())
            .unwrap()
            .compose("default"),
        "default"
    );
    let grants = PromptReadGrants {
        project: Some(project.clone()),
        global: Some(global.clone()),
    };
    let first = factory.load(&grants).unwrap();
    assert_eq!(first.compose("default"), "project\n\nglobal append");
    assert_eq!(
        first.system.as_ref().unwrap().source.as_deref(),
        Some(project.join(".pi/SYSTEM.md").as_path())
    );
    fs::remove_file(project.join(".pi/SYSTEM.md")).unwrap();
    fs::write(project.join(".pi/APPEND_SYSTEM.md"), "project append").unwrap();
    let next = factory.load(&grants).unwrap();
    assert_eq!(next.compose("default"), "global\n\nproject append");
    assert_eq!(first.compose("default"), "project\n\nglobal append");
    let revoked = factory
        .load(&PromptReadGrants {
            project: None,
            global: Some(global),
        })
        .unwrap();
    assert_eq!(revoked.compose("default"), "global\n\nglobal append");
}

#[test]
fn explicit_values_override_discovery_and_empty_append_disables_it() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("SYSTEM.md"), "disk").unwrap();
    fs::write(temp.path().join("APPEND_SYSTEM.md"), "disk append").unwrap();
    let grants = PromptReadGrants {
        global: Some(temp.path().to_owned()),
        project: None,
    };
    let factory = PromptResourceFactory {
        system: Some("literal".into()),
        append: Some(vec![]),
    };
    let snapshot = factory.load(&grants).unwrap();
    assert_eq!(snapshot.compose("default"), "literal");
    assert_eq!(snapshot.system.unwrap().source, None);
    let factory = PromptResourceFactory {
        system: None,
        append: Some(vec!["a".into(), "b".into()]),
    };
    assert_eq!(
        factory.load(&grants).unwrap().compose("default"),
        "disk\n\na\n\nb"
    );
}

#[test]
fn failed_reload_does_not_replace_existing_snapshot_or_fall_back() {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join("project");
    let global = temp.path().join("global");
    fs::create_dir_all(project.join(".pi")).unwrap();
    fs::create_dir_all(&global).unwrap();
    fs::write(global.join("SYSTEM.md"), "safe").unwrap();
    let grants = PromptReadGrants {
        project: Some(project.clone()),
        global: Some(global),
    };
    let factory = PromptResourceFactory::default();
    let snapshot = factory.load(&grants).unwrap();
    fs::write(project.join(".pi/SYSTEM.md"), [0xff]).unwrap();
    assert!(factory.load(&grants).is_err());
    assert_eq!(snapshot.compose("default"), "safe");
    fs::write(project.join(".pi/SYSTEM.md"), vec![b'x'; 256 * 1024 + 1]).unwrap();
    assert!(factory.load(&grants).is_err());
}

#[derive(Default)]
struct Capture(std::sync::Mutex<Vec<pi_rs::ModelRequest>>);

#[async_trait::async_trait]
impl pi_rs::Model for Capture {
    async fn stream(&self, request: pi_rs::ModelRequest) -> pi_rs::Result<pi_rs::ModelStream> {
        self.0.lock().unwrap().push(request);
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(pi_rs::ModelEvent::Text("done".into())),
            Ok(pi_rs::ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])))
    }
}

#[tokio::test]
async fn applied_snapshot_reaches_model_and_reload_is_host_controlled() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("SYSTEM.md");
    fs::write(&path, "first").unwrap();
    let grants = PromptReadGrants {
        global: Some(temp.path().to_owned()),
        project: None,
    };
    let factory = PromptResourceFactory::default();
    let snapshot = factory.load(&grants).unwrap();
    let model = std::sync::Arc::new(Capture::default());
    let mut session = snapshot
        .apply(
            pi_rs::SessionBuilder::new(model.clone(), "fixture"),
            "default",
        )
        .in_memory()
        .unwrap();
    fs::write(path, "second").unwrap();
    let next = factory.load(&grants).unwrap();
    session.prompt("hello").await.unwrap();
    assert!(
        model.0.lock().unwrap()[0]
            .messages
            .iter()
            .any(|m| matches!(m, pi_rs::Message::System { content } if content == "first"))
    );
    session.set_system_prompt(next.compose("default")).unwrap();
    session.prompt("again").await.unwrap();
    assert!(
        model.0.lock().unwrap()[1]
            .messages
            .iter()
            .any(|m| matches!(m, pi_rs::Message::System { content } if content == "second"))
    );
    session.close().await.unwrap();
}

#[test]
fn empty_system_uses_default_without_falling_back_to_global() {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join("project");
    let global = temp.path().join("global");
    fs::create_dir_all(project.join(".pi")).unwrap();
    fs::create_dir_all(&global).unwrap();
    fs::write(project.join(".pi/SYSTEM.md"), "").unwrap();
    fs::write(global.join("SYSTEM.md"), "global").unwrap();
    let grants = PromptReadGrants {
        project: Some(project),
        global: Some(global),
    };
    let snapshot = PromptResourceFactory::default().load(&grants).unwrap();
    assert_eq!(snapshot.system.as_ref().unwrap().text, "");
    assert_eq!(snapshot.compose("default"), "default");
    let explicit = PromptResourceFactory {
        system: Some(String::new()),
        append: Some(vec!["append".into()]),
    };
    assert_eq!(
        explicit.load(&grants).unwrap().compose("default"),
        "default\n\nappend"
    );
}

#[cfg(unix)]
#[test]
fn fifo_prompt_is_rejected_without_blocking() {
    use std::os::unix::ffi::OsStrExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    const CHILD: &str = "PI_RS_PROMPT_FIFO_TEST_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let grants = PromptReadGrants {
            global: Some(directory.into()),
            project: None,
        };
        let error = PromptResourceFactory::default().load(&grants).unwrap_err();
        assert!(error.to_string().contains("not a regular file"));
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("SYSTEM.md");
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fifo_prompt_is_rejected_without_blocking"])
        .env(CHILD, temp.path())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "FIFO check subprocess failed: {status}");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("opening FIFO blocked");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn context_snapshots_use_granted_order_and_pi_filename_precedence() {
    use pi_rs_workbench::prompt_resources::ContextReadGrants;
    let root = tempfile::tempdir().unwrap();
    let global = root.path().join("global");
    let project = root.path().join("project");
    std::fs::create_dir_all(&global).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir(global.join("AGENTS.override.md")).unwrap();
    std::fs::write(global.join("CLAUDE.md"), "global").unwrap();
    std::fs::write(project.join("AGENTS.md"), "normal").unwrap();
    std::fs::write(project.join("AGENTS.override.md"), "\u{feff}override").unwrap();
    let grants = ContextReadGrants {
        directories: vec![global.clone(), project.clone(), global.clone()],
    };
    let snapshot = grants.load().unwrap();
    assert_eq!(
        snapshot
            .files
            .iter()
            .map(|file| file.text.as_str())
            .collect::<Vec<_>>(),
        ["global", "override"]
    );
    assert!(snapshot.render().contains(&format!(
        "<project_instructions path=\"{}\">\noverride\n</project_instructions>",
        project.join("AGENTS.override.md").display()
    )));
    std::fs::remove_file(project.join("AGENTS.override.md")).unwrap();
    assert_eq!(grants.load().unwrap().files[1].text, "normal");
    assert_eq!(snapshot.files[1].text, "override");
    assert!(
        ContextReadGrants::default()
            .load()
            .unwrap()
            .render()
            .is_empty()
    );
}

#[test]
fn context_read_failure_does_not_return_partial_snapshot() {
    use pi_rs_workbench::prompt_resources::ContextReadGrants;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("AGENTS.md"), [0xff]).unwrap();
    assert!(
        ContextReadGrants {
            directories: vec![root.path().to_owned()]
        }
        .load()
        .is_err()
    );
}

#[test]
fn nested_worktree_context_shadows_matching_main_filename_only() {
    use pi_rs_workbench::prompt_resources::ContextReadGrants;
    let temp = tempfile::tempdir().unwrap();
    let main = temp.path().join("main");
    let nested = main.join("worktrees/feature");
    let other = main.join("worktrees");
    let sibling = temp.path().join("sibling");
    for dir in [&main, &nested, &other, &sibling] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(main.join("AGENTS.md"), "main").unwrap();
    std::fs::write(nested.join("AGENTS.md"), "worktree").unwrap();
    std::fs::write(other.join("AGENTS.md"), "ancestor").unwrap();
    std::fs::write(sibling.join("AGENTS.md"), "sibling").unwrap();
    let grants = ContextReadGrants {
        directories: vec![main.clone(), other.clone(), nested.clone(), sibling.clone()],
    };
    let snapshot = grants
        .load_with_nested_worktree(&main, &nested, None)
        .unwrap();
    assert_eq!(
        snapshot
            .files
            .iter()
            .map(|f| f.text.as_str())
            .collect::<Vec<_>>(),
        ["ancestor", "worktree", "sibling"]
    );
    assert_eq!(
        grants
            .load_with_nested_worktree(&main, &sibling, None)
            .unwrap()
            .files
            .len(),
        4
    );
    assert!(
        grants
            .load_with_nested_worktree(&main, temp.path(), None)
            .is_err()
    );
    std::fs::remove_file(nested.join("AGENTS.md")).unwrap();
    std::fs::write(nested.join("CLAUDE.md"), "different").unwrap();
    assert_eq!(
        grants
            .load_with_nested_worktree(&main, &nested, None)
            .unwrap()
            .files
            .len(),
        4
    );
}

#[cfg(unix)]
#[test]
fn worktree_context_symlink_shadows_main_by_directory_not_file_target() {
    use pi_rs_workbench::prompt_resources::ContextReadGrants;
    let temp = tempfile::tempdir().unwrap();
    let main = temp.path().join("main");
    let nested = main.join("worktrees/feature");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(main.join("AGENTS.md"), "instructions").unwrap();
    std::os::unix::fs::symlink(main.join("AGENTS.md"), nested.join("AGENTS.md")).unwrap();
    let grants = ContextReadGrants {
        directories: vec![main.clone(), nested.clone()],
    };
    let result = grants
        .load_with_nested_worktree(&main, &nested, None)
        .unwrap();
    assert_eq!(result.files.len(), 1);
    assert_eq!(result.files[0].path, nested.join("AGENTS.md"));
}

#[cfg(unix)]
#[test]
fn worktree_shadow_preserves_separately_granted_main_alias() {
    use pi_rs_workbench::prompt_resources::ContextReadGrants;
    let temp = tempfile::tempdir().unwrap();
    let main = temp.path().join("main");
    let alias = temp.path().join("global");
    let nested = main.join("worktrees/feature");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(main.join("AGENTS.md"), "global and main").unwrap();
    std::fs::write(nested.join("AGENTS.md"), "worktree").unwrap();
    std::os::unix::fs::symlink(&main, &alias).unwrap();
    for directories in [
        vec![alias.clone(), main.clone(), nested.clone()],
        vec![main.clone(), alias.clone(), nested.clone()],
    ] {
        let result = ContextReadGrants { directories }
            .load_with_nested_worktree(&main, &nested, None)
            .unwrap();
        assert_eq!(
            result.files.iter().map(|f| &f.path).collect::<Vec<_>>(),
            [&alias.join("AGENTS.md"), &nested.join("AGENTS.md")]
        );
    }
}

#[test]
fn worktree_shadow_preserves_global_role_with_identical_path() {
    use pi_rs_workbench::prompt_resources::ContextReadGrants;
    let temp = tempfile::tempdir().unwrap();
    let main = temp.path().join("main");
    let nested = main.join("worktree");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(main.join("AGENTS.md"), "global").unwrap();
    std::fs::write(nested.join("AGENTS.md"), "worktree").unwrap();
    let grants = ContextReadGrants {
        directories: vec![main.clone(), main.clone(), nested.clone()],
    };
    let result = grants
        .load_with_nested_worktree(&main, &nested, Some(&main))
        .unwrap();
    assert_eq!(
        result
            .files
            .iter()
            .map(|f| f.text.as_str())
            .collect::<Vec<_>>(),
        ["global", "worktree"]
    );
    assert!(
        grants
            .load_with_nested_worktree(&main, &nested, Some(temp.path()))
            .is_err()
    );
}

#[test]
fn explicit_prompt_files_require_grants_and_preserve_snapshots() {
    use pi_rs_workbench::prompt_resources::{
        ExplicitPromptResourceFactory, PromptInput, PromptReadGrants,
    };
    let root = tempfile::tempdir().unwrap();
    let system = root.path().join("system.md");
    let appendix = root.path().join("append.md");
    std::fs::write(&system, "\u{feff}system").unwrap();
    std::fs::write(&appendix, "appendix").unwrap();
    let factory = ExplicitPromptResourceFactory {
        system: Some(PromptInput::File(system.clone())),
        append: Some(vec![
            PromptInput::Literal("literal".into()),
            PromptInput::File(appendix.clone()),
        ]),
    };
    let grants = PromptReadGrants::default();
    assert!(
        factory
            .load(&grants, std::slice::from_ref(&system))
            .is_err()
    );
    let files = vec![system.clone(), appendix.clone()];
    let snapshot = factory.load(&grants, &files).unwrap();
    assert_eq!(snapshot.system.as_ref().unwrap().text, "system");
    assert_eq!(
        snapshot.system.as_ref().unwrap().source.as_ref(),
        Some(&system)
    );
    assert_eq!(
        snapshot
            .append
            .iter()
            .map(|p| p.text.as_str())
            .collect::<Vec<_>>(),
        ["literal", "appendix"]
    );
    assert_eq!(snapshot.append[1].source.as_ref(), Some(&appendix));
    std::fs::remove_file(&appendix).unwrap();
    assert!(factory.load(&grants, &files).is_err());
    assert_eq!(snapshot.append[1].text, "appendix");
    std::fs::write(&appendix, [0xff]).unwrap();
    assert!(factory.load(&grants, &files).is_err());
    std::fs::write(&appendix, vec![b'a'; 256 * 1024 + 1]).unwrap();
    assert!(factory.load(&grants, &files).is_err());
    let relative = ExplicitPromptResourceFactory {
        system: Some(PromptInput::File("relative.md".into())),
        append: None,
    };
    assert!(relative.load(&grants, &["relative.md".into()]).is_err());
    let traversal = root.path().join("../system.md");
    let invalid = ExplicitPromptResourceFactory {
        system: Some(PromptInput::File(traversal.clone())),
        append: None,
    };
    assert!(invalid.load(&grants, &[traversal]).is_err());
}

#[test]
fn explicit_prompt_inputs_enforce_aggregate_limits() {
    use pi_rs_workbench::prompt_resources::{
        ExplicitPromptResourceFactory, PromptInput, PromptReadGrants,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("large.md");
    std::fs::write(&path, vec![b'a'; 256 * 1024]).unwrap();
    let mut factory = ExplicitPromptResourceFactory {
        system: None,
        append: Some(vec![PromptInput::File(path.clone()); 16]),
    };
    let grants = PromptReadGrants::default();
    let files = [path.clone()];
    assert_eq!(factory.load(&grants, &files).unwrap().append.len(), 16);
    factory
        .append
        .as_mut()
        .unwrap()
        .push(PromptInput::File(path));
    assert!(factory.load(&grants, &files).is_err());
    factory.append = Some(vec![PromptInput::Literal("a".repeat(4 * 1024 * 1024 + 1))]);
    assert!(factory.load(&grants, &[]).is_err());
    factory.append = Some(vec![PromptInput::Literal(String::new()); 128]);
    assert_eq!(factory.load(&grants, &[]).unwrap().append.len(), 128);
    factory
        .append
        .as_mut()
        .unwrap()
        .push(PromptInput::Literal(String::new()));
    assert!(factory.load(&grants, &[]).is_err());
}
