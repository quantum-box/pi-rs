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
