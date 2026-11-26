use std::sync::Arc;

use pi_rs::{CustomMessage, Model, ModelEvent, ModelRequest, ModelStream, Session, SessionBuilder};
use pi_rs_workbench::session_html;

struct FixtureModel;

#[async_trait::async_trait]
impl Model for FixtureModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs::Result<ModelStream> {
        Ok(Box::pin(futures_util::stream::iter([
            Ok(ModelEvent::Text("fixture answer".into())),
            Ok(ModelEvent::Complete {
                calls: Vec::new(),
                usage: None,
            }),
        ])))
    }
}

#[tokio::test]
async fn in_memory_sdk_export_uses_the_selected_branch_and_native_html() {
    let mut sdk = SessionBuilder::new(Arc::new(FixtureModel), "fixture")
        .in_memory()
        .unwrap();
    sdk.set_name("Report <draft>").unwrap();
    let root = sdk.journal().unwrap().head();
    sdk.prompt("other branch only").await.unwrap();
    sdk.checkout(root).unwrap();
    sdk.prompt("selected **branch** <script>unsafe literal</script>")
        .await
        .unwrap();
    sdk.append_custom_message(CustomMessage {
        custom_type: "host note".into(),
        content: pi_rs::ContextContent::Text("hidden native note".into()),
        display: false,
        details: None,
    })
    .unwrap();

    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("chosen.html");
    let document = {
        let journal = sdk.journal().unwrap();
        let error = session_html::export_agent_session(&sdk, &output).unwrap_err();
        assert!(error.to_string().contains("journal is in use"));
        assert!(!output.exists());
        session_html::render(&journal)
    };
    session_html::export_agent_session(&sdk, &output).unwrap();
    assert_eq!(std::fs::read_to_string(&output).unwrap(), document);
    assert!(document.contains("Report &lt;draft&gt;"));
    assert!(document.contains("default-src 'none'; img-src data:; style-src 'unsafe-inline'"));
    assert!(document.contains("selected <strong>branch</strong>"));
    assert!(document.contains("&lt;script&gt;unsafe literal&lt;/script&gt;"));
    assert!(document.contains("fixture answer"));
    assert!(document.contains("Hidden in terminal"));
    assert!(document.contains("hidden native note"));
    assert!(!document.contains("<script>"));
    assert!(!document.contains("other branch only"));

    let error = session_html::export_agent_session(&sdk, &output).unwrap_err();
    assert!(
        matches!(error, pi_rs::Error::Io(error) if error.kind() == std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(std::fs::read_to_string(output).unwrap(), document);
}

fn assert_destination_exists(result: pi_rs::Result<()>) {
    assert!(
        matches!(result, Err(pi_rs::Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists)
    );
}

#[tokio::test]
async fn file_backed_sdk_and_shared_export_only_create_new_files() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("session.jsonl");
    let sdk_output = directory.path().join("sdk.html");
    let shared_output = directory.path().join("shared.html");
    let mut sdk = SessionBuilder::new(Arc::new(FixtureModel), "fixture")
        .open(&source)
        .unwrap();
    sdk.prompt("disk transcript").await.unwrap();
    let original = std::fs::read(&source).unwrap();
    let shared = Session::snapshot(&source).unwrap();

    session_html::export_agent_session(&sdk, &sdk_output).unwrap();
    session_html::export(&shared, &shared_output).unwrap();
    let document = session_html::render(&shared);
    assert!(document.contains("disk transcript"));
    assert_eq!(std::fs::read_to_string(&sdk_output).unwrap(), document);
    assert_eq!(std::fs::read_to_string(&shared_output).unwrap(), document);

    std::fs::write(&sdk_output, "old SDK export").unwrap();
    std::fs::write(&shared_output, "old shared export").unwrap();
    assert_destination_exists(session_html::export_agent_session(&sdk, &sdk_output));
    assert_destination_exists(session_html::export(&shared, &shared_output));
    assert_eq!(
        std::fs::read_to_string(&sdk_output).unwrap(),
        "old SDK export"
    );
    assert_eq!(
        std::fs::read_to_string(&shared_output).unwrap(),
        "old shared export"
    );

    assert_destination_exists(session_html::export_agent_session(&sdk, &source));
    assert_destination_exists(session_html::export(&shared, &source));
    #[cfg(any(unix, windows))]
    {
        let alias = directory.path().join("alias.html");
        std::fs::hard_link(&source, &alias).unwrap();
        assert_destination_exists(session_html::export_agent_session(&sdk, &alias));
        assert_destination_exists(session_html::export(&shared, &alias));
        assert_eq!(std::fs::read(&alias).unwrap(), original);
    }
    assert_eq!(std::fs::read(&source).unwrap(), original);
}

#[test]
fn concurrent_sdk_and_shared_exports_publish_only_one_document() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("export.html");
    let ready = std::sync::Barrier::new(2);
    let sdk = SessionBuilder::new(Arc::new(FixtureModel), "fixture")
        .in_memory()
        .unwrap();
    let mut shared = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
    shared.set_name("Shared transcript").unwrap();
    let sdk_document = session_html::render(&sdk.journal().unwrap());
    let shared_document = session_html::render(&shared);

    let (sdk_result, shared_result) = std::thread::scope(|scope| {
        let ready = &ready;
        let output = &output;
        let sdk_export = scope.spawn(move || {
            ready.wait();
            session_html::export_agent_session(&sdk, output)
        });
        let shared_export = scope.spawn(move || {
            ready.wait();
            session_html::export(&shared, output)
        });
        (sdk_export.join().unwrap(), shared_export.join().unwrap())
    });

    let expected = if sdk_result.is_ok() {
        assert_destination_exists(shared_result);
        sdk_document
    } else {
        assert_destination_exists(sdk_result);
        shared_result.unwrap();
        shared_document
    };
    assert_eq!(std::fs::read_to_string(&output).unwrap(), expected);
}

#[cfg(unix)]
#[tokio::test]
async fn sdk_and_shared_export_protect_the_open_journal_after_its_old_name_is_reused() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("session.jsonl");
    let moved = directory.path().join("moved.jsonl");
    let mut sdk = SessionBuilder::new(Arc::new(FixtureModel), "fixture")
        .open(&source)
        .unwrap();
    std::fs::rename(&source, &moved).unwrap();
    std::fs::write(&source, "unrelated file").unwrap();
    sdk.set_name("Moved session").unwrap();
    let original = std::fs::read(&moved).unwrap();

    assert_destination_exists(session_html::export_agent_session(&sdk, &moved));
    {
        let journal = sdk.journal().unwrap();
        assert_destination_exists(session_html::export(&journal, &moved));
    }
    assert_eq!(std::fs::read(&moved).unwrap(), original);
    assert_eq!(std::fs::read_to_string(&source).unwrap(), "unrelated file");

    let output = directory.path().join("new.html");
    session_html::export_agent_session(&sdk, &output).unwrap();
    assert!(
        std::fs::read_to_string(&output)
            .unwrap()
            .contains("Moved session")
    );
    assert_eq!(std::fs::read(&moved).unwrap(), original);
}

#[cfg(unix)]
#[test]
fn export_leaves_output_untouched_if_opening_parent_for_sync_fails() {
    use std::fs::{self, File, Permissions};
    use std::io::ErrorKind;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("session.jsonl");
    let journal = Session::open(&source).unwrap();
    let parent = directory.path().join("write-only");
    fs::create_dir(&parent).unwrap();
    let output = parent.join("existing.html");
    fs::write(&output, "old export").unwrap();
    fs::set_permissions(&parent, Permissions::from_mode(0o300)).unwrap();

    // Privileged test processes can open a directory regardless of its mode.
    let open = File::open(&parent);
    let results = open.is_err().then(|| {
        let new_output = parent.join("new.html");
        let create = session_html::export(&journal, &new_output);
        let existing = session_html::export(&journal, &output);
        (create, existing, new_output)
    });
    fs::set_permissions(&parent, Permissions::from_mode(0o700)).unwrap();
    if let Some((create, existing, new_output)) = results {
        assert_eq!(open.unwrap_err().kind(), ErrorKind::PermissionDenied);
        for result in [create, existing] {
            assert!(
                matches!(result, Err(pi_rs::Error::Io(error)) if error.kind() == ErrorKind::PermissionDenied)
            );
        }
        assert!(!new_output.exists());
        assert_eq!(fs::read_to_string(&output).unwrap(), "old export");
    }
}
