//! Local bug reports from an explicitly selected session snapshot.
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use pi_rs::{Error, Event, ModelApi, ModelFailure, Result, Session};
use serde::Serialize;
use uuid::Uuid;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

const SCHEMA_VERSION: u32 = 1;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report<'a> {
    schema_version: u32,
    id: Uuid,
    created_at: String,
    description: Option<&'a str>,
    version: &'static str,
    platform: &'static str,
    architecture: &'static str,
    session_id: Uuid,
    transcript_included: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Diagnostics {
    schema_version: u32,
    session_id: Uuid,
    branch_head: u64,
    branch_records: usize,
    user_messages: usize,
    assistant_messages: usize,
    model_requests: usize,
    failed_requests: Vec<FailedRequest>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailedRequest {
    record_id: u64,
    failure: ModelFailure,
    api: Option<ModelApi>,
}

impl Diagnostics {
    fn collect(session: &Session) -> Self {
        let branch = session.branch();
        let mut diagnostics = Self {
            schema_version: SCHEMA_VERSION,
            session_id: session.session_id(),
            branch_head: session.head(),
            branch_records: branch.len(),
            user_messages: 0,
            assistant_messages: 0,
            model_requests: 0,
            failed_requests: Vec::new(),
        };
        for record in branch {
            match &record.event {
                Event::User { .. } | Event::UserWithImages { .. } => diagnostics.user_messages += 1,
                Event::Assistant { .. } => diagnostics.assistant_messages += 1,
                Event::ModelRequested { .. } => diagnostics.model_requests += 1,
                Event::ModelFinished {
                    metadata,
                    failure: Some(failure),
                    ..
                } => diagnostics.failed_requests.push(FailedRequest {
                    record_id: record.id,
                    failure: *failure,
                    api: metadata.provider_api,
                }),
                _ => {}
            }
        }
        diagnostics
    }
}

pub(super) fn write(
    directory: &Path,
    session: &Session,
    id: Uuid,
    description: &str,
    include_transcript: bool,
) -> Result<PathBuf> {
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "managed sessions must be exported by their host".into(),
        ));
    }
    let directory = directory.canonicalize()?;
    let destination = directory.join(format!("pi-bug-report-{id}.zip"));
    let description = description.trim();
    let report = Report {
        schema_version: SCHEMA_VERSION,
        id,
        created_at: chrono::Utc::now().to_rfc3339(),
        description: (!description.is_empty()).then_some(description),
        version: env!("CARGO_PKG_VERSION"),
        platform: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
        session_id: session.session_id(),
        transcript_included: include_transcript,
    };

    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    let mut archive = ZipWriter::new(temporary.as_file_mut());
    write_json(&mut archive, "report.json", &report)?;
    write_json(
        &mut archive,
        "diagnostics.json",
        &Diagnostics::collect(session),
    )?;
    if include_transcript {
        let mut staging = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            staging.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let staging = staging.prefix(".pi-bug-report-").tempdir_in(&directory)?;
        let transcript = staging.path().join("session.jsonl");
        session
            .fork_session(session.head())?
            .export_jsonl(&transcript)?;
        let mut file = File::open(transcript)?;
        archive
            .start_file(
                "session.jsonl",
                file_options().large_file(file.metadata()?.len() >= u32::MAX as u64),
            )
            .map_err(io::Error::from)?;
        io::copy(&mut file, &mut archive)?;
    }
    archive.finish().map_err(io::Error::from)?.sync_all()?;
    temporary
        .persist_noclobber(&destination)
        .map_err(|error| Error::Io(error.error))?;
    Ok(destination)
}

fn file_options() -> SimpleFileOptions {
    SimpleFileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o600)
}

fn write_json(
    archive: &mut ZipWriter<&mut File>,
    name: &str,
    value: &impl Serialize,
) -> Result<()> {
    archive
        .start_file(name, file_options())
        .map_err(io::Error::from)?;
    serde_json::to_writer_pretty(&mut *archive, value)?;
    archive.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bug_report_respects_workspace_and_managed_session_grants() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let id = Uuid::new_v4();
        let destination = workspace.join(format!("pi-bug-report-{id}.zip"));
        let outside = root.path().join("outside");
        std::fs::write(&outside, "untouched").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &destination).unwrap();
        #[cfg(not(unix))]
        std::fs::write(&destination, "occupied").unwrap();

        assert!(matches!(
            write(&workspace, &session, id, "../../outside", false),
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists
        ));
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "untouched");
        assert_eq!(workspace.read_dir().unwrap().count(), 1);
        std::fs::remove_file(&destination).unwrap();
        assert_eq!(
            write(&workspace, &session, id, "../../outside", false).unwrap(),
            workspace
                .canonicalize()
                .unwrap()
                .join(destination.file_name().unwrap())
        );
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "untouched");

        let missing = root.path().join("missing");
        assert!(write(&missing, &session, id, "", false).is_err());
        assert!(!missing.exists());

        let mut managed = session;
        managed.bind_host("host-scope").unwrap();
        for include_transcript in [false, true] {
            assert!(matches!(
                write(&workspace, &managed, Uuid::new_v4(), "", include_transcript),
                Err(Error::Session(_))
            ));
        }
        assert_eq!(workspace.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn bug_report_diagnostics_allowlist_excludes_raw_failure_data() {
        let session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let mut records = session.records().to_vec();
        let version = records[0].version;
        let request_id = Uuid::new_v4();
        records.extend([
            pi_rs::Record {
                timestamp: None,
                run_id: None,
                version,
                id: 2,
                parent: Some(1),
                event: Event::ModelRequested {
                    model: "private-model".into(),
                    messages: 0,
                    tools: 0,
                    max_output_tokens: 16,
                    request_id: Some(request_id),
                    context_head: None,
                    context_sha256: None,
                    context_transformed: None,
                    context_configuration_sha256: None,
                    tool_decision: None,
                },
            },
            pi_rs::Record {
                timestamp: None,
                run_id: None,
                version,
                id: 3,
                parent: Some(2),
                event: Event::ModelFinished {
                    request_id,
                    metadata: pi_rs::ModelMetadata {
                        provider_api: Some(ModelApi::OpenaiCompletions),
                        provider_request_id: Some("private-provider-request".into()),
                        ..Default::default()
                    },
                    usage: pi_rs::UsageReport::Unconfirmed,
                    failure: Some(ModelFailure::Retryable),
                    partial: "private-partial-response".into(),
                    reply: None,
                },
            },
        ]);
        let session = Session::from_records(records).unwrap();
        let diagnostics = serde_json::to_value(Diagnostics::collect(&session)).unwrap();
        assert_eq!(
            diagnostics,
            serde_json::json!({
                "schemaVersion": 1,
                "sessionId": session.session_id(),
                "branchHead": 3,
                "branchRecords": 3,
                "userMessages": 0,
                "assistantMessages": 0,
                "modelRequests": 1,
                "failedRequests": [
                    {"recordId": 3, "failure": "retryable", "api": "openai-completions"}
                ]
            })
        );
    }
}
