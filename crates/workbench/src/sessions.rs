//! Local session presentation, snapshot exports, and independent branch copies.
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use pi_rs::{Error, Event, Result, Session};
use serde::Serialize;

pub fn validate_name(value: &str) -> std::result::Result<String, String> {
    let name = pi_rs::sanitize_session_name(value);
    if name.is_empty() {
        return Err("Session name cannot be empty".into());
    }
    Ok(name)
}

pub fn validate_id(id: &str) -> std::result::Result<String, String> {
    let valid = !id.is_empty()
        && id.len() <= 128
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id.as_bytes()[id.len() - 1].is_ascii_alphanumeric()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(id.to_owned())
    } else {
        Err("session ID must be 1-128 ASCII letters, numbers, dots, underscores or hyphens, beginning and ending with a letter or number".into())
    }
}

/// Resolve a stored filename or journal ID without opening it for mutation.
pub fn resolve_reference(input: &Path, directory: &Path) -> Result<PathBuf> {
    if input.is_file() || input.components().count() != 1 {
        return Ok(input.to_path_buf());
    }
    let Some(query) = input.to_str().filter(|query| !query.is_empty()) else {
        return Ok(input.to_path_buf());
    };
    if !directory.is_dir() {
        return Ok(input.to_path_buf());
    }
    let mut matches = Vec::new();
    let mut exact_ids = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path
            .extension()
            .is_none_or(|extension| extension != "jsonl")
            || !path.is_file()
        {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default();
        if stem == query {
            return Ok(path);
        }
        let id = stored_id(&path).unwrap_or_default();
        if id == query {
            exact_ids.push(path.clone());
        }
        if stem.starts_with(query) || id.starts_with(query) {
            matches.push(path);
        }
    }
    if !exact_ids.is_empty() {
        matches = exact_ids;
    }
    match matches.len() {
        0 => Ok(input.to_path_buf()),
        1 => Ok(matches.remove(0)),
        _ => Err(Error::Config(format!(
            "ambiguous session ID {query:?}; use a longer ID or a file path"
        ))),
    }
}

/// Include the active storage directory and other workspace groups in local storage.
pub fn stored_paths(current: &Path) -> Result<Vec<PathBuf>> {
    let root = super::config::agent_dir().join("sessions");
    let mut directories = vec![current.to_path_buf()];
    if root.is_dir() {
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && !directories.contains(&entry.path()) {
                directories.push(entry.path());
            }
        }
    }
    let mut paths = Vec::new();
    for directory in directories.into_iter().filter(|path| path.is_dir()) {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "jsonl")
            {
                paths.push(entry.path());
            }
        }
    }
    sort_by_activity(&mut paths);
    Ok(paths)
}

fn sort_by_activity(paths: &mut [PathBuf]) {
    paths.sort_by_cached_key(|path| {
        std::cmp::Reverse(
            super::session_import::read(path)
                .ok()
                .and_then(|session| session.activity_time())
                .or_else(|| {
                    path.metadata()
                        .and_then(|metadata| metadata.modified())
                        .ok()
                }),
        )
    });
}

fn stored_id(path: &Path) -> Option<String> {
    use std::io::{BufRead, Read};
    let mut line = String::new();
    let mut reader = std::io::BufReader::new(std::fs::File::open(path).ok()?).take(1024 * 1024);
    reader.read_line(&mut line).ok()?;
    let record: serde_json::Value = serde_json::from_str(&line).ok()?;
    record["event"]["session_id"]
        .as_str()
        .or_else(|| record["id"].as_str())
        .map(str::to_owned)
}

#[derive(Serialize)]
pub struct SessionSummary {
    pub path: PathBuf,
    pub name: Option<String>,
    pub cwd: Option<String>,
    pub parent_session: Option<String>,
    pub head: u64,
    pub messages: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
}
pub fn summary(path: &Path, session: &Session) -> SessionSummary {
    let mut summary = SessionSummary {
        path: path.into(),
        name: session.name().map(str::to_owned),
        cwd: session.working_directory().map(str::to_owned),
        parent_session: session.parent_session().map(str::to_owned),
        head: session.head(),
        messages: 0,
        input_tokens: 0,
        output_tokens: 0,
    };
    for record in session.branch() {
        match &record.event {
            Event::User { .. } | Event::UserWithImages { .. } | Event::Assistant { .. } => {
                summary.messages += 1
            }
            Event::ModelFinished {
                usage: pi_rs::UsageReport::Measured { usage },
                ..
            } => {
                summary.input_tokens = summary.input_tokens.saturating_add(usage.input_tokens);
                summary.output_tokens = summary.output_tokens.saturating_add(usage.output_tokens);
            }
            _ => {}
        }
    }
    summary
}

pub fn fork(
    session: &Session,
    source: &Path,
    target: u64,
    destination: &Path,
    workspace: &Path,
) -> Result<()> {
    let mut copy = Session::from_records(session.fork_records(target)?)?;
    copy.set_parent_session(&source.to_string_lossy())?;
    copy.set_working_directory(workspace)?;
    if let Some(name) = session.name() {
        copy.set_name(name)?;
    }
    copy.export_jsonl(destination)
}

pub fn import(source: &Path, destination: &Path) -> Result<()> {
    if !source.is_file() {
        return Err(Error::Config("session file not found".into()));
    }
    let source = super::session_import::read(source)?;
    if source.host_binding().is_some() {
        return Err(Error::Session(
            "managed sessions must be imported by their host".into(),
        ));
    }
    source.export_jsonl(destination)
}

pub fn export(session: &Session, destination: &Path) -> Result<()> {
    if destination
        .extension()
        .is_some_and(|extension| extension == "jsonl")
    {
        return session.export_jsonl(destination);
    }
    if destination.exists() {
        return Err(Error::Config("export destination already exists".into()));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    match destination
        .extension()
        .and_then(|extension| extension.to_str())
    {
        Some("html" | "htm") => {
            temporary.write_all(html(session).as_bytes())?;
            temporary.as_file().sync_all()?;
        }
        _ => {
            return Err(Error::Config(
                "export filename must end in .html or .jsonl".into(),
            ));
        }
    }
    temporary
        .persist_noclobber(destination)
        .map_err(|error| Error::Io(error.error))?;
    sync_directory(parent)
}
fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
pub fn html(session: &Session) -> String {
    let title = escape(session.name().unwrap_or("Pi session"));
    let mut html = format!(
        "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; img-src data:; style-src 'unsafe-inline'\"><title>{title}</title><style>body{{max-width:960px;margin:40px auto;padding:0 20px;font:16px system-ui;background:#16181d;color:#e4e6eb}}article,details{{padding:16px;margin:12px 0;border-radius:8px;background:#252932}}.user{{background:#243448}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;font:14px ui-monospace,monospace}}summary{{cursor:pointer}}.thinking{{color:#a5abb6}}h1{{font-size:24px}}</style><h1>{title}</h1>"
    );
    for record in session.branch() {
        match &record.event {
            Event::User { content, .. } => html.push_str(&format!(
                "<article class=\"user\"><b>You</b><pre>{}</pre></article>",
                escape(content)
            )),
            Event::UserWithImages {
                content, images, ..
            } => html.push_str(&format!(
                "<article class=\"user\"><b>You</b><pre>{}</pre>{}</article>",
                escape(content),
                images_html(images)
            )),
            Event::UserImages { images } => html.push_str(&images_html(images)),
            Event::Assistant {
                content,
                invocations,
                ..
            } => {
                if !content.is_empty() {
                    html.push_str(&format!(
                        "<article><b>Pi</b><pre>{}</pre></article>",
                        escape(content)
                    ));
                }
                for invocation in invocations {
                    html.push_str(&format!(
                        "<details><summary>{}</summary><pre>{}</pre></details>",
                        escape(&invocation.call.name),
                        escape(&invocation.call.arguments)
                    ));
                }
            }
            Event::ToolFinished { outcome, .. } | Event::ToolReconciled { outcome, .. } => {
                html.push_str(&tool_html(outcome))
            }
            Event::ModelFinished { metadata, .. } => {
                if let Some(reasoning) = &metadata.reasoning {
                    html.push_str(&format!("<details class=\"thinking\"><summary>Thinking</summary><pre>{}</pre></details>", escape(reasoning)));
                }
            }
            Event::Compacted { summary, .. } => html.push_str(&format!(
                "<details><summary>Compacted context</summary><pre>{}</pre></details>",
                escape(summary)
            )),
            _ => {}
        }
    }
    html.push_str("</html>");
    html
}

fn images_html(images: &[pi_rs::ImageContent]) -> String {
    images.iter().filter(|image| image.validate().is_ok()).map(|image| format!(
        "<img alt=\"Attached image\" style=\"max-width:100%;height:auto\" src=\"data:{};base64,{}\">", image.mime_type, image.data
    )).collect()
}
fn tool_html(outcome: &pi_rs::Outcome) -> String {
    let mut value = serde_json::to_value(outcome).expect("serializable outcome");
    let mut images = Vec::new();
    if let Some(blocks) = value
        .get_mut("data")
        .and_then(|data| data.get_mut("content"))
        .and_then(serde_json::Value::as_array_mut)
    {
        for block in blocks.iter_mut().filter(|block| block["type"] == "image") {
            if let Ok(image) = serde_json::from_value::<pi_rs::ImageContent>(block.clone()) {
                images.push(image);
            }
            block["data"] = serde_json::json!("[image attached]");
        }
    }
    let text = if images.is_empty() {
        outcome.tool_content()
    } else {
        value.to_string()
    };
    format!(
        "<details><summary>Tool result</summary><pre>{}</pre>{}</details>",
        escape(&text),
        images_html(&images)
    )
}

pub fn copy_last(session: &Session) -> Result<()> {
    let text = session
        .branch()
        .into_iter()
        .rev()
        .find_map(|record| match &record.event {
            Event::Assistant { content, .. } if !content.is_empty() => Some(content.as_str()),
            _ => None,
        })
        .ok_or_else(|| Error::Config("no assistant response to copy".into()))?;
    copy_text(text)
}

pub fn tree_record_text(session: &Session, id: u64) -> Result<String> {
    use std::borrow::Cow;

    let text = session
        .records()
        .get(id.saturating_sub(1) as usize)
        .filter(|record| record.id == id)
        .and_then(|record| match &record.event {
            Event::User { content, .. }
            | Event::UserWithImages { content, .. }
            | Event::Assistant { content, .. } => Some(Cow::Borrowed(content.as_str())),
            Event::BashExecution { execution } => Some(Cow::Borrowed(execution.command.as_str())),
            Event::ToolFinished {
                outcome: pi_rs::Outcome::Structured { data, .. },
                ..
            }
            | Event::ToolReconciled {
                outcome: pi_rs::Outcome::Structured { data, .. },
                ..
            } => data["content"]
                .as_array()
                .map(|blocks| {
                    Cow::Owned(
                        blocks
                            .iter()
                            .filter(|block| block["type"] == "text")
                            .filter_map(|block| block["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n"),
                    )
                })
                .or_else(|| data["output"].as_str().map(Cow::Borrowed)),
            Event::ToolFinished { outcome, .. } | Event::ToolReconciled { outcome, .. } => {
                Some(Cow::Owned(outcome.tool_content()))
            }
            Event::ModelFinished {
                failure: Some(_),
                partial,
                ..
            } => Some(Cow::Borrowed(partial.as_str())),
            Event::Compacted { summary, .. } => Some(Cow::Borrowed(summary.as_str())),
            _ => None,
        })
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::Config("selected entry has no text to copy".into()))?;
    Ok(text.into_owned())
}

pub fn copy_text(text: &str) -> Result<()> {
    let candidates: &[(&str, &[&str])] = if cfg!(target_os = "macos") {
        &[("pbcopy", &[])]
    } else if cfg!(windows) {
        &[("clip", &[])]
    } else {
        &[
            ("wl-copy", &[]),
            ("xclip", &["-selection", "clipboard"]),
            ("xsel", &["--clipboard", "--input"]),
        ]
    };
    for (program, arguments) in candidates {
        let child = Command::new(program)
            .args(*arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let result = child
            .stdin
            .take()
            .expect("piped clipboard")
            .write_all(text.as_bytes());
        let status = child.wait()?;
        result?;
        if status.success() {
            return Ok(());
        }
    }
    Err(Error::Config("no working clipboard command found".into()))
}

pub fn share_gist(session: &Session) -> Result<String> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("session.html");
    export(session, &file)?;
    let output = Command::new("gh")
        .args([
            "gist",
            "create",
            "--desc",
            session.name().unwrap_or("Pi session"),
        ])
        .arg(&file)
        .output()?;
    if !output.status.success() {
        return Err(Error::Config(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_ids_and_prefixes_resolve_without_changing_journals() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("work-1.jsonl");
        let session = Session::open(&path).unwrap();
        let id = session.session_id().to_string();
        drop(session);
        let before = std::fs::read(&path).unwrap();
        assert_eq!(
            resolve_reference(Path::new("work-1"), root.path()).unwrap(),
            path
        );
        assert_eq!(
            resolve_reference(Path::new(&id[..12]), root.path()).unwrap(),
            path
        );
        std::fs::copy(&path, root.path().join("work-2.jsonl")).unwrap();
        assert!(resolve_reference(Path::new("work-"), root.path()).is_err());
        assert!(resolve_reference(Path::new(&id), root.path()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        for invalid in [
            "",
            "../escape",
            ".hidden",
            "trailing-",
            "two words",
            "日本語",
        ] {
            assert!(validate_id(invalid).is_err());
        }
        for valid in ["one", "Run_2.3", "a-b"] {
            assert!(validate_id(valid).is_ok());
        }
    }

    #[test]
    fn session_order_uses_conversation_activity_instead_of_file_copy_time() {
        let directory = tempfile::tempdir().unwrap();
        let older = directory.path().join("older.jsonl");
        let newer = directory.path().join("newer.jsonl");
        for (path, timestamp) in [
            (&older, "2020-01-01T00:00:00.000Z"),
            (&newer, "2021-01-01T00:00:00.000Z"),
        ] {
            let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
            session
                .append_user_with_images("hello".into(), vec![], None)
                .unwrap();
            let mut records = session.records().to_vec();
            records[0].timestamp = Some("2019-01-01T00:00:00.000Z".into());
            records[1].timestamp = Some(timestamp.into());
            Session::from_records(records)
                .unwrap()
                .export_jsonl(path)
                .unwrap();
        }
        let late = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2_000_000_000);
        std::fs::File::options()
            .write(true)
            .open(&older)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(late))
            .unwrap();
        let mut paths = vec![older.clone(), newer.clone()];
        sort_by_activity(&mut paths);
        assert_eq!(paths, vec![newer, older]);
    }

    #[test]
    fn exports_and_forks_preserve_session_and_escape_html() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.jsonl");
        let mut session = Session::open(&source).unwrap();
        session.set_parent_session("ancestor.jsonl").unwrap();
        session.set_name("<script>alert('name')</script>").unwrap();
        let exported = directory.path().join("export.jsonl");
        export(&session, &exported).unwrap();
        assert!(export(&session, &exported).is_err());
        assert!(html(&session).contains("&lt;script&gt;"));
        assert!(!html(&session).contains("<script>"));
        let copy = directory.path().join("fork.jsonl");
        fork(&session, &source, session.head(), &copy, directory.path()).unwrap();
        let copy = Session::open(copy).unwrap();
        assert_eq!(copy.name(), session.name());
        assert_eq!(
            copy.parent_session(),
            Some(source.to_string_lossy().as_ref())
        );
        assert_ne!(
            serde_json::to_value(&copy.records()[0]).unwrap(),
            serde_json::to_value(&session.records()[0]).unwrap()
        );
        let imported = directory.path().join("import.jsonl");
        import(&exported, &imported).unwrap();
        assert_eq!(Session::open(imported).unwrap().name(), session.name());
    }
    #[test]
    fn html_embeds_user_images_with_a_data_only_image_policy() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .append_user_with_images(
                "look".into(),
                vec![pi_rs::ImageContent {
                    data: "aW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                }],
                None,
            )
            .unwrap();
        let document = html(&session);
        assert!(document.contains("img-src data:"));
        assert!(document.contains("src=\"data:image/png;base64,aW1hZ2U=\""));
        assert!(!document.contains("https://"));
    }
}
