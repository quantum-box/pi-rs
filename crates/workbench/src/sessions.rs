//! Local session presentation, snapshot exports, and independent branch copies.
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use pi_rs::{Error, Event, Result, Session};
use serde::Serialize;

#[derive(Serialize)]
pub struct SessionSummary {
    pub path: PathBuf,
    pub name: Option<String>,
    pub cwd: Option<String>,
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
        head: session.head(),
        messages: 0,
        input_tokens: 0,
        output_tokens: 0,
    };
    for record in session.branch() {
        match &record.event {
            Event::User { .. } | Event::Assistant { .. } => summary.messages += 1,
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

pub fn fork(session: &Session, target: u64, destination: &Path, workspace: &Path) -> Result<()> {
    let mut copy = Session::from_records(session.fork_records(target)?)?;
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
        "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'\"><title>{title}</title><style>body{{max-width:960px;margin:40px auto;padding:0 20px;font:16px system-ui;background:#16181d;color:#e4e6eb}}article,details{{padding:16px;margin:12px 0;border-radius:8px;background:#252932}}.user{{background:#243448}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;font:14px ui-monospace,monospace}}summary{{cursor:pointer}}.thinking{{color:#a5abb6}}h1{{font-size:24px}}</style><h1>{title}</h1>"
    );
    for record in session.branch() {
        match &record.event {
            Event::User { content, .. } => html.push_str(&format!(
                "<article class=\"user\"><b>You</b><pre>{}</pre></article>",
                escape(content)
            )),
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
            Event::ToolFinished { outcome, .. } | Event::ToolReconciled { outcome, .. } => html
                .push_str(&format!(
                    "<details><summary>Tool result</summary><pre>{}</pre></details>",
                    escape(&outcome.tool_content())
                )),
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
    fn exports_and_forks_preserve_session_and_escape_html() {
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(directory.path().join("source.jsonl")).unwrap();
        session.set_name("<script>alert('name')</script>").unwrap();
        let exported = directory.path().join("export.jsonl");
        export(&session, &exported).unwrap();
        assert!(export(&session, &exported).is_err());
        assert!(html(&session).contains("&lt;script&gt;"));
        assert!(!html(&session).contains("<script>"));
        let copy = directory.path().join("fork.jsonl");
        fork(&session, session.head(), &copy, directory.path()).unwrap();
        let copy = Session::open(copy).unwrap();
        assert_eq!(copy.name(), session.name());
        assert_ne!(
            serde_json::to_value(&copy.records()[0]).unwrap(),
            serde_json::to_value(&session.records()[0]).unwrap()
        );
        let imported = directory.path().join("import.jsonl");
        import(&exported, &imported).unwrap();
        assert_eq!(Session::open(imported).unwrap().name(), session.name());
    }
}
