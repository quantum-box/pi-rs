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
    let explicit_filename = input
        .extension()
        .is_some_and(|extension| extension == "jsonl");
    let Some(query) = input.to_str().filter(|query| !query.is_empty()) else {
        return Ok(input.to_path_buf());
    };
    let mut matches = Vec::new();
    let mut exact_ids = Vec::new();
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    for entry in entries.into_iter().flatten() {
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
        if !explicit_filename && (stem.starts_with(query) || id.starts_with(query)) {
            matches.push(path);
        }
    }
    if !exact_ids.is_empty() {
        matches = exact_ids;
    }
    match matches.len() {
        0 if explicit_filename => Ok(input.to_path_buf()),
        0 => Err(Error::Config(format!(
            "No session found matching '{query}'"
        ))),
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
pub(super) fn model_operation_usage(
    records: impl IntoIterator<Item = impl std::borrow::Borrow<pi_rs::Record>>,
) -> pi_rs_extensions::codemode::ModelUsage {
    use pi_rs_extensions::codemode::ModelUsage;
    let mut pending = std::collections::HashSet::new();
    let mut usage = ModelUsage::default();
    for record in records {
        match &record.borrow().event {
            Event::ToolPlanned { invocation, .. }
                if invocation.call.name == super::operation_models::TOOL =>
            {
                pending.insert(invocation.operation_id);
            }
            Event::ToolFinished {
                operation_id,
                outcome,
                ..
            }
            | Event::ToolReconciled {
                operation_id,
                outcome,
            } if pending.remove(operation_id) => {
                if let Some(measured) = ModelUsage::from_outcome(outcome) {
                    usage.add(&measured);
                }
            }
            _ => {}
        }
    }
    usage
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
            }
            | Event::CacheWarm { usage, .. } => {
                summary.input_tokens = summary.input_tokens.saturating_add(usage.input_tokens);
                summary.output_tokens = summary.output_tokens.saturating_add(usage.output_tokens);
            }
            _ => {}
        }
    }
    let operations = model_operation_usage(session.branch());
    summary.input_tokens = summary
        .input_tokens
        .saturating_add(operations.input)
        .saturating_add(operations.cache_read)
        .saturating_add(operations.cache_write);
    summary.output_tokens = summary.output_tokens.saturating_add(operations.output);
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

pub(super) fn default_html_destination(source: &Path) -> PathBuf {
    let basename = source.file_name().unwrap_or_default().to_string_lossy();
    let basename = basename.strip_suffix(".jsonl").unwrap_or(&basename);
    format!("pi-session-{basename}.html").into()
}

pub(super) fn export_html(session: &Session, source: &Path, destination: &Path) -> Result<()> {
    if let Ok(resolved) = destination.canonicalize()
        && resolved == source.canonicalize()?
    {
        return Err(Error::Config(
            "Cannot export HTML over the source session file".into(),
        ));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(html(session).as_bytes())?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(destination)
        .map_err(|error| Error::Io(error.error))?;
    sync_directory(parent)
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
    let mut imported_assistant = None;
    let mut has_system_prompt = false;
    for record in session.branch() {
        match &record.event {
            Event::SourceEntry { format, entry } => {
                imported_assistant = (format == "pi"
                    && entry["type"] == "message"
                    && entry["message"]["role"] == "assistant")
                    .then_some(&entry["message"]);
            }
            Event::User { content, .. } => render_user_html(&mut html, content, &[]),
            Event::UserWithImages {
                content, images, ..
            } => render_user_html(&mut html, content, images),
            Event::UserImages { images } => html.push_str(&images_html(images)),
            Event::CustomMessage { message } | Event::CustomMessageDelivered { message, .. } => {
                // Older unmarked journals are ambiguous when a native message
                // exactly matches the legacy source. Prefer hiding that content.
                let legacy = {
                    record
                        .parent
                        .and_then(|id| id.checked_sub(1))
                        .and_then(|index| usize::try_from(index).ok())
                        .and_then(|index| session.records().get(index))
                        .is_some_and(|parent| match &parent.event {
                            Event::SourceEntry { format, entry }
                                if format == "pi"
                                    && entry["type"] == "message"
                                    && matches!(
                                        entry["message"]["role"].as_str(),
                                        Some("custom" | "hookMessage")
                                    ) =>
                            {
                                super::session_import::imported_custom_message(
                                    &entry["message"],
                                    true,
                                )
                                .ok()
                                .is_some_and(|original| {
                                    serde_json::to_value(original)
                                        .ok()
                                        .zip(serde_json::to_value(message).ok())
                                        .is_some_and(|(original, message)| original == message)
                                })
                            }
                            _ => false,
                        })
                };
                let show = message.display || !legacy;
                if !show {
                    continue;
                }
                html.push_str(&format!("<article><b>{}</b>", escape(&message.custom_type)));
                if !message.display {
                    html.push_str(" <span>Hidden in terminal</span>");
                }
                match &message.content {
                    pi_rs::ContextContent::Text(text) => render_export_markdown(&mut html, text),
                    pi_rs::ContextContent::Blocks(blocks) => {
                        for block in blocks {
                            match block {
                                pi_rs::ContentBlock::Text { text, .. } => {
                                    render_export_markdown(&mut html, text)
                                }
                                pi_rs::ContentBlock::Image { data, mime_type } => {
                                    html.push_str(&images_html(&[pi_rs::ImageContent {
                                        data: data.clone(),
                                        mime_type: mime_type.clone(),
                                    }]));
                                }
                                _ => {}
                            }
                        }
                    }
                }
                html.push_str("</article>");
            }
            Event::SystemCheckpoint {
                content,
                sections,
                tools_added,
                tools_removed,
            } => {
                render_system_html(
                    &mut html,
                    content,
                    sections,
                    tools_added,
                    tools_removed,
                    !has_system_prompt,
                );
                has_system_prompt = true;
            }
            Event::BashExecution { execution } => render_shell_html(&mut html, execution),
            Event::Assistant {
                content,
                invocations,
                ..
            } => {
                if !content.is_empty() {
                    html.push_str("<article><b>Pi</b>");
                    render_export_markdown(&mut html, content);
                    html.push_str("</article>");
                }
                for invocation in invocations {
                    html.push_str(&format!(
                        "<details><summary>{}</summary><pre>{}</pre></details>",
                        escape(&invocation.call.name),
                        escape(&invocation.call.arguments)
                    ));
                }
                if let Some(message) = imported_assistant.take() {
                    match message["stopReason"].as_str() {
                        Some("aborted") => render_export_failure(&mut html, "Aborted"),
                        Some("error") => render_export_failure(
                            &mut html,
                            &format!(
                                "Error: {}",
                                message["errorMessage"]
                                    .as_str()
                                    .filter(|text| !text.is_empty())
                                    .unwrap_or("Unknown error")
                            ),
                        ),
                        _ => {}
                    }
                }
            }
            Event::ToolFinished { outcome, .. } | Event::ToolReconciled { outcome, .. } => {
                html.push_str(&tool_html(outcome))
            }
            Event::ModelFinished {
                metadata,
                failure,
                partial,
                ..
            } => {
                if let Some(reasoning) = &metadata.reasoning
                    && !reasoning.trim().is_empty()
                {
                    html.push_str(&format!("<details class=\"thinking\"><summary>Thinking</summary><pre>{}</pre></details>", escape(reasoning)));
                }
                if let Some(failure) = failure
                    && imported_assistant.is_none()
                {
                    if !partial.trim().is_empty() {
                        html.push_str("<article><b>Pi</b>");
                        render_export_markdown(&mut html, partial);
                        html.push_str("</article>");
                    }
                    let status = match failure {
                        pi_rs::ModelFailure::Cancelled => "Aborted",
                        pi_rs::ModelFailure::Retryable => "Error: Model request failed temporarily",
                        pi_rs::ModelFailure::Failed => "Error: Model request failed",
                    };
                    render_export_failure(&mut html, status);
                }
            }
            Event::Compacted { summary, .. }
            | Event::CompactionSummaryMessage { summary, .. }
            | Event::BranchSummary { summary, .. } => {
                if matches!(
                    record.event,
                    Event::Compacted {
                        system_prompt: Some(_),
                        ..
                    }
                ) {
                    has_system_prompt = true;
                }
                let label = if matches!(
                    record.event,
                    Event::Compacted { .. } | Event::CompactionSummaryMessage { .. }
                ) {
                    "Compacted context"
                } else {
                    "Branch summary"
                };
                html.push_str(&format!("<details><summary>{label}</summary>"));
                render_export_markdown(&mut html, summary);
                html.push_str("</details>");
            }
            _ => {}
        }
    }
    html.push_str("</html>");
    html
}

fn render_system_html(
    html: &mut String,
    content: &str,
    sections: &[(String, Option<String>)],
    tools_added: &[pi_rs::ToolSpec],
    tools_removed: &[String],
    leading: bool,
) {
    let mut text = Vec::new();
    let mut changes = Vec::new();
    if !content.is_empty() {
        text.push(content.to_owned());
        changes.push("instructions".to_owned());
    }
    for (name, value) in sections {
        match (leading, value) {
            (true, Some(value)) if !value.is_empty() => text.push(value.clone()),
            (false, Some(value)) => {
                text.push(format!(
                    "Updated system prompt section \"{name}\":\n\n{value}"
                ));
                changes.push(name.clone());
            }
            (false, None) => {
                text.push(format!("Removed system prompt section \"{name}\"."));
                changes.push(format!("-{name}"));
            }
            _ => {}
        }
    }
    if !leading && (!tools_added.is_empty() || !tools_removed.is_empty()) {
        let tool_changes = tools_added
            .iter()
            .map(|tool| format!("+{}", tool.name))
            .chain(tools_removed.iter().map(|name| format!("-{name}")))
            .collect::<Vec<_>>()
            .join(" ");
        changes.push(format!("tools {tool_changes}"));
    }
    if !text.is_empty() {
        let label = if leading {
            "System Prompt".to_owned()
        } else if changes.is_empty() {
            "System Prompt Update".to_owned()
        } else {
            format!("System Prompt Update: {}", changes.join(", "))
        };
        html.push_str(&format!(
            "<details class=\"system\"><summary>{}</summary><pre>{}</pre></details>",
            escape(&label),
            escape(&text.join("\n\n"))
        ));
    }
    if !tools_added.is_empty() {
        let label = if leading {
            "Available Tools"
        } else {
            "Tools Added"
        };
        html.push_str(&format!(
            "<details class=\"system\"><summary>{label}</summary><ul>"
        ));
        for tool in tools_added {
            render_system_tool_html(html, tool);
        }
        html.push_str("</ul></details>");
    }
    if !leading && !tools_removed.is_empty() {
        html.push_str("<details class=\"system\"><summary>Tools Removed</summary><ul>");
        for name in tools_removed {
            html.push_str(&format!("<li>{}</li>", escape(name)));
        }
        html.push_str("</ul></details>");
    }
}

fn render_system_tool_html(html: &mut String, tool: &pi_rs::ToolSpec) {
    let title = format!(
        "<b>{}</b> {}",
        escape(&tool.name),
        escape(&tool.description)
    );
    let Some(properties) = tool.parameters["properties"]
        .as_object()
        .filter(|properties| !properties.is_empty())
    else {
        html.push_str(&format!("<li>{title}</li>"));
        return;
    };
    let required = tool.parameters["required"].as_array();
    html.push_str(&format!("<li><details><summary>{title}</summary><ul>"));
    for (name, property) in properties {
        let kind = match &property["type"] {
            serde_json::Value::String(kind) if !kind.is_empty() => kind.clone(),
            serde_json::Value::Array(kinds) => kinds
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
                .join(","),
            _ => "any".to_owned(),
        };
        let requirement = if required
            .is_some_and(|names| names.iter().any(|entry| entry.as_str() == Some(name)))
        {
            "required"
        } else {
            "optional"
        };
        html.push_str(&format!(
            "<li><code>{}</code> <code>{}</code> {requirement}",
            escape(name),
            escape(&kind)
        ));
        if let Some(description) = property["description"]
            .as_str()
            .filter(|text| !text.is_empty())
        {
            html.push_str(&format!("<div>{}</div>", escape(description)));
        }
        html.push_str("</li>");
    }
    html.push_str("</ul></details></li>");
}

fn render_shell_html(html: &mut String, execution: &pi_rs::BashExecution) {
    let failed = execution.cancelled || execution.exit_code.is_some_and(|code| code != 0);
    let status = if failed { "error" } else { "success" };
    html.push_str(&format!(
        "<article class=\"tool-execution {status}\"><pre>$ {}</pre>",
        escape(&execution.command)
    ));
    if !execution.output.is_empty() {
        let mut lines = execution.output.splitn(11, '\n');
        let preview = lines.by_ref().take(10).collect::<Vec<_>>().join("\n");
        html.push_str(&format!("<pre>{}</pre>", escape(&preview)));
        if let Some(remaining) = lines.next() {
            html.push_str(&format!(
                "<details><summary>Show remaining output</summary><pre>{}</pre></details>",
                escape(remaining)
            ));
        }
    }
    if execution.cancelled {
        render_export_failure(html, "(cancelled)");
    } else if let Some(code) = execution.exit_code.filter(|code| *code != 0) {
        render_export_failure(html, &format!("(exit {code})"));
    }
    html.push_str("</article>");
}

fn render_export_failure(html: &mut String, status: &str) {
    html.push_str(&format!(
        "<div class=\"error-text\">{}</div>",
        escape(status)
    ));
}

fn render_user_html(html: &mut String, content: &str, images: &[pi_rs::ImageContent]) {
    let text = if let Some((name, body, message)) = super::resources::skill_invocation(content) {
        html.push_str(&format!(
            "<details class=\"skill\"><summary>[skill] {}</summary>",
            escape(name)
        ));
        render_export_markdown(html, body);
        html.push_str("</details>");
        if message.is_none() && images.is_empty() {
            return;
        }
        message.unwrap_or_default()
    } else {
        content
    };
    html.push_str("<article class=\"user\"><b>You</b>");
    render_export_markdown(html, text);
    html.push_str(&images_html(images));
    html.push_str("</article>");
}

fn render_export_markdown(html: &mut String, markdown: &str) {
    use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let mut safe_links = Vec::new();
    let events = Parser::new_ext(markdown, options).filter_map(|event| match event {
        Event::Html(text) | Event::InlineHtml(text) => Some(Event::Text(text)),
        Event::Start(Tag::Link { ref dest_url, .. }) => {
            let safe = dest_url.starts_with('#')
                || reqwest::Url::parse(dest_url)
                    .is_ok_and(|url| matches!(url.scheme(), "https" | "http" | "mailto"));
            safe_links.push(safe);
            safe.then_some(event)
        }
        Event::End(TagEnd::Link) => safe_links.pop().unwrap_or(false).then_some(event),
        Event::Start(Tag::Image { .. }) | Event::End(TagEnd::Image) => None,
        _ => Some(event),
    });
    pulldown_cmark::html::push_html(html, events);
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
    let text = super::rpc_messages::last_assistant_text(session)
        .ok_or_else(|| Error::Config("No agent messages to copy yet.".into()))?;
    copy_text(&text)
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
                outcome:
                    pi_rs::Outcome::Structured { data, .. }
                    | pi_rs::Outcome::StructuredWithDetails { data, .. },
                ..
            }
            | Event::ToolReconciled {
                outcome:
                    pi_rs::Outcome::Structured { data, .. }
                    | pi_rs::Outcome::StructuredWithDetails { data, .. },
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
            Event::Compacted { summary, .. } | Event::BranchSummary { summary, .. } => {
                Some(Cow::Borrowed(summary.as_str()))
            }
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

// Async variant for cancellable device-code actions. Dropping the future
// terminates an in-progress clipboard process rather than leaving a late write.
pub(super) async fn copy_text_async(text: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
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
        let mut command = tokio::process::Command::new(program);
        command
            .args(*arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut stdin = child.stdin.take().expect("piped clipboard");
        let result = stdin.write_all(text.as_bytes()).await;
        drop(stdin);
        let status = child.wait().await?;
        result?;
        if status.success() {
            return Ok(());
        }
    }
    Err(Error::Config("no working clipboard command found".into()))
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ShareContext {
    pub system_prompt: String,
    pub tools: Vec<pi_rs::ToolSpec>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ShareSnapshot {
    pub session_id: uuid::Uuid,
    pub head: u64,
    pub context: ShareContext,
    // Present only on the private live-worker response. The ephemeral key is
    // held by the requesting frontend and is never written to the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_tag: Option<[u8; 32]>,
}

type ShareJournalMac = hmac::Hmac<sha2::Sha256>;

struct ShareJournalWriter<'a>(&'a mut ShareJournalMac);
impl std::io::Write for ShareJournalWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        use hmac::Mac;
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn share_journal_mac(
    session: &Session,
    context: &ShareContext,
    key: &[u8; 32],
) -> Result<ShareJournalMac> {
    use hmac::Mac;
    let mut mac = ShareJournalMac::new_from_slice(key).expect("HMAC accepts 32 bytes");
    mac.update(b"pi-rs-live-share-context-v2\0");
    // Avoid allocating another complete JSON copy of a large journal while
    // batching the serializer's small writes into efficient MAC updates.
    {
        let mut writer = std::io::BufWriter::new(ShareJournalWriter(&mut mac));
        serde_json::to_writer(&mut writer, &(session.records(), context))?;
        writer.flush()?;
    }
    Ok(mac)
}

pub(super) fn share_journal_tag(
    session: &Session,
    context: &ShareContext,
    key: &[u8; 32],
) -> Result<[u8; 32]> {
    use hmac::Mac;
    Ok(share_journal_mac(session, key)?
        .finalize()
        .into_bytes()
        .into())
}

pub(super) fn verify_share_journal(
    session: &Session,
    context: &ShareContext,
    key: &[u8; 32],
    tag: &[u8; 32],
) -> Result<bool> {
    use hmac::Mac;
    Ok(share_journal_mac(session, context, key)?
        .verify_slice(tag)
        .is_ok())
}

pub(super) fn validate_live_share_journal(
    session: &Session,
    snapshot: &ShareSnapshot,
    key: Option<&[u8; 32]>,
) -> Result<()> {
    let changed = || Error::Config("Session changed while preparing the share; try again".into());
    if snapshot.session_id != session.session_id() || snapshot.head != session.head() {
        return Err(changed());
    }
    let (Some(key), Some(tag)) = (key, snapshot.journal_tag.as_ref()) else {
        return Err(changed());
    };
    if !verify_share_journal(session, &snapshot.context, key, tag)? {
        return Err(changed());
    }
    Ok(())
}

pub(super) enum ShareDestination {
    Radius {
        endpoint: reqwest::Url,
        credential: Box<RadiusCredential>,
        proxies: Vec<reqwest::Proxy>,
        idle_timeout: Option<std::time::Duration>,
    },
    Gist,
}

pub(super) enum RadiusCredential {
    ApiKey(String),
    OAuth(super::radius_auth::Gateway),
}

#[derive(Serialize)]
pub(super) struct SharedSession {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gist: Option<String>,
}

impl ShareDestination {
    pub(super) fn configured(settings: &super::config::Settings) -> Result<Self> {
        let destination = settings.string("sessionShareDestination").unwrap_or("auto");
        if destination == "gist" {
            return Ok(Self::Gist);
        }
        let gateway = super::radius_auth::Gateway::configured_with_settings(settings)?;
        let endpoint = match std::env::var("PI_RS_RADIUS_SHARE_URL") {
            Ok(url) => share_url(&url)?,
            Err(_) => gateway.url("/v1/artifacts"),
        };
        let key = std::env::var("RADIUS_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty());
        let credential = if let Some(key) = key {
            RadiusCredential::ApiKey(key)
        } else if gateway.url("/").origin() == endpoint.origin() && gateway.has_credential()? {
            RadiusCredential::OAuth(gateway)
        } else {
            return if destination == "radius" {
                Err(Error::Config(
                    "Radius authentication is not configured. Run '/login radius' or set RADIUS_API_KEY.".into(),
                ))
            } else {
                Ok(Self::Gist)
            };
        };
        if endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err(Error::Config(
                "Radius share endpoint cannot include a query or fragment".into(),
            ));
        }
        Ok(Self::Radius {
            endpoint,
            credential: Box::new(credential),
            proxies: super::http_proxy::configured(settings)?,
            idle_timeout: settings.http_idle_timeout(),
        })
    }

    pub(super) fn is_radius(&self) -> bool {
        matches!(self, Self::Radius { .. })
    }

    pub(super) fn status(&self) -> &'static str {
        match self {
            Self::Radius { .. } => "Uploading to Radius",
            Self::Gist => "Creating gist",
        }
    }

    pub(super) async fn share(
        self,
        session: Session,
        workspace: &Path,
        snapshot: Option<ShareSnapshot>,
        journal_key: Option<[u8; 32]>,
    ) -> Result<SharedSession> {
        match self {
            Self::Radius {
                mut endpoint,
                credential,
                proxies,
                idle_timeout,
            } => {
                if journal_key.is_some() && snapshot.is_none() {
                    return Err(Error::Config(
                        "Live share context is unavailable; try sharing again".into(),
                    ));
                }
                if let Some(snapshot) = &snapshot
                    && (journal_key.is_some() || snapshot.journal_tag.is_some())
                {
                    validate_live_share_journal(&session, snapshot, journal_key.as_ref())?;
                }
                if snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot.session_id != session.session_id() || snapshot.head != session.head()
                }) {
                    return Err(Error::Config(
                        "Session changed while preparing the share; try again".into(),
                    ));
                }
                let context = snapshot;
                let body = super::rpc_entries::Entries::new(&session)
                    .write_share_branch_with_context(
                        &session,
                        workspace,
                        context.as_ref().map(|snapshot| &snapshot.context),
                    )?;
                drop(session);
                let token = match *credential {
                    RadiusCredential::ApiKey(key) => key,
                    RadiusCredential::OAuth(gateway) => gateway
                        .access(std::time::Duration::from_secs(300))
                        .await?
                        .ok_or_else(|| {
                            Error::Config("Radius credentials were removed. Sign in again.".into())
                        })?,
                };
                endpoint
                    .query_pairs_mut()
                    .append_pair("visibility", "organization")
                    .append_pair("title", "Pi session");
                let mut builder = reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(std::time::Duration::from_secs(120));
                for proxy in proxies {
                    builder = builder.proxy(proxy);
                }
                if let Some(timeout) = idle_timeout {
                    builder = builder.read_timeout(timeout);
                }
                let client = builder.build().map_err(|_| {
                    Error::Config("Could not initialize the Radius share client".into())
                })?;
                let mut response = client
                    .post(endpoint)
                    .bearer_auth(token)
                    .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
                    .body(body)
                    .send()
                    .await
                    .map_err(|_| Error::Config("Could not upload the session to Radius".into()))?;
                if !response.status().is_success() {
                    return Err(Error::Config(format!(
                        "Radius upload failed ({})",
                        response.status()
                    )));
                }
                let mut body = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| Error::Config("Could not read the Radius share response".into()))?
                {
                    if body.len() + chunk.len() > 64 * 1024 {
                        return Err(Error::Config("Radius share response exceeds 64 KiB".into()));
                    }
                    body.extend_from_slice(&chunk);
                }
                let body: serde_json::Value = serde_json::from_slice(&body).map_err(|_| {
                    Error::Config("Radius returned an invalid share response".into())
                })?;
                let url = body["artifact"]["canonical_url"]
                    .as_str()
                    .ok_or_else(|| Error::Config("Radius did not return a share URL".into()))?;
                Ok(SharedSession {
                    url: share_url(url)?.into(),
                    gist: None,
                })
            }
            Self::Gist => {
                let directory = tempfile::tempdir()?;
                let file = directory.path().join("session.html");
                export(&session, &file)?;
                let name = session.name().unwrap_or("Pi session").to_owned();
                drop(session);
                share_gist(&file, &name).await
            }
        }
    }
}

fn share_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| Error::Config("Invalid session share URL".into()))?;
    let loopback = url
        .host_str()
        .and_then(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .ok()
        })
        .is_some_and(|ip| ip.is_loopback());
    if url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
    {
        return Err(Error::Config(
            "Session share URL must use HTTPS or a loopback HTTP address".into(),
        ));
    }
    Ok(url)
}

async fn share_gist(file: &Path, name: &str) -> Result<SharedSession> {
    let mut viewer = share_url(
        &std::env::var("PI_SHARE_VIEWER_URL").unwrap_or_else(|_| "https://pi.dev/session/".into()),
    )?;
    let output = tokio::process::Command::new("gh")
        .args(["gist", "create", "--public=false", "--desc", name])
        .arg(file)
        .stdin(Stdio::null())
        .env("GH_PROMPT_DISABLED", "1")
        .kill_on_drop(true)
        .output()
        .await?;
    if !output.status.success() {
        return Err(Error::Config(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ));
    }
    let gist = share_url(String::from_utf8_lossy(&output.stdout).trim())?;
    let id = gist
        .path_segments()
        .and_then(Iterator::last)
        .filter(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| Error::Config("GitHub CLI did not return a gist ID".into()))?;
    viewer.set_fragment(Some(id));
    Ok(SharedSession {
        url: viewer.into(),
        gist: Some(gist.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn radius_share_uses_configured_proxy() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path().join("session.jsonl")).unwrap();
        let (proxy, server) = super::super::http_proxy::test_proxy(
            r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#,
        )
        .await;
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture-token".into())),
            proxies: vec![proxy],
            idle_timeout: None,
        };
        let shared = destination
            .share(session, root.path(), None, None)
            .await
            .unwrap();
        assert_eq!(shared.url, "https://example.com/shared");
        let (request, headers, _) = server.await.unwrap();
        assert!(request.starts_with("POST http://127.0.0.1:1/v1/artifacts?"));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token")
        );
    }
    #[test]
    fn live_share_rejects_different_journal_with_same_session_and_head() {
        let mut original = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        original
            .append_user_with_images("original".into(), Vec::new(), None)
            .unwrap();
        let key = [42; 32];
        let mut snapshot = ShareSnapshot {
            session_id: original.session_id(),
            head: original.head(),
            context: ShareContext {
                system_prompt: "live".into(),
                tools: vec![],
            },
            journal_tag: None,
        };
        snapshot.journal_tag = Some(share_journal_tag(&original, &snapshot.context, &key).unwrap());
        validate_live_share_journal(&original, &snapshot, Some(&key)).unwrap();
        let mut records = original.records().to_vec();
        match &mut records.last_mut().unwrap().event {
            Event::User { content, .. } => *content = "changed".into(),
            _ => panic!("expected user record"),
        }
        let altered = Session::from_records(records).unwrap();
        assert_eq!(original.session_id(), altered.session_id());
        assert_eq!(original.head(), altered.head());
        assert!(validate_live_share_journal(&altered, &snapshot, Some(&key)).is_err());
        assert!(validate_live_share_journal(&original, &snapshot, Some(&[43; 32])).is_err());
        assert!(validate_live_share_journal(&original, &snapshot, None).is_err());
        let mut changed_context = snapshot.clone();
        changed_context.context.system_prompt.push_str(" tampered");
        assert!(validate_live_share_journal(&original, &changed_context, Some(&key)).is_err());
        let mut changed_tools = snapshot.clone();
        changed_tools.context.tools.push(pi_rs::ToolSpec {
            name: "extra".into(),
            description: "tampered".into(),
            parameters: serde_json::json!({}),
        });
        assert!(validate_live_share_journal(&original, &changed_tools, Some(&key)).is_err());
    }

    #[tokio::test]
    async fn radius_share_uploads_context_for_matching_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path().join("session.jsonl")).unwrap();
        let key = [19; 32];
        let mut snapshot = ShareSnapshot {
            journal_tag: None,
            session_id: session.session_id(),
            head: session.head(),
            context: ShareContext {
                system_prompt: "live instructions".into(),
                tools: vec![pi_rs::ToolSpec {
                    name: "read".into(),
                    description: "Read a file".into(),
                    parameters: serde_json::json!({"type":"object"}),
                }],
            },
        };
        snapshot.journal_tag = Some(share_journal_tag(&session, &snapshot.context, &key).unwrap());
        let (proxy, server) = super::super::http_proxy::test_proxy(
            r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#,
        )
        .await;
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture".into())),
            proxies: vec![proxy],
            idle_timeout: None,
        };
        destination
            .share(session, root.path(), Some(snapshot), Some(key))
            .await
            .unwrap();
        let (_, _, body) = server.await.unwrap();
        let entries: Vec<serde_json::Value> = body
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1]["type"], "custom");
        assert_eq!(entries[1]["customType"], "pi.share");
        assert!(entries[1]["parentId"].is_null());
        assert_eq!(entries[1]["data"]["systemPrompt"], "live instructions");
        assert!(entries[1]["data"].get("journalTag").is_none());
        assert!(entries[1]["data"].get("journalKey").is_none());
        assert_eq!(
            entries[1]["data"]["tools"][0],
            serde_json::json!({"name":"read","description":"Read a file","parameters":{"type":"object"}})
        );
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn radius_upload_uses_reopened_journal_bound_cached_context() {
        use pi_rs_workbench::private_share_store::Store;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let session = Session::open(&path).unwrap();
        let provenance = serde_json::json!({"workspace":"fixture", "configuration":"v1"});
        let context = ShareSnapshot {
            journal_tag: None,
            session_id: session.session_id(),
            head: session.head(),
            context: ShareContext {
                system_prompt: "cached live instructions".into(),
                tools: vec![],
            },
        };
        let store = Store::open(root.path()).unwrap();
        let binding = store.binding_for_session(&session, &provenance).unwrap();
        store
            .begin(&path, binding)
            .unwrap()
            .publish_session(&session, &provenance, &context)
            .unwrap();
        drop(store);
        drop(session);
        let session = Session::snapshot(&path).unwrap();
        let store = Store::open_existing(root.path()).unwrap().unwrap();
        let snapshot: ShareSnapshot = store
            .read_session(&path, &session, &provenance)
            .unwrap()
            .unwrap();
        let (proxy, server) = super::super::http_proxy::test_proxy(
            r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#,
        )
        .await;
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture".into())),
            proxies: vec![proxy],
            idle_timeout: None,
        };
        destination
            .share(session, root.path(), Some(snapshot), None)
            .await
            .unwrap();
        let (_, _, body) = server.await.unwrap();
        let entries: Vec<serde_json::Value> = body
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(
            entries.last().unwrap()["data"]["systemPrompt"],
            "cached live instructions"
        );
        assert_eq!(entries.last().unwrap()["customType"], "pi.share");
    }

    #[tokio::test]
    async fn radius_share_rejects_missing_live_context_before_upload() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture".into())),
            proxies: vec![],
            idle_timeout: None,
        };
        let error = match destination
            .share(session, root.path(), None, Some([1; 32]))
            .await
        {
            Ok(_) => panic!("missing live context was shared"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Live share context is unavailable")
        );
    }

    #[tokio::test]
    async fn radius_share_rejects_missing_live_tag_before_upload() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let snapshot = ShareSnapshot {
            session_id: session.session_id(),
            head: session.head(),
            context: ShareContext {
                system_prompt: "live".into(),
                tools: vec![],
            },
            journal_tag: None,
        };
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture".into())),
            proxies: vec![],
            idle_timeout: None,
        };
        let error = match destination
            .share(session, root.path(), Some(snapshot), Some([1; 32]))
            .await
        {
            Ok(_) => panic!("untagged live context was shared"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Session changed"));
    }

    #[tokio::test]
    async fn radius_share_rejects_live_journal_mismatch_before_upload() {
        let root = tempfile::tempdir().unwrap();
        let mut original = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        original
            .append_user_with_images("original".into(), Vec::new(), None)
            .unwrap();
        let key = [23; 32];
        let mut snapshot = ShareSnapshot {
            session_id: original.session_id(),
            head: original.head(),
            context: ShareContext {
                system_prompt: "live".into(),
                tools: vec![],
            },
            journal_tag: None,
        };
        snapshot.journal_tag = Some(share_journal_tag(&original, &snapshot.context, &key).unwrap());
        let mut records = original.records().to_vec();
        match &mut records.last_mut().unwrap().event {
            Event::User { content, .. } => *content = "changed".into(),
            _ => panic!("expected user record"),
        }
        let altered = Session::from_records(records).unwrap();
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture".into())),
            proxies: vec![],
            idle_timeout: None,
        };
        let error = match destination
            .share(altered, root.path(), Some(snapshot), Some(key))
            .await
        {
            Ok(_) => panic!("changed journal was shared"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Session changed"));
    }

    #[tokio::test]
    async fn radius_share_rejects_stale_snapshot_before_upload() {
        let root = tempfile::tempdir().unwrap();
        let session = Session::open(root.path().join("session.jsonl")).unwrap();
        let snapshot = ShareSnapshot {
            journal_tag: None,
            session_id: session.session_id(),
            head: session.head() + 1,
            context: ShareContext {
                system_prompt: "stale".into(),
                tools: vec![],
            },
        };
        let destination = ShareDestination::Radius {
            endpoint: reqwest::Url::parse("http://127.0.0.1:1/v1/artifacts").unwrap(),
            credential: Box::new(RadiusCredential::ApiKey("fixture".into())),
            proxies: vec![],
            idle_timeout: None,
        };
        let error = match destination
            .share(session, root.path(), Some(snapshot), None)
            .await
        {
            Ok(_) => panic!("stale context accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Session changed"));
    }

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
    fn missing_session_id_is_not_treated_as_a_new_file() {
        let root = tempfile::tempdir().unwrap();
        for directory in [root.path().to_path_buf(), root.path().join("missing")] {
            let error = resolve_reference(Path::new("unknown-id"), &directory).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("No session found matching 'unknown-id'")
            );
            assert_eq!(
                resolve_reference(Path::new("new.jsonl"), &directory).unwrap(),
                Path::new("new.jsonl")
            );
        }
    }

    #[test]
    fn jsonl_reference_prefers_exact_stored_stem_before_filename_fallback() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("work.jsonl.jsonl");
        drop(Session::open(&path).unwrap());
        assert_eq!(
            resolve_reference(Path::new("work.jsonl"), root.path()).unwrap(),
            path
        );
        assert_eq!(
            resolve_reference(Path::new("other.jsonl"), root.path()).unwrap(),
            Path::new("other.jsonl")
        );
        let other = tempfile::tempdir().unwrap();
        drop(Session::open(other.path().join("work.jsonl-extra.jsonl")).unwrap());
        // An explicit filename must not select a longer, merely prefix-matching stem.
        assert_eq!(
            resolve_reference(Path::new("work.jsonl"), other.path()).unwrap(),
            Path::new("work.jsonl")
        );
        let header_path = other.path().join("header.jsonl");
        std::fs::write(&header_path, "{\"id\":\"journal.jsonl\"}\n").unwrap();
        assert_eq!(
            resolve_reference(Path::new("journal.jsonl"), other.path()).unwrap(),
            header_path
        );
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
    fn html_includes_user_shell_output_and_status_even_when_excluded_from_context() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let mut execution = pi_rs::BashExecution {
            command: "fixture <command>".into(),
            output: format!(
                "{}\n<script>last line</script>",
                "preview\n".repeat(9) + "preview"
            ),
            exit_code: Some(7),
            cancelled: false,
            truncated: false,
            full_output_path: None,
            exclude_from_context: true,
        };
        session.append_bash_execution(execution.clone()).unwrap();
        execution.cancelled = true;
        execution.exit_code = None;
        session.append_bash_execution(execution).unwrap();
        let document = html(&session);
        assert!(document.contains(
            "<article class=\"tool-execution error\"><pre>$ fixture &lt;command&gt;</pre>"
        ));
        assert!(document.contains("<details><summary>Show remaining output</summary><pre>&lt;script&gt;last line&lt;/script&gt;</pre></details>"));
        assert!(document.contains("(exit 7)"));
        assert!(document.contains("(cancelled)"));
        assert!(!document.contains("<script>"));
    }

    #[test]
    fn html_displays_initial_system_prompt_and_later_updates() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"message","id":"s1","parentId":null,"message":{
                "role":"system","content":"base","sections":{"rules":"first"},
                "toolsAdded":[{"name":"read","description":"Read","parameters":{"type":"object","required":["path"],"properties":{"path":{"type":"string","description":"<script>path</script>"},"limit":{"type":["integer","null"]},"options":true}}}]
            }}),
            serde_json::json!({"type":"message","id":"u","parentId":"s1","message":{
                "role":"user","content":"Question"
            }}),
            serde_json::json!({"type":"message","id":"s2","parentId":"u","message":{
                "role":"system","content":"","sections":{"rules":null,"project":"<script>"},
                "toolsAdded":[{"name":"write","description":"Write","parameters":{"type":"object"}}],
                "toolsRemoved":[{"name":"read"}]
            }}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(fixture.path()).unwrap();
        let document = html(&session);
        assert!(document.contains("<summary>System Prompt</summary><pre>base\n\nfirst</pre>"));
        assert!(document.contains(
            "<summary>Available Tools</summary><ul><li><details><summary><b>read</b> Read</summary>"
        ));
        assert!(document.contains(
            "<summary>System Prompt Update: -rules, project, tools +write -read</summary>"
        ));
        assert!(document.contains("Removed system prompt section &quot;rules&quot;."));
        assert!(
            document
                .contains("Updated system prompt section &quot;project&quot;:\n\n&lt;script&gt;")
        );
        assert!(document.contains("<summary>Tools Added</summary><ul><li><b>write</b> Write</li>"));
        assert!(document.contains("<summary>Tools Removed</summary><ul><li>read</li>"));
        assert!(
            document.find("Question").unwrap() < document.find("System Prompt Update").unwrap()
        );
        assert!(document.contains("<code>path</code> <code>string</code> required<div>&lt;script&gt;path&lt;/script&gt;</div>"));
        assert!(document.contains("<code>limit</code> <code>integer,null</code> optional"));
        assert!(document.contains("<code>options</code> <code>any</code> optional"));
        assert!(!document.contains("<script>"));
    }

    #[test]
    fn html_respects_imported_custom_message_visibility() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"message","id":"a","parentId":null,"message":{"role":"custom","customType":"private","content":"secret context","display":false}}),
            serde_json::json!({"type":"message","id":"b","parentId":"a","message":{"role":"custom","customType":"note","content":"visible **note**","display":true}}),
            serde_json::json!({"type":"message","id":"c","parentId":"b","message":{"role":"hookMessage","content":"hidden hook explicit","display":false}}),
            serde_json::json!({"type":"message","id":"d","parentId":"c","message":{"role":"hookMessage","content":"hidden hook default"}}),
            serde_json::json!({"type":"message","id":"e","parentId":"d","message":{"role":"hookMessage","content":"visible hook","display":true}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(fixture.path()).unwrap();
        let document = html(&session);
        assert!(!document.contains("secret context"));
        assert!(!document.contains("hidden hook explicit"));
        assert!(!document.contains("hidden hook default"));
        assert!(document.contains("visible hook"));
        assert!(document.contains("visible <strong>note</strong>"));
    }

    #[test]
    fn html_exports_hidden_custom_entries_with_terminal_visibility_label() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"custom_message","id":"a","parentId":null,"customType":"<note>","content":"hidden **context**", "display":false}),
            serde_json::json!({"type":"custom_message","id":"b","parentId":"a","customType":"visible","content":"shown", "display":true}),
            serde_json::json!({"type":"message","id":"c","parentId":"b","message":{"role":"custom","customType":"private","content":"private role", "display":false}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(fixture.path()).unwrap();
        let document = html(&session);
        assert!(document.contains("<b>&lt;note&gt;</b> <span>Hidden in terminal</span>"));
        assert!(document.contains("hidden <strong>context</strong>"));
        assert!(document.contains("<b>visible</b>"));
        assert_eq!(document.matches("Hidden in terminal").count(), 1);
        assert!(!document.contains("private role"));
    }

    #[test]
    fn html_exports_native_hidden_custom_messages() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        for (custom_type, content, display) in [
            ("<native>", "hidden **native**", false),
            ("visible", "visible native", true),
        ] {
            session
                .append_custom_message(pi_rs::CustomMessage {
                    custom_type: custom_type.into(),
                    content: pi_rs::ContextContent::Text(content.into()),
                    display,
                    details: None,
                })
                .unwrap();
        }
        let document = html(&session);
        assert!(document.contains("<b>&lt;native&gt;</b> <span>Hidden in terminal</span>"));
        assert!(document.contains("hidden <strong>native</strong>"));
        assert!(document.contains("visible native"));
        assert_eq!(document.matches("Hidden in terminal").count(), 1);
    }

    #[test]
    fn html_exports_native_hidden_message_after_checkout_to_legacy_source() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"message","id":"a","parentId":null,"message":{"role":"hookMessage","content":"legacy hidden","display":false}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let mut session = super::super::session_import::read(fixture.path()).unwrap();
        let source = session
            .records()
            .iter()
            .find(|record| {
                matches!(
                    &record.event,
                    Event::SourceEntry { entry, .. } if entry["id"] == "a"
                )
            })
            .unwrap()
            .id;
        session.checkout(source).unwrap();
        session
            .append_custom_message(pi_rs::CustomMessage {
                custom_type: "native".into(),
                content: pi_rs::ContextContent::Text("native after checkout".into()),
                display: false,
                details: None,
            })
            .unwrap();
        let document = html(&session);
        assert!(document.contains("<b>native</b> <span>Hidden in terminal</span>"));
        assert!(document.contains("native after checkout"));
        assert!(!document.contains("legacy hidden"));
    }

    #[test]
    fn html_exports_native_message_after_fork_at_legacy_source_even_if_content_matches() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"message","id":"a","parentId":null,"message":{"role":"hookMessage","content":"same content","display":false}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let imported = super::super::session_import::read(fixture.path()).unwrap();
        let source = imported
            .records()
            .iter()
            .find(|record| {
                matches!(
                    &record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a"
                )
            })
            .unwrap()
            .id;
        let mut fork = Session::from_records(imported.fork_records(source).unwrap()).unwrap();
        fork.append_custom_message(pi_rs::CustomMessage {
            custom_type: String::new(),
            content: pi_rs::ContextContent::Text("same content".into()),
            display: false,
            details: None,
        })
        .unwrap();
        let records = serde_json::to_string(fork.records()).unwrap();
        let reopened = Session::from_records(serde_json::from_str(&records).unwrap()).unwrap();
        let document = html(&reopened);
        assert!(document.contains("Hidden in terminal"));
        assert!(document.contains("same content"));
    }

    #[test]
    fn html_hides_ambiguous_unmarked_native_message_matching_legacy_source() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"message","id":"a","parentId":null,"message":{"role":"hookMessage","content":"ambiguous private content","display":false}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let imported = super::super::session_import::read(fixture.path()).unwrap();
        let source = imported
            .records()
            .iter()
            .find(|record| {
                matches!(
                    &record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a"
                )
            })
            .unwrap()
            .id;
        let mut records = imported.fork_records(source).unwrap();
        let parent = records.last().unwrap().id;
        // Simulate an old native append without the new associated source marker.
        records.push(pi_rs::Record {
            timestamp: None,
            run_id: None,
            version: 2,
            id: parent + 1,
            parent: Some(parent),
            event: Event::CustomMessage {
                message: pi_rs::CustomMessage {
                    custom_type: String::new(),
                    content: pi_rs::ContextContent::Text("ambiguous private content".into()),
                    display: false,
                    details: None,
                },
            },
        });
        let mut wire = serde_json::to_value(&records).unwrap();
        // Journals produced by the interim optional-field format remain readable.
        wire.as_array_mut().unwrap().last_mut().unwrap()["event"]["legacy_source"] =
            serde_json::json!(false);
        let decoded: Vec<pi_rs::Record> = serde_json::from_value(wire).unwrap();
        let old = Session::from_records(decoded).unwrap();
        assert!(!html(&old).contains("ambiguous private content"));
    }

    #[test]
    fn html_displays_imported_assistant_stop_reasons() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            serde_json::json!({"type":"session","version":3}),
            serde_json::json!({"type":"message","id":"a","parentId":null,"message":{
                "role":"assistant","content":[{"type":"text","text":"**partial**"}],"stopReason":"aborted"
            }}),
            serde_json::json!({"type":"message","id":"b","parentId":"a","message":{
                "role":"assistant","content":[],"stopReason":"error","errorMessage":"<script>error</script>"
            }}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(fixture.path()).unwrap();
        let document = html(&session);
        assert!(document.contains("<strong>partial</strong>"));
        assert!(document.contains("<div class=\"error-text\">Aborted</div>"));
        assert!(document.contains("Error: &lt;script&gt;error&lt;/script&gt;"));
        assert_eq!(document.matches("<div class=\"error-text\">").count(), 2);
        assert!(!document.contains("Error: Model request failed"));
        assert!(!document.contains("<script>"));
    }

    #[test]
    fn html_keeps_skill_body_folded_and_escapes_untrusted_text() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session.append_user_with_images(
            "<skill name=\"demo&fixture\" location=\"/tmp/SKILL.md\">\n## Steps\n\n```text\nexample\n```\n\n[unsafe](javascript:alert) [docs](https://example.test) ![remote image](https://example.test/a.png)\n\n<script>example</script>\n</skill>\n\nvisible **request** <request>".into(),
            vec![pi_rs::ImageContent { data: "aW1hZ2U=".into(), mime_type: "image/png".into() }],
            None,
        ).unwrap();
        let document = html(&session);
        assert!(document.contains(
            "<details class=\"skill\"><summary>[skill] demo&amp;fixture</summary><h2>Steps</h2>"
        ));
        assert!(document.contains("<pre><code class=\"language-text\">example\n</code></pre>"));
        assert!(document.contains("&lt;script&gt;example&lt;/script&gt;"));
        assert!(document.contains("<article class=\"user\"><b>You</b><p>visible <strong>request</strong> &lt;request&gt;</p>"));
        assert!(document.contains("<a href=\"https://example.test\">docs</a> remote image"));
        assert!(!document.contains("javascript:"));
        assert!(!document.contains("https://example.test/a.png"));
        assert!(document.contains("src=\"data:image/png;base64,aW1hZ2U=\""));
        assert!(!document.contains("<script>"));
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
