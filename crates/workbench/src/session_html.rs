//! Native HTML transcript rendering and export for SDK hosts.

use std::io::{ErrorKind, Write};
use std::path::Path;

use pi_rs::{AgentSession, Error, Event, Result, Session};

/// Export the selected branch of an owned SDK session to an explicit path.
///
/// This uses the SDK-managed journal path to protect the source. Without a
/// managed path, including for in-memory sessions, it creates a new file
/// without replacing an existing one. The destination's parent must exist.
/// A busy journal returns the same error as [`AgentSession::journal`].
pub fn export_agent_session(session: &AgentSession, destination: &Path) -> Result<()> {
    let document = {
        let journal = session.journal()?;
        render(&journal)
    };
    publish(&document, destination, session.session_file())
}

/// Export the selected branch of a journal to an explicit path.
///
/// Supply the source path for a file-backed journal or imported snapshot.
/// With a source path, an existing destination can be replaced unless it refers
/// to the source file. Without one, publication never replaces an existing file.
/// The destination's parent must exist.
pub fn export(session: &Session, destination: &Path, source: Option<&Path>) -> Result<()> {
    publish(&render(session), destination, source)
}

fn publish(document: &str, destination: &Path, source: Option<&Path>) -> Result<()> {
    if let Some(source) = source {
        match destination.canonicalize() {
            Ok(destination) => {
                let source = source.canonicalize()?;
                if same_export_file(&source, &destination)? {
                    return Err(Error::Config(
                        "Cannot export HTML over the source session file".into(),
                    ));
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(document.as_bytes())?;
    temporary.as_file().sync_all()?;
    let published = if source.is_some() {
        temporary.persist(destination)
    } else {
        temporary.persist_noclobber(destination)
    };
    published.map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Compare canonical paths without reading their contents.
#[doc(hidden)]
pub fn same_export_file(source: &Path, destination: &Path) -> std::io::Result<bool> {
    if source == destination {
        return Ok(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let source = source.metadata()?;
        let destination = destination.metadata()?;
        Ok(source.dev() == destination.dev() && source.ino() == destination.ino())
    }
    #[cfg(not(unix))]
    {
        if !destination.metadata()?.is_file() {
            return Ok(false);
        }
        same_file::is_same_file(source, destination)
    }
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Render the selected journal branch as standalone HTML without filesystem I/O.
pub fn render(session: &Session) -> String {
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
                                pi_rs::pi_import::imported_custom_message(&entry["message"], true)
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
            Event::ImportedUnattributedToolResult {
                call_id,
                tool_name,
                content,
                is_error,
                details,
                ..
            } => {
                html.push_str(&format!(
                    "<p>Unattributed tool result: {} (call {}, {})</p>",
                    escape(tool_name),
                    escape(call_id),
                    if *is_error { "error" } else { "success" }
                ));
                html.push_str(&tool_html(&unattributed_outcome(
                    content, *is_error, details,
                )));
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
    let text =
        if let Some((name, body, message)) = crate::skill_resources::skill_invocation(content) {
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
#[doc(hidden)]
pub fn unattributed_outcome(
    content: &pi_rs::ContextContent,
    is_error: bool,
    details: &Option<serde_json::Value>,
) -> pi_rs::Outcome {
    match content {
        pi_rs::ContextContent::Text(text) => pi_rs::Outcome::TextResult {
            output: text.clone(),
            is_error,
            details: details.clone(),
        },
        pi_rs::ContextContent::Blocks(blocks)
            if blocks
                .iter()
                .all(|block| matches!(block, pi_rs::ContentBlock::Text { .. })) =>
        {
            pi_rs::Outcome::TextResult {
                output: blocks
                    .iter()
                    .filter_map(|block| match block {
                        pi_rs::ContentBlock::Text { text, .. } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(""),
                is_error,
                details: details.clone(),
            }
        }
        pi_rs::ContextContent::Blocks(blocks) => pi_rs::Outcome::StructuredWithDetails {
            data: serde_json::json!({"content":blocks,"isError":is_error}),
            attachments: Vec::new(),
            details: details.clone().unwrap_or(serde_json::Value::Null),
        },
    }
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
