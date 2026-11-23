//! Project MCP call results into the provider request without changing the journal.
use std::collections::{BTreeMap, BTreeSet};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use pi_rs::{ImageContent, Message, ModelRequest};
use serde_json::{Value, json};

// MCP tools use the core tool output limit. A projected payload must fit it too.
const MAX_MODEL_CONTENT_BYTES: usize = 1024 * 1024;

pub(super) fn project_tool_results(request: &mut ModelRequest) {
    let readable = request
        .tools
        .iter()
        .filter_map(|spec| spec.name.strip_suffix("__read_resource"))
        .collect::<BTreeSet<_>>();
    let mut calls = BTreeMap::new();
    for message in &mut request.messages {
        match message {
            Message::Assistant { calls: items, .. } => {
                for call in items.iter().filter(|call| call.name.starts_with("mcp__")) {
                    calls.insert(call.id.as_str(), call.name.as_str());
                }
            }
            Message::Tool {
                call_id,
                content,
                is_structured,
                ..
            } if let Some(name) = calls.remove(call_id.as_str())
                && let Some((prefix, tool)) = mcp_tool_name(name, &readable)
                && let Some(server) = prefix.strip_prefix("mcp__")
                && pi_rs::tool_content_is_structured(content, *is_structured) =>
            {
                let read_tool = readable
                    .contains(prefix)
                    .then(|| format!("{prefix}__read_resource"));
                if let Some((text, structured)) =
                    project_result(content, server, tool, read_tool.as_deref())
                {
                    *content = text;
                    *is_structured = Some(structured);
                }
            }
            _ => {}
        }
    }
}

fn mcp_tool_name<'a>(name: &'a str, readable: &BTreeSet<&str>) -> Option<(&'a str, &'a str)> {
    for (separator, _) in name.rmatch_indices("__") {
        let (prefix, suffix) = name.split_at(separator);
        if readable.contains(prefix) {
            return Some((prefix, &suffix[2..]));
        }
    }
    let (server, tool) = name.strip_prefix("mcp__")?.split_once("__")?;
    Some((&name[..5 + server.len()], tool))
}

fn project_result(
    content: &str,
    server: &str,
    tool: &str,
    read_tool: Option<&str>,
) -> Option<(String, bool)> {
    let value: Value = serde_json::from_str(content).ok()?;
    let response = value.get("data")?;
    let mut projected = if tool == "read_resource"
        && let Some(resources) = response["contents"].as_array()
    {
        resources
            .iter()
            .map(|resource| project_resource(resource))
            .collect::<Vec<_>>()
    } else {
        let blocks = response["content"].as_array()?;
        blocks
            .iter()
            .map(|block| project_block(block, server, read_tool))
            .collect()
    };
    if let Some(data) = response.get("structuredContent")
        && !projected.iter().any(|block| {
            block["text"]
                .as_str()
                .and_then(|body| serde_json::from_str::<Value>(body).ok())
                .as_ref()
                == Some(data)
        })
    {
        projected.push(text(serde_json::to_string_pretty(data).ok()?));
    }
    let mut text_content = joined_text(&projected);
    if text_content.is_empty() && response["isError"] == true {
        text_content = format!("MCP tool {server}/{tool} returned an error");
        projected.push(text(&text_content));
    }
    let has_structured_blocks = projected.iter().any(|block| block["type"] != "text");
    let output = if has_structured_blocks {
        // Provider adapters recognize images in the native structured tool envelope.
        json!({"data":{"content":projected},"attachments":[]}).to_string()
    } else {
        text_content
    };
    (output.len() <= MAX_MODEL_CONTENT_BYTES).then_some((output, has_structured_blocks))
}

fn project_block(block: &Value, server: &str, read_tool: Option<&str>) -> Value {
    match block["type"].as_str() {
        Some("text") => block.clone(),
        Some("image") => block["data"]
            .as_str()
            .zip(block["mimeType"].as_str())
            .map_or_else(
                || text("[invalid image omitted]"),
                |(data, mime)| valid_image(data, mime),
            ),
        Some("audio") => {
            let mime = block["mimeType"].as_str().unwrap_or("unknown type");
            text(format!("[audio {mime} omitted]"))
        }
        Some("resource") => project_resource(&block["resource"]),
        Some("resource_link") => {
            resource_link(block, server, read_tool).unwrap_or_else(|| block.clone())
        }
        kind => text(format!(
            "[unsupported MCP content {}]",
            kind.unwrap_or("unknown")
        )),
    }
}

fn project_resource(resource: &Value) -> Value {
    if let Some(content) = resource["text"].as_str() {
        return text(content);
    }
    let mime = resource["mimeType"].as_str().unwrap_or_default();
    if let Some(blob) = resource["blob"].as_str() {
        if mime.starts_with("image/") {
            return valid_image(blob, mime);
        }
        let media = mime
            .split_once(';')
            .map_or(mime, |(media, _)| media)
            .trim()
            .to_ascii_lowercase();
        if (media.starts_with("text/")
            || media == "application/json"
            || media.ends_with("+json")
            || media.ends_with("+xml"))
            && let Ok(decoded) = STANDARD
                .decode(blob)
                .or_else(|_| STANDARD_NO_PAD.decode(blob))
        {
            return text(String::from_utf8_lossy(&decoded));
        }
    }
    // Preserve the payload for native consumers until a readable file can be offered.
    json!({"type":"resource","resource":resource})
}

fn resource_link(block: &Value, server: &str, read_tool: Option<&str>) -> Option<Value> {
    let uri = block["uri"].as_str()?;
    let title = block["title"].as_str().or_else(|| block["name"].as_str())?;
    let mut details = Vec::new();
    if let Some(mime) = block["mimeType"].as_str().filter(|mime| !mime.is_empty()) {
        details.push(mime.to_owned());
    }
    if let Some(size) = block["size"].as_u64() {
        details.push(format_size(size));
    }
    let details = if details.is_empty() {
        String::new()
    } else {
        format!(" ({})", details.join(", "))
    };
    let description = block["description"]
        .as_str()
        .filter(|description| !description.is_empty())
        .map_or_else(String::new, |description| format!(": {description}"));
    let read = read_tool.map_or_else(String::new, |tool| {
        format!(r#". Read it with {tool} (server "{server}")"#)
    });
    Some(text(format!(
        r#"[Resource {uri} "{title}"{details}{description}{read}]"#
    )))
}

fn text(content: impl AsRef<str>) -> Value {
    json!({"type":"text","text":content.as_ref()})
}

fn joined_text(blocks: &[Value]) -> String {
    blocks
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn valid_image(data: &str, mime_type: &str) -> Value {
    let image = ImageContent {
        data: data.into(),
        mime_type: mime_type.into(),
    };
    if image.validate().is_ok() {
        json!({"type":"image","data":image.data,"mimeType":image.mime_type})
    } else {
        text(format!(
            "[unsupported or invalid image {mime_type} omitted]"
        ))
    }
}

fn format_size(bytes: u64) -> String {
    match bytes {
        0..1024 => format!("{bytes}B"),
        1024..1_048_576 => format!("{:.1}KB", bytes as f64 / 1024.0),
        _ => format!("{:.1}MB", bytes as f64 / 1_048_576.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs::{ToolCall, ToolSpec};

    fn tool_content(data: Value) -> String {
        json!({"data":data,"attachments":[]}).to_string()
    }

    #[test]
    fn mcp_model_content_projects_resources_only_for_mcp_calls() {
        let content = tool_content(json!({
            "content":[
                {"type":"resource","resource":{"uri":"mock://readme","mimeType":"Text/Plain; charset=utf-8","blob":"aGVsbG8"}},
                {"type":"resource_link","uri":"mock://notes","title":"Notes","name":"notes","size":2048,"mimeType":"text/plain","description":"Read me"},
                {"type":"audio","mimeType":"audio/wav","data":"YQ=="}
            ],
            "structuredContent":{"internal":"raw result retained"}
        }));
        let mut request = ModelRequest {
            model: "fixture".into(),
            max_output_tokens: 1,
            tools: vec![ToolSpec {
                name: "mcp__fixture__read_resource".into(),
                description: "".into(),
                parameters: json!({"type":"object"}),
            }],
            messages: vec![Message::Assistant {
                content: "".into(),
                replay: None,
                calls: [("mcp", "mcp__fixture__fetch__notes"), ("native", "run")]
                    .into_iter()
                    .map(|(id, name)| ToolCall {
                        id: id.into(),
                        name: name.into(),
                        arguments: "{}".into(),
                    })
                    .collect(),
            }],
        };
        for call_id in ["mcp", "native"] {
            request.messages.push(Message::Tool {
                call_id: call_id.into(),
                content: content.clone(),
                is_error: false,
                is_structured: Some(true),
            });
        }
        let original = request.clone();
        project_tool_results(&mut request);
        assert_eq!(request.messages[2], original.messages[2]);
        assert_eq!(request.messages[1], Message::Tool {
            call_id: "mcp".into(),
            content: "hello\n[Resource mock://notes \"Notes\" (text/plain, 2.0KB): Read me. Read it with mcp__fixture__read_resource (server \"fixture\")]\n[audio audio/wav omitted]\n{\n  \"internal\": \"raw result retained\"\n}".into(),
            is_error: false,
            is_structured: Some(false),
        });
        let once = request.messages.clone();
        project_tool_results(&mut request);
        assert_eq!(once, request.messages);
    }

    #[test]
    fn mcp_model_content_keeps_embedded_images_available_to_providers() {
        let content = tool_content(json!({"content":[
            {"type":"resource","resource":{"uri":"mock://image","mimeType":"image/png","blob":"YQ=="}},
            {"type":"text","text":"caption"},
            {"type":"resource","resource":{"uri":"mock://binary","mimeType":"application/octet-stream","blob":"dGVzdA=="}},
        ]}));
        let (projected, structured) =
            project_result(&content, "fixture", "get_image", None).unwrap();
        assert!(structured);
        let (caption, images) = pi_rs::extract_tool_images(projected);
        assert_eq!(
            images,
            vec![ImageContent {
                data: "YQ==".into(),
                mime_type: "image/png".into()
            }]
        );
        assert!(caption.contains("caption"));
        assert!(!caption.contains("YQ=="));
        assert!(caption.contains("dGVzdA=="));
    }

    #[test]
    fn mcp_model_content_preserves_binary_and_projects_structured_fallback_and_errors() {
        let binary = tool_content(json!({"content":[{"type":"resource","resource":{
            "uri":"mock://binary","mimeType":"application/octet-stream","blob":"YQ==",
        }}]}));
        assert_eq!(
            project_result(&binary, "fixture", "get", None),
            Some((binary.clone(), true))
        );
        let read = tool_content(json!({"contents":[{"uri":"mock://notes","text":"contents"}]}));
        assert_eq!(
            project_result(&read, "fixture", "read_resource", None),
            Some(("contents".into(), false))
        );
        assert_eq!(project_result(&read, "fixture", "other", None), None);
        let data_only = tool_content(json!({"content":[],"structuredContent":{"a":1}}));
        assert_eq!(
            project_result(&data_only, "fixture", "get", None),
            Some(("{\n  \"a\": 1\n}".into(), false))
        );
        let error_only = tool_content(json!({"content":[],"isError":true}));
        assert_eq!(
            project_result(&error_only, "fixture", "get", None),
            Some(("MCP tool fixture/get returned an error".into(), false))
        );
    }
}
