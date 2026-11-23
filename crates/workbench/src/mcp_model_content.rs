//! Project MCP call results into the provider request without changing the journal.
use std::{
    collections::BTreeMap,
    io::{self, Write},
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use pi_rs::{ImageContent, Message, ModelRequest};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

// MCP tools use the core tool output limit. A projected payload must fit it too.
const MAX_MODEL_CONTENT_BYTES: usize = 1024 * 1024;
const MAX_OUTPUT_FILES: usize = 64;
const TEXT_PREVIEW_BYTES: usize = 20 * 1024;
const MAX_OUTPUT_FILE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Default)]
struct ResourceOutputs {
    directory: Option<tempfile::TempDir>,
    files: BTreeMap<([u8; 32], String), PathBuf>,
    bytes: usize,
}

impl ResourceOutputs {
    fn save(&mut self, data: &[u8], extension: &str) -> io::Result<PathBuf> {
        let key = (Sha256::digest(data).into(), extension.to_owned());
        if let Some(path) = self.files.get(&key).filter(|path| path.is_file()) {
            return Ok(path.clone());
        }
        let new = !self.files.contains_key(&key);
        if new
            && (self.files.len() >= MAX_OUTPUT_FILES
                || self.bytes + data.len() > MAX_OUTPUT_FILE_BYTES)
        {
            return Err(io::Error::other("native MCP output file limit reached"));
        }
        let directory = match &self.directory {
            Some(directory) => directory,
            None => self
                .directory
                .insert(tempfile::Builder::new().prefix("pi-mcp-").tempdir()?),
        };
        let mut file = tempfile::Builder::new()
            .prefix("resource-")
            .suffix(extension)
            .tempfile_in(directory.path())?;
        file.write_all(data)?;
        let (_, path) = file.keep().map_err(|error| error.error)?;
        self.files.insert(key, path.clone());
        if new {
            self.bytes += data.len();
        }
        Ok(path)
    }
}

// Estimation and dispatch can project the same history repeatedly.
static RESOURCE_OUTPUTS: OnceLock<Mutex<ResourceOutputs>> = OnceLock::new();

pub(super) fn cleanup_output_files() {
    if let Some(outputs) = RESOURCE_OUTPUTS.get() {
        let mut outputs = outputs.lock().unwrap_or_else(|error| error.into_inner());
        *outputs = ResourceOutputs::default();
    }
}

fn save_output_file(data: &[u8], extension: &str) -> io::Result<PathBuf> {
    RESOURCE_OUTPUTS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| io::Error::other("MCP output file store unavailable"))?
        .save(data, extension)
}

#[derive(Clone, Copy)]
struct FileReaders {
    read: bool,
    shell: bool,
}

impl FileReaders {
    fn from_request(request: &ModelRequest) -> Self {
        Self {
            read: request.tools.iter().any(|tool| {
                tool.name == "read" && tool.parameters["properties"]["offset"]["type"] == "number"
            }),
            shell: request.tools.iter().any(|tool| {
                matches!(tool.name.as_str(), "bash" | "powershell")
                    && tool.parameters["properties"]["timeout"] == json!({"type":"number"})
            }),
        }
    }
}

pub(super) fn project_tool_results(request: &mut ModelRequest) {
    let files = FileReaders::from_request(request);
    let readable = request
        .tools
        .iter()
        .filter(|spec| spec.description == pi_rs_workbench::mcp::READ_RESOURCE_DESCRIPTION)
        .filter_map(|spec| {
            let (prefix, name) = spec.name.rsplit_once("__")?;
            is_resource_read(name).then_some((prefix, spec.name.as_str()))
        })
        .collect::<BTreeMap<_, _>>();
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
            } if *is_structured != Some(false) => {
                let Some((prefix, tool)) = calls
                    .remove(call_id.as_str())
                    .and_then(|name| mcp_tool_name(name, &readable))
                else {
                    continue;
                };
                let Some(server) = prefix.strip_prefix("mcp__") else {
                    continue;
                };
                let read_tool = readable.get(prefix).copied();
                if let Some((text, structured)) =
                    project_result_with_files(content, server, tool, read_tool, files)
                {
                    *content = text;
                    *is_structured = Some(structured);
                } else if is_structured.is_none()
                    && is_resource_read(tool)
                    && serde_json::from_str::<Value>(content)
                        .is_ok_and(|value| value["data"]["contents"].is_array())
                {
                    // Historical resource reads predate the structured provenance flag.
                    *is_structured = Some(true);
                }
            }
            _ => {}
        }
    }
}

fn mcp_tool_name<'a>(name: &'a str, readable: &BTreeMap<&str, &str>) -> Option<(&'a str, &'a str)> {
    for (separator, _) in name.rmatch_indices("__") {
        let (prefix, suffix) = name.split_at(separator);
        if readable.contains_key(prefix) {
            return Some((prefix, &suffix[2..]));
        }
    }
    if let Some((separator, _)) = name
        .rmatch_indices("__")
        .find(|(index, _)| is_resource_read(&name[index + 2..]))
    {
        return Some((&name[..separator], &name[separator + 2..]));
    }
    let (server, tool) = name.strip_prefix("mcp__")?.split_once("__")?;
    Some((&name[..5 + server.len()], tool))
}

fn is_resource_read(name: &str) -> bool {
    let Some(mut suffix) = name.strip_prefix("read_resource") else {
        return false;
    };
    while let Some(rest) = suffix.strip_prefix("_resource") {
        suffix = rest;
    }
    suffix.is_empty()
}

#[cfg(test)]
fn project_result(
    content: &str,
    server: &str,
    tool: &str,
    read_tool: Option<&str>,
) -> Option<(String, bool)> {
    project_result_with_files(
        content,
        server,
        tool,
        read_tool,
        FileReaders {
            read: true,
            shell: true,
        },
    )
}

fn project_result_with_files(
    content: &str,
    server: &str,
    tool: &str,
    read_tool: Option<&str>,
    files: FileReaders,
) -> Option<(String, bool)> {
    let value: Value = serde_json::from_str(content).ok()?;
    let response = value.get("data")?;
    let mut projected = if is_resource_read(tool)
        && let Some(resources) = response["contents"].as_array()
    {
        resources
            .iter()
            .map(|resource| project_resource(resource, resources.len() > 1, files.shell))
            .collect::<Vec<_>>()
    } else {
        let blocks = response["content"].as_array()?;
        let multiple_resources = blocks
            .iter()
            .filter(|block| block["type"] == "resource")
            .take(2)
            .count()
            > 1;
        blocks
            .iter()
            .map(|block| project_block(block, server, read_tool, multiple_resources, files.shell))
            .collect()
    };
    if response["isError"] == true {
        // Some provider tool-result formats have no separate error field.
        projected.insert(
            0,
            text(format!("MCP tool {server}/{tool} returned an error")),
        );
    }
    let distinct_data = response.get("structuredContent").filter(|data| {
        !projected.iter().any(|block| {
            block["text"]
                .as_str()
                .and_then(|body| serde_json::from_str::<Value>(body).ok())
                .as_ref()
                == Some(*data)
        })
    });
    if let Some(data) = distinct_data {
        projected.push(text(serde_json::to_string_pretty(data).ok()?));
    }
    limit_text_preview(&mut projected, files);
    let (output, structured) = encode_blocks(&projected);
    Some(if output.len() <= MAX_MODEL_CONTENT_BYTES {
        (output, structured)
    } else {
        encode_limited_blocks(&projected)
    })
}

fn limit_text_preview(projected: &mut Vec<Value>, files: FileReaders) {
    let full = joined_text(projected);
    if full.len() <= TEXT_PREVIEW_BYTES {
        return;
    }
    let page_by_line = files.read
        && full
            .lines()
            .all(|line| line.len() <= pi_rs_workbench::coding::READ_PAGE_BYTES);
    let hint = if page_by_line {
        "read it with offset/limit"
    } else if files.shell {
        "read byte ranges with the local shell"
    } else {
        return;
    };
    // Keep the inline form if there is no usable copy on disk.
    let Ok(path) = save_output_file(full.as_bytes(), ".txt") else {
        return;
    };
    let mut head = TEXT_PREVIEW_BYTES / 2;
    let mut tail = full.len() - TEXT_PREVIEW_BYTES / 2;
    while !full.is_char_boundary(head) {
        head -= 1;
    }
    while !full.is_char_boundary(tail) {
        tail += 1;
    }
    let tokens = full.len().div_ceil(4);
    let total_lines = full.lines().count();
    let removed = full[head..tail].chars().count();
    let location = format!("[Full output: {} ({hint})]", path.display());
    let preview = format!(
        "Warning: truncated output (original token count: {tokens})\nTotal output lines: {total_lines}\n\n{}…{removed} chars truncated…{}\n\n{location}",
        &full[..head],
        &full[tail..]
    );
    projected.retain(|block| block["type"] != "text");
    projected.insert(0, text(preview));
}

fn encode_limited_blocks(projected: &[Value]) -> (String, bool) {
    // Text may expand when escaped in an image-bearing JSON envelope. Leave
    // room for the envelope and notice, and reserve the complete media first.
    let mut available = MAX_MODEL_CONTENT_BYTES - 512;
    let mut blocks = Vec::new();
    let mut omitted_media = false;
    for block in projected.iter().filter(|block| block["type"] != "text") {
        let bytes = block.to_string().len() + 1;
        if bytes <= available {
            available -= bytes;
            blocks.push(block.clone());
        } else {
            omitted_media = true;
        }
    }
    let mut preview = joined_text(projected);
    let text_limit = if blocks.is_empty() {
        available
    } else {
        available / 6
    };
    let truncated_text = preview.len() > text_limit;
    if truncated_text {
        let mut boundary = text_limit;
        while !preview.is_char_boundary(boundary) {
            boundary -= 1;
        }
        preview.truncate(boundary);
    }
    if truncated_text {
        preview.push_str("\n[MCP text truncated at the native output limit]");
    }
    if omitted_media {
        preview.push_str("\n[MCP resource content omitted at the native output limit]");
    }
    blocks.insert(0, text(preview));
    encode_blocks(&blocks)
}

fn encode_blocks(projected: &[Value]) -> (String, bool) {
    if projected.iter().any(|block| block["type"] != "text") {
        // Provider adapters recognize images in the native structured tool envelope.
        (
            json!({"data":{"content":projected},"attachments":[]}).to_string(),
            true,
        )
    } else {
        (joined_text(projected), false)
    }
}

fn project_block(
    block: &Value,
    server: &str,
    read_tool: Option<&str>,
    multiple_resources: bool,
    binary_file: bool,
) -> Value {
    match block["type"].as_str() {
        Some("text") => block.clone(),
        Some("image") => block["data"]
            .as_str()
            .zip(block["mimeType"].as_str())
            .and_then(|(data, mime)| provider_image(data, mime))
            .unwrap_or_else(|| block.clone()),
        Some("audio") => {
            let mime = block["mimeType"].as_str().unwrap_or("unknown type");
            text(format!("[audio {mime} omitted]"))
        }
        Some("resource") => project_resource(&block["resource"], multiple_resources, binary_file),
        Some("resource_link") => {
            resource_link(block, server, read_tool).unwrap_or_else(|| block.clone())
        }
        kind => text(format!(
            "[unsupported MCP content {}]",
            kind.unwrap_or("unknown")
        )),
    }
}

fn project_resource(resource: &Value, identify: bool, binary_file: bool) -> Value {
    let content_text = |content: &str| {
        if identify && let Some(uri) = resource["uri"].as_str() {
            text(format!("[Resource {uri}]\n{content}"))
        } else {
            text(content)
        }
    };
    if let Some(content) = resource["text"].as_str() {
        return content_text(content);
    }
    let mime = resource["mimeType"].as_str().unwrap_or_default();
    if let Some(blob) = resource["blob"].as_str() {
        if mime.starts_with("image/")
            && let Some(image) = provider_image(blob, mime)
        {
            return image;
        }
        let media = mime
            .split_once(';')
            .map_or(mime, |(media, _)| media)
            .trim()
            .to_ascii_lowercase();
        let textual = media.starts_with("text/")
            || media == "application/json"
            || media.ends_with("+json")
            || media.ends_with("+xml");
        if textual && let Ok(decoded) = decode_blob(blob) {
            return content_text(&String::from_utf8_lossy(&decoded));
        }
        // Keep unsupported image formats subject to the native image policy.
        if binary_file && !media.starts_with("image/") {
            let uri = resource["uri"].as_str().unwrap_or("unknown URI");
            if let Some(output) = binary_resource(uri, mime, blob) {
                return output;
            }
        }
    }
    json!({"type":"resource","resource":resource})
}

fn decode_blob(blob: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    STANDARD
        .decode(blob)
        .or_else(|_| STANDARD_NO_PAD.decode(blob))
}

fn binary_resource(uri: &str, mime: &str, blob: &str) -> Option<Value> {
    if blob.len() > MAX_MODEL_CONTENT_BYTES {
        return None;
    }
    let mime = if mime.is_empty() {
        "unknown type"
    } else {
        mime
    };
    let data = decode_blob(blob).ok()?;
    let size = format_size(data.len() as u64);
    let extension = resource_extension(uri);
    let path = save_output_file(&data, &extension).ok()?;
    Some(text(format!(
        "[Binary resource {uri} ({mime}, {size}) saved to {}]",
        path.display()
    )))
}

fn resource_extension(uri: &str) -> String {
    let url = reqwest::Url::parse(uri).ok();
    let path = url.as_ref().map_or(uri, |url| url.path());
    path.rsplit_once('.')
        .map(|(_, extension)| extension)
        .filter(|extension| {
            (1..=8).contains(&extension.len())
                && extension.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
        .map_or_else(|| ".bin".into(), |extension| format!(".{extension}"))
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

fn provider_image(data: &str, mime_type: &str) -> Option<Value> {
    let image = ImageContent {
        data: data.into(),
        mime_type: mime_type.into(),
    };
    image.validate().ok()?;
    Some(json!({"type":"image","data":image.data,"mimeType":image.mime_type}))
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
                name: "mcp__fixture__read_resource_resource".into(),
                description: pi_rs_workbench::mcp::READ_RESOURCE_DESCRIPTION.into(),
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
            content: "hello\n[Resource mock://notes \"Notes\" (text/plain, 2.0KB): Read me. Read it with mcp__fixture__read_resource_resource (server \"fixture\")]\n[audio audio/wav omitted]\n{\n  \"internal\": \"raw result retained\"\n}".into(),
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
        assert!(
            caption.contains(
                "[Binary resource mock://binary (application/octet-stream, 4B) saved to "
            )
        );
        assert!(!caption.contains("dGVzdA=="));
    }

    #[test]
    fn mcp_model_content_projects_legacy_structured_resource_reads() {
        let mut request = ModelRequest {
            model: "fixture".into(),
            max_output_tokens: 1,
            tools: vec![],
            messages: vec![
                Message::Assistant {
                    content: "".into(),
                    replay: None,
                    calls: vec![ToolCall {
                        id: "read".into(),
                        name: "mcp__foo__bar__read_resource".into(),
                        arguments: "{}".into(),
                    }],
                },
                Message::Tool {
                    call_id: "read".into(),
                    content: tool_content(
                        json!({"contents":[{"uri":"mock://notes","text":"notes"}]}),
                    ),
                    is_error: false,
                    is_structured: None,
                },
            ],
        };
        project_tool_results(&mut request);
        assert_eq!(
            request.messages[1],
            Message::Tool {
                call_id: "read".into(),
                content: "notes".into(),
                is_error: false,
                is_structured: Some(false),
            }
        );
    }

    #[test]
    fn mcp_model_content_compacts_large_structured_data_and_recognizes_historical_reads() {
        assert_eq!(
            mcp_tool_name("mcp__foo__bar__read_resource_resource", &BTreeMap::new()),
            Some(("mcp__foo__bar", "read_resource_resource"))
        );
        let data =
            json!({"large":("a".repeat(MAX_MODEL_CONTENT_BYTES - 4096)), "array": vec![0; 1000]});
        let original = tool_content(json!({
            "content":[{"type":"resource","resource":{"uri":"mock://notes","text":"notes"}}],
            "structuredContent":data
        }));
        let (output, structured) = project_result(&original, "fixture", "get", None).unwrap();
        assert!(!structured);
        assert!(output.contains("notes\n"));
        assert!(output.contains("Warning: truncated output"));
        assert!(output.len() < TEXT_PREVIEW_BYTES + 1024);
        let path = output
            .split_once("[Full output: ")
            .unwrap()
            .1
            .strip_suffix(" (read byte ranges with the local shell)]")
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            format!("notes\n{}", serde_json::to_string_pretty(&data).unwrap())
        );
    }

    #[test]
    fn mcp_model_content_preserves_unsupported_image_resources() {
        let original = tool_content(json!({"content":[
            {"type":"resource","resource":{"uri":"mock://vector","mimeType":"image/svg+xml","blob":"PHN2Zy8+"}},
            {"type":"resource","resource":{"uri":"mock://scan","mimeType":"image/tiff","blob":"dGlmZg=="}}
        ]}));
        let (output, structured) = project_result(&original, "fixture", "get", None).unwrap();
        assert!(structured);
        let value: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(
            value["data"]["content"][0]["text"],
            "[Resource mock://vector]\n<svg/>"
        );
        assert_eq!(value["data"]["content"][1]["resource"]["blob"], "dGlmZg==");
    }

    #[test]
    fn mcp_model_content_keeps_images_when_text_expands_past_the_output_limit() {
        let original = tool_content(json!({"content":[
            {"type":"resource","resource":{"uri":"mock://image","mimeType":"image/png","blob":"YQ=="}},
            {"type":"resource","resource":{"uri":"mock://text","mimeType":"text/plain",
                "blob":STANDARD.encode(vec![0; 270_000])}},
        ]}));
        assert!(original.len() < MAX_MODEL_CONTENT_BYTES);
        let (projected, structured) = project_result(&original, "fixture", "get", None).unwrap();
        assert!(structured);
        assert!(projected.len() <= MAX_MODEL_CONTENT_BYTES);
        let (text, images) = pi_rs::extract_tool_images(projected);
        assert!(text.contains("Warning: truncated output"));
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].data, "YQ==");
    }

    #[test]
    fn mcp_model_content_keeps_inline_output_without_usable_file_tools() {
        let long = "x".repeat(TEXT_PREVIEW_BYTES * 3);
        let original = tool_content(json!({"content":[{"type":"text","text":long}]}));
        for read in [false, true] {
            let output = project_result_with_files(
                &original,
                "fixture",
                "get",
                None,
                FileReaders { read, shell: false },
            );
            assert_eq!(output, Some((long.clone(), false)));
        }
        let short_lines = "line\n".repeat(TEXT_PREVIEW_BYTES);
        let original = tool_content(json!({"content":[{"type":"text","text":short_lines}]}));
        let (output, _) = project_result_with_files(
            &original,
            "fixture",
            "get",
            None,
            FileReaders {
                read: true,
                shell: false,
            },
        )
        .unwrap();
        assert!(output.ends_with(" (read it with offset/limit)]"));
    }

    #[test]
    fn mcp_model_content_uses_bounded_private_binary_paths() {
        assert_eq!(
            resource_extension("mock://example/a.pdf?param=value"),
            ".pdf"
        );
        assert_eq!(
            resource_extension("mock://example/../../a.unusual-extension"),
            ".bin"
        );
        let mut outputs = ResourceOutputs::default();
        let file = outputs.save(b"hello", ".pdf").unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"hello");
        assert_eq!(outputs.save(b"hello", ".pdf").unwrap(), file);
        assert_eq!(outputs.bytes, 5);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(file.metadata().unwrap().permissions().mode() & 0o077, 0);
        }
        outputs.bytes = MAX_OUTPUT_FILE_BYTES;
        assert!(outputs.save(b"new", ".pdf").is_err());
        drop(outputs);
        assert!(!file.exists());
    }

    #[test]
    fn mcp_model_content_preserves_binary_and_projects_structured_fallback_and_errors() {
        let binary = tool_content(json!({"content":[{"type":"resource","resource":{
            "uri":"mock://binary","mimeType":"application/octet-stream","blob":"YQ==",
        }}]}));
        let (output, structured) = project_result(&binary, "fixture", "get", None).unwrap();
        let path = output
            .strip_prefix("[Binary resource mock://binary (application/octet-stream, 1B) saved to ")
            .and_then(|output| output.strip_suffix(']'))
            .unwrap();
        assert!(!structured);
        let workspace = tempfile::tempdir().unwrap();
        let tools = pi_rs_workbench::coding::CodingTools::local(workspace.path()).unwrap();
        let readable = tools.resolve_read_path(std::path::Path::new(path)).unwrap();
        assert_eq!(std::fs::read(readable).unwrap(), b"a");
        assert_eq!(
            project_result(&binary, "fixture", "get", None),
            Some((output, false))
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
        let with_error_text = tool_content(json!({
            "content":[{"type":"text","text":"try again later"}],"isError":true
        }));
        assert_eq!(
            project_result(&with_error_text, "fixture", "get", None),
            Some((
                "MCP tool fixture/get returned an error\ntry again later".into(),
                false
            ))
        );
        let error_only = tool_content(json!({"content":[],"isError":true}));
        assert_eq!(
            project_result(&error_only, "fixture", "get", None),
            Some(("MCP tool fixture/get returned an error".into(), false))
        );
    }
}
