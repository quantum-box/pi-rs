//! Apply workbench request policy and catalog usage estimates.
use futures_util::StreamExt;
use pi_rs::{Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Result};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{model_profiles::ChatPrice, providers::Client};

pub(super) const BLOCKED_IMAGE: &str = "Image reading is disabled.";
const NON_VISION_USER_IMAGE: &str = "(image omitted: model does not support images)";
const NON_VISION_TOOL_IMAGE: &str = "(tool image omitted: model does not support images)";

pub(super) struct WorkbenchModel {
    pub client: Client,
    pub block_images: bool,
    pub supports_images: Option<bool>,
    pub session_affinity: &'static [&'static str],
    pub price: Option<ChatPrice>,
}

#[async_trait::async_trait]
impl Model for WorkbenchModel {
    async fn prepare_context<'a>(
        &'a self,
        request: &ModelRequest,
    ) -> Result<Option<std::sync::Arc<dyn Model + 'a>>> {
        let Some((client, identity)) = self.client.prepare_for_context(&request.model).await?
        else {
            return Ok(None);
        };
        let identity = identity.and_then(|identity| {
            pi_rs::fingerprint_model_context(&(
                "workbench-policy-v2",
                identity,
                self.block_images,
                self.supports_images,
                self.session_affinity,
            ))
        });
        Ok(Some(std::sync::Arc::new(PreparedWorkbenchModel {
            inner: WorkbenchModel {
                client,
                block_images: self.block_images,
                supports_images: self.supports_images,
                session_affinity: self.session_affinity,
                price: self.price.clone(),
            },
            identity,
        })))
    }
    fn context_identity(&self) -> Option<String> {
        pi_rs::fingerprint_model_context(&(
            "workbench-policy-v2",
            self.client.context_identity()?,
            self.block_images,
            self.supports_images,
            self.session_affinity,
        ))
    }

    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.client
            .estimate_request_tokens(&self.filtered(request.clone()))
    }
    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let mut one = request.clone();
        one.messages = vec![message.clone()];
        let filtered = self.filtered(one);
        filtered
            .messages
            .iter()
            .map(|m| self.client.estimate_message_tokens(&filtered, m))
            .fold(0_u64, u64::saturating_add)
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        self.client.context_window(model).await
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let stream = self.client.stream(self.filtered(request)).await?;
        Ok(self.estimate_usage(stream))
    }

    async fn stream_in_session(
        &self,
        _request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_in_session_with_identity(
            _request_id,
            session_id,
            &session_id.to_string(),
            request,
        )
        .await
    }

    async fn stream_in_session_with_identity(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        let mut headers = HeaderMap::new();
        if !self.session_affinity.is_empty() && !external_id.is_empty() {
            // Fetch's Headers converts JS strings to ByteString (Latin-1),
            // then strips HTTP whitespace at the edges before validation.
            // Reject unrepresentable or invalid bytes rather than dispatching
            // a request with a different or missing affinity identity.
            let bytes = external_id
                .chars()
                .map(|ch| u8::try_from(u32::from(ch)))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| pi_rs::Error::Config("Invalid session affinity ID".into()))?;
            let whitespace = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\r' | b'\n');
            let start = bytes
                .iter()
                .position(|byte| !whitespace(byte))
                .unwrap_or(bytes.len());
            let end = bytes
                .iter()
                .rposition(|byte| !whitespace(byte))
                .map_or(start, |index| index + 1);
            let trimmed = &bytes[start..end];
            let value = HeaderValue::from_bytes(trimmed)
                .map_err(|_| pi_rs::Error::Config("Invalid session affinity ID".into()))?;
            for &name in self.session_affinity {
                headers.insert(name, value.clone());
            }
        }
        let stream = self
            .client
            .stream_with_headers(self.filtered(request), &headers, false)
            .await?;
        Ok(self.estimate_usage(stream))
    }
    async fn stream_for_summary(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        let stream = self
            .client
            .stream_with_headers(self.filtered(request), &HeaderMap::new(), true)
            .await?;
        Ok(self.estimate_usage(stream))
    }
}

struct PreparedWorkbenchModel {
    inner: WorkbenchModel,
    identity: Option<String>,
}

#[async_trait::async_trait]
impl Model for PreparedWorkbenchModel {
    fn context_identity(&self) -> Option<String> {
        self.identity.clone()
    }
    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.inner.estimate_request_tokens(request)
    }
    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        self.inner.estimate_message_tokens(request, message)
    }
    async fn context_window(&self, model: &str) -> Option<u32> {
        self.inner.context_window(model).await
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        self.inner.stream(request).await
    }
    async fn stream_in_session(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.inner
            .stream_in_session(request_id, session_id, request)
            .await
    }
    async fn stream_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.inner
            .stream_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }
    async fn stream_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.inner
            .stream_for_summary(request_id, session_id, request)
            .await
    }
}

impl WorkbenchModel {
    fn estimate_usage(&self, mut stream: ModelStream) -> ModelStream {
        let Some(price) = self.price.clone() else {
            return stream;
        };
        Box::pin(async_stream::try_stream! {
            let mut metadata = ModelMetadata::default();
            while let Some(event) = stream.next().await {
                let event = event?;
                if let ModelEvent::Metadata(current) = &event {
                    metadata.clone_from(current);
                }
                if let ModelEvent::Complete { usage: Some(usage), .. } = &event
                    && metadata.cost_breakdown.is_none()
                    && let Some(breakdown) = price.estimate(*usage, &metadata)
                    && let Some(total) = breakdown.total()
                {
                    metadata.cost_breakdown = Some(breakdown);
                    if metadata.cost_usd.is_none() {
                        metadata.cost_usd = Some(total);
                        metadata.cost_from_provider = Some(false);
                    }
                    yield ModelEvent::Metadata(metadata.clone());
                }
                yield event;
            }
        })
    }

    fn filtered(&self, mut request: ModelRequest) -> ModelRequest {
        if self.block_images {
            remove_images(&mut request.messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        } else if self.supports_images == Some(false) {
            remove_images(
                &mut request.messages,
                NON_VISION_USER_IMAGE,
                NON_VISION_TOOL_IMAGE,
            );
        }
        request
    }
}

fn remove_images(messages: &mut [Message], user_placeholder: &str, tool_placeholder: &str) {
    for message in messages {
        match message {
            Message::UserWithImages { content, images } if !images.is_empty() => {
                let mut content = std::mem::take(content);
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(user_placeholder);
                *message = Message::User { content };
            }
            Message::Tool {
                content,
                is_structured,
                ..
            } => {
                if pi_rs::tool_content_is_structured(content, *is_structured)
                    && let Some(filtered) = remove_tool_images(content, tool_placeholder)
                {
                    *content = filtered;
                }
            }
            _ => {}
        }
    }
}

fn remove_tool_images(content: &str, placeholder: &str) -> Option<String> {
    let mut result: Value = serde_json::from_str(content).ok()?;
    let blocks = result.get_mut("data")?.get_mut("content")?.as_array_mut()?;
    let mut previous_placeholder = false;
    let mut changed = false;
    blocks.retain_mut(|block| {
        if block["type"] == "image" {
            changed = true;
            if previous_placeholder {
                return false;
            }
            *block = json!({"type":"text","text":placeholder});
        }
        previous_placeholder = block["type"] == "text" && block["text"] == placeholder;
        true
    });
    changed.then(|| result.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn external_identity_is_used_for_affinity_header() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        for (identity, affinity, expected_header) in [
            ("custom-id.1", true, Some(&b"custom-id.1"[..])),
            ("", true, None),
            ("café", true, Some(&b"caf\xe9"[..])),
            ("\u{80}ÿ", true, Some(&b"\x80\xff"[..])),
            (" \tcafé\r\n", true, Some(&b"caf\xe9"[..])),
            ("a\tb", true, Some(&b"a\tb"[..])),
            (" \t ", true, Some(&b""[..])),
            ("日本", false, None),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("import.jsonl");
            std::fs::write(
                &path,
                format!(
                    "{}\n",
                    serde_json::json!({"type":"session", "version":3, "id": identity})
                ),
            )
            .unwrap();
            let session = crate::session_import::read(&path).unwrap();
            assert_eq!(session.external_session_id(), identity);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut block = [0; 1024];
                    let count = socket.read(&mut block).unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                bytes
            });
            let model = WorkbenchModel {
                client: Client::Ollama(
                    pi_rs::ollama::Ollama::new(
                        &format!("http://{address}"),
                        Some("fixture-key".into()),
                    )
                    .unwrap()
                    .with_native_probes(false),
                ),
                block_images: false,
                supports_images: None,
                session_affinity: if affinity { &["x-session-id"] } else { &[] },
                price: None,
            };
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            };
            let _ = model
                .stream_in_session_with_identity(
                    Uuid::new_v4(),
                    session.session_id(),
                    &session.external_session_id(),
                    request,
                )
                .await;
            let headers = server.join().unwrap();
            let value = headers.split(|byte| *byte == b'\n').find_map(|line| {
                let colon = line.iter().position(|byte| *byte == b':')?;
                if !line[..colon].eq_ignore_ascii_case(b"x-session-id") {
                    return None;
                }
                let value = line[colon + 1..]
                    .strip_suffix(b"\r")
                    .unwrap_or(&line[colon + 1..]);
                Some(value.strip_prefix(b" ").unwrap_or(value).to_vec())
            });
            assert_eq!(value.as_deref(), expected_header, "identity {identity:?}");
        }
    }

    #[tokio::test]
    async fn unrepresentable_affinity_identity_fails_before_dispatch() {
        use std::net::TcpListener;
        for identity in [
            "日本",
            "\u{100}",
            "🤖",
            "unsafe\r\nx-injected: yes",
            "invalid\0id",
            "bad\x7fid",
            "\x0cid",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let model = WorkbenchModel {
                client: Client::Ollama(
                    pi_rs::ollama::Ollama::new(
                        &format!("http://{address}"),
                        Some("fixture-key".into()),
                    )
                    .unwrap()
                    .with_native_probes(false),
                ),
                block_images: false,
                supports_images: None,
                session_affinity: &["x-session-id"],
                price: None,
            };
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                model.stream_in_session_with_identity(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    identity,
                    request,
                ),
            )
            .await
            .expect("invalid ID must fail before network access");
            assert!(matches!(result, Err(pi_rs::Error::Config(_))));
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn effective_context_identity_includes_image_policy_and_adapter() {
        let make = |endpoint: &str, block_images, supports_images| WorkbenchModel {
            client: Client::Google(pi_rs::google::Google::new(endpoint, "fixture-key").unwrap()),
            block_images,
            supports_images,
            session_affinity: &[],
            price: None,
        };
        let original = make("https://example.com/v1", false, Some(true));
        let blocked = make("https://example.com/v1", true, Some(true));
        let nonvision = make("https://example.com/v1", false, Some(false));
        let other = make("https://other.example.com/v1", false, Some(true));
        assert!(original.context_identity().is_some());
        assert_ne!(original.context_identity(), blocked.context_identity());
        assert_ne!(original.context_identity(), nonvision.context_identity());
        assert_ne!(original.context_identity(), other.context_identity());
        let request = ModelRequest {
            model: "fixture".into(),
            tools: vec![],
            max_output_tokens: 1,
            messages: vec![Message::UserWithImages {
                content: "look".into(),
                images: vec![pi_rs::ImageContent {
                    mime_type: "image/png".into(),
                    data: "AAAA".into(),
                }],
            }],
        };
        assert_ne!(
            original.filtered(request.clone()).messages,
            blocked.filtered(request).messages
        );
    }

    #[test]
    fn image_policy_replaces_only_model_visible_image_blocks() {
        let original = json!({"data":{"content":[
            {"type":"text","text":"before"},
            {"type":"image","mimeType":"image/png","data":"private-one"},
            {"type":"image","data":"private-two"},
            {"type":"text","text":"after"},
        ]}});
        let mut messages = vec![
            Message::UserWithImages {
                content: "prompt".into(),
                images: vec![pi_rs::ImageContent {
                    data: "private-user".into(),
                    mime_type: "image/png".into(),
                }],
            },
            Message::Tool {
                is_structured: Some(true),
                is_error: false,
                call_id: "call".into(),
                content: original.to_string(),
            },
        ];
        remove_images(&mut messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        assert_eq!(
            messages[0],
            Message::User {
                content: format!("prompt\n\n{BLOCKED_IMAGE}")
            }
        );
        let Message::Tool { content, .. } = &messages[1] else {
            panic!("expected tool message")
        };
        assert_eq!(
            serde_json::from_str::<Value>(content).unwrap()["data"]["content"],
            json!([{"type":"text","text":"before"},{"type":"text","text":BLOCKED_IMAGE},{"type":"text","text":"after"}]),
        );
    }

    #[test]
    fn plain_json_tool_text_is_not_changed_by_image_policy() {
        let content = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"private"}] } }"#;
        let mut messages = vec![Message::Tool {
            call_id: "id".into(),
            content: content.into(),
            is_error: false,
            is_structured: Some(false),
        }];
        remove_images(&mut messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        assert!(matches!(&messages[0], Message::Tool { content: actual, .. } if actual == content));
        let legacy =
            serde_json::from_value(json!({"role":"tool", "call_id":"id", "content":content}))
                .unwrap();
        let mut messages = vec![legacy];
        remove_images(&mut messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        let Message::Tool {
            content: filtered,
            is_structured: None,
            ..
        } = &messages[0]
        else {
            panic!("legacy tool message missing")
        };
        assert!(!filtered.contains("private"));
        assert!(filtered.contains(BLOCKED_IMAGE));
    }

    #[test]
    fn non_vision_models_receive_reference_placeholders_for_user_and_tool_images() {
        let mut messages = vec![
            Message::UserWithImages {
                content: "look".into(),
                images: vec![pi_rs::ImageContent { data: "private".into(), mime_type: "image/png".into() }],
            },
            Message::Tool {
                is_structured: Some(true),
                is_error: false, call_id: "call".into(),
                content: json!({"data":{"content":[{"type":"text","text":"before"},{"type":"image","data":"private"},{"type":"image","data":"private"},{"type":"text","text":"after"}]}}).to_string(),
            },
        ];
        remove_images(&mut messages, NON_VISION_USER_IMAGE, NON_VISION_TOOL_IMAGE);
        assert_eq!(
            messages[0],
            Message::User {
                content: format!("look\n\n{NON_VISION_USER_IMAGE}")
            }
        );
        let Message::Tool { content, .. } = &messages[1] else {
            panic!("expected tool result")
        };
        assert_eq!(
            serde_json::from_str::<Value>(content).unwrap()["data"]["content"],
            json!([
                {"type":"text","text":"before"},
                {"type":"text","text":NON_VISION_TOOL_IMAGE},
                {"type":"text","text":"after"}
            ])
        );
    }
}
