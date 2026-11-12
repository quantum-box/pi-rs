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
        let mut headers = HeaderMap::new();
        if !self.session_affinity.is_empty() {
            let value = HeaderValue::from_str(&session_id.to_string()).expect("UUID header value");
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
