//! Apply the workbench's image policy to the final outbound model context.
use pi_rs::{Message, Model, ModelRequest, ModelStream, Result};
use serde_json::{Value, json};

use super::providers::Client;

pub(super) const BLOCKED_IMAGE: &str = "Image reading is disabled.";

pub(super) struct WorkbenchModel {
    pub client: Client,
    pub block_images: bool,
}

#[async_trait::async_trait]
impl Model for WorkbenchModel {
    async fn context_window(&self, model: &str) -> Option<u32> {
        self.client.context_window(model).await
    }

    async fn stream(&self, mut request: ModelRequest) -> Result<ModelStream> {
        if self.block_images {
            remove_images(&mut request.messages);
        }
        self.client.stream(request).await
    }
}

fn remove_images(messages: &mut [Message]) {
    for message in messages {
        match message {
            Message::UserWithImages { content, images } if !images.is_empty() => {
                let mut content = std::mem::take(content);
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(BLOCKED_IMAGE);
                *message = Message::User { content };
            }
            Message::Tool { content, .. } => {
                if let Some(filtered) = remove_tool_images(content) {
                    *content = filtered;
                }
            }
            _ => {}
        }
    }
}

fn remove_tool_images(content: &str) -> Option<String> {
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
            *block = json!({"type":"text","text":BLOCKED_IMAGE});
        }
        previous_placeholder = block["type"] == "text" && block["text"] == BLOCKED_IMAGE;
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
                call_id: "call".into(),
                content: original.to_string(),
            },
        ];
        remove_images(&mut messages);
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
}
