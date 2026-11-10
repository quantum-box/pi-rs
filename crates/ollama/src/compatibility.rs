//! Wire differences for explicitly configured chat-completions providers.
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ChatCompatibility {
    supports_store: Option<bool>,
    pub(super) supports_developer_role: Option<bool>,
    supports_usage_in_streaming: Option<bool>,
    pub(super) supports_finish_reason: Option<bool>,
    supports_reasoning_effort: Option<bool>,
    pub(super) requires_reasoning_content_on_assistant_messages: Option<bool>,
    max_tokens_field: Option<MaxTokensField>,
    thinking_format: Option<ThinkingFormat>,
    zai_tool_stream: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MaxTokensField {
    MaxTokens,
    MaxCompletionTokens,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ThinkingFormat {
    #[serde(rename = "openai")]
    Standard,
    Deepseek,
    Openrouter,
    Zai,
    Qwen,
    QwenChatTemplate,
    Together,
    StringThinking,
    #[serde(other)]
    Other,
}

impl ChatCompatibility {
    /// Model-specific values override the provider defaults field by field.
    pub fn overlay(self, model: Self) -> Self {
        Self {
            supports_store: model.supports_store.or(self.supports_store),
            supports_developer_role: model
                .supports_developer_role
                .or(self.supports_developer_role),
            supports_usage_in_streaming: model
                .supports_usage_in_streaming
                .or(self.supports_usage_in_streaming),
            supports_finish_reason: model.supports_finish_reason.or(self.supports_finish_reason),
            supports_reasoning_effort: model
                .supports_reasoning_effort
                .or(self.supports_reasoning_effort),
            requires_reasoning_content_on_assistant_messages: model
                .requires_reasoning_content_on_assistant_messages
                .or(self.requires_reasoning_content_on_assistant_messages),
            max_tokens_field: model.max_tokens_field.or(self.max_tokens_field),
            thinking_format: model.thinking_format.or(self.thinking_format),
            zai_tool_stream: model.zai_tool_stream.or(self.zai_tool_stream),
        }
    }

    pub(super) fn preserves_reasoning_details(&self) -> bool {
        matches!(self.thinking_format, Some(ThinkingFormat::Openrouter))
    }

    pub(super) fn request_options(&self, body: &mut Value, max_tokens: u32, has_tools: bool) {
        let tokens = match self.max_tokens_field {
            Some(MaxTokensField::MaxCompletionTokens) => "max_completion_tokens",
            _ => "max_tokens",
        };
        body[tokens] = json!(max_tokens);
        if self.supports_usage_in_streaming != Some(false) {
            body["stream_options"] = json!({"include_usage":true});
        }
        if self.supports_store == Some(true) {
            body["store"] = json!(false);
        }
        if has_tools && self.zai_tool_stream == Some(true) {
            body["tool_stream"] = json!(true);
        }
    }

    pub(super) fn reasoning_options(
        &self,
        body: &mut Value,
        supported: Option<bool>,
        enabled: bool,
        effort: Option<&str>,
        allow_off: bool,
    ) {
        if supported == Some(false) {
            return;
        }
        let support_effort = self.supports_reasoning_effort.unwrap_or(!matches!(
            self.thinking_format,
            Some(ThinkingFormat::Together | ThinkingFormat::Zai | ThinkingFormat::QwenChatTemplate)
        ));
        let effective = effort.unwrap_or(if enabled { "medium" } else { "none" });
        match self.thinking_format {
            Some(ThinkingFormat::Deepseek) => {
                if enabled || allow_off {
                    body["thinking"] = json!({"type":if enabled { "enabled" } else { "disabled" }});
                }
            }
            Some(ThinkingFormat::Openrouter) => {
                if enabled || allow_off {
                    body["reasoning"] = json!({"effort":effective});
                }
                return;
            }
            Some(ThinkingFormat::Zai) => {
                body["thinking"] = if enabled {
                    json!({"type":"enabled","clear_thinking":false})
                } else {
                    json!({"type":"disabled"})
                };
            }
            Some(ThinkingFormat::Qwen) => body["enable_thinking"] = json!(enabled),
            Some(ThinkingFormat::QwenChatTemplate) => {
                body["chat_template_kwargs"] =
                    json!({"enable_thinking":enabled,"preserve_thinking":true});
                return;
            }
            Some(ThinkingFormat::Together) => body["reasoning"] = json!({"enabled":enabled}),
            Some(ThinkingFormat::StringThinking) => {
                if enabled || allow_off {
                    body["thinking"] = json!(effective);
                }
                return;
            }
            _ => {
                if support_effort && let Some(effort) = effort {
                    body["reasoning_effort"] = json!(effort);
                }
                return;
            }
        }
        if support_effort
            && enabled
            && let Some(effort) = effort
        {
            body["reasoning_effort"] = json!(effort);
        }
    }
}
