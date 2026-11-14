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
    pub(super) supports_strict_mode: Option<bool>,
    pub(super) requires_reasoning_content_on_assistant_messages: Option<bool>,
    pub(super) requires_thinking_as_text: Option<bool>,
    pub(super) reasoning_field: Option<String>,
    max_tokens_field: Option<MaxTokensField>,
    thinking_format: Option<ThinkingFormat>,
    chat_template_args: Option<serde_json::Map<String, Value>>,
    zai_tool_stream: Option<bool>,
    vllm_priority: Option<serde_json::Number>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vllm_priority_is_optional_numeric_and_overlaid_per_model() {
        let default = ChatCompatibility::default();
        let mut body = json!({});
        default.request_options(&mut body, 16, false);
        assert!(body.get("priority").is_none());
        let provider: ChatCompatibility =
            serde_json::from_value(json!({"vllmPriority": 5})).unwrap();
        for (model, expected) in [
            (json!({}), json!(5)),
            (json!({"vllmPriority": 0}), json!(0)),
            (json!({"vllmPriority": -2.5}), json!(-2.5)),
        ] {
            let compat = provider
                .clone()
                .overlay(serde_json::from_value(model).unwrap());
            let mut body = json!({});
            compat.request_options(&mut body, 16, false);
            assert_eq!(body["priority"], expected);
        }
        assert!(
            serde_json::from_value::<ChatCompatibility>(json!({"vllmPriority":"high"})).is_err()
        );
    }

    #[test]
    fn ant_ling_sends_only_explicit_reasoning_effort() {
        let compat: ChatCompatibility =
            serde_json::from_value(json!({"thinkingFormat":"ant-ling"})).unwrap();
        let mut body = json!({});
        compat.reasoning_options(&mut body, Some(true), true, Some("high"), false);
        assert_eq!(body, json!({"reasoning":{"effort":"high"}}));
        for (supported, enabled, effort) in [
            (Some(true), true, None),
            (Some(true), false, Some("high")),
            (Some(false), true, Some("high")),
        ] {
            let mut body = json!({});
            compat.reasoning_options(&mut body, supported, enabled, effort, false);
            assert_eq!(body, json!({}));
        }
    }

    #[test]
    fn baseten_template_tracks_reasoning_and_mapped_effort() {
        let compat: ChatCompatibility = serde_json::from_value(json!({
            "thinkingFormat":"baseten",
            "supportsReasoningEffort":true,
            "chatTemplateArgs":{"enable_thinking":{"$var":"thinking.enabled"}}
        }))
        .unwrap();
        let mut enabled = json!({});
        compat.reasoning_options(&mut enabled, Some(true), true, Some("high"), true);
        assert_eq!(
            enabled,
            json!({"chat_template_args":{"enable_thinking":true},"reasoning_effort":"high"})
        );
        let mut disabled = json!({});
        compat.reasoning_options(&mut disabled, Some(true), false, Some("none"), true);
        assert_eq!(
            disabled,
            json!({"chat_template_args":{"enable_thinking":false},"reasoning_effort":"none"})
        );
    }
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
    AntLing,
    Deepseek,
    Openrouter,
    Zai,
    Qwen,
    QwenChatTemplate,
    Together,
    Baseten,
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
            supports_strict_mode: model.supports_strict_mode.or(self.supports_strict_mode),
            requires_reasoning_content_on_assistant_messages: model
                .requires_reasoning_content_on_assistant_messages
                .or(self.requires_reasoning_content_on_assistant_messages),
            requires_thinking_as_text: model
                .requires_thinking_as_text
                .or(self.requires_thinking_as_text),
            reasoning_field: model.reasoning_field.or(self.reasoning_field),
            max_tokens_field: model.max_tokens_field.or(self.max_tokens_field),
            thinking_format: model.thinking_format.or(self.thinking_format),
            chat_template_args: model.chat_template_args.or(self.chat_template_args),
            zai_tool_stream: model.zai_tool_stream.or(self.zai_tool_stream),
            vllm_priority: model.vllm_priority.or(self.vllm_priority),
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
        if let Some(priority) = &self.vllm_priority {
            body["priority"] = Value::Number(priority.clone());
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
            Some(ThinkingFormat::AntLing) => {
                if enabled && let Some(level) = effort {
                    body["reasoning"] = json!({"effort":level});
                }
                return;
            }
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
            Some(ThinkingFormat::Baseten) => {
                if let Some(template) = &self.chat_template_args {
                    let mut values = serde_json::Map::new();
                    for (key, value) in template {
                        let resolved = match value.as_object() {
                            Some(spec)
                                if !enabled && spec.get("omitWhenOff") == Some(&json!(true)) =>
                            {
                                None
                            }
                            Some(spec) if spec.get("$var") == Some(&json!("thinking.enabled")) => {
                                Some(json!(enabled))
                            }
                            Some(spec) if spec.get("$var") == Some(&json!("thinking.effort")) => {
                                effort.map(|level| json!(level))
                            }
                            Some(_) => None,
                            None => Some(value.clone()),
                        };
                        if let Some(value) = resolved {
                            values.insert(key.clone(), value);
                        }
                    }
                    if !values.is_empty() {
                        body["chat_template_args"] = Value::Object(values);
                    }
                }
                if support_effort && let Some(level) = effort {
                    body["reasoning_effort"] = json!(level);
                }
                return;
            }
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
