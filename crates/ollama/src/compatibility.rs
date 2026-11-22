//! Wire differences for explicitly configured chat-completions providers.
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Value, json};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ChatCompatibility {
    supports_store: Option<bool>,
    supports_mid_convo_system_messages: Option<bool>,
    pub(super) supports_developer_role: Option<bool>,
    supports_usage_in_streaming: Option<bool>,
    pub(super) supports_finish_reason: Option<bool>,
    supports_reasoning_effort: Option<bool>,
    pub(super) supports_strict_mode: Option<bool>,
    #[serde(rename = "supportsOpenAIGrammarTools")]
    supports_openai_grammar_tools: Option<bool>,
    pub(super) requires_reasoning_content_on_assistant_messages: Option<bool>,
    pub(super) requires_thinking_as_text: Option<bool>,
    pub(super) reasoning_field: Option<String>,
    max_tokens_field: Option<MaxTokensField>,
    thinking_format: Option<ThinkingFormat>,
    chat_template_args: Option<serde_json::Map<String, Value>>,
    #[serde(deserialize_with = "deserialize_chat_template_kwargs")]
    chat_template_kwargs: Option<serde_json::Map<String, Value>>,
    open_router_routing: Option<serde_json::Map<String, Value>>,
    vercel_gateway_routing: Option<VercelGatewayRouting>,
    zai_tool_stream: Option<bool>,
    vllm_priority: Option<serde_json::Number>,
    supports_thinking_token_budget: Option<bool>,
    thinking_token_budget_field: Option<ThinkingBudgetField>,
}

fn deserialize_chat_template_kwargs<'de, D>(
    deserializer: D,
) -> Result<Option<serde_json::Map<String, Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = serde_json::Map::<String, Value>::deserialize(deserializer)?;
    for (key, value) in &values {
        let valid = match value {
            Value::Array(_) => false,
            Value::Object(spec) => {
                matches!(
                    spec.get("$var").and_then(Value::as_str),
                    Some("thinking.enabled" | "thinking.effort" | "thinking.budget")
                ) && spec.get("omitWhenOff").is_none_or(Value::is_boolean)
            }
            _ => true,
        };
        if !valid {
            return Err(de::Error::custom(format!(
                "chatTemplateKwargs.{key} must be a scalar, null, or a supported thinking variable"
            )));
        }
    }
    Ok(Some(values))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct VercelGatewayRouting {
    #[serde(skip_serializing_if = "Option::is_none")]
    only: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order: Option<Vec<String>>,
}

fn overlay_map(
    base: Option<serde_json::Map<String, Value>>,
    newer: Option<serde_json::Map<String, Value>>,
) -> Option<serde_json::Map<String, Value>> {
    match (base, newer) {
        (Some(mut base), Some(newer)) => {
            base.extend(newer);
            Some(base)
        }
        (base, newer) => newer.or(base),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_preferences_use_the_gateway_wire_fields_and_model_overrides() {
        let provider: ChatCompatibility = serde_json::from_value(json!({
            "openRouterRouting":{"only":["first"],"zdr":true,"data_collection":"deny"},
            "vercelGatewayRouting":{"only":["anthropic"],"order":["first"]}
        }))
        .unwrap();
        let model = serde_json::from_value(json!({
            "openRouterRouting":{"order":["second"]},
            "vercelGatewayRouting":{"order":["bedrock"],"ignored":true}
        }))
        .unwrap();
        let mut body = json!({});
        provider
            .clone()
            .overlay(model)
            .request_options(&mut body, 32, false);
        assert_eq!(
            body["provider"],
            json!({"only":["first"],"order":["second"],"zdr":true,"data_collection":"deny"})
        );
        assert_eq!(
            body["providerOptions"],
            json!({"gateway":{"order":["bedrock"],"only":["anthropic"]}})
        );

        let model = serde_json::from_value(json!({
            "openRouterRouting":{}, "vercelGatewayRouting":{}
        }))
        .unwrap();
        let mut body = json!({});
        provider
            .clone()
            .overlay(model)
            .request_options(&mut body, 32, false);
        assert_eq!(
            body["provider"],
            json!({"only":["first"],"zdr":true,"data_collection":"deny"})
        );
        assert_eq!(
            body["providerOptions"],
            json!({"gateway":{"order":["first"],"only":["anthropic"]}})
        );

        let model = serde_json::from_value(json!({"vercelGatewayRouting":{"only":[]}})).unwrap();
        let mut body = json!({});
        provider
            .overlay(model)
            .request_options(&mut body, 32, false);
        assert_eq!(
            body["providerOptions"],
            json!({"gateway":{"order":["first"],"only":[]}})
        );
    }

    #[test]
    fn chat_template_kwargs_validate_upstream_value_types() {
        for unsupported in [
            json!({"nested": true}),
            json!(["array"]),
            json!({"$var": "unknown"}),
            json!({"$var": "thinking.budget", "omitWhenOff": "true"}),
        ] {
            let error = serde_json::from_value::<ChatCompatibility>(
                json!({"chatTemplateKwargs": {"setting": unsupported}}),
            )
            .unwrap_err();
            assert!(error.to_string().contains("chatTemplateKwargs.setting"));
        }
    }

    #[test]
    fn overlay_merges_chat_template_arguments_by_key() {
        let base: ChatCompatibility = serde_json::from_value(json!({
            "chatTemplateArgs":{"enable_thinking":true,"shared":"base"},
            "chatTemplateKwargs":{"budget":{"$var":"thinking.budget"},"shared":"base"},
        }))
        .unwrap();
        let provider: ChatCompatibility = serde_json::from_value(json!({
            "chatTemplateArgs":{"provider":1,"shared":"provider"},
            "chatTemplateKwargs":{"shared":"provider"},
        }))
        .unwrap();
        let model: ChatCompatibility = serde_json::from_value(json!({
            "chatTemplateArgs":{"model":2,"shared":"model"},
            "chatTemplateKwargs":{"effort":{"$var":"thinking.effort"}},
        }))
        .unwrap();
        let compat = base.overlay(provider).overlay(model);
        assert_eq!(
            compat.chat_template_args,
            json!({"enable_thinking":true,"provider":1,"model":2,"shared":"model"})
                .as_object()
                .cloned()
        );
        assert_eq!(
            compat.chat_template_kwargs,
            json!({"budget":{"$var":"thinking.budget"},"shared":"provider","effort":{"$var":"thinking.effort"}})
                .as_object().cloned()
        );
    }

    #[test]
    fn configured_thinking_budget_uses_selected_level_and_answer_room() {
        let provider: ChatCompatibility =
            serde_json::from_value(json!({"supportsThinkingTokenBudget":true})).unwrap();
        let model: ChatCompatibility =
            serde_json::from_value(json!({"thinkingTokenBudgetField":"thinking_budget"})).unwrap();
        let compat = provider.overlay(model);
        for (level, max, expected) in [
            ("low", 4096, Some(1500)),
            ("xhigh", 2000, Some(976)),
            ("off", 4096, None),
            ("high", 1024, None),
        ] {
            let mut body = json!({});
            compat
                .thinking_budget_options(
                    &mut body,
                    Some(level),
                    Some(&json!({"low":1500})),
                    max,
                    Some(true),
                )
                .unwrap();
            assert_eq!(
                body.get("thinking_budget").and_then(Value::as_u64),
                expected
            );
            assert!(body.get("thinking_token_budget").is_none());
        }
        let mut body = json!({});
        compat
            .thinking_budget_options(&mut body, Some("low"), None, 4096, Some(false))
            .unwrap();
        assert_eq!(body, json!({}));
        let default: ChatCompatibility =
            serde_json::from_value(json!({"supportsThinkingTokenBudget":true})).unwrap();
        default
            .thinking_budget_options(&mut body, Some("low"), None, 4096, Some(true))
            .unwrap();
        assert_eq!(body["thinking_token_budget"], 2048);
    }

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
        compat.reasoning_options(&mut body, Some(true), true, Some("high"), false, None);
        assert_eq!(body, json!({"reasoning":{"effort":"high"}}));
        for (supported, enabled, effort) in [
            (Some(true), true, None),
            (Some(true), false, Some("high")),
            (Some(false), true, Some("high")),
        ] {
            let mut body = json!({});
            compat.reasoning_options(&mut body, supported, enabled, effort, false, None);
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
        compat.reasoning_options(&mut enabled, Some(true), true, Some("high"), true, None);
        assert_eq!(
            enabled,
            json!({"chat_template_args":{"enable_thinking":true},"reasoning_effort":"high"})
        );
        let mut disabled = json!({});
        compat.reasoning_options(&mut disabled, Some(true), false, Some("none"), true, None);
        assert_eq!(
            disabled,
            json!({"chat_template_args":{"enable_thinking":false},"reasoning_effort":"none"})
        );
    }

    #[test]
    fn template_budget_is_omitted_when_thinking_is_off_or_has_no_answer_room() {
        let compat: ChatCompatibility = serde_json::from_value(json!({
            "thinkingFormat": "chat-template",
            "chatTemplateKwargs": {
                "enabled":{"$var":"thinking.enabled"},
                "budget":{"$var":"thinking.budget"},
                "effort":{"$var":"thinking.effort", "omitWhenOff":true},
                "literal":null,
            }
        }))
        .unwrap();
        for (level, enabled, max, expected) in [
            ("off", false, 4096, json!({"enabled":false,"literal":null})),
            (
                "low",
                true,
                1024,
                json!({"enabled":true,"effort":"low","literal":null}),
            ),
            (
                "low",
                true,
                1700,
                json!({"enabled":true,"effort":"low","budget":676,"literal":null}),
            ),
        ] {
            for supported in [Some(true), None] {
                let mut body = json!({});
                let budget = compat
                    .thinking_budget_options(&mut body, Some(level), None, max, supported)
                    .unwrap();
                let effort = if enabled { Some(level) } else { None };
                compat.reasoning_options(&mut body, supported, enabled, effort, true, budget);
                assert_eq!(body, json!({"chat_template_kwargs":expected}));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ThinkingBudgetField {
    #[serde(rename = "thinking_token_budget")]
    TokenBudget,
    #[serde(rename = "thinking_budget")]
    Budget,
    #[serde(rename = "thinking_budget_tokens")]
    BudgetTokens,
}
impl ThinkingBudgetField {
    fn as_str(self) -> &'static str {
        match self {
            Self::TokenBudget => "thinking_token_budget",
            Self::Budget => "thinking_budget",
            Self::BudgetTokens => "thinking_budget_tokens",
        }
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
    ChatTemplate,
    Together,
    Baseten,
    StringThinking,
    #[serde(other)]
    Other,
}

impl ChatCompatibility {
    pub fn supports_mid_conversation_system_messages(&self) -> bool {
        self.supports_mid_convo_system_messages.unwrap_or(false)
    }

    /// Whether the model explicitly declares strict tool support.
    pub fn supports_strict_mode(&self) -> Option<bool> {
        self.supports_strict_mode
    }

    /// Whether the model accepts OpenAI custom tools constrained by a grammar.
    pub fn supports_openai_grammar_tools(&self) -> Option<bool> {
        self.supports_openai_grammar_tools
    }

    pub(super) fn set_openai_grammar_tool_support(&mut self, supported: bool) {
        self.supports_openai_grammar_tools = Some(supported);
    }

    /// Whether the configured model accepts the developer instruction role.
    pub fn supports_developer_role(&self) -> Option<bool> {
        self.supports_developer_role
    }

    /// Model-specific values override the provider defaults field by field.
    pub fn overlay(self, model: Self) -> Self {
        let vercel_gateway_routing =
            match (self.vercel_gateway_routing, model.vercel_gateway_routing) {
                (Some(base), Some(newer)) => Some(VercelGatewayRouting {
                    only: newer.only.or(base.only),
                    order: newer.order.or(base.order),
                }),
                (base, newer) => newer.or(base),
            };
        Self {
            supports_store: model.supports_store.or(self.supports_store),
            supports_mid_convo_system_messages: model
                .supports_mid_convo_system_messages
                .or(self.supports_mid_convo_system_messages),
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
            supports_openai_grammar_tools: model
                .supports_openai_grammar_tools
                .or(self.supports_openai_grammar_tools),
            requires_reasoning_content_on_assistant_messages: model
                .requires_reasoning_content_on_assistant_messages
                .or(self.requires_reasoning_content_on_assistant_messages),
            requires_thinking_as_text: model
                .requires_thinking_as_text
                .or(self.requires_thinking_as_text),
            reasoning_field: model.reasoning_field.or(self.reasoning_field),
            max_tokens_field: model.max_tokens_field.or(self.max_tokens_field),
            thinking_format: model.thinking_format.or(self.thinking_format),
            chat_template_args: overlay_map(self.chat_template_args, model.chat_template_args),
            chat_template_kwargs: overlay_map(
                self.chat_template_kwargs,
                model.chat_template_kwargs,
            ),
            open_router_routing: overlay_map(self.open_router_routing, model.open_router_routing),
            vercel_gateway_routing,
            zai_tool_stream: model.zai_tool_stream.or(self.zai_tool_stream),
            vllm_priority: model.vllm_priority.or(self.vllm_priority),
            supports_thinking_token_budget: model
                .supports_thinking_token_budget
                .or(self.supports_thinking_token_budget),
            thinking_token_budget_field: model
                .thinking_token_budget_field
                .or(self.thinking_token_budget_field),
        }
    }

    pub(super) fn preserves_reasoning_details(&self) -> bool {
        matches!(self.thinking_format, Some(ThinkingFormat::Openrouter))
    }

    pub(super) fn leading_system_message(
        &self,
        messages: &[pi_rs_core::Message],
    ) -> Option<pi_rs_core::Message> {
        if self.supports_mid_conversation_system_messages() {
            return None;
        }
        let mut contents = messages.iter().filter_map(|message| match message {
            pi_rs_core::Message::System { content } => Some(content.as_str()),
            _ => None,
        });
        let first = contents.next()?;
        let content = std::iter::once(first)
            .chain(contents)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        Some(pi_rs_core::Message::System { content })
    }

    pub(super) fn request_options(&self, body: &mut Value, max_tokens: u32, has_tools: bool) {
        let tokens = match self.max_tokens_field {
            Some(MaxTokensField::MaxCompletionTokens) => "max_completion_tokens",
            _ => "max_tokens",
        };
        body[tokens] = json!(max_tokens);
        if let Some(routing) = &self.open_router_routing {
            body["provider"] = json!(routing);
        }
        if let Some(routing) = &self.vercel_gateway_routing
            && (routing.only.is_some() || routing.order.is_some())
        {
            body["providerOptions"] = json!({"gateway":routing});
        }
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

    pub(super) fn thinking_budget_options(
        &self,
        body: &mut Value,
        level: Option<&str>,
        budgets: Option<&Value>,
        max_tokens: u32,
        supported: Option<bool>,
    ) -> pi_rs_core::Result<Option<u32>> {
        if supported == Some(false) {
            return Ok(None);
        }
        let field = if supported == Some(true) {
            self.thinking_token_budget_field
                .map(ThinkingBudgetField::as_str)
                .or_else(|| {
                    (self.supports_thinking_token_budget == Some(true))
                        .then_some("thinking_token_budget")
                })
        } else {
            None
        };
        let template_wants_budget = self.thinking_template().is_some_and(|(_, template)| {
            template
                .values()
                .any(|value| value.get("$var").and_then(Value::as_str) == Some("thinking.budget"))
        });
        if field.is_none() && !template_wants_budget {
            return Ok(None);
        }
        let (key, default) = match level {
            Some("minimal") => ("minimal", 1024),
            Some("low") => ("low", 2048),
            Some("medium") => ("medium", 8192),
            Some("high" | "xhigh" | "max") => ("high", 16384),
            _ => return Ok(None),
        };
        let budget = match budgets {
            None => default,
            Some(value) => match value
                .as_object()
                .ok_or_else(|| {
                    pi_rs_core::Error::Config("thinkingBudgets must be an object".into())
                })?
                .get(key)
            {
                None => default,
                Some(value) => value
                    .as_u64()
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| {
                        pi_rs_core::Error::Config(format!(
                            "thinkingBudgets.{key} must be a nonnegative integer"
                        ))
                    })?,
            },
        }
        .min(max_tokens.saturating_sub(1024));
        if budget == 0 {
            return Ok(None);
        }
        if let Some(field) = field {
            body[field] = json!(budget);
        }
        Ok(Some(budget))
    }

    pub(super) fn uses_chat_template(&self) -> bool {
        matches!(self.thinking_format, Some(ThinkingFormat::ChatTemplate))
    }

    fn thinking_template(&self) -> Option<(&'static str, &serde_json::Map<String, Value>)> {
        match self.thinking_format {
            Some(ThinkingFormat::Baseten) => self
                .chat_template_args
                .as_ref()
                .map(|template| ("chat_template_args", template)),
            Some(ThinkingFormat::ChatTemplate) => self
                .chat_template_kwargs
                .as_ref()
                .map(|template| ("chat_template_kwargs", template)),
            _ => None,
        }
    }

    pub(super) fn reasoning_options(
        &self,
        body: &mut Value,
        supported: Option<bool>,
        enabled: bool,
        effort: Option<&str>,
        allow_off: bool,
        thinking_budget: Option<u32>,
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
            Some(ThinkingFormat::Baseten | ThinkingFormat::ChatTemplate) => {
                if let Some((field, template)) = self.thinking_template() {
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
                            Some(spec) if spec.get("$var") == Some(&json!("thinking.budget")) => {
                                thinking_budget
                                    .filter(|_| enabled)
                                    .map(|budget| json!(budget))
                            }
                            Some(_) => None,
                            None => Some(value.clone()),
                        };
                        if let Some(value) = resolved {
                            values.insert(key.clone(), value);
                        }
                    }
                    if !values.is_empty() {
                        body[field] = Value::Object(values);
                    }
                }
                if matches!(self.thinking_format, Some(ThinkingFormat::Baseten))
                    && support_effort
                    && let Some(level) = effort
                {
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
