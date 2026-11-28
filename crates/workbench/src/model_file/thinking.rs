use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use pi_rs::{Error, Model, ModelCatalog};
use serde_json::{Value, json};

use super::bound_model::BoundModel;
use super::{DisplayMetadata, ModelFileRoute, NativeModelApi, NativeRouteClient};
use crate::model_thinking_levels::LEVELS;

/// A rejected selection from a granted model file snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ModelFileBindingError {
    UnknownModel,
    UnknownLevel,
    DisabledLevel,
    MissingMapping,
    UnsupportedMapping { api: NativeModelApi },
}

impl fmt::Display for ModelFileBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownModel => f.write_str("model is outside the granted file snapshot"),
            Self::UnknownLevel => f.write_str("unknown Pi thinking level"),
            Self::DisabledLevel => f.write_str("thinking level is disabled for this model"),
            Self::MissingMapping => f.write_str("thinking level requires an explicit map entry"),
            Self::UnsupportedMapping { api } => {
                write!(f, "{api:?} cannot represent this thinking selection")
            }
        }
    }
}

impl std::error::Error for ModelFileBindingError {}

impl From<ModelFileBindingError> for Error {
    fn from(error: ModelFileBindingError) -> Self {
        Self::Config(error.to_string())
    }
}

/// An immutable catalog and the route-authorized settings for its exact identities.
#[derive(Clone)]
pub struct ModelFileSnapshot {
    catalog: ModelCatalog,
    entries: BindingEntries,
}

impl ModelFileSnapshot {
    pub(super) fn new(catalog: ModelCatalog, entries: BindingEntries) -> Self {
        Self { catalog, entries }
    }

    /// The ordinary catalog, with the same default clients as the file loader.
    pub fn catalog(&self) -> &ModelCatalog {
        &self.catalog
    }

    /// Prepare a client for the exact provider, model and effective Pi level.
    /// The returned client rejects requests for any other model. Binding does not read
    /// files, resolve credentials, probe a model, or start a request.
    pub fn bind_thinking(
        &self,
        provider: &str,
        model: &str,
        level: &str,
    ) -> Result<Arc<dyn Model>, ModelFileBindingError> {
        let client = self
            .entries
            .get(provider)
            .and_then(|models| models.get(model))
            .ok_or(ModelFileBindingError::UnknownModel)?
            .bind(model, level)?;
        Ok(Arc::new(BoundModel::new(model, client)))
    }
}

pub(super) type BindingEntries = BTreeMap<String, BTreeMap<String, BindingEntry>>;

#[derive(Clone)]
pub(super) struct BindingEntry {
    route: ModelFileRoute,
    client: NativeRouteClient,
    name: Option<String>,
    reasoning: bool,
    map: Option<BTreeMap<String, Option<String>>>,
    compat: BTreeMap<&'static str, Value>,
}

impl BindingEntry {
    pub(super) fn new(
        route: &ModelFileRoute,
        client: NativeRouteClient,
        metadata: &DisplayMetadata,
        provider: &Value,
        declaration: &Value,
        model_override: Option<&Value>,
    ) -> Self {
        let compat = [
            "thinkingFormat",
            "supportsReasoningEffort",
            "forceAdaptiveThinking",
            "chatTemplateArgs",
            "chatTemplateKwargs",
        ]
        .into_iter()
        .filter_map(|key| {
            let mut layers = [Some(provider), Some(declaration), model_override]
                .into_iter()
                .flatten()
                .filter_map(|value| value.get("compat")?.get(key));
            let value = if matches!(key, "chatTemplateArgs" | "chatTemplateKwargs") {
                layers.cloned().reduce(|base, newer| match (base, newer) {
                    (Value::Object(mut base), Value::Object(newer)) => {
                        base.extend(newer);
                        Value::Object(base)
                    }
                    (_, newer) => newer,
                })
            } else {
                layers.next_back().cloned()
            };
            value.map(|value| (key, value))
        })
        .collect();
        Self {
            route: route.clone(),
            client,
            name: metadata.name.clone(),
            reasoning: metadata.reasoning.unwrap_or(false),
            map: metadata.thinking_level_map.clone(),
            compat,
        }
    }

    fn unsupported(&self) -> ModelFileBindingError {
        ModelFileBindingError::UnsupportedMapping {
            api: self.route.api,
        }
    }

    fn bind(&self, model: &str, level: &str) -> Result<Arc<dyn Model>, ModelFileBindingError> {
        use ModelFileBindingError::{DisabledLevel, MissingMapping, UnknownLevel};
        use NativeRouteClient as Client;

        if !LEVELS.contains(&level) {
            return Err(UnknownLevel);
        }
        let enabled = level != "off";
        if !self.reasoning {
            if enabled {
                return Err(DisabledLevel);
            }
            if self
                .map
                .as_ref()
                .and_then(|map| map.get("off"))
                .is_some_and(Option::is_some)
            {
                return Err(self.unsupported());
            }
            return Ok(match &self.client {
                Client::Responses(client) => {
                    Arc::new((**client).clone().with_exact_reasoning_effort(false, None))
                }
                Client::Gemini(client) => {
                    Arc::new((**client).clone().with_reasoning_support(Some(false)))
                }
                Client::Bedrock(client) => Arc::new((**client).clone().without_reasoning_fields()),
                _ => self.client.model(),
            });
        }
        let map = self.map.as_ref().ok_or(MissingMapping)?;
        let explicit = match map.get(level) {
            Some(Some(effort)) if !effort.trim().is_empty() => Some(effort.as_str()),
            Some(Some(_)) => return Err(self.unsupported()),
            Some(None) => return Err(DisabledLevel),
            None if matches!(level, "xhigh" | "max") => return Err(MissingMapping),
            None => None,
        };
        let effort = explicit.unwrap_or(level);
        Ok(match &self.client {
            Client::Chat(client) => {
                Arc::new(self.chat((**client).clone(), enabled, explicit, effort)?)
            }
            Client::Responses(client) => {
                let effort = if enabled {
                    Some(effort)
                } else if self.route.provider == "github-copilot" {
                    if explicit.is_some() {
                        return Err(self.unsupported());
                    }
                    None
                } else {
                    Some(explicit.unwrap_or("none"))
                };
                Arc::new(
                    (**client)
                        .clone()
                        .with_exact_reasoning_effort(enabled, effort),
                )
            }
            Client::Anthropic(client) => {
                let adaptive = self.compat_bool("forceAdaptiveThinking")?.unwrap_or(false);
                let effort = if !enabled {
                    if explicit.is_some() {
                        return Err(self.unsupported());
                    }
                    "off"
                } else if adaptive {
                    match explicit.unwrap_or(if level == "minimal" { "low" } else { level }) {
                        effort @ ("low" | "medium" | "high" | "xhigh" | "max") => effort,
                        _ => return Err(self.unsupported()),
                    }
                } else if explicit.is_none() {
                    level
                } else {
                    return Err(self.unsupported());
                };
                Arc::new(
                    (**client)
                        .clone()
                        .with_configured_thinking(effort, adaptive, true),
                )
            }
            Client::Gemini(client) => {
                let supported = pi_rs::google::model_thinking_levels(model);
                let effort = effort.to_ascii_lowercase();
                if (!enabled && explicit.is_some())
                    || (enabled && effort == "off")
                    || !supported.iter().any(|supported| supported == &effort)
                {
                    return Err(self.unsupported());
                }
                let thinking = effort.as_str();
                Arc::new(
                    (**client)
                        .clone()
                        .with_reasoning_support(Some(true))
                        .with_thinking(Some(thinking)),
                )
            }
            Client::Bedrock(client) => {
                let mut client = (**client)
                    .clone()
                    .with_model_reasoning(model, self.name.as_deref(), level, explicit)
                    .map_err(|_| self.unsupported())?;
                if let Some(effort) = map.get("off").and_then(Option::as_deref)
                    && !effort.trim().is_empty()
                {
                    client = client.with_reasoning_off_effort(effort);
                }
                Arc::new(client)
            }
            Client::PiMessages(client) => {
                if (!enabled && explicit.is_some())
                    || (enabled && explicit.is_some_and(|effort| effort != level))
                {
                    return Err(self.unsupported());
                }
                Arc::new((**client).clone().with_thinking(enabled.then_some(level)))
            }
        })
    }

    fn compat_bool(&self, key: &str) -> Result<Option<bool>, ModelFileBindingError> {
        self.compat
            .get(key)
            .map(|value| value.as_bool().ok_or_else(|| self.unsupported()))
            .transpose()
    }

    fn chat(
        &self,
        client: pi_rs::ollama::Ollama,
        enabled: bool,
        explicit: Option<&str>,
        effort: &str,
    ) -> Result<pi_rs::ollama::Ollama, ModelFileBindingError> {
        let provider = self.route.provider.as_str();
        let base = self.route.base_url.as_str();
        let deepseek = provider == "deepseek" || base.to_ascii_lowercase().contains("deepseek.com");
        let ant = provider == "ant-ling" || base.contains("api.ant-ling.com");
        let zai = matches!(provider, "zai" | "zai-coding-cn")
            || base.contains("api.z.ai")
            || base.contains("open.bigmodel.cn");
        let together = provider == "together"
            || base.contains("api.together.ai")
            || base.contains("api.together.xyz");
        let router = provider == "openrouter" || base.contains("openrouter.ai");
        let default_format = if deepseek || ant {
            "deepseek"
        } else if zai {
            "zai"
        } else if together {
            "together"
        } else if router {
            "openrouter"
        } else {
            "openai"
        };
        let format = self
            .compat
            .get("thinkingFormat")
            .map_or(Ok(default_format), |value| {
                value.as_str().ok_or_else(|| self.unsupported())
            })?;
        let supports_effort = self
            .compat_bool("supportsReasoningEffort")?
            .unwrap_or_else(|| {
                !(ant
                    || zai
                    || together
                    || matches!(
                        provider,
                        "xai" | "moonshotai" | "moonshotai-cn" | "cloudflare-ai-gateway" | "nvidia"
                    )
                    || [
                        "api.x.ai",
                        "api.moonshot.",
                        "gateway.ai.cloudflare.com",
                        "integrate.api.nvidia.com",
                    ]
                    .iter()
                    .any(|host| base.contains(host)))
            });
        let template_key = match format {
            "chat-template" => Some("chatTemplateKwargs"),
            "baseten" => Some("chatTemplateArgs"),
            _ => None,
        };
        let template = template_key.and_then(|key| self.compat.get(key));
        let template_takes_effort = template.and_then(Value::as_object).is_some_and(|entries| {
            entries.values().any(|entry| {
                entry.get("$var").and_then(Value::as_str) == Some("thinking.effort")
                    && (enabled || entry.get("omitWhenOff") != Some(&Value::Bool(true)))
            })
        });
        let mapped =
            match format {
                "openai" if supports_effort => {
                    if enabled {
                        Some(effort)
                    } else {
                        explicit
                    }
                }
                "openai" if !enabled && explicit.is_none() => None,
                "openrouter" | "string-thinking" => Some(if enabled {
                    effort
                } else {
                    explicit.unwrap_or("none")
                }),
                "deepseek" | "zai" | "qwen" | "together" if enabled && supports_effort => {
                    Some(effort)
                }
                "baseten" | "chat-template"
                    if template_takes_effort || format == "baseten" && supports_effort =>
                {
                    if enabled { Some(effort) } else { explicit }
                }
                "baseten" | "chat-template" if explicit.is_none() => None,
                "deepseek" | "zai" | "qwen" | "together" | "qwen-chat-template"
                    if explicit.is_none() =>
                {
                    None
                }
                _ => return Err(self.unsupported()),
            };
        let mut compatibility = json!({
            "thinkingFormat": format,
            "supportsReasoningEffort": supports_effort,
        });
        if let Some((key, template)) = template_key.zip(template) {
            compatibility[key] = template.clone();
        }
        let compatibility =
            serde_json::from_value(compatibility).map_err(|_| self.unsupported())?;
        Ok(client
            .with_chat_compatibility(compatibility, Some(true), enabled, true)
            .with_mapped_reasoning_effort(mapped))
    }
}
