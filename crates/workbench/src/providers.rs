//! Provider selection and shared workbench catalog and progress controls.
use std::collections::BTreeMap;

use pi_rs::anthropic::Anthropic;
use pi_rs::google::Google;
use pi_rs::ollama::{ModelDetails, Ollama};
use pi_rs::{Error, Model, ModelRequest, ModelStream, Result};
use serde_json::json;
use std::sync::Arc;

use super::pi_messages::PiMessages;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum Provider {
    #[default]
    Ollama,
    Anthropic,
    Google,
    Radius,
    Custom {
        id: Arc<str>,
        adapter: Adapter,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Adapter {
    ChatCompletions,
    Anthropic,
    Google,
    PiMessages,
}

impl Adapter {
    pub fn named(api: &str) -> Option<Self> {
        match api {
            "openai-completions" => Some(Self::ChatCompletions),
            "anthropic-messages" => Some(Self::Anthropic),
            "google-generative-ai" => Some(Self::Google),
            "pi-messages" => Some(Self::PiMessages),
            _ => None,
        }
    }

    fn api(self) -> &'static str {
        match self {
            Self::ChatCompletions => "openai-completions",
            Self::Anthropic => "anthropic-messages",
            Self::Google => "google-generative-ai",
            Self::PiMessages => "pi-messages",
        }
    }
}

impl Provider {
    pub const ALL: [Self; 4] = [Self::Ollama, Self::Anthropic, Self::Google, Self::Radius];

    pub fn adapter(&self) -> Adapter {
        match self {
            Self::Ollama => Adapter::ChatCompletions,
            Self::Anthropic => Adapter::Anthropic,
            Self::Google => Adapter::Google,
            Self::Radius => Adapter::PiMessages,
            Self::Custom { adapter, .. } => *adapter,
        }
    }

    pub fn api(&self) -> &'static str {
        self.adapter().api()
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Ollama => "ollama",
            Self::Anthropic => "anthropic",
            Self::Google => "google",
            Self::Radius => "radius",
            Self::Custom { id, .. } => id,
        }
    }

    pub fn named(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| name.eq_ignore_ascii_case(provider.id()))
    }

    pub fn default_endpoint(&self) -> &'static str {
        match self {
            Self::Ollama => "http://127.0.0.1:11434/v1/chat/completions",
            Self::Anthropic => pi_rs::anthropic::DEFAULT_ENDPOINT,
            Self::Google => pi_rs::google::DEFAULT_ENDPOINT,
            Self::Radius => "https://radius.pi.dev/messages",
            Self::Custom { .. } => "",
        }
    }

    pub fn connect(&self, endpoint: &str, token: Option<String>) -> Result<Client> {
        let missing_key = || {
            Error::Config(match self {
                Self::Anthropic => "set ANTHROPIC_API_KEY or pass --api-key".into(),
                Self::Google => "set GEMINI_API_KEY or pass --api-key".into(),
                Self::Radius => {
                    "sign in with /login radius, set RADIUS_API_KEY, or pass --api-key".into()
                }
                _ => format!("configure an API key for {}", self.id()),
            })
        };
        match self.adapter() {
            Adapter::ChatCompletions => Ok(Client::Ollama(Ollama::new(endpoint, token)?)),
            Adapter::Anthropic => {
                let token = token.ok_or_else(missing_key)?;
                Ok(Client::Anthropic(Anthropic::new(endpoint, &token)?))
            }
            Adapter::Google => {
                let token = token.ok_or_else(missing_key)?;
                Ok(Client::Google(Google::new(endpoint, &token)?))
            }
            Adapter::PiMessages => {
                if self == &Self::Radius {
                    let gateway = super::radius_auth::Gateway::configured()?;
                    if gateway.url("/messages").as_str() == endpoint {
                        if token.is_none() && !gateway.has_credential()? {
                            return Err(missing_key());
                        }
                        return Ok(Client::PiMessages(PiMessages::radius(
                            endpoint, token, gateway,
                        )?));
                    }
                }
                Ok(Client::PiMessages(PiMessages::new(
                    self.id(),
                    endpoint,
                    token.ok_or_else(missing_key)?,
                )?))
            }
        }
    }
}

#[derive(Clone)]
pub(super) enum Client {
    Ollama(Ollama),
    Anthropic(Anthropic),
    Google(Google),
    PiMessages(PiMessages),
}

impl Client {
    pub fn aliases(&self) -> Vec<(String, String)> {
        match self {
            Self::PiMessages(client) => client.aliases(),
            _ => Vec::new(),
        }
    }

    pub fn with_headers(self, headers: reqwest::header::HeaderMap) -> Self {
        match self {
            Self::Ollama(client) => Self::Ollama(client.with_headers(headers)),
            Self::Anthropic(client) => Self::Anthropic(client.with_headers(headers)),
            Self::Google(client) => Self::Google(client.with_headers(headers)),
            Self::PiMessages(client) => Self::PiMessages(client.with_headers(headers)),
        }
    }

    pub async fn models(&self) -> Result<Vec<String>> {
        match self {
            Self::Ollama(client) => client.models().await,
            Self::Anthropic(client) => Ok(client
                .models()
                .await?
                .into_iter()
                .map(|model| model.id)
                .collect()),
            Self::Google(client) => Ok(client
                .models()
                .await?
                .into_iter()
                .map(|model| model.id)
                .collect()),
            Self::PiMessages(client) => client.models().await,
        }
    }

    pub async fn model_details(&self, models: &[String]) -> BTreeMap<String, ModelDetails> {
        match self {
            Self::Ollama(client) => client.model_details(models).await,
            Self::Anthropic(client) => {
                if models
                    .iter()
                    .any(|model| client.cached_model(model).is_none())
                {
                    let _ = client.models().await;
                }
                models
                    .iter()
                    .filter_map(|model| {
                        let entry = client.cached_model(model)?;
                        Some((
                            model.clone(),
                            ModelDetails {
                                context_window: entry.context_window,
                                max_output_tokens: entry.max_output_tokens,
                                reasoning: entry.reasoning,
                                images: entry.images,
                                thinking_levels: Some(entry.thinking_levels),
                            },
                        ))
                    })
                    .collect()
            }
            Self::Google(client) => {
                if models
                    .iter()
                    .any(|model| client.cached_model(model).is_none())
                {
                    let _ = client.models().await;
                }
                models
                    .iter()
                    .filter_map(|model| {
                        let entry = client.cached_model(model)?;
                        Some((
                            model.clone(),
                            ModelDetails {
                                context_window: entry.context_window,
                                max_output_tokens: entry.max_output_tokens,
                                reasoning: Some(entry.reasoning),
                                images: None,
                                thinking_levels: Some(entry.thinking_levels),
                            },
                        ))
                    })
                    .collect()
            }
            Self::PiMessages(client) => client.model_details(models).await,
        }
    }

    pub fn with_context_probes(self, enabled: bool) -> Self {
        match self {
            Self::Ollama(client) => Self::Ollama(client.with_native_probes(enabled)),
            Self::Anthropic(client) => Self::Anthropic(client.with_context_probe(enabled)),
            Self::Google(client) => Self::Google(client.with_context_probe(enabled)),
            Self::PiMessages(client) => Self::PiMessages(client.with_context_probe(enabled)),
        }
    }

    pub fn with_model_options(self, args: &super::Cli, reported: bool) -> Result<Self> {
        let thinking = args.provider_thinking();
        Ok(match self {
            Self::Ollama(client) => {
                let configured = args.model_profiles.model(&args.provider, &args.model);
                let mapped = configured
                    .as_ref()
                    .and_then(|model| model.mapped_effort(thinking?));
                let client = if let Some(mapped) = mapped {
                    client.with_mapped_reasoning_effort(mapped)
                } else if reported {
                    client.with_reported_reasoning_effort(thinking)
                } else {
                    client.with_reasoning_effort(thinking)
                };
                let sampling = configured
                    .map(|model| model.sampling(thinking.unwrap_or("off")))
                    .unwrap_or_default();
                Self::Ollama(client.with_sampling(sampling)?)
            }
            Self::Anthropic(client) => Self::Anthropic(client.with_thinking(thinking)),
            Self::Google(client) => Self::Google(client.with_thinking(thinking)),
            Self::PiMessages(client) => Self::PiMessages(client.with_thinking(thinking)),
        })
    }

    pub async fn thinking_levels(&self, model: &str) -> Option<Vec<String>> {
        match self {
            Self::Ollama(client) => {
                client
                    .model_details(&[model.into()])
                    .await
                    .remove(model)?
                    .thinking_levels
            }
            Self::Anthropic(client) => client.model(model).await.map(|entry| entry.thinking_levels),
            Self::Google(client) => client.model(model).await.map(|entry| entry.thinking_levels),
            Self::PiMessages(client) => {
                client
                    .model_details(&[model.into()])
                    .await
                    .remove(model)?
                    .thinking_levels
            }
        }
    }

    pub fn observe(self, trace: bool) -> Self {
        fn progress(stage: impl serde::Serialize) {
            super::output(json!({"type":"model_progress","stage":stage}));
        }
        let reasoning = move |delta: &str| {
            if trace {
                super::output(json!({"type":"reasoning_delta","delta":delta}));
            }
        };
        match self {
            Self::Ollama(client) => {
                Self::Ollama(client.with_progress(progress).with_reasoning(reasoning))
            }
            Self::Anthropic(client) => {
                Self::Anthropic(client.with_progress(progress).with_reasoning(reasoning))
            }
            Self::Google(client) => {
                Self::Google(client.with_progress(progress).with_reasoning(reasoning))
            }
            Self::PiMessages(client) => Self::PiMessages(client.observe(progress, reasoning)),
        }
    }
}

#[async_trait::async_trait]
impl Model for Client {
    async fn context_window(&self, model: &str) -> Option<u32> {
        match self {
            Self::Ollama(client) => client.context_window(model).await,
            Self::Anthropic(client) => client.context_window(model).await,
            Self::Google(client) => client.context_window(model).await,
            Self::PiMessages(client) => client.context_window(model).await,
        }
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        match self {
            Self::Ollama(client) => client.stream(request).await,
            Self::Anthropic(client) => client.stream(request).await,
            Self::Google(client) => client.stream(request).await,
            Self::PiMessages(client) => client.stream(request).await,
        }
    }
}
