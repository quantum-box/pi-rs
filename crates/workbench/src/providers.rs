//! Provider selection and shared workbench catalog and progress controls.
use std::collections::BTreeMap;

use clap::ValueEnum;
use pi_rs::anthropic::Anthropic;
use pi_rs::google::Google;
use pi_rs::ollama::{ModelDetails, Ollama};
use pi_rs::{Error, Model, ModelRequest, ModelStream, Result};
use serde_json::json;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(super) enum Provider {
    Ollama,
    Anthropic,
    Google,
}

impl Provider {
    pub const ALL: [Self; 3] = [Self::Ollama, Self::Anthropic, Self::Google];

    pub fn api(self) -> &'static str {
        match self {
            Self::Ollama => "openai-completions",
            Self::Anthropic => "anthropic-messages",
            Self::Google => "google-generative-ai",
        }
    }

    pub fn id(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
            Self::Anthropic => "anthropic",
            Self::Google => "google",
        }
    }

    pub fn named(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| name.eq_ignore_ascii_case(provider.id()))
    }

    pub fn default_endpoint(self) -> &'static str {
        match self {
            Self::Ollama => "http://127.0.0.1:11434/v1/chat/completions",
            Self::Anthropic => pi_rs::anthropic::DEFAULT_ENDPOINT,
            Self::Google => pi_rs::google::DEFAULT_ENDPOINT,
        }
    }

    pub fn connect(self, endpoint: &str, token: Option<String>) -> Result<Client> {
        match self {
            Self::Ollama => Ok(Client::Ollama(Ollama::new(endpoint, token)?)),
            Self::Anthropic => {
                let token = token.ok_or_else(|| {
                    Error::Config("set ANTHROPIC_API_KEY or pass --api-key".into())
                })?;
                Ok(Client::Anthropic(Anthropic::new(endpoint, &token)?))
            }
            Self::Google => {
                let token = token
                    .ok_or_else(|| Error::Config("set GEMINI_API_KEY or pass --api-key".into()))?;
                Ok(Client::Google(Google::new(endpoint, &token)?))
            }
        }
    }
}

#[derive(Clone)]
pub(super) enum Client {
    Ollama(Ollama),
    Anthropic(Anthropic),
    Google(Google),
}

impl Client {
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
        }
    }

    pub fn with_context_probes(self, enabled: bool) -> Self {
        match self {
            Self::Ollama(client) => Self::Ollama(client.with_native_probes(enabled)),
            Self::Anthropic(client) => Self::Anthropic(client.with_context_probe(enabled)),
            Self::Google(client) => Self::Google(client.with_context_probe(enabled)),
        }
    }

    pub fn with_thinking(self, thinking: Option<&str>, reported: bool) -> Self {
        match self {
            Self::Ollama(client) if reported => {
                Self::Ollama(client.with_reported_reasoning_effort(thinking))
            }
            Self::Ollama(client) => Self::Ollama(client.with_reasoning_effort(thinking)),
            Self::Anthropic(client) => Self::Anthropic(client.with_thinking(thinking)),
            Self::Google(client) => Self::Google(client.with_thinking(thinking)),
        }
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
        }
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        match self {
            Self::Ollama(client) => client.stream(request).await,
            Self::Anthropic(client) => client.stream(request).await,
            Self::Google(client) => client.stream(request).await,
        }
    }
}
