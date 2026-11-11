//! Provider selection and shared workbench catalog and progress controls.
use std::collections::BTreeMap;

use pi_rs::anthropic::Anthropic;
use pi_rs::google::Google;
use pi_rs::ollama::{ModelDetails, Ollama};
use pi_rs::{CacheRetention, Error, Model, ModelRequest, ModelStream, Result};
use reqwest::header::HeaderMap;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use super::{Cli, model_auth::Credential, model_profiles::RequestHeaders, pi_messages::PiMessages};

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

    pub fn api(self) -> &'static str {
        match self {
            Self::ChatCompletions => "openai-completions",
            Self::Anthropic => "anthropic-messages",
            Self::Google => "google-generative-ai",
            Self::PiMessages => "pi-messages",
        }
    }
}

pub(super) fn cache_retention(supports_long: bool) -> CacheRetention {
    if supports_long && std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long") {
        CacheRetention::Long
    } else {
        CacheRetention::Short
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
            .or_else(|| super::builtin_chat::find(name).map(|builtin| builtin.provider()))
    }

    pub fn default_endpoint(&self) -> &'static str {
        match self {
            Self::Ollama => "http://127.0.0.1:11434/v1/chat/completions",
            Self::Anthropic => pi_rs::anthropic::DEFAULT_ENDPOINT,
            Self::Google => pi_rs::google::DEFAULT_ENDPOINT,
            Self::Radius => super::radius_auth::DEFAULT_MODEL_ENDPOINT,
            Self::Custom { id, .. } => {
                super::builtin_chat::find(id).map_or("", |builtin| builtin.endpoint())
            }
        }
    }

    fn replay_scope(&self, adapter: Adapter, endpoint: &str) -> String {
        format!(
            "{}:{}:{:x}",
            adapter.api(),
            self.id(),
            Sha256::digest(endpoint)
        )
    }

    pub fn connect(
        &self,
        adapter: Adapter,
        endpoint: &str,
        token: Option<String>,
        bearer: bool,
    ) -> Result<Client> {
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
        match adapter {
            Adapter::ChatCompletions => {
                if token.is_none()
                    && let Some(builtin) = super::builtin_chat::find(self.id())
                    && builtin.endpoint() == endpoint
                {
                    return Err(Error::Config(format!(
                        "set {} or pass --api-key for {}",
                        builtin.env(),
                        self.id()
                    )));
                }
                Ok(Client::Ollama(Ollama::new(endpoint, token)?))
            }
            Adapter::Anthropic => {
                let token = token.ok_or_else(missing_key)?;
                let mut client = if bearer || matches!(self.id(), "fireworks" | "openrouter") {
                    Anthropic::with_bearer_token(endpoint, &token)?
                } else {
                    Anthropic::new(endpoint, &token)?
                };
                if self != &Self::Anthropic {
                    client = client.with_provider(self.id(), self.replay_scope(adapter, endpoint));
                }
                Ok(Client::Anthropic(client))
            }
            Adapter::Google => {
                let token = token.ok_or_else(missing_key)?;
                let mut client = Google::new(endpoint, &token)?;
                if self != &Self::Google {
                    client = client.with_provider(self.id(), self.replay_scope(adapter, endpoint));
                }
                Ok(Client::Google(client))
            }
            Adapter::PiMessages => {
                if self == &Self::Radius {
                    let gateway = super::radius_auth::Gateway::configured()?;
                    if gateway.model_endpoint().as_str() == endpoint {
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
    Deferred(Box<Deferred>),
}

#[derive(Clone)]
pub(super) struct Deferred {
    provider: Provider,
    adapter: Adapter,
    endpoint: String,
    credential: Option<Credential>,
    headers: RequestHeaders,
    http_settings: Option<super::config::Settings>,
    options: Option<(Box<Cli>, bool)>,
    probe_context: bool,
    observed: bool,
}

impl Deferred {
    async fn resolve(&self) -> Result<Client> {
        let token = match &self.credential {
            Some(credential) => Some(credential.resolve().await?),
            None => None,
        };
        let headers = self
            .headers
            .resolve(&self.provider, token.as_deref())
            .await?;
        let bearer = self.credential.as_ref().is_some_and(Credential::is_bearer);
        let mut client = self
            .provider
            .connect(self.adapter, &self.endpoint, token, bearer)?
            .with_headers(headers)
            .with_context_probes(self.probe_context);
        if let Some(settings) = &self.http_settings {
            client = client.with_http_settings(settings)?;
        }
        if let Some((args, reported)) = &self.options {
            client = client.with_model_options(args, *reported)?;
        }
        Ok(if self.observed {
            client.observe()
        } else {
            client
        })
    }
}

impl Client {
    pub fn deferred(
        provider: Provider,
        adapter: Adapter,
        endpoint: &str,
        credential: Option<Credential>,
        headers: RequestHeaders,
    ) -> Self {
        Self::Deferred(Box::new(Deferred {
            provider,
            adapter,
            endpoint: endpoint.into(),
            credential,
            headers,
            http_settings: None,
            options: None,
            probe_context: true,
            observed: false,
        }))
    }

    pub fn with_http_settings(self, settings: &super::config::Settings) -> Result<Self> {
        let proxies = super::http_proxy::configured(settings)?;
        let timeout = settings.http_idle_timeout();
        Ok(match self {
            Self::Ollama(client) => Self::Ollama(client.with_http_options(timeout, &proxies)?),
            Self::Anthropic(client) => {
                Self::Anthropic(client.with_http_options(timeout, &proxies)?)
            }
            Self::Google(client) => Self::Google(client.with_http_options(timeout, &proxies)?),
            Self::PiMessages(client) => {
                Self::PiMessages(client.with_http_options(timeout, &proxies)?)
            }
            Self::Deferred(mut client) => {
                client.http_settings = Some(settings.clone());
                Self::Deferred(client)
            }
        })
    }

    pub async fn stream_with_headers(
        &self,
        request: ModelRequest,
        defaults: &HeaderMap,
        cache_disabled: bool,
    ) -> Result<ModelStream> {
        match self {
            Self::Ollama(client) if cache_disabled => {
                client
                    .clone()
                    .with_cache_retention(CacheRetention::None)
                    .stream(request)
                    .await
            }
            Self::Ollama(client) if !defaults.is_empty() => {
                client
                    .clone()
                    .with_default_headers(defaults)
                    .stream(request)
                    .await
            }
            Self::Anthropic(client) if cache_disabled => {
                client.clone().without_cache().stream(request).await
            }
            Self::PiMessages(client) if cache_disabled => {
                client.clone().without_cache().stream(request).await
            }
            Self::Anthropic(client) if !defaults.is_empty() => {
                client
                    .clone()
                    .with_default_headers(defaults)
                    .stream(request)
                    .await
            }
            Self::Deferred(client) => {
                if client.observed {
                    super::output(json!({"type":"model_progress","stage":"resolving_credentials"}));
                }
                Box::pin(client.resolve().await?.stream_with_headers(
                    request,
                    defaults,
                    cache_disabled,
                ))
                .await
            }
            _ => self.stream(request).await,
        }
    }

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
            Self::Deferred(_) => unreachable!("deferred provider headers resolve per request"),
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
            Self::Deferred(client) => Box::pin(client.resolve().await?.models()).await,
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
            Self::Deferred(client) => match client.resolve().await {
                Ok(client) => Box::pin(client.model_details(models)).await,
                Err(_) => BTreeMap::new(),
            },
        }
    }

    pub fn with_context_probes(self, enabled: bool) -> Self {
        match self {
            Self::Ollama(client) => Self::Ollama(client.with_native_probes(enabled)),
            Self::Anthropic(client) => Self::Anthropic(client.with_context_probe(enabled)),
            Self::Google(client) => Self::Google(client.with_context_probe(enabled)),
            Self::PiMessages(client) => Self::PiMessages(client.with_context_probe(enabled)),
            Self::Deferred(mut client) => {
                client.probe_context = enabled;
                Self::Deferred(client)
            }
        }
    }

    pub fn with_model_options(self, args: &super::Cli, reported: bool) -> Result<Self> {
        let thinking = args.provider_thinking();
        Ok(match self {
            Self::Ollama(client) => {
                let configured = args.model_profiles.model(&args.provider, &args.model);
                let retention = args
                    .model_profiles
                    .chat_cache_support(&args.provider, &args.model, configured.as_ref())
                    .map(cache_retention)
                    .unwrap_or_default();
                let client = client.with_cache_retention(retention);
                let mapped = configured.as_ref().and_then(|model| {
                    let level = thinking.or((model.reasoning == Some(true)).then_some("off"))?;
                    model.mapped_effort(level)
                });
                let client = if let Some(mapped) = mapped {
                    client.with_mapped_reasoning_effort(mapped)
                } else if reported {
                    client.with_reported_reasoning_effort(thinking)
                } else {
                    client.with_reasoning_effort(thinking)
                };
                let compatibility = args
                    .model_profiles
                    .chat_compatibility(&args.provider, configured.as_ref());
                let client = client.with_chat_compatibility(
                    compatibility,
                    configured.as_ref().and_then(|model| model.reasoning),
                    thinking.is_some_and(|level| level != "off"),
                    configured
                        .as_ref()
                        .is_none_or(|model| model.mapped_effort("off") != Some(None)),
                );
                let sampling = configured
                    .map(|model| model.sampling(thinking.unwrap_or("off")))
                    .unwrap_or_default();
                Self::Ollama(client.with_sampling(sampling)?)
            }
            Self::Anthropic(client) => {
                let metadata = args.model_profiles.model(&args.provider, &args.model);
                let (supports_long, cache_tools) = args
                    .model_profiles
                    .messages_cache_support(&args.provider, metadata.as_ref());
                let retention = cache_retention(supports_long);
                let gateway = matches!(args.provider.id(), "fireworks" | "openrouter");
                let mut client = client
                    .with_thinking(thinking)
                    .with_cache_retention(retention, cache_tools);
                if let Some(metadata) = metadata {
                    client =
                        client.with_empty_thinking_signature(metadata.allows_empty_signature());
                    let adaptive = metadata.adaptive_thinking();
                    if metadata.reasoning == Some(true) && (gateway || adaptive.is_some()) {
                        let selected = thinking.unwrap_or("off");
                        let mapped = metadata.mapped_effort(selected);
                        if selected == "off" && mapped == Some(None) {
                            client = client.with_thinking(None);
                        } else {
                            let effort = mapped
                                .flatten()
                                .filter(|_| selected != "off")
                                .unwrap_or(selected);
                            client = client.with_configured_thinking(
                                effort,
                                adaptive.unwrap_or(false),
                                true,
                            );
                        }
                    }
                }
                Self::Anthropic(client)
            }
            Self::Google(client) => Self::Google(client.with_thinking(thinking)),
            Self::PiMessages(client) => Self::PiMessages(client.with_thinking(thinking)),
            Self::Deferred(mut client) => {
                client.options = Some((Box::new(args.clone()), reported));
                Self::Deferred(client)
            }
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
            Self::Deferred(client) => {
                Box::pin(client.resolve().await.ok()?.thinking_levels(model)).await
            }
        }
    }

    pub fn observe(self) -> Self {
        fn progress(stage: impl serde::Serialize) {
            super::output(json!({"type":"model_progress","stage":stage}));
        }
        match self {
            Self::Ollama(client) => Self::Ollama(client.with_progress(progress)),
            Self::Anthropic(client) => Self::Anthropic(client.with_progress(progress)),
            Self::Google(client) => Self::Google(client.with_progress(progress)),
            Self::PiMessages(client) => Self::PiMessages(client.observe(progress)),
            Self::Deferred(mut client) => {
                client.observed = true;
                Self::Deferred(client)
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
            Self::PiMessages(client) => client.context_window(model).await,
            Self::Deferred(client) => client.resolve().await.ok()?.context_window(model).await,
        }
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        match self {
            Self::Ollama(client) => client.stream(request).await,
            Self::Anthropic(client) => client.stream(request).await,
            Self::Google(client) => client.stream(request).await,
            Self::PiMessages(client) => client.stream(request).await,
            Self::Deferred(_) => {
                self.stream_with_headers(request, &HeaderMap::new(), false)
                    .await
            }
        }
    }
}
