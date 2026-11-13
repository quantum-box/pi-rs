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
    Bedrock,
    Radius,
    Custom {
        id: Arc<str>,
        adapter: Adapter,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Adapter {
    ChatCompletions,
    OpenAiResponses,
    Anthropic,
    Google,
    Bedrock,
    PiMessages,
}

impl Adapter {
    pub fn named(api: &str) -> Option<Self> {
        match api {
            "openai-completions" => Some(Self::ChatCompletions),
            "openai-responses" => Some(Self::OpenAiResponses),
            "anthropic-messages" => Some(Self::Anthropic),
            "google-generative-ai" => Some(Self::Google),
            "bedrock-converse-stream" => Some(Self::Bedrock),
            "pi-messages" => Some(Self::PiMessages),
            _ => None,
        }
    }

    pub fn api(self) -> &'static str {
        match self {
            Self::ChatCompletions => "openai-completions",
            Self::OpenAiResponses => "openai-responses",
            Self::Anthropic => "anthropic-messages",
            Self::Google => "google-generative-ai",
            Self::Bedrock => "bedrock-converse-stream",
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
    pub const ALL: [Self; 5] = [
        Self::Ollama,
        Self::Anthropic,
        Self::Google,
        Self::Bedrock,
        Self::Radius,
    ];

    pub fn adapter(&self) -> Adapter {
        match self {
            Self::Ollama => Adapter::ChatCompletions,
            Self::Anthropic => Adapter::Anthropic,
            Self::Google => Adapter::Google,
            Self::Bedrock => Adapter::Bedrock,
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
            Self::Bedrock => "amazon-bedrock",
            Self::Radius => "radius",
            Self::Custom { id, .. } => id,
        }
    }

    pub fn named(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("openai-chatgpt") {
            return Some(Self::Custom {
                id: Arc::from("openai-chatgpt"),
                adapter: Adapter::OpenAiResponses,
            });
        }
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
            Self::Bedrock => "",
            Self::Radius => super::radius_auth::DEFAULT_MODEL_ENDPOINT,
            Self::Custom { id, .. } => {
                if id.as_ref() == "openai-chatgpt" {
                    return pi_rs::openai::DEFAULT_ENDPOINT;
                }
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
            Adapter::OpenAiResponses => {
                let token = token.ok_or_else(missing_key)?;
                Ok(Client::OpenAi(if self.id() == "openai-chatgpt" {
                    pi_rs::openai::OpenAiResponses::new(endpoint, token)?
                } else {
                    pi_rs::openai::OpenAiResponses::new_api_provider(endpoint, token, self.id())?
                }))
            }
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
                let mut client = if bearer
                    || matches!(
                        self.id(),
                        "fireworks" | "openrouter" | "minimax" | "minimax-cn"
                    ) {
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
            Adapter::Bedrock => {
                if !endpoint.is_empty() {
                    return Err(Error::Config(
                        "custom Bedrock model endpoints are not supported".into(),
                    ));
                }
                let mut client = pi_rs::bedrock::Bedrock::from_env();
                if let Some(token) = token {
                    client = client.with_bearer_token(token)?;
                }
                Ok(Client::Bedrock(client))
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
    Bedrock(pi_rs::bedrock::Bedrock),
    PiMessages(PiMessages),
    OpenAi(pi_rs::openai::OpenAiResponses),
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
            Some(Credential::ChatGpt) if self.http_settings.is_some() => Some(
                super::chatgpt_auth::access(self.http_settings.as_ref().expect("settings checked"))
                    .await?,
            ),
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
            Self::OpenAi(client) => Self::OpenAi(client.with_http_options(timeout, &proxies)?),
            Self::Bedrock(client) => {
                if !proxies.is_empty() {
                    return Err(Error::Config(
                        "workbench HTTP proxy settings are not supported for Bedrock".into(),
                    ));
                }
                Self::Bedrock(client)
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
            Self::Bedrock(_) if !defaults.is_empty() => Err(Error::Config(
                "custom Bedrock headers are not supported".into(),
            )),
            Self::Bedrock(client) if cache_disabled => {
                client.clone().for_summary().stream(request).await
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
            Self::OpenAi(client) => Self::OpenAi(client.with_headers(headers)),
            Self::Bedrock(client) => Self::Bedrock(if headers.is_empty() {
                client
            } else {
                client.unsupported("custom Bedrock headers are not supported")
            }),
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
            Self::OpenAi(client) => client.models().await,
            Self::Bedrock(_) => Ok(Vec::new()),
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
            Self::OpenAi(_) | Self::Bedrock(_) => BTreeMap::new(),
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
            Self::OpenAi(client) => Self::OpenAi(client),
            Self::Bedrock(client) => Self::Bedrock(client),
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
                let gateway = matches!(
                    args.provider.id(),
                    "fireworks" | "openrouter" | "vercel-ai-gateway"
                );
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
                            if matches!(args.provider.id(), "minimax" | "minimax-cn")
                                && args.model == "MiniMax-M3"
                            {
                                client = client.without_thinking_effort();
                            }
                        }
                    }
                }
                Self::Anthropic(client)
            }
            Self::Google(client) => Self::Google(client.with_thinking(thinking)),
            Self::PiMessages(client) => Self::PiMessages(client.with_thinking(thinking)),
            Self::OpenAi(client) => {
                let metadata = args.model_profiles.model(&args.provider, &args.model);
                let sampling = metadata
                    .as_ref()
                    .map(|model| model.sampling(thinking.unwrap_or("off")))
                    .unwrap_or_default();
                let levels = metadata
                    .as_ref()
                    .and_then(|model| model.levels(Adapter::OpenAiResponses))
                    .unwrap_or_else(|| pi_rs::openai::thinking_levels(&args.model));
                let thinking = if metadata
                    .as_ref()
                    .is_some_and(|model| model.reasoning == Some(false))
                    || levels.len() == 1 && levels[0] == "off"
                {
                    None
                } else {
                    thinking
                        .filter(|level| levels.iter().any(|item| item == level))
                        .and_then(|level| {
                            metadata
                                .as_ref()
                                .and_then(|model| model.mapped_effort(level))
                                .unwrap_or(Some(level))
                        })
                };
                let client = client
                    .with_thinking(thinking)
                    .with_sampling(sampling)
                    .map_err(|error| {
                        pi_rs::Error::Config(format!(
                            "invalid sampling for {}/{}: {error}",
                            args.provider.id(),
                            args.model
                        ))
                    })?;
                Self::OpenAi(client)
            }
            Self::Bedrock(client) => {
                let metadata = args.model_profiles.model(&args.provider, &args.model);
                let mapped = thinking.and_then(|level| {
                    metadata
                        .as_ref()
                        .and_then(|model| model.mapped_effort(level))
                });
                let level = if mapped == Some(None) { None } else { thinking };
                let (supports_long, _) = args
                    .model_profiles
                    .messages_cache_support(&args.provider, metadata.as_ref());
                let client = client
                    .with_model_name(
                        &args.model,
                        metadata.as_ref().and_then(|model| model.name.as_deref()),
                    )
                    .with_cache_retention(cache_retention(supports_long))
                    .with_reasoning(level, mapped.flatten())?;
                Self::Bedrock(client)
            }
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
            Self::OpenAi(_) => Some(pi_rs::openai::thinking_levels(model)),
            Self::Bedrock(_) => None,
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
            Self::OpenAi(client) => Self::OpenAi(client),
            Self::Bedrock(client) => Self::Bedrock(client),
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
            Self::OpenAi(client) => client.context_window(model).await,
            Self::Bedrock(client) => client.context_window(model).await,
            Self::Deferred(client) => client.resolve().await.ok()?.context_window(model).await,
        }
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        match self {
            Self::Ollama(client) => client.stream(request).await,
            Self::Anthropic(client) => client.stream(request).await,
            Self::Google(client) => client.stream(request).await,
            Self::PiMessages(client) => client.stream(request).await,
            Self::OpenAi(client) => client.stream(request).await,
            Self::Bedrock(client) => client.stream(request).await,
            Self::Deferred(_) => {
                self.stream_with_headers(request, &HeaderMap::new(), false)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod bedrock_tests {
    use super::*;
    use clap::Parser;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bedrock_summary_and_configured_effort_reach_workbench() {
        const CHILD: &str = "PI_RS_BEDROCK_WORKBENCH_SUMMARY_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let mut args = super::super::Cli::try_parse_from(["pi", "--offline"]).unwrap();
            args.model_profiles=super::super::model_profiles::Profiles::parse(&json!({"providers":{"amazon-bedrock":{"compat":{"supportsLongCacheRetention":false},"models":[
                {"id":"anthropic.claude-opus-4-8","name":"Claude Opus 4.8","reasoning":true,"thinkingLevelMap":{"xhigh":"high"}},
                {"id":"anthropic.claude-sonnet-4-6","name":"Claude Sonnet 4.6","reasoning":true,"compat":{"supportsLongCacheRetention":true}},
                {"id":"openai.gpt-5-test","reasoning":true,"thinkingLevelMap":{"off":"none"}},
                {"id":"openai.gpt-5-null","reasoning":true,"thinkingLevelMap":{"off":null}}
            ]}}})).unwrap();
            args.provider = args.model_profiles.provider("amazon-bedrock").unwrap();
            let request = |model: &str| ModelRequest {
                model: model.into(),
                messages: vec![
                    pi_rs::Message::System {
                        content: "system".into(),
                    },
                    pi_rs::Message::User {
                        content: "hello".into(),
                    },
                ],
                tools: vec![],
                max_output_tokens: 512,
            };
            args.model = "anthropic.claude-sonnet-4-5-20250929-v1:0".into();
            args.thinking = Some("high".into());
            let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                .with_model_options(&args, false)
                .unwrap();
            let _stream = client
                .stream_with_headers(request(&args.model), &HeaderMap::new(), true)
                .await
                .unwrap();
            args.model = "anthropic.claude-opus-4-8".into();
            args.thinking = Some("xhigh".into());
            let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                .with_model_options(&args, false)
                .unwrap();
            let _stream = client
                .stream_with_headers(request(&args.model), &HeaderMap::new(), false)
                .await
                .unwrap();
            for (model, level) in [
                ("anthropic.claude-sonnet-4-6", "high"),
                ("openai.gpt-5-test", "off"),
                ("openai.gpt-5-null", "off"),
            ] {
                args.model = model.into();
                args.thinking = Some(level.into());
                let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                    .with_model_options(&args, false)
                    .unwrap();
                let _stream = client
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), false)
                    .await
                    .unwrap();
            }
            args.model_profiles=super::super::model_profiles::Profiles::parse(&json!({"providers":{"amazon-bedrock":{"compat":{"supportsLongCacheRetention":true},"models":[{"id":"anthropic.claude-opus-4-8","name":"Claude Opus 4.8","reasoning":true,"compat":{"supportsLongCacheRetention":false}}]}}})).unwrap();
            args.model = "anthropic.claude-opus-4-8".into();
            args.thinking = Some("high".into());
            let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                .with_model_options(&args, false)
                .unwrap();
            let _stream = client
                .stream_with_headers(request(&args.model), &HeaderMap::new(), false)
                .await
                .unwrap();
            return;
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "providers::bedrock_tests::bedrock_summary_and_configured_effort_reach_workbench",
            ])
            .env(CHILD, "1")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ACCESS_KEY_ID", "SYNTHETIC_ACCESS_KEY")
            .env("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
            .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", format!("http://{addr}"))
            .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("PI_CACHE_RETENTION", "long")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env_remove("AWS_BEARER_TOKEN_BEDROCK")
            .env_remove("AWS_SESSION_TOKEN")
            .env_remove("AWS_PROFILE")
            .spawn()
            .unwrap();
        let server = async {
            let mut requests = Vec::new();
            for _ in 0..6 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 8192];
                let (end, len) = loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..pos]);
                        let len = headers
                            .lines()
                            .find_map(|l| {
                                l.split_once(':')
                                    .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                    .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (pos + 4, len);
                    }
                };
                while bytes.len() < end + len {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                requests.push(
                    serde_json::from_slice::<serde_json::Value>(&bytes[end..end + len]).unwrap(),
                );
                socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: 0\r\n\r\n").await.unwrap();
            }
            requests
        };
        let requests = match tokio::time::timeout(std::time::Duration::from_secs(20), server).await
        {
            Ok(requests) => requests,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("workbench child did not complete loopback requests")
            }
        };
        assert!(child.wait().unwrap().success());
        let summary = &requests[0];
        assert_eq!(summary["inferenceConfig"]["maxTokens"], 512);
        assert!(summary.get("additionalModelRequestFields").is_none());
        assert_eq!(summary["system"].as_array().unwrap().len(), 1);
        assert_eq!(
            summary["messages"][0]["content"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            requests[1]["additionalModelRequestFields"]["output_config"]["effort"],
            "high"
        );
        assert_eq!(
            requests[1]["system"][1]["cachePoint"],
            json!({"type":"default"})
        );
        assert_eq!(
            requests[2]["system"][1]["cachePoint"],
            json!({"type":"default","ttl":"1h"})
        );
        assert_eq!(
            requests[3]["additionalModelRequestFields"]["reasoning"]["effort"],
            "none"
        );
        assert!(requests[4].get("additionalModelRequestFields").is_none());
        assert_eq!(
            requests[5]["system"][1]["cachePoint"],
            json!({"type":"default"})
        );
    }
}
