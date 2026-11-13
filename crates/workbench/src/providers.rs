//! Provider selection and shared workbench catalog and progress controls.
use std::collections::BTreeMap;

use pi_rs::anthropic::Anthropic;
use pi_rs::google::Google;
use pi_rs::ollama::{ModelDetails, Ollama};
use pi_rs::{CacheRetention, Error, Model, ModelRequest, ModelStream, Result};
use reqwest::header::HeaderMap;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::{Arc, OnceLock};

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
        self.connect_prechecked(adapter, endpoint, token, bearer, false)
    }

    fn connect_prechecked(
        &self,
        adapter: Adapter,
        endpoint: &str,
        token: Option<String>,
        bearer: bool,
        radius_checked: bool,
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
                Ok(Client::Ollama(
                    Ollama::new(endpoint, token)?.with_provider(self.id()),
                ))
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
                        if token.is_none() && !radius_checked && !gateway.has_credential()? {
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
    estimator: Arc<OnceLock<Option<Box<Client>>>>,
}

impl Deferred {
    // Construct without resolving credentials or dynamic headers. Clones share
    // the result until an option that affects serialization changes.
    fn estimation_client(&self) -> Option<&Client> {
        self.estimator
            .get_or_init(|| {
                let credential = (self.adapter != Adapter::Bedrock).then(|| "estimate".into());
                self.provider
                    .connect(self.adapter, &self.endpoint, credential, false)
                    .and_then(|client| match &self.options {
                        Some((args, reported)) => client.with_model_options(args, *reported),
                        None => Ok(client),
                    })
                    .ok()
                    .map(Box::new)
            })
            .as_deref()
    }

    fn context_identity(&self) -> Option<String> {
        // Dynamic credential commands and headers may change on each request.
        if self
            .credential
            .as_ref()
            .is_some_and(Credential::requires_resolution)
            || self.headers.has_command()
        {
            return None;
        }
        let token = self.credential.as_ref().and_then(Credential::literal);
        if token.is_none() && self.adapter != Adapter::ChatCompletions {
            return None;
        }
        if !self
            .headers
            .static_headers(&self.provider, token)
            .ok()?
            .is_empty()
        {
            return None;
        }
        if let Some(settings) = &self.http_settings
            && !super::http_proxy::configured(settings).ok()?.is_empty()
        {
            return None;
        }
        let bearer = self.credential.as_ref().is_some_and(Credential::is_bearer);
        pi_rs::protected_model_fingerprint(&(
            "deferred-context-v1",
            self.provider.id(),
            self.adapter.api(),
            &self.endpoint,
            token,
            bearer,
            self.estimation_client()?.context_identity()?,
            std::env::var_os("PI_CACHE_RETENTION")
                .map(|value| value.to_string_lossy().into_owned()),
        ))
    }

    async fn resolve(&self) -> Result<Client> {
        self.resolve_with_headers().await.map(|(client, _)| client)
    }

    #[cfg(test)]
    async fn resolve_at(&self, auth_path: &std::path::Path) -> Result<Client> {
        self.resolve_with_headers_at(auth_path)
            .await
            .map(|(client, _)| client)
    }

    async fn resolve_with_headers(&self) -> Result<(Client, HeaderMap)> {
        self.resolve_with_headers_at(&super::config::agent_dir().join("auth.json"))
            .await
    }

    async fn resolve_with_headers_at(
        &self,
        auth_path: &std::path::Path,
    ) -> Result<(Client, HeaderMap)> {
        let token = match &self.credential {
            Some(Credential::ChatGpt) if self.http_settings.is_some() => Some(
                super::chatgpt_auth::access(self.http_settings.as_ref().expect("settings checked"))
                    .await?,
            ),
            Some(Credential::Kimi) if self.http_settings.is_some() => Some(
                super::kimi_auth::access(
                    self.http_settings.as_ref().expect("settings checked"),
                    None,
                    false,
                )
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
        let mut radius_checked = false;
        if self.provider == Provider::Radius
            && self.adapter == Adapter::PiMessages
            && token.is_none()
        {
            let gateway = super::radius_auth::Gateway::configured()?;
            if gateway.model_endpoint().as_str() == self.endpoint {
                if gateway.cached_access_at(auth_path).await?.is_none() {
                    return Err(Error::Config(
                        "sign in with /login radius, set RADIUS_API_KEY, or pass --api-key".into(),
                    ));
                }
                radius_checked = true;
            }
        }
        let mut client = self
            .provider
            .connect_prechecked(self.adapter, &self.endpoint, token, bearer, radius_checked)?
            .with_headers(headers.clone())
            .with_context_probes(self.probe_context);
        if let Some(settings) = &self.http_settings {
            client = client.with_http_settings(settings)?;
        }
        if let Some((args, reported)) = &self.options {
            client = client.with_model_options(args, *reported)?;
        }
        Ok((
            if self.observed {
                client.observe()
            } else {
                client
            },
            headers,
        ))
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
            estimator: Arc::new(OnceLock::new()),
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

    /// Resolve dynamic credentials and freeze any catalog route before estimating.
    pub(super) async fn prepare_for_context(
        &self,
        model: &str,
    ) -> Result<Option<(Client, Option<String>)>> {
        let (client, headers) = match self {
            Self::Deferred(deferred) => deferred.resolve_with_headers().await?,
            Self::Ollama(client) => (self.clone(), client.context_headers()),
            Self::Anthropic(client) => (self.clone(), client.context_headers()),
            Self::Google(client) => (self.clone(), client.context_headers()),
            Self::OpenAi(client) => (self.clone(), client.context_headers()),
            Self::PiMessages(client) => (self.clone(), client.context_headers()),
            Self::Bedrock(_) => (self.clone(), HeaderMap::new()),
        };
        let client = match client {
            Self::PiMessages(client) => Self::PiMessages(client.prepare_context(model).await?),
            Self::Bedrock(client) => match client.prepare_bedrock().await? {
                Some(prepared) => Self::Bedrock(prepared),
                None => Self::Bedrock(client),
            },
            other => other,
        };
        // The keyed fingerprint protects low-entropy headers and adapter credentials.
        let base = client
            .clone()
            .with_headers(HeaderMap::new())
            .context_identity();
        let mut header_values: Vec<_> = headers
            .iter()
            .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
            .collect();
        header_values.sort();
        let identity = base.and_then(|base| {
            pi_rs::protected_model_fingerprint(&("resolved-client-v2", base, header_values))
        });
        Ok(Some((client, identity)))
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
                let thinking = thinking.or_else(|| {
                    metadata
                        .as_ref()
                        .filter(|model| {
                            model.reasoning == Some(true)
                                && !pi_rs::bedrock::requires_thinking(
                                    &args.model,
                                    model.name.as_deref(),
                                )
                        })
                        .map(|_| "off")
                });
                let mapped = thinking.and_then(|level| {
                    metadata
                        .as_ref()
                        .and_then(|model| model.mapped_effort(level))
                });
                let level = if mapped == Some(None) { None } else { thinking };
                let long_cache_override = args
                    .model_profiles
                    .bedrock_long_cache_override(&args.provider, metadata.as_ref());
                let budgets = args
                    .settings
                    .value()
                    .get("thinkingBudgets")
                    .map(pi_rs::bedrock::ThinkingBudgets::from_json)
                    .transpose()?
                    .unwrap_or_default();
                let client = client
                    .with_model_name(
                        &args.model,
                        metadata.as_ref().and_then(|model| model.name.as_deref()),
                    )
                    .with_cache_retention(cache_retention(true))
                    .with_long_cache_support(long_cache_override)
                    .with_thinking_budgets(budgets)
                    .with_reasoning(level, mapped.flatten())?;
                Self::Bedrock(client)
            }
            Self::Deferred(mut client) => {
                client.options = Some((Box::new(args.clone()), reported));
                client.estimator = Arc::new(OnceLock::new());
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
    fn context_identity(&self) -> Option<String> {
        match self {
            Self::Ollama(c) => c.context_identity(),
            Self::Anthropic(c) => c.context_identity(),
            Self::Google(c) => c.context_identity(),
            Self::PiMessages(c) => c.context_identity(),
            Self::OpenAi(c) => c.context_identity(),
            Self::Bedrock(c) => c.context_identity(),
            Self::Deferred(client) => client.context_identity(),
        }
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &pi_rs::Message) -> u64 {
        match self {
            Self::Ollama(c) => c.estimate_message_tokens(request, message),
            Self::Anthropic(c) => c.estimate_message_tokens(request, message),
            Self::Google(c) => c.estimate_message_tokens(request, message),
            Self::PiMessages(c) => c.estimate_message_tokens(request, message),
            Self::OpenAi(c) => c.estimate_message_tokens(request, message),
            Self::Bedrock(c) => c.estimate_message_tokens(request, message),
            Self::Deferred(c) => c.estimation_client().map_or_else(
                || pi_rs::estimate_message_tokens(message),
                |client| client.estimate_message_tokens(request, message),
            ),
        }
    }

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
mod contention_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn deferred_radius_connect_waits_for_shared_credentials_and_can_cancel() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.json");
        let gateway = super::super::radius_auth::Gateway::configured().unwrap();
        std::fs::write(
            &path,
            json!({"radius":{
                "type":"oauth", "gateway":gateway.url("/").as_str(),
                "access":"cached", "refresh":"refresh", "expires":0
            }})
            .to_string(),
        )
        .unwrap();
        let Client::Deferred(deferred) = Client::deferred(
            Provider::Radius,
            Adapter::PiMessages,
            gateway.model_endpoint().as_str(),
            None,
            RequestHeaders::default(),
        ) else {
            panic!("expected deferred provider");
        };
        let held = super::super::auth_lock::acquire_async(&path).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(120), deferred.resolve_at(&path))
                .await
                .is_err()
        );
        let mut waiting = tokio::spawn({
            let path = path.clone();
            async move { deferred.resolve_at(&path).await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(120), &mut waiting)
                .await
                .is_err()
        );
        drop(held);
        assert!(matches!(
            waiting.await.unwrap().unwrap(),
            Client::PiMessages(_)
        ));
    }
}

#[cfg(test)]
mod estimation_tests {
    use super::*;
    use clap::Parser;

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn kimi_oauth_prepare_preserves_protected_resolved_identity() {
        const CHILD: &str = "PI_RS_TEST_KIMI_PREPARED_IDENTITY_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let path = super::super::config::agent_dir().join("auth.json");
            let save = |access: &str| {
                std::fs::write(&path, json!({"kimi-coding":{
                    "type":"oauth", "access":access, "refresh":"fixture-refresh", "expires":9_000_000_000_000_u64
                }}).to_string()).unwrap();
            };
            save("fixture-first-access");
            let provider = Provider::named("kimi-coding").unwrap();
            let client = Client::deferred(
                provider.clone(),
                provider.adapter(),
                provider.default_endpoint(),
                Some(Credential::Kimi),
                RequestHeaders::default(),
            );
            assert!(client.context_identity().is_none());
            let (prepared, first) = client
                .prepare_for_context("kimi-for-coding")
                .await
                .unwrap()
                .unwrap();
            let (_, repeated) = client
                .prepare_for_context("kimi-for-coding")
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(prepared, Client::Ollama(_)));
            assert!(first.is_some());
            assert_eq!(first, repeated);
            save("fixture-second-access");
            let (_, changed) = client
                .prepare_for_context("kimi-for-coding")
                .await
                .unwrap()
                .unwrap();
            assert_ne!(first, changed);
            assert!(!first.unwrap().contains("fixture-first-access"));
            assert!(!changed.unwrap().contains("fixture-second-access"));
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "providers::estimation_tests::kimi_oauth_prepare_preserves_protected_resolved_identity"])
            .env(CHILD, "1")
            .env("PI_RS_HOME", directory.path())
            .env_remove("KIMI_API_KEY")
            .current_dir(directory.path())
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn deferred_bedrock_estimates_tool_images_without_synthetic_credentials() {
        let client = Client::deferred(
            Provider::Bedrock,
            Adapter::Bedrock,
            "",
            None,
            RequestHeaders::default(),
        );
        let Client::Deferred(deferred) = &client else {
            unreachable!()
        };
        assert!(matches!(
            deferred.estimation_client(),
            Some(Client::Bedrock(_))
        ));
        assert!(client.context_identity().is_none());
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 1,
        };
        let message = pi_rs::Message::Tool { call_id: "id".into(), is_error: false, is_structured: Some(true),
            content: json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(20000)}]}}).to_string() };
        assert!((1200..1300).contains(&client.estimate_message_tokens(&request, &message)));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn deferred_static_credentials_have_stable_route_bound_identity() {
        let client = |provider, adapter, endpoint: &str, credential| {
            Client::deferred(
                provider,
                adapter,
                endpoint,
                credential,
                RequestHeaders::default(),
            )
        };
        let anthropic = client(
            Provider::Anthropic,
            Adapter::Anthropic,
            "https://api.anthropic.com/v1/messages",
            Some(Credential::Literal("key-a".into())),
        );
        let clone = anthropic.clone();
        assert!(anthropic.context_identity().is_some());
        assert_eq!(anthropic.context_identity(), clone.context_identity());
        let other_key = client(
            Provider::Anthropic,
            Adapter::Anthropic,
            "https://api.anthropic.com/v1/messages",
            Some(Credential::Literal("key-b".into())),
        );
        let other_route = client(
            Provider::Anthropic,
            Adapter::Anthropic,
            "https://example.com/v1/messages",
            Some(Credential::Literal("key-a".into())),
        );
        assert_ne!(anthropic.context_identity(), other_key.context_identity());
        assert_ne!(anthropic.context_identity(), other_route.context_identity());
        let dynamic = client(
            Provider::Anthropic,
            Adapter::Anthropic,
            "https://api.anthropic.com/v1/messages",
            Some(Credential::Command("!read-key".into())),
        );
        assert!(dynamic.context_identity().is_none());
        let google = client(
            Provider::Google,
            Adapter::Google,
            "https://example.com/v1",
            Some(Credential::Literal("key-a".into())),
        );
        assert!(google.context_identity().is_some());
        assert_ne!(anthropic.context_identity(), google.context_identity());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn anthropic_direct_and_deferred_reuse_measured_baselines() {
        struct MeasuredFixture {
            wrapped: super::super::image_policy::WorkbenchModel,
        }
        #[async_trait::async_trait]
        impl Model for MeasuredFixture {
            fn context_identity(&self) -> Option<String> {
                self.wrapped.context_identity()
            }
            fn estimate_message_tokens(
                &self,
                request: &ModelRequest,
                message: &pi_rs::Message,
            ) -> u64 {
                self.wrapped.estimate_message_tokens(request, message)
            }
            fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
                self.wrapped.estimate_request_tokens(request)
            }
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
                Ok(Box::pin(futures_util::stream::iter(vec![
                    Ok(pi_rs::ModelEvent::Text("reply".into())),
                    Ok(pi_rs::ModelEvent::Complete {
                        calls: vec![],
                        usage: Some(pi_rs::Usage {
                            input_tokens: 3000,
                            output_tokens: 8,
                        }),
                    }),
                ])))
            }
        }
        for deferred in [false, true] {
            let client = if deferred {
                Client::deferred(
                    Provider::Anthropic,
                    Adapter::Anthropic,
                    "https://api.anthropic.com/v1/messages",
                    Some(Credential::Literal("key".into())),
                    RequestHeaders::default(),
                )
            } else {
                Client::Anthropic(
                    Anthropic::new("https://api.anthropic.com/v1/messages", "key").unwrap(),
                )
            };
            let model = MeasuredFixture {
                wrapped: super::super::image_policy::WorkbenchModel {
                    client,
                    block_images: false,
                    supports_images: Some(true),
                    session_affinity: &[],
                    price: None,
                },
            };
            assert!(model.context_identity().is_some());
            let registry = pi_rs::Registry::default();
            let mut agent = pi_rs::Agent {
                registry: &registry,
                system_prompt: "short",
                model: &model,
                model_name: "fixture",
                limits: pi_rs::Limits::default(),
            };
            agent.limits.output_tokens_per_request = 64;
            let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
            session.auto_compaction_control().set_enabled(false);
            let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
            agent
                .run(
                    &mut session,
                    Some("first".into()),
                    &mut inputs,
                    &pi_rs::CancellationToken::new(),
                    &mut |_| {},
                )
                .await
                .unwrap();
            agent.limits.context_window = Some(1000);
            let result = agent
                .run(
                    &mut session,
                    Some("next".into()),
                    &mut inputs,
                    &pi_rs::CancellationToken::new(),
                    &mut |_| {},
                )
                .await;
            assert!(
                matches!(result, Err(pi_rs::Error::Model(message)) if message.contains("automatic compaction is disabled")),
                "deferred={deferred}"
            );
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_command_credentials_resolve_with_protected_identity() {
        let client = Client::deferred(
            Provider::Google,
            Adapter::Google,
            "https://example.com/v1",
            Some(Credential::Command("!echo key-one".into())),
            RequestHeaders::for_test("x-routing", "!echo route-one"),
        );
        let (prepared, identity) = client
            .prepare_for_context("fixture")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(prepared, Client::Google(_)));
        assert!(identity.is_some());
    }

    #[cfg(not(any(unix, windows)))]
    #[tokio::test]
    async fn unprotected_platform_does_not_persist_header_identity() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-routing",
            reqwest::header::HeaderValue::from_static("secret"),
        );
        let client = Client::Google(
            pi_rs::google::Google::new("https://example.com/v1", "key")
                .unwrap()
                .with_headers(headers),
        );
        let (_, identity) = client
            .prepare_for_context("fixture")
            .await
            .unwrap()
            .unwrap();
        assert!(identity.is_none());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn direct_static_headers_reuse_baseline_without_unkeyed_header_fingerprint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let header_end = loop {
                let mut buffer = [0_u8; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let request = String::from_utf8_lossy(&bytes[..header_end]).to_lowercase();
            let length = request
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while bytes.len() < header_end + length {
                let mut buffer = [0_u8; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
            }
            assert!(request.contains("x-routing: route-one"));
            assert!(request.contains("x-goog-api-key: key-one"));
            let body = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"reply\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":3000,\"candidatesTokenCount\":8}}\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let mut headers = HeaderMap::new();
        let mut routing = reqwest::header::HeaderValue::from_static("route-one");
        routing.set_sensitive(true);
        headers.insert("x-routing", routing);
        let client = Client::Google(
            pi_rs::google::Google::new(&endpoint, "key-one")
                .unwrap()
                .with_context_probe(false)
                .with_headers(headers),
        );
        let model = super::super::image_policy::WorkbenchModel {
            client,
            block_images: false,
            supports_images: Some(true),
            session_affinity: &[],
            price: None,
        };
        let registry = pi_rs::Registry::default();
        let mut agent = pi_rs::Agent {
            registry: &registry,
            system_prompt: "short",
            model: &model,
            model_name: "fixture",
            limits: pi_rs::Limits::default(),
        };
        agent.limits.context_window = Some(5000);
        agent.limits.output_tokens_per_request = 64;
        let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session.auto_compaction_control().set_enabled(false);
        let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        agent
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        server.await.unwrap();
        let record = session
            .records()
            .iter()
            .find_map(|record| match &record.event {
                pi_rs::Event::ModelRequested {
                    context_configuration_sha256,
                    ..
                } => context_configuration_sha256.as_ref(),
                _ => None,
            })
            .unwrap();
        let base = model
            .client
            .clone()
            .with_headers(HeaderMap::new())
            .context_identity()
            .unwrap();
        let guessed = pi_rs::fingerprint_model_context(&(
            "resolved-client-v2",
            base,
            vec![("x-routing".to_owned(), b"route-one".to_vec())],
        ))
        .unwrap();
        let guessed_policy = pi_rs::fingerprint_model_context(&(
            "workbench-policy-v2",
            guessed,
            false,
            Some(true),
            &[] as &[&str],
        ))
        .unwrap();
        let guessed_journal = pi_rs::fingerprint_model_context(&(
            "effective-context-v2",
            guessed_policy,
            "fixture",
            vec!["short"],
            Vec::<pi_rs::ToolSpec>::new(),
        ))
        .unwrap();
        assert_ne!(*record, guessed_journal);
        assert!(
            !serde_json::to_string(&session.records())
                .unwrap()
                .contains("route-one")
        );
        agent.limits.context_window = Some(1000);
        let result = agent
            .run(
                &mut session,
                Some("next".into()),
                &mut inputs,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        assert!(
            matches!(result, Err(pi_rs::Error::Model(message)) if message.contains("automatic compaction is disabled"))
        );
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn dynamic_credentials_and_headers_reuse_measured_usage_after_dispatch() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let header_end = loop {
                let mut buffer = [0_u8; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let request = String::from_utf8_lossy(&bytes[..header_end]).to_lowercase();
            let length = request
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while bytes.len() < header_end + length {
                let mut buffer = [0_u8; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
            }
            assert!(request.contains("x-routing: route-one"));
            assert!(request.contains("x-goog-api-key: key-one"));
            let body = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"reply\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":3000,\"candidatesTokenCount\":8}}\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client = Client::deferred(
            Provider::Google,
            Adapter::Google,
            &endpoint,
            Some(Credential::Command("!echo key-one".into())),
            RequestHeaders::for_test("x-routing", "!echo route-one"),
        )
        .with_context_probes(false);
        let model = super::super::image_policy::WorkbenchModel {
            client,
            block_images: false,
            supports_images: Some(true),
            session_affinity: &[],
            price: None,
        };
        let registry = pi_rs::Registry::default();
        let mut agent = pi_rs::Agent {
            registry: &registry,
            system_prompt: "short",
            model: &model,
            model_name: "fixture",
            limits: pi_rs::Limits::default(),
        };
        agent.limits.context_window = Some(5000);
        agent.limits.output_tokens_per_request = 64;
        let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session.auto_compaction_control().set_enabled(false);
        let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        agent
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        server.await.unwrap();
        agent.limits.context_window = Some(1000);
        let result = agent
            .run(
                &mut session,
                Some("next".into()),
                &mut inputs,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        assert!(
            matches!(result, Err(pi_rs::Error::Model(message)) if message.contains("automatic compaction is disabled"))
        );
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn resolved_dynamic_headers_are_included_in_prepared_identity() {
        let make = |header: &str| {
            Client::deferred(
                Provider::Google,
                Adapter::Google,
                "https://example.com/v1",
                Some(Credential::Literal("key".into())),
                RequestHeaders::for_test("x-routing", header),
            )
        };
        let first = make("!echo route-one");
        let second = make("!echo route-two");
        let (_, a) = first.prepare_for_context("fixture").await.unwrap().unwrap();
        let (_, same) = first.prepare_for_context("fixture").await.unwrap().unwrap();
        let (_, b) = second
            .prepare_for_context("fixture")
            .await
            .unwrap()
            .unwrap();
        assert!(a.is_some());
        assert_eq!(a, same);
        assert_ne!(a, b);
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn dynamic_credentials_are_resolved_once_for_each_prepared_client() {
        let make = |command: &str| {
            Client::deferred(
                Provider::Google,
                Adapter::Google,
                "https://example.com/v1",
                Some(Credential::Command(command.into())),
                RequestHeaders::default(),
            )
        };
        let first = make("!echo key-one");
        let second = make("!echo key-two");
        assert!(first.context_identity().is_none());
        let (prepared, first_identity) =
            first.prepare_for_context("fixture").await.unwrap().unwrap();
        let (_, repeated_identity) = first.prepare_for_context("fixture").await.unwrap().unwrap();
        let (_, second_identity) = second
            .prepare_for_context("fixture")
            .await
            .unwrap()
            .unwrap();
        assert!(first_identity.is_some());
        assert_eq!(first_identity, repeated_identity);
        assert_ne!(first_identity, second_identity);
        assert!(prepared.context_identity().is_some());
    }

    #[test]
    fn deferred_estimator_is_reused_and_options_invalidate_cache() {
        let client = Client::deferred(
            Provider::Google,
            Adapter::Google,
            "https://example.com/v1",
            None,
            RequestHeaders::default(),
        );
        let Client::Deferred(deferred) = &client else {
            panic!("expected deferred adapter")
        };
        assert!(deferred.estimator.get().is_none());
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 1,
        };
        let message = pi_rs::Message::User {
            content: "hello".into(),
        };
        assert!(client.estimate_message_tokens(&request, &message) > 0);
        let first = deferred.estimation_client().unwrap();
        for _ in 0..32 {
            assert!(client.estimate_message_tokens(&request, &message) > 0);
            assert!(std::ptr::eq(first, deferred.estimation_client().unwrap()));
        }
        let clone = client.clone();
        let Client::Deferred(shared) = &clone else {
            unreachable!()
        };
        assert!(std::ptr::eq(first, shared.estimation_client().unwrap()));
        let changed = clone
            .with_model_options(&Cli::parse_from(["pi"]), false)
            .unwrap();
        let Client::Deferred(changed) = &changed else {
            unreachable!()
        };
        assert!(changed.estimator.get().is_none());
        assert!(!std::ptr::eq(first, changed.estimation_client().unwrap()));
        assert!(std::ptr::eq(first, deferred.estimation_client().unwrap()));
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
                {"id":"openai.gpt-5-null","reasoning":true,"thinkingLevelMap":{"off":null}},
                {"id":"openai.gpt-5-unmapped","reasoning":true},
                {"id":"openai.gpt-5-nonreason","reasoning":false,"thinkingLevelMap":{"off":"none"}}
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
            for model in [
                "openai.gpt-5-test",
                "openai.gpt-5-null",
                "openai.gpt-5-unmapped",
                "openai.gpt-5-nonreason",
            ] {
                args.model = model.into();
                args.thinking = None;
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
            args.model_profiles = super::super::model_profiles::Profiles::parse(
                &json!({"providers":{"amazon-bedrock":{"models":[
                    {"id":"anthropic.claude-opus-4-1","name":"Claude Opus 4.1"},
                    {"id":"anthropic.claude-opus-4-6","name":"Claude Opus 4.6","reasoning":true},
                    {"id":"anthropic.claude-sonnet-5","name":"Claude Sonnet 5","reasoning":true},
                    {"id":"opaque-fable","name":"Claude Fable 5","reasoning":true,"thinkingLevelMap":{"high":"high"}},
                    {"id":"anthropic.claude-opus-5-5","name":"Claude Opus 5.5","reasoning":true}
                ]}}}),
            )
            .unwrap();
            for (model, thinking) in [
                ("anthropic.claude-opus-4-1", None),
                ("anthropic.claude-opus-4-6", Some("max")),
            ] {
                args.model = model.into();
                args.thinking = thinking.map(str::to_owned);
                let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                    .with_model_options(&args, false)
                    .unwrap();
                let _stream = client
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), false)
                    .await
                    .unwrap();
            }
            args.model = "anthropic.claude-sonnet-5".into();
            for (thinking, summary) in [(None, false), (Some("high"), true)] {
                args.thinking = thinking.map(str::to_owned);
                let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                    .with_model_options(&args, false)
                    .unwrap();
                let _stream = client
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), summary)
                    .await
                    .unwrap();
            }
            for model in ["opaque-fable", "anthropic.claude-opus-5-5"] {
                args.model = model.into();
                for summary in [false, true] {
                    args.thinking = None;
                    let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                        .with_model_options(&args, false)
                        .unwrap();
                    let _stream = client
                        .stream_with_headers(request(&args.model), &HeaderMap::new(), summary)
                        .await
                        .unwrap();
                }
            }
            let workspace = tempfile::tempdir().unwrap();
            std::fs::create_dir(workspace.path().join(".pi")).unwrap();
            std::fs::write(
                workspace.path().join(".pi/settings.json"),
                r#"{"thinkingBudgets":{"high":3456}}"#,
            )
            .unwrap();
            args.settings = super::super::config::Settings::load(workspace.path(), true).unwrap();
            args.model = "anthropic.claude-sonnet-4-5".into();
            args.thinking = Some("high".into());
            let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                .with_model_options(&args, false)
                .unwrap();
            let mut req = request(&args.model);
            req.max_output_tokens = 8192;
            let _stream = client
                .stream_with_headers(req, &HeaderMap::new(), false)
                .await
                .unwrap();
            return;
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let agent_dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "providers::bedrock_tests::bedrock_summary_and_configured_effort_reach_workbench",
            ])
            .env(CHILD, "1")
            .env("PI_RS_HOME", agent_dir.path())
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
            for _ in 0..19 {
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
            requests[5]["additionalModelRequestFields"]["reasoning"]["effort"],
            "none"
        );
        assert!(requests[6].get("additionalModelRequestFields").is_none());
        assert!(requests[7].get("additionalModelRequestFields").is_none());
        assert!(requests[8].get("additionalModelRequestFields").is_none());
        assert_eq!(
            requests[9]["system"][1]["cachePoint"],
            json!({"type":"default"})
        );
        assert_eq!(
            requests[10]["system"][1]["cachePoint"],
            json!({"type":"default"})
        );
        assert_eq!(
            requests[11]["system"][1]["cachePoint"],
            json!({"type":"default","ttl":"1h"})
        );
        assert_eq!(
            requests[11]["additionalModelRequestFields"]["output_config"]["effort"],
            "max"
        );
        for request in &requests[12..14] {
            assert_eq!(
                request["additionalModelRequestFields"],
                json!({"thinking":{"type":"disabled"}})
            );
        }
        for (normal, summary) in [(14, 15), (16, 17)] {
            assert!(
                requests[normal]
                    .get("additionalModelRequestFields")
                    .is_none()
            );
            assert_eq!(
                requests[summary]["additionalModelRequestFields"]["thinking"]["type"],
                "adaptive"
            );
            assert_eq!(
                requests[summary]["additionalModelRequestFields"]["output_config"]["effort"],
                "low"
            );
            assert_eq!(requests[summary]["inferenceConfig"]["maxTokens"], 512);
        }
        assert_eq!(
            requests[18]["additionalModelRequestFields"]["thinking"]["budget_tokens"],
            3456
        );
    }
}
