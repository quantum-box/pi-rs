//! Provider selection and shared workbench catalog and progress controls.
use futures_util::StreamExt;
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
                let mut client = if self == &Self::Anthropic && token.contains("sk-ant-oat") {
                    Anthropic::with_oauth_token(endpoint, &token)?
                } else if bearer
                    || matches!(
                        self.id(),
                        "fireworks" | "openrouter" | "minimax" | "minimax-cn"
                    )
                {
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
            Some(Credential::BedrockProfile(_) | Credential::BedrockChain) => None,
            Some(Credential::AnthropicOAuth) if self.http_settings.is_some() => Some(
                super::anthropic_auth::access(
                    self.http_settings.as_ref().expect("settings checked"),
                    None,
                    false,
                )
                .await?,
            ),
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
            Some(Credential::Xai) if self.http_settings.is_some() => Some(
                super::xai_auth::access(
                    self.http_settings.as_ref().expect("settings checked"),
                    None,
                    false,
                )
                .await?,
            ),
            Some(Credential::Meta) if self.http_settings.is_some() => Some(
                super::meta_auth::access(
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
        match (&self.credential, &mut client) {
            (Some(Credential::BedrockProfile(profile)), Client::Bedrock(bedrock)) => {
                *bedrock = bedrock.clone().with_profile(profile.clone());
            }
            (Some(Credential::BedrockChain), Client::Bedrock(bedrock)) => {
                *bedrock = bedrock.clone().with_credential_chain();
            }
            _ => {}
        }
        if let Some(settings) = &self.http_settings {
            client = client.with_http_settings_and_env(settings, self.headers.environment())?;
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
    pub fn supports_prepared_tool_dispatch(&self) -> bool {
        match self {
            Self::OpenAi(_) | Self::Ollama(_) => true,
            Self::Deferred(client) => matches!(
                client.adapter,
                Adapter::OpenAiResponses | Adapter::ChatCompletions
            ),
            _ => false,
        }
    }

    /// Resolve dynamic credentials and request headers once before deciding
    /// which affinity defaults are actually needed.
    pub(super) async fn resolve_affinity_headers(&self) -> Result<(Self, HeaderMap)> {
        match self {
            Self::Deferred(deferred) => deferred.resolve_with_headers().await,
            Self::Ollama(client) => Ok((self.clone(), client.context_headers())),
            Self::Anthropic(client) => Ok((self.clone(), client.context_headers())),
            Self::Google(client) => Ok((self.clone(), client.context_headers())),
            Self::OpenAi(client) => Ok((self.clone(), client.context_headers())),
            Self::PiMessages(client) => Ok((self.clone(), client.context_headers())),
            _ => Ok((self.clone(), HeaderMap::new())),
        }
    }

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
        self.with_http_settings_and_env(settings, None)
    }

    pub fn with_http_settings_and_env(
        self,
        settings: &super::config::Settings,
        env: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Result<Self> {
        let this = match self {
            Self::Bedrock(client) => {
                return Ok(match super::http_proxy::bedrock_client(settings, env)? {
                    Some((runtime, credentials)) => {
                        Self::Bedrock(client.with_http_clients(runtime, credentials))
                    }
                    None => Self::Bedrock(client),
                });
            }
            other => other,
        };
        let proxies = super::http_proxy::configured(settings)?;
        let timeout = settings.http_idle_timeout();
        Ok(match this {
            Self::Ollama(client) => Self::Ollama(client.with_http_options(timeout, &proxies)?),
            Self::Anthropic(client) => {
                Self::Anthropic(client.with_http_options(timeout, &proxies)?)
            }
            Self::Google(client) => Self::Google(client.with_http_options(timeout, &proxies)?),
            Self::PiMessages(client) => {
                Self::PiMessages(client.with_http_options(timeout, &proxies)?)
            }
            Self::OpenAi(client) => Self::OpenAi(client.with_http_options(timeout, &proxies)?),
            Self::Bedrock(_) => unreachable!("Bedrock handled above"),
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
        let base = match &client {
            Self::Bedrock(_) => client.context_identity(),
            _ => client
                .clone()
                .with_headers(HeaderMap::new())
                .context_identity(),
        };
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
        session_cache_key: Option<&str>,
    ) -> Result<ModelStream> {
        match self {
            Self::Ollama(client) => {
                let client = if cache_disabled {
                    client.clone().without_cache()
                } else {
                    let client = client.clone().with_default_headers(defaults);
                    if let Some(key) = session_cache_key {
                        client.with_session_cache_key(key)
                    } else {
                        client
                    }
                };
                client.stream(request).await
            }
            Self::OpenAi(client) => {
                let client = if cache_disabled {
                    client.clone().without_cache()
                } else if let Some(key) = session_cache_key {
                    client.clone().with_session_cache_key(key)
                } else {
                    client.clone()
                };
                let client = if cache_disabled {
                    client
                } else {
                    client.with_default_headers(defaults)
                };
                client.stream(request).await
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
            Self::Bedrock(client) => {
                let client = client.clone().with_default_headers(defaults)?;
                if cache_disabled {
                    client.for_summary().stream(request).await
                } else {
                    client.stream(request).await
                }
            }
            Self::Deferred(client) => {
                if client.observed {
                    super::output(json!({"type":"model_progress","stage":"resolving_credentials"}));
                }
                Box::pin(client.resolve().await?.stream_with_headers(
                    request,
                    defaults,
                    cache_disabled,
                    session_cache_key,
                ))
                .await
            }
            _ => self.stream(request).await,
        }
    }

    pub async fn stream_enriched_with_headers(
        &self,
        request: ModelRequest,
        defaults: &HeaderMap,
        cache_disabled: bool,
        session_cache_key: Option<&str>,
    ) -> Result<pi_rs::EnrichedModelStream> {
        match self {
            Self::OpenAi(client) => {
                if cache_disabled {
                    client
                        .stream_enriched_for_summary(uuid::Uuid::nil(), uuid::Uuid::nil(), request)
                        .await
                } else if let Some(key) = session_cache_key {
                    // Workbench applies its own model-specific affinity headers.
                    // Do not let the standalone SDK add a second default set.
                    client
                        .clone()
                        .with_default_headers(defaults)
                        .with_session_affinity_format(
                            pi_rs::openai::SessionAffinityFormat::Disabled,
                        )
                        .stream_enriched_in_session_with_identity(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            key,
                            request,
                        )
                        .await
                } else {
                    let stream = client.stream(request).await?;
                    Ok(Box::pin(
                        stream.map(|event| event.map(pi_rs::EnrichedModelEvent::Legacy)),
                    ))
                }
            }
            Self::Anthropic(client) => {
                if cache_disabled {
                    client
                        .clone()
                        .without_cache()
                        .stream_enriched_for_summary(uuid::Uuid::nil(), uuid::Uuid::nil(), request)
                        .await
                } else {
                    let client = if defaults.is_empty() {
                        client.clone()
                    } else {
                        client.clone().with_default_headers(defaults)
                    };
                    client
                        .stream_enriched_in_session_with_identity(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            session_cache_key.unwrap_or_default(),
                            request,
                        )
                        .await
                }
            }
            Self::Deferred(client) => {
                if client.observed {
                    super::output(json!({"type":"model_progress","stage":"resolving_credentials"}));
                }
                Box::pin(client.resolve().await?.stream_enriched_with_headers(
                    request,
                    defaults,
                    cache_disabled,
                    session_cache_key,
                ))
                .await
            }
            _ => {
                let stream = self
                    .stream_with_headers(request, defaults, cache_disabled, session_cache_key)
                    .await?;
                Ok(Box::pin(
                    stream.map(|event| event.map(pi_rs::EnrichedModelEvent::Legacy)),
                ))
            }
        }
    }

    pub async fn stream_detailed_with_headers(
        &self,
        request: ModelRequest,
        defaults: &HeaderMap,
        cache_disabled: bool,
        session_cache_key: Option<&str>,
    ) -> Result<pi_rs::DetailedModelStream> {
        match self {
            Self::Anthropic(client) => {
                if cache_disabled {
                    client
                        .clone()
                        .without_cache()
                        .stream_detailed_for_summary(uuid::Uuid::nil(), uuid::Uuid::nil(), request)
                        .await
                } else {
                    let client = if defaults.is_empty() {
                        client.clone()
                    } else {
                        client.clone().with_default_headers(defaults)
                    };
                    client
                        .stream_detailed_in_session_with_identity(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            session_cache_key.unwrap_or_default(),
                            request,
                        )
                        .await
                }
            }
            Self::Deferred(client) => {
                if client.observed {
                    super::output(json!({"type":"model_progress","stage":"resolving_credentials"}));
                }
                Box::pin(client.resolve().await?.stream_detailed_with_headers(
                    request,
                    defaults,
                    cache_disabled,
                    session_cache_key,
                ))
                .await
            }
            _ => Ok(Box::pin(
                self.stream_enriched_with_headers(
                    request,
                    defaults,
                    cache_disabled,
                    session_cache_key,
                )
                .await?
                .map(|event| event.map(Into::into)),
            )),
        }
    }

    pub async fn prepare_tool_dispatch_with_headers(
        &self,
        request: &ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
        context: &pi_rs::ToolDispatchContext,
        defaults: &HeaderMap,
        workbench_routing: bool,
    ) -> Result<Option<Box<dyn pi_rs::PreparedToolDispatch>>> {
        match self {
            Self::Ollama(client) => {
                let client = if context.summary {
                    client.clone()
                } else {
                    client.clone().with_default_headers(defaults)
                };
                Ok(Some(client.prepare_bound_tool_dispatch(
                    request,
                    declarations,
                    context,
                )?))
            }
            Self::OpenAi(client) => {
                let client = client.clone().with_default_headers(defaults);
                let client = if workbench_routing {
                    client.with_session_affinity_format(
                        pi_rs::openai::SessionAffinityFormat::Disabled,
                    )
                } else {
                    client
                };
                Ok(Some(client.prepare_bound_tool_dispatch(
                    request,
                    declarations,
                    context,
                )?))
            }
            Self::Deferred(client)
                if matches!(
                    client.adapter,
                    Adapter::OpenAiResponses | Adapter::ChatCompletions
                ) =>
            {
                Box::pin(client.resolve().await?.prepare_tool_dispatch_with_headers(
                    request,
                    declarations,
                    context,
                    defaults,
                    workbench_routing,
                ))
                .await
            }
            _ => {
                pi_rs::validate_legacy_tool_declarations(request, declarations)?;
                Ok(None)
            }
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
            Self::Bedrock(client) => Self::Bedrock(match client.clone().with_headers(headers) {
                Ok(client) => client,
                Err(_) => client.unsupported("invalid Bedrock custom headers"),
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
                let (key_enabled, long) = args.model_profiles.chat_prompt_cache_policy(
                    &args.provider,
                    &args.model,
                    configured.as_ref(),
                    args.model_endpoint_for(&args.provider, Some(&args.model)),
                    std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long"),
                );
                let client = client
                    .with_cache_retention(retention)
                    .with_prompt_cache_policy(key_enabled, long);
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
                    super::thinking::adopted_reasoning(args),
                    thinking.is_some_and(|level| level != "off"),
                    configured
                        .as_ref()
                        .is_none_or(|model| model.mapped_effort("off") != Some(None)),
                );
                let sampling = configured
                    .map(|model| model.sampling(thinking.unwrap_or("off")))
                    .unwrap_or_default();
                Self::Ollama(
                    client
                        .with_thinking_budgets(
                            thinking,
                            args.settings.value().get("thinkingBudgets").cloned(),
                        )
                        .with_sampling(sampling)?,
                )
            }
            Self::Anthropic(client) => {
                let metadata = args.model_profiles.model(&args.provider, &args.model);
                let (supports_long, cache_tools) = args
                    .model_profiles
                    .messages_cache_support(&args.provider, metadata.as_ref());
                let retention = cache_retention(supports_long);
                let mut client = client
                    .with_thinking(thinking)
                    .with_thinking_budget_settings(
                        args.settings.value().get("thinkingBudgets").cloned(),
                    )
                    .with_cache_retention(retention, cache_tools);
                if let Some(metadata) = metadata {
                    client =
                        client.with_empty_thinking_signature(metadata.allows_empty_signature());
                    let adaptive = metadata.adaptive_thinking();
                    if metadata.reasoning == Some(true) {
                        // Messages-compatible gateways need not implement Anthropic's
                        // Models API. Keep their configured capabilities local.
                        if args.provider.id() != "anthropic" {
                            client = client.with_context_probe(false);
                        }
                        let selected = thinking.unwrap_or("off");
                        let mapped = metadata.mapped_effort(selected);
                        if selected == "off" && mapped == Some(None) {
                            client = client.with_thinking(None);
                        } else {
                            let effort = if adaptive == Some(true) && selected != "off" {
                                mapped.flatten().unwrap_or(if selected == "minimal" {
                                    "low"
                                } else {
                                    selected
                                })
                            } else {
                                selected
                            };
                            client = if adaptive.is_some() || metadata.has_thinking_level_map() {
                                client.with_configured_thinking(
                                    effort,
                                    adaptive.unwrap_or(false),
                                    true,
                                )
                            } else {
                                client.with_fallback_thinking(effort, false, true)
                            };
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
            Self::Google(client) => {
                let metadata = args.model_profiles.model(&args.provider, &args.model);
                let levels = metadata
                    .as_ref()
                    .and_then(|model| model.levels(Adapter::Google));
                let resolve = |level: &str| -> Result<String> {
                    let mapped = metadata
                        .as_ref()
                        .and_then(|model| model.mapped_effort(level))
                        .flatten()
                        .unwrap_or(level)
                        .to_ascii_lowercase();
                    if !matches!(mapped.as_str(), "minimal" | "low" | "medium" | "high") {
                        return Err(Error::Config(format!(
                            "unsupported Google thinking level mapping {mapped:?}"
                        )));
                    }
                    Ok(mapped)
                };
                let selected = thinking.map(|level| {
                    levels
                        .as_ref()
                        .map_or(level, |levels| super::thinking::clamp(level, levels))
                });
                let mapped = selected
                    .filter(|level| *level != "off")
                    .map(resolve)
                    .transpose()?;
                let fallback =
                    if thinking.is_none() && pi_rs::google::uses_discrete_thinking(&args.model) {
                        levels
                            .as_ref()
                            .filter(|levels| !levels.iter().any(|level| level == "off"))
                            .map(|levels| resolve(super::thinking::clamp("off", levels)))
                            .transpose()?
                    } else {
                        None
                    };
                Self::Google(
                    client
                        .with_thinking(mapped.as_deref().or(selected))
                        .with_disabled_thinking_fallback(fallback.as_deref())
                        .with_reasoning_support(metadata.as_ref().and_then(|model| model.reasoning))
                        .with_thinking_budget_settings(
                            args.settings.value().get("thinkingBudgets").cloned(),
                        ),
                )
            }
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
                let (supports_long, explicit_cache) = args
                    .model_profiles
                    .responses_cache_support(&args.provider, metadata.as_ref());
                let supports_developer = args
                    .model_profiles
                    .chat_compatibility(&args.provider, metadata.as_ref())
                    .supports_developer_role();
                let use_developer = metadata.as_ref().and_then(|model| model.reasoning)
                    == Some(true)
                    && supports_developer != Some(false);
                let client = client
                    .with_developer_role(use_developer)
                    .with_cache_retention(cache_retention(supports_long), explicit_cache)
                    .with_strict_tool_support(
                        args.model_profiles
                            .chat_compatibility(&args.provider, metadata.as_ref())
                            .supports_strict_mode()
                            .unwrap_or(false),
                    )
                    .with_prompt_cache_key_enabled(
                        args.model_profiles
                            .responses_prompt_cache_key_enabled(&args.provider, metadata.as_ref()),
                    )
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
                let mut client = client
                    .with_model_name(
                        &args.model,
                        metadata.as_ref().and_then(|model| model.name.as_deref()),
                    )
                    .with_cache_retention(cache_retention(true))
                    .with_long_cache_support(long_cache_override)
                    .with_reasoning(level, mapped.flatten())?;
                if let Some(budgets) = args.settings.value().get("thinkingBudgets") {
                    client = client.with_thinking_budget_settings(budgets.clone());
                }
                Self::Bedrock(client)
            }
            Self::Deferred(mut client) => {
                client.options = Some((Box::new(args.clone()), reported));
                client.estimator = Arc::new(OnceLock::new());
                Self::Deferred(client)
            }
        })
    }

    pub async fn thinking_levels(
        &self,
        model: &str,
    ) -> Option<(Option<Vec<String>>, Option<bool>)> {
        match self {
            Self::Ollama(client) => {
                let details = client.model_details(&[model.into()]).await.remove(model)?;
                (details.thinking_levels.is_some() || details.reasoning.is_some())
                    .then_some((details.thinking_levels, details.reasoning))
            }
            Self::Anthropic(client) => client
                .model(model)
                .await
                .filter(|entry| entry.has_known_thinking())
                .map(|entry| (Some(entry.thinking_levels), entry.reasoning)),
            Self::Google(client) => client
                .model(model)
                .await
                .map(|entry| (Some(entry.thinking_levels), Some(entry.reasoning))),
            Self::PiMessages(client) => {
                let details = client.model_details(&[model.into()]).await.remove(model)?;
                (details.thinking_levels.is_some() || details.reasoning.is_some())
                    .then_some((details.thinking_levels, details.reasoning))
            }
            Self::OpenAi(_) => Some((Some(pi_rs::openai::thinking_levels(model)), None)),
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
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
        context: &pi_rs::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs::PreparedToolDispatch + 'a>>> {
        self.prepare_tool_dispatch_with_headers(
            request,
            declarations,
            context,
            &HeaderMap::new(),
            false,
        )
        .await
    }

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

    async fn stream_detailed_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs::DetailedModelStream> {
        self.stream_detailed_with_headers(request, &HeaderMap::new(), false, Some(external_id))
            .await
    }

    async fn stream_detailed_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs::DetailedModelStream> {
        self.stream_detailed_with_headers(request, &HeaderMap::new(), true, None)
            .await
    }

    async fn stream_in_session(
        &self,
        request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_in_session_with_identity(
            request_id,
            session_id,
            &session_id.to_string(),
            request,
        )
        .await
    }

    async fn stream_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_with_headers(request, &HeaderMap::new(), false, Some(external_id))
            .await
    }

    async fn stream_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_with_headers(request, &HeaderMap::new(), true, None)
            .await
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs::EnrichedModelStream> {
        self.stream_enriched_with_headers(request, &HeaderMap::new(), false, Some(external_id))
            .await
    }

    async fn stream_enriched_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs::EnrichedModelStream> {
        self.stream_enriched_with_headers(request, &HeaderMap::new(), true, None)
            .await
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
                self.stream_with_headers(request, &HeaderMap::new(), false, None)
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
    async fn prepared_responses_dispatch_preserves_capability_and_session_policy() {
        use clap::Parser;
        use serde_json::Value;
        use std::io::{Read, Write};
        for (deferred, summary, direct) in [false, true].into_iter().flat_map(|deferred| {
            [false, true].into_iter().flat_map(move |summary| {
                [false, true]
                    .into_iter()
                    .map(move |direct| (deferred, summary, direct))
            })
        }) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(10))
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                    if let Some(position) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break position + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + length {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                let body: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                (headers, body)
            });
            let endpoint = format!("http://{address}/responses");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
                "api":"openai-responses","baseUrl":endpoint,"compat":{"supportsStrictMode":true},"models":[{"id":"m"}]
            }}})).unwrap();
            args.provider = args.model_profiles.provider("test").unwrap();
            args.model = "m".into();
            let client = if deferred {
                Client::deferred(
                    args.provider.clone(),
                    Adapter::OpenAiResponses,
                    &endpoint,
                    Some(Credential::Literal("fixture".into())),
                    RequestHeaders::default(),
                )
            } else {
                Client::OpenAi(
                    pi_rs::openai::OpenAiResponses::new_api(&endpoint, "fixture".into()).unwrap(),
                )
            }
            .with_model_options(&args, false)
            .unwrap();
            let direct_client = client.clone();
            let model = super::super::image_policy::WorkbenchModel {
                client,
                block_images: true,
                supports_images: None,
                session_affinity: &["session-id"],
                price: None,
            };
            let spec = pi_rs::ToolSpec {
                name: "read".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            };
            let req = ModelRequest {
                model: "m".into(),
                messages: vec![pi_rs::Message::UserWithImages {
                    content: "image".into(),
                    images: vec![pi_rs::ImageContent {
                        data: "aA==".into(),
                        mime_type: "image/png".into(),
                    }],
                }],
                tools: vec![spec.clone()],
                max_output_tokens: 10,
            };
            let declaration = pi_rs::ToolDeclaration {
                spec,
                constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                    strict: pi_rs::StrictToolPolicy::Require,
                }),
            };
            let context = pi_rs::ToolDispatchContext {
                request_id: uuid::Uuid::new_v4(),
                session_id: uuid::Uuid::new_v4(),
                external_id: "session-fixture".into(),
                summary,
            };
            let selected = if direct {
                None
            } else {
                model.prepare_context(&req).await.unwrap()
            };
            let selected: &dyn Model = selected.as_deref().unwrap_or(&direct_client);
            let prepared = selected
                .prepare_tool_dispatch(&req, std::slice::from_ref(&declaration), &context)
                .await
                .unwrap()
                .unwrap();
            assert!(
                prepared
                    .decision_snapshot()
                    .validate(
                        &context,
                        &req,
                        prepared.dispatch_request(),
                        std::slice::from_ref(&declaration)
                    )
                    .is_ok()
            );
            assert_eq!(
                prepared.decision_snapshot().request_sha256()
                    != prepared.decision_snapshot().dispatch_sha256(),
                !direct
            );
            assert!(matches!(
                prepared.effective_tools()[0].sampling,
                pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
            ));
            let _ = prepared.dispatch().await;
            let (headers, body) = server.join().unwrap();
            assert_eq!(body["tools"][0]["strict"], true);
            assert_eq!(
                body.get("prompt_cache_key").and_then(Value::as_str),
                (!summary).then_some("session-fixture")
            );
            assert_eq!(
                headers
                    .lines()
                    .any(|line| line == "session-id: session-fixture"),
                !summary
            );
        }
    }

    #[tokio::test]
    async fn prepared_chat_dispatch_preserves_capability_cache_and_session_policy() {
        use clap::Parser;
        use serde_json::Value;
        use std::io::{Read, Write};
        for (deferred, summary, path) in [false, true].into_iter().flat_map(|deferred| {
            [false, true].into_iter().flat_map(move |summary| {
                [0, 1, 2]
                    .into_iter()
                    .map(move |path| (deferred, summary, path))
            })
        }) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(10))
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                    if let Some(position) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break position + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + length {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                let body: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                (headers, body)
            });
            let endpoint = format!("http://{address}/v1/chat/completions");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
                "api":"openai-completions","baseUrl":endpoint,"compat":{"supportsStrictMode":true,"promptCacheKeyMode":"enabled","cacheControlFormat":"anthropic","supportsLongCacheRetention":false},"models":[{"id":"m"}]
            }}})).unwrap();
            args.provider = args.model_profiles.provider("test").unwrap();
            args.model = "m".into();
            let client = if deferred {
                Client::deferred(
                    args.provider.clone(),
                    Adapter::ChatCompletions,
                    &endpoint,
                    Some(Credential::Literal("fixture".into())),
                    RequestHeaders::default(),
                )
            } else {
                Client::Ollama(
                    pi_rs::ollama::Ollama::new(&endpoint, Some("fixture".into()))
                        .unwrap()
                        .with_provider("test")
                        .with_native_probes(false),
                )
            }
            .with_model_options(&args, false)
            .unwrap();
            let direct_client = client.clone();
            let model = super::super::image_policy::WorkbenchModel {
                client,
                block_images: true,
                supports_images: None,
                session_affinity: &["session-id"],
                price: None,
            };
            let spec = pi_rs::ToolSpec {
                name: "read".into(),
                description: String::new(),
                parameters: json!({"type":"object", "properties":{"optional":{"type":"string"}}}),
            };
            let req = ModelRequest {
                model: "m".into(),
                messages: vec![pi_rs::Message::UserWithImages {
                    content: "image".into(),
                    images: vec![pi_rs::ImageContent {
                        data: "aA==".into(),
                        mime_type: "image/png".into(),
                    }],
                }],
                tools: vec![spec.clone()],
                max_output_tokens: 10,
            };
            let declaration = pi_rs::ToolDeclaration {
                spec,
                constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                    strict: pi_rs::StrictToolPolicy::Require,
                }),
            };
            let context = pi_rs::ToolDispatchContext {
                request_id: uuid::Uuid::new_v4(),
                session_id: uuid::Uuid::new_v4(),
                external_id: "session-fixture".into(),
                summary,
            };
            let selected = if path == 2 {
                model.prepare_context(&req).await.unwrap()
            } else {
                None
            };
            let selected: &dyn Model = match path {
                0 => &direct_client,
                1 => &model,
                _ => selected.as_deref().unwrap(),
            };
            let prepared = selected
                .prepare_tool_dispatch(&req, std::slice::from_ref(&declaration), &context)
                .await
                .unwrap()
                .unwrap();
            assert!(
                prepared
                    .decision_snapshot()
                    .validate(
                        &context,
                        &req,
                        prepared.dispatch_request(),
                        std::slice::from_ref(&declaration)
                    )
                    .is_ok()
            );
            assert_eq!(
                prepared.decision_snapshot().request_sha256()
                    != prepared.decision_snapshot().dispatch_sha256(),
                path != 0
            );
            assert!(matches!(
                prepared.effective_tools()[0].sampling,
                pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
            ));
            let expected_schema = prepared.reservation_request().tools[0].parameters.clone();
            let _ = prepared.dispatch().await;
            let (headers, body) = server.join().unwrap();
            assert_eq!(body["tools"][0]["function"]["strict"], true);
            assert_eq!(body["tools"][0]["function"]["parameters"], expected_schema);
            assert_eq!(body["tools"][0].get("cache_control").is_some(), !summary);
            assert!(body.get("prompt_cache_retention").is_none());
            assert_eq!(
                body.get("prompt_cache_key").and_then(Value::as_str),
                (!summary).then_some("session-fixture")
            );
            assert_eq!(
                headers
                    .lines()
                    .any(|line| line == "session-id: session-fixture"),
                !summary && path != 0
            );
        }
    }

    #[tokio::test]
    async fn deferred_legacy_preparation_does_not_resolve_credentials() {
        let client = Client::deferred(
            Provider::named("anthropic").unwrap(),
            Adapter::Anthropic,
            "https://api.anthropic.com/v1/messages",
            None,
            RequestHeaders::default(),
        );
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 1,
        };
        let context = pi_rs::ToolDispatchContext {
            request_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            external_id: "s".into(),
            summary: false,
        };
        assert!(
            client
                .prepare_tool_dispatch(&request, &[], &context)
                .await
                .unwrap()
                .is_none()
        );
        let model = super::super::image_policy::WorkbenchModel {
            client,
            block_images: false,
            supports_images: None,
            session_affinity: &["x-session-affinity"],
            price: None,
        };
        assert!(
            model
                .prepare_tool_dispatch(&request, &[], &context)
                .await
                .unwrap()
                .is_none()
        );
    }

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

    #[tokio::test]
    async fn bundled_chat_models_send_model_specific_strict_field() {
        use std::io::{Read, Write};
        for (provider, model_id, config, expected) in [
            (
                "huggingface",
                "deepseek-ai/DeepSeek-R1",
                serde_json::json!({}),
                Some(false),
            ),
            (
                "huggingface",
                "deepseek-ai/DeepSeek-R1",
                serde_json::json!({"providers":{"huggingface":{"compat":{"supportsStrictMode":false}}}}),
                None,
            ),
            (
                "openrouter",
                "~deepseek/deepseek-flash-latest",
                serde_json::json!({}),
                Some(false),
            ),
            (
                "together",
                "deepseek-ai/DeepSeek-V4-Pro-0813",
                serde_json::json!({}),
                None,
            ),
            (
                "together",
                "deepseek-ai/DeepSeek-V4-Pro-0813",
                serde_json::json!({"providers":{"together":{"compat":{"supportsStrictMode":true}}}}),
                Some(false),
            ),
            (
                "huggingface",
                "deepseek-ai/DeepSeek-R1",
                serde_json::json!({"providers":{"huggingface":{"compat":{"supportsStrictMode":true},"modelOverrides":{"deepseek-ai/DeepSeek-R1":{"compat":{"supportsStrictMode":false}}}}}}),
                None,
            ),
            (
                "huggingface",
                "custom",
                serde_json::json!({"providers":{"huggingface":{"compat":{"supportsStrictMode":true},"models":[{"id":"custom","compat":{"supportsStrictMode":false}}]}}}),
                None,
            ),
            (
                "together",
                "deepseek-ai/DeepSeek-V4-Pro-0813",
                serde_json::json!({"providers":{"together":{"compat":{"supportsStrictMode":false},"models":[{"id":"deepseek-ai/DeepSeek-V4-Pro-0813","compat":{"supportsStrictMode":true}}]}}}),
                Some(false),
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(e) => panic!("fixture accept failed: {e}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut block = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let size: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + size {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end..end + size]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                body
            });
            let endpoint = format!("http://{address}/v1/chat/completions");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&config).unwrap();
            args.provider = args.model_profiles.provider(provider).unwrap();
            args.model = model_id.into();
            let client = Client::Ollama(
                pi_rs::ollama::Ollama::new(&endpoint, Some("fixture".into()))
                    .unwrap()
                    .with_provider(provider)
                    .with_native_probes(false),
            )
            .with_model_options(&args, false)
            .unwrap();
            let _ = client
                .stream(ModelRequest {
                    model: model_id.into(),
                    messages: vec![],
                    tools: vec![pi_rs::ToolSpec {
                        name: "probe".into(),
                        description: "probe".into(),
                        parameters: serde_json::json!({"type":"object","properties":{}}),
                    }],
                    max_output_tokens: 10,
                })
                .await;
            let body = server.join().unwrap();
            assert_eq!(
                body["tools"][0]["function"]
                    .get("strict")
                    .and_then(serde_json::Value::as_bool),
                expected,
                "{provider}/{model_id}"
            );
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn google_thinking_budget_settings_follow_model_options() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::create_dir(workspace.path().join(".pi")).unwrap();
        std::fs::write(
            workspace.path().join(".pi/settings.json"),
            r#"{"thinkingBudgets":{"high":9000}}"#,
        )
        .unwrap();
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.settings = super::super::config::Settings::load(workspace.path(), true).unwrap();
        args.thinking = Some("high".into());
        let base =
            Client::Google(pi_rs::google::Google::new("https://example.com", "key").unwrap());
        let configured = base.clone().with_model_options(&args, false).unwrap();
        let expected = Client::Google(
            pi_rs::google::Google::new("https://example.com", "key")
                .unwrap()
                .with_thinking(Some("high"))
                .with_thinking_budget_settings(
                    args.settings.value().get("thinkingBudgets").cloned(),
                ),
        );
        assert_eq!(configured.context_identity(), expected.context_identity());
        args.settings = super::super::config::Settings::default();
        let reset = configured.with_model_options(&args, false).unwrap();
        let expected = Client::Google(
            pi_rs::google::Google::new("https://example.com", "key")
                .unwrap()
                .with_thinking(Some("high")),
        );
        assert_eq!(reset.context_identity(), expected.context_identity());
    }

    #[tokio::test]
    async fn chat_cache_key_reaches_resolved_and_deferred_client_sessions() {
        use std::io::{Read, Write};
        for (deferred, summary, path) in [false, true].into_iter().flat_map(|deferred| {
            [false, true].into_iter().flat_map(move |summary| {
                ["legacy", "enriched", "detailed"]
                    .into_iter()
                    .map(move |path| (deferred, summary, path))
            })
        }) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(e) => panic!("fixture accept failed: {e}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut block = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let size: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + size {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end..end + size]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                body
            });
            let endpoint = format!("http://{address}/v1/chat/completions");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({"providers":{"test":{
                "api":"openai-completions", "baseUrl":endpoint, "compat":{"promptCacheKeyMode":"enabled"}, "models":[{"id":"m"}]
            }}})).unwrap();
            args.provider = args.model_profiles.provider("test").unwrap();
            args.model = "m".into();
            let client = if deferred {
                Client::deferred(
                    args.provider.clone(),
                    Adapter::ChatCompletions,
                    &endpoint,
                    Some(Credential::Literal("fixture".into())),
                    RequestHeaders::default(),
                )
            } else {
                Client::Ollama(
                    pi_rs::ollama::Ollama::new(&endpoint, Some("fixture".into()))
                        .unwrap()
                        .with_provider("test")
                        .with_native_probes(false),
                )
            }
            .with_model_options(&args, false)
            .unwrap();
            let model: &dyn Model = &client;
            let request = ModelRequest {
                model: "m".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 10,
            };
            let request_id = uuid::Uuid::new_v4();
            let session_id = uuid::Uuid::new_v4();
            match (summary, path) {
                (true, "legacy") => {
                    let _ = model
                        .stream_for_summary(request_id, session_id, request)
                        .await;
                }
                (false, "legacy") => {
                    let _ = model
                        .stream_in_session_with_identity(
                            request_id,
                            session_id,
                            "imported-session",
                            request,
                        )
                        .await;
                }
                (true, "enriched") => {
                    let _ = model
                        .stream_enriched_for_summary(request_id, session_id, request)
                        .await;
                }
                (false, "enriched") => {
                    let _ = model
                        .stream_enriched_in_session_with_identity(
                            request_id,
                            session_id,
                            "imported-session",
                            request,
                        )
                        .await;
                }
                (true, "detailed") => {
                    let _ = model
                        .stream_detailed_for_summary(request_id, session_id, request)
                        .await;
                }
                (false, "detailed") => {
                    let _ = model
                        .stream_detailed_in_session_with_identity(
                            request_id,
                            session_id,
                            "imported-session",
                            request,
                        )
                        .await;
                }
                _ => unreachable!(),
            }
            let body = server.join().unwrap();
            assert_eq!(
                body.get("prompt_cache_key")
                    .and_then(serde_json::Value::as_str),
                (!summary).then_some("imported-session")
            );
        }
    }

    #[tokio::test]
    async fn responses_instruction_role_follows_model_capabilities() {
        use std::io::{Read, Write};
        for (models, overrides, provider_compat, expected) in [
            (
                serde_json::json!([{"id":"m","reasoning":true}]),
                serde_json::json!({}),
                serde_json::json!({}),
                "developer",
            ),
            (
                serde_json::json!([{"id":"m","reasoning":false}]),
                serde_json::json!({}),
                serde_json::json!({}),
                "system",
            ),
            (
                serde_json::json!([{"id":"m","reasoning":true}]),
                serde_json::json!({}),
                serde_json::json!({"supportsDeveloperRole":false}),
                "system",
            ),
            (
                serde_json::json!([{"id":"m","reasoning":true,"compat":{"supportsDeveloperRole":true}}]),
                serde_json::json!({}),
                serde_json::json!({"supportsDeveloperRole":false}),
                "developer",
            ),
            (
                serde_json::json!([]),
                serde_json::json!({}),
                serde_json::json!({}),
                "system",
            ),
            (
                serde_json::json!([]),
                serde_json::json!({"m":{"name":"partial"}}),
                serde_json::json!({}),
                "system",
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(e) => panic!("fixture accept failed: {e}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut block = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let size: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + size {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end..end + size]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                body
            });
            let endpoint = format!("http://{address}/responses");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(
                &serde_json::json!({"providers":{"test":{
                    "api":"openai-responses", "baseUrl":endpoint, "compat":provider_compat,
                    "models":models, "modelOverrides":overrides
                }}}),
            )
            .unwrap();
            args.provider = args.model_profiles.provider("test").unwrap();
            args.model = "m".into();
            let client = Client::OpenAi(
                pi_rs::openai::OpenAiResponses::new_api_provider(
                    &endpoint,
                    "fixture".into(),
                    "test",
                )
                .unwrap(),
            )
            .with_model_options(&args, false)
            .unwrap();
            let _ = client
                .stream(ModelRequest {
                    model: "m".into(),
                    messages: vec![pi_rs::Message::System {
                        content: "rules".into(),
                    }],
                    tools: vec![],
                    max_output_tokens: 10,
                })
                .await;
            let body = server.join().unwrap();
            assert_eq!(body["input"][0]["role"], expected);
        }
    }

    #[tokio::test]
    async fn responses_cache_key_modes_reach_workbench_requests() {
        use std::io::{Read, Write};
        for (mode, expected) in [("disabled", None), ("enabled", Some("external-session"))] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let endpoint = format!("http://{address}/responses");
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut block = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let size: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + size {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end..end + size]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                body
            });
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({"providers":{"test":{
                "api":"openai-responses", "baseUrl":endpoint, "compat":{"promptCacheKeyMode":"disabled"},
                "models":[{"id":"m", "compat":{"promptCacheKeyMode":mode}}]
            }}})).unwrap();
            args.provider = args.model_profiles.provider("test").unwrap();
            args.model = "m".into();
            let client = Client::OpenAi(
                pi_rs::openai::OpenAiResponses::new_api_provider(
                    &endpoint,
                    "fixture".into(),
                    "test",
                )
                .unwrap(),
            )
            .with_model_options(&args, false)
            .unwrap();
            let model = super::super::image_policy::WorkbenchModel {
                client,
                block_images: false,
                supports_images: None,
                session_affinity: &[],
                price: None,
            };
            let _ = model
                .stream_enriched_in_session_with_identity(
                    uuid::Uuid::new_v4(),
                    uuid::Uuid::new_v4(),
                    "external-session",
                    ModelRequest {
                        model: "m".into(),
                        messages: vec![],
                        tools: vec![],
                        max_output_tokens: 10,
                    },
                )
                .await;
            let body = server.join().unwrap();
            assert_eq!(
                body.get("prompt_cache_key").and_then(|v| v.as_str()),
                expected
            );
        }
    }

    #[tokio::test]
    async fn google_mapped_levels_select_the_resolved_budget_on_the_wire() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let workspace = tempfile::tempdir().unwrap();
        std::fs::create_dir(workspace.path().join(".pi")).unwrap();
        std::fs::write(
            workspace.path().join(".pi/settings.json"),
            r#"{"thinkingBudgets":{"high":9000,"low":1234}}"#,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let (end, length) = loop {
                let mut chunk = [0; 2048];
                let n = stream.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&chunk[..n]);
                if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    let end = position + 4;
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end, length);
                }
            };
            while request.len() < end + length {
                let mut chunk = [0; 2048];
                let n = stream.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&chunk[..n]);
            }
            let body: serde_json::Value =
                serde_json::from_slice(&request[end..end + length]).unwrap();
            let response = "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n";
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
            body
        });
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.provider = Provider::Google;
        args.model = "gemini-2.5-flash".into();
        args.thinking = Some("high".into());
        args.settings = super::super::config::Settings::load(workspace.path(), true).unwrap();
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"google":{"modelOverrides":{"gemini-2.5-flash":{"thinkingLevelMap":{"high":"LOW","max":"high"}}}}}})).unwrap();
        let metadata = args
            .model_profiles
            .model(&args.provider, &args.model)
            .unwrap();
        assert!(
            metadata
                .levels(Adapter::Google)
                .unwrap()
                .contains(&"max".to_owned())
        );
        let base = Client::Google(
            pi_rs::google::Google::new(&endpoint, "fixture")
                .unwrap()
                .with_context_probe(false),
        );
        let mapped = base.clone().with_model_options(&args, false).unwrap();
        let expected = Client::Google(
            pi_rs::google::Google::new(&endpoint, "fixture")
                .unwrap()
                .with_context_probe(false)
                .with_thinking(Some("low"))
                .with_reasoning_support(Some(true))
                .with_thinking_budget_settings(Some(json!({"high":9000,"low":1234}))),
        );
        assert_eq!(mapped.context_identity(), expected.context_identity());
        let request = ModelRequest {
            model: args.model.clone(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 256,
        };
        let _stream = mapped.stream(request).await.unwrap();
        let body = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            body["generationConfig"]["thinkingConfig"],
            json!({"thinkingBudget":1234,"includeThoughts":true})
        );
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"google":{"modelOverrides":{"gemini-2.5-flash":{"thinkingLevelMap":{"high":"extreme"}}}}}})).unwrap();
        assert!(base.with_model_options(&args, false).is_err());
    }

    #[tokio::test]
    async fn google_discrete_mappings_and_disabled_fallback_reach_the_wire() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (selected, profile, expected) in [
            (
                Some("high"),
                1,
                json!({"thinkingLevel":"MINIMAL","includeThoughts":true}),
            ),
            (None, 1, json!({"thinkingLevel":"HIGH"})),
            (None, 0, json!({"thinkingLevel":"LOW"})),
            (None, 3, json!({"thinkingLevel":"LOW"})),
            (None, 2, json!({"thinkingBudget":0})),
            (
                Some("high"),
                2,
                json!({"thinkingBudget":-1,"includeThoughts":true}),
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (end, size) = loop {
                    let mut chunk = [0; 2048];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let end = pos + 4;
                        let size = std::str::from_utf8(&bytes[..end])
                            .unwrap()
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end, size);
                    }
                };
                while bytes.len() < end + size {
                    let mut chunk = [0; 2048];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end..end + size]).unwrap();
                let response = "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n";
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
                body
            });
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.provider = Provider::Google;
            args.model = match profile {
                2 => "custom-reasoner",
                3 => "gemini-4-argon",
                _ => "gemini-3.1-pro-preview",
            }
            .into();
            args.thinking = selected.map(str::to_owned);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&if profile == 1 {
                json!({"providers":{"google":{"modelOverrides":{"gemini-3.1-pro-preview":{"thinkingLevelMap":{"off":null,"minimal":null,"low":"HIGH","high":"MINIMAL"}}}}}})
            } else if profile == 2 {
                json!({"providers":{"google":{"models":[{"id":"custom-reasoner","reasoning":true,"thinkingLevelMap":{"high":"low"}}]}}})
            } else { json!({}) }).unwrap();
            if profile == 0 || profile == 3 {
                let model = args
                    .model_profiles
                    .model(&args.provider, &args.model)
                    .unwrap();
                assert_eq!(
                    model.levels(Adapter::Google).unwrap(),
                    vec!["low", "medium", "high"]
                );
            }
            let client = Client::Google(
                pi_rs::google::Google::new(&endpoint, "fixture")
                    .unwrap()
                    .with_context_probe(false),
            )
            .with_model_options(&args, false)
            .unwrap();
            let _stream = client
                .stream(ModelRequest {
                    model: args.model.clone(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 256,
                })
                .await
                .unwrap();
            let body = tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(body["generationConfig"]["thinkingConfig"], expected);
        }
    }

    #[test]
    fn google_discrete_mappings_and_disabled_fallback_propagate() {
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.provider = Provider::Google;
        args.model = "gemini-3.1-pro-preview".into();
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"google":{"modelOverrides":{"gemini-3.1-pro-preview":{"thinkingLevelMap":{"off":null,"minimal":null,"low":"HIGH","high":"MINIMAL"}}}}}})).unwrap();
        let base =
            Client::Google(pi_rs::google::Google::new("https://example.com", "key").unwrap());
        args.thinking = Some("high".into());
        let selected = base.clone().with_model_options(&args, false).unwrap();
        let expected = Client::Google(
            pi_rs::google::Google::new("https://example.com", "key")
                .unwrap()
                .with_thinking(Some("minimal"))
                .with_reasoning_support(Some(true)),
        );
        assert_eq!(selected.context_identity(), expected.context_identity());
        args.thinking = None;
        let disabled = base.clone().with_model_options(&args, false).unwrap();
        let expected = Client::Google(
            pi_rs::google::Google::new("https://example.com", "key")
                .unwrap()
                .with_disabled_thinking_fallback(Some("high"))
                .with_reasoning_support(Some(true)),
        );
        assert_eq!(disabled.context_identity(), expected.context_identity());
        args.thinking = Some("off".into());
        let clamped = base.with_model_options(&args, false).unwrap();
        let expected = Client::Google(
            pi_rs::google::Google::new("https://example.com", "key")
                .unwrap()
                .with_thinking(Some("high"))
                .with_reasoning_support(Some(true)),
        );
        assert_eq!(clamped.context_identity(), expected.context_identity());
    }

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
                .stream_with_headers(request(&args.model), &HeaderMap::new(), true, None)
                .await
                .unwrap();
            args.model = "anthropic.claude-opus-4-8".into();
            args.thinking = Some("xhigh".into());
            let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                .with_model_options(&args, false)
                .unwrap();
            let _stream = client
                .stream_with_headers(request(&args.model), &HeaderMap::new(), false, None)
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
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), false, None)
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
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), false, None)
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
                .stream_with_headers(request(&args.model), &HeaderMap::new(), false, None)
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
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), false, None)
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
                    .stream_with_headers(request(&args.model), &HeaderMap::new(), summary, None)
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
                        .stream_with_headers(request(&args.model), &HeaderMap::new(), summary, None)
                        .await
                        .unwrap();
                }
            }
            let workspace = tempfile::tempdir().unwrap();
            std::fs::create_dir(workspace.path().join(".pi")).unwrap();
            std::fs::write(
                workspace.path().join(".pi/settings.json"),
                r#"{"thinkingBudgets":{"high":3456,"low":"invalid"}}"#,
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
                .stream_with_headers(req, &HeaderMap::new(), false, None)
                .await
                .unwrap();
            args.thinking = Some("low".into());
            let client = Client::Bedrock(pi_rs::bedrock::Bedrock::from_env())
                .with_model_options(&args, false)
                .unwrap();
            let mut req = request(&args.model);
            req.max_output_tokens = 8192;
            let error = match client
                .stream_with_headers(req, &HeaderMap::new(), false, None)
                .await
            {
                Ok(_) => panic!("invalid selected budget accepted"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("thinkingBudgets.low"));
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
            assert_eq!(
                requests[normal]["additionalModelRequestFields"]["thinking"]["type"],
                "adaptive"
            );
            assert_eq!(
                requests[normal]["additionalModelRequestFields"]["thinking"]["display"],
                "summarized"
            );
            assert!(
                requests[normal]["additionalModelRequestFields"]
                    .get("output_config")
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
