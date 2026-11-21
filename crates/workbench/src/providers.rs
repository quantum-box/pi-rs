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
    MistralConversations,
    OpenAiResponses,
    CodexResponses,
    Anthropic,
    Google,
    Bedrock,
    PiMessages,
}

impl Adapter {
    pub fn named(api: &str) -> Option<Self> {
        match api {
            "openai-completions" => Some(Self::ChatCompletions),
            "mistral-conversations" => Some(Self::MistralConversations),
            "openai-responses" => Some(Self::OpenAiResponses),
            "openai-codex-responses" => Some(Self::CodexResponses),
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
            Self::MistralConversations => "mistral-conversations",
            Self::OpenAiResponses => "openai-responses",
            Self::CodexResponses => "openai-codex-responses",
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
            Adapter::CodexResponses => {
                let token = token.ok_or_else(|| {
                    Error::Config("no Pi Codex OAuth credential for this endpoint".into())
                })?;
                Ok(Client::OpenAi(pi_rs::openai::OpenAiResponses::new_codex(
                    endpoint,
                    token,
                    self.id(),
                )?))
            }
            Adapter::OpenAiResponses => {
                let token = token.ok_or_else(missing_key)?;
                Ok(Client::OpenAi(if self.id() == "openai-chatgpt" {
                    pi_rs::openai::OpenAiResponses::new(endpoint, token)?
                } else {
                    pi_rs::openai::OpenAiResponses::new_api_provider(endpoint, token, self.id())?
                }))
            }
            Adapter::MistralConversations => Ok(Client::Ollama(
                Ollama::new(endpoint, Some(token.ok_or_else(missing_key)?))?
                    .with_provider(self.id())
                    .with_mistral(false, false, None),
            )),
            Adapter::ChatCompletions => {
                if self.id() == "cloudflare-ai-gateway" && endpoint.is_empty() {
                    return Err(Error::Config(
                        "set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_GATEWAY_ID for cloudflare-ai-gateway".into(),
                    ));
                }
                if self.id() == "cloudflare-workers-ai" && endpoint.is_empty() {
                    return Err(Error::Config(
                        "set CLOUDFLARE_ACCOUNT_ID for cloudflare-workers-ai".into(),
                    ));
                }
                if token.is_none()
                    && let Some(builtin) = super::builtin_chat::find(self.id())
                    && (builtin.endpoint() == endpoint
                        || (self.id() == "cloudflare-ai-gateway"
                            && super::cloudflare_gateway::Route::is_endpoint(endpoint))
                        || (self.id() == "cloudflare-workers-ai"
                            && super::model_profiles::cloudflare_workers_chat_account(endpoint)
                                .is_some()))
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
                let mut client = if self.id() == "cloudflare-ai-gateway" {
                    Anthropic::with_cloudflare_gateway_token(endpoint, &token)?
                } else if self == &Self::Anthropic && token.contains("sk-ant-oat") {
                    Anthropic::with_oauth_token(endpoint, &token)?
                } else if bearer
                    || matches!(
                        self.id(),
                        "fireworks" | "openrouter" | "minimax" | "minimax-cn" | "github-copilot"
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
            Some(Credential::Codex) => {
                let settings = match &self.http_settings {
                    Some(settings) => settings.clone(),
                    None => super::config::Settings::load(&std::env::current_dir()?, false)?,
                };
                Some(super::codex_auth::access(auth_path, &settings, None, false).await?)
            }
            Some(Credential::Copilot) => {
                let settings = match &self.http_settings {
                    Some(settings) => settings.clone(),
                    None => super::config::Settings::load(&std::env::current_dir()?, false)?,
                };
                Some(super::copilot_auth::access_for_path(auth_path, &settings, None, false).await?)
            }
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
        let endpoint = if matches!(self.credential, Some(Credential::Copilot)) {
            super::copilot_auth::routed_endpoint(
                &self.endpoint,
                token.as_deref().expect("resolved Copilot access"),
            )?
        } else {
            self.endpoint.clone()
        };
        let mut client = self
            .provider
            .connect_prechecked(self.adapter, &endpoint, token, bearer, radius_checked)?
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
    pub(super) fn uses_final_strict_policy(&self) -> bool {
        match self {
            Self::OpenAi(_) | Self::Ollama(_) => true,
            Self::Deferred(client) => matches!(
                client.adapter,
                Adapter::OpenAiResponses
                    | Adapter::CodexResponses
                    | Adapter::ChatCompletions
                    | Adapter::MistralConversations
            ),
            _ => false,
        }
    }

    pub(super) async fn with_final_strict_support(self, supported: bool) -> Result<Self> {
        Ok(match self {
            Self::Ollama(client) => Self::Ollama(client.with_strict_tool_support(supported)),
            Self::OpenAi(client) => Self::OpenAi(client.with_strict_tool_support(supported)),
            Self::Deferred(client)
                if matches!(
                    client.adapter,
                    Adapter::OpenAiResponses
                        | Adapter::CodexResponses
                        | Adapter::ChatCompletions
                        | Adapter::MistralConversations
                ) =>
            {
                return Box::pin(client.resolve().await?.with_final_strict_support(supported))
                    .await;
            }
            other => other,
        })
    }

    pub fn supports_prepared_tool_dispatch(&self) -> bool {
        match self {
            Self::OpenAi(_) | Self::Ollama(_) | Self::Google(_) | Self::Anthropic(_) => true,
            Self::Deferred(client) => matches!(
                client.adapter,
                Adapter::OpenAiResponses
                    | Adapter::CodexResponses
                    | Adapter::ChatCompletions
                    | Adapter::MistralConversations
                    | Adapter::Google
                    | Adapter::Anthropic
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
            Self::Google(client) if !defaults.is_empty() => {
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
            Self::Anthropic(client) => {
                let client = if context.summary {
                    client.clone().without_cache()
                } else {
                    client.clone().with_default_headers(defaults)
                };
                Ok(Some(client.prepare_bound_tool_dispatch(
                    request,
                    declarations,
                    context,
                )?))
            }
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
            Self::Google(client) => Ok(Some(
                client
                    .clone()
                    .with_default_headers(defaults)
                    .prepare_bound_tool_dispatch(request, declarations, context)?,
            )),
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
                    Adapter::OpenAiResponses
                        | Adapter::CodexResponses
                        | Adapter::ChatCompletions
                        | Adapter::MistralConversations
                        | Adapter::Google
                        | Adapter::Anthropic
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
                let mistral = args
                    .model_profiles
                    .model_adapter(&args.provider, &args.model)
                    == Adapter::MistralConversations;
                let client = if mistral {
                    let thinking_enabled = thinking.is_some_and(|level| level != "off");
                    let reasoning_allowed = configured
                        .as_ref()
                        .is_some_and(|model| model.reasoning == Some(true));
                    let has_map = configured
                        .as_ref()
                        .is_some_and(|model| model.has_thinking_map());
                    let effort = configured
                        .as_ref()
                        .filter(|_| reasoning_allowed && has_map)
                        .and_then(|model| {
                            let level = thinking.unwrap_or("off");
                            model
                                .mapped_effort(level)
                                .flatten()
                                .or(thinking_enabled.then_some("high"))
                        });
                    let images = configured
                        .as_ref()
                        .and_then(|model| model.supports_images())
                        == Some(true);
                    client.with_mistral(
                        images,
                        reasoning_allowed && !has_map && thinking_enabled,
                        effort,
                    )
                } else {
                    client
                };
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
                    .with_prompt_cache_policy(key_enabled || mistral, long && !mistral);
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
                let default_mistral_strict =
                    mistral && compatibility.supports_strict_mode().is_none();
                let client = client.with_chat_compatibility(
                    compatibility,
                    super::thinking::adopted_reasoning(args),
                    thinking.is_some_and(|level| level != "off"),
                    configured
                        .as_ref()
                        .is_none_or(|model| model.mapped_effort("off") != Some(None)),
                );
                let client = if default_mistral_strict {
                    client.with_strict_tool_support(true)
                } else {
                    client
                };
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
                let strict = args
                    .model_profiles
                    .anthropic_strict_tool_support(&args.provider, metadata.as_ref());
                let mut client = client
                    .with_strict_tool_support(strict)
                    .with_strict_tool_models(args.model_profiles.anthropic_strict_tool_models(
                        &args.provider,
                        &args.model,
                        args.explicit_endpoint_for(&args.provider),
                    ))
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
                let (strict_default, strict_models) = args
                    .model_profiles
                    .google_strict_tool_policy(&args.provider);
                // The final model still dispatches on the selected client's route.
                let selected_endpoint = args.model_endpoint_for(&args.provider, Some(&args.model));
                let same_route = |id: &str| {
                    args.model_profiles.model_adapter(&args.provider, id) == Adapter::Google
                        && args.model_endpoint_for(&args.provider, Some(id)) == selected_endpoint
                };
                let unknown_route = args.provider.adapter() == Adapter::Google
                    && args.model_endpoint_for(&args.provider, None) == selected_endpoint;
                let configured_models: std::collections::BTreeSet<String> = args
                    .model_profiles
                    .model_ids(&args.provider)
                    .into_iter()
                    .collect();
                let mut route_models = configured_models.clone();
                route_models.extend(strict_models.keys().cloned());
                let mut route_names = route_models.clone();
                for id in &route_models {
                    let canonical = id.strip_prefix("models/").unwrap_or(id);
                    route_names.insert(canonical.to_owned());
                    route_names.insert(format!("models/{canonical}"));
                }
                let routes = route_names
                    .into_iter()
                    .map(|id| {
                        let canonical = id.strip_prefix("models/").unwrap_or(&id);
                        let alias = if id == canonical {
                            format!("models/{canonical}")
                        } else {
                            canonical.to_owned()
                        };
                        // A spelling without its own profile cannot bypass an explicit route
                        // for the same wire model.
                        let inherited_route_safe = configured_models.contains(&id)
                            || !configured_models.contains(&alias)
                            || same_route(&alias);
                        // The adapter falls back to a canonical model-specific override;
                        // that override must also belong to this route.
                        let inherited_override_safe = id == canonical
                            || strict_models.contains_key(&id)
                            || !strict_models.contains_key(canonical)
                            || same_route(canonical);
                        let allowed =
                            same_route(&id) && inherited_route_safe && inherited_override_safe;
                        (id, allowed)
                    })
                    .collect();
                Self::Google(
                    client
                        .with_strict_tool_policy(strict_default, strict_models)
                        .with_strict_tool_routes(unknown_route, routes)
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
                    .and_then(|model| {
                        model.levels(
                            args.model_profiles
                                .model_adapter(&args.provider, &args.model),
                        )
                    })
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
                let compat = args
                    .model_profiles
                    .chat_compatibility(&args.provider, metadata.as_ref());
                let use_developer = metadata.as_ref().and_then(|model| model.reasoning)
                    == Some(true)
                    && compat.supports_developer_role() != Some(false);
                let client = if args
                    .model_profiles
                    .model_adapter(&args.provider, &args.model)
                    == Adapter::CodexResponses
                {
                    let off = metadata.as_ref().map_or(Some("none"), |model| {
                        if model.reasoning == Some(false) {
                            None
                        } else {
                            model.mapped_effort("off").unwrap_or(Some("none"))
                        }
                    });
                    client.with_codex_off_effort(off)
                } else {
                    client
                };
                let (system_default, system_models) = args
                    .model_profiles
                    .responses_mid_conversation_system_policy(
                        &args.provider,
                        &args.model,
                        args.explicit_endpoint_for(&args.provider),
                    );
                let client = client
                    .with_mid_conversation_system_policy(system_default, system_models)
                    .with_max_output_tokens_field(
                        args.model_profiles
                            .responses_max_output_tokens(&args.provider, metadata.as_ref()),
                    )
                    .with_developer_role(use_developer)
                    .with_cache_retention(cache_retention(supports_long), explicit_cache)
                    .with_strict_tool_support(
                        args.model_profiles
                            .responses_strict_tool_support(&args.provider, metadata.as_ref()),
                    )
                    .with_grammar_tool_support(
                        compat.supports_openai_grammar_tools().unwrap_or(false),
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

/// Route declaration-aware streams through the same request-bound decisions as
/// prepared dispatch, retaining the legacy fallback for unsupported adapters.
pub(super) async fn stream_with_declarations(
    model: &dyn Model,
    request: ModelRequest,
    declarations: &[pi_rs::ToolDeclaration],
    context: pi_rs::ToolDispatchContext,
) -> Result<pi_rs::DetailedModelStream> {
    if let Some(prepared) = model
        .prepare_tool_dispatch(&request, declarations, &context)
        .await?
    {
        pi_rs::validate_prepared_tool_dispatch(
            prepared.as_ref(),
            &context,
            &request,
            declarations,
        )?;
        if prepared.effective_tools().iter().any(|tool| {
            matches!(
                tool.sampling,
                pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
            )
        }) {
            return Err(Error::Config("strict tool sampling requires prepared dispatch so its effective decisions can be retained".into()));
        }
        prepared.dispatch().await
    } else if context.summary {
        pi_rs::validate_legacy_tool_declarations(&request, declarations)?;
        model
            .stream_detailed_for_summary(context.request_id, context.session_id, request.clone())
            .await
    } else {
        pi_rs::validate_legacy_tool_declarations(&request, declarations)?;
        model
            .stream_detailed_in_session_with_identity(
                context.request_id,
                context.session_id,
                &context.external_id,
                request.clone(),
            )
            .await
    }
}

#[async_trait::async_trait]
impl Model for Client {
    fn minimum_output_tokens(&self) -> u32 {
        match self {
            Self::OpenAi(client) => client.minimum_output_tokens(),
            _ => 1,
        }
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
    ) -> Result<pi_rs::DetailedModelStream> {
        stream_with_declarations(
            self,
            request,
            declarations,
            pi_rs::ToolDispatchContext {
                request_id,
                session_id,
                external_id: external_id.into(),
                summary: false,
            },
        )
        .await
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        request: ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
    ) -> Result<pi_rs::DetailedModelStream> {
        stream_with_declarations(
            self,
            request,
            declarations,
            pi_rs::ToolDispatchContext {
                request_id,
                session_id,
                external_id: session_id.to_string(),
                summary: true,
            },
        )
        .await
    }

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

    #[test]
    fn final_strict_policy_only_applies_to_openai_adapters() {
        let endpoint = "http://127.0.0.1:1234/v1";
        for (adapter, expected) in [
            (Adapter::OpenAiResponses, true),
            (Adapter::ChatCompletions, true),
            (Adapter::Anthropic, false),
            (Adapter::Google, false),
            (Adapter::Bedrock, false),
            (Adapter::PiMessages, false),
        ] {
            let provider = Provider::Custom {
                id: Arc::from("fixture"),
                adapter,
            };
            let client =
                Client::deferred(provider, adapter, endpoint, None, RequestHeaders::default());
            assert_eq!(client.uses_final_strict_policy(), expected, "{adapter:?}");
        }
        assert!(
            !Client::Anthropic(
                Anthropic::new("http://127.0.0.1:1234/v1/messages", "fixture").unwrap()
            )
            .uses_final_strict_policy()
        );
        assert!(
            !Client::Google(pi_rs::google::Google::new(endpoint, "fixture").unwrap())
                .uses_final_strict_policy()
        );
        assert!(
            Client::Ollama(Ollama::new(endpoint, Some("fixture".into())).unwrap())
                .uses_final_strict_policy()
        );
        assert!(
            Client::OpenAi(
                pi_rs::openai::OpenAiResponses::new_api(endpoint, "fixture".into()).unwrap()
            )
            .uses_final_strict_policy()
        );
    }

    #[tokio::test]
    async fn responses_grammar_capability_follows_the_selected_model() {
        use clap::Parser;

        let endpoint = "http://localhost:9/responses";
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers":{"fixture":{
                "api":"openai-responses","baseUrl":endpoint,
                "compat":{"supportsOpenAIGrammarTools":true},
                "models":[{"id":"grammar"},{"id":"function","compat":{"supportsOpenAIGrammarTools":false}}]
            }}
        })).unwrap();
        args.provider = args.model_profiles.provider("fixture").unwrap();
        let declaration = pi_rs::ToolDeclaration {
            spec: pi_rs::ToolSpec {
                name: "write".into(),
                description: String::new(),
                parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            },
            constrained_sampling: Some(pi_rs::ToolConstraint::Grammar {
                variants: pi_rs::ToolGrammarVariants {
                    openai_regex: Some(".+".into()),
                    ..Default::default()
                },
            }),
        };
        let context = pi_rs::ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "fixture".into(),
            summary: false,
        };
        for (model, support) in [("grammar", true), ("function", false)] {
            args.model = model.into();
            let client = Client::OpenAi(
                pi_rs::openai::OpenAiResponses::new_api(endpoint, "key".into()).unwrap(),
            )
            .with_model_options(&args, false)
            .unwrap();
            let request = ModelRequest {
                model: model.into(),
                messages: vec![],
                tools: vec![declaration.spec.clone()],
                max_output_tokens: 32,
            };
            let prepared = client
                .prepare_tool_dispatch(&request, std::slice::from_ref(&declaration), &context)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                matches!(
                    prepared.effective_tools()[0].sampling,
                    pi_rs::EffectiveToolSampling::OpenAiGrammar { .. }
                ),
                support
            );
        }
    }

    #[tokio::test]
    async fn anthropic_rewrite_uses_explicit_endpoint_for_both_models() {
        use clap::Parser;
        let endpoint = "http://127.0.0.1:1234/v1/messages";
        for deferred in [false, true] {
            for (selected, rewritten, strict) in [("a", "b", false), ("b", "a", true)] {
                let mut args = super::super::Cli::parse_from(["pi"]);
                args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
                    "api":"anthropic-messages", "baseUrl":"http://127.0.0.1:1235/v1/messages",
                    "models":[
                        {"id":"a", "baseUrl":"http://127.0.0.1:1236/v1/messages", "compat":{"supportsStrictTools":true}},
                        {"id":"b", "baseUrl":"http://127.0.0.1:1237/v1/messages", "compat":{"supportsStrictTools":false}}
                    ]
                }}})).unwrap();
                args.provider = args.model_profiles.provider("test").unwrap();
                args.model = selected.into();
                args.endpoint = endpoint.into();
                args.endpoint_explicit = true;
                args.initial_connection = Some((args.provider.clone(), endpoint.into()));
                let client = if deferred {
                    Client::deferred(
                        args.provider.clone(),
                        Adapter::Anthropic,
                        endpoint,
                        Some(Credential::Literal("fixture".into())),
                        RequestHeaders::default(),
                    )
                } else {
                    Client::Anthropic(Anthropic::new(endpoint, "fixture").unwrap())
                }
                .with_context_probes(false)
                .with_model_options(&args, false)
                .unwrap();
                let spec = pi_rs::ToolSpec {
                    name: "t".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                };
                let request = ModelRequest {
                    model: rewritten.into(),
                    messages: vec![],
                    tools: vec![spec.clone()],
                    max_output_tokens: 1,
                };
                let declaration = pi_rs::ToolDeclaration {
                    spec,
                    constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                        strict: pi_rs::StrictToolPolicy::Prefer,
                    }),
                };
                let context = pi_rs::ToolDispatchContext {
                    request_id: uuid::Uuid::new_v4(),
                    session_id: uuid::Uuid::new_v4(),
                    external_id: "s".into(),
                    summary: false,
                };
                let prepared = client
                    .prepare_tool_dispatch(&request, &[declaration], &context)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    matches!(
                        prepared.effective_tools()[0].sampling,
                        pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                    ),
                    strict,
                    "deferred={deferred}, {selected} -> {rewritten}"
                );
            }
        }
    }

    #[test]
    fn anthropic_disabled_capability_preserves_workbench_context_fingerprint() {
        use clap::Parser;
        let endpoint = "http://127.0.0.1:1234/v1/messages";
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
            "api":"anthropic-messages","baseUrl":endpoint,"compat":{"supportsStrictTools":false},"models":[{"id":"known"}]
        }}})).unwrap();
        args.provider = args.model_profiles.provider("test").unwrap();
        args.model = "selected-unknown".into();
        let base = Anthropic::new(endpoint, "fixture")
            .unwrap()
            .with_context_probe(false);
        let legacy = Client::Anthropic(
            base.clone()
                .with_thinking(args.provider_thinking())
                .with_thinking_budget_settings(
                    args.settings.value().get("thinkingBudgets").cloned(),
                )
                .with_cache_retention(cache_retention(true), true),
        );
        let current = Client::Anthropic(base)
            .with_model_options(&args, false)
            .unwrap();
        assert!(legacy.context_identity().is_some());
        assert_eq!(current.context_identity(), legacy.context_identity());
        let wrap = |client| super::super::image_policy::WorkbenchModel {
            strict_policy: None,
            client,
            block_images: false,
            supports_images: None,
            session_affinity: &[],
            price: None,
        };
        assert_eq!(
            wrap(current).context_identity(),
            wrap(legacy).context_identity()
        );
    }

    #[tokio::test]
    async fn prepared_anthropic_selected_unknown_uses_provider_default() {
        use clap::Parser;
        for (setting, supported) in [(None, false), (Some(false), false), (Some(true), true)] {
            for deferred in [false, true] {
                let endpoint = "http://127.0.0.1:1234/v1/messages";
                let mut args = super::super::Cli::parse_from(["pi"]);
                args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
                    "api":"anthropic-messages", "baseUrl":endpoint, "compat":{"supportsStrictTools":setting}, "models":[{"id":"catalogued"}],
                    "modelOverrides":{"another-unknown":{"name":"partial override"}}
                }}})).unwrap();
                args.provider = args.model_profiles.provider("test").unwrap();
                args.model = "selected-unknown".into();
                let client = if deferred {
                    Client::deferred(
                        args.provider.clone(),
                        Adapter::Anthropic,
                        endpoint,
                        Some(Credential::Literal("fixture".into())),
                        RequestHeaders::default(),
                    )
                } else {
                    Client::Anthropic(Anthropic::new(endpoint, "fixture").unwrap())
                };
                let client = client
                    .with_context_probes(false)
                    .with_model_options(&args, false)
                    .unwrap();
                let spec = pi_rs::ToolSpec {
                    name: "lookup".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                };
                let mut request = ModelRequest {
                    model: args.model.clone(),
                    messages: vec![pi_rs::Message::User {
                        content: "hi".into(),
                    }],
                    tools: vec![spec.clone()],
                    max_output_tokens: 16,
                };
                let context = pi_rs::ToolDispatchContext {
                    request_id: uuid::Uuid::nil(),
                    session_id: uuid::Uuid::nil(),
                    external_id: "s".into(),
                    summary: false,
                };
                for policy in [
                    pi_rs::StrictToolPolicy::Prefer,
                    pi_rs::StrictToolPolicy::Require,
                ] {
                    let declaration = pi_rs::ToolDeclaration {
                        spec: spec.clone(),
                        constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                            strict: policy,
                        }),
                    };
                    request.model = "selected-unknown".into();
                    let result = client
                        .prepare_tool_dispatch(
                            &request,
                            std::slice::from_ref(&declaration),
                            &context,
                        )
                        .await;
                    if !supported && policy == pi_rs::StrictToolPolicy::Require {
                        assert!(result.is_err());
                    } else {
                        let prepared = result.unwrap().unwrap();
                        pi_rs::validate_prepared_tool_dispatch(
                            prepared.as_ref(),
                            &context,
                            &request,
                            std::slice::from_ref(&declaration),
                        )
                        .unwrap();
                        assert_eq!(
                            matches!(
                                prepared.effective_tools()[0].sampling,
                                pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                            ),
                            supported
                        );
                    }
                    request.model = "another-unknown".into();
                    assert!(
                        client
                            .prepare_tool_dispatch(&request, &[declaration], &context)
                            .await
                            .err()
                            .unwrap()
                            .to_string()
                            .contains("cannot be resolved")
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn prepared_anthropic_resolves_model_rewrites_in_both_directions() {
        use clap::Parser;
        let endpoint = "http://127.0.0.1:1234/v1/messages";
        for (configured, rewritten, supported) in
            [("strict", "loose", true), ("loose", "strict", false)]
        {
            for deferred in [false, true] {
                let mut args = super::super::Cli::parse_from(["pi"]);
                args.model_profiles =
                    super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
                        "api":"anthropic-messages", "baseUrl":endpoint, "models":[
                            {"id":"strict","compat":{"supportsStrictTools":true}},
                            {"id":"loose","compat":{"supportsStrictTools":false}},
                            {"id":"other-route","baseUrl":"http://127.0.0.1:1235/v1/messages","compat":{"supportsStrictTools":true}}]
                    }}}))
                    .unwrap();
                args.provider = args.model_profiles.provider("test").unwrap();
                args.model = configured.into();
                let client = if deferred {
                    Client::deferred(
                        args.provider.clone(),
                        Adapter::Anthropic,
                        endpoint,
                        Some(Credential::Literal("fixture".into())),
                        RequestHeaders::default(),
                    )
                } else {
                    Client::Anthropic(Anthropic::new(endpoint, "fixture").unwrap())
                }
                .with_context_probes(false)
                .with_model_options(&args, false)
                .unwrap();
                let model = super::super::image_policy::WorkbenchModel {
                    strict_policy: None,
                    client,
                    block_images: false,
                    supports_images: None,
                    session_affinity: &[],
                    price: None,
                };
                let spec = pi_rs::ToolSpec {
                    name: "lookup".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                };
                let mut request = ModelRequest {
                    model: configured.into(),
                    messages: vec![pi_rs::Message::User {
                        content: "hi".into(),
                    }],
                    tools: vec![spec.clone()],
                    max_output_tokens: 16,
                };
                for summary in [false, true] {
                    let context = pi_rs::ToolDispatchContext {
                        request_id: uuid::Uuid::nil(),
                        session_id: uuid::Uuid::nil(),
                        external_id: "s".into(),
                        summary,
                    };
                    for policy in [
                        pi_rs::StrictToolPolicy::Prefer,
                        pi_rs::StrictToolPolicy::Require,
                    ] {
                        let declaration = pi_rs::ToolDeclaration {
                            spec: spec.clone(),
                            constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                                strict: policy,
                            }),
                        };
                        request.model = configured.into();
                        let original = model
                            .prepare_tool_dispatch(
                                &request,
                                std::slice::from_ref(&declaration),
                                &context,
                            )
                            .await;
                        if !supported && policy == pi_rs::StrictToolPolicy::Require {
                            assert!(original.is_err());
                        } else {
                            let prepared = original.unwrap().unwrap();
                            assert_eq!(
                                matches!(
                                    prepared.effective_tools()[0].sampling,
                                    pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                                ),
                                supported
                            );
                        }
                        // The final model determines the frozen sampling decision.
                        request.model = rewritten.into();
                        let rewritten_result = model
                            .prepare_tool_dispatch(
                                &request,
                                std::slice::from_ref(&declaration),
                                &context,
                            )
                            .await;
                        if supported && policy == pi_rs::StrictToolPolicy::Require {
                            assert!(rewritten_result.is_err());
                        } else {
                            let prepared = rewritten_result.unwrap().unwrap();
                            pi_rs::validate_prepared_tool_dispatch(
                                prepared.as_ref(),
                                &context,
                                &request,
                                std::slice::from_ref(&declaration),
                            )
                            .unwrap();
                            assert_eq!(
                                matches!(
                                    prepared.effective_tools()[0].sampling,
                                    pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                                ),
                                !supported
                            );
                            assert_eq!(prepared.reservation_request().model, rewritten);
                        }
                        for unresolved in ["unknown", "other-route"] {
                            request.model = unresolved.into();
                            let error = model
                                .prepare_tool_dispatch(
                                    &request,
                                    std::slice::from_ref(&declaration),
                                    &context,
                                )
                                .await
                                .err()
                                .expect("unresolved model must fail closed");
                            assert!(error.to_string().contains("cannot be resolved"), "{error}");
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn prepared_anthropic_dispatch_preserves_config_wire_and_routing() {
        use clap::Parser;
        use std::io::{Read, Write};
        for (deferred, summary, direct) in [
            (false, false, false),
            (true, false, false),
            (true, true, false),
            (true, false, true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((s, _)) => break s,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(10))
                        }
                        Err(e) => panic!("fixture accept failed: {e}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .map(|n| n.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end..end + length]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                (headers, body)
            });
            let endpoint = format!("http://{address}/v1/messages");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"test":{
                "api":"anthropic-messages","baseUrl":endpoint,"models":[{"id":"m-base","compat":{"supportsStrictTools":false}},{"id":"m","compat":{"supportsStrictTools":true}}]
            }}})).unwrap();
            args.provider = args.model_profiles.provider("test").unwrap();
            args.model = "m-base".into();
            let client = if deferred {
                Client::deferred(
                    args.provider.clone(),
                    Adapter::Anthropic,
                    &endpoint,
                    Some(Credential::Literal("fixture".into())),
                    RequestHeaders::for_test("x-configured", "configured"),
                )
            } else {
                Client::Anthropic(Anthropic::new(&endpoint, "fixture").unwrap().with_headers({
                    let mut h = HeaderMap::new();
                    h.insert("x-configured", "configured".parse().unwrap());
                    h
                }))
            }
            .with_context_probes(false)
            .with_model_options(&args, false)
            .unwrap();
            let model = super::super::image_policy::WorkbenchModel {
                strict_policy: None,
                client: client.clone(),
                block_images: true,
                supports_images: None,
                session_affinity: &["session-id"],
                price: None,
            };
            let spec = pi_rs::ToolSpec {
                name: "read".into(),
                description: String::new(),
                parameters: json!({"type":"object","properties":{"q":{"type":"string"}}}),
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
            let decl = pi_rs::ToolDeclaration {
                spec,
                constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                    strict: pi_rs::StrictToolPolicy::Require,
                }),
            };
            let context = pi_rs::ToolDispatchContext {
                request_id: uuid::Uuid::nil(),
                session_id: uuid::Uuid::nil(),
                external_id: "session-fixture".into(),
                summary,
            };
            let selected: &dyn Model = if direct { &client } else { &model };
            let prepared = selected
                .prepare_tool_dispatch(&req, std::slice::from_ref(&decl), &context)
                .await
                .unwrap()
                .unwrap();
            pi_rs::validate_prepared_tool_dispatch(
                prepared.as_ref(),
                &context,
                &req,
                std::slice::from_ref(&decl),
            )
            .unwrap();
            assert!(matches!(
                prepared.effective_tools()[0].sampling,
                pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
            ));
            let reserved = prepared.reservation_request().tools[0].parameters.clone();
            let _ = prepared.dispatch().await;
            let (headers, body) = server.join().unwrap();
            assert_eq!(body["model"], "m");
            assert_eq!(body["tools"][0]["strict"], true);
            assert_eq!(body["tools"][0]["input_schema"], reserved);
            assert!(headers.contains("x-configured: configured"));
            assert_eq!(
                headers.contains("session-id: session-fixture"),
                !summary && !direct
            );
            assert_eq!(body["tools"][0].get("cache_control").is_some(), !summary);
        }
    }

    #[tokio::test]
    async fn prepared_anthropic_unknown_capability_falls_back_or_fails() {
        let client =
            Client::Anthropic(Anthropic::new("http://127.0.0.1:1234/v1/messages", "key").unwrap());
        let spec = pi_rs::ToolSpec {
            name: "x".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let request = ModelRequest {
            model: "unknown".into(),
            messages: vec![pi_rs::Message::User {
                content: "hi".into(),
            }],
            tools: vec![spec.clone()],
            max_output_tokens: 10,
        };
        let context = pi_rs::ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "s".into(),
            summary: false,
        };
        for (policy, ok) in [
            (pi_rs::StrictToolPolicy::Prefer, true),
            (pi_rs::StrictToolPolicy::Require, false),
        ] {
            let decl = pi_rs::ToolDeclaration {
                spec: spec.clone(),
                constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema { strict: policy }),
            };
            let result = client
                .prepare_tool_dispatch(&request, &[decl], &context)
                .await;
            assert_eq!(result.is_ok(), ok);
            if let Ok(Some(prepared)) = result {
                assert!(matches!(
                    prepared.effective_tools()[0].sampling,
                    pi_rs::EffectiveToolSampling::Unconstrained
                ));
            }
        }
    }

    #[tokio::test]
    async fn google_strict_rewrites_require_matching_effective_route() {
        use clap::Parser;
        let route_a = "http://127.0.0.1:9876/v1beta";
        let route_b = "http://127.0.0.1:9877/v1beta";
        for deferred in [false, true] {
            for explicit in [false, true] {
                for (selected, other) in [("a", "b"), ("b", "a")] {
                    let mut args = super::super::Cli::parse_from(["pi"]);
                    args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{
                        "api":"google-generative-ai", "baseUrl":route_a,
                        "compat":{"supportsStrictMode":true},
                        "models":[
                            {"id":"a", "baseUrl":route_a, "compat":{"supportsStrictMode":false}},
                            {"id":"b", "baseUrl":route_b, "compat":{"supportsStrictMode":true}},
                            {"id":"b-default", "baseUrl":route_b},
                            {"id":"models/c", "baseUrl":route_b},
                            {"id":"chat", "api":"openai-completions", "baseUrl":route_a, "compat":{"supportsStrictMode":true}}
                        ],
                        "modelOverrides":{
                            "unknown-partial":{"reasoning":false},
                            "models/b-default":{"compat":{"supportsStrictMode":true}}
                        }
                    }}})).unwrap();
                    args.provider = args.model_profiles.provider("fixture").unwrap();
                    args.model = selected.into();
                    args.endpoint = route_a.into();
                    if explicit {
                        args.endpoint_explicit = true;
                        let endpoint = if selected == "b" { route_b } else { route_a };
                        args.initial_connection = Some((args.provider.clone(), endpoint.into()));
                    }
                    let actual = args
                        .model_endpoint_for(&args.provider, Some(selected))
                        .to_owned();
                    let client = if deferred {
                        Client::deferred(
                            args.provider.clone(),
                            Adapter::Google,
                            &actual,
                            Some(Credential::Literal("key".into())),
                            RequestHeaders::default(),
                        )
                    } else {
                        Client::Google(pi_rs::google::Google::new(&actual, "key").unwrap())
                    }
                    .with_model_options(&args, false)
                    .unwrap()
                    .with_context_probes(false);
                    let model = super::super::image_policy::WorkbenchModel {
                        strict_policy: None,
                        client,
                        block_images: false,
                        supports_images: None,
                        session_affinity: &[],
                        price: None,
                    };
                    let spec = pi_rs::ToolSpec {
                        name: "t".into(),
                        description: "".into(),
                        parameters: json!({"type":"object"}),
                    };
                    for (target, allowed, strict) in [
                        (selected, true, selected == "b"),
                        (other, explicit, other == "b"),
                        ("chat", false, false),
                        ("unknown-partial", explicit || selected == "a", true),
                        ("gemini-3-unknown", explicit || selected == "a", true),
                        ("models/a", explicit || selected == "a", false),
                        ("models/b", explicit, true),
                        ("models/b-default", explicit, true),
                        ("c", explicit, true),
                    ] {
                        let request = ModelRequest {
                            model: target.into(),
                            messages: vec![],
                            tools: vec![spec.clone()],
                            max_output_tokens: 10,
                        };
                        let context = pi_rs::ToolDispatchContext {
                            request_id: uuid::Uuid::new_v4(),
                            session_id: uuid::Uuid::new_v4(),
                            external_id: "s".into(),
                            summary: false,
                        };
                        let bound = model.prepare_context(&request).await.unwrap().unwrap();
                        for policy in [
                            pi_rs::StrictToolPolicy::Prefer,
                            pi_rs::StrictToolPolicy::Require,
                        ] {
                            let decl = pi_rs::ToolDeclaration {
                                spec: spec.clone(),
                                constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                                    strict: policy,
                                }),
                            };
                            let result = bound
                                .prepare_tool_dispatch(&request, &[decl], &context)
                                .await;
                            if !allowed {
                                let error = result.err().expect("different route must fail closed");
                                assert!(
                                    error.to_string().contains("final request model and route")
                                );
                            } else if !strict && matches!(policy, pi_rs::StrictToolPolicy::Require)
                            {
                                assert!(result.is_err());
                            } else {
                                let prepared = result.unwrap().unwrap();
                                assert_eq!(
                                    matches!(
                                        prepared.effective_tools()[0].sampling,
                                        pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                                    ),
                                    strict,
                                    "{selected}->{target} explicit={explicit} deferred={deferred}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn google_before_request_rewrite_resolves_final_model_strict_policy() {
        use clap::Parser;
        use pi_rs::Extension;
        struct Rewrite(&'static str);
        #[async_trait::async_trait]
        impl Extension for Rewrite {
            fn id(&self) -> &str {
                "rewrite-model"
            }
            async fn before_request(
                &self,
                request: &mut pi_rs::ContextRequest<'_>,
                _: pi_rs::RequestPurpose,
                _: Option<&pi_rs::ExtensionState>,
            ) -> pi_rs::ExtensionResult<pi_rs::ContextChange> {
                request.model = self.0.into();
                Ok(pi_rs::ContextChange::Changed)
            }
        }
        for deferred in [false, true] {
            for (original, final_model, overrides, expected) in [
                ("gemini-2-test", "gemini-3-test", json!({}), true),
                ("gemini-3-test", "gemini-2-test", json!({}), false),
                (
                    "gemini-2-test",
                    "gemini-3-test",
                    json!({"gemini-2-test":{"compat":{"supportsStrictMode":true}}, "gemini-3-test":{"compat":{"supportsStrictMode":false}}}),
                    false,
                ),
                (
                    "gemini-3-test",
                    "gemini-2-test",
                    json!({"gemini-2-test":{"compat":{"supportsStrictMode":true}}, "gemini-3-test":{"compat":{"supportsStrictMode":false}}}),
                    true,
                ),
                (
                    "gemini-2-test",
                    "gemini-3-test",
                    json!({"gemini-2-test":{"compat":{"supportsStrictMode":false}}}),
                    true,
                ),
                (
                    "gemini-3-test",
                    "gemini-2-test",
                    json!({"gemini-3-test":{"compat":{"supportsStrictMode":true}}}),
                    false,
                ),
            ] {
                let endpoint = "http://localhost:9876";
                let mut args = super::super::Cli::parse_from(["pi"]);
                args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"together":{
                    "api":"openai-completions", "baseUrl":endpoint,
                    "models":[{"id":"gemini-2-test", "api":"google-generative-ai", "baseUrl":endpoint}, {"id":"gemini-3-test", "api":"google-generative-ai", "baseUrl":endpoint}],
                    "modelOverrides":overrides
                }}})).unwrap();
                args.provider = args.model_profiles.provider("together").unwrap();
                args.model = original.into();
                let client = if deferred {
                    Client::deferred(
                        args.provider.clone(),
                        Adapter::Google,
                        endpoint,
                        Some(Credential::Literal("key".into())),
                        RequestHeaders::default(),
                    )
                } else {
                    Client::Google(pi_rs::google::Google::new(endpoint, "key").unwrap())
                }
                .with_model_options(&args, false)
                .unwrap()
                .with_context_probes(false);
                let model = super::super::image_policy::WorkbenchModel {
                    strict_policy: None,
                    client,
                    block_images: false,
                    supports_images: None,
                    session_affinity: &[],
                    price: None,
                };
                let spec = pi_rs::ToolSpec {
                    name: "t".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                };
                let mut request = ModelRequest {
                    model: original.into(),
                    messages: vec![],
                    tools: vec![spec.clone()],
                    max_output_tokens: 10,
                };
                Rewrite(final_model)
                    .before_request(
                        &mut pi_rs::ContextRequest::new(&mut request),
                        pi_rs::RequestPurpose::Turn,
                        None,
                    )
                    .await
                    .unwrap();
                let ctx = pi_rs::ToolDispatchContext {
                    request_id: uuid::Uuid::new_v4(),
                    session_id: uuid::Uuid::new_v4(),
                    external_id: "s".into(),
                    summary: false,
                };
                let bound_model = model.prepare_context(&request).await.unwrap().unwrap();
                for policy in [
                    pi_rs::StrictToolPolicy::Prefer,
                    pi_rs::StrictToolPolicy::Require,
                ] {
                    let decl = pi_rs::ToolDeclaration {
                        spec: spec.clone(),
                        constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                            strict: policy,
                        }),
                    };
                    let result = bound_model
                        .prepare_tool_dispatch(&request, &[decl], &ctx)
                        .await;
                    if !expected && matches!(policy, pi_rs::StrictToolPolicy::Require) {
                        assert!(
                            result.is_err(),
                            "required strict must fail: {original} -> {final_model}, deferred={deferred}"
                        );
                    } else {
                        let prepared = result.unwrap().unwrap();
                        assert_eq!(
                            matches!(
                                prepared.effective_tools()[0].sampling,
                                pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                            ),
                            expected,
                            "{original} -> {final_model}, deferred={deferred}"
                        );
                        assert_eq!(prepared.dispatch_request().model, final_model);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn google_strict_support_uses_only_explicit_overrides_in_direct_and_deferred_clients() {
        use clap::Parser;
        for deferred in [false, true] {
            for (provider_compat, model_compat, override_compat, expected) in [
                (json!({}), json!({}), json!({}), true),
                (
                    json!({"supportsStrictMode":false}),
                    json!({}),
                    json!({}),
                    false,
                ),
                (
                    json!({"supportsStrictMode":false}),
                    json!({"supportsStrictMode":true}),
                    json!({}),
                    true,
                ),
                (
                    json!({"supportsStrictMode":true}),
                    json!({"supportsStrictMode":true}),
                    json!({"supportsStrictMode":false}),
                    false,
                ),
            ] {
                let endpoint = "http://localhost:9876";
                let mut args = super::super::Cli::parse_from(["pi"]);
                args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"together":{
                    "api":"openai-completions", "baseUrl":endpoint, "compat":provider_compat,
                    "models":[{"id":"gemini-3-test", "api":"google-generative-ai", "baseUrl":endpoint, "compat":model_compat}],
                    "modelOverrides":{"gemini-3-test":{"compat":override_compat}}
                }}})).unwrap();
                args.provider = args.model_profiles.provider("together").unwrap();
                args.model = "gemini-3-test".into();
                let client = if deferred {
                    Client::deferred(
                        args.provider.clone(),
                        Adapter::Google,
                        endpoint,
                        Some(Credential::Literal("key".into())),
                        RequestHeaders::default(),
                    )
                } else {
                    Client::Google(pi_rs::google::Google::new(endpoint, "key").unwrap())
                }
                .with_model_options(&args, false)
                .unwrap();
                let spec = pi_rs::ToolSpec {
                    name: "t".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                };
                let request = ModelRequest {
                    model: args.model.clone(),
                    messages: vec![],
                    tools: vec![spec.clone()],
                    max_output_tokens: 10,
                };
                let declaration = pi_rs::ToolDeclaration {
                    spec,
                    constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                        strict: pi_rs::StrictToolPolicy::Prefer,
                    }),
                };
                let context = pi_rs::ToolDispatchContext {
                    request_id: uuid::Uuid::new_v4(),
                    session_id: uuid::Uuid::new_v4(),
                    external_id: "s".into(),
                    summary: false,
                };
                let prepared = client
                    .prepare_tool_dispatch(&request, &[declaration], &context)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    matches!(
                        prepared.effective_tools()[0].sampling,
                        pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                    ),
                    expected,
                    "deferred={deferred}"
                );
            }
        }
    }

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
                strict_policy: None,
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
                strict_policy: None,
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
    async fn declaration_responses_stream_preserves_dispatch_policy() {
        use clap::Parser;
        use serde_json::Value;
        use std::io::{Read, Write};
        for (deferred, summary, path, prefer, supported) in
            [false, true].into_iter().flat_map(|deferred| {
                [false, true].into_iter().flat_map(move |summary| {
                    [0, 1, 2].into_iter().flat_map(move |path| {
                        [false, true].into_iter().flat_map(move |prefer| {
                            [false, true]
                                .into_iter()
                                .filter(move |supported| *supported || prefer)
                                .map(move |supported| (deferred, summary, path, prefer, supported))
                        })
                    })
                })
            })
        {
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
                "api":"openai-responses","baseUrl":endpoint,"compat":{"supportsStrictMode":supported},"models":[{"id":"m"}]
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
                strict_policy: None,
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
                    strict: if prefer {
                        pi_rs::StrictToolPolicy::Prefer
                    } else {
                        pi_rs::StrictToolPolicy::Require
                    },
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
            assert_eq!(
                matches!(
                    prepared.effective_tools()[0].sampling,
                    pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                ),
                supported
            );
            let result = if summary {
                selected
                    .stream_detailed_summary_with_tool_declarations(
                        context.request_id,
                        context.session_id,
                        req.clone(),
                        std::slice::from_ref(&declaration),
                    )
                    .await
            } else {
                selected
                    .stream_detailed_with_tool_declarations(
                        context.request_id,
                        context.session_id,
                        &context.external_id,
                        req.clone(),
                        std::slice::from_ref(&declaration),
                    )
                    .await
            };
            if supported {
                assert!(
                    matches!(result, Err(Error::Config(message)) if message.contains("requires prepared dispatch"))
                );
                let _ = prepared.dispatch().await;
            } else {
                assert!(result.is_err());
            }
            let (headers, body) = server.join().unwrap();
            assert_eq!(
                body["tools"][0].get("strict").and_then(Value::as_bool),
                supported.then_some(true)
            );
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
    async fn declaration_chat_stream_preserves_dispatch_policy() {
        use clap::Parser;
        use serde_json::Value;
        use std::io::{Read, Write};
        for (deferred, summary, path, prefer, supported) in
            [false, true].into_iter().flat_map(|deferred| {
                [false, true].into_iter().flat_map(move |summary| {
                    [0, 1, 2].into_iter().flat_map(move |path| {
                        [false, true].into_iter().flat_map(move |prefer| {
                            [false, true]
                                .into_iter()
                                .filter(move |supported| *supported || prefer)
                                .map(move |supported| (deferred, summary, path, prefer, supported))
                        })
                    })
                })
            })
        {
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
                "api":"openai-completions","baseUrl":endpoint,"compat":{"supportsStrictMode":supported,"promptCacheKeyMode":"enabled","cacheControlFormat":"anthropic","supportsLongCacheRetention":false},"models":[{"id":"m"}]
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
                strict_policy: None,
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
                    strict: if prefer {
                        pi_rs::StrictToolPolicy::Prefer
                    } else {
                        pi_rs::StrictToolPolicy::Require
                    },
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
            assert_eq!(
                matches!(
                    prepared.effective_tools()[0].sampling,
                    pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                ),
                supported
            );
            let expected_schema = prepared.reservation_request().tools[0].parameters.clone();
            let result = if summary {
                selected
                    .stream_detailed_summary_with_tool_declarations(
                        context.request_id,
                        context.session_id,
                        req.clone(),
                        std::slice::from_ref(&declaration),
                    )
                    .await
            } else {
                selected
                    .stream_detailed_with_tool_declarations(
                        context.request_id,
                        context.session_id,
                        &context.external_id,
                        req.clone(),
                        std::slice::from_ref(&declaration),
                    )
                    .await
            };
            if supported {
                assert!(
                    matches!(result, Err(Error::Config(message)) if message.contains("requires prepared dispatch"))
                );
                let _ = prepared.dispatch().await;
            } else {
                assert!(result.is_err());
            }
            let (headers, body) = server.join().unwrap();
            assert_eq!(
                body["tools"][0]["function"]
                    .get("strict")
                    .and_then(Value::as_bool),
                supported.then_some(true)
            );
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
            Provider::Radius,
            Adapter::PiMessages,
            "https://example.invalid/v1/messages",
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
            strict_policy: None,
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
    async fn responses_wire_fields_follow_model_capabilities() {
        use std::io::{Read, Write};
        for (models, overrides, provider_compat, expected, output_limit) in [
            (
                serde_json::json!([{"id":"m","reasoning":true}]),
                serde_json::json!({}),
                serde_json::json!({}),
                "developer",
                true,
            ),
            (
                serde_json::json!([{"id":"m","reasoning":false}]),
                serde_json::json!({}),
                serde_json::json!({}),
                "system",
                true,
            ),
            (
                serde_json::json!([{"id":"m","reasoning":true}]),
                serde_json::json!({}),
                serde_json::json!({"supportsDeveloperRole":false,"supportsMaxOutputTokens":false}),
                "system",
                false,
            ),
            (
                serde_json::json!([{"id":"m","reasoning":true,"compat":{"supportsDeveloperRole":true,"supportsMaxOutputTokens":true}}]),
                serde_json::json!({}),
                serde_json::json!({"supportsDeveloperRole":false,"supportsMaxOutputTokens":false}),
                "developer",
                true,
            ),
            (
                serde_json::json!([]),
                serde_json::json!({}),
                serde_json::json!({}),
                "system",
                true,
            ),
            (
                serde_json::json!([]),
                serde_json::json!({"m":{"name":"partial","compat":{"supportsMaxOutputTokens":false}}}),
                serde_json::json!({}),
                "system",
                false,
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
            assert_eq!(
                body["max_output_tokens"].as_u64(),
                output_limit.then_some(10)
            );
        }
    }

    #[tokio::test]
    async fn opencode_prepared_requests_send_route_headers_for_every_api() {
        use clap::Parser;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let mut args = super::super::Cli::parse_from(["pi"]);
        for (provider, model, target, auth_header, override_session) in [
            (
                "opencode",
                "kimi-k2.7-code",
                "/relay/chat/completions",
                "authorization: bearer fixture",
                true,
            ),
            (
                "opencode-go",
                "claude-haiku-5-5",
                "/relay/messages?beta=true",
                "x-api-key: fixture",
                false,
            ),
            (
                "opencode",
                "gemini-4-argon",
                "/relay/models/gemini-4-argon:streamGenerateContent?alt=sse",
                "x-goog-api-key: fixture",
                false,
            ),
            (
                "opencode-go",
                "gpt-6-luna",
                "/relay/responses",
                "authorization: bearer fixture",
                false,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/relay", listener.local_addr().unwrap());
            let headers = if override_session {
                json!({"X-OpenCode-Session": "explicit"})
            } else {
                json!({})
            };
            args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
                "providers":{provider:{"baseUrl":base,"apiKey":"fixture","headers":headers}}
            }))
            .unwrap();
            args.provider = args.model_profiles.provider(provider).unwrap();
            args.model = model.into();
            let endpoint = args.model_endpoint_for(&args.provider, Some(model));
            let client = args
                .connect_provider(&args.provider, Some(model))
                .unwrap()
                .with_context_probes(false)
                .with_model_options(&args, false)
                .unwrap();
            let routed = super::super::image_policy::WorkbenchModel {
                strict_policy: None,
                client,
                block_images: false,
                supports_images: None,
                session_affinity: args.model_profiles.session_affinity(
                    &args.provider,
                    model,
                    endpoint,
                ),
                price: None,
            };
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    request.push_str(&line);
                }
                socket.get_mut().write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                request.to_ascii_lowercase()
            });
            let context = pi_rs::ToolDispatchContext {
                request_id: uuid::Uuid::new_v4(),
                session_id: uuid::Uuid::new_v4(),
                external_id: "routing-fixture".into(),
                summary: false,
            };
            let request = ModelRequest {
                model: model.into(),
                messages: vec![pi_rs::Message::User {
                    content: "hello".into(),
                }],
                tools: vec![],
                max_output_tokens: 32,
            };
            let prepared = routed
                .prepare_tool_dispatch(&request, &[], &context)
                .await
                .unwrap()
                .unwrap();
            let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                if let Ok(mut stream) = prepared.dispatch().await {
                    assert!(stream.next().await.unwrap().is_err());
                }
            })
            .await;
            assert!(
                dispatched.is_ok(),
                "request timed out for {provider}/{model}"
            );
            let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            assert!(
                dispatched.starts_with(&format!("post {} http/", target.to_ascii_lowercase())),
                "{dispatched}"
            );
            assert!(
                dispatched.lines().any(|line| line == auth_header),
                "{dispatched}"
            );
            let session_header = if override_session {
                "x-opencode-session: explicit"
            } else {
                "x-opencode-session: routing-fixture"
            };
            assert!(
                dispatched.lines().any(|line| line == session_header),
                "{dispatched}"
            );
            if model == "gpt-6-luna" {
                assert!(
                    dispatched
                        .lines()
                        .any(|line| line == "x-client-request-id: routing-fixture"),
                    "{dispatched}"
                );
            }
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
                strict_policy: None,
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
                    strict_policy: None,
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
            strict_policy: None,
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
            strict_policy: None,
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

#[cfg(test)]
mod final_model_strict_tests {
    use super::*;
    use clap::Parser;
    use std::{
        io::{Read, Write},
        sync::Arc,
    };

    #[tokio::test]
    async fn final_model_drives_strict_decision_for_resolved_and_deferred_clients() {
        for api in ["openai-responses", "openai-completions"] {
            for deferred in [false, true] {
                for selected in ["yes", "no"] {
                    for target in ["yes", "no", "unknown"] {
                        for require in [false, true] {
                            for prepared_context in [false, true] {
                                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                                let endpoint = format!(
                                    "http://{}/v1/{}",
                                    listener.local_addr().unwrap(),
                                    if api == "openai-responses" {
                                        "responses"
                                    } else {
                                        "chat/completions"
                                    }
                                );
                                let mut args = super::super::Cli::parse_from(["pi"]);
                                args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{
                                    "api":api,"baseUrl":endpoint,"compat":{"supportsStrictMode":true},
                                    "models":[{"id":"yes","compat":{"supportsStrictMode":true}},{"id":"no","compat":{"supportsStrictMode":false}}]
                                }}})).unwrap();
                                args.provider = args.model_profiles.provider("fixture").unwrap();
                                args.model = selected.into();
                                let adapter = if api == "openai-responses" {
                                    Adapter::OpenAiResponses
                                } else {
                                    Adapter::ChatCompletions
                                };
                                let client = if deferred {
                                    Client::deferred(
                                        args.provider.clone(),
                                        adapter,
                                        &endpoint,
                                        Some(Credential::Literal("fixture".into())),
                                        RequestHeaders::default(),
                                    )
                                } else if api == "openai-responses" {
                                    Client::OpenAi(
                                        pi_rs::openai::OpenAiResponses::new_api(
                                            &endpoint,
                                            "fixture".into(),
                                        )
                                        .unwrap(),
                                    )
                                } else {
                                    Client::Ollama(
                                        pi_rs::ollama::Ollama::new(
                                            &endpoint,
                                            Some("fixture".into()),
                                        )
                                        .unwrap()
                                        .with_native_probes(false),
                                    )
                                }
                                .with_model_options(&args, false)
                                .unwrap();
                                let model = super::super::image_policy::WorkbenchModel {
                                    strict_policy: Some(Arc::new(
                                        super::super::image_policy::FinalStrictPolicy {
                                            profiles: args.model_profiles.clone(),
                                            provider: args.provider.clone(),
                                            selected_model: selected.into(),
                                            explicit_endpoint: false,
                                            bound_support: None,
                                        },
                                    )),
                                    client,
                                    block_images: false,
                                    supports_images: None,
                                    session_affinity: &[],
                                    price: None,
                                };
                                let spec = pi_rs::ToolSpec {
                                    name: "read".into(),
                                    description: String::new(),
                                    parameters: json!({"type":"object","properties":{"x":{"type":"string"}}}),
                                };
                                let req = ModelRequest {
                                    model: target.into(),
                                    messages: vec![pi_rs::Message::User {
                                        content: "test".into(),
                                    }],
                                    tools: vec![spec.clone()],
                                    max_output_tokens: 10,
                                };
                                let declaration = pi_rs::ToolDeclaration {
                                    spec,
                                    constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                                        strict: if require {
                                            pi_rs::StrictToolPolicy::Require
                                        } else {
                                            pi_rs::StrictToolPolicy::Prefer
                                        },
                                    }),
                                };
                                let context = pi_rs::ToolDispatchContext {
                                    request_id: uuid::Uuid::new_v4(),
                                    session_id: uuid::Uuid::new_v4(),
                                    external_id: "fixture".into(),
                                    summary: false,
                                };
                                let prepared_model = if prepared_context {
                                    model.prepare_context(&req).await.unwrap()
                                } else {
                                    None
                                };
                                if prepared_context && selected == "yes" && target == "no" {
                                    let mut other = req.clone();
                                    other.model = "yes".into();
                                    let yes = model.prepare_context(&other).await.unwrap().unwrap();
                                    assert_ne!(
                                        yes.context_identity(),
                                        prepared_model.as_ref().unwrap().context_identity()
                                    );
                                }
                                let active: &dyn Model =
                                    prepared_model.as_deref().unwrap_or(&model);
                                let result = active
                                    .prepare_tool_dispatch(
                                        &req,
                                        std::slice::from_ref(&declaration),
                                        &context,
                                    )
                                    .await;
                                let supported = target == "yes";
                                if require && !supported {
                                    assert!(
                                        result.is_err(),
                                        "{api} {deferred} {selected}->{target}"
                                    );
                                    continue;
                                }
                                let dispatch = result.unwrap().unwrap();
                                assert_eq!(
                                    matches!(
                                        dispatch.effective_tools()[0].sampling,
                                        pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                                    ),
                                    supported
                                );
                                let reservation =
                                    &dispatch.reservation_request().tools[0].parameters;
                                if supported {
                                    assert_eq!(reservation["additionalProperties"], false);
                                } else {
                                    assert_eq!(*reservation, declaration.spec.parameters);
                                }
                                let server = std::thread::spawn(move || {
                                    let (mut stream, _) = listener.accept().unwrap();
                                    stream
                                        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                                        .unwrap();
                                    let mut data = Vec::new();
                                    let (offset, length) = loop {
                                        let mut buf = [0; 4096];
                                        let n = stream.read(&mut buf).unwrap();
                                        assert!(n > 0);
                                        data.extend_from_slice(&buf[..n]);
                                        if let Some(pos) =
                                            data.windows(4).position(|v| v == b"\r\n\r\n")
                                        {
                                            let header = String::from_utf8_lossy(&data[..pos]);
                                            let length = header
                                                .lines()
                                                .find_map(|line| {
                                                    line.to_ascii_lowercase()
                                                        .strip_prefix("content-length:")
                                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                                })
                                                .unwrap();
                                            break (pos + 4, length);
                                        }
                                    };
                                    while data.len() < offset + length {
                                        let mut buf = [0; 4096];
                                        let n = stream.read(&mut buf).unwrap();
                                        assert!(n > 0);
                                        data.extend_from_slice(&buf[..n]);
                                    }
                                    let body: serde_json::Value =
                                        serde_json::from_slice(&data[offset..offset + length])
                                            .unwrap();
                                    stream.write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                                    body
                                });
                                let _ = dispatch.dispatch().await;
                                let body = server.join().unwrap();
                                assert_eq!(body["model"], target);
                                let tool = if api == "openai-responses" {
                                    &body["tools"][0]
                                } else {
                                    &body["tools"][0]["function"]
                                };
                                assert_eq!(
                                    tool.get("strict").and_then(serde_json::Value::as_bool),
                                    supported.then_some(true)
                                );
                                if supported {
                                    assert_eq!(tool["parameters"]["additionalProperties"], false);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod final_model_hook_tests {
    use super::*;
    use clap::Parser;
    use std::sync::Arc;

    struct RequiredTool;
    #[async_trait::async_trait]
    impl pi_rs::Tool for RequiredTool {
        fn spec(&self) -> pi_rs::ToolSpec {
            pi_rs::ToolSpec {
                name: "required".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        fn constrained_sampling(&self) -> Option<pi_rs::ToolConstraint> {
            Some(pi_rs::ToolConstraint::JsonSchema {
                strict: pi_rs::StrictToolPolicy::Require,
            })
        }
        async fn invoke(&self, _: serde_json::Value, _: pi_rs::ToolContext) -> pi_rs::Outcome {
            panic!("must not dispatch")
        }
    }
    struct Rewrite;
    #[async_trait::async_trait]
    impl pi_rs::Extension for Rewrite {
        fn id(&self) -> &str {
            "rewrite"
        }
        fn tools(&self) -> Vec<Arc<dyn pi_rs::Tool>> {
            vec![Arc::new(RequiredTool)]
        }
        async fn before_request(
            &self,
            request: &mut pi_rs::ContextRequest<'_>,
            _: pi_rs::RequestPurpose,
            _: Option<&pi_rs::ExtensionState>,
        ) -> pi_rs::ExtensionResult<pi_rs::ContextChange> {
            request.model = "no".into();
            Ok(pi_rs::ContextChange::Changed)
        }
    }
    struct RewriteTurnOnly {
        calls: Arc<std::sync::Mutex<Vec<(pi_rs::RequestPurpose, String)>>>,
    }

    #[async_trait::async_trait]
    impl pi_rs::Extension for RewriteTurnOnly {
        fn id(&self) -> &str {
            "rewrite-turn-only"
        }
        fn tools(&self) -> Vec<Arc<dyn pi_rs::Tool>> {
            vec![Arc::new(RequiredTool)]
        }
        async fn before_request(
            &self,
            request: &mut pi_rs::ContextRequest<'_>,
            purpose: pi_rs::RequestPurpose,
            _: Option<&pi_rs::ExtensionState>,
        ) -> pi_rs::ExtensionResult<pi_rs::ContextChange> {
            self.calls
                .lock()
                .unwrap()
                .push((purpose, request.model.clone()));
            if purpose == pi_rs::RequestPurpose::Turn {
                request.model = "b".into();
                Ok(pi_rs::ContextChange::Changed)
            } else {
                Ok(pi_rs::ContextChange::Unchanged)
            }
        }
    }

    #[tokio::test]
    async fn final_model_hook_compaction_summary_uses_selected_model_then_resumes_turn() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for text in ["old conversation summary", "turn answer"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (end, size) = loop {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
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
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                bodies.push(
                    serde_json::from_slice::<serde_json::Value>(&bytes[end..end + size]).unwrap(),
                );
                let events = [
                    json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message"}}),
                    json!({"type":"response.output_text.delta","output_index":0,"delta":text}),
                    json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","content":[{"type":"output_text","text":text}]}}),
                    json!({"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":12,"output_tokens":1}}}),
                ];
                let body: String = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            bodies
        });
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{
            "api":"openai-responses","baseUrl":endpoint,
            "models":[{"id":"a","compat":{"supportsStrictMode":false}}, {"id":"b","compat":{"supportsStrictMode":true}}]
        }}})).unwrap();
        args.provider = args.model_profiles.provider("fixture").unwrap();
        args.model = "a".into();
        let client = Client::OpenAi(
            pi_rs::openai::OpenAiResponses::new_api(&endpoint, "fixture".into()).unwrap(),
        )
        .with_model_options(&args, false)
        .unwrap();
        let model = super::super::image_policy::WorkbenchModel {
            strict_policy: Some(Arc::new(super::super::image_policy::FinalStrictPolicy {
                profiles: args.model_profiles.clone(),
                provider: args.provider.clone(),
                selected_model: "a".into(),
                explicit_endpoint: false,
                bound_support: None,
            })),
            client,
            block_images: false,
            supports_images: None,
            session_affinity: &[],
            price: None,
        };
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut registry = pi_rs::Registry::default();
        registry
            .register(Arc::new(RewriteTurnOnly {
                calls: calls.clone(),
            }))
            .unwrap();
        let agent = pi_rs::Agent {
            model: &model,
            model_name: "a",
            registry: &registry,
            system_prompt: "",
            limits: pi_rs::Limits {
                context_window: Some(4096),
                output_tokens_per_request: 64,
                compaction_tokens: Some(pi_rs::CompactionTokens {
                    reserve_tokens: Some(4096),
                    keep_recent_tokens: None,
                }),
                model_timeout: std::time::Duration::from_secs(5),
                ..pi_rs::Limits::default()
            },
        };
        let mut seed = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        seed.append_user_with_images("earlier question".into(), vec![], None)
            .unwrap();
        let mut records = seed.records().to_vec();
        records.push(pi_rs::Record {
            timestamp: None,
            run_id: None,
            version: records.last().unwrap().version,
            id: records.len() as u64 + 1,
            parent: Some(seed.head()),
            event: pi_rs::Event::Assistant {
                content: "earlier answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            },
        });
        let mut session = pi_rs::Session::from_records(records).unwrap();
        let (_sender, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let answer = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            agent.run(
                &mut session,
                Some("new question".into()),
                &mut inputs,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            ),
        )
        .await
        .expect("agent timed out")
        .unwrap();
        assert_eq!(answer, "turn answer");
        let bodies = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("loopback timed out")
            .unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0]["model"], "a");
        assert!(
            bodies[0]
                .get("tools")
                .is_none_or(|tools| tools.as_array().is_some_and(Vec::is_empty))
        );
        assert!(
            bodies[0]
                .to_string()
                .contains("Summarize only this older conversation")
        );
        assert_eq!(bodies[1]["model"], "b");
        assert_eq!(bodies[1]["tools"][0]["name"], "required");
        assert_eq!(bodies[1]["tools"][0]["strict"], true);
        assert_eq!(
            bodies[1]["tools"][0]["parameters"]["additionalProperties"],
            false
        );
        let seen = calls.lock().unwrap();
        assert!(
            seen.iter()
                .any(|(purpose, model)| *purpose == pi_rs::RequestPurpose::Turn && model == "a")
        );
        assert!(
            seen.iter()
                .any(|(purpose, model)| *purpose == pi_rs::RequestPurpose::Summary && model == "a")
        );
        drop(seen);
        assert!(session.records().iter().any(|record| matches!(&record.event, pi_rs::Event::Compacted { summary, .. } if summary == "old conversation summary")));
        let requests: Vec<_> = session
            .records()
            .iter()
            .filter_map(|record| match &record.event {
                pi_rs::Event::ModelRequested {
                    model,
                    request_id: Some(id),
                    ..
                } => Some((record.id, model.as_str(), *id)),
                _ => None,
            })
            .collect();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1, "a");
        assert_eq!(requests[1].1, "b");
        for (index, (record, model, id)) in requests.iter().enumerate() {
            let resolved = session
                .records()
                .iter()
                .find_map(|entry| match &entry.event {
                    pi_rs::Event::ModelFinished {
                        request_id,
                        metadata,
                        ..
                    } if request_id == id => metadata.resolved_model.as_ref(),
                    _ => None,
                })
                .expect("missing resolved model identity");
            assert_eq!(resolved.model, *model);
            let decision = session.recorded_tool_decision(*record).unwrap();
            if index == 0 {
                assert!(matches!(decision, pi_rs::RecordedToolDecision::Legacy));
                continue;
            }
            let pi_rs::RecordedToolDecision::Current(snapshot) = decision else {
                panic!("missing current decision")
            };
            assert_eq!(snapshot.context().request_id, *id);
            assert_eq!(snapshot.context().session_id, session.session_id());
            assert_eq!(snapshot.context().summary, index == 0);
            let value = serde_json::to_value(snapshot).unwrap();
            assert_eq!(
                value["effective"][0]["sampling"]["type"],
                "strict_json_schema"
            );
        }
    }

    #[tokio::test]
    async fn final_model_hook_rejects_required_downgrade_before_journaling() {
        for api in ["openai-responses", "openai-completions"] {
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{"api":api,"baseUrl":"http://127.0.0.1:9/v1","models":[{"id":"yes","compat":{"supportsStrictMode":true}},{"id":"no","compat":{"supportsStrictMode":false}}]}}})).unwrap();
            args.provider = args.model_profiles.provider("fixture").unwrap();
            args.model = "yes".into();
            let client = if api == "openai-responses" {
                Client::OpenAi(
                    pi_rs::openai::OpenAiResponses::new_api(
                        "http://127.0.0.1:9/v1/responses",
                        "fixture".into(),
                    )
                    .unwrap(),
                )
            } else {
                Client::Ollama(
                    pi_rs::ollama::Ollama::new(
                        "http://127.0.0.1:9/v1/chat/completions",
                        Some("fixture".into()),
                    )
                    .unwrap()
                    .with_native_probes(false),
                )
            }
            .with_model_options(&args, false)
            .unwrap();
            let model = super::super::image_policy::WorkbenchModel {
                strict_policy: Some(Arc::new(super::super::image_policy::FinalStrictPolicy {
                    profiles: args.model_profiles.clone(),
                    provider: args.provider.clone(),
                    selected_model: "yes".into(),
                    explicit_endpoint: false,
                    bound_support: None,
                })),
                client,
                block_images: false,
                supports_images: None,
                session_affinity: &[],
                price: None,
            };
            let mut registry = pi_rs::Registry::default();
            registry.register(Arc::new(Rewrite)).unwrap();
            let agent = pi_rs::Agent {
                model: &model,
                model_name: "yes",
                registry: &registry,
                system_prompt: "",
                limits: pi_rs::Limits::default(),
            };
            let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
            let (_, mut input) = tokio::sync::mpsc::channel::<String>(1);
            let error = agent
                .run(
                    &mut session,
                    Some("hi".into()),
                    &mut input,
                    &pi_rs::CancellationToken::new(),
                    &mut |_| {},
                )
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("requires JSON-schema"),
                "{error}"
            );
            assert!(
                !session
                    .records()
                    .iter()
                    .any(|r| matches!(r.event, pi_rs::Event::ModelRequested { .. }))
            );
        }
    }
}
