//! Amazon Bedrock ConverseStream adapter.
mod headers;
mod options;
use aws_sdk_bedrockruntime::{
    Client,
    types::{
        self, ContentBlock, ContentBlockDelta, ContentBlockStart, ConversationRole,
        ConverseStreamOutput,
    },
};
use aws_smithy_types::{Blob, Document, Number};
use aws_types::request_id::RequestId;
use base64::Engine;
pub use options::{
    ReasoningLevel, ThinkingBudgets, ThinkingDisplay, requires_thinking, thinking_levels,
};
use pi_rs_core::{
    CacheRetention, Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    ProviderReplay, ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// A client configured with the AWS SDK's region and refreshing credential provider chain.
#[derive(Clone)]
pub struct Bedrock {
    client: std::sync::Arc<tokio::sync::OnceCell<Client>>,
    headers: BTreeMap<String, Vec<String>>,
    unsupported: Option<String>,
    bearer: Option<String>,
    force_sigv4: bool,
    profile: Option<String>,
    model_name: Option<(String, String)>,
    known_config: Option<std::sync::Arc<tokio::sync::OnceCell<aws_types::SdkConfig>>>,
    prepared_identity: Option<String>,
    environment_config: bool,
    http_client: Option<aws_smithy_runtime_api::client::http::SharedHttpClient>,
    credentials_http_client: Option<aws_smithy_runtime_api::client::http::SharedHttpClient>,
    reasoning: Option<ReasoningLevel>,
    mapped_effort: Option<String>,
    thinking_budgets: ThinkingBudgets,
    configured_thinking_budgets: Option<Value>,
    thinking_display: ThinkingDisplay,
    interleaved_thinking: bool,
    cache_retention: CacheRetention,
    long_cache_support: Option<bool>,
    force_cache: bool,
    summary: bool,
}
impl Bedrock {
    /// Resolve the region and standard AWS credential chain lazily.
    pub fn from_env() -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            headers: BTreeMap::new(),
            unsupported: None,
            bearer: None,
            force_sigv4: false,
            profile: None,
            model_name: None,
            known_config: Some(std::sync::Arc::new(tokio::sync::OnceCell::new())),
            prepared_identity: None,
            environment_config: true,
            http_client: None,
            credentials_http_client: None,
            reasoning: None,
            mapped_effort: None,
            thinking_budgets: ThinkingBudgets::default(),
            configured_thinking_budgets: None,
            thinking_display: ThinkingDisplay::default(),
            interleaved_thinking: true,
            cache_retention: default_cache_retention(),
            long_cache_support: None,
            force_cache: std::env::var("AWS_BEDROCK_FORCE_CACHE").is_ok_and(|v| v == "1"),
            summary: false,
        }
    }
    /// Use a caller-configured SDK client, including custom profile or endpoint settings.
    pub fn new(client: Client) -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new_with(Some(client))),
            headers: BTreeMap::new(),
            unsupported: None,
            bearer: None,
            force_sigv4: false,
            profile: None,
            model_name: None,
            known_config: None,
            prepared_identity: None,
            environment_config: false,
            http_client: None,
            credentials_http_client: None,
            reasoning: None,
            mapped_effort: None,
            thinking_budgets: ThinkingBudgets::default(),
            configured_thinking_budgets: None,
            thinking_display: ThinkingDisplay::default(),
            interleaved_thinking: true,
            cache_retention: default_cache_retention(),
            long_cache_support: None,
            force_cache: std::env::var("AWS_BEDROCK_FORCE_CACHE").is_ok_and(|v| v == "1"),
            summary: false,
        }
    }
    /// Construct from a shared SDK configuration whose route can be inspected.
    pub fn from_sdk_config(config: aws_types::SdkConfig) -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new_with(Some(Client::new(
                &config,
            )))),
            headers: BTreeMap::new(),
            unsupported: None,
            bearer: None,
            force_sigv4: false,
            profile: None,
            model_name: None,
            known_config: Some(std::sync::Arc::new(tokio::sync::OnceCell::new_with(Some(
                config,
            )))),
            prepared_identity: None,
            environment_config: false,
            http_client: None,
            credentials_http_client: None,
            reasoning: None,
            mapped_effort: None,
            thinking_budgets: ThinkingBudgets::default(),
            configured_thinking_budgets: None,
            thinking_display: ThinkingDisplay::default(),
            interleaved_thinking: true,
            cache_retention: default_cache_retention(),
            long_cache_support: None,
            force_cache: std::env::var("AWS_BEDROCK_FORCE_CACHE").is_ok_and(|v| v == "1"),
            summary: false,
        }
    }
    /// Replace custom request headers; authentication and SigV4 headers are ignored.
    pub fn with_headers(mut self, headers: http::HeaderMap) -> Result<Self> {
        self.headers = headers::filtered(headers)?;
        self.prepared_identity = None;
        Ok(self)
    }
    /// Add missing default headers without overriding configured request headers.
    pub fn with_default_headers(mut self, headers: &http::HeaderMap) -> Result<Self> {
        for (name, value) in headers::filtered(headers.clone())? {
            self.headers.entry(name).or_insert(value);
        }
        self.prepared_identity = None;
        Ok(self)
    }
    /// Prefer bearer authentication over the AWS credential chain.
    pub fn with_bearer_token(mut self, token: String) -> Result<Self> {
        if token.is_empty() || !token.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return Err(Error::Config("invalid Bedrock bearer token".into()));
        }
        let client = self
            .client
            .get()
            .map(|client| bearer_client(client.config().to_builder(), &token));
        self.client = std::sync::Arc::new(tokio::sync::OnceCell::new_with(client));
        self.bearer = Some(token);
        self.force_sigv4 = false;
        self.prepared_identity = None;
        Ok(self)
    }
    /// Rebuild from the standard SDK configuration using a named profile, without
    /// modifying the process environment. This replaces any caller-supplied client.
    pub fn with_profile(mut self, profile: String) -> Self {
        self.profile = Some(profile);
        self.bearer = None;
        self.force_sigv4 = true;
        self.client = std::sync::Arc::new(tokio::sync::OnceCell::new());
        self.known_config = Some(std::sync::Arc::new(tokio::sync::OnceCell::new()));
        self.prepared_identity = None;
        self
    }

    /// Rebuild from the standard SDK configuration using the credential chain,
    /// ignoring any ambient Bedrock bearer token and replacing a supplied client.
    pub fn with_credential_chain(mut self) -> Self {
        self.profile = None;
        self.force_sigv4 = true;
        self.bearer = None;
        self.client = std::sync::Arc::new(tokio::sync::OnceCell::new());
        self.known_config = Some(std::sync::Arc::new(tokio::sync::OnceCell::new()));
        self.prepared_identity = None;
        self
    }

    async fn load_config(&self) -> aws_types::SdkConfig {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(client) = &self.credentials_http_client {
            loader = loader.http_client(client.clone());
        }
        if let Some(profile) = &self.profile {
            // Load the effective region first, including the selected profile's region.
            // The profile provider uses it for STS when assuming a role.
            let config = loader.profile_name(profile).load().await;
            let mut provider_config = aws_config::provider_config::ProviderConfig::without_region()
                .with_region(config.region().cloned());
            if let Some(client) = &self.credentials_http_client {
                provider_config = provider_config.with_http_client(client.clone());
            }
            let credentials = aws_config::profile::ProfileFileCredentialsProvider::builder()
                .configure(&provider_config)
                .profile_name(profile)
                .build();
            config
                .to_builder()
                .credentials_provider(
                    aws_credential_types::provider::SharedCredentialsProvider::new(credentials),
                )
                .build()
        } else {
            loader.load().await
        }
    }
    /// Bind a configured model name to its ID for model-specific Converse behavior.
    pub fn with_model_name(mut self, model: &str, name: Option<&str>) -> Self {
        self.model_name = name.map(|name| (model.to_owned(), name.to_owned()));
        self.prepared_identity = None;
        self
    }
    fn is_claude(&self, model: &str) -> bool {
        model.to_ascii_lowercase().contains("claude")
            || self.model_name.as_ref().is_some_and(|(id, name)| {
                id == model && name.to_ascii_lowercase().contains("claude")
            })
    }
    /// Freeze an inspectable SDK configuration and its effective authentication for one request.
    pub async fn prepare_bedrock(&self) -> Result<Option<Self>> {
        use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
        if self.unsupported.is_some() || self.http_client.is_some() {
            return Ok(None);
        }
        let Some(config) = &self.known_config else {
            return Ok(None);
        };
        let config = config
            .get_or_init(|| async { self.load_config().await })
            .await;
        // A caller-supplied HTTP transport can reroute requests independently
        // of the SDK endpoint configuration.
        if config.http_client().is_some() {
            return Ok(None);
        }
        let Some(region) = config.region().map(|region| region.as_ref().to_owned()) else {
            return Ok(None);
        };
        // A caller-provided bearer resolver may change independently; explicit
        // bearer tokens and the service-specific environment token are fixed here.
        let service_value = |service: &'static str, env: &'static str, profile: &'static str| {
            let key = aws_types::service_config::ServiceConfigKey::builder()
                .service_id(service)
                .env(env)
                .profile(profile)
                .build()
                .ok()?;
            config.service_config()?.load_config(key)
        };
        let endpoint = if config.get_origin("endpoint_url").is_client_config() {
            config.endpoint_url().map(str::to_owned)
        } else {
            service_value("Bedrock Runtime", "AWS_ENDPOINT_URL", "endpoint_url")
                .or_else(|| config.endpoint_url().map(str::to_owned))
        };
        let bearer = self.bearer.clone().or_else(|| {
            if self.force_sigv4
                || (!self.environment_config && config.token_provider().is_some())
                || config
                    .get_origin("auth_scheme_preference")
                    .is_client_config()
            {
                return None;
            }
            service_value("bedrock", "AWS_BEARER_TOKEN", "")
        });
        let mut frozen = config.clone();
        let auth_identity = if let Some(token) = bearer.as_ref() {
            if token.is_empty() || !token.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
                return Ok(None);
            }
            ("bearer", token.clone(), String::new(), None, None)
        } else {
            if !self.force_sigv4
                && ((!self.environment_config && config.token_provider().is_some())
                    || config.auth_scheme_preference().is_some()
                    || config
                        .get_origin("auth_scheme_preference")
                        .is_client_config())
            {
                return Ok(None);
            }
            let Some(provider) = config.credentials_provider() else {
                return Ok(None);
            };
            let Ok(credentials) = provider.provide_credentials().await else {
                return Ok(None);
            };
            let identity = (
                "sigv4",
                credentials.access_key_id().to_owned(),
                credentials.secret_access_key().to_owned(),
                credentials.session_token().map(str::to_owned),
                credentials.account_id().map(|id| id.as_str().to_owned()),
            );
            frozen = config
                .to_builder()
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build();
            identity
        };
        let identity = pi_rs_core::protected_model_fingerprint(&(
            "bedrock-converse-v1",
            &region,
            &endpoint,
            config.use_fips(),
            config.use_dual_stack(),
            auth_identity,
            &self.model_name,
            &self.headers,
            format!("{:?}", self.reasoning),
            &self.mapped_effort,
            (
                format!("{:?}", self.thinking_budgets),
                &self.configured_thinking_budgets,
                format!("{:?}", self.thinking_display),
                self.interleaved_thinking,
            ),
            match self.cache_retention {
                CacheRetention::None => "none",
                CacheRetention::Short => "short",
                CacheRetention::Long => "long",
            },
            self.long_cache_support,
            self.force_cache,
            self.summary,
        ));
        let mut builder = aws_sdk_bedrockruntime::config::Builder::from(&frozen)
            .identity_cache(aws_sdk_bedrockruntime::config::IdentityCache::no_cache());
        // Freeze the endpoint selected above instead of consulting service
        // environment settings again during dispatch.
        builder.set_endpoint_url(endpoint);
        let client = if let Some(token) = bearer.as_deref() {
            bearer_client(builder, token)
        } else {
            use aws_smithy_runtime_api::client::auth::AuthSchemeId;
            Client::from_conf(
                builder
                    .auth_scheme_preference([AuthSchemeId::new("sigv4")])
                    .build(),
            )
        };
        let mut prepared = self.clone();
        prepared.client = std::sync::Arc::new(tokio::sync::OnceCell::new_with(Some(client)));
        prepared.bearer = bearer;
        prepared.known_config = None;
        prepared.prepared_identity = identity;
        Ok(Some(prepared))
    }

    /// Configure Pi's reasoning level and an optional model-specific effort mapping.
    pub fn with_reasoning(
        mut self,
        level: Option<&str>,
        mapped_effort: Option<&str>,
    ) -> Result<Self> {
        self.reasoning = level.map(ReasoningLevel::parse).transpose()?;
        self.mapped_effort = mapped_effort.map(str::to_owned);
        self.prepared_identity = None;
        Ok(self)
    }
    /// Override per-level budgets for non-adaptive Claude thinking.
    pub fn with_thinking_budgets(mut self, budgets: ThinkingBudgets) -> Self {
        self.thinking_budgets = budgets;
        self.configured_thinking_budgets = None;
        self.prepared_identity = None;
        self
    }
    /// Set Pi's raw budget settings; only the budget used by a request is validated.
    pub fn with_thinking_budget_settings(mut self, settings: Value) -> Self {
        self.configured_thinking_budgets = Some(settings);
        self.prepared_identity = None;
        self
    }
    /// Control whether Claude returns readable reasoning text.
    pub fn with_thinking_display(mut self, display: ThinkingDisplay) -> Self {
        self.thinking_display = display;
        self.prepared_identity = None;
        self
    }
    /// Control Pi's interleaved-thinking beta for budgeted Claude requests.
    pub fn with_interleaved_thinking(mut self, enabled: bool) -> Self {
        self.interleaved_thinking = enabled;
        self.prepared_identity = None;
        self
    }
    /// Set explicit cache retention. Use `None` for compaction and other one-off requests.
    pub fn with_cache_retention(mut self, retention: CacheRetention) -> Self {
        self.cache_retention = retention;
        self.prepared_identity = None;
        self
    }
    /// Override model-based one-hour cache support; `None` uses known model capabilities.
    pub fn with_long_cache_support(mut self, supported: Option<bool>) -> Self {
        self.long_cache_support = supported;
        self.prepared_identity = None;
        self
    }
    /// Prepare a one-off summary without cache checkpoints or optional thinking.
    pub fn for_summary(mut self) -> Self {
        self.cache_retention = CacheRetention::None;
        self.reasoning = Some(ReasoningLevel::Off);
        self.mapped_effort = None;
        self.summary = true;
        self.prepared_identity = None;
        self
    }
    /// Enable cache checkpoints for a model whose capabilities cannot be inferred from its name.
    pub fn with_force_cache(mut self, enabled: bool) -> Self {
        self.force_cache = enabled;
        self.prepared_identity = None;
        self
    }
    /// Route requests through a caller-supplied Smithy HTTP client.
    pub fn with_http_client(
        self,
        client: aws_smithy_runtime_api::client::http::SharedHttpClient,
    ) -> Self {
        self.with_http_clients(client.clone(), client)
    }

    /// Route Runtime and SDK credential-provider traffic through separate transports.
    pub fn with_http_clients(
        mut self,
        client: aws_smithy_runtime_api::client::http::SharedHttpClient,
        credentials_client: aws_smithy_runtime_api::client::http::SharedHttpClient,
    ) -> Self {
        // Environment-backed credential providers must be rebuilt with the
        // transport as well, including any previously loaded SDK config.
        let existing = if self.environment_config {
            self.known_config = Some(std::sync::Arc::new(tokio::sync::OnceCell::new()));
            None
        } else {
            self.client.get().map(|current| {
                Client::from_conf(
                    current
                        .config()
                        .to_builder()
                        .http_client(client.clone())
                        .build(),
                )
            })
        };
        self.client = std::sync::Arc::new(tokio::sync::OnceCell::new_with(existing));
        self.http_client = Some(client);
        self.credentials_http_client = Some(credentials_client);
        self.prepared_identity = None;
        self
    }
    /// Mark unsupported host settings so they fail explicitly when used.
    pub fn unsupported(mut self, reason: impl Into<String>) -> Self {
        self.unsupported = Some(reason.into());
        self.prepared_identity = None;
        self
    }
}
fn default_cache_retention() -> CacheRetention {
    if std::env::var("PI_CACHE_RETENTION").is_ok_and(|v| v == "long") {
        CacheRetention::Long
    } else {
        CacheRetention::Short
    }
}
#[derive(Default)]
struct ToolIds {
    mapped: BTreeMap<String, String>,
    used: BTreeSet<String>,
}
impl ToolIds {
    fn get(&mut self, original: &str) -> String {
        if let Some(id) = self.mapped.get(original) {
            return id.clone();
        }
        let base: String = original
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .take(64)
            .collect();
        let base = if base.is_empty() {
            "_".to_owned()
        } else {
            base
        };
        let mut candidate = base.clone();
        let mut suffix = 2_u64;
        while self.used.contains(&candidate) {
            let tail = format!("_{suffix}");
            candidate = format!("{}{}", &base[..base.len().min(64 - tail.len())], tail);
            suffix += 1;
        }
        self.used.insert(candidate.clone());
        self.mapped.insert(original.to_owned(), candidate.clone());
        candidate
    }
}
fn required_text(text: String) -> String {
    if text.trim().is_empty() {
        "<empty>".into()
    } else {
        text
    }
}
fn diagnostic(
    meta: &aws_smithy_types::error::metadata::ErrorMetadata,
    fallback_id: Option<&str>,
    secrets: &[String],
) -> String {
    let redact = |value: &str| {
        let mut value = value.to_owned();
        for secret in secrets {
            if !secret.is_empty() {
                value = value.replace(secret, "[REDACTED]");
            }
        }
        value
    };
    let clean = |s: &str| {
        s.chars()
            .filter(|c| !c.is_control())
            .take(512)
            .collect::<String>()
    };
    let mut parts = vec!["Bedrock error".to_owned()];
    if let Some(code) = meta.code().filter(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.#".contains(&b))
    }) {
        parts.push(code.into());
    }
    if let Some(message) = meta
        .message()
        .map(|message| clean(&redact(message)))
        .filter(|s| !s.trim().is_empty())
    {
        parts.push(message);
    }
    if let Some(id) = meta.request_id().or(fallback_id).filter(|s| {
        s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    }) {
        parts.push(format!("request id: {id}"));
    }
    redact(&parts.join(": "))
}
fn bearer_client(builder: aws_sdk_bedrockruntime::config::Builder, token: &str) -> Client {
    let config = builder
        .bearer_token(aws_sdk_bedrockruntime::config::Token::new(
            token.to_owned(),
            None,
        ))
        .auth_scheme_preference([
            aws_smithy_runtime_api::client::auth::http::HTTP_BEARER_AUTH_SCHEME_ID,
        ])
        .build();
    Client::from_conf(config)
}
fn image_block(image: pi_rs_core::ImageContent) -> Result<types::ImageBlock> {
    image.validate()?;
    let format = match image.mime_type.as_str() {
        "image/png" => types::ImageFormat::Png,
        "image/jpeg" => types::ImageFormat::Jpeg,
        "image/gif" => types::ImageFormat::Gif,
        "image/webp" => types::ImageFormat::Webp,
        _ => return Err(Error::Config("unsupported Bedrock image format".into())),
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(image.data)
        .map_err(build_error)?;
    types::ImageBlock::builder()
        .format(format)
        .source(types::ImageSource::Bytes(Blob::new(bytes)))
        .build()
        .map_err(build_error)
}
#[derive(Default)]
struct ReasoningBlock {
    text: String,
    signature: String,
    redacted: Vec<u8>,
}

fn push_replay_text(out: &mut Vec<ContentBlock>, pending_whitespace: &mut String, text: &str) {
    if text.trim().is_empty() {
        if let Some(ContentBlock::Text(previous)) = out.last_mut() {
            previous.push_str(text);
        } else {
            pending_whitespace.push_str(text);
        }
    } else {
        pending_whitespace.push_str(text);
        out.push(ContentBlock::Text(std::mem::take(pending_whitespace)));
    }
}

fn replay_blocks(
    replay: &ProviderReplay,
    model: &str,
    is_claude: bool,
    text: &str,
    calls: &[ToolCall],
    ids: &mut ToolIds,
) -> Option<Vec<ContentBlock>> {
    if !replay.supports_api(pi_rs_core::ModelApi::BedrockConverseStream)
        || replay.identity.provider != "amazon-bedrock"
        || replay.identity.model != model
    {
        return None;
    }
    let rendered: String = replay
        .blocks
        .iter()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect();
    if rendered != text {
        return None;
    }
    let replay_calls: Vec<_> = replay
        .blocks
        .iter()
        .filter(|b| b["type"] == "tool_use")
        .collect();
    if replay_calls.len() != calls.len()
        || replay_calls.iter().zip(calls).any(|(b, c)| {
            b["id"] != c.id
                || b["name"] != c.name
                || b.get("raw_arguments")
                    .and_then(Value::as_str)
                    .is_some_and(|raw| !matches_journal_arguments(raw, &c.arguments))
                || history_tool_input(&c.arguments) != b["input"]
        })
    {
        return None;
    }
    let mut out: Vec<ContentBlock> = Vec::new();
    let mut pending_whitespace = String::new();
    for b in &replay.blocks {
        if b.get("_pi_context_edit_api").is_some() {
            continue;
        }
        match b["type"].as_str()? {
            "text" => push_replay_text(&mut out, &mut pending_whitespace, b["text"].as_str()?),
            "tool_use" => {
                pending_whitespace.clear();
                out.push(ContentBlock::ToolUse(
                    types::ToolUseBlock::builder()
                        .tool_use_id(ids.get(b["id"].as_str()?))
                        .name(b["name"].as_str()?)
                        .input(doc(b["input"].clone()))
                        .build()
                        .ok()?,
                ));
            }
            "reasoning" => {
                let reasoning = if let Some(encoded) = b["redacted"].as_str() {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()?;
                    if bytes.is_empty() {
                        return None;
                    }
                    types::ReasoningContentBlock::RedactedContent(Blob::new(bytes))
                } else {
                    let text = b["text"].as_str()?;
                    let signature = b["signature"].as_str().filter(|s| !s.is_empty());
                    if text.is_empty() && (!is_claude || signature.is_none()) {
                        return None;
                    }
                    if is_claude && signature.is_none() {
                        push_replay_text(&mut out, &mut pending_whitespace, text);
                        continue;
                    }
                    let sig = if is_claude {
                        signature.map(str::to_owned)
                    } else {
                        None
                    };
                    types::ReasoningContentBlock::ReasoningText(
                        types::ReasoningTextBlock::builder()
                            .text(text)
                            .set_signature(sig)
                            .build()
                            .ok()?,
                    )
                };
                pending_whitespace.clear();
                out.push(ContentBlock::ReasoningContent(reasoning));
            }
            _ => return None,
        }
    }
    Some(out)
}
fn matches_journal_arguments(raw: &str, current: &str) -> bool {
    if raw == current {
        return true;
    }
    if serde_json::from_str::<Value>(raw).is_ok_and(|value| value.is_object()) {
        return false;
    }
    let projected = pi_rs_core::parse_tool_arguments(raw)
        .map_or_else(|_| "{}".to_owned(), |value| value.to_string());
    current == projected
}
fn history_tool_input(raw: &str) -> Value {
    pi_rs_core::parse_tool_arguments(raw).unwrap_or_else(|_| serde_json::json!({}))
}
fn doc(v: Value) -> Document {
    match v {
        Value::Null => Document::Null,
        Value::Bool(x) => Document::Bool(x),
        Value::String(x) => Document::String(x),
        Value::Number(x) => Document::Number(if let Some(n) = x.as_i64() {
            if n < 0 {
                Number::NegInt(n)
            } else {
                Number::PosInt(n as u64)
            }
        } else if let Some(n) = x.as_u64() {
            Number::PosInt(n)
        } else {
            Number::Float(x.as_f64().unwrap_or_default())
        }),
        Value::Array(xs) => Document::Array(xs.into_iter().map(doc).collect()),
        Value::Object(xs) => Document::Object(xs.into_iter().map(|(k, v)| (k, doc(v))).collect()),
    }
}
fn build_error(e: impl std::fmt::Display) -> Error {
    Error::Config(format!("invalid Bedrock request: {e}"))
}

// Pi's agent does not retry account or billing exhaustion even when a
// provider reports it through a normally retryable service exception.
// Classify the already redacted diagnostic and never echo the source text in
// the permanent-limit message.
fn permanent_bedrock_limit(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    const MARKERS: &[&str] = &[
        "gousagelimiterror",
        "freeusagelimiterror",
        "monthly usage limit reached",
        "available balance",
        "insufficient_quota",
        "out of budget",
        "quota exceeded",
        "billing",
        "subscription_sharing_usage_limit_exceeded",
        "subscription_sharing_workplace_individual_usage_limit_exceeded",
    ];
    if MARKERS.iter().any(|marker| message.contains(marker)) {
        return true;
    }
    message.match_indices("\"code\"").any(|(index, _)| {
        let rest = message[index + 6..].trim_start();
        let Some(rest) = rest.strip_prefix(':') else {
            return false;
        };
        let rest = rest
            .trim_start()
            .strip_prefix('\"')
            .unwrap_or(rest.trim_start());
        rest.strip_prefix("1308")
            .is_some_and(|tail| !tail.starts_with(|c: char| c.is_ascii_digit()))
    })
}

fn bedrock_service_error(message: String, retryable: bool) -> Error {
    if retryable && permanent_bedrock_limit(&message) {
        Error::Model("Bedrock account quota or billing limit reached".into())
    } else if retryable {
        Error::RetryableModel(message)
    } else {
        Error::Model(message)
    }
}

fn request_error(
    e: aws_sdk_bedrockruntime::error::SdkError<
        aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamError,
    >,
    secrets: &[String],
) -> Error {
    if let Some(service) = e.as_service_error() {
        let message = diagnostic(service.meta(), service.request_id(), secrets);
        let retryable = service.is_throttling_exception()
            || service.is_model_timeout_exception()
            || service.is_service_unavailable_exception()
            || service.is_internal_server_exception();
        bedrock_service_error(message, retryable)
    } else {
        match &e {
                    aws_sdk_bedrockruntime::error::SdkError::TimeoutError(_) => Error::RetryableModel("Bedrock transport timed out".into()),
                    aws_sdk_bedrockruntime::error::SdkError::DispatchFailure(failure) if failure.is_io() || failure.is_timeout() || matches!(failure.as_other(), Some(aws_smithy_types::retry::ErrorKind::TransientError | aws_smithy_types::retry::ErrorKind::ThrottlingError | aws_smithy_types::retry::ErrorKind::ServerError)) => Error::RetryableModel("Bedrock transport failed".into()),
                    aws_sdk_bedrockruntime::error::SdkError::DispatchFailure(_) | aws_sdk_bedrockruntime::error::SdkError::ConstructionFailure(_) => Error::Config("Bedrock could not resolve AWS credentials or configure the request; check AWS_PROFILE, credentials, and region".into()),
                    _ => Error::Model("Bedrock request failed".into()),
                }
    }
}

#[async_trait::async_trait]
impl Model for Bedrock {
    // Only a prepared, inspectable SDK configuration has a stable identity.
    async fn prepare_context<'a>(
        &'a self,
        _: &ModelRequest,
    ) -> Result<Option<std::sync::Arc<dyn Model + 'a>>> {
        Ok(self
            .prepare_bedrock()
            .await?
            .map(|model| std::sync::Arc::new(model) as std::sync::Arc<dyn Model + 'a>))
    }

    fn context_identity(&self) -> Option<String> {
        self.prepared_identity.clone()
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        fn text_tokens(text: &str) -> u64 {
            (text.encode_utf16().count() as u64).div_ceil(4)
        }
        match message {
            Message::System { content } if content.trim().is_empty() => 0,
            Message::User { content } => text_tokens(&required_text(content.clone())),
            Message::UserWithImages { content, images }
                if images.is_empty() && content.trim().is_empty() =>
            {
                text_tokens("<empty>")
            }
            Message::Tool {
                call_id,
                content,
                is_error,
                is_structured,
            } => {
                let (text, images) =
                    if pi_rs_core::tool_content_is_structured(content, *is_structured) {
                        pi_rs_core::extract_tool_images(content.clone())
                    } else {
                        (content.clone(), Vec::new())
                    };
                let text = if request.tools.is_empty() {
                    format!(
                        "Historical tool result ({}; {}):\n{}",
                        call_id,
                        if *is_error { "error" } else { "success" },
                        text
                    )
                } else {
                    let mut text = required_text(text);
                    if *is_error && self.is_claude(&request.model) && !images.is_empty() {
                        text.push_str("Images from the tool error result:");
                    }
                    text
                };
                pi_rs_core::estimate_message_tokens(&Message::UserWithImages {
                    content: text,
                    images,
                })
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                let tool_free = request.tools.is_empty();
                let mut visible = if content.trim().is_empty() {
                    String::new()
                } else {
                    content.clone()
                };
                for call in calls {
                    if tool_free {
                        visible.push_str(&format!(
                            "Historical tool call ({}; {}):\n{}",
                            call.id, call.name, call.arguments
                        ));
                    } else {
                        visible.push_str(&call.name);
                        visible.push_str(&history_tool_input(&call.arguments).to_string());
                    }
                }
                let base = text_tokens(&visible);
                let Some(replay) = replay.as_deref().filter(|_| !tool_free || calls.is_empty())
                else {
                    return base;
                };
                let claude = self.is_claude(&request.model);
                if replay_blocks(
                    replay,
                    &request.model,
                    claude,
                    content,
                    calls,
                    &mut ToolIds::default(),
                )
                .is_none()
                {
                    return base;
                }
                let mut ids = ToolIds::default();
                let blocks: Vec<_> = replay.blocks.iter().map(|block| match block["type"].as_str() {
                    Some("text") => serde_json::json!({"type":"text","text":block["text"]}),
                    Some("tool_use") => serde_json::json!({"type":"tool_use","id":ids.get(block["id"].as_str().unwrap_or_default()),"name":block["name"],"input":block["input"]}),
                    Some("reasoning") if block["redacted"].is_string() => serde_json::json!({"type":"reasoning","redacted":block["redacted"]}),
                    Some("reasoning") if claude && block["signature"].as_str().is_some_and(|signature| !signature.is_empty()) => serde_json::json!({"type":"reasoning","text":block["text"],"signature":block["signature"]}),
                    Some("reasoning") => serde_json::json!({"type":"reasoning","text":block["text"]}),
                    _ => serde_json::Value::Null,
                }).collect();
                pi_rs_core::estimate_replay_tokens(&blocks).max(base)
            }
            _ => pi_rs_core::estimate_message_tokens(message),
        }
    }

    async fn stream_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.clone().for_summary().stream(request).await
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        if let Some(reason) = &self.unsupported {
            return Err(Error::Config(reason.clone()));
        }
        if request.max_output_tokens == 0 || request.model.contains(":prompt/") {
            return Err(Error::Config("Bedrock requires a positive max output token limit and does not support managed prompt ARNs".into()));
        }
        let tool_free = request.tools.is_empty();
        let is_claude = self.is_claude(&request.model);
        let model_name = self
            .model_name
            .as_ref()
            .filter(|(id, _)| id == &request.model)
            .map(|(_, name)| name.as_str());
        let checkpoint = if options::supports_cache(&request.model, model_name, self.force_cache) {
            let retention = if self.cache_retention == CacheRetention::Long
                && !self
                    .long_cache_support
                    .unwrap_or_else(|| options::supports_long_cache(&request.model, model_name))
            {
                CacheRetention::Short
            } else {
                self.cache_retention
            };
            options::cache_point(retention)?
        } else {
            None
        };
        let mut ids = ToolIds::default();
        let mut system = Vec::new();
        let mut messages: Vec<types::Message> = Vec::new();
        let mut pending_error_images = Vec::new();
        for message in request.messages {
            if !matches!(&message, Message::Tool { .. })
                && let Some(last) = messages.last_mut()
            {
                last.content.append(&mut pending_error_images);
            }
            let (role, blocks) = match message {
                Message::System { content } => {
                    if !content.trim().is_empty() {
                        system.push(types::SystemContentBlock::Text(content));
                    }
                    continue;
                }
                Message::User { content } => (
                    ConversationRole::User,
                    vec![ContentBlock::Text(required_text(content))],
                ),
                Message::UserWithImages { content, images } => {
                    let mut blocks = Vec::new();
                    if !content.trim().is_empty() {
                        blocks.push(ContentBlock::Text(content));
                    }
                    for image in images {
                        blocks.push(ContentBlock::Image(image_block(image)?));
                    }
                    if blocks.is_empty() {
                        blocks.push(ContentBlock::Text("<empty>".into()));
                    }
                    (ConversationRole::User, blocks)
                }
                Message::Assistant {
                    content,
                    calls,
                    replay,
                } => {
                    if let Some(blocks) = replay
                        .as_deref()
                        .filter(|_| !tool_free || calls.is_empty())
                        .and_then(|r| {
                            replay_blocks(r, &request.model, is_claude, &content, &calls, &mut ids)
                        })
                    {
                        (ConversationRole::Assistant, blocks)
                    } else {
                        let mut blocks = Vec::new();
                        if !content.trim().is_empty() {
                            blocks.push(ContentBlock::Text(content));
                        }
                        for call in calls {
                            if tool_free {
                                blocks.push(ContentBlock::Text(format!(
                                    "Historical tool call ({}; {}):\n{}",
                                    call.id, call.name, call.arguments
                                )));
                                continue;
                            }
                            let input = history_tool_input(&call.arguments);
                            blocks.push(ContentBlock::ToolUse(
                                types::ToolUseBlock::builder()
                                    .tool_use_id(ids.get(&call.id))
                                    .name(call.name)
                                    .input(doc(input))
                                    .build()
                                    .map_err(build_error)?,
                            ));
                        }
                        (ConversationRole::Assistant, blocks)
                    }
                }
                Message::Tool {
                    call_id,
                    content,
                    is_error,
                    is_structured,
                } => {
                    if tool_free {
                        let (content, images) =
                            if pi_rs_core::tool_content_is_structured(&content, is_structured) {
                                pi_rs_core::extract_tool_images(content)
                            } else {
                                (content, Vec::new())
                            };
                        let mut blocks = vec![ContentBlock::Text(format!(
                            "Historical tool result ({}; {}):\n{}",
                            call_id,
                            if is_error { "error" } else { "success" },
                            content
                        ))];
                        for image in images {
                            blocks.push(ContentBlock::Image(image_block(image)?));
                        }
                        (ConversationRole::User, blocks)
                    } else {
                        let (content, images) =
                            if pi_rs_core::tool_content_is_structured(&content, is_structured) {
                                pi_rs_core::extract_tool_images(content)
                            } else {
                                (content, Vec::new())
                            };
                        let mut tool = types::ToolResultBlock::builder()
                            .tool_use_id(ids.get(&call_id))
                            .content(types::ToolResultContentBlock::Text(required_text(content)))
                            .status(if is_error {
                                types::ToolResultStatus::Error
                            } else {
                                types::ToolResultStatus::Success
                            });
                        let mut trailing = Vec::new();
                        let claude_error = is_error && is_claude;
                        for image in images {
                            let image = image_block(image)?;
                            if claude_error {
                                trailing.push(ContentBlock::Image(image));
                            } else {
                                tool = tool.content(types::ToolResultContentBlock::Image(image));
                            }
                        }
                        let blocks =
                            vec![ContentBlock::ToolResult(tool.build().map_err(build_error)?)];
                        if !trailing.is_empty() {
                            pending_error_images.push(ContentBlock::Text(
                                "Images from the tool error result:".into(),
                            ));
                            pending_error_images.extend(trailing);
                        }
                        (ConversationRole::User, blocks)
                    }
                }
            };
            if blocks.is_empty() {
                continue;
            }
            if let Some(last) = messages.last_mut()
                && last.role == role
            {
                last.content.extend(blocks);
                continue;
            }
            messages.push(
                types::Message::builder()
                    .role(role)
                    .set_content(Some(blocks))
                    .build()
                    .map_err(build_error)?,
            );
        }
        if let Some(last) = messages.last_mut() {
            last.content.append(&mut pending_error_images);
        }
        if let Some(point) = &checkpoint {
            if !system.is_empty() {
                system.push(types::SystemContentBlock::CachePoint(point.clone()));
            }
            if let Some(last) = messages.last_mut()
                && last.role == ConversationRole::User
            {
                last.content.push(ContentBlock::CachePoint(point.clone()));
            }
        }
        let tools: Vec<_> = request
            .tools
            .into_iter()
            .map(|t| {
                Ok(types::Tool::ToolSpec(
                    types::ToolSpecification::builder()
                        .name(t.name)
                        .description(t.description)
                        .input_schema(types::ToolInputSchema::Json(doc(t.parameters)))
                        .build()
                        .map_err(build_error)?,
                ))
            })
            .collect::<Result<_>>()?;
        let client = self
            .client
            .get_or_init(|| async {
                {
                    let config = if let Some(known) = &self.known_config {
                        known
                            .get_or_init(|| async { self.load_config().await })
                            .await
                            .clone()
                    } else {
                        aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
                    };
                    let config = if let Some(http_client) = &self.http_client {
                        config.to_builder().http_client(http_client.clone()).build()
                    } else {
                        config
                    };
                    if let Some(token) = &self.bearer {
                        bearer_client(
                            aws_sdk_bedrockruntime::config::Builder::from(&config),
                            token,
                        )
                    } else if self.force_sigv4 {
                        use aws_smithy_runtime_api::client::auth::AuthSchemeId;
                        Client::from_conf(
                            aws_sdk_bedrockruntime::config::Builder::from(&config)
                                .auth_scheme_preference([AuthSchemeId::new("sigv4")])
                                .build(),
                        )
                    } else {
                        Client::new(&config)
                    }
                }
            })
            .await;
        if client.config().region().is_none() {
            return Err(Error::Config("Bedrock requires an AWS region; set AWS_REGION or configure a region in the selected AWS profile".into()));
        }
        let reasoning = if self.summary
            && options::requires_thinking(&request.model, model_name)
            && !(tool_free && options::sonnet_55(&request.model, model_name))
        {
            Some(ReasoningLevel::Low)
        } else {
            self.reasoning
        };
        let fields = options::reasoning_fields(
            &request.model,
            model_name,
            reasoning,
            self.mapped_effort.as_deref(),
            request.max_output_tokens,
            options::ReasoningContext {
                region: client.config().region().map(|region| region.as_ref()),
                tool_free,
                budgets: &self.thinking_budgets,
                configured_budgets: self.configured_thinking_budgets.as_ref(),
                display: self.thinking_display,
                interleaved: self.interleaved_thinking,
            },
        )?;
        let requested_model = request.model.clone();
        let mut command = client
            .converse_stream()
            .model_id(request.model)
            .set_messages(Some(messages))
            .set_system((!system.is_empty()).then_some(system))
            .inference_config(
                types::InferenceConfiguration::builder()
                    .max_tokens(i32::try_from(request.max_output_tokens).map_err(build_error)?)
                    .build(),
            );
        if let Some(fields) = fields {
            command = command.additional_model_request_fields(doc(fields));
        }
        if !tools.is_empty() {
            command = command.tool_config(
                types::ToolConfiguration::builder()
                    .set_tools(Some(tools))
                    .build()
                    .map_err(build_error)?,
            );
        }
        let headers = self.headers.clone();
        let mut secrets: Vec<_> = self
            .headers
            .values()
            .flatten()
            .filter(|value| !value.is_empty())
            .cloned()
            .collect();
        if let Some(token) = self.bearer.as_ref().filter(|token| !token.is_empty()) {
            secrets.push(token.clone());
        }
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        use tracing::instrument::WithSubscriber;
        let has_secrets = !secrets.is_empty();
        let mut request = command.customize();
        if !headers.is_empty() {
            request = request.mutate_request(move |request| headers::apply(request, &headers));
        }
        let request = request.send();
        let guarded_dispatch = has_secrets.then(|| {
            let dispatch = tracing::dispatcher::get_default(Clone::clone);
            tracing::Dispatch::new(headers::SigningTraceGuard(dispatch))
        });
        let response = if let Some(dispatch) = &guarded_dispatch {
            request.with_subscriber(dispatch.clone()).await
        } else {
            request.await
        };
        let mut response = response.map_err(|error| request_error(error, &secrets))?;
        let request_id = response.request_id().map(str::to_owned);
        Ok(Box::pin(async_stream::try_stream! {
            let mut calls: BTreeMap<i32, ToolCall> = BTreeMap::new();
            let mut usage=None; let mut stop=None; let mut cache_read=None; let mut cache_write=None; let mut cache_write_1h=None; let mut received=0usize;
            let mut open_tools = std::collections::BTreeSet::new();
            let mut text_blocks: BTreeMap<i32, String> = BTreeMap::new();
            let mut reasoning: BTreeMap<i32, ReasoningBlock> = BTreeMap::new();
            let mut stopped_blocks = BTreeSet::new();
            loop {
                let receive = response.stream.recv();
                let next = if let Some(dispatch) = &guarded_dispatch {
                    receive.with_subscriber(dispatch.clone()).await
                } else {
                    receive.await
                };
                let Some(event) = next.map_err(|e| {
                if let Some(service) = e.as_service_error() {
                    let mut message=diagnostic(service.meta(), request_id.as_deref(), &secrets);
                    if service.meta().code().is_none() {
                        let code = if service.is_validation_exception() { Some("ValidationException") } else if service.is_throttling_exception() { Some("ThrottlingException") } else if service.is_service_unavailable_exception() { Some("ServiceUnavailableException") } else if service.is_internal_server_exception() { Some("InternalServerException") } else if service.is_model_stream_error_exception() { Some("ModelStreamErrorException") } else { None };
                        if let Some(code) = code { message.insert_str(0, &format!("{code}: ")); }
                    }
                    let retryable = service.is_throttling_exception() || service.is_service_unavailable_exception() || service.is_internal_server_exception() || service.is_model_stream_error_exception();
                    bedrock_service_error(message, retryable)
                } else { Error::RetryableModel("Bedrock stream transport failed".into()) }
            })? else { break };
                match event {
                    ConverseStreamOutput::ContentBlockStart(event) => if let Some(ContentBlockStart::ToolUse(tool))=event.start {
                        let index=event.content_block_index;
                        if index < 0 || calls.contains_key(&index) || text_blocks.contains_key(&index) || reasoning.contains_key(&index) || stopped_blocks.contains(&index) { Err(Error::Model("invalid Bedrock tool index".into()))?; }
                        open_tools.insert(index);
                        let call=ToolCall {id:tool.tool_use_id().into(), name:tool.name().into(), arguments:String::new()};
                        yield ModelEvent::ToolCallDelta(ToolCallDelta {index:index as u64,id:call.id.clone(),name:call.name.clone(),arguments:String::new()}); calls.insert(index,call);
                    },
                    ConverseStreamOutput::ContentBlockDelta(event) => match event.delta {
                        Some(ContentBlockDelta::Text(text)) => { let index=event.content_block_index; if index < 0 || stopped_blocks.contains(&index) || reasoning.contains_key(&index) || calls.contains_key(&index) { Err(Error::Model("invalid Bedrock text block".into()))?; } received=received.saturating_add(text.len()); if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; } text_blocks.entry(index).or_default().push_str(&text); yield ModelEvent::Text(text); },
                        Some(ContentBlockDelta::ReasoningContent(delta)) => {
                            let index=event.content_block_index;
                            if index < 0 || stopped_blocks.contains(&index) || text_blocks.contains_key(&index) || calls.contains_key(&index) { Err(Error::Model("invalid Bedrock reasoning block".into()))?; }
                            let block=reasoning.entry(index).or_default();
                            match delta {
                                types::ReasoningContentBlockDelta::Text(text) => { if !block.redacted.is_empty() { Err(Error::Model("mixed Bedrock reasoning block".into()))?; } received=received.saturating_add(text.len()); block.text.push_str(&text); yield ModelEvent::Reasoning(text); },
                                types::ReasoningContentBlockDelta::Signature(signature) => { if !block.redacted.is_empty() { Err(Error::Model("mixed Bedrock reasoning block".into()))?; } received=received.saturating_add(signature.len()); block.signature.push_str(&signature); },
                                types::ReasoningContentBlockDelta::RedactedContent(blob) => { if !block.text.is_empty() || !block.signature.is_empty() { Err(Error::Model("mixed Bedrock reasoning block".into()))?; } received=received.saturating_add(blob.as_ref().len()); block.redacted.extend_from_slice(blob.as_ref()); },
                                _ => Err(Error::Model("unsupported Bedrock reasoning delta".into()))?,
                            }
                            if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; }
                        },
                        Some(ContentBlockDelta::ToolUse(delta)) => { let index=event.content_block_index; if !open_tools.contains(&index) { Err(Error::Model("Bedrock tool delta without open block".into()))?; } received=received.saturating_add(delta.input().len()); if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; } let call=calls.get_mut(&index).ok_or_else(|| Error::Model("Bedrock tool delta without start".into()))?; call.arguments.push_str(delta.input()); yield ModelEvent::ToolCallDelta(ToolCallDelta {index:index as u64,id:String::new(),name:String::new(),arguments:delta.input().into()}); },
                        _ => Err(Error::Model("unsupported Bedrock content delta".into()))?,
                    },
                    ConverseStreamOutput::MessageStop(event) => { let reason=event.stop_reason().as_str().to_owned(); if !matches!(reason.as_str(),"end_turn"|"stop_sequence"|"tool_use"|"max_tokens"|"model_context_window_exceeded") { Err(Error::Model(format!("Bedrock stopped with: {reason}")))?; } stop=Some(reason); },
                    ConverseStreamOutput::Metadata(event) => if let Some(u)=event.usage {
                        cache_read=u.cache_read_input_tokens().map(|n| n.max(0) as u64); cache_write=u.cache_write_input_tokens().map(|n| n.max(0) as u64);
                        cache_write_1h = u.cache_details.as_ref().map(|details| details.iter().filter(|d| d.ttl().as_str()=="1h").fold(0u64, |sum,d|sum.saturating_add(d.input_tokens().max(0) as u64)));
                        usage=Some(Usage {input_tokens:(u.input_tokens().max(0) as u64).saturating_add(cache_read.unwrap_or(0)).saturating_add(cache_write.unwrap_or(0)),output_tokens:u.output_tokens().max(0) as u64});
                    },
                    ConverseStreamOutput::ContentBlockStop(event) => { open_tools.remove(&event.content_block_index); stopped_blocks.insert(event.content_block_index); },
                    ConverseStreamOutput::MessageStart(_) => {},
                    _ => Err(Error::Model("unsupported Bedrock stream event".into()))?,
                }
            }
            let reason=stop.ok_or_else(|| Error::RetryableModel("Bedrock stream ended before messageStop".into()))?;
            if !open_tools.is_empty() || (!calls.is_empty() && reason != "tool_use") || (calls.is_empty() && reason == "tool_use") { Err(Error::Model("incomplete Bedrock tool response".into()))?; }
            if reasoning.keys().any(|i| !stopped_blocks.contains(i)) { Err(Error::Model("incomplete Bedrock reasoning block".into()))?; }
            let reasoning_text: String = reasoning.values().map(|b| b.text.as_str()).collect();
            let mut ordered = BTreeMap::new();
            for (i,text) in text_blocks { ordered.insert(i, serde_json::json!({"type":"text", "text":text})); }
            for (i,call) in &calls { let input=history_tool_input(&call.arguments); ordered.insert(*i, serde_json::json!({"type":"tool_use", "id":call.id,"name":call.name,"input":input,"raw_arguments":call.arguments})); }
            for (i,block) in &reasoning {
                let value=if block.redacted.is_empty() { serde_json::json!({"type":"reasoning", "text":block.text, "signature":block.signature}) } else { serde_json::json!({"type":"reasoning", "redacted":base64::engine::general_purpose::STANDARD.encode(&block.redacted)}) };
                ordered.insert(*i,value);
            }
            let replay=(!reasoning.is_empty() && reason != "max_tokens" && reason != "model_context_window_exceeded").then(|| Box::new(ProviderReplay { identity:ResolvedModelIdentity {provider:"amazon-bedrock".into(),model:requested_model.clone()},blocks:ordered.into_values().collect() }));
            let finish_reason = match reason.as_str() { "end_turn" | "stop_sequence" => "stop", "tool_use" => "tool_calls", "max_tokens" | "model_context_window_exceeded" => "length", _ => unreachable!() };
            yield ModelEvent::Metadata(ModelMetadata {finish_reason:Some(finish_reason.into()), cached_input_tokens:cache_read, cache_creation_input_tokens:cache_write, cache_write_1h_tokens:cache_write_1h, provider_api:Some(pi_rs_core::ModelApi::BedrockConverseStream), replay_identity:Some(ResolvedModelIdentity { provider:"amazon-bedrock".into(), model:requested_model }), reasoning:(!reasoning_text.is_empty()).then_some(reasoning_text), replay, ..Default::default()});
            yield ModelEvent::Complete { calls:calls.into_values().collect(), usage };
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quota_classification_keeps_transient_and_secret_safe() {
        for message in [
            "Insufficient_Quota",
            "Monthly usage limit reached",
            "Billing issue",
            r#"{"code":"1308"}"#,
        ] {
            assert!(
                matches!(bedrock_service_error(message.into(), true), Error::Model(text) if text == "Bedrock account quota or billing limit reached")
            );
        }
        for message in ["rate limit reached", r#"{"code":13080}"#] {
            assert!(matches!(
                bedrock_service_error(message.into(), true),
                Error::RetryableModel(_)
            ));
        }
        let metadata = aws_smithy_types::error::metadata::ErrorMetadata::builder()
            .message("quota exceeded secret-token")
            .build();
        let redacted = diagnostic(&metadata, None, &["secret-token".into()]);
        let error = bedrock_service_error(redacted, true);
        assert!(!error.to_string().contains("secret-token"));
        assert!(matches!(
            bedrock_service_error("validation failed".into(), false),
            Error::Model(_)
        ));
    }

    #[test]
    fn omitted_thinking_replays_empty_text_with_signature() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "anthropic.claude-opus-4-8".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"reasoning", "text":"", "signature":"signed"}),
                serde_json::json!({"type":"text", "text":"answer"}),
            ],
        };
        let blocks = replay_blocks(
            &replay,
            "anthropic.claude-opus-4-8",
            true,
            "answer",
            &[],
            &mut ToolIds::default(),
        )
        .unwrap();
        assert!(
            matches!(&blocks[0], ContentBlock::ReasoningContent(types::ReasoningContentBlock::ReasoningText(block)) if block.text() == "" && block.signature() == Some("signed"))
        );
        assert!(matches!(&blocks[1], ContentBlock::Text(text) if text == "answer"));
    }
    #[tokio::test]
    async fn prepared_identity_changes_with_effective_custom_headers() {
        let config = aws_types::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new("us-east-1"))
            .endpoint_url("https://example.invalid")
            .credentials_provider(
                aws_credential_types::provider::SharedCredentialsProvider::new(
                    aws_credential_types::Credentials::new("access", "secret", None, None, "test"),
                ),
            )
            .build();
        let identity = |value: &'static str, reserved: &'static str| {
            let config = config.clone();
            async move {
                let mut headers = http::HeaderMap::new();
                headers.insert("x-context", http::HeaderValue::from_static(value));
                headers.insert("authorization", http::HeaderValue::from_static(reserved));
                Bedrock::from_sdk_config(config)
                    .with_headers(headers)
                    .unwrap()
                    .prepare_bedrock()
                    .await
                    .unwrap()
                    .unwrap()
                    .context_identity()
                    .unwrap()
            }
        };
        let a = identity("private-a", "ignored-a").await;
        let same = identity("private-a", "ignored-b").await;
        let b = identity("private-b", "ignored-a").await;
        assert_eq!(a, same);
        assert_ne!(a, b);
        let mut repeated = http::HeaderMap::new();
        repeated.append("x-context", http::HeaderValue::from_static("private-a"));
        repeated.append("x-context", http::HeaderValue::from_static("private-b"));
        let repeated_identity = Bedrock::from_sdk_config(config)
            .with_headers(repeated)
            .unwrap()
            .prepare_bedrock()
            .await
            .unwrap()
            .unwrap()
            .context_identity()
            .unwrap();
        assert_ne!(a, repeated_identity);
        assert!(!a.contains("private-a"));
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn selected_profile_wins_over_ambient_keys_without_mutating_environment() {
        use aws_credential_types::provider::ProvideCredentials;
        if std::env::var_os("PI_RS_PROFILE_TEST_CHILD").is_some() {
            let before = std::env::var("AWS_PROFILE").unwrap();
            let model = Bedrock::from_env().with_profile("selected".into());
            let config = model.load_config().await;
            let credentials = config
                .credentials_provider()
                .unwrap()
                .provide_credentials()
                .await
                .unwrap();
            assert_eq!(credentials.access_key_id(), "SELECTEDKEY");
            assert_eq!(config.region().unwrap().as_ref(), "us-west-2");

            // The last selection must win in either order, even after a
            // previously selected profile has populated its SDK config.
            let _ = model.prepare_bedrock().await.unwrap();
            let chain = model.with_credential_chain();
            let chain_config = chain.load_config().await;
            assert_eq!(chain_config.region().unwrap().as_ref(), "ap-southeast-1");
            let credentials = chain_config
                .credentials_provider()
                .unwrap()
                .provide_credentials()
                .await
                .unwrap();
            assert_eq!(credentials.access_key_id(), "AMBIENTKEY");
            let prepared = chain.prepare_bedrock().await.unwrap().unwrap();
            let fresh = Bedrock::from_env()
                .with_credential_chain()
                .prepare_bedrock()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(prepared.context_identity(), fresh.context_identity());
            assert!(prepared.context_identity().is_some());
            let mut custom = http::HeaderMap::new();
            custom.append("x-profile-route", http::HeaderValue::from_static("first"));
            custom.append("x-profile-route", http::HeaderValue::from_static("second"));
            let profile = chain
                .with_headers(custom)
                .unwrap()
                .with_profile("selected".into());
            let prepared = profile.prepare_bedrock().await.unwrap().unwrap();
            assert_eq!(prepared.headers["x-profile-route"], ["first", "second"]);
            let credentials = profile
                .load_config()
                .await
                .credentials_provider()
                .unwrap()
                .provide_credentials()
                .await
                .unwrap();
            assert_eq!(credentials.access_key_id(), "SELECTEDKEY");
            assert_eq!(std::env::var("AWS_PROFILE").unwrap(), before);
            assert_eq!(std::env::var("AWS_ACCESS_KEY_ID").unwrap(), "AMBIENTKEY");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials");
        let config = dir.path().join("config");
        std::fs::write(
            &credentials,
            "[selected]\naws_access_key_id=SELECTEDKEY\naws_secret_access_key=SELECTEDSECRET\n",
        )
        .unwrap();
        std::fs::write(
            &config,
            "[profile selected]\nregion=us-west-2\n[profile ambient]\nregion=ap-southeast-1\n",
        )
        .unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::selected_profile_wins_over_ambient_keys_without_mutating_environment",
            ])
            .env("PI_RS_PROFILE_TEST_CHILD", "1")
            .env("AWS_PROFILE", "ambient")
            .env("AWS_ACCESS_KEY_ID", "AMBIENTKEY")
            .env("AWS_SECRET_ACCESS_KEY", "AMBIENTSECRET")
            .env("AWS_SHARED_CREDENTIALS_FILE", credentials)
            .env("AWS_CONFIG_FILE", config)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("AWS_REGION")
            .env_remove("AWS_DEFAULT_REGION")
            .env_remove("AWS_BEARER_TOKEN_BEDROCK")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(any(unix, windows))]
    fn configure_sts_fixture_socket(
        socket: &std::net::TcpStream,
        timeout: std::time::Duration,
    ) -> std::io::Result<()> {
        // Accepted sockets may inherit the listener's nonblocking mode on macOS.
        socket.set_nonblocking(false)?;
        socket.set_read_timeout(Some(timeout))?;
        socket.set_write_timeout(Some(timeout))
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn sts_fixture_socket_waits_for_data_with_a_timeout() {
        use std::io::{Read, Write};
        use std::time::Duration;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_nonblocking(true).unwrap();
        configure_sts_fixture_socket(&socket, Duration::from_millis(200)).unwrap();
        let (release, hold) = std::sync::mpsc::channel::<()>();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            client.write_all(b"x").unwrap();
            // Keep the connection open until the bounded read times out.
            let _ = hold.recv_timeout(Duration::from_secs(5));
        });
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        assert_eq!(&byte, b"x");
        let error = socket.read(&mut byte).unwrap_err();
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ));
        release.send(()).unwrap();
        writer.join().unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn selected_assume_role_profile_uses_its_region_for_sts() {
        use aws_credential_types::provider::ProvideCredentials;
        if std::env::var_os("PI_RS_ASSUME_ROLE_TEST_CHILD").is_some() {
            let config = Bedrock::from_env()
                .with_profile("selected".into())
                .load_config()
                .await;
            assert_eq!(config.region().unwrap().as_ref(), "us-west-2");
            let credentials = config
                .credentials_provider()
                .unwrap()
                .provide_credentials()
                .await
                .unwrap();
            assert_eq!(credentials.access_key_id(), "ASSUMEDKEY");
            assert_eq!(std::env::var("AWS_PROFILE").unwrap(), "ambient");
            return;
        }
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials");
        let config = dir.path().join("config");
        std::fs::write(
            &credentials,
            "[source]\naws_access_key_id=SOURCEKEY\naws_secret_access_key=SOURCESECRET\n",
        )
        .unwrap();
        std::fs::write(&config, "[profile selected]\nregion=us-west-2\nrole_arn=arn:aws:iam::123456789012:role/test\nsource_profile=source\n").unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::selected_assume_role_profile_uses_its_region_for_sts",
            ])
            .env("PI_RS_ASSUME_ROLE_TEST_CHILD", "1")
            .env("AWS_PROFILE", "ambient")
            .env("AWS_ACCESS_KEY_ID", "AMBIENTKEY")
            .env("AWS_SECRET_ACCESS_KEY", "AMBIENTSECRET")
            .env("AWS_SHARED_CREDENTIALS_FILE", credentials)
            .env("AWS_CONFIG_FILE", config)
            .env("AWS_ENDPOINT_URL_STS", endpoint)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("AWS_REGION")
            .env_remove("AWS_DEFAULT_REGION")
            .env_remove("AWS_ENDPOINT_URL")
            .env_remove("AWS_BEARER_TOKEN_BEDROCK")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if std::time::Instant::now() >= deadline || child.try_wait().unwrap().is_some()
                    {
                        let _ = child.kill();
                        let output = child.wait_with_output().unwrap();
                        panic!(
                            "STS was not called: {} {}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr)
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => panic!("STS listener: {e}"),
            }
        };
        configure_sts_fixture_socket(&socket, std::time::Duration::from_secs(5)).unwrap();
        let mut data = Vec::new();
        let mut buffer = [0; 4096];
        let (header_end, length) = loop {
            let n = socket.read(&mut buffer).unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buffer[..n]);
            if let Some(pos) = data.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..pos]);
                let len = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (pos + 4, len);
            }
        };
        while data.len() < header_end + length {
            let n = socket.read(&mut buffer).unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buffer[..n]);
        }
        let request = String::from_utf8_lossy(&data);
        assert!(
            request.contains("Credential=SOURCEKEY/")
                && request.contains("/us-west-2/sts/aws4_request"),
            "{request}"
        );
        assert!(request.contains("Action=AssumeRole"));
        let body = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><AssumeRoleResult><Credentials><AccessKeyId>ASSUMEDKEY</AccessKeyId><SecretAccessKey>ASSUMEDSECRET</SecretAccessKey><SessionToken>SESSION</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>fixture</RequestId></ResponseMetadata></AssumeRoleResponse>";
        write!(socket, "HTTP/1.1 200 OK\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn environment_auth_produces_a_prepared_identity() {
        if std::env::var_os("PI_RS_BEDROCK_AUTH_CHILD").is_some() {
            let prepared = Bedrock::from_env()
                .prepare_bedrock()
                .await
                .unwrap()
                .unwrap();
            assert!(prepared.context_identity().is_some());
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        for bearer in [false, true] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "tests::environment_auth_produces_a_prepared_identity",
                ])
                .env("PI_RS_BEDROCK_AUTH_CHILD", "1")
                .env("PI_RS_HOME", dir.path())
                .env("AWS_REGION", "us-east-1")
                .env("AWS_DEFAULT_REGION", "us-east-1")
                .env("AWS_ACCESS_KEY_ID", "fixture-access")
                .env("AWS_SECRET_ACCESS_KEY", "fixture-secret")
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env("AWS_CONFIG_FILE", dir.path().join("missing-config"))
                .env(
                    "AWS_SHARED_CREDENTIALS_FILE",
                    dir.path().join("missing-credentials"),
                )
                .env_remove("AWS_PROFILE")
                .env_remove("AWS_SESSION_TOKEN")
                .env_remove("AWS_ENDPOINT_URL")
                .env_remove("AWS_ENDPOINT_URL_BEDROCK_RUNTIME");
            if bearer {
                command.env("AWS_BEARER_TOKEN_BEDROCK", "fixture-bearer");
            } else {
                command.env_remove("AWS_BEARER_TOKEN_BEDROCK");
            }
            let result = command.output().unwrap();
            assert!(
                result.status.success(),
                "child failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
        }
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn inspectable_sdk_config_produces_route_and_auth_bound_identity() {
        use aws_credential_types::provider::SharedCredentialsProvider;
        let make = |region: &str, endpoint: &str, access: &str, secret: &str| {
            let credentials =
                aws_credential_types::Credentials::new(access, secret, None, None, "fixture");
            aws_types::SdkConfig::builder()
                .behavior_version(aws_config::BehaviorVersion::latest())
                .region(aws_types::region::Region::new(region.to_owned()))
                .endpoint_url(endpoint)
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
        };
        let prepare = |config| async move {
            Bedrock::from_sdk_config(config)
                .prepare_bedrock()
                .await
                .unwrap()
                .unwrap()
                .context_identity()
                .unwrap()
        };
        let first = prepare(make("us-east-1", "https://one.example", "access", "secret")).await;
        let same = prepare(make("us-east-1", "https://one.example", "access", "secret")).await;
        let region = prepare(make("us-west-2", "https://one.example", "access", "secret")).await;
        let endpoint = prepare(make("us-east-1", "https://two.example", "access", "secret")).await;
        let credential = prepare(make("us-east-1", "https://one.example", "other", "secret")).await;
        assert_eq!(first, same);
        assert_ne!(first, region);
        assert_ne!(first, endpoint);
        assert_ne!(first, credential);
        assert!(!first.contains("secret"));
        let config = make("us-east-1", "https://one.example", "access", "secret");
        let bearer = Bedrock::from_sdk_config(config.clone())
            .with_bearer_token("token-a".into())
            .unwrap()
            .prepare_bedrock()
            .await
            .unwrap()
            .unwrap();
        let same_bearer = Bedrock::from_sdk_config(config.clone())
            .with_bearer_token("token-a".into())
            .unwrap()
            .prepare_bedrock()
            .await
            .unwrap()
            .unwrap();
        let other_bearer = Bedrock::from_sdk_config(config)
            .with_bearer_token("token-b".into())
            .unwrap()
            .prepare_bedrock()
            .await
            .unwrap()
            .unwrap();
        assert!(bearer.context_identity().is_some());
        assert_eq!(bearer.context_identity(), same_bearer.context_identity());
        assert_ne!(bearer.context_identity(), other_bearer.context_identity());
        assert_ne!(Some(first), bearer.context_identity());
        let token_provider = aws_credential_types::provider::token::SharedTokenProvider::new(
            aws_credential_types::Token::new("unresolved-token", None),
        );
        let config = make("us-east-1", "https://one.example", "access", "secret")
            .to_builder()
            .token_provider(token_provider)
            .build();
        let mut untrusted = Bedrock::from_sdk_config(config);
        assert!(untrusted.prepare_bedrock().await.unwrap().is_none());
        // The default environment loader always installs an SSO token provider;
        // SDK default auth still selects SigV4 first when credentials exist.
        untrusted.environment_config = true;
        assert!(
            untrusted
                .prepare_bedrock()
                .await
                .unwrap()
                .unwrap()
                .context_identity()
                .is_some()
        );
        let opaque = Bedrock::new(Client::from_conf(
            aws_sdk_bedrockruntime::Config::builder()
                .behavior_version_latest()
                .region(aws_types::region::Region::new("us-east-1"))
                .build(),
        ))
        .with_bearer_token("token-a".into())
        .unwrap();
        assert!(opaque.prepare_bedrock().await.unwrap().is_none());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn reasoning_and_cache_options_are_bound_to_prepared_identity() {
        use aws_credential_types::provider::SharedCredentialsProvider;
        let credentials =
            aws_credential_types::Credentials::new("access", "secret", None, None, "fixture");
        let config = aws_types::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .build();
        let base = Bedrock::from_sdk_config(config)
            .with_cache_retention(CacheRetention::Short)
            .with_force_cache(false);
        let expected = base
            .prepare_bedrock()
            .await
            .unwrap()
            .unwrap()
            .context_identity()
            .unwrap();
        let options = [
            base.clone().with_reasoning(Some("high"), None).unwrap(),
            base.clone().with_reasoning(None, Some("high")).unwrap(),
            base.clone().with_thinking_budgets(ThinkingBudgets {
                low: Some(1500),
                ..Default::default()
            }),
            base.clone()
                .with_thinking_budget_settings(serde_json::json!({"low": 1500})),
            base.clone().with_thinking_display(ThinkingDisplay::Omitted),
            base.clone().with_interleaved_thinking(false),
            base.clone().with_cache_retention(CacheRetention::None),
            base.clone().with_long_cache_support(Some(false)),
            base.clone().with_force_cache(true),
            base.clone().for_summary(),
        ];
        for option in options {
            let prepared = option.prepare_bedrock().await.unwrap().unwrap();
            assert_ne!(prepared.context_identity().unwrap(), expected);
            assert!(
                prepared
                    .with_cache_retention(CacheRetention::Long)
                    .context_identity()
                    .is_none()
            );
        }
    }

    #[test]
    fn capacity_estimate_matches_multimodal_tool_and_reasoning_replay() {
        let model = Bedrock::from_env();
        assert!(model.context_identity().is_none());
        let mut request = ModelRequest {
            model: "anthropic.claude-test".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 1,
        };
        let tool = |size, is_error| {
            Message::Tool { call_id: "id".into(), content: serde_json::json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(size)}]}}).to_string(), is_error, is_structured: Some(true) }
        };
        assert_eq!(
            model.estimate_message_tokens(&request, &tool(4, false)),
            model.estimate_message_tokens(&request, &tool(20000, false))
        );
        request.tools.push(pi_rs_core::ToolSpec {
            name: "lookup".into(),
            description: "".into(),
            parameters: serde_json::json!({}),
        });
        let normal = model.estimate_message_tokens(&request, &tool(4, false));
        assert_eq!(
            normal,
            model.estimate_message_tokens(&request, &tool(20000, false))
        );
        assert!(model.estimate_message_tokens(&request, &tool(4, true)) > normal);
        let mut plain = tool(20000, false);
        if let Message::Tool { is_structured, .. } = &mut plain {
            *is_structured = Some(false);
        }
        assert!(model.estimate_message_tokens(&request, &plain) > 4000);
        let user_image = Message::UserWithImages {
            content: "look".into(),
            images: vec![pi_rs_core::ImageContent {
                mime_type: "image/png".into(),
                data: "A".repeat(20000),
            }],
        };
        assert_eq!(model.estimate_message_tokens(&request, &user_image), 1201);
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: request.model.clone(),
            },
            blocks: vec![
                serde_json::json!({"type":"reasoning","text":"x".repeat(8000),"signature":"sig"}),
                serde_json::json!({"type":"text","text":"ok"}),
            ],
        };
        let assistant = Message::Assistant {
            content: "ok".into(),
            calls: vec![],
            replay: Some(Box::new(replay)),
        };
        let replay_cost = model.estimate_message_tokens(&request, &assistant);
        assert!(replay_cost > 1900);
        // Reasoning and cache settings add request controls/checkpoints, not
        // additional replay text or base64-sized image content.
        request.messages = vec![user_image.clone(), assistant.clone(), tool(20000, false)];
        let estimate = model.estimate_request_tokens(&request);
        for configured in [
            model.clone().with_reasoning(Some("high"), None).unwrap(),
            model.clone().with_cache_retention(CacheRetention::Long),
            model.clone().with_force_cache(true),
        ] {
            assert_eq!(configured.estimate_request_tokens(&request), estimate);
            assert_eq!(
                configured.estimate_message_tokens(&request, &assistant),
                replay_cost
            );
        }
        let mut with_unused_metadata = assistant.clone();
        if let Message::Assistant {
            replay: Some(replay),
            ..
        } = &mut with_unused_metadata
        {
            replay.blocks[0]["ignored"] = serde_json::json!("x".repeat(30000));
        }
        assert_eq!(
            model.estimate_message_tokens(&request, &with_unused_metadata),
            replay_cost
        );
        let mut changed = assistant.clone();
        if let Message::Assistant {
            replay: Some(replay),
            ..
        } = &mut changed
        {
            replay.identity.model = "other".into();
        }
        assert_eq!(model.estimate_message_tokens(&request, &changed), 1);
        let call = ToolCall {
            id: "id".into(),
            name: "lookup".into(),
            arguments: "{\"x\":1}".into(),
        };
        let mut with_call = assistant.clone();
        if let Message::Assistant {
            calls,
            replay: Some(replay),
            ..
        } = &mut with_call
        {
            calls.push(call);
            replay.blocks.push(
                serde_json::json!({"type":"tool_use","id":"id","name":"lookup","input":{"x":1}}),
            );
        }
        assert!(model.estimate_message_tokens(&request, &with_call) > 1900);
        request.tools.clear();
        assert!(model.estimate_message_tokens(&request, &with_call) < 100);
    }

    #[test]
    fn edited_replay_omits_blank_text_without_losing_signed_reasoning() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "anthropic.claude-test".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"text","text":""}),
                serde_json::json!({"type":"text","text":"first"}),
                serde_json::json!({"type":"text","text":" \n"}),
                serde_json::json!({"type":"text","text":"second"}),
                serde_json::json!({"type":"reasoning","text":"private","signature":"sig"}),
                serde_json::json!({"type":"text","text":"   "}),
                serde_json::json!({"type":"tool_use","id":"id","name":"lookup","input":{}}),
                serde_json::json!({"_pi_context_edit_api":"bedrock-converse-stream"}),
            ],
        };
        let call = ToolCall {
            id: "id".into(),
            name: "lookup".into(),
            arguments: "{}".into(),
        };
        let blocks = replay_blocks(
            &replay,
            "anthropic.claude-test",
            true,
            "first \nsecond   ",
            &[call],
            &mut ToolIds::default(),
        )
        .unwrap();
        assert!(matches!(&blocks[0], ContentBlock::Text(text) if text == "first \n"));
        assert!(matches!(&blocks[1], ContentBlock::Text(text) if text == "second"));
        assert!(
            matches!(&blocks[2], ContentBlock::ReasoningContent(types::ReasoningContentBlock::ReasoningText(text)) if text.signature() == Some("sig"))
        );
        assert!(matches!(&blocks[3], ContentBlock::ToolUse(_)));
        assert!(
            blocks
                .iter()
                .all(|block| !matches!(block, ContentBlock::Text(text) if text.trim().is_empty()))
        );
    }

    #[test]
    fn whitespace_before_unsigned_reasoning_is_preserved_as_text() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "anthropic.claude-test".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"text","text":" \n"}),
                serde_json::json!({"type":"reasoning","text":"reason","signature":""}),
                serde_json::json!({"type":"text","text":"visible"}),
                serde_json::json!({"type":"tool_use","id":"id","name":"lookup","input":{}}),
                serde_json::json!({"_pi_context_edit_api":"bedrock-converse-stream"}),
            ],
        };
        let call = ToolCall {
            id: "id".into(),
            name: "lookup".into(),
            arguments: "{}".into(),
        };
        let blocks = replay_blocks(
            &replay,
            "anthropic.claude-test",
            true,
            " \nvisible",
            std::slice::from_ref(&call),
            &mut ToolIds::default(),
        )
        .unwrap();
        assert!(matches!(&blocks[0], ContentBlock::Text(text) if text == " \nreason"));
        assert!(matches!(&blocks[1], ContentBlock::Text(text) if text == "visible"));
        assert!(matches!(&blocks[2], ContentBlock::ToolUse(_)));
        let mut signed = replay.clone();
        signed.blocks[1]["signature"] = serde_json::json!("sig");
        let blocks = replay_blocks(
            &signed,
            "anthropic.claude-test",
            true,
            " \nvisible",
            &[call],
            &mut ToolIds::default(),
        )
        .unwrap();
        assert!(
            matches!(&blocks[0], ContentBlock::ReasoningContent(types::ReasoningContentBlock::ReasoningText(text)) if text.signature() == Some("sig"))
        );
        assert!(matches!(&blocks[1], ContentBlock::Text(text) if text == "visible"));
    }

    #[test]
    fn unsigned_whitespace_reasoning_never_emits_blank_text_blocks() {
        for thinking_first in [true, false] {
            let thinking = serde_json::json!({"type":"reasoning","text":" \n","signature":""});
            let visible = serde_json::json!({"type":"text","text":"visible"});
            let mut blocks = if thinking_first {
                vec![thinking, visible]
            } else {
                vec![visible, thinking]
            };
            blocks
                .push(serde_json::json!({"type":"tool_use","id":"id","name":"lookup","input":{}}));
            blocks.push(serde_json::json!({"_pi_context_edit_api":"bedrock-converse-stream"}));
            let replay = ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: "amazon-bedrock".into(),
                    model: "anthropic.claude-test".into(),
                },
                blocks,
            };
            let call = ToolCall {
                id: "id".into(),
                name: "lookup".into(),
                arguments: "{}".into(),
            };
            let blocks = replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "visible",
                &[call],
                &mut ToolIds::default(),
            )
            .unwrap();
            assert!(
                blocks.iter().all(
                    |block| !matches!(block, ContentBlock::Text(text) if text.trim().is_empty())
                )
            );
            assert!(blocks.iter().any(
                |block| matches!(block, ContentBlock::Text(text) if text.contains("visible"))
            ));
            assert!(
                blocks
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolUse(_)))
            );
        }
    }

    #[test]
    fn replay_requires_unchanged_model_text_and_tool_calls() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "anthropic.claude-test".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"reasoning","text":"thought","signature":"sig"}),
                serde_json::json!({"type":"text","text":"answer"}),
                serde_json::json!({"type":"tool_use","id":"id!","name":"lookup","input":{"x":1}}),
            ],
        };
        let call = ToolCall {
            id: "id!".into(),
            name: "lookup".into(),
            arguments: "{\"x\":1}".into(),
        };
        assert!(
            replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "answer",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_some()
        );
        assert!(
            replay_blocks(
                &replay,
                "other-model",
                false,
                "answer",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_none()
        );
        assert!(
            replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "edited",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_none()
        );
        let changed = ToolCall {
            arguments: "{\"x\":2}".into(),
            ..call
        };
        assert!(
            replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "answer",
                &[changed],
                &mut ToolIds::default()
            )
            .is_none()
        );
    }

    #[test]
    fn malformed_replayed_arguments_must_remain_unchanged() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "model".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"tool_use","id":"id","name":"tool","input":{},"raw_arguments":"{bad"}),
            ],
        };
        let call = ToolCall {
            id: "id".into(),
            name: "tool".into(),
            arguments: "{bad".into(),
        };
        assert!(
            replay_blocks(
                &replay,
                "model",
                false,
                "",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_some()
        );
        let changed = ToolCall {
            arguments: "{different".into(),
            ..call
        };
        assert!(
            replay_blocks(
                &replay,
                "model",
                false,
                "",
                &[changed],
                &mut ToolIds::default()
            )
            .is_none()
        );
    }

    #[test]
    fn journal_projection_preserves_reasoning_and_detects_changed_arguments() {
        for raw in ["", "{broken", "[1]", r#"{"path":"C:\work"}"#] {
            let input = history_tool_input(raw);
            let replay = ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: "amazon-bedrock".into(),
                    model: "model".into(),
                },
                blocks: vec![
                    serde_json::json!({"type":"reasoning","text":"think","signature":"sig"}),
                    serde_json::json!({"type":"tool_use","id":"id","name":"tool","input":input,"raw_arguments":raw}),
                ],
            };
            let normalized = pi_rs_core::parse_tool_arguments(raw)
                .map_or_else(|_| "{}".to_owned(), |value| value.to_string());
            let call = ToolCall {
                id: "id".into(),
                name: "tool".into(),
                arguments: normalized,
            };
            let blocks = replay_blocks(
                &replay,
                "model",
                true,
                "",
                std::slice::from_ref(&call),
                &mut ToolIds::default(),
            )
            .expect("journal-normalized arguments should replay");
            assert!(
                matches!(&blocks[0], ContentBlock::ReasoningContent(types::ReasoningContentBlock::ReasoningText(text)) if text.signature()==Some("sig"))
            );
            let edited = ToolCall {
                arguments: "{\"edited\":true}".into(),
                ..call
            };
            assert!(
                replay_blocks(
                    &replay,
                    "model",
                    true,
                    "",
                    &[edited],
                    &mut ToolIds::default()
                )
                .is_none()
            );
        }
        assert!(!matches_journal_arguments("{broken", "{different"));
        assert!(!matches_journal_arguments("{\"x\":1}", "{\"x\": 1}"));
    }

    #[test]
    fn configured_model_name_is_scoped_to_its_id() {
        let model = Bedrock::from_env().with_model_name("opaque-profile", Some("Claude Sonnet"));
        assert!(model.is_claude("opaque-profile"));
        assert!(!model.is_claude("another-profile"));
        assert!(model.is_claude("anthropic.claude-sonnet"));
    }

    #[test]
    fn dispatch_retry_classification_respects_smithy_transient_kind() {
        use aws_sdk_bedrockruntime::error::{ConnectorError, SdkError};
        use aws_smithy_types::retry::ErrorKind;
        let error = |kind| {
            SdkError::dispatch_failure(ConnectorError::other(
                Box::new(std::io::Error::other("incomplete HTTP response")),
                kind,
            ))
        };
        for kind in [
            ErrorKind::TransientError,
            ErrorKind::ThrottlingError,
            ErrorKind::ServerError,
        ] {
            assert!(matches!(
                request_error(error(Some(kind)), &[]),
                Error::RetryableModel(_)
            ));
        }
        assert!(matches!(request_error(error(None), &[]), Error::Config(_)));
        assert!(matches!(
            request_error(error(Some(ErrorKind::ClientError)), &[]),
            Error::Config(_)
        ));
    }

    #[test]
    fn diagnostic_strips_control_characters_and_rejects_unsafe_ids() {
        let meta = aws_smithy_types::error::metadata::ErrorMetadata::builder()
            .code("ValidationException")
            .message("bad\u{1b}[31m input\nnext")
            .build();
        let text = diagnostic(&meta, Some("unsafe\nrequest-id"), &[]);
        assert_eq!(
            text,
            "Bedrock error: ValidationException: bad[31m inputnext"
        );
    }
    #[tokio::test]
    async fn custom_transport_is_not_assigned_a_prepared_context_identity() {
        use aws_smithy_runtime_api::client::http::http_client_fn;
        let config = aws_types::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new("us-east-1"))
            .build();
        let http = http_client_fn(|_, _| panic!("transport must not be invoked"));
        let model = Bedrock::from_sdk_config(config).with_http_client(http);
        assert!(model.context_identity().is_none());
        assert!(model.prepare_bedrock().await.unwrap().is_none());
    }
}
