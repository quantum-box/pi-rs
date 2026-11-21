//! Native Anthropic Messages and Models API integration.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    AnthropicInputTransformations, AssistantDiagnostic, Error, Message, Model, ModelApi,
    ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay, ReportedUsageCost,
    ResolvedModelIdentity, Result, ToolCall, Usage,
};
use reqwest::{
    Client, Response, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub const DEFAULT_ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub struct CatalogEntry {
    pub id: String,
    pub context_window: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub reasoning: Option<bool>,
    pub images: Option<bool>,
    pub thinking_levels: Vec<String>,
    thinking: Option<ThinkingMode>,
    can_disable_thinking: bool,
    effort_levels: bool,
}

impl CatalogEntry {
    /// Whether discovery explicitly identified a usable mode or disabled thinking.
    pub fn has_known_thinking(&self) -> bool {
        self.reasoning == Some(false) || self.thinking.is_some()
    }
}

#[derive(Clone, Copy, Debug)]
enum ThinkingMode {
    Adaptive,
    Budgeted,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Progress {
    WaitingForModel,
    Thinking,
    Generating,
    PreparingTool,
}

pub use pi_rs_core::CacheRetention;

type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct Anthropic {
    client: Client,
    http_proxies: Vec<reqwest::Proxy>,
    endpoint: Url,
    auth: (HeaderName, HeaderValue),
    oauth: bool,
    provider: Arc<str>,
    replay_scope: Arc<str>,
    headers: HeaderMap,
    catalog: Arc<RwLock<BTreeMap<String, CatalogEntry>>>,
    probe_context: bool,
    thinking_level: Option<String>,
    thinking_budgets: Option<Value>,
    configured_thinking: Option<CatalogEntry>,
    configured_thinking_is_fallback: bool,
    allow_empty_signature: bool,
    cache_retention: CacheRetention,
    cache_tools: bool,
    strict_tool_support: bool,
    strict_tool_models: Option<Arc<BTreeMap<String, bool>>>,
    progress: Option<Arc<dyn Fn(Progress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
}

impl Anthropic {
    pub fn new(endpoint: &str, api_key: &str) -> Result<Self> {
        Self::connect(endpoint, HeaderName::from_static("x-api-key"), api_key)
    }

    /// Connect to a Messages-compatible service using bearer authentication.
    pub fn with_bearer_token(endpoint: &str, token: &str) -> Result<Self> {
        if token.trim().is_empty() {
            return Err(Error::Config(
                "missing Anthropic-compatible bearer token".into(),
            ));
        }
        Self::connect(endpoint, AUTHORIZATION, &format!("Bearer {token}"))
    }

    /// Connect using Anthropic subscription OAuth credentials.
    pub fn with_oauth_token(endpoint: &str, token: &str) -> Result<Self> {
        let mut client = Self::with_bearer_token(endpoint, token)?;
        let official = client.endpoint.as_str() == DEFAULT_ENDPOINT;
        let test_loopback = cfg!(test)
            && client.endpoint.scheme() == "http"
            && matches!(
                client.endpoint.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]")
            );
        if !official && !test_loopback {
            return Err(Error::Config(
                "Anthropic OAuth requires the official endpoint".into(),
            ));
        }
        client.oauth = true;
        Ok(client)
    }

    fn connect(endpoint: &str, name: HeaderName, credential: &str) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid Anthropic endpoint".into()))?;
        let loopback = matches!(
            endpoint.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.path().ends_with("/messages")
        {
            return Err(Error::Config(
                "Anthropic endpoint must be HTTPS or loopback HTTP with a /messages path and no embedded credentials, query, or fragment".into(),
            ));
        }
        let mut value = HeaderValue::from_str(credential)
            .ok()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| Error::Config("missing or invalid Anthropic API key".into()))?;
        value.set_sensitive(true);
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::Config("could not build Anthropic HTTP client".into()))?;
        let replay_scope: Arc<str> = if endpoint.as_str() == DEFAULT_ENDPOINT {
            Arc::from("anthropic")
        } else {
            pi_rs_core::endpoint_replay_scope("anthropic", endpoint.as_str()).into()
        };
        Ok(Self {
            client,
            http_proxies: Vec::new(),
            endpoint,
            auth: (name, value),
            oauth: false,
            provider: Arc::from("anthropic"),
            replay_scope,
            headers: HeaderMap::new(),
            catalog: Default::default(),
            probe_context: true,
            thinking_level: None,
            thinking_budgets: None,
            configured_thinking: None,
            configured_thinking_is_fallback: false,
            allow_empty_signature: false,
            cache_retention: CacheRetention::None,
            cache_tools: true,
            strict_tool_support: false,
            strict_tool_models: None,
            progress: None,
            reasoning: None,
        })
    }

    /// Set the selected model's JSON Schema strict-tool capability.
    /// Unknown and compatible endpoints default to unsupported.
    pub fn with_strict_tool_support(mut self, supported: bool) -> Self {
        self.strict_tool_support = supported;
        self
    }

    /// Freeze strict-tool capabilities for models on this route.
    /// A constrained request for an absent model fails closed.
    pub fn with_strict_tool_models(mut self, models: BTreeMap<String, bool>) -> Self {
        self.strict_tool_models = Some(Arc::new(models));
        self
    }

    /// Bind the configured strict capability to a single selected model.
    pub fn with_strict_tool_model(self, model: &str) -> Self {
        let supported = self.strict_tool_support;
        self.with_strict_tool_models([(model.to_owned(), supported)].into_iter().collect())
    }

    /// Resolve and bind the exact tools to be sent for this dispatch.
    pub fn prepare_bound_tool_dispatch(
        &self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Box<dyn pi_rs_core::PreparedToolDispatch>> {
        let constrained = declarations.iter().any(|declaration| {
            matches!(
                declaration.constrained_sampling,
                Some(pi_rs_core::ToolConstraint::JsonSchema { .. })
            )
        });
        let supported = match (&self.strict_tool_models, constrained) {
            (Some(models), true) => *models.get(&request.model).ok_or_else(|| {
                Error::Config("Anthropic strict-tool capability cannot be resolved for the final request model and route".into())
            })?,
            _ => self.strict_tool_support,
        };
        let (wire_tools, effective) = resolve_tools(request, declarations, supported)?;
        let snapshot =
            pi_rs_core::ToolDecisionSnapshot::capture(context, request, declarations, &effective)
                .map_err(|error| Error::Config(error.to_string()))?;
        let mut reservation = request.clone();
        for (tool, wire) in reservation.tools.iter_mut().zip(&wire_tools) {
            tool.parameters = wire["input_schema"].clone();
        }
        Ok(Box::new(PreparedAnthropicDispatch {
            client: self.clone(),
            request: request.clone(),
            reservation,
            wire_tools,
            effective,
            snapshot,
        }))
    }

    /// Set the per-read HTTP idle timeout; `None` disables it.
    pub fn with_http_idle_timeout(self, timeout: Option<Duration>) -> Result<Self> {
        let proxies = self.http_proxies.clone();
        self.with_http_options(timeout, &proxies)
    }

    /// Configure the per-read deadline and explicit HTTP proxies.
    pub fn with_http_options(
        mut self,
        timeout: Option<Duration>,
        proxies: &[reqwest::Proxy],
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10));
        for proxy in proxies {
            builder = builder.proxy(proxy.clone());
        }
        if let Some(timeout) = timeout {
            builder = builder.read_timeout(timeout);
        }
        self.client = builder
            .build()
            .map_err(|_| Error::Config("could not build Anthropic HTTP client".into()))?;
        self.http_proxies = proxies.to_vec();
        Ok(self)
    }

    /// Isolate opaque reasoning blocks when another service uses the Messages protocol.
    pub fn with_provider(mut self, provider: &str, replay_scope: impl Into<Arc<str>>) -> Self {
        self.provider = Arc::from(provider);
        self.replay_scope = replay_scope.into();
        self
    }

    /// Add request defaults while preserving explicitly configured headers.
    pub fn with_default_headers(mut self, headers: &HeaderMap) -> Self {
        for (name, value) in headers {
            self.headers
                .entry(name.clone())
                .or_insert_with(|| value.clone());
        }
        self
    }

    /// Return the configured request headers for protected context identity.
    pub fn context_headers(&self) -> reqwest::header::HeaderMap {
        self.headers.clone()
    }

    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_context_probe(mut self, enabled: bool) -> Self {
        self.probe_context = enabled;
        self
    }

    pub fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking_level = level.map(str::to_owned);
        self
    }

    pub fn with_thinking_budget_settings(mut self, budgets: Option<Value>) -> Self {
        self.thinking_budgets = budgets;
        self
    }

    /// Use known thinking capabilities for a gateway that lacks a Models API.
    pub fn with_configured_thinking(mut self, level: &str, adaptive: bool, disable: bool) -> Self {
        self.thinking_level = Some(level.into());
        self.configured_thinking_is_fallback = false;
        self.configured_thinking = Some(CatalogEntry {
            thinking_levels: vec![level.into()],
            thinking: Some(if adaptive {
                ThinkingMode::Adaptive
            } else {
                ThinkingMode::Budgeted
            }),
            can_disable_thinking: disable,
            effort_levels: adaptive,
            ..CatalogEntry::default()
        });
        self
    }

    /// Use declared capabilities only when model discovery is unavailable.
    pub fn with_fallback_thinking(mut self, level: &str, adaptive: bool, disable: bool) -> Self {
        self = self.with_configured_thinking(level, adaptive, disable);
        self.configured_thinking_is_fallback = true;
        self
    }

    /// Adaptive thinking can be supported without an output effort selector.
    pub fn without_thinking_effort(mut self) -> Self {
        if let Some(details) = &mut self.configured_thinking {
            details.effort_levels = false;
        }
        self
    }

    pub fn with_empty_thinking_signature(mut self, enabled: bool) -> Self {
        self.allow_empty_signature = enabled;
        self
    }

    pub fn with_cache_retention(mut self, retention: CacheRetention, cache_tools: bool) -> Self {
        self.cache_retention = retention;
        self.cache_tools = cache_tools;
        self
    }

    pub fn without_cache(mut self) -> Self {
        self.cache_retention = CacheRetention::None;
        self
    }

    pub async fn model(&self, model: &str) -> Option<CatalogEntry> {
        if let Some(details) = self.cached_model(model) {
            return Some(details);
        }
        if !self.probe_context {
            return None;
        }
        let mut url = self.models_url();
        url.path_segments_mut().ok()?.push(model);
        let details = tokio::time::timeout(Duration::from_secs(2), async {
            let response = send(
                self.authenticate(self.client.get(url)),
                &self.error_secrets(),
            )
            .await
            .ok()?;
            decode_model(&read_json(response).await.ok()?)
        })
        .await
        .ok()
        .flatten()?;
        self.catalog
            .write()
            .ok()?
            .insert(model.into(), details.clone());
        Some(details)
    }

    pub fn with_progress(mut self, observer: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(observer));
        self
    }

    pub fn with_reasoning(mut self, observer: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.reasoning = Some(Arc::new(observer));
        self
    }

    pub fn cached_model(&self, model: &str) -> Option<CatalogEntry> {
        self.catalog.read().ok()?.get(model).cloned()
    }

    fn models_url(&self) -> Url {
        let mut url = self.endpoint.clone();
        let base = url
            .path()
            .strip_suffix("/messages")
            .expect("validated endpoint");
        url.set_path(&format!("{base}/models"));
        url
    }

    fn error_secrets(&self) -> Vec<String> {
        let mut secrets = Vec::new();
        for value in std::iter::once(&self.auth.1).chain(self.headers.values()) {
            if let Ok(value) = value.to_str() {
                if !value.is_empty() {
                    secrets.push(value.to_owned());
                }
                if let Some((scheme, token)) = value.split_once(' ')
                    && scheme.eq_ignore_ascii_case("bearer")
                    && !token.trim().is_empty()
                {
                    secrets.push(token.trim().to_owned());
                }
            }
        }
        secrets
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let request = request
            .header("anthropic-version", API_VERSION)
            .header(self.auth.0.clone(), self.auth.1.clone());
        let request = if self.oauth {
            let request = request
                .header("accept", "application/json")
                .header("anthropic-dangerous-direct-browser-access", "true")
                .header("user-agent", "claude-cli/2.1.280")
                .header("x-app", "cli");
            if self.headers.contains_key("anthropic-beta") {
                request
            } else {
                request.header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
            }
        } else {
            request
        };
        request.headers(self.headers.clone())
    }

    /// Return paginated models in server order and retain their metadata for this client.
    pub async fn models(&self) -> Result<Vec<CatalogEntry>> {
        tokio::time::timeout(Duration::from_secs(10), self.load_models())
            .await
            .map_err(|_| Error::RetryableModel("Anthropic model listing timed out".into()))?
    }

    async fn load_models(&self) -> Result<Vec<CatalogEntry>> {
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let mut url = self.models_url();
            url.query_pairs_mut().append_pair("limit", "100");
            if let Some(cursor) = &cursor {
                url.query_pairs_mut().append_pair("after_id", cursor);
            }
            let response = send(
                self.authenticate(self.client.get(url)),
                &self.error_secrets(),
            )
            .await?;
            let page = read_json(response).await?;
            let models = page["data"]
                .as_array()
                .ok_or_else(|| Error::Model("invalid Anthropic model list".into()))?;
            for model in models {
                if let Some(model) = decode_model(model)
                    && seen.insert(model.id.clone())
                {
                    result.push(model);
                }
            }
            if page["has_more"] != true {
                self.catalog
                    .write()
                    .expect("model catalog")
                    .extend(result.iter().map(|entry| (entry.id.clone(), entry.clone())));
                return Ok(result);
            }
            let next = page["last_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| Error::Model("missing Anthropic model cursor".into()))?;
            if !cursors.insert(next.to_owned()) {
                return Err(Error::Model("repeated Anthropic model cursor".into()));
            }
            cursor = Some(next.to_owned());
        }
        Err(Error::Model(
            "Anthropic model listing exceeds page limit".into(),
        ))
    }
}

fn decode_model(model: &Value) -> Option<CatalogEntry> {
    let tokens = |key: &str| {
        model[key]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|count| *count > 0)
    };
    let capabilities = &model["capabilities"];
    let types = &capabilities["thinking"]["types"];
    let thinking = if capabilities["thinking"]["supported"] == false {
        None
    } else if types["adaptive"]["supported"] == true {
        Some(ThinkingMode::Adaptive)
    } else if types["enabled"]["supported"] == true {
        Some(ThinkingMode::Budgeted)
    } else {
        None
    };
    let can_disable_thinking = types["disabled"]["supported"].as_bool().unwrap_or(true);
    let mut thinking_levels = Vec::new();
    let mut effort_levels = false;
    if can_disable_thinking || thinking.is_none() {
        thinking_levels.push("off".into());
    }
    match thinking {
        Some(ThinkingMode::Adaptive) => {
            thinking_levels.extend(
                ["low", "medium", "high", "xhigh", "max"]
                    .into_iter()
                    .filter(|level| capabilities["effort"][*level]["supported"] == true)
                    .map(str::to_owned),
            );
            effort_levels = thinking_levels.iter().any(|level| level != "off");
            if !effort_levels {
                thinking_levels.push("high".into());
            }
        }
        Some(ThinkingMode::Budgeted) => thinking_levels.extend(
            ["minimal", "low", "medium", "high", "xhigh", "max"]
                .into_iter()
                .map(str::to_owned),
        ),
        None => {}
    }
    Some(CatalogEntry {
        id: model["id"].as_str().filter(|id| !id.is_empty())?.into(),
        context_window: tokens("max_input_tokens"),
        max_output_tokens: tokens("max_tokens"),
        reasoning: model["capabilities"]["thinking"]["supported"].as_bool(),
        images: capabilities["image_input"]["supported"].as_bool(),
        thinking_levels,
        thinking,
        can_disable_thinking,
        effort_levels,
    })
}

// Pi excludes account and billing limits from agent retries even if a
// provider wraps them in HTTP 429. Inspect only a bounded prefix and never
// include provider response bodies in errors or logs.
fn permanent_quota_text(body: &[u8], complete: bool, secrets: &[String]) -> bool {
    let mut text = String::from_utf8_lossy(body).into_owned();
    for secret in secrets {
        if !secret.is_empty() {
            text = text.replace(secret, "[REDACTED]");
        }
    }
    let text = text.to_ascii_lowercase();
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
    if MARKERS.iter().any(|marker| text.contains(marker)) {
        return true;
    }
    text.match_indices("\"code\"").any(|(index, _)| {
        let rest = text[index + 6..].trim_start();
        let Some(rest) = rest.strip_prefix(':') else {
            return false;
        };
        let rest = rest
            .trim_start()
            .strip_prefix('\"')
            .unwrap_or(rest.trim_start());
        rest.strip_prefix("1308").is_some_and(|tail| {
            (complete || !tail.is_empty()) && !tail.starts_with(|c: char| c.is_ascii_digit())
        })
    })
}

fn permanent_quota_value(value: &Value, secrets: &[String]) -> bool {
    let error = value.get("error").unwrap_or(value);
    if let Some(text) = error.as_str() {
        return permanent_quota_text(text.as_bytes(), true, secrets);
    }
    let Some(object) = error.as_object() else {
        return false;
    };
    ["message", "code", "type", "errorMessage"]
        .iter()
        .any(|key| {
            let Some(value) = object.get(*key) else {
                return false;
            };
            if *key == "code" && value.as_u64() == Some(1308) {
                return true;
            }
            if *key == "code"
                && value.as_str() == Some("1308")
                && !secrets
                    .iter()
                    .any(|secret| !secret.is_empty() && "1308".contains(secret))
            {
                return true;
            }
            value
                .as_str()
                .is_some_and(|text| permanent_quota_text(text.as_bytes(), true, secrets))
        })
}

// Keep only complete, directly nested error fields observed before an EOF at
// the bounded prefix. Unrelated metadata and incomplete values are ignored.
fn quota_prefix_value(body: &[u8]) -> Option<Value> {
    use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
    use std::fmt;
    struct Root<'a>(&'a mut serde_json::Map<String, Value>);
    struct Fields<'a>(&'a mut serde_json::Map<String, Value>);
    impl<'de> DeserializeSeed<'de> for Root<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<(), D::Error> {
            struct RootVisitor<'a>(&'a mut serde_json::Map<String, Value>);
            impl<'de> Visitor<'de> for RootVisitor<'_> {
                type Value = ();
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("error envelope")
                }
                fn visit_map<M: MapAccess<'de>>(
                    self,
                    mut map: M,
                ) -> std::result::Result<(), M::Error> {
                    while let Some(key) = map.next_key::<String>()? {
                        if key == "error" {
                            self.0.clear();
                            map.next_value_seed(Fields(self.0))?;
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                    Ok(())
                }
            }
            d.deserialize_map(RootVisitor(self.0))
        }
    }
    impl<'de> DeserializeSeed<'de> for Fields<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<(), D::Error> {
            struct FieldsVisitor<'a>(&'a mut serde_json::Map<String, Value>);
            impl<'de> Visitor<'de> for FieldsVisitor<'_> {
                type Value = ();
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("error fields")
                }
                fn visit_map<M: MapAccess<'de>>(
                    self,
                    mut map: M,
                ) -> std::result::Result<(), M::Error> {
                    while let Some(key) = map.next_key::<String>()? {
                        if matches!(key.as_str(), "message" | "code" | "type" | "errorMessage") {
                            self.0.remove(&key);
                            let value = map.next_value::<Value>()?;
                            self.0.insert(key, value);
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                    Ok(())
                }
            }
            d.deserialize_map(FieldsVisitor(self.0))
        }
    }
    let mut fields = serde_json::Map::new();
    let mut de = serde_json::Deserializer::from_slice(body);
    match Root(&mut fields).deserialize(&mut de) {
        Err(error) if error.is_eof() && !fields.is_empty() => Some(Value::Object(fields)),
        _ => None,
    }
}

fn permanent_quota_error(body: &[u8], complete: bool, secrets: &[String]) -> bool {
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => permanent_quota_value(&value, secrets),
        Err(_) if !complete && String::from_utf8_lossy(body).trim_start().starts_with('{') => {
            // A JSON parser can accept an unfinished number at EOF. Probe a
            // trailing digit with another digit; otherwise preserve an already
            // observed delimiter, including whitespace.
            let Some(value) = quota_prefix_value(body) else {
                return false;
            };
            let mut probe = Vec::with_capacity(body.len() + 1);
            probe.extend_from_slice(body);
            probe.push(if body.last().is_some_and(u8::is_ascii_digit) {
                b'0'
            } else {
                b' '
            });
            quota_prefix_value(&probe).is_some_and(|next| next == value)
                && permanent_quota_value(&value, secrets)
        }
        Err(_)
            if String::from_utf8_lossy(body)
                .trim_start()
                .starts_with(['{', '[']) =>
        {
            false
        }
        Err(_) => permanent_quota_text(body, complete, secrets),
    }
}

async fn is_permanent_quota(mut response: Response, secrets: &[String]) -> bool {
    const MAX_ERROR_BODY: usize = 64 * 1024;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut body = Vec::new();
        let mut complete = false;
        while body.len() < MAX_ERROR_BODY {
            let Some(chunk) = response.chunk().await.ok()? else {
                complete = true;
                break;
            };
            let remaining = MAX_ERROR_BODY - body.len();
            body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        }
        Some(permanent_quota_error(&body, complete, secrets))
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(false)
}

async fn send(request: reqwest::RequestBuilder, secrets: &[String]) -> Result<Response> {
    check_status(send_http(request).await?, secrets).await
}

async fn send_http(request: reqwest::RequestBuilder) -> Result<Response> {
    request.send().await.map_err(|error| {
        Error::RetryableModel(
            if error.is_timeout() && !error.is_connect() {
                "HTTP idle timeout"
            } else {
                "Anthropic HTTP connection failed"
            }
            .into(),
        )
    })
}

async fn check_status(response: Response, secrets: &[String]) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        let message = format!("Anthropic returned HTTP {status}");
        if status.as_u16() == 413 {
            return Err(Error::Model("413 request too large".into()));
        }
        if matches!(status.as_u16(), 400 | 422) {
            let overflow =
                pi_rs_core::provider_context_overflow_stream(response.bytes_stream(), secrets)
                    .await;
            return Err(Error::Model(if overflow {
                "context window exceeded".into()
            } else {
                message
            }));
        }
        if (matches!(status.as_u16(), 408 | 429) || status.is_server_error())
            && is_permanent_quota(response, secrets).await
        {
            Err(Error::Model(
                "Anthropic account quota or billing limit reached".into(),
            ))
        } else if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
            Err(Error::RetryableModel(message))
        } else {
            Err(Error::Model(message))
        }
    }
}

async fn read_json(mut response: Response) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::RetryableModel("broken Anthropic HTTP response".into()))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(Error::Model("Anthropic response exceeds size limit".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Model("invalid Anthropic response".into()))
}

fn encode_request(
    request: ModelRequest,
    replay_scope: &str,
    edited_scope: &str,
    allow_empty_signature: bool,
) -> Result<Value> {
    let mut system = Vec::new();
    let mut turns: Vec<(&str, Vec<Value>)> = Vec::new();
    for message in request.messages {
        let (role, blocks) = match message {
            Message::System { content } => {
                system.push(content);
                continue;
            }
            Message::User { content } => ("user", text_block(content).into_iter().collect()),
            Message::UserWithImages { content, images } => {
                let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                for image in images {
                    image.validate()?;
                    blocks.push(json!({"type":"image","source":{
                        "type":"base64","media_type":image.mime_type,"data":image.data
                    }}));
                }
                ("user", blocks)
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                let blocks = match replay {
                    Some(replay)
                        if replay_matches(
                            &replay,
                            if replay.is_context_edit() { edited_scope } else { replay_scope },
                            &request.model,
                            &content,
                            &calls,
                        ) =>
                    {
                        replay
                            .blocks
                            .into_iter()
                            .filter(|block| block.get("_pi_context_edit_api").is_none() && block["_pi_omitted_text"] != true)
                            .map(|mut block| {
                                if block["_pi_thinking"] == true && allow_empty_signature {
                                    return json!({"type":"thinking","thinking":block["text"],"signature":""});
                                }
                                if let Some(object) = block.as_object_mut() {
                                    object.remove("_pi_thinking");
                                }
                                block
                            })
                            .collect()
                    }
                    _ => {
                        let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                        blocks.extend(calls.into_iter().map(|call| tool_block(&call)));
                        blocks
                    }
                };
                ("assistant", blocks)
            }
            Message::Tool {
                call_id,
                content,
                is_error,
                ..
            } => {
                let mut block =
                    json!({"type":"tool_result","tool_use_id":call_id,"content":content});
                if is_error {
                    block["is_error"] = json!(true);
                }
                ("user", vec![block])
            }
        };
        if blocks.is_empty() {
            continue;
        }
        if let Some((previous, content)) = turns.last_mut()
            && *previous == role
        {
            content.extend(blocks);
        } else {
            turns.push((role, blocks));
        }
    }
    if turns.is_empty() {
        return Err(Error::Model(
            "Anthropic requires at least one conversation message".into(),
        ));
    }
    let messages: Vec<_> = turns
        .into_iter()
        .map(|(role, content)| json!({"role":role,"content":content}))
        .collect();
    let mut body = json!({"model":request.model,"messages":messages,"max_tokens":request.max_output_tokens,"stream":true});
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(request.tools.into_iter().map(|tool| {
            json!({"name":tool.name,"description":tool.description,"input_schema":tool.parameters})
        }).collect::<Vec<_>>());
    }
    Ok(body)
}

fn apply_cache_control(body: &mut Value, retention: CacheRetention, cache_tools: bool) {
    let Some(control) = retention.anthropic_control() else {
        return;
    };
    if let Some(system) = body.get("system").and_then(Value::as_str) {
        body["system"] = json!([{"type":"text","text":system,"cache_control":control}]);
    }
    if cache_tools
        && let Some(tool) = body
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .and_then(|tools| tools.last_mut())
    {
        tool["cache_control"] = control.clone();
    }
    if let Some(message) = body
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .and_then(|messages| messages.last_mut())
        && message["role"] == "user"
        && let Some(block) = message
            .get_mut("content")
            .and_then(Value::as_array_mut)
            .and_then(|blocks| blocks.last_mut())
        && matches!(
            block["type"].as_str(),
            Some("text" | "image" | "tool_result")
        )
    {
        block["cache_control"] = control;
    }
}

const CLAUDE_CODE_TOOLS: &[&str] = &[
    "Read",
    "Write",
    "Edit",
    "Bash",
    "Grep",
    "Glob",
    "AskUserQuestion",
    "EnterPlanMode",
    "ExitPlanMode",
    "KillShell",
    "NotebookEdit",
    "Skill",
    "Task",
    "TaskOutput",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
];

fn oauth_tool_name(name: &str) -> &str {
    CLAUDE_CODE_TOOLS
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(name))
        .unwrap_or(name)
}

fn prepare_oauth_request(body: &mut Value, retention: CacheRetention) {
    let mut identity =
        json!({"type":"text", "text":"You are Claude Code, Anthropic's official CLI for Claude."});
    if let Some(control) = retention.anthropic_control() {
        identity["cache_control"] = control;
    }
    let mut system = vec![identity];
    match body.get_mut("system").map(Value::take) {
        Some(Value::String(text)) => system.push(json!({"type":"text","text":text})),
        Some(Value::Array(blocks)) => system.extend(blocks),
        _ => {}
    }
    body["system"] = Value::Array(system);
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            if let Some(name) = tool["name"].as_str() {
                tool["name"] = json!(oauth_tool_name(name));
            }
        }
    }
    if let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            if let Some(blocks) = message["content"].as_array_mut() {
                for block in blocks {
                    if block["type"] == "tool_use"
                        && let Some(name) = block["name"].as_str()
                    {
                        block["name"] = json!(oauth_tool_name(name));
                    }
                }
            }
        }
    }
}

fn text_block(content: String) -> Option<Value> {
    (!content.is_empty()).then(|| json!({"type":"text","text":content}))
}

fn configure_thinking(
    body: &mut Value,
    details: Option<&CatalogEntry>,
    level: &str,
    max_output: u32,
    budgets: Option<&Value>,
) -> Result<()> {
    if level == "off" {
        if let Some(model) = details.filter(|model| model.thinking.is_some()) {
            if !model.can_disable_thinking {
                return Err(Error::Config(
                    "selected Anthropic model cannot disable thinking".into(),
                ));
            }
            body["thinking"] = json!({"type":"disabled"});
        }
        return Ok(());
    }
    let details = details
        .filter(|model| model.thinking_levels.iter().any(|value| value == level))
        .ok_or_else(|| {
            Error::Config("selected Anthropic model does not advertise that thinking level".into())
        })?;
    match details.thinking {
        Some(ThinkingMode::Adaptive) => {
            body["thinking"] = json!({"type":"adaptive","display":"summarized"});
            // A model can advertise adaptive thinking without effort controls.
            if details.effort_levels {
                body["output_config"] = json!({"effort":level});
            }
        }
        Some(ThinkingMode::Budgeted) => {
            let key = match level {
                "minimal" => "minimal",
                "low" => "low",
                "medium" => "medium",
                _ => "high",
            };
            let default = match level {
                "minimal" => 1024,
                "low" => 2048,
                "medium" => 8192,
                _ => 16_384,
            };
            let preferred = if let Some(budgets) = budgets {
                let object = budgets
                    .as_object()
                    .ok_or_else(|| Error::Config("thinkingBudgets must be an object".into()))?;
                match object.get(key) {
                    None => default,
                    Some(value) => value
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .filter(|n| *n >= 1024)
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "thinkingBudgets.{key} must be an integer of at least 1024"
                            ))
                        })?,
                }
            } else {
                default
            };
            let budget = preferred.min(max_output.saturating_sub(1024).max(1024));
            if budget >= max_output {
                return Err(Error::Config(
                    "Anthropic budgeted thinking needs at least 1025 output tokens".into(),
                ));
            }
            body["thinking"] =
                json!({"type":"enabled","budget_tokens":budget,"display":"summarized"});
        }
        None => {}
    }
    Ok(())
}

fn tool_block(call: &ToolCall) -> Value {
    let input = serde_json::from_str::<Value>(&call.arguments)
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    json!({"type":"tool_use","id":call.id,"name":call.name,"input":input})
}

fn replay_matches(
    replay: &ProviderReplay,
    scope: &str,
    model: &str,
    text: &str,
    calls: &[ToolCall],
) -> bool {
    if !replay.supports_api(ModelApi::AnthropicMessages)
        || replay.identity.provider != scope
        || replay.identity.model != model
    {
        return false;
    }
    let rendered = replay
        .blocks
        .iter()
        .filter(|block| block["type"] == "text" && block["_pi_thinking"] != true)
        .filter_map(|block| block["text"].as_str())
        .collect::<String>();
    if rendered != text {
        return false;
    }
    let mut replay_calls = replay
        .blocks
        .iter()
        .filter(|block| block["type"] == "tool_use");
    for call in calls {
        let Some(block) = replay_calls.next() else {
            return false;
        };
        let expected = tool_block(call);
        if ["id", "name", "input"]
            .iter()
            .any(|key| block[*key] != expected[*key])
        {
            return false;
        }
    }
    replay_calls.next().is_none()
}

struct ResponseBlock {
    raw: Value,
    text: String,
    signature: String,
    complete: bool,
}

impl ResponseBlock {
    fn new(raw: &Value) -> Self {
        let key = if raw["type"] == "thinking" {
            "thinking"
        } else {
            "text"
        };
        Self {
            raw: raw.clone(),
            text: raw[key].as_str().unwrap_or_default().into(),
            signature: raw["signature"].as_str().unwrap_or_default().into(),
            complete: false,
        }
    }

    fn signed(&self) -> bool {
        matches!(
            self.raw["type"].as_str(),
            Some("thinking" | "redacted_thinking")
        )
    }

    fn finish(mut self, allow_empty_signature: bool) -> Result<Value> {
        if !self.complete {
            return Err(Error::Model(
                "Anthropic content block ended prematurely".into(),
            ));
        }
        match self.raw["type"].as_str() {
            Some("text") => self.raw["text"] = json!(self.text),
            Some("thinking") if allow_empty_signature || !self.signature.is_empty() => {
                self.raw["thinking"] = json!(self.text);
                self.raw["signature"] = json!(self.signature);
            }
            Some("thinking") => {
                return Err(Error::Model(
                    "Anthropic thinking block is missing a signature".into(),
                ));
            }
            Some("redacted_thinking") if self.raw["data"].as_str().is_none_or(str::is_empty) => {
                return Err(Error::Model(
                    "Anthropic redacted thinking block is empty".into(),
                ));
            }
            _ => {}
        }
        Ok(self.raw)
    }
}

struct Call {
    id: String,
    name: String,
    raw: Value,
    fragments: String,
    complete: bool,
}

impl Call {
    fn finish(mut self) -> Result<(ToolCall, Value)> {
        if !self.complete {
            return Err(Error::Model(
                "Anthropic tool block ended prematurely".into(),
            ));
        }
        let input = if self.fragments.is_empty() {
            self.raw["input"].take()
        } else {
            serde_json::from_str(&self.fragments)
                .map_err(|_| Error::Model("invalid Anthropic tool input JSON".into()))?
        };
        if !input.is_object() {
            return Err(Error::Model(
                "Anthropic tool input must be an object".into(),
            ));
        }
        let call = ToolCall {
            id: self.id,
            name: self.name,
            arguments: input.to_string(),
        };
        self.raw["input"] = input;
        Ok((call, self.raw))
    }
}

#[derive(Default)]
struct StreamUsage {
    input: Option<u64>,
    output: Option<u64>,
    cached: Option<u64>,
    cache_written: Option<u64>,
    cache_written_1h: Option<u64>,
}
impl StreamUsage {
    fn update(&mut self, value: &Value, metadata: &mut ModelMetadata) {
        self.input = value["input_tokens"].as_u64().or(self.input);
        self.output = value["output_tokens"].as_u64().or(self.output);
        self.cached = value["cache_read_input_tokens"].as_u64().or(self.cached);
        self.cache_written = value["cache_creation_input_tokens"]
            .as_u64()
            .or(self.cache_written);
        self.cache_written_1h = value["cache_creation"]["ephemeral_1h_input_tokens"]
            .as_u64()
            .or(self.cache_written_1h);
        metadata.cache_write_1h_tokens = self.cache_written_1h;
        metadata.cached_input_tokens = self.cached;
        metadata.cache_creation_input_tokens = self.cache_written;
        metadata.provider_prompt_tokens = self.total_input();
    }
    fn total_input(&self) -> Option<u64> {
        Some(
            self.input?
                .saturating_add(self.cached.unwrap_or(0))
                .saturating_add(self.cache_written.unwrap_or(0)),
        )
    }
    fn finish(&self) -> Option<Usage> {
        Some(Usage {
            input_tokens: self.total_input()?,
            output_tokens: self.output?,
        })
    }
}

struct PreparedAnthropicDispatch {
    client: Anthropic,
    request: ModelRequest,
    reservation: ModelRequest,
    wire_tools: Vec<Value>,
    effective: Vec<pi_rs_core::EffectiveToolDeclaration>,
    snapshot: pi_rs_core::ToolDecisionSnapshot,
}

#[async_trait::async_trait]
impl pi_rs_core::PreparedToolDispatch for PreparedAnthropicDispatch {
    fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
        &self.effective
    }
    fn decision_snapshot(&self) -> &pi_rs_core::ToolDecisionSnapshot {
        &self.snapshot
    }
    fn dispatch_request(&self) -> &ModelRequest {
        &self.request
    }
    fn reservation_request(&self) -> &ModelRequest {
        &self.reservation
    }
    async fn dispatch(self: Box<Self>) -> Result<pi_rs_core::DetailedModelStream> {
        self.client
            .stream_detailed_with_tools(self.request, Some(self.wire_tools))
            .await
    }
}

fn anthropic_unsupported_keyword(key: &str, value: &Value) -> bool {
    match key {
        "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" | "multipleOf"
        | "maxItems" | "uniqueItems" | "minContains" | "maxContains" | "minProperties"
        | "maxProperties" => true,
        "minItems" => !matches!(value.as_f64(), Some(n) if n == 0.0 || n == 1.0),
        "format" => !matches!(
            value.as_str(),
            Some(
                "date-time"
                    | "time"
                    | "date"
                    | "duration"
                    | "email"
                    | "hostname"
                    | "uri"
                    | "ipv4"
                    | "ipv6"
                    | "uuid"
            )
        ),
        _ => false,
    }
}

// Pi keeps strict root annotations, while ordinary tools use only the
// Anthropic-supported root fields. Root combinators cannot be sent.
fn anthropic_input_schema(parameters: &Value, strict: bool) -> Value {
    let mut root = if strict {
        parameters.as_object().cloned().unwrap_or_default()
    } else {
        serde_json::Map::new()
    };
    for key in ["anyOf", "oneOf", "allOf"] {
        root.remove(key);
    }
    root.insert("type".into(), json!("object"));
    for (key, default) in [("properties", json!({})), ("required", json!([]))] {
        let value = parameters
            .get(key)
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or(default);
        root.insert(key.into(), value);
    }
    for key in ["additionalProperties", "$defs", "definitions"] {
        if let Some(value) = parameters.get(key) {
            root.insert(key.into(), value.clone());
        }
    }
    Value::Object(root)
}

fn resolve_tools(
    request: &ModelRequest,
    declarations: &[pi_rs_core::ToolDeclaration],
    supported: bool,
) -> Result<(Vec<Value>, Vec<pi_rs_core::EffectiveToolDeclaration>)> {
    if request.tools.len() != declarations.len()
        || request
            .tools
            .iter()
            .zip(declarations)
            .any(|(a, b)| a != &b.spec)
    {
        return Err(Error::Config(
            "tool declarations do not match the effective request".into(),
        ));
    }
    let mut wire = Vec::new();
    let mut effective = Vec::new();
    for declaration in declarations {
        let spec = &declaration.spec;
        // Removing a root constraint would change what strict sampling promises.
        // Fall back or fail before advertising such a decision.
        let root_combinator = ["anyOf", "oneOf", "allOf"]
            .iter()
            .any(|key| spec.parameters.get(key).is_some());
        let strict = match &declaration.constrained_sampling {
            Some(pi_rs_core::ToolConstraint::JsonSchema { strict }) => {
                pi_rs_core::resolve_strict_tool_schema(
                    &spec.name,
                    &spec.parameters,
                    *strict,
                    supported && !root_combinator,
                    anthropic_unsupported_keyword,
                )
                .map_err(|error| Error::Config(error.to_string()))?
            }
            _ => None,
        };
        let sampling = if strict.is_some() {
            pi_rs_core::EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters.clone(),
                normalization_version: pi_rs_core::STRICT_TOOL_NORMALIZATION_VERSION,
            }
        } else {
            pi_rs_core::EffectiveToolSampling::Unconstrained
        };
        let schema = anthropic_input_schema(
            strict.as_ref().unwrap_or(&spec.parameters),
            strict.is_some(),
        );
        let mut tool =
            json!({"name":spec.name,"description":spec.description,"input_schema":schema});
        if strict.is_some() {
            tool["strict"] = json!(true);
        }
        wire.push(tool);
        effective.push(pi_rs_core::EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling,
        });
    }
    pi_rs_core::validate_effective_tool_declarations(request, declarations, &effective)
        .map_err(|error| Error::Config(error.to_string()))?;
    Ok((wire, effective))
}

#[async_trait::async_trait]
impl Model for Anthropic {
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs_core::PreparedToolDispatch + 'a>>> {
        Ok(Some(self.prepare_bound_tool_dispatch(
            request,
            declarations,
            context,
        )?))
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let context = pi_rs_core::ToolDispatchContext {
            request_id,
            session_id,
            external_id: external_id.into(),
            summary: false,
        };
        self.stream_without_retained_decision(request, declarations, context)
            .await
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let context = pi_rs_core::ToolDispatchContext {
            request_id,
            session_id,
            external_id: session_id.to_string(),
            summary: true,
        };
        self.clone()
            .without_cache()
            .stream_without_retained_decision(request, declarations, context)
            .await
    }

    fn context_identity(&self) -> Option<String> {
        // Thinking and discovered output limits can depend on a changing catalog.
        if !self.http_proxies.is_empty()
            || (self.thinking_level.is_some()
                && (self.configured_thinking.is_none() || self.probe_context))
        {
            return None;
        }
        let mut headers: Vec<_> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort();
        let identity = (
            headers,
            "anthropic-v1",
            self.endpoint.as_str(),
            self.provider.as_ref(),
            self.replay_scope.as_ref(),
            self.auth.0.as_str(),
            self.auth.1.as_bytes(),
            &self.thinking_level,
            &self.thinking_budgets,
            format!("{:?}", self.configured_thinking),
            self.configured_thinking_is_fallback,
            self.allow_empty_signature,
            self.cache_retention.anthropic_control(),
            self.cache_tools,
        );
        let base = if self.strict_tool_support {
            pi_rs_core::protected_model_fingerprint(&(identity, "strict-tools-v1"))
        } else {
            pi_rs_core::protected_model_fingerprint(&identity)
        }?;
        match &self.strict_tool_models {
            // A map containing only unsupported models cannot change any
            // successful request's sampling compared with the default.
            Some(models) if models.values().any(|supported| *supported) => {
                pi_rs_core::protected_model_fingerprint(&(
                    base,
                    "strict-models-v1",
                    models.as_ref(),
                ))
            }
            _ => Some(base),
        }
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let edited_scope =
            pi_rs_core::endpoint_replay_scope(&self.replay_scope, self.endpoint.as_str());
        match message {
            Message::Tool { content, .. } => content.encode_utf16().count().div_ceil(4) as u64,
            Message::Assistant {
                content,
                calls,
                replay: Some(replay),
            } if replay_matches(
                replay,
                if replay.is_context_edit() {
                    &edited_scope
                } else {
                    &self.replay_scope
                },
                &request.model,
                content,
                calls,
            ) =>
            {
                pi_rs_core::estimate_replay_tokens(&replay.blocks)
                    .max(pi_rs_core::estimate_message_tokens(message))
            }
            _ => pi_rs_core::estimate_message_tokens(message),
        }
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        self.model(model).await?.context_window
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let stream = self.stream_enriched(request).await?;
        Ok(Box::pin(stream.filter_map(|event| async move {
            match event {
                Ok(pi_rs_core::EnrichedModelEvent::Legacy(event)) => Some(Ok(event)),
                Ok(pi_rs_core::EnrichedModelEvent::TextDelta { delta, .. }) => {
                    Some(Ok(ModelEvent::Text(delta)))
                }
                Ok(pi_rs_core::EnrichedModelEvent::ReasoningDelta { delta, .. }) => {
                    Some(Ok(ModelEvent::Reasoning(delta)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })))
    }
    async fn stream_enriched_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        _external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        self.stream_enriched(request).await
    }
    async fn stream_enriched_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        self.stream_enriched(request).await
    }
    async fn stream_detailed_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        _external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.stream_detailed(request).await
    }
    async fn stream_detailed_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.stream_detailed(request).await
    }
}

impl Anthropic {
    async fn stream_without_retained_decision(
        &self,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: pi_rs_core::ToolDispatchContext,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let prepared = self.prepare_bound_tool_dispatch(&request, declarations, &context)?;
        pi_rs_core::validate_prepared_tool_dispatch(
            prepared.as_ref(),
            &context,
            &request,
            declarations,
        )?;
        if prepared.effective_tools().iter().any(|tool| {
            matches!(
                tool.sampling,
                pi_rs_core::EffectiveToolSampling::StrictJsonSchema { .. }
            )
        }) {
            return Err(Error::Config("strict tool sampling requires prepared dispatch so its effective decisions can be retained".into()));
        }
        prepared.dispatch().await
    }

    async fn stream_enriched(
        &self,
        request: ModelRequest,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        Ok(Box::pin(
            self.stream_detailed(request)
                .await?
                .map(|event| event.map(|event| event.event)),
        ))
    }

    async fn stream_detailed(
        &self,
        request: ModelRequest,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.stream_detailed_with_tools(request, None).await
    }

    async fn stream_detailed_with_tools(
        &self,
        mut request: ModelRequest,
        wire_tools: Option<Vec<Value>>,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let discovered = if self.thinking_level.is_some() {
            self.model(&request.model).await
        } else {
            self.cached_model(&request.model)
        };
        let details = if self.configured_thinking_is_fallback
            && discovered
                .as_ref()
                .is_some_and(CatalogEntry::has_known_thinking)
        {
            discovered
        } else if let Some(mut configured) = self.configured_thinking.clone() {
            if let Some(discovered) = discovered {
                configured.id = discovered.id;
                configured.context_window = discovered.context_window;
                configured.max_output_tokens = discovered.max_output_tokens;
                configured.reasoning = discovered.reasoning;
                configured.images = discovered.images;
            }
            Some(configured)
        } else {
            discovered
        };
        if let Some(limit) = details.as_ref().and_then(|model| model.max_output_tokens) {
            request.max_output_tokens = request.max_output_tokens.min(limit);
        }
        let requested_model = request.model.clone();
        let max_output_tokens = request.max_output_tokens;
        let provider = self.provider.clone();
        let openrouter =
            provider.as_ref() == "openrouter" || self.endpoint.host_str() == Some("openrouter.ai");
        let replay_scope = self.replay_scope.clone();
        let edited_scope = pi_rs_core::endpoint_replay_scope(&replay_scope, self.endpoint.as_str());
        let oauth_tool_names: Vec<String> = if self.oauth {
            let names: Vec<String> = request.tools.iter().map(|tool| tool.name.clone()).collect();
            for (index, name) in names.iter().enumerate() {
                if names[..index]
                    .iter()
                    .any(|other| other.eq_ignore_ascii_case(name))
                {
                    return Err(Error::Config(
                        "Anthropic OAuth tool names must be unique ignoring case".into(),
                    ));
                }
            }
            names
        } else {
            Vec::new()
        };
        let mut body = encode_request(
            request,
            &replay_scope,
            &edited_scope,
            self.allow_empty_signature,
        )?;
        if let Some(tools) = wire_tools
            && !tools.is_empty()
        {
            body["tools"] = json!(tools);
        }
        apply_cache_control(&mut body, self.cache_retention, self.cache_tools);
        if self.oauth {
            prepare_oauth_request(&mut body, self.cache_retention);
        }
        if let Some(level) = self.thinking_level.as_deref() {
            configure_thinking(
                &mut body,
                details.as_ref(),
                level,
                max_output_tokens,
                self.thinking_budgets.as_ref(),
            )?;
        }
        if let Some(progress) = &self.progress {
            progress(Progress::WaitingForModel);
        }
        let error_secrets = self.error_secrets();
        let request = self.client.post(self.endpoint.clone()).json(&body);
        let request = if matches!(self.provider.as_ref(), "opencode" | "opencode-go") {
            request.query(&[("beta", "true")])
        } else {
            request
        };
        let response = send_http(self.authenticate(request)).await?;
        pi_rs_core::observe_provider_response(
            response.status(),
            response.headers(),
            &error_secrets,
            &self.headers,
        )
        .await;
        let response = check_status(response, &error_secrets).await?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected an Anthropic SSE response".into()));
        }
        let progress = self.progress.clone();
        let allow_empty_signature = self.allow_empty_signature;
        let reasoning = self.reasoning.clone();
        let observation_headers =
            pi_rs_core::observes_provider_stream().then(|| self.headers.clone());
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0_usize;
            let mut events = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|error| Error::RetryableModel(if error.is_timeout() && !error.is_connect() { "HTTP idle timeout" } else { "broken Anthropic HTTP stream" }.into()))?;
                size = size.saturating_add(chunk.len());
                if size > MAX_RESPONSE_BYTES {
                    return Err(Error::Model("Anthropic stream exceeds size limit".into()));
                }
                Ok(chunk)
            }).eventsource();
            let mut last_progress = Some(Progress::WaitingForModel);
            let mut report = |stage| {
                if last_progress != Some(stage) {
                    last_progress = Some(stage);
                    if let Some(progress) = &progress { progress(stage); }
                }
            };
            let mut calls = BTreeMap::<u64, Call>::new();
            let mut blocks = BTreeMap::<u64, ResponseBlock>::new();
            let mut usage = StreamUsage::default();
            let mut metadata = ModelMetadata {
                provider_api: Some(ModelApi::AnthropicMessages),
                replay_identity: Some(ResolvedModelIdentity { provider: edited_scope, model: requested_model.clone() }),
                ..ModelMetadata::default()
            };
            let mut input_transformations = None;
            let mut started = false;
            while let Some(event) = events.next().await {
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) => error,
                    _ => Error::RetryableModel("broken Anthropic SSE stream".into()),
                })?;
                let value: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("malformed Anthropic SSE event".into()))?;
                if let Some(headers) = &observation_headers {
                    pi_rs_core::observe_provider_stream(
                        &provider, ModelApi::AnthropicMessages, &requested_model, &value, &error_secrets, headers,
                    ).await;
                }
                let previous = metadata.clone();
                match value["type"].as_str() {
                    Some("message_start") if !started => {
                        started = true;
                        let message = &value["message"];
                        metadata.provider_request_id = message["id"].as_str().filter(|id|
                            id.len() <= 128 && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                        ).map(str::to_owned);
                        metadata.resolved_model = Some(ResolvedModelIdentity {
                            provider: provider.to_string(),
                            model: message["model"].as_str().unwrap_or(&requested_model).into(),
                        });
                        usage.update(&message["usage"], &mut metadata);
                        input_transformations = AnthropicInputTransformations::from_value(&message["input_transformations"]);
                    }
                    Some("content_block_start") if started => {
                        let block = &value["content_block"];
                        if block["type"] == "fallback" {
                            if !calls.is_empty() || blocks.values().any(|block| matches!(block.raw["type"].as_str(), Some("text" | "thinking" | "redacted_thinking"))) {
                                Err(Error::Model("Anthropic performed an unsupported mid-output model fallback".into()))?;
                            }
                            // Pi ignores unknown blocks; they must not cross the fallback boundary in replay.
                            blocks.clear();
                            continue;
                        }
                        let index = block_index(&value)?;
                        if blocks.contains_key(&index) || calls.contains_key(&index)
                            || blocks.len().saturating_add(calls.len()) >= 256
                        {
                            Err(Error::Model("invalid Anthropic content block index".into()))?;
                        }
                        if block["type"] == "tool_use" {
                            let id = required(&block["id"], "tool ID")?;
                            let wire_name = required(&block["name"], "tool name")?;
                            let name = oauth_tool_names.iter().find(|name| name.eq_ignore_ascii_case(wire_name)).map_or(wire_name, String::as_str);
                            if calls.len() >= 64 {
                                Err(Error::Model("too many Anthropic tool calls".into()))?;
                            }
                            let mut raw = block.clone();
                            raw["name"] = json!(name);
                            calls.insert(index, Call {
                                id: id.into(), name: name.into(), raw,
                                fragments: String::new(), complete: false,
                            });
                            report(Progress::PreparingTool);
                            yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                                index,
                                id: id.into(),
                                name: name.into(),
                                arguments: block["input"].as_object().filter(|input| !input.is_empty())
                                    .map_or_else(String::new, |input| Value::Object(input.clone()).to_string()),
                            })));
                        } else {
                            let captured = ResponseBlock::new(block);
                            match block["type"].as_str() {
                                Some("text") => {
                                    report(Progress::Generating);
                                    yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::TextStart { index });
                                    if !captured.text.is_empty() { yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::TextDelta { index, delta: captured.text.clone() }); }
                                }
                                Some("thinking" | "redacted_thinking") => {
                                    report(Progress::Thinking);
                                    yield if block["type"] == "redacted_thinking" {
                                        pi_rs_core::DetailedModelEvent::redacted_start(index)
                                    } else {
                                        pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::ReasoningStart { index })
                                    };
                                    if block["type"] == "redacted_thinking" {
                                        let text = "[Reasoning redacted]";
                                        if let Some(observer) = &reasoning { observer(text); }
                                        yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::ReasoningDelta { index, delta: text.into() });
                                    }
                                    if !captured.text.is_empty() && block["type"] == "thinking" {
                                        if let Some(observer) = &reasoning { observer(&captured.text); }
                                        yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::ReasoningDelta { index, delta: captured.text.clone() });
                                    }
                                }
                                _ => {}
                            }
                            blocks.insert(index, captured);
                        }
                    }
                    Some("content_block_delta") if started => {
                        let index = block_index(&value)?;
                        let delta = &value["delta"];
                        let captured = blocks.get_mut(&index).filter(|block| !block.complete);
                        match delta["type"].as_str() {
                            Some("text_delta") => {
                                let captured = captured.filter(|block| block.raw["type"] == "text")
                                    .ok_or_else(|| Error::Model("Anthropic text delta has no open block".into()))?;
                                if let Some(text) = delta["text"].as_str().filter(|text| !text.is_empty()) {
                                    captured.text.push_str(text);
                                    report(Progress::Generating);
                                    yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::TextDelta { index, delta: text.into() });
                                }
                            }
                            Some("thinking_delta") => {
                                let captured = captured.filter(|block| block.raw["type"] == "thinking")
                                    .ok_or_else(|| Error::Model("Anthropic thinking delta has no open block".into()))?;
                                if let Some(text) = delta["thinking"].as_str().filter(|text| !text.is_empty()) {
                                    captured.text.push_str(text);
                                    report(Progress::Thinking);
                                    if let Some(observer) = &reasoning { observer(text); }
                                    yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::ReasoningDelta { index, delta: text.into() });
                                }
                            }
                            Some("signature_delta") => {
                                let captured = captured.filter(|block| block.raw["type"] == "thinking")
                                    .ok_or_else(|| Error::Model("Anthropic signature delta has no open block".into()))?;
                                captured.signature.push_str(delta["signature"].as_str().unwrap_or_default());
                            }
                            Some("citations_delta") => {
                                if let Some(captured) = captured.filter(|block| block.raw["type"] == "text") {
                                    if !captured.raw["citations"].is_array() {
                                        captured.raw["citations"] = json!([]);
                                    }
                                    captured.raw["citations"].as_array_mut().expect("citations array")
                                        .push(delta["citation"].clone());
                                }
                            }
                            Some("input_json_delta") => {
                                let call = calls.get_mut(&index).filter(|call| !call.complete)
                                    .ok_or_else(|| Error::Model("Anthropic tool input has no open block".into()))?;
                                if let Some(arguments) = delta["partial_json"].as_str().filter(|text| !text.is_empty()) {
                                    call.fragments.push_str(arguments);
                                    yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                                        index, id: String::new(), name: String::new(), arguments: arguments.into(),
                                    })));
                                }
                            }
                            _ => {}
                        }
                    }
                    Some("content_block_stop") if started => {
                        let index = block_index(&value)?;
                        if let Some(call) = calls.get_mut(&index) {
                            call.complete = true;
                        }
                        if let Some(block) = blocks.get_mut(&index) {
                            if !block.complete {
                                match block.raw["type"].as_str() {
                                    Some("text") => yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::TextEnd { index, content: block.text.clone() }),
                                    Some("thinking") => yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::ReasoningEnd { index, content: block.text.clone() }),
                                    Some("redacted_thinking") => yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::ReasoningEnd { index, content: "[Reasoning redacted]".into() }),
                                    _ => {}
                                }
                            }
                            block.complete = true;
                        }
                    }
                    Some("message_delta") if started => {
                        usage.update(&value["usage"], &mut metadata);
                        if value["input_transformations"].is_array() {
                            input_transformations = AnthropicInputTransformations::from_value(&value["input_transformations"]);
                        }
                        if openrouter
                            && let Some(cost) = ReportedUsageCost::deserialize(&value["usage"])
                                .ok().and_then(|accounting| accounting.total())
                        {
                            metadata.cost_usd = Some(cost);
                        metadata.cost_from_provider = Some(true);
                        }
                        if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                            metadata.finish_reason = Some(match reason {
                                "end_turn" | "stop_sequence" => "stop",
                                "max_tokens" => "length",
                                "tool_use" => "tool_calls",
                                "refusal" => "content_filter",
                                _ => "other",
                            }.into());
                        }
                    }
                    Some("message_stop") if started => {
                        match metadata.finish_reason.as_deref() {
                            Some("stop" | "length") if calls.is_empty() => {}
                            Some("tool_calls") if !calls.is_empty() => {}
                            _ => Err(Error::Model("Anthropic response did not finish successfully".into()))?,
                        }
                        let reasoning_text: String = blocks.values().filter_map(|block| match block.raw["type"].as_str() {
                            Some("thinking") => Some(block.text.as_str()),
                            Some("redacted_thinking") => Some("[Reasoning redacted]"),
                            _ => None,
                        }).collect();
                        let signed = blocks.values().any(ResponseBlock::signed);
                        let mut replay = BTreeMap::new();
                        if signed {
                            for (index, block) in blocks {
                                replay.insert(index, block.finish(allow_empty_signature)?);
                            }
                        }
                        let mut completed_calls = Vec::new();
                        for (index, call) in calls {
                            let (call, raw) = call.finish()?;
                            if signed {
                                replay.insert(index, raw);
                            }
                            completed_calls.push(call);
                        }
                        if !reasoning_text.is_empty() {
                            metadata.reasoning = Some(reasoning_text);
                        }
                        if let Some(details) = input_transformations {
                            let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
                            let timestamp = i64::try_from(millis).unwrap_or(i64::MAX);
                            metadata.diagnostics.push(AssistantDiagnostic::AnthropicInputTransformations { timestamp, details });
                        }
                        if signed {
                            metadata.replay = Some(Box::new(ProviderReplay {
                                identity: ResolvedModelIdentity { provider: replay_scope.to_string(), model: requested_model },
                                blocks: replay.into_values().collect(),
                            }));
                        }
                        if metadata != previous {
                            yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Metadata(metadata)));
                        }
                        yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete { calls: completed_calls, usage: usage.finish() }));
                        return;
                    }
                    Some("error") => {
                        let kind = value["error"]["type"].as_str().unwrap_or_default();
                        Err(if permanent_quota_value(&value["error"], &error_secrets) {
                            Error::Model("Anthropic account quota or billing limit reached".into())
                        } else if matches!(kind, "overloaded_error" | "api_error" | "rate_limit_error") {
                            Error::RetryableModel("Anthropic returned a transient stream error".into())
                        } else if pi_rs_core::provider_context_overflow(&value["error"], &error_secrets) {
                            Error::Model("context window exceeded".into())
                        } else {
                            Error::Model("Anthropic returned a stream error".into())
                        })?;
                    }
                    _ => {}
                }
                if metadata != previous {
                    yield pi_rs_core::DetailedModelEvent::from(pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Metadata(metadata.clone())));
                }
            }
            Err(Error::RetryableModel("Anthropic stream ended before message_stop".into()))?;
        }))
    }
}

fn block_index(value: &Value) -> Result<u64> {
    value["index"]
        .as_u64()
        .ok_or_else(|| Error::Model("missing Anthropic block index".into()))
}

fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Model(format!("missing Anthropic {field}")))
}

#[cfg(test)]
mod tests {
    fn constrained_fixture(
        policy: pi_rs_core::StrictToolPolicy,
        schema: Value,
    ) -> (ModelRequest, pi_rs_core::ToolDeclaration) {
        let spec = pi_rs_core::ToolSpec {
            name: "lookup".into(),
            description: "lookup".into(),
            parameters: schema,
        };
        (
            ModelRequest {
                model: "claude".into(),
                messages: vec![Message::User {
                    content: "hello".into(),
                }],
                tools: vec![spec.clone()],
                max_output_tokens: 32,
            },
            pi_rs_core::ToolDeclaration {
                spec,
                constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                    strict: policy,
                }),
            },
        )
    }

    #[test]
    fn constrained_anthropic_policy_and_schema_decisions() {
        use pi_rs_core::{EffectiveToolSampling as S, StrictToolPolicy as P};
        let schema =
            json!({"type":"object","properties":{"query":{"type":"string"}},"required":[]});
        for policy in [P::Prefer, P::Require] {
            let (req, decl) = constrained_fixture(policy, schema.clone());
            let (wire, effective) =
                super::resolve_tools(&req, std::slice::from_ref(&decl), true).unwrap();
            assert_eq!(wire[0]["strict"], true);
            assert_eq!(wire[0]["input_schema"]["required"], json!(["query"]));
            assert!(matches!(effective[0].sampling, S::StrictJsonSchema { .. }));
            let no_support = super::resolve_tools(&req, &[decl], false);
            if policy == P::Require {
                assert!(no_support.is_err());
            } else {
                let (wire, effective) = no_support.unwrap();
                assert!(wire[0].get("strict").is_none());
                assert_eq!(wire[0]["input_schema"], schema);
                assert!(matches!(effective[0].sampling, S::Unconstrained));
            }
        }
        for unsupported in [
            json!({"type":"integer","minimum":1}),
            json!({"type":"string","format":"regex"}),
            json!({"type":"array","items":{"type":"string"},"minItems":2}),
        ] {
            let schema = json!({"type":"object","properties":{"x":unsupported}});
            for policy in [P::Prefer, P::Require] {
                let (req, decl) = constrained_fixture(policy, schema.clone());
                let result = super::resolve_tools(&req, &[decl], true);
                if policy == P::Require {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().0[0].get("strict").is_none());
                }
            }
        }
        let (req, mut decl) = constrained_fixture(P::Require, schema);
        decl.spec.name = "different".into();
        assert!(super::resolve_tools(&req, &[decl], true).is_err());
    }

    #[test]
    fn constrained_root_projection_and_reservation_match() {
        use pi_rs_core::{StrictToolPolicy as P, ToolDispatchContext};
        let model = Anthropic::new(DEFAULT_ENDPOINT, "key")
            .unwrap()
            .with_strict_tool_support(true);
        let context = ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "s".into(),
            summary: false,
        };
        let schema = json!({"type":"object", "anyOf":[{"type":"object"}], "oneOf":[], "allOf":[], "description":"omit", "additionalProperties":false, "$defs":{"x":{"type":"string"}}, "definitions":{}});
        let (request, decl) = constrained_fixture(P::Prefer, schema);
        let (wire, effective) =
            super::resolve_tools(&request, std::slice::from_ref(&decl), true).unwrap();
        assert!(matches!(
            effective[0].sampling,
            pi_rs_core::EffectiveToolSampling::Unconstrained
        ));
        assert_eq!(
            wire[0]["input_schema"],
            json!({"type":"object","properties":{},"required":[],"additionalProperties":false,"$defs":{"x":{"type":"string"}},"definitions":{}})
        );
        let prepared = model
            .prepare_bound_tool_dispatch(&request, &[decl], &context)
            .unwrap();
        assert_eq!(
            prepared.reservation_request().tools[0].parameters,
            wire[0]["input_schema"]
        );
        let (required, declaration) = constrained_fixture(
            P::Require,
            json!({"type":"object","anyOf":[{"type":"string"}]}),
        );
        assert!(
            model
                .prepare_bound_tool_dispatch(&required, &[declaration], &context)
                .is_err()
        );

        let (request, decl) =
            constrained_fixture(P::Require, json!({"type":"object","title":"strict title"}));
        let (wire, _) = super::resolve_tools(&request, std::slice::from_ref(&decl), true).unwrap();
        assert_eq!(wire[0]["strict"], true);
        assert_eq!(wire[0]["input_schema"]["properties"], json!({}));
        assert_eq!(wire[0]["input_schema"]["required"], json!([]));
        assert_eq!(wire[0]["input_schema"]["title"], "strict title");
        let prepared = model
            .prepare_bound_tool_dispatch(&request, &[decl], &context)
            .unwrap();
        assert_eq!(
            prepared.reservation_request().tools[0].parameters,
            wire[0]["input_schema"]
        );
        let defaults =
            super::anthropic_input_schema(&json!({"properties":null,"required":null}), false);
        assert_eq!(
            defaults,
            json!({"type":"object","properties":{},"required":[]})
        );
    }

    #[test]
    fn constrained_min_items_accepts_javascript_numeric_equivalents() {
        for number in [json!(0), json!(0.0), json!(1), json!(1.0), json!(-0.0)] {
            assert!(
                !super::anthropic_unsupported_keyword("minItems", &number),
                "{number}"
            );
        }
        for value in [json!(2), json!(0.5), json!("1"), Value::Null] {
            assert!(
                super::anthropic_unsupported_keyword("minItems", &value),
                "{value}"
            );
        }
        for number in [json!(0.0), json!(1.0)] {
            let (request, decl) = constrained_fixture(
                pi_rs_core::StrictToolPolicy::Require,
                json!({"type":"object","properties":{"items":{"type":"array","items":{"type":"string"},"minItems":number}}}),
            );
            assert!(super::resolve_tools(&request, &[decl], true).is_ok());
        }
    }

    #[tokio::test]
    async fn constrained_declaration_streams_reject_strict_and_dispatch_fallback() {
        use pi_rs_core::StrictToolPolicy as P;
        use std::io::{Read, Write};
        for summary in [false, true] {
            for policy in [P::Prefer, P::Require] {
                let (request, declaration) = constrained_fixture(policy, json!({"type":"object"}));
                let client = Anthropic::new("http://127.0.0.1:9/v1/messages", "key")
                    .unwrap()
                    .with_strict_tool_support(true)
                    .with_context_probe(false);
                let result = if summary {
                    client
                        .stream_detailed_summary_with_tool_declarations(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            request,
                            &[declaration],
                        )
                        .await
                } else {
                    client
                        .stream_detailed_with_tool_declarations(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            "session",
                            request,
                            &[declaration],
                        )
                        .await
                };
                assert!(
                    result
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("effective decisions can be retained")
                );
            }
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = socket.read(&mut buf).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&buf[..n]);
                    if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..pos]);
                        let len: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(|s| s.parse().unwrap())
                            })
                            .unwrap();
                        if data.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                let pos = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
                let body: Value = serde_json::from_slice(&data[pos + 4..]).unwrap();
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                body
            });
            let client = Anthropic::new(&format!("http://{address}/v1/messages"), "key")
                .unwrap()
                .with_context_probe(false)
                .with_cache_retention(CacheRetention::Short, true);
            let (request, declaration) = constrained_fixture(
                P::Prefer,
                json!({"type":"object","anyOf":[{"type":"object"}]}),
            );
            let result = if summary {
                client
                    .stream_detailed_summary_with_tool_declarations(
                        uuid::Uuid::nil(),
                        uuid::Uuid::nil(),
                        request,
                        &[declaration],
                    )
                    .await
            } else {
                client
                    .stream_detailed_with_tool_declarations(
                        uuid::Uuid::nil(),
                        uuid::Uuid::nil(),
                        "session",
                        request,
                        &[declaration],
                    )
                    .await
            };
            assert!(result.is_ok());
            let body = server.join().unwrap();
            assert!(body["tools"][0].get("strict").is_none());
            assert_eq!(
                body["tools"][0]["input_schema"],
                json!({"type":"object","properties":{},"required":[]})
            );
            assert_eq!(body["tools"][0].get("cache_control").is_some(), !summary);
        }
    }

    #[test]
    fn constrained_bound_model_rejects_rewritten_request() {
        use pi_rs_core::{StrictToolPolicy as P, ToolDispatchContext};
        let context = ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "s".into(),
            summary: false,
        };
        for supported in [false, true] {
            let client = Anthropic::new(DEFAULT_ENDPOINT, "key")
                .unwrap()
                .with_strict_tool_support(supported)
                .with_strict_tool_model("selected");
            for policy in [P::Prefer, P::Require] {
                let (mut request, decl) = constrained_fixture(policy, json!({"type":"object"}));
                request.model = "rewritten".into();
                let error = client
                    .prepare_bound_tool_dispatch(&request, &[decl], &context)
                    .err()
                    .unwrap();
                assert!(error.to_string().contains("cannot be resolved"));
            }
            let (mut request, mut decl) = constrained_fixture(P::Prefer, json!({"type":"object"}));
            request.model = "rewritten".into();
            decl.constrained_sampling = None;
            assert!(
                client
                    .prepare_bound_tool_dispatch(&request, &[decl], &context)
                    .is_ok()
            );
            if supported {
                assert_ne!(
                    client.context_identity(),
                    client
                        .clone()
                        .with_strict_tool_model("other")
                        .context_identity()
                );
            } else {
                assert_eq!(
                    client.context_identity(),
                    client
                        .clone()
                        .with_strict_tool_model("other")
                        .context_identity()
                );
            }
        }
    }

    #[test]
    fn constrained_final_model_uses_frozen_capability_map() {
        use pi_rs_core::{EffectiveToolSampling as S, StrictToolPolicy as P, ToolDispatchContext};
        let context = ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "s".into(),
            summary: false,
        };
        let client = Anthropic::new(DEFAULT_ENDPOINT, "key")
            .unwrap()
            .with_strict_tool_models(
                [("strict".into(), true), ("loose".into(), false)]
                    .into_iter()
                    .collect(),
            );
        let other = Anthropic::new(DEFAULT_ENDPOINT, "key")
            .unwrap()
            .with_strict_tool_models(
                [("strict".into(), false), ("loose".into(), false)]
                    .into_iter()
                    .collect(),
            );
        assert_ne!(client.context_identity(), other.context_identity());
        for (model, supported) in [("strict", true), ("loose", false)] {
            for policy in [P::Prefer, P::Require] {
                let (mut request, declaration) =
                    constrained_fixture(policy, json!({"type":"object"}));
                request.model = model.into();
                let result = client.prepare_bound_tool_dispatch(
                    &request,
                    std::slice::from_ref(&declaration),
                    &context,
                );
                if !supported && policy == P::Require {
                    assert!(result.is_err());
                } else {
                    let prepared = result.unwrap();
                    pi_rs_core::validate_prepared_tool_dispatch(
                        prepared.as_ref(),
                        &context,
                        &request,
                        &[declaration],
                    )
                    .unwrap();
                    assert_eq!(
                        matches!(
                            prepared.effective_tools()[0].sampling,
                            S::StrictJsonSchema { .. }
                        ),
                        supported
                    );
                }
            }
        }
    }

    #[test]
    fn constrained_disabled_capability_map_preserves_legacy_fingerprint() {
        let legacy = Anthropic::new(DEFAULT_ENDPOINT, "key").unwrap();
        let disabled = legacy.clone().with_strict_tool_models(
            [("selected".into(), false), ("other".into(), false)]
                .into_iter()
                .collect(),
        );
        assert_eq!(legacy.context_identity(), disabled.context_identity());
        let enabled = legacy.clone().with_strict_tool_models(
            [("selected".into(), false), ("other".into(), true)]
                .into_iter()
                .collect(),
        );
        assert_ne!(legacy.context_identity(), enabled.context_identity());
        // Prepared fallback can still alter the wire even with strict disabled.
        let (request, declaration) = constrained_fixture(
            pi_rs_core::StrictToolPolicy::Prefer,
            json!({"type":"object","title":"dropped"}),
        );
        let old = encode_request(request.clone(), &legacy.replay_scope, "", false).unwrap();
        let (wire, _) = super::resolve_tools(&request, &[declaration], false).unwrap();
        assert_ne!(old["tools"][0]["input_schema"], wire[0]["input_schema"]);
        assert_eq!(
            wire[0]["input_schema"],
            json!({"type":"object","properties":{},"required":[]})
        );
    }

    #[test]
    fn constrained_capability_preserves_legacy_context_identity() {
        let model = Anthropic::new(DEFAULT_ENDPOINT, "key").unwrap();
        let mut headers: Vec<_> = model
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort();
        let legacy = pi_rs_core::protected_model_fingerprint(&(
            headers,
            "anthropic-v1",
            model.endpoint.as_str(),
            model.provider.as_ref(),
            model.replay_scope.as_ref(),
            model.auth.0.as_str(),
            model.auth.1.as_bytes(),
            &model.thinking_level,
            &model.thinking_budgets,
            format!("{:?}", model.configured_thinking),
            model.configured_thinking_is_fallback,
            model.allow_empty_signature,
            model.cache_retention.anthropic_control(),
            model.cache_tools,
        ));
        assert!(legacy.is_some());
        assert_eq!(model.context_identity(), legacy);
        assert_eq!(
            model
                .clone()
                .with_strict_tool_support(false)
                .context_identity(),
            legacy
        );
        assert_ne!(
            model.with_strict_tool_support(true).context_identity(),
            legacy
        );
    }

    #[tokio::test]
    async fn constrained_prepared_dispatch_sends_schema_and_observes_model_tool_output() {
        use pi_rs_core::{EnrichedModelEvent, StrictToolPolicy as P, ToolDispatchContext};
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut data = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
                if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&data[..pos]);
                    let len: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|s| s.parse().unwrap())
                        })
                        .unwrap();
                    if data.len() >= pos + 4 + len {
                        break;
                    }
                }
            }
            let pos = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
            let body: Value = serde_json::from_slice(&data[pos + 4..]).unwrap();
            assert_eq!(body["tools"][0]["strict"], true);
            assert_eq!(
                body["tools"][0]["input_schema"]["required"],
                json!(["query"])
            );
            let events = [
                json!({"type":"message_start","message":{"id":"msg","model":"claude","usage":{}}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"lookup","input":{}}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":null}"}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
                json!({"type":"message_stop"}),
            ];
            let body: String = events
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        });
        let client = Anthropic::new(&format!("http://{addr}/v1/messages"), "key")
            .unwrap()
            .with_context_probe(false)
            .with_strict_tool_support(true);
        let (request, decl) = constrained_fixture(
            P::Require,
            json!({"type":"object","properties":{"query":{"type":"string"}},"required":[]}),
        );
        let context = ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "session".into(),
            summary: false,
        };
        let prepared = client
            .prepare_bound_tool_dispatch(&request, std::slice::from_ref(&decl), &context)
            .unwrap();
        pi_rs_core::validate_prepared_tool_dispatch(prepared.as_ref(), &context, &request, &[decl])
            .unwrap();
        assert_eq!(
            prepared.reservation_request().tools[0].parameters["required"],
            json!(["query"])
        );
        let mut stream = prepared.dispatch().await.unwrap();
        let mut found = false;
        while let Some(event) = stream.next().await {
            if let EnrichedModelEvent::Legacy(ModelEvent::Complete { calls, .. }) =
                event.unwrap().event
            {
                assert_eq!(calls[0].name, "lookup");
                let raw: Value = serde_json::from_str(&calls[0].arguments).unwrap();
                assert_eq!(raw, json!({"query":null}));
                assert_eq!(
                    pi_rs_core::normalize_strict_tool_arguments(&request.tools[0].parameters, &raw)
                        .unwrap(),
                    json!({})
                );
                found = true;
            }
        }
        assert!(found);
        worker.join().unwrap();
    }

    #[test]
    fn oauth_and_api_key_replay_share_only_the_official_endpoint() {
        let oauth = Anthropic::with_oauth_token(DEFAULT_ENDPOINT, "sk-ant-oat-secret").unwrap();
        let api_key = Anthropic::new(DEFAULT_ENDPOINT, "key").unwrap();
        let custom = Anthropic::new("https://other.example/v1/messages", "key").unwrap();
        assert_eq!(oauth.replay_scope, api_key.replay_scope);
        assert_ne!(oauth.replay_scope, custom.replay_scope);
        let request = ModelRequest {
            model: "claude".into(),
            tools: vec![],
            max_output_tokens: 16,
            messages: vec![Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: ResolvedModelIdentity {
                        provider: oauth.replay_scope.to_string(),
                        model: "claude".into(),
                    },
                    blocks: vec![
                        json!({"type":"thinking","thinking":"secret","signature":"signed"}),
                        json!({"type":"text","text":"answer"}),
                    ],
                })),
            }],
        };
        let same = encode_request(request.clone(), &api_key.replay_scope, "", false).unwrap();
        let other = encode_request(request, &custom.replay_scope, "", false).unwrap();
        assert!(same.to_string().contains("secret"));
        assert!(!other.to_string().contains("secret"));
    }

    #[test]
    fn oauth_credentials_are_rejected_for_untrusted_endpoints() {
        assert!(
            Anthropic::with_oauth_token("https://example.com/v1/messages", "sk-ant-oat-secret")
                .is_err()
        );
        assert!(Anthropic::with_oauth_token(DEFAULT_ENDPOINT, "sk-ant-oat-secret").is_ok());
    }

    #[tokio::test]
    async fn oauth_rejects_ambiguous_tool_names_before_network() {
        let model = Anthropic::with_oauth_token(DEFAULT_ENDPOINT, "sk-ant-oat-secret")
            .unwrap()
            .with_context_probe(false);
        let spec = |name: &str| pi_rs_core::ToolSpec {
            name: name.into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::User {
                content: "hello".into(),
            }],
            tools: vec![spec("read"), spec("Read")],
            max_output_tokens: 16,
        };
        assert!(model.stream(request).await.is_err());
    }

    #[tokio::test]
    async fn oauth_round_trip_uses_canonical_wire_names_and_restores_local_tool() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let n = socket.read(&mut buffer).unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buffer[..n]);
                if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..pos]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|n| n.parse().unwrap())
                        })
                        .unwrap();
                    if request.len() >= pos + 4 + length {
                        break;
                    }
                }
            }
            let pos = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
            let headers = String::from_utf8_lossy(&request[..pos]).to_ascii_lowercase();
            assert!(headers.contains("authorization: bearer fixture-oauth"));
            assert!(headers.contains("anthropic-beta: claude-code-20250219,oauth-2025-04-20"));
            let body: Value = serde_json::from_slice(&request[pos + 4..]).unwrap();
            assert_eq!(body["tools"][0]["name"], "Read");
            assert_eq!(
                body["system"][0]["text"],
                "You are Claude Code, Anthropic's official CLI for Claude."
            );
            let events = [
                json!({"type":"message_start","message":{"id":"msg","model":"m","usage":{}}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"Read","input":{}}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
                json!({"type":"message_stop"}),
            ];
            let body: String = events
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        });
        let model =
            Anthropic::with_oauth_token(&format!("http://{address}/v1/messages"), "fixture-oauth")
                .unwrap()
                .with_context_probe(false);
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::User {
                content: "hello".into(),
            }],
            tools: vec![pi_rs_core::ToolSpec {
                name: "read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }],
            max_output_tokens: 16,
        };
        let mut stream = model.stream(request).await.unwrap();
        let mut observed = false;
        while let Some(event) = stream.next().await {
            if let ModelEvent::Complete { calls, .. } = event.unwrap() {
                assert_eq!(calls[0].name, "read");
                observed = true;
            }
        }
        assert!(observed);
        worker.join().unwrap();
    }

    #[test]
    fn oauth_request_adds_identity_and_maps_tool_names() {
        let mut body = json!({"system":"project instructions", "tools":[{"name":"read"},{"name":"custom"}], "messages":[{"role":"assistant","content":[{"type":"tool_use","id":"one","name":"read","input":{}}]}]});
        super::prepare_oauth_request(&mut body, pi_rs_core::CacheRetention::None);
        assert_eq!(
            body["system"][0]["text"],
            "You are Claude Code, Anthropic's official CLI for Claude."
        );
        assert_eq!(body["system"][1]["text"], "project instructions");
        assert_eq!(body["tools"][0]["name"], "Read");
        assert_eq!(body["tools"][1]["name"], "custom");
        assert_eq!(body["messages"][0]["content"][0]["name"], "Read");
    }

    #[tokio::test]
    async fn enriched_stream_preserves_anthropic_block_boundaries_and_redaction() {
        use pi_rs_core::EnrichedModelEvent as E;
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let events = [
                    json!({"type":"message_start","message":{"id":"msg","model":"m","usage":{}}}),
                    json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"a"}}),
                    json!({"type":"content_block_stop","index":0}),
                    json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"b"}}),
                    json!({"type":"content_block_stop","index":1}),
                    json!({"type":"content_block_start","index":2,"content_block":{"type":"thinking","thinking":"","signature":""}}),
                    json!({"type":"content_block_delta","index":2,"delta":{"type":"thinking_delta","thinking":"thought"}}),
                    json!({"type":"content_block_start","index":3,"content_block":{"type":"thinking","thinking":"","signature":""}}),
                    json!({"type":"content_block_delta","index":3,"delta":{"type":"thinking_delta","thinking":"next"}}),
                    json!({"type":"content_block_delta","index":2,"delta":{"type":"thinking_delta","thinking":"-end"}}),
                    json!({"type":"content_block_delta","index":2,"delta":{"type":"signature_delta","signature":"sig"}}),
                    json!({"type":"content_block_stop","index":2}),
                    json!({"type":"content_block_delta","index":3,"delta":{"type":"signature_delta","signature":"sig2"}}),
                    json!({"type":"content_block_stop","index":3}),
                    json!({"type":"content_block_start","index":4,"content_block":{"type":"redacted_thinking","data":"opaque"}}),
                    json!({"type":"content_block_stop","index":4}),
                    json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{}}),
                    json!({"type":"message_stop"}),
                ];
                let body: String = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        let model = Anthropic::new(&format!("http://{address}/v1/messages"), "key")
            .unwrap()
            .with_context_probe(false);
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let events = model
            .stream_enriched(request.clone())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let items: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                E::TextStart { index } => Some(("text_start", *index)),
                E::TextEnd { index, .. } => Some(("text_end", *index)),
                E::ReasoningStart { index } => Some(("thinking_start", *index)),
                E::ReasoningEnd { index, .. } => Some(("thinking_end", *index)),
                _ => None,
            })
            .collect();
        assert_eq!(
            items,
            [
                ("text_start", 0),
                ("text_end", 0),
                ("text_start", 1),
                ("text_end", 1),
                ("thinking_start", 2),
                ("thinking_start", 3),
                ("thinking_end", 2),
                ("thinking_end", 3),
                ("thinking_start", 4),
                ("thinking_end", 4)
            ]
        );
        assert!(events.iter().any(|event| matches!(event, E::ReasoningDelta { index: 4, delta } if delta == "[Reasoning redacted]")));
        let metadata = events
            .iter()
            .rev()
            .find_map(|event| match event {
                E::Legacy(ModelEvent::Metadata(metadata)) if metadata.replay.is_some() => {
                    Some(metadata)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            metadata.reasoning.as_deref(),
            Some("thought-endnext[Reasoning redacted]")
        );
        let replay = metadata.replay.as_ref().unwrap();
        assert_eq!(replay.blocks[2]["signature"], "sig");
        assert_eq!(replay.blocks[3]["signature"], "sig2");
        assert_eq!(replay.blocks[4]["data"], "opaque");
        for summary in [false, true] {
            let detailed = if summary {
                model
                    .stream_detailed_for_summary(
                        uuid::Uuid::nil(),
                        uuid::Uuid::nil(),
                        request.clone(),
                    )
                    .await
                    .unwrap()
            } else {
                model
                    .stream_detailed_in_session_with_identity(
                        uuid::Uuid::nil(),
                        uuid::Uuid::nil(),
                        "session",
                        request.clone(),
                    )
                    .await
                    .unwrap()
            }
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
            assert!(detailed.iter().any(|event| matches!(
                event.event,
                E::ReasoningStart { index: 4 }
            ) && event.is_redacted_start()));
            assert!(detailed.iter().any(|event| matches!(&event.event, E::ReasoningDelta { index: 4, delta } if delta == "[Reasoning redacted]")));
        }
        let legacy = model
            .stream(request)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(legacy.iter().any(
            |event| matches!(event, ModelEvent::Reasoning(text) if text == "[Reasoning redacted]")
        ));
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn fallback_is_only_allowed_before_output_blocks() {
        use std::io::{Read, Write};
        for block in [
            None,
            Some(json!({"type":"future_block"})),
            Some(json!({"type":"text","text":""})),
            Some(json!({"type":"thinking","thinking":""})),
            Some(json!({"type":"redacted_thinking","data":"opaque"})),
            Some(json!({"type":"tool_use","id":"call","name":"read","input":{}})),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let unknown = block
                .as_ref()
                .is_some_and(|block| block["type"] == "future_block");
            let expect_error = block.is_some() && !unknown;
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let mut events = vec![
                    json!({"type":"message_start","message":{"id":"msg","model":"m","usage":{}}}),
                ];
                if let Some(block) = block {
                    events.push(
                        json!({"type":"content_block_start","index":0,"content_block":block}),
                    );
                }
                events.push(json!({"type":"content_block_start","index":1,"content_block":{"type":"fallback"}}));
                if unknown {
                    events.push(json!({"type":"content_block_start","index":2,"content_block":{"type":"thinking","thinking":"thought","signature":"sig"}}));
                    events.push(json!({"type":"content_block_stop","index":2}));
                }
                events.push(
                    json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{}}),
                );
                events.push(json!({"type":"message_stop"}));
                let body: String = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = Anthropic::new(&format!("http://{address}/v1/messages"), "key")
                .unwrap()
                .with_context_probe(false);
            let request = ModelRequest {
                model: "m".into(),
                messages: vec![Message::User {
                    content: "hello".into(),
                }],
                tools: vec![],
                max_output_tokens: 16,
            };
            let mut stream = model.stream(request).await.unwrap();
            let mut completed = false;
            let mut fallback_error = false;
            let mut replay = None;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ModelEvent::Complete { .. }) => completed = true,
                    Ok(ModelEvent::Metadata(metadata)) if metadata.replay.is_some() => {
                        replay = metadata.replay
                    }
                    Err(Error::Model(message))
                        if message.contains("unsupported mid-output model fallback") =>
                    {
                        fallback_error = true
                    }
                    Err(error) => panic!("unexpected error: {error}"),
                    _ => {}
                }
            }
            worker.join().unwrap();
            assert_eq!(fallback_error, expect_error);
            assert_eq!(completed, !expect_error);
            if unknown {
                let replay = replay.expect("signed thinking replay");
                assert_eq!(replay.blocks.len(), 1);
                assert_eq!(replay.blocks[0]["type"], "thinking");
            }
        }
    }

    #[test]
    fn overflow_http_classification_is_bounded_and_hides_credentials() {
        use std::io::{Read, Write};
        for (status, body, overflow) in [
            (400, r#"{"error":{"message":"prompt is too long"}}"#, true),
            (
                400,
                r#"{"error":{"message":"other"},"request_id":"context_length_exceeded"}"#,
                false,
            ),
            (
                400,
                r#"{"error":{"message":"prompt is too long secret-token"}}"#,
                true,
            ),
            (
                429,
                r#"{"error":{"message":"rate limit: too many tokens"}}"#,
                false,
            ),
            (413, r#"{"error":{"message":"other"}}"#, true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 2048];
                while !request.ends_with(b"\r\n\r\n") {
                    let count = socket.read(&mut buffer).unwrap();
                    assert!(count > 0 && request.len() + count <= 16 * 1024);
                    request.extend_from_slice(&buffer[..count]);
                }
                write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let error = runtime
                .block_on(super::send(
                    reqwest::Client::new().get(format!("http://{address}/")),
                    &["secret-token".into()],
                ))
                .unwrap_err();
            let message = error.to_string();
            assert_eq!(
                message.contains("context window exceeded")
                    || message.contains("413 request too large"),
                overflow,
                "{message}"
            );
            assert!(!message.contains("secret-token"));
            worker.join().unwrap();
        }
    }

    #[test]
    fn tool_arguments_use_correctly_rounded_decimal_on_wire() {
        use pi_rs_core::{Message, Model, ModelRequest, ToolCall};
        use std::io::{BufRead, Read, Write};
        use std::time::Duration;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let worker = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "request did not connect"
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut socket = std::io::BufReader::new(socket);
            let mut length = None;
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let mut body = vec![0; length.expect("request content length")];
            socket.read_exact(&mut body).unwrap();
            socket.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            String::from_utf8(body).unwrap()
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let model =
                super::Anthropic::new(&format!("http://{address}/messages"), "key").unwrap();
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![Message::Assistant {
                    content: String::new(),
                    calls: vec![ToolCall {
                        id: "id".into(),
                        name: "c".into(),
                        arguments: r#"{"x":34450847700860466777e-23}"#.into(),
                    }],
                    replay: None,
                }],
                tools: vec![],
                max_output_tokens: 16,
            };
            let _stream = model.stream(request).await.unwrap();
        });
        let wire = worker.join().unwrap();
        assert!(
            wire.contains(r#""input":{"x":0.00034450847700860464}"#),
            "{wire}"
        );
    }

    #[test]
    fn streamed_tool_argument_decimals_are_rounded_before_replay_and_execution() {
        let call = super::Call {
            id: "id".into(),
            name: "capture".into(),
            raw: serde_json::json!({"type":"tool_use", "id":"id", "name":"capture", "input":{}}),
            fragments: r#"{"x":34450847700860466777e-23}"#.into(),
            complete: true,
        };
        let (call, replay) = call.finish().unwrap();
        let execution = pi_rs_core::parse_tool_arguments(&call.arguments).unwrap();
        let expected = 0.00034450847700860464_f64.to_bits();
        assert_eq!(execution["x"].as_f64().unwrap().to_bits(), expected);
        assert_eq!(replay["input"]["x"].as_f64().unwrap().to_bits(), expected);
    }

    #[test]
    fn rate_limit_response_classification_and_redaction() {
        use std::io::{Read, Write};
        for (status, body, permanent) in [
            (
                429,
                r#"{"error":{"code":"insufficient_quota","secret":"sentinel-secret"}}"#,
                true,
            ),
            (503, r#"{"error":{"message":"quota exceeded"}}"#, true),
            (408, r#"{"error":{"message":"quota exceeded"}}"#, true),
            (
                429,
                r#"{"error":{"message":"rate limit exceeded"},"request_id":"billing-sentinel-secret"}"#,
                false,
            ),
            (
                503,
                r#"{"error":{"message":"temporarily unavailable","request_id":"billing-sentinel-secret"}}"#,
                false,
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0; 2048];
                let _ = stream.read(&mut request).unwrap();
                write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let error = runtime
                .block_on(super::send(
                    reqwest::Client::new().get(format!("http://{address}/")),
                    &[],
                ))
                .unwrap_err();
            assert_eq!(matches!(error, pi_rs_core::Error::Model(_)), permanent);
            assert_eq!(
                matches!(error, pi_rs_core::Error::RetryableModel(_)),
                !permanent
            );
            if permanent {
                assert!(
                    matches!(&error, pi_rs_core::Error::Model(message) if message == "Anthropic account quota or billing limit reached")
                );
            } else {
                assert!(
                    matches!(&error, pi_rs_core::Error::RetryableModel(message) if message.starts_with(&format!("Anthropic returned HTTP {status} ")))
                );
            }
            assert!(!error.to_string().contains("sentinel-secret"));
            server.join().unwrap();
        }
    }

    #[test]
    fn permanent_quota_markers_do_not_retry() {
        for body in [
            r#"{"error":{"code":"insufficient_quota"}}"#,
            r#"{"code" : "1308"}"#,
            "FREEUSAGELIMITERROR",
            "Monthly usage limit reached",
            "BILLING",
        ] {
            assert!(
                super::permanent_quota_error(body.as_bytes(), true, &[]),
                "{body}"
            );
        }
        assert!(!super::permanent_quota_error(
            br#"{"code":1308"#,
            false,
            &[]
        ));
        assert!(!super::permanent_quota_error(
            br#"{"error":{"message":"rate limit"},"request_id":"billing"}"#,
            true,
            &[]
        ));
        for body in [r#"{"code":13080}"#, "rate limit exceeded", "server busy"] {
            assert!(
                !super::permanent_quota_error(body.as_bytes(), true, &[]),
                "{body}"
            );
        }
    }

    #[test]
    fn stream_quota_error_precedes_transient_type_without_metadata_leaks() {
        use futures_util::StreamExt;
        use pi_rs_core::Model;
        use std::io::{Read, Write};
        for case in [0, 1, 2] {
            let permanent = case == 1;
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let event = serde_json::json!({"type":"error","error":{"type":"overloaded_error", "code": if case == 2 {13080} else {0}, "message": if permanent {"quota exceeded"} else {"temporarily overloaded sk-ant-billing-secret"}, "request_id":"billing-sentinel-secret"}});
                let body = format!("event: error\ndata: {event}\n\n");
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let error = runtime.block_on(async {
                let model = super::Anthropic::new(
                    &format!("http://{address}/messages"),
                    "sk-ant-billing-secret",
                )
                .unwrap();
                let request = pi_rs_core::ModelRequest {
                    model: "m".into(),
                    messages: vec![pi_rs_core::Message::User {
                        content: "hello".into(),
                    }],
                    tools: vec![],
                    max_output_tokens: 16,
                };
                let mut stream = match model.stream(request).await {
                    Ok(stream) => stream,
                    Err(error) => panic!("{error}"),
                };
                loop {
                    match stream.next().await {
                        Some(Err(error)) => break error,
                        Some(Ok(_)) => {}
                        None => panic!("missing error"),
                    }
                }
            });
            assert_eq!(matches!(error, pi_rs_core::Error::Model(_)), permanent);
            assert_eq!(
                matches!(error, pi_rs_core::Error::RetryableModel(_)),
                !permanent
            );
            assert!(!error.to_string().contains("sentinel-secret"));
            assert!(!error.to_string().contains("sk-ant-billing-secret"));
            worker.join().unwrap();
        }
    }

    #[test]
    fn bounded_http_error_keeps_complete_quota_field() {
        use std::io::{Read, Write};
        for case in [0, 1, 2] {
            let permanent = case == 1;
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 2048];
                let _ = socket.read(&mut request).unwrap();
                let body = if case == 2 {
                    let head = r#"{"error":{"padding":""#;
                    let tail = r#"","code":1308"#;
                    format!(
                        "{head}{}{tail}0}}}}",
                        "x".repeat(64 * 1024 - head.len() - tail.len())
                    )
                } else {
                    let kind = if permanent {
                        "insufficient_quota"
                    } else {
                        "overloaded_error"
                    };
                    format!(
                        "{{\"error\":{{\"type\":\"{kind}\",\"request_id\":\"billing-{}\"}}}}",
                        "x".repeat(70_000)
                    )
                };
                let _ = write!(
                    socket,
                    "HTTP/1.1 503 Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
            });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let error = runtime
                .block_on(super::send(
                    reqwest::Client::builder()
                        .no_proxy()
                        .build()
                        .unwrap()
                        .get(format!("http://{address}/")),
                    &[],
                ))
                .unwrap_err();
            assert_eq!(matches!(error, pi_rs_core::Error::Model(_)), permanent);
            assert_eq!(
                matches!(error, pi_rs_core::Error::RetryableModel(_)),
                !permanent
            );
            assert!(!error.to_string().contains("billing-"));
            worker.join().unwrap();
        }
    }

    #[test]
    fn numeric_prefix_requires_a_delimiter_and_credentials_are_masked() {
        let head = r#"{"error":{"padding":""#;
        let tail = r#"","code":1308"#;
        let mut bytes = format!(
            "{head}{}{tail}",
            "x".repeat(64 * 1024 - head.len() - tail.len())
        )
        .into_bytes();
        assert_eq!(bytes.len(), 64 * 1024);
        assert!(!super::permanent_quota_error(&bytes, false, &[]));
        bytes.push(b' ');
        assert!(super::permanent_quota_error(&bytes, false, &[]));
        bytes.pop();
        let delimited = format!(
            "{{\"error\":{{\"code\":1308,\"padding\":\"{}\"}}}}",
            "x".repeat(70_000)
        );
        assert!(super::permanent_quota_error(
            &delimited.as_bytes()[..64 * 1024],
            false,
            &[]
        ));
        let string_tail = r#"","type":"insufficient_quota""#;
        let string_boundary = format!(
            "{head}{}{string_tail}",
            "x".repeat(64 * 1024 - head.len() - string_tail.len())
        );
        assert_eq!(string_boundary.len(), 64 * 1024);
        assert!(super::permanent_quota_error(
            string_boundary.as_bytes(),
            false,
            &[]
        ));
        bytes.extend_from_slice(b"0}} ");
        assert!(!super::permanent_quota_error(&bytes, true, &[]));
        let secret = "sk-ant-billing-secret".to_string();
        let body = br#"{"error":{"type":"overloaded_error","message":"rate limited sk-ant-billing-secret"}}"#;
        assert!(!super::permanent_quota_error(body, true, &[secret]));
    }

    #[test]
    fn http_error_masks_the_actual_request_credential() {
        use pi_rs_core::Model;
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 8192];
            let _ = socket.read(&mut request).unwrap();
            let body = r#"{"error":{"type":"overloaded_error","message":"rate limited sk-ant-billing-secret"}}"#;
            write!(
                socket,
                "HTTP/1.1 503 Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime.block_on(async {
            let model = super::Anthropic::new(
                &format!("http://{address}/messages"),
                "sk-ant-billing-secret",
            )
            .unwrap();
            let request = pi_rs_core::ModelRequest {
                model: "m".into(),
                messages: vec![pi_rs_core::Message::User {
                    content: "hello".into(),
                }],
                tools: vec![],
                max_output_tokens: 16,
            };
            match model.stream(request).await {
                Err(error) => error,
                Ok(_) => panic!("expected error"),
            }
        });
        assert!(matches!(error, pi_rs_core::Error::RetryableModel(_)));
        assert!(!error.to_string().contains("sk-ant-billing-secret"));
        worker.join().unwrap();
    }

    #[test]
    fn bounded_json_retains_only_complete_error_fields() {
        let long = "x".repeat(70_000);
        for prefix in [
            r#"{"error":{"type":"insufficient_quota","detail":""#,
            r#"{"request_id":"other","error":{"message":"quota exceeded","extra":""#,
        ] {
            let body = format!("{prefix}{long}\"}}}}");
            assert!(super::permanent_quota_error(
                &body.as_bytes()[..64 * 1024],
                false,
                &[]
            ));
        }
        for prefix in [
            r#"{"request_id":"billing","error":{"message":"temporary","extra":""#,
            r#"{"error":{"metadata":{"type":"insufficient_quota"},"message":"temporary","extra":""#,
            r#"{"error":{"message":"billing"#,
            r#"{"error":{"type":"insufficient_quota","type":""#,
        ] {
            let body = format!("{prefix}{long}");
            assert!(!super::permanent_quota_error(
                &body.as_bytes()[..64 * 1024],
                false,
                &[]
            ));
        }
        assert!(!super::permanent_quota_error(
            br#"{"error":{"type":"insufficient_quota",!"#,
            false,
            &[]
        ));
    }

    use super::*;

    #[cfg(any(unix, windows))]
    #[test]
    fn context_identity_tracks_static_route_and_configuration() {
        let make = |endpoint: &str, key: &str| Anthropic::new(endpoint, key).unwrap();
        let first = make(DEFAULT_ENDPOINT, "key-a");
        assert!(first.context_identity().is_some());
        assert_eq!(first.context_identity(), first.clone().context_identity());
        assert_ne!(
            first.context_identity(),
            make(DEFAULT_ENDPOINT, "key-b").context_identity()
        );
        assert_ne!(
            first.context_identity(),
            make("https://example.com/v1/messages", "key-a").context_identity()
        );
        assert_ne!(
            first.context_identity(),
            first
                .clone()
                .with_configured_thinking("high", true, true)
                .context_identity()
        );
        let configured = first.clone().with_configured_thinking("high", true, true);
        assert!(configured.context_identity().is_none());
        assert!(
            configured
                .with_context_probe(false)
                .context_identity()
                .is_some()
        );
        assert!(
            first
                .with_thinking(Some("high"))
                .context_identity()
                .is_none()
        );
    }

    #[test]
    fn capacity_estimate_matches_image_and_replay_serialization() {
        let model = Anthropic::new(DEFAULT_ENDPOINT, "key").unwrap();
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 16,
        };
        let tool = |n| {
            Message::Tool { call_id: "c".into(), content: json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(n)}]}}).to_string(), is_error: false, is_structured: Some(true) }
        };
        let small = model.estimate_message_tokens(&request, &tool(4));
        let large = model.estimate_message_tokens(&request, &tool(20000));
        assert!(large > small + 4000);
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "anthropic".into(),
                model: "fixture".into(),
            },
            blocks: vec![
                json!({"type":"text","text":"answer"}),
                json!({"type":"thinking","thinking":"x".repeat(5000),"signature":"sig"}),
            ],
        };
        let assistant = Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: Some(Box::new(replay)),
        };
        assert!(model.estimate_message_tokens(&request, &assistant) > 1000);
        let mismatch = if let Message::Assistant {
            replay: Some(mut replay),
            ..
        } = assistant.clone()
        {
            replay.identity.model = "different".into();
            Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(replay),
            }
        } else {
            unreachable!()
        };
        assert_eq!(
            model.estimate_message_tokens(&request, &mismatch),
            pi_rs_core::estimate_message_tokens(&mismatch)
        );
    }

    #[test]
    fn edited_replay_preserves_whitespace_and_thinking() {
        let scope = pi_rs_core::endpoint_replay_scope("anthropic", "https://one.example/messages");
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::Assistant {
                content: "first \nsecond".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: ResolvedModelIdentity {
                        provider: scope.clone(),
                        model: "m".into(),
                    },
                    blocks: vec![
                        json!({"type":"text","text":"first \n"}),
                        json!({"type":"text","text":"second"}),
                        json!({"type":"thinking","thinking":"secret","signature":"sig"}),
                        json!({"_pi_context_edit_api":"anthropic-messages"}),
                    ],
                })),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let body = encode_request(request, "anthropic", &scope, false).unwrap();
        assert_eq!(
            body["messages"][0]["content"],
            json!([{"type":"text","text":"first \n"},{"type":"text","text":"second"},{"type":"thinking","thinking":"secret","signature":"sig"}])
        );
    }

    #[test]
    fn edited_thinking_is_bound_to_the_original_endpoint() {
        let scope_a = pi_rs_core::endpoint_replay_scope("anthropic", "https://a.example/messages");
        let scope_b = pi_rs_core::endpoint_replay_scope("anthropic", "https://b.example/messages");
        let request = ModelRequest {
            model: "claude".into(),
            messages: vec![
                Message::User {
                    content: "question".into(),
                },
                Message::Assistant {
                    content: String::new(),
                    calls: vec![],
                    replay: Some(Box::new(ProviderReplay {
                        identity: ResolvedModelIdentity {
                            provider: scope_a.clone(),
                            model: "claude".into(),
                        },
                        blocks: vec![
                            json!({"type":"thinking","thinking":"secret","signature":"sig"}),
                            json!({"_pi_context_edit_api":"anthropic-messages"}),
                        ],
                    })),
                },
            ],
            tools: vec![],
            max_output_tokens: 16,
        };
        let same = encode_request(request.clone(), "anthropic", &scope_a, false).unwrap();
        assert_eq!(same["messages"][1]["content"][0]["thinking"], "secret");
        let other = encode_request(request, "anthropic", &scope_b, false).unwrap();
        assert_eq!(other["messages"].as_array().unwrap().len(), 1);
        assert!(!other.to_string().contains("secret"));
    }

    #[test]
    fn edited_replay_is_scoped_and_private_marker_is_not_sent() {
        let make = |model: &str| ModelRequest {
            model: model.into(),
            tools: vec![],
            max_output_tokens: 16,
            messages: vec![Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: ResolvedModelIdentity {
                        provider: "anthropic".into(),
                        model: "claude".into(),
                    },
                    blocks: vec![
                        json!({"type":"thinking","thinking":"secret","signature":"signed"}),
                        json!({"type":"text","text":"answer"}),
                        json!({"_pi_context_edit_api":"anthropic-messages"}),
                    ],
                })),
            }],
        };
        let same = encode_request(make("claude"), "anthropic", "anthropic", false).unwrap();
        assert_eq!(
            same["messages"][0]["content"],
            json!([{"type":"thinking","thinking":"secret","signature":"signed"},{"type":"text","text":"answer"}])
        );
        let other = encode_request(make("other"), "anthropic", "anthropic", false).unwrap();
        assert_eq!(
            other["messages"][0]["content"],
            json!([{"type":"text","text":"answer"}])
        );
    }

    #[test]
    fn tool_result_error_status_is_encoded_independently_of_text() {
        let body = encode_request(
            ModelRequest {
                model: "fixture".into(),
                messages: vec![
                    Message::User {
                        content: "go".into(),
                    },
                    Message::Tool {
                        call_id: "failed".into(),
                        content: "normal text".into(),
                        is_error: true,
                        is_structured: Some(false),
                    },
                    Message::Tool {
                        call_id: "ok".into(),
                        content: "Tool failed: but succeeded".into(),
                        is_error: false,
                        is_structured: Some(false),
                    },
                ],
                tools: vec![],
                max_output_tokens: 128,
            },
            "fixture",
            "fixture",
            false,
        )
        .unwrap();
        let blocks = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["content"].as_array().unwrap())
            .filter(|block| block["type"] == "tool_result")
            .collect::<Vec<_>>();
        assert_eq!(blocks[0]["is_error"], true);
        assert_eq!(blocks[0]["content"], "normal text");
        assert!(blocks[1].get("is_error").is_none());
        assert_eq!(blocks[1]["content"], "Tool failed: but succeeded");
    }

    #[test]
    fn input_transformations_keep_only_diagnostic_fields() {
        let details = AnthropicInputTransformations::from_value(&json!([
            {"type":"thinking_dropped","path":"messages[1].content[0]","reason":"invalid_signature","thinking":"private"},
            null,
            {"type":"other","reason":3}
        ]))
        .unwrap();
        let diagnostic = AssistantDiagnostic::AnthropicInputTransformations {
            timestamp: 123,
            details,
        };
        assert_eq!(diagnostic.dropped_thinking_blocks(), 1);
        assert_eq!(
            serde_json::to_value(diagnostic).unwrap(),
            json!({
                "type":"anthropic_input_transformations",
                "timestamp":123,
                "details":{"transformations":[
                    {"type":"thinking_dropped","path":"messages[1].content[0]","reason":"invalid_signature"},
                    {"type":"other"}
                ]}
            })
        );
        assert!(AnthropicInputTransformations::from_value(&json!([])).is_none());
    }

    #[test]
    fn thinking_controls_respect_advertised_effort_and_output_budget() {
        let adaptive = decode_model(&json!({
            "id":"adaptive",
            "capabilities":{
                "thinking":{"types":{"adaptive":{"supported":true},"disabled":{"supported":false}}},
                "effort":{"low":{"supported":true},"high":{"supported":true}}
            }
        }))
        .unwrap();
        assert_eq!(adaptive.thinking_levels, ["low", "high"]);
        let mut body = json!({});
        assert!(configure_thinking(&mut body, Some(&adaptive), "off", 2048, None).is_err());
        configure_thinking(&mut body, Some(&adaptive), "high", 2048, None).unwrap();
        assert_eq!(
            body["thinking"],
            json!({"type":"adaptive","display":"summarized"})
        );
        assert_eq!(body["output_config"]["effort"], "high");

        let legacy = decode_model(&json!({
            "id":"legacy","capabilities":{"thinking":{"types":{"enabled":{"supported":true}}}}
        }))
        .unwrap();
        let mut body = json!({});
        assert!(configure_thinking(&mut body, Some(&legacy), "high", 1024, None).is_err());
        configure_thinking(&mut body, Some(&legacy), "high", 2048, None).unwrap();
        assert_eq!(
            body["thinking"],
            json!({"type":"enabled","budget_tokens":1024,"display":"summarized"})
        );
        let budgets = json!({"high": 3456, "low": "invalid"});
        configure_thinking(&mut body, Some(&legacy), "xhigh", 10000, Some(&budgets)).unwrap();
        assert_eq!(body["thinking"]["budget_tokens"], 3456);
        assert!(
            configure_thinking(&mut body, Some(&legacy), "low", 10000, Some(&budgets)).is_err()
        );
        configure_thinking(&mut body, Some(&legacy), "high", 2048, Some(&budgets)).unwrap();
        assert_eq!(body["thinking"]["budget_tokens"], 1024);
        for (level, expected) in [
            ("minimal", 1024),
            ("low", 2048),
            ("medium", 8192),
            ("high", 16384),
            ("xhigh", 16384),
        ] {
            configure_thinking(&mut body, Some(&legacy), level, 20000, None).unwrap();
            assert_eq!(body["thinking"]["budget_tokens"], expected);
        }
    }

    #[test]
    fn tool_blocks_must_close_with_an_object_before_dispatch() {
        let call = |complete, fragments: &str| Call {
            id: "toolu_fixture".into(),
            name: "read".into(),
            raw: json!({"type":"tool_use","id":"toolu_fixture","name":"read","input":{}}),
            fragments: fragments.into(),
            complete,
        };
        assert!(call(false, r#"{"path":"file.txt"}"#).finish().is_err());
        assert!(call(true, r#"{"path":"file.txt""#).finish().is_err());
        assert!(call(true, "[]").finish().is_err());
        let (result, _) = call(true, r#"{"path":"file.txt"}"#).finish().unwrap();
        assert_eq!(result.arguments, r#"{"path":"file.txt"}"#);
    }
}
