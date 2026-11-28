//! CLI configuration and Radius account state for the native Pi Messages transport.
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
use futures_util::StreamExt;
use pi_rs::{Error, Message, Model, ModelRequest, ModelStream, Result};
use reqwest::{Url, header::HeaderMap};
#[cfg(test)]
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use pi_rs_workbench::image_profile::ImageResizeProfile;
pub(super) use pi_rs_workbench::native_pi_messages::{Progress, is_pi_messages_replay};
use pi_rs_workbench::native_pi_messages::{ProgressObserver, RequestOptions, Transport};

use super::{Provider, model_profiles::Profiles, radius_auth::Gateway, radius_catalog};

#[derive(Clone)]
pub(super) struct PiMessages {
    transport: Transport,
    is_radius: bool,
    token: Option<String>,
    headers: HeaderMap,
    thinking: Option<String>,
    cache_disabled: bool,
    progress: Option<ProgressObserver>,
    radius: Option<Radius>,
    probe_catalog: bool,
    identity_safe_transport: bool,
    prepared_limits: Option<(Option<u32>, Option<u32>)>,
    request_image_profiles: Option<Arc<Profiles>>,
    prepared_image_resize: Option<ImageResizeProfile>,
}

#[derive(Clone)]
struct Radius {
    gateway: Gateway,
    catalog: RadiusCatalogCache,
}

#[derive(Clone, Default)]
pub(super) struct RadiusCatalogCache(Arc<tokio::sync::Mutex<Option<CachedCatalog>>>);

struct CachedCatalog {
    gateway: Url,
    binding: String,
    token_hash: Option<[u8; 32]>,
    value: Arc<radius_catalog::Catalog>,
}

impl PiMessages {
    pub(super) fn new(provider: &str, endpoint: &str, token: String) -> Result<Self> {
        Self::build(provider, endpoint, Some(token))
    }

    pub(super) fn radius(endpoint: &str, token: Option<String>, gateway: Gateway) -> Result<Self> {
        let mut client = Self::build("radius", endpoint, token)?;
        client.radius = Some(Radius {
            gateway,
            catalog: RadiusCatalogCache::default(),
        });
        Ok(client)
    }

    fn build(provider: &str, endpoint: &str, token: Option<String>) -> Result<Self> {
        Ok(Self {
            transport: Transport::new(provider, endpoint)?,
            is_radius: provider == "radius",
            token,
            headers: HeaderMap::new(),
            thinking: None,
            cache_disabled: false,
            progress: None,
            radius: None,
            probe_catalog: true,
            identity_safe_transport: true,
            prepared_limits: None,
            request_image_profiles: None,
            prepared_image_resize: None,
        })
    }

    pub(super) fn with_radius_catalog_cache(mut self, cache: RadiusCatalogCache) -> Self {
        if let Some(radius) = &mut self.radius {
            radius.catalog = cache;
        }
        self
    }

    pub(super) fn with_context_probe(mut self, enabled: bool) -> Self {
        self.probe_catalog = enabled;
        self
    }

    pub(super) fn with_radius_request_profiles(mut self, profiles: Profiles) -> Self {
        if self.radius.is_some() {
            self.request_image_profiles = Some(Arc::new(profiles));
        }
        self
    }

    fn catalog_image_resize(
        &self,
        catalog: &radius_catalog::Catalog,
        model: &str,
    ) -> Option<ImageResizeProfile> {
        let profiles = self.request_image_profiles.as_ref()?;
        let configured = profiles.model(&Provider::Radius, model);
        catalog
            .metadata(model)?
            .input_limits(configured.as_ref())?
            .image_resize()
    }

    async fn access(&self) -> Result<(String, String)> {
        if let Some(token) = &self.token {
            return Ok((token.clone(), key_binding(token)));
        }
        let radius = self.radius.as_ref().expect("OAuth is only used by Radius");
        radius
            .gateway
            .access_with_login(std::time::Duration::from_secs(60))
            .await?
            .map(|(token, login)| (token, format!("oauth:{login}")))
            .ok_or_else(|| Error::Config("Radius credentials were removed. Sign in again.".into()))
    }

    pub(super) async fn catalog(&self) -> Result<Option<Arc<radius_catalog::Catalog>>> {
        let Some(radius) = &self.radius else {
            return Ok(None);
        };
        if !self.probe_catalog {
            let binding = match &self.token {
                Some(token) => Some(key_binding(token)),
                None => radius
                    .gateway
                    .cached_login_id()?
                    .map(|login| format!("oauth:{login}")),
            };
            return match binding {
                Some(binding) => self.catalog_for(&binding, None).await,
                None => Ok(None),
            };
        }
        let (token, binding) = self.access().await?;
        self.catalog_for(&binding, Some(&token)).await
    }

    pub(super) async fn operation_catalog_with_token(
        &self,
    ) -> Result<Option<(Arc<radius_catalog::Catalog>, String)>> {
        if self.radius.is_none() {
            return Ok(None);
        }
        let (token, binding) = self.access().await?;
        let catalog = self
            .catalog_for(&binding, self.probe_catalog.then_some(token.as_str()))
            .await?;
        Ok(catalog.map(|catalog| (catalog, token)))
    }

    async fn catalog_for(
        &self,
        binding: &str,
        token: Option<&str>,
    ) -> Result<Option<Arc<radius_catalog::Catalog>>> {
        let Some(radius) = &self.radius else {
            return Ok(None);
        };
        let token_hash = token.map(|token| <[u8; 32]>::from(Sha256::digest(token.as_bytes())));
        let gateway = radius.gateway.url("/");
        let mut cached = radius.catalog.0.lock().await;
        if let Some(cached) = cached.as_ref().filter(|cached| {
            cached.gateway == gateway
                && cached.binding == binding
                && (token.is_none() || cached.token_hash == token_hash)
        }) {
            return Ok(Some(cached.value.clone()));
        }
        let fallback = || {
            radius_catalog::cached(&gateway, binding).or_else(|| radius_catalog::baseline(&gateway))
        };
        let (value, confirmed_token_hash) = if let Some(token) = token {
            match radius_catalog::load(self.transport.http_client(), &gateway, token).await {
                Ok(catalog) => {
                    let _ = radius_catalog::remember(&gateway, binding, &catalog);
                    (Some(Arc::new(catalog)), token_hash)
                }
                // A fallback can serve this request, but does not confirm
                // what the gateway offers for the current token.
                Err(error @ Error::RetryableModel(_)) => (Some(fallback().ok_or(error)?), None),
                Err(error) => return Err(error),
            }
        } else {
            (fallback(), None)
        };
        *cached = value.as_ref().map(|value| CachedCatalog {
            gateway,
            binding: binding.to_owned(),
            token_hash: confirmed_token_hash,
            value: value.clone(),
        });
        Ok(value)
    }

    pub(super) fn context_headers(&self) -> HeaderMap {
        self.headers.clone()
    }

    pub(super) async fn prepare_header_override(&self, headers: &HeaderMap) -> Option<HeaderMap> {
        pi_rs::prepare_provider_headers(headers, &self.token.iter().cloned().collect::<Vec<_>>())
            .await
    }

    /// Freeze credentials and the catalog-selected route for one request.
    pub(super) async fn prepare_context(&self, model: &str) -> Result<Self> {
        if self.radius.is_none() {
            return Ok(self.clone());
        }
        let (token, binding) = self.access().await?;
        let catalog = match self
            .catalog_for(&binding, self.probe_catalog.then_some(token.as_str()))
            .await
        {
            Ok(catalog) => catalog,
            Err(Error::RetryableModel(_)) => None,
            Err(error) => return Err(error),
        };
        let mut prepared = self.clone();
        let details = catalog
            .as_ref()
            .and_then(|catalog| catalog.models.iter().find(|item| item.id == model))
            .map(|item| item.details());
        if let Some(catalog) = catalog {
            prepared.transport.set_endpoint(catalog.endpoint.clone())?;
            prepared.prepared_image_resize = self.catalog_image_resize(&catalog, model);
        }
        prepared.prepared_limits = Some((
            details.as_ref().and_then(|d| d.context_window),
            details.as_ref().and_then(|d| d.max_output_tokens),
        ));
        prepared.token = Some(token);
        prepared.radius = None;
        Ok(prepared)
    }

    pub(super) async fn models(&self) -> Result<Vec<String>> {
        Ok(self.catalog().await?.map_or_else(Vec::new, |catalog| {
            catalog
                .models
                .iter()
                .map(|model| model.id.clone())
                .collect()
        }))
    }

    pub(super) fn aliases(&self) -> Vec<(String, String)> {
        self.radius
            .as_ref()
            .and_then(|radius| {
                let cache = radius.catalog.0.try_lock().ok()?;
                Some(
                    cache
                        .as_ref()?
                        .value
                        .models
                        .iter()
                        .map(|model| (model.id.clone(), model.name.clone()))
                        .collect(),
                )
            })
            .unwrap_or_default()
    }

    pub(super) async fn discovered_catalog(&self) -> Option<Arc<radius_catalog::Catalog>> {
        let cache = self.radius.as_ref()?.catalog.0.lock().await;
        cache.as_ref().map(|entry| entry.value.clone())
    }

    pub(super) async fn model_details(
        &self,
        ids: &[String],
    ) -> BTreeMap<String, pi_rs::ollama::ModelDetails> {
        self.catalog()
            .await
            .ok()
            .flatten()
            .map_or_else(BTreeMap::new, |catalog| {
                catalog
                    .models
                    .iter()
                    .filter(|model| ids.contains(&model.id))
                    .map(|model| (model.id.clone(), model.details()))
                    .collect()
            })
    }

    pub(super) fn with_http_options(
        mut self,
        timeout: Option<std::time::Duration>,
        proxies: &[reqwest::Proxy],
    ) -> Result<Self> {
        self.transport.with_http_options(timeout, proxies)?;
        self.identity_safe_transport = proxies.is_empty();
        if let Some(radius) = &mut self.radius {
            radius.gateway = radius.gateway.clone().with_http_options(proxies, timeout)?;
        }
        Ok(self)
    }

    pub(super) fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    pub(super) fn without_cache(mut self) -> Self {
        self.cache_disabled = true;
        self
    }

    pub(super) fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking = level.map(str::to_owned);
        self
    }

    pub(super) fn observe(mut self, progress: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(progress));
        self
    }

    fn request_options(&self, image_resize: Option<ImageResizeProfile>) -> RequestOptions {
        let cache_retention = if self.cache_disabled {
            Some("none")
        } else if std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long") {
            Some("long")
        } else {
            None
        };
        RequestOptions {
            headers: self.headers.clone(),
            thinking: self.thinking.clone(),
            cache_retention,
            progress: self.progress.clone(),
            image_resize,
        }
    }

    #[cfg(test)]
    fn encode(&self, request: ModelRequest) -> Value {
        self.transport.encode(request, &self.request_options(None))
    }
}

fn key_binding(token: &str) -> String {
    format!("key:{:x}", Sha256::digest(token.as_bytes()))
}

#[async_trait::async_trait]
impl Model for PiMessages {
    fn context_identity(&self) -> Option<String> {
        // Radius discovers its route and credentials dynamically.
        if (self.is_radius && self.prepared_limits.is_none())
            || self.radius.is_some()
            || !self.identity_safe_transport
        {
            return None;
        }
        let identity = self.transport.context_identity(
            self.token.as_deref(),
            &self.headers,
            self.thinking.as_deref(),
            self.cache_disabled,
        )?;
        match self.prepared_image_resize {
            Some(profile) => pi_rs::protected_model_fingerprint(&(identity, profile)),
            None => Some(identity),
        }
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        self.transport.estimate_message_tokens(request, message)
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        if let Some((window, _)) = self.prepared_limits {
            return window;
        }
        self.model_details(&[model.into()])
            .await
            .remove(model)?
            .context_window
    }

    async fn stream(&self, mut request: ModelRequest) -> Result<ModelStream> {
        if let Some(progress) = &self.progress {
            progress(Progress::WaitingForModel);
        }
        let mut transport = self.transport.clone();
        let mut image_resize = self.prepared_image_resize;
        if let Some((_, Some(limit))) = self.prepared_limits {
            request.max_output_tokens = request.max_output_tokens.min(limit);
        }
        let (token, binding) = self.access().await?;
        let catalog = match self
            .catalog_for(&binding, self.probe_catalog.then_some(token.as_str()))
            .await
        {
            Ok(catalog) => catalog,
            Err(Error::RetryableModel(_)) => None,
            Err(error) => return Err(error),
        };
        if let Some(catalog) = catalog {
            transport.set_endpoint(catalog.endpoint.clone())?;
            image_resize = self.catalog_image_resize(&catalog, &request.model);
            if let Some(model) = catalog
                .models
                .iter()
                .find(|model| model.id == request.model)
                && let Some(limit) = model.details().max_output_tokens
            {
                request.max_output_tokens = request.max_output_tokens.min(limit);
            }
        }
        transport
            .stream(&token, request, self.request_options(image_resize))
            .await
    }
}

#[cfg(test)]
mod tool_status_tests {
    #[tokio::test]
    async fn auth_header_error_literals_are_not_classified() {
        use std::io::{Read, Write};
        for (name, value) in [("x-api-key", "quota exceeded"), ("x-auth-token", "timeout")] {
            for streaming in [false, true] {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let event = json!({"type":"error","reason":"error","errorMessage":value});
                let worker = std::thread::spawn(move || {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut chunk = [0; 2048];
                    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        let count = socket.read(&mut chunk).unwrap();
                        assert!(
                            count != 0 && request.len() + count <= 32 * 1024,
                            "incomplete request headers"
                        );
                        request.extend_from_slice(&chunk[..count]);
                    }
                    let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
                    assert!(
                        request.contains(&format!("{name}: {value}")),
                        "header missing from fixture request"
                    );
                    let body = if streaming {
                        format!("data: {event}\n\n")
                    } else {
                        json!({"error":{"message":value}}).to_string()
                    };
                    let status = if streaming { "200 OK" } else { "503 Fixture" };
                    let content_type = if streaming {
                        "text/event-stream"
                    } else {
                        "application/json"
                    };
                    write!(socket, "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                });
                let model = PiMessages::new(
                    "fixture",
                    &format!("http://{address}/"),
                    "ordinary-key".into(),
                )
                .unwrap()
                .with_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(name, value.parse().unwrap());
                    headers
                });
                let request = ModelRequest {
                    model: "m".into(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 16,
                };
                let error = match model.stream(request).await {
                    Err(error) => error,
                    Ok(mut stream) => loop {
                        match stream.next().await {
                            Some(Err(error)) => break error,
                            Some(Ok(_)) => {}
                            None => panic!("missing stream error"),
                        }
                    },
                };
                assert_eq!(
                    matches!(error, Error::RetryableModel(_)),
                    !streaming,
                    "{name} {streaming}"
                );
                assert!(!error.to_string().contains(value));
                worker.join().unwrap();
            }
        }
    }

    #[tokio::test]
    async fn quota_stream_error_ignores_metadata_and_redacts() {
        use std::io::{Read, Write};
        for (message, permanent, retryable) in [
            ("quota exceeded sk-billing-timeout-secret", true, false),
            ("timeout sk-billing-timeout-secret", false, true),
            ("sk-billing-timeout-secret", false, false),
            ("sk-timeout-secret", false, false),
            ("service-is-unavailable", false, true),
            ("sk-header-billing-timeout-secret", false, false),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let event = json!({"type":"error","reason":"error","errorMessage":message,"request_id":"billing-sentinel-secret"});
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let body = format!("data: {event}\n\n");
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = PiMessages::new(
                "fixture",
                &format!("http://{address}/"),
                "sk-billing-timeout-secret".into(),
            )
            .unwrap()
            .with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-routing", "timeout".parse().unwrap());
                headers.insert("x-secret", "sk-timeout-secret".parse().unwrap());
                headers.insert(
                    "x-bearer",
                    "Bearer  sk-header-billing-timeout-secret".parse().unwrap(),
                );
                headers.insert("x-service", "service-is-unavailable".parse().unwrap());
                headers
            });
            let request = ModelRequest {
                model: "m".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 16,
            };
            let mut stream = match model.stream(request).await {
                Ok(stream) => stream,
                Err(error) => panic!("{error}"),
            };
            let error = loop {
                match stream.next().await {
                    Some(Err(error)) => break error,
                    Some(Ok(_)) => {}
                    None => panic!("stream ended without an error"),
                }
            };
            assert_eq!(
                error.to_string().contains("quota or billing limit"),
                permanent
            );
            assert_eq!(matches!(error, Error::RetryableModel(_)), retryable);
            assert!(!error.to_string().contains("sentinel-secret"));
            assert!(!error.to_string().contains("sk-billing-timeout-secret"));
            assert!(!error.to_string().contains("sk-timeout-secret"));
            assert!(
                !error
                    .to_string()
                    .contains("sk-header-billing-timeout-secret")
            );
            worker.join().unwrap();
        }
    }

    use super::*;
    use pi_rs::{ProviderReplay, ResolvedModelIdentity};

    #[tokio::test]
    async fn quota_429_preserves_transient_retry_and_redacts_body() {
        use std::io::{Read, Write};
        for (status, body, permanent) in [
            (
                503,
                r#"{"error":{"message":"sk-header-billing-timeout-secret"}}"#,
                false,
            ),
            (503, r#"{"error":{"message":"sk-timeout-secret"}}"#, false),
            (
                503,
                r#"{"error":{"message":"sk-billing-timeout-secret"}}"#,
                false,
            ),
            (
                429,
                r#"{"error":{"code":"insufficient_quota","secret":"sentinel-secret"}}"#,
                true,
            ),
            (
                503,
                r#"{"error":{"message":"quota exceeded","secret":"sentinel-secret"}}"#,
                true,
            ),
            (
                503,
                r#"{"error":{"message":"quota exceeded sk-billing-timeout-secret"}}"#,
                true,
            ),
            (
                429,
                r#"{"error":{"message":"rate limit exceeded"},"request_id":"billing-sentinel-secret"}"#,
                false,
            ),
            (
                503,
                r#"{"error":{"message":"temporarily unavailable"},"request_id":"billing-sentinel-secret"}"#,
                false,
            ),
            (
                429,
                r#"{"error":{"message":"rate limit","request_id":"billing-sentinel-secret"}}"#,
                false,
            ),
            (
                429,
                r#"{"error":{"message":"rate limit sk-billing-timeout-secret"}}"#,
                false,
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let credential = if body.contains("sk-timeout-secret") {
                "sk-timeout-secret"
            } else {
                "sk-billing-timeout-secret"
            };
            let model =
                PiMessages::new("fixture", &format!("http://{address}/"), credential.into())
                    .unwrap();
            let model = model.with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "x-bearer",
                    "Bearer  sk-header-billing-timeout-secret".parse().unwrap(),
                );
                headers.insert("x-service", "service-is-unavailable".parse().unwrap());
                headers
            });
            let result = model
                .stream(ModelRequest {
                    model: "m".into(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 16,
                })
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("expected error"),
            };
            assert_eq!(matches!(error, Error::Model(_)), permanent);
            assert_eq!(matches!(error, Error::RetryableModel(_)), !permanent);
            if permanent {
                assert!(error.to_string().contains("quota or billing limit"));
            }
            assert!(!error.to_string().contains("sentinel-secret"));
            assert!(
                !error
                    .to_string()
                    .contains("sk-header-billing-timeout-secret")
            );
            assert!(!error.to_string().contains(credential));
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn radius_estimation_matches_replay_origin_after_route_change() {
        let client = PiMessages::radius(
            "https://gateway.example/v1",
            Some("token".into()),
            Gateway::configured().unwrap(),
        )
        .unwrap();
        let radius = client.radius.as_ref().unwrap();
        let cache = &radius.catalog.0;
        let update = |endpoint: &str| CachedCatalog {
            gateway: radius.gateway.url("/"),
            binding: key_binding("token"),
            token_hash: Some(Sha256::digest(b"token").into()),
            value: Arc::new(radius_catalog::Catalog {
                endpoint: Url::parse(endpoint).unwrap(),
                base: Url::parse(endpoint).unwrap(),
                models: vec![],
                classifiers: vec![],
                images: vec![],
            }),
        };
        *cache.lock().await = Some(update("https://one.example/v1"));
        let first = client.prepare_context("fixture").await.unwrap();
        let first_origin = format!("{:x}", Sha256::digest("https://one.example/v1"));
        *cache.lock().await = Some(update("https://two.example/v1"));
        let second = client.prepare_context("fixture").await.unwrap();
        let replay = Message::Assistant {
            content: "ok".into(),
            calls: vec![],
            replay: Some(Box::new(ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: "radius".into(),
                    model: "fixture".into(),
                },
                blocks: vec![
                    json!({"type":"pi-rs:pi-messages-origin","endpoint":first_origin}),
                    json!({"type":"thinking","thinking":"x".repeat(8000)}),
                ],
            })),
        };
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![replay.clone()],
            tools: vec![],
            max_output_tokens: 10,
        };
        assert!(first.estimate_message_tokens(&request, &replay) > 1900);
        assert_eq!(
            second.estimate_message_tokens(&request, &replay),
            pi_rs::estimate_message_tokens(&replay)
        );
        assert_eq!(
            first.encode(request.clone())["context"]["messages"][0]["content"][0]["type"],
            "thinking"
        );
        assert_eq!(
            second.encode(request)["context"]["messages"][0]["content"][0]["type"],
            "text"
        );
        let thinking_only = Message::Assistant {
            content: String::new(),
            calls: vec![],
            replay: Some(Box::new(ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: "radius".into(),
                    model: "fixture".into(),
                },
                blocks: vec![
                    json!({"type":"pi-rs:pi-messages-origin","endpoint":first_origin}),
                    json!({"type":"thinking","thinking":"private"}),
                    json!({"_pi_context_edit_api":"pi-messages"}),
                ],
            })),
        };
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![thinking_only],
            tools: vec![],
            max_output_tokens: 10,
        };
        assert_eq!(
            first.encode(request.clone())["context"]["messages"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(
            second.encode(request)["context"]["messages"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        #[cfg(any(unix, windows))]
        assert_ne!(first.context_identity(), second.context_identity());
    }

    #[test]
    fn structured_tool_content_is_still_extracted_by_pi_messages() {
        let text = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        let client = PiMessages::new("fixture", "http://127.0.0.1/", "token".into()).unwrap();
        let body = client.encode(ModelRequest {
            model: "fixture".into(),
            messages: vec![Message::Tool {
                call_id: "call".into(),
                content: text.into(),
                is_error: false,
                is_structured: Some(true),
            }],
            tools: vec![],
            max_output_tokens: 10,
        });
        let blocks = &body["context"]["messages"][0]["content"];
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["data"], "aW1hZ2U=");
        assert!(!blocks[0]["text"].as_str().unwrap().contains("aW1hZ2U="));
        let legacy =
            serde_json::from_value(json!({"role":"tool","call_id":"call","content":text})).unwrap();
        let body = client.encode(ModelRequest {
            model: "fixture".into(),
            messages: vec![legacy],
            tools: vec![],
            max_output_tokens: 10,
        });
        assert_eq!(
            body["context"]["messages"][0]["content"][1]["data"],
            "aW1hZ2U="
        );
    }

    #[test]
    fn imported_tool_status_survives_journal_and_pi_messages_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pi.jsonl");
        let plain = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        let entries = [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"one","name":"read","arguments":{}},{"type":"toolCall","id":"two","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"t1","parentId":"a","message":{"role":"toolResult","toolCallId":"one","content":[{"type":"text","text":plain}],"isError":true,"details":{"private":"metadata"}}}),
            json!({"type":"message","id":"t2","parentId":"t1","message":{"role":"toolResult","toolCallId":"two","content":[{"type":"text","text":plain}],"isError":false}}),
        ];
        std::fs::write(
            &path,
            entries
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let session = super::super::session_import::read(&path).unwrap();
        let saved = dir.path().join("native.jsonl");
        session.export_jsonl(&saved).unwrap();
        let reopened = super::super::session_import::read(&saved).unwrap();
        assert!(reopened.records().iter().any(|record| matches!(&record.event, pi_rs::Event::ToolFinished { outcome: pi_rs::Outcome::TextResult { output, details: Some(details), is_error: true }, .. } if output == plain && details["private"] == "metadata")));
        let messages = reopened.current_messages();
        assert!(messages.iter().any(|message| matches!(message, Message::Tool { content, is_error: true, .. } if content == plain)));
        let client = PiMessages::new("fixture", "http://127.0.0.1/", "token".into()).unwrap();
        let body = client.encode(ModelRequest {
            model: "fixture".into(),
            messages,
            tools: vec![],
            max_output_tokens: 256,
        });
        let results = body["context"]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|value| value["role"] == "toolResult")
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["isError"], true);
        assert_eq!(results[0]["content"][0]["text"], plain);
        assert_eq!(results[1]["isError"], false);
        assert_eq!(results[1]["content"][0]["text"], plain);
        assert!(!body.to_string().contains("metadata"));
    }
}
