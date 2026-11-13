//! Persistent, generation-bound credentials for native MCP OAuth sessions.
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pi_rs::{Error, Result};
use rmcp::transport::auth::{
    AuthClient, AuthError, AuthorizationManager, AuthorizationMetadata, CredentialRefreshGuard,
    CredentialStore, StoredCredentials,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

// Match rmcp's address policy, but never exempt an OAuth destination that is
// also a proxy host. A resolver cannot distinguish a proxy lookup from a
// NO_PROXY direct destination lookup with the same name.
#[derive(Clone)]
struct OAuthResolver {
    base_ip: Option<std::net::IpAddr>,
    base_class: Option<u8>,
    proxy_hosts: Vec<String>,
}
impl OAuthResolver {
    fn class(ip: std::net::IpAddr) -> u8 {
        use std::net::{IpAddr, Ipv4Addr};
        match ip {
            IpAddr::V4(v) => {
                let o = v.octets();
                if v.is_loopback() {
                    1
                } else if v.is_private() || (o[0] == 100 && (64..=127).contains(&o[1])) {
                    2
                } else if v.is_link_local()
                    || v.is_broadcast()
                    || v.is_unspecified()
                    || v.is_multicast()
                    || o[0] == 0
                    || (o[0] == 198 && matches!(o[1], 18 | 19))
                {
                    3
                } else {
                    0
                }
            }
            IpAddr::V6(v) => {
                let [a, b, c, d, e, f, g, h] = v.segments();
                let low = Ipv4Addr::from((u32::from(g) << 16) | u32::from(h));
                let embedded = match [a, b, c, d, e, f] {
                    [0, 0, 0, 0, 0, 0xffff] | [0x64, 0xff9b, 0, 0, 0, 0] => Some(low),
                    [0, 0, 0, 0, 0, 0] if !v.is_loopback() && !v.is_unspecified() => Some(low),
                    [0x2002, ..] => Some(Ipv4Addr::from((u32::from(b) << 16) | u32::from(c))),
                    _ => None,
                };
                if let Some(v) = embedded {
                    return Self::class(v.into());
                }
                if v.is_loopback() {
                    1
                } else if a & 0xfe00 == 0xfc00 {
                    2
                } else if v.is_unspecified()
                    || v.is_multicast()
                    || a & 0xffc0 == 0xfe80
                    || [a, b] == [0x2001, 0]
                    || [a, b, c] == [0x64, 0xff9b, 1]
                {
                    3
                } else {
                    0
                }
            }
        }
    }
    fn allowed(&self, ip: std::net::IpAddr) -> bool {
        let class = Self::class(ip);
        class == 0 || (class != 3 && self.base_class == Some(class))
    }
    fn allows_url(&self, url: &reqwest_mcp::Url) -> bool {
        let Some(host) = url.host_str() else {
            return false;
        };
        if let Ok(ip) = host.trim_matches(['[', ']']).parse() {
            return self.base_ip == Some(ip) || self.allowed(ip);
        }
        !self.proxy_hosts.iter().any(|proxy| {
            proxy
                .trim_end_matches('.')
                .eq_ignore_ascii_case(host.trim_end_matches('.'))
        })
    }
}
impl reqwest_mcp::dns::Resolve for OAuthResolver {
    fn resolve(&self, name: reqwest_mcp::dns::Name) -> reqwest_mcp::dns::Resolving {
        let this = self.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            use std::net::ToSocketAddrs;
            let lookup = host.clone();
            let addresses =
                tokio::task::spawn_blocking(move || (lookup.as_str(), 0).to_socket_addrs())
                    .await??;
            let exempt = this.proxy_hosts.iter().any(|proxy| {
                proxy
                    .trim_end_matches('.')
                    .eq_ignore_ascii_case(host.trim_end_matches('.'))
            });
            let allowed: Vec<_> = addresses
                .filter(|addr| exempt || this.allowed(addr.ip()))
                .collect();
            if allowed.is_empty() {
                return Err("MCP OAuth DNS address rejected".into());
            }
            Ok(Box::new(allowed.into_iter()) as reqwest_mcp::dns::Addrs)
        })
    }
}

// OAuth uses separate clients so token requests never follow redirects.
struct OAuthHttp {
    follow: reqwest_mcp::Client,
    stop: reqwest_mcp::Client,
    resolver: Arc<OAuthResolver>,
}

impl rmcp::transport::auth::OAuthHttpClient for OAuthHttp {
    fn execute(
        &self,
        operation: rmcp::transport::auth::OAuthHttpRequest,
    ) -> rmcp::transport::auth::OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            let client = match operation.redirect_policy {
                rmcp::transport::auth::OAuthHttpRedirectPolicy::Follow => &self.follow,
                _ => &self.stop,
            };
            let request = reqwest_mcp::Request::try_from(operation.request)?;
            if !self.resolver.allows_url(request.url()) {
                return Err("MCP OAuth address rejected".into());
            }
            let mut response = client.execute(request).await?;
            let mut builder = http::Response::builder()
                .status(response.status())
                .version(response.version());
            *builder.headers_mut().expect("response builder") = response.headers().clone();
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if chunk.len() > 1024 * 1024 - body.len() {
                    return Err("MCP OAuth response exceeds 1 MiB".into());
                }
                body.extend_from_slice(&chunk);
            }
            Ok(builder.body(body)?)
        })
    }
}

fn oauth_http(
    url: &str,
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
) -> Result<OAuthHttp> {
    let base =
        reqwest_mcp::Url::parse(url).map_err(|_| Error::Config("invalid MCP OAuth URL".into()))?;
    let base_ip = base
        .host_str()
        .and_then(|host| host.trim_matches(['[', ']']).parse().ok());
    let base_class = base_ip.map(OAuthResolver::class).or_else(|| {
        base.host_str()
            .filter(|host| *host == "localhost" || host.ends_with(".localhost"))
            .map(|_| 1)
    });
    let resolver = Arc::new(OAuthResolver {
        base_ip,
        base_class,
        proxy_hosts: options.map(|o| o.proxy_hosts.clone()).unwrap_or_else(|| {
            [
                "http_proxy",
                "HTTP_PROXY",
                "https_proxy",
                "HTTPS_PROXY",
                "all_proxy",
                "ALL_PROXY",
            ]
            .into_iter()
            .filter_map(|name| std::env::var(name).ok())
            .filter_map(|value| {
                let value = if value.contains("://") {
                    value
                } else {
                    format!("http://{value}")
                };
                reqwest_mcp::Url::parse(&value).ok()
            })
            .filter_map(|url| url.host_str().map(str::to_owned))
            .collect()
        }),
    });
    let build = |redirect| {
        let mut builder = reqwest_mcp::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .dns_resolver(resolver.clone())
            .redirect(redirect);
        if let Some(options) = options {
            builder = builder.no_proxy();
            for proxy in &options.proxies {
                builder = builder.proxy(proxy.clone());
            }
            if let Some(timeout) = options.idle_timeout {
                builder = builder.read_timeout(timeout);
            }
        }
        builder
            .build()
            .map_err(|_| Error::Config("could not build MCP OAuth HTTP client".into()))
    };
    Ok(OAuthHttp {
        follow: build(reqwest_mcp::redirect::Policy::custom({
            let resolver = resolver.clone();
            move |attempt| {
                if !resolver.allows_url(attempt.url()) || attempt.previous().len() > 10 {
                    attempt.error(std::io::Error::other("MCP OAuth redirect rejected"))
                } else {
                    attempt.follow()
                }
            }
        }))?,
        stop: build(reqwest_mcp::redirect::Policy::none())?,
        resolver,
    })
}

async fn oauth_manager(
    url: &str,
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
) -> Result<AuthorizationManager> {
    AuthorizationManager::new_with_oauth_http_client(url, Arc::new(oauth_http(url, options)?))
        .await
        .map_err(auth_error)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum OAuthSetting {
    Enabled(bool),
    Client(OAuthClientSettings),
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct OAuthClientSettings {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub client_name: Option<String>,
    pub callback_port: Option<u16>,
    pub callback_url: Option<String>,
    pub scope: Option<String>,
    pub client_metadata_url: Option<String>,
    pub auth_server_metadata_url: Option<String>,
    pub client_registration: ClientRegistrationMode,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClientRegistrationMode {
    #[default]
    Auto,
    Dcr,
    Cimd,
}

#[derive(Clone)]
struct FileCredentials {
    path: PathBuf,
    generation: Option<Uuid>,
    new_login: bool,
    upgrade: Option<Arc<UpgradeGuard>>,
    registration: Arc<Mutex<Option<ClientRegistration>>>,
    // Blocking writes retain this lock even if their async caller is cancelled.
    lease: Arc<Mutex<Weak<std::fs::File>>>,
}
#[derive(Clone, Serialize, Deserialize)]
struct ClientRegistration {
    client_secret: Option<String>,
    redirect_uri: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    requested_scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_server_metadata_url: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Envelope {
    generation: Uuid,
    credentials: StoredCredentials,
    #[serde(default)]
    registration: Option<ClientRegistration>,
}

struct UpgradeGuard {
    generation: Uuid,
    scopes: Vec<String>,
}

struct Upgrade {
    previous: Envelope,
    scopes: Vec<String>,
}

#[derive(Clone, Copy, Default)]
struct ClientOptions<'a> {
    client_id: Option<&'a str>,
    client_secret: Option<&'a str>,
    client_name: Option<&'a str>,
    metadata_url: Option<&'a str>,
    challenge: Option<&'a str>,
    callback_port: Option<u16>,
    callback_url: Option<&'a str>,
    auth_server_metadata_url: Option<&'a str>,
    registration: ClientRegistrationMode,
}

enum Registration<'a> {
    New(ClientOptions<'a>),
    Upgrade(Box<Upgrade>),
}
impl FileCredentials {
    fn new(name: &str, url: &str) -> Result<Self> {
        let url = reqwest_mcp::Url::parse(url)
            .map_err(|_| Error::Config("invalid MCP OAuth resource URL".into()))?;
        let digest = Sha256::digest(serde_json::to_vec(&(name, url.as_str()))?);
        Ok(Self {
            path: crate::agent_dir()
                .join("mcp-auth")
                .join(format!("{digest:x}.json")),
            generation: None,
            new_login: false,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        })
    }
    fn read(&self) -> std::result::Result<Option<Envelope>, AuthError> {
        let file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(store_error()),
        };
        let mut bytes = Vec::new();
        file.take(128 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| store_error())?;
        if bytes.len() > 128 * 1024 {
            return Err(store_error());
        }
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| store_error())?;
        if !self.new_login
            && self
                .generation
                .is_some_and(|generation| generation != envelope.generation)
        {
            return Err(AuthError::AuthorizationRequired);
        }
        Ok(Some(envelope))
    }
    fn write(&self, mut credentials: StoredCredentials) -> std::result::Result<(), AuthError> {
        let current = if self.new_login && self.upgrade.is_none() {
            None
        } else {
            self.read()?
        };
        if self.upgrade.as_ref().is_some_and(|upgrade| {
            current.as_ref().map(|envelope| envelope.generation) != Some(upgrade.generation)
        }) {
            return Err(AuthError::AuthorizationRequired);
        }
        if !self.new_login && self.generation.is_some() && current.is_none() {
            return Err(AuthError::AuthorizationRequired);
        }
        let registration = self
            .registration
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .or_else(|| {
                current
                    .as_ref()
                    .and_then(|envelope| envelope.registration.clone())
            });
        if self.new_login
            && credentials.granted_scopes.is_empty()
            && let Some(registration) = &registration
        {
            // OAuth omits scope when it grants exactly the requested set.
            credentials.granted_scopes = registration.requested_scopes.clone();
        }
        if self.upgrade.as_ref().is_some_and(|upgrade| {
            upgrade
                .scopes
                .iter()
                .any(|scope| !credentials.granted_scopes.contains(scope))
        }) {
            return Err(AuthError::AuthorizationRequired);
        }
        let generation = if self.new_login {
            self.generation.unwrap_or_else(Uuid::new_v4)
        } else {
            current
                .map(|envelope| envelope.generation)
                .unwrap_or_else(Uuid::new_v4)
        };
        let parent = self.path.parent().ok_or_else(store_error)?;
        std::fs::create_dir_all(parent).map_err(|_| store_error())?;
        let mut file = tempfile::NamedTempFile::new_in(parent).map_err(|_| store_error())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|_| store_error())?;
        }
        let bytes = serde_json::to_vec(&Envelope {
            generation,
            credentials,
            registration,
        })
        .map_err(|_| store_error())?;
        if bytes.len() >= 128 * 1024 {
            return Err(store_error());
        }
        file.write_all(&bytes).map_err(|_| store_error())?;
        file.write_all(b"\n").map_err(|_| store_error())?;
        file.as_file().sync_all().map_err(|_| store_error())?;
        file.persist(&self.path).map_err(|_| store_error())?;
        #[cfg(unix)]
        std::fs::File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|_| store_error())?;
        Ok(())
    }
}
fn store_error() -> AuthError {
    AuthError::CredentialStoreError("could not access the private MCP credential store".into())
}
fn auth_error(error: AuthError) -> Error {
    let message = match error {
        AuthError::AuthorizationRequired
        | AuthError::TokenExpired
        | AuthError::TokenRefreshRejected(_) => {
            return Error::AuthorizationRequired {
                message: "MCP OAuth login required; run pi mcp login for this server".into(),
                challenge: None,
            };
        }
        AuthError::RegistrationFailed(_) => {
            "MCP OAuth client registration failed; use --client-id for a pre-registered client"
        }
        AuthError::PkceUnsupported => "MCP authorization server does not support PKCE S256",
        AuthError::NoAuthorizationSupport => "MCP server does not advertise OAuth authorization",
        AuthError::CredentialStoreError(_) => "could not access the private MCP credential store",
        _ => "MCP OAuth operation failed",
    };
    Error::Config(message.into())
}

#[async_trait::async_trait]
impl CredentialStore for FileCredentials {
    async fn load(&self) -> std::result::Result<Option<StoredCredentials>, AuthError> {
        if self.new_login {
            return Ok(None);
        }
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store
                .read()
                .map(|envelope| envelope.map(|value| value.credentials))
        })
        .await
        .map_err(|_| store_error())?
    }
    async fn save(&self, credentials: StoredCredentials) -> std::result::Result<(), AuthError> {
        let store = self.clone();
        let lease = self
            .lease
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            store.write(credentials)
        })
        .await
        .map_err(|_| store_error())?
    }
    async fn clear(&self) -> std::result::Result<(), AuthError> {
        let store = self.clone();
        let lease = self
            .lease
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            match std::fs::remove_file(store.path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(_) => Err(store_error()),
            }
        })
        .await
        .map_err(|_| store_error())?
    }
    async fn acquire_refresh_guard(
        &self,
    ) -> std::result::Result<Option<CredentialRefreshGuard>, AuthError> {
        let path = self.path.with_extension("lock");
        std::fs::create_dir_all(path.parent().ok_or_else(store_error)?)
            .map_err(|_| store_error())?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| store_error())?;
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await
                }
                Err(std::fs::TryLockError::Error(_)) => return Err(store_error()),
            }
        }
        let file = Arc::new(file);
        *self.lease.lock().unwrap_or_else(|error| error.into_inner()) = Arc::downgrade(&file);
        let guard = crate::mcp_refresh::guard(file).await?;
        Ok(Some(CredentialRefreshGuard::new(guard)))
    }
}

/// Restore a previously authorized client. Each connection is bound to one login generation.
pub async fn client(
    name: &str,
    url: &str,
    client: reqwest_mcp::Client,
) -> Result<(crate::mcp_oauth_client::SafeAuthClient, String)> {
    client_with_http_options(name, url, client, None).await
}

pub async fn client_with_http_options(
    name: &str,
    url: &str,
    client: reqwest_mcp::Client,
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
) -> Result<(crate::mcp_oauth_client::SafeAuthClient, String)> {
    let mut store = FileCredentials::new(name, url)?;
    let guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
    let envelope = store
        .read()
        .map_err(auth_error)?
        .ok_or_else(|| auth_error(AuthError::AuthorizationRequired))?;
    store.generation = Some(envelope.generation);
    let identity = envelope.generation.to_string();
    let mut manager = oauth_manager(url, options).await?;
    manager.set_credential_store(store);
    if let Some(metadata_url) = envelope
        .registration
        .as_ref()
        .and_then(|registration| registration.auth_server_metadata_url.as_deref())
    {
        manager.set_metadata(configured_metadata(metadata_url, url, options).await?);
    }
    if !manager.initialize_from_store().await.map_err(auth_error)? {
        return Err(auth_error(AuthError::AuthorizationRequired));
    }
    if let Some(registration) = envelope.registration {
        let mut config = rmcp::transport::auth::OAuthClientConfig::new(
            envelope.credentials.client_id,
            registration.redirect_uri,
        )
        .with_scopes(envelope.credentials.granted_scopes);
        if let Some(secret) = registration.client_secret {
            config = config.with_client_secret(secret);
        }
        manager.configure_client(config).map_err(auth_error)?;
    }
    drop(guard);
    Ok((
        crate::mcp_oauth_client::SafeAuthClient(AuthClient::new(client, manager)),
        identity,
    ))
}

pub(crate) fn has_credentials(name: &str, url: &str) -> Result<bool> {
    Ok(FileCredentials::new(name, url)?
        .read()
        .map_err(auth_error)?
        .is_some())
}

pub async fn logout(name: &str, url: &str) -> Result<()> {
    let store = FileCredentials::new(name, url)?;
    let _guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
    store.clear().await.map_err(auth_error)
}

/// Start an authorization-code flow with server-provided client registration.
pub async fn login(
    name: &str,
    url: &str,
    client_id: Option<&str>,
    scopes: &[String],
    announce: impl Fn(&str),
) -> Result<()> {
    login_with_secret(name, url, client_id, None, scopes, announce).await
}

/// Authorize a public or confidential client with an optional client secret.
pub async fn login_with_secret(
    name: &str,
    url: &str,
    client_id: Option<&str>,
    client_secret: Option<&str>,
    scopes: &[String],
    announce: impl Fn(&str),
) -> Result<()> {
    login_with_secret_with_http_options(name, url, client_id, client_secret, scopes, None, announce)
        .await
}

pub async fn login_with_secret_with_http_options(
    name: &str,
    url: &str,
    client_id: Option<&str>,
    client_secret: Option<&str>,
    scopes: &[String],
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
    announce: impl Fn(&str),
) -> Result<()> {
    authorize(
        name,
        url,
        Registration::New(ClientOptions {
            client_id,
            client_secret,
            ..ClientOptions::default()
        }),
        scopes,
        options,
        announce,
    )
    .await
    .map(|_| ())
}

/// Use a hosted client metadata document, falling back to dynamic registration.
pub async fn login_with_metadata(
    name: &str,
    url: &str,
    client_metadata_url: &str,
    scopes: &[String],
    announce: impl Fn(&str),
) -> Result<()> {
    login_with_metadata_with_http_options(name, url, client_metadata_url, scopes, None, announce)
        .await
}

pub async fn login_with_metadata_with_http_options(
    name: &str,
    url: &str,
    client_metadata_url: &str,
    scopes: &[String],
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
    announce: impl Fn(&str),
) -> Result<()> {
    authorize(
        name,
        url,
        Registration::New(ClientOptions {
            metadata_url: Some(client_metadata_url),
            ..ClientOptions::default()
        }),
        scopes,
        options,
        announce,
    )
    .await
    .map(|_| ())
}

pub async fn login_configured(
    name: &str,
    url: &str,
    settings: Option<&OAuthClientSettings>,
    challenge: Option<&str>,
    scopes: &[String],
    announce: impl Fn(&str),
) -> Result<()> {
    login_configured_with_http_options(name, url, settings, challenge, scopes, None, announce).await
}

pub async fn login_configured_with_http_options(
    name: &str,
    url: &str,
    settings: Option<&OAuthClientSettings>,
    challenge: Option<&str>,
    scopes: &[String],
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
    announce: impl Fn(&str),
) -> Result<()> {
    let default = OAuthClientSettings::default();
    let settings = settings.unwrap_or(&default);
    let client_secret = if let Some(secret) = &settings.client_secret {
        Some(crate::mcp::resolve_value(secret).await?)
    } else {
        None
    };
    let configured_scopes: Vec<_> = settings
        .scope
        .as_deref()
        .unwrap_or_default()
        .split_ascii_whitespace()
        .map(str::to_owned)
        .collect();
    let scopes = if scopes.is_empty() {
        &configured_scopes
    } else {
        scopes
    };
    authorize(
        name,
        url,
        Registration::New(ClientOptions {
            client_id: settings.client_id.as_deref(),
            client_secret: client_secret.as_deref(),
            client_name: settings.client_name.as_deref(),
            metadata_url: settings.client_metadata_url.as_deref(),
            challenge,
            callback_port: settings.callback_port,
            callback_url: settings.callback_url.as_deref(),
            auth_server_metadata_url: settings.auth_server_metadata_url.as_deref(),
            registration: settings.client_registration,
        }),
        scopes,
        options,
        announce,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn upgrade(
    name: &str,
    url: &str,
    previous: Uuid,
    requested: &[String],
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
    announce: impl Fn(&str),
) -> Result<Uuid> {
    let store = FileCredentials::new(name, url)?;
    let guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
    let envelope = store
        .read()
        .map_err(auth_error)?
        .filter(|envelope| envelope.generation == previous)
        .ok_or_else(|| auth_error(AuthError::AuthorizationRequired))?;
    drop(guard);
    let mut scopes = envelope.credentials.granted_scopes.clone();
    for scope in requested {
        if !scopes.contains(scope) {
            scopes.push(scope.clone());
        }
    }
    if scopes.len() == envelope.credentials.granted_scopes.len() {
        return Err(Error::Config(
            "MCP server rejected credentials that already have the requested scopes".into(),
        ));
    }
    let upgrade = Upgrade {
        previous: envelope,
        scopes: scopes.clone(),
    };
    authorize(
        name,
        url,
        Registration::Upgrade(Box::new(upgrade)),
        &scopes,
        options,
        announce,
    )
    .await
}

fn safe_http_url(value: &str) -> Result<reqwest_mcp::Url> {
    let url = reqwest_mcp::Url::parse(value)
        .map_err(|_| Error::Config("invalid MCP OAuth URL".into()))?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Config(
            "MCP OAuth requires HTTPS or loopback HTTP without embedded credentials or fragments"
                .into(),
        ));
    }
    Ok(url)
}

fn client_metadata_base() -> Result<Option<reqwest_mcp::Url>> {
    let path = crate::agent_dir().join("settings.json");
    let settings: serde_json::Value = match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|_| Error::Config("invalid MCP client metadata settings".into()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let Some(value) = settings.get("mcpClientMetadataBaseUrl") else {
        return Ok(None);
    };
    let invalid =
        || Error::Config("mcpClientMetadataBaseUrl requires an HTTPS directory URL".into());
    let base = value.as_str().ok_or_else(invalid).and_then(safe_http_url)?;
    if base.scheme() != "https" || base.query().is_some() {
        return Err(invalid());
    }
    Ok(Some(base))
}

fn advertised(metadata: &AuthorizationMetadata, key: &str) -> bool {
    metadata
        .additional_fields
        .get(key)
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

fn supports_public_metadata(metadata: &AuthorizationMetadata) -> bool {
    advertised(metadata, "client_id_metadata_document_supported")
        && metadata
            .additional_fields
            .get("token_endpoint_auth_methods_supported")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|methods| methods.iter().any(|method| method == "none"))
}

fn metadata_callback(callback: &reqwest_mcp::Url) -> bool {
    callback.path() == "/callback" && matches!(callback.host_str(), Some("localhost" | "127.0.0.1"))
}

fn metadata_document(
    mut base: reqwest_mcp::Url,
    resource: &str,
    callback: &mut reqwest_mcp::Url,
    metadata: &AuthorizationMetadata,
) -> Result<reqwest_mcp::Url> {
    {
        let mut path = base
            .path_segments_mut()
            .map_err(|()| Error::Config("invalid MCP client metadata directory".into()))?;
        path.pop_if_empty();
        if metadata.issuer.is_none()
            || !advertised(metadata, "authorization_response_iss_parameter_supported")
        {
            let resource = safe_http_url(resource)?;
            let hash = Sha256::digest(resource.as_str().as_bytes());
            let id = URL_SAFE_NO_PAD.encode(&hash[..9]);
            path.push(&id);
            callback.set_path(&format!("/callback/{id}"));
        }
        path.push("client.json");
    }
    Ok(base)
}

async fn callback_listener(
    url: Option<&str>,
    port: Option<u16>,
) -> Result<(tokio::net::TcpListener, reqwest_mcp::Url)> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let mut callback = safe_http_url(url.unwrap_or("http://127.0.0.1/callback"))?;
    let address = match callback.host_str() {
        Some("127.0.0.1" | "localhost") => IpAddr::V4(Ipv4Addr::LOCALHOST),
        Some("[::1]") => IpAddr::V6(Ipv6Addr::LOCALHOST),
        _ => {
            return Err(Error::Config(
                "MCP callbackUrl requires a loopback host".into(),
            ));
        }
    };
    if callback.scheme() != "http"
        || callback.query().is_some()
        || callback.path().starts_with("//")
        || port == Some(0)
    {
        return Err(Error::Config(
            "invalid MCP callbackUrl or callbackPort".into(),
        ));
    }
    if let (Some(url_port), Some(port)) = (callback.port(), port)
        && url_port != port
    {
        return Err(Error::Config(
            "MCP callbackUrl and callbackPort disagree".into(),
        ));
    }
    let listener =
        tokio::net::TcpListener::bind((address, callback.port().or(port).unwrap_or_default()))
            .await?;
    callback
        .set_port(Some(listener.local_addr()?.port()))
        .map_err(|()| Error::Config("invalid MCP callback port".into()))?;
    Ok((listener, callback))
}

async fn configured_metadata(
    value: &str,
    resource: &str,
    options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
) -> Result<AuthorizationMetadata> {
    let http = oauth_http(resource, options)?;
    let target = safe_http_url(value)?;
    if !http.resolver.allows_url(&target) {
        return Err(Error::Config("MCP OAuth metadata address rejected".into()));
    }
    configured_metadata_with_client(value, &http.stop).await
}

async fn configured_metadata_with_client(
    value: &str,
    client: &reqwest_mcp::Client,
) -> Result<rmcp::transport::auth::AuthorizationMetadata> {
    const LIMIT: usize = 128 * 1024;
    let url = safe_http_url(value)?;
    let failure = |_| Error::Config("could not read configured MCP authorization metadata".into());
    let mut response = client.get(url).send().await.map_err(failure)?;
    if !response.status().is_success() {
        return Err(Error::Config(
            "configured MCP authorization metadata was rejected".into(),
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(failure)? {
        if bytes.len() + chunk.len() > LIMIT {
            return Err(Error::Config(
                "MCP authorization metadata exceeds 128 KiB".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let metadata: rmcp::transport::auth::AuthorizationMetadata = serde_json::from_slice(&bytes)
        .map_err(|_| Error::Config("invalid configured MCP authorization metadata".into()))?;
    let issuer = metadata.issuer.as_deref().ok_or_else(|| {
        Error::Config("configured MCP authorization metadata requires an issuer".into())
    })?;
    for endpoint in [
        Some(issuer),
        Some(metadata.authorization_endpoint.as_str()),
        Some(metadata.token_endpoint.as_str()),
        metadata.registration_endpoint.as_deref(),
        metadata.jwks_uri.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        safe_http_url(endpoint)?;
    }
    Ok(metadata)
}

async fn authorize(
    name: &str,
    url: &str,
    registration: Registration<'_>,
    scopes: &[String],
    http_options: Option<&crate::mcp::HttpOptionsWithProxyHosts>,
    announce: impl Fn(&str),
) -> Result<Uuid> {
    use rmcp::transport::auth::{AuthorizationRequest, AuthorizationSession};
    use std::time::Duration;

    safe_http_url(url)?;
    let (options, upgrade) = match &registration {
        Registration::New(options) => (*options, None),
        Registration::Upgrade(upgrade) => {
            let client = upgrade.previous.registration.as_ref().ok_or_else(|| {
                Error::Config("MCP registration is missing; sign in again before upgrading".into())
            })?;
            let redirect = reqwest_mcp::Url::parse(&client.redirect_uri)
                .map_err(|_| Error::Config("invalid saved MCP callback URL".into()))?;
            (
                ClientOptions {
                    client_id: Some(upgrade.previous.credentials.client_id.as_str()),
                    client_secret: client.client_secret.as_deref(),
                    callback_port: redirect.port_or_known_default(),
                    callback_url: Some(&client.redirect_uri),
                    auth_server_metadata_url: client.auth_server_metadata_url.as_deref(),
                    ..ClientOptions::default()
                },
                Some(upgrade),
            )
        }
    };
    if options.client_secret.is_some() && options.client_id.is_none() {
        return Err(Error::Config(
            "OAuth client secret requires a client ID".into(),
        ));
    }
    if options.client_id.is_some_and(|id| id.trim().is_empty())
        || options
            .client_name
            .is_some_and(|name| name.trim().is_empty())
    {
        return Err(Error::Config(
            "OAuth client ID and name must be non-empty".into(),
        ));
    }
    if options.registration == ClientRegistrationMode::Cimd
        && (options.client_id.is_some() || options.client_name.is_some())
    {
        return Err(Error::Config(
            "OAuth cimd registration requires a metadata document without clientId or clientName"
                .into(),
        ));
    }
    let (listener, mut callback) =
        callback_listener(options.callback_url, options.callback_port).await?;
    let explicit_metadata = options
        .metadata_url
        .filter(|_| options.registration != ClientRegistrationMode::Dcr);
    if let Some(url) = explicit_metadata
        && safe_http_url(url)?.scheme() != "https"
    {
        return Err(Error::Config(
            "OAuth clientMetadataUrl requires HTTPS".into(),
        ));
    }
    let mut store = FileCredentials::new(name, url)?;
    store.new_login = true;
    let generation = Uuid::new_v4();
    store.generation = Some(generation);
    if let Some(upgrade) = upgrade {
        store.upgrade = Some(Arc::new(UpgradeGuard {
            generation: upgrade.previous.generation,
            scopes: upgrade.scopes.clone(),
        }));
    }
    let mut manager = oauth_manager(url, http_options).await?;
    manager.set_credential_store(store.clone());
    let metadata = if let Some(metadata_url) = options.auth_server_metadata_url {
        configured_metadata(metadata_url, url, http_options).await?
    } else {
        let metadata = manager
            .resolve_metadata_from_challenge(options.challenge)
            .await
            .map_err(auth_error)?;
        if options.challenge.is_some() && !metadata.source.is_discovered() {
            return Err(auth_error(AuthError::NoAuthorizationSupport));
        }
        metadata.metadata
    };
    if let Some(upgrade) = upgrade
        && (metadata.issuer.is_none() || metadata.issuer != upgrade.previous.credentials.issuer)
    {
        return Err(Error::Config(
            "MCP authorization issuer changed; sign in again before upgrading".into(),
        ));
    }
    let supports_client_metadata = advertised(&metadata, "client_id_metadata_document_supported");
    let generated_metadata = if explicit_metadata.is_none()
        && options.registration != ClientRegistrationMode::Dcr
        && options.client_id.is_none()
        && options.client_name.is_none()
        && supports_public_metadata(&metadata)
        && metadata_callback(&callback)
        && let Some(base) = client_metadata_base()?
    {
        Some(metadata_document(base, url, &mut callback, &metadata)?)
    } else {
        None
    };
    let client_metadata_url =
        explicit_metadata.or_else(|| generated_metadata.as_ref().map(reqwest_mcp::Url::as_str));
    if options.registration == ClientRegistrationMode::Cimd
        && (!supports_client_metadata || client_metadata_url.is_none())
    {
        return Err(Error::Config(
            "OAuth cimd registration requires a supported client metadata document".into(),
        ));
    }
    manager.set_metadata(metadata);
    let client_name = options.client_name.unwrap_or("pi-rs");
    let scopes = if scopes.is_empty() {
        manager.select_initial_scopes(&[]).await
    } else {
        scopes.to_vec()
    };
    let (client_id, secret) = if let Some(client_id) = options.client_id {
        (
            Some(client_id.to_owned()),
            options.client_secret.map(str::to_owned),
        )
    } else if client_metadata_url.is_some() && supports_client_metadata {
        (None, None)
    } else {
        let refs: Vec<_> = scopes.iter().map(String::as_str).collect();
        let registration = manager
            .register_client(client_name, callback.as_str(), &refs)
            .await
            .map_err(auth_error)?;
        (Some(registration.client_id), registration.client_secret)
    };
    *store
        .registration
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(ClientRegistration {
        client_secret: secret.clone(),
        redirect_uri: callback.to_string(),
        requested_scopes: scopes.clone(),
        auth_server_metadata_url: options.auth_server_metadata_url.map(str::to_owned),
    });
    let mut request = AuthorizationRequest::new(callback.as_str())
        .with_client_name(client_name)
        .with_initial_scope_selection()
        .with_scopes(scopes);
    if let Some(client_id) = client_id {
        request = request.with_preregistered_client(client_id);
    } else if let Some(url) = client_metadata_url {
        request = request.with_client_metadata_url(url);
    }
    if let Some(secret) = secret {
        request = request.with_client_secret(secret);
    }
    let session = AuthorizationSession::new(manager, request)
        .await
        .map_err(|(_, error)| auth_error(error))?;
    let authorization = reqwest_mcp::Url::parse(session.get_authorization_url())
        .map_err(|_| Error::Config("invalid MCP authorization URL".into()))?;
    if let Some((_, requested)) = authorization
        .query_pairs()
        .find(|(name, _)| name == "scope")
        && let Some(registration) = store
            .registration
            .lock()
            .expect("MCP client registration")
            .as_mut()
    {
        registration.requested_scopes = requested.split_whitespace().map(str::to_owned).collect();
    }
    let expected_state = authorization
        .query_pairs()
        .find(|(name, _)| name == "state")
        .map(|(_, state)| state.into_owned())
        .ok_or_else(|| Error::Config("OAuth state is missing".into()))?;
    announce(session.get_authorization_url());
    crate::oauth_callback::open_browser(session.get_authorization_url()).await?;
    let receive = async {
        let incoming =
            crate::oauth_callback::receive(&listener, &callback, &expected_state).await?;
        if incoming.code().is_none() {
            incoming.reply(false).await;
            return Err(Error::Config("MCP OAuth authorization was declined".into()));
        }
        let _guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
        let result = session.handle_callback_url(incoming.url.as_str()).await;
        incoming.reply(result.is_ok()).await;
        result.map(|_| ()).map_err(auth_error)
    };
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(600), receive) => result.map_err(|_| Error::Config("MCP login timed out".into()))?,
        _ = tokio::signal::ctrl_c() => Err(Error::Cancelled),
    }?;
    Ok(generation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_runtime_persists_rotated_mcp_refresh_token() {
        use std::io::{Read as _, Write as _};
        use std::time::{Duration, Instant};
        let directory = tempfile::tempdir().unwrap();
        let store = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: Some(Uuid::new_v4()),
            new_login: true,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        let creds: StoredCredentials = serde_json::from_value(serde_json::json!({
            "client_id":"fixture", "token_response":{"access_token":"old", "token_type":"Bearer", "refresh_token":"old-refresh"}
        })).unwrap();
        store.write(creds).unwrap();
        let store = FileCredentials {
            new_login: false,
            ..store
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).unwrap() > 0);
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let body = r#"{"access_token":"new","token_type":"Bearer","refresh_token":"rotated"}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let task_store = store.clone();
        let task = runtime.block_on(async move {
            let url = format!("http://{address}");
            let mut manager = oauth_manager(&format!("{url}/mcp"), None).await.unwrap();
            manager.set_metadata(serde_json::from_value(serde_json::json!({
                "issuer":url, "authorization_endpoint":format!("http://{address}/authorize"),
                "token_endpoint":format!("http://{address}/token"), "response_types_supported":["code"]
            })).unwrap());
            manager.configure_client_id("fixture").unwrap();
            manager.set_credential_store(task_store);
            let task = tokio::spawn(async move { crate::mcp_refresh::run(async move { manager.refresh_token().await }).await });
            tokio::time::timeout(Duration::from_secs(5), async {
                while started_rx.try_recv().is_err() { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.unwrap();
            task.abort_handle()
        });
        task.abort();
        drop(runtime);
        release_tx.send(()).unwrap();
        server.join().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let bytes = std::fs::read(&store.path).unwrap();
            if String::from_utf8_lossy(&bytes).contains("rotated") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "rotated refresh token was not persisted"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn cancelled_mcp_request_waiting_for_refresh_lock_does_not_commit() {
        use std::time::Duration;
        let directory = tempfile::tempdir().unwrap();
        let store = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: None,
            new_login: false,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        let held = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(store.path.with_extension("lock"))
            .unwrap();
        held.lock().unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (committed_tx, committed_rx) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let task = runtime.block_on(async move {
            let task = tokio::spawn(async move {
                crate::mcp_refresh::run(async move {
                    entered_tx.send(()).unwrap();
                    let _guard = store.acquire_refresh_guard().await.unwrap();
                    let _ = committed_tx.send(());
                })
                .await
            });
            tokio::time::timeout(Duration::from_secs(3), async {
                while entered_rx.try_recv().is_err() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            task.abort_handle()
        });
        task.abort();
        drop(runtime);
        std::thread::sleep(Duration::from_millis(100));
        held.unlock().unwrap();
        assert!(
            committed_rx
                .recv_timeout(Duration::from_millis(250))
                .is_err()
        );
    }

    #[tokio::test]
    async fn oauth_manager_uses_proxy_for_registration_and_refresh() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let options = crate::mcp::HttpOptionsWithProxyHosts {
            http: crate::mcp::HttpOptions {
                proxies: vec![
                    reqwest_mcp::Proxy::all(format!(
                        "http://localhost:{}",
                        listener.local_addr().unwrap().port()
                    ))
                    .unwrap(),
                ],
                idle_timeout: None,
            },
            proxy_hosts: vec!["localhost".into()],
        };
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for body in [
                r#"{"client_id":"fixture","redirect_uris":["http://127.0.0.1/callback"]}"#,
                r#"{"access_token":"new","token_type":"Bearer","refresh_token":"next"}"#,
            ] {
                let (socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut socket = BufReader::new(socket);
                let mut first = String::new();
                socket.read_line(&mut first).await.unwrap();
                let mut len = 0;
                loop {
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(n) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = n.trim().parse().unwrap();
                    }
                }
                let mut data = vec![0; len];
                socket.read_exact(&mut data).await.unwrap();
                requests.push((first, String::from_utf8(data).unwrap()));
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        let mut manager = oauth_manager("http://127.0.0.1:1/mcp", Some(&options))
            .await
            .unwrap();
        let metadata: AuthorizationMetadata = serde_json::from_value(serde_json::json!({
            "issuer":"http://127.0.0.1:1", "authorization_endpoint":"http://127.0.0.1:1/authorize", "token_endpoint":"http://127.0.0.1:1/token", "registration_endpoint":"http://127.0.0.1:1/register", "response_types_supported":["code"]
        })).unwrap();
        manager.set_metadata(metadata);
        let config = manager
            .register_client("fixture", "http://127.0.0.1/callback", &[])
            .await
            .unwrap();
        manager.configure_client(config).unwrap();
        let store = rmcp::transport::auth::InMemoryCredentialStore::new();
        let creds: StoredCredentials = serde_json::from_value(serde_json::json!({"client_id":"fixture", "token_response":{"access_token":"old", "token_type":"Bearer", "refresh_token":"old-refresh"}})).unwrap();
        store.save(creds).await.unwrap();
        manager.set_credential_store(store);
        manager.refresh_token().await.unwrap();
        let requests = server.await.unwrap();
        assert!(
            requests[0]
                .0
                .starts_with("POST http://127.0.0.1:1/register ")
        );
        assert!(requests[1].0.starts_with("POST http://127.0.0.1:1/token "));
        assert!(requests[1].1.contains("refresh_token=old-refresh"));
    }

    #[tokio::test]
    async fn oauth_discovery_uses_configured_proxy() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let options = crate::mcp::HttpOptionsWithProxyHosts {
            http: crate::mcp::HttpOptions {
                proxies: vec![
                    reqwest_mcp::Proxy::all(format!("http://{}", listener.local_addr().unwrap()))
                        .unwrap(),
                ],
                idle_timeout: None,
            },
            proxy_hosts: vec!["127.0.0.1".into()],
        };
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let mut socket = BufReader::new(socket);
                let mut first = String::new();
                socket.read_line(&mut first).await.unwrap();
                let _ = sender.send(first);
                loop {
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                socket
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        });
        let manager = oauth_manager("http://127.0.0.1:1/mcp", Some(&options))
            .await
            .unwrap();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            manager.resolve_metadata(),
        )
        .await
        .unwrap();
        let first = receiver.recv().await.unwrap();
        assert!(first.starts_with("GET http://127.0.0.1:1/"), "{first}");
        server.abort();
    }

    #[tokio::test]
    async fn no_proxy_cannot_use_proxy_name_as_direct_metadata_destination() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let options = crate::mcp::HttpOptionsWithProxyHosts {
            http: crate::mcp::HttpOptions {
                // Simulate NO_PROXY choosing direct access for every request.
                proxies: vec![reqwest_mcp::Proxy::custom(|_| None::<reqwest_mcp::Url>)],
                idle_timeout: None,
            },
            proxy_hosts: vec!["LOCALHOST.".into()],
        };
        let target = format!(
            "http://localhost:{}/metadata",
            listener.local_addr().unwrap().port()
        );
        assert!(
            configured_metadata(&target, "https://public.example/mcp", Some(&options))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn registration_redirect_to_proxy_hostname_is_rejected() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = origin.local_addr().unwrap().port();
        let target_port = destination.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut buffer = [0; 4096];
            let _ = socket.read(&mut buffer).await.unwrap();
            socket.write_all(format!("HTTP/1.1 302 Found\r\nLocation: http://localhost:{target_port}/register\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        });
        let options = crate::mcp::HttpOptionsWithProxyHosts {
            http: crate::mcp::HttpOptions {
                proxies: vec![],
                idle_timeout: None,
            },
            proxy_hosts: vec!["localhost".into()],
        };
        let base = format!("http://127.0.0.1:{port}");
        let mut manager = oauth_manager(&base, Some(&options)).await.unwrap();
        manager.set_metadata(serde_json::from_value(serde_json::json!({
            "issuer":base, "authorization_endpoint":format!("{base}/authorize"),
            "token_endpoint":format!("{base}/token"), "registration_endpoint":format!("{base}/register"),
            "response_types_supported":["code"]
        })).unwrap());
        assert!(
            manager
                .register_client("fixture", "http://127.0.0.1/callback", &[])
                .await
                .is_err()
        );
        server.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), destination.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn resolver_filters_direct_dns_and_only_exempts_proxy_lookup() {
        use reqwest_mcp::dns::Resolve;
        let mut resolver = OAuthResolver {
            base_ip: None,
            base_class: None,
            proxy_hosts: vec![],
        };
        assert!(
            resolver
                .resolve("localhost".parse().unwrap())
                .await
                .is_err()
        );
        resolver.proxy_hosts.push("LOCALHOST.".into());
        let addresses: Vec<_> = resolver
            .resolve("localhost".parse().unwrap())
            .await
            .unwrap()
            .collect();
        assert!(!addresses.is_empty());
        assert!(!resolver.allows_url(&reqwest_mcp::Url::parse("http://localhost/").unwrap()));
    }

    #[test]
    fn http_options_original_struct_literal_remains_valid() {
        let _ = crate::mcp::HttpOptions {
            proxies: Vec::new(),
            idle_timeout: None,
        };
    }

    #[test]
    fn oauth_address_policy_rejects_rebinding_and_metadata_addresses() {
        let resolver = OAuthResolver {
            base_ip: None,
            base_class: None,
            proxy_hosts: vec![],
        };
        assert!(resolver.allowed("8.8.8.8".parse().unwrap()));
        assert!(!resolver.allowed("127.0.0.1".parse().unwrap()));
        assert!(!resolver.allowed("169.254.169.254".parse().unwrap()));
        assert!(!resolver.allowed("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!resolver.allows_url(&reqwest_mcp::Url::parse("http://127.0.0.1/").unwrap()));
    }

    #[tokio::test]
    async fn configured_metadata_uses_the_supplied_http_client() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy =
            reqwest_mcp::Proxy::all(format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let client = reqwest_mcp::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .build()
            .unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let mut socket = BufReader::new(socket);
            let mut first = String::new();
            socket.read_line(&mut first).await.unwrap();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let body = r#"{"issuer":"https://example.com","authorization_endpoint":"https://example.com/authorize","token_endpoint":"https://example.com/token","response_types_supported":["code"]}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            first
        });
        let metadata = configured_metadata_with_client("http://127.0.0.1:1/metadata", &client)
            .await
            .unwrap();
        assert_eq!(metadata.issuer.as_deref(), Some("https://example.com"));
        assert!(
            server
                .await
                .unwrap()
                .starts_with("GET http://127.0.0.1:1/metadata ")
        );
    }

    #[tokio::test]
    async fn scope_upgrade_preserves_prior_credentials_without_all_scopes() {
        let directory = tempfile::tempdir().unwrap();
        let first = Uuid::new_v4();
        let login = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: Some(first),
            new_login: true,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        let credential = |scopes: &[&str]| {
            StoredCredentials::new(
                "client".into(),
                None,
                scopes.iter().map(|scope| (*scope).to_owned()).collect(),
                None,
            )
        };
        login.save(credential(&["read"])).await.unwrap();
        let upgraded_generation = Uuid::new_v4();
        let upgrade = FileCredentials {
            generation: Some(upgraded_generation),
            upgrade: Some(Arc::new(UpgradeGuard {
                generation: first,
                scopes: vec!["read".into(), "extra".into()],
            })),
            ..login.clone()
        };
        assert!(upgrade.save(credential(&["extra"])).await.is_err());
        assert_eq!(login.read().unwrap().unwrap().generation, first);
        upgrade.save(credential(&["read", "extra"])).await.unwrap();
        assert_eq!(
            login.read().unwrap().unwrap().generation,
            upgraded_generation
        );
        assert!(upgrade.save(credential(&["read", "extra"])).await.is_err());
    }

    #[tokio::test]
    async fn generations_survive_refresh_and_reject_replaced_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let first = Uuid::new_v4();
        let login = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: Some(first),
            new_login: true,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        *login.registration.lock().unwrap() = Some(ClientRegistration {
            client_secret: Some("fixture-client-secret".into()),
            redirect_uri: "http://127.0.0.1:12345/callback".into(),
            requested_scopes: Vec::new(),
            auth_server_metadata_url: None,
        });
        login
            .save(StoredCredentials::new("first".into(), None, vec![], None))
            .await
            .unwrap();
        let client = FileCredentials {
            generation: Some(first),
            new_login: false,
            upgrade: None,
            registration: Arc::default(),
            ..login.clone()
        };
        client
            .save(StoredCredentials::new(
                "first-refreshed".into(),
                None,
                vec![],
                None,
            ))
            .await
            .unwrap();
        assert_eq!(
            client.load().await.unwrap().unwrap().client_id,
            "first-refreshed"
        );
        assert_eq!(client.read().unwrap().unwrap().generation, first);
        let registered = client.read().unwrap().unwrap().registration.unwrap();
        assert_eq!(
            registered.client_secret.as_deref(),
            Some("fixture-client-secret")
        );
        assert_eq!(registered.redirect_uri, "http://127.0.0.1:12345/callback");
        assert!(
            client
                .save(StoredCredentials::new(
                    "x".repeat(128 * 1024),
                    None,
                    vec![],
                    None
                ))
                .await
                .is_err()
        );
        assert_eq!(
            client.load().await.unwrap().unwrap().client_id,
            "first-refreshed"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&client.path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let second = FileCredentials {
            generation: Some(Uuid::new_v4()),
            ..login
        };
        second
            .save(StoredCredentials::new("second".into(), None, vec![], None))
            .await
            .unwrap();
        assert!(matches!(
            client.load().await,
            Err(AuthError::AuthorizationRequired)
        ));
        assert!(matches!(
            client
                .save(StoredCredentials::new("stale".into(), None, vec![], None))
                .await,
            Err(AuthError::AuthorizationRequired)
        ));
        second.clear().await.unwrap();
        assert!(second.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn successful_login_can_replace_a_damaged_store() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        std::fs::write(&path, "damaged").unwrap();
        let login = FileCredentials {
            path,
            generation: Some(Uuid::new_v4()),
            new_login: true,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        assert!(login.load().await.unwrap().is_none());
        login
            .save(StoredCredentials::new(
                "recovered".into(),
                None,
                vec![],
                None,
            ))
            .await
            .unwrap();
        let client = FileCredentials {
            new_login: false,
            ..login
        };
        assert_eq!(client.load().await.unwrap().unwrap().client_id, "recovered");
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&client.path).unwrap()).unwrap();
        legacy.as_object_mut().unwrap().remove("registration");
        std::fs::write(&client.path, serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert!(client.read().unwrap().unwrap().registration.is_none());
        assert_eq!(client.load().await.unwrap().unwrap().client_id, "recovered");
    }

    #[test]
    fn cancelled_refresh_guard_wait_does_not_delay_runtime_shutdown() {
        use std::time::{Duration, Instant};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        std::fs::write(&path, b"unchanged").unwrap();
        let held = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))
            .unwrap();
        held.lock().unwrap();
        let store = FileCredentials {
            path: path.clone(),
            generation: None,
            new_login: false,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let waiting = tokio::spawn(async move { store.acquire_refresh_guard().await });
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!waiting.is_finished());
            waiting.abort();
            let _ = waiting.await;
        });
        let started = Instant::now();
        drop(runtime);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(std::fs::read(path).unwrap(), b"unchanged");
        held.unlock().unwrap();
    }

    #[tokio::test]
    async fn refresh_guard_serializes_process_store_access() {
        let directory = tempfile::tempdir().unwrap();
        let store = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: None,
            new_login: false,
            upgrade: None,
            registration: Arc::default(),
            lease: Arc::default(),
        };
        let first = store.acquire_refresh_guard().await.unwrap();
        let lease = store.lease.lock().unwrap().upgrade();
        let mut second = tokio::spawn(async move { store.acquire_refresh_guard().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut second)
                .await
                .is_err()
        );
        drop(first);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut second)
                .await
                .is_err()
        );
        drop(lease);
        assert!(second.await.unwrap().unwrap().is_some());
    }
}
