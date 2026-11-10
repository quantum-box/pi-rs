//! Persistent, generation-bound credentials for native MCP OAuth sessions.
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use pi_rs::{Error, Result};
use rmcp::transport::auth::{
    AuthClient, AuthError, AuthorizationManager, CredentialRefreshGuard, CredentialStore,
    StoredCredentials,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

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

enum Registration<'a> {
    New {
        client_id: Option<&'a str>,
        client_secret: Option<&'a str>,
        metadata_url: Option<&'a str>,
    },
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
            return Error::AuthorizationRequired(
                "MCP OAuth login required; run pi mcp login for this server".into(),
            );
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
        let file = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(path.parent().ok_or_else(store_error)?)
                .map_err(|_| store_error())?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)
                .map_err(|_| store_error())?;
            file.lock().map_err(|_| store_error())?;
            Ok::<_, AuthError>(Arc::new(file))
        })
        .await
        .map_err(|_| store_error())??;
        *self.lease.lock().unwrap_or_else(|error| error.into_inner()) = Arc::downgrade(&file);
        Ok(Some(CredentialRefreshGuard::new(file)))
    }
}

/// Restore a previously authorized client. Each connection is bound to one login generation.
pub async fn client(
    name: &str,
    url: &str,
    client: reqwest_mcp::Client,
) -> Result<(AuthClient<reqwest_mcp::Client>, String)> {
    let mut store = FileCredentials::new(name, url)?;
    let guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
    let envelope = store
        .read()
        .map_err(auth_error)?
        .ok_or_else(|| auth_error(AuthError::AuthorizationRequired))?;
    store.generation = Some(envelope.generation);
    let identity = envelope.generation.to_string();
    let mut manager = AuthorizationManager::new(url).await.map_err(auth_error)?;
    manager.set_credential_store(store);
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
    Ok((AuthClient::new(client, manager), identity))
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
    authorize(
        name,
        url,
        Registration::New {
            client_id,
            client_secret,
            metadata_url: None,
        },
        scopes,
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
    authorize(
        name,
        url,
        Registration::New {
            client_id: None,
            client_secret: None,
            metadata_url: Some(client_metadata_url),
        },
        scopes,
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
        announce,
    )
    .await
}

async fn authorize(
    name: &str,
    url: &str,
    registration: Registration<'_>,
    scopes: &[String],
    announce: impl Fn(&str),
) -> Result<Uuid> {
    use rmcp::transport::auth::{AuthorizationRequest, AuthorizationSession};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let parsed = reqwest_mcp::Url::parse(url)
        .map_err(|_| Error::Config("invalid MCP OAuth resource URL".into()))?;
    let loopback = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback))
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(Error::Config(
            "MCP OAuth requires HTTPS or loopback HTTP without embedded credentials".into(),
        ));
    }
    let (client_id, client_secret, client_metadata_url, upgrade) = match &registration {
        Registration::New {
            client_id,
            client_secret,
            metadata_url,
        } => (*client_id, *client_secret, *metadata_url, None),
        Registration::Upgrade(upgrade) => {
            let client = upgrade.previous.registration.as_ref().ok_or_else(|| {
                Error::Config("MCP registration is missing; sign in again before upgrading".into())
            })?;
            (
                Some(upgrade.previous.credentials.client_id.as_str()),
                client.client_secret.as_deref(),
                None,
                Some(upgrade),
            )
        }
    };
    if client_secret.is_some() && client_id.is_none() {
        return Err(Error::Config(
            "--client-secret-env requires --client-id".into(),
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
    let mut manager = AuthorizationManager::new(url).await.map_err(auth_error)?;
    manager.set_credential_store(store.clone());
    let metadata = manager.resolve_metadata().await.map_err(auth_error)?;
    if let Some(upgrade) = upgrade
        && (metadata.metadata.issuer.is_none()
            || metadata.metadata.issuer != upgrade.previous.credentials.issuer)
    {
        return Err(Error::Config(
            "MCP authorization issuer changed; sign in again before upgrading".into(),
        ));
    }
    let supports_client_metadata = metadata
        .metadata
        .additional_fields
        .get("client_id_metadata_document_supported")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    manager.set_metadata(metadata.metadata);
    let port = if let Some(upgrade) = upgrade {
        let redirect = &upgrade
            .previous
            .registration
            .as_ref()
            .expect("registered client")
            .redirect_uri;
        let redirect = reqwest_mcp::Url::parse(redirect)
            .map_err(|_| Error::Config("invalid saved MCP callback URL".into()))?;
        redirect
            .port()
            .filter(|_| {
                redirect.scheme() == "http"
                    && redirect.host_str() == Some("127.0.0.1")
                    && redirect.path() == "/callback"
            })
            .ok_or_else(|| Error::Config("invalid saved MCP callback URL".into()))?
    } else {
        0
    };
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let callback = format!(
        "http://127.0.0.1:{}/callback",
        listener.local_addr()?.port()
    );
    let scopes = if scopes.is_empty() {
        manager.select_initial_scopes(&[]).await
    } else {
        scopes.to_vec()
    };
    let (client_id, secret) = if let Some(client_id) = client_id {
        (Some(client_id.to_owned()), client_secret.map(str::to_owned))
    } else if client_metadata_url.is_some() && supports_client_metadata {
        (None, None)
    } else {
        let refs: Vec<_> = scopes.iter().map(String::as_str).collect();
        let registration = manager
            .register_client("pi-rs", &callback, &refs)
            .await
            .map_err(auth_error)?;
        (Some(registration.client_id), registration.client_secret)
    };
    *store
        .registration
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(ClientRegistration {
        client_secret: secret.clone(),
        redirect_uri: callback.clone(),
        requested_scopes: scopes.clone(),
    });
    let mut request = AuthorizationRequest::new(&callback)
        .with_client_name("pi-rs")
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
    crate::mcp::open_browser(session.get_authorization_url()).await?;
    let receive = async {
        loop {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            let read = async {
                while bytes.len() <= 8192 && !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut chunk = [0_u8; 1024];
                    let count = stream.read(&mut chunk).await?;
                    if count == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                Ok::<_, std::io::Error>(())
            };
            if !tokio::time::timeout(Duration::from_secs(3), read)
                .await
                .is_ok_and(|result| result.is_ok())
            {
                continue;
            }
            let target = std::str::from_utf8(&bytes)
                .ok()
                .and_then(|request| request.lines().next())
                .and_then(|line| line.strip_prefix("GET "))
                .and_then(|line| line.strip_suffix(" HTTP/1.1"));
            let target = target.filter(|target| {
                target.starts_with("/callback?")
                    && bytes.len() <= 8192
                    && bytes.windows(4).any(|part| part == b"\r\n\r\n")
            });
            let redirect = target
                .and_then(|target| reqwest_mcp::Url::parse(&callback).ok()?.join(target).ok());
            let valid = redirect.filter(|redirect| {
                let states: Vec<_> = redirect
                    .query_pairs()
                    .filter(|(name, _)| name == "state")
                    .collect();
                let codes = redirect
                    .query_pairs()
                    .filter(|(name, _)| name == "code")
                    .count();
                let errors = redirect
                    .query_pairs()
                    .filter(|(name, _)| name == "error")
                    .count();
                states.len() == 1 && states[0].1 == expected_state && codes + errors == 1
            });
            let Some(redirect) = valid else {
                let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 17\r\nConnection: close\r\n\r\nInvalid callback.").await;
                continue;
            };
            if redirect.query_pairs().any(|(name, _)| name == "error") {
                let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 21\r\nConnection: close\r\n\r\nAuthorization failed.").await;
                return Err(Error::Config("MCP OAuth authorization was declined".into()));
            }
            let _guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
            let result = session.handle_callback_url(redirect.as_str()).await;
            let response = if result.is_ok() {
                b"HTTP/1.1 200 OK\r\nContent-Length: 34\r\nConnection: close\r\n\r\nSigned in. You may close this tab.".as_slice()
            } else {
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 21\r\nConnection: close\r\n\r\nAuthorization failed.".as_slice()
            };
            let _ = stream.write_all(response).await;
            return result.map(|_| ()).map_err(auth_error);
        }
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
