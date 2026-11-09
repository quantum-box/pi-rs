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
    // Blocking writes retain this lock even if their async caller is cancelled.
    lease: Arc<Mutex<Weak<std::fs::File>>>,
}
#[derive(Serialize, Deserialize)]
struct Envelope {
    generation: Uuid,
    credentials: StoredCredentials,
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
    fn write(&self, credentials: StoredCredentials) -> std::result::Result<(), AuthError> {
        let current = if self.new_login { None } else { self.read()? };
        if !self.new_login && self.generation.is_some() && current.is_none() {
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
            "MCP OAuth login required; run pi mcp login for this server"
        }
        AuthError::RegistrationFailed(_) => {
            "MCP OAuth client registration failed; use --client-id for a pre-registered public client"
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
    drop(guard);
    Ok((AuthClient::new(client, manager), identity))
}

pub async fn logout(name: &str, url: &str) -> Result<()> {
    let store = FileCredentials::new(name, url)?;
    let _guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
    store.clear().await.map_err(auth_error)
}

/// Run a native authorization-code flow with a loopback callback and PKCE.
pub async fn login(
    name: &str,
    url: &str,
    client_id: Option<&str>,
    scopes: &[String],
    announce: impl Fn(&str),
) -> Result<()> {
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
    let mut store = FileCredentials::new(name, url)?;
    store.new_login = true;
    store.generation = Some(Uuid::new_v4());
    let mut manager = AuthorizationManager::new(url).await.map_err(auth_error)?;
    manager.set_credential_store(store.clone());
    let metadata = manager.resolve_metadata().await.map_err(auth_error)?;
    manager.set_metadata(metadata.metadata);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let callback = format!(
        "http://127.0.0.1:{}/callback",
        listener.local_addr()?.port()
    );
    let mut request = AuthorizationRequest::new(&callback)
        .with_client_name("pi-rs")
        .with_initial_scope_selection()
        .with_scopes(scopes.iter().cloned());
    if let Some(client_id) = client_id {
        request = request.with_preregistered_client(client_id);
    }
    let session = AuthorizationSession::new(manager, request)
        .await
        .map_err(|(_, error)| auth_error(error))?;
    let authorization = reqwest_mcp::Url::parse(session.get_authorization_url())
        .map_err(|_| Error::Config("invalid MCP authorization URL".into()))?;
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
            let valid = redirect.as_ref().is_some_and(|redirect| {
                let states: Vec<_> = redirect
                    .query_pairs()
                    .filter(|(name, _)| name == "state")
                    .collect();
                let codes = redirect
                    .query_pairs()
                    .filter(|(name, _)| name == "code")
                    .count();
                states.len() == 1 && states[0].1 == expected_state && codes == 1
            });
            if !valid {
                let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 17\r\nConnection: close\r\n\r\nInvalid callback.").await;
                continue;
            }
            let _guard = store.acquire_refresh_guard().await.map_err(auth_error)?;
            let result = session
                .handle_callback_url(redirect.as_ref().expect("validated redirect").as_str())
                .await;
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generations_survive_refresh_and_reject_replaced_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let first = Uuid::new_v4();
        let login = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: Some(first),
            new_login: true,
            lease: Arc::default(),
        };
        login
            .save(StoredCredentials::new("first".into(), None, vec![], None))
            .await
            .unwrap();
        let client = FileCredentials {
            generation: Some(first),
            new_login: false,
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
    }

    #[tokio::test]
    async fn refresh_guard_serializes_process_store_access() {
        let directory = tempfile::tempdir().unwrap();
        let store = FileCredentials {
            path: directory.path().join("credentials.json"),
            generation: None,
            new_login: false,
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
