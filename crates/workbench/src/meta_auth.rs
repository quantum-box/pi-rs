//! Refresh Pi-compatible Meta subscription credentials without forwarding the identity token to inference.
use pi_rs::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MINT_URL: &str = "https://api.meta.ai/muse-code/key";
const MINIMUM: Duration = Duration::from_secs(300);
const KEY_LIFETIME_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Credential {
    pub access: String,
    pub refresh: String,
    pub expires: u64,
    #[serde(rename = "type")]
    pub kind: String,
}

fn error(message: &str) -> Error {
    Error::Config(format!("Meta OAuth: {message}"))
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn expires_soon(value: &Credential, minimum: Duration) -> bool {
    value.expires <= now().saturating_add(minimum.as_millis().try_into().unwrap_or(u64::MAX))
}
fn parse(value: &Value) -> Result<Credential> {
    let entry = value
        .get("meta")
        .filter(|entry| entry["type"] == "oauth")
        .ok_or_else(|| error("credentials not configured"))?;
    if let Some(endpoint) = entry.get("endpoint")
        && endpoint.as_str() != Some("https://api.meta.ai/v1/responses")
    {
        return Err(error(
            "credentials are not bound to the official Meta endpoint",
        ));
    }
    let credential: Credential =
        serde_json::from_value(entry.clone()).map_err(|_| error("invalid saved credentials"))?;
    if !super::model_auth::valid_key(&credential.access)
        || !super::model_auth::valid_key(&credential.refresh)
    {
        return Err(error("invalid saved credentials"));
    }
    Ok(credential)
}

#[derive(Clone)]
struct Service {
    url: reqwest::Url,
    client: reqwest::Client,
}
impl Service {
    fn new(settings: &super::config::Settings) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15));
        for proxy in super::http_proxy::configured(settings)? {
            builder = builder.proxy(proxy);
        }
        if let Some(timeout) = settings.http_idle_timeout() {
            builder = builder.read_timeout(timeout);
        }
        Ok(Self {
            url: reqwest::Url::parse(MINT_URL).expect("constant Meta URL"),
            client: builder.build().map_err(|_| error("cannot create client"))?,
        })
    }
    async fn mint(&self, identity: &str) -> Result<String> {
        let mut response = self
            .client
            .post(self.url.clone())
            .bearer_auth(identity)
            .header("accept", "application/json")
            .header("x-api-version", "1.0.0")
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|_| error("key mint request failed"))?;
        let status = response.status();
        if status == 401 || status == 403 {
            return Err(error("identity expired; sign in again with Pi"));
        }
        if !status.is_success() {
            return Err(error(&format!("key mint failed ({status})")));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error("key mint response failed"))?
        {
            if body.len().saturating_add(chunk.len()) > 65536 {
                return Err(error("key mint response too large"));
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| error("invalid key mint response"))?;
        value["api_key"]
            .as_str()
            .filter(|key| super::model_auth::valid_key(key))
            .map(str::to_owned)
            .ok_or_else(|| error("key mint response did not contain a valid API key"))
    }
}

async fn access_at(
    path: PathBuf,
    service: Service,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    let window = minimum.unwrap_or_default().max(MINIMUM);
    let guard = match super::auth_lock::acquire_async(&path).await {
        Ok(guard) => guard,
        Err(err) => {
            let stored = super::config::read_readonly_auth(&path, err)?;
            let value = parse(&stored)?;
            if no_refresh || !expires_soon(&value, window) {
                return Ok(value.access);
            }
            return Err(error("cannot refresh credentials in a read-only store"));
        }
    };
    let stored = super::config::read_object_unlocked(&path)?;
    let value = parse(&stored)?;
    guard.check()?;
    if no_refresh || !expires_soon(&value, window) {
        return Ok(value.access);
    }
    let previous = stored["meta"].clone();
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| error("cannot start refresh runtime"))?;
        let _barrier = super::process::refresh_barrier()?;
        let access = runtime.block_on(service.mint(&value.refresh))?;
        let expires = now().saturating_add(KEY_LIFETIME_MS);
        super::config::update_object_locked(&path, Some(&guard), |data| {
            let current = data
                .get_mut("meta")
                .ok_or_else(|| error("credentials changed during refresh"))?;
            if ["type", "access", "refresh", "expires", "endpoint"]
                .iter()
                .any(|key| current.get(*key) != previous.get(*key))
            {
                return Err(error("credentials changed during refresh"));
            }
            current["access"] = access.clone().into();
            current["expires"] = expires.into();
            Ok(())
        })?;
        if minimum.is_some()
            && expires <= now().saturating_add(window.as_millis().try_into().unwrap_or(u64::MAX))
        {
            return Err(error("refreshed key expires too soon"));
        }
        Ok(access)
    })
    .await
    .map_err(|_| error("refresh task failed"))?
}

pub(super) async fn access(
    settings: &super::config::Settings,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    access_at(
        super::config::agent_dir().join("auth.json"),
        Service::new(settings)?,
        minimum,
        no_refresh,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn fixture(path: &std::path::Path, expires: u64) {
        std::fs::write(path, serde_json::json!({"meta":{"type":"oauth","access":"old-key","refresh":"identity-secret","expires":expires,"extra":"preserve"},"other":{"type":"api_key","key":"keep"}}).to_string()).unwrap();
    }
    async fn server(
        status: &'static str,
        body: &'static str,
    ) -> (Service, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = reqwest::Url::parse(&format!(
            "http://{}/muse-code/key",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut bytes = [0; 2048];
                let count = socket.read(&mut bytes).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&bytes[..count]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&request[..end]);
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.split_once(':').and_then(|(key, value)| {
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                        })
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (
            Service {
                url,
                client: reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .unwrap(),
            },
            task,
        )
    }

    #[tokio::test]
    async fn expired_pi_credential_remints_and_persists_once() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        fixture(&path, 1);
        let (service, server) = server("200 OK", r#"{"api_key":"minted-key"}"#).await;
        let (first, second) = tokio::join!(
            access_at(path.clone(), service.clone(), None, false),
            access_at(path.clone(), service, None, false)
        );
        assert_eq!(first.unwrap(), "minted-key");
        assert_eq!(second.unwrap(), "minted-key");
        let request = server.await.unwrap().to_ascii_lowercase();
        assert!(request.starts_with("post /muse-code/key "));
        assert!(request.contains("authorization: bearer identity-secret"));
        assert!(request.contains("x-api-version: 1.0.0"));
        assert!(request.ends_with("{}"));
        let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved["meta"]["access"], "minted-key");
        assert_eq!(saved["meta"]["refresh"], "identity-secret");
        assert_eq!(saved["meta"]["extra"], "preserve");
        assert_eq!(saved["other"]["key"], "keep");
        assert!(saved["meta"]["expires"].as_u64().unwrap() >= now() + KEY_LIFETIME_MS - 10_000);
    }

    #[tokio::test]
    async fn denied_mint_preserves_credentials_and_masks_secrets() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        fixture(&path, 1);
        let before = std::fs::read(&path).unwrap();
        let (service, server) = server("401 Unauthorized", r#"{"error":"identity-secret"}"#).await;
        let message = access_at(path.clone(), service, None, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains("sign in"));
        assert!(!message.contains("identity-secret"));
        assert_eq!(std::fs::read(path).unwrap(), before);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn explicit_minimum_is_checked_after_the_new_key_is_saved() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        fixture(&path, 1);
        let (service, server) = server("200 OK", r#"{"api_key":"minted-key"}"#).await;
        let error = access_at(
            path.clone(),
            service,
            Some(Duration::from_secs(25 * 60 * 60)),
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("expires too soon"));
        let stored: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(stored["meta"]["access"], "minted-key");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cached_no_refresh_and_wrong_endpoint_are_safe() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        fixture(&path, 1);
        let service = Service {
            url: reqwest::Url::parse("http://127.0.0.1:1/").unwrap(),
            client: reqwest::Client::new(),
        };
        assert_eq!(
            access_at(path.clone(), service.clone(), None, true)
                .await
                .unwrap(),
            "old-key"
        );
        fixture(&path, now() + KEY_LIFETIME_MS);
        assert_eq!(
            access_at(path.clone(), service.clone(), None, false)
                .await
                .unwrap(),
            "old-key"
        );
        let mut stored: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        stored["meta"]["endpoint"] = "https://attacker.example/v1/responses".into();
        std::fs::write(&path, stored.to_string()).unwrap();
        assert!(access_at(path, service, None, false).await.is_err());
    }
}
