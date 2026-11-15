//! Refresh Pi-compatible Anthropic subscription credentials.
use pi_rs::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Credential {
    pub access: String,
    pub refresh: String,
    pub expires: u64,
    #[serde(rename = "type")]
    pub kind: String,
}
fn error(message: &str) -> Error {
    Error::Config(format!("Anthropic OAuth: {message}"))
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn parse(value: &Value) -> Result<Option<Credential>> {
    let Some(entry) = value
        .get("anthropic")
        .filter(|entry| entry["type"] == "oauth")
    else {
        return Ok(None);
    };
    let credential: Credential =
        serde_json::from_value(entry.clone()).map_err(|_| error("invalid stored credential"))?;
    if !super::model_auth::valid_key(&credential.access)
        || !super::model_auth::valid_key(&credential.refresh)
    {
        return Err(error("invalid stored credential"));
    }
    Ok(Some(credential))
}
fn expires_soon(value: &Credential, window: Duration) -> bool {
    value.expires <= now().saturating_add(window.as_millis().try_into().unwrap_or(u64::MAX))
}
#[derive(Clone)]
struct Service {
    client: reqwest::Client,
    url: reqwest::Url,
}
impl Service {
    fn new(settings: &super::config::Settings) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30));
        for proxy in super::http_proxy::configured(settings)? {
            builder = builder.proxy(proxy);
        }
        if let Some(timeout) = settings.http_idle_timeout() {
            builder = builder.read_timeout(timeout);
        }
        Ok(Self {
            client: builder.build().map_err(|_| error("cannot create client"))?,
            url: reqwest::Url::parse(TOKEN_URL).expect("fixed URL"),
        })
    }
    async fn refresh(&self, previous: &Credential) -> Result<Credential> {
        let mut response = self.client.post(self.url.clone()).json(&serde_json::json!({"grant_type":"refresh_token","client_id":CLIENT_ID,"refresh_token":previous.refresh})).send().await.map_err(|_| error("refresh request failed"))?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error("refresh response failed"))?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(error("refresh response too large"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(error(&format!("refresh failed ({status})")));
        }
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
            refresh_token: String,
            expires_in: u64,
        }
        let token: Token =
            serde_json::from_slice(&bytes).map_err(|_| error("invalid refresh response"))?;
        if !super::model_auth::valid_key(&token.access_token)
            || !super::model_auth::valid_key(&token.refresh_token)
            || token.expires_in == 0
        {
            return Err(error("invalid refresh response"));
        }
        Ok(Credential {
            access: token.access_token,
            refresh: token.refresh_token,
            expires: now()
                .saturating_add(token.expires_in.saturating_mul(1000))
                .saturating_sub(300_000),
            kind: "oauth".into(),
        })
    }
}
async fn access_at<F>(
    path: std::path::PathBuf,
    service: F,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String>
where
    F: FnOnce() -> Result<Service> + Send + 'static,
{
    let window = minimum.unwrap_or_default().max(Duration::from_secs(300));
    let guard = match super::auth_lock::acquire_async(&path).await {
        Ok(guard) => guard,
        Err(err) => {
            let stored = super::config::read_readonly_auth(&path, err)?;
            let value = parse(&stored)?.ok_or_else(|| error("credentials not configured"))?;
            if no_refresh || !expires_soon(&value, window) {
                return Ok(value.access);
            }
            return Err(error("cannot refresh credentials in a read-only store"));
        }
    };
    let stored = super::config::read_object_unlocked(&path)?;
    let value = parse(&stored)?.ok_or_else(|| error("credentials not configured"))?;
    let previous = stored["anthropic"].clone();
    guard.check()?;
    if no_refresh || !expires_soon(&value, window) {
        return Ok(value.access);
    }
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| error("cannot start refresh runtime"))?;
        let service = service()?;
        let _barrier = super::process::refresh_barrier()?;
        let value = runtime.block_on(service.refresh(&value))?;
        super::config::update_object_locked(&path, Some(&guard), |data| {
            let current = data
                .get_mut("anthropic")
                .ok_or_else(|| error("credentials changed during refresh"))?;
            if *current != previous {
                return Err(error("credentials changed during refresh"));
            }
            current["access"] = value.access.clone().into();
            current["refresh"] = value.refresh.clone().into();
            current["expires"] = value.expires.into();
            Ok(())
        })?;
        if minimum.is_some() && expires_soon(&value, window) {
            return Err(error("refreshed token expires too soon"));
        }
        Ok(value.access)
    })
    .await
    .map_err(|_| error("refresh task failed"))?
}
pub(super) async fn access(
    settings: &super::config::Settings,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    let settings = settings.clone();
    access_at(
        super::config::agent_dir().join("auth.json"),
        move || Service::new(&settings),
        minimum,
        no_refresh,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn no_refresh_returns_cached_token_without_building_http_client() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, serde_json::json!({"anthropic":{"type":"oauth","access":"old","refresh":"refresh","expires":0}}).to_string()).unwrap();
        let token = access_at(
            path,
            || panic!("HTTP client should not be constructed"),
            None,
            true,
        )
        .await
        .unwrap();
        assert_eq!(token, "old");
    }

    #[tokio::test]
    async fn refresh_persists_rotated_credentials_before_minimum_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, serde_json::json!({"anthropic":{"type":"oauth","access":"old","refresh":"old-refresh","expires":0,"extra":"keep"},"other":{"key":"keep"}}).to_string()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            let body_start = loop {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buffer[..n]);
                if let Some(pos) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&request[..pos]);
                    let length: usize = header
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|n| n.parse().unwrap())
                        })
                        .unwrap();
                    if request.len() >= pos + 4 + length {
                        break pos + 4;
                    }
                }
            };
            assert!(
                String::from_utf8_lossy(&request[..body_start])
                    .starts_with("POST /v1/oauth/token ")
            );
            let sent: Value = serde_json::from_slice(&request[body_start..]).unwrap();
            assert_eq!(sent["grant_type"], "refresh_token");
            assert_eq!(sent["client_id"], CLIENT_ID);
            assert_eq!(sent["refresh_token"], "old-refresh");
            let body =
                r#"{"access_token":"sk-ant-oat-new","refresh_token":"rotated","expires_in":600}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let service = Service {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            url: reqwest::Url::parse(&format!("http://{address}/v1/oauth/token")).unwrap(),
        };
        let failure = access_at(
            path.clone(),
            move || Ok(service),
            Some(Duration::from_secs(1800)),
            false,
        )
        .await
        .unwrap_err();
        assert!(failure.to_string().contains("expires too soon"));
        server.await.unwrap();
        let stored = super::super::config::read_auth_object(&path).await.unwrap();
        assert_eq!(stored["anthropic"]["refresh"], "rotated");
        assert_eq!(stored["anthropic"]["extra"], "keep");
        assert_eq!(stored["other"]["key"], "keep");
    }
}
