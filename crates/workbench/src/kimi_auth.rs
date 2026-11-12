//! Kimi Code device authorization and Pi-compatible credentials.
use pi_rs::{Error, Result};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CLIENT_ID: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
const GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Credential {
    pub access: String,
    pub refresh: String,
    pub expires: u64,
    #[serde(rename = "type")]
    pub kind: String,
}
#[derive(Deserialize)]
struct Token {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
}
#[derive(Deserialize)]
struct Device {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    interval: Option<f64>,
    expires_in: Option<f64>,
}
fn error(message: &str) -> Error {
    Error::Config(format!("Kimi Code OAuth: {message}"))
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn seconds(value: Option<f64>, default: f64) -> Duration {
    Duration::from_secs_f64(
        value
            .filter(|n| n.is_finite() && *n > 0.0)
            .unwrap_or(default)
            .min(86400.0),
    )
}
fn trusted(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| error("invalid URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(error("invalid URL"));
    }
    Ok(url)
}
fn host() -> Result<Url> {
    let value = std::env::var("KIMI_CODE_OAUTH_HOST")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("KIMI_OAUTH_HOST")
                .ok()
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "https://auth.kimi.com".into());
    let url = trusted(&value)?;
    if url.scheme() != "https"
        && !matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
    {
        return Err(error("OAuth host must use HTTPS or loopback HTTP"));
    }
    if url.query().is_some() {
        return Err(error("invalid OAuth host"));
    }
    Ok(url)
}
struct Service {
    host: Url,
    client: Client,
}
impl Service {
    fn new(settings: &super::config::Settings) -> Result<Self> {
        let mut builder = Client::builder()
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
            host: host()?,
            client: builder.build().map_err(|_| error("cannot create client"))?,
        })
    }
    async fn post(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<(reqwest::StatusCode, Value)> {
        let url = format!("{}/{}", self.host.as_str().trim_end_matches('/'), path);
        let mut encoded = Url::parse("https://localhost").unwrap();
        encoded.query_pairs_mut().extend_pairs(form.iter().copied());
        let mut response = self
            .client
            .post(url)
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(encoded.query().unwrap_or_default().to_owned())
            .send()
            .await
            .map_err(|_| error("request failed"))?;
        let status = response.status();
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error("request failed"))?
        {
            if body.len() + chunk.len() > 65536 {
                return Err(error("response too large"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, serde_json::from_slice(&body).unwrap_or(Value::Null)))
    }
    async fn login(&self, announce: impl Fn(&Url, &str)) -> Result<Credential> {
        let (status, json) = self
            .post(
                "api/oauth/device_authorization",
                &[("client_id", CLIENT_ID)],
            )
            .await?;
        if !status.is_success() {
            return Err(error(&format!("device authorization failed ({status})")));
        }
        let device: Device = serde_json::from_value(json)
            .map_err(|_| error("invalid device authorization response"))?;
        if !super::model_auth::valid_key(&device.device_code)
            || !super::model_auth::valid_key(&device.user_code)
        {
            return Err(error("invalid device code"));
        }
        trusted(&device.verification_uri)?;
        let verification = trusted(&device.verification_uri_complete)?;
        announce(&verification, &device.user_code);
        let mut interval = seconds(device.interval, 5.0).max(Duration::from_secs(1));
        let poll = async {
            loop {
                tokio::time::sleep(interval).await;
                let (status, json) = self
                    .post(
                        "api/oauth/token",
                        &[
                            ("client_id", CLIENT_ID),
                            ("device_code", &device.device_code),
                            ("grant_type", GRANT),
                        ],
                    )
                    .await?;
                if status.is_success() && json["access_token"].is_string() {
                    return credential(json);
                }
                match json["error"].as_str() {
                    Some("authorization_pending") => (),
                    Some("slow_down") => {
                        interval = json["interval"]
                            .as_f64()
                            .filter(|v| v.is_finite() && *v > 0.0)
                            .map(|v| seconds(Some(v), 5.0).max(Duration::from_secs(1)))
                            .unwrap_or(interval.saturating_add(Duration::from_secs(5)))
                    }
                    Some("expired_token") => return Err(error("device authorization expired")),
                    Some("access_denied") => return Err(error("authorization denied")),
                    _ => return Err(error(&format!("device token request failed ({status})"))),
                }
            }
        };
        tokio::time::timeout(seconds(device.expires_in, 900.0), poll)
            .await
            .map_err(|_| error("device authorization expired"))?
    }
    async fn refresh(&self, refresh: &str) -> Result<Credential> {
        for attempt in 0..4 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(1 << (attempt - 1))).await;
            }
            let response = self
                .post(
                    "api/oauth/token",
                    &[
                        ("client_id", CLIENT_ID),
                        ("grant_type", "refresh_token"),
                        ("refresh_token", refresh),
                    ],
                )
                .await;
            match response {
                Ok((status, json)) if status.is_success() => return credential(json),
                Ok((status, json))
                    if status == 401 || status == 403 || json["error"] == "invalid_grant" =>
                {
                    return Err(error("refresh unauthorized; sign in again"));
                }
                Ok((status, _)) if (status == 429 || status.is_server_error()) && attempt < 3 => (),
                Ok((status, _)) => return Err(error(&format!("refresh failed ({status})"))),
                Err(_) if attempt < 3 => (),
                Err(e) => return Err(e),
            }
        }
        Err(error("refresh failed"))
    }
}
fn credential(json: Value) -> Result<Credential> {
    let token: Token = serde_json::from_value(json).map_err(|_| error("invalid token response"))?;
    if !super::model_auth::valid_key(&token.access_token)
        || !super::model_auth::valid_key(&token.refresh_token)
        || token.expires_in == 0
    {
        return Err(error("invalid token response"));
    }
    Ok(Credential {
        kind: "oauth".into(),
        access: token.access_token,
        refresh: token.refresh_token,
        expires: now().saturating_add(token.expires_in.saturating_mul(1000)),
    })
}
fn load() -> Result<Option<Credential>> {
    let value = super::config::read_object(&super::config::agent_dir().join("auth.json"))?;
    let Some(entry) = value.get("kimi-coding").filter(|e| e["type"] == "oauth") else {
        return Ok(None);
    };
    let credential: Credential =
        serde_json::from_value(entry.clone()).map_err(|_| error("invalid saved credentials"))?;
    if !super::model_auth::valid_key(&credential.access)
        || !super::model_auth::valid_key(&credential.refresh)
    {
        return Err(error("invalid saved credentials"));
    }
    Ok(Some(credential))
}
fn save(value: &Credential) -> Result<()> {
    super::config::update_object(&super::config::agent_dir().join("auth.json"), |data| {
        data.insert("kimi-coding".into(), serde_json::to_value(value)?);
        Ok(())
    })
}
async fn lock() -> Result<std::fs::File> {
    let dir = super::config::agent_dir();
    std::fs::create_dir_all(&dir)?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join("kimi-oauth.lock"))?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(std::fs::TryLockError::Error(e)) => return Err(Error::Io(e)),
            }
        }
    })
    .await
    .map_err(|_| error("credential storage is busy"))?
}
pub(super) async fn login(
    settings: &super::config::Settings,
    announce: impl Fn(&Url, &str),
) -> Result<()> {
    let value = Service::new(settings)?.login(announce).await?;
    let _guard = lock().await?;
    save(&value)
}
pub(super) async fn logout() -> Result<bool> {
    let _guard = lock().await?;
    super::model_auth::remove(&super::Provider::named("kimi-coding").expect("Kimi provider"))
}
pub(super) async fn access(
    settings: &super::config::Settings,
    minimum: Duration,
    no_refresh: bool,
) -> Result<String> {
    let value = load()?.ok_or_else(|| error("credentials not configured"))?;
    if no_refresh
        || value.expires > now().saturating_add(minimum.as_millis().try_into().unwrap_or(u64::MAX))
    {
        return Ok(value.access);
    }
    let _guard = lock().await?;
    let value = load()?.ok_or_else(|| error("credentials not configured"))?;
    if value.expires > now().saturating_add(minimum.as_millis().try_into().unwrap_or(u64::MAX)) {
        return Ok(value.access);
    }
    let value = Service::new(settings)?.refresh(&value.refresh).await?;
    save(&value)?;
    Ok(value.access)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn device_poll_and_rotating_refresh_use_upstream_protocol() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let responses = [
                r#"{"device_code":"device","user_code":"CODE","verification_uri":"https://kimi.example/code","verification_uri_complete":"https://kimi.example/code?user_code=CODE","interval":0.01,"expires_in":10}"#,
                r#"{"error":"authorization_pending"}"#,
                r#"{"access_token":"first","refresh_token":"rotate-1","expires_in":3600}"#,
                r#"{"access_token":"second","refresh_token":"rotate-2","expires_in":3600}"#,
            ];
            let mut requests = Vec::new();
            for (index, body) in responses.iter().enumerate() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buf = [0u8; 4096];
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if bytes.len() >= end + 4 + len {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                let status = if index == 1 {
                    "400 Bad Request"
                } else {
                    "200 OK"
                };
                stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        let service = Service {
            host: Url::parse(&origin).unwrap(),
            client: Client::builder().no_proxy().build().unwrap(),
        };
        let token = service
            .login(|url, code| {
                assert_eq!(url.as_str(), "https://kimi.example/code?user_code=CODE");
                assert_eq!(code, "CODE");
            })
            .await
            .unwrap();
        assert_eq!(token.access, "first");
        let updated = service.refresh(&token.refresh).await.unwrap();
        assert_eq!(updated.refresh, "rotate-2");
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("POST /api/oauth/device_authorization "));
        assert!(requests[0].contains(&format!("client_id={CLIENT_ID}")));
        assert!(requests[1].contains("device_code=device"));
        assert!(
            requests[1]
                .contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code")
        );
        assert!(requests[3].contains("grant_type=refresh_token"));
        assert!(requests[3].contains("refresh_token=rotate-1"));
        let json = serde_json::to_value(updated).unwrap();
        assert_eq!(json["type"], "oauth");
        assert_eq!(json["access"], "second");
        assert_eq!(json["refresh"], "rotate-2");
        assert!(json["expires"].as_u64().unwrap() > now());
    }
    #[test]
    fn rejects_invalid_tokens_and_urls() {
        assert!(credential(serde_json::json!({"access_token":"a","expires_in":5})).is_err());
        assert!(
            credential(serde_json::json!({"access_token":"a","refresh_token":"b","expires_in":0}))
                .is_err()
        );
        assert!(trusted("javascript:alert(1)").is_err());
    }
}
