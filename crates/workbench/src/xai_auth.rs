//! xAI device authorization and Pi-compatible credentials.
use pi_rs::{Error, Result};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
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
struct Device {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Value,
    #[serde(default)]
    interval: Value,
    expires_in: f64,
}
fn error(message: &str) -> Error {
    Error::Config(format!("xAI OAuth: {message}"))
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
    if url.scheme() != "https"
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(error("invalid URL"));
    }
    Ok(url)
}
#[derive(Clone)]
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
            host: Url::parse("https://auth.x.ai/").expect("constant xAI OAuth host"),
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
                "oauth2/device/code",
                &[
                    ("client_id", CLIENT_ID),
                    (
                        "scope",
                        "openid profile email offline_access grok-cli:access api:access",
                    ),
                    ("referrer", "pi"),
                ],
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
        let verification = match device
            .verification_uri_complete
            .as_str()
            .filter(|s| !s.is_empty())
        {
            Some(url) => trusted(url)?,
            None => trusted(&device.verification_uri)?,
        };
        if !device.expires_in.is_finite() || device.expires_in <= 0.0 {
            return Err(error("invalid device expiry"));
        }
        announce(&verification, &device.user_code);
        let mut interval = seconds(device.interval.as_f64(), 5.0).max(Duration::from_secs(1));
        let deadline = tokio::time::Instant::now() + seconds(Some(device.expires_in), 900.0);
        loop {
            tokio::time::sleep(
                interval.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            )
            .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(error("device authorization expired"));
            }
            let (status, json) = self
                .post(
                    "oauth2/token",
                    &[
                        ("client_id", CLIENT_ID),
                        ("device_code", &device.device_code),
                        ("grant_type", GRANT),
                    ],
                )
                .await?;
            if status.is_success() {
                return credential(json, None);
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
                Some("access_denied" | "authorization_denied") => {
                    return Err(error("authorization denied"));
                }
                _ => return Err(error(&format!("device token request failed ({status})"))),
            }
        }
    }
    async fn refresh(&self, refresh: &str) -> Result<Credential> {
        let (status, json) = self
            .post(
                "oauth2/token",
                &[
                    ("grant_type", "refresh_token"),
                    ("client_id", CLIENT_ID),
                    ("refresh_token", refresh),
                ],
            )
            .await?;
        if !status.is_success() {
            return Err(error(&format!("refresh failed ({status})")));
        }
        credential(json, Some(refresh))
    }
}
fn credential(json: Value, previous: Option<&str>) -> Result<Credential> {
    let access = json["access_token"]
        .as_str()
        .filter(|v| super::model_auth::valid_key(v))
        .ok_or_else(|| error("invalid token response"))?;
    let refresh = match json.get("refresh_token") {
        None => previous,
        Some(value) => value.as_str(),
    }
    .filter(|v| super::model_auth::valid_key(v))
    .ok_or_else(|| error("invalid token response"))?;
    let seconds = match json.get("expires_in") {
        None => 3600.0,
        Some(value) => value
            .as_f64()
            .filter(|n| n.is_finite() && *n > 0.0)
            .ok_or_else(|| error("invalid token expiry"))?,
    };
    let lifetime = (seconds * 1000.0).min(u64::MAX as f64) as u64;
    Ok(Credential {
        kind: "oauth".into(),
        access: access.into(),
        refresh: refresh.into(),
        expires: now().saturating_add(lifetime).saturating_sub(300_000),
    })
}

fn auth_path() -> std::path::PathBuf {
    super::config::agent_dir().join("auth.json")
}
fn parse_credential(value: &Value) -> Result<Option<Credential>> {
    let Some(entry) = value.get("xai").filter(|e| e["type"] == "oauth") else {
        return Ok(None);
    };
    if entry
        .get("endpoint")
        .is_some_and(|v| v.as_str() != Some("https://api.x.ai/v1/responses"))
    {
        return Err(error(
            "credentials are not bound to the official xAI endpoint",
        ));
    }
    let credential: Credential =
        serde_json::from_value(entry.clone()).map_err(|_| error("invalid saved credentials"))?;
    if !super::model_auth::valid_key(&credential.access)
        || !super::model_auth::valid_key(&credential.refresh)
    {
        return Err(error("invalid saved credentials"));
    }
    Ok(Some(credential))
}
pub(super) async fn login(
    settings: &super::config::Settings,
    control: super::meta_auth::LoginControl,
    announce: impl Fn(&Url, &str),
) -> Result<()> {
    let path = auth_path();
    let previous = {
        let guard = super::auth_lock::acquire_async(&path).await?;
        let value = super::config::read_object_unlocked(&path)?
            .get("xai")
            .cloned();
        guard.check()?;
        value
    };
    let value = Service::new(settings)?.login(announce).await?;
    let guard = super::auth_lock::acquire_async(&path).await?;
    control.begin_commit()?;
    super::config::update_object_locked(&path, Some(&guard), |data| {
        if data.get("xai") != previous.as_ref() {
            return Err(error("credentials changed during login"));
        }
        data.insert("xai".into(), serde_json::to_value(value)?);
        Ok(())
    })
}
pub(super) async fn logout() -> Result<bool> {
    let path = auth_path();
    let guard = super::auth_lock::acquire_async(&path).await?;
    let mut removed = false;
    super::config::update_object_locked(&path, Some(&guard), |data| {
        if data
            .get("xai")
            .is_some_and(|v| v["type"] == "oauth" || v["type"] == "api_key")
        {
            data.remove("xai");
            removed = true;
        }
        Ok(())
    })?;
    Ok(removed)
}
fn expires_soon(value: &Credential, minimum: Duration) -> bool {
    value.expires <= now().saturating_add(minimum.as_millis().try_into().unwrap_or(u64::MAX))
}
async fn access_at(
    path: std::path::PathBuf,
    service: Service,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    access_at_inner(
        path,
        service,
        minimum,
        no_refresh,
        #[cfg(test)]
        None,
    )
    .await
}

async fn access_at_inner(
    path: std::path::PathBuf,
    service: Service,
    minimum: Option<Duration>,
    no_refresh: bool,
    #[cfg(test)] observer: Option<
        tokio::sync::mpsc::UnboundedSender<(&'static str, Option<String>)>,
    >,
) -> Result<String> {
    let refresh_window = minimum.unwrap_or_default().max(Duration::from_secs(300));
    let guard = match super::auth_lock::acquire_async(&path).await {
        Ok(guard) => guard,
        Err(err) => {
            let stored = super::config::read_readonly_auth(&path, err)?;
            let value =
                parse_credential(&stored)?.ok_or_else(|| error("credentials not configured"))?;
            if no_refresh || !expires_soon(&value, refresh_window) {
                return Ok(value.access);
            }
            return Err(error("cannot refresh credentials in a read-only store"));
        }
    };
    let stored = super::config::read_object_unlocked(&path)?;
    let value = parse_credential(&stored)?.ok_or_else(|| error("credentials not configured"))?;
    let previous = stored["xai"].clone();
    guard.check()?;
    if no_refresh || !expires_soon(&value, refresh_window) {
        return Ok(value.access);
    }
    // Tokio waits for started blocking tasks on runtime shutdown. The separate
    // runtime keeps the exchange alive even when the caller's runtime stops.
    tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        let report = |stage, error| {
            if let Some(observer) = &observer {
                let _ = observer.send((stage, error));
            }
        };
        #[cfg(test)]
        report("runtime", None);
        let result = (|| -> Result<String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| error("cannot start refresh runtime"))?;
            let _barrier = super::process::refresh_barrier()?;
            #[cfg(test)]
            report("exchange", None);
            let value = runtime.block_on(service.refresh(&value.refresh))?;
            #[cfg(test)]
            report("save", None);
            super::config::update_object_locked(&path, Some(&guard), |data| {
                let current = data
                    .get_mut("xai")
                    .ok_or_else(|| error("credentials changed during refresh"))?;
                if *current != previous {
                    return Err(error("credentials changed during refresh"));
                }
                current["access"] = value.access.clone().into();
                current["refresh"] = value.refresh.clone().into();
                current["expires"] = value.expires.into();
                Ok(())
            })?;
            // Preserve a successfully rotated credential even if its lifetime
            // does not satisfy the caller's explicit minimum.
            if minimum.is_some() && expires_soon(&value, refresh_window) {
                return Err(error("refreshed token expires too soon"));
            }
            Ok(value.access)
        })();
        #[cfg(test)]
        report("finished", result.as_ref().err().map(ToString::to_string));
        result
    })
    .await
    .map_err(|_| error("refresh task failed"))?
}
pub(super) async fn access(
    settings: &super::config::Settings,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    access_at(auth_path(), Service::new(settings)?, minimum, no_refresh).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn device_login_and_refresh_follow_xai_protocol() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let responses = [
                (
                    "200 OK",
                    r#"{"device_code":"device","user_code":"CODE","verification_uri":"https://auth.x.ai/activate","interval":0.1,"expires_in":10}"#,
                ),
                ("400 Bad Request", r#"{"error":"authorization_pending"}"#),
                (
                    "200 OK",
                    r#"{"access_token":"access-1","refresh_token":"refresh-1","expires_in":3600}"#,
                ),
                (
                    "200 OK",
                    r#"{"access_token":"access-2","refresh_token":"refresh-2","expires_in":3600}"#,
                ),
            ];
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 2048];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert_ne!(n, 0);
                    request.extend_from_slice(&bytes[..n]);
                    if let Some(end) = request.windows(4).position(|x| x == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..end]);
                        let len = headers
                            .lines()
                            .find_map(|line| {
                                line.split_once(':').and_then(|(key, value)| {
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                            })
                            .unwrap();
                        if request.len() >= end + 4 + len {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(request).unwrap());
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        let service = Service {
            host: Url::parse(&origin).unwrap(),
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        };
        let value = service
            .login(|url, code| {
                assert_eq!(url.as_str(), "https://auth.x.ai/activate");
                assert_eq!(code, "CODE");
            })
            .await
            .unwrap();
        assert_eq!(value.access, "access-1");
        assert_eq!(value.refresh, "refresh-1");
        assert!(value.expires >= now() + 3_290_000 && value.expires <= now() + 3_300_000);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        std::fs::write(&path, serde_json::json!({"xai":{"type":"oauth","access":"access-1","refresh":"refresh-1","expires":now() + 299_000,"extra":"preserve"},"other":{"key":"keep"}}).to_string()).unwrap();
        assert_eq!(
            access_at(path.clone(), service, None, false).await.unwrap(),
            "access-2"
        );
        let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["xai"]["refresh"], "refresh-2");
        assert_eq!(saved["xai"]["extra"], "preserve");
        assert_eq!(saved["other"]["key"], "keep");
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("POST /oauth2/device/code "));
        assert!(requests[0].contains("referrer=pi"));
        assert!(requests[0].contains("grok-cli%3Aaccess"));
        assert!(requests[1].contains("device_code=device"));
        assert!(requests[3].contains("grant_type=refresh_token"));
        assert!(requests[3].contains("refresh_token=refresh-1"));
    }
    #[tokio::test]
    async fn successful_http_response_without_token_fails_without_polling_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for body in [
                r#"{"device_code":"device","user_code":"CODE","verification_uri":"https://auth.x.ai/activate","interval":1,"expires_in":10}"#,
                r#"{"error":"authorization_pending"}"#,
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 2048];
                assert!(socket.read(&mut bytes).await.unwrap() > 0);
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(300), listener.accept())
                    .await
                    .is_err()
            );
        });
        let service = Service {
            host: Url::parse(&origin).unwrap(),
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        };
        let result = service.login(|_, _| {}).await;
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("invalid token response")
        );
        server.await.unwrap();
    }

    async fn cancelled_refresh_case(success: bool) {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut request_line = String::new();
            socket.read_line(&mut request_line).await.unwrap();
            assert!(request_line.starts_with("POST /oauth2/token "));
            let mut length = None;
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                assert!(!line.is_empty(), "request ended before headers");
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let length = length.expect("refresh request has content length");
            assert!(length <= 4096);
            let mut request_body = vec![0; length];
            socket.read_exact(&mut request_body).await.unwrap();
            started_tx.send(()).unwrap();
            reply_rx.await.unwrap();
            let (status, body) = if success {
                (
                    "200 OK",
                    r#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}"#,
                )
            } else {
                ("400 Bad Request", r#"{"error":"refresh-secret"}"#)
            };
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        std::fs::write(&path, serde_json::json!({"xai":{"type":"oauth","access":"old","refresh":"old-refresh","expires":1}}).to_string()).unwrap();
        let service = Service {
            host: Url::parse(&origin).unwrap(),
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        };
        let (observer, mut events) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(access_at_inner(
            path.clone(),
            service,
            None,
            false,
            Some(observer),
        ));
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("refresh request did not arrive")
            .unwrap();
        task.abort();
        reply_tx.send(()).unwrap();
        server.await.unwrap();
        let mut stages = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let observed = loop {
            let event = tokio::time::timeout_at(deadline, events.recv())
                .await
                .unwrap_or_else(|_| panic!("refresh worker did not finish; stages: {stages:?}"))
                .unwrap_or_else(|| {
                    panic!("refresh worker observation channel closed; stages: {stages:?}")
                });
            stages.push(event.0);
            if event.0 == "finished" {
                break event.1;
            }
        };
        assert!(stages.contains(&"exchange"));
        if success {
            assert!(stages.contains(&"save"));
            assert_eq!(observed, None);
        } else {
            assert!(!stages.contains(&"save"));
            let error = observed.expect("failed exchange must be observable");
            assert!(error.contains("refresh failed"));
            assert!(!error.contains("refresh-secret"));
        }
        let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            saved["xai"]["refresh"],
            if success {
                "new-refresh"
            } else {
                "old-refresh"
            }
        );
    }

    #[tokio::test]
    async fn cancelled_caller_still_saves_rotated_refresh_token() {
        cancelled_refresh_case(true).await;
    }

    #[tokio::test]
    async fn cancelled_caller_observes_refresh_worker_failure() {
        cancelled_refresh_case(false).await;
    }

    #[tokio::test]
    async fn rejected_refresh_neither_retries_nor_changes_saved_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 2048];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            let body = r#"{"error":"refresh-secret"}"#;
            socket.write_all(format!("HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(300), listener.accept())
                    .await
                    .is_err()
            );
        });
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        let before = serde_json::json!({"xai":{"type":"oauth","access":"old-access","refresh":"refresh-secret","expires":1}}).to_string();
        std::fs::write(&path, &before).unwrap();
        let service = Service {
            host: Url::parse(&origin).unwrap(),
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        };
        let err = access_at(path.clone(), service, None, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.contains("refresh-secret"));
        assert_eq!(std::fs::read_to_string(path).unwrap(), before);
        server.await.unwrap();
    }

    #[test]
    fn omitted_refresh_token_retains_previous_and_invalid_data_fails_closed() {
        let value = credential(serde_json::json!({"access_token":"new"}), Some("old")).unwrap();
        assert_eq!(value.refresh, "old");
        assert!(credential(serde_json::json!({"access_token":"new"}), None).is_err());
        assert!(
            credential(
                serde_json::json!({"access_token":"new","refresh_token":null}),
                Some("old")
            )
            .is_err()
        );
        assert!(
            credential(
                serde_json::json!({"access_token":"new","refresh_token":"r","expires_in":0}),
                None
            )
            .is_err()
        );
        let device: Device = serde_json::from_value(serde_json::json!({"device_code":"d","user_code":"c","verification_uri":"https://auth.x.ai/","interval":"invalid","verification_uri_complete":false,"expires_in":60})).unwrap();
        assert_eq!(
            seconds(device.interval.as_f64(), 5.0),
            Duration::from_secs(5)
        );
        assert!(device.verification_uri_complete.as_str().is_none());
        assert!(trusted("http://127.0.0.1/").is_err());
        assert!(trusted("https://user@auth.x.ai/").is_err());
        let wrong = serde_json::json!({"xai":{"type":"oauth","access":"a","refresh":"r","expires":1,"endpoint":"https://attacker.example/"}});
        assert!(parse_credential(&wrong).is_err());
    }
}
