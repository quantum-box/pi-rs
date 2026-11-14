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
        Self::with_proxies(settings, super::http_proxy::configured(settings)?)
    }
    fn with_proxies(
        settings: &super::config::Settings,
        proxies: Vec<reqwest::Proxy>,
    ) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30));
        for proxy in proxies {
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

const DEVICE_AUTH_URL: &str = "https://auth.meta.com/oidc/device/authorization/";
const DEVICE_TOKEN_URL: &str = "https://auth.meta.com/oidc/device/token/";
const CLIENT_ID: &str = "1031625952748946";
const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

#[derive(Clone)]
struct DeviceService {
    authorization: reqwest::Url,
    token: reqwest::Url,
    mint: Service,
}
impl DeviceService {
    fn new(settings: &super::config::Settings) -> Result<Self> {
        Ok(Self {
            authorization: reqwest::Url::parse(DEVICE_AUTH_URL).expect("constant URL"),
            token: reqwest::Url::parse(DEVICE_TOKEN_URL).expect("constant URL"),
            mint: Service::new(settings)?,
        })
    }
    async fn post(
        &self,
        url: &reqwest::Url,
        form: &[(&str, &str)],
    ) -> Result<(reqwest::StatusCode, Value)> {
        let mut encoded = reqwest::Url::parse("https://localhost/").unwrap();
        encoded.query_pairs_mut().extend_pairs(form.iter().copied());
        let mut response = self
            .mint
            .client
            .post(url.clone())
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(encoded.query().unwrap_or_default().to_owned())
            .send()
            .await
            .map_err(|_| error("device request failed"))?;
        let status = response.status();
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error("device response failed"))?
        {
            if body.len().saturating_add(chunk.len()) > 65536 {
                return Err(error("device response too large"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, serde_json::from_slice(&body).unwrap_or(Value::Null)))
    }
    async fn login(&self, announce: impl Fn(&reqwest::Url, &str)) -> Result<Credential> {
        let (status, device) = self
            .post(&self.authorization, &[("client_id", CLIENT_ID)])
            .await?;
        if !status.is_success() {
            return Err(error(&format!("device authorization failed ({status})")));
        }
        let device_code = device["device_code"]
            .as_str()
            .filter(|s| super::model_auth::valid_key(s))
            .ok_or_else(|| error("invalid device authorization response"))?;
        let user_code = device["user_code"]
            .as_str()
            .filter(|s| super::model_auth::valid_key(s))
            .ok_or_else(|| error("invalid device authorization response"))?;
        let verification = ["verification_uri_complete", "verification_uri"]
            .iter()
            .filter_map(|key| device[*key].as_str())
            .find_map(verification_url)
            .ok_or_else(|| error("invalid verification URL"))?;
        announce(&verification, user_code);
        let mut interval =
            poll_duration(device["interval"].as_f64(), 5.0).max(Duration::from_secs(1));
        let expires = device["expires_in"]
            .as_f64()
            .filter(|v| v.is_finite() && *v > 0.0)
            .map(|v| poll_duration(Some(v), 900.0));
        let deadline = expires.map(|duration| tokio::time::Instant::now() + duration);
        let identity = loop {
            let wait = match deadline {
                Some(deadline) => {
                    interval.min(deadline.saturating_duration_since(tokio::time::Instant::now()))
                }
                None => interval,
            };
            tokio::time::sleep(wait).await;
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                return Err(error("device authorization expired; restart login"));
            }
            let (status, json) = self
                .post(
                    &self.token,
                    &[
                        ("grant_type", DEVICE_GRANT),
                        ("device_code", device_code),
                        ("client_id", CLIENT_ID),
                    ],
                )
                .await?;
            if status.is_success()
                && let Some(identity) = json["access_token"]
                    .as_str()
                    .filter(|s| super::model_auth::valid_key(s))
            {
                break identity.to_owned();
            }
            match json["error"].as_str() {
                Some("authorization_pending") => {}
                Some("slow_down") => {
                    interval = json["interval"]
                        .as_f64()
                        .filter(|v| v.is_finite() && *v > 0.0)
                        .map(|v| poll_duration(Some(v), 5.0).max(Duration::from_secs(1)))
                        .unwrap_or(interval.saturating_add(Duration::from_secs(5)))
                }
                Some("access_denied") => return Err(error("login denied")),
                Some("expired_token") => {
                    return Err(error("device authorization expired; restart login"));
                }
                _ => return Err(error(&format!("device token request failed ({status})"))),
            }
        };
        let access = self.mint.mint(&identity).await?;
        Ok(Credential {
            kind: "oauth".into(),
            access,
            refresh: identity,
            expires: now().saturating_add(KEY_LIFETIME_MS),
        })
    }
}
fn poll_duration(value: Option<f64>, default: f64) -> Duration {
    Duration::from_secs_f64(
        value
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(default)
            .min(86400.0),
    )
}
fn verification_url(value: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(value).ok()?;
    (url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none())
    .then_some(url)
}

#[derive(Clone, Default)]
pub(super) struct LoginControl(std::sync::Arc<std::sync::atomic::AtomicU8>);
impl LoginControl {
    // Only one of cancellation and the start of the synchronous commit wins.
    pub(super) fn cancel_before_commit(&self) -> bool {
        self.0
            .compare_exchange(
                0,
                2,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
    }
    #[cfg(test)]
    pub(super) fn is_committing(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst) == 1
    }
    pub(super) fn begin_commit(&self) -> Result<()> {
        self.0
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|_| error("login cancelled"))
    }
}

pub(super) async fn save_login(
    path: &std::path::Path,
    previous: Option<Value>,
    credential: Credential,
    control: LoginControl,
) -> Result<()> {
    let guard = super::auth_lock::acquire_async(path).await?;
    // update_object_locked can block on the independent file lock. Once it
    // starts, the UI must retain the task and observe its actual result.
    control.begin_commit()?;
    super::config::update_object_locked(path, Some(&guard), |data| {
        if data.get("meta") != previous.as_ref() {
            return Err(error("credentials changed during login"));
        }
        data.insert("meta".into(), serde_json::to_value(&credential)?);
        Ok(())
    })
}

pub(super) async fn login(
    settings: &super::config::Settings,
    control: LoginControl,
    announce: impl Fn(&reqwest::Url, &str),
) -> Result<()> {
    let path = super::config::agent_dir().join("auth.json");
    let previous = {
        let guard = super::auth_lock::acquire_async(&path).await?;
        let value = super::config::read_object_unlocked(&path)?
            .get("meta")
            .cloned();
        guard.check()?;
        value
    };
    let credential = DeviceService::new(settings)?.login(announce).await?;
    save_login(&path, previous, credential, control).await
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
    let _cancellation = super::process::RefreshCancellation::new();
    let state = _cancellation.state();
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| error("cannot start refresh runtime"))?;
        let _barrier = super::process::RefreshCancellation::enter(&state)?;
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
        server_with_delay(status, body, Duration::ZERO).await
    }
    async fn server_with_delay(
        status: &'static str,
        body: &'static str,
        delay: Duration,
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
            tokio::time::sleep(delay).await;
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let mut service =
            Service::with_proxies(&super::super::config::Settings::default(), vec![]).unwrap();
        service.url = url;
        (service, task)
    }

    async fn device_login_fixture(delayed_response: usize, delay: Duration) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let responses = [
                (
                    "200 OK",
                    r#"{"device_code":"device-secret","user_code":"CODE","verification_uri":"https://auth.meta.com/device","verification_uri_complete":"https://auth.meta.com/device?code=CODE","interval":0.01,"expires_in":4}"#,
                ),
                ("400 Bad Request", r#"{"error":"authorization_pending"}"#),
                ("200 OK", r#"{"access_token":"identity-secret"}"#),
                ("200 OK", r#"{"api_key":"inference-key"}"#),
            ];
            let mut requests = Vec::new();
            for (index, (status, body)) in responses.into_iter().enumerate() {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buf = [0; 2048];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.split_once(':').and_then(|(key, value)| {
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                            })
                            .unwrap();
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                // Delay either the in-flight token response or the mint past expiry.
                if index == delayed_response {
                    tokio::time::sleep(delay).await;
                }
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        // Exercise the same timeout and redirect configuration used in production;
        // only destinations are replaced with the isolated loopback fixture.
        let mut mint =
            Service::with_proxies(&super::super::config::Settings::default(), vec![]).unwrap();
        mint.url = reqwest::Url::parse(&format!("{base}/muse-code/key")).unwrap();
        let service = DeviceService {
            authorization: reqwest::Url::parse(&format!("{base}/oidc/device/authorization/"))
                .unwrap(),
            token: reqwest::Url::parse(&format!("{base}/oidc/device/token/")).unwrap(),
            mint,
        };
        let credential = service
            .login(|url, code| {
                assert_eq!(url.as_str(), "https://auth.meta.com/device?code=CODE");
                assert_eq!(code, "CODE");
            })
            .await
            .unwrap();
        assert_eq!(credential.access, "inference-key");
        assert_eq!(credential.refresh, "identity-secret");
        let requests = task.await.unwrap();
        assert!(requests[0].starts_with("POST /oidc/device/authorization/ "));
        assert!(requests[0].contains("client_id=1031625952748946"));
        assert!(requests[1].starts_with("POST /oidc/device/token/ "));
        assert!(requests[1].contains("device_code=device-secret"));
        assert!(
            requests[2]
                .contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code")
        );
        assert!(
            requests[3]
                .to_ascii_lowercase()
                .contains("authorization: bearer identity-secret")
        );
        assert!(requests[3].ends_with("{}"));
    }
    #[tokio::test]
    async fn device_login_accepts_token_response_after_expiry() {
        device_login_fixture(2, Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn device_login_allows_mint_to_finish_after_expiry() {
        device_login_fixture(3, Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn device_poll_uses_production_30_second_request_timeout() {
        device_login_fixture(2, Duration::from_secs(16)).await;
    }

    #[tokio::test]
    async fn device_login_does_not_start_poll_after_expiry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 2048];
            assert!(socket.read(&mut buf).await.unwrap() > 0);
            let body = r#"{"device_code":"device","user_code":"CODE","verification_uri":"https://auth.meta.com/device","interval":1,"expires_in":0.05}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(300), listener.accept())
                    .await
                    .is_err()
            );
        });
        let service = DeviceService {
            authorization: reqwest::Url::parse(&format!("{base}/authorization")).unwrap(),
            token: reqwest::Url::parse(&format!("{base}/token")).unwrap(),
            mint: Service {
                url: reqwest::Url::parse(&format!("{base}/mint")).unwrap(),
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            },
        };
        let message = match service.login(|_, _| {}).await {
            Ok(_) => panic!("expired login succeeded"),
            Err(error) => error.to_string(),
        };
        assert!(message.contains("expired"));
        server.await.unwrap();
    }

    #[test]
    fn blocked_file_lock_keeps_commit_observable_after_escape() {
        use std::sync::atomic::Ordering;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        std::fs::write(&path, "{}").unwrap();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))
            .unwrap();
        lock.lock().unwrap();
        let control = LoginControl::default();
        let worker_control = control.clone();
        let worker_path = path.clone();
        let worker = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(save_login(
                &worker_path,
                None,
                Credential {
                    kind: "oauth".into(),
                    access: "key".into(),
                    refresh: "identity".into(),
                    expires: 1,
                },
                worker_control,
            ))
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while control.0.load(Ordering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline, "commit never started");
            std::thread::yield_now();
        }
        assert!(
            !control.cancel_before_commit(),
            "Escape must retain the committing task"
        );
        assert!(
            !worker.is_finished(),
            "the file lock must still block the write"
        );
        lock.unlock().unwrap();
        worker.join().unwrap().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["meta"]["access"], "key");
    }

    #[tokio::test]
    async fn cancellation_before_commit_never_writes_credentials() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        std::fs::write(&path, "{}").unwrap();
        let control = LoginControl::default();
        assert!(control.cancel_before_commit());
        let result = save_login(
            &path,
            None,
            Credential {
                kind: "oauth".into(),
                access: "key".into(),
                refresh: "identity".into(),
                expires: 1,
            },
            control,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "{}");
    }

    #[test]
    fn verification_urls_do_not_accept_unsafe_schemes_or_credentials() {
        assert!(verification_url("javascript:alert(1)").is_none());
        assert!(verification_url("http://127.0.0.1/").is_none());
        assert!(verification_url("https://user@auth.meta.com/").is_none());
        assert!(verification_url("https://auth.meta.com/device").is_some());
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
    async fn refresh_uses_production_30_second_request_timeout() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        fixture(&path, 1);
        let (service, server) = server_with_delay(
            "200 OK",
            r#"{"api_key":"minted-key"}"#,
            Duration::from_secs(16),
        )
        .await;
        assert_eq!(
            access_at(path.clone(), service, None, false).await.unwrap(),
            "minted-key"
        );
        let request = server.await.unwrap().to_ascii_lowercase();
        assert!(request.contains("authorization: bearer identity-secret"));
        let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["meta"]["access"], "minted-key");
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
