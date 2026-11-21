//! Refresh Pi-compatible Anthropic subscription credentials.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pi_rs::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{net::TcpListener, sync::oneshot};

const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const AUTHORIZE_URL: &str = "https://claude.ai/oauth/authorize";
const COPY_REDIRECT: &str = "https://platform.claude.com/oauth/code/callback";
const SCOPES: &str = "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload";

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
        self.token(serde_json::json!({"grant_type":"refresh_token","client_id":CLIENT_ID,"refresh_token":previous.refresh})).await
    }
    async fn token(&self, request: Value) -> Result<Credential> {
        let mut response = self
            .client
            .post(self.url.clone())
            .json(&request)
            .send()
            .await
            .map_err(|_| error("token request failed"))?;
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

#[derive(Clone, Copy)]
pub(super) enum LoginMethod {
    Browser,
    CopyCode,
}
enum LoginInput {
    Callback(pi_rs_workbench::oauth_callback::Callback),
    Manual(String),
}

fn login_url(redirect: &str, verifier: &str, state: &str) -> reqwest::Url {
    let mut url = reqwest::Url::parse(AUTHORIZE_URL).expect("fixed URL");
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    url.query_pairs_mut().extend_pairs([
        ("code", "true"),
        ("client_id", CLIENT_ID),
        ("response_type", "code"),
        ("redirect_uri", redirect),
        ("scope", SCOPES),
        ("code_challenge", &challenge),
        ("code_challenge_method", "S256"),
        ("state", state),
    ]);
    url
}

fn manual_code(input: &str, state: &str) -> Result<String> {
    let input = input.trim();
    let (code, returned_state) = if let Ok(url) = reqwest::Url::parse(input) {
        (
            url.query_pairs()
                .find_map(|(k, v)| (k == "code").then(|| v.into_owned())),
            url.query_pairs()
                .find_map(|(k, v)| (k == "state").then(|| v.into_owned())),
        )
    } else if input.contains('#') {
        let (code, state) = input.split_once('#').expect("checked");
        (Some(code.into()), Some(state.into()))
    } else if input.contains("code=") {
        let url = reqwest::Url::parse(&format!(
            "http://localhost/?{}",
            input.trim_start_matches('?')
        ))
        .map_err(|_| error("invalid authorization code"))?;
        (
            url.query_pairs()
                .find_map(|(k, v)| (k == "code").then(|| v.into_owned())),
            url.query_pairs()
                .find_map(|(k, v)| (k == "state").then(|| v.into_owned())),
        )
    } else {
        (Some(input.into()), None)
    };
    if returned_state
        .as_deref()
        .is_some_and(|value| value != state)
    {
        return Err(error("authorization state mismatch"));
    }
    code.filter(|value| super::model_auth::valid_key(value))
        .ok_or_else(|| error("invalid authorization code"))
}

pub(super) async fn login(
    settings: &super::config::Settings,
    method: LoginMethod,
    control: super::meta_auth::LoginControl,
    announce: impl FnOnce(&reqwest::Url, oneshot::Sender<String>),
) -> Result<()> {
    let listener = bind_listener(method).await;
    login_at(
        super::config::agent_dir().join("auth.json"),
        Service::new(settings)?,
        method,
        control,
        announce,
        true,
        listener,
    )
    .await
}

async fn bind_listener(method: LoginMethod) -> Option<TcpListener> {
    if !matches!(method, LoginMethod::Browser) {
        return None;
    }
    match TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 53692)).await {
        Ok(listener) => Some(listener),
        Err(_) => TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .ok(),
    }
}

async fn login_at(
    path: std::path::PathBuf,
    service: Service,
    method: LoginMethod,
    control: super::meta_auth::LoginControl,
    announce: impl FnOnce(&reqwest::Url, oneshot::Sender<String>),
    open_browser: bool,
    listener: Option<TcpListener>,
) -> Result<()> {
    let previous = super::config::read_auth_object(&path)
        .await?
        .get("anthropic")
        .cloned();
    let redirect = match &listener {
        Some(listener) => format!(
            "http://127.0.0.1:{}/callback",
            listener.local_addr()?.port()
        ),
        None if matches!(method, LoginMethod::Browser) => "http://127.0.0.1:53692/callback".into(),
        None => COPY_REDIRECT.into(),
    };
    let verifier = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let state = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let url = login_url(&redirect, &verifier, &state);
    let (manual, input) = oneshot::channel();
    announce(&url, manual);
    if open_browser {
        let _ = pi_rs_workbench::oauth_callback::open_browser(url.as_str()).await;
    }
    let callback = reqwest::Url::parse(&redirect).map_err(|_| error("invalid callback"))?;
    let incoming = tokio::time::timeout(Duration::from_secs(300), async {
        if let Some(listener) = &listener {
            tokio::select! {
                result = pi_rs_workbench::oauth_callback::receive(listener, &callback, &state) => result.map(LoginInput::Callback),
                result = input => match result {
                    Ok(value) => Ok(LoginInput::Manual(value)),
                    Err(_) => pi_rs_workbench::oauth_callback::receive(listener, &callback, &state).await.map(LoginInput::Callback),
                },
            }
        } else { input.await.map(LoginInput::Manual).map_err(|_| error("login cancelled")) }
    }).await.map_err(|_| error("login timed out"))??;
    let result = async {
        let code = match &incoming {
            LoginInput::Callback(callback) => callback
                .code()
                .filter(|code| super::model_auth::valid_key(code))
                .ok_or_else(|| error("invalid authorization code"))?,
            LoginInput::Manual(input) => manual_code(input, &state)?,
        };
        let credential = service
            .token(serde_json::json!({
                "grant_type":"authorization_code", "client_id":CLIENT_ID,
                "code":code, "state":state, "redirect_uri":redirect, "code_verifier":verifier,
            }))
            .await?;
        let guard = super::auth_lock::acquire_async(&path).await?;
        control.begin_commit()?;
        super::config::update_object_locked(&path, Some(&guard), |data| {
            if data.get("anthropic") != previous.as_ref() {
                return Err(error("credentials changed during login"));
            }
            data.insert("anthropic".into(), serde_json::to_value(&credential)?);
            Ok(())
        })
    }
    .await;
    if let LoginInput::Callback(callback) = incoming {
        callback.reply(result.is_ok()).await;
    }
    result
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
    super::model_auth::await_admitted_oauth_refresh("anthropic", move |admitted| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| error("cannot start refresh runtime"))?;
        let service = service()?;
        let barrier = super::process::refresh_barrier()?;
        let guard = super::process::guard_refresh_auth(barrier, guard);
        let _ = admitted.send(());
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
    async fn share_barrier_refresh_fixture() {
        let Some(path) = std::env::var_os("PI_RS_TEST_SHARE_BARRIER_AUTH") else {
            return;
        };
        let url = std::env::var("PI_RS_TEST_SHARE_BARRIER_URL").unwrap();
        let service = Service {
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            url: reqwest::Url::parse(&url).unwrap(),
        };
        access_at(path.into(), move || Ok(service), None, false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    async fn token_request(socket: &mut tokio::net::TcpStream) -> Value {
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        loop {
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
                    return serde_json::from_slice(&request[pos + 4..pos + 4 + length]).unwrap();
                }
            }
        }
    }

    #[test]
    fn login_url_and_manual_state_follow_pkce_protocol() {
        let url = login_url(COPY_REDIRECT, "verifier", "state");
        let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["client_id"], CLIENT_ID);
        assert_eq!(params["state"], "state");
        assert_eq!(params["redirect_uri"], COPY_REDIRECT);
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(manual_code("code#verifier", "verifier").unwrap(), "code");
        assert_eq!(
            manual_code("https://example.test/?code=code&state=verifier", "verifier").unwrap(),
            "code"
        );
        assert!(manual_code("code#wrong", "verifier").is_err());
    }

    #[tokio::test]
    async fn browser_login_rejects_wrong_state_before_valid_callback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let sent = token_request(&mut socket).await;
            assert_eq!(sent["grant_type"], "authorization_code");
            assert_eq!(sent["code"], "test-code");
            assert_eq!(sent["client_id"], CLIENT_ID);
            assert_ne!(sent["state"], sent["code_verifier"]);
            assert!(
                sent["redirect_uri"]
                    .as_str()
                    .unwrap()
                    .starts_with("http://127.0.0.1:")
            );
            let body =
                r#"{"access_token":"sk-ant-oat-new","refresh_token":"rotated","expires_in":3600}"#;
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
            url: reqwest::Url::parse(&format!("http://{address}/token")).unwrap(),
        };
        let mut callback_task = None;
        let callback_listener = bind_listener(LoginMethod::Browser).await.unwrap();
        let callback_port = callback_listener.local_addr().unwrap().port();
        let ipv6_competitor = TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, callback_port))
            .await
            .ok();
        login_at(path.clone(), service, LoginMethod::Browser, super::super::meta_auth::LoginControl::default(), |url, manual| {
            let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
            let redirect = reqwest::Url::parse(&params["redirect_uri"]).unwrap();
            assert_eq!(redirect.host_str(), Some("127.0.0.1"));
            let port = redirect.port().unwrap();
            let state = params["state"].clone();
            callback_task = Some(tokio::spawn(async move {
                let _keep_manual_open = manual;
                for (state, expected) in [("wrong".to_owned(), "400"), (state, "200")] {
                    let mut socket = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await.unwrap();
                    socket.write_all(format!("GET /callback?state={state}&code=test-code HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes()).await.unwrap();
                    let mut reply = Vec::new();
                    socket.read_to_end(&mut reply).await.unwrap();
                    assert!(String::from_utf8_lossy(&reply).starts_with(&format!("HTTP/1.1 {expected}")));
                }
            }));
        }, false, Some(callback_listener)).await.unwrap();
        callback_task.unwrap().await.unwrap();
        if let Some(listener) = ipv6_competitor {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        }
        server.await.unwrap();
        let stored = super::super::config::read_auth_object(&path).await.unwrap();
        assert_eq!(stored["anthropic"]["refresh"], "rotated");
    }

    #[tokio::test]
    async fn manual_login_persists_token_and_rejects_concurrent_credential_change() {
        for (method, change, cancel) in [
            (LoginMethod::CopyCode, false, false),
            (LoginMethod::CopyCode, true, false),
            (LoginMethod::Browser, false, false),
            (LoginMethod::CopyCode, false, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("auth.json");
            std::fs::write(&path, r#"{"other":{"key":"keep"}}"#).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let sent = token_request(&mut socket).await;
                assert_eq!(sent["grant_type"], "authorization_code");
                assert_eq!(sent["code"], "code");
                assert_eq!(
                    sent["redirect_uri"],
                    if matches!(method, LoginMethod::CopyCode) {
                        COPY_REDIRECT
                    } else {
                        "http://127.0.0.1:53692/callback"
                    }
                );
                assert_ne!(sent["state"], sent["code_verifier"]);
                let body = r#"{"access_token":"sk-ant-oat-new","refresh_token":"rotated","expires_in":3600}"#;
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            let service = Service {
                client: reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .unwrap(),
                url: reqwest::Url::parse(&format!("http://{address}/token")).unwrap(),
            };
            let path_for_callback = path.clone();
            let control = super::super::meta_auth::LoginControl::default();
            let callback_control = control.clone();
            let result = login_at(path.clone(), service, method, control, move |url, sender| {
                let state = url.query_pairs().find_map(|(key,value)| (key == "state").then(|| value.into_owned())).unwrap();
                if change { std::fs::write(&path_for_callback, r#"{"anthropic":{"type":"api_key","key":"new"},"other":{"key":"keep"}}"#).unwrap(); }
                if cancel { assert!(callback_control.cancel_before_commit()); }
                sender.send(format!("code#{state}")).unwrap();
            }, false, None).await;
            server.await.unwrap();
            let stored = super::super::config::read_auth_object(&path).await.unwrap();
            assert_eq!(stored["other"]["key"], "keep");
            if cancel {
                assert!(result.is_err());
                assert!(stored.get("anthropic").is_none());
            } else if change {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("credentials changed")
                );
                assert_eq!(stored["anthropic"]["key"], "new");
            } else {
                result.unwrap();
                assert_eq!(stored["anthropic"]["refresh"], "rotated");
            }
        }
    }

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
