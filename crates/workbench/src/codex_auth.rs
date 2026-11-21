//! Pi-compatible saved ChatGPT Codex credentials.
use base64::Engine;
use pi_rs::{Error, Result};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PROVIDER: &str = "openai-codex";
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const TOKEN_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const USER_CODE_ENDPOINT: &str = "https://auth.openai.com/api/accounts/deviceauth/usercode";
const DEVICE_TOKEN_ENDPOINT: &str = "https://auth.openai.com/api/accounts/deviceauth/token";
const DEVICE_VERIFICATION: &str = "https://auth.openai.com/codex/device";
const DEVICE_REDIRECT: &str = "https://auth.openai.com/deviceauth/callback";
const BROWSER_AUTHORIZE: &str = "https://auth.openai.com/oauth/authorize";
const BROWSER_REDIRECT: &str = "http://localhost:1455/auth/callback";

#[derive(Clone, Copy)]
pub(super) enum Method {
    Browser,
    Device,
}

pub(super) enum Notice {
    Browser(Url, tokio::sync::oneshot::Sender<String>),
    Device { url: &'static str, code: String },
}

#[derive(Deserialize)]
struct Credential {
    access: String,
    refresh: String,
    expires: u64,
}

fn error(message: &str) -> Error {
    Error::Config(format!("OpenAI Codex OAuth: {message}"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn read_entry(value: &Value) -> Result<Credential> {
    let credential: Credential =
        serde_json::from_value(value.clone()).map_err(|_| error("invalid saved credential"))?;
    if !super::model_auth::valid_key(&credential.access)
        || !super::model_auth::valid_key(&credential.refresh)
        || credential.expires == 0
        || pi_rs::openai::codex_account_id(&credential.access).is_none()
    {
        return Err(error("invalid saved credential"));
    }
    Ok(credential)
}

pub(super) fn valid_entry(value: &Value) -> bool {
    read_entry(value).is_ok()
}

async fn read_json(mut response: reqwest::Response) -> Result<Value> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| error("invalid OAuth response"))?
    {
        if body.len() + chunk.len() > 128 * 1024 {
            return Err(error("invalid OAuth response"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| error("invalid OAuth response"))
}

fn tokens(value: &Value) -> Result<Credential> {
    let field = |name: &str| {
        value[name]
            .as_str()
            .filter(|token| super::model_auth::valid_key(token))
            .map(str::to_owned)
            .ok_or_else(|| error("missing token in OAuth response"))
    };
    let access = field("access_token")?;
    let refresh = field("refresh_token")?;
    let expires = value["expires_in"]
        .as_u64()
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| seconds.checked_mul(1000))
        .and_then(|millis| now_ms().checked_add(millis))
        .ok_or_else(|| error("invalid token expiry"))?;
    if pi_rs::openai::codex_account_id(&access).is_none() {
        return Err(error("access token lacks an account ID"));
    }
    Ok(Credential {
        access,
        refresh,
        expires,
    })
}

pub(super) async fn login(
    settings: &super::config::Settings,
    control: super::meta_auth::LoginControl,
    method: Method,
    announce: impl Fn(Notice),
) -> Result<()> {
    let path = super::config::agent_dir().join("auth.json");
    let previous = super::config::read_auth_object(&path)
        .await?
        .get(PROVIDER)
        .cloned();
    let client = http_client(settings)?;
    let (credential, callback) = match method {
        Method::Browser => browser_login(&client, &announce).await?,
        Method::Device => (
            device_login(
                &client,
                USER_CODE_ENDPOINT,
                DEVICE_TOKEN_ENDPOINT,
                TOKEN_ENDPOINT,
                |url, code| {
                    announce(Notice::Device {
                        url,
                        code: code.to_owned(),
                    })
                },
            )
            .await?,
            None,
        ),
    };
    let result = async {
        let account = pi_rs::openai::codex_account_id(&credential.access)
            .ok_or_else(|| error("access token lacks an account ID"))?;
        let guard = super::auth_lock::acquire_async(&path).await?;
        control.begin_commit()?;
        super::config::update_object_locked(&path, Some(&guard), |data| {
            if data.get(PROVIDER) != previous.as_ref() {
                return Err(error("credentials changed during sign-in"));
            }
            data.insert(
                PROVIDER.into(),
                json!({
                    "type":"oauth","access":credential.access,"refresh":credential.refresh,
                    "expires":credential.expires,"accountId":account,
                }),
            );
            Ok(())
        })
    }
    .await;
    if let Some(callback) = callback {
        callback.reply(result.is_ok()).await;
    }
    result
}

fn manual_code(input: &str, expected_state: &str) -> Option<String> {
    let content = input.trim();
    let query = |pairs: Vec<(String, String)>| {
        let mut code = None;
        let mut state = None;
        for (key, value) in pairs {
            match key.as_str() {
                "code" if code.is_none() => code = Some(value),
                "state" if state.is_none() => state = Some(value),
                "code" | "state" | "error" => return None,
                _ => {}
            }
        }
        if state.is_some_and(|state| state != expected_state) {
            return None;
        }
        code.filter(|code| super::model_auth::valid_key(code))
    };
    if let Ok(url) = Url::parse(content) {
        let callback = Url::parse(BROWSER_REDIRECT).expect("official Codex callback URL");
        if url.origin() != callback.origin()
            || url.path() != callback.path()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        query(url.query_pairs().into_owned().collect())
    } else if let Some((code, state)) = content.split_once('#') {
        (state == expected_state && super::model_auth::valid_key(code)).then(|| code.to_owned())
    } else if content.contains("code=") {
        let url = Url::parse(&format!(
            "{BROWSER_REDIRECT}?{}",
            content.trim_start_matches('?')
        ))
        .ok()?;
        query(url.query_pairs().into_owned().collect())
    } else {
        super::model_auth::valid_key(content).then(|| content.to_owned())
    }
}

fn browser_url(state: &str, verifier: &str) -> Url {
    let mut url = Url::parse(BROWSER_AUTHORIZE).expect("official Codex authorization URL");
    let challenge =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier));
    url.query_pairs_mut().extend_pairs([
        ("response_type", "code"),
        ("client_id", CLIENT_ID),
        ("redirect_uri", BROWSER_REDIRECT),
        ("scope", "openid profile email offline_access"),
        ("code_challenge", &challenge),
        ("code_challenge_method", "S256"),
        ("state", state),
        ("id_token_add_organizations", "true"),
        ("codex_cli_simplified_flow", "true"),
        ("originator", "pi"),
    ]);
    url
}

async fn browser_login(
    client: &Client,
    announce: impl Fn(Notice),
) -> Result<(
    Credential,
    Option<pi_rs_workbench::oauth_callback::Callback>,
)> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 1455))
        .await
        .ok();
    let callback = Url::parse(BROWSER_REDIRECT).expect("official Codex callback URL");
    let state = uuid::Uuid::new_v4().simple().to_string();
    let verifier = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let url = browser_url(&state, &verifier);
    let (send, receive) = tokio::sync::oneshot::channel();
    announce(Notice::Browser(url.clone(), send));
    let _ = pi_rs_workbench::oauth_callback::open_browser(url.as_str()).await;
    let (code, incoming) = tokio::time::timeout(Duration::from_secs(300), async {
        let browser = async {
            let Some(listener) = listener.as_ref() else {
                return std::future::pending().await;
            };
            pi_rs_workbench::oauth_callback::receive(listener, &callback, &state).await
        };
        let manual = async {
            match receive.await {
                Ok(input) => input,
                Err(_) => std::future::pending().await,
            }
        };
        tokio::select! {
            callback = browser => {
                let incoming = callback?;
                let code = incoming.code().filter(|code| super::model_auth::valid_key(code));
                Ok::<_, Error>((code, Some(incoming)))
            }
            input = manual => Ok((manual_code(&input, &state), None)),
        }
    })
    .await
    .map_err(|_| error("browser sign-in timed out"))??;
    let result = match code {
        Some(code) => {
            exchange_code(client, TOKEN_ENDPOINT, &code, &verifier, BROWSER_REDIRECT).await
        }
        None => Err(error(
            "authorization was denied or the pasted callback is invalid",
        )),
    };
    match result {
        Ok(credential) => Ok((credential, incoming)),
        Err(error) => {
            if let Some(incoming) = incoming {
                incoming.reply(false).await;
            }
            Err(error)
        }
    }
}

async fn exchange_code(
    client: &Client,
    token_endpoint: &str,
    code: &str,
    verifier: &str,
    redirect: &str,
) -> Result<Credential> {
    let response = client
        .post(token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", CLIENT_ID),
            ("code", code),
            ("code_verifier", verifier),
            ("redirect_uri", redirect),
        ])
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| error("authorization code exchange failed"))?;
    if !response.status().is_success() {
        return Err(error(&format!(
            "authorization code exchange returned {}",
            response.status()
        )));
    }
    tokens(&read_json(response).await?)
}

async fn device_login(
    client: &Client,
    user_code_endpoint: &str,
    device_token_endpoint: &str,
    token_endpoint: &str,
    announce: impl Fn(&'static str, &str),
) -> Result<Credential> {
    let response = client
        .post(user_code_endpoint)
        .json(&json!({"client_id": CLIENT_ID}))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| error("device code request failed"))?;
    if !response.status().is_success() {
        return Err(error(&format!(
            "device code request returned {}",
            response.status()
        )));
    }
    let device = read_json(response).await?;
    let id = device["device_auth_id"]
        .as_str()
        .filter(|id| super::model_auth::valid_key(id))
        .ok_or_else(|| error("missing device ID"))?;
    let code = device["user_code"]
        .as_str()
        .filter(|code| {
            !code.is_empty()
                && code.len() <= 64
                && code
                    .bytes()
                    .all(|character| character.is_ascii_alphanumeric() || character == b'-')
        })
        .ok_or_else(|| error("invalid user code"))?;
    let seconds = device["interval"]
        .as_u64()
        .or_else(|| {
            device["interval"]
                .as_str()
                .and_then(|seconds| seconds.trim().parse().ok())
        })
        .unwrap_or(5);
    let mut interval = Duration::from_secs(seconds.clamp(1, 60));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15 * 60);
    announce(DEVICE_VERIFICATION, code);
    tokio::time::timeout_at(deadline, async {
        loop {
            let response = client
                .post(device_token_endpoint)
                .json(&json!({"device_auth_id": id, "user_code": code}))
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .map_err(|_| error("device authorization request failed"))?;
            let status = response.status();
            if status.is_success() {
                let result = read_json(response).await?;
                let field = |name: &str| {
                    result[name]
                        .as_str()
                        .filter(|value| super::model_auth::valid_key(value))
                        .ok_or_else(|| error("invalid device authorization response"))
                };
                return exchange_code(
                    client,
                    token_endpoint,
                    field("authorization_code")?,
                    field("code_verifier")?,
                    DEVICE_REDIRECT,
                )
                .await;
            }
            if !matches!(status.as_u16(), 403 | 404) {
                let response = read_json(response).await?;
                match response["error"]
                    .as_str()
                    .or_else(|| response["error"]["code"].as_str())
                {
                    Some("deviceauth_authorization_pending") => {}
                    Some("slow_down") => interval += Duration::from_secs(5),
                    _ => return Err(error(&format!("device authorization returned {status}"))),
                }
            }
            tokio::time::sleep(interval).await;
        }
    })
    .await
    .map_err(|_| error("device code expired"))?
}

fn http_client(settings: &super::config::Settings) -> Result<Client> {
    pi_rs::openai::oauth::http_client_with_options(
        &super::http_proxy::configured(settings)?,
        settings.http_idle_timeout(),
    )
}

pub(super) async fn access(
    auth_path: &std::path::Path,
    settings: &super::config::Settings,
    minimum: Duration,
    no_refresh: bool,
) -> Result<String> {
    let origin = Url::parse(TOKEN_ENDPOINT).expect("official OpenAI OAuth token URL");
    let client = http_client(settings)?;
    access_at(auth_path, &client, &origin, minimum, no_refresh).await
}

async fn access_at(
    path: &std::path::Path,
    client: &Client,
    token_endpoint: &Url,
    minimum: Duration,
    no_refresh: bool,
) -> Result<String> {
    fn saved(data: &Value) -> Result<Credential> {
        let entry = &data[PROVIDER];
        if entry["type"] != "oauth" {
            return Err(error("no saved Pi Codex OAuth credential"));
        }
        read_entry(entry)
    }

    if no_refresh {
        return Ok(saved(&super::config::read_auth_object(path).await?)?.access);
    }
    let guard = super::auth_lock::acquire_async(path).await?;
    let stored = saved(&super::config::read_object_unlocked(path)?)?;
    guard.check()?;
    let minimum_ms = minimum.as_millis().min(u128::from(u64::MAX)) as u64;
    if stored.expires > now_ms().saturating_add(minimum_ms.max(30_000)) {
        return Ok(stored.access);
    }
    let response = client
        .post(token_endpoint.clone())
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", &stored.refresh),
            ("client_id", CLIENT_ID),
        ])
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| error("refresh request failed"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(error(&format!("refresh returned {status}")));
    }
    let replacement = tokens(&read_json(response).await?)?;
    let Credential {
        access,
        refresh,
        expires,
    } = replacement;
    let account = pi_rs::openai::codex_account_id(&access)
        .ok_or_else(|| error("refreshed access token lacks an account ID"))?;
    if Some(account.as_str()) != pi_rs::openai::codex_account_id(&stored.access).as_deref() {
        return Err(error("refresh returned a different account"));
    }
    super::config::update_object_locked(path, Some(&guard), |data| {
        let entry = data
            .get_mut(PROVIDER)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| error("saved credential disappeared"))?;
        entry.insert("access".into(), Value::String(access.clone()));
        entry.insert("refresh".into(), Value::String(refresh));
        entry.insert("accountId".into(), Value::String(account));
        entry.insert("expires".into(), Value::from(expires));
        Ok(())
    })?;
    if expires <= now_ms().saturating_add(minimum_ms) {
        return Err(error("refreshed token expires too soon"));
    }
    Ok(access)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Provider, model_auth::Keys, model_profiles::Profiles};
    use serde_json::json;
    use std::io::{BufRead, BufReader, Read, Write};

    fn fake_jwt(signature: &str) -> String {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({"https://api.openai.com/auth":{
                "chatgpt_account_id": "fixture-account"
            }}))
            .unwrap(),
        );
        format!("e30.{payload}.{signature}")
    }

    #[tokio::test]
    async fn imported_codex_tokens_refresh_once_and_preserve_foreign_auth_fields() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("auth.json");
        let old = fake_jwt("old");
        std::fs::write(&path, serde_json::to_vec(&json!({
            "openai-codex":{"type":"oauth","access":old,"refresh":"old-fixture","expires":1,"extra":"preserved"},
            "openai-chatgpt":{"type":"oauth","access":"fixture-placeholder"}
        })).unwrap()).unwrap();
        let fresh = fake_jwt("new");
        let expected = fresh.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = Url::parse(&format!(
            "http://{}/oauth/token",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let worker = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).unwrap();
            assert!(line.starts_with("POST /oauth/token "));
            let mut length = 0;
            loop {
                line.clear();
                socket.read_line(&mut line).unwrap();
                if line.trim().is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut form = vec![0; length];
            socket.read_exact(&mut form).unwrap();
            let form = String::from_utf8(form).unwrap();
            assert!(form.contains("grant_type=refresh_token&refresh_token=old-fixture"));
            assert!(form.contains(CLIENT_ID));
            let response = json!({
                "access_token": fresh, "refresh_token":"new-fixture", "expires_in":3600
            })
            .to_string();
            write!(
                socket.get_mut(),
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len(),
            ).unwrap();
        });
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        assert_eq!(
            access_at(&path, &client, &endpoint, Duration::ZERO, true)
                .await
                .unwrap(),
            old
        );
        let (first, second) = tokio::join!(
            access_at(&path, &client, &endpoint, Duration::ZERO, false),
            access_at(&path, &client, &endpoint, Duration::ZERO, false),
        );
        assert_eq!(first.unwrap(), expected);
        assert_eq!(second.unwrap(), expected);
        worker.join().unwrap();
        let data = crate::config::read_auth_object(&path).await.unwrap();
        assert_eq!(data[PROVIDER]["refresh"], "new-fixture");
        assert_eq!(data[PROVIDER]["accountId"], "fixture-account");
        assert_eq!(data[PROVIDER]["extra"], "preserved");
        assert_eq!(data["openai-chatgpt"]["access"], "fixture-placeholder");
    }

    #[test]
    fn browser_authorization_uses_the_registered_callback_and_s256() {
        let url = browser_url(
            "fixture-state",
            "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
        );
        assert_eq!(url.host_str(), Some("auth.openai.com"));
        let values: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(values["redirect_uri"], BROWSER_REDIRECT);
        assert_eq!(values["state"], "fixture-state");
        assert_eq!(values["code_challenge_method"], "S256");
        assert_eq!(
            values["code_challenge"],
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        for input in [
            "fixture-code",
            "fixture-code#fixture-state",
            "code=fixture-code&state=fixture-state",
            "http://localhost:1455/auth/callback?code=fixture-code&state=fixture-state",
        ] {
            assert_eq!(
                manual_code(input, "fixture-state").as_deref(),
                Some("fixture-code")
            );
        }
        for input in [
            "fixture-code#wrong",
            "http://localhost:1455/auth/callback?code=fixture-code&state=wrong",
            "https://other.example/auth/callback?code=fixture-code&state=fixture-state",
            "code=one&code=two&state=fixture-state",
        ] {
            assert!(manual_code(input, "fixture-state").is_none());
        }
    }

    #[tokio::test]
    async fn device_code_polls_pending_and_exchanges_the_code() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let expected = fake_jwt("device");
        let access = expected.clone();
        let worker = std::thread::spawn(move || {
            for (path, status, expected, response) in [
                (
                    "/user-code",
                    "200 OK",
                    CLIENT_ID,
                    json!({
                        "device_auth_id":"device-fixture","user_code":"ABCD-1234","interval":"1",
                    }),
                ),
                (
                    "/device-token",
                    "403 Forbidden",
                    "device-fixture",
                    json!({}),
                ),
                (
                    "/device-token",
                    "200 OK",
                    "ABCD-1234",
                    json!({
                        "authorization_code":"auth-fixture","code_verifier":"verifier-fixture",
                    }),
                ),
                (
                    "/oauth",
                    "200 OK",
                    "code_verifier=verifier-fixture",
                    json!({
                        "access_token":access,"refresh_token":"refresh-fixture","expires_in":3600,
                    }),
                ),
            ] {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).unwrap();
                assert_eq!(line.trim_end(), format!("POST {path} HTTP/1.1"));
                let mut length = 0;
                loop {
                    line.clear();
                    socket.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut request = vec![0; length];
                socket.read_exact(&mut request).unwrap();
                assert!(String::from_utf8(request).unwrap().contains(expected));
                let response = response.to_string();
                write!(socket.get_mut(),
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                ).unwrap();
            }
        });
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let credential = device_login(
            &client,
            &format!("{base}/user-code"),
            &format!("{base}/device-token"),
            &format!("{base}/oauth"),
            |url, code| {
                assert_eq!(url, DEVICE_VERIFICATION);
                assert_eq!(code, "ABCD-1234");
            },
        )
        .await
        .unwrap();
        assert_eq!(credential.access, expected);
        assert_eq!(credential.refresh, "refresh-fixture");
        worker.join().unwrap();
    }

    #[test]
    fn codex_has_pinned_defaults_and_oauth_stays_on_its_endpoint() {
        let provider = Provider::named(PROVIDER).unwrap();
        let profiles = Profiles::default();
        assert_eq!(profiles.model_ids(&provider).len(), 8);
        assert_eq!(profiles.model_ids(&provider)[0], "gpt-6.1-sol");
        let upstream = json!({"openai-codex":{
            "type":"oauth","access":fake_jwt("fixture"),"refresh":"fixture","expires":1,
        }});
        let keys = Keys::parse(&upstream);
        assert!(keys.oauth(&provider, pi_rs::openai::CODEX_ENDPOINT));
        assert!(!keys.oauth(&provider, pi_rs::openai::DEFAULT_ENDPOINT));
        assert!(!keys.oauth(
            &provider,
            "https://relay.example/backend-api/codex/responses"
        ));
    }
}
