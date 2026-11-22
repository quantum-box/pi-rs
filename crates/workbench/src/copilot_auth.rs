//! GitHub Copilot access tokens and Pi-compatible subscription credentials.
use pi_rs::{Error, Result};
use reqwest::{Client, RequestBuilder, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PROVIDER: &str = "github-copilot";
const DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
const GITHUB_ACCESS_URL: &str = "https://github.com/login/oauth/access_token";
const CLIENT_ID: &str = "Iv1.b507a08c87ecfe98";
const TOKEN_URL: &str = "https://api.github.com/copilot_internal/v2/token";
const API: &str = "api.individual.githubcopilot.com";
pub(super) const CLASSIFIER_ENDPOINT: &str =
    "https://api.individual.githubcopilot.com/v1/systemone";

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Credential {
    pub access: String,
    pub refresh: String,
    pub expires: u64,
    pub available_model_ids: Option<Vec<String>>,
    enterprise_url: Option<String>,
}

fn error(message: &str) -> Error {
    Error::Config(format!("GitHub Copilot OAuth: {message}"))
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) fn read_entry(entry: &Value) -> Result<Credential> {
    let credential: Credential =
        serde_json::from_value(entry.clone()).map_err(|_| error("invalid saved credential"))?;
    if !super::model_auth::valid_key(&credential.access)
        || !super::model_auth::valid_key(&credential.refresh)
        || credential.expires == 0
    {
        return Err(error("invalid saved credential"));
    }
    Ok(credential)
}

fn endpoint_host(token: &str) -> Result<&'static str> {
    let proxy = token
        .split(';')
        .find_map(|field| field.strip_prefix("proxy-ep="));
    match proxy {
        None | Some("proxy.individual.githubcopilot.com" | "api.individual.githubcopilot.com") => {
            Ok(API)
        }
        Some("proxy.business.githubcopilot.com" | "api.business.githubcopilot.com") => {
            Ok("api.business.githubcopilot.com")
        }
        Some("proxy.enterprise.githubcopilot.com" | "api.enterprise.githubcopilot.com") => {
            Ok("api.enterprise.githubcopilot.com")
        }
        Some(_) => Err(error("unrecognized token routing host")),
    }
}

pub(super) fn routed_endpoint(endpoint: &str, token: &str) -> Result<String> {
    if !super::builtin_chat::find(PROVIDER)
        .is_some_and(|builtin| builtin.is_official_endpoint(endpoint))
    {
        return Err(error(
            "saved credentials require an official Copilot API route",
        ));
    }
    let url = Url::parse(endpoint).map_err(|_| error("invalid API endpoint"))?;
    if url.scheme() != "https" || url.host_str() != Some(API) || url.port().is_some() {
        return Err(error(
            "saved credentials require an official Copilot API route",
        ));
    }
    Ok(route_subscriber_endpoint(url, token)?.to_string())
}

pub(super) fn routed_classifier_endpoint(endpoint: &Url, token: &str) -> Result<Url> {
    if endpoint.as_str() != CLASSIFIER_ENDPOINT {
        return Err(error(
            "saved credentials require the official Copilot classifier endpoint",
        ));
    }
    route_subscriber_endpoint(endpoint.clone(), token)
}

fn route_subscriber_endpoint(mut url: Url, token: &str) -> Result<Url> {
    url.set_host(Some(endpoint_host(token)?))
        .map_err(|_| error("invalid API endpoint"))?;
    Ok(url)
}

fn poll_interval(seconds: &Value) -> Option<Duration> {
    let seconds = seconds
        .as_f64()
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)?;
    let millis = (seconds * 1000.0).floor() as u64;
    Some(Duration::from_millis(millis.max(1000)))
}

struct Service {
    client: Client,
    device_code_url: String,
    github_access_url: String,
    token_url: String,
    catalog_url: Option<String>,
}

impl Service {
    fn new(settings: &super::config::Settings) -> Result<Self> {
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .user_agent("GitHubCopilotChat/0.35.0");
        for proxy in super::http_proxy::configured(settings)? {
            builder = builder.proxy(proxy);
        }
        if let Some(timeout) = settings.http_idle_timeout() {
            builder = builder.read_timeout(timeout);
        }
        Ok(Self {
            client: builder
                .build()
                .map_err(|_| error("could not configure HTTP client"))?,
            device_code_url: DEVICE_CODE_URL.into(),
            github_access_url: GITHUB_ACCESS_URL.into(),
            token_url: TOKEN_URL.into(),
            catalog_url: None,
        })
    }

    async fn device_login(&self, announce: impl Fn(&Url, &str)) -> Result<Credential> {
        let request = self
            .client
            .post(&self.device_code_url)
            .form(&[("client_id", CLIENT_ID), ("scope", "read:user")]);
        let device = read_json(request).await?;
        let code = device["device_code"]
            .as_str()
            .filter(|code| super::model_auth::valid_key(code))
            .ok_or_else(|| error("invalid device code"))?;
        let user_code = device["user_code"]
            .as_str()
            .filter(|code| {
                !code.is_empty()
                    && code.len() <= 64
                    && code
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
            .ok_or_else(|| error("invalid user code"))?;
        let url = Url::parse(device["verification_uri"].as_str().unwrap_or_default())
            .ok()
            .filter(|url| {
                url.scheme() == "https"
                    && url.host_str() == Some("github.com")
                    && url.port().is_none()
                    && url.username().is_empty()
                    && url.password().is_none()
            })
            .ok_or_else(|| error("unexpected verification URL"))?;
        let lifetime = device["expires_in"]
            .as_u64()
            .filter(|seconds| (1..=1800).contains(seconds))
            .ok_or_else(|| error("invalid device code expiry"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(lifetime);
        let mut interval = poll_interval(&device["interval"]).unwrap_or(Duration::from_secs(5));
        announce(&url, user_code);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(interval.min(remaining)).await;
            if tokio::time::Instant::now() >= deadline {
                return Err(error("device code expired"));
            }
            let request = self.client.post(&self.github_access_url).form(&[
                ("client_id", CLIENT_ID),
                ("device_code", code),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ]);
            let token = tokio::time::timeout_at(deadline, read_json(request))
                .await
                .map_err(|_| error("device code expired"))??;
            if let Some(refresh) = token["access_token"]
                .as_str()
                .filter(|token| super::model_auth::valid_key(token))
            {
                return self
                    .refresh(&Credential {
                        access: String::new(),
                        refresh: refresh.into(),
                        expires: 0,
                        available_model_ids: None,
                        enterprise_url: None,
                    })
                    .await;
            }
            match token["error"].as_str() {
                Some("authorization_pending") => {}
                Some("slow_down") => {
                    interval = poll_interval(&token["interval"])
                        .unwrap_or_else(|| interval.saturating_add(Duration::from_secs(5)));
                }
                Some("access_denied") => return Err(error("access denied")),
                Some("expired_token") => return Err(error("device code expired")),
                _ => return Err(error("device authorization failed")),
            }
        }
    }

    async fn refresh(&self, credential: &Credential) -> Result<Credential> {
        if credential
            .enterprise_url
            .as_deref()
            .is_some_and(|domain| !domain.is_empty() && domain != "github.com")
        {
            return Err(error(
                "a private GitHub Enterprise authentication host is unsupported",
            ));
        }
        let token = read_json(
            self.client
                .get(&self.token_url)
                .bearer_auth(&credential.refresh),
        )
        .await?;
        let access = token["token"]
            .as_str()
            .filter(|token| super::model_auth::valid_key(token))
            .ok_or_else(|| error("invalid short-lived access token"))?;
        let expires = token["expires_at"]
            .as_u64()
            .and_then(|seconds| seconds.checked_mul(1000))
            .filter(|deadline| *deadline > now())
            .ok_or_else(|| error("invalid token expiry"))?
            .saturating_sub(300_000);
        let host = endpoint_host(access)?;
        let catalog_url = self
            .catalog_url
            .clone()
            .unwrap_or_else(|| format!("https://{host}/models"));
        let available_model_ids = read_json(
            self.client
                .get(catalog_url)
                .bearer_auth(access)
                .header("x-github-api-version", "2026-06-01"),
        )
        .await
        .and_then(|catalog| available_models(&catalog, host == API))
        .ok()
        .or_else(|| credential.available_model_ids.clone());
        Ok(Credential {
            access: access.into(),
            expires,
            available_model_ids,
            ..credential.clone()
        })
    }
}

async fn read_json(request: RequestBuilder) -> Result<Value> {
    let mut response = request
        .header("accept", "application/json")
        .header("editor-version", "vscode/1.107.0")
        .header("editor-plugin-version", "copilot-chat/0.35.0")
        .header("copilot-integration-id", "vscode-chat")
        .send()
        .await
        .map_err(|_| error("request failed"))?;
    if !response.status().is_success() {
        return Err(error(&format!("request failed ({})", response.status())));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| error("response failed"))?
    {
        if body.len() + chunk.len() > 1024 * 1024 {
            return Err(error("response too large"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| error("invalid response"))
}

fn available_models(value: &Value, policy_fallback: bool) -> Result<Vec<String>> {
    let data = value["data"]
        .as_array()
        .ok_or_else(|| error("invalid model catalogue"))?;
    let mut picker = Vec::new();
    let mut enabled = Vec::new();
    for item in data {
        let Some(id) = item["id"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 256)
        else {
            continue;
        };
        if item["capabilities"]["supports"]["tool_calls"] == false {
            continue;
        }
        let state = item["policy"]["state"].as_str();
        if item["model_picker_enabled"] == true && state != Some("disabled") {
            picker.push(id.into());
        }
        if state == Some("enabled") {
            enabled.push(id.into());
        }
    }
    if picker.is_empty() && policy_fallback {
        Ok(enabled)
    } else {
        Ok(picker)
    }
}

pub(super) async fn login(
    settings: &super::config::Settings,
    control: super::meta_auth::LoginControl,
    announce: impl Fn(&Url, &str),
) -> Result<()> {
    let path = super::config::agent_dir().join("auth.json");
    let previous = super::config::read_auth_object(&path)
        .await?
        .get(PROVIDER)
        .cloned();
    let credential = Service::new(settings)?.device_login(announce).await?;
    let guard = super::auth_lock::acquire_async(&path).await?;
    control.begin_commit()?;
    super::config::update_object_locked(&path, Some(&guard), |data| {
        if data.get(PROVIDER) != previous.as_ref() {
            return Err(error("credentials changed during login"));
        }
        let mut entry = serde_json::to_value(credential)?;
        entry["type"] = Value::String("oauth".into());
        data.insert(PROVIDER.into(), entry);
        Ok(())
    })
}

pub(super) async fn access(
    settings: &super::config::Settings,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    access_for_path(
        &super::config::agent_dir().join("auth.json"),
        settings,
        minimum,
        no_refresh,
    )
    .await
}

pub(super) async fn access_for_path(
    path: &std::path::Path,
    settings: &super::config::Settings,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    access_at(path, Service::new(settings)?, minimum, no_refresh).await
}

async fn access_at(
    path: &std::path::Path,
    service: Service,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    let read = |data: &Value| {
        if data[PROVIDER]["type"] != "oauth" {
            return Err(error("no saved GitHub Copilot subscription credential"));
        }
        read_entry(&data[PROVIDER])
    };
    if no_refresh {
        return Ok(read(&super::config::read_auth_object(path).await?)?.access);
    }
    let window_ms = minimum
        .unwrap_or_default()
        .max(super::model_auth::PI_OAUTH_MINIMUM_VALIDITY)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let fresh = move |expires: u64| expires > now().saturating_add(window_ms);
    let guard = match super::auth_lock::acquire_async(path).await {
        Ok(guard) => guard,
        Err(lock_error) => {
            let credential = read(&super::config::read_readonly_auth(path, lock_error)?)?;
            if fresh(credential.expires) {
                return Ok(credential.access);
            }
            return Err(error("cannot refresh credentials in a read-only store"));
        }
    };
    let stored = super::config::read_auth_object_locked(&guard)?;
    let credential = read(&stored)?;
    guard.check()?;
    if fresh(credential.expires) {
        return Ok(credential.access);
    }

    let path = path.to_owned();
    let previous = stored[PROVIDER].clone();
    // The refresh and its write continue together after the caller is cancelled.
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| error("cannot start refresh runtime"))?;
        let barrier = super::process::refresh_barrier()?;
        let guard = super::process::guard_refresh_auth(barrier, guard);
        let credential = runtime.block_on(service.refresh(&credential))?;
        super::config::update_object_locked(&path, Some(&guard), |data| {
            let entry = data
                .get_mut(PROVIDER)
                .ok_or_else(|| error("saved credential disappeared"))?;
            if *entry != previous {
                return Err(error("saved credential changed during refresh"));
            }
            entry["access"] = serde_json::json!(credential.access);
            entry["expires"] = serde_json::json!(credential.expires);
            entry["availableModelIds"] = serde_json::json!(credential.available_model_ids);
            Ok(())
        })?;
        if minimum.is_some() && !fresh(credential.expires) {
            return Err(error("refreshed token expires too soon"));
        }
        Ok(credential.access)
    })
    .await
    .map_err(|_| error("refresh task failed"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    struct Exchange {
        request: &'static str,
        header: Option<&'static str>,
        form: Option<&'static str>,
        response: Value,
    }

    fn fixture(exchanges: Vec<Exchange>) -> (Service, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            for exchange in exchanges {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).unwrap();
                assert!(line.starts_with(exchange.request), "{line}");
                let mut length = 0;
                let mut headers = String::new();
                loop {
                    line.clear();
                    socket.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                    headers.push_str(&lower);
                }
                if let Some(header) = exchange.header {
                    assert!(headers.contains(header), "{headers}");
                }
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                if let Some(form) = exchange.form {
                    assert!(String::from_utf8(body).unwrap().contains(form));
                }
                let response = exchange.response.to_string();
                write!(
                    socket.get_mut(),
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                ).unwrap();
            }
        });
        let service = Service {
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            device_code_url: format!("{base}/device"),
            github_access_url: format!("{base}/access"),
            token_url: format!("{base}/token"),
            catalog_url: Some(format!("{base}/models")),
        };
        (service, handle)
    }

    fn token_and_catalogue() -> Vec<Exchange> {
        vec![
            Exchange {
                request: "GET /token ",
                header: Some("authorization: bearer long-lived-fixture"),
                form: None,
                response: json!({
                    "token":"proxy-ep=proxy.individual.githubcopilot.com;fixture=1",
                    "expires_at":now()/1000+3600,
                }),
            },
            Exchange {
                request: "GET /models ",
                header: Some("x-github-api-version: 2026-06-01"),
                form: None,
                response: json!({"data":[
                    {"id":"gpt-6.1-sol","model_picker_enabled":true,"policy":{"state":"enabled"}},
                    {"id":"unsupported-tools","model_picker_enabled":true,"capabilities":{"supports":{"tool_calls":false}}},
                    {"id":"disabled","model_picker_enabled":true,"policy":{"state":"disabled"}},
                ]}),
            },
        ]
    }

    #[tokio::test]
    async fn refreshes_imported_copilot_access_and_retains_auth_file_fields() {
        let root = tempfile::tempdir().unwrap();
        let auth = root.path().join("auth.json");
        let original = json!({"github-copilot":{
            "type":"oauth","access":"expired","refresh":"long-lived-fixture","expires":now()+120_000,
            "extra":{"preserve":true},
        },"another":{"type":"api_key","key":"other"}});
        std::fs::write(&auth, serde_json::to_vec(&original).unwrap()).unwrap();

        let (service, worker) = fixture(token_and_catalogue());
        let token = access_at(&auth, service, None, false).await.unwrap();
        worker.join().unwrap();
        assert_eq!(endpoint_host(&token).unwrap(), API);
        let saved = super::super::config::read_auth_object(&auth).await.unwrap();
        let credential = &saved[PROVIDER];
        assert_eq!(credential["access"], token.as_str());
        assert_eq!(credential["refresh"], "long-lived-fixture");
        assert_eq!(credential["extra"], original[PROVIDER]["extra"]);
        assert_eq!(credential["availableModelIds"], json!(["gpt-6.1-sol"]));
        assert_eq!(saved["another"], original["another"]);
    }

    #[tokio::test]
    async fn cancelled_copilot_refresh_still_saves_the_token_and_catalog() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let root = tempfile::tempdir().unwrap();
        let auth = root.path().join("auth.json");
        let original = json!({"github-copilot":{
            "type":"oauth","access":"old","refresh":"long-lived-fixture",
            "expires":1,"extra":"preserve",
        }});
        std::fs::write(&auth, original.to_string()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let service = Service {
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            device_code_url: format!("{base}/device"),
            github_access_url: format!("{base}/access"),
            token_url: format!("{base}/token"),
            catalog_url: Some(format!("{base}/models")),
        };
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (reply, reply_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first = Some((started, reply_rx));
            for exchange in token_and_catalogue() {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                assert!(line.starts_with(exchange.request));
                loop {
                    line.clear();
                    assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                if let Some((started, reply)) = first.take() {
                    started.send(()).unwrap();
                    reply.await.unwrap();
                }
                let body = exchange.response.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
            }
        });
        let pending = {
            let auth = auth.clone();
            tokio::spawn(async move { access_at(&auth, service, None, false).await })
        };
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        let mut read = tokio::spawn(async move { crate::config::read_auth_object(&auth).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut read)
                .await
                .is_err()
        );
        reply.send(()).unwrap();
        let saved = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_ne!(saved[PROVIDER]["access"], "old");
        assert_eq!(saved[PROVIDER]["refresh"], "long-lived-fixture");
        assert_eq!(saved[PROVIDER]["extra"], "preserve");
        assert_eq!(saved[PROVIDER]["availableModelIds"], json!(["gpt-6.1-sol"]));
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn short_copilot_refresh_is_saved_before_explicit_validity_is_enforced() {
        for minimum in [None, Some(Duration::ZERO)] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("auth.json");
            let original = json!({"github-copilot":{
                "type":"oauth","access":"old","refresh":"long-lived-fixture","expires":now()+120_000
            }});
            std::fs::write(&path, original.to_string()).unwrap();
            let mut exchanges = token_and_catalogue();
            exchanges[0].response["expires_at"] = json!(now() / 1000 + 480);
            let (service, worker) = fixture(exchanges);
            let result = access_at(&path, service, minimum, false).await;
            if minimum.is_some() {
                assert!(result.unwrap_err().to_string().contains("expires too soon"));
            } else {
                assert_eq!(endpoint_host(&result.unwrap()).unwrap(), API);
            }
            worker.join().unwrap();
            let saved = crate::config::read_auth_object(&path).await.unwrap();
            assert_ne!(saved[PROVIDER]["access"], "old");
            assert_eq!(saved[PROVIDER]["availableModelIds"], json!(["gpt-6.1-sol"]));
        }
    }

    #[tokio::test]
    async fn copilot_refresh_preserves_catalogue_when_discovery_fails() {
        let mut exchanges = token_and_catalogue();
        exchanges.last_mut().unwrap().response = json!({"invalid":"catalog"});
        let (service, worker) = fixture(exchanges);
        let saved = Credential {
            access: "old".into(),
            refresh: "long-lived-fixture".into(),
            expires: 1,
            available_model_ids: Some(vec!["cached-model".into()]),
            enterprise_url: None,
        };
        let refreshed = service.refresh(&saved).await.unwrap();
        worker.join().unwrap();
        assert_ne!(refreshed.access, saved.access);
        assert_eq!(refreshed.available_model_ids, saved.available_model_ids);
    }

    #[tokio::test]
    async fn copilot_device_flow_polls_then_exchanges_github_token() {
        let mut exchanges = vec![
            Exchange {
                request: "POST /device ",
                header: None,
                form: Some("scope=read%3Auser"),
                response: json!({
                    "device_code":"fixture-device","user_code":"AB12-CD34",
                    "verification_uri":"https://github.com/login/device","interval":1,"expires_in":7,
                }),
            },
            Exchange {
                request: "POST /access ",
                header: None,
                form: Some("device_code=fixture-device"),
                response: json!({"error":"authorization_pending"}),
            },
            Exchange {
                request: "POST /access ",
                header: None,
                form: Some("device_code=fixture-device"),
                response: json!({"error":"slow_down","interval":1}),
            },
            Exchange {
                request: "POST /access ",
                header: None,
                form: Some("device_code=fixture-device"),
                response: json!({"access_token":"long-lived-fixture"}),
            },
        ];
        exchanges.extend(token_and_catalogue());
        let (service, worker) = fixture(exchanges);
        let observed = std::cell::Cell::new(false);
        let credential = service
            .device_login(|url, code| {
                assert_eq!(url.as_str(), "https://github.com/login/device");
                assert_eq!(code, "AB12-CD34");
                observed.set(true);
            })
            .await
            .unwrap();
        worker.join().unwrap();
        assert!(observed.get());
        assert_eq!(credential.refresh, "long-lived-fixture");
        assert_eq!(credential.available_model_ids.unwrap(), ["gpt-6.1-sol"]);
    }

    #[test]
    fn classifier_routes_only_to_official_copilot_hosts() {
        let endpoint = Url::parse(CLASSIFIER_ENDPOINT).unwrap();
        for (token, host) in [
            ("fixture", "api.individual.githubcopilot.com"),
            (
                "proxy-ep=proxy.business.githubcopilot.com",
                "api.business.githubcopilot.com",
            ),
            (
                "proxy-ep=proxy.enterprise.githubcopilot.com",
                "api.enterprise.githubcopilot.com",
            ),
        ] {
            let routed = routed_classifier_endpoint(&endpoint, token).unwrap();
            assert_eq!(routed.as_str(), format!("https://{host}/v1/systemone"));
        }
        assert!(routed_classifier_endpoint(&endpoint, "proxy-ep=other.invalid").is_err());
        for endpoint in [
            "https://other.invalid/v1/systemone",
            "https://api.individual.githubcopilot.com/systemone",
            "https://api.individual.githubcopilot.com/v1/systemone?redirect=fixture",
        ] {
            assert!(routed_classifier_endpoint(&Url::parse(endpoint).unwrap(), "fixture").is_err());
        }
    }

    #[test]
    fn copilot_catalogue_and_credentials_use_pinned_official_routes() {
        use super::super::{Provider, model_auth::Keys, model_profiles::Profiles};
        let profile = Profiles::parse(&json!({})).unwrap();
        let provider = Provider::named(PROVIDER).unwrap();
        assert_eq!(profile.model_ids(&provider).len(), 35);
        for (model, adapter) in [
            (
                "gemini-4-carbon",
                crate::providers::Adapter::ChatCompletions,
            ),
            ("claude-fable-5.2", crate::providers::Adapter::Anthropic),
            ("gpt-6.2-sol", crate::providers::Adapter::OpenAiResponses),
            ("mai-code-2", crate::providers::Adapter::OpenAiResponses),
        ] {
            assert!(profile.model_ids(&provider).iter().any(|id| id == model));
            assert_eq!(profile.model_adapter(&provider, model), adapter);
        }
        let claude = profile.model(&provider, "claude-fable-5.2").unwrap();
        assert!(profile.anthropic_strict_tool_support(&provider, Some(&claude)));
        let gpt = profile.model(&provider, "gpt-6.2-sol").unwrap();
        assert!(profile.responses_strict_tool_support(
            &provider,
            crate::providers::Adapter::OpenAiResponses,
            Some(&gpt)
        ));
        let keys = Keys::parse(&json!({"github-copilot": {
            "type":"oauth","access":"fixture","refresh":"refresh-fixture","expires":1,
        }}));
        for route in ["/chat/completions", "/v1/messages", "/responses"] {
            let endpoint = format!("https://{API}{route}");
            assert!(keys.oauth(&provider, &endpoint));
            assert_eq!(
                routed_endpoint(&endpoint, "proxy-ep=proxy.business.githubcopilot.com").unwrap(),
                format!("https://api.business.githubcopilot.com{route}")
            );
            assert!(routed_endpoint(&endpoint, "proxy-ep=external.example").is_err());
        }
        assert!(!keys.oauth(&provider, "https://external.example/v1/messages"));
        assert!(routed_endpoint("https://external.example/v1/messages", "fixture").is_err());
    }
}
