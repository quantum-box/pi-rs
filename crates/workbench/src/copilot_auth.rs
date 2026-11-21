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
    let mut url = Url::parse(endpoint).map_err(|_| error("invalid API endpoint"))?;
    if url.scheme() != "https" || url.host_str() != Some(API) || url.port().is_some() {
        return Err(error(
            "saved credentials require an official Copilot API route",
        ));
    }
    url.set_host(Some(endpoint_host(token)?))
        .map_err(|_| error("invalid API endpoint"))?;
    Ok(url.to_string())
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
        let mut interval =
            Duration::from_secs(device["interval"].as_u64().unwrap_or(5).clamp(1, 60));
        announce(&url, user_code);
        loop {
            if deadline.saturating_duration_since(tokio::time::Instant::now()) <= interval {
                return Err(error("device code expired"));
            }
            tokio::time::sleep(interval).await;
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
                    let updated = token["interval"].as_u64().map(Duration::from_secs);
                    interval = interval
                        .saturating_add(Duration::from_secs(5))
                        .max(updated.unwrap_or_default());
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
    let min_ms = minimum
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let window = min_ms;
    let guard = super::auth_lock::acquire_async(path).await?;
    let mut credential = read(&super::config::read_object_unlocked(path)?)?;
    guard.check()?;
    if credential.expires <= now().saturating_add(window) {
        credential = service.refresh(&credential).await?;
        super::config::update_object_locked(path, Some(&guard), |data| {
            let entry = data
                .get_mut(PROVIDER)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| error("saved credential disappeared"))?;
            entry.insert("access".into(), serde_json::json!(credential.access));
            entry.insert("expires".into(), serde_json::json!(credential.expires));
            entry.insert(
                "availableModelIds".into(),
                serde_json::json!(credential.available_model_ids),
            );
            Ok(())
        })?;
        if minimum.is_some() && credential.expires <= now().saturating_add(min_ms) {
            return Err(error("refreshed token expires too soon"));
        }
    }
    Ok(credential.access)
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
            "type":"oauth","access":"expired","refresh":"long-lived-fixture","expires":1,
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
                    "verification_uri":"https://github.com/login/device","interval":1,"expires_in":90,
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
    fn copilot_catalogue_and_credentials_use_pinned_official_routes() {
        use super::super::{Provider, model_auth::Keys, model_profiles::Profiles};
        let profile = Profiles::parse(&json!({})).unwrap();
        let provider = Provider::named(PROVIDER).unwrap();
        assert_eq!(profile.model_ids(&provider).len(), 27);
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
