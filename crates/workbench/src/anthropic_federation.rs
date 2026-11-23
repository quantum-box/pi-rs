use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pi_rs::{Error, Result, anthropic::OAUTH_API_BETA};
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use super::config::Settings;

const TOKEN_URL: &str = "https://api.anthropic.com/v1/oauth/token";
const JWT_LIMIT: usize = 16 * 1024;
const RESPONSE_LIMIT: usize = 1024 * 1024;
const EXPIRY_MARGIN: Duration = Duration::from_secs(30);
const ADVISORY_MARGIN: Duration = Duration::from_secs(120);
const ADVISORY_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Eq, PartialEq)]
struct Config {
    rule: String,
    organization: String,
    identity_file: PathBuf,
    service_account: Option<String>,
    workspace: Option<String>,
}

struct AccessToken {
    token: String,
    expires: Instant,
}

pub(super) struct Federation {
    config: Config,
    cache: Mutex<Cache>,
    refreshing: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Cache {
    token: Option<AccessToken>,
    advisory_attempt: Option<Instant>,
}

static CURRENT: Mutex<Option<Arc<Federation>>> = Mutex::new(None);

impl Federation {
    pub(super) fn from_env(env: impl Fn(&str) -> Option<String>) -> Option<Arc<Self>> {
        let read = |name| env(name).filter(|value| !value.trim().is_empty());
        let config = Config {
            rule: read("ANTHROPIC_FEDERATION_RULE_ID")?,
            organization: read("ANTHROPIC_ORGANIZATION_ID")?,
            identity_file: read("ANTHROPIC_IDENTITY_TOKEN_FILE")?.into(),
            service_account: read("ANTHROPIC_SERVICE_ACCOUNT_ID"),
            workspace: read("ANTHROPIC_WORKSPACE_ID"),
        };
        let mut current = CURRENT.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = current.as_ref().filter(|entry| entry.config == config) {
            return Some(Arc::clone(existing));
        }
        let federation = Arc::new(Self {
            config,
            cache: Mutex::new(Cache::default()),
            refreshing: tokio::sync::Mutex::new(()),
        });
        *current = Some(Arc::clone(&federation));
        Some(federation)
    }

    pub(super) async fn access(self: &Arc<Self>, settings: &Settings) -> Result<String> {
        if let Some(token) = self.cached_or_advisory(settings) {
            return Ok(token);
        }
        self.cached_access(Duration::ZERO, || self.exchange_official(settings))
            .await
    }

    pub(super) async fn access_with_minimum(
        &self,
        settings: &Settings,
        minimum: Duration,
    ) -> Result<String> {
        self.cached_access(minimum, || self.exchange_official(settings))
            .await
    }

    pub(super) async fn check_identity_file(&self) -> Result<()> {
        self.read_assertion().await.map(|_| ())
    }

    fn cached_or_advisory(self: &Arc<Self>, settings: &Settings) -> Option<String> {
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        let token = cache.token.as_ref()?;
        let remaining = token.expires.saturating_duration_since(Instant::now());
        if remaining <= EXPIRY_MARGIN {
            return None;
        }
        let access = token.token.clone();
        if remaining <= ADVISORY_MARGIN
            && cache
                .advisory_attempt
                .is_none_or(|attempt| attempt.elapsed() >= ADVISORY_BACKOFF)
        {
            cache.advisory_attempt = Some(Instant::now());
            let settings = settings.clone();
            let federation = Arc::clone(self);
            tokio::spawn(async move {
                let Ok(_refresh) = federation.refreshing.try_lock() else {
                    return;
                };
                if let Ok(token) = federation.exchange_official(&settings).await {
                    federation.store(token);
                }
            });
        }
        Some(access)
    }

    pub(super) fn invalidate(&self, rejected: &str) {
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        if cache
            .token
            .as_ref()
            .is_some_and(|entry| entry.token == rejected)
        {
            cache.token = None;
        }
    }

    fn store(&self, entry: AccessToken) {
        self.cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .token = Some(entry);
    }

    async fn exchange_official(&self, settings: &Settings) -> Result<AccessToken> {
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
        let client = builder
            .build()
            .map_err(|_| failure("cannot create client"))?;
        self.exchange(&client, TOKEN_URL).await
    }

    async fn cached_access(
        &self,
        minimum: Duration,
        exchange: impl AsyncFnOnce() -> Result<AccessToken>,
    ) -> Result<String> {
        let _refresh = self.refreshing.lock().await;
        {
            let cached = self.cache.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(token) = cached.token.as_ref()
                && token.expires.saturating_duration_since(Instant::now())
                    > minimum.max(EXPIRY_MARGIN)
            {
                return Ok(token.token.clone());
            }
        }
        let token = exchange().await?;
        let access = token.token.clone();
        let too_short = token.expires.saturating_duration_since(Instant::now()) < minimum;
        self.store(token);
        if too_short {
            Err(failure("exchanged token expires sooner than requested"))
        } else {
            Ok(access)
        }
    }

    async fn read_assertion(&self) -> Result<String> {
        let mut jwt = String::new();
        tokio::fs::File::open(&self.config.identity_file)
            .await
            .map_err(|_| failure("cannot open identity token file"))?
            .take((JWT_LIMIT + 1) as u64)
            .read_to_string(&mut jwt)
            .await
            .map_err(|_| failure("cannot read identity token file"))?;
        if jwt.len() > JWT_LIMIT || jwt.trim().is_empty() {
            return Err(failure("identity token is empty or exceeds 16 KiB"));
        }
        Ok(jwt)
    }

    async fn exchange(&self, client: &reqwest::Client, endpoint: &str) -> Result<AccessToken> {
        let jwt = self.read_assertion().await?;
        let mut body = serde_json::json!({
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.trim(),
            "federation_rule_id": self.config.rule,
            "organization_id": self.config.organization,
        });
        if let Some(account) = &self.config.service_account {
            body["service_account_id"] = serde_json::json!(account);
        }
        if let Some(workspace) = &self.config.workspace {
            body["workspace_id"] = serde_json::json!(workspace);
        }
        let started = Instant::now();
        let mut response = client
            .post(endpoint)
            .header(
                "anthropic-beta",
                format!("{OAUTH_API_BETA},oidc-federation-2026-04-01"),
            )
            .json(&body)
            .send()
            .await
            .map_err(|_| failure("cannot reach token endpoint"))?;
        if !response.status().is_success() {
            return Err(failure(&format!(
                "token exchange returned HTTP {}",
                response.status().as_u16()
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failure("cannot read token response"))?
        {
            if bytes.len() + chunk.len() > RESPONSE_LIMIT {
                return Err(failure("token response exceeds 1 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let parsed: ExchangeResponse =
            serde_json::from_slice(&bytes).map_err(|_| failure("invalid token response"))?;
        let expires = started
            .checked_add(Duration::from_secs(parsed.expires_in))
            .filter(|expires| *expires > Instant::now())
            .ok_or_else(|| failure("invalid token expiry"))?;
        if parsed.access_token.is_empty()
            || parsed
                .access_token
                .bytes()
                .any(|byte| !byte.is_ascii_graphic())
            || parsed
                .token_type
                .is_some_and(|kind| !kind.eq_ignore_ascii_case("bearer"))
        {
            return Err(failure("invalid bearer token in response"));
        }
        Ok(AccessToken {
            token: parsed.access_token,
            expires,
        })
    }
}

#[derive(Deserialize)]
struct ExchangeResponse {
    access_token: String,
    expires_in: u64,
    token_type: Option<String>,
}

fn failure(message: &str) -> Error {
    Error::Config(format!("Anthropic workload identity: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    async fn request(socket: &mut TcpStream) -> (String, Value) {
        let mut bytes = Vec::new();
        loop {
            let mut buffer = [0; 4096];
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    let body = serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                    return (headers.to_owned(), body);
                }
            }
        }
    }

    #[tokio::test]
    async fn exchange_shares_concurrent_access_and_reloads_rotated_identity_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identity");
        tokio::fs::write(&path, "assertion-one\n").await.unwrap();
        let config = |name: &str| match name {
            "ANTHROPIC_FEDERATION_RULE_ID" => Some("rule-fixture".into()),
            "ANTHROPIC_ORGANIZATION_ID" => Some("organization-fixture".into()),
            "ANTHROPIC_IDENTITY_TOKEN_FILE" => Some(path.to_string_lossy().into_owned()),
            "ANTHROPIC_SERVICE_ACCOUNT_ID" => Some("account-fixture".into()),
            "ANTHROPIC_WORKSPACE_ID" => Some("workspace-fixture".into()),
            _ => None,
        };
        assert!(Federation::from_env(|_| Some(" ".into())).is_none());
        let federation = Federation::from_env(config).unwrap();
        assert!(Arc::ptr_eq(
            &federation,
            &Federation::from_env(config).unwrap()
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (assertion, access) in [
                ("assertion-one", "token-one"),
                ("assertion-two", "token-two"),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (headers, body) = request(&mut socket).await;
                assert!(headers.starts_with("POST /v1/oauth/token HTTP/1.1"));
                assert!(
                    headers.contains("anthropic-beta: oauth-2025-04-20,oidc-federation-2026-04-01")
                );
                assert_eq!(
                    body,
                    json!({
                        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                        "assertion": assertion,
                        "federation_rule_id": "rule-fixture",
                        "organization_id": "organization-fixture",
                        "service_account_id": "account-fixture",
                        "workspace_id": "workspace-fixture",
                    })
                );
                let body = json!({"access_token":access,"expires_in":3600,"token_type":"Bearer"});
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.to_string().len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let access =
            || federation.cached_access(Duration::ZERO, || federation.exchange(&client, &endpoint));
        let (first, parallel) = tokio::join!(access(), access());
        assert_eq!(first.unwrap(), "token-one");
        assert_eq!(parallel.unwrap(), "token-one");
        federation.invalidate("old-rejected-token");
        assert_eq!(access().await.unwrap(), "token-one");
        federation.invalidate("token-one");
        tokio::fs::write(path, "assertion-two").await.unwrap();
        assert_eq!(access().await.unwrap(), "token-two");
        server.await.unwrap();

        let insufficient = federation
            .cached_access(Duration::from_secs(7200), async || {
                Ok(AccessToken {
                    token: "token-three".into(),
                    expires: Instant::now() + Duration::from_secs(3600),
                })
            })
            .await;
        assert!(matches!(insufficient, Err(Error::Config(message)) if message.contains("sooner")));
        assert_eq!(access().await.unwrap(), "token-three");
    }
}
