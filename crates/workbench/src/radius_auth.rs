//! OAuth and endpoint-bound credential storage for the Radius gateway.
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pi_rs::{Error, Result};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use uuid::Uuid;

pub(super) const DEFAULT_GATEWAY: &str = "https://radius.pi.dev/";
pub(super) const DEFAULT_MODEL_ENDPOINT: &str = "https://radius.pi.dev/v1/messages";
const CALLBACK: &str = "http://127.0.0.1:1456/oauth/callback";
const CLIENT_ID: &str = "pi-gateway";
const SCOPE: &str = "gateway offline_access";
const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(super) enum Method {
    Browser,
    Device,
}

pub(super) enum Notice {
    Browser(Url),
    Device { verification: Url, code: String },
}

impl std::fmt::Display for Notice {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Browser(url) => write!(output, "Continue in your browser: {url}"),
            Self::Device { verification, code } => {
                write!(output, "Enter code {code} at {verification}")
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Gateway {
    origin: Url,
    client: Client,
}

#[derive(Deserialize, Serialize)]
struct Credential {
    #[serde(rename = "type")]
    kind: String,
    gateway: String,
    access: String,
    refresh: String,
    expires: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    login_id: Option<String>,
}

impl Credential {
    fn login_id(&self) -> String {
        self.login_id
            .clone()
            .unwrap_or_else(|| format!("{:x}", Sha256::digest(self.refresh.as_bytes())))
    }

    fn access(self) -> (String, String) {
        let binding = self.login_id();
        (self.access, binding)
    }
}

#[derive(Deserialize)]
struct Token {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
    scope: Option<String>,
}

enum TokenResult {
    Granted(Token),
    Pending,
    SlowDown,
}

impl Gateway {
    pub(super) fn configured_origin() -> Result<Url> {
        let value = std::env::var("PI_RADIUS_GATEWAY").unwrap_or_else(|_| DEFAULT_GATEWAY.into());
        let origin = if value.contains("://") {
            trusted_url(&value)?
        } else {
            trusted_url(&format!("https://{value}"))?
        };
        if origin.path() != "/" || origin.query().is_some() {
            return Err(Error::Config(
                "PI_RADIUS_GATEWAY must be a gateway origin".into(),
            ));
        }
        Ok(origin)
    }

    pub(super) fn configured() -> Result<Self> {
        let origin = Self::configured_origin()?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| Error::Config("Could not initialize Radius OAuth client".into()))?;
        Ok(Self { origin, client })
    }

    pub(super) fn url(&self, path: &str) -> Url {
        self.origin.join(path).expect("valid Radius endpoint")
    }

    pub(super) fn model_endpoint(&self) -> Url {
        model_endpoint(&self.origin)
    }

    fn credential(&self) -> Result<Option<Credential>> {
        let value = super::config::read_object(&auth_path())?;
        let Some(entry) = value.get("radius").filter(|entry| entry["type"] == "oauth") else {
            return Ok(None);
        };
        if entry["gateway"] != self.origin.as_str() {
            return Ok(None);
        }
        let credential: Credential = serde_json::from_value(entry.clone())
            .map_err(|_| Error::Config("Invalid saved Radius credentials".into()))?;
        if !valid_secret(&credential.access) || !valid_secret(&credential.refresh) {
            return Err(Error::Config("Invalid saved Radius credentials".into()));
        }
        Ok(Some(credential))
    }

    pub(super) fn has_credential(&self) -> Result<bool> {
        Ok(self.credential()?.is_some())
    }

    pub(super) fn cached_access(&self) -> Result<Option<String>> {
        Ok(self.credential()?.map(|credential| credential.access))
    }

    pub(super) fn cached_login_id(&self) -> Result<Option<String>> {
        Ok(self.credential()?.map(|credential| credential.login_id()))
    }

    pub(super) async fn login(&self, method: Method, announce: impl Fn(Notice)) -> Result<()> {
        let token = match method {
            Method::Browser => return self.login_browser(announce).await,
            Method::Device => self.login_device(announce).await?,
        };
        let credential = self.credential_from_token(token, None)?;
        let _guard = auth_lock().await?;
        save(&credential)
    }

    pub(super) async fn logout(&self) -> Result<bool> {
        let _guard = auth_lock().await?;
        if !self.has_credential()? {
            return Ok(false);
        }
        super::config::update_object(&auth_path(), |data| {
            data.remove("radius");
            Ok(())
        })?;
        Ok(true)
    }

    pub(super) async fn access(&self, min_validity: Duration) -> Result<Option<String>> {
        Ok(self
            .access_with_login(min_validity)
            .await?
            .map(|(token, _)| token))
    }

    pub(super) async fn access_with_login(
        &self,
        min_validity: Duration,
    ) -> Result<Option<(String, String)>> {
        let Some(credential) = self.credential()? else {
            return Ok(None);
        };
        let min_validity = u64::try_from(min_validity.as_millis()).unwrap_or(u64::MAX);
        if credential.expires > now().saturating_add(min_validity) {
            return Ok(Some(credential.access()));
        }
        let _guard = auth_lock().await?;
        let Some(credential) = self.credential()? else {
            return Ok(None);
        };
        if credential.expires > now().saturating_add(min_validity) {
            return Ok(Some(credential.access()));
        }
        let result = self
            .token(&[
                ("grant_type", "refresh_token"),
                ("client_id", CLIENT_ID),
                ("refresh_token", &credential.refresh),
            ])
            .await?;
        let TokenResult::Granted(token) = result else {
            return Err(Error::Config(
                "Radius OAuth refresh did not return a token".into(),
            ));
        };
        let refreshed = self.credential_from_token(token, Some(credential))?;
        save(&refreshed)?;
        Ok(Some(refreshed.access()))
    }

    async fn login_browser(&self, announce: impl Fn(Notice)) -> Result<()> {
        let response = self
            .client
            .get(self.url("/v1/oauth"))
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| request_error())?;
        let discovery: Value = read_response(response).await?;
        let authorization = discovery["authorizationEndpoint"].as_str().ok_or_else(|| {
            Error::Config("Radius OAuth discovery has no authorization endpoint".into())
        })?;
        let mut authorization = trusted_url(authorization)?;
        authorization.set_query(None);
        let callback = Url::parse(CALLBACK).expect("valid Radius callback");
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 1456)).await?;
        let state = Uuid::new_v4().to_string();
        let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        authorization
            .query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", CLIENT_ID)
            .append_pair("redirect_uri", CALLBACK)
            .append_pair("scope", SCOPE)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("handoff", "url")
            .append_pair("state", &state);
        announce(Notice::Browser(authorization.clone()));
        let _ = pi_rs_workbench::oauth_callback::open_browser(authorization.as_str()).await;
        let incoming = tokio::time::timeout(
            Duration::from_secs(600),
            pi_rs_workbench::oauth_callback::receive(&listener, &callback, &state),
        )
        .await
        .map_err(|_| Error::Config("Radius login timed out".into()))??;
        let result = async {
            let code = incoming
                .code()
                .filter(|code| !code.is_empty())
                .ok_or_else(|| Error::Config("Radius login was declined".into()))?;
            let result = self
                .token(&[
                    ("grant_type", "authorization_code"),
                    ("client_id", CLIENT_ID),
                    ("redirect_uri", CALLBACK),
                    ("code", &code),
                    ("code_verifier", &verifier),
                ])
                .await?;
            let TokenResult::Granted(token) = result else {
                return Err(Error::Config("Radius login did not return a token".into()));
            };
            let credential = self.credential_from_token(token, None)?;
            let _guard = auth_lock().await?;
            save(&credential)
        }
        .await;
        incoming.reply(result.is_ok()).await;
        result
    }

    async fn login_device(&self, announce: impl Fn(Notice)) -> Result<Token> {
        #[derive(Deserialize)]
        struct DeviceCode {
            device_code: String,
            user_code: String,
            verification_uri: String,
            expires_in: u64,
            interval: Option<u64>,
        }
        let response = self
            .post(
                "/v1/oauth/device",
                &[("client_id", CLIENT_ID), ("scope", SCOPE)],
            )
            .await?;
        let device: DeviceCode = read_response(response).await?;
        if !valid_secret(&device.device_code)
            || !valid_secret(&device.user_code)
            || device.expires_in == 0
        {
            return Err(Error::Config(
                "Radius returned an invalid device code".into(),
            ));
        }
        let verification = trusted_url(&device.verification_uri)?;
        announce(Notice::Device {
            verification,
            code: device.user_code,
        });
        let mut interval = device.interval.unwrap_or(5).clamp(1, 60);
        let poll = async {
            loop {
                tokio::time::sleep(Duration::from_secs(interval)).await;
                match self
                    .token(&[
                        ("grant_type", DEVICE_GRANT),
                        ("client_id", CLIENT_ID),
                        ("device_code", &device.device_code),
                    ])
                    .await?
                {
                    TokenResult::Granted(token) => return Ok(token),
                    TokenResult::Pending => {}
                    TokenResult::SlowDown => interval = interval.saturating_add(5),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(device.expires_in.min(3600)), poll)
            .await
            .map_err(|_| Error::Config("Radius device authorization expired".into()))?
    }

    async fn token(&self, form: &[(&str, &str)]) -> Result<TokenResult> {
        let response = self.post("/v1/oauth/token", form).await?;
        let status = response.status();
        if status.is_success() {
            return read_response(response).await.map(TokenResult::Granted);
        }
        let value: Value = read_json(response).await.unwrap_or_default();
        match value["error"].as_str() {
            Some("authorization_pending") => Ok(TokenResult::Pending),
            Some("slow_down") => Ok(TokenResult::SlowDown),
            Some("expired_token") => {
                Err(Error::Config("Radius device authorization expired".into()))
            }
            Some("access_denied") => Err(Error::Config("Radius authorization was denied".into())),
            _ => Err(status_error(status)),
        }
    }

    async fn post(&self, path: &str, form: &[(&str, &str)]) -> Result<reqwest::Response> {
        let mut body = Url::parse("https://localhost").expect("form base");
        body.query_pairs_mut().extend_pairs(form.iter().copied());
        self.client
            .post(self.url(path))
            .header(reqwest::header::ACCEPT, "application/json")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body.query().unwrap_or_default().to_owned())
            .send()
            .await
            .map_err(|_| request_error())
    }

    fn credential_from_token(
        &self,
        token: Token,
        previous: Option<Credential>,
    ) -> Result<Credential> {
        let login_id = previous
            .as_ref()
            .map_or_else(|| Uuid::new_v4().to_string(), Credential::login_id);
        let (prior_refresh, prior_scope) = previous
            .map(|credential| (Some(credential.refresh), credential.scope))
            .unwrap_or_default();
        let refresh = token.refresh_token.or(prior_refresh).unwrap_or_default();
        if !valid_secret(&token.access_token) || !valid_secret(&refresh) || token.expires_in == 0 {
            return Err(Error::Config(
                "Radius returned an invalid OAuth credential".into(),
            ));
        }
        Ok(Credential {
            kind: "oauth".into(),
            gateway: self.origin.to_string(),
            access: token.access_token,
            refresh,
            expires: now()
                .saturating_add(token.expires_in.saturating_mul(1000).saturating_sub(60_000)),
            scope: token.scope.or(prior_scope),
            login_id: Some(login_id),
        })
    }
}

pub(super) fn model_endpoint(origin: &Url) -> Url {
    let path = if origin.as_str() == DEFAULT_GATEWAY {
        "/v1/messages"
    } else {
        "/messages"
    };
    origin.join(path).expect("valid Radius endpoint")
}

fn trusted_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| Error::Config("Invalid Radius OAuth URL".into()))?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(url.scheme() == "https" || url.scheme() == "http" && loopback)
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Config(
            "Radius OAuth requires HTTPS or loopback HTTP without credentials or fragments".into(),
        ));
    }
    Ok(url)
}

async fn read_response<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    if !response.status().is_success() {
        return Err(status_error(response.status()));
    }
    read_json(response).await
}

async fn read_json<T: DeserializeOwned>(mut response: reqwest::Response) -> Result<T> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| request_error())? {
        if body.len() + chunk.len() > 64 * 1024 {
            return Err(Error::Config("Radius OAuth response exceeds 64 KiB".into()));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .map_err(|_| Error::Config("Radius returned an invalid OAuth response".into()))
}

fn auth_path() -> PathBuf {
    super::config::agent_dir().join("auth.json")
}

async fn auth_lock() -> Result<std::fs::File> {
    let directory = super::config::agent_dir();
    std::fs::create_dir_all(&directory)?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("radius-oauth.lock"))?;
    let wait = async {
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(Error::Io(error)),
            }
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, wait)
        .await
        .map_err(|_| Error::Config("Radius credential storage is busy".into()))?
}

fn save(credential: &Credential) -> Result<()> {
    super::config::update_object(&auth_path(), |data| {
        data.insert("radius".into(), serde_json::to_value(credential)?);
        Ok(())
    })
}

fn valid_secret(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 16 * 1024
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn status_error(status: StatusCode) -> Error {
    Error::Config(format!("Radius OAuth request failed ({status})"))
}

fn request_error() -> Error {
    Error::Config("Radius OAuth request failed".into())
}
