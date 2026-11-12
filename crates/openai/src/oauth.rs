//! Sign in with ChatGPT protocol primitives. Callers must store tokens securely.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pi_rs_core::{Error, Result};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const AUTHORIZE: &str = "https://auth.openai.com/api/accounts/authorize";
const TOKEN: &str = "https://auth.openai.com/api/accounts/oauth/token";
const JWKS: &str = "https://auth.openai.com/.well-known/jwks.json";
const ISSUER: &str = "https://auth.openai.com";
const RESOURCE: &str = "https://api.openai.com/v1";
const PLAN_SCOPE: &str = "chatgpt.tokens.use.direct";
const SCOPE: &str = "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct";

fn random() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}
fn failed(message: &str) -> Error {
    Error::Config(format!("ChatGPT sign-in: {message}"))
}

/// A pending one-time browser authorization; secrets must never be logged.
pub struct Authorization {
    callback: Url,
    state: String,
    nonce: String,
    verifier: String,
    client_id: Option<String>,
    host_id: String,
}

/// Tokens for one issued dynamic registration. Never log or expose this value.
#[derive(Clone, Serialize, Deserialize)]
pub struct Credential {
    pub client_id: String,
    pub subject: String,
    pub email: Option<String>,
    pub id_token: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: u64,
    pub scopes: Vec<String>,
}

impl Credential {
    pub async fn revoke(&self, client: &Client) -> Result<()> {
        let response = client
            .post("https://auth.openai.com/api/accounts/oauth/revoke")
            .form(&[
                ("token", self.refresh_token.as_str()),
                ("token_type_hint", "refresh_token"),
                ("client_id", self.client_id.as_str()),
            ])
            .send()
            .await
            .map_err(|_| failed("session revocation failed"))?;
        if !response.status().is_success() {
            return Err(failed("session revocation was not confirmed"));
        }
        Ok(())
    }
    /// Replace a rotating refresh token. The caller must serialize refreshes
    /// across processes and atomically persist the returned credential.
    pub async fn refresh(&self, client: &Client) -> Result<Self> {
        #[derive(Deserialize)]
        struct Refreshed {
            access_token: String,
            refresh_token: String,
            expires_in: u64,
            token_type: String,
            scope: Option<String>,
        }
        let mut response = client
            .post(TOKEN)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", self.client_id.as_str()),
                ("refresh_token", self.refresh_token.as_str()),
                ("resource", RESOURCE),
            ])
            .send()
            .await
            .map_err(|_| failed("token refresh failed"))?;
        if !response.status().is_success() {
            return Err(failed("token refresh was rejected"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failed("token response failed"))?
        {
            if bytes.len() + chunk.len() > 64 * 1024 {
                return Err(failed("token response too large"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let refreshed: Refreshed =
            serde_json::from_slice(&bytes).map_err(|_| failed("invalid token response"))?;
        if refreshed.access_token.is_empty()
            || refreshed.refresh_token.is_empty()
            || refreshed.expires_in == 0
            || !refreshed.token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(failed("invalid token response"));
        }
        let scopes: Vec<String> = refreshed
            .scope
            .as_deref()
            .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
            .unwrap_or_else(|| self.scopes.clone());
        if !scopes.iter().any(|scope| scope == PLAN_SCOPE) {
            return Err(failed("ChatGPT plan permission was not granted"));
        }
        let expires_at = now()?
            .checked_add(refreshed.expires_in)
            .ok_or_else(|| failed("invalid token expiry"))?;
        Ok(Self {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            expires_at,
            scopes,
            ..self.clone()
        })
    }
}

impl Authorization {
    pub fn state(&self) -> &str {
        &self.state
    }
    pub fn new(callback: Url, host_id: String, client_id: Option<String>) -> Result<Self> {
        if callback.scheme() != "http"
            || callback.host_str() != Some("127.0.0.1")
            || callback.port().is_none()
            || callback.path() != "/auth/callback"
            || callback.query().is_some()
            || callback.fragment().is_some()
            || !callback.username().is_empty()
            || callback.password().is_some()
        {
            return Err(failed(
                "callback must use http://127.0.0.1:<port>/auth/callback",
            ));
        }
        let Some(host) = host_id.strip_prefix("urn:uuid:") else {
            return Err(failed("invalid host ID"));
        };
        let id = Uuid::parse_str(host).map_err(|_| failed("invalid host ID"))?;
        if id.get_version_num() != 4 {
            return Err(failed("invalid host ID"));
        }
        if client_id
            .as_deref()
            .is_some_and(|id| id.is_empty() || id == "dynamic_agent_client")
        {
            return Err(failed("invalid registered client ID"));
        }
        Ok(Self {
            callback,
            state: random(),
            nonce: random(),
            verifier: random(),
            client_id,
            host_id,
        })
    }

    pub fn url(&self) -> Url {
        let mut url = Url::parse(AUTHORIZE).expect("static authorization URL");
        let mut query = url.query_pairs_mut();
        query.append_pair(
            "client_id",
            self.client_id.as_deref().unwrap_or("dynamic_agent_client"),
        );
        if self.client_id.is_none() {
            query.append_pair("agent_name_hint", "pi-rs");
        }
        query
            .append_pair("ext_agent_host_id", &self.host_id)
            .append_pair("redirect_uri", self.callback.as_str())
            .append_pair("response_type", "code")
            .append_pair("scope", SCOPE)
            .append_pair("resource", RESOURCE)
            .append_pair("state", &self.state)
            .append_pair("nonce", &self.nonce)
            .append_pair("code_challenge_method", "S256")
            .append_pair(
                "code_challenge",
                &URL_SAFE_NO_PAD.encode(Sha256::digest(self.verifier.as_bytes())),
            );
        drop(query);
        url
    }

    /// Validate the loopback callback and return its authorization code and issued client ID.
    pub fn callback(&self, redirect: &Url) -> Result<(String, String)> {
        if redirect.origin() != self.callback.origin()
            || redirect.path() != self.callback.path()
            || redirect.fragment().is_some()
        {
            return Err(failed("invalid callback URL"));
        }
        let mut state = None;
        let mut code = None;
        let mut issued = None;
        let mut error = None;
        for (key, value) in redirect.query_pairs() {
            let slot = match key.as_ref() {
                "state" => &mut state,
                "code" => &mut code,
                "client_id" => &mut issued,
                "error" => &mut error,
                _ => continue,
            };
            if slot.replace(value.into_owned()).is_some() {
                return Err(failed("duplicate callback parameter"));
            }
        }
        if state.as_deref() != Some(&self.state) {
            return Err(failed("callback state mismatch"));
        }
        if error.is_some() {
            return Err(failed("authorization was declined"));
        }
        let code = code
            .filter(|s| !s.is_empty())
            .ok_or_else(|| failed("missing authorization code"))?;
        let client_id = match (&self.client_id, issued) {
            (Some(expected), Some(actual)) if expected != &actual => {
                return Err(failed("callback client ID mismatch"));
            }
            (Some(expected), _) => expected.clone(),
            (None, Some(actual)) if !actual.is_empty() && actual != "dynamic_agent_client" => {
                actual
            }
            _ => return Err(failed("missing issued client ID")),
        };
        Ok((code, client_id))
    }

    pub async fn exchange(&self, client: &Client, redirect: &Url) -> Result<Credential> {
        let (code, client_id) = self.callback(redirect)?;
        let response = token_request(
            client,
            &[
                ("grant_type", "authorization_code"),
                ("client_id", &client_id),
                ("code", &code),
                ("code_verifier", &self.verifier),
                ("redirect_uri", self.callback.as_str()),
                ("resource", RESOURCE),
            ],
        )
        .await?;
        let claims = verify_id_token(client, &response.id_token, &client_id, &self.nonce).await?;
        response.into_credential(client_id, claims)
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    id_token: String,
    expires_in: u64,
    scope: String,
    token_type: String,
}
struct Identity {
    subject: String,
    email: Option<String>,
}
impl TokenResponse {
    fn into_credential(self, client_id: String, identity: Identity) -> Result<Credential> {
        if self.access_token.is_empty()
            || self.refresh_token.is_empty()
            || self.id_token.is_empty()
            || !self.token_type.eq_ignore_ascii_case("bearer")
            || self.expires_in == 0
        {
            return Err(failed("invalid token response"));
        }
        let scopes: Vec<String> = self.scope.split_whitespace().map(str::to_owned).collect();
        if !scopes.iter().any(|scope| scope == PLAN_SCOPE) {
            return Err(failed("ChatGPT plan permission was not granted"));
        }
        let expires_at = now()?
            .checked_add(self.expires_in)
            .ok_or_else(|| failed("invalid token expiry"))?;
        Ok(Credential {
            client_id,
            subject: identity.subject,
            email: identity.email,
            id_token: self.id_token,
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            expires_at,
            scopes,
        })
    }
}
fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .map_err(|_| failed("invalid system clock"))
}
async fn token_request(client: &Client, fields: &[(&str, &str)]) -> Result<TokenResponse> {
    let mut response = client
        .post(TOKEN)
        .form(fields)
        .send()
        .await
        .map_err(|_| failed("token exchange failed"))?;
    if !response.status().is_success() {
        return Err(failed("token exchange was rejected"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| failed("token response failed"))?
    {
        if bytes.len() + chunk.len() > 64 * 1024 {
            return Err(failed("token response too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| failed("invalid token response"))
}

async fn verify_id_token(
    client: &Client,
    token: &str,
    client_id: &str,
    nonce: &str,
) -> Result<Identity> {
    let mut response = client
        .get(JWKS)
        .send()
        .await
        .map_err(|_| failed("could not retrieve signing keys"))?;
    if !response.status().is_success() {
        return Err(failed("could not retrieve signing keys"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| failed("could not retrieve signing keys"))?
    {
        if bytes.len() + chunk.len() > 256 * 1024 {
            return Err(failed("signing key set too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let keys: Value = serde_json::from_slice(&bytes).map_err(|_| failed("invalid signing keys"))?;
    validate_id_token(token, &keys, client_id, nonce, now()?)
}

fn validate_id_token(
    token: &str,
    keys: &Value,
    client_id: &str,
    nonce: &str,
    now: u64,
) -> Result<Identity> {
    let invalid = || failed("invalid ID token");
    if token.len() > 64 * 1024 {
        return Err(invalid());
    }
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(invalid());
    }
    let header: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
    let claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
    if header["alg"] != "RS256" {
        return Err(invalid());
    }
    let kid = header["kid"].as_str().ok_or_else(invalid)?;
    let key = keys["keys"]
        .as_array()
        .and_then(|keys| {
            keys.iter().find(|key| {
                key["kid"] == kid
                    && key["kty"] == "RSA"
                    && (key["alg"].is_null() || key["alg"] == "RS256")
                    && (key["use"].is_null() || key["use"] == "sig")
            })
        })
        .ok_or_else(invalid)?;
    let modulus = URL_SAFE_NO_PAD
        .decode(key["n"].as_str().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    let exponent = URL_SAFE_NO_PAD
        .decode(key["e"].as_str().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    let sig = URL_SAFE_NO_PAD.decode(parts[2]).map_err(|_| invalid())?;
    ring::signature::RsaPublicKeyComponents {
        n: &modulus,
        e: &exponent,
    }
    .verify(
        &ring::signature::RSA_PKCS1_2048_8192_SHA256,
        format!("{}.{}", parts[0], parts[1]).as_bytes(),
        &sig,
    )
    .map_err(|_| invalid())?;
    let audience = claims["aud"].as_str() == Some(client_id)
        || claims["aud"]
            .as_array()
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(client_id)));
    if claims["iss"] != ISSUER
        || !audience
        || claims["aud"]
            .as_array()
            .is_some_and(|values| values.len() > 1)
            && claims["azp"] != client_id
        || !claims["azp"].is_null() && claims["azp"] != client_id
        || claims["nonce"] != nonce
        || claims["exp"].as_u64().is_none_or(|exp| exp <= now)
        || claims["iat"]
            .as_u64()
            .is_none_or(|iat| iat > now.saturating_add(60))
        || claims["nbf"].as_u64().is_some_and(|nbf| nbf > now)
    {
        return Err(invalid());
    }
    let subject = claims["sub"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)?
        .to_owned();
    let email = claims["email"].as_str().map(str::to_owned);
    Ok(Identity { subject, email })
}

/// Build a redirect-disabled HTTP client for exchanging and verifying credentials.
pub fn http_client() -> Result<Client> {
    http_client_with_options(&[], None)
}

pub fn http_client_with_options(
    proxies: &[reqwest::Proxy],
    idle_timeout: Option<std::time::Duration>,
) -> Result<Client> {
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30));
    if let Some(timeout) = idle_timeout {
        builder = builder.read_timeout(timeout);
    }
    for proxy in proxies {
        builder = builder.proxy(proxy.clone());
    }
    builder
        .build()
        .map_err(|_| failed("could not initialize HTTP client"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_registration_and_callback_require_state_and_issued_client_id() {
        let auth = Authorization::new(
            Url::parse("http://127.0.0.1:12345/auth/callback").unwrap(),
            format!("urn:uuid:{}", Uuid::new_v4()),
            None,
        )
        .unwrap();
        let url = auth.url();
        let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(query["client_id"], "dynamic_agent_client");
        assert_eq!(query["scope"], SCOPE);
        assert_eq!(query["resource"], RESOURCE);
        assert_eq!(query["code_challenge_method"], "S256");
        assert!(!url.as_str().contains(&auth.verifier));
        let redirect = auth
            .callback
            .join(&format!(
                "/auth/callback?state={}&code=code&client_id=oaiapp_fixture",
                auth.state
            ))
            .unwrap();
        assert_eq!(
            auth.callback(&redirect).unwrap(),
            ("code".into(), "oaiapp_fixture".into())
        );
        for suffix in [
            format!("state={}&code=code", auth.state),
            "state=wrong&code=code&client_id=x".into(),
            format!(
                "state={}&state={}&code=code&client_id=x",
                auth.state, auth.state
            ),
            format!("state={}&error=access_denied", auth.state),
        ] {
            assert!(
                auth.callback(
                    &auth
                        .callback
                        .join(&format!("/auth/callback?{suffix}"))
                        .unwrap()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn id_token_signature_and_claims_must_match() {
        let private = include_bytes!("../tests/fixtures/test-signing-key.der");
        let key = ring::signature::RsaKeyPair::from_pkcs8(private).unwrap();
        let keys: Value =
            serde_json::from_str(include_str!("../tests/fixtures/test-jwks.json")).unwrap();
        let sign = |claims: Value| {
            let h = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"test"}"#);
            let p = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
            let msg = format!("{h}.{p}");
            let mut sig = vec![0; key.public().modulus_len()];
            key.sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                msg.as_bytes(),
                &mut sig,
            )
            .unwrap();
            format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig))
        };
        let claims = serde_json::json!({"iss":ISSUER,"aud":"client","nonce":"nonce","sub":"user","exp":200,"iat":90});
        let valid = sign(claims.clone());
        assert_eq!(
            validate_id_token(&valid, &keys, "client", "nonce", 100)
                .unwrap()
                .subject,
            "user"
        );
        assert!(validate_id_token(&valid, &keys, "other", "nonce", 100).is_err());
        assert!(validate_id_token(&valid, &keys, "client", "other", 100).is_err());
        assert!(validate_id_token(&valid, &keys, "client", "nonce", 200).is_err());
        let mut wrong_authorized_party = claims.clone();
        wrong_authorized_party["azp"] = "other".into();
        assert!(
            validate_id_token(&sign(wrong_authorized_party), &keys, "client", "nonce", 100)
                .is_err()
        );
        let mut forged = claims;
        forged["sub"] = "attacker".into();
        let parts: Vec<_> = valid.split('.').collect();
        let tampered = format!(
            "{}.{}.{}",
            parts[0],
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&forged).unwrap()),
            parts[2]
        );
        assert!(validate_id_token(&tampered, &keys, "client", "nonce", 100).is_err());
    }

    #[test]
    fn token_exchange_requires_the_plan_scope() {
        let token = || TokenResponse {
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            id_token: "id".into(),
            expires_in: 3600,
            scope: "openid profile email".into(),
            token_type: "Bearer".into(),
        };
        let identity = || Identity {
            subject: "user".into(),
            email: None,
        };
        assert!(
            token()
                .into_credential("client".into(), identity())
                .is_err()
        );
        let mut granted = token();
        granted.scope.push_str(" chatgpt.tokens.use.direct");
        assert!(granted.into_credential("client".into(), identity()).is_ok());
    }
}
