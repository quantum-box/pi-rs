//! OpenRouter browser authorization with a one-time callback and PKCE.
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pi_rs::{Error, Result};
use reqwest::Url;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, sync::oneshot};
use uuid::Uuid;

const AUTHORIZE: &str = "https://openrouter.ai/auth";
const EXCHANGE: &str = "https://openrouter.ai/api/v1/auth/keys";

pub(super) async fn login(
    settings: &super::config::Settings,
    announce: impl FnOnce(&Url, oneshot::Sender<String>),
) -> Result<()> {
    let proxies = super::http_proxy::configured(settings)?;
    let idle_timeout = settings.http_idle_timeout();
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let callback = Url::parse(&format!(
        "http://127.0.0.1:{}/oauth/callback/{}",
        listener.local_addr()?.port(),
        Uuid::new_v4(),
    ))
    .expect("valid OpenRouter callback");
    let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let authorization = authorization_url(&callback, &verifier);
    let (manual, input) = oneshot::channel();
    announce(&authorization, manual);
    let _ = pi_rs_workbench::oauth_callback::open_browser(authorization.as_str()).await;

    let incoming = tokio::time::timeout(Duration::from_secs(300), async {
        tokio::select! {
            browser = pi_rs_workbench::oauth_callback::receive_path(&listener, &callback) => browser.map(Authorization::Browser),
            manual = input => manual
                .map(Authorization::Manual)
                .map_err(|_| Error::Config("OpenRouter login cancelled".into())),
        }
    })
    .await
    .map_err(|_| Error::Config("OpenRouter login timed out".into()))??;
    let code = match &incoming {
        Authorization::Browser(redirect) => redirect
            .code()
            .filter(|code| super::model_auth::valid_key(code)),
        Authorization::Manual(value) => super::model_auth::valid_key(value).then(|| value.clone()),
    };
    let result = async {
        let code =
            code.ok_or_else(|| Error::Config("Missing OpenRouter authorization code".into()))?;
        let key = exchange(EXCHANGE, &code, &verifier, &proxies, idle_timeout).await?;
        super::model_auth::save_openrouter_oauth(&key).await
    }
    .await;
    if let Authorization::Browser(redirect) = incoming {
        redirect.reply(result.is_ok()).await;
    }
    result
}

enum Authorization {
    Browser(pi_rs_workbench::oauth_callback::Callback),
    Manual(String),
}

fn authorization_url(callback: &Url, verifier: &str) -> Url {
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut url = Url::parse(AUTHORIZE).expect("valid OpenRouter authorization URL");
    url.query_pairs_mut()
        .append_pair("callback_url", callback.as_str())
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256");
    url
}

pub(super) fn manual_code(input: &str) -> Option<String> {
    let input = input.trim();
    let code = if let Ok(url) = Url::parse(input) {
        url.query_pairs()
            .find_map(|(key, value)| (key == "code").then(|| value.into_owned()))?
    } else if input.starts_with("code=") || input.starts_with("?code=") {
        let url = Url::parse(&format!(
            "http://localhost/?{}",
            input.trim_start_matches('?')
        ))
        .ok()?;
        url.query_pairs()
            .find_map(|(key, value)| (key == "code").then(|| value.into_owned()))?
    } else {
        input.to_owned()
    };
    super::model_auth::valid_key(&code).then_some(code)
}

async fn exchange(
    endpoint: &str,
    code: &str,
    verifier: &str,
    proxies: &[reqwest::Proxy],
    idle_timeout: Option<Duration>,
) -> Result<String> {
    #[derive(Deserialize)]
    struct KeyResponse {
        key: String,
    }

    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30));
    for proxy in proxies {
        builder = builder.proxy(proxy.clone());
    }
    if let Some(timeout) = idle_timeout {
        builder = builder.read_timeout(timeout);
    }
    let client = builder
        .build()
        .map_err(|_| Error::Config("Could not initialize OpenRouter OAuth".into()))?;
    let failed = || Error::Config("OpenRouter key exchange failed".into());
    let mut response = client
        .post(endpoint)
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&json!({"code":code,"code_verifier":verifier,"code_challenge_method":"S256"}))
        .send()
        .await
        .map_err(|_| failed())?;
    if !response.status().is_success() {
        return Err(Error::Config(format!(
            "OpenRouter key exchange failed (HTTP {})",
            response.status()
        )));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| failed())? {
        if bytes.len() + chunk.len() > 64 * 1024 {
            return Err(Error::Config(
                "OpenRouter key response exceeds 64 KiB".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let response: KeyResponse = serde_json::from_slice(&bytes)
        .map_err(|_| Error::Config("Invalid OpenRouter key response".into()))?;
    if !super::model_auth::valid_key(&response.key) {
        return Err(Error::Config("Invalid OpenRouter API key".into()));
    }
    Ok(response.key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn authorization_uses_s256_and_manual_input_accepts_the_same_code() {
        let callback = Url::parse("http://127.0.0.1:54321/oauth/callback/nonce").unwrap();
        let url = authorization_url(&callback, "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk");
        let parameters: std::collections::HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(parameters["callback_url"], callback.as_str());
        assert_eq!(parameters["code_challenge_method"], "S256");
        assert_eq!(
            parameters["code_challenge"],
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        assert!(!url.as_str().contains("dBjftJeZ4CVP"));
        for input in [
            " fixture-code ",
            "code=fixture-code",
            "http://localhost/?code=fixture-code",
        ] {
            assert_eq!(manual_code(input).as_deref(), Some("fixture-code"));
        }
        for invalid in ["", "not a code", "http://localhost/?error=access_denied"] {
            assert!(manual_code(invalid).is_none());
        }
    }

    #[tokio::test]
    async fn exchange_posts_only_the_code_and_pkce_verifier() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let endpoint = format!("http://{}/api/v1/auth/keys", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            assert_eq!(line, "POST /api/v1/auth/keys HTTP/1.1\r\n");
            let mut length = None;
            loop {
                line.clear();
                assert!(stream.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                assert!(!line.to_ascii_lowercase().starts_with("authorization:"));
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = Some(value.trim().parse().unwrap());
                }
            }
            let mut request = vec![0; length.unwrap()];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request).unwrap(),
                json!({
                    "code":"fixture-code","code_verifier":"fixture-verifier","code_challenge_method":"S256",
                })
            );
            let body = br#"{"key":"fixture-key"}"#;
            stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            ).as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        assert_eq!(
            exchange(&endpoint, "fixture-code", "fixture-verifier", &[], None)
                .await
                .unwrap(),
            "fixture-key"
        );
        fixture.await.unwrap();
    }

    #[tokio::test]
    async fn exchange_uses_configured_proxy() {
        let (proxy, server) =
            super::super::http_proxy::test_proxy(r#"{"key":"fixture-key"}"#).await;
        assert_eq!(
            exchange(
                "http://127.0.0.1:1/api/v1/auth/keys",
                "fixture-code",
                "fixture-verifier",
                &[proxy],
                None
            )
            .await
            .unwrap(),
            "fixture-key"
        );
        let (request, _, body) = server.await.unwrap();
        assert!(request.starts_with("POST http://127.0.0.1:1/api/v1/auth/keys "));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["code"],
            "fixture-code"
        );
    }
}
