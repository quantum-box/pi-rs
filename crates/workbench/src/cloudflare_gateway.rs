//! Official Cloudflare AI Gateway account and gateway route selection.
use super::providers::Adapter;

const PREFIX: &str = "https://gateway.ai.cloudflare.com/v1/";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Route {
    account: String,
    gateway: String,
}

impl Route {
    pub fn new(account: &str, gateway: &str) -> Option<Self> {
        (super::operation_models::valid_cloudflare_account(account)
            && !gateway.is_empty()
            && gateway.len() <= 128
            && gateway
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
        .then(|| Self {
            account: account.into(),
            gateway: gateway.into(),
        })
    }

    pub fn from_endpoint(endpoint: &str) -> Option<Self> {
        let after_prefix = endpoint.strip_prefix(PREFIX)?.trim_end_matches('/');
        let (account, tail) = after_prefix.split_once('/')?;
        let (gateway, path) = tail.split_once('/').unwrap_or((tail, ""));
        if !matches!(
            path,
            "" | "compat"
                | "compat/chat/completions"
                | "anthropic"
                | "anthropic/v1"
                | "anthropic/v1/messages"
                | "openai"
                | "openai/responses"
        ) {
            return None;
        }
        Self::new(account, gateway)
    }

    pub fn from_configured(base: &str, fallback: Option<&Self>) -> Option<Self> {
        if let Some(route) = Self::from_endpoint(base) {
            return Some(route);
        }
        let fallback = fallback?;
        let rest = base.strip_prefix(PREFIX)?;
        let (account, tail) = rest.split_once('/')?;
        let (gateway, path) = tail.split_once('/').unwrap_or((tail, ""));
        let account = if account == "{CLOUDFLARE_ACCOUNT_ID}" {
            &fallback.account
        } else {
            account
        };
        let gateway = if gateway == "{CLOUDFLARE_GATEWAY_ID}" {
            &fallback.gateway
        } else {
            gateway
        };
        Self::from_endpoint(&format!("{PREFIX}{account}/{gateway}/{path}"))
    }

    pub fn endpoint(&self, adapter: Adapter) -> Option<String> {
        let path = match adapter {
            Adapter::ChatCompletions => "compat/chat/completions",
            Adapter::Anthropic => "anthropic/v1/messages",
            Adapter::OpenAiResponses => "openai/responses",
            _ => return None,
        };
        Some(format!("{PREFIX}{}/{}/{path}", self.account, self.gateway))
    }

    pub fn is_endpoint(endpoint: &str) -> bool {
        Self::from_endpoint(endpoint).is_some_and(|route| {
            [
                Adapter::ChatCompletions,
                Adapter::Anthropic,
                Adapter::OpenAiResponses,
            ]
            .iter()
            .any(|adapter| route.endpoint(*adapter).as_deref() == Some(endpoint))
        })
    }

    pub fn with_route_identifiers(
        account: Option<&str>,
        gateway: Option<&str>,
        fallback: Option<Self>,
    ) -> Option<Self> {
        match (account, gateway) {
            (Some(account), Some(gateway)) => Self::new(account, gateway),
            (account, gateway) => fallback.filter(|route| {
                account.is_none_or(|account| route.account == account)
                    && gateway.is_none_or(|gateway| route.gateway == gateway)
            }),
        }
    }

    pub fn matches_environment(
        &self,
        env: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> bool {
        let matches = |name: &str, expected: &str| {
            env.and_then(|env| env.get(name))
                .is_none_or(|value| value.as_str() == Some(expected))
        };
        matches("CLOUDFLARE_ACCOUNT_ID", &self.account)
            && matches("CLOUDFLARE_GATEWAY_ID", &self.gateway)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Provider, model_auth::Keys, model_profiles::Profiles};
    use pi_rs::{Model, ModelRequest};
    use serde_json::json;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    fn official_routes_use_their_api_and_keep_scoped_keys_private() {
        let route = Route::new("account123", "my-gateway").unwrap();
        let endpoint = route.endpoint(Adapter::ChatCompletions).unwrap();
        let provider = Provider::named("cloudflare-ai-gateway").unwrap();
        let profiles = Profiles::parse_with_cloudflare_routes(
            &json!({"providers":{"cloudflare-ai-gateway":{
                "baseUrl":"https://gateway.ai.cloudflare.com/v1/{CLOUDFLARE_ACCOUNT_ID}/{CLOUDFLARE_GATEWAY_ID}/compat"
            }}}), None, Some(route.clone()),
        ).unwrap();
        assert_eq!(profiles.endpoint(&provider), endpoint);
        let models = profiles.model_ids(&provider);
        assert_eq!(models.len(), 59);
        assert_eq!(models[0], "workers-ai/@cf/moonshotai/kimi-k2.6");
        let scoped = Keys::parse(&json!({"cloudflare-ai-gateway":{
            "type":"api_key","key":"fixture","endpoint":endpoint,
        }}));
        let imported = Keys::parse(&json!({"cloudflare-ai-gateway":{
            "type":"api_key","key":"fixture","env":{"CLOUDFLARE_ACCOUNT_ID":"account123","CLOUDFLARE_GATEWAY_ID":"my-gateway"},
        }}));
        for adapter in [
            Adapter::Anthropic,
            Adapter::ChatCompletions,
            Adapter::OpenAiResponses,
        ] {
            let model = models
                .iter()
                .find(|model| profiles.model_adapter(&provider, model) == adapter)
                .unwrap();
            let endpoint = route.endpoint(adapter).unwrap();
            assert_eq!(
                profiles.model_endpoint(&provider, model),
                Some(endpoint.as_str())
            );
            assert!(scoped.credential(&provider, &endpoint).is_some());
            assert!(
                imported
                    .custom_credential(&provider, &endpoint, &profiles)
                    .is_some()
            );
        }
        for endpoint in [
            "https://gateway.ai.cloudflare.com/v1/other/my-gateway/compat/chat/completions",
            "https://gateway.ai.cloudflare.com/v1/account123/other/openai/responses",
            "https://gateway.ai.cloudflare.com.example/v1/account123/my-gateway/compat/chat/completions",
        ] {
            assert!(scoped.credential(&provider, endpoint).is_none());
            assert!(
                imported
                    .custom_credential(&provider, endpoint, &profiles)
                    .is_none()
            );
        }
    }

    #[test]
    fn saved_gateway_key_requires_a_matching_resolved_route() {
        let account = uuid::Uuid::new_v4().simple().to_string();
        let endpoint = Route::new(&account, "other-gateway")
            .unwrap()
            .endpoint(Adapter::ChatCompletions)
            .unwrap();
        let provider = Provider::named("cloudflare-ai-gateway").unwrap();
        let profiles = Profiles::parse_with_cloudflare_routes(
            &json!({"providers":{"cloudflare-ai-gateway":{"baseUrl":endpoint}}}),
            None,
            None,
        )
        .unwrap();
        assert!(profiles.has_provider_endpoint(&provider, &endpoint));
        let keys = Keys::parse(&json!({"cloudflare-ai-gateway":{
            "type":"api_key","key":"fixture","env":{"CLOUDFLARE_GATEWAY_ID":"saved-gateway"},
        }}));
        assert!(
            keys.custom_credential(&provider, &endpoint, &profiles)
                .is_none()
        );
    }

    #[test]
    fn configured_gateway_fills_missing_route_identifiers() {
        let route = Route::new("account123", "configured").unwrap();
        assert_eq!(
            Route::with_route_identifiers(None, None, Some(route.clone())),
            Some(route.clone())
        );
        assert_eq!(
            Route::with_route_identifiers(Some("account123"), None, Some(route.clone())),
            Some(route.clone())
        );
        assert_eq!(
            Route::with_route_identifiers(Some("other"), None, Some(route)),
            None
        );
    }

    #[tokio::test]
    async fn cloudflare_gateway_adapters_send_only_gateway_authentication() {
        let provider = Provider::named("cloudflare-ai-gateway").unwrap();
        for (adapter, path, request_path) in [
            (
                Adapter::ChatCompletions,
                "/compat/chat/completions",
                "/compat/chat/completions",
            ),
            (
                Adapter::Anthropic,
                "/anthropic/v1/messages",
                "/anthropic/v1/messages?beta=true",
            ),
            (
                Adapter::OpenAiResponses,
                "/openai/responses",
                "/openai/responses",
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}{path}", listener.local_addr().unwrap());
            let worker = std::thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).unwrap();
                assert_eq!(line.trim_end(), format!("POST {request_path} HTTP/1.1"));
                let mut auth_count = 0;
                loop {
                    line.clear();
                    socket.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    let line = line.to_ascii_lowercase();
                    assert!(!line.starts_with("authorization:"));
                    assert!(!line.starts_with("x-api-key:"));
                    if line.trim_end() == "cf-aig-authorization: bearer gateway-fixture" {
                        auth_count += 1;
                    }
                }
                assert_eq!(auth_count, 1);
                write!(
                    socket.get_mut(),
                    "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            });
            let client = provider
                .connect(adapter, &endpoint, Some("gateway-fixture".into()), false)
                .unwrap();
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![pi_rs::Message::User {
                    content: "hello".into(),
                }],
                tools: vec![],
                max_output_tokens: 64,
            };
            assert!(client.stream(request).await.is_err());
            worker.join().unwrap();
        }
    }
}
