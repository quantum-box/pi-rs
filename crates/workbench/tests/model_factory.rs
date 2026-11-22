use pi_rs::{HostAuthBuilder, HostCredential, HostCredentialKind, ModelCatalogBuilder};
use pi_rs_workbench::model_factory::{ModelClientFactory, NativeModelApi};

#[test]
fn model_factory_builds_an_exact_catalog_with_host_metadata() {
    use pi_rs::CatalogModelInfo;
    use pi_rs_workbench::model_factory::NativeModelDefinition;

    let endpoint = "https://api.example.test/v1/chat/completions";
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "example",
        endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
    )
    .unwrap();
    let factory = ModelClientFactory::new(auth.build());
    let definition = NativeModelDefinition {
        api: NativeModelApi::OpenAiChat,
        provider: "example",
        endpoint,
        model: "demo",
        info: Some(CatalogModelInfo {
            display_name: Some("Demo model".into()),
            ..Default::default()
        }),
    };
    let catalog = factory.catalog([definition.clone()]).unwrap();
    assert_eq!(
        catalog
            .info("example", "demo")
            .unwrap()
            .display_name
            .as_deref(),
        Some("Demo model")
    );
    assert!(
        factory
            .catalog([definition.clone(), definition.clone()])
            .is_err()
    );
    assert!(
        factory
            .catalog([NativeModelDefinition {
                endpoint: "https://ungranted.example.test/v1/chat/completions",
                ..definition
            }])
            .is_err()
    );
    assert!(catalog.session_builder("example", "demo").is_ok());
}

#[test]
fn factory_uses_only_exact_host_authorized_routes_and_credential_kinds() {
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "custom",
        "https://api.example/v1/responses",
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
    )
    .unwrap();
    auth.register(
        "anthropic",
        "https://api.example/v1/messages",
        HostCredential::new(HostCredentialKind::Bearer, "fixture-token").unwrap(),
    )
    .unwrap();
    let factory = ModelClientFactory::new(auth.build());
    let mut catalog = ModelCatalogBuilder::new();
    factory
        .register(
            &mut catalog,
            NativeModelApi::OpenAiResponses,
            "custom",
            "https://api.example/v1/responses",
            "example",
        )
        .unwrap();
    assert!(
        factory
            .register(
                &mut catalog,
                NativeModelApi::OpenAiResponses,
                "custom",
                "https://api.example/v1/responses",
                "example"
            )
            .is_err()
    );
    assert!(
        factory
            .client(
                NativeModelApi::OpenAiResponses,
                "custom",
                "https://other.example/v1/responses"
            )
            .is_err()
    );
    assert!(
        factory
            .client(
                NativeModelApi::GoogleGemini,
                "anthropic",
                "https://api.example/v1/messages"
            )
            .is_err()
    );
    assert!(
        factory
            .client(
                NativeModelApi::ChatGptResponses,
                "anthropic",
                "https://api.example/v1/messages"
            )
            .is_err()
    );
    assert!(
        factory
            .client(
                NativeModelApi::AnthropicMessages,
                "anthropic",
                "https://api.example/v1/messages"
            )
            .is_ok()
    );
    assert!(catalog.build().get("custom", "example").is_some());
}

#[test]
fn invalid_endpoint_and_secrets_do_not_leak_in_errors() {
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "provider",
        "http://public.example/v1",
        HostCredential::new(HostCredentialKind::ApiKey, "private-fixture").unwrap(),
    )
    .unwrap();
    let factory = ModelClientFactory::new(auth.build());
    let error = match factory.client(
        NativeModelApi::OpenAiResponses,
        "provider",
        "http://public.example/v1",
    ) {
        Ok(_) => panic!("insecure endpoint accepted"),
        Err(error) => error,
    };
    assert!(!error.to_string().contains("private-fixture"));
}

async fn capture_model_headers(
    listener: tokio::net::TcpListener,
    model: std::sync::Arc<dyn pi_rs::Model>,
    request: pi_rs::ModelRequest,
) -> String {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let serve = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 2048];
        loop {
            let count = socket.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        socket
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        String::from_utf8(bytes).unwrap()
    };
    let (captured, _) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(serve, model.stream(request))
    })
    .await
    .unwrap();
    captured
}

#[tokio::test]
async fn model_requests_use_authorized_routes_without_metadata_probes() {
    use pi_rs::ModelRequest;
    use std::time::Duration;
    use tokio::net::TcpListener;

    for (api, provider, endpoint_path, expected_path, auth_header) in [
        (
            NativeModelApi::OpenAiChat,
            "chat",
            "/v1/chat/completions",
            "/v1/chat/completions",
            "authorization: Bearer fixture-key",
        ),
        (
            NativeModelApi::OpenAiResponses,
            "custom",
            "/v1/responses",
            "/v1/responses",
            "authorization: Bearer fixture-key",
        ),
        (
            NativeModelApi::AnthropicMessages,
            "anthropic",
            "/v1/messages",
            "/v1/messages",
            "x-api-key: fixture-key",
        ),
        (
            NativeModelApi::GoogleGemini,
            "google",
            "/v1beta",
            "/v1beta/models/test-model:streamGenerateContent?alt=sse",
            "x-goog-api-key: fixture-key",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}{endpoint_path}", listener.local_addr().unwrap());
        let mut auth = HostAuthBuilder::new();
        auth.register(
            provider,
            &endpoint,
            HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
        )
        .unwrap();
        let model = ModelClientFactory::new(auth.build())
            .client(api, provider, &endpoint)
            .unwrap();
        let _ = model.context_window("test-model").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        let request = ModelRequest {
            model: "test-model".into(),
            messages: vec![pi_rs::Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let captured = capture_model_headers(listener, model, request).await;
        assert!(
            captured.starts_with(&format!("POST {expected_path} HTTP/1.1\r\n")),
            "{captured}"
        );
        assert!(
            captured
                .to_ascii_lowercase()
                .contains(&auth_header.to_ascii_lowercase())
        );
    }
}

#[test]
fn anonymous_chat_is_explicit_and_validates_provider() {
    let factory = ModelClientFactory::default();
    assert!(
        factory
            .anonymous_chat("ollama", "http://127.0.0.1:11434/v1/chat/completions")
            .is_ok()
    );
    assert!(
        factory
            .anonymous_chat(" ", "http://127.0.0.1:11434/v1/chat/completions")
            .is_err()
    );
    assert!(
        factory
            .client(
                NativeModelApi::OpenAiChat,
                "ollama",
                "http://127.0.0.1:11434/v1/chat/completions"
            )
            .is_err()
    );
}

#[tokio::test]
async fn typed_chat_supports_host_compatibility_and_explicit_anonymous_requests() {
    use pi_rs::{Message, Model, ModelRequest};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    for anonymous in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let mut auth = HostAuthBuilder::new();
        auth.register(
            "custom",
            &endpoint,
            HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
        )
        .unwrap();
        let factory = ModelClientFactory::new(auth.build());
        let client = if anonymous {
            factory.anonymous_chat("custom", &endpoint).unwrap()
        } else {
            factory.chat_client("custom", &endpoint).unwrap()
        };
        let compatibility =
            serde_json::from_value(serde_json::json!({"maxTokensField":"max_completion_tokens"}))
                .unwrap();
        let client = client.with_chat_compatibility(compatibility, None, false, true);
        let request = ModelRequest {
            model: "example".into(),
            messages: vec![Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 2048];
            let header_end = loop {
                let count = socket.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while bytes.len() - header_end < length {
                let count = socket.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                bytes.extend_from_slice(&chunk[..count]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            (
                headers,
                serde_json::from_slice::<serde_json::Value>(
                    &bytes[header_end..header_end + length],
                )
                .unwrap(),
            )
        };
        let ((headers, body), _) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(serve, client.stream(request))
        })
        .await
        .unwrap();
        assert_eq!(body["max_completion_tokens"], 16);
        assert!(body.get("max_tokens").is_none());
        let has_auth = headers
            .lines()
            .any(|line| line.to_ascii_lowercase().starts_with("authorization:"));
        assert_eq!(has_auth, !anonymous);
    }
}

#[test]
fn typed_clients_allow_host_configuration_without_changing_credential_rules() {
    let mut auth = HostAuthBuilder::new();
    for (provider, endpoint, kind) in [
        (
            "custom",
            "https://example.com/v1/responses",
            HostCredentialKind::ApiKey,
        ),
        (
            "anthropic",
            "https://example.com/v1/messages",
            HostCredentialKind::Bearer,
        ),
        (
            "google",
            "https://example.com/v1beta",
            HostCredentialKind::ApiKey,
        ),
    ] {
        auth.register(
            provider,
            endpoint,
            HostCredential::new(kind, "fixture-key").unwrap(),
        )
        .unwrap();
    }
    let factory = ModelClientFactory::new(auth.build());
    let _ = factory
        .responses_client("custom", "https://example.com/v1/responses")
        .unwrap()
        .with_thinking(Some("low"));
    let _ = factory
        .anthropic_client("anthropic", "https://example.com/v1/messages")
        .unwrap()
        .with_thinking(Some("low"));
    let _ = factory
        .gemini_client("google", "https://example.com/v1beta")
        .unwrap()
        .with_thinking(Some("low"));
    assert!(
        factory
            .responses_client("anthropic", "https://example.com/v1/messages")
            .is_err()
    );
    assert!(
        factory
            .gemini_client("anthropic", "https://example.com/v1/messages")
            .is_err()
    );
}

#[test]
fn typed_chatgpt_client_enforces_provider_and_plan_endpoint() {
    let mut auth = HostAuthBuilder::new();
    for (provider, endpoint) in [
        ("openai-chatgpt", pi_rs::openai::DEFAULT_ENDPOINT),
        ("openai-chatgpt", "https://other.example/v1/responses"),
        ("other", pi_rs::openai::DEFAULT_ENDPOINT),
    ] {
        auth.register(
            provider,
            endpoint,
            HostCredential::new(HostCredentialKind::Bearer, "fixture-token").unwrap(),
        )
        .unwrap();
    }
    let factory = ModelClientFactory::new(auth.build());
    let _ = factory
        .chatgpt_client("openai-chatgpt", pi_rs::openai::DEFAULT_ENDPOINT)
        .unwrap()
        .with_thinking(Some("low"));
    assert!(
        factory
            .chatgpt_client("openai-chatgpt", "https://other.example/v1/responses")
            .is_err()
    );
    assert!(
        factory
            .chatgpt_client("other", pi_rs::openai::DEFAULT_ENDPOINT)
            .is_err()
    );
}

#[test]
fn auth_file_snapshot_binds_only_granted_routes() {
    use pi_rs_workbench::auth_factory::{AuthFileReadGrants, AuthRoute};

    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("auth.json");
    let endpoint = "https://model.example.test/v1/chat/completions";
    let other = "https://other.example.test/v1/chat/completions";
    let keys = serde_json::json!({
        "compatible": { "type": "api_key", "key": "prefix-${TOKEN}-$!$$", "env": { "TOKEN": "saved" } },
        "malformed": { "type": "api_key", "key": "${bad-$INNER}-${TOKEN}-${open-$TOKEN", "env": { "INNER": "private", "TOKEN": "saved" } },
        "bound": { "type": "api_key", "key": "$LITERAL", "literal": true, "endpoint": endpoint },
        "command": { "type": "api_key", "key": "!printf should-not-be-read" },
        "process_env": { "type": "api_key", "key": "$PATH" },
        "login": { "type": "oauth", "access": "oauth-token", "refresh": "refresh-token", "expires": 9999999999999_u64 },
        "ungranted": { "type": "api_key", "key": "private" },
    });
    std::fs::write(&file, format!("\u{feff}{keys}")).unwrap();
    let grants = AuthFileReadGrants {
        file: file.clone(),
        routes: [
            ("compatible", endpoint),
            ("malformed", endpoint),
            ("bound", endpoint),
            ("bound", other),
            ("command", endpoint),
            ("process_env", endpoint),
            ("login", endpoint),
        ]
        .into_iter()
        .map(|(provider, endpoint)| AuthRoute {
            provider: provider.into(),
            endpoint: endpoint.into(),
        })
        .collect(),
    };
    let auth = grants.load().unwrap();
    assert_eq!(
        auth.get("compatible", endpoint).unwrap().secret(),
        "prefix-saved-!$"
    );
    assert_eq!(auth.get("bound", endpoint).unwrap().secret(), "$LITERAL");
    assert_eq!(
        auth.get("malformed", endpoint).unwrap().secret(),
        "${bad-$INNER}-saved-${open-saved"
    );
    for provider in ["command", "process_env", "login", "ungranted"] {
        assert!(auth.get(provider, endpoint).is_none());
    }
    assert!(auth.get("bound", other).is_none());
    assert!(auth.get("compatible", other).is_none());

    let factory = ModelClientFactory::new(auth.clone());
    assert!(
        factory
            .client(NativeModelApi::OpenAiChat, "compatible", endpoint)
            .is_ok()
    );
    assert!(
        factory
            .client(NativeModelApi::OpenAiChat, "bound", other)
            .is_err()
    );

    // A malformed replacement cannot disclose credentials through errors or
    // invalidate the key previously supplied to a host.
    std::fs::write(
        &file,
        r#"{"compatible": {"type":"api_key","key":"private value"}}"#,
    )
    .unwrap();
    let error = grants.load().err().unwrap().to_string();
    assert!(error.contains("invalid saved API key"));
    assert!(!error.contains("private value"));
    assert_eq!(
        auth.get("compatible", endpoint).unwrap().secret(),
        "prefix-saved-!$"
    );
    let too_large = serde_json::json!({
        "compatible": { "type": "api_key", "key": "${A}".repeat(100), "env": { "A": "x".repeat(100) } }
    });
    std::fs::write(&file, too_large.to_string()).unwrap();
    assert!(
        grants
            .load()
            .err()
            .unwrap()
            .to_string()
            .contains("exceeds limit")
    );
    std::fs::remove_file(&file).unwrap();
    assert!(grants.load().unwrap().get("compatible", endpoint).is_none());
}

#[test]
fn granted_auth_only_resolves_real_variables_on_matching_routes() {
    use pi_rs_workbench::auth_factory::{AuthFileReadGrants, AuthRoute};
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let auth_path = directory.path().join("auth.json");
    let endpoint = "https://example.test/v1/messages";
    std::fs::write(&auth_path, json!({
        "allowed": {"type":"api_key","key":"${BAD$SECRET}-${LOCAL}-$GRANTED","env":{"LOCAL":"file"}},
        "unmatched": {"type":"api_key","key":"$SECRET","endpoint":"https://outside.example.test"},
        "ungranted": {"type":"api_key","key":"$SECRET"},
    }).to_string()).unwrap();
    let grants = AuthFileReadGrants {
        file: auth_path,
        routes: ["allowed", "unmatched"]
            .into_iter()
            .map(|provider| AuthRoute {
                provider: provider.into(),
                endpoint: endpoint.into(),
            })
            .collect(),
    };
    let mut requested = Vec::new();
    let auth = grants
        .load_with_variables(|route, variable| {
            requested.push((route.provider.clone(), variable.to_owned()));
            Some("host".into())
        })
        .unwrap();
    assert_eq!(requested, [("allowed".into(), "GRANTED".into())]);
    assert_eq!(
        auth.get("allowed", endpoint).unwrap().secret(),
        "${BAD$SECRET}-file-host"
    );
    assert!(auth.get("unmatched", endpoint).is_none());
}
#[tokio::test]
async fn granted_codex_model_routes_use_the_codex_bearer_protocol() {
    use base64::Engine;
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("models.json");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/backend-api/codex/responses",
        listener.local_addr().unwrap()
    );
    let base_url = "https://chatgpt.com/backend-api/codex";
    std::fs::write(
        &file,
        json!({"providers":{"openai-codex":{
            "api":"openai-codex-responses", "baseUrl":base_url, "models":[{"id":"fixture"}]
        }}})
        .to_string(),
    )
    .unwrap();
    let route = ModelFileRoute {
        provider: "openai-codex".into(),
        api: NativeModelApi::OpenAiCodexResponses,
        base_url: base_url.into(),
        endpoint: endpoint.clone(),
    };
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"test-account"}}"#);
    let token = format!("fixture.{payload}.signature");
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "openai-codex",
        &endpoint,
        HostCredential::new(HostCredentialKind::Bearer, token).unwrap(),
    )
    .unwrap();
    let factory = ModelClientFactory::new(auth.build());
    let catalog = ModelFileReadGrants {
        file,
        routes: vec![route],
    }
    .load(&factory)
    .unwrap();
    let model = catalog.get("openai-codex", "fixture").unwrap();
    let captured = capture_model_headers(
        listener,
        model,
        pi_rs::ModelRequest {
            model: "fixture".into(),
            messages: vec![pi_rs::Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 16,
        },
    )
    .await;
    assert!(captured.starts_with("POST /backend-api/codex/responses HTTP/1.1\r\n"));
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("chatgpt-account-id: test-account")
    );
}

#[tokio::test]
async fn granted_model_file_composes_saved_auth_with_exact_api_and_base_routes() {
    use pi_rs_workbench::auth_factory::{AuthFileReadGrants, AuthRoute};
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let auth_path = directory.path().join("auth.json");
    let models_path = directory.path().join("models.json");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let private_base = "https://relay.example.test/v1";
    let endpoint = format!("{base_url}/chat/completions");
    let responses_endpoint = "https://relay.example.test/v1/responses";
    std::fs::write(
        &auth_path,
        json!({
            "relay": {"type":"api_key","key":"token-${TOKEN}"},
        })
        .to_string(),
    )
    .unwrap();
    let auth_grants = AuthFileReadGrants {
        file: auth_path,
        routes: [endpoint.as_str(), responses_endpoint]
            .into_iter()
            .map(|endpoint| AuthRoute {
                provider: "relay".into(),
                endpoint: endpoint.into(),
            })
            .collect(),
    };
    assert!(
        auth_grants
            .load()
            .unwrap()
            .get("relay", &endpoint)
            .is_none()
    );
    let auth = auth_grants
        .load_with_variables(|route, name| {
            (route.provider == "relay" && name == "TOKEN").then(|| "fixture".into())
        })
        .unwrap();
    assert_eq!(
        auth.get("relay", &endpoint).unwrap().secret(),
        "token-fixture"
    );
    let factory = ModelClientFactory::new(auth);
    let definition = json!({
        "providers":{
            "relay":{
                "baseUrl":base_url,
                "api":"openai-completions",
                "apiKey":"!untrusted-command",
                "headers":{"Authorization":"Bearer should-not-be-used"},
                "modelOverrides":{
                    "chat":{"name":"Final display","contextWindow":144000,"cost":{"input":3},"baseUrl":"https://outside.example.test"},
                    "missing":{"name":"never registered"}
                },
                "models":[
                    {"id":"chat","name":"Demo chat","reasoning":true,"input":["text","image"],"maxTokens":2400,
                     "cost":{"input":1,"output":2,"cacheRead":0.5,"cacheWrite":0.125}},
                    {"id":"responses","api":"openai-responses","baseUrl":private_base},
                    {"id":"wrong-api","api":"anthropic-messages"},
                    {"id":"redirect","baseUrl":"https://outside.example.test/v1"},
                    {"id":"classifier","type":"classifier"},
                ]
            },
            "unknown":{"models":true}
        }
    });
    std::fs::write(&models_path, format!("\u{feff}{definition}")).unwrap();
    let mut grants = ModelFileReadGrants {
        file: models_path.clone(),
        routes: vec![
            ModelFileRoute {
                provider: "relay".into(),
                api: NativeModelApi::OpenAiChat,
                base_url: base_url.clone(),
                endpoint: endpoint.clone(),
            },
            ModelFileRoute {
                provider: "relay".into(),
                api: NativeModelApi::OpenAiResponses,
                base_url: private_base.into(),
                endpoint: responses_endpoint.into(),
            },
        ],
    };

    let catalog = grants.load(&factory).unwrap();
    assert_eq!(
        catalog.identities().collect::<Vec<_>>(),
        [("relay", "chat"), ("relay", "responses")]
    );
    let chat = catalog.info("relay", "chat").unwrap();
    assert_eq!(chat.display_name.as_deref(), Some("Final display"));
    assert_eq!(chat.context_window, Some(144_000));
    assert_eq!(
        chat.cost.as_ref().unwrap().input.as_ref().unwrap().as_u64(),
        Some(3)
    );
    assert_eq!(
        chat.cost
            .as_ref()
            .unwrap()
            .output
            .as_ref()
            .unwrap()
            .as_u64(),
        Some(2)
    );
    assert_eq!(chat.max_output_tokens, Some(2400));
    assert_eq!(chat.reasoning, Some(true));
    assert_eq!(chat.images, Some(true));
    assert_eq!(
        chat.cost
            .as_ref()
            .unwrap()
            .cache_read
            .as_ref()
            .unwrap()
            .as_f64(),
        Some(0.5)
    );

    let responses = catalog.info("relay", "responses").unwrap();
    assert_eq!(responses.context_window, Some(128_000));
    assert_eq!(responses.max_output_tokens, Some(16_384));
    assert_eq!(responses.reasoning, Some(false));
    assert!(catalog.session_builder("relay", "responses").is_ok());
    let captured = capture_model_headers(
        listener,
        catalog.get("relay", "chat").unwrap(),
        pi_rs::ModelRequest {
            model: "chat".into(),
            messages: vec![pi_rs::Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 16,
        },
    )
    .await;
    assert!(captured.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("authorization: bearer token-fixture")
    );
    assert!(!captured.contains("should-not-be-used"));

    assert!(grants.load(&ModelClientFactory::default()).is_err());
    grants.routes.push(grants.routes[0].clone());
    assert!(grants.load(&factory).is_err());
    grants.routes.pop();
    let mut duplicate = definition;
    let models = duplicate["providers"]["relay"]["models"]
        .as_array_mut()
        .unwrap();
    models.push(models[0].clone());
    std::fs::write(&models_path, duplicate.to_string()).unwrap();
    assert!(grants.load(&factory).is_err());
    assert_eq!(catalog.identities().len(), 2);
    std::fs::remove_file(models_path).unwrap();
    assert_eq!(grants.load(&factory).unwrap().identities().len(), 0);
}

#[tokio::test]
async fn configured_model_key_requires_exact_opt_in_and_yields_to_saved_auth() {
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use tokio::net::TcpListener;

    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("models.json");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut route = ModelFileRoute {
        provider: "relay".into(),
        api: NativeModelApi::OpenAiChat,
        base_url: "https://relay.example.test/v1".into(),
        endpoint: format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        ),
    };
    let mut grants = ModelFileReadGrants {
        file: file.clone(),
        routes: vec![route.clone()],
    };
    let fixture = r#"{
        // Pi provider configuration
        "providers": {
            "relay": {
                "baseUrl": "https://relay.example.test/v1",
                "api": "openai-completions",
                "apiKey": "cfg-$KEY-$!$$",
                "models": [
                    {"id":"one","name":"Quoted \"// not a comment\"",},
                    {"id":"two"}, // trailing array separator
                ],
            },
        },
    }"#;
    std::fs::write(&file, fixture).unwrap();
    let empty = ModelClientFactory::default();
    assert!(grants.load(&empty).is_err());
    assert!(
        grants
            .load_with_api_keys(
                &empty,
                &[ModelFileRoute {
                    endpoint: "https://other.example.test/v1/chat/completions".into(),
                    ..route.clone()
                }],
                |_, _| panic!("ungranted route looked up a variable"),
            )
            .is_err()
    );
    let mut requested = Vec::new();
    let catalog = grants
        .load_with_api_keys(&empty, std::slice::from_ref(&route), |grant, name| {
            requested.push((grant.clone(), name.to_owned()));
            Some("fixture".into())
        })
        .unwrap();
    assert_eq!(requested, [(route.clone(), "KEY".into())]);
    assert_eq!(catalog.identities().len(), 2);
    assert_eq!(
        catalog
            .info("relay", "one")
            .unwrap()
            .display_name
            .as_deref(),
        Some(r#"Quoted "// not a comment""#),
    );
    let request = || pi_rs::ModelRequest {
        model: "one".into(),
        messages: vec![pi_rs::Message::User {
            content: "hello".into(),
        }],
        tools: vec![],
        max_output_tokens: 16,
    };
    let captured =
        capture_model_headers(listener, catalog.get("relay", "one").unwrap(), request()).await;
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("authorization: bearer cfg-fixture-!$")
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    route.endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    grants.routes = vec![route.clone()];
    let mut saved = HostAuthBuilder::new();
    saved
        .register(
            "relay",
            &route.endpoint,
            HostCredential::new(HostCredentialKind::ApiKey, "stored").unwrap(),
        )
        .unwrap();
    let factory = ModelClientFactory::new(saved.build());
    std::fs::write(&file, fixture.replace(r#""cfg-$KEY-$!$$""#, "false")).unwrap();
    let catalog = grants
        .load_with_api_keys(&factory, std::slice::from_ref(&route), |_, _| {
            panic!("saved key must win without reading configured key variables")
        })
        .unwrap();
    let captured =
        capture_model_headers(listener, catalog.get("relay", "one").unwrap(), request()).await;
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("authorization: bearer stored")
    );
    assert!(!captured.contains("cfg-fixture"));

    std::fs::write(&file, fixture.replace("cfg-$KEY-$!$$", "!printf ignored")).unwrap();
    assert!(
        grants
            .load_with_api_keys(&empty, std::slice::from_ref(&route), |_, _| {
                panic!("command requested key expansion")
            })
            .is_err()
    );
    std::fs::write(&file, fixture).unwrap();
    assert!(
        grants
            .load_with_api_keys(&empty, std::slice::from_ref(&route), |_, _| None)
            .is_err()
    );
    std::fs::write(
        &file,
        fixture.replace("// Pi provider configuration", "/* block comment */"),
    )
    .unwrap();
    assert!(grants.load(&factory).is_err());
}
