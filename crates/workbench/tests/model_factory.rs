use pi_rs::{HostAuthBuilder, HostCredential, HostCredentialKind, ModelCatalogBuilder};
use pi_rs_workbench::model_factory::{ModelClientFactory, NativeModelApi};

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

#[tokio::test]
async fn model_requests_use_authorized_routes_without_metadata_probes() {
    use pi_rs::ModelRequest;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
