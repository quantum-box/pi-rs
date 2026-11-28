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
    capture_model_response(
        &listener,
        model,
        request,
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await
    .0
}

async fn capture_model_response(
    listener: &tokio::net::TcpListener,
    model: std::sync::Arc<dyn pi_rs::Model>,
    request: pi_rs::ModelRequest,
    response: &str,
) -> (String, pi_rs::Result<pi_rs::ModelStream>) {
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
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                bytes.truncate(end + 4);
                break;
            }
        }
        socket.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(bytes).unwrap()
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(serve, model.stream(request))
    })
    .await
    .unwrap()
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

#[tokio::test]
async fn auth_reload_publishes_only_a_complete_catalog_and_keeps_clients_route_bound() {
    use pi_rs::{ModelCatalog, ModelRequest};
    use pi_rs_workbench::auth_factory::{AuthFileReadGrants, AuthRoute};
    use pi_rs_workbench::model_factory::NativeModelDefinition;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;

    let directory = tempfile::tempdir().unwrap();
    let granted = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ungranted = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        granted.local_addr().unwrap()
    );
    let other_endpoint = format!("http://{}/ungranted", ungranted.local_addr().unwrap());
    let grants = AuthFileReadGrants {
        file: directory.path().join("auth.json"),
        routes: vec![AuthRoute {
            provider: "relay".into(),
            endpoint: endpoint.clone(),
        }],
    };
    let definition = |model| NativeModelDefinition {
        api: NativeModelApi::OpenAiChat,
        provider: "relay",
        endpoint: &endpoint,
        model,
        info: None,
    };
    let save = |key| {
        let auth = serde_json::json!({ "relay": { "type": "api_key", "key": key } });
        std::fs::write(&grants.file, auth.to_string()).unwrap();
    };
    let reload = |live: &mut ModelCatalog, definitions: Vec<NativeModelDefinition<'_>>| {
        let factory = ModelClientFactory::new(grants.load()?);
        *live = factory.catalog(definitions)?;
        Ok::<_, pi_rs::Error>(())
    };

    save("Fixture-Old");
    let mut live = ModelCatalog::default();
    reload(&mut live, vec![definition("stable")]).unwrap();
    let retained = live.get("relay", "stable").unwrap();
    let old_client = Arc::downgrade(&retained);

    std::fs::write(&grants.file, "{").unwrap();
    assert!(reload(&mut live, vec![definition("stable")]).is_err());
    assert!(Arc::ptr_eq(
        &live.get("relay", "stable").unwrap(),
        &retained
    ));

    save("Fixture-New");
    let error = reload(
        &mut live,
        vec![
            definition("candidate-only"),
            NativeModelDefinition {
                endpoint: &other_endpoint,
                ..definition("ungranted")
            },
        ],
    )
    .unwrap_err();
    assert!(
        matches!(error, pi_rs::Error::Config(message) if message == "model credential is not registered")
    );
    assert!(Arc::ptr_eq(
        &live.get("relay", "stable").unwrap(),
        &retained
    ));
    assert!(live.get("relay", "candidate-only").is_none());

    reload(&mut live, vec![definition("stable")]).unwrap();
    assert!(live.get("relay", "ungranted").is_none());
    let replacement = live.get("relay", "stable").unwrap();
    assert!(!Arc::ptr_eq(&replacement, &retained));
    let request = || ModelRequest {
        model: "stable".into(),
        messages: vec![pi_rs::Message::User {
            content: "hello".into(),
        }],
        tools: vec![],
        max_output_tokens: 16,
    };
    let authorization = |headers: &str, expected: &str| {
        let values = headers.lines().filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("authorization")
                .then_some(value.trim())
        });
        assert_eq!(values.collect::<Vec<_>>(), [expected]);
    };
    let body = "data: [DONE]\n\n";
    let success = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    for (model, key) in [
        (replacement.clone(), "Bearer Fixture-New"),
        (retained.clone(), "Bearer Fixture-Old"),
    ] {
        let (headers, result) = capture_model_response(&granted, model, request(), &success).await;
        assert!(result.is_ok());
        authorization(&headers, key);
    }
    drop(retained);
    assert!(old_client.upgrade().is_none());

    let redirect = format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {other_endpoint}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let (headers, result) =
        capture_model_response(&granted, replacement, request(), &redirect).await;
    let Err(error) = result else {
        panic!("the redirected request must fail");
    };
    assert!(error.to_string().contains("307"));
    authorization(&headers, "Bearer Fixture-New");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), ungranted.accept())
            .await
            .is_err(),
        "the ungranted endpoint received a connection"
    );
}

#[test]
fn bedrock_catalog_uses_only_granted_regional_runtime_endpoints() {
    use pi_rs_workbench::model_factory::NativeModelDefinition;

    let ordinary = "https://bedrock-runtime.us-west-2.amazonaws.com";
    let fips = "https://bedrock-runtime-fips.us-east-1.amazonaws.com";
    let custom = "https://bedrock.gateway.example/runtime";
    let mut auth = HostAuthBuilder::new();
    for (endpoint, kind) in [
        (ordinary, HostCredentialKind::ApiKey),
        (fips, HostCredentialKind::Bearer),
        (custom, HostCredentialKind::ApiKey),
    ] {
        auth.register(
            "amazon-bedrock",
            endpoint,
            HostCredential::new(kind, "fixture-bedrock-token").unwrap(),
        )
        .unwrap();
    }
    let factory = ModelClientFactory::new(auth.build());
    let definition = NativeModelDefinition {
        api: NativeModelApi::BedrockConverse,
        provider: "amazon-bedrock",
        endpoint: ordinary,
        model: "example",
        info: None,
    };
    let catalog = factory.catalog([definition.clone()]).unwrap();
    assert!(catalog.session_builder("amazon-bedrock", "example").is_ok());
    assert!(
        factory
            .catalog([NativeModelDefinition {
                endpoint: fips,
                ..definition.clone()
            }])
            .is_ok()
    );
    for endpoint in [
        "https://bedrock-runtime.us-west-2.amazonaws.com/",
        "https://bedrock-runtime.us-west-2.amazonaws.com.example",
        custom,
    ] {
        assert!(
            factory
                .catalog([NativeModelDefinition {
                    endpoint,
                    ..definition.clone()
                }])
                .is_err()
        );
    }
    assert!(
        factory
            .bedrock_client("amazon-bedrock", custom, "us-west-2")
            .is_ok()
    );
    assert!(
        factory
            .bedrock_client("alias", custom, "us-west-2")
            .is_err()
    );
    assert!(
        factory
            .bedrock_client("amazon-bedrock", custom, "us west 2")
            .is_err()
    );
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
        HostCredential::new(HostCredentialKind::Bearer, token.clone()).unwrap(),
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
    let bearer = format!("Bearer {token}");
    assert!(captured.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("authorization") && value.trim() == bearer
        })
    }));
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("chatgpt-account-id: test-account")
    );
}

#[tokio::test]
async fn bedrock_explicit_factory_prepares_and_sends_bearer_to_the_granted_route() {
    use pi_rs::{Message, Model, ModelRequest};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/gateway", listener.local_addr().unwrap());
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "amazon-bedrock",
        &endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-bedrock-token").unwrap(),
    )
    .unwrap();
    let factory = ModelClientFactory::new(auth.build());
    let model = factory
        .bedrock_client("amazon-bedrock", &endpoint, "eu-west-1")
        .unwrap();
    let request = ModelRequest {
        model: "test-model".into(),
        messages: vec![Message::User {
            content: "hello".into(),
        }],
        tools: vec![],
        max_output_tokens: 16,
    };
    let prepared = model.prepare_context(&request).await.unwrap().unwrap();
    assert!(prepared.context_identity().is_some());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );

    let serve = || async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (head, body) = read_model_json_request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        (head, body)
    };
    for client in [&model as &dyn Model, prepared.as_ref()] {
        let ((head, body), response) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(serve(), client.stream(request.clone()))
        })
        .await
        .unwrap();
        assert!(response.is_err());
        assert!(head.starts_with("POST /gateway/model/test-model/converse-stream HTTP/1.1\r\n"));
        assert!(
            head.lines().any(
                |line| line.eq_ignore_ascii_case("authorization: Bearer fixture-bedrock-token")
            )
        );
        assert_eq!(body["inferenceConfig"]["maxTokens"], 16);
        assert_eq!(body["messages"][0]["content"][0]["text"], "hello");
    }
}

#[test]
fn bedrock_explicit_factory_ignores_ambient_routing_and_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config");
    let credentials = directory.path().join("credentials");
    std::fs::write(
        &config,
        "[profile ambient]\nregion = ap-northeast-1\nendpoint_url = http://127.0.0.1:9\n",
    )
    .unwrap();
    std::fs::write(
        &credentials,
        "[ambient]\naws_access_key_id = ambient-key\naws_secret_access_key = ambient-secret\n",
    )
    .unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bedrock_explicit_factory_prepares_and_sends_bearer_to_the_granted_route",
        ])
        .env("AWS_CONFIG_FILE", config)
        .env("AWS_SHARED_CREDENTIALS_FILE", credentials)
        .env("AWS_PROFILE", "ambient")
        .env("AWS_REGION", "ap-northeast-1")
        .env("AWS_ACCESS_KEY_ID", "ambient-key")
        .env("AWS_SECRET_ACCESS_KEY", "ambient-secret")
        .env("AWS_BEARER_TOKEN_BEDROCK", "ambient-token")
        .env("AWS_ENDPOINT_URL", "http://127.0.0.1:9")
        .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", "http://127.0.0.1:9")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .env("http_proxy", "http://127.0.0.1:9")
        .env("https_proxy", "http://127.0.0.1:9")
        .env("no_proxy", "")
        .output()
        .unwrap();
    assert!(
        child.status.success() && String::from_utf8_lossy(&child.stdout).contains("1 passed"),
        "{}\n{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr),
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

fn load_file_thinking_catalog(declaration: serde_json::Value) -> pi_rs::ModelCatalog {
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("models.json");
    let base_url = "http://127.0.0.1:1/v1";
    let endpoint = format!("{base_url}/chat/completions");
    let mut provider = json!({"api":"openai-completions","baseUrl":base_url});
    provider
        .as_object_mut()
        .unwrap()
        .extend(declaration.as_object().unwrap().clone());
    std::fs::write(
        &file,
        json!({"providers":{"relay":provider,"ungranted":{"models":true}}}).to_string(),
    )
    .unwrap();
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "relay",
        &endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
    )
    .unwrap();
    ModelFileReadGrants {
        file,
        routes: vec![ModelFileRoute {
            provider: "relay".into(),
            api: NativeModelApi::OpenAiChat,
            base_url: base_url.into(),
            endpoint,
        }],
    }
    .load(&ModelClientFactory::new(auth.build()))
    .unwrap()
}

#[test]
fn file_thinking_levels() {
    let catalog = load_file_thinking_catalog(serde_json::json!({"models":[
        {"id":"mapped","reasoning":true,
         "thinkingLevelMap":{"off":null,"medium":null,"xhigh":"strong","max":null}},
        {"id":"legacy","reasoning":true},
        {"id":"legacy-nonreasoning","reasoning":false},
        {"id":"empty-map","reasoning":true,"thinkingLevelMap":{}},
        {"id":"nonreasoning","reasoning":false,"thinkingLevelMap":{"off":null,"max":"very-strong"}},
        {"id":"all-disabled","reasoning":true,
         "thinkingLevelMap":{"off":null,"minimal":null,"low":null,"medium":null,"high":null}}
    ]}));
    assert_eq!(
        catalog.thinking_levels("relay", "mapped").unwrap(),
        ["minimal", "low", "high", "xhigh"]
    );
    assert_eq!(catalog.thinking_levels("relay", "legacy"), None);
    assert_eq!(
        catalog.thinking_levels("relay", "legacy-nonreasoning"),
        None
    );
    assert_eq!(
        catalog.thinking_levels("relay", "empty-map").unwrap(),
        ["off", "minimal", "low", "medium", "high"]
    );
    assert_eq!(
        catalog.thinking_levels("relay", "nonreasoning").unwrap(),
        ["off"]
    );
    assert!(
        catalog
            .thinking_levels("relay", "all-disabled")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_thinking_overrides() {
    let catalog = load_file_thinking_catalog(serde_json::json!({
        "models":[
            {"id":"mapped","reasoning":true,
             "thinkingLevelMap":{"off":null,"medium":null,"xhigh":"strong","max":null}},
            {"id":"override-only","reasoning":false},
            {"id":"outside","baseUrl":"http://127.0.0.1:2/v1","thinkingLevelMap":true},
            {"id":"classifier","type":"classifier","thinkingLevelMap":true}
        ],
        "modelOverrides":{
            "mapped":{"thinkingLevelMap":{"medium":"normal","high":null,"xhigh":null,"max":"ultra"}},
            "override-only":{"reasoning":true,"thinkingLevelMap":{"low":null}},
            "outside":{"thinkingLevelMap":true},
            "classifier":{"thinkingLevelMap":true}
        }
    }));
    assert_eq!(
        catalog.identities().collect::<Vec<_>>(),
        [("relay", "mapped"), ("relay", "override-only")]
    );
    assert_eq!(
        catalog.thinking_levels("relay", "mapped").unwrap(),
        ["minimal", "low", "medium", "max"]
    );
    assert_eq!(
        catalog.thinking_levels("relay", "override-only").unwrap(),
        ["off", "minimal", "medium", "high"]
    );
}

#[test]
fn configured_model_key_grants_reject_codex_oauth_routes() {
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};

    let directory = tempfile::tempdir().unwrap();
    let route = ModelFileRoute {
        provider: "openai-codex".into(),
        api: NativeModelApi::OpenAiCodexResponses,
        base_url: "https://chatgpt.com/backend-api/codex".into(),
        endpoint: "https://chatgpt.com/backend-api/codex/responses".into(),
    };
    let models = ModelFileReadGrants {
        file: directory.path().join("models.json"),
        routes: vec![route.clone()],
    };
    let error = models
        .load_with_api_keys(&ModelClientFactory::default(), &[route], |_, _| None)
        .err()
        .expect("configured API key grant must not authorize Codex OAuth");
    assert!(error.to_string().contains("host-provided OAuth"));
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

#[test]
fn pi_messages_ignores_ambient_sdk_configuration() {
    let ambient_home = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "pi_messages_streams_on_explicit_sdk_routes"])
        .env("PI_RS_HOME", ambient_home.path())
        .env("PI_RADIUS_GATEWAY", "invalid-gateway")
        .env("RADIUS_API_KEY", "ambient-radius-key")
        .env("PI_CACHE_RETENTION", "long")
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("http_proxy", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .output()
        .unwrap();
    assert!(
        child.status.success() && String::from_utf8_lossy(&child.stdout).contains("1 passed"),
        "{}\n{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr),
    );
    assert!(
        std::fs::read_dir(ambient_home.path())
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn pi_messages_streams_on_explicit_sdk_routes() {
    use futures_util::TryStreamExt;
    use pi_rs::{Message, ModelApi, ModelEvent, ModelRequest, Usage};
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("http://{address}/private/messages");
    let api = NativeModelApi::PiMessages;
    let provider = "radius";
    let mut auth = HostAuthBuilder::new();
    auth.register(
        provider,
        &endpoint,
        HostCredential::new(HostCredentialKind::Bearer, "fixture-access").unwrap(),
    )
    .unwrap();
    let factory = ModelClientFactory::new(auth.build());
    assert!(
        factory
            .client(api, provider, &endpoint)
            .unwrap()
            .context_identity()
            .is_none()
    );
    for (provider, endpoint) in [
        ("Radius", endpoint.clone()),
        ("relay", endpoint.clone()),
        (
            provider,
            format!("http://localhost:{}/private/messages", address.port()),
        ),
        (provider, format!("http://{address}/private/messages/")),
    ] {
        assert!(factory.client(api, provider, &endpoint).is_err());
    }
    let empty_factory = ModelClientFactory::default();
    assert!(empty_factory.client(api, provider, &endpoint).is_err());

    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("models.json");
    let declared_base = "https://declared.example.invalid/v1";
    std::fs::write(
        &file,
        json!({"providers":{"radius":{
            "api":"pi-messages","baseUrl":declared_base,"apiKey":"fixture-file-key",
            "headers":{"Authorization":"Bearer ignored-file-header"},
            "models":[{"id":"fixture-model"},{"id":"ungranted","baseUrl":endpoint}]
        }}})
        .to_string(),
    )
    .unwrap();
    let grants = ModelFileReadGrants {
        file,
        routes: vec![ModelFileRoute {
            provider: provider.into(),
            api,
            base_url: declared_base.into(),
            endpoint: endpoint.clone(),
        }],
    };
    assert!(grants.load(&empty_factory).is_err());
    let file_catalog = grants
        .load_with_api_keys(&empty_factory, &grants.routes, |_, _| None)
        .unwrap();
    assert_eq!(
        file_catalog.identities().collect::<Vec<_>>(),
        [(provider, "fixture-model")]
    );
    let file_model = file_catalog.get(provider, "fixture-model").unwrap();
    let host_model = factory.client(api, provider, &endpoint).unwrap();

    let request = ModelRequest {
        model: "fixture-model".into(),
        messages: vec![
            Message::System {
                content: "system prompt".into(),
            },
            Message::User {
                content: "hello".into(),
            },
        ],
        tools: vec![],
        max_output_tokens: 16,
    };

    for (model, bearer) in [
        (host_model, "fixture-access"),
        (file_model, "fixture-file-key"),
    ] {
        let (window, prepared) = tokio::time::timeout(Duration::from_secs(5), async {
            (
                model.context_window(&request.model).await,
                model.prepare_context(&request).await,
            )
        })
        .await
        .unwrap();
        assert_eq!(window, None);
        assert!(prepared.unwrap().is_none());

        let (received_delta, wait_for_delta) = tokio::sync::oneshot::channel();
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (head, body) = read_model_json_request(&mut socket).await;
            assert!(head.starts_with("POST /private/messages HTTP/1.1\r\n"));
            assert!(
                head.lines()
                    .any(|line| line.eq_ignore_ascii_case("accept: text/event-stream"))
            );
            let auth = head
                .lines()
                .filter_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then_some(value.trim())
                })
                .collect::<Vec<_>>();
            assert_eq!(auth, [format!("Bearer {bearer}")]);
            assert_eq!(
                body,
                json!({
                    "model":"fixture-model",
                    "context":{"systemPrompt":"system prompt","messages":[
                        {"role":"user","content":"hello","timestamp":0}
                    ],"tools":[]},
                    "options":{"maxTokens":16}
                })
            );
            let first_events = concat!(
                "data: {\"type\":\"text_start\",\"contentIndex\":0}\n\n",
                "data: {\"type\":\"text_delta\",\"contentIndex\":0,\"delta\":\"hel\"}\n\n",
            );
            let remaining_events = concat!(
                "data: {\"type\":\"text_end\",\"contentIndex\":0,\"content\":\"hello\"}\n\n",
                "data: {\"type\":\"done\",\"reason\":\"stop\",\"responseModel\":\"routed-model\",\"usage\":{\"input\":3,\"output\":2,\"cacheRead\":4}}\n\n",
            );
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{first_events}",
                first_events.len() + remaining_events.len()
            ).as_bytes()).await.unwrap();
            wait_for_delta.await.unwrap();
            socket.write_all(remaining_events.as_bytes()).await.unwrap();
        };
        let (_, (first, remaining)) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(serve, async {
                let mut stream = model.stream(request.clone()).await.unwrap();
                let first = stream.try_next().await.unwrap().unwrap();
                received_delta.send(()).unwrap();
                (first, stream.try_collect::<Vec<_>>().await.unwrap())
            })
        })
        .await
        .unwrap();
        assert!(matches!(first, ModelEvent::Text(text) if text == "hel"));
        let [
            ModelEvent::Text(last),
            ModelEvent::Metadata(metadata),
            ModelEvent::Complete { calls, usage },
        ] = remaining.as_slice()
        else {
            panic!("unexpected Pi Messages events: {remaining:?}")
        };
        assert_eq!(last, "lo");
        assert_eq!(metadata.provider_api, Some(ModelApi::PiMessages));
        let identity = metadata.resolved_model.as_ref().unwrap();
        assert_eq!(
            (identity.provider.as_str(), identity.model.as_str()),
            (provider, "routed-model")
        );
        assert_eq!(metadata.cached_input_tokens, Some(4));
        assert!(calls.is_empty());
        let usage: Usage = usage.unwrap();
        assert_eq!((usage.input_tokens, usage.output_tokens), (7, 2));
    }
}

async fn capture_thinking_request(
    listener: &tokio::net::TcpListener,
    client: std::sync::Arc<dyn pi_rs::Model>,
    model: &str,
) -> (String, serde_json::Value) {
    use tokio::io::AsyncWriteExt;

    let serve = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let captured = read_model_json_request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        captured
    };
    let request = pi_rs::ModelRequest {
        model: model.into(),
        messages: vec![pi_rs::Message::User {
            content: "hello".into(),
        }],
        tools: vec![],
        max_output_tokens: 2_048,
    };
    let (captured, _) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(serve, client.stream(request))
    })
    .await
    .unwrap();
    captured
}

#[tokio::test]
async fn file_thinking_snapshot_binds_models_and_keys_once() {
    use pi_rs_workbench::model_file::{
        ModelFileBindingError as Rejection, ModelFileReadGrants, ModelFileRoute,
    };
    use serde_json::json;

    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("models.json");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let base = "https://relay.example.test/v1";
    let route = ModelFileRoute {
        provider: "relay".into(),
        api: NativeModelApi::OpenAiChat,
        base_url: base.into(),
        endpoint: endpoint.clone(),
    };
    let grants = ModelFileReadGrants {
        file: file.clone(),
        routes: vec![route.clone()],
    };
    let keys = std::slice::from_ref(&route);
    std::fs::write(&file, json!({"providers":{"relay":{
        "api":"openai-completions", "baseUrl":base, "apiKey":"${KEY}",
        "models":[
            {"id":"alpha","reasoning":true,"thinkingLevelMap":{"high":"old-a","low":null,"off":"off"}},
            {"id":"beta","reasoning":true,"thinkingLevelMap":{"high":"b-high","off":null}},
            {"id":"legacy","reasoning":true},
            {"id":"outside","reasoning":true,"baseUrl":"https://outside.example.test","thinkingLevelMap":false}
        ],
        "modelOverrides":{"alpha":{"thinkingLevelMap":{"high":"a-high"}}}
    }}}).to_string()).unwrap();

    let factory = ModelClientFactory::default();
    assert!(grants.snapshot(&factory).is_err());
    let wrong_key_route = ModelFileRoute {
        endpoint: format!("{endpoint}/other"),
        ..route.clone()
    };
    assert!(
        grants
            .snapshot_with_api_keys(&factory, &[wrong_key_route], |_, _| {
                panic!("ungranted key route must fail before lookup")
            })
            .is_err()
    );
    assert!(
        grants
            .snapshot_with_api_keys(&factory, keys, |_, _| Some("bad\nkey".into()))
            .is_err()
    );
    let mut lookups = 0;
    let from_file = grants
        .snapshot_with_api_keys(&factory, keys, |requested, name| {
            assert_eq!(requested, &route);
            assert_eq!(name, "KEY");
            lookups += 1;
            Some("file-key".into())
        })
        .unwrap();
    assert_eq!(lookups, 1);
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "relay",
        &endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "host-key").unwrap(),
    )
    .unwrap();
    let from_host = grants
        .snapshot_with_api_keys(&ModelClientFactory::new(auth.build()), keys, |_, _| {
            panic!("the exact host key takes precedence over the granted file key")
        })
        .unwrap();
    std::fs::remove_file(file).unwrap();

    for (model, level, rejection) in [
        ("outside", "high", Rejection::UnknownModel),
        ("alpha", "HIGH", Rejection::UnknownLevel),
        ("alpha", "low", Rejection::DisabledLevel),
        ("beta", "off", Rejection::DisabledLevel),
        ("beta", "xhigh", Rejection::MissingMapping),
        ("legacy", "high", Rejection::MissingMapping),
    ] {
        assert_eq!(
            from_file.bind_thinking("relay", model, level).err(),
            Some(rejection)
        );
    }
    assert_eq!(
        from_file.bind_thinking("Relay", "alpha", "high").err(),
        Some(Rejection::UnknownModel)
    );
    let alpha = from_file.bind_thinking("relay", "alpha", "high").unwrap();
    let wrong_model = pi_rs::ModelRequest {
        model: "beta".into(),
        messages: vec![],
        tools: vec![],
        max_output_tokens: 2_048,
    };
    assert!(matches!(
        alpha.prepare_context(&wrong_model).await,
        Err(pi_rs::Error::Config(_))
    ));
    assert!(matches!(
        alpha.stream(wrong_model).await,
        Err(pi_rs::Error::Config(_))
    ));

    for (snapshot, model, level, expected, key) in [
        (&from_file, "alpha", "high", "a-high", "file-key"),
        (&from_file, "beta", "high", "b-high", "file-key"),
        (&from_host, "alpha", "off", "off", "host-key"),
        (&from_host, "beta", "medium", "medium", "host-key"),
    ] {
        let client = snapshot.bind_thinking("relay", model, level).unwrap();
        let (head, body) = capture_thinking_request(&listener, client, model).await;
        assert!(head.starts_with("POST /v1/chat/completions HTTP/1.1"));
        assert!(
            head.lines()
                .any(|line| line.eq_ignore_ascii_case(&format!("authorization: Bearer {key}")))
        );
        assert_eq!(body["model"], model);
        assert_eq!(body["reasoning_effort"], expected);
    }
    let ordinary = from_file.catalog().get("relay", "alpha").unwrap();
    let (_, body) = capture_thinking_request(&listener, ordinary, "alpha").await;
    assert!(body.get("reasoning_effort").is_none());
}

#[tokio::test]
async fn file_thinking_responses_off_is_literal_and_has_no_summary() {
    use pi_rs_workbench::model_file::{
        ModelFileBindingError as Rejection, ModelFileReadGrants, ModelFileRoute,
    };
    use serde_json::json;

    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("models.json");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let base = "https://relay.example.test/v1";
    std::fs::write(
        &file,
        json!({"providers":{"relay":{
            "api":"openai-responses","baseUrl":base,"models":[
                {"id":"one","reasoning":true,"thinkingLevelMap":{"off":"off","xhigh":"ultra"}},
                {"id":"two","reasoning":true,"thinkingLevelMap":{}},
                {"id":"three","reasoning":true,"thinkingLevelMap":{"off":null}}
            ]
        }}})
        .to_string(),
    )
    .unwrap();
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "relay",
        &endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
    )
    .unwrap();
    let snapshot = ModelFileReadGrants {
        file,
        routes: vec![ModelFileRoute {
            provider: "relay".into(),
            api: NativeModelApi::OpenAiResponses,
            base_url: base.into(),
            endpoint,
        }],
    }
    .snapshot(&ModelClientFactory::new(auth.build()))
    .unwrap();
    for (model, level, reasoning) in [
        ("one", "off", json!({"effort":"off"})),
        ("one", "xhigh", json!({"effort":"ultra","summary":"auto"})),
        ("two", "off", json!({"effort":"none"})),
    ] {
        let client = snapshot.bind_thinking("relay", model, level).unwrap();
        let (_, body) = capture_thinking_request(&listener, client, model).await;
        assert_eq!(body["reasoning"], reasoning);
    }
    assert_eq!(
        snapshot.bind_thinking("relay", "three", "off").err(),
        Some(Rejection::DisabledLevel)
    );
}

#[tokio::test]
async fn file_thinking_uses_native_adapter_shapes() {
    use pi_rs_workbench::model_file::{
        ModelFileBindingError as Rejection, ModelFileReadGrants, ModelFileRoute,
    };
    use serde_json::json;

    for (provider, api, protocol, model, effort, pointer, expected) in [
        (
            "openrouter",
            NativeModelApi::OpenAiChat,
            "openai-completions",
            "fixture",
            "strong",
            "/reasoning/effort",
            "strong",
        ),
        (
            "relay",
            NativeModelApi::AnthropicMessages,
            "anthropic-messages",
            "fixture",
            "max",
            "/output_config/effort",
            "max",
        ),
        (
            "relay",
            NativeModelApi::GoogleGemini,
            "google-generative-ai",
            "gemini-3-flash",
            "LOW",
            "/generationConfig/thinkingConfig/thinkingLevel",
            "LOW",
        ),
        (
            "relay",
            NativeModelApi::PiMessages,
            "pi-messages",
            "fixture",
            "high",
            "/options/reasoning",
            "high",
        ),
        (
            "openai-codex",
            NativeModelApi::OpenAiCodexResponses,
            "openai-codex-responses",
            "fixture",
            "strong",
            "/reasoning/effort",
            "strong",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("models.json");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let path = match api {
            NativeModelApi::AnthropicMessages => "/v1/messages",
            NativeModelApi::GoogleGemini => "/v1beta",
            NativeModelApi::PiMessages => "/messages",
            NativeModelApi::OpenAiCodexResponses => "/backend-api/codex/responses",
            _ => "/v1/chat/completions",
        };
        let endpoint = format!("http://{}{path}", listener.local_addr().unwrap());
        let base = "https://relay.example.test/v1";
        std::fs::write(
            &file,
            json!({"providers":{(provider):{
                "api":protocol,"baseUrl":base,"compat":{"forceAdaptiveThinking":true},
                "models":[{"id":model,"reasoning":true,
                "thinkingLevelMap":{"high":effort,"xhigh":"opaque-effort"}}]
            }}})
            .to_string(),
        )
        .unwrap();
        let mut auth = HostAuthBuilder::new();
        let credential = if api == NativeModelApi::OpenAiCodexResponses {
            use base64::Engine;
            let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"fixture"}}"#);
            HostCredential::new(
                HostCredentialKind::Bearer,
                format!("fixture.{payload}.signature"),
            )
        } else {
            HostCredential::new(HostCredentialKind::ApiKey, "fixture-key")
        }
        .unwrap();
        auth.register(provider, &endpoint, credential).unwrap();
        let snapshot = ModelFileReadGrants {
            file,
            routes: vec![ModelFileRoute {
                provider: provider.into(),
                api,
                base_url: base.into(),
                endpoint,
            }],
        }
        .snapshot(&ModelClientFactory::new(auth.build()))
        .unwrap();
        let client = snapshot.bind_thinking(provider, model, "high").unwrap();
        let (_, body) = capture_thinking_request(&listener, client, model).await;
        assert_eq!(
            body.pointer(pointer),
            Some(&json!(expected)),
            "{api:?}: {body}"
        );
        if matches!(
            api,
            NativeModelApi::AnthropicMessages
                | NativeModelApi::GoogleGemini
                | NativeModelApi::PiMessages
        ) {
            assert_eq!(
                snapshot.bind_thinking(provider, model, "xhigh").err(),
                Some(Rejection::UnsupportedMapping { api })
            );
        }
    }
}

#[test]
fn file_thinking_bedrock_keeps_the_adapter_context_reuse_policy() {
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("models.json");
    let endpoint = "https://bedrock-runtime.us-east-1.amazonaws.com";
    let provider = "amazon-bedrock";
    let model = "moonshotai.kimi-k3";
    std::fs::write(
        &file,
        json!({"providers":{(provider):{
            "api":"bedrock-converse-stream", "baseUrl":endpoint,
            "models":[{"id":model,"reasoning":true,"thinkingLevelMap":{"high":"high"}}]
        }}})
        .to_string(),
    )
    .unwrap();
    let mut auth = HostAuthBuilder::new();
    auth.register(
        provider,
        endpoint,
        HostCredential::new(HostCredentialKind::Bearer, "fixture-key").unwrap(),
    )
    .unwrap();
    let snapshot = ModelFileReadGrants {
        file,
        routes: vec![ModelFileRoute {
            provider: provider.into(),
            api: NativeModelApi::BedrockConverse,
            base_url: endpoint.into(),
            endpoint: endpoint.into(),
        }],
    }
    .snapshot(&ModelClientFactory::new(auth.build()))
    .unwrap();
    let bound = snapshot.bind_thinking(provider, model, "high").unwrap();
    let request = pi_rs::ModelRequest {
        model: model.into(),
        messages: vec![],
        tools: vec![],
        max_output_tokens: 2048,
    };
    assert!(!bound.can_reuse_measured_context(&request));
}

#[tokio::test]
async fn bedrock_snapshot_effort_reaches_converse_fields() {
    use serde_json::json;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/gateway", listener.local_addr().unwrap());
    let client =
        pi_rs::bedrock::Bedrock::from_bearer_endpoint(&endpoint, "us-east-1", "fixture-key".into())
            .unwrap();
    for (model, level, effort, fields) in [
        ("kimi.k3", "off", "none", json!({"reasoning_effort":"none"})),
        (
            "openai.gpt-5",
            "high",
            "xhigh",
            json!({"reasoning":{"effort":"xhigh"}}),
        ),
        (
            "openai.gpt-5",
            "off",
            "none",
            json!({"reasoning":{"effort":"none"}}),
        ),
        (
            "openai.gpt-oss-120b",
            "high",
            "low",
            json!({"reasoning_effort":"low"}),
        ),
    ] {
        let bound = client
            .clone()
            .with_model_reasoning(model, None, level, Some(effort))
            .unwrap();
        let (_, body) =
            capture_thinking_request(&listener, std::sync::Arc::new(bound), model).await;
        assert_eq!(body["additionalModelRequestFields"], fields);
    }
    let model = "anthropic.claude-fable-5";
    let plain = client.clone().without_reasoning_fields();
    let (_, body) = capture_thinking_request(&listener, std::sync::Arc::new(plain), model).await;
    assert!(body.get("additionalModelRequestFields").is_none(), "{body}");
    assert!(
        client
            .with_model_reasoning("anthropic.claude-3-7-sonnet", None, "high", Some("high"))
            .is_err()
    );
}

async fn read_model_json_request(
    socket: &mut tokio::net::TcpStream,
) -> (String, serde_json::Value) {
    use tokio::io::AsyncReadExt;

    let mut bytes = Vec::new();
    let mut chunk = [0; 2048];
    let (header_end, body_size) = loop {
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let size = std::str::from_utf8(&bytes[..position])
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("Content-Length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (position + 4, size);
        }
    };
    while bytes.len() < header_end + body_size {
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
    }
    let head = std::str::from_utf8(&bytes[..header_end])
        .unwrap()
        .to_owned();
    let bytes = &bytes[header_end..header_end + body_size];
    let body = if head
        .lines()
        .any(|line| line.eq_ignore_ascii_case("content-encoding: zstd"))
    {
        serde_json::from_slice(&zstd::bulk::decompress(bytes, 65_536).unwrap()).unwrap()
    } else {
        serde_json::from_slice(bytes).unwrap()
    };
    (head, body)
}

#[tokio::test]
async fn pi_messages_does_not_follow_redirects_from_the_granted_endpoint() {
    use pi_rs::ModelRequest;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirected = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/messages", listener.local_addr().unwrap());
    let target = format!("http://{}/messages", redirected.local_addr().unwrap());
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "custom",
        &endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
    )
    .unwrap();
    let model = ModelClientFactory::new(auth.build())
        .client(NativeModelApi::PiMessages, "custom", &endpoint)
        .unwrap();
    let request = ModelRequest {
        model: "fixture-model".into(),
        messages: vec![],
        tools: vec![],
        max_output_tokens: 16,
    };
    let serve = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_model_json_request(&mut socket).await;
        socket.write_all(format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ).as_bytes()).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = redirected.accept() => panic!("Pi Messages followed a redirect"),
            ((), result) = async { tokio::join!(serve, model.stream(request)) } => {
                assert!(matches!(result, Err(pi_rs::Error::Model(message)) if message.contains("307")));
            }
        }
    }).await.unwrap();
}

#[tokio::test]
async fn file_thinking_rejects_unrepresentable_off_and_gemini_family_levels() {
    use pi_rs_workbench::model_file::{
        ModelFileBindingError as Rejection, ModelFileReadGrants, ModelFileRoute,
    };
    use serde_json::json;

    for (provider, api, protocol, model, path) in [
        (
            "relay",
            NativeModelApi::GoogleGemini,
            "google-generative-ai",
            "gemini-3.1-pro",
            "/v1beta",
        ),
        (
            "relay",
            NativeModelApi::AnthropicMessages,
            "anthropic-messages",
            "fixture",
            "/v1/messages",
        ),
        (
            "relay",
            NativeModelApi::PiMessages,
            "pi-messages",
            "fixture",
            "/messages",
        ),
        (
            "github-copilot",
            NativeModelApi::OpenAiResponses,
            "openai-responses",
            "fixture",
            "/v1/responses",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("models.json");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}{path}", listener.local_addr().unwrap());
        let base = "https://relay.example.test/v1";
        std::fs::write(&file, json!({"providers":{(provider):{
            "api":protocol,"baseUrl":base,"models":[
                {"id":model,"reasoning":true,"thinkingLevelMap":{"off":"opaque", "minimal":"MINIMAL", "medium":"MEDIUM", "high":"HIGH"}},
                {"id":"plain","reasoning":false,"thinkingLevelMap":{"off":"opaque"}},
                {"id":"gemini-2.5-flash","reasoning":true,"thinkingLevelMap":{"high":"OFF"}}
            ]
        }}}).to_string()).unwrap();
        let mut auth = HostAuthBuilder::new();
        auth.register(
            provider,
            &endpoint,
            HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
        )
        .unwrap();
        let snapshot = ModelFileReadGrants {
            file,
            routes: vec![ModelFileRoute {
                provider: provider.into(),
                api,
                base_url: base.into(),
                endpoint,
            }],
        }
        .snapshot(&ModelClientFactory::new(auth.build()))
        .unwrap();
        for id in [model, "plain"] {
            assert_eq!(
                snapshot.bind_thinking(provider, id, "off").err(),
                Some(Rejection::UnsupportedMapping { api })
            );
        }
        if api == NativeModelApi::GoogleGemini {
            assert_eq!(
                snapshot
                    .bind_thinking(provider, "gemini-2.5-flash", "high")
                    .err(),
                Some(Rejection::UnsupportedMapping { api })
            );
            for level in ["minimal", "medium"] {
                assert_eq!(
                    snapshot.bind_thinking(provider, model, level).err(),
                    Some(Rejection::UnsupportedMapping { api })
                );
            }
            let high = snapshot.bind_thinking(provider, model, "high").unwrap();
            let (_, body) = capture_thinking_request(&listener, high, model).await;
            assert_eq!(
                body.pointer("/generationConfig/thinkingConfig/thinkingLevel"),
                Some(&json!("HIGH"))
            );
        }
    }
}

#[tokio::test]
async fn file_thinking_gemini_bindings_are_isolated_concurrently() {
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("models.json");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
    let base = "https://relay.example.test/v1";
    std::fs::write(
        &file,
        json!({"providers":{"relay":{"api":"google-generative-ai","baseUrl":base,"models":[
            {"id":"gemini-3-flash","reasoning":true,"thinkingLevelMap":{"low":"LOW","high":"HIGH"}}
        ]}}})
        .to_string(),
    )
    .unwrap();
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "relay",
        &endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
    )
    .unwrap();
    let snapshot = ModelFileReadGrants {
        file,
        routes: vec![ModelFileRoute {
            provider: "relay".into(),
            api: NativeModelApi::GoogleGemini,
            base_url: base.into(),
            endpoint,
        }],
    }
    .snapshot(&ModelClientFactory::new(auth.build()))
    .unwrap();
    let low = snapshot
        .bind_thinking("relay", "gemini-3-flash", "low")
        .unwrap();
    let high = snapshot
        .bind_thinking("relay", "gemini-3-flash", "high")
        .unwrap();
    let (a, b) = tokio::join!(
        capture_thinking_request(&listener, low, "gemini-3-flash"),
        capture_thinking_request(&listener, high, "gemini-3-flash")
    );
    let mut values = [
        a.1["generationConfig"]["thinkingConfig"]["thinkingLevel"]
            .as_str()
            .unwrap()
            .to_owned(),
        b.1["generationConfig"]["thinkingConfig"]["thinkingLevel"]
            .as_str()
            .unwrap()
            .to_owned(),
    ];
    values.sort();
    assert_eq!(values, ["HIGH", "LOW"]);
}

#[tokio::test]
async fn file_thinking_unmapped_off_keeps_native_request_shapes() {
    use pi_rs_workbench::model_file::{ModelFileReadGrants, ModelFileRoute};
    use serde_json::json;
    for (provider, api, protocol, model, path) in [
        (
            "relay",
            NativeModelApi::GoogleGemini,
            "google-generative-ai",
            "gemini-2.5-flash",
            "/v1beta",
        ),
        (
            "relay",
            NativeModelApi::AnthropicMessages,
            "anthropic-messages",
            "fixture",
            "/v1/messages",
        ),
        (
            "relay",
            NativeModelApi::PiMessages,
            "pi-messages",
            "fixture",
            "/messages",
        ),
        (
            "github-copilot",
            NativeModelApi::OpenAiResponses,
            "openai-responses",
            "fixture",
            "/v1/responses",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("models.json");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}{path}", listener.local_addr().unwrap());
        let base = "https://relay.example.test/v1";
        std::fs::write(&file, json!({"providers":{(provider):{"api":protocol,"baseUrl":base,"models":[{"id":model,"reasoning":true,"thinkingLevelMap":{}}]}}}).to_string()).unwrap();
        let mut auth = HostAuthBuilder::new();
        auth.register(
            provider,
            &endpoint,
            HostCredential::new(HostCredentialKind::ApiKey, "fixture-key").unwrap(),
        )
        .unwrap();
        let snapshot = ModelFileReadGrants {
            file,
            routes: vec![ModelFileRoute {
                provider: provider.into(),
                api,
                base_url: base.into(),
                endpoint,
            }],
        }
        .snapshot(&ModelClientFactory::new(auth.build()))
        .unwrap();
        let client = snapshot.bind_thinking(provider, model, "off").unwrap();
        let (_, body) = capture_thinking_request(&listener, client, model).await;
        match api {
            NativeModelApi::GoogleGemini => assert_eq!(
                body.pointer("/generationConfig/thinkingConfig/thinkingBudget"),
                Some(&json!(0))
            ),
            NativeModelApi::AnthropicMessages => {
                assert_eq!(body.pointer("/thinking/type"), Some(&json!("disabled")))
            }
            NativeModelApi::PiMessages => assert!(body.pointer("/options/reasoning").is_none()),
            NativeModelApi::OpenAiResponses => assert!(body.get("reasoning").is_none()),
            _ => unreachable!(),
        }
    }
}
