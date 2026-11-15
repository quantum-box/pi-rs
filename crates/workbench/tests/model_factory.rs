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
