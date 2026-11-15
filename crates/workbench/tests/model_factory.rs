use pi_rs::{HostAuthBuilder, HostCredential, HostCredentialKind, ModelCatalogBuilder};
use pi_rs_workbench::model_factory::{ModelClientFactory, NativeModelApi};

#[test]
fn factory_uses_only_exact_host_authorized_routes_and_credential_kinds() {
    let mut auth = HostAuthBuilder::new();
    auth.register(
        "custom",
        "https://api.example/v1",
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
            "https://api.example/v1",
            "example",
        )
        .unwrap();
    assert!(
        factory
            .register(
                &mut catalog,
                NativeModelApi::OpenAiResponses,
                "custom",
                "https://api.example/v1",
                "example"
            )
            .is_err()
    );
    assert!(
        factory
            .client(
                NativeModelApi::OpenAiResponses,
                "custom",
                "https://other.example/v1"
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
