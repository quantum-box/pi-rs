use std::sync::Arc;

use pi_rs::{
    CatalogModelDescriptor, CatalogModelInfo, Model, ModelCatalog, ModelCatalogBuilder,
    ModelRequest, ModelStream,
};

struct OfflineModel;

#[async_trait::async_trait]
impl Model for OfflineModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs::Result<ModelStream> {
        panic!("catalog inspection must not stream a request");
    }

    fn context_identity(&self) -> Option<String> {
        panic!("catalog inspection must not inspect the client");
    }
}

#[test]
fn finds_and_lists_exact_configured_descriptors_through_the_public_sdk() {
    let mut builder = ModelCatalogBuilder::new();
    let client = Arc::new(OfflineModel);
    let info = CatalogModelInfo {
        display_name: Some("Custom model".into()),
        api: Some("host-defined-api".into()),
        ..Default::default()
    };
    builder
        .register("provider/ext", "same", client.clone())
        .unwrap();
    builder
        .register_with_info("provider", "same", client.clone(), info.clone())
        .unwrap();
    builder.register("provider", "Same", client).unwrap();
    let catalog = builder.build();

    assert_eq!(
        catalog.find("provider", "same"),
        Some(CatalogModelDescriptor {
            provider: "provider",
            model: "same",
            info: Some(&info),
        })
    );
    assert_eq!(catalog.find("provider", "Same").unwrap().info, None);
    for (provider, model) in [
        ("provider", "SAME"),
        ("Provider", "same"),
        ("provider ", "same"),
        ("provider", "same "),
        ("missing", "same"),
    ] {
        assert!(catalog.find(provider, model).is_none());
    }

    assert_eq!(
        catalog
            .providers()
            .map(|provider| provider.id())
            .collect::<Vec<_>>(),
        ["provider", "provider/ext"]
    );
    assert!(catalog.find_provider("Provider").is_none());
    assert!(catalog.find_provider("provide").is_none());
    let provider = catalog.find_provider("provider").unwrap();
    assert_eq!(
        provider
            .models()
            .map(|entry| entry.model)
            .collect::<Vec<_>>(),
        ["Same", "same"]
    );
    assert_eq!(
        catalog
            .models()
            .map(|entry| (entry.provider, entry.model))
            .collect::<Vec<_>>(),
        [
            ("provider", "Same"),
            ("provider", "same"),
            ("provider/ext", "same")
        ]
    );
    let empty = ModelCatalog::default();
    assert_eq!(empty.providers().count(), 0);
    assert_eq!(empty.models().len(), 0);
    assert!(empty.find_provider("provider").is_none());
}

#[test]
fn host_reconfiguration_publishes_an_independent_snapshot() {
    let build = |label: &str, other_provider| {
        let mut builder = ModelCatalogBuilder::new();
        let client = Arc::new(OfflineModel);
        builder
            .register_with_info(
                "primary",
                "shared",
                client.clone(),
                CatalogModelInfo {
                    display_name: Some(label.into()),
                    ..Default::default()
                },
            )
            .unwrap();
        builder.register(other_provider, "shared", client).unwrap();
        builder.build()
    };
    let original = build("Old label", "retired");
    let retained = original.clone();
    let replacement = build("New label", "added");
    drop(original);

    let label = |catalog: &ModelCatalog| {
        catalog
            .find("primary", "shared")
            .unwrap()
            .info
            .unwrap()
            .display_name
            .clone()
    };
    assert_eq!(label(&retained).as_deref(), Some("Old label"));
    assert_eq!(label(&replacement).as_deref(), Some("New label"));
    assert!(retained.find_provider("retired").is_some());
    assert!(retained.find("added", "shared").is_none());
    assert!(replacement.find_provider("retired").is_none());
    assert!(replacement.find("retired", "shared").is_none());
    assert!(replacement.find("added", "shared").is_some());
}
