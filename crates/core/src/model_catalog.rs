//! Host-owned, provider-qualified model clients for SDK sessions.
use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::sync::Arc;

use crate::{
    Error, HistoricalModelResolution, HistoricalModelResolver, Model, Result, SessionBuilder,
};

/// Host-supplied display and capability information for a catalog model.
/// These values are descriptive only: the catalog does not probe a provider or
/// configure request limits, thinking, image support, costs, or authentication.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CatalogModelInfo {
    pub display_name: Option<String>,
    /// The host-supplied API name, including custom or future Pi API names.
    pub api: Option<String>,
    pub context_window: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub reasoning: Option<bool>,
    pub images: Option<bool>,
    pub cost: Option<CatalogModelCost>,
}

/// Host-supplied USD prices per million tokens, absent when unknown.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CatalogModelCost {
    pub input: Option<serde_json::Number>,
    pub output: Option<serde_json::Number>,
    pub cache_read: Option<serde_json::Number>,
    pub cache_write: Option<serde_json::Number>,
}

/// The exact identity and optional host-supplied metadata of a registered model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogModelDescriptor<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub info: Option<&'a CatalogModelInfo>,
}

/// A provider represented by at least one model in this catalog snapshot.
#[derive(Clone, Copy)]
pub struct CatalogProviderDescriptor<'a> {
    id: &'a str,
    catalog: &'a ModelCatalog,
}

impl<'a> CatalogProviderDescriptor<'a> {
    /// The provider's exact registered identity.
    pub fn id(&self) -> &'a str {
        self.id
    }

    /// The provider's registered model descriptors, ordered by model identity.
    pub fn models(self) -> impl Iterator<Item = CatalogModelDescriptor<'a>> + 'a {
        let id = self.id;
        self.catalog
            .bindings_from_provider(id)
            .take_while(move |((provider, _), _)| provider == id)
            .map(describe_model)
    }
}

/// An immutable catalog of clients constructed and authorized by the host.
///
/// The provider and model names are exact, case-sensitive identities. A client
/// may hold credentials, so the catalog never discovers endpoints or creates
/// clients from journal data. Construct a new catalog when authorization or
/// endpoint configuration changes; existing sessions retain their snapshot.
#[derive(Clone, Default)]
pub struct ModelCatalog {
    clients: Arc<BTreeMap<(String, String), CatalogBinding>>,
}

/// Collects trusted model clients before publishing an immutable catalog.
#[derive(Default)]
pub struct ModelCatalogBuilder {
    clients: BTreeMap<(String, String), CatalogBinding>,
}

struct CatalogBinding {
    client: Arc<dyn Model>,
    info: Option<CatalogModelInfo>,
    thinking_levels: Option<Vec<String>>,
}

fn describe_model<'a>(
    ((provider, model), binding): (&'a (String, String), &'a CatalogBinding),
) -> CatalogModelDescriptor<'a> {
    CatalogModelDescriptor {
        provider,
        model,
        info: binding.info.as_ref(),
    }
}

impl ModelCatalogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one exact identity. Duplicate identities are rejected so a
    /// later registration cannot silently replace an endpoint or credential.
    pub fn register(
        &mut self,
        provider: impl Into<String>,
        model: impl Into<String>,
        client: Arc<dyn Model>,
    ) -> Result<&mut Self> {
        self.register_binding(provider.into(), model.into(), client, None)
    }

    /// Register a client with host-provided descriptive information. Duplicate
    /// identities fail without changing either the old client or its metadata.
    pub fn register_with_info(
        &mut self,
        provider: impl Into<String>,
        model: impl Into<String>,
        client: Arc<dyn Model>,
        info: CatalogModelInfo,
    ) -> Result<&mut Self> {
        self.register_binding(provider.into(), model.into(), client, Some(info))
    }

    /// Assign the host's selectable thinking levels to an existing exact identity.
    /// Order is preserved. An empty list records that no levels are selectable.
    /// Invalid names or levels leave any earlier declaration intact.
    pub fn set_thinking_levels(
        &mut self,
        provider: &str,
        model: &str,
        levels: impl IntoIterator<Item = String>,
    ) -> Result<&mut Self> {
        let binding = self
            .clients
            .get_mut(&(provider.to_owned(), model.to_owned()))
            .ok_or_else(|| Error::Config("model is not registered in the catalog".into()))?;
        let levels: Vec<String> = levels.into_iter().collect();
        let mut seen = BTreeSet::new();
        if levels
            .iter()
            .any(|level| level.trim().is_empty() || !seen.insert(level))
        {
            return Err(Error::Config(
                "thinking levels must be nonempty and unique".into(),
            ));
        }
        binding.thinking_levels = Some(levels);
        Ok(self)
    }

    fn register_binding(
        &mut self,
        provider: String,
        model: String,
        client: Arc<dyn Model>,
        info: Option<CatalogModelInfo>,
    ) -> Result<&mut Self> {
        if provider.trim().is_empty() || model.trim().is_empty() {
            return Err(Error::Config("provider or model name is empty".into()));
        }
        match self.clients.entry((provider, model)) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(CatalogBinding {
                    client,
                    info,
                    thinking_levels: None,
                });
                Ok(self)
            }
            btree_map::Entry::Occupied(_) => Err(Error::Config(
                "duplicate provider and model identity".into(),
            )),
        }
    }

    pub fn build(self) -> ModelCatalog {
        ModelCatalog {
            clients: Arc::new(self.clients),
        }
    }
}

impl ModelCatalog {
    /// Return only the client registered for this exact identity.
    pub fn get(&self, provider: &str, model: &str) -> Option<Arc<dyn Model>> {
        self.clients
            .get(&(provider.to_owned(), model.to_owned()))
            .map(|binding| binding.client.clone())
    }

    /// Find a registered model by exact, case-sensitive provider and model
    /// identities. A registered model without metadata still has a descriptor.
    /// This is a snapshot read and does not access the client or resolve auth.
    pub fn find(&self, provider: &str, model: &str) -> Option<CatalogModelDescriptor<'_>> {
        self.clients
            .get_key_value(&(provider.to_owned(), model.to_owned()))
            .map(describe_model)
    }

    /// List all registered model descriptors, ordered by provider then model.
    pub fn models(&self) -> impl ExactSizeIterator<Item = CatalogModelDescriptor<'_>> {
        self.clients.iter().map(describe_model)
    }

    /// Find a provider by exact, case-sensitive identity.
    pub fn find_provider(&self, id: &str) -> Option<CatalogProviderDescriptor<'_>> {
        let ((provider, _), _) = self.bindings_from_provider(id).next()?;
        (provider == id).then_some(CatalogProviderDescriptor {
            id: provider,
            catalog: self,
        })
    }

    /// List each registered provider once, ordered by provider identity.
    pub fn providers(&self) -> impl Iterator<Item = CatalogProviderDescriptor<'_>> {
        let mut previous = None;
        self.identities().filter_map(move |(id, _)| {
            if previous == Some(id) {
                return None;
            }
            previous = Some(id);
            Some(CatalogProviderDescriptor { id, catalog: self })
        })
    }

    fn bindings_from_provider(
        &self,
        id: &str,
    ) -> btree_map::Range<'_, (String, String), CatalogBinding> {
        self.clients.range((id.to_owned(), String::new())..)
    }

    /// List the exact identities registered by the host, ordered by provider
    /// and then model. This does not probe clients or guarantee that credentials
    /// remain valid. The iterator cannot expose clients or their credentials.
    pub fn identities(&self) -> impl ExactSizeIterator<Item = (&str, &str)> + '_ {
        self.clients
            .keys()
            .map(|(provider, model)| (provider.as_str(), model.as_str()))
    }

    /// Host-supplied information for this exact identity, if registered with it.
    /// This is not a live capability or credential availability check.
    pub fn info(&self, provider: &str, model: &str) -> Option<&CatalogModelInfo> {
        self.find(provider, model)?.info
    }

    /// Return host-declared thinking levels in selection order for this exact identity.
    /// `None` means no declaration is available. An empty slice means the host
    /// explicitly declared no selectable levels. This lookup never consults a client.
    pub fn thinking_levels(&self, provider: &str, model: &str) -> Option<&[String]> {
        self.clients
            .get(&(provider.to_owned(), model.to_owned()))?
            .thinking_levels
            .as_deref()
    }

    /// Ordered catalog identities and their optional host-supplied information.
    pub fn entries(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &str, Option<&CatalogModelInfo>)> + '_ {
        self.models()
            .map(|entry| (entry.provider, entry.model, entry.info))
    }

    /// Create a session builder and configure exact historical model recovery.
    /// Missing historical identities remain unavailable; no fallback or
    /// name-only lookup is inferred from untrusted journal contents.
    pub fn session_builder(&self, provider: &str, model: &str) -> Result<SessionBuilder> {
        let client = self
            .get(provider, model)
            .ok_or_else(|| Error::Config("model is not registered in the catalog".into()))?;
        Ok(SessionBuilder::new(client, model)
            .provider(provider)?
            .historical_model_resolver(Arc::new(self.clone())))
    }

    /// Restore an exact catalog model when available. If a provider-qualified
    /// historical model is absent, use this explicitly authorized default and
    /// report the fallback through `AgentSession::model_fallback`.
    pub fn session_builder_with_fallback(
        &self,
        provider: &str,
        model: &str,
    ) -> Result<SessionBuilder> {
        let client = self.get(provider, model).ok_or_else(|| {
            Error::Config("fallback model is not registered in the catalog".into())
        })?;
        let catalog = self.clone();
        let fallback_provider = provider.to_owned();
        let fallback_model = model.to_owned();
        let fallback_client = client.clone();
        let resolver = move |provider: &str, model: &str| {
            Ok(match catalog.get(provider, model) {
                Some(exact) => HistoricalModelResolution::Historical(exact),
                None => HistoricalModelResolution::Fallback {
                    provider: fallback_provider.clone(),
                    model: fallback_model.clone(),
                    client: fallback_client.clone(),
                },
            })
        };
        Ok(SessionBuilder::new(client, model)
            .provider(provider)?
            .historical_model_resolver(Arc::new(resolver)))
    }

    /// Explicitly bind a legacy, provider-less model name to a trusted client.
    /// Use only when the host knows which provider owned that historical name.
    pub fn session_builder_with_legacy_model(
        &self,
        provider: &str,
        model: &str,
    ) -> Result<SessionBuilder> {
        let client = self
            .get(provider, model)
            .ok_or_else(|| Error::Config("model is not registered in the catalog".into()))?;
        self.session_builder(provider, model)?
            .branch_model(model, client)
    }
}

impl HistoricalModelResolver for ModelCatalog {
    fn resolve(&self, provider: &str, model: &str) -> Result<HistoricalModelResolution> {
        Ok(match self.get(provider, model) {
            Some(client) => HistoricalModelResolution::Historical(client),
            None => HistoricalModelResolution::Unavailable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStore, ModelEvent, ModelRequest, ModelStream, Session};

    struct NamedModel(&'static str);
    #[async_trait::async_trait]
    impl Model for NamedModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text(self.0.into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }

    #[test]
    fn registration_is_exact_and_rejects_replacement() {
        let a: Arc<dyn Model> = Arc::new(NamedModel("a"));
        let b: Arc<dyn Model> = Arc::new(NamedModel("b"));
        let mut builder = ModelCatalogBuilder::new();
        builder.register("a", "shared", a.clone()).unwrap();
        assert!(builder.register("a", "shared", b.clone()).is_err());
        assert!(builder.register(" ", "shared", b.clone()).is_err());
        builder.register("b", "shared", b.clone()).unwrap();
        let catalog = builder.build();
        assert!(Arc::ptr_eq(&catalog.get("a", "shared").unwrap(), &a));
        assert!(Arc::ptr_eq(&catalog.get("b", "shared").unwrap(), &b));
        assert!(catalog.get("A", "shared").is_none());
        assert!(catalog.session_builder("missing", "shared").is_err());
        assert!(matches!(
            catalog.resolve("missing", "shared").unwrap(),
            HistoricalModelResolution::Unavailable
        ));
        fn send_sync<T: Send + Sync>() {}
        send_sync::<ModelCatalog>();
    }

    #[test]
    fn identities_are_exact_sorted_and_snapshot_owned() {
        let mut builder = ModelCatalogBuilder::new();
        assert_eq!(builder.build().identities().count(), 0);
        builder = ModelCatalogBuilder::new();
        builder
            .register("z", "same", Arc::new(NamedModel("secret")))
            .unwrap();
        builder
            .register("a", "z", Arc::new(NamedModel("secret")))
            .unwrap();
        builder
            .register("a", "A", Arc::new(NamedModel("secret")))
            .unwrap();
        let catalog = builder.build();
        let copy = catalog.clone();
        drop(catalog);
        assert_eq!(
            copy.identities().collect::<Vec<_>>(),
            vec![("a", "A"), ("a", "z"), ("z", "same")]
        );
    }

    #[tokio::test]
    async fn selected_provider_tracks_switch_and_branch_checkout() {
        let mut builder = ModelCatalogBuilder::new();
        let a: Arc<dyn Model> = Arc::new(NamedModel("a"));
        let b: Arc<dyn Model> = Arc::new(NamedModel("b"));
        builder.register("provider-a", "shared", a).unwrap();
        builder.register("provider-b", "shared", b.clone()).unwrap();
        let catalog = builder.build();
        let mut session = catalog
            .session_builder("provider-a", "shared")
            .unwrap()
            .in_memory()
            .unwrap();
        assert_eq!(
            (session.model_provider().unwrap(), session.model_name()),
            (Some("provider-a"), "shared")
        );
        let before = session.journal().unwrap().head();
        session
            .set_provider_model(b, "provider-b", "shared")
            .unwrap();
        assert_eq!(
            (session.model_provider().unwrap(), session.model_name()),
            (Some("provider-b"), "shared")
        );
        session.checkout(before).unwrap();
        assert_eq!(
            (session.model_provider().unwrap(), session.model_name()),
            (Some("provider-a"), "shared")
        );
        {
            let mut journal = session.journal().unwrap();
            journal
                .select_provider_model(Some("provider-b"), "shared", None)
                .unwrap();
        }
        assert!(session.model_provider().is_err());
        session
            .set_provider_model(
                catalog.get("provider-b", "shared").unwrap(),
                "provider-b",
                "shared",
            )
            .unwrap();
        assert_eq!(session.model_provider().unwrap(), Some("provider-b"));
        session
            .set_model(Arc::new(NamedModel("legacy")), "legacy")
            .unwrap();
        assert_eq!(
            (session.model_provider().unwrap(), session.model_name()),
            (None, "legacy")
        );
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn selected_provider_rejects_revoked_client() {
        let registry = crate::Registry::default();
        let mut session = SessionBuilder::new(Arc::new(NamedModel("a")), "shared")
            .provider("provider-a")
            .unwrap()
            .registry(registry.clone())
            .in_memory()
            .unwrap();
        assert_eq!(session.model_provider().unwrap(), Some("provider-a"));
        let id = session.journal().unwrap().session_id();
        registry.remove_session_branch_models(id);
        assert!(session.model_provider().is_err());
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn selected_provider_rejects_unavailable_historical_model() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut original = ModelCatalogBuilder::new();
        original
            .register("provider-b", "shared", Arc::new(NamedModel("b")))
            .unwrap();
        let mut session = original
            .build()
            .session_builder("provider-b", "shared")
            .unwrap()
            .open(&path)
            .unwrap();
        session.close().await.unwrap();
        drop(session);
        let mut replacement = ModelCatalogBuilder::new();
        replacement
            .register("provider-a", "shared", Arc::new(NamedModel("a")))
            .unwrap();
        let mut session = replacement
            .build()
            .session_builder("provider-a", "shared")
            .unwrap()
            .open(&path)
            .unwrap();
        assert!(session.model_provider().is_err());
        session
            .set_provider_model(Arc::new(NamedModel("b")), "provider-b", "shared")
            .unwrap();
        assert_eq!(session.model_provider().unwrap(), Some("provider-b"));
        session.close().await.unwrap();
    }

    #[test]
    fn host_model_info_is_exact_immutable_and_cannot_replace_a_binding() {
        let mut builder = ModelCatalogBuilder::new();
        let first: Arc<dyn Model> = Arc::new(NamedModel("first"));
        let info = CatalogModelInfo {
            display_name: Some("Example".into()),
            api: Some("openai-codex-responses".into()),
            context_window: Some(12345),
            reasoning: Some(true),
            cost: Some(CatalogModelCost {
                input: Some(serde_json::json!(1.25).as_number().unwrap().clone()),
                ..Default::default()
            }),
            ..Default::default()
        };
        builder
            .register_with_info("p", "m", first.clone(), info.clone())
            .unwrap();
        builder
            .register("p", "other", Arc::new(NamedModel("other")))
            .unwrap();
        assert!(
            builder
                .register_with_info(
                    "p",
                    "m",
                    Arc::new(NamedModel("replacement")),
                    CatalogModelInfo::default()
                )
                .is_err()
        );
        let catalog = builder.build();
        assert_eq!(catalog.info("p", "m"), Some(&info));
        assert!(catalog.info("P", "m").is_none());
        assert!(Arc::ptr_eq(&catalog.get("p", "m").unwrap(), &first));
        assert_eq!(
            catalog
                .entries()
                .map(|(p, m, i)| (p, m, i.is_some()))
                .collect::<Vec<_>>(),
            vec![("p", "m", true), ("p", "other", false)]
        );
        let cloned = catalog.clone();
        drop(catalog);
        assert_eq!(cloned.info("p", "m"), Some(&info));
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["contextWindow"], 12345);
        let custom: CatalogModelInfo =
            serde_json::from_value(serde_json::json!({"api":"custom-future-api"})).unwrap();
        assert_eq!(custom.api.as_deref(), Some("custom-future-api"));
        assert_eq!(
            serde_json::to_value(custom).unwrap()["api"],
            "custom-future-api"
        );
        assert_eq!(
            serde_json::from_value::<CatalogModelInfo>(json).unwrap(),
            info
        );
    }

    #[test]
    fn catalog_thinking_levels_keep_host_order_and_exact_identity() {
        let client: Arc<dyn Model> = Arc::new(NamedModel("test"));
        let mut builder = ModelCatalogBuilder::new();
        for provider in ["one", "two", "three"] {
            builder
                .register(provider, "shared", client.clone())
                .unwrap();
        }

        let levels = ["off", "high", "low"].map(str::to_owned);
        builder
            .set_thinking_levels("one", "shared", levels.clone())
            .unwrap();
        builder.set_thinking_levels("two", "shared", []).unwrap();
        assert!(builder.set_thinking_levels("ONE", "shared", []).is_err());
        assert!(builder.set_thinking_levels("one", "Shared", []).is_err());
        for invalid in [["off", "off"], ["off", " "]] {
            assert!(
                builder
                    .set_thinking_levels("one", "shared", invalid.map(str::to_owned))
                    .is_err()
            );
        }
        assert!(builder.register("one", "shared", client.clone()).is_err());

        let catalog = builder.build().clone();
        assert_eq!(
            catalog.thinking_levels("one", "shared"),
            Some(levels.as_slice())
        );
        assert_eq!(catalog.thinking_levels("two", "shared"), Some(&[][..]));
        assert_eq!(catalog.thinking_levels("three", "shared"), None);
        assert_eq!(catalog.thinking_levels("ONE", "shared"), None);
        assert_eq!(catalog.thinking_levels("one", "Shared"), None);
        assert_eq!(catalog.thinking_levels("missing", "shared"), None);
        assert!(Arc::ptr_eq(&catalog.get("one", "shared").unwrap(), &client));
    }

    #[tokio::test]
    async fn catalog_session_supports_repeated_prompts_and_reopen() {
        let mut builder = ModelCatalogBuilder::new();
        builder
            .register("a", "shared", Arc::new(NamedModel("first")))
            .unwrap();
        builder
            .register("b", "shared", Arc::new(NamedModel("second")))
            .unwrap();
        let catalog = builder.build();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut session = catalog
            .session_builder("a", "shared")
            .unwrap()
            .open(&path)
            .unwrap();
        assert_eq!(session.prompt("one").await.unwrap(), "first");
        assert_eq!(session.prompt("two").await.unwrap(), "first");
        session.close().await.unwrap();
        drop(session);

        let mut reopened = catalog
            .session_builder("b", "shared")
            .unwrap()
            .open(&path)
            .unwrap();
        assert_eq!(reopened.prompt("three").await.unwrap(), "first");
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_fork_before_selection_retains_provider() {
        let mut builder = ModelCatalogBuilder::new();
        builder
            .register("a", "shared", Arc::new(NamedModel("first")))
            .unwrap();
        let catalog = builder.build();
        let mut journal = Session::with_store(MemoryStore::default()).unwrap();
        journal
            .append(crate::Event::SessionInfo {
                name: Some("before".into()),
                cwd: None,
            })
            .unwrap();
        let before_selection = journal.head();
        let mut session = catalog
            .session_builder("a", "shared")
            .unwrap()
            .build(journal)
            .unwrap();
        let mut fork = session.fork_at(before_selection).unwrap();
        assert_eq!(fork.prompt("one").await.unwrap(), "first");
        assert_eq!(fork.prompt("two").await.unwrap(), "first");
        fork.close().await.unwrap();
        session.checkout(before_selection).unwrap();
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_at(head).unwrap();
        assert_eq!(fork.prompt("fork one").await.unwrap(), "first");
        assert_eq!(fork.prompt("fork two").await.unwrap(), "first");
        fork.close().await.unwrap();
        assert_eq!(session.prompt("one").await.unwrap(), "first");
        assert_eq!(session.prompt("two").await.unwrap(), "first");
        session
            .navigate_tree(before_selection, crate::TreeNavigationOptions::default())
            .await
            .unwrap();
        assert_eq!(session.prompt("tree one").await.unwrap(), "first");
        assert_eq!(session.prompt("tree two").await.unwrap(), "first");
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_legacy_binding_requires_host_opt_in() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy.jsonl");
        let mut legacy = SessionBuilder::new(Arc::new(NamedModel("old")), "shared")
            .open(&path)
            .unwrap();
        assert_eq!(legacy.prompt("one").await.unwrap(), "old");
        legacy.close().await.unwrap();
        drop(legacy);
        let mut builder = ModelCatalogBuilder::new();
        builder
            .register("a", "shared", Arc::new(NamedModel("first")))
            .unwrap();
        let catalog = builder.build();
        let mut unbound = catalog
            .session_builder("a", "shared")
            .unwrap()
            .open(&path)
            .unwrap();
        assert!(unbound.prompt("two").await.is_err());
        unbound.close().await.unwrap();
        drop(unbound);
        let mut reopened = catalog
            .session_builder_with_legacy_model("a", "shared")
            .unwrap()
            .open(&path)
            .unwrap();
        assert_eq!(reopened.prompt("two").await.unwrap(), "first");
        assert_eq!(reopened.prompt("three").await.unwrap(), "first");
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_fallback_restores_exact_clients_and_reports_missing_models() {
        let mut builder = ModelCatalogBuilder::new();
        builder
            .register("other", "shared", Arc::new(NamedModel("historical")))
            .unwrap();
        builder
            .register("default", "shared", Arc::new(NamedModel("fallback")))
            .unwrap();
        let catalog = builder.build();
        assert!(
            catalog
                .session_builder_with_fallback("absent", "shared")
                .is_err()
        );

        for (provider, response) in [("other", "historical"), ("absent", "fallback")] {
            let mut journal = Session::with_store(MemoryStore::default()).unwrap();
            journal
                .select_provider_model(Some(provider), "shared", Some("high"))
                .unwrap();
            let mut session = catalog
                .session_builder_with_fallback("default", "shared")
                .unwrap()
                .build(journal)
                .unwrap();
            if provider == "absent" {
                let fallback = session.model_fallback().unwrap();
                assert_eq!(fallback.requested_provider, "absent");
                assert_eq!(fallback.provider, "default");
                assert_eq!(
                    session.journal().unwrap().model_selection(),
                    Some(("shared".into(), None))
                );
            } else {
                assert!(session.model_fallback().is_none());
                assert_eq!(session.model_provider().unwrap(), Some("other"));
            }
            assert_eq!(session.prompt("hello").await.unwrap(), response);
            session.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn builder_restores_exact_provider_client_from_journal() {
        let mut builder = ModelCatalogBuilder::new();
        builder
            .register("a", "shared", Arc::new(NamedModel("first")))
            .unwrap();
        builder
            .register("b", "shared", Arc::new(NamedModel("second")))
            .unwrap();
        let catalog = builder.build();
        let mut journal = Session::with_store(MemoryStore::default()).unwrap();
        journal
            .select_provider_model(Some("b"), "shared", None)
            .unwrap();
        let mut session = catalog
            .session_builder("a", "shared")
            .unwrap()
            .build(journal)
            .unwrap();
        assert_eq!(session.prompt("hello").await.unwrap(), "second");
        assert!(session.model_fallback().is_none());
        session.close().await.unwrap();
    }
}
