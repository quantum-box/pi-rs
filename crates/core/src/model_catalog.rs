//! Host-owned, provider-qualified model clients for SDK sessions.
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{
    Error, HistoricalModelResolution, HistoricalModelResolver, Model, Result, SessionBuilder,
};

/// An immutable catalog of clients constructed and authorized by the host.
///
/// The provider and model names are exact, case-sensitive identities. A client
/// may hold credentials, so the catalog never discovers endpoints or creates
/// clients from journal data. Construct a new catalog when authorization or
/// endpoint configuration changes; existing sessions retain their snapshot.
#[derive(Clone, Default)]
pub struct ModelCatalog {
    clients: Arc<BTreeMap<(String, String), Arc<dyn Model>>>,
}

/// Collects trusted model clients before publishing an immutable catalog.
#[derive(Default)]
pub struct ModelCatalogBuilder {
    clients: BTreeMap<(String, String), Arc<dyn Model>>,
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
        let (provider, model) = (provider.into(), model.into());
        if provider.trim().is_empty() || model.trim().is_empty() {
            return Err(Error::Config("provider or model name is empty".into()));
        }
        match self.clients.entry((provider, model)) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(client);
                Ok(self)
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(Error::Config(
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
            .cloned()
    }

    /// List the exact identities registered by the host, ordered by provider
    /// and then model. This does not probe clients or guarantee that credentials
    /// remain valid. The iterator cannot expose clients or their credentials.
    pub fn identities(&self) -> impl ExactSizeIterator<Item = (&str, &str)> + '_ {
        self.clients
            .keys()
            .map(|(provider, model)| (provider.as_str(), model.as_str()))
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
