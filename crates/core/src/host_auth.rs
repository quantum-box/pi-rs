//! Explicit, host-owned credential snapshots for native SDK integrations.
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::{Error, Result};

/// The way a host intends to use a literal credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostCredentialKind {
    ApiKey,
    Bearer,
}

/// A host-provided literal credential. Debug output never includes its value.
/// The caller must avoid logging or serializing the value returned by `secret`.
#[derive(Clone)]
pub struct HostCredential {
    kind: HostCredentialKind,
    secret: Arc<str>,
}

impl HostCredential {
    pub fn new(kind: HostCredentialKind, secret: impl Into<String>) -> Result<Self> {
        let secret = secret.into();
        if secret.trim().is_empty() {
            return Err(Error::Config("credential is empty".into()));
        }
        Ok(Self {
            kind,
            secret: secret.into(),
        })
    }

    pub fn kind(&self) -> HostCredentialKind {
        self.kind
    }

    /// Expose the value for constructing a host-authorized provider client.
    pub fn secret(&self) -> &str {
        &self.secret
    }
}

impl fmt::Debug for HostCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostCredential")
            .field("kind", &self.kind)
            .field("secret", &"[redacted]")
            .finish()
    }
}

/// Builder for exact, case-sensitive provider and endpoint credential bindings.
#[derive(Default)]
pub struct HostAuthBuilder {
    entries: BTreeMap<(String, String), HostCredential>,
}

impl HostAuthBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a credential to exact host-supplied identifiers. This does not parse
    /// URLs or authorize network access; the host must use the same endpoint to
    /// construct the client, and enforce its own endpoint and redirect policy.
    pub fn register(
        &mut self,
        provider: impl Into<String>,
        endpoint: impl Into<String>,
        credential: HostCredential,
    ) -> Result<&mut Self> {
        let (provider, endpoint) = (provider.into(), endpoint.into());
        if provider.trim().is_empty() || endpoint.trim().is_empty() {
            return Err(Error::Config(
                "credential provider or endpoint is empty".into(),
            ));
        }
        match self.entries.entry((provider, endpoint)) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(credential);
                Ok(self)
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                Err(Error::Config("duplicate credential binding".into()))
            }
        }
    }

    pub fn build(self) -> HostAuthSnapshot {
        HostAuthSnapshot {
            entries: Arc::new(self.entries),
        }
    }
}

/// Immutable in-memory snapshot. Rebuilding without a binding affects future
/// snapshots only; it cannot revoke secrets or clients already given to a host.
/// No file, environment, command, OAuth, or network discovery occurs.
#[derive(Clone, Default)]
pub struct HostAuthSnapshot {
    entries: Arc<BTreeMap<(String, String), HostCredential>>,
}

impl HostAuthSnapshot {
    /// Return only the credential explicitly registered for both exact values.
    pub fn get(&self, provider: &str, endpoint: &str) -> Option<&HostCredential> {
        self.entries
            .get(&(provider.to_owned(), endpoint.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_exact_and_never_replaces_or_logs_secrets() {
        let credential =
            HostCredential::new(HostCredentialKind::ApiKey, "private-fixture").unwrap();
        assert!(!format!("{credential:?}").contains("private-fixture"));
        assert!(HostCredential::new(HostCredentialKind::Bearer, " ").is_err());
        let mut builder = HostAuthBuilder::new();
        builder
            .register("provider", "https://api.example/v1", credential)
            .unwrap();
        let other = HostCredential::new(HostCredentialKind::Bearer, "other").unwrap();
        assert!(
            builder
                .register("provider", "https://api.example/v1", other)
                .is_err()
        );
        let snapshot = builder.build();
        let clone = snapshot.clone();
        drop(snapshot);
        assert_eq!(
            clone
                .get("provider", "https://api.example/v1")
                .unwrap()
                .secret(),
            "private-fixture"
        );
        assert!(clone.get("Provider", "https://api.example/v1").is_none());
        assert!(clone.get("provider", "https://api.example/v2").is_none());
        assert!(
            HostAuthSnapshot::default()
                .get("provider", "https://api.example/v1")
                .is_none()
        );
    }
}
