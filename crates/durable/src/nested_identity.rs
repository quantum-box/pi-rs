//! Host identities for nested tool backends. Identities are non-secret,
//! durable declarations covering implementation, configuration and tenant.
use pi_rs_core::Tool;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

type Entry = (Weak<dyn Tool>, Arc<RwLock<Option<String>>>);
static ENTRIES: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();

/// The host must change backend configuration only inside `update`, so a
/// pending-job poll can hold a read lease until its outcome is committed.
#[derive(Clone)]
pub struct IdentityHandle(Arc<RwLock<Option<String>>>);
impl IdentityHandle {
    /// Change configuration only when no invocation or poll holds a lease.
    /// Returns a busy error without calling `apply` rather than waiting, so it
    /// is safe to call from tool code; retry after the active call ends.
    /// `apply` must be synchronous; a panic leaves the identity unset.
    pub async fn update(
        &self,
        identity: Option<String>,
        apply: impl FnOnce(),
    ) -> Result<(), String> {
        validate(identity.as_deref())?;
        let mut guard = self.0.try_write().map_err(|_| "nested identity is busy")?;
        // A panicking host update must not leave the old identity attached to
        // possibly changed configuration.
        *guard = None;
        apply();
        *guard = identity;
        Ok(())
    }
}
fn validate(identity: Option<&str>) -> Result<(), String> {
    if identity.is_some_and(|v| v.is_empty() || v.len() > 1024) {
        return Err("nested identity must be 1..=1024 bytes".into());
    }
    Ok(())
}
/// Register the exact tool allocation. The non-secret identity must include its
/// implementation, configuration and tenant and is persisted in plaintext.
/// Re-register after process reopen; duplicate registration of a live allocation
/// is rejected. Use the returned handle for every configuration change.
pub fn register(tool: &Arc<dyn Tool>, identity: Option<String>) -> Result<IdentityHandle, String> {
    validate(identity.as_deref())?;
    let mut entries = ENTRIES
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| "nested identity registry poisoned")?;
    entries.retain(|(tool, _)| tool.strong_count() > 0);
    if entries
        .iter()
        .any(|(existing, _)| existing.ptr_eq(&Arc::downgrade(tool)))
    {
        return Err("nested tool identity already registered".into());
    }
    let binding = Arc::new(RwLock::new(identity));
    entries.push((Arc::downgrade(tool), binding.clone()));
    Ok(IdentityHandle(binding))
}
fn binding(tool: &Arc<dyn Tool>) -> Option<Arc<RwLock<Option<String>>>> {
    let mut entries = ENTRIES.get_or_init(Default::default).lock().ok()?;
    entries.retain(|(tool, _)| tool.strong_count() > 0);
    entries.iter().find_map(|(registered, binding)| {
        let registered = registered.upgrade()?;
        Arc::ptr_eq(&registered, tool).then(|| binding.clone())
    })
}
pub(crate) fn current(tool: &Arc<dyn Tool>) -> Option<String> {
    binding(tool)?.try_read().ok()?.clone()
}
pub(crate) fn execution_lease(
    tool: &Arc<dyn Tool>,
) -> Result<Option<OwnedRwLockReadGuard<Option<String>>>, ()> {
    let Some(binding) = binding(tool) else {
        return Ok(None);
    };
    binding.try_read_owned().map(Some).map_err(|_| ())
}
pub(crate) fn lease(
    tool: &Arc<dyn Tool>,
    expected: &str,
) -> Option<OwnedRwLockReadGuard<Option<String>>> {
    let guard = binding(tool)?.try_read_owned().ok()?;
    (guard.as_deref() == Some(expected)).then_some(guard)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseFailure {
    Busy,
    Mismatch,
}
pub(crate) fn checked_lease(
    tool: &Arc<dyn Tool>,
    expected: &str,
) -> Result<OwnedRwLockReadGuard<Option<String>>, LeaseFailure> {
    let binding = binding(tool).ok_or(LeaseFailure::Mismatch)?;
    let guard = binding.try_read_owned().map_err(|_| LeaseFailure::Busy)?;
    if guard.as_deref() == Some(expected) {
        Ok(guard)
    } else {
        Err(LeaseFailure::Mismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use pi_rs_core::{Outcome, ToolContext, ToolSpec};
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Dummy;
    #[async_trait::async_trait]
    impl Tool for Dummy {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "identity-test".into(),
                description: String::new(),
                parameters: json!({}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded(String::new())
        }
    }
    #[tokio::test]
    async fn identity_update_panic_is_fail_closed_and_can_be_repaired() {
        let tool: Arc<dyn Tool> = Arc::new(Dummy);
        let binding = register(&tool, Some("tenant-a".into())).unwrap();
        let changed = Arc::new(AtomicBool::new(false));
        let flag = changed.clone();
        let result =
            std::panic::AssertUnwindSafe(binding.update(Some("tenant-b".into()), move || {
                flag.store(true, Ordering::SeqCst);
                panic!("failed after changing configuration")
            }))
            .catch_unwind()
            .await;
        assert!(result.is_err());
        assert!(changed.load(Ordering::SeqCst));
        assert_eq!(current(&tool), None);
        assert!(lease(&tool, "tenant-a").is_none());
        assert!(lease(&tool, "tenant-b").is_none());
        binding
            .update(Some("tenant-b".into()), || {})
            .await
            .unwrap();
        assert_eq!(current(&tool).as_deref(), Some("tenant-b"));
    }
    #[tokio::test]
    async fn registration_and_updates_are_fenced_by_poll_lease() {
        let tool: Arc<dyn Tool> = Arc::new(Dummy);
        let binding = register(&tool, Some("a".into())).unwrap();
        assert!(register(&tool, Some("other".into())).is_err());
        let guard = lease(&tool, "a").unwrap();
        let changed = Arc::new(AtomicBool::new(false));
        let flag = changed.clone();
        assert!(
            binding
                .update(Some("b".into()), move || flag.store(true, Ordering::SeqCst))
                .await
                .is_err()
        );
        assert!(!changed.load(Ordering::SeqCst));
        drop(guard);
        let flag = changed.clone();
        binding
            .update(Some("b".into()), move || flag.store(true, Ordering::SeqCst))
            .await
            .unwrap();
        assert!(changed.load(Ordering::SeqCst));
        assert!(lease(&tool, "a").is_none());
        assert!(lease(&tool, "b").is_some());
        let weak = Arc::downgrade(&tool);
        drop(tool);
        assert!(weak.upgrade().is_none());
        let replacement: Arc<dyn Tool> = Arc::new(Dummy);
        register(&replacement, Some("new".into())).unwrap();
        assert_eq!(current(&replacement).as_deref(), Some("new"));
    }
}
