//! Process-local conversation fences, cancellation epochs and worker locks.
//!
//! The registry lives as long as the exclusive store owner. Always acquire the
//! registry before poll gates; never await while holding either. Poll code must
//! not acquire the registry while holding a gate. Acquire task gates only after
//! these fences, and never invoke a fence while holding a task gate or poll gate. A worker lock may span async
//! execution, but must not be held while waiting for another conversation.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A callback's first poll excludes conversation mutations. Mutations may
/// overlap each other and are serialized by their existing durable checks.
#[derive(Clone, Default)]
pub struct CallbackStartGate(Arc<std::sync::atomic::AtomicUsize>);

const CALLBACK_ACTIVE: usize = 1 << (usize::BITS - 1);

#[must_use = "retain the lease until the callback's first poll finishes"]
pub struct CallbackStartLease(Arc<std::sync::atomic::AtomicUsize>);

#[must_use = "retain the lease until the mutation finishes"]
pub struct MutationLease(Arc<std::sync::atomic::AtomicUsize>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallbackStartGateError {
    Poisoned,
    Missing,
}

impl CallbackStartGate {
    /// Enter as a mutation; other mutations may enter concurrently.
    pub fn try_enter(&self) -> Option<MutationLease> {
        use std::sync::atomic::Ordering;
        let mut state = self.0.load(Ordering::Relaxed);
        loop {
            if state >= CALLBACK_ACTIVE - 1 {
                return None;
            }
            match self.0.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(MutationLease(self.0.clone())),
                Err(observed) => state = observed,
            }
        }
    }

    /// Acquire exclusive permission for the callback's first poll.
    pub fn try_start(&self) -> Option<CallbackStartLease> {
        use std::sync::atomic::Ordering;
        self.0
            .compare_exchange(0, CALLBACK_ACTIVE, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;
        Some(CallbackStartLease(self.0.clone()))
    }
}
impl CallbackStartLease {
    /// Consume the exclusive lease around exactly one poll.
    pub fn poll_first<F: std::future::Future + ?Sized>(
        self,
        future: std::pin::Pin<&mut F>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        future.poll(context)
    }
}
impl Drop for CallbackStartLease {
    fn drop(&mut self) {
        self.0.store(0, std::sync::atomic::Ordering::Release);
    }
}
impl Drop for MutationLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

#[derive(Clone)]
pub struct Control {
    /// Shared start/mutation gate. Callers must opt in at both boundaries;
    /// creation alone does not fence a conversation write.
    pub callback_start: CallbackStartGate,
    pub gate: Arc<Mutex<()>>,
    pub worker: Arc<tokio::sync::Mutex<()>>,
    /// Serializes abort cleanup across direct and scheduled workers.
    pub abort_execution: Arc<tokio::sync::Mutex<()>>,
    /// Serializes summary execution with standalone cleanup marker creation.
    pub summary_execution: Arc<tokio::sync::RwLock<()>>,
    /// Serializes autonomous nested polling without holding the conversation worker.
    pub nested_poll: Arc<tokio::sync::Mutex<()>>,
    /// Next periodic drive, shared by all owners of this conversation.
    /// Read or update only while holding `worker`.
    pub next_poll: Arc<Mutex<Option<Instant>>>,
    pub cancel: CancellationToken,
    /// Cleanup survives scheduler shutdown but is fenced by later aborts.
    pub cleanup_cancel: CancellationToken,
}

/// A poisoned poll gate or a failed durable commit.
#[derive(Debug)]
pub enum FenceError<E> {
    Poisoned,
    Commit(E),
}

#[derive(Default)]
pub struct ConversationControls {
    root: CancellationToken,
    entries: Mutex<HashMap<Uuid, Control>>,
    worker_errors: Arc<Mutex<HashMap<Uuid, String>>>,
}

impl ConversationControls {
    /// Failures are shared by all drivers attached to this store owner.
    pub fn worker_errors(&self) -> Arc<Mutex<HashMap<Uuid, String>>> {
        self.worker_errors.clone()
    }

    pub fn root(&self) -> CancellationToken {
        self.root.clone()
    }

    pub fn get(&self, id: Uuid) -> Control {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .entry(id)
            .or_insert_with(|| Control {
                callback_start: CallbackStartGate::default(),
                gate: Arc::new(Mutex::new(())),
                worker: Arc::new(tokio::sync::Mutex::new(())),
                abort_execution: Arc::new(tokio::sync::Mutex::new(())),
                summary_execution: Arc::new(tokio::sync::RwLock::new(())),
                nested_poll: Arc::new(tokio::sync::Mutex::new(())),
                next_poll: Arc::new(Mutex::new(None)),
                cancel: self.root.child_token(),
                cleanup_cancel: CancellationToken::new(),
            })
            .clone()
    }

    /// Obtain a target conversation's callback-start lease without waiting.
    /// The registry lock is released before returning the lease, so a held
    /// start does not prevent an unrelated conversation from acquiring one.
    /// `None` means either the registry or the target is temporarily busy.
    pub fn try_callback_start(
        &self,
        id: Uuid,
    ) -> Result<Option<CallbackStartLease>, CallbackStartGateError> {
        use std::sync::TryLockError;
        let gate = {
            let entries = match self.entries.try_lock() {
                Ok(entries) => entries,
                Err(TryLockError::WouldBlock) => return Ok(None),
                Err(TryLockError::Poisoned(_)) => return Err(CallbackStartGateError::Poisoned),
            };
            entries
                .get(&id)
                .ok_or(CallbackStartGateError::Missing)?
                .callback_start
                .clone()
        };
        Ok(gate.try_start())
    }

    /// Fence all registered polls while `commit` discovers and persists its
    /// affected graph. Only after a successful commit are their epochs cancelled.
    /// The callback must not acquire a conversation control or await.
    pub fn fence<E>(
        &self,
        commit: impl FnOnce() -> Result<Vec<Uuid>, E>,
    ) -> Result<Vec<Uuid>, FenceError<E>> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let gates: Vec<_> = entries.values().map(|c| c.gate.clone()).collect();
        let _guards: Vec<_> = gates
            .iter()
            .map(|g| g.lock().map_err(|_| FenceError::Poisoned))
            .collect::<Result<_, _>>()?;
        let affected = commit().map_err(FenceError::Commit)?;
        for id in &affected {
            if let Some(control) = entries.get_mut(id) {
                control.cancel.cancel();
                control.cleanup_cancel.cancel();
                control.cancel = self.root.child_token();
                control.cleanup_cancel = CancellationToken::new();
            }
        }
        Ok(affected)
    }

    /// Try all current poll gates. Busy performs no commit and cancels no epochs.
    pub fn try_fence<E>(
        &self,
        commit: impl FnOnce() -> Result<Vec<Uuid>, E>,
    ) -> Result<Option<Vec<Uuid>>, FenceError<E>> {
        use std::sync::TryLockError;
        let mut entries = match self.entries.try_lock() {
            Ok(entries) => entries,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => return Err(FenceError::Poisoned),
        };
        // A nonblocking graph mutation must not cross a callback start lease.
        // Acquire every currently registered start gate before the poll gates.
        let mut start_leases = Vec::with_capacity(entries.len());
        for control in entries.values() {
            let Some(lease) = control.callback_start.try_enter() else {
                return Ok(None);
            };
            start_leases.push(lease);
        }
        let gates: Vec<_> = entries
            .values()
            .map(|control| control.gate.clone())
            .collect();
        let mut guards = Vec::with_capacity(gates.len());
        for gate in &gates {
            match gate.try_lock() {
                Ok(guard) => guards.push(guard),
                Err(TryLockError::WouldBlock) => return Ok(None),
                Err(TryLockError::Poisoned(_)) => return Err(FenceError::Poisoned),
            }
        }
        let affected = commit().map_err(FenceError::Commit)?;
        for id in &affected {
            if let Some(control) = entries.get_mut(id) {
                control.cancel.cancel();
                control.cleanup_cancel.cancel();
                control.cancel = self.root.child_token();
                control.cleanup_cancel = CancellationToken::new();
            }
        }
        Ok(Some(affected))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::owner::OwnedStore;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn callback_start_gate_is_shared_nonblocking_and_released() {
        let controls = Arc::new(ConversationControls::default());
        let id = Uuid::from_u128(1);
        let first = controls.get(id).callback_start;
        let same = controls.get(id).callback_start;
        let other = controls.get(Uuid::from_u128(2)).callback_start;
        let mutation = first.try_enter().unwrap();
        let nested = same.try_enter().unwrap();
        assert!(first.try_start().is_none());
        assert!(other.try_start().is_some());
        drop(mutation);
        assert!(first.try_start().is_none());
        drop(nested);
        let callback = first.try_start().unwrap();
        assert!(same.try_enter().is_none());
        assert!(same.try_start().is_none());
        drop(callback);
        assert!(first.try_enter().is_some());
    }

    #[test]
    fn first_poll_holds_only_target_start_lease_and_releases_it() {
        let controls = ConversationControls::default();
        let id = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        controls.get(id);
        controls.get(other);
        let lease = controls.try_callback_start(id).unwrap().unwrap();
        assert!(controls.try_callback_start(id).unwrap().is_none());
        assert!(controls.try_callback_start(other).unwrap().is_some());
        let mut future = std::pin::pin!(std::future::poll_fn(|_| {
            // Reentrant acquisition returns immediately; a distinct graph is
            // not blocked and no controls registry lock is held while polling.
            assert!(controls.try_callback_start(id).unwrap().is_none());
            assert!(controls.get(id).callback_start.try_enter().is_none());
            assert!(controls.get(other).callback_start.try_enter().is_some());
            assert!(controls.try_callback_start(other).unwrap().is_some());
            std::task::Poll::<()>::Pending
        }));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(lease.poll_first(future.as_mut(), &mut context).is_pending());
        assert!(controls.try_callback_start(id).unwrap().is_some());
    }

    #[test]
    fn callback_start_for_unknown_conversations_does_not_allocate_controls() {
        let controls = ConversationControls::default();
        for id in 1..=1024 {
            assert!(matches!(
                controls.try_callback_start(Uuid::from_u128(id)),
                Err(CallbackStartGateError::Missing)
            ));
        }
        assert!(controls.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn callback_start_lease_releases_during_unwind() {
        let gate = CallbackStartGate::default();
        let result = std::panic::catch_unwind(|| {
            let _lease = gate.try_start().unwrap();
            panic!("test unwind");
        });
        assert!(result.is_err());
        assert!(gate.try_enter().is_some());
    }

    #[test]
    fn callback_start_defers_try_fence_and_reentrant_try_is_nonblocking() {
        let controls = ConversationControls::default();
        let id = Uuid::from_u128(9);
        let start = controls.get(id).callback_start;
        let lease = start.try_start().unwrap();
        let called = std::cell::Cell::new(false);
        assert!(controls
            .try_fence(|| {
                called.set(true);
                Ok::<_, ()>(vec![id])
            })
            .unwrap()
            .is_none());
        assert!(!called.get());
        drop(lease);
        assert_eq!(
            controls
                .try_fence(|| {
                    assert!(start.try_enter().is_some());
                    assert!(start.try_start().is_none());
                    Ok::<_, ()>(vec![])
                })
                .unwrap(),
            Some(vec![])
        );
        assert!(start.try_enter().is_some());
    }

    #[test]
    fn try_fence_busy_does_not_commit_or_cancel() {
        let controls = ConversationControls::default();
        let id = Uuid::from_u128(1);
        let control = controls.get(id);
        let guard = control.gate.lock().unwrap();
        let called = std::cell::Cell::new(false);
        assert!(controls
            .try_fence(|| {
                called.set(true);
                Ok::<_, ()>(vec![id])
            })
            .unwrap()
            .is_none());
        assert!(!called.get());
        assert!(!control.cancel.is_cancelled());
        drop(guard);
        assert_eq!(
            controls.try_fence(|| Ok::<_, ()>(vec![id])).unwrap(),
            Some(vec![id])
        );
        assert!(control.cancel.is_cancelled());
        assert!(!controls.get(id).cancel.is_cancelled());
        assert!(controls.try_fence(|| Err::<Vec<Uuid>, _>(())).is_err());
        assert!(!controls.get(id).cancel.is_cancelled());
    }

    #[test]
    fn fence_waits_for_poll_then_cancels_only_committed_epochs() {
        let controls = Arc::new(ConversationControls::default());
        let id = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        let old = controls.get(id);
        let untouched = controls.get(other);
        let guard = old.gate.lock().unwrap();
        let (tx, rx) = mpsc::channel();
        let registry = controls.clone();
        let thread = std::thread::spawn(move || {
            registry
                .fence(|| {
                    tx.send(()).unwrap();
                    Ok::<_, ()>(vec![id])
                })
                .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(!old.cancel.is_cancelled());
        drop(guard);
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
        thread.join().unwrap();
        assert!(old.cancel.is_cancelled());
        assert!(!untouched.cancel.is_cancelled());
        let fresh = controls.get(id);
        assert!(!fresh.cancel.is_cancelled());
        assert!(Arc::ptr_eq(&old.gate, &fresh.gate));
        assert!(Arc::ptr_eq(&old.worker, &fresh.worker));
        assert!(controls.fence(|| Err::<Vec<Uuid>, _>(())).is_err());
        assert!(!fresh.cancel.is_cancelled());
    }

    #[test]
    fn owner_clones_share_controls_and_reopen_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let id = Uuid::from_u128(1);
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let attached = owner.clone();
        let first = owner.conversation_controls().get(id);
        let second = attached.conversation_controls().get(id);
        let failures = owner.conversation_controls().worker_errors();
        let attached_failures = attached.conversation_controls().worker_errors();
        failures.lock().unwrap().insert(id, "failed".into());
        assert_eq!(
            attached_failures
                .lock()
                .unwrap()
                .get(&id)
                .map(String::as_str),
            Some("failed")
        );
        assert!(Arc::ptr_eq(&first.worker, &second.worker));
        let worker_guard = first.worker.try_lock().unwrap();
        assert!(second.worker.try_lock().is_err());
        drop(worker_guard);
        assert!(second.worker.try_lock().is_ok());
        attached
            .conversation_controls()
            .fence(|| Ok::<_, ()>(vec![id]))
            .unwrap();
        assert!(first.cancel.is_cancelled());
        assert!(first.cleanup_cancel.is_cancelled());
        assert!(!attached
            .conversation_controls()
            .get(id)
            .cleanup_cancel
            .is_cancelled());
        attached.conversation_controls().root().cancel();
        let cleanup = attached.conversation_controls().get(id);
        assert!(cleanup.cancel.is_cancelled());
        assert!(!cleanup.cleanup_cancel.is_cancelled());
        drop(attached);
        drop(owner);
        let reopened = OwnedStore::open(&path).unwrap();
        let fresh = reopened.conversation_controls().get(id);
        assert!(reopened
            .conversation_controls()
            .worker_errors()
            .lock()
            .unwrap()
            .is_empty());
        assert!(!fresh.cancel.is_cancelled());
        assert!(!Arc::ptr_eq(&first.gate, &fresh.gate));
    }
}
