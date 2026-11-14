//! Durable conversation signal inbox primitives for a future hook sleep gate.
//!
//! A prepared wake must be committed in the same transaction as the
//! conversation phase transition and its associated observer/fork writes.
//! A pending scan is only a snapshot; callers must periodically rescan.

use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("conversation not found")]
    NotFound,
    #[error("signal ID already used with different content")]
    Mismatch,
}

/// Signal IDs are scoped to a conversation and remain reserved after delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub name: String,
    pub payload: Value,
    consumed: bool,
}
impl Signal {
    pub fn new(id: Uuid, conversation_id: Uuid, name: String, payload: Value) -> Self {
        Self {
            id,
            conversation_id,
            name,
            payload,
            consumed: false,
        }
    }
    pub fn is_consumed(&self) -> bool {
        self.consumed
    }
}

fn conversation_key(id: Uuid) -> Vec<u8> {
    format!("runtime/conversation/{id}").into_bytes()
}
fn prefix(id: Uuid) -> Vec<u8> {
    format!("runtime/conversation-signal/{id}/").into_bytes()
}
fn key(conversation: Uuid, id: Uuid) -> Vec<u8> {
    format!("runtime/conversation-signal/{conversation}/{id}").into_bytes()
}
fn revision(id: Uuid) -> Vec<u8> {
    format!("runtime/conversation-signal-revision/{id}").into_bytes()
}

pub struct Inbox<'a> {
    store: &'a Store,
}
impl<'a> Inbox<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Insert once, checking the target version so deletion cannot race insertion.
    pub fn send(&self, signal: &Signal) -> Result<(), Error> {
        let k = key(signal.conversation_id, signal.id);
        let target = conversation_key(signal.conversation_id);
        let mut fresh = signal.clone();
        fresh.consumed = false;
        loop {
            if let Some(entry) = self.store.get(&k)? {
                let old: Signal = serde_json::from_slice(&entry.value)?;
                return if old.id == fresh.id
                    && old.conversation_id == fresh.conversation_id
                    && old.name == fresh.name
                    && old.payload == fresh.payload
                {
                    Ok(())
                } else {
                    Err(Error::Mismatch)
                };
            }
            let entry = self.store.get(&target)?.ok_or(Error::NotFound)?;
            let checks = [
                Check {
                    key: k.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: target.clone(),
                    expected: Expected::Version(entry.version),
                },
            ];
            let writes = [
                Mutation::Put {
                    key: k.clone(),
                    value: serde_json::to_vec(&fresh)?,
                },
                Mutation::Put {
                    key: revision(signal.conversation_id),
                    value: vec![],
                },
            ];
            match self.store.commit(&checks, &writes) {
                Ok(()) => return Ok(()),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub fn receipt(&self, conversation: Uuid, id: Uuid) -> Result<Option<Signal>, Error> {
        self.store
            .get(&key(conversation, id))?
            .map(|e| serde_json::from_slice(&e.value).map_err(Error::from))
            .transpose()
    }

    /// Prepare a wake for an absolute Unix-millisecond deadline or matching signal.
    /// Commit the returned checks and mutations together with a version-checked
    /// conversation transition. Conflict requires reloading and rescanning.
    pub fn prepare_wake(
        &self,
        conversation: Uuid,
        names: &[String],
        deadline: Option<i64>,
        now: i64,
    ) -> Result<Option<PreparedWake>, Error> {
        let revision_key = revision(conversation);
        let rev = self.store.get(&revision_key)?;
        let mut checks = vec![Check {
            key: revision_key,
            expected: rev.map_or(Expected::Missing, |e| Expected::Version(e.version)),
        }];
        let mut mutations = Vec::new();
        let mut delivered = None;
        for (k, entry) in self.store.list(&prefix(conversation))? {
            let mut signal: Signal = serde_json::from_slice(&entry.value)?;
            if !signal.consumed && names.contains(&signal.name) {
                signal.consumed = true;
                checks.push(Check {
                    key: k.clone(),
                    expected: Expected::Version(entry.version),
                });
                mutations.push(Mutation::Put {
                    key: k,
                    value: serde_json::to_vec(&signal)?,
                });
                delivered = Some(signal);
                break;
            }
        }
        if delivered.is_none() && !deadline.is_some_and(|d| now >= d) {
            return Ok(None);
        }
        Ok(Some(PreparedWake {
            signal: delivered,
            checks,
            mutations,
        }))
    }
}

/// Transaction fragments; the caller must add the conversation CAS and phase write.
pub struct PreparedWake {
    pub signal: Option<Signal>,
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn create(store: &Store, id: Uuid) {
        store
            .commit(
                &[Check {
                    key: conversation_key(id),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: conversation_key(id),
                    value: b"sleeping".to_vec(),
                }],
            )
            .unwrap();
    }
    fn commit_wake(
        store: &Store,
        id: Uuid,
        version: u64,
        mut wake: PreparedWake,
    ) -> Result<(), durable_store::Error> {
        wake.checks.push(Check {
            key: conversation_key(id),
            expected: Expected::Version(version),
        });
        wake.mutations.push(Mutation::Put {
            key: conversation_key(id),
            value: b"awake".to_vec(),
        });
        store.commit(&wake.checks, &wake.mutations)
    }

    #[test]
    fn concurrent_dedup_and_atomic_consumption_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("signals.db");
        let id = Uuid::new_v4();
        let signal = Signal::new(Uuid::new_v4(), id, "go".into(), serde_json::json!({"x": 1}));
        let store = Arc::new(Store::open(&path).unwrap());
        create(&store, id);
        let barrier = Arc::new(Barrier::new(3));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let store = store.clone();
                let signal = signal.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    Inbox::new(&store).send(&signal).unwrap();
                })
            })
            .collect();
        barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
        let inbox = Inbox::new(&store);
        let mut mismatch = signal.clone();
        mismatch.payload = serde_json::json!(2);
        assert!(matches!(inbox.send(&mismatch), Err(Error::Mismatch)));
        let version = store.get(&conversation_key(id)).unwrap().unwrap().version;
        let first = inbox
            .prepare_wake(id, &["go".into()], None, 0)
            .unwrap()
            .unwrap();
        let second = inbox
            .prepare_wake(id, &["go".into()], None, 0)
            .unwrap()
            .unwrap();
        commit_wake(&store, id, version, first).unwrap();
        assert!(matches!(
            commit_wake(&store, id, version, second),
            Err(durable_store::Error::Conflict { .. })
        ));
        drop(store);
        let store = Store::open(&path).unwrap();
        let inbox = Inbox::new(&store);
        assert!(inbox.receipt(id, signal.id).unwrap().unwrap().is_consumed());
        assert_eq!(
            store.get(&conversation_key(id)).unwrap().unwrap().value,
            b"awake"
        );
        inbox.send(&signal).unwrap();
        assert!(
            inbox
                .prepare_wake(id, &["go".into()], None, 0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn insertion_fences_a_prepared_deadline_wake() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&store, id);
        let inbox = Inbox::new(&store);
        assert!(
            inbox
                .prepare_wake(id, &["go".into()], Some(100), 99)
                .unwrap()
                .is_none()
        );
        let version = store.get(&conversation_key(id)).unwrap().unwrap().version;
        let wake = inbox
            .prepare_wake(id, &["go".into()], Some(100), 100)
            .unwrap()
            .unwrap();
        let signal = Signal::new(Uuid::new_v4(), id, "go".into(), Value::Null);
        inbox.send(&signal).unwrap();
        assert!(matches!(
            commit_wake(&store, id, version, wake),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(!inbox.receipt(id, signal.id).unwrap().unwrap().is_consumed());
        let wake = inbox
            .prepare_wake(id, &["go".into()], Some(100), 100)
            .unwrap()
            .unwrap();
        assert_eq!(wake.signal.as_ref().unwrap().id, signal.id);
        commit_wake(&store, id, version, wake).unwrap();
    }
    #[test]
    fn conversation_change_fences_wake_without_consuming() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&store, id);
        let inbox = Inbox::new(&store);
        let signal = Signal::new(Uuid::new_v4(), id, "go".into(), Value::Null);
        inbox.send(&signal).unwrap();
        let version = store.get(&conversation_key(id)).unwrap().unwrap().version;
        let wake = inbox
            .prepare_wake(id, &["go".into()], None, 0)
            .unwrap()
            .unwrap();
        store
            .commit(
                &[Check {
                    key: conversation_key(id),
                    expected: Expected::Version(version),
                }],
                &[Mutation::Put {
                    key: conversation_key(id),
                    value: b"aborted".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            commit_wake(&store, id, version, wake),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(!inbox.receipt(id, signal.id).unwrap().unwrap().is_consumed());
        let other = Uuid::new_v4();
        assert!(inbox.receipt(other, signal.id).unwrap().is_none());
        let missing = Signal::new(Uuid::new_v4(), other, "go".into(), Value::Null);
        assert!(matches!(inbox.send(&missing), Err(Error::NotFound)));
    }
}
