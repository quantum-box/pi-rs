//! Optional, isolated generation progress checkpoints (not a conversation log).
//!
//! Upstream pi-durable publishes partials at roughly 100 ms and normally persists
//! within 1 s; a crash can lose that window. This primitive has no timer or
//! background queue: the host chooses when to checkpoint, and only a successful
//! store commit is durable. It is intentionally not wired into the runtime.
//!
//! Reserve `durable/generation-progress/` and use a fresh request UUID per attempt.
//! All writers must use the same CAS protocol. A host must include its task/lease
//! checks in *every* commit, and atomically combine `prepare_finish` with the
//! terminal task transition / aborted transcript entry. Never delete the terminal
//! marker or reuse an ID: it fences late writers. Conflict means discard the stale
//! writer, not blind retry of its deltas. Storage errors can have unknown commit
//! outcome; reopen and inspect before acting. SQLite commits are synchronous and
//! may block; run them away from latency-sensitive executor threads.
//!
//! Each snapshot's serialized bytes are capped, including metadata. Overflow is
//! reported without changing the accumulator. There is no queue, spawn, implicit
//! retry or persistence deadline: the caller must stop reading or coalesce updates
//! while storage is busy (backpressure), and decide how to handle overflow. A
//! synchronous callback cannot await storage; do not put an unbounded channel
//! between it and this API. Cancellation does not cancel an in-flight commit: wait
//! for its result or treat it as unknown, then fence and finalize atomically. Drop
//! or cancellation before a commit loses uncommitted updates. Completion, abort,
//! retry accounting and cleanup remain host responsibilities.
//!
//! Recovered data is display/audit-only: it has no conversion to `Message`, must
//! never enter model context or provider requests, and includes no reasoning,
//! tool-call deltas or executable calls. Metadata may contain provider replay
//! material; treat it as opaque and do not replay it. The host may create an
//! aborted entry from the last committed snapshot; if none was committed, there
//! is no partial to restore. Upstream's retry/backoff and close-vs-crash policy
//! are not implemented here.

use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_core::ModelMetadata;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::GenerationProgress;

/// Maximum JSON-encoded bytes per stored record, including metadata and envelope.
pub const MAX_PROGRESS_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("progress exceeds the encoded byte limit")]
    TooLarge,
    #[error("generation progress is already finished")]
    Finished,
}

/// Display-only partial output. No usage is inferred from partial events.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PartialGeneration {
    pub text: String,
    pub metadata: ModelMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", content = "partial", rename_all = "snake_case")]
pub enum State {
    Running(PartialGeneration),
    /// The host may convert this into its own non-context aborted entry.
    Aborted(PartialGeneration),
    Completed,
}

/// A loaded, versioned snapshot. A stale snapshot cannot overwrite a newer one.
pub struct Snapshot {
    id: Uuid,
    version: Option<u64>,
    state: State,
}

/// Uncommitted changes; append both vectors intact to the host's atomic commit.
pub struct Prepared {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

impl Snapshot {
    /// Load an existing snapshot or start a new, as-yet uncommitted one.
    pub fn load(store: &Store, id: Uuid) -> Result<Self, Error> {
        let entry = store.get(&key(id))?;
        let (version, state) = match entry {
            Some(entry) => {
                if entry.value.len() > MAX_PROGRESS_BYTES {
                    return Err(Error::TooLarge);
                }
                (Some(entry.version), serde_json::from_slice(&entry.value)?)
            }
            None => (None, State::Running(PartialGeneration::default())),
        };
        Ok(Self { id, version, state })
    }

    /// Whether this snapshot was loaded from a committed record.
    pub fn is_persisted(&self) -> bool {
        self.version.is_some()
    }

    /// Inspect the loaded or staged display-only state.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Stage an update. Ignored events are deliberately never stored.
    /// On overflow the state remains unchanged; no text is silently truncated.
    pub fn push(&mut self, event: &GenerationProgress) -> Result<(), Error> {
        let State::Running(current) = &self.state else {
            return Err(Error::Finished);
        };
        let mut next = current.clone();
        match event {
            GenerationProgress::Text(delta) => {
                if delta.len() > MAX_PROGRESS_BYTES.saturating_sub(next.text.len()) {
                    return Err(Error::TooLarge);
                }
                next.text.push_str(delta);
            }
            GenerationProgress::Metadata(metadata) => next.metadata = metadata.clone(),
            GenerationProgress::Reasoning(_) | GenerationProgress::ToolCallDelta(_) => {
                return Ok(());
            }
        }
        encode(&State::Aborted(next.clone()))?;
        let state = State::Running(next);
        encode(&state)?;
        self.state = state;
        Ok(())
    }

    /// Prepare a checkpoint. After committing, reload before preparing another.
    pub fn prepare_checkpoint(&self) -> Result<Prepared, Error> {
        if !matches!(self.state, State::Running(_)) {
            return Err(Error::Finished);
        }
        self.prepare(&self.state)
    }

    /// Fence the attempt and retain the last staged partial for an aborted entry.
    /// Combine with the host's state/entry commit; the snapshot may include
    /// uncommitted updates observed before cancellation.
    pub fn prepare_abort(&self) -> Result<Prepared, Error> {
        let State::Running(partial) = &self.state else {
            return Err(Error::Finished);
        };
        self.prepare(&State::Aborted(partial.clone()))
    }

    /// Fence the attempt and discard its partial on authoritative completion.
    pub fn prepare_complete(&self) -> Result<Prepared, Error> {
        if !matches!(self.state, State::Running(_)) {
            return Err(Error::Finished);
        }
        self.prepare(&State::Completed)
    }

    fn prepare(&self, state: &State) -> Result<Prepared, Error> {
        let key = key(self.id);
        Ok(Prepared {
            checks: vec![Check {
                key: key.clone(),
                expected: self.version.map_or(Expected::Missing, Expected::Version),
            }],
            mutations: vec![Mutation::Put {
                key,
                value: encode(state)?,
            }],
        })
    }
}

fn key(id: Uuid) -> Vec<u8> {
    format!("durable/generation-progress/{id}").into_bytes()
}
fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() > MAX_PROGRESS_BYTES {
        return Err(Error::TooLarge);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, future, stream};
    use pi_rs_core::{CancellationToken, Error as ModelError, ModelEvent, ToolCallDelta};

    fn commit(store: &Store, prepared: Prepared) -> Result<(), durable_store::Error> {
        store.commit(&prepared.checks, &prepared.mutations)
    }

    #[tokio::test]
    async fn stream_checkpoint_reopens_and_aborts_without_context() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress.sqlite");
        let id = Uuid::new_v4();
        let metadata = ModelMetadata {
            provider_request_id: Some("req".into()),
            ..Default::default()
        };
        {
            let store = Store::open(&path).unwrap();
            let mut snapshot = Snapshot::load(&store, id).unwrap();
            let events = vec![
                Ok(ModelEvent::Text("partial".into())),
                Ok(ModelEvent::Metadata(metadata.clone())),
                Ok(ModelEvent::Reasoning("secret".into())),
                Ok(ModelEvent::ToolCallDelta(ToolCallDelta {
                    index: 0,
                    id: "id".into(),
                    name: "tool".into(),
                    arguments: "secret".into(),
                })),
                Err(ModelError::RetryableModel("retry".into())),
            ];
            let result = super::super::collect_generation_with_metadata(
                future::ready(Ok(stream::iter(events).boxed())),
                &CancellationToken::new(),
                |event| snapshot.push(&event).unwrap(),
            )
            .await;
            assert!(matches!(result, Err(ModelError::RetryableModel(_))));
            commit(&store, snapshot.prepare_checkpoint().unwrap()).unwrap();
        }
        let store = Store::open(&path).unwrap();
        let recovered = Snapshot::load(&store, id).unwrap();
        assert_eq!(
            recovered.state(),
            &State::Running(PartialGeneration {
                text: "partial".into(),
                metadata: metadata.clone()
            })
        );
        commit(&store, recovered.prepare_abort().unwrap()).unwrap();
        assert_eq!(
            Snapshot::load(&store, id).unwrap().state(),
            &State::Aborted(PartialGeneration {
                text: "partial".into(),
                metadata
            })
        );
    }

    #[tokio::test]
    async fn cancellation_keeps_only_committed_progress() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let mut staged = Snapshot::load(&store, id).unwrap();
        staged
            .push(&GenerationProgress::Text("saved".into()))
            .unwrap();
        commit(&store, staged.prepare_checkpoint().unwrap()).unwrap();
        let mut staged = Snapshot::load(&store, id).unwrap();
        let cancel = CancellationToken::new();
        let events = stream::iter(vec![Ok(ModelEvent::Text(" lost".into()))]);
        let result = super::super::collect_generation_with_metadata(
            future::ready(Ok(events.boxed())),
            &cancel,
            |event| {
                staged.push(&event).unwrap();
                cancel.cancel();
            },
        )
        .await;
        assert!(matches!(result, Err(ModelError::Cancelled)));
        let recovered = Snapshot::load(&store, id).unwrap();
        assert!(matches!(recovered.state(), State::Running(partial) if partial.text == "saved"));
        commit(&store, recovered.prepare_abort().unwrap()).unwrap();
    }

    #[test]
    fn stale_and_terminal_writers_are_fenced_atomically() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let mut a = Snapshot::load(&store, id).unwrap();
        let mut b = Snapshot::load(&store, id).unwrap();
        a.push(&GenerationProgress::Text("a".into())).unwrap();
        b.push(&GenerationProgress::Text("b".into())).unwrap();
        commit(&store, a.prepare_checkpoint().unwrap()).unwrap();
        assert!(matches!(
            commit(&store, b.prepare_checkpoint().unwrap()),
            Err(durable_store::Error::Conflict { .. })
        ));
        let current = Snapshot::load(&store, id).unwrap();
        let stale = current.prepare_checkpoint().unwrap();
        commit(&store, current.prepare_complete().unwrap()).unwrap();
        assert!(matches!(
            commit(&store, stale),
            Err(durable_store::Error::Conflict { .. })
        ));
        let mut terminal = Snapshot::load(&store, id).unwrap();
        assert!(matches!(
            terminal.push(&GenerationProgress::Text("late".into())),
            Err(Error::Finished)
        ));
        assert!(matches!(terminal.prepare_abort(), Err(Error::Finished)));
    }

    #[test]
    fn bound_covers_escaped_text_and_metadata_and_preserves_previous_value() {
        let store = Store::memory().unwrap();
        let mut snapshot = Snapshot::load(&store, Uuid::new_v4()).unwrap();
        snapshot
            .push(&GenerationProgress::Text("ok".into()))
            .unwrap();
        assert!(matches!(
            snapshot.push(&GenerationProgress::Text(
                "\n".repeat(MAX_PROGRESS_BYTES / 2)
            )),
            Err(Error::TooLarge)
        ));
        let metadata = ModelMetadata {
            provider_request_id: Some("x".repeat(MAX_PROGRESS_BYTES)),
            ..Default::default()
        };
        assert!(matches!(
            snapshot.push(&GenerationProgress::Metadata(metadata)),
            Err(Error::TooLarge)
        ));
        assert_eq!(
            snapshot.state(),
            &State::Running(PartialGeneration {
                text: "ok".into(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn host_guard_conflict_rolls_back_finish() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let snapshot = Snapshot::load(&store, id).unwrap();
        let mut prepared = snapshot.prepare_abort().unwrap();
        prepared.checks.push(Check {
            key: b"host".to_vec(),
            expected: Expected::Version(1),
        });
        assert!(matches!(
            commit(&store, prepared),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(store.get(&key(id)).unwrap().is_none());
    }
}
