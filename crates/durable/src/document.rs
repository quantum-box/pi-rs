//! Optimistic JSON document storage and checked batch composition.
//!
//! Documents are whole JSON objects. Reads are snapshots, and every write or
//! delete requires the version read (or an explicit missing precondition).
//! A batch is inert until committed; all conditions see the pre-commit state.

use crate::task_store::OwnedBatch;
use durable_store::{Check, Expected, Mutation, Store};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

const PREFIX: &[u8] = b"durable/document/";
const OPS_PREFIX: &[u8] = b"durable/document-operations/v1/";
const POLICY_PREFIX: &[u8] = b"durable/document-policy/";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    /// A task handler and scheduler share this storage failure until settlement.
    #[error("storage: {0}")]
    TaskInputStore(#[source] std::sync::Arc<durable_store::Error>),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Signal(#[from] crate::task_store::Error),
    /// Retained for compatibility with older signal composition errors.
    #[error("signal composition is unsupported")]
    SignalComposition,
    #[error("invalid or unsupported document operations or stale base")]
    InvalidOperations,
    #[error("stored document is not a JSON object")]
    InvalidDocument,
    #[error("a batch has duplicate checks or writes, or an unchecked write")]
    InvalidBatch,
    #[error("read is not permitted after this write")]
    ReadAfterWrite,
    #[error("document must be read before writing, and written at most once")]
    UnreadOrWritten,
    #[error("duplicate task in admission")]
    DuplicateTask,
    #[error("application entries require a valid active task ancestry and a nonempty non-pi. kind")]
    InvalidApplicationEntry,
    #[error("application entry head does not match the expected ID")]
    ApplicationEntryHeadMismatch,
    #[error("session entry kind must be nonempty and outside pi.")]
    InvalidSessionEntry,
    #[error("session entries cannot pass a pending reset, summary, or write")]
    SessionEntryBlocked,
    #[error(transparent)]
    SessionAdmission(#[from] crate::state::AdmissionError),
    #[error("invalid session admission: {0}")]
    InvalidSessionAdmission(String),
}

/// An application-defined address. Components are encoded unambiguously;
/// the scope is a host convention, not an enforced ownership relationship.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Address {
    pub scope: String,
    pub name: String,
    pub key: String,
}
impl Address {
    pub(crate) fn storage_key(&self) -> Result<Vec<u8>, Error> {
        let mut result = PREFIX.to_vec();
        result.extend(serde_json::to_vec(&[&self.scope, &self.name, &self.key])?);
        Ok(result)
    }
    fn policy_key(&self) -> Result<Vec<u8>, Error> {
        let mut key = POLICY_PREFIX.to_vec();
        key.extend(serde_json::to_vec(&[&self.scope, &self.name, &self.key])?);
        Ok(key)
    }
    pub(crate) fn from_storage_key(key: &[u8]) -> Result<Self, Error> {
        let [scope, name, key] =
            serde_json::from_slice(key.strip_prefix(PREFIX).ok_or(Error::InvalidDocument)?)?;
        Ok(Self { scope, name, key })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub value: Map<String, Value>,
    pub version: u64,
}

// Immutable, forward-only evidence keyed by the globally unique predecessor version.
fn ops_key(address: &Address, version: u64) -> Result<Vec<u8>, Error> {
    let mut key = OPS_PREFIX.to_vec();
    key.extend(serde_json::to_vec(&(
        &address.scope,
        &address.name,
        &address.key,
        version,
    ))?);
    Ok(key)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredOperations {
    version: u8,
    ops: Vec<Value>,
}

// Chord numbers are JavaScript numbers. Larger integer inputs cannot be
// represented exactly, even if serde_json can hold them as u64.
fn chord_index(value: &Value) -> Result<usize, Error> {
    const MAX_SAFE: u64 = 9_007_199_254_740_991;
    let number = if let Some(n) = value.as_u64() {
        n
    } else if let Some(n) = value.as_f64() {
        if !n.is_finite() || n < 0.0 || n.fract() != 0.0 || n > MAX_SAFE as f64 {
            return Err(Error::InvalidOperations);
        }
        n as u64
    } else {
        return Err(Error::InvalidOperations);
    };
    if number > MAX_SAFE {
        return Err(Error::InvalidOperations);
    }
    usize::try_from(number).map_err(|_| Error::InvalidOperations)
}

fn path_segment(value: &Value) -> Result<String, Error> {
    if let Some(key) = value.as_str() {
        if matches!(key, "__proto__" | "constructor" | "prototype") {
            return Err(Error::InvalidOperations);
        }
        Ok(key.to_owned())
    } else {
        Ok(chord_index(value)?.to_string())
    }
}

fn resolve_path<'a>(mut value: &'a mut Value, path: &[Value]) -> Result<&'a mut Value, Error> {
    for segment in path {
        value = match value {
            Value::Object(object) => object.get_mut(&path_segment(segment)?),
            Value::Array(array) => array.get_mut(chord_index(segment)?),
            _ => None,
        }
        .ok_or(Error::InvalidOperations)?;
    }
    Ok(value)
}

fn truncate_utf16(text: &mut String, offset: usize) -> Result<(), Error> {
    let mut units = 0usize;
    let mut byte = text.len();
    for (index, ch) in text.char_indices() {
        if units == offset {
            byte = index;
            break;
        }
        let next = units + ch.len_utf16();
        if offset < next {
            return Err(Error::InvalidOperations);
        }
        units = next;
    }
    text.replace_range(..byte, "");
    Ok(())
}

// Apply decoded Chord operations with bounded work. We reject paths that would
// produce sparse arrays and UTF-16 slices that would create lone surrogates.
fn apply_operations(base: &Map<String, Value>, ops: &[Value]) -> Result<Map<String, Value>, Error> {
    if ops.is_empty()
        || ops.len() > 128
        || serde_json::to_vec(ops)?.len() > 1_048_576
        || serde_json::to_vec(base)?.len() > 2_097_152
    {
        return Err(Error::InvalidOperations);
    }
    let mut root = Value::Object(base.clone());
    for op in ops {
        let tuple = op.as_array().ok_or(Error::InvalidOperations)?;
        let verb = tuple
            .first()
            .and_then(Value::as_str)
            .ok_or(Error::InvalidOperations)?;
        if verb == "r" && tuple.len() == 2 {
            root = tuple[1].clone();
        } else {
            let path = tuple
                .get(1)
                .and_then(Value::as_array)
                .ok_or(Error::InvalidOperations)?;
            if path.len() > 8 {
                return Err(Error::InvalidOperations);
            }
            // Validate even paths whose target will not be reached.
            for segment in path {
                path_segment(segment)?;
            }
            if matches!(verb, "p" | "m") {
                let array = resolve_path(&mut root, path)?
                    .as_array_mut()
                    .ok_or(Error::InvalidOperations)?;
                match (verb, tuple.len()) {
                    ("p", 5) => {
                        let start = chord_index(&tuple[2])?.min(array.len());
                        let count = chord_index(&tuple[3])?.min(array.len() - start);
                        let items = tuple[4].as_array().ok_or(Error::InvalidOperations)?;
                        array.splice(start..start + count, items.iter().cloned());
                    }
                    ("m", 3) => {
                        let permutation = tuple[2].as_array().ok_or(Error::InvalidOperations)?;
                        if permutation.len() != array.len() {
                            return Err(Error::InvalidOperations);
                        }
                        let mut seen = vec![false; array.len()];
                        let mut reordered = Vec::with_capacity(array.len());
                        for item in permutation {
                            let index = chord_index(item)?;
                            if index >= array.len() || seen[index] {
                                return Err(Error::InvalidOperations);
                            }
                            seen[index] = true;
                            reordered.push(array[index].clone());
                        }
                        *array = reordered;
                    }
                    _ => return Err(Error::InvalidOperations),
                }
            } else {
                let (last, parents) = path.split_last().ok_or(Error::InvalidOperations)?;
                let parent = resolve_path(&mut root, parents)?;
                match parent {
                    Value::Object(object) => {
                        let key = path_segment(last)?;
                        match (verb, tuple.len()) {
                            ("s", 3) => {
                                object.insert(key, tuple[2].clone());
                            }
                            ("d", 2) => {
                                object.remove(&key);
                            }
                            ("a", 3) | ("t", 3) => {
                                let Some(Value::String(text)) = object.get_mut(&key) else {
                                    return Err(Error::InvalidOperations);
                                };
                                if verb == "a" {
                                    text.push_str(
                                        tuple[2].as_str().ok_or(Error::InvalidOperations)?,
                                    );
                                } else {
                                    truncate_utf16(text, chord_index(&tuple[2])?)?;
                                }
                            }
                            _ => return Err(Error::InvalidOperations),
                        }
                    }
                    Value::Array(array) => {
                        let index = chord_index(last)?;
                        if index > array.len() {
                            return Err(Error::InvalidOperations);
                        }
                        match (verb, tuple.len()) {
                            ("s", 3) => {
                                if index == array.len() {
                                    array.push(tuple[2].clone());
                                } else {
                                    array[index] = tuple[2].clone();
                                }
                            }
                            ("d", 2) if index < array.len() => {
                                array.remove(index);
                            }
                            ("a", 3) | ("t", 3) if index < array.len() => {
                                let Some(Value::String(text)) = array.get_mut(index) else {
                                    return Err(Error::InvalidOperations);
                                };
                                if verb == "a" {
                                    text.push_str(
                                        tuple[2].as_str().ok_or(Error::InvalidOperations)?,
                                    );
                                } else {
                                    truncate_utf16(text, chord_index(&tuple[2])?)?;
                                }
                            }
                            _ => return Err(Error::InvalidOperations),
                        }
                    }
                    _ => return Err(Error::InvalidOperations),
                }
            }
        }
        if serde_json::to_vec(&root)?.len() > 2_097_152 {
            return Err(Error::InvalidOperations);
        }
    }
    root.as_object().cloned().ok_or(Error::InvalidOperations)
}

/// Returns original operations only when their immutable evidence shares the
/// document commit and applying them to the proven predecessor yields the result.
pub(crate) fn committed_operations(
    store: &Store,
    address: &Address,
    previous: &Document,
    current: &Document,
    sequence: u64,
) -> Result<Option<Vec<Value>>, Error> {
    let key = ops_key(address, previous.version)?;
    let entry = match store.get_bounded(&key, 1_100_000) {
        Ok(entry) => entry,
        Err(durable_store::Error::DocumentFrameTooLarge) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let Some(entry) = entry else { return Ok(None) };
    if store.commit_for_version(&key, entry.version)? != Some(sequence) {
        return Ok(None);
    }
    if entry.value.len() > 1_100_000 {
        return Ok(None);
    }
    let Ok(record) = serde_json::from_slice::<StoredOperations>(&entry.value) else {
        return Ok(None);
    };
    if record.version != 1
        || apply_operations(&previous.value, &record.ops).ok().as_ref() != Some(&current.value)
    {
        return Ok(None);
    }
    Ok(Some(record.ops))
}

/// Fork behavior for a document address. Missing policies retain legacy copy behavior.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ForkPolicy {
    /// Copy the value at the selected boundary.
    Copy,
    /// Copy the latest committed value and policy when the fork commits.
    Current,
    /// Start each fork with this stored initial JSON object.
    Initial { value: Map<String, Value> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocumentForkPolicy {
    pub policy: ForkPolicy,
    pub version: u64,
}

/// A database-local committed document state, including deletion.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentRevision {
    pub revision: u64,
    pub document: Option<Document>,
}

/// Final document states written by one atomic store commit.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentCommit {
    pub revision: u64,
    pub changes: Vec<(Address, Option<Document>)>,
}

/// Pull-based stream of complete document commits across the database.
pub struct DocumentCommitWatch<'a> {
    store: &'a Store,
    cursor: u64,
}
impl DocumentCommitWatch<'_> {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Return up to `limit` complete commits (1..=256). Errors do not advance the cursor.
    pub fn poll(&mut self, limit: usize) -> Result<Vec<DocumentCommit>, Error> {
        let frames = self.store.document_commits(self.cursor, limit)?;
        let result = frames
            .into_iter()
            .map(|(revision, changes)| {
                Ok(DocumentCommit {
                    revision,
                    changes: changes
                        .into_iter()
                        .map(|(key, entry)| {
                            Ok((Address::from_storage_key(&key)?, Documents::decode(entry)?))
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if let Some(last) = result.last() {
            self.cursor = last.revision;
        }
        Ok(result)
    }
}

/// One committed document operation frame. `actual` marks a verified original
/// batch; other puts are derived or replacements. A deletion is a retirement.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentOperationFrame {
    pub revision: u64,
    pub document: Option<Document>,
    pub ops: Vec<Value>,
    pub retired: bool,
    /// True only for a verified original opt-in operation batch.
    pub actual: bool,
}

/// Pull-based watch. Persist the cursor only after consuming its entire page.
pub struct DocumentWatch<'a> {
    store: &'a Store,
    key: Vec<u8>,
    cursor: u64,
}
impl DocumentWatch<'_> {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Return one operation frame per committed state. A put is a root replacement;
    /// a delete is a retirement. On error the cursor is unchanged.
    pub fn poll_operations(&mut self, limit: usize) -> Result<Vec<DocumentOperationFrame>, Error> {
        Ok(self
            .poll(limit)?
            .into_iter()
            .map(|change| {
                let ops = change
                    .document
                    .as_ref()
                    .map(|document| vec![serde_json::json!(["r", document.value])])
                    .unwrap_or_default();
                DocumentOperationFrame {
                    revision: change.revision,
                    retired: change.document.is_none(),
                    document: change.document,
                    ops,
                    actual: false,
                }
            })
            .collect())
    }

    /// Derive incremental Chord operations from successive immutable document
    /// states. Replacements are used for creation or unsafe/large diffs.
    /// The caller must retain the value at the input cursor. On error the
    /// cursor is unchanged; deletions remain retirement frames.
    pub fn poll_incremental_operations(
        &mut self,
        limit: usize,
    ) -> Result<Vec<DocumentOperationFrame>, Error> {
        self.poll_incremental(limit, false)
    }

    /// Prefer the original opt-in batches when their atomic commit and result
    /// can be verified. Legacy writes and missing evidence use derived operations.
    /// The cursor advances only after the entire page has been validated.
    pub fn poll_recorded_operations(
        &mut self,
        limit: usize,
    ) -> Result<Vec<DocumentOperationFrame>, Error> {
        self.poll_incremental(limit, true)
    }

    fn poll_incremental(
        &mut self,
        limit: usize,
        prefer_recorded: bool,
    ) -> Result<Vec<DocumentOperationFrame>, Error> {
        let mut previous = Documents::decode(self.store.document_at(&self.key, self.cursor)?)?;
        let changes = self.store.document_changes(&self.key, self.cursor, limit)?;
        let mut frames = Vec::with_capacity(changes.len());
        let address = if prefer_recorded {
            Some(Address::from_storage_key(&self.key)?)
        } else {
            None
        };
        for (revision, entry) in changes {
            let document = Documents::decode(entry)?;
            let recorded = match (&address, &previous, &document) {
                (Some(address), Some(old), Some(new)) => {
                    match self.store.document_commit_sequence(revision)? {
                        Some(sequence)
                            if self.store.commit_for_version(&self.key, new.version)?
                                == Some(sequence) =>
                        {
                            committed_operations(self.store, address, old, new, sequence)?
                        }
                        _ => None,
                    }
                }
                _ => None,
            };
            let actual = recorded.is_some();
            let ops = if let Some(ops) = recorded {
                ops
            } else {
                match (&previous, &document) {
                    (Some(old), Some(new)) => crate::chord_ops::diff(
                        &Value::Object(old.value.clone()),
                        &Value::Object(new.value.clone()),
                    ),
                    (None, Some(new)) => {
                        crate::chord_ops::replacement(&Value::Object(new.value.clone()))
                    }
                    (_, None) => Vec::new(),
                }
            };
            frames.push(DocumentOperationFrame {
                revision,
                retired: document.is_none(),
                document: document.clone(),
                ops,
                actual,
            });
            previous = document;
        }
        if let Some(last) = frames.last() {
            self.cursor = last.revision;
        }
        Ok(frames)
    }

    /// Return at most `limit` changes (1..=256), without waiting.
    /// On error the cursor is unchanged.
    pub fn poll(&mut self, limit: usize) -> Result<Vec<DocumentRevision>, Error> {
        let changes = self.store.document_changes(&self.key, self.cursor, limit)?;
        let result = changes
            .into_iter()
            .map(|(revision, entry)| {
                Ok(DocumentRevision {
                    revision,
                    document: Documents::decode(entry)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if let Some(last) = result.last() {
            self.cursor = last.revision;
        }
        Ok(result)
    }
}

/// Pull watch for an explicitly acquired task document. It ends after the
/// terminal commit's retirement has been delivered (or after all changes have
/// been drained if the document was already absent).
pub struct TaskDocumentWatch<'a> {
    inner: DocumentWatch<'a>,
    owner: uuid::Uuid,
    last: Option<Document>,
    ended: bool,
}
impl TaskDocumentWatch<'_> {
    pub fn ended(&self) -> bool {
        self.ended
    }
    pub fn cursor(&self) -> u64 {
        self.inner.cursor()
    }
    pub fn poll(&mut self, limit: usize) -> Result<Vec<DocumentOperationFrame>, Error> {
        self.poll_inner(limit, || {})
    }

    #[cfg(test)]
    pub(crate) fn poll_after_boundary<F: FnOnce()>(
        &mut self,
        limit: usize,
        after_boundary: F,
    ) -> Result<Vec<DocumentOperationFrame>, Error> {
        // Model a commit arriving immediately after the former boundary read.
        self.inner.store.commit_sequence()?;
        self.poll_inner(limit, after_boundary)
    }

    fn poll_inner<F: FnOnce()>(
        &mut self,
        limit: usize,
        before_fetch: F,
    ) -> Result<Vec<DocumentOperationFrame>, Error> {
        if self.ended {
            return Ok(Vec::new());
        }
        let cursor = self.inner.cursor;
        before_fetch();
        let result = (|| {
            let mut frames = self.inner.poll_operations(limit)?;
            // Read the task *after* the page. A terminal task cannot admit a
            // later document mutation, so a subsequent empty lookahead proves
            // this page contains the final change even after terminal re-saves.
            let terminal = crate::task_store::TaskStore::new(self.inner.store)
                .load(self.owner)?
                .is_some_and(|stored| stored.task.is_terminal());
            let drained = if terminal {
                self.inner
                    .store
                    .document_changes(&self.inner.key, self.inner.cursor, 1)?
                    .is_empty()
            } else {
                false
            };
            let mut last = self.last.clone();
            for frame in &mut frames {
                if frame.retired {
                    frame.document = last.clone();
                } else {
                    last = frame.document.clone();
                }
            }
            self.last = last;
            self.ended = terminal && drained;
            Ok(frames)
        })();
        if result.is_err() {
            self.inner.cursor = cursor;
        }
        result
    }
}

/// Invalid configuration for a document listener.
#[derive(Debug, thiserror::Error)]
#[error("document listener interval must be nonzero")]
pub struct DocumentListenerConfigError;

/// Callback driver for a borrowed document watch. It reads at most one
/// size-checked frame per callback and acknowledges only successful delivery.
pub struct DocumentListener<'a> {
    watch: Option<DocumentWatch<'a>>,
    cursor: u64,
    interval: std::time::Duration,
    end: Option<crate::runtime::MergedListenerEnd>,
}

impl<'a> DocumentWatch<'a> {
    /// Transfer this watch into an opt-in callback driver. Source closure is
    /// host-signalled because a borrowed Store cannot know its writers' lifetime.
    pub fn into_listener(
        self,
        interval: std::time::Duration,
    ) -> Result<DocumentListener<'a>, DocumentListenerConfigError> {
        if interval.is_zero() {
            return Err(DocumentListenerConfigError);
        }
        Ok(DocumentListener {
            cursor: self.cursor,
            watch: Some(self),
            interval,
            end: None,
        })
    }

    fn bounded_next(&self) -> Result<Option<DocumentOperationFrame>, Error> {
        const MAX_DOCUMENT: usize = 2 * 1024 * 1024;
        let Some((revision, entry)) =
            self.store
                .next_document_change_bounded(&self.key, self.cursor, MAX_DOCUMENT)?
        else {
            return Ok(None);
        };
        let previous = Documents::decode(self.store.document_at_bounded(
            &self.key,
            self.cursor,
            MAX_DOCUMENT,
        )?)?;
        let document = Documents::decode(entry)?;
        let address = Address::from_storage_key(&self.key)?;
        let recorded = match (&previous, &document) {
            (Some(old), Some(new)) => match self.store.document_commit_sequence(revision)? {
                Some(seq)
                    if self.store.commit_for_version(&self.key, new.version)? == Some(seq) =>
                {
                    committed_operations(self.store, &address, old, new, seq)?
                }
                _ => None,
            },
            _ => None,
        };
        let actual = recorded.is_some();
        let ops = if let Some(ops) = recorded {
            ops
        } else {
            match (&previous, &document) {
                (Some(old), Some(new)) => crate::chord_ops::diff(
                    &Value::Object(old.value.clone()),
                    &Value::Object(new.value.clone()),
                ),
                (None, Some(new)) => {
                    crate::chord_ops::replacement(&Value::Object(new.value.clone()))
                }
                (_, None) => Vec::new(),
            }
        };
        Ok(Some(DocumentOperationFrame {
            revision,
            retired: document.is_none(),
            document,
            ops,
            actual,
        }))
    }
}

impl DocumentListener<'_> {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
    pub fn end_reason(&self) -> Option<&crate::runtime::MergedListenerEnd> {
        self.end.as_ref()
    }
    fn finish(
        &mut self,
        reason: crate::runtime::MergedListenerEnd,
    ) -> crate::runtime::MergedListenerEnd {
        if self.end.is_none() {
            self.end = Some(reason);
            self.watch = None;
        }
        self.end.as_ref().expect("terminal reason recorded").clone()
    }

    /// Deliver frames serially. Stop, abort and source-close are checked in
    /// that order; an in-flight callback is awaited before acknowledging it.
    /// Dropping this future leaves its in-flight frame unacknowledged.
    pub async fn run<F, Fut>(
        &mut self,
        signals: &crate::runtime::MergedListenerSignals,
        mut listener: F,
    ) -> crate::runtime::MergedListenerEnd
    where
        F: FnMut(DocumentOperationFrame) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        use crate::runtime::MergedListenerEnd as End;
        use futures_util::FutureExt;
        if let Some(end) = &self.end {
            return end.clone();
        }
        loop {
            if signals.stop.is_cancelled() {
                return self.finish(End::Stopped);
            }
            if signals.abort.is_cancelled() {
                return self.finish(End::Aborted);
            }
            if signals.source_closed.is_cancelled() {
                return self.finish(End::Ended);
            }
            let frame = match self.watch.as_ref().expect("listener open").bounded_next() {
                Ok(frame) => frame,
                Err(error) => return self.finish(End::Failed(error.to_string())),
            };
            let Some(frame) = frame else {
                tokio::select! { biased;
                    _ = signals.stop.cancelled() => return self.finish(End::Stopped),
                    _ = signals.abort.cancelled() => return self.finish(End::Aborted),
                    _ = signals.source_closed.cancelled() => return self.finish(End::Ended),
                    _ = tokio::time::sleep(self.interval) => {},
                }
                continue;
            };
            if signals.stop.is_cancelled() {
                return self.finish(End::Stopped);
            }
            if signals.abort.is_cancelled() {
                return self.finish(End::Aborted);
            }
            if signals.source_closed.is_cancelled() {
                return self.finish(End::Ended);
            }
            let revision = frame.revision;
            let callback =
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener(frame))) {
                    Ok(callback) => callback,
                    Err(payload) => {
                        crate::extension::discard_panic_payload(payload);
                        return self.finish(End::ListenerError("listener panicked".into()));
                    }
                };
            let callback = std::panic::AssertUnwindSafe(callback).catch_unwind();
            tokio::pin!(callback);
            let (result, signalled) = tokio::select! { biased;
                result = &mut callback => (result, None),
                _ = signals.stop.cancelled() => (callback.await, Some(End::Stopped)),
                _ = signals.abort.cancelled() => (callback.await, Some(End::Aborted)),
                _ = signals.source_closed.cancelled() => (callback.await, Some(End::Ended)),
            };
            let failure = match result {
                Ok(Ok(())) => {
                    self.cursor = revision;
                    self.watch.as_mut().expect("listener open").cursor = revision;
                    None
                }
                Ok(Err(error)) => Some(End::ListenerError(error)),
                Err(payload) => {
                    crate::extension::discard_panic_payload(payload);
                    Some(End::ListenerError("listener panicked".into()))
                }
            };
            if let Some(end) = signalled.or(failure) {
                return self.finish(end);
            }
        }
    }
}

pub struct Documents<'a> {
    store: &'a Store,
}
impl<'a> Documents<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }
    /// Capture all live documents and attach a commit stream at the same boundary.
    pub fn watch_commits(&self) -> Result<(DocumentCommit, DocumentCommitWatch<'a>), Error> {
        let revision = self.store.document_snapshot()?;
        let changes = self
            .store
            .documents_at(revision)?
            .into_iter()
            .map(|(key, entry)| Ok((Address::from_storage_key(&key)?, Self::decode(Some(entry))?)))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok((
            DocumentCommit { revision, changes },
            DocumentCommitWatch {
                store: self.store,
                cursor: revision,
            },
        ))
    }

    /// Resume after a fully consumed commit in the same database.
    pub fn resume_commit_watch(&self, cursor: u64) -> Result<DocumentCommitWatch<'a>, Error> {
        if cursor > self.store.document_snapshot()? {
            return Err(durable_store::Error::InvalidSnapshot.into());
        }
        Ok(DocumentCommitWatch {
            store: self.store,
            cursor,
        })
    }
    /// Attach at a consistent immutable boundary, even if a writer commits
    /// between capturing the boundary and reading the initial document.
    pub fn watch(&self, address: &Address) -> Result<(DocumentRevision, DocumentWatch<'a>), Error> {
        let key = address.storage_key()?;
        let revision = self.store.document_snapshot()?;
        let document = Self::decode(self.store.document_at(&key, revision)?)?;
        Ok((
            DocumentRevision { revision, document },
            DocumentWatch {
                store: self.store,
                key,
                cursor: revision,
            },
        ))
    }

    /// Attach a pull watch to an explicitly task-owned document.
    pub fn watch_task_document(
        &self,
        address: &Address,
    ) -> Result<(DocumentRevision, TaskDocumentWatch<'a>), Error> {
        let owner = self
            .store
            .task_document_owner(&address.storage_key()?)?
            .ok_or(Error::InvalidBatch)?;
        let (initial, inner) = self.watch(address)?;
        let last = initial.document.clone();
        Ok((
            initial,
            TaskDocumentWatch {
                inner,
                owner,
                last,
                ended: false,
            },
        ))
    }

    /// Resume a task-document watch from a consumed document revision.
    pub fn resume_task_document_watch(
        &self,
        address: &Address,
        cursor: u64,
    ) -> Result<TaskDocumentWatch<'a>, Error> {
        let owner = self
            .store
            .task_document_owner(&address.storage_key()?)?
            .ok_or(Error::InvalidBatch)?;
        let last = self.get_at(address, cursor)?;
        let inner = self.resume_watch(address, cursor)?;
        Ok(TaskDocumentWatch {
            inner,
            owner,
            last,
            ended: false,
        })
    }

    /// Resume after a previously consumed cursor in the same database.
    pub fn resume_watch(&self, address: &Address, cursor: u64) -> Result<DocumentWatch<'a>, Error> {
        let key = address.storage_key()?;
        // Validate the boundary without consuming changes.
        self.store.document_at(&key, cursor)?;
        Ok(DocumentWatch {
            store: self.store,
            key,
            cursor,
        })
    }
    /// Capture a store-wide document boundary, reusable after reopen.
    pub fn snapshot(&self) -> Result<u64, Error> {
        Ok(self.store.document_snapshot()?)
    }
    /// Prepare copies of one conversation scope at an immutable boundary.
    pub(crate) fn fork_scope(
        &self,
        parent: uuid::Uuid,
        child: uuid::Uuid,
        revision: u64,
    ) -> Result<OwnedBatch, Error> {
        self.fork_scope_from(parent, parent, child, revision)
    }

    /// Historical values come from the entry owner; current-policy values come
    /// from the immediate parent. Both are covered by the same revision fence.
    pub(crate) fn fork_scope_from(
        &self,
        parent: uuid::Uuid,
        current_parent: uuid::Uuid,
        child: uuid::Uuid,
        revision: u64,
    ) -> Result<OwnedBatch, Error> {
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        let source = crate::application_entry::key(parent);
        if let Some(entry) = self.store.application_entries_at(&source, revision)? {
            // Validate before copying; the original IDs and authors remain immutable.
            let _: Vec<crate::application_entry::ApplicationEntry> =
                serde_json::from_slice(&entry.value)?;
            let target = crate::application_entry::key(child);
            batch.checks.push(Check {
                key: target.clone(),
                expected: Expected::Missing,
            });
            batch.mutations.push(Mutation::Put {
                key: target,
                value: entry.value,
            });
        }
        // A global revision fence also detects addresses created or deleted while
        // preparing the copy. The caller retries this batch on a fence conflict.
        let current = self.store.document_snapshot()?;
        let lineage_key = crate::application_entry::lineage_key(child);
        batch.checks.push(Check {
            key: lineage_key.clone(),
            expected: Expected::Missing,
        });
        batch.mutations.push(Mutation::Put {
            key: lineage_key,
            value: serde_json::to_vec(&crate::application_entry::Lineage {
                parent: current_parent,
                parent_revision: current,
            })?,
        });
        batch.checks.push(Check {
            key: b"durable/document-fork-revision".to_vec(),
            expected: Expected::DocumentRevision(current),
        });
        // Addresses are canonical JSON arrays; this includes the closing quote
        // and comma, so one scope cannot match a different scope's prefix.
        let scope_prefix = |base: &[u8], scope: uuid::Uuid| {
            let mut key = base.to_vec();
            key.extend_from_slice(format!("[\"{scope}\",").as_bytes());
            key
        };
        let historical_docs: HashMap<_, _> = self
            .store
            .scoped_documents_at(revision, &scope_prefix(PREFIX, parent))?
            .into_iter()
            .collect();
        let historical_policies: HashMap<_, _> = self
            .store
            .scoped_documents_at(revision, &scope_prefix(POLICY_PREFIX, parent))?
            .into_iter()
            .collect();
        let current_docs: HashMap<_, _> = self
            .store
            .scoped_documents_at(current, &scope_prefix(PREFIX, current_parent))?
            .into_iter()
            .collect();
        let current_policies: HashMap<_, _> = self
            .store
            .scoped_documents_at(current, &scope_prefix(POLICY_PREFIX, current_parent))?
            .into_iter()
            .collect();
        // Normalize both scopes to the historical owner's address; matching
        // names and family keys identify the same logical document.
        let mut addresses = HashSet::new();
        for (key, scope) in historical_docs
            .keys()
            .map(|k| (k, parent))
            .chain(current_docs.keys().map(|k| (k, current_parent)))
        {
            let mut address = Address::from_storage_key(key)?;
            if address.scope == scope.to_string() {
                address.scope = parent.to_string();
                addresses.insert(address);
            }
        }
        for (key, scope) in historical_policies
            .keys()
            .map(|k| (k, parent))
            .chain(current_policies.keys().map(|k| (k, current_parent)))
        {
            let [found_scope, name, key]: [String; 3] = serde_json::from_slice(
                key.strip_prefix(POLICY_PREFIX)
                    .ok_or(Error::InvalidDocument)?,
            )?;
            if found_scope == scope.to_string() {
                addresses.insert(Address {
                    scope: parent.to_string(),
                    name,
                    key,
                });
            }
        }
        for address in addresses {
            let doc_key = address.storage_key()?;
            let policy_key = address.policy_key()?;
            let current_address = Address {
                scope: current_parent.to_string(),
                ..address.clone()
            };
            let current_doc_key = current_address.storage_key()?;
            let current_policy_key = current_address.policy_key()?;
            let old_policy = historical_policies.get(&policy_key);
            let latest_policy = current_policies.get(&current_policy_key);
            let parse =
                |entry: Option<&durable_store::Entry>| -> Result<Option<ForkPolicy>, Error> {
                    entry
                        .map(|e| serde_json::from_slice(&e.value).map_err(Error::from))
                        .transpose()
                };
            let old = parse(old_policy)?;
            let latest = parse(latest_policy)?;
            // Existing historical addresses keep their cut policy; addresses first
            // introduced later participate only when their latest policy is Current.
            let use_current = matches!(old, Some(ForkPolicy::Current))
                || (old_policy.is_none()
                    && !historical_docs.contains_key(&doc_key)
                    && matches!(latest, Some(ForkPolicy::Current)));
            let (doc, policy_entry, policy) = if use_current {
                (current_docs.get(&current_doc_key), latest_policy, latest)
            } else {
                (historical_docs.get(&doc_key), old_policy, old)
            };
            let target_address = Address {
                scope: child.to_string(),
                name: address.name,
                key: address.key,
            };
            if let Some(entry) = doc {
                Self::decode(Some(entry.clone()))?;
                let value = if !use_current {
                    match policy {
                        Some(ForkPolicy::Initial { value }) => serde_json::to_vec(&value)?,
                        _ => entry.value.clone(),
                    }
                } else {
                    entry.value.clone()
                };
                let target = target_address.storage_key()?;
                batch.checks.push(Check {
                    key: target.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put { key: target, value });
            }
            if let Some(entry) = policy_entry {
                let target = target_address.policy_key()?;
                batch.checks.push(Check {
                    key: target.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put {
                    key: target,
                    value: entry.value.clone(),
                });
            }
        }
        Ok(batch)
    }
    /// Read one address at a previously captured document boundary.
    pub fn get_at(&self, address: &Address, snapshot: u64) -> Result<Option<Document>, Error> {
        Self::decode(self.store.document_at(&address.storage_key()?, snapshot)?)
    }
    /// Read an explicitly configured policy and its CAS version; absence means copy.
    pub fn fork_policy(&self, address: &Address) -> Result<Option<DocumentForkPolicy>, Error> {
        self.store
            .get(&address.policy_key()?)?
            .map(|entry| {
                Ok(DocumentForkPolicy {
                    policy: serde_json::from_slice(&entry.value)?,
                    version: entry.version,
                })
            })
            .transpose()
    }
    pub fn get(&self, address: &Address) -> Result<Option<Document>, Error> {
        Self::decode(self.store.get(&address.storage_key()?)?)
    }
    pub(crate) fn decode(entry: Option<durable_store::Entry>) -> Result<Option<Document>, Error> {
        entry
            .map(|entry| {
                let value = serde_json::from_slice::<Value>(&entry.value)?;
                let Value::Object(value) = value else {
                    return Err(Error::InvalidDocument);
                };
                Ok(Document {
                    value,
                    version: entry.version,
                })
            })
            .transpose()
    }
}

/// An all-or-nothing collection of checked writes. Identical checks may be
/// shared, but writes must be unique and ownership insertions must not
/// invalidate another component's view of the graph.
#[derive(Clone, Default)]
pub struct Transaction {
    checks: Vec<Check>,
    mutations: Vec<Mutation>,
    checked: HashSet<Vec<u8>>,
    written: HashSet<Vec<u8>>,
    acquisitions: Vec<durable_store::TaskDocumentAcquisition>,
}

// Independently prepared task, ownership, conversation and signal observations can race.
// Compose their fences before appending batches so disagreement becomes a
// normal commit conflict.
fn merge_task_expected(a: &Expected, b: &Expected) -> Expected {
    use Expected::*;
    match (a, b) {
        (Missing, MissingBefore(d)) | (MissingBefore(d), Missing) => MissingBefore(*d),
        (MissingBefore(a), MissingBefore(b)) => MissingBefore((*a).min(*b)),
        (Missing, MissingUnchangedSince(v)) | (MissingUnchangedSince(v), Missing) => {
            MissingUnchangedSince(*v)
        }
        (MissingUnchangedSince(a), MissingUnchangedSince(b)) => MissingUnchangedSince((*a).min(*b)),
        (MissingBefore(d), MissingUnchangedSince(v))
        | (MissingUnchangedSince(v), MissingBefore(d)) => MissingUnchangedSinceBefore(*v, *d),
        (Missing, MissingUnchangedSinceBefore(v, d))
        | (MissingUnchangedSinceBefore(v, d), Missing) => MissingUnchangedSinceBefore(*v, *d),
        (MissingBefore(a), MissingUnchangedSinceBefore(v, b))
        | (MissingUnchangedSinceBefore(v, b), MissingBefore(a)) => {
            MissingUnchangedSinceBefore(*v, (*a).min(*b))
        }
        (MissingUnchangedSince(a), MissingUnchangedSinceBefore(b, d))
        | (MissingUnchangedSinceBefore(b, d), MissingUnchangedSince(a)) => {
            MissingUnchangedSinceBefore((*a).min(*b), *d)
        }
        (MissingUnchangedSinceBefore(a, x), MissingUnchangedSinceBefore(b, y)) => {
            MissingUnchangedSinceBefore((*a).min(*b), (*x).min(*y))
        }
        (Version(a), VersionBefore(b, d)) | (VersionBefore(b, d), Version(a)) if a == b => {
            VersionBefore(*a, *d)
        }
        (VersionBefore(a, x), VersionBefore(b, y)) if a == b => VersionBefore(*a, (*x).min(*y)),
        (a, b) if a == b => a.clone(),
        _ => VersionBefore(0, std::time::Instant::now()),
    }
}

pub(crate) fn reconcile_task_checks(documents: &mut OwnedBatch, graph: &mut OwnedBatch) {
    for other in &mut graph.checks {
        if !other.key.starts_with(crate::task_store::PREFIX)
            && !other.key.starts_with(b"durable/signal-revision/")
            && !other.key.starts_with(crate::ownership_edge::PREFIX)
            && !other.key.starts_with(b"durable/ownership-abort-fence/v1/")
            && !other.key.starts_with(b"runtime/conversation/")
            && !other.key.starts_with(b"runtime/owned-cleanup-attempt/")
        {
            continue;
        }
        if let Some(check) = documents.checks.iter_mut().find(|c| c.key == other.key) {
            let expected = merge_task_expected(&check.expected, &other.expected);
            check.expected = expected.clone();
            other.expected = expected;
        }
    }
}
impl Transaction {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn put(
        &mut self,
        address: &Address,
        expected: Expected,
        value: &Map<String, Value>,
    ) -> Result<(), Error> {
        let key = address.storage_key()?;
        self.append(OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected,
            }],
            mutations: vec![Mutation::Put {
                key,
                value: serde_json::to_vec(value)?,
            }],
        })
    }
    /// Explicitly acquire an absent address for a task. Acquisition and any
    /// document write are committed atomically. Existing host documents cannot
    /// be silently converted; ownership is permanent for this address.
    pub fn acquire_task_document(
        &mut self,
        address: &Address,
        task: uuid::Uuid,
    ) -> Result<(), Error> {
        let doc = address.storage_key()?;
        if self
            .acquisitions
            .iter()
            .any(|item| item.document_key == doc)
        {
            return Err(Error::InvalidBatch);
        }
        self.acquisitions
            .push(durable_store::TaskDocumentAcquisition {
                document_key: doc,
                owner: task,
            });
        Ok(())
    }
    /// Apply and retain a bounded decoded Chord batch against an existing version.
    /// The operation record and document update commit atomically; stale bases fail.
    pub fn apply_operations(
        &mut self,
        store: &Store,
        address: &Address,
        version: u64,
        ops: &[Value],
    ) -> Result<(), Error> {
        let base = Documents::new(store)
            .get(address)?
            .filter(|d| d.version == version)
            .ok_or(Error::InvalidOperations)?;
        let value = apply_operations(&base.value, ops)?;
        let key = address.storage_key()?;
        let evidence = ops_key(address, version)?;
        let record = serde_json::to_vec(&StoredOperations {
            version: 1,
            ops: ops.to_vec(),
        })?;
        self.append(OwnedBatch {
            checks: vec![
                Check {
                    key: key.clone(),
                    expected: Expected::Version(version),
                },
                Check {
                    key: evidence.clone(),
                    expected: Expected::Missing,
                },
            ],
            mutations: vec![
                Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value)?,
                },
                Mutation::Put {
                    key: evidence,
                    value: record,
                },
            ],
        })
    }
    /// Set a policy using its own CAS version; can be composed atomically with document writes.
    pub fn set_fork_policy(
        &mut self,
        address: &Address,
        expected: Expected,
        policy: &ForkPolicy,
    ) -> Result<(), Error> {
        let key = address.policy_key()?;
        self.append(OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected,
            }],
            mutations: vec![Mutation::Put {
                key,
                value: serde_json::to_vec(policy)?,
            }],
        })
    }
    pub fn delete(&mut self, address: &Address, version: u64) -> Result<(), Error> {
        let key = address.storage_key()?;
        self.append(OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected: Expected::Version(version),
            }],
            mutations: vec![Mutation::Delete { key }],
        })
    }
    /// Compose a prepared task or host batch. Every mutated key must be checked
    /// in this component; no state is added if validation fails. Never use raw
    /// host batches to bypass the document API's JSON or namespace invariants.
    pub fn append(&mut self, batch: OwnedBatch) -> Result<(), Error> {
        let mut checks = self.checked.clone();
        let mut writes = self.written.clone();
        let mut local_checks = HashSet::new();
        let mut unique = Vec::new();
        for check in &batch.checks {
            if checks.insert(check.key.clone()) {
                unique.push(check.clone());
            } else if !self
                .checks
                .iter()
                .chain(unique.iter())
                .any(|existing| existing == check)
            {
                return Err(Error::InvalidBatch);
            }
            local_checks.insert(check.key.clone());
        }
        for mutation in &batch.mutations {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if !local_checks.contains(key) || !writes.insert(key.clone()) {
                return Err(Error::InvalidBatch);
            }
        }
        // Independently prepared graph changes can each observe a missing
        // owner and together introduce a cycle. Checks see the pre-commit
        // state, so identical checks alone cannot detect this composition.
        let graph_key = |key: &[u8]| {
            key.starts_with(crate::task_store::PREFIX)
                || key.starts_with(crate::ownership_edge::PREFIX)
        };
        if batch.checks.iter().any(|check| {
            graph_key(&check.key)
                && matches!(
                    check.expected,
                    Expected::Missing
                        | Expected::MissingBefore(_)
                        | Expected::MissingUnchangedSince(_)
                        | Expected::MissingUnchangedSinceBefore(_, _)
                )
                && ((self.checked.contains(&check.key)
                    && writes.contains(&check.key)
                    && !self.written.contains(&check.key))
                    || self.written.contains(&check.key))
        }) {
            return Err(Error::InvalidBatch);
        }
        self.checked = checks;
        self.written = writes;
        self.checks.extend(unique);
        self.mutations.extend(batch.mutations);
        Ok(())
    }
    pub(crate) fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }
    pub(crate) fn checks(&self) -> &[Check] {
        &self.checks
    }

    // Only used after a reservation-only receipt scan is repeated. A handler's
    // own signal observation, including a deadline, must never be rebased.
    pub(crate) fn refresh_reservation_revision(
        &mut self,
        key: &[u8],
        old: &Expected,
        new: Expected,
    ) -> bool {
        if !matches!(old, Expected::Missing | Expected::Version(_))
            || !matches!(new, Expected::Missing | Expected::Version(_))
            || self.mutations.iter().any(|mutation| match mutation {
                Mutation::Put { key: written, .. } | Mutation::Delete { key: written } => {
                    written == key
                }
            })
        {
            return false;
        }
        let Some(check) = self.checks.iter_mut().find(|check| check.key == key) else {
            return false;
        };
        if &check.expected != old {
            return false;
        }
        check.expected = new;
        true
    }

    pub(crate) fn refresh_reservation_conversations(&mut self, old: u64, new: u64) -> bool {
        let Some(check) = self.checks.iter_mut().find(|check| {
            check.key == b"runtime/conversation/"
                && check.expected == Expected::PrefixUnchangedSince(old)
        }) else {
            return false;
        };
        check.expected = Expected::PrefixUnchangedSince(new);
        true
    }

    pub(crate) fn assemble_submission_notifications(
        self,
        store: &Store,
        settlements: &[crate::runtime::SettlementNotification],
        now: &[i64],
    ) -> Result<Self, Error> {
        if settlements.is_empty() {
            return Ok(self);
        }
        let mut host = HostTransaction::new(store);
        host.append_runtime_candidate(OwnedBatch {
            checks: self.checks,
            mutations: self.mutations,
        })?;
        let token = crate::runtime::submission_settled_signal();
        if settlements.len() != now.len() {
            return Err(Error::InvalidBatch);
        }
        for ((notify, value, settlement), now) in settlements.iter().zip(now) {
            if host.signal_submission_at(
                notify.task_id,
                &token,
                &notify.key,
                value.clone(),
                *now,
                *settlement,
            )? == crate::task_store::SignalResult::Invalid
            {
                return Err(Error::InvalidSessionAdmission(
                    "invalid submission notification".into(),
                ));
            }
        }
        let mut transaction = Transaction::new();
        transaction.append(host.into_batch()?)?;
        transaction.acquisitions = self.acquisitions;
        Ok(transaction)
    }

    pub(crate) fn into_owned_batch(self) -> Result<OwnedBatch, Error> {
        if !self.acquisitions.is_empty() {
            return Err(Error::InvalidBatch);
        }
        Ok(OwnedBatch {
            checks: self.checks,
            mutations: self.mutations,
        })
    }
    pub fn commit(self, store: &Store) -> Result<(), Error> {
        store.commit_with_task_documents(&self.checks, &self.mutations, &self.acquisitions)?;
        Ok(())
    }
    /// Rebase only observations of the same running task, preserving deadline
    /// fences and every independently observed version. Replace exactly the
    /// prepared candidate, leaving staged documents, signals and entries intact.
    pub(crate) fn rebase_task_candidate(
        &mut self,
        id: uuid::Uuid,
        old_version: u64,
        new_version: u64,
        old: &crate::task::Task,
        next: &crate::task::Task,
    ) -> Result<(), Error> {
        let key = crate::task_store::key(id);
        let old_value = serde_json::to_vec(old)?;
        let next_value = serde_json::to_vec(next)?;
        let mut found_check = false;
        for check in &mut self.checks {
            if check.key != key {
                continue;
            }
            match &mut check.expected {
                Expected::Version(version) | Expected::VersionBefore(version, _)
                    if *version == old_version =>
                {
                    *version = new_version;
                    found_check = true;
                }
                _ => (),
            }
        }
        let mut found_mutation = false;
        for mutation in &mut self.mutations {
            if let Mutation::Put {
                key: mutation_key,
                value,
            } = mutation
                && *mutation_key == key
            {
                if found_mutation || *value != old_value {
                    return Err(Error::InvalidBatch);
                }
                *value = next_value.clone();
                found_mutation = true;
            }
        }
        if !found_check || !found_mutation {
            return Err(Error::InvalidBatch);
        }
        Ok(())
    }

    pub(crate) fn commit_task_graph(&self, store: &Store, version: u64) -> Result<(), Error> {
        store.commit_task_documents_with_prefix_put_fence(
            &self.checks,
            &self.mutations,
            &self.acquisitions,
            &[
                crate::task_store::PREFIX,
                crate::ownership_edge::PREFIX,
                b"runtime/conversation/",
            ],
            version,
        )?;
        Ok(())
    }
}

/// A store-bound checked host transaction with provisional typed deliveries.
/// It supports multiple typed deliveries and an explicit typed-signal wake.
/// Deliveries evaluate task and signal writes already staged in the transaction.
pub struct HostTransaction<'a> {
    store: &'a Store,
    transaction: Transaction,
    signal_targets: HashSet<Vec<u8>>,
    missing_signal_targets: HashSet<Vec<u8>>,
    effect_signal_targets: HashSet<Vec<u8>>,
    wake_targets: HashSet<Vec<u8>>,
    signals: StagedSignals,
}
impl<'a> HostTransaction<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            transaction: Transaction::new(),
            signal_targets: HashSet::new(),
            missing_signal_targets: HashSet::new(),
            effect_signal_targets: HashSet::new(),
            wake_targets: HashSet::new(),
            signals: StagedSignals::new(),
        }
    }
    pub fn put(
        &mut self,
        address: &Address,
        expected: Expected,
        value: &Map<String, Value>,
    ) -> Result<(), Error> {
        self.transaction.put(address, expected, value)
    }
    pub fn delete(&mut self, address: &Address, version: u64) -> Result<(), Error> {
        self.transaction.delete(address, version)
    }
    /// Compose a checked batch. Raw signal namespace writes and later writes
    /// to already-signalled tasks are rejected.
    pub fn append(&mut self, batch: OwnedBatch) -> Result<(), Error> {
        if batch.mutations.iter().any(|mutation| {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            self.signal_targets.contains(key)
                || key.starts_with(b"durable/signal/")
                || key.starts_with(b"durable/signal-revision/")
        }) {
            return Err(Error::InvalidBatch);
        }
        self.transaction.append(batch)
    }

    /// Compose session task creations after a no-op signal observed the same
    /// target missing. All other post-signal task mutations remain forbidden.
    pub(crate) fn append_session_candidate(
        &mut self,
        batch: OwnedBatch,
        created: &[crate::task::Task],
        parents: &[crate::task_store::StoredTask],
    ) -> Result<(), Error> {
        for mutation in &batch.mutations {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if key.starts_with(b"durable/signal/") || key.starts_with(b"durable/signal-revision/") {
                return Err(Error::InvalidBatch);
            }
            if self.signal_targets.contains(key) {
                let allowed = matches!(mutation, Mutation::Put { .. })
                    && self.missing_signal_targets.contains(key)
                    && !self.wake_targets.contains(key)
                    && created
                        .iter()
                        .any(|task| crate::task_store::key(task.id()) == *key)
                    && batch.checks.iter().any(|check| {
                        check.key == *key
                            && matches!(
                                check.expected,
                                Expected::MissingUnchangedSince(_)
                                    | Expected::MissingUnchangedSinceBefore(_, _)
                            )
                    });
                let same_parent = match mutation {
                    Mutation::Put { key, value }
                        if !self.missing_signal_targets.contains(key)
                            && !self.wake_targets.contains(key) =>
                    {
                        parents.iter().any(|parent| {
                            crate::task_store::key(parent.task.id()) == *key
                                && serde_json::to_vec(&parent.task).is_ok_and(|old| old == *value)
                                && batch.checks.iter().any(|check| {
                                    check.key == *key
                                        && match check.expected {
                                            Expected::Version(v)
                                            | Expected::VersionBefore(v, _) => v == parent.version,
                                            _ => false,
                                        }
                                })
                        })
                    }
                    _ => false,
                };
                if !allowed && !same_parent {
                    return Err(Error::InvalidBatch);
                }
            }
        }
        self.transaction.append(batch)
    }
    /// Import an already prepared runtime batch before assembling deliveries.
    /// Signal writes remain private to this host transaction and are visible
    /// to later deliveries; public callers cannot inject raw signal records.
    pub(crate) fn append_runtime_candidate(&mut self, batch: OwnedBatch) -> Result<(), Error> {
        if !self.transaction.checks.is_empty() || !self.signals.batch.checks.is_empty() {
            return Err(Error::InvalidBatch);
        }
        let mut validated = Transaction::new();
        validated.append(batch)?;
        let is_signal = |key: &[u8]| {
            key.starts_with(b"durable/signal/") || key.starts_with(b"durable/signal-revision/")
        };
        let mut ordinary = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for check in validated.checks {
            if is_signal(&check.key) {
                self.signals.batch.checks.push(check);
            } else {
                ordinary.checks.push(check);
            }
        }
        for mutation in validated.mutations {
            let key = match &mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if is_signal(key) {
                self.signals.batch.mutations.push(mutation);
            } else {
                ordinary.mutations.push(mutation);
            }
        }
        self.transaction.append(ordinary)
    }
    pub(crate) fn has_signal_mutations(&self) -> bool {
        !self.signals.batch.mutations.is_empty()
    }
    /// Stage typed deliveries after candidate task and signal writes. Results
    /// remain provisional until commit; later target writes are rejected.
    pub fn signal_at(
        &mut self,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
    ) -> Result<crate::task_store::SignalResult, Error> {
        self.signal_at_inner(task_id, token, key, value, now_millis, None)
    }

    // Only the runtime's settlement commit may supply this authorization.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signal_submission_at(
        &mut self,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
        settlement: (uuid::Uuid, uuid::Uuid),
    ) -> Result<crate::task_store::SignalResult, Error> {
        self.signal_at_inner(task_id, token, key, value, now_millis, Some(settlement))
    }

    #[allow(clippy::too_many_arguments)]
    fn signal_at_inner(
        &mut self,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
        settlement: Option<(uuid::Uuid, uuid::Uuid)>,
    ) -> Result<crate::task_store::SignalResult, Error> {
        let target = crate::task_store::key(task_id);
        if self.wake_targets.contains(&target) {
            return Err(Error::SignalComposition);
        }
        let candidate = self
            .transaction
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                Mutation::Put { key, value } if *key == target => {
                    Some(serde_json::from_slice(value).map(Some))
                }
                Mutation::Delete { key } if *key == target => Some(Ok(None)),
                _ => None,
            })
            .transpose()?;
        let mutations = self.signals.batch.mutations.clone();
        let previous_mutations = self.signals.batch.mutations.len();
        let result = self.signals.signal_at_overlay(
            self.store, task_id, token, key, value, now_millis, None, candidate, &mutations,
            settlement,
        )?;
        if self.signals.batch.mutations.len() != previous_mutations {
            self.effect_signal_targets.insert(target.clone());
        }
        if result == crate::task_store::SignalResult::NotFound
            && !self.effect_signal_targets.contains(&target)
        {
            self.missing_signal_targets.insert(target.clone());
        } else {
            self.missing_signal_targets.remove(&target);
        }
        self.signal_targets.insert(target);
        Ok(result)
    }
    /// Wake a typed wait using committed and staged signals, choosing the first
    /// available key in wait order. The transition and consumption are atomic.
    /// This does not perform time-only or deadline-expiration wakes.
    pub fn wake_with_signal(&mut self, task_id: uuid::Uuid) -> Result<(), Error> {
        self.prepare_wake(task_id, None)
    }
    /// Wake a typed sleep using the supplied scheduler clock. Available signals
    /// take precedence; otherwise the deadline is due at `now_millis >= until`.
    /// A not-yet-due wait returns an error without modifying the transaction.
    pub fn wake_at(&mut self, task_id: uuid::Uuid, now_millis: i64) -> Result<(), Error> {
        self.prepare_wake(task_id, Some(now_millis))
    }
    fn prepare_wake(&mut self, task_id: uuid::Uuid, now_millis: Option<i64>) -> Result<(), Error> {
        let target = crate::task_store::key(task_id);
        if self.transaction.written.contains(&target) {
            return Err(Error::SignalComposition);
        }
        // Prepare in private copies so a failed validation cannot alter a
        // previously staged delivery or any checked document writes.
        let mut signals = OwnedBatch {
            checks: self.signals.batch.checks.clone(),
            mutations: self.signals.batch.mutations.clone(),
        };
        let mut graph = crate::task_store::TaskStore::new(self.store).prepare_staged_signal_wake(
            task_id,
            &mut signals,
            now_millis,
        )?;
        let mut transaction = Transaction {
            checks: self.transaction.checks.clone(),
            mutations: self.transaction.mutations.clone(),
            checked: self.transaction.checked.clone(),
            written: self.transaction.written.clone(),
            acquisitions: self.transaction.acquisitions.clone(),
        };
        let mut existing = OwnedBatch {
            checks: std::mem::take(&mut transaction.checks),
            mutations: Vec::new(),
        };
        reconcile_task_checks(&mut signals, &mut graph);
        reconcile_task_checks(&mut signals, &mut existing);
        reconcile_task_checks(&mut graph, &mut existing);
        transaction.checks = existing.checks;
        transaction.append(graph)?;
        self.signals.batch = signals;
        self.transaction = transaction;
        self.wake_targets.insert(target.clone());
        self.signal_targets.insert(target);
        Ok(())
    }
    /// Backward-compatible name for `wake_with_signal`.
    pub fn wake_with_staged_signal(&mut self, task_id: uuid::Uuid) -> Result<(), Error> {
        self.wake_with_signal(task_id)
    }
    pub(crate) fn into_batch(mut self) -> Result<OwnedBatch, Error> {
        if !self.transaction.acquisitions.is_empty() {
            return Err(Error::InvalidBatch);
        }
        let mut existing = OwnedBatch {
            checks: std::mem::take(&mut self.transaction.checks),
            mutations: Vec::new(),
        };
        reconcile_task_checks(&mut self.signals.batch, &mut existing);
        // Candidate task writes already carry the reconciled pre-commit
        // check. Do not re-append a Missing check for the same task creation:
        // Transaction normally rejects independently prepared graph adoption.
        self.signals.batch.checks.retain(|check| {
            !(self.signal_targets.contains(&check.key)
                && self.transaction.written.contains(&check.key)
                && existing.checks.iter().any(|old| old == check))
        });
        self.transaction.checks = existing.checks;
        self.transaction.append(self.signals.batch)?;
        Ok(OwnedBatch {
            checks: self.transaction.checks,
            mutations: self.transaction.mutations,
        })
    }
    pub fn commit(self) -> Result<(), Error> {
        let store = self.store;
        let batch = self.into_batch()?;
        store.commit_with_task_documents(&batch.checks, &batch.mutations, &[])?;
        Ok(())
    }
}

/// Checked document operations for one admission attempt. Reads are cached and
/// validated at commit, including missing and read-only documents. Once any
/// write is staged, further reads are rejected. A conflict reruns the caller.
pub struct AdmissionDocuments<'a> {
    store: &'a Store,
    reads: HashMap<Address, Option<Document>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    tasks: Vec<crate::task::Task>,
}

type PendingEntries = (
    uuid::Uuid,
    Option<u64>,
    Vec<crate::application_entry::ApplicationEntry>,
    Vec<Check>,
    bool, // whether an entry was staged
);

#[derive(Clone, Copy)]
enum StagedSignalStatus {
    Invalid,
    NotFound,
    ClosedTarget,
    ClosedKey,
    Present,
}

#[derive(Clone)]
struct StagedSignals {
    batch: OwnedBatch,
    keys: HashMap<(uuid::Uuid, String, String), StagedSignalStatus>,
    equivalent_self_versions: HashSet<u64>,
    canonical_self_version: Option<u64>,
}
impl StagedSignals {
    fn new() -> Self {
        Self {
            batch: OwnedBatch {
                checks: Vec::new(),
                mutations: Vec::new(),
            },
            keys: HashMap::new(),
            equivalent_self_versions: HashSet::new(),
            canonical_self_version: None,
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn signal_at(
        &mut self,
        store: &Store,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
        invocation: Option<&crate::task::Task>,
    ) -> Result<crate::task_store::SignalResult, Error> {
        self.signal_at_overlay(
            store,
            task_id,
            token,
            key,
            value,
            now_millis,
            invocation,
            None,
            &[],
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn signal_at_overlay(
        &mut self,
        store: &Store,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
        invocation: Option<&crate::task::Task>,
        candidate: Option<Option<crate::task::Task>>,
        mutations: &[Mutation],
        settlement: Option<(uuid::Uuid, uuid::Uuid)>,
    ) -> Result<crate::task_store::SignalResult, Error> {
        let identity = (task_id, token.name().to_owned(), key.to_owned());
        // A generic token of another version can have staged this same key.
        // Built-in delivery must inspect that candidate, not trust the cached
        // status produced under the other token's schema.
        let builtin = token.name() == "pi.submission-settled" && token.version() == 1;
        if !builtin && let Some(previous) = self.keys.get(&identity).copied() {
            match previous {
                StagedSignalStatus::NotFound => {
                    return Ok(crate::task_store::SignalResult::NotFound);
                }
                StagedSignalStatus::ClosedTarget => {
                    return Ok(crate::task_store::SignalResult::Closed);
                }
                StagedSignalStatus::Present | StagedSignalStatus::ClosedKey => {
                    return match crate::task_store::Signal::new_typed(
                        uuid::Uuid::nil(),
                        task_id,
                        token,
                        key,
                        value,
                    ) {
                        Ok(_) => Ok(match previous {
                            StagedSignalStatus::Present => {
                                crate::task_store::SignalResult::Duplicate
                            }
                            StagedSignalStatus::ClosedKey => {
                                crate::task_store::SignalResult::Closed
                            }
                            _ => unreachable!(),
                        }),
                        Err(crate::task_store::Error::InvalidSignalPayload) => {
                            Ok(crate::task_store::SignalResult::Invalid)
                        }
                        Err(error) => Err(error.into()),
                    };
                }
                StagedSignalStatus::Invalid => {}
            }
        }
        let target = crate::task_store::key(task_id);
        let tasks = crate::task_store::TaskStore::new(store);
        let overlay = if builtin && mutations.is_empty() {
            &self.batch.mutations
        } else {
            mutations
        };
        let (result, mut batch, equivalent_version) = loop {
            let (result, batch) = tasks.prepare_signal_at_overlay(
                task_id,
                token,
                key,
                value.clone(),
                || now_millis,
                candidate.clone(),
                overlay,
                settlement,
            )?;
            let Some(task) = invocation.filter(|task| {
                task.id() == task_id
                    && task.state() == &crate::task::TaskState::Running
                    && !task.abort_requested()
            }) else {
                break (result, batch, None);
            };
            let observed = batch
                .checks
                .iter()
                .find(|check| check.key == target)
                .and_then(|check| match check.expected {
                    Expected::Version(version) | Expected::VersionBefore(version, _) => {
                        Some(version)
                    }
                    _ => None,
                });
            let Some(version) = observed else {
                break (result, batch, None);
            };
            let current = tasks.load(task_id)?;
            match current {
                Some(current) if current.task == *task && current.version == version => {
                    break (result, batch, Some(version));
                }
                Some(current) if current.task == *task => continue,
                _ => break (result, batch, None),
            }
        };
        if let Some(version) = equivalent_version {
            self.equivalent_self_versions.insert(version);
            let canonical = *self.canonical_self_version.get_or_insert(version);
            for check in &mut batch.checks {
                if check.key == target {
                    match &mut check.expected {
                        Expected::Version(v) | Expected::VersionBefore(v, _) if *v == version => {
                            *v = canonical
                        }
                        _ => {}
                    }
                }
            }
        }
        let staged_status = match result {
            crate::task_store::SignalResult::Invalid => StagedSignalStatus::Invalid,
            crate::task_store::SignalResult::NotFound => StagedSignalStatus::NotFound,
            crate::task_store::SignalResult::Delivered
            | crate::task_store::SignalResult::Duplicate => StagedSignalStatus::Present,
            crate::task_store::SignalResult::Closed
                if batch
                    .checks
                    .iter()
                    .any(|check| check.key == crate::task_store::signal_revision_key(task_id)) =>
            {
                StagedSignalStatus::ClosedKey
            }
            crate::task_store::SignalResult::Closed => StagedSignalStatus::ClosedTarget,
        };
        // Only the target and its revision may be shared between deliveries.
        // Other overlaps remain errors when the complete transaction is composed.
        let shared = [
            crate::task_store::key(task_id),
            crate::task_store::signal_revision_key(task_id),
        ];
        for check in batch.checks {
            if shared.contains(&check.key)
                && let Some(existing) = self.batch.checks.iter_mut().find(|c| c.key == check.key)
            {
                existing.expected = merge_task_expected(&existing.expected, &check.expected);
                continue;
            }
            self.batch.checks.push(check);
        }
        for mutation in batch.mutations {
            if let Mutation::Put { key, .. } = &mutation
                && *key == shared[1]
                && self
                    .batch
                    .mutations
                    .iter()
                    .any(|m| matches!(m, Mutation::Put { key: k, .. } if k == key))
            {
                continue;
            }
            self.batch.mutations.push(mutation);
        }
        self.keys.insert(identity, staged_status);
        Ok(result)
    }

    fn rebase_self_check(&mut self, task: uuid::Uuid, version: u64) {
        let key = crate::task_store::key(task);
        for check in &mut self.batch.checks {
            if check.key == key {
                match &mut check.expected {
                    Expected::Version(v) | Expected::VersionBefore(v, _)
                        if self.equivalent_self_versions.contains(v) =>
                    {
                        *v = version
                    }
                    _ => {}
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct TaskInputIntent {
    pub conversation: uuid::Uuid,
    pub version: u64,
    pub ancestry_checks: Vec<Check>,
    pub cross_conversation: bool,
    pub snapshot: Vec<u8>,
    pub admissions: Vec<TaskAdmission>,
}

#[derive(Clone)]
pub(crate) struct TaskAdmission {
    pub request_id: Option<String>,
    pub draft: TaskSubmissionDraft,
    pub notify: Option<crate::runtime::SubmissionNotify>,
    pub notification_binding: Option<Vec<u8>>,
}

#[derive(Clone)]
pub(crate) enum TaskSubmissionDraft {
    Continue,
    Input {
        content: String,
        options: crate::state::AdmissionOptions,
    },
    Write {
        kind: String,
        data: Option<Value>,
    },
}

/// Checked document operations at one invocation-wide immutable revision.
/// Staged values are visible to later reads. A staged new document has version
/// zero; existing staged documents retain their original version until commit.
pub struct TaskDocuments<'a> {
    store: &'a Store,
    input_validator: Option<&'a crate::runtime::Harness>,
    revision: u64,
    reads: HashMap<Address, Option<Document>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    acquired: HashSet<Address>,
    task: crate::task::Task,
    entries: Option<PendingEntries>,
    entry_initial_len: usize,
    runtime_cut: Option<usize>,
    runtime_config: Option<crate::runtime::Config>,
    task_input: Option<TaskInputIntent>,
    pub(crate) task_input_conflict: Option<Vec<u8>>,
    pub(crate) task_input_storage: Option<std::sync::Arc<durable_store::Error>>,
    signals: StagedSignals,
    child_reads: HashMap<uuid::Uuid, Option<crate::task_store::StoredTask>>,
    pub(crate) keyed_children: Vec<(String, crate::task::Task, crate::keyed_child::Prepared)>,
    pub(crate) keyed_child_handlers:
        HashMap<String, std::sync::Arc<dyn crate::task_scheduler::Handler>>,
}

/// An invocation's staged effects, retaining its store owner without borrowing it.
/// This is not an executor lease: the scheduler must still fence the attempt,
/// validate children and commit the parent transition with the prepared batch.
#[allow(dead_code)]
#[derive(Clone)]
pub struct DetachedTaskDocuments {
    owner: std::sync::Arc<durable_store::owner::OwnedStore>,
    reservation: crate::task_store::StoredTask,
    revision: u64,
    reads: HashMap<Address, Option<Document>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    acquired: HashSet<Address>,
    entries: Option<PendingEntries>,
    entry_initial_len: usize,
    runtime_cut: Option<usize>,
    runtime_config: Option<crate::runtime::Config>,
    task_input: Option<TaskInputIntent>,
    pub(crate) task_input_conflict: Option<Vec<u8>>,
    pub(crate) task_input_storage: Option<std::sync::Arc<durable_store::Error>>,
    signals: StagedSignals,
    child_reads: HashMap<uuid::Uuid, Option<crate::task_store::StoredTask>>,
    pub(crate) keyed_children: Vec<(String, crate::task::Task, crate::keyed_child::Prepared)>,
    pub(crate) keyed_child_handlers:
        HashMap<String, std::sync::Arc<dyn crate::task_scheduler::Handler>>,
}

/// Read-only inspection of a staged keyed child and its original checks.
pub struct StagedKeyedChild<'a> {
    pub key: &'a str,
    pub task: &'a crate::task::Task,
    pub tool: &'a str,
    pub arguments: &'a Value,
    pub parent_version: u64,
    pub checks: &'a [Check],
    pub mutations: &'a [Mutation],
    pub handler_bound: bool,
}

/// Read-only inspection of staged application entries and their checks.
pub struct StagedApplicationEntries<'a> {
    pub conversation: uuid::Uuid,
    pub version: Option<u64>,
    pub entries: &'a [crate::application_entry::ApplicationEntry],
    pub checks: &'a [Check],
    pub dirty: bool,
    pub initial_len: usize,
    pub runtime_cut: Option<usize>,
    pub runtime_config: Option<&'a crate::runtime::Config>,
}

impl DetachedTaskDocuments {
    pub fn reservation(&self) -> &crate::task_store::StoredTask {
        &self.reservation
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn reads(&self) -> &HashMap<Address, Option<Document>> {
        &self.reads
    }
    pub fn writes(&self) -> &HashMap<Address, Option<Map<String, Value>>> {
        &self.writes
    }
    pub fn acquisitions(&self) -> &HashSet<Address> {
        &self.acquired
    }
    pub fn signal_batch(&self) -> &OwnedBatch {
        &self.signals.batch
    }
    pub fn child_reads(&self) -> &HashMap<uuid::Uuid, Option<crate::task_store::StoredTask>> {
        &self.child_reads
    }
    pub fn keyed_children(&self) -> impl Iterator<Item = StagedKeyedChild<'_>> {
        self.keyed_children
            .iter()
            .map(|(key, task, prepared)| StagedKeyedChild {
                key,
                task,
                tool: prepared.tool(),
                arguments: prepared.arguments(),
                parent_version: prepared.parent_version,
                checks: &prepared.checks,
                mutations: &prepared.mutations,
                handler_bound: self.keyed_child_handlers.contains_key(key),
            })
    }
    pub fn application_entries(&self) -> Option<StagedApplicationEntries<'_>> {
        self.entries
            .as_ref()
            .map(
                |(conversation, version, entries, checks, dirty)| StagedApplicationEntries {
                    conversation: *conversation,
                    version: *version,
                    entries,
                    checks,
                    dirty: *dirty,
                    initial_len: self.entry_initial_len,
                    runtime_cut: self.runtime_cut,
                    runtime_config: self.runtime_config.as_ref(),
                },
            )
    }
}

/// Prepared effects remain bound to the same owner and reservation. The caller
/// must compose the parent and validated child graph before committing.
#[allow(dead_code)]
pub(crate) struct PreparedTaskDocuments {
    pub(crate) owner: std::sync::Arc<durable_store::owner::OwnedStore>,
    pub(crate) reservation: crate::task_store::StoredTask,
    pub(crate) transaction: Transaction,
    pub(crate) document_batch: OwnedBatch,
    pub(crate) task_input: Option<TaskInputIntent>,
    pub(crate) keyed_children: Vec<(String, crate::task::Task, crate::keyed_child::Prepared)>,
    pub(crate) keyed_child_handlers:
        HashMap<String, std::sync::Arc<dyn crate::task_scheduler::Handler>>,
}

#[allow(dead_code)]
impl DetachedTaskDocuments {
    /// Prepare only against the same unchanged running attempt. Same-value
    /// reservation rewrites may advance its version, as in the scheduler.
    /// The eventual atomic commit fences the supplied version; re-reading here
    /// would discard a completed invocation before the scheduler can rebase it.
    pub(crate) fn prepare(
        self,
        current: &crate::task_store::StoredTask,
    ) -> Result<PreparedTaskDocuments, Error> {
        if current.task != self.reservation.task
            || current.task.state() != &crate::task::TaskState::Running
            || current.task.abort_requested()
        {
            return Err(Error::Store(durable_store::Error::Conflict {
                key: crate::task_store::key(self.reservation.task.id()),
            }));
        }
        if let Some(error) = &self.task_input_storage {
            return Err(Error::TaskInputStore(error.clone()));
        }
        if let Some(key) = &self.task_input_conflict {
            return Err(Error::Store(durable_store::Error::Conflict {
                key: key.clone(),
            }));
        }
        let Self {
            owner,
            reservation,
            revision,
            reads,
            writes,
            acquired,
            entries,
            entry_initial_len,
            runtime_cut,
            runtime_config,
            task_input,
            task_input_conflict,
            task_input_storage,
            signals,
            child_reads,
            mut keyed_children,
            keyed_child_handlers,
        } = self;
        let mut docs = TaskDocuments {
            store: owner.store(),
            input_validator: None,
            revision,
            reads,
            writes,
            acquired,
            task: reservation.task.clone(),
            entries,
            entry_initial_len,
            runtime_cut,
            runtime_config,
            task_input: None,
            task_input_conflict,
            task_input_storage,
            signals,
            child_reads,
            keyed_children: Vec::new(),
            keyed_child_handlers: HashMap::new(),
        };
        let parent_key = crate::task_store::key(current.task.id());
        for (_, _, prepared) in &mut keyed_children {
            for check in &mut prepared.checks {
                if check.key == parent_key
                    && check.expected == Expected::Version(prepared.parent_version)
                {
                    check.expected = Expected::Version(current.version);
                }
            }
        }
        let mut transaction = Transaction::new();
        docs.stage_acquisitions(&mut transaction)?;
        docs.rebase_self_signal_check(current.version);
        let mut document_batch = docs.into_batch()?;
        for check in &mut document_batch.checks {
            if check.key == parent_key {
                match &mut check.expected {
                    Expected::Version(version) | Expected::VersionBefore(version, _)
                        if *version == reservation.version =>
                    {
                        *version = current.version
                    }
                    _ => (),
                }
            }
        }
        Ok(PreparedTaskDocuments {
            owner,
            reservation: current.clone(),
            transaction,
            document_batch,
            task_input,
            keyed_children,
            keyed_child_handlers,
        })
    }
}

impl<'a> TaskDocuments<'a> {
    /// Read unsafe child evidence together with a prefix fence for the
    /// parent's eventual checked transition. Terminal state is checked by the
    /// caller and its own child read remains part of the same transaction.
    pub(crate) fn read_unsafe_child_evidence(
        &mut self,
        id: uuid::Uuid,
    ) -> Result<(Vec<crate::async_task_tool::UnsafeDurableReceipt>, bool), String> {
        if self.input_validator.is_some() {
            return Err("unsafe child evidence requires a standalone scheduler".into());
        }
        let boundary = self.store.put_version().map_err(|e| e.to_string())?;
        let prefix = format!("durable/unsafe-park-async/{id}/").into_bytes();
        let has_intent = self
            .store
            .list(&prefix)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|(key, _)| key.ends_with(b"/intent"));
        let receipts = crate::async_task_tool::unsafe_park_proof::read_receipts(self.store, id)?;
        if receipts.len() > 1 {
            return Err("unsafe child has conflicting terminal receipts".into());
        }
        if let Some(check) = self
            .signals
            .batch
            .checks
            .iter_mut()
            .find(|check| check.key == prefix)
        {
            match &mut check.expected {
                Expected::PrefixUnchangedSince(old) => *old = (*old).min(boundary),
                _ => return Err("incompatible unsafe child evidence fence".into()),
            }
        } else {
            self.signals.batch.checks.push(Check {
                key: prefix,
                expected: Expected::PrefixUnchangedSince(boundary),
            });
        }
        Ok((receipts, has_intent))
    }
    pub(crate) fn stage_unsafe_park_proof(
        &mut self,
        checkpoint: &crate::task::Checkpoint,
        wait: &crate::task::WaitFor,
    ) -> Result<(), String> {
        let (check, mutation) = crate::async_task_tool::unsafe_park_proof::prepare_park(
            self.store, &self.task, checkpoint, wait,
        )?;
        self.signals.batch.checks.push(check);
        self.signals.batch.mutations.push(mutation);
        Ok(())
    }
    pub(crate) fn stage_checkpointable_durable_result(
        &mut self,
        intent_key: Vec<u8>,
        result: &impl serde::Serialize,
    ) -> Result<(), String> {
        let intent = self
            .store
            .get(&intent_key)
            .map_err(|e| e.to_string())?
            .ok_or("missing intent")?;
        let recorded: crate::task::Task =
            serde_json::from_slice(&intent.value).map_err(|e| e.to_string())?;
        if recorded != self.task {
            return Err("stale attempt".into());
        }
        let mut key = intent_key.clone();
        key.extend_from_slice(b"/result");
        self.signals.batch.checks.push(Check {
            key: intent_key,
            expected: Expected::Version(intent.version),
        });
        self.signals.batch.checks.push(Check {
            key: key.clone(),
            expected: Expected::Missing,
        });
        self.signals.batch.mutations.push(Mutation::Put {
            key,
            value: serde_json::to_vec(result).map_err(|e| e.to_string())?,
        });
        Ok(())
    }
    /// Discard a handler's staged work without performing further storage I/O.
    /// A panicked invocation must still be able to publish its failure even
    /// when the handler left a conflict or staged input in this context.
    pub(crate) fn discard_staged(self) -> Self {
        Self {
            store: self.store,
            input_validator: self.input_validator,
            task: self.task,
            revision: self.revision,
            acquired: HashSet::new(),
            entries: None,
            entry_initial_len: 0,
            runtime_cut: None,
            runtime_config: None,
            task_input: None,
            task_input_conflict: None,
            task_input_storage: None,
            signals: StagedSignals::new(),
            child_reads: HashMap::new(),
            keyed_children: Vec::new(),
            keyed_child_handlers: HashMap::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
        }
    }

    /// Detach only from the actual store owner and the handler's reservation.
    /// No writes are committed and preparation errors remain deferred.
    #[allow(dead_code)]
    pub(crate) fn detach(
        self,
        owner: std::sync::Arc<durable_store::owner::OwnedStore>,
        reservation: &crate::task_store::StoredTask,
    ) -> Result<DetachedTaskDocuments, Error> {
        if !self.belongs_to(owner.store(), &reservation.task)
            || reservation.task.state() != &crate::task::TaskState::Running
            || reservation.task.abort_requested()
        {
            return Err(Error::InvalidBatch);
        }
        let Self {
            store: _,
            input_validator: _,
            revision,
            reads,
            writes,
            acquired,
            task: _,
            entries,
            entry_initial_len,
            runtime_cut,
            runtime_config,
            task_input,
            task_input_conflict,
            task_input_storage,
            signals,
            child_reads,
            keyed_children,
            keyed_child_handlers,
        } = self;
        Ok(DetachedTaskDocuments {
            owner,
            reservation: reservation.clone(),
            revision,
            reads,
            writes,
            acquired,
            entries,
            entry_initial_len,
            runtime_cut,
            runtime_config,
            task_input,
            task_input_conflict,
            task_input_storage,
            signals,
            child_reads,
            keyed_children,
            keyed_child_handlers,
        })
    }
    pub(crate) fn belongs_to(&self, store: &Store, task: &crate::task::Task) -> bool {
        std::ptr::eq(self.store, store) && &self.task == task
    }

    pub(crate) fn new(store: &'a Store, task: &crate::task::Task) -> Result<Self, Error> {
        Ok(Self {
            store,
            input_validator: None,
            task: task.clone(),
            acquired: HashSet::new(),
            entries: None,
            entry_initial_len: 0,
            runtime_cut: None,
            runtime_config: None,
            task_input: None,
            task_input_conflict: None,
            task_input_storage: None,
            signals: StagedSignals::new(),
            child_reads: HashMap::new(),
            keyed_children: Vec::new(),
            keyed_child_handlers: HashMap::new(),
            revision: store.document_snapshot()?,
            reads: HashMap::new(),
            writes: HashMap::new(),
        })
    }
    pub(crate) fn set_input_validator(&mut self, validator: Option<&'a crate::runtime::Harness>) {
        self.input_validator = validator;
    }
    /// Stage one input for a different conversation explicitly enrolled by the
    /// host in the same session as this task's owning conversation. This is the
    /// admissions in the step must target the same conversation; UUIDs alone
    /// convey no authority.
    pub fn submit_input_to_session_conversation(
        &mut self,
        target: uuid::Uuid,
        request_id: String,
        content: String,
        options: crate::state::AdmissionOptions,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_session_submission(
            target,
            Some(request_id),
            TaskSubmissionDraft::Input { content, options },
            notify,
        )
    }

    /// Stage one keyed continue for an explicitly same-session conversation.
    pub fn submit_continue_to_session_conversation(
        &mut self,
        target: uuid::Uuid,
        request_id: String,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_session_submission(
            target,
            Some(request_id),
            TaskSubmissionDraft::Continue,
            notify,
        )
    }

    /// Stage one keyed display-only write for an explicitly same-session conversation.
    pub fn submit_write_to_session_conversation(
        &mut self,
        target: uuid::Uuid,
        request_id: String,
        kind: String,
        data: Option<Value>,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_session_submission(
            target,
            Some(request_id),
            TaskSubmissionDraft::Write { kind, data },
            notify,
        )
    }

    /// Stage a distinct input without a request ID for a same-session target.
    pub fn submit_input_to_session_conversation_without_request_id(
        &mut self,
        target: uuid::Uuid,
        content: String,
        options: crate::state::AdmissionOptions,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_session_submission(
            target,
            None,
            TaskSubmissionDraft::Input { content, options },
            notify,
        )
    }

    /// Stage a distinct continue without a request ID for a same-session target.
    pub fn submit_continue_to_session_conversation_without_request_id(
        &mut self,
        target: uuid::Uuid,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_session_submission(target, None, TaskSubmissionDraft::Continue, notify)
    }

    /// Stage a distinct display write without a request ID for a same-session target.
    pub fn submit_write_to_session_conversation_without_request_id(
        &mut self,
        target: uuid::Uuid,
        kind: String,
        data: Option<Value>,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_session_submission(
            target,
            None,
            TaskSubmissionDraft::Write { kind, data },
            notify,
        )
    }

    fn stage_session_submission(
        &mut self,
        target: uuid::Uuid,
        request_id: Option<String>,
        draft: TaskSubmissionDraft,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        if notify
            .as_ref()
            .is_some_and(|item| item.task_id != self.task.id())
        {
            return Err(Error::InvalidSessionAdmission(
                "cross-conversation notification must target the submitting task".into(),
            ));
        }
        self.stage_submission(Some(target), request_id, draft, notify, || {})
    }

    /// Stage a text input in the task's owning conversation. The request ID
    /// is required; all effects remain provisional until the task step commits.
    pub fn submit_input(&mut self, request_id: String, content: String) -> Result<(), Error> {
        self.submit_input_with_options(
            request_id,
            content,
            crate::state::AdmissionOptions::default(),
            None,
        )
    }

    /// Stage an input with busy/idle policy and an optional settlement destination.
    pub fn submit_input_with_options(
        &mut self,
        request_id: String,
        content: String,
        options: crate::state::AdmissionOptions,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_submission(
            None,
            Some(request_id),
            TaskSubmissionDraft::Input { content, options },
            notify,
            || {},
        )
    }

    /// Stage a continuation of the owning conversation in this task step.
    /// A duplicate request retains its original submission and notification.
    pub fn submit_continue(
        &mut self,
        request_id: String,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_submission(
            None,
            Some(request_id),
            TaskSubmissionDraft::Continue,
            notify,
            || {},
        )
    }

    /// Stage an input without a deduplication request ID.
    pub fn submit_input_without_request_id(
        &mut self,
        content: String,
        options: crate::state::AdmissionOptions,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_submission(
            None,
            None,
            TaskSubmissionDraft::Input { content, options },
            notify,
            || {},
        )
    }

    /// Stage a continuation without a deduplication request ID.
    pub fn submit_continue_without_request_id(
        &mut self,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_submission(None, None, TaskSubmissionDraft::Continue, notify, || {})
    }

    /// Stage a display write without a deduplication request ID.
    pub fn submit_write_without_request_id(
        &mut self,
        kind: String,
        data: Option<Value>,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_submission(
            None,
            None,
            TaskSubmissionDraft::Write { kind, data },
            notify,
            || {},
        )
    }

    /// Stage a display-only write, including its optional settlement notification.
    pub fn submit_write(
        &mut self,
        request_id: String,
        kind: String,
        data: Option<Value>,
        notify: Option<crate::runtime::SubmissionNotify>,
    ) -> Result<(), Error> {
        self.stage_submission(
            None,
            Some(request_id),
            TaskSubmissionDraft::Write { kind, data },
            notify,
            || {},
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_input_with_snapshot_hook(
        &mut self,
        request_id: String,
        content: String,
        hook: impl FnOnce(),
    ) -> Result<(), Error> {
        self.stage_submission(
            None,
            Some(request_id),
            TaskSubmissionDraft::Input {
                content,
                options: crate::state::AdmissionOptions::default(),
            },
            None,
            hook,
        )
    }

    fn remember_task_input_error(&mut self, error: Error) -> Error {
        match error {
            Error::Store(durable_store::Error::Conflict { key })
            | Error::Signal(crate::task_store::Error::Store(durable_store::Error::Conflict {
                key,
            })) => {
                self.task_input_conflict = Some(key.clone());
                Error::Store(durable_store::Error::Conflict { key })
            }
            Error::Store(error) | Error::Signal(crate::task_store::Error::Store(error)) => {
                let error = std::sync::Arc::new(error);
                self.task_input_storage = Some(error.clone());
                Error::TaskInputStore(error)
            }
            error => error,
        }
    }

    fn stage_submission(
        &mut self,
        target: Option<uuid::Uuid>,
        request_id: Option<String>,
        draft: TaskSubmissionDraft,
        notify: Option<crate::runtime::SubmissionNotify>,
        after_snapshot: impl FnOnce(),
    ) -> Result<(), Error> {
        if !self.keyed_children.is_empty()
            || self.entries.as_ref().is_some_and(|entry| entry.4)
            || !self.signals.batch.mutations.is_empty()
        {
            return Err(Error::ReadAfterWrite);
        }
        if request_id.as_ref().is_some_and(String::is_empty) {
            return Err(Error::InvalidSessionAdmission("request ID required".into()));
        }
        if let Some(intent) = &self.task_input {
            if target.is_some() != intent.cross_conversation
                || target.is_some_and(|target| target != intent.conversation)
            {
                return Err(Error::InvalidSessionAdmission(
                    "all admissions in a step must use the same conversation".into(),
                ));
            }
            if intent.admissions.len() >= 16 {
                return Err(Error::InvalidSessionAdmission(
                    "at most 16 task admissions per step".into(),
                ));
            }
            let mut admissions = intent.admissions.clone();
            let prior = crate::runtime::validate_task_submissions(
                self.store,
                &intent.snapshot,
                &mut admissions,
                intent.cross_conversation,
            );
            if let Err(error) = prior {
                let error = match error {
                    Error::InvalidSessionAdmission(_) => {
                        Error::Store(durable_store::Error::Conflict {
                            key: b"runtime/conversation/".to_vec(),
                        })
                    }
                    error => error,
                };
                return Err(self.remember_task_input_error(error));
            }
            admissions.push(TaskAdmission {
                request_id,
                draft,
                notify,
                notification_binding: None,
            });
            let snapshot = intent.snapshot.clone();
            let conversation = intent.conversation;
            let result = crate::runtime::validate_task_submissions(
                self.store,
                &snapshot,
                &mut admissions,
                intent.cross_conversation,
            );
            if let Err(mut error) = result {
                let mut prior = admissions[..admissions.len() - 1].to_vec();
                match crate::runtime::validate_task_submissions(
                    self.store,
                    &snapshot,
                    &mut prior,
                    intent.cross_conversation,
                ) {
                    Err(Error::InvalidSessionAdmission(_)) => {
                        error = Error::Store(durable_store::Error::Conflict {
                            key: b"runtime/conversation/".to_vec(),
                        });
                    }
                    Err(other) => error = other,
                    Ok(()) => {}
                }
                return Err(self.remember_task_input_error(error));
            }
            if admissions.iter().any(|item| {
                matches!(
                    item.draft,
                    TaskSubmissionDraft::Input { .. } | TaskSubmissionDraft::Continue
                )
            }) {
                let validation = self
                    .input_validator
                    .ok_or_else(|| {
                        Error::InvalidSessionAdmission(
                            "task submission requires an attached harness".into(),
                        )
                    })?
                    .validate_task_input_context(conversation, &snapshot);
                validation.map_err(|error| self.remember_task_input_error(error))?;
            }
            self.task_input.as_mut().unwrap().admissions = admissions;
            return Ok(());
        }
        let mut owner = self.task.owner().clone();
        let mut seen = HashSet::from([self.task.id()]);
        let mut ancestry_checks = Vec::new();
        let conversation = loop {
            match owner {
                crate::task::TaskOwner::Conversation(id) => break id,
                crate::task::TaskOwner::Task(id) => {
                    if !seen.insert(id) {
                        return Err(Error::InvalidSessionAdmission(
                            "cyclic task ancestry".into(),
                        ));
                    }
                    let key = crate::task_store::key(id);
                    let record = self
                        .store
                        .get(&key)
                        .map_err(|error| self.remember_task_input_error(Error::Store(error)))?
                        .ok_or_else(|| {
                            Error::InvalidSessionAdmission("task ancestor missing".into())
                        })?;
                    let ancestor: crate::task::Task = serde_json::from_slice(&record.value)?;
                    if ancestor.id() != id || ancestor.is_terminal() || ancestor.abort_requested() {
                        return Err(Error::InvalidSessionAdmission(
                            "task ancestor is inactive".into(),
                        ));
                    }
                    ancestry_checks.push(Check {
                        key,
                        expected: Expected::Version(record.version),
                    });
                    owner = ancestor.owner().clone();
                }
            }
        };
        let cross_conversation = target.is_some();
        let conversation = if let Some(target) = target {
            if target == conversation {
                return Err(Error::InvalidSessionAdmission(
                    "target must differ from the owning conversation".into(),
                ));
            }
            let proof = crate::session_membership::task_pair(self.store, conversation, target)
                .map_err(|error| self.remember_task_input_error(error))?;
            ancestry_checks.extend(proof);
            let target_checks =
                crate::ownership_edge::target_foreground_admission_checks(self.store, target)
                    .map_err(|error| {
                        let error = match error {
                            crate::ownership_edge::Error::Store(error)
                            | crate::ownership_edge::Error::Task(
                                crate::task_store::Error::Store(error),
                            ) => Error::Store(error),
                            _ => Error::InvalidSessionAdmission(
                                "target foreground ownership is unavailable".into(),
                            ),
                        };
                        self.remember_task_input_error(error)
                    })?;
            for check in target_checks {
                if let Some(existing) = ancestry_checks
                    .iter_mut()
                    .find(|item| item.key == check.key)
                {
                    existing.expected = merge_task_expected(&existing.expected, &check.expected);
                } else {
                    ancestry_checks.push(check);
                }
            }
            target
        } else {
            conversation
        };
        let key = crate::runtime::Harness::key(conversation);
        let record = self
            .store
            .get(&key)
            .map_err(|error| self.remember_task_input_error(Error::Store(error)))?
            .ok_or_else(|| Error::InvalidSessionAdmission("conversation missing".into()))?;
        after_snapshot();
        let validation = (|| {
            if cross_conversation {
                crate::session_membership::validate_conversation(conversation, &record.value)
                    .map_err(|_| {
                        Error::InvalidSessionAdmission(
                            "target conversation lacks valid authorization evidence".into(),
                        )
                    })?;
            }
            let state: crate::runtime::Conversation = serde_json::from_slice(&record.value)?;
            if state.id != conversation || state.abort_requested {
                return Err(Error::InvalidSessionAdmission(
                    "conversation is aborting or invalid".into(),
                ));
            }
            let mut admissions = vec![TaskAdmission {
                request_id,
                draft,
                notify,
                notification_binding: None,
            }];
            crate::runtime::validate_task_submissions(
                self.store,
                &record.value,
                &mut admissions,
                cross_conversation,
            )?;
            if matches!(
                admissions[0].draft,
                TaskSubmissionDraft::Input { .. } | TaskSubmissionDraft::Continue
            ) {
                self.input_validator
                    .ok_or_else(|| {
                        Error::InvalidSessionAdmission(
                            "task submission requires an attached harness".into(),
                        )
                    })?
                    .validate_task_input_context(conversation, &record.value)?;
            } else if self.input_validator.is_none() {
                return Err(Error::InvalidSessionAdmission(
                    "task submission requires an attached harness".into(),
                ));
            }
            Ok(admissions)
        })();
        let admissions = validation.map_err(|error| self.remember_task_input_error(error))?;
        self.task_input = Some(TaskInputIntent {
            conversation,
            version: record.version,
            ancestry_checks,
            cross_conversation,
            snapshot: record.value,
            admissions,
        });
        Ok(())
    }

    /// Stage a keyed direct child for this invocation. Nothing is persisted until
    /// the scheduler commits the handler's transition and documents. The returned
    /// ID is stable for an existing matching key; candidate IDs need not match.
    /// A key may be staged once per invocation. Handler errors discard the stage.
    pub fn stage_keyed_child(
        &mut self,
        key: &str,
        tool: &str,
        arguments: &Value,
        child: crate::task::Task,
    ) -> Result<uuid::Uuid, String> {
        self.stage_keyed_child_inner(key, tool, arguments, child, || {})
    }

    #[cfg(test)]
    pub(crate) fn stage_keyed_child_after_load<F: FnMut()>(
        &mut self,
        key: &str,
        tool: &str,
        arguments: &Value,
        child: crate::task::Task,
        after_load: F,
    ) -> Result<uuid::Uuid, String> {
        self.stage_keyed_child_inner(key, tool, arguments, child, after_load)
    }

    fn stage_keyed_child_inner<F: FnMut()>(
        &mut self,
        key: &str,
        tool: &str,
        arguments: &Value,
        child: crate::task::Task,
        mut after_load: F,
    ) -> Result<uuid::Uuid, String> {
        let tasks = crate::task_store::TaskStore::new(self.store);
        let prepared = loop {
            let parent = tasks
                .load(self.task.id())
                .map_err(|e| e.to_string())?
                .ok_or("parent missing")?;
            if parent.task != self.task
                || parent.task.abort_requested()
                || parent.task.state() != &crate::task::TaskState::Running
            {
                return Err("parent changed during invocation".into());
            }
            after_load();
            match crate::keyed_child::prepare(self.store, &parent, key, tool, arguments, &child) {
                Ok(prepared) => {
                    let parent_key = crate::task_store::key(parent.task.id());
                    if prepared
                        .checks
                        .iter()
                        .filter(|check| check.key == parent_key)
                        .all(|check| check.expected == Expected::Version(parent.version))
                    {
                        break prepared;
                    }
                    // Preparation may have read the owner a second time while
                    // assembling child-creation checks. Rebuild the whole
                    // prepared batch only after verifying the same invocation.
                    let current = tasks
                        .load(self.task.id())
                        .map_err(|e| e.to_string())?
                        .ok_or("parent missing")?;
                    if current.version == parent.version
                        || current.task != self.task
                        || current.task.abort_requested()
                        || current.task.state() != &crate::task::TaskState::Running
                    {
                        return Err("parent changed during invocation".into());
                    }
                }
                Err(crate::keyed_child::Error::ParentClosed) => {
                    let current = tasks
                        .load(self.task.id())
                        .map_err(|e| e.to_string())?
                        .ok_or("parent missing")?;
                    if current.version == parent.version
                        || current.task != self.task
                        || current.task.abort_requested()
                        || current.task.state() != &crate::task::TaskState::Running
                    {
                        return Err("parent changed during invocation".into());
                    }
                }
                Err(error) => return Err(error.to_string()),
            }
        };
        if let Some((_, _, existing)) = self
            .keyed_children
            .iter()
            .find(|(existing, _, _)| existing == key)
        {
            return if existing.same_identity(&prepared) {
                Ok(existing.child)
            } else {
                Err("key already belongs to a different child identity".into())
            };
        }
        let id = prepared.child;
        self.keyed_children.push((key.into(), child, prepared));
        Ok(id)
    }

    pub(crate) fn stage_task_tool_child(
        &mut self,
        key: &str,
        tool: &str,
        arguments: &Value,
        child: crate::task::Task,
        handler: std::sync::Arc<dyn crate::task_scheduler::Handler>,
    ) -> Result<uuid::Uuid, String> {
        if self
            .keyed_children
            .iter()
            .any(|(existing, _, _)| existing == key)
        {
            match self.keyed_child_handlers.get(key) {
                Some(existing) if std::sync::Arc::ptr_eq(existing, &handler) => {}
                _ => return Err("key already staged with a different handler".into()),
            }
        }
        let id = self.stage_keyed_child(key, tool, arguments, child)?;
        self.keyed_child_handlers.insert(key.to_owned(), handler);
        Ok(id)
    }

    pub(crate) fn require_standalone_for_sealed_child(&self) -> Result<(), String> {
        if self.input_validator.is_some() {
            return Err("sealed tools require a standalone scheduler".into());
        }
        Ok(())
    }

    /// Add the sealed Tool identity to the already staged keyed child. The
    /// scheduler commits these checks and writes with the parent transition.
    pub(crate) fn bind_sealed_child_identity(
        &mut self,
        key: &str,
        identity: &crate::async_task_tool::ToolIdentity,
    ) -> Result<(), String> {
        if self.input_validator.is_some() {
            return Err("sealed tools require a standalone scheduler".into());
        }
        let (_, _, prepared) = self
            .keyed_children
            .iter_mut()
            .find(|(existing, _, _)| existing == key)
            .ok_or("sealed child not staged")?;
        let batch = crate::async_task_protocol::prepare_child_identity(
            self.store,
            prepared.child,
            identity,
        )
        .map_err(|e| e.to_string())?;
        // Idempotent repeated calls within the same handler must not duplicate
        // checks or write a second identity mutation.
        for check in &batch.checks {
            if let Some(existing) = prepared
                .checks
                .iter()
                .find(|candidate| candidate.key == check.key)
                && existing.expected != check.expected
            {
                return Err("sealed identity check changed".into());
            }
        }
        for mutation in &batch.mutations {
            if let durable_store::Mutation::Put { key, value } = mutation
                && let Some(durable_store::Mutation::Put { value: existing, .. }) = prepared.mutations.iter().find(|candidate| matches!(candidate, durable_store::Mutation::Put { key: existing_key, .. } if existing_key == key))
                && existing != value
            {
                return Err("sealed identity changed".into());
            }
        }
        for check in batch.checks {
            if !prepared
                .checks
                .iter()
                .any(|existing| existing.key == check.key)
            {
                prepared.checks.push(check);
            }
        }
        for mutation in batch.mutations {
            let duplicate = match &mutation { durable_store::Mutation::Put { key, .. } => prepared.mutations.iter().any(|candidate| matches!(candidate, durable_store::Mutation::Put { key: existing, .. } if existing == key)), _ => false };
            if !duplicate {
                prepared.mutations.push(mutation);
            }
        }
        Ok(())
    }

    pub(crate) fn read_sealed_child_receipt(
        &mut self,
        id: uuid::Uuid,
    ) -> Result<Option<crate::async_task_protocol::ReceiptRead>, String> {
        if self.input_validator.is_some() {
            return Err("sealed tools require a standalone scheduler".into());
        }
        self.get_owned_child(id)?;
        let (outcome, checks) = crate::async_task_protocol::prepare_receipt_read(self.store, id)
            .map_err(|e| e.to_string())?;
        for check in checks {
            if let Some(existing) = self
                .signals
                .batch
                .checks
                .iter()
                .find(|existing| existing.key == check.key)
            {
                if existing.expected != check.expected {
                    return Err("sealed receipt changed during invocation".into());
                }
            } else {
                self.signals.batch.checks.push(check);
            }
        }
        Ok(outcome)
    }

    /// Read a direct owned child and fence the handler's whole transition on
    /// that version. Repeated reads use the same snapshot. This does not read
    /// children staged for creation in the current uncommitted invocation.
    pub fn get_owned_child(&mut self, id: uuid::Uuid) -> Result<crate::task::Task, String> {
        if !self.child_reads.contains_key(&id) {
            let child = crate::task_store::TaskStore::new(self.store)
                .load(id)
                .map_err(|e| e.to_string())?;
            let expected = child
                .as_ref()
                .map_or(Expected::Missing, |child| Expected::Version(child.version));
            let key = crate::task_store::key(id);
            if let Some(check) = self.signals.batch.checks.iter_mut().find(|c| c.key == key) {
                let compatible = match (&check.expected, &expected) {
                    (Expected::Version(a), Expected::Version(b))
                    | (Expected::VersionBefore(a, _), Expected::Version(b)) => a == b,
                    (Expected::Missing, Expected::Missing) => true,
                    _ => false,
                };
                if !compatible {
                    check.expected = Expected::VersionBefore(0, std::time::Instant::now());
                }
            } else {
                self.signals.batch.checks.push(Check { key, expected });
            }
            self.child_reads.insert(id, child);
        }
        let child = self.child_reads[&id]
            .as_ref()
            .ok_or("owned child missing")?;
        if child.task.owner() != &crate::task::TaskOwner::Task(self.task.id()) {
            return Err("task is not a direct child".into());
        }
        Ok(child.task.clone())
    }

    /// Read a task-owned document, acquiring an absent address for this task.
    /// Existing owned addresses may be reacquired by the same task; an
    /// unowned existing document is never implicitly taken over.
    pub fn acquire_owned(&mut self, address: &Address) -> Result<Option<Document>, Error> {
        if self.acquired.contains(address) {
            return self.get(address);
        }
        let doc = self.get(address)?;
        let key = address.storage_key()?;
        match self.store.task_document_owner(&key)? {
            Some(owner) if owner == self.task.id() => {}
            Some(_) => return Err(Error::InvalidBatch),
            None if doc.is_none() => {
                self.acquired.insert(address.clone());
            }
            None => return Err(Error::InvalidBatch),
        }
        Ok(doc)
    }
    pub fn get(&mut self, address: &Address) -> Result<Option<Document>, Error> {
        if let Some(value) = self.writes.get(address) {
            return Ok(value.as_ref().map(|value| Document {
                value: value.clone(),
                version: self.reads[address].as_ref().map_or(0, |doc| doc.version),
            }));
        }
        if let Some(value) = self.reads.get(address) {
            return Ok(value.clone());
        }
        let value = Documents::new(self.store).get_at(address, self.revision)?;
        self.reads.insert(address.clone(), value.clone());
        Ok(value)
    }
    pub fn put(&mut self, address: &Address, value: Map<String, Value>) -> Result<(), Error> {
        self.write(address, Some(value))
    }
    pub fn delete(&mut self, address: &Address) -> Result<(), Error> {
        self.write(address, None)
    }
    fn write(&mut self, address: &Address, value: Option<Map<String, Value>>) -> Result<(), Error> {
        if !self.reads.contains_key(address) {
            return Err(Error::UnreadOrWritten);
        }
        self.writes.insert(address.clone(), value);
        Ok(())
    }
    /// Stage typed deliveries alongside document and application-entry writes.
    /// The outcome is provisional until the task checkpoint commits. A conflict
    /// replays only registered replay-safe handlers. Supply a fresh clock value
    /// on replay; elapsed monotonic time is checked at the storage commit boundary.
    /// Clock jumps after staging are not observed. Repeating a delivered or
    /// duplicate key validates the new payload and returns duplicate without
    /// staging another receipt; an invalid value may be retried. Repeated
    /// closed or missing outcomes retain their original fenced observation.
    /// Self-delivery commits with the handler's task transition.
    pub fn signal_at(
        &mut self,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
    ) -> Result<crate::task_store::SignalResult, Error> {
        if token.name() == crate::runtime::submission_settled_signal().name()
            && self.task_input.as_ref().is_some_and(|intent| {
                intent.admissions.iter().any(|item| {
                    item.notification_binding.is_some()
                        && item
                            .notify
                            .as_ref()
                            .is_some_and(|notify| notify.task_id == task_id && notify.key == key)
                })
            })
        {
            return Err(Error::SignalComposition);
        }
        self.signals.signal_at(
            self.store,
            task_id,
            token,
            key,
            value,
            now_millis,
            Some(&self.task),
        )
    }

    /// Stage an immutable application record with this task as its author.
    /// The root conversation and every task ancestor must exist and not be aborting. The returned ID is
    /// provisional until the step commits and can change on replay.
    pub fn append_entry(&mut self, kind: &str, data: Value) -> Result<uuid::Uuid, Error> {
        self.append_entry_inner(None, kind, data)
    }

    /// Stage one unchanged runtime checkpoint after the entries already staged.
    /// At least one entry must precede it. Further entries may be appended; all
    /// effects commit with the task step. The runtime version and observer head
    /// are fenced, so a concurrent runtime change causes the whole step to retry.
    pub fn checkpoint_runtime(&mut self) -> Result<(), Error> {
        if self.task_input.is_some() {
            return Err(Error::InvalidApplicationEntry);
        }
        let Some((_, _, entries, _, true)) = &self.entries else {
            return Err(Error::InvalidApplicationEntry);
        };
        if self.runtime_cut.is_some() {
            return Err(Error::InvalidApplicationEntry);
        }
        self.runtime_cut = Some(entries.len());
        Ok(())
    }

    /// Stage an ordered checkpoint that changes configuration for an idle root
    /// conversation. The new configuration is the end-of-commit agent state,
    /// including for entries before the checkpoint. Model context is unchanged.
    pub fn configure_runtime_at_checkpoint(
        &mut self,
        config: crate::runtime::Config,
    ) -> Result<(), Error> {
        if config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::InvalidApplicationEntry);
        }
        self.checkpoint_runtime()?;
        self.runtime_config = Some(config);
        Ok(())
    }

    /// Append only if the application log's last entry matches `expected`.
    /// `None` expects an empty log. Earlier entries staged in this invocation
    /// are included. Even a mismatch retains a read dependency on the observed
    /// log and subsequent checks use that observation. The log version is fenced
    /// at commit; IDs are provisional and may change on replay.
    pub fn append_entry_if_head(
        &mut self,
        expected: Option<uuid::Uuid>,
        kind: &str,
        data: Value,
    ) -> Result<uuid::Uuid, Error> {
        self.append_entry_inner(Some(expected), kind, data)
    }

    fn append_entry_inner(
        &mut self,
        expected: Option<Option<uuid::Uuid>>,
        kind: &str,
        data: Value,
    ) -> Result<uuid::Uuid, Error> {
        if kind.is_empty() || kind.starts_with("pi.") {
            return Err(Error::InvalidApplicationEntry);
        }
        if self.entries.is_none() {
            let mut owner = self.task.owner().clone();
            let mut seen = HashSet::from([self.task.id()]);
            let mut checks = Vec::new();
            let conversation = loop {
                match owner {
                    crate::task::TaskOwner::Conversation(id) => break id,
                    crate::task::TaskOwner::Task(id) => {
                        if !seen.insert(id) {
                            return Err(Error::InvalidApplicationEntry);
                        }
                        let key = crate::task_store::key(id);
                        let record = self
                            .store
                            .get(&key)?
                            .ok_or(Error::InvalidApplicationEntry)?;
                        let ancestor: crate::task::Task = serde_json::from_slice(&record.value)?;
                        if ancestor.id() != id
                            || ancestor.is_terminal()
                            || ancestor.abort_requested()
                        {
                            return Err(Error::InvalidApplicationEntry);
                        }
                        checks.push(Check {
                            key,
                            expected: Expected::Version(record.version),
                        });
                        owner = ancestor.owner().clone();
                    }
                }
            };
            let key = format!("runtime/conversation/{conversation}").into_bytes();
            let root = self
                .store
                .get(&key)?
                .ok_or(Error::InvalidApplicationEntry)?;
            let state: crate::runtime::Conversation = serde_json::from_slice(&root.value)?;
            if state.id != conversation || state.abort_requested {
                return Err(Error::InvalidApplicationEntry);
            }
            checks.push(Check {
                key,
                expected: Expected::Version(root.version),
            });
            let record = self
                .store
                .get(&crate::application_entry::key(conversation))?;
            let version = record.as_ref().map(|r| r.version);
            let entries = record
                .map(|r| serde_json::from_slice(&r.value))
                .transpose()?
                .unwrap_or_default();
            let entries: Vec<crate::application_entry::ApplicationEntry> = entries;
            self.entry_initial_len = entries.len();
            self.entries = Some((conversation, version, entries, checks, false));
        }
        if expected.is_some_and(|head| {
            self.entries
                .as_ref()
                .unwrap()
                .2
                .last()
                .map(|entry| entry.id)
                != head
        }) {
            return Err(Error::ApplicationEntryHeadMismatch);
        }
        let id = uuid::Uuid::new_v4();
        let conversation = self.entries.as_ref().unwrap().0;
        self.entries.as_mut().unwrap().4 = true;
        self.entries
            .as_mut()
            .unwrap()
            .2
            .push(crate::application_entry::ApplicationEntry {
                id,
                conversation_id: conversation,
                author_task_id: self.task.id(),
                kind: kind.into(),
                data,
            });
        Ok(id)
    }
    pub(crate) fn stage_acquisitions(&self, transaction: &mut Transaction) -> Result<(), Error> {
        for address in &self.acquired {
            transaction.acquire_task_document(address, self.task.id())?;
        }
        Ok(())
    }
    pub(crate) fn rebase_self_signal_check(&mut self, version: u64) {
        self.signals.rebase_self_check(self.task.id(), version);
    }
    pub(crate) fn into_batch(self) -> Result<OwnedBatch, Error> {
        // Only the scheduler can validate and commit staged keyed children.
        if !self.keyed_children.is_empty() {
            return Err(Error::InvalidBatch);
        }
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for address in self.reads.keys() {
            let key = address.storage_key()?;
            batch.checks.push(Check {
                key: key.clone(),
                expected: Expected::DocumentUnchangedSince(self.revision),
            });
            if let Some(value) = self.writes.get(address) {
                batch.mutations.push(match value {
                    Some(value) => Mutation::Put {
                        key,
                        value: serde_json::to_vec(value)?,
                    },
                    None => Mutation::Delete { key },
                });
            }
        }
        batch.checks.extend(self.signals.batch.checks);
        batch.mutations.extend(self.signals.batch.mutations);
        if let Some((conversation, version, entries, checks, dirty)) = self.entries {
            if let Some(index) = self.runtime_cut {
                let cut = crate::runtime::prepare_ordered_task_checkpoint(
                    self.store,
                    conversation,
                    &checks,
                    self.entry_initial_len,
                    index,
                    &entries,
                    self.runtime_config.as_ref(),
                )?;
                batch.checks.extend(cut.checks);
                batch.mutations.extend(cut.mutations);
            }
            batch.checks.extend(checks);
            let key = crate::application_entry::key(conversation);
            batch.checks.push(Check {
                key: key.clone(),
                expected: version.map_or(Expected::Missing, Expected::Version),
            });
            if dirty {
                batch.mutations.push(Mutation::Put {
                    key,
                    value: serde_json::to_vec(&entries)?,
                });
            }
        }
        // Entry ancestry and signal delivery can observe the same task at
        // different instants. Preserve both fences as one checked condition;
        // disagreement forces a CAS conflict and the ordinary retry policy.
        let mut checked: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut normalized: Vec<Check> = Vec::with_capacity(batch.checks.len());
        for check in batch.checks {
            if (check.key.starts_with(crate::task_store::PREFIX)
                || check.key.starts_with(b"durable/signal-revision/"))
                && let Some(&index) = checked.get(&check.key)
            {
                let existing = &mut normalized[index].expected;
                *existing = match (&*existing, &check.expected) {
                    (Expected::Version(a), Expected::VersionBefore(b, t))
                    | (Expected::VersionBefore(b, t), Expected::Version(a))
                        if a == b =>
                    {
                        Expected::VersionBefore(*a, *t)
                    }
                    (Expected::VersionBefore(a, x), Expected::VersionBefore(b, y)) if a == b => {
                        Expected::VersionBefore(*a, (*x).min(*y))
                    }
                    (a, b) if a == b => a.clone(),
                    _ => Expected::VersionBefore(0, std::time::Instant::now()),
                };
            } else {
                checked.insert(check.key.clone(), normalized.len());
                normalized.push(check);
            }
        }
        batch.checks = normalized;
        Ok(batch)
    }
}
impl<'a> AdmissionDocuments<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self {
            store,
            reads: HashMap::new(),
            writes: HashMap::new(),
            tasks: Vec::new(),
        }
    }
    pub fn get(&mut self, address: &Address) -> Result<Option<Document>, Error> {
        if !self.writes.is_empty() {
            return Err(Error::ReadAfterWrite);
        }
        if let Some(value) = self.reads.get(address) {
            return Ok(value.clone());
        }
        let value = Documents::new(self.store).get(address)?;
        self.reads.insert(address.clone(), value.clone());
        Ok(value)
    }
    pub fn put(&mut self, address: &Address, value: Map<String, Value>) -> Result<(), Error> {
        self.write(address, Some(value))
    }
    pub fn delete(&mut self, address: &Address) -> Result<(), Error> {
        self.write(address, None)
    }
    /// Stage a conversation-owned registered task for the admission commit.
    pub fn create_task(&mut self, task: crate::task::Task) -> Result<(), Error> {
        if self.tasks.iter().any(|existing| existing.id() == task.id()) {
            return Err(Error::DuplicateTask);
        }
        self.tasks.push(task);
        Ok(())
    }
    pub(crate) fn take_tasks(&mut self) -> Vec<crate::task::Task> {
        std::mem::take(&mut self.tasks)
    }
    fn write(&mut self, address: &Address, value: Option<Map<String, Value>>) -> Result<(), Error> {
        if !self.reads.contains_key(address) || self.writes.contains_key(address) {
            return Err(Error::UnreadOrWritten);
        }
        self.writes.insert(address.clone(), value);
        Ok(())
    }
    pub(crate) fn into_batch(self) -> Result<OwnedBatch, Error> {
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for (address, document) in self.reads {
            let key = address.storage_key()?;
            batch.checks.push(Check {
                key: key.clone(),
                expected: document.map_or(Expected::Missing, |d| Expected::Version(d.version)),
            });
            if let Some(value) = self.writes.get(&address) {
                batch.mutations.push(match value {
                    Some(value) => Mutation::Put {
                        key,
                        value: serde_json::to_vec(value)?,
                    },
                    None => Mutation::Delete { key },
                });
            }
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod nested_entry_tests {
    use super::*;
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn task_submission_rejects_missing_inactive_and_cyclic_ancestors() {
        let db = Store::memory().unwrap();
        let author = task(3, TaskOwner::Task(Uuid::from_u128(1)));
        let check = || {
            let mut docs = TaskDocuments::new(&db, &author).unwrap();
            assert!(matches!(
                docs.submit_write("request".into(), "app.test".into(), None, None),
                Err(Error::InvalidSessionAdmission(_))
            ));
        };
        check();
        let mut inactive = task(1, TaskOwner::Conversation(Uuid::from_u128(99)));
        inactive.request_abort().unwrap();
        let key = crate::task_store::key(inactive.id());
        db.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&inactive).unwrap(),
            }],
        )
        .unwrap();
        check();
        let mut terminal = task(1, TaskOwner::Conversation(Uuid::from_u128(99)));
        terminal.start().unwrap();
        terminal.succeed(json!(null)).unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&terminal).unwrap(),
            }],
        )
        .unwrap();
        check();
        let first = task(1, TaskOwner::Task(Uuid::from_u128(2)));
        let second = task(2, TaskOwner::Task(first.id()));
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key,
                    value: serde_json::to_vec(&first).unwrap(),
                },
                Mutation::Put {
                    key: crate::task_store::key(second.id()),
                    value: serde_json::to_vec(&second).unwrap(),
                },
            ],
        )
        .unwrap();
        check();
    }

    #[test]
    fn missing_aba_fences_compose_with_earliest_deadline() {
        use std::time::{Duration, Instant};
        let earlier = Instant::now() + Duration::from_secs(10);
        let later = earlier + Duration::from_secs(10);
        let strict = Expected::MissingUnchangedSinceBefore(4, earlier);
        let cases = [
            (
                Expected::MissingUnchangedSince(4),
                Expected::MissingBefore(earlier),
            ),
            (
                Expected::MissingBefore(earlier),
                Expected::MissingUnchangedSince(4),
            ),
            (
                Expected::MissingUnchangedSinceBefore(9, later),
                Expected::MissingUnchangedSinceBefore(4, earlier),
            ),
            (
                Expected::MissingUnchangedSinceBefore(4, earlier),
                Expected::Missing,
            ),
        ];
        for (a, b) in cases {
            assert_eq!(merge_task_expected(&a, &b), strict);
        }
        assert_eq!(
            merge_task_expected(
                &Expected::MissingUnchangedSince(4),
                &Expected::MissingUnchangedSince(9)
            ),
            Expected::MissingUnchangedSince(4)
        );
    }

    #[test]
    fn late_child_rebase_keeps_original_deadline_and_task_version_fence() {
        use std::time::{Duration, Instant};
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let mut running = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        running.start_attempt(Uuid::new_v4()).unwrap();
        store.save(&[Save::Create(&running)]).unwrap();
        let before = store.load(running.id()).unwrap().unwrap();
        let mut candidate = before.clone();
        candidate.task.succeed(json!("done")).unwrap();
        candidate
            .task
            .record_completed_attempt(&before.task)
            .unwrap();
        let mut held = candidate.task.clone();
        held.hold_prepared_success().unwrap();
        let mut graph = store.prepare_save(&[Save::Update(&candidate)]).unwrap();
        let key = crate::task_store::key(running.id());
        let deadline = Instant::now() - Duration::from_secs(1);
        graph
            .checks
            .iter_mut()
            .find(|check| check.key == key)
            .unwrap()
            .expected = Expected::VersionBefore(before.version, deadline);
        let mut tx = Transaction::new();
        tx.append(graph).unwrap();
        store.update(&before, |_| Ok(())).unwrap();
        let after = store.load(running.id()).unwrap().unwrap();
        assert_eq!(before.task, after.task);
        tx.rebase_task_candidate(
            running.id(),
            before.version,
            after.version,
            &candidate.task,
            &held,
        )
        .unwrap();
        assert!(
            matches!(tx.checks.iter().find(|check| check.key == key).unwrap().expected, Expected::VersionBefore(version, when) if version == after.version && when == deadline)
        );
        assert!(matches!(
            tx.commit_task_graph(&db, db.put_version().unwrap()),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(
            store.load(running.id()).unwrap().unwrap().task.state(),
            &crate::task::TaskState::Running
        );
    }

    #[test]
    fn host_overlay_reads_existing_staged_signal_mutation() {
        use crate::task_store::{SignalResult, SignalToken};
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let target = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        ts.save(&[Save::Create(&target)]).unwrap();
        let token = SignalToken::new("answer", 1).unwrap();
        let (status, prepared) = ts
            .prepare_signal_at(target.id(), &token, "a", json!(1), || 0)
            .unwrap();
        assert_eq!(status, SignalResult::Delivered);
        let mut host = HostTransaction::new(&db);
        host.append_runtime_candidate(prepared).unwrap();
        assert_eq!(
            host.signal_at(target.id(), &token, "a", json!(2), 0)
                .unwrap(),
            SignalResult::Duplicate
        );
        assert_eq!(
            host.signal_at(target.id(), &token, "b", json!(3), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        host.commit().unwrap();
        assert_eq!(
            ts.read_typed_signal(target.id(), &token, "a").unwrap(),
            Some(json!(1))
        );
        assert_eq!(
            ts.read_typed_signal(target.id(), &token, "b").unwrap(),
            Some(json!(3))
        );
    }

    #[test]
    fn racing_child_observations_conflict_and_preserve_deadlines() {
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        let child = task(2, TaskOwner::Task(parent.id()));
        store
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        docs.get_owned_child(child.id()).unwrap();
        let old = store.load(child.id()).unwrap().unwrap();
        store.update(&old, |t| t.start()).unwrap();
        let new = store.load(child.id()).unwrap().unwrap();
        let mut document_batch = docs.into_batch().unwrap();
        let mut graph = OwnedBatch {
            checks: vec![Check {
                key: crate::task_store::key(child.id()),
                expected: Expected::Version(new.version),
            }],
            mutations: vec![],
        };
        reconcile_task_checks(&mut document_batch, &mut graph);
        let mut tx = Transaction::new();
        tx.append(document_batch).unwrap();
        tx.append(graph).unwrap();
        assert!(matches!(
            tx.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let key = crate::task_store::key(child.id());
        let mut document_batch = OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected: Expected::VersionBefore(new.version, deadline),
            }],
            mutations: vec![],
        };
        let mut graph = OwnedBatch {
            checks: vec![Check {
                key,
                expected: Expected::Version(new.version),
            }],
            mutations: vec![],
        };
        reconcile_task_checks(&mut document_batch, &mut graph);
        assert_eq!(
            graph.checks[0].expected,
            Expected::VersionBefore(new.version, deadline)
        );
        let mut tx = Transaction::new();
        tx.append(document_batch).unwrap();
        tx.append(graph).unwrap();
        tx.commit(&db).unwrap();
    }

    #[test]
    fn owned_child_read_is_cached_fenced_and_rejects_foreign_tasks() {
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        let child = task(2, TaskOwner::Task(parent.id()));
        let foreign = task(3, TaskOwner::Conversation(Uuid::from_u128(9)));
        store
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&foreign),
            ])
            .unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        assert!(docs.get_owned_child(foreign.id()).is_err());
        assert!(docs.get_owned_child(Uuid::from_u128(99)).is_err());
        let first = docs.get_owned_child(child.id()).unwrap();
        let address = Address {
            scope: "test".into(),
            name: "result".into(),
            key: "".into(),
        };
        docs.get(&address).unwrap();
        docs.put(
            &address,
            json!({"committed": true}).as_object().unwrap().clone(),
        )
        .unwrap();
        let stored = store.load(child.id()).unwrap().unwrap();
        store.update(&stored, |t| t.start()).unwrap();
        assert_eq!(docs.get_owned_child(child.id()).unwrap(), first);
        let mut transaction = Transaction::new();
        transaction.append(docs.into_batch().unwrap()).unwrap();
        assert!(matches!(
            transaction.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(Documents::new(&db).get(&address).unwrap().is_none());
    }

    #[test]
    fn handled_missing_child_read_is_fenced_against_creation() {
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        store.save(&[Save::Create(&parent)]).unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        let id = Uuid::from_u128(2);
        assert!(docs.get_owned_child(id).is_err());
        let child = task(2, TaskOwner::Task(parent.id()));
        store.save(&[Save::Create(&child)]).unwrap();
        assert!(docs.get_owned_child(id).is_err());
        let mut tx = Transaction::new();
        tx.append(docs.into_batch().unwrap()).unwrap();
        assert!(matches!(
            tx.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
    }

    #[test]
    fn unsafe_child_evidence_read_fences_later_intent_or_receipt() {
        for suffix in ["intent", "intent/receipt"] {
            let db = Store::memory().unwrap();
            let tasks = TaskStore::new(&db);
            let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
            let child = task(2, TaskOwner::Task(parent.id()));
            tasks
                .save(&[Save::Create(&parent), Save::Create(&child)])
                .unwrap();
            let mut docs = TaskDocuments::new(&db, &parent).unwrap();
            docs.get_owned_child(child.id()).unwrap();
            let (receipts, has_intent) = docs.read_unsafe_child_evidence(child.id()).unwrap();
            assert!(receipts.is_empty() && !has_intent);
            let key = format!(
                "durable/unsafe-park-async/{}/{}/{}",
                child.id(),
                Uuid::new_v4(),
                suffix
            )
            .into_bytes();
            db.commit(
                &[],
                &[Mutation::Put {
                    key,
                    value: b"{}".to_vec(),
                }],
            )
            .unwrap();
            let mut tx = Transaction::new();
            tx.append(docs.into_batch().unwrap()).unwrap();
            assert!(matches!(
                tx.commit(&db),
                Err(Error::Store(durable_store::Error::Conflict { .. }))
            ));
        }
    }

    #[test]
    fn repeated_unsafe_child_evidence_reads_coalesce_fences() {
        let db = Store::memory().unwrap();
        let tasks = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        let child = task(2, TaskOwner::Task(parent.id()));
        tasks
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        docs.get_owned_child(child.id()).unwrap();
        docs.read_unsafe_child_evidence(child.id()).unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: b"unrelated".to_vec(),
                value: vec![1],
            }],
        )
        .unwrap();
        docs.read_unsafe_child_evidence(child.id()).unwrap();
        let mut tx = Transaction::new();
        tx.append(docs.into_batch().unwrap()).unwrap();
        tx.commit(&db).unwrap();
    }

    #[test]
    fn transaction_coalesces_identical_checks_but_rejects_conflicts() {
        let mut tx = Transaction::new();
        let batch = |expected| OwnedBatch {
            checks: vec![Check {
                key: b"owner".to_vec(),
                expected,
            }],
            mutations: vec![],
        };
        tx.append(batch(Expected::Missing)).unwrap();
        tx.append(batch(Expected::Missing)).unwrap();
        assert!(matches!(
            tx.append(batch(Expected::Version(1))),
            Err(Error::InvalidBatch)
        ));
        tx.commit(&Store::memory().unwrap()).unwrap();
    }

    #[test]
    fn transaction_rejects_independently_prepared_ownership_cycles() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = task(1, TaskOwner::Task(Uuid::from_u128(2)));
        let b = task(2, TaskOwner::Task(a.id()));
        for reverse in [false, true] {
            let first = ts
                .prepare_save(&[Save::Create(if reverse { &b } else { &a })])
                .unwrap();
            let second = ts
                .prepare_save(&[Save::Create(if reverse { &a } else { &b })])
                .unwrap();
            let mut tx = Transaction::new();
            tx.append(first).unwrap();
            assert!(matches!(tx.append(second), Err(Error::InvalidBatch)));
            tx.commit(&db).unwrap();
            assert!(
                ts.load(if reverse { b.id() } else { a.id() })
                    .unwrap()
                    .is_some()
            );
            // Start the next ordering against the same empty graph.
            let id = if reverse { b.id() } else { a.id() };
            db.commit(
                &[],
                &[Mutation::Delete {
                    key: crate::task_store::key(id),
                }],
            )
            .unwrap();
        }
    }

    #[test]
    fn transaction_allows_shared_missing_ownership_roots() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = task(1, TaskOwner::Task(Uuid::from_u128(3)));
        let b = task(2, TaskOwner::Task(Uuid::from_u128(3)));
        let mut tx = Transaction::new();
        tx.append(ts.prepare_save(&[Save::Create(&a)]).unwrap())
            .unwrap();
        tx.append(ts.prepare_save(&[Save::Create(&b)]).unwrap())
            .unwrap();
        tx.commit(&db).unwrap();
        assert_eq!(ts.list().unwrap().len(), 2);
    }

    #[test]
    fn transaction_rejects_cross_component_missing_edge_in_both_orders() {
        let key = crate::ownership_edge::edge_key(Uuid::from_u128(7));
        for (write_first, timed) in [(false, false), (true, false), (false, true), (true, true)] {
            let check = Check {
                key: key.clone(),
                expected: if timed {
                    Expected::MissingBefore(
                        std::time::Instant::now() + std::time::Duration::from_secs(60),
                    )
                } else {
                    Expected::Missing
                },
            };
            let read = || OwnedBatch {
                checks: vec![check.clone()],
                mutations: vec![],
            };
            let write = || OwnedBatch {
                checks: vec![check.clone()],
                mutations: vec![Mutation::Put {
                    key: key.clone(),
                    value: vec![],
                }],
            };
            let mut tx = Transaction::new();
            tx.append(if write_first { write() } else { read() })
                .unwrap();
            assert!(matches!(
                tx.append(if write_first { read() } else { write() }),
                Err(Error::InvalidBatch)
            ));
        }
    }

    fn task(id: u128, owner: TaskOwner) -> Task {
        Task::new(
            Uuid::from_u128(id),
            owner,
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
    }

    fn conversation(db: &Store, id: Uuid) {
        let state = crate::runtime::Harness::empty_conversation(
            id,
            crate::runtime::Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 1,
            },
        );
        let key = format!("runtime/conversation/{id}").into_bytes();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&state).unwrap(),
            }],
        )
        .unwrap();
    }

    #[test]
    fn application_entry_expected_head_is_atomic_and_includes_staged_entries() {
        for sqlite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("entries.sqlite");
            let db = if sqlite {
                Store::open(&path).unwrap()
            } else {
                Store::memory().unwrap()
            };
            let id = Uuid::from_u128(123);
            conversation(&db, id);
            let author = task(1, TaskOwner::Conversation(id));
            let mut first = TaskDocuments::new(&db, &author).unwrap();
            let mut stale = TaskDocuments::new(&db, &author).unwrap();
            let addr = Address {
                scope: "test".into(),
                name: "atomic".into(),
                key: "".into(),
            };
            stale.get(&addr).unwrap();
            stale
                .put(&addr, json!({"stale": true}).as_object().unwrap().clone())
                .unwrap();
            stale
                .append_entry_if_head(None, "app.stale", json!(0))
                .unwrap();
            let first_id = first
                .append_entry_if_head(None, "app.first", json!(1))
                .unwrap();
            assert!(matches!(
                first.append_entry_if_head(None, "app.invalid", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            let second_id = first
                .append_entry_if_head(Some(first_id), "app.second", json!(2))
                .unwrap();
            let batch = first.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let batch = stale.into_batch().unwrap();
            assert!(matches!(
                db.commit(&batch.checks, &batch.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(Documents::new(&db).get(&addr).unwrap().is_none());
            let mut mismatch = TaskDocuments::new(&db, &author).unwrap();
            assert!(matches!(
                mismatch.append_entry_if_head(Some(first_id), "app.invalid", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            let empty = mismatch.into_batch().unwrap();
            assert!(empty.mutations.is_empty());
            let mut next = TaskDocuments::new(&db, &author).unwrap();
            let third_id = next
                .append_entry_if_head(Some(second_id), "app.third", json!(3))
                .unwrap();
            let batch = next.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let expected = vec![first_id, second_id, third_id];
            if sqlite {
                drop(db);
                let reopened = Store::open(&path).unwrap();
                let entries = crate::application_entry::list(&reopened, id).unwrap();
                assert_eq!(entries.iter().map(|e| e.id).collect::<Vec<_>>(), expected);
            } else {
                let entries = crate::application_entry::list(&db, id).unwrap();
                assert_eq!(entries.iter().map(|e| e.id).collect::<Vec<_>>(), expected);
            }
        }
    }

    #[test]
    fn handled_head_mismatch_fences_document_writes_and_keeps_snapshot() {
        for sqlite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = if sqlite {
                Store::open(dir.path().join("db")).unwrap()
            } else {
                Store::memory().unwrap()
            };
            let id = Uuid::from_u128(124);
            conversation(&db, id);
            let author = task(1, TaskOwner::Conversation(id));
            let address = Address {
                scope: "test".into(),
                name: "decision".into(),
                key: "".into(),
            };
            let mut decision = TaskDocuments::new(&db, &author).unwrap();
            decision.get(&address).unwrap();
            assert!(matches!(
                decision.append_entry_if_head(Some(Uuid::from_u128(99)), "app.x", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            let mut concurrent = TaskDocuments::new(&db, &author).unwrap();
            concurrent
                .append_entry_if_head(None, "app.concurrent", json!(1))
                .unwrap();
            let batch = concurrent.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            // A second check sees the same transaction-local observation.
            assert!(matches!(
                decision.append_entry_if_head(Some(Uuid::from_u128(99)), "app.x", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            decision
                .put(
                    &address,
                    json!({"decision": true}).as_object().unwrap().clone(),
                )
                .unwrap();
            let batch = decision.into_batch().unwrap();
            assert!(matches!(
                db.commit(&batch.checks, &batch.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(Documents::new(&db).get(&address).unwrap().is_none());
        }
    }

    #[test]
    fn inherited_head_is_checked_independently_on_each_fork() {
        let db = Store::memory().unwrap();
        let parent = Uuid::from_u128(125);
        let child = Uuid::from_u128(126);
        conversation(&db, parent);
        conversation(&db, child);
        let mut parent_tx =
            TaskDocuments::new(&db, &task(1, TaskOwner::Conversation(parent))).unwrap();
        let inherited = parent_tx
            .append_entry_if_head(None, "app.parent", json!(1))
            .unwrap();
        let batch = parent_tx.into_batch().unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let docs = Documents::new(&db);
        let batch = docs
            .fork_scope(parent, child, docs.snapshot().unwrap())
            .unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let mut parent_tx =
            TaskDocuments::new(&db, &task(1, TaskOwner::Conversation(parent))).unwrap();
        parent_tx
            .append_entry_if_head(Some(inherited), "app.parent2", json!(2))
            .unwrap();
        let batch = parent_tx.into_batch().unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let mut child_tx =
            TaskDocuments::new(&db, &task(2, TaskOwner::Conversation(child))).unwrap();
        child_tx
            .append_entry_if_head(Some(inherited), "app.child", json!(3))
            .unwrap();
        let batch = child_tx.into_batch().unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        assert_eq!(
            crate::application_entry::list(&db, parent).unwrap().len(),
            2
        );
        assert_eq!(crate::application_entry::list(&db, child).unwrap().len(), 2);
    }

    #[test]
    fn handled_mismatch_fences_root_deletion() {
        let db = Store::memory().unwrap();
        let id = Uuid::from_u128(127);
        conversation(&db, id);
        let mut tx = TaskDocuments::new(&db, &task(1, TaskOwner::Conversation(id))).unwrap();
        assert!(matches!(
            tx.append_entry_if_head(Some(Uuid::from_u128(99)), "app.x", json!(0)),
            Err(Error::ApplicationEntryHeadMismatch)
        ));
        db.commit(
            &[],
            &[Mutation::Delete {
                key: format!("runtime/conversation/{id}").into_bytes(),
            }],
        )
        .unwrap();
        let batch = tx.into_batch().unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
    }

    #[test]
    fn root_conversation_fences_both_abort_orderings() {
        let db = Store::memory().unwrap();
        let id = Uuid::from_u128(99);
        let root = task(1, TaskOwner::Conversation(id));
        let author = task(2, TaskOwner::Task(root.id()));
        TaskStore::new(&db)
            .save(&[Save::Create(&root), Save::Create(&author)])
            .unwrap();
        assert!(matches!(
            TaskDocuments::new(&db, &author)
                .unwrap()
                .append_entry("app.test", json!(1)),
            Err(Error::InvalidApplicationEntry)
        ));
        conversation(&db, id);
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        let addr = Address {
            scope: "s".into(),
            name: "race".into(),
            key: "".into(),
        };
        docs.get(&addr).unwrap();
        docs.put(&addr, json!({"value": 1}).as_object().unwrap().clone())
            .unwrap();
        docs.append_entry("app.test", json!(1)).unwrap();
        let batch = docs.into_batch().unwrap();
        let key = format!("runtime/conversation/{id}").into_bytes();
        let record = db.get(&key).unwrap().unwrap();
        let mut state: crate::runtime::Conversation =
            serde_json::from_slice(&record.value).unwrap();
        state.abort_requested = true;
        let checks = [Check {
            key: key.clone(),
            expected: Expected::Version(record.version),
        }];
        let mutations = [Mutation::Put {
            key,
            value: serde_json::to_vec(&state).unwrap(),
        }];
        db.commit(&checks, &mutations).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(Documents::new(&db).get(&addr).unwrap().is_none());
        assert!(crate::application_entry::list(&db, id).unwrap().is_empty());
        assert!(matches!(
            TaskDocuments::new(&db, &author)
                .unwrap()
                .append_entry("app.test", json!(1)),
            Err(Error::InvalidApplicationEntry)
        ));

        let other = Uuid::from_u128(100);
        conversation(&db, other);
        let direct = task(4, TaskOwner::Conversation(other));
        let mut docs = TaskDocuments::new(&db, &direct).unwrap();
        docs.append_entry("app.test", json!(2)).unwrap();
        let batch = docs.into_batch().unwrap();
        let key = format!("runtime/conversation/{other}").into_bytes();
        let record = db.get(&key).unwrap().unwrap();
        let mut state: crate::runtime::Conversation =
            serde_json::from_slice(&record.value).unwrap();
        state.abort_requested = true;
        db.commit(&batch.checks, &batch.mutations).unwrap();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(record.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&state).unwrap(),
            }],
        )
        .unwrap();
        assert_eq!(crate::application_entry::list(&db, other).unwrap().len(), 1);
    }

    #[test]
    fn ancestry_checks_fence_atomic_commit_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        conversation(&db, Uuid::from_u128(99));
        let root = task(1, TaskOwner::Conversation(Uuid::from_u128(99)));
        let middle = task(2, TaskOwner::Task(root.id()));
        let author = task(3, TaskOwner::Task(middle.id()));
        ts.save(&[
            Save::Create(&root),
            Save::Create(&middle),
            Save::Create(&author),
        ])
        .unwrap();
        let addr = Address {
            scope: "s".into(),
            name: "n".into(),
            key: "k".into(),
        };
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        docs.get(&addr).unwrap();
        docs.put(&addr, json!({"value": 1}).as_object().unwrap().clone())
            .unwrap();
        docs.append_entry("app.test", json!(1)).unwrap();
        let batch = docs.into_batch().unwrap();
        let snapshot = ts.load(root.id()).unwrap().unwrap();
        ts.update(&snapshot, |t| t.request_abort()).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        drop(db);
        let db = Store::open(&path).unwrap();
        assert!(Documents::new(&db).get(&addr).unwrap().is_none());
        assert!(
            crate::application_entry::list(&db, Uuid::from_u128(99))
                .unwrap()
                .is_empty()
        );
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(1)),
            Err(Error::InvalidApplicationEntry)
        ));
    }

    #[test]
    fn missing_terminal_and_cyclic_ancestors_fail_closed() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let author = task(3, TaskOwner::Task(Uuid::from_u128(2)));
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(null)),
            Err(Error::InvalidApplicationEntry)
        ));
        let mut terminal = task(2, TaskOwner::Conversation(Uuid::from_u128(99)));
        terminal.cancel().unwrap();
        ts.save(&[Save::Create(&terminal)]).unwrap();
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(null)),
            Err(Error::InvalidApplicationEntry)
        ));
        let cycle_a = task(4, TaskOwner::Task(Uuid::from_u128(5)));
        let cycle_b = task(5, TaskOwner::Task(cycle_a.id()));
        let cyclic_author = task(6, TaskOwner::Task(cycle_a.id()));
        // Simulate a legacy corrupt store, bypassing current admission checks.
        let corrupt = [cycle_a, cycle_b]
            .iter()
            .map(|task| Mutation::Put {
                key: crate::task_store::key(task.id()),
                value: serde_json::to_vec(task).unwrap(),
            })
            .collect::<Vec<_>>();
        db.commit(&[], &corrupt).unwrap();
        let mut docs = TaskDocuments::new(&db, &cyclic_author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(null)),
            Err(Error::InvalidApplicationEntry)
        ));
    }
    #[test]
    fn process_exit_discards_nested_staging() {
        const ENV: &str = "PI_NESTED_ENTRY_CRASH_DB";
        if let Ok(path) = std::env::var(ENV) {
            let db = Store::open(path).unwrap();
            let author = task(3, TaskOwner::Task(Uuid::from_u128(2)));
            let mut docs = TaskDocuments::new(&db, &author).unwrap();
            let addr = Address {
                scope: "s".into(),
                name: "crash".into(),
                key: "".into(),
            };
            docs.get(&addr).unwrap();
            docs.put(&addr, json!({"value": 1}).as_object().unwrap().clone())
                .unwrap();
            docs.append_entry("app.crash", json!(1)).unwrap();
            std::process::exit(0);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = Store::open(&path).unwrap();
            conversation(&db, Uuid::from_u128(99));
            let root = task(1, TaskOwner::Conversation(Uuid::from_u128(99)));
            let middle = task(2, TaskOwner::Task(root.id()));
            let author = task(3, TaskOwner::Task(middle.id()));
            TaskStore::new(&db)
                .save(&[
                    Save::Create(&root),
                    Save::Create(&middle),
                    Save::Create(&author),
                ])
                .unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("document::nested_entry_tests::process_exit_discards_nested_staging")
            .env(ENV, &path)
            .status()
            .unwrap();
        assert!(status.success());
        let db = Store::open(&path).unwrap();
        let addr = Address {
            scope: "s".into(),
            name: "crash".into(),
            key: "".into(),
        };
        assert!(Documents::new(&db).get(&addr).unwrap().is_none());
        assert!(
            crate::application_entry::list(&db, Uuid::from_u128(99))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            TaskStore::new(&db)
                .load(Uuid::from_u128(3))
                .unwrap()
                .unwrap()
                .task
                .checkpoint()
                .phase,
            "start"
        );
    }
    #[test]
    fn deep_author_commits_to_root_conversation_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root_id = Uuid::from_u128(99);
        {
            let db = Store::open(&path).unwrap();
            conversation(&db, root_id);
            let root = task(1, TaskOwner::Conversation(root_id));
            let middle = task(2, TaskOwner::Task(root.id()));
            let author = task(3, TaskOwner::Task(middle.id()));
            TaskStore::new(&db)
                .save(&[
                    Save::Create(&root),
                    Save::Create(&middle),
                    Save::Create(&author),
                ])
                .unwrap();
            let mut docs = TaskDocuments::new(&db, &author).unwrap();
            docs.append_entry("app.deep", json!(42)).unwrap();
            let batch = docs.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
        }
        let db = Store::open(&path).unwrap();
        let entries = crate::application_entry::list(&db, root_id).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].conversation_id, root_id);
        assert_eq!(entries[0].author_task_id, Uuid::from_u128(3));
        assert_eq!(entries[0].data, json!(42));
    }
}

#[cfg(test)]
mod fork_current_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fork_fence_rejects_concurrent_creation_and_atomic_value_policy_change() {
        let db = Store::memory().unwrap();
        let parent = uuid::Uuid::new_v4();
        let child = uuid::Uuid::new_v4();
        let address = Address {
            scope: parent.to_string(),
            name: "a".into(),
            key: "".into(),
        };
        let mut tx = Transaction::new();
        tx.put(
            &address,
            Expected::Missing,
            json!({"n": 1}).as_object().unwrap(),
        )
        .unwrap();
        tx.set_fork_policy(&address, Expected::Missing, &ForkPolicy::Current)
            .unwrap();
        tx.commit(&db).unwrap();
        let docs = Documents::new(&db);
        let cut = docs.snapshot().unwrap();
        let batch = docs.fork_scope(parent, child, cut).unwrap();
        // A writer on another thread commits while the prepared fork is pending.
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut tx = Transaction::new();
                    tx.put(
                        &address,
                        Expected::Version(docs.get(&address).unwrap().unwrap().version),
                        json!({"n": 2}).as_object().unwrap(),
                    )
                    .unwrap();
                    tx.set_fork_policy(
                        &address,
                        Expected::Version(docs.fork_policy(&address).unwrap().unwrap().version),
                        &ForkPolicy::Copy,
                    )
                    .unwrap();
                    tx.commit(&db).unwrap();
                })
                .join()
                .unwrap();
        });
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let batch = docs.fork_scope(parent, child, cut).unwrap();
        let new_address = Address {
            name: "new".into(),
            ..address.clone()
        };
        let mut tx = Transaction::new();
        tx.set_fork_policy(&new_address, Expected::Missing, &ForkPolicy::Current)
            .unwrap();
        tx.commit(&db).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let batch = docs.fork_scope(parent, child, cut).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let copied = Address {
            scope: child.to_string(),
            ..address
        };
        assert_eq!(docs.get(&copied).unwrap().unwrap().value["n"], 2);
        assert_eq!(
            docs.fork_policy(&copied).unwrap().unwrap().policy,
            ForkPolicy::Copy
        );
    }
}

#[cfg(test)]
mod chord_application_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_arrays_unicode_and_splice_match_chord() {
        let base = json!({"rows":[{"v":["a😀z", 1, 2]}], "0":"number key"});
        let ops = vec![
            json!(["t", ["rows", 0, "v", 0], 3]),
            json!(["a", ["rows", 0, "v", 0], "!"]),
            json!(["s", ["rows", 0, "v", 3], 4]),
            json!(["d", ["rows", 0, "v", 1]]),
            json!(["p", ["rows", 0, "v"], 99, 99, [5]]),
            json!(["m", ["rows", 0, "v"], [3, 2, 1, 0]]),
            json!(["s", [0], "number segment"]),
        ];
        assert_eq!(
            Value::Object(apply_operations(base.as_object().unwrap(), &ops).unwrap()),
            json!({"rows":[{"v":[5,4,2,"z!"]}],"0":"number segment"})
        );
        let clamp = vec![json!(["p", ["rows", 0, "v"], 1, 99, []])];
        assert_eq!(
            apply_operations(base.as_object().unwrap(), &clamp).unwrap()["rows"][0]["v"],
            json!(["a😀z"])
        );
        let root = vec![
            json!(["r", [1, 2]]),
            json!(["p", [], 1, 0, [3]]),
            json!(["m", [], [2, 1, 0]]),
            json!(["r",{"ok":true}]),
        ];
        assert_eq!(
            apply_operations(base.as_object().unwrap(), &root).unwrap()["ok"],
            true
        );
    }

    #[test]
    fn rejects_surrogate_splits_sparse_arrays_unsafe_paths_and_bad_permutations() {
        let base = json!({"a":["😀z",1]});
        for op in [
            json!(["t", ["a", 0], 1]),
            json!(["s", ["a", 3], 0]),
            json!(["s", ["a", "0"], 0]),
            json!(["s", ["__proto__"], 0]),
            json!(["m", ["a"], [0, 0]]),
            json!(["p", ["a"], 9007199254740992u64, 0, []]),
        ] {
            assert!(apply_operations(base.as_object().unwrap(), &[op]).is_err());
        }
        let result =
            apply_operations(base.as_object().unwrap(), &[json!(["t", ["a", 0], 99])]).unwrap();
        assert_eq!(result["a"][0], "");
    }
}

#[cfg(all(test, unix))]
mod detached_task_documents_tests {
    use super::*;
    use crate::task::{
        Checkpoint, Task, TaskOwner, WaitFor,
        scheduler::{Handler, Step},
    };
    use crate::task_store::{Save, SignalResult, SignalToken, TaskStore};
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    fn setup() -> (
        tempfile::TempDir,
        Arc<durable_store::owner::OwnedStore>,
        crate::task_store::StoredTask,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let owner =
            Arc::new(durable_store::owner::OwnedStore::open(dir.path().join("db")).unwrap());
        let mut task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), true);
        task.start_attempt(Uuid::from_u128(100)).unwrap();
        let ts = TaskStore::new(owner.store());
        ts.save(&[Save::Create(&task)]).unwrap();
        let running = ts.load(task.id()).unwrap().unwrap();
        (dir, owner, running)
    }

    #[test]
    fn detached_effects_prepare_on_another_thread_and_commit_as_one_graph() {
        fn assert_send<T: Send>() {}
        assert_send::<DetachedTaskDocuments>();
        let (_dir, owner, running) = setup();
        let store = owner.store();
        let ts = TaskStore::new(store);
        let conversation = Uuid::from_u128(9);
        let conversation_key = format!("runtime/conversation/{conversation}").into_bytes();
        let conversation_state = crate::runtime::Harness::empty_conversation(
            conversation,
            crate::runtime::Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 1,
            },
        );
        store
            .commit(
                &[Check {
                    key: conversation_key.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: conversation_key,
                    value: serde_json::to_vec(&conversation_state).unwrap(),
                }],
            )
            .unwrap();
        let mut receiver = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            Checkpoint {
                phase: "signal".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap();
        receiver.start().unwrap();
        let token = SignalToken::new("answer", 1).unwrap();
        receiver
            .wait(WaitFor::TypedSleep {
                until_millis: Some(60_000),
                signals: vec![token.key("k").unwrap()],
            })
            .unwrap();
        ts.save(&[Save::Create(&receiver)]).unwrap();
        let address = Address {
            scope: "test".into(),
            name: "owned".into(),
            key: "1".into(),
        };
        let mut docs = TaskDocuments::new(store, &running.task).unwrap();
        docs.acquire_owned(&address).unwrap();
        docs.put(&address, json!({"value":42}).as_object().unwrap().clone())
            .unwrap();
        assert_eq!(
            docs.signal_at(receiver.id(), &token, "k", json!(7), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        assert_eq!(
            docs.signal_at(running.task.id(), &token, "self", json!(8), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        let first = docs.append_entry("first", json!(1)).unwrap();
        let second = docs.append_entry("second", json!(2)).unwrap();
        let child = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(running.task.id()),
            Checkpoint {
                phase: "child".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        let handler: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
        docs.stage_task_tool_child(
            "child",
            "tool",
            &json!(null),
            child.clone(),
            handler.clone(),
        )
        .unwrap();
        let original_deadline = docs
            .signals
            .batch
            .checks
            .iter()
            .find_map(|check| match check.expected {
                Expected::VersionBefore(_, deadline) => Some(deadline),
                _ => None,
            })
            .unwrap();
        // Build a reference invocation with the identical staged receipt IDs,
        // then prepare it using the existing borrowed scheduler sequence.
        let mut reference = TaskDocuments {
            store,
            input_validator: docs.input_validator,
            revision: docs.revision,
            reads: docs.reads.clone(),
            writes: docs.writes.clone(),
            acquired: docs.acquired.clone(),
            task: docs.task.clone(),
            entries: docs.entries.clone(),
            entry_initial_len: docs.entry_initial_len,
            runtime_cut: docs.runtime_cut,
            runtime_config: docs.runtime_config.clone(),
            task_input: docs.task_input.clone(),
            task_input_conflict: docs.task_input_conflict.clone(),
            task_input_storage: docs.task_input_storage.clone(),
            signals: StagedSignals {
                batch: OwnedBatch {
                    checks: docs.signals.batch.checks.clone(),
                    mutations: docs.signals.batch.mutations.clone(),
                },
                keys: docs.signals.keys.clone(),
                equivalent_self_versions: docs.signals.equivalent_self_versions.clone(),
                canonical_self_version: docs.signals.canonical_self_version,
            },
            child_reads: docs.child_reads.clone(),
            keyed_children: Vec::new(),
            keyed_child_handlers: HashMap::new(),
        };
        reference
            .stage_task_tool_child(
                "child",
                "tool",
                &json!(null),
                child.clone(),
                handler.clone(),
            )
            .unwrap();
        let detached = docs.detach(owner.clone(), &running).unwrap();
        // A same-value reservation rewrite is allowed and must rebase only its checks.
        ts.update(&running, |_| Ok(())).unwrap();
        let current = ts.load(running.task.id()).unwrap().unwrap();
        let prepared = std::thread::spawn(move || detached.prepare(&current).unwrap())
            .join()
            .unwrap();
        let parent_key = crate::task_store::key(running.task.id());
        let mut reference_children = std::mem::take(&mut reference.keyed_children);
        for (_, _, child) in &mut reference_children {
            for check in &mut child.checks {
                if check.key == parent_key
                    && check.expected == Expected::Version(child.parent_version)
                {
                    check.expected = Expected::Version(prepared.reservation.version);
                }
            }
        }
        let mut reference_tx = Transaction::new();
        reference.stage_acquisitions(&mut reference_tx).unwrap();
        reference.rebase_self_signal_check(prepared.reservation.version);
        let mut reference_batch = reference.into_batch().unwrap();
        for check in &mut reference_batch.checks {
            if check.key == parent_key {
                match &mut check.expected {
                    Expected::Version(v) | Expected::VersionBefore(v, _)
                        if *v == running.version =>
                    {
                        *v = prepared.reservation.version
                    }
                    _ => (),
                }
            }
        }
        assert_eq!(prepared.transaction.acquisitions, reference_tx.acquisitions);
        assert_eq!(prepared.document_batch.checks, reference_batch.checks);
        assert_eq!(prepared.document_batch.mutations, reference_batch.mutations);
        assert_eq!(
            prepared.keyed_children[0].2.checks,
            reference_children[0].2.checks
        );
        assert_eq!(
            prepared.keyed_children[0].2.mutations,
            reference_children[0].2.mutations
        );
        assert!(Arc::ptr_eq(
            prepared.keyed_child_handlers.get("child").unwrap(),
            &handler
        ));
        assert_eq!(prepared.keyed_children[0].2.child, child.id());
        assert!(
            prepared.keyed_children[0]
                .2
                .checks
                .iter()
                .any(
                    |check| check.key == crate::task_store::key(running.task.id())
                        && check.expected == Expected::Version(prepared.reservation.version)
                )
        );
        assert!(prepared.document_batch.checks.iter().any(|check| matches!(check.expected, Expected::VersionBefore(_, deadline) if deadline == original_deadline)));
        let PreparedTaskDocuments {
            owner,
            reservation,
            mut transaction,
            mut document_batch,
            keyed_children,
            ..
        } = prepared;
        let mut next = reservation.clone();
        next.task
            .advance(Checkpoint {
                phase: "next".into(),
                data: json!(null),
            })
            .unwrap();
        next.task.yield_ready().unwrap();
        next.task
            .record_completed_attempt(&reservation.task)
            .unwrap();
        let ts = TaskStore::new(owner.store());
        let mut graph = ts
            .prepare_save(&[Save::Update(&next), Save::Create(&child)])
            .unwrap();
        let parent_key = crate::task_store::key(reservation.task.id());
        for (_, _, prepared) in keyed_children {
            for check in prepared.checks {
                if !graph.checks.contains(&check) {
                    graph.checks.push(check);
                }
            }
            for mutation in prepared.mutations {
                let key = match &mutation {
                    Mutation::Put { key, .. } | Mutation::Delete { key } => key,
                };
                if *key != parent_key && !key.starts_with(crate::task_store::PREFIX) {
                    graph.mutations.push(mutation);
                }
            }
        }
        reconcile_task_checks(&mut document_batch, &mut graph);
        transaction.append(document_batch).unwrap();
        transaction.append(graph).unwrap();
        transaction.commit(owner.store()).unwrap();
        assert_eq!(
            Documents::new(owner.store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"],
            42
        );
        assert_eq!(
            owner
                .store()
                .task_document_owner(&address.storage_key().unwrap())
                .unwrap(),
            Some(reservation.task.id())
        );
        assert_eq!(
            ts.read_typed_signal(receiver.id(), &token, "k").unwrap(),
            Some(json!(7))
        );
        assert_eq!(
            ts.read_typed_signal(reservation.task.id(), &token, "self")
                .unwrap(),
            Some(json!(8))
        );
        assert_eq!(
            crate::application_entry::list(owner.store(), conversation)
                .unwrap()
                .iter()
                .map(|entry| entry.id)
                .collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(
            ts.load(child.id()).unwrap().unwrap().task.owner(),
            &TaskOwner::Task(reservation.task.id())
        );
        assert_eq!(
            ts.load(reservation.task.id())
                .unwrap()
                .unwrap()
                .task
                .checkpoint()
                .phase,
            "next"
        );
    }

    #[test]
    fn detach_rejects_wrong_owner_and_prepare_fences_changed_attempt() {
        let (_dir, owner, running) = setup();
        let other_dir = tempfile::tempdir().unwrap();
        let other =
            Arc::new(durable_store::owner::OwnedStore::open(other_dir.path().join("db")).unwrap());
        let docs = TaskDocuments::new(owner.store(), &running.task).unwrap();
        assert!(matches!(
            docs.detach(other, &running),
            Err(Error::InvalidBatch)
        ));
        let docs = TaskDocuments::new(owner.store(), &running.task).unwrap();
        let detached = docs.detach(owner.clone(), &running).unwrap();
        let ts = TaskStore::new(owner.store());
        ts.update(&running, |t| t.request_abort()).unwrap();
        let changed = ts.load(running.task.id()).unwrap().unwrap();
        assert!(matches!(
            detached.prepare(&changed),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
    }

    #[test]
    fn detached_prepare_keeps_effects_when_reservation_version_moves() {
        let (_dir, owner, running) = setup();
        let docs = TaskDocuments::new(owner.store(), &running.task).unwrap();
        let detached = docs.detach(owner.clone(), &running).unwrap();
        let ts = TaskStore::new(owner.store());
        ts.update(&running, |_| Ok(())).unwrap();
        let newer = ts.load(running.task.id()).unwrap().unwrap();
        assert_ne!(newer.version, running.version);
        let prepared = detached.prepare(&running).unwrap();
        let mut next = prepared.reservation;
        next.task.succeed(json!("done")).unwrap();
        let mut tx = prepared.transaction;
        tx.append(prepared.document_batch).unwrap();
        tx.append(ts.prepare_save(&[Save::Update(&next)]).unwrap())
            .unwrap();
        assert!(matches!(
            tx.commit(owner.store()),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(
            ts.load(running.task.id()).unwrap().unwrap().task,
            running.task
        );
    }

    #[test]
    fn detached_deadline_expiry_rolls_back_document_and_parent() {
        let (_dir, owner, running) = setup();
        let address = Address {
            scope: "test".into(),
            name: "atomic".into(),
            key: "".into(),
        };
        let mut docs = TaskDocuments::new(owner.store(), &running.task).unwrap();
        docs.get(&address).unwrap();
        docs.put(&address, json!({"value":1}).as_object().unwrap().clone())
            .unwrap();
        let deadline = std::time::Instant::now() - std::time::Duration::from_secs(1);
        docs.signals.batch.checks.push(Check {
            key: crate::task_store::key(running.task.id()),
            expected: Expected::VersionBefore(running.version, deadline),
        });
        let prepared = docs
            .detach(owner.clone(), &running)
            .unwrap()
            .prepare(&running)
            .unwrap();
        assert!(prepared.document_batch.checks.iter().any(|check| matches!(check.expected, Expected::VersionBefore(v, d) if v == running.version && d == deadline)));
        let mut next = running.clone();
        next.task.succeed(json!("done")).unwrap();
        let mut graph = TaskStore::new(owner.store())
            .prepare_save(&[Save::Update(&next)])
            .unwrap();
        let mut doc = prepared.document_batch;
        reconcile_task_checks(&mut doc, &mut graph);
        let mut tx = prepared.transaction;
        tx.append(doc).unwrap();
        tx.append(graph).unwrap();
        assert!(matches!(
            tx.commit(owner.store()),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(
            Documents::new(owner.store())
                .get(&address)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            TaskStore::new(owner.store())
                .load(running.task.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &crate::task::TaskState::Running
        );
    }
}
