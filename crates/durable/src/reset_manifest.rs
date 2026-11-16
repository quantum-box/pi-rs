//! Private structural format for a future atomic reset-batch proof.
//!
//! The private lookup checks storage evidence but does not decode a candidate
//! or authorize forks. Callers must obtain `ExpectedBinding` from authenticated
//! committed state, independently derive each cut, and prove the stored cursor
//! chain and snapshots in one transaction.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_CUTS: usize = 64;
// Version 2 stores the batch once, with a small same-commit reference per cut.
// Bound reads of untrusted storage independently of the number of queued resets.
const MAX_SHARED_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_SHARED_CUTS: usize = 16_384;
pub(crate) const MANIFEST_CHUNK_BYTES: usize = 64 * 1024;
const MAX_TRAILING_ENTRIES: u64 = 64;
const MAX_FINITE_WRITE_LIMIT: u64 = 1_000_000;

#[cfg(test)]
thread_local! {
    pub(crate) static RESET_MANIFEST_CHUNK_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A same-commit candidate, not a decoded or authenticated manifest. The
/// caller must independently derive and validate its binding before use.
pub(crate) struct RawCandidate {
    pub(crate) bytes: Vec<u8>,
    pub(crate) shared_reference: Option<SharedReference>,
    /// Carry these checks into the transaction publishing any dependent fork.
    /// They include the candidate namespace's put fence.
    pub(crate) checks: Vec<durable_store::Check>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LookupError {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Transcript(#[from] crate::transcript::Error),
    #[error("invalid or ambiguous reset manifest evidence")]
    Invalid,
}

#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_CANDIDATE_CHECK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    pub(crate) static CANDIDATE_ERROR: std::cell::RefCell<Option<LookupError>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn candidate_prefix(id: Uuid, entry: Uuid, sequence: u64) -> Vec<u8> {
    format!("runtime/reset-manifest/{id}/{entry}/{sequence:020}/").into_bytes()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedReference {
    pub(crate) version: u32,
    pub(crate) batch: Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChunkHeader {
    pub(crate) version: u32,
    pub(crate) chunks: usize,
}

pub(crate) fn chunk_key(id: Uuid, batch: Uuid, index: usize) -> Vec<u8> {
    format!("runtime/reset-batch/{id}/{batch}/{index:020}").into_bytes()
}

/// Authenticate one cut's small reference without rereading its already
/// validated shared manifest. The caller retains and rechecks the shared
/// manifest's original fences together with these checks.
pub(crate) fn lookup_shared_reference_checks(
    store: &durable_store::Store,
    id: Uuid,
    cursor: &crate::transcript::Cursor,
    expected: &SharedReference,
    commit: u64,
) -> Result<Vec<durable_store::Check>, LookupError> {
    use durable_store::{Check, Expected};
    let (entry, sequence) = cursor.entry_identity().ok_or(LookupError::Invalid)?;
    if cursor.transcript_id() != id || entry.is_nil() || sequence == 0 {
        return Err(LookupError::Invalid);
    }
    let prefix = candidate_prefix(id, entry, sequence);
    let boundary = store.put_version()?;
    let keys = store.prefix_keys_bounded(&prefix, 2, prefix.len() + 36)?;
    if keys.len() != 1 {
        return Err(LookupError::Invalid);
    }
    let key = &keys[0];
    let suffix = std::str::from_utf8(&key[prefix.len()..]).map_err(|_| LookupError::Invalid)?;
    let nonce = Uuid::parse_str(suffix).map_err(|_| LookupError::Invalid)?;
    if nonce.is_nil() || nonce.to_string() != suffix {
        return Err(LookupError::Invalid);
    }
    let reference = store
        .get_bounded(key, MAX_MANIFEST_BYTES)?
        .ok_or(LookupError::Invalid)?;
    let actual: SharedReference =
        serde_json::from_slice(&reference.value).map_err(|_| LookupError::Invalid)?;
    if &actual != expected || store.commit_for_version(key, reference.version)? != Some(commit) {
        return Err(LookupError::Invalid);
    }
    Ok(vec![
        Check {
            key: key.clone(),
            expected: Expected::Version(reference.version),
        },
        Check {
            key: prefix,
            expected: Expected::PrefixUnchangedSince(boundary),
        },
    ])
}

pub(crate) fn shared_key(id: Uuid, batch: Uuid) -> Vec<u8> {
    format!("runtime/reset-batch/{id}/{batch}").into_bytes()
}

/// Find one raw candidate for this cursor and validate its storage evidence.
/// `document_revision` must be independently obtained by the caller. This
/// verifies the recorded cut and the writer's tracked document-revision check.
/// It does not decode the manifest, prove reachability, validate
/// reset state, or authorize a fork.
pub(crate) fn lookup_candidate(
    store: &durable_store::Store,
    id: Uuid,
    cursor: &crate::transcript::Cursor,
    anchor_observation: u64,
    document_revision: u64,
) -> Result<Option<RawCandidate>, LookupError> {
    use durable_store::{Check, Expected};
    if id.is_nil() || cursor.transcript_id() != id || anchor_observation == 0 {
        return Err(LookupError::Invalid);
    }
    let (entry_id, entry_sequence) = cursor.entry_identity().ok_or(LookupError::Invalid)?;
    if entry_id.is_nil() || entry_sequence == 0 {
        return Err(LookupError::Invalid);
    }
    let prefix = candidate_prefix(id, entry_id, entry_sequence);
    let boundary = store.put_version()?;
    let keys = store.prefix_keys_bounded(&prefix, 2, prefix.len() + 36)?;
    if keys.is_empty() {
        // A concurrent insertion must not turn a stale miss into a result.
        store.commit_with_prefix_put_fence(&[], &[], &[&prefix], boundary)?;
        return Ok(None);
    }
    if keys.len() != 1 {
        return Err(LookupError::Invalid);
    }
    let key = &keys[0];
    let suffix = std::str::from_utf8(&key[prefix.len()..]).map_err(|_| LookupError::Invalid)?;
    let nonce = Uuid::parse_str(suffix).map_err(|_| LookupError::Invalid)?;
    if nonce.is_nil() || nonce.to_string() != suffix {
        return Err(LookupError::Invalid);
    }
    let reference = store
        .get_bounded(key, MAX_MANIFEST_BYTES)?
        .ok_or(LookupError::Invalid)?;
    let shared_reference = serde_json::from_slice::<SharedReference>(&reference.value).ok();
    let mut chunks = Vec::new();
    let mut chunked_bytes = None;
    let shared = match serde_json::from_slice::<SharedReference>(&reference.value) {
        Ok(reference)
            if (reference.version == 2 || reference.version == 3) && !reference.batch.is_nil() =>
        {
            let batch_key = shared_key(id, reference.batch);
            let entry = store
                .get_bounded(&batch_key, MAX_SHARED_MANIFEST_BYTES)?
                .ok_or(LookupError::Invalid)?;
            if reference.version == 3 {
                let header: ChunkHeader =
                    serde_json::from_slice(&entry.value).map_err(|_| LookupError::Invalid)?;
                if header.version != 3 || header.chunks == 0 {
                    return Err(LookupError::Invalid);
                }
                let mut bytes = Vec::new();
                for index in 0..header.chunks {
                    #[cfg(test)]
                    RESET_MANIFEST_CHUNK_READS.with(|count| count.set(count.get() + 1));
                    let chunk_key = chunk_key(id, reference.batch, index);
                    let chunk = store
                        .get_bounded(&chunk_key, MANIFEST_CHUNK_BYTES)?
                        .ok_or(LookupError::Invalid)?;
                    if chunk.value.is_empty()
                        || (index + 1 < header.chunks && chunk.value.len() != MANIFEST_CHUNK_BYTES)
                    {
                        return Err(LookupError::Invalid);
                    }
                    bytes.extend_from_slice(&chunk.value);
                    chunks.push((chunk_key, chunk.version));
                }
                chunked_bytes = Some(bytes);
            }
            Some((batch_key, entry))
        }
        Ok(_) => return Err(LookupError::Invalid),
        Err(_) => None,
    };
    if chunked_bytes.is_none()
        && let Some((_, batch)) = &shared
    {
        let header: serde_json::Value =
            serde_json::from_slice(&batch.value).map_err(|_| LookupError::Invalid)?;
        if header.get("version") != Some(&serde_json::json!(2)) {
            return Err(LookupError::Invalid);
        }
    }
    let root = format!("runtime/conversation/{id}").into_bytes();
    // These are runtime-owned snapshots, whose size scales with the admitted
    // conversation. They are already read by the runtime without a fixed cap;
    // a proof lookup must not reject a snapshot that settlement accepted.
    let root_entry = store.get(&root)?.ok_or(LookupError::Invalid)?;
    let root_json: serde_json::Value =
        serde_json::from_slice(&root_entry.value).map_err(|_| LookupError::Invalid)?;
    if root_json.get("id") != Some(&serde_json::json!(id)) {
        return Err(LookupError::Invalid);
    }
    let anchor = format!("runtime/fork/{id}/{anchor_observation:020}").into_bytes();
    let fork_prefix = format!("runtime/fork/{id}/");
    let anchor_entry = store.get(&anchor)?.ok_or(LookupError::Invalid)?;
    let mut event_key = b"durable/observer/".to_vec();
    event_key.extend_from_slice(id.as_bytes());
    event_key.extend_from_slice(b"/r");
    event_key.extend_from_slice(&anchor_observation.to_be_bytes());
    let event = store.get(&event_key)?.ok_or(LookupError::Invalid)?;
    let event_json: serde_json::Value =
        serde_json::from_slice(&event.value).map_err(|_| LookupError::Invalid)?;
    let anchor_json: serde_json::Value =
        serde_json::from_slice(&anchor_entry.value).map_err(|_| LookupError::Invalid)?;
    if anchor_json.get("version") != Some(&serde_json::json!(1))
        || anchor_json.get("conversation_id") != Some(&serde_json::json!(id))
        || anchor_json
            .get("_transcript")
            .is_none_or(serde_json::Value::is_null)
        || anchor_json.get("_transcript") != event_json.get("_transcript")
        || store.document_cut_for_version(&anchor, anchor_entry.version)? != Some(document_revision)
    {
        return Err(LookupError::Invalid);
    }
    let fenced_revision = store
        .document_fence_for_version(&anchor, anchor_entry.version)?
        .ok_or(LookupError::Invalid)?;
    if document_revision != fenced_revision
        && fenced_revision.checked_add(1) != Some(document_revision)
    {
        return Err(LookupError::Invalid);
    }
    let anchor_cursor: crate::transcript::Cursor = serde_json::from_value(
        anchor_json
            .get("_transcript")
            .cloned()
            .ok_or(LookupError::Invalid)?,
    )
    .map_err(|_| LookupError::Invalid)?;
    if anchor_cursor.transcript_id() != id
        || anchor_cursor
            .entry_identity()
            .is_none_or(|(entry, sequence)| entry.is_nil() || sequence < entry_sequence)
    {
        return Err(LookupError::Invalid);
    }
    let (entry_key, entry_version) =
        crate::transcript::Transcript::new(store, id).entry_evidence(cursor)?;
    let (anchor_entry_key, anchor_entry_version) =
        crate::transcript::Transcript::new(store, id).entry_evidence(&anchor_cursor)?;
    let sequence = store
        .commit_for_version(&anchor, anchor_entry.version)?
        .ok_or(LookupError::Invalid)?;
    let shared_sequence = shared
        .as_ref()
        .map(|(batch_key, batch)| store.commit_for_version(batch_key, batch.version))
        .transpose()?;
    for (chunk_key, version) in &chunks {
        if store.commit_for_version(chunk_key, *version)? != Some(sequence) {
            return Err(LookupError::Invalid);
        }
    }
    if store.commit_for_version(&event_key, event.version)? != Some(sequence)
        || store.commit_for_version(key, reference.version)? != Some(sequence)
        || shared_sequence.is_some_and(|batch| batch != Some(sequence))
        || store.commit_for_version(&entry_key, entry_version)? != Some(sequence)
        || store.commit_for_version(&anchor_entry_key, anchor_entry_version)? != Some(sequence)
        || !store.runtime_snapshot_exact_at(&root, fork_prefix.as_bytes(), &anchor, sequence)?
    {
        return Err(LookupError::Invalid);
    }
    let mut checks = vec![
        Check {
            key: root,
            expected: Expected::Version(root_entry.version),
        },
        Check {
            key: anchor,
            expected: Expected::Version(anchor_entry.version),
        },
        Check {
            key: event_key,
            expected: Expected::Version(event.version),
        },
        Check {
            key: entry_key,
            expected: Expected::Version(entry_version),
        },
        Check {
            key: anchor_entry_key,
            expected: Expected::Version(anchor_entry_version),
        },
        Check {
            key: key.clone(),
            expected: Expected::Version(reference.version),
        },
    ];
    if let Some((batch_key, batch)) = &shared {
        checks.push(Check {
            key: batch_key.clone(),
            expected: Expected::Version(batch.version),
        });
    }
    checks.extend(chunks.into_iter().map(|(key, version)| Check {
        key,
        expected: Expected::Version(version),
    }));
    checks.push(Check {
        key: prefix,
        expected: Expected::PrefixUnchangedSince(boundary),
    });
    #[cfg(test)]
    BEFORE_CANDIDATE_CHECK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    #[cfg(test)]
    if let Some(error) = CANDIDATE_ERROR.with(|slot| slot.borrow_mut().take()) {
        return Err(error);
    }
    store.commit(&checks, &[])?;
    Ok(Some(RawCandidate {
        bytes: chunked_bytes
            .unwrap_or_else(|| shared.map_or(reference.value, |(_, batch)| batch.value)),
        shared_reference,
        checks,
    }))
}

/// Mirrors the serialized transcript cursor without accepting extra fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCursor {
    pub(crate) transcript: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) next: Option<ManifestLink>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestLink {
    pub(crate) id: Uuid,
    pub(crate) sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResetCut {
    pub(crate) reset_id: Uuid,
    pub(crate) context_generation: Uuid,
    /// `u64::MAX` means all preceding writes; finite limits are bounded.
    pub(crate) write_limit: u64,
    pub(crate) cursor: ManifestCursor,
    /// Lowercase hex SHA-256 of the independently derived cut snapshot.
    pub(crate) state_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResetManifest {
    pub(crate) version: u32,
    pub(crate) conversation_id: Uuid,
    pub(crate) predecessor_observation: u64,
    pub(crate) predecessor_root_version: u64,
    pub(crate) anchor_observation: u64,
    pub(crate) document_revision: u64,
    pub(crate) predecessor_context_generation: Option<Uuid>,
    pub(crate) predecessor_cursor: ManifestCursor,
    pub(crate) anchor_cursor: ManifestCursor,
    pub(crate) cuts: Vec<ResetCut>,
}

// Only the bounded codec serializes this representation. Callers cannot
// bypass binding and structural checks through serde traits on ResetManifest.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawResetManifest {
    version: u32,
    conversation_id: Uuid,
    predecessor_observation: u64,
    predecessor_root_version: u64,
    anchor_observation: u64,
    document_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    predecessor_context_generation: Option<Uuid>,
    predecessor_cursor: ManifestCursor,
    anchor_cursor: ManifestCursor,
    cuts: Vec<ResetCut>,
}

impl From<&ResetManifest> for RawResetManifest {
    fn from(manifest: &ResetManifest) -> Self {
        Self {
            version: manifest.version,
            conversation_id: manifest.conversation_id,
            predecessor_observation: manifest.predecessor_observation,
            predecessor_root_version: manifest.predecessor_root_version,
            anchor_observation: manifest.anchor_observation,
            document_revision: manifest.document_revision,
            predecessor_context_generation: manifest.predecessor_context_generation,
            predecessor_cursor: manifest.predecessor_cursor.clone(),
            anchor_cursor: manifest.anchor_cursor.clone(),
            cuts: manifest.cuts.clone(),
        }
    }
}

impl From<RawResetManifest> for ResetManifest {
    fn from(raw: RawResetManifest) -> Self {
        Self {
            version: raw.version,
            conversation_id: raw.conversation_id,
            predecessor_observation: raw.predecessor_observation,
            predecessor_root_version: raw.predecessor_root_version,
            anchor_observation: raw.anchor_observation,
            document_revision: raw.document_revision,
            predecessor_context_generation: raw.predecessor_context_generation,
            predecessor_cursor: raw.predecessor_cursor,
            anchor_cursor: raw.anchor_cursor,
            cuts: raw.cuts,
        }
    }
}

/// These values must come from authenticated root, observer and document data,
/// never from the untrusted manifest itself. Versions are known before commit;
/// the final store commit sequence is deliberately not encoded here.
#[derive(Clone, Debug)]
pub(crate) struct ExpectedBinding {
    pub(crate) conversation_id: Uuid,
    pub(crate) predecessor_observation: u64,
    pub(crate) predecessor_root_version: u64,
    pub(crate) anchor_observation: u64,
    pub(crate) document_revision: u64,
    pub(crate) predecessor_context_generation: Option<Uuid>,
    pub(crate) predecessor_cursor: ManifestCursor,
    pub(crate) anchor_cursor: ManifestCursor,
    /// Ordered cut identities and state digests derived independently.
    pub(crate) cuts: Vec<ResetCut>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("reset manifest exceeds its size limit")]
    TooLarge,
    #[error("invalid reset manifest encoding")]
    Encoding,
    #[error("reset manifest does not match the authenticated binding")]
    Binding,
    #[error("invalid reset manifest order or identity")]
    Invalid,
}

/// Untrusted locations used only to find evidence for independent replay.
/// These values must never be used as an expected binding without first
/// authenticating the predecessor, transcript chain and derived reset states.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReplayHints {
    pub(crate) version: u32,
    pub(crate) predecessor_observation: u64,
    pub(crate) predecessor_root_version: u64,
    pub(crate) cuts: Vec<ReplayCutHint>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReplayCutHint {
    pub(crate) reset_id: Uuid,
    pub(crate) context_generation: Uuid,
    pub(crate) write_limit: u64,
    pub(crate) cursor: ManifestCursor,
}

impl RawCandidate {
    /// Parse bounded, structurally valid hints. This is not authentication:
    /// callers must construct an independent binding and call `decode`.
    pub(crate) fn replay_hints(&self) -> Result<ReplayHints, Error> {
        let raw: RawResetManifest = serde_json::from_slice(&self.bytes).map_err(|_| {
            if self.bytes.len() > MAX_SHARED_MANIFEST_BYTES {
                Error::TooLarge
            } else {
                Error::Encoding
            }
        })?;
        if raw.version != 3 && self.bytes.len() > MAX_SHARED_MANIFEST_BYTES {
            return Err(Error::TooLarge);
        }
        let manifest = ResetManifest::from(raw);
        let structural_binding = ExpectedBinding {
            conversation_id: manifest.conversation_id,
            predecessor_observation: manifest.predecessor_observation,
            predecessor_root_version: manifest.predecessor_root_version,
            anchor_observation: manifest.anchor_observation,
            document_revision: manifest.document_revision,
            predecessor_context_generation: manifest.predecessor_context_generation,
            predecessor_cursor: manifest.predecessor_cursor.clone(),
            anchor_cursor: manifest.anchor_cursor.clone(),
            cuts: manifest.cuts.clone(),
        };
        manifest.validate(&structural_binding)?;
        Ok(ReplayHints {
            version: manifest.version,
            predecessor_observation: manifest.predecessor_observation,
            predecessor_root_version: manifest.predecessor_root_version,
            cuts: manifest
                .cuts
                .into_iter()
                .map(|cut| ReplayCutHint {
                    reset_id: cut.reset_id,
                    context_generation: cut.context_generation,
                    write_limit: cut.write_limit,
                    cursor: cut.cursor,
                })
                .collect(),
        })
    }
}

impl ResetManifest {
    pub(crate) fn encode(&self, expected: &ExpectedBinding) -> Result<Vec<u8>, Error> {
        self.validate(expected)?;
        let bytes =
            serde_json::to_vec(&RawResetManifest::from(self)).map_err(|_| Error::Encoding)?;
        if self.version != 3
            && bytes.len()
                > if self.version == 1 {
                    MAX_MANIFEST_BYTES
                } else {
                    MAX_SHARED_MANIFEST_BYTES
                }
        {
            return Err(Error::TooLarge);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], expected: &ExpectedBinding) -> Result<Self, Error> {
        let raw: RawResetManifest = serde_json::from_slice(bytes).map_err(|_| {
            if bytes.len() > MAX_SHARED_MANIFEST_BYTES {
                Error::TooLarge
            } else {
                Error::Encoding
            }
        })?;
        if raw.version != 3 && bytes.len() > MAX_SHARED_MANIFEST_BYTES {
            return Err(Error::TooLarge);
        }
        let manifest = Self::from(raw);
        if manifest.version == 1 && bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::TooLarge);
        }
        manifest.validate(expected)?;
        Ok(manifest)
    }

    fn validate(&self, expected: &ExpectedBinding) -> Result<(), Error> {
        if self.conversation_id != expected.conversation_id
            || self.predecessor_observation != expected.predecessor_observation
            || self.predecessor_root_version != expected.predecessor_root_version
            || self.anchor_observation != expected.anchor_observation
            || self.document_revision != expected.document_revision
            || self.predecessor_context_generation != expected.predecessor_context_generation
            || self.predecessor_cursor != expected.predecessor_cursor
            || self.anchor_cursor != expected.anchor_cursor
            || self.cuts != expected.cuts
        {
            return Err(Error::Binding);
        }
        if !(self.version == FORMAT_VERSION || self.version == 2 || self.version == 3)
            || self.conversation_id.is_nil()
            || self.predecessor_observation == 0
            || self.anchor_observation <= self.predecessor_observation
            || self.predecessor_root_version == 0
            || self.predecessor_root_version > i64::MAX as u64
            || self.document_revision > i64::MAX as u64
            || self
                .predecessor_context_generation
                .is_some_and(|id| id.is_nil())
            || self.predecessor_cursor.transcript != self.conversation_id
            || self.anchor_cursor.transcript != self.conversation_id
            || self.cuts.is_empty()
            || self.version != 3
                && self.cuts.len()
                    > if self.version == 1 {
                        MAX_CUTS
                    } else {
                        MAX_SHARED_CUTS
                    }
        {
            return Err(Error::Invalid);
        }
        let mut sequence = match &self.predecessor_cursor.next {
            Some(link) if !link.id.is_nil() && link.sequence > 0 => link.sequence,
            Some(_) => return Err(Error::Invalid),
            None => 0,
        };
        let mut entry_ids = HashSet::new();
        if let Some(link) = &self.predecessor_cursor.next {
            entry_ids.insert(link.id);
        }
        let mut reset_ids = HashSet::new();
        let mut generations = HashSet::new();
        if let Some(id) = self.predecessor_context_generation {
            generations.insert(id);
        }
        for cut in &self.cuts {
            let link = cut.cursor.next.as_ref().ok_or(Error::Invalid)?;
            sequence = sequence.checked_add(1).ok_or(Error::Invalid)?;
            if cut.cursor.transcript != self.conversation_id
                || link.id.is_nil()
                || link.sequence != sequence
                || !entry_ids.insert(link.id)
                || cut.reset_id.is_nil()
                || !reset_ids.insert(cut.reset_id)
                || cut.context_generation.is_nil()
                || !generations.insert(cut.context_generation)
                || (cut.write_limit != u64::MAX && cut.write_limit > MAX_FINITE_WRITE_LIMIT)
                || cut.state_sha256.len() != 64
                || !cut
                    .state_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(Error::Invalid);
            }
        }
        let anchor = self.anchor_cursor.next.as_ref().ok_or(Error::Invalid)?;
        if anchor.id.is_nil()
            || anchor.sequence < sequence
            || anchor.sequence - sequence > MAX_TRAILING_ENTRIES
        {
            return Err(Error::Invalid);
        }
        let last = self
            .cuts
            .last()
            .ok_or(Error::Invalid)?
            .cursor
            .next
            .as_ref()
            .ok_or(Error::Invalid)?;
        if (anchor.sequence == sequence && anchor != last)
            || (anchor.sequence > sequence && entry_ids.contains(&anchor.id))
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LookupFixture {
        dir: tempfile::TempDir,
        store: durable_store::Store,
        id: Uuid,
        cursor: crate::transcript::Cursor,
        final_cursor: crate::transcript::Cursor,
        root: Vec<u8>,
        anchor: Vec<u8>,
        event: Vec<u8>,
        manifest: Vec<u8>,
        revision: u64,
    }

    fn lookup_fixture(include_root: bool) -> LookupFixture {
        use durable_store::Mutation;
        let dir = tempfile::tempdir().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let id = Uuid::new_v4();
        let transcript = crate::transcript::Transcript::new(&store, id);
        let create = transcript.prepare_create().unwrap();
        store.commit(&create.checks, &create.mutations).unwrap();
        let (batch, positions) = transcript
            .prepare_append_batch(&[
                (serde_json::json!({"cut": 1}), None),
                (serde_json::json!({"cut": 2}), None),
            ])
            .unwrap();
        let cursor = positions[0].clone();
        let (entry, sequence) = cursor.entry_identity().unwrap();
        let mut manifest = candidate_prefix(id, entry, sequence);
        manifest.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
        let root = format!("runtime/conversation/{id}").into_bytes();
        let anchor = format!("runtime/fork/{id}/{:020}", 1).into_bytes();
        let mut event = b"durable/observer/".to_vec();
        event.extend_from_slice(id.as_bytes());
        event.extend_from_slice(b"/r");
        event.extend_from_slice(&1u64.to_be_bytes());
        let transcript = serde_json::to_value(&positions[1]).unwrap();
        let revision = store.document_snapshot().unwrap();
        let mut mutations = batch.mutations;
        if include_root {
            mutations.push(Mutation::Put {
                key: root.clone(),
                value: serde_json::to_vec(&serde_json::json!({"id": id})).unwrap(),
            });
        }
        mutations.extend([
            Mutation::Put {
                key: anchor.clone(),
                value: serde_json::to_vec(
                    &serde_json::json!({"version":1,"conversation_id":id,"_transcript":transcript}),
                )
                .unwrap(),
            },
            Mutation::Put {
                key: event.clone(),
                value: serde_json::to_vec(&serde_json::json!({"_transcript":transcript})).unwrap(),
            },
            // Deliberately not JSON: lookup must return bytes without decoding.
            Mutation::Put {
                key: manifest.clone(),
                value: b"raw candidate".to_vec(),
            },
        ]);
        let mut checks = batch.checks;
        checks.push(durable_store::Check {
            key: b"durable/document-fork-revision".to_vec(),
            expected: durable_store::Expected::DocumentRevision(revision),
        });
        store.commit(&checks, &mutations).unwrap();
        LookupFixture {
            dir,
            store,
            id,
            cursor,
            final_cursor: positions[1].clone(),
            root,
            anchor,
            event,
            manifest,
            revision,
        }
    }

    #[test]
    fn lookup_returns_raw_candidate_for_atomic_evidence() {
        let f = lookup_fixture(true);
        let found = lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision)
            .unwrap()
            .unwrap();
        assert_eq!(found.bytes, b"raw candidate");
        f.store
            .commit(
                &[],
                &[
                    durable_store::Mutation::Put {
                        key: f.root.clone(),
                        value: serde_json::to_vec(&serde_json::json!({"id": f.id, "later": true}))
                            .unwrap(),
                    },
                    durable_store::Mutation::Put {
                        key: b"durable/document/later".to_vec(),
                        value: b"{}".to_vec(),
                    },
                ],
            )
            .unwrap();
        let reopened = durable_store::Store::open(f.dir.path().join("db")).unwrap();
        assert_eq!(
            lookup_candidate(&reopened, f.id, &f.cursor, 1, f.revision)
                .unwrap()
                .unwrap()
                .bytes,
            b"raw candidate"
        );
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision + 1).is_err());
        assert!(lookup_candidate(&f.store, Uuid::new_v4(), &f.cursor, 1, f.revision).is_err());
    }

    #[test]
    fn returned_fork_checks_fence_rewrites_and_new_candidate_aba() {
        use durable_store::Mutation;
        let valid = lookup_fixture(true);
        let found = lookup_candidate(&valid.store, valid.id, &valid.cursor, 1, valid.revision)
            .unwrap()
            .unwrap();
        valid
            .store
            .commit(
                &found.checks,
                &[Mutation::Put {
                    key: b"fixture/fork".to_vec(),
                    value: vec![1],
                }],
            )
            .unwrap();
        for tamper in ["anchor", "candidate", "additional"] {
            let f = lookup_fixture(true);
            let found = lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision)
                .unwrap()
                .unwrap();
            let key = match tamper {
                "anchor" => f.anchor.clone(),
                "candidate" => f.manifest.clone(),
                _ => {
                    let (id, sequence) = f.cursor.entry_identity().unwrap();
                    let mut key = candidate_prefix(f.id, id, sequence);
                    key.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
                    key
                }
            };
            let value = f
                .store
                .get(&key)
                .unwrap()
                .map_or_else(|| b"other".to_vec(), |v| v.value);
            f.store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: key.clone(),
                        value,
                    }],
                )
                .unwrap();
            if tamper == "additional" {
                f.store.commit(&[], &[Mutation::Delete { key }]).unwrap();
            }
            let fork = b"fixture/fork".to_vec();
            assert!(matches!(
                f.store.commit(
                    &found.checks,
                    &[Mutation::Put {
                        key: fork.clone(),
                        value: vec![1]
                    }]
                ),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(f.store.get(&fork).unwrap().is_none());
        }
    }

    #[test]
    fn lookup_rejects_forged_and_missing_root_evidence() {
        use durable_store::Mutation;
        let f = lookup_fixture(false);
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
        for field in ["manifest", "anchor", "event", "root", "candidate"] {
            let f = lookup_fixture(true);
            let (candidate, _) = crate::transcript::Transcript::new(&f.store, f.id)
                .entry_evidence(&f.cursor)
                .unwrap();
            let key = match field {
                "manifest" => &f.manifest,
                "anchor" => &f.anchor,
                "event" => &f.event,
                "root" => &f.root,
                _ => &candidate,
            };
            if field == "root" {
                f.store
                    .commit(&[], &[Mutation::Delete { key: key.clone() }])
                    .unwrap();
            } else {
                let value = f.store.get(key).unwrap().unwrap().value;
                f.store
                    .commit(
                        &[],
                        &[Mutation::Put {
                            key: key.clone(),
                            value,
                        }],
                    )
                    .unwrap();
            }
            assert!(
                lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err(),
                "{field}"
            );
        }
        let f = lookup_fixture(true);
        f.store
            .commit(
                &[],
                &[Mutation::Put {
                    key: f.root.clone(),
                    value: serde_json::to_vec(&serde_json::json!({"id": Uuid::new_v4()})).unwrap(),
                }],
            )
            .unwrap();
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
    }

    #[test]
    fn lookup_rejects_ambiguous_candidates_and_detects_absence() {
        use durable_store::Mutation;
        let f = lookup_fixture(true);
        let (entry, sequence) = f.cursor.entry_identity().unwrap();
        let mut second = candidate_prefix(f.id, entry, sequence);
        second.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
        f.store
            .commit(
                &[],
                &[Mutation::Put {
                    key: second,
                    value: vec![],
                }],
            )
            .unwrap();
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
        let f = lookup_fixture(true);
        f.store
            .commit(
                &[],
                &[Mutation::Delete {
                    key: f.manifest.clone(),
                }],
            )
            .unwrap();
        assert!(
            lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn lookup_rejects_rewritten_anchor_with_stale_key_only_document_cut() {
        use durable_store::Mutation;
        let f = lookup_fixture(true);
        f.store
            .commit(
                &[],
                &[Mutation::Put {
                    key: b"durable/document/change".to_vec(),
                    value: b"{}".to_vec(),
                }],
            )
            .unwrap();
        let (entry, _) = crate::transcript::Transcript::new(&f.store, f.id)
            .entry_evidence(&f.cursor)
            .unwrap();
        let (final_entry, _) = crate::transcript::Transcript::new(&f.store, f.id)
            .entry_evidence(&f.final_cursor)
            .unwrap();
        let mut mutations = Vec::new();
        for key in [
            &f.root,
            &f.anchor,
            &f.event,
            &f.manifest,
            &entry,
            &final_entry,
        ] {
            mutations.push(Mutation::Put {
                key: key.clone(),
                value: f.store.get(key).unwrap().unwrap().value,
            });
        }
        let current = f.store.document_snapshot().unwrap();
        f.store
            .commit(
                &[durable_store::Check {
                    key: b"durable/document-fork-revision".to_vec(),
                    expected: durable_store::Expected::DocumentRevision(current),
                }],
                &mutations,
            )
            .unwrap();
        assert_eq!(f.store.document_cut(&f.anchor).unwrap(), Some(f.revision));
        let version = f.store.get(&f.anchor).unwrap().unwrap().version;
        assert_ne!(
            f.store
                .document_cut_for_version(&f.anchor, version)
                .unwrap(),
            Some(f.revision)
        );
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, current).is_ok());
    }

    #[test]
    fn lookup_rejects_atomic_evidence_without_document_fence() {
        use durable_store::Mutation;
        let f = lookup_fixture(true);
        let transcript = crate::transcript::Transcript::new(&f.store, f.id);
        let (entry, _) = transcript.entry_evidence(&f.cursor).unwrap();
        let (final_entry, _) = transcript.entry_evidence(&f.final_cursor).unwrap();
        let mutations: Vec<_> = [
            &f.root,
            &f.anchor,
            &f.event,
            &f.manifest,
            &entry,
            &final_entry,
        ]
        .iter()
        .map(|key| Mutation::Put {
            key: (*key).clone(),
            value: f.store.get(key).unwrap().unwrap().value,
        })
        .collect();
        f.store.commit(&[], &mutations).unwrap();
        let version = f.store.get(&f.anchor).unwrap().unwrap().version;
        assert_eq!(
            f.store
                .document_cut_for_version(&f.anchor, version)
                .unwrap(),
            Some(f.revision)
        );
        assert_eq!(
            f.store
                .document_fence_for_version(&f.anchor, version)
                .unwrap(),
            None
        );
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
    }

    #[test]
    fn lookup_accepts_document_write_fenced_in_same_commit() {
        use durable_store::{Check, Expected, Mutation};
        let f = lookup_fixture(true);
        let transcript = crate::transcript::Transcript::new(&f.store, f.id);
        let (entry, _) = transcript.entry_evidence(&f.cursor).unwrap();
        let (last, _) = transcript.entry_evidence(&f.final_cursor).unwrap();
        let mut mutations: Vec<_> = [&f.root, &f.anchor, &f.event, &f.manifest, &entry, &last]
            .iter()
            .map(|key| Mutation::Put {
                key: (*key).clone(),
                value: f.store.get(key).unwrap().unwrap().value,
            })
            .collect();
        mutations.push(Mutation::Put {
            key: b"durable/document/same-commit".to_vec(),
            value: b"{}".to_vec(),
        });
        f.store
            .commit(
                &[Check {
                    key: b"durable/document-fork-revision".to_vec(),
                    expected: Expected::DocumentRevision(f.revision),
                }],
                &mutations,
            )
            .unwrap();
        let version = f.store.get(&f.anchor).unwrap().unwrap().version;
        assert_eq!(
            f.store
                .document_fence_for_version(&f.anchor, version)
                .unwrap(),
            Some(f.revision)
        );
        assert_eq!(
            f.store
                .document_cut_for_version(&f.anchor, version)
                .unwrap(),
            Some(f.revision + 1)
        );
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision + 1).is_ok());
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
    }

    #[test]
    fn chunked_lookup_fences_all_chunks_and_rejects_rewrites() {
        use durable_store::{Check, Expected, Mutation};
        let f = lookup_fixture(true);
        let transcript = crate::transcript::Transcript::new(&f.store, f.id);
        let (entry, _) = transcript.entry_evidence(&f.cursor).unwrap();
        let (last, _) = transcript.entry_evidence(&f.final_cursor).unwrap();
        let batch = Uuid::new_v4();
        let mut mutations: Vec<_> = [&f.root, &f.anchor, &f.event, &entry, &last]
            .iter()
            .map(|key| Mutation::Put {
                key: (*key).clone(),
                value: f.store.get(key).unwrap().unwrap().value,
            })
            .collect();
        mutations.push(Mutation::Put {
            key: f.manifest.clone(),
            value: serde_json::to_vec(&SharedReference { version: 3, batch }).unwrap(),
        });
        let count = MAX_SHARED_MANIFEST_BYTES / MANIFEST_CHUNK_BYTES + 1;
        mutations.push(Mutation::Put {
            key: shared_key(f.id, batch),
            value: serde_json::to_vec(&ChunkHeader {
                version: 3,
                chunks: count,
            })
            .unwrap(),
        });
        let first = chunk_key(f.id, batch, 0);
        let second = chunk_key(f.id, batch, count - 1);
        for index in 0..count - 1 {
            mutations.push(Mutation::Put {
                key: chunk_key(f.id, batch, index),
                value: vec![b'a'; MANIFEST_CHUNK_BYTES],
            });
        }
        mutations.push(Mutation::Put {
            key: second.clone(),
            value: vec![b'b'],
        });
        f.store
            .commit(
                &[Check {
                    key: b"durable/document-fork-revision".to_vec(),
                    expected: Expected::DocumentRevision(f.revision),
                }],
                &mutations,
            )
            .unwrap();
        let candidate = lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision)
            .unwrap()
            .unwrap();
        assert_eq!(candidate.bytes.len(), MAX_SHARED_MANIFEST_BYTES + 1);
        assert!(candidate.checks.iter().any(|check| check.key == first));
        assert!(candidate.checks.iter().any(|check| check.key == second));
        f.store
            .commit(
                &[],
                &[Mutation::Put {
                    key: second,
                    value: vec![b'c'],
                }],
            )
            .unwrap();
        assert!(f.store.commit(&candidate.checks, &[]).is_err());
        assert!(lookup_candidate(&f.store, f.id, &f.cursor, 1, f.revision).is_err());
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn cursor(conversation: Uuid, entry: u128, sequence: u64) -> ManifestCursor {
        ManifestCursor {
            transcript: conversation,
            next: Some(ManifestLink {
                id: id(entry),
                sequence,
            }),
        }
    }

    fn fixture() -> (ResetManifest, ExpectedBinding) {
        let conversation = id(1);
        let prior = cursor(conversation, 2, 7);
        let anchor = cursor(conversation, 5, 10);
        let cuts = vec![
            ResetCut {
                reset_id: id(10),
                context_generation: id(20),
                write_limit: u64::MAX,
                cursor: cursor(conversation, 3, 8),
                state_sha256: "a".repeat(64),
            },
            ResetCut {
                reset_id: id(11),
                context_generation: id(21),
                write_limit: 4,
                cursor: cursor(conversation, 4, 9),
                state_sha256: "b".repeat(64),
            },
        ];
        let manifest = ResetManifest {
            version: FORMAT_VERSION,
            conversation_id: conversation,
            predecessor_observation: 12,
            predecessor_root_version: 31,
            anchor_observation: 13,
            document_revision: 9,
            predecessor_context_generation: Some(id(19)),
            predecessor_cursor: prior.clone(),
            anchor_cursor: anchor.clone(),
            cuts: cuts.clone(),
        };
        let binding = ExpectedBinding {
            conversation_id: conversation,
            predecessor_observation: 12,
            predecessor_root_version: 31,
            anchor_observation: 13,
            document_revision: 9,
            predecessor_context_generation: Some(id(19)),
            predecessor_cursor: prior,
            anchor_cursor: anchor,
            cuts,
        };
        (manifest, binding)
    }

    #[test]
    fn version_three_accepts_more_than_legacy_cut_limit() {
        let (mut manifest, mut binding) = fixture();
        manifest.version = 3;
        manifest.cuts = (0..MAX_SHARED_CUTS + 1)
            .map(|index| ResetCut {
                reset_id: id(100_000 + index as u128),
                context_generation: id(200_000 + index as u128),
                write_limit: u64::MAX,
                cursor: cursor(
                    manifest.conversation_id,
                    300_000 + index as u128,
                    8 + index as u64,
                ),
                state_sha256: "a".repeat(64),
            })
            .collect();
        manifest.anchor_cursor = manifest.cuts.last().unwrap().cursor.clone();
        binding.anchor_cursor = manifest.anchor_cursor.clone();
        binding.cuts = manifest.cuts.clone();
        let encoded = manifest.encode(&binding).unwrap();
        assert_eq!(ResetManifest::decode(&encoded, &binding).unwrap(), manifest);
    }

    fn reject_structural_change(mut change: impl FnMut(&mut ResetManifest)) {
        let (mut manifest, mut binding) = fixture();
        change(&mut manifest);
        // Keep the independently supplied binding consistent so this exercises
        // structural checks instead of failing at the equality boundary.
        binding.conversation_id = manifest.conversation_id;
        binding.predecessor_observation = manifest.predecessor_observation;
        binding.predecessor_root_version = manifest.predecessor_root_version;
        binding.anchor_observation = manifest.anchor_observation;
        binding.document_revision = manifest.document_revision;
        binding.predecessor_context_generation = manifest.predecessor_context_generation;
        binding.predecessor_cursor = manifest.predecessor_cursor.clone();
        binding.anchor_cursor = manifest.anchor_cursor.clone();
        binding.cuts = manifest.cuts.clone();
        assert!(matches!(manifest.encode(&binding), Err(Error::Invalid)));
    }

    #[test]
    fn replay_hints_are_bounded_structural_and_do_not_authorize_a_binding() {
        let (manifest, binding) = fixture();
        let bytes = manifest.encode(&binding).unwrap();
        let candidate = RawCandidate {
            bytes: bytes.clone(),
            shared_reference: None,
            checks: vec![],
        };
        let hints = candidate.replay_hints().unwrap();
        assert_eq!(
            hints.predecessor_observation,
            binding.predecessor_observation
        );
        assert_eq!(hints.cuts.len(), binding.cuts.len());
        assert_eq!(hints.cuts[0].cursor, binding.cuts[0].cursor);
        let mut wrong = binding.clone();
        wrong.document_revision += 1;
        assert!(matches!(
            ResetManifest::decode(&bytes, &wrong),
            Err(Error::Binding)
        ));

        let mut malformed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        malformed["cuts"][1]["reset_id"] = malformed["cuts"][0]["reset_id"].clone();
        let candidate = RawCandidate {
            bytes: serde_json::to_vec(&malformed).unwrap(),
            shared_reference: None,
            checks: vec![],
        };
        assert!(matches!(candidate.replay_hints(), Err(Error::Invalid)));
        let candidate = RawCandidate {
            bytes: vec![b' '; MAX_SHARED_MANIFEST_BYTES + 1],
            shared_reference: None,
            checks: vec![],
        };
        assert!(matches!(candidate.replay_hints(), Err(Error::TooLarge)));
    }

    #[test]
    fn round_trip_binds_all_independent_expectations() {
        let (manifest, binding) = fixture();
        let bytes = manifest.encode(&binding).unwrap();
        assert_eq!(ResetManifest::decode(&bytes, &binding).unwrap(), manifest);
        let stored_cursor: crate::transcript::Cursor =
            serde_json::from_value(serde_json::to_value(&manifest.cuts[0].cursor).unwrap())
                .unwrap();
        assert_eq!(
            serde_json::to_value(stored_cursor).unwrap(),
            serde_json::to_value(&manifest.cuts[0].cursor).unwrap()
        );
        for field in 0..9 {
            let mut changed = binding.clone();
            match field {
                0 => changed.conversation_id = id(99),
                1 => changed.predecessor_observation += 1,
                2 => changed.predecessor_root_version += 1,
                3 => changed.anchor_observation += 1,
                4 => changed.document_revision += 1,
                5 => changed.predecessor_cursor.next.as_mut().unwrap().id = id(99),
                6 => changed.anchor_cursor.next.as_mut().unwrap().id = id(99),
                7 => changed.predecessor_context_generation = Some(id(99)),
                _ => changed.cuts[0].state_sha256 = "c".repeat(64),
            }
            assert!(matches!(
                ResetManifest::decode(&bytes, &changed),
                Err(Error::Binding)
            ));
        }
    }

    #[test]
    fn accepts_empty_predecessor_and_anchor_at_last_cut() {
        let (mut manifest, mut binding) = fixture();
        manifest.predecessor_cursor.next = None;
        manifest.cuts[0].cursor.next.as_mut().unwrap().sequence = 1;
        manifest.cuts[1].cursor.next.as_mut().unwrap().sequence = 2;
        manifest.anchor_cursor = manifest.cuts[1].cursor.clone();
        binding.predecessor_cursor = manifest.predecessor_cursor.clone();
        binding.anchor_cursor = manifest.anchor_cursor.clone();
        binding.cuts = manifest.cuts.clone();
        let bytes = manifest.encode(&binding).unwrap();
        assert_eq!(ResetManifest::decode(&bytes, &binding).unwrap(), manifest);
    }

    #[test]
    fn rejects_reordered_duplicate_skipped_and_substituted_cursors() {
        reject_structural_change(|m| m.cuts.swap(0, 1));
        reject_structural_change(|m| m.cuts[1].cursor = m.cuts[0].cursor.clone());
        reject_structural_change(|m| m.cuts[0].cursor.next.as_mut().unwrap().sequence += 1);
        reject_structural_change(|m| m.cuts[0].cursor.next.as_mut().unwrap().id = id(2));
        reject_structural_change(|m| m.cuts[0].cursor.transcript = id(99));
        reject_structural_change(|m| m.anchor_cursor.next.as_mut().unwrap().id = id(3));
        reject_structural_change(|m| m.anchor_cursor.next.as_mut().unwrap().sequence = 8);
        reject_structural_change(|m| {
            m.predecessor_cursor.next.as_mut().unwrap().sequence = u64::MAX
        });
        reject_structural_change(|m| {
            m.anchor_cursor.next.as_mut().unwrap().sequence = 9 + MAX_TRAILING_ENTRIES + 1
        });
        let (manifest, binding) = fixture();
        let mut substituted = manifest.clone();
        substituted.cuts[0].cursor.next.as_mut().unwrap().id = id(99);
        assert!(matches!(substituted.encode(&binding), Err(Error::Binding)));
    }

    #[test]
    fn rejects_invalid_context_reset_digest_and_bounds() {
        reject_structural_change(|m| m.cuts[1].reset_id = m.cuts[0].reset_id);
        reject_structural_change(|m| m.cuts[1].context_generation = m.cuts[0].context_generation);
        reject_structural_change(|m| {
            m.cuts[0].context_generation = m.predecessor_context_generation.unwrap()
        });
        reject_structural_change(|m| m.cuts[0].write_limit = MAX_FINITE_WRITE_LIMIT + 1);
        reject_structural_change(|m| m.cuts[0].state_sha256 = "A".repeat(64));
        reject_structural_change(|m| m.cuts[0].state_sha256 = "a".repeat(63));
        reject_structural_change(|m| m.cuts.clear());
        reject_structural_change(|m| m.cuts = vec![m.cuts[0].clone(); MAX_CUTS + 1]);
        reject_structural_change(|m| m.version = 4);
        reject_structural_change(|m| m.anchor_observation = m.predecessor_observation);
        reject_structural_change(|m| m.predecessor_root_version = 0);
        reject_structural_change(|m| m.document_revision = i64::MAX as u64 + 1);
        let (_, binding) = fixture();
        assert!(matches!(
            ResetManifest::decode(&vec![b' '; MAX_SHARED_MANIFEST_BYTES + 1], &binding),
            Err(Error::TooLarge)
        ));
    }

    #[test]
    fn rejects_unknown_nested_fields_duplicate_fields_and_malformed_json() {
        let (manifest, binding) = fixture();
        let mut value: serde_json::Value =
            serde_json::from_slice(&manifest.encode(&binding).unwrap()).unwrap();
        value["cuts"][0]["cursor"]["unexpected"] = serde_json::json!(true);
        assert!(matches!(
            ResetManifest::decode(&serde_json::to_vec(&value).unwrap(), &binding),
            Err(Error::Encoding)
        ));
        let bytes = manifest.encode(&binding).unwrap();
        let mut duplicate = String::from_utf8(bytes).unwrap();
        duplicate.insert_str(1, "\"version\":1,");
        assert!(matches!(
            ResetManifest::decode(duplicate.as_bytes(), &binding),
            Err(Error::Encoding)
        ));
        assert!(matches!(
            ResetManifest::decode(b"{", &binding),
            Err(Error::Encoding)
        ));
        let mut missing: serde_json::Value =
            serde_json::from_slice(&manifest.encode(&binding).unwrap()).unwrap();
        missing["predecessor_cursor"]
            .as_object_mut()
            .unwrap()
            .remove("next");
        assert!(matches!(
            ResetManifest::decode(&serde_json::to_vec(&missing).unwrap(), &binding),
            Err(Error::Encoding)
        ));
        let mut missing: serde_json::Value =
            serde_json::from_slice(&manifest.encode(&binding).unwrap()).unwrap();
        missing
            .as_object_mut()
            .unwrap()
            .remove("predecessor_context_generation");
        assert!(matches!(
            ResetManifest::decode(&serde_json::to_vec(&missing).unwrap(), &binding),
            Err(Error::Encoding)
        ));
    }
}
