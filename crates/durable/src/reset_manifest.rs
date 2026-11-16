//! Private structural format for a future atomic reset-batch proof.
//!
//! This module does not read storage or authorize forks. Callers must obtain
//! `ExpectedBinding` from authenticated committed state, independently derive
//! each cut, and prove the stored cursor chain and snapshots in one transaction.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_CUTS: usize = 64;
const MAX_TRAILING_ENTRIES: u64 = 64;
const MAX_FINITE_WRITE_LIMIT: u64 = 1_000_000;

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

impl ResetManifest {
    pub(crate) fn encode(&self, expected: &ExpectedBinding) -> Result<Vec<u8>, Error> {
        self.validate(expected)?;
        let bytes =
            serde_json::to_vec(&RawResetManifest::from(self)).map_err(|_| Error::Encoding)?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], expected: &ExpectedBinding) -> Result<Self, Error> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::TooLarge);
        }
        let raw: RawResetManifest = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
        let manifest = Self::from(raw);
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
        if self.version != FORMAT_VERSION
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
            || self.cuts.len() > MAX_CUTS
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
        reject_structural_change(|m| m.version += 1);
        reject_structural_change(|m| m.anchor_observation = m.predecessor_observation);
        reject_structural_change(|m| m.predecessor_root_version = 0);
        reject_structural_change(|m| m.document_revision = i64::MAX as u64 + 1);
        let (_, binding) = fixture();
        assert!(matches!(
            ResetManifest::decode(&vec![b' '; MAX_MANIFEST_BYTES + 1], &binding),
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
