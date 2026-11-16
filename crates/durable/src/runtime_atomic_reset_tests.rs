use super::*;
use futures_util::{StreamExt, stream};
use pi_rs_core::{ModelEvent, ModelRequest, ModelStream};

struct Stub;
#[async_trait::async_trait]
impl Model for Stub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Stub),
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn setup(path: &std::path::Path) -> (Harness, Uuid, u64, Vec<ResetDerivationStep>) {
    let mut h = Harness::open_paused(path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let (first, _) = h
        .reset_with_blocks(
            id,
            Some("first".into()),
            vec![vec![serde_json::json!({"type":"text","text":"a"})]],
        )
        .unwrap();
    let (second, _) = h
        .reset_with_blocks(
            id,
            Some("second".into()),
            vec![vec![serde_json::json!({"type":"text","text":"b"})]],
        )
        .unwrap();
    let (_, version) = h.load(id).unwrap();
    let steps = vec![first, second]
        .into_iter()
        .map(|reset_id| ResetDerivationStep {
            reset_id,
            context_generation: Uuid::new_v4(),
            write_limit: usize::MAX,
        })
        .collect();
    (h, id, version, steps)
}

#[test]
fn atomic_writer_publishes_cuts_and_manifest_together_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, version, steps) = setup(&path);
    let predecessor = h.load(id).unwrap().0;
    let predecessor_observation = crate::observer::Observer::new(h.owner.store(), id)
        .head()
        .unwrap()
        .0;
    let expected_revision = h.owner.store().document_snapshot().unwrap();
    let derived = h.derive_anchored_reset_states(id, version, &steps).unwrap();
    let first_id = derived[0]
        .content_blocks
        .values()
        .next()
        .unwrap()
        .message_id;
    let batch = h.prepare_atomic_reset_save(id, version, &steps).unwrap();
    let candidates: Vec<_> = batch
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                Some((key.clone(), value.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(candidates.len(), 2);
    let entries: Vec<_> = batch
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, .. } if key.starts_with(b"durable/transcript/entry/") => {
                Some(key.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(entries.len(), 2);
    for key in &entries {
        assert!(h.owner.store().get(key).unwrap().is_none());
    }
    for (key, _) in &candidates {
        assert!(h.owner.store().get(key).unwrap().is_none());
    }
    assert!(h.conversation(id).unwrap().resets.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&candidates[0].1).unwrap();
    let cursors: Vec<crate::transcript::Cursor> = value["cuts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|cut| serde_json::from_value(cut["cursor"].clone()).unwrap())
        .collect();
    let observation = value["anchor_observation"].as_u64().unwrap();
    let revision = value["document_revision"].as_u64().unwrap();
    let as_manifest_cursor = |cursor: &crate::transcript::Cursor| {
        serde_json::from_value(serde_json::to_value(cursor).unwrap()).unwrap()
    };
    let expected = crate::reset_manifest::ExpectedBinding {
        conversation_id: id,
        predecessor_observation,
        predecessor_root_version: version,
        anchor_observation: predecessor_observation + 1,
        document_revision: expected_revision,
        predecessor_context_generation: predecessor.context_generation,
        predecessor_cursor: as_manifest_cursor(predecessor.transcript.as_ref().unwrap()),
        anchor_cursor: as_manifest_cursor(cursors.last().unwrap()),
        cuts: steps
            .iter()
            .zip(&derived)
            .zip(&cursors)
            .map(|((step, cut), cursor)| crate::reset_manifest::ResetCut {
                reset_id: step.reset_id,
                context_generation: step.context_generation,
                write_limit: u64::MAX,
                cursor: as_manifest_cursor(cursor),
                state_sha256: reset_baseline_digest(
                    &serde_json::json!({"state":cut,"blocks":cut.content_blocks}),
                )
                .unwrap(),
            })
            .collect(),
    };
    crate::reset_manifest::ResetManifest::decode(&candidates[0].1, &expected).unwrap();
    h.commit_conversation_save(batch).unwrap();
    for key in &entries {
        assert!(h.owner.store().get(key).unwrap().is_some());
    }
    assert_eq!(h.conversation(id).unwrap().resets.len(), 2);
    assert!(h.fork_by_entry_id(id, first_id).is_err());
    for cursor in &cursors {
        assert_eq!(
            crate::reset_manifest::lookup_candidate(
                h.owner.store(),
                id,
                cursor,
                observation,
                revision
            )
            .unwrap()
            .unwrap()
            .bytes,
            candidates[0].1
        );
    }
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().resets.len(), 2);
    for cursor in &cursors {
        assert!(
            crate::reset_manifest::lookup_candidate(
                h.owner.store(),
                id,
                cursor,
                observation,
                revision
            )
            .unwrap()
            .is_some()
        );
    }
    let anchor = format!("runtime/fork/{id}/{observation:020}").into_bytes();
    let value = h.owner.store().get(&anchor).unwrap().unwrap().value;
    h.owner
        .store()
        .commit(&[], &[Mutation::Put { key: anchor, value }])
        .unwrap();
    assert!(
        crate::reset_manifest::lookup_candidate(
            h.owner.store(),
            id,
            &cursors[0],
            observation,
            revision
        )
        .is_err()
    );
}

#[test]
fn atomic_writer_rolls_back_for_document_race_and_tampered_anchor() {
    for tamper in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, version, steps) = setup(&path);
        let batch = h.prepare_atomic_reset_save(id, version, &steps).unwrap();
        let manifests: Vec<_> = batch
            .mutations
            .iter()
            .filter_map(|m| match m {
                Mutation::Put { key, .. } if key.starts_with(b"runtime/reset-manifest/") => {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect();
        let entries: Vec<_> = batch
            .mutations
            .iter()
            .filter_map(|m| match m {
                Mutation::Put { key, .. } if key.starts_with(b"durable/transcript/entry/") => {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(entries.len(), 2);
        if tamper {
            let key = batch
                .checks
                .iter()
                .find(|c| {
                    c.key.starts_with(format!("runtime/fork/{id}/").as_bytes())
                        && matches!(c.expected, Expected::Version(_))
                })
                .unwrap()
                .key
                .clone();
            let value = h.owner.store().get(&key).unwrap().unwrap().value;
            h.owner
                .store()
                .commit(&[], &[Mutation::Put { key, value }])
                .unwrap();
        } else {
            let mut tx = crate::document::Transaction::new();
            tx.put(
                &crate::document::Address {
                    scope: id.to_string(),
                    name: "race".into(),
                    key: "one".into(),
                },
                Expected::Missing,
                serde_json::json!({"x":1}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(h.owner.store()).unwrap();
        }
        assert!(matches!(
            h.commit_conversation_save(batch),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        for key in manifests {
            assert!(h.owner.store().get(&key).unwrap().is_none());
        }
        for key in entries {
            assert!(h.owner.store().get(&key).unwrap().is_none());
        }
        assert!(h.conversation(id).unwrap().resets.is_empty());
    }
}
