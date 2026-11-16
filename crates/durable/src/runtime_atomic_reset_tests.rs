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
                state_sha256: reset_cut_digest(
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

#[test]
fn post_failure_candidate_places_resets_and_replacement_in_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let failed = h.submit(id, None, "active".into()).unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
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
    let steps: Vec<_> = [first, second]
        .into_iter()
        .map(|reset_id| ResetDerivationStep {
            reset_id,
            context_generation: Uuid::new_v4(),
            write_limit: usize::MAX,
        })
        .collect();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    let mut failure_base = failure.clone();
    apply_failed_run_policy(&mut failure_base);
    let expected_cuts = derive_ordered_reset_states(&failure_base, &steps).unwrap();
    let batch = h
        .prepare_atomic_failure_reset_save(&failure, version, &steps)
        .unwrap();
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
    let manifests: Vec<_> = batch
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                Some((key.clone(), value.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(entries.len(), 3);
    assert_eq!(manifests.len(), 2);
    assert!(
        entries
            .iter()
            .all(|key| h.owner.store().get(key).unwrap().is_none())
    );
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Queued
    );
    h.commit_conversation_save(batch).unwrap();
    let actual = h.conversation(id).unwrap();
    assert_eq!(
        actual.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(
        actual
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    assert_eq!(actual.run_failure.as_ref().unwrap().inputs, vec![failed]);
    assert!(matches!(actual.phase, Phase::ModelReady { .. }));
    assert!(
        matches!(actual.messages.last(), Some(Message::User { content }) if content == "replacement")
    );
    let manifest: serde_json::Value = serde_json::from_slice(&manifests[0].1).unwrap();
    for (saved, expected) in manifest["cuts"]
        .as_array()
        .unwrap()
        .iter()
        .zip(&expected_cuts)
    {
        assert_eq!(
            saved["state_sha256"],
            reset_cut_digest(
                &serde_json::json!({"state":expected,"blocks":expected.content_blocks})
            )
            .unwrap()
        );
        assert_eq!(expected.run_failure.as_ref().unwrap().inputs, vec![failed]);
        assert_eq!(
            expected
                .submissions
                .iter()
                .find(|s| s.id == replacement)
                .unwrap()
                .status,
            Status::Queued
        );
    }
    let cursors: Vec<crate::transcript::Cursor> = manifest["cuts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|cut| serde_json::from_value(cut["cursor"].clone()).unwrap())
        .collect();
    let anchor: crate::transcript::Cursor =
        serde_json::from_value(manifest["anchor_cursor"].clone()).unwrap();
    assert_ne!(anchor, *cursors.last().unwrap());
    let observation = manifest["anchor_observation"].as_u64().unwrap();
    let revision = manifest["document_revision"].as_u64().unwrap();
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
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let actual = h.conversation(id).unwrap();
    assert_eq!(
        actual
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    assert_eq!(actual.run_failure.unwrap().inputs, vec![failed]);
    assert!(
        matches!(actual.messages.last(), Some(Message::User { content }) if content == "replacement")
    );
}

#[test]
fn post_failure_writer_rolls_back_on_document_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    let batch = h
        .prepare_atomic_failure_reset_save(&failure, version, &steps)
        .unwrap();
    let unpublished: Vec<_> = batch
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, .. }
                if key.starts_with(b"durable/transcript/entry/")
                    || key.starts_with(b"runtime/reset-manifest/") =>
            {
                Some(key.clone())
            }
            _ => None,
        })
        .collect();
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
    assert!(matches!(
        h.commit_conversation_save(batch),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(
        unpublished
            .iter()
            .all(|key| h.owner.store().get(key).unwrap().is_none())
    );
    let current = h.conversation(id).unwrap();
    assert!(current.resets.is_empty());
    assert_eq!(
        current
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Queued
    );
    assert!(current.run_failure.is_none());
}

#[test]
fn stopped_model_failure_reset_cut_proof_reopens_and_fences_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue).unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = h.load(id).unwrap();
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&stopped, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    let batch = h.prepare_atomic_failure_reset_save(&failure, version, &steps).unwrap();
    let bytes = batch.mutations.iter().find_map(|mutation| match mutation {
        Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => Some(value.clone()),
        _ => None,
    }).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let cursor: crate::transcript::Cursor = serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
    let observation = manifest["anchor_observation"].as_u64().unwrap();
    h.commit_conversation_save(batch).unwrap();
    let (cut, _, _) = h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
    assert_eq!(cut.resets.iter().map(|r| r.id).collect::<Vec<_>>(), vec![steps[0].reset_id]);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let (_, _, checks) = h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
    let predecessor = checks.iter().find(|check| check.key.starts_with(format!("runtime/fork/{id}/").as_bytes()) && check.key != format!("runtime/fork/{id}/{observation:020}").as_bytes()).unwrap();
    let value = h.owner.store().get(&predecessor.key).unwrap().unwrap().value;
    h.owner.store().commit(&[], &[Mutation::Put { key: predecessor.key.clone(), value }]).unwrap();
    assert!(h.owner.store().commit(&checks, &[]).is_err());
    assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
}
