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
struct BlockAdapter;
#[async_trait::async_trait]
impl crate::model::ProviderAdapter for BlockAdapter {
    fn id(&self) -> &str {
        "atomic-reset-test/v1"
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &crate::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> Result<serde_json::Value, pi_rs_core::Error> {
        Ok(serde_json::json!({}))
    }
    fn validate(
        &self,
        _: &crate::model::ProviderAttempt,
        _: &ModelRequest,
        _: &serde_json::Value,
    ) -> Result<(), pi_rs_core::Error> {
        Ok(())
    }
    async fn prepare_durable(
        &self,
        _: &crate::model::ProviderAttempt,
        request: &crate::model::DurableRequest,
    ) -> Result<serde_json::Value, pi_rs_core::Error> {
        let _ = request;
        Ok(serde_json::json!({}))
    }
    fn validate_durable(
        &self,
        _: &crate::model::ProviderAttempt,
        request: &crate::model::DurableRequest,
        _: &serde_json::Value,
    ) -> Result<(), pi_rs_core::Error> {
        let _ = request;
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &crate::model::ProviderAttempt,
        request: &ModelRequest,
        _: serde_json::Value,
    ) -> Result<ModelStream, pi_rs_core::Error> {
        Stub.stream(request.clone()).await
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
fn stage_limit_evidence(
    h: &mut Harness,
    id: Uuid,
    kind: LimitFailureKind,
    count: u32,
    limit: u32,
) -> (Conversation, u64, u64) {
    let (mut evidence, version) = h.load(id).unwrap();
    let source_observation = h.limit_source_observation(id, version).unwrap().unwrap();
    evidence.phase = Phase::LimitEvidence {
        kind,
        count,
        limit,
        source_observation,
        source_root_version: version,
    };
    h.save(&evidence, Expected::Version(version)).unwrap();
    let (evidence, version) = h.load(id).unwrap();
    let observation = h.limit_source_observation(id, version).unwrap().unwrap();
    (evidence, version, observation)
}
// Build runtime-shaped forged evidence while bypassing its production gate.
fn forge_limit_evidence(
    h: &mut Harness,
    id: Uuid,
    kind: LimitFailureKind,
    count: u32,
    limit: u32,
) -> (Conversation, u64, u64) {
    let (mut evidence, version) = h.load(id).unwrap();
    let source_observation = h.limit_source_observation(id, version).unwrap().unwrap();
    evidence.phase = Phase::LimitEvidence {
        kind,
        count,
        limit,
        source_observation,
        source_root_version: version,
    };
    let staged = h
        .prepare_save(&evidence, Expected::Version(version))
        .unwrap();
    h.commit_conversation_save(staged).unwrap();
    let (evidence, version) = h.load(id).unwrap();
    let observation = h.limit_source_observation(id, version).unwrap().unwrap();
    (evidence, version, observation)
}
fn setup(path: &std::path::Path) -> (Harness, Uuid, u64, Vec<ResetDerivationStep>) {
    setup_with_adapter(path, false)
}
fn setup_with_adapter(
    path: &std::path::Path,
    adapter: bool,
) -> (Harness, Uuid, u64, Vec<ResetDerivationStep>) {
    let mut h = Harness::open_paused(path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    if adapter {
        h.install_provider_adapter(Arc::new(BlockAdapter)).unwrap();
        h.select_provider_adapter(id, true).unwrap();
    }
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

fn add_many_resets(h: &mut Harness, id: Uuid) {
    for n in 2..66 {
        h.reset_with_blocks(
            id,
            Some(format!("reset {n}")),
            vec![vec![
                serde_json::json!({"type":"text","text":format!("block {n}")}),
            ]],
        )
        .unwrap();
    }
}

#[test]
fn chunked_failure_settlement_and_historical_fork_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = h.load(id).unwrap();
    let occurrence = stopped.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&stopped, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    RESET_FORCE_CHUNKED.with(|flag| flag.set(true));
    let result = h.save(&failure, Expected::Version(version));
    RESET_FORCE_CHUNKED.with(|flag| flag.set(false));
    result.unwrap();
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().resets[0].id,
        steps[0].reset_id
    );
    let prefix = format!("runtime/reset-batch/{id}/");
    let chunk = h
        .owner
        .store()
        .prefix_keys_bounded(prefix.as_bytes(), 4, prefix.len() + 64)
        .unwrap()
        .into_iter()
        .find(|key| key.len() > prefix.len() + 36)
        .unwrap();
    let value = h.owner.store().get(&chunk).unwrap().unwrap().value;
    h.owner
        .store()
        .commit(&[], &[Mutation::Put { key: chunk, value }])
        .unwrap();
    assert!(h.fork_by_entry_id(id, occurrence).is_err());
}

#[test]
fn oversized_reset_settlement_and_fork_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = h.load(id).unwrap();
    let occurrence = stopped.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    for _ in stopped.pending_resets.len()..=crate::reset_manifest::MAX_SHARED_CUTS {
        stopped.pending_resets.push(crate::reset::Reset::new(
            Uuid::new_v4(),
            Some("x".repeat(520)),
        ));
    }
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&stopped, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    let state = h.conversation(id).unwrap();
    let root = format!("runtime/conversation/{id}");
    assert!(
        h.owner
            .store()
            .get(root.as_bytes())
            .unwrap()
            .unwrap()
            .value
            .len()
            > 8 * 1024 * 1024
    );
    assert_eq!(
        state.resets.len(),
        crate::reset_manifest::MAX_SHARED_CUTS + 1
    );
    assert_eq!(
        state
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().resets[0].id,
        steps[0].reset_id
    );
}

#[test]
fn late_block_reset_validates_pending_queue_once() {
    let dir = tempfile::tempdir().unwrap();
    let (h, id, _, steps) = setup(&dir.path().join("db"));
    let (mut baseline, _) = h.load(id).unwrap();
    let blocks = baseline
        .pending_reset_blocks
        .remove(&steps[0].reset_id)
        .unwrap();
    baseline.pending_reset_blocks.clear();
    baseline.pending_resets.clear();
    for _ in 0..1024 {
        baseline
            .pending_resets
            .push(crate::reset::Reset::new(Uuid::new_v4(), None));
    }
    let final_reset = baseline.pending_resets.last().unwrap().id;
    baseline
        .pending_reset_blocks
        .insert(final_reset, blocks.clone());
    let steps: Vec<_> = baseline
        .pending_resets
        .iter()
        .map(|reset| ResetDerivationStep {
            reset_id: reset.id,
            context_generation: Uuid::new_v4(),
            write_limit: usize::MAX,
        })
        .collect();
    RESET_PENDING_VALIDATION_VISITS.with(|count| count.set(0));
    let final_state = visit_ordered_reset_states(&baseline, &steps, |_| Ok(())).unwrap();
    assert_eq!(
        RESET_PENDING_VALIDATION_VISITS.with(std::cell::Cell::get),
        steps.len()
    );
    assert_eq!(final_state.resets.len(), steps.len());
    assert_eq!(
        final_state
            .content_blocks
            .values()
            .cloned()
            .collect::<Vec<_>>(),
        blocks
    );
}

#[test]
fn large_atomic_failure_shared_evidence_reopens_and_fences_tampering() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    add_many_resets(&mut h, id);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = h.load(id).unwrap();
    let occurrence = stopped.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&stopped, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    let (_, selected, _) = h
        .atomic_failure_reset_steps(&failure, &Expected::Version(version))
        .unwrap()
        .unwrap();
    let batch = h
        .prepare_atomic_failure_reset_save(&failure, version, &selected)
        .unwrap();
    let references: Vec<_> = batch
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                Some(value)
            }
            _ => None,
        })
        .collect();
    let shared: Vec<_> = batch
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, value } if key.starts_with(b"runtime/reset-batch/") => {
                Some((key.clone(), value.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(references.len(), 66);
    assert_eq!(shared.len(), 1);
    assert!(
        references
            .iter()
            .all(|v| v.len() < 128 && *v == references[0])
    );
    assert!(shared[0].1.len() < 66 * 1024);
    h.commit_conversation_save(batch).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 66);
    assert_eq!(
        state
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(h.conversation(fork).unwrap().resets.len(), 1);
    let manifest: serde_json::Value = serde_json::from_slice(&shared[0].1).unwrap();
    let cursor: crate::transcript::Cursor =
        serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
    let observation = manifest["anchor_observation"].as_u64().unwrap();
    {
        let _scope = ResetProofScope::enter();
        RESET_PROOF_REPLAYS.with(|count| count.set(0));
        RESET_PROOF_TRANSITIONS.with(|count| count.set(0));
        let cuts = manifest["cuts"].as_array().unwrap();
        h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
        RESET_PROOF_CHECK_WORK.with(|count| count.set(0));
        for cut in cuts.iter().rev() {
            let cursor = serde_json::from_value(cut["cursor"].clone()).unwrap();
            h.verify_failure_reset_cut(id, &cursor, observation)
                .unwrap();
        }
        assert_eq!(RESET_PROOF_TRANSITIONS.with(std::cell::Cell::get), 66);
        assert_eq!(
            RESET_PROOF_CHECK_WORK.with(std::cell::Cell::get),
            2 * cuts.len()
        );
        RESET_PROOF_TRANSITIONS.with(|count| count.set(0));
        for (index, cut) in cuts.iter().enumerate().rev().chain(cuts.iter().enumerate()) {
            let cursor = serde_json::from_value(cut["cursor"].clone()).unwrap();
            let (state, _, _) = h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
            assert_eq!(state.resets.len(), index + 1);
        }
        assert_eq!(RESET_PROOF_REPLAYS.with(std::cell::Cell::get), 1);
        assert!(RESET_PROOF_TRANSITIONS.with(std::cell::Cell::get) <= 130);
    }
    let _scope = ResetProofScope::enter();
    let (_, _, checks) = h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
    let later: crate::transcript::Cursor =
        serde_json::from_value(manifest["cuts"][65]["cursor"].clone()).unwrap();
    h.verify_failure_reset_cut(id, &later, observation).unwrap();
    assert!(checks.iter().any(|c| c.key == shared[0].0));
    let mut tampered = manifest.clone();
    tampered["cuts"][0]["state_sha256"] = serde_json::json!("0".repeat(64));
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: shared[0].0.clone(),
                value: serde_json::to_vec(&tampered).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.owner.store().commit(&checks, &[]).is_err());
    assert!(h.verify_failure_reset_cut(id, &later, observation).is_err());
    assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
    drop(_scope);
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.fork_by_entry_id(id, occurrence).is_err());
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
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
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
    let batch = h
        .prepare_atomic_failure_reset_save(&failure, version, &steps)
        .unwrap();
    let bytes = batch
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                Some(value.clone())
            }
            _ => None,
        })
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let cursor: crate::transcript::Cursor =
        serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
    let observation = manifest["anchor_observation"].as_u64().unwrap();
    h.commit_conversation_save(batch).unwrap();
    let (cut, _, _) = h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
    assert_eq!(
        cut.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![steps[0].reset_id]
    );
    let child = h
        .fork_at_position(
            id,
            ViewCursor {
                cursor: cursor.clone(),
                remaining: Some(cut.messages.len()),
            },
        )
        .unwrap();
    let occurrence = cut.content_blocks.values().next().unwrap().message_id;
    let original = h
        .original_occurrence_position(
            &MessageHistory {
                id: occurrence,
                commit: observation,
                origin: id,
                commit_boundary: true,
            },
            cut.messages.last().unwrap(),
        )
        .unwrap();
    assert_eq!(original.cursor, cursor);
    let by_id = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(
        h.conversation(by_id).unwrap().content_blocks,
        cut.content_blocks
    );
    let forked = h.conversation(child).unwrap();
    assert_eq!(forked.messages, cut.messages);
    assert_eq!(forked.content_blocks, cut.content_blocks);
    assert_eq!(
        forked.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![steps[0].reset_id]
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(child).unwrap().content_blocks,
        cut.content_blocks
    );
    let (_, _, checks) = h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
    let prior_cursor: crate::transcript::Cursor =
        serde_json::from_value(manifest["predecessor_cursor"].clone()).unwrap();
    let (prior_entry, prior_version) = crate::transcript::Transcript::new(h.owner.store(), id)
        .entry_evidence(&prior_cursor)
        .unwrap();
    assert!(checks.iter().any(
        |check| check.key == prior_entry && check.expected == Expected::Version(prior_version)
    ));
    let predecessor = checks
        .iter()
        .find(|check| {
            check
                .key
                .starts_with(format!("runtime/fork/{id}/").as_bytes())
                && check.key != format!("runtime/fork/{id}/{observation:020}").as_bytes()
        })
        .unwrap();
    let value = h
        .owner
        .store()
        .get(&predecessor.key)
        .unwrap()
        .unwrap()
        .value;
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: predecessor.key.clone(),
                value,
            }],
        )
        .unwrap();
    assert!(h.owner.store().commit(&checks, &[]).is_err());
    assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
}

#[test]
fn production_stopped_model_failure_commits_reset_chain_and_forks_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = h.load(id).unwrap();
    let occurrence = stopped.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&stopped, Expected::Version(version)).unwrap();
    let before = crate::observer::Observer::new(h.owner.store(), id)
        .head()
        .unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    let after = crate::observer::Observer::new(h.owner.store(), id)
        .head()
        .unwrap();
    assert_eq!(after.0, before.0 + 1);
    let state = h.conversation(id).unwrap();
    assert_eq!(
        state.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        steps.iter().map(|s| s.reset_id).collect::<Vec<_>>()
    );
    assert_eq!(
        state
            .submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    let child = h.conversation(fork).unwrap();
    assert_eq!(
        child.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![steps[0].reset_id]
    );
    assert_eq!(
        child.content_blocks.values().next().unwrap().message_id,
        occurrence
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(fork)
            .unwrap()
            .content_blocks
            .values()
            .next()
            .unwrap()
            .message_id,
        occurrence
    );
}

#[test]
fn atomic_failure_rebuilds_after_reset_admission_race() {
    let dir = tempfile::tempdir().unwrap();
    let (mut owner, id, _, steps) = setup(&dir.path().join("db"));
    add_many_resets(&mut owner, id);
    owner
        .set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    owner.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = owner.load(id).unwrap();
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    owner.save(&stopped, Expected::Version(version)).unwrap();
    let mut worker = owner.worker();
    let (mut failure, version) = worker.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    let (third, _) = owner
        .reset_with_blocks(
            id,
            Some("third".into()),
            vec![vec![serde_json::json!({"type":"text","text":"c"})]],
        )
        .unwrap();
    worker.save(&failure, Expected::Version(version)).unwrap();
    let state = owner.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 67);
    assert_eq!(state.resets[0].id, steps[0].reset_id);
    assert_eq!(state.resets[1].id, steps[1].reset_id);
    assert_eq!(state.resets.last().unwrap().id, third);
    assert!(matches!(state.phase, Phase::ModelReady { .. }));
}

#[cfg(unix)]
#[test]
fn atomic_failure_kill_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_ATOMIC_FAILURE_KILL_DB") else {
        return;
    };
    let (mut h, id, _, steps) = setup_with_adapter(std::path::Path::new(&path), true);
    if std::env::var_os("PI_ATOMIC_FAILURE_MANY").is_some() {
        add_many_resets(&mut h, id);
    }
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (mut stopped, version) = h.load(id).unwrap();
    let occurrence = stopped.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    stopped.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&stopped, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    RESET_FORCE_CHUNKED.with(|flag| flag.set(true));
    h.save(&failure, Expected::Version(version)).unwrap();
    println!(
        "ATOMIC_FAILURE_COMMITTED {id} {occurrence} {}",
        steps[0].reset_id
    );
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn production_atomic_failure_survives_process_kill_and_forks() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::runtime_atomic_reset_tests::atomic_failure_kill_child",
            "--nocapture",
        ])
        .env("PI_ATOMIC_FAILURE_KILL_DB", &path)
        .env("PI_ATOMIC_FAILURE_MANY", "1")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some((_, rest)) = line.split_once("ATOMIC_FAILURE_COMMITTED ") {
                let _ = tx.send(rest.to_owned());
                break;
            }
        }
    });
    let marker = rx.recv_timeout(std::time::Duration::from_secs(120));
    child.kill().unwrap();
    child.wait().unwrap();
    let marker = marker.expect("child did not commit");
    let mut fields = marker.split_whitespace();
    let id = Uuid::parse_str(fields.next().unwrap()).unwrap();
    let occurrence = Uuid::parse_str(fields.next().unwrap()).unwrap();
    let reset = Uuid::parse_str(fields.next().unwrap()).unwrap();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(Arc::new(BlockAdapter)).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 66);
    assert!(matches!(state.phase, Phase::ModelReady { .. }));
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(h.conversation(fork).unwrap().resets[0].id, reset);
    h.drive(id, &CancellationToken::new()).await.unwrap();
    let state = h.conversation(id).unwrap();
    assert!(
        state
            .submissions
            .iter()
            .any(|s| matches!(s.status, Status::Done { .. }))
    );
    let later_fork = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(h.conversation(later_fork).unwrap().resets[0].id, reset);
}

#[test]
fn below_limit_forged_atomic_failure_manifests_fail_closed() {
    for restart in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, _, steps) = setup(&path);
        h.set_after_failed_run(id, AfterFailedRun::Continue)
            .unwrap();
        h.submit(id, None, "replacement".into()).unwrap();
        let (mut checkpoint, version) = h.load(id).unwrap();
        let occurrence = checkpoint.pending_reset_blocks[&steps[0].reset_id][0].message_id;
        checkpoint.phase = if restart {
            Phase::AbandonedModel {
                turns: 0,
                request_id: Uuid::new_v4(),
                restarts: 0,
            }
        } else {
            Phase::ModelReady { turns: 0 }
        };
        h.save(&checkpoint, Expected::Version(version)).unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        if restart {
            finish_failure(
                &mut failure,
                "model restart limit".into(),
                FailureCause::RestartLimit(0),
            );
        } else {
            finish_failure(
                &mut failure,
                "turn limit".into(),
                FailureCause::TurnLimit(0),
            );
        }
        assert!(
            h.atomic_failure_reset_steps(&failure, &Expected::Version(version))
                .unwrap()
                .is_none()
        );
        // Bypass production admission to emulate an impossible same-commit
        // manifest and transcript from an untrusted storage writer.
        let batch = h
            .prepare_atomic_failure_reset_save(&failure, version, &steps)
            .unwrap();
        let bytes = batch
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                    Some(value.clone())
                }
                _ => None,
            })
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let cursor = serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
        let observation = manifest["anchor_observation"].as_u64().unwrap();
        h.commit_conversation_save(batch).unwrap();
        assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
        assert!(h.fork_by_entry_id(id, occurrence).is_err());
    }
}

#[test]
fn limit_failures_keep_staged_reset_path() {
    for restart in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, _, _) = setup(&path);
        h.set_after_failed_run(id, AfterFailedRun::Continue)
            .unwrap();
        h.submit(id, None, "replacement".into()).unwrap();
        let (mut checkpoint, version) = h.load(id).unwrap();
        checkpoint.phase = if restart {
            Phase::AbandonedModel {
                turns: 0,
                request_id: Uuid::new_v4(),
                restarts: 3,
            }
        } else {
            Phase::ModelReady { turns: 5 }
        };
        h.save(&checkpoint, Expected::Version(version)).unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        if restart {
            finish_failure(
                &mut failure,
                "model restart limit".into(),
                FailureCause::RestartLimit(3),
            );
        } else {
            finish_failure(
                &mut failure,
                "turn limit".into(),
                FailureCause::TurnLimit(5),
            );
        }
        h.save(&failure, Expected::Version(version)).unwrap();
        let state = h.conversation(id).unwrap();
        assert!(state.resets.is_empty());
        assert_eq!(state.pending_resets.len(), 2);
        assert!(matches!(state.phase, Phase::Idle));
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let state = h.conversation(id).unwrap();
        assert!(state.resets.is_empty());
        assert_eq!(state.pending_resets.len(), 2);
    }
}

#[test]
fn atomic_failure_fork_preserves_writes_at_each_reset() {
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
    h.submit(id, None, "active".into()).unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (first, occurrences) = h
        .reset_with_blocks(
            id,
            Some("first".into()),
            vec![vec![serde_json::json!({"type":"text","text":"a"})]],
        )
        .unwrap();
    let first_write = h.submit_write(id, None, "app.first".into(), None).unwrap();
    let (second, _) = h
        .reset_with_blocks(
            id,
            Some("second".into()),
            vec![vec![serde_json::json!({"type":"text","text":"b"})]],
        )
        .unwrap();
    let second_write = h.submit_write(id, None, "app.second".into(), None).unwrap();
    let (mut checkpoint, version) = h.load(id).unwrap();
    checkpoint.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(
        state.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![first, second]
    );
    assert!(
        state
            .application_writes
            .iter()
            .find(|w| w.id == first_write)
            .unwrap()
            .placed
    );
    assert!(
        state
            .application_writes
            .iter()
            .find(|w| w.id == second_write)
            .unwrap()
            .placed
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(id, occurrences[0]).unwrap();
    let writes = h.conversation(fork).unwrap().application_writes;
    assert_eq!(
        writes.iter().map(|w| w.id).collect::<Vec<_>>(),
        vec![first_write]
    );
}

#[test]
fn repeated_atomic_failure_batches_keep_prior_fork_proofs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup_with_adapter(&path, true);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement one".into()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    let old_occurrence = state.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    state.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "first error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&state, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "first error".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    h.submit(id, None, "replacement two".into()).unwrap();
    let (third, occurrences) = h
        .reset_with_blocks(
            id,
            Some("third".into()),
            vec![vec![serde_json::json!({"type":"text","text":"c"})]],
        )
        .unwrap();
    h.reset_with_blocks(
        id,
        Some("fourth".into()),
        vec![vec![serde_json::json!({"type":"text","text":"d"})]],
    )
    .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "second error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&state, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(
        &mut failure,
        "second error".into(),
        FailureCause::ModelError,
    );
    h.save(&failure, Expected::Version(version)).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let old = h.fork_by_entry_id(id, old_occurrence).unwrap();
    assert_eq!(h.conversation(old).unwrap().resets.len(), 1);
    let new = h.fork_by_entry_id(id, occurrences[0]).unwrap();
    assert_eq!(
        h.conversation(new).unwrap().resets.last().unwrap().id,
        third
    );
}

#[test]
fn later_batch_reuses_chunked_predecessor_evidence_and_fences_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, _) = setup_with_adapter(&path, true);
    add_many_resets(&mut h, id);
    for _ in 66..350 {
        h.reset_with_blocks(
            id,
            Some("more".into()),
            vec![vec![serde_json::json!({"type":"text","text":"block"})]],
        )
        .unwrap();
    }
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    for batch in 0..2 {
        h.submit(id, None, format!("replacement {batch}")).unwrap();
        if batch == 1 {
            for _ in 0..2 {
                h.reset_with_blocks(
                    id,
                    Some("next".into()),
                    vec![vec![serde_json::json!({"type":"text","text":"block"})]],
                )
                .unwrap();
            }
        }
        let (mut stopped, version) = h.load(id).unwrap();
        stopped.phase = Phase::FailedModel {
            turns: 0,
            request_id: Uuid::new_v4(),
            reason: "error".into(),
            decision: ModelFailureDecision::Stop,
        };
        h.save(&stopped, Expected::Version(version)).unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        finish_failure(&mut failure, "error".into(), FailureCause::ModelError);
        RESET_FORCE_CHUNKED.with(|flag| flag.set(batch == 0));
        let result = h.save(&failure, Expected::Version(version));
        RESET_FORCE_CHUNKED.with(|flag| flag.set(false));
        result.unwrap();
    }
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let prefix = format!("runtime/reset-batch/{id}/");
    assert!(
        h.owner
            .store()
            .prefix_keys_bounded(prefix.as_bytes(), 16, prefix.len() + 64)
            .unwrap()
            .len()
            >= 3
    );
    let (state, _) = h.load(id).unwrap();
    let observation = crate::observer::Observer::new(h.owner.store(), id)
        .head()
        .unwrap()
        .0;
    let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
    let tail = transcript
        .read_with_blocks(
            state.transcript.as_ref().unwrap(),
            std::num::NonZeroUsize::MIN,
        )
        .unwrap();
    let second = transcript
        .read_with_blocks(&tail.next, std::num::NonZeroUsize::MIN)
        .unwrap();
    let target = second.next;
    crate::reset_manifest::RESET_MANIFEST_CHUNK_READS.with(|count| count.set(0));
    let (_, _, checks) = h.prove_failure_reset_cut(id, &target, observation).unwrap();
    let reads = crate::reset_manifest::RESET_MANIFEST_CHUNK_READS.with(std::cell::Cell::get);
    assert!(reads <= 12, "read {reads} predecessor chunks for 350 cuts");
    assert!(checks.len() < 350 * 20, "{} evidence checks", checks.len());
    let candidate_prefix = format!("runtime/reset-manifest/{id}/");
    assert!(
        checks
            .iter()
            .filter(|check| check.key.starts_with(candidate_prefix.as_bytes())
                && matches!(check.expected, Expected::Version(_)))
            .count()
            >= 350
    );
    let chunk = checks
        .iter()
        .find(|check| {
            check
                .key
                .starts_with(format!("runtime/reset-batch/{id}/").as_bytes())
                && check.key.iter().filter(|byte| **byte == b'/').count() == 4
        })
        .unwrap()
        .key
        .clone();
    let value = h.owner.store().get(&chunk).unwrap().unwrap().value;
    h.owner
        .store()
        .commit(&[], &[Mutation::Put { key: chunk, value }])
        .unwrap();
    assert!(h.owner.store().commit(&checks, &[]).is_err());
    assert!(h.prove_failure_reset_cut(id, &target, observation).is_err());
}

#[test]
fn production_replays_nothing_to_answer_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (mut checkpoint, version) = h.load(id).unwrap();
    let occurrence = checkpoint.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    checkpoint.messages.push(Message::Assistant {
        content: "already answered".into(),
        calls: vec![],
        replay: None,
    });
    checkpoint.phase = Phase::ModelReady { turns: 0 };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(
        &mut failure,
        "nothing_to_answer".into(),
        FailureCause::NothingToAnswer,
    );
    h.save(&failure, Expected::Version(version)).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(id).unwrap().run_failure.unwrap().reason,
        "nothing_to_answer"
    );
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().resets[0].id,
        steps[0].reset_id
    );
}

#[test]
fn reset_proof_replays_each_prior_cut_once_per_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, _) = setup_with_adapter(&path, true);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let batches = 6;
    for index in 0..batches {
        h.submit(id, None, format!("replacement {index}")).unwrap();
        if index > 0 {
            for part in 0..2 {
                h.reset_with_blocks(
                    id,
                    Some(format!("{index}-{part}")),
                    vec![vec![serde_json::json!({"type":"text","text":"block"})]],
                )
                .unwrap();
            }
        }
        let (mut checkpoint, version) = h.load(id).unwrap();
        checkpoint.phase = Phase::FailedModel {
            turns: 0,
            request_id: Uuid::new_v4(),
            reason: "error".into(),
            decision: ModelFailureDecision::Stop,
        };
        h.save(&checkpoint, Expected::Version(version)).unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        finish_failure(&mut failure, "error".into(), FailureCause::ModelError);
        h.save(&failure, Expected::Version(version)).unwrap();
    }
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let (state, _) = h.load(id).unwrap();
    let observation = crate::observer::Observer::new(h.owner.store(), id)
        .head()
        .unwrap()
        .0;
    let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
    let tail = transcript
        .read_with_blocks(
            state.transcript.as_ref().unwrap(),
            std::num::NonZeroUsize::MIN,
        )
        .unwrap();
    let second = transcript
        .read_with_blocks(&tail.next, std::num::NonZeroUsize::MIN)
        .unwrap();
    let first = second.next;
    RESET_PROOF_REPLAYS.with(|count| count.set(0));
    let (cut, _, _) = h.prove_failure_reset_cut(id, &first, observation).unwrap();
    let replay_count = RESET_PROOF_REPLAYS.with(std::cell::Cell::get);
    assert!(
        replay_count <= 2 * batches,
        "{replay_count} replays for {batches} batches"
    );
    let occurrence = cut.content_blocks.values().next().unwrap().message_id;
    RESET_PROOF_REPLAYS.with(|count| count.set(0));
    let fork = h.fork_by_entry_id(id, occurrence).unwrap();
    let replay_count = RESET_PROOF_REPLAYS.with(std::cell::Cell::get);
    assert!(
        replay_count <= 2 * batches,
        "{replay_count} fork replays for {batches} batches"
    );
    assert_eq!(h.conversation(fork).unwrap().resets.len(), 2 * batches - 1);

    // Change an earlier proof's evidence from another thread between cache
    // hits. The old supporting fences must survive in both the cached result
    // and any transaction that depends on the outer proof.
    let _scope = ResetProofScope::enter();
    let (_, _, checks) = h.prove_failure_reset_cut(id, &first, observation).unwrap();
    let (entry, sequence) = first.entry_identity().unwrap();
    let own_prefix = crate::reset_manifest::candidate_prefix(id, entry, sequence);
    let nested = checks
        .iter()
        .find(|check| {
            check.key.starts_with(b"runtime/reset-manifest/")
                && !check.key.starts_with(&own_prefix)
                && matches!(check.expected, Expected::Version(_))
        })
        .unwrap()
        .key
        .clone();
    let owner = h.owner.clone();
    std::thread::spawn(move || {
        let value = owner.store().get(&nested).unwrap().unwrap().value;
        owner
            .store()
            .commit(&[], &[Mutation::Put { key: nested, value }])
            .unwrap();
    })
    .join()
    .unwrap();
    assert!(h.owner.store().commit(&checks, &[]).is_err());
    assert!(h.prove_failure_reset_cut(id, &first, observation).is_err());
}

#[tokio::test]
async fn limit_decisions_produce_atomic_failure_and_historical_forks() {
    for restart in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, _, steps) = setup_with_adapter(&path, true);
        h.set_after_failed_run(id, AfterFailedRun::Continue)
            .unwrap();
        let replacement = h.submit(id, None, "replacement".into()).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        let occurrence = state.pending_reset_blocks[&steps[0].reset_id][0].message_id;
        state.phase = if restart {
            Phase::AbandonedModel {
                turns: 0,
                request_id: Uuid::new_v4(),
                restarts: 3,
            }
        } else {
            Phase::ModelReady { turns: 5 }
        };
        h.save(&state, Expected::Version(version)).unwrap();
        h.drive(id, &CancellationToken::new()).await.unwrap();
        let state = h.conversation(id).unwrap();
        assert!(matches!(
            state
                .submissions
                .iter()
                .find(|s| s.id == replacement)
                .unwrap()
                .status,
            Status::Done { .. }
        ));
        let page = crate::observer::Observer::new(h.owner.store(), id)
            .read(
                crate::observer::Cursor(0),
                std::num::NonZeroUsize::new(128).unwrap(),
            )
            .unwrap();
        let decision = page
            .records
            .iter()
            .find_map(|r| {
                let key = format!("runtime/fork/{id}/{:020}", r.cursor.0);
                let entry = h.owner.store().get(key.as_bytes()).unwrap()?;
                let value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
                value["phase"].get("LimitFailure").cloned()
            })
            .expect("missing persisted terminal decision");
        assert_eq!(decision["count"], if restart { 3 } else { 5 });
        assert_eq!(decision["limit"], if restart { 3 } else { 5 });
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let fork = h.fork_by_entry_id(id, occurrence).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().resets[0].id,
            steps[0].reset_id
        );
        let source = format!(
            "runtime/fork/{id}/{:020}",
            decision["source_observation"].as_u64().unwrap()
        )
        .into_bytes();
        let value = h.owner.store().get(&source).unwrap().unwrap().value;
        h.owner
            .store()
            .commit(&[], &[Mutation::Put { key: source, value }])
            .unwrap();
        assert!(h.fork_by_entry_id(id, occurrence).is_err());
    }
}

#[test]
fn malformed_limit_decisions_do_not_authorize_forged_manifests() {
    for (count, limit) in [(0, 3), (4, 5), (0, 0), (1, 1)] {
        for restart in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (mut h, id, _, steps) = setup(&path);
            h.set_after_failed_run(id, AfterFailedRun::Continue)
                .unwrap();
            h.submit(id, None, "replacement".into()).unwrap();
            let (mut checkpoint, version) = h.load(id).unwrap();
            let source_observation = h.limit_source_observation(id, version).unwrap().unwrap();
            checkpoint.phase = Phase::LimitFailure {
                kind: if restart {
                    LimitFailureKind::Restart
                } else {
                    LimitFailureKind::Turn
                },
                count,
                limit,
                source_observation,
                source_root_version: version,
            };
            h.save(&checkpoint, Expected::Version(version)).unwrap();
            let (mut failure, version) = h.load(id).unwrap();
            if restart {
                finish_failure(
                    &mut failure,
                    "model restart limit".into(),
                    FailureCause::RestartLimit(count),
                );
            } else {
                finish_failure(
                    &mut failure,
                    "turn limit".into(),
                    FailureCause::TurnLimit(count),
                );
            }
            assert!(
                h.atomic_failure_reset_steps(&failure, &Expected::Version(version))
                    .unwrap()
                    .is_none()
            );
            let batch = h
                .prepare_atomic_failure_reset_save(&failure, version, &steps)
                .unwrap();
            let bytes = batch
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                        Some(value.clone())
                    }
                    _ => None,
                })
                .unwrap();
            let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let cursor = serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
            let observation = manifest["anchor_observation"].as_u64().unwrap();
            h.commit_conversation_save(batch).unwrap();
            assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
            drop(h);
            let h = Harness::open_paused(&path, options()).unwrap();
            assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
        }
    }
}

#[cfg(unix)]
#[test]
fn limit_decision_kill_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_LIMIT_DECISION_KILL_DB") else {
        return;
    };
    let restart = std::env::var("PI_LIMIT_DECISION_KIND").unwrap() == "restart";
    let (mut h, id, _, steps) = setup_with_adapter(std::path::Path::new(&path), true);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut checkpoint, version) = h.load(id).unwrap();
    let occurrence = checkpoint.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    checkpoint.phase = if restart {
        Phase::AbandonedModel {
            turns: 0,
            request_id: Uuid::new_v4(),
            restarts: 3,
        }
    } else {
        Phase::ModelReady { turns: 5 }
    };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let kind = if restart {
        LimitFailureKind::Restart
    } else {
        LimitFailureKind::Turn
    };
    let count = if restart { 3 } else { 5 };
    let (mut checkpoint, version, source_observation) =
        stage_limit_evidence(&mut h, id, kind, count, count);
    if std::env::var("PI_LIMIT_DECISION_STAGE").unwrap_or_default() != "evidence" {
        checkpoint.phase = Phase::LimitFailure {
            kind: if restart {
                LimitFailureKind::Restart
            } else {
                LimitFailureKind::Turn
            },
            count: if restart { 3 } else { 5 },
            limit: if restart { 3 } else { 5 },
            source_observation,
            source_root_version: version,
        };
        h.save(&checkpoint, Expected::Version(version)).unwrap();
    }
    println!("LIMIT_DECISION_COMMITTED {id} {occurrence} {replacement}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn killed_limit_decision_resumes_with_saved_limit_and_forks() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    for kind in ["turn", "restart"] {
        for stage in ["evidence", "decision"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::runtime_atomic_reset_tests::limit_decision_kill_child",
                    "--nocapture",
                ])
                .env("PI_LIMIT_DECISION_KILL_DB", &path)
                .env("PI_LIMIT_DECISION_KIND", kind)
                .env("PI_LIMIT_DECISION_STAGE", stage)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if let Some((_, rest)) = line.split_once("LIMIT_DECISION_COMMITTED ") {
                        let _ = tx.send(rest.to_owned());
                        break;
                    }
                }
            });
            let marker = rx.recv_timeout(std::time::Duration::from_secs(20));
            child.kill().unwrap();
            child.wait().unwrap();
            let marker = marker.expect("child did not commit decision");
            let mut fields = marker.split_whitespace();
            let id = Uuid::parse_str(fields.next().unwrap()).unwrap();
            let occurrence = Uuid::parse_str(fields.next().unwrap()).unwrap();
            let replacement = Uuid::parse_str(fields.next().unwrap()).unwrap();
            let mut changed = options();
            changed.max_turns = 100;
            changed.restart_limit = 100;
            let mut h = Harness::open_paused(&path, changed).unwrap();
            h.install_provider_adapter(Arc::new(BlockAdapter)).unwrap();
            h.submit(id, None, "after decision".into()).unwrap();
            h.reset_with_blocks(
                id,
                Some("later reset".into()),
                vec![vec![serde_json::json!({"type":"text","text":"later"})]],
            )
            .unwrap();
            h.drive(id, &CancellationToken::new()).await.unwrap();
            assert!(matches!(
                h.conversation(id)
                    .unwrap()
                    .submissions
                    .iter()
                    .find(|s| s.id == replacement)
                    .unwrap()
                    .status,
                Status::Done { .. }
            ));
            let fork = h.fork_by_entry_id(id, occurrence).unwrap();
            assert_eq!(h.conversation(fork).unwrap().resets.len(), 1);
        }
    }
}

#[test]
fn atomic_reset_save_fences_predecessor_proofs_until_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, _) = setup_with_adapter(&path, true);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement one".into()).unwrap();
    let (mut checkpoint, version) = h.load(id).unwrap();
    checkpoint.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "first".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "first".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    let (previous, _) = h.load(id).unwrap();
    let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
    let tail = transcript
        .read_with_blocks(
            previous.transcript.as_ref().unwrap(),
            std::num::NonZeroUsize::MIN,
        )
        .unwrap();
    let last_reset = transcript
        .read_with_blocks(&tail.next, std::num::NonZeroUsize::MIN)
        .unwrap();
    let (entry, sequence) = last_reset.next.entry_identity().unwrap();
    let prefix = crate::reset_manifest::candidate_prefix(id, entry, sequence);
    let old_manifest = h
        .owner
        .store()
        .prefix_keys_bounded(&prefix, 2, prefix.len() + 36)
        .unwrap()
        .pop()
        .unwrap();
    h.submit(id, None, "replacement two".into()).unwrap();
    for text in ["third", "fourth"] {
        h.reset_with_blocks(
            id,
            Some(text.into()),
            vec![vec![serde_json::json!({"type":"text","text":text})]],
        )
        .unwrap();
    }
    let (mut checkpoint, version) = h.load(id).unwrap();
    checkpoint.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "second".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "second".into(), FailureCause::ModelError);
    let owner = h.owner.clone();
    RESET_PROOF_BEFORE_SAVE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                let value = owner.store().get(&old_manifest).unwrap().unwrap().value;
                owner
                    .store()
                    .commit(
                        &[],
                        &[Mutation::Put {
                            key: old_manifest,
                            value,
                        }],
                    )
                    .unwrap();
            })
            .join()
            .unwrap();
        }))
    });
    assert!(matches!(
        h.save(&failure, Expected::Version(version)),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    let state = h.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 2);
    assert_eq!(state.pending_resets.len(), 2);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 2);
    assert_eq!(state.pending_resets.len(), 2);
}

#[test]
fn stale_counter_checkpoint_cannot_authorize_limit_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (mut source, version) = h.load(id).unwrap();
    source.phase = Phase::ModelReady { turns: 5 };
    h.save(&source, Expected::Version(version)).unwrap();
    let (_, source_version) = h.load(id).unwrap();
    let source_observation = h
        .limit_source_observation(id, source_version)
        .unwrap()
        .unwrap();
    h.submit(id, None, "intervening admission".into()).unwrap();
    let (mut forged, version) = h.load(id).unwrap();
    forged.phase = Phase::LimitFailure {
        kind: LimitFailureKind::Turn,
        count: 5,
        limit: 5,
        source_observation,
        source_root_version: source_version,
    };
    h.save(&forged, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(
        &mut failure,
        "turn limit".into(),
        FailureCause::TurnLimit(5),
    );
    assert!(
        h.atomic_failure_reset_steps(&failure, &Expected::Version(version))
            .unwrap()
            .is_none()
    );
    let batch = h
        .prepare_atomic_failure_reset_save(&failure, version, &steps)
        .unwrap();
    let bytes = batch
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                Some(value.clone())
            }
            _ => None,
        })
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let cursor = serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
    let observation = manifest["anchor_observation"].as_u64().unwrap();
    h.commit_conversation_save(batch).unwrap();
    assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
}

#[tokio::test]
async fn limit_decision_retries_admissions_before_recording_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, steps) = setup_with_adapter(&path, true);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (mut source, version) = h.load(id).unwrap();
    source.phase = Phase::ModelReady { turns: 5 };
    h.save(&source, Expected::Version(version)).unwrap();
    let mut admission = h.worker();
    RESET_PROOF_BEFORE_SAVE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                admission
                    .reset_with_blocks(
                        id,
                        Some("racing reset".into()),
                        vec![vec![serde_json::json!({"type":"text","text":"racing"})]],
                    )
                    .unwrap();
            })
            .join()
            .unwrap();
        }))
    });
    h.drive(id, &CancellationToken::new()).await.unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 3);
    assert_eq!(state.resets[0].id, steps[0].reset_id);
    assert_eq!(state.resets[1].id, steps[1].reset_id);
    assert_eq!(state.resets[2].handoff.as_deref(), Some("racing reset"));
}

#[test]
fn limit_failure_atomic_save_fences_counter_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, _) = setup(&path);
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "replacement".into()).unwrap();
    let (mut source, version) = h.load(id).unwrap();
    source.phase = Phase::ModelReady { turns: 5 };
    h.save(&source, Expected::Version(version)).unwrap();
    let (mut decision, source_version, source_observation) =
        stage_limit_evidence(&mut h, id, LimitFailureKind::Turn, 5, 5);
    decision.phase = Phase::LimitFailure {
        kind: LimitFailureKind::Turn,
        count: 5,
        limit: 5,
        source_observation,
        source_root_version: source_version,
    };
    h.save(&decision, Expected::Version(source_version))
        .unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(
        &mut failure,
        "turn limit".into(),
        FailureCause::TurnLimit(5),
    );
    let source_key = format!("runtime/fork/{id}/{source_observation:020}").into_bytes();
    let owner = h.owner.clone();
    RESET_PROOF_BEFORE_SAVE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                let value = owner.store().get(&source_key).unwrap().unwrap().value;
                owner
                    .store()
                    .commit(
                        &[],
                        &[Mutation::Put {
                            key: source_key,
                            value,
                        }],
                    )
                    .unwrap();
            })
            .join()
            .unwrap();
        }))
    });
    assert!(matches!(
        h.save(&failure, Expected::Version(version)),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    let state = h.conversation(id).unwrap();
    assert!(state.resets.is_empty());
    assert_eq!(state.pending_resets.len(), 2);
    assert!(matches!(state.phase, Phase::LimitFailure { .. }));
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().pending_resets.len(), 2);
}

#[test]
fn forged_lower_bound_and_wrong_turn_branch_fail_closed() {
    for wrong_branch in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, _, steps) = setup(&path);
        h.set_after_failed_run(id, AfterFailedRun::Continue)
            .unwrap();
        h.submit(id, None, "replacement".into()).unwrap();
        let (mut original, version) = h.load(id).unwrap();
        let count = if wrong_branch { 5 } else { 1 };
        original.phase = Phase::ModelReady { turns: count };
        if wrong_branch {
            original.messages.push(Message::Assistant {
                content: "already answered".into(),
                calls: vec![],
                replay: None,
            });
        }
        h.save(&original, Expected::Version(version)).unwrap();
        let (mut forged_evidence, version) = h.load(id).unwrap();
        let original_observation = h.limit_source_observation(id, version).unwrap().unwrap();
        forged_evidence.phase = Phase::LimitEvidence {
            kind: LimitFailureKind::Turn,
            count,
            limit: if wrong_branch { 5 } else { 1 },
            source_observation: original_observation,
            source_root_version: version,
        };
        assert!(
            h.save(&forged_evidence, Expected::Version(version))
                .is_err()
        );
        let (mut decision, source_version, source_observation) =
            forge_limit_evidence(&mut h, id, LimitFailureKind::Turn, count, 5);
        decision.phase = Phase::LimitFailure {
            kind: LimitFailureKind::Turn,
            count,
            limit: if wrong_branch { 5 } else { 1 },
            source_observation,
            source_root_version: source_version,
        };
        // Forge a same-commit runtime-shaped record without going through the
        // production transition guard.
        let staged = h
            .prepare_save(&decision, Expected::Version(source_version))
            .unwrap();
        h.commit_conversation_save(staged).unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        finish_failure(
            &mut failure,
            "turn limit".into(),
            FailureCause::TurnLimit(count),
        );
        assert!(h.save(&failure, Expected::Version(version)).is_err());
        let batch = h
            .prepare_atomic_failure_reset_save(&failure, version, &steps)
            .unwrap();
        let bytes = batch
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                Mutation::Put { key, value } if key.starts_with(b"runtime/reset-manifest/") => {
                    Some(value.clone())
                }
                _ => None,
            })
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let cursor = serde_json::from_value(manifest["cuts"][0]["cursor"].clone()).unwrap();
        let observation = manifest["anchor_observation"].as_u64().unwrap();
        h.commit_conversation_save(batch).unwrap();
        assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.prove_failure_reset_cut(id, &cursor, observation).is_err());
    }
}

#[test]
fn staged_limit_failure_fences_independent_source_before_commit() {
    for policy in [
        AfterFailedRun::Keep,
        AfterFailedRun::Withdraw,
        AfterFailedRun::Continue,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, _, _) = setup(&path);
        h.set_after_failed_run(id, policy).unwrap();
        h.submit(id, None, "replacement".into()).unwrap();
        if policy == AfterFailedRun::Continue {
            for _ in 0..63 {
                h.reset(id, None).unwrap();
            }
        }
        let (mut source, version) = h.load(id).unwrap();
        source.phase = Phase::ModelReady { turns: 5 };
        h.save(&source, Expected::Version(version)).unwrap();
        let (mut decision, evidence_version, evidence_observation) =
            stage_limit_evidence(&mut h, id, LimitFailureKind::Turn, 5, 5);
        decision.phase = Phase::LimitFailure {
            kind: LimitFailureKind::Turn,
            count: 5,
            limit: 5,
            source_observation: evidence_observation,
            source_root_version: evidence_version,
        };
        h.save(&decision, Expected::Version(evidence_version))
            .unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        finish_failure(
            &mut failure,
            "turn limit".into(),
            FailureCause::TurnLimit(5),
        );
        assert_eq!(
            h.atomic_failure_reset_steps(&failure, &Expected::Version(version))
                .unwrap()
                .is_some(),
            policy == AfterFailedRun::Continue
        );
        let key = format!("runtime/fork/{id}/{evidence_observation:020}").into_bytes();
        let owner = h.owner.clone();
        RESET_PROOF_BEFORE_SAVE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::thread::spawn(move || {
                    let value = owner.store().get(&key).unwrap().unwrap().value;
                    owner
                        .store()
                        .commit(&[], &[Mutation::Put { key, value }])
                        .unwrap();
                })
                .join()
                .unwrap();
            }))
        });
        assert!(matches!(
            h.save(&failure, Expected::Version(version)),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        let state = h.conversation(id).unwrap();
        assert!(matches!(state.phase, Phase::LimitFailure { .. }));
        assert!(state.run_failure.is_none());
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().phase,
            Phase::LimitFailure { .. }
        ));
    }
}

#[test]
fn limit_configuration_checkpoint_fences_source_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id, _, _) = setup(&path);
    let (mut source, version) = h.load(id).unwrap();
    source.phase = Phase::ModelReady { turns: 5 };
    h.save(&source, Expected::Version(version)).unwrap();
    let (mut evidence, version) = h.load(id).unwrap();
    let source_observation = h.limit_source_observation(id, version).unwrap().unwrap();
    evidence.phase = Phase::LimitEvidence {
        kind: LimitFailureKind::Turn,
        count: 5,
        limit: 5,
        source_observation,
        source_root_version: version,
    };
    let key = format!("runtime/fork/{id}/{source_observation:020}").into_bytes();
    let owner = h.owner.clone();
    RESET_PROOF_BEFORE_SAVE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                let value = owner.store().get(&key).unwrap().unwrap().value;
                owner
                    .store()
                    .commit(&[], &[Mutation::Put { key, value }])
                    .unwrap();
            })
            .join()
            .unwrap();
        }))
    });
    assert!(matches!(
        h.save(&evidence, Expected::Version(version)),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(matches!(
        h.conversation(id).unwrap().phase,
        Phase::ModelReady { turns: 5 }
    ));
}

#[test]
fn atomic_failure_preserves_abort_policy_and_publishes_task_notification() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
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
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        },
        3,
    )
    .unwrap();
    TaskStore::new(h.owner.store())
        .save(&[Save::Create(&task)])
        .unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let failed = h
        .submit_with_notification(
            id,
            None,
            "active".into(),
            AdmissionOptions::default(),
            SubmissionNotify {
                task_id: task.id(),
                key: "failure".into(),
            },
        )
        .unwrap();
    let queued = h
        .submit_with_options(
            id,
            None,
            "replacement".into(),
            AdmissionOptions {
                on_abort: OnAbort::Keep,
                ..Default::default()
            },
        )
        .unwrap();
    let (first, occurrences) = h
        .reset_with_blocks(
            id,
            Some("first".into()),
            vec![vec![serde_json::json!({"type":"text","text":"a"})]],
        )
        .unwrap();
    h.reset_with_blocks(
        id,
        Some("second".into()),
        vec![vec![serde_json::json!({"type":"text","text":"b"})]],
    )
    .unwrap();
    add_many_resets(&mut h, id);
    let (mut checkpoint, version) = h.load(id).unwrap();
    checkpoint.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "model error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(id).unwrap();
    finish_failure(&mut failure, "model error".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    let signal = TaskStore::new(h.owner.store())
        .read_typed_signal(task.id(), &submission_settled_signal(), "failure")
        .unwrap()
        .unwrap();
    assert_eq!(signal["id"], serde_json::json!(failed));
    assert_eq!(signal["status"], "unanswered");
    let state = h.conversation(id).unwrap();
    let input = state.submissions.iter().find(|s| s.id == queued).unwrap();
    assert_eq!(input.on_abort, OnAbort::Keep);
    assert_eq!(input.status, Status::Running);
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(id, occurrences[0]).unwrap();
    let child = h.conversation(fork).unwrap();
    assert_eq!(child.resets[0].id, first);
    assert!(child.submission_notifications.is_empty());
}

#[test]
fn task_input_on_reset_fork_fences_original_proof() {
    for (continuation, duplicate) in [(false, false), (false, true), (true, false), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id, _, steps) = setup_with_adapter(&path, true);
        h.set_after_failed_run(id, AfterFailedRun::Continue)
            .unwrap();
        h.submit(id, None, "replacement".into()).unwrap();
        let (mut checkpoint, version) = h.load(id).unwrap();
        let occurrence = checkpoint.pending_reset_blocks[&steps[0].reset_id][0].message_id;
        checkpoint.phase = Phase::FailedModel {
            turns: 0,
            request_id: Uuid::new_v4(),
            reason: "error".into(),
            decision: ModelFailureDecision::Stop,
        };
        h.save(&checkpoint, Expected::Version(version)).unwrap();
        let (mut failure, version) = h.load(id).unwrap();
        finish_failure(&mut failure, "error".into(), FailureCause::ModelError);
        h.save(&failure, Expected::Version(version)).unwrap();
        let fork = h.fork_by_entry_id(id, occurrence).unwrap();
        if duplicate {
            if continuation {
                h.submit_continue(fork, Some("task-request".into()))
                    .unwrap();
            } else {
                h.submit(fork, Some("task-request".into()), "input".into())
                    .unwrap();
            }
        }
        let record = h.owner.store().get(&Harness::key(fork)).unwrap().unwrap();
        let intent = crate::document::TaskInputIntent {
            conversation: fork,
            version: record.version,
            ancestry_checks: Vec::new(),
            snapshot: record.value,
            admissions: vec![crate::document::TaskAdmission {
                request_id: "task-request".into(),
                draft: if continuation {
                    crate::document::TaskSubmissionDraft::Continue
                } else {
                    crate::document::TaskSubmissionDraft::Input {
                        content: "input".into(),
                        options: Default::default(),
                    }
                },
                notify: None,
                notification_binding: None,
            }],
        };
        // Reuse an active traversal, as happens when task admission is composed
        // with a caller that has already authenticated a nested reset cut.
        let scope = ResetProofScope::enter();
        let (_, entry) = h
            .owner
            .store()
            .list(b"runtime/reset-manifest/")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        let cursor = serde_json::from_value(value["cuts"][0]["cursor"].clone()).unwrap();
        let observation = value["anchor_observation"].as_u64().unwrap();
        h.prove_failure_reset_cut(id, &cursor, observation).unwrap();
        let batch = h.prepare_task_input(&intent).unwrap();
        drop(scope);
        let manifest = batch
            .checks
            .iter()
            .find(|check| {
                check.key.starts_with(b"runtime/reset-manifest/")
                    && matches!(check.expected, Expected::Version(_))
            })
            .expect("reset proof must be retained")
            .key
            .clone();
        let value = h.owner.store().get(&manifest).unwrap().unwrap().value;
        h.owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key: manifest,
                    value,
                }],
            )
            .unwrap();
        assert!(matches!(
            h.owner.store().commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
    }
}

fn task_input_reset_fork(path: &std::path::Path) -> (Harness, Uuid, Uuid) {
    let (mut h, origin, _, steps) = setup_with_adapter(path, true);
    h.set_after_failed_run(origin, AfterFailedRun::Continue)
        .unwrap();
    h.submit(origin, None, "replacement".into()).unwrap();
    let (mut checkpoint, version) = h.load(origin).unwrap();
    let occurrence = checkpoint.pending_reset_blocks[&steps[0].reset_id][0].message_id;
    checkpoint.phase = Phase::FailedModel {
        turns: 0,
        request_id: Uuid::new_v4(),
        reason: "error".into(),
        decision: ModelFailureDecision::Stop,
    };
    h.save(&checkpoint, Expected::Version(version)).unwrap();
    let (mut failure, version) = h.load(origin).unwrap();
    finish_failure(&mut failure, "error".into(), FailureCause::ModelError);
    h.save(&failure, Expected::Version(version)).unwrap();
    let fork = h.fork_by_entry_id(origin, occurrence).unwrap();
    let (mut state, version) = h.load(fork).unwrap();
    state.messages.push(Message::User {
        content: "one".into(),
    });
    state.messages.push(Message::User {
        content: "two".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let state = h.conversation(fork).unwrap();
    let child = h
        .fork_at_position(
            fork,
            ViewCursor {
                cursor: state.transcript.unwrap(),
                remaining: Some(1),
            },
        )
        .unwrap();
    (h, origin, child)
}

#[test]
fn task_input_retries_origin_advance_during_fork_proof() {
    use crate::document::{Address, Documents, TaskDocuments};
    use crate::task::scheduler::{Handler, Registry, Step};
    use crate::task::{Checkpoint, Task, TaskOwner, TaskState};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn race(owner: Arc<durable_store::owner::OwnedStore>, origin: Uuid) {
        crate::reset_manifest::BEFORE_CANDIDATE_CHECK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let key = Harness::key(origin);
                let record = owner.store().get(&key).unwrap().unwrap();
                owner
                    .store()
                    .commit(
                        &[Check {
                            key: key.clone(),
                            expected: Expected::Version(record.version),
                        }],
                        &[Mutation::Put {
                            key,
                            value: record.value,
                        }],
                    )
                    .unwrap();
            }));
        });
    }
    struct Submit {
        calls: Arc<AtomicUsize>,
        owner: Arc<durable_store::owner::OwnedStore>,
        origin: Uuid,
        during_prepare: bool,
    }
    impl Handler for Submit {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            docs.submit_input("race".into(), "hello".into())
                .map_err(|e| e.to_string())?;
            if first && self.during_prepare {
                race(self.owner.clone(), self.origin);
            }
            let address = Address {
                scope: "test".into(),
                name: "proof-retry".into(),
                key: "".into(),
            };
            docs.get(&address).map_err(|e| e.to_string())?;
            docs.put(
                &address,
                serde_json::json!({"ok":true}).as_object().unwrap().clone(),
            )
            .map_err(|e| e.to_string())?;
            Ok(Step::Succeed(serde_json::json!(null)))
        }
    }
    for (replay_safe, during_prepare) in
        [(true, false), (false, false), (true, true), (false, true)]
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, origin, child) = task_input_reset_fork(&path);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "submit",
            replay_safe,
            Submit {
                calls: calls.clone(),
                owner: h.owner.clone(),
                origin,
                during_prepare,
            },
        );
        let mut scheduler = h.attach_tasks(registry).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::json!(null),
            },
            3,
        )
        .unwrap()
        .registered("submit".into(), replay_safe);
        scheduler.submit(&task).unwrap();
        if !during_prepare {
            race(h.owner.clone(), origin);
        }
        if during_prepare {
            assert!(scheduler.tick(0).unwrap());
        } else {
            let crate::task_scheduler::TickReservation::Invocation(invocation) =
                scheduler.reserve_tick(0).unwrap()
            else {
                panic!("expected task invocation");
            };
            let completed = invocation.execute().unwrap();
            let publication = scheduler.failure_publication();
            let admission = publication.admission().unwrap();
            assert_eq!(
                admission.permits_admission(&h.owner, child, Some(&task)),
                replay_safe
            );
            drop(admission);
            assert!(scheduler.settle_invocation(completed).unwrap());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = crate::task_store::TaskStore::new(&store);
        if replay_safe {
            assert_eq!(
                tasks.load(task.id()).unwrap().unwrap().task.state(),
                &TaskState::Ready
            );
        } else {
            assert!(
                matches!(tasks.load(task.id()).unwrap().unwrap().task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
            );
        }
        assert!(h.conversation(child).unwrap().submissions.is_empty());
        let address = Address {
            scope: "test".into(),
            name: "proof-retry".into(),
            key: "".into(),
        };
        assert!(Documents::new(&store).get(&address).unwrap().is_none());
        if replay_safe {
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert!(matches!(
                tasks.load(task.id()).unwrap().unwrap().task.state(),
                TaskState::Succeeded(_)
            ));
            assert_eq!(h.conversation(child).unwrap().submissions.len(), 1);
            assert!(Documents::new(&store).get(&address).unwrap().is_some());
        }
    }
}

#[test]
fn task_input_proof_store_failures_remain_storage_errors() {
    use crate::document::{Address, Documents, TaskDocuments};
    use crate::task::scheduler::{Handler, Registry, Step};
    use crate::task::{Checkpoint, Task, TaskOwner, TaskState};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn inject(transcript: bool, io_error: bool) {
        let error = if io_error {
            durable_store::Error::Storage(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                Some("injected I/O failure".into()),
            ))
        } else {
            durable_store::Error::LockPoisoned
        };
        let error = if transcript {
            crate::reset_manifest::LookupError::Transcript(crate::transcript::Error::Store(error))
        } else {
            crate::reset_manifest::LookupError::Store(error)
        };
        crate::reset_manifest::CANDIDATE_ERROR.with(|slot| *slot.borrow_mut() = Some(error));
    }
    struct Submit {
        during_prepare: bool,
        transcript: bool,
        io_error: bool,
        saw_storage: Arc<AtomicBool>,
    }
    impl Handler for Submit {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            let result = docs.submit_input("storage".into(), "hello".into());
            if !self.during_prepare {
                self.saw_storage.store(
                    matches!(result, Err(crate::document::Error::TaskInputStore(_))),
                    Ordering::SeqCst,
                );
                // A handler cannot turn an indeterminate storage failure into success.
                return Ok(Step::Succeed(serde_json::json!(null)));
            }
            result.map_err(|e| e.to_string())?;
            let address = Address {
                scope: "test".into(),
                name: "proof-storage".into(),
                key: "".into(),
            };
            docs.get(&address).map_err(|e| e.to_string())?;
            docs.put(
                &address,
                serde_json::json!({"ok":true}).as_object().unwrap().clone(),
            )
            .map_err(|e| e.to_string())?;
            inject(self.transcript, self.io_error);
            Ok(Step::Succeed(serde_json::json!(null)))
        }
    }
    assert!(matches!(
        reset_manifest_lookup_error(crate::reset_manifest::LookupError::Invalid),
        Error::Fork(crate::fork::Error::InvalidSnapshot)
    ));
    assert!(matches!(
        reset_manifest_lookup_error(crate::reset_manifest::LookupError::Transcript(
            crate::transcript::Error::Corrupt
        )),
        Error::Fork(crate::fork::Error::InvalidSnapshot)
    ));
    for (during_prepare, transcript, io_error) in [
        (false, false, false),
        (false, true, true),
        (true, false, true),
        (true, true, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, _, child) = task_input_reset_fork(&path);
        let record = h.owner.store().get(&Harness::key(child)).unwrap().unwrap();
        inject(transcript, io_error);
        let error = h
            .validate_task_input_context(child, &record.value)
            .unwrap_err();
        if io_error {
            assert!(
                matches!(error, crate::document::Error::Store(durable_store::Error::Storage(rusqlite::Error::SqliteFailure(code, _))) if code.extended_code == rusqlite::ffi::SQLITE_IOERR)
            );
        } else {
            assert!(matches!(
                error,
                crate::document::Error::Store(durable_store::Error::LockPoisoned)
            ));
        }
        let saw_storage = Arc::new(AtomicBool::new(false));
        let mut registry = Registry::default();
        registry.register(
            "submit",
            true,
            Submit {
                during_prepare,
                transcript,
                io_error,
                saw_storage: saw_storage.clone(),
            },
        );
        let mut scheduler = h.attach_tasks(registry).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::json!(null),
            },
            3,
        )
        .unwrap()
        .registered("submit".into(), true);
        scheduler.submit(&task).unwrap();
        if !during_prepare {
            inject(transcript, io_error);
        }
        let error = scheduler.tick(0).unwrap_err();
        let storage = match error {
            crate::task_scheduler::Error::Document(crate::document::Error::TaskInputStore(
                error,
            )) => error,
            crate::task_scheduler::Error::Document(crate::document::Error::Store(error)) => {
                Arc::new(error)
            }
            other => panic!("expected storage error, got {other:?}"),
        };
        if io_error {
            assert!(
                matches!(&*storage, durable_store::Error::Storage(rusqlite::Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_IOERR)
            );
        } else {
            assert!(matches!(&*storage, durable_store::Error::LockPoisoned));
        }
        assert_eq!(saw_storage.load(Ordering::SeqCst), !during_prepare);
        let store = durable_store::Store::open(&path).unwrap();
        let task = crate::task_store::TaskStore::new(&store)
            .load(task.id())
            .unwrap()
            .unwrap();
        assert_eq!(task.task.state(), &TaskState::Running);
        assert!(h.conversation(child).unwrap().submissions.is_empty());
        let address = Address {
            scope: "test".into(),
            name: "proof-storage".into(),
            key: "".into(),
        };
        assert!(Documents::new(&store).get(&address).unwrap().is_none());
        assert!(matches!(
            scheduler.tick(0),
            Err(crate::task_scheduler::Error::Poisoned)
        ));
    }
}

#[test]
fn task_input_rejects_tampered_reset_proof_as_invalid() {
    for _ in 0..16 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, _, child) = task_input_reset_fork(&path);
        let record = h.owner.store().get(&Harness::key(child)).unwrap().unwrap();
        // The store can contain several reset manifests, and UUID ordering does
        // not identify the one reachable from this fork. Capture the actual proof
        // dependencies before tampering with one of them.
        let scope = ResetProofScope::enter();
        h.validate_task_input_context(child, &record.value).unwrap();
        let checks = cached_reset_proof_checks(&h.owner);
        drop(scope);
        let key = checks
            .into_iter()
            .find(|check| {
                check.key.starts_with(b"runtime/reset-manifest/")
                    && matches!(check.expected, Expected::Version(_))
            })
            .expect("fork validation must authenticate a reset manifest")
            .key;
        let entry = h.owner.store().get(&key).unwrap().unwrap();
        h.owner
            .store()
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key,
                    value: b"{}".to_vec(),
                }],
            )
            .unwrap();
        let result = h.validate_task_input_context(child, &record.value);
        assert!(
            matches!(
                result,
                Err(crate::document::Error::InvalidSessionAdmission(_))
            ),
            "tampered reset proof must be rejected as invalid: {result:?}"
        );
    }
}
