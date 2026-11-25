//! Commit-time authority for work dispatched by a live conversation tool attempt.
use crate::runtime::{Conversation, Harness, ToolAttempt};
use durable_store::{Check, Expected, Mutation, Store};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Binding {
    pub conversation: Uuid,
    pub attempt: ToolAttempt,
    pub suspended: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("nested root attempt is no longer active")]
    Stale,
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
thread_local! {
    static BEFORE_COMMIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

impl Binding {
    /// Retry only a root-version conflict. Every other check remains unchanged.
    pub fn commit(
        &self,
        store: &Store,
        checks: &[Check],
        mutations: &[Mutation],
    ) -> Result<(), Error> {
        if self.conversation.is_nil()
            || self.attempt.operation_id.is_nil()
            || self.attempt.attempt_id.is_nil()
        {
            return Err(Error::Stale);
        }
        let key = Harness::key(self.conversation);
        // Never rebase an independently captured root check. The caller must
        // resolve that overlap explicitly rather than weaken or spin on it.
        if checks.iter().any(|check| check.key == key) {
            return Err(durable_store::Error::Conflict { key }.into());
        }
        loop {
            let entry = store.get(&key)?.ok_or(Error::Stale)?;
            // Conversation decoding accepts historical snapshots without an abort
            // field. Such a default is not proof of an un-aborted executor.
            #[derive(serde::Deserialize)]
            struct AbortEvidence {
                abort_requested: bool,
            }
            let evidence: AbortEvidence =
                serde_json::from_slice(&entry.value).map_err(|_| Error::Stale)?;
            if evidence.abort_requested {
                return Err(Error::Stale);
            }
            let state: Conversation = serde_json::from_slice(&entry.value)?;
            let matches = if self.suspended {
                state.suspended_nested_attempt() == Some(self.attempt)
            } else {
                state.active_tool_attempt() == Some(self.attempt)
            };
            if state.id != self.conversation || !matches {
                return Err(Error::Stale);
            }
            #[cfg(test)]
            BEFORE_COMMIT.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let mut all = checks.to_vec();
            all.push(Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            });
            match store.commit(&all, mutations) {
                Err(durable_store::Error::Conflict { key: conflict }) if conflict == key => {
                    continue;
                }
                result => return result.map_err(Error::from),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        recovery::UncertainPolicy,
        runtime::{Config, Harness, Options},
    };
    use async_trait::async_trait;
    use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
    use serde_json::json;
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    struct Stub;
    #[async_trait]
    impl Model for Stub {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
            unreachable!()
        }
    }
    fn fixture(path: &std::path::Path) -> (Rc<RefCell<Harness>>, Uuid, Binding) {
        let mut h = Harness::open_paused(
            path,
            Options {
                model: Arc::new(Stub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: UncertainPolicy::RequireReconciliation,
            },
        )
        .unwrap();
        let id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 10,
            })
            .unwrap();
        let store = Store::open(path).unwrap();
        let key = Harness::key(id);
        let old = store.get(&key).unwrap().unwrap();
        let attempt = ToolAttempt {
            operation_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
        };
        let mut value: serde_json::Value = serde_json::from_slice(&old.value).unwrap();
        value["phase"] = json!({"ToolRunning": {"turns": 1, "calls": [], "index": 0,
            "operation_id": attempt.operation_id, "replay": "Safe", "arguments": {}, "restarts": 0, "replay_ready": false}});
        value["tool_attempt"] = serde_json::to_value(attempt).unwrap();
        store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(old.version),
                }],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        (
            Rc::new(RefCell::new(h)),
            id,
            Binding {
                conversation: id,
                attempt,
                suspended: false,
            },
        )
    }
    fn rewrite(path: &std::path::Path, id: Uuid, change: impl FnOnce(&mut serde_json::Value)) {
        let store = Store::open(path).unwrap();
        let key = Harness::key(id);
        let entry = store.get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        change(&mut value);
        store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
    }
    #[test]
    fn retries_only_unrelated_root_changes_and_rejects_retired_attempts() {
        for case in [0, 1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (h, id, binding) = fixture(&path);
            let store = Store::open(&path).unwrap();
            let task_path = path.clone();
            let queued = h.clone();
            BEFORE_COMMIT.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || match case {
                    0 => {
                        queued
                            .borrow_mut()
                            .submit(id, None, "unrelated".into())
                            .unwrap();
                    }
                    1 => rewrite(&task_path, id, |v| v["abort_requested"] = json!(true)),
                    _ => rewrite(&task_path, id, |v| {
                        v["tool_attempt"]["attempt_id"] = json!(Uuid::new_v4())
                    }),
                }))
            });
            let key = b"test/nested-effect".to_vec();
            let result = binding.commit(
                &store,
                &[Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value: b"committed".to_vec(),
                }],
            );
            if case == 0 {
                assert!(result.is_ok(), "{result:?}");
                assert_eq!(store.get(&key).unwrap().unwrap().value, b"committed");
                assert_eq!(h.borrow().conversation(id).unwrap().submissions.len(), 1);
            } else {
                assert!(matches!(result, Err(Error::Stale)), "{result:?}");
                assert!(store.get(&key).unwrap().is_none());
            }
        }
    }
    #[tokio::test]
    async fn nested_result_is_not_published_after_root_abort_and_reopen() {
        use crate::{nested::NestedHost, recovery::ReplayPolicy, runtime::RegisteredTool};
        use pi_rs_core::{CancellationToken, Outcome, Tool, ToolContext, ToolSpec};
        struct AbortChild {
            path: std::path::PathBuf,
            id: Uuid,
        }
        #[async_trait]
        impl Tool for AbortChild {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
                rewrite(&self.path, self.id, |v| v["abort_requested"] = json!(true));
                Outcome::Succeeded("must not publish".into())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, id, binding) = fixture(&path);
        drop(h);
        let owner = Arc::new(durable_store::owner::OwnedStore::open(&path).unwrap());
        let host = NestedHost::new_with_schemas(
            owner.clone(),
            binding.attempt.operation_id,
            vec![RegisteredTool {
                tool: Arc::new(AbortChild {
                    path: path.clone(),
                    id,
                }),
                replay: ReplayPolicy::Safe,
            }],
            CancellationToken::new(),
            std::time::Duration::from_secs(5),
            4,
            (Default::default(), None, Some(binding.clone())),
        )
        .unwrap();
        let result = host.call_keyed("key", "child", json!({})).await;
        assert!(matches!(result, Outcome::Unknown(_)), "{result:?}");
        let prefix = format!("nested/{}/", binding.attempt.operation_id);
        let entries = owner.store().list(prefix.as_bytes()).unwrap();
        assert_eq!(entries.len(), 1);
        let record: serde_json::Value = serde_json::from_slice(&entries[0].1.value).unwrap();
        assert!(record["outcome"].is_null());
        drop(host);
        drop(owner);
        let reopened = Store::open(&path).unwrap();
        let entries = reopened.list(prefix.as_bytes()).unwrap();
        let record: serde_json::Value = serde_json::from_slice(&entries[0].1.value).unwrap();
        assert!(record["outcome"].is_null());
    }

    #[tokio::test]
    async fn nested_memo_and_report_do_not_commit_after_attempt_replacement() {
        use crate::extension::Memo;
        use std::sync::atomic::AtomicBool;
        for is_report in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (h, id, binding) = fixture(&path);
            drop(h);
            let owner = Arc::new(durable_store::owner::OwnedStore::open(&path).unwrap());
            let task_path = path.clone();
            BEFORE_COMMIT.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    rewrite(&task_path, id, |v| {
                        v["tool_attempt"]["attempt_id"] = json!(Uuid::new_v4())
                    });
                }))
            });
            if is_report {
                let key = format!("runtime/extension-report/{id}/test/after_tool").into_bytes();
                let report = crate::runtime::ExtensionReportBatch {
                    conversation_id: id,
                    operation_id: Uuid::new_v4(),
                    phase: "after_tool".into(),
                    reports: vec![],
                };
                assert!(
                    crate::report::index_with_fence(
                        owner.store(),
                        key.clone(),
                        &report,
                        false,
                        Some(&binding)
                    )
                    .is_err()
                );
                assert!(owner.store().get(&key).unwrap().is_none());
            } else {
                let operation = Uuid::new_v4();
                let memo = crate::extension::DurableMemo::new(
                    owner.clone(),
                    operation,
                    Arc::new(AtomicBool::new(false)),
                )
                .with_nested_fence(Some(binding));
                assert!(memo.get_or_insert("key", json!(1)).await.is_err());
                assert!(
                    owner
                        .store()
                        .list(format!("runtime/extension-memo/{operation}/").as_bytes())
                        .unwrap()
                        .is_empty()
                );
            }
            drop(owner);
            let reopened = Store::open(&path).unwrap();
            assert!(
                reopened
                    .list(b"runtime/extension-report/")
                    .unwrap()
                    .is_empty()
            );
            assert!(
                reopened
                    .list(b"runtime/extension-memo/")
                    .unwrap()
                    .is_empty()
            );
        }
    }
    #[tokio::test]
    async fn nested_diagnostic_cannot_publish_after_attempt_replacement() {
        use crate::{
            keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
            nested::NestedHost,
            recovery::ReplayPolicy,
        };
        use pi_rs_core::{
            CancellationToken, ExtensionState, Outcome, ToolContext, ToolReply, ToolSpec,
        };
        struct Child {
            path: std::path::PathBuf,
            id: Uuid,
        }
        #[async_trait]
        impl KeyedTool for Child {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn invoke(
                &self,
                _: serde_json::Value,
                _: ToolContext,
                _: Option<&ExtensionState>,
                host: KeyedHost,
            ) -> ToolReply {
                rewrite(&self.path, self.id, |v| {
                    v["tool_attempt"]["attempt_id"] = json!(Uuid::new_v4())
                });
                assert!(
                    host.diagnostic(crate::runtime::ToolDiagnostic {
                        severity: crate::runtime::DiagnosticSeverity::Warn,
                        code: None,
                        message: "must not publish".into()
                    })
                    .is_err()
                );
                Outcome::Succeeded("ignored".into()).into()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, id, binding) = fixture(&path);
        drop(h);
        let owner = Arc::new(durable_store::owner::OwnedStore::open(&path).unwrap());
        let tool = Arc::new(KeyedToolAdapter::new(Child {
            path: path.clone(),
            id,
        }));
        let host = NestedHost::new_with_schemas(
            owner.clone(),
            binding.attempt.operation_id,
            vec![tool.register(ReplayPolicy::Safe)],
            CancellationToken::new(),
            std::time::Duration::from_secs(5),
            4,
            (Default::default(), None, Some(binding.clone())),
        )
        .unwrap();
        assert!(matches!(
            host.call_keyed("key", "child", json!({})).await,
            Outcome::Unknown(_)
        ));
        let entries = owner
            .store()
            .list(format!("nested/{}/", binding.attempt.operation_id).as_bytes())
            .unwrap();
        let record: serde_json::Value = serde_json::from_slice(&entries[0].1.value).unwrap();
        assert!(record.get("diagnostics").is_none());
        assert!(record["outcome"].is_null());
    }
    #[test]
    fn legacy_abort_evidence_and_independent_conflicts_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (_h, id, binding) = fixture(&path);
        let store = Store::open(&path).unwrap();
        let key = b"test/fence".to_vec();
        let task_path = path.clone();
        let changed = key.clone();
        BEFORE_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                Store::open(&task_path)
                    .unwrap()
                    .commit(
                        &[],
                        &[Mutation::Put {
                            key: changed,
                            value: vec![1],
                        }],
                    )
                    .unwrap();
            }))
        });
        let result = binding.commit(
            &store,
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key: key.clone(),
                value: vec![2],
            }],
        );
        assert!(matches!(
            result,
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(store.get(&key).unwrap().unwrap().value, vec![1]);
        rewrite(&path, id, |v| {
            v.as_object_mut().unwrap().remove("abort_requested");
        });
        assert!(matches!(
            binding.commit(&store, &[], &[]),
            Err(Error::Stale)
        ));
    }
    #[test]
    fn suspended_binding_retries_queued_input_but_rejects_resume_and_mismatched_operation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, id, mut binding) = fixture(&path);
        let attempt = binding.attempt;
        rewrite(&path, id, |v| {
            v["phase"] = json!({"Blocked":{"turns":1,"calls":[],"index":0,"operation_id":attempt.operation_id,"reason":"parked","arguments":{}}});
            v["nested_resume"] = json!({"operation_id":attempt.operation_id,"restarts":0,"dispatch_identity":"Other","explicit_suspend":true});
        });
        binding.suspended = true;
        let store = Store::open(&path).unwrap();
        let queued = h.clone();
        BEFORE_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                queued
                    .borrow_mut()
                    .submit(id, None, "queued".into())
                    .unwrap();
            }))
        });
        let key = b"test/suspended".to_vec();
        binding
            .commit(
                &store,
                &[Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value: vec![1],
                }],
            )
            .unwrap();
        assert_eq!(store.get(&key).unwrap().unwrap().value, vec![1]);
        rewrite(&path, id, |v| {
            v["nested_resume"]["operation_id"] = json!(Uuid::new_v4())
        });
        assert!(matches!(
            binding.commit(&store, &[], &[]),
            Err(Error::Stale)
        ));
    }

    struct AnyRaceFixture {
        harness: Rc<RefCell<Harness>>,
        store: Store,
        binding: Binding,
        middle: Uuid,
        a: Uuid,
        b: Uuid,
        b_key: Vec<u8>,
    }

    fn any_race_fixture(path: &std::path::Path) -> AnyRaceFixture {
        use pi_rs_core::Outcome;
        let (harness, id, mut binding) = fixture(path);
        let attempt = binding.attempt;
        rewrite(path, id, |v| {
            v["phase"] = json!({"Blocked":{"turns":1,"calls":[],"index":0,"operation_id":attempt.operation_id,"reason":"parked","arguments":{}}});
            v["nested_resume"] = json!({"operation_id":attempt.operation_id,"restarts":0,"dispatch_identity":"Other","explicit_suspend":true});
        });
        binding.suspended = true;
        let store = Store::open(path).unwrap();
        let root = attempt.operation_id;
        let middle = Uuid::new_v4();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let suspension = Uuid::new_v4();
        let a_key = format!("nested/{root}/{middle}/k61").into_bytes();
        let b_key = format!("nested/{root}/{middle}/k62").into_bytes();
        let parent = json!({"name":"middle","arguments":{},"prepared":{},"policy":"Safe","operation_id":middle,"outcome":Outcome::Pending {reference:format!("nested-child-park:{suspension}")},"child_park":{"suspension":suspension,"root_attempt":attempt.attempt_id,"leaf_key":a_key,"additional_leaf_keys":[b_key],"any":true}});
        let a_record = json!({"name":"child","arguments":{},"prepared":{},"policy":"Safe","operation_id":a,"hooks_required":true,"outcome":Outcome::Succeeded("hook:a".into()),"resolution":{"reference":"a","outcome":Outcome::Succeeded("raw-a".into()),"finalized_outcome":Outcome::Succeeded("hook:a".into())}});
        let b_record = json!({"name":"child","arguments":{},"prepared":{},"policy":"Safe","operation_id":b,"hooks_required":true,"outcome":Outcome::Pending {reference:"b".into()}});
        let batch = crate::nested_suspension::prepare_retired(
            id,
            middle,
            suspension,
            attempt.attempt_id,
            json!({}),
            vec![
                crate::nested_suspension::PendingJob {
                    operation_id: a,
                    reference: "a".into(),
                },
                crate::nested_suspension::PendingJob {
                    operation_id: b,
                    reference: "b".into(),
                },
            ],
        )
        .unwrap();
        store.commit(&batch.checks, &batch.mutations).unwrap();
        store
            .commit(
                &[],
                &[
                    Mutation::Put {
                        key: format!("nested/{root}/{root}/k6d").into_bytes(),
                        value: serde_json::to_vec(&parent).unwrap(),
                    },
                    Mutation::Put {
                        key: a_key,
                        value: serde_json::to_vec(&a_record).unwrap(),
                    },
                    Mutation::Put {
                        key: b_key.clone(),
                        value: serde_json::to_vec(&b_record).unwrap(),
                    },
                ],
            )
            .unwrap();
        AnyRaceFixture {
            harness,
            store,
            binding,
            middle,
            a,
            b,
            b_key,
        }
    }

    #[test]
    fn any_freeze_retries_if_reconciliation_commits_before_its_cas() {
        use pi_rs_core::Outcome;
        let dir = tempfile::tempdir().unwrap();
        let f = any_race_fixture(&dir.path().join("db"));
        let id = f.binding.conversation;
        let root = f.binding.attempt.operation_id;
        let b = f.b;
        let host = f.harness.clone();
        // Real freeze_any_wake has read both leaves. Commit B's receipt in the
        // host at the boundary immediately before its version-checked freeze.
        BEFORE_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                host.borrow_mut()
                    .reconcile_nested_pending(id, root, b, "b", Outcome::Succeeded("raw-b".into()))
                    .unwrap();
            }))
        });
        assert!(matches!(
            crate::nested::freeze_any_wake(&f.store, &f.binding).unwrap(),
            crate::nested::AnyWake::Retry
        ));
        let snapshot = crate::nested_suspension::load(&f.store, id, f.middle)
            .unwrap()
            .unwrap();
        assert!(snapshot.frozen_ended().is_none());
        let staged: serde_json::Value =
            serde_json::from_slice(&f.store.get(&f.b_key).unwrap().unwrap().value).unwrap();
        assert_eq!(
            staged["staged_outcome"],
            json!(Outcome::Succeeded("raw-b".into()))
        );
        assert!(matches!(
            crate::nested::freeze_any_wake(&f.store, &f.binding).unwrap(),
            crate::nested::AnyWake::Retry
        ));
    }

    #[test]
    fn abort_after_any_freeze_accepts_authoritative_pending_receipt() {
        use pi_rs_core::Outcome;
        for cancelled in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let f = any_race_fixture(&dir.path().join("db"));
            let id = f.binding.conversation;
            let root = f.binding.attempt.operation_id;
            assert!(
                matches!(crate::nested::freeze_any_wake(&f.store,&f.binding).unwrap(),crate::nested::AnyWake::Ready {ended,..} if ended == vec![f.a])
            );
            f.harness.borrow_mut().request_abort(id).unwrap();
            if cancelled {
                f.harness
                    .borrow_mut()
                    .cancel_nested_pending(id, root, f.b, "b", "cancelled".into())
                    .unwrap();
            } else {
                f.harness
                    .borrow_mut()
                    .reconcile_nested_pending(
                        id,
                        root,
                        f.b,
                        "b",
                        Outcome::Succeeded("raw-b".into()),
                    )
                    .unwrap();
            }
            let record: serde_json::Value =
                serde_json::from_slice(&f.store.get(&f.b_key).unwrap().unwrap().value).unwrap();
            let expected = if cancelled {
                Outcome::Failed("cancelled".into())
            } else {
                Outcome::Succeeded("raw-b".into())
            };
            assert_eq!(record["staged_outcome"], json!(expected));
            assert_eq!(record["resolution"]["cancelled"], json!(cancelled));
            assert!(f.harness.borrow().conversation(id).unwrap().abort_requested);
            let snapshot = crate::nested_suspension::load(&f.store, id, f.middle)
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.frozen_ended(), Some(&[f.a][..]));
        }
    }
    #[tokio::test]
    async fn replaced_root_cannot_admit_a_nested_intent() {
        use crate::{nested::NestedHost, recovery::ReplayPolicy, runtime::RegisteredTool};
        use pi_rs_core::{CancellationToken, Outcome, Tool, ToolContext, ToolSpec};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Child(Arc<AtomicUsize>);
        #[async_trait]
        impl Tool for Child {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
                self.0.fetch_add(1, Ordering::SeqCst);
                Outcome::Succeeded("unexpected".into())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, id, binding) = fixture(&path);
        drop(h);
        let owner = Arc::new(durable_store::owner::OwnedStore::open(&path).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let host = NestedHost::new_with_schemas(
            owner.clone(),
            binding.attempt.operation_id,
            vec![RegisteredTool {
                tool: Arc::new(Child(calls.clone())),
                replay: ReplayPolicy::Safe,
            }],
            CancellationToken::new(),
            std::time::Duration::from_secs(5),
            4,
            (Default::default(), None, Some(binding.clone())),
        )
        .unwrap();
        let task_path = path.clone();
        BEFORE_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                rewrite(&task_path, id, |v| {
                    v["tool_attempt"]["attempt_id"] = json!(Uuid::new_v4())
                });
            }))
        });
        assert!(matches!(
            host.call_keyed("key", "child", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            owner
                .store()
                .list(format!("nested/{}/", binding.attempt.operation_id).as_bytes())
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn retired_intermediate_claim_fences_a_stale_effect_with_live_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (_h, id, binding) = fixture(&path);
        let store = Store::open(&path).unwrap();
        let child = Uuid::new_v4();
        let old = Uuid::new_v4();
        let batch = crate::nested_suspension::prepare_retired(
            id,
            child,
            Uuid::new_v4(),
            old,
            json!({"saved": 1}),
            vec![crate::nested_suspension::PendingJob {
                operation_id: Uuid::new_v4(),
                reference: "opaque".into(),
            }],
        )
        .unwrap();
        binding
            .commit(&store, &batch.checks, &batch.mutations)
            .unwrap();
        let retired = crate::nested_suspension::load(&store, id, child)
            .unwrap()
            .unwrap();
        let executor = Uuid::new_v4();
        let batch = retired.claim(executor).unwrap();
        binding
            .commit(&store, &batch.checks, &batch.mutations)
            .unwrap();
        let active = crate::nested_suspension::load(&store, id, child)
            .unwrap()
            .unwrap();
        let claim = active.claim_check(executor).unwrap();
        let task_path = path.clone();
        BEFORE_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let store = Store::open(task_path).unwrap();
                let batch = crate::nested_suspension::load(&store, id, child)
                    .unwrap()
                    .unwrap()
                    .retire(executor)
                    .unwrap();
                store.commit(&batch.checks, &batch.mutations).unwrap();
            }))
        });
        let effect = b"test/stale-child".to_vec();
        let result = binding.commit(
            &store,
            &[claim],
            &[Mutation::Put {
                key: effect.clone(),
                value: vec![1],
            }],
        );
        assert!(matches!(
            result,
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(store.get(&effect).unwrap().is_none());
    }

    #[tokio::test]
    async fn retired_intermediate_claim_rejects_memo_write() {
        use crate::extension::Memo;
        use std::sync::atomic::AtomicBool;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, id, binding) = fixture(&path);
        drop(h);
        let owner = Arc::new(durable_store::owner::OwnedStore::open(&path).unwrap());
        let child = Uuid::new_v4();
        let batch = crate::nested_suspension::prepare_retired(
            id,
            child,
            Uuid::new_v4(),
            Uuid::new_v4(),
            json!({}),
            vec![crate::nested_suspension::PendingJob {
                operation_id: Uuid::new_v4(),
                reference: "job".into(),
            }],
        )
        .unwrap();
        binding
            .commit(owner.store(), &batch.checks, &batch.mutations)
            .unwrap();
        let retired = crate::nested_suspension::load(owner.store(), id, child)
            .unwrap()
            .unwrap();
        let executor = Uuid::new_v4();
        let batch = retired.claim(executor).unwrap();
        binding
            .commit(owner.store(), &batch.checks, &batch.mutations)
            .unwrap();
        let active = crate::nested_suspension::load(owner.store(), id, child)
            .unwrap()
            .unwrap();
        let memo = crate::extension::DurableMemo::new(
            owner.clone(),
            child,
            Arc::new(AtomicBool::new(false)),
        )
        .with_nested_fence(Some(binding.clone()))
        .with_nested_claim(Some(active.claim_check(executor).unwrap()));
        assert_eq!(
            memo.get_or_insert("existing", json!(1)).await.unwrap(),
            json!(1)
        );
        let batch = active.retire(executor).unwrap();
        binding
            .commit(owner.store(), &batch.checks, &batch.mutations)
            .unwrap();
        assert!(memo.get_or_insert("stale", json!(1)).await.is_err());
        assert!(memo.get("existing").await.is_err());
        assert!(memo.get_or_insert("existing", json!(2)).await.is_err());
        assert_eq!(
            owner
                .store()
                .list(format!("runtime/extension-memo/{child}/").as_bytes())
                .unwrap()
                .len(),
            1
        );
    }
}
