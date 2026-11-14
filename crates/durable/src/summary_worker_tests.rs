use super::*;
use crate::summary_store::{State, SummaryStore};
use futures_util::{StreamExt, stream};
use pi_rs_core::{ModelEvent, ModelMetadata, ModelRequest, ModelStream, Usage, UsageReport};
use std::sync::{Mutex, atomic::AtomicBool};
use tokio::sync::Semaphore;

struct TestModel {
    calls: Mutex<Vec<(Uuid, Uuid)>>,
    accepted: Mutex<HashMap<Uuid, UsageReport>>,
    settlements: Mutex<Vec<(Uuid, UsageReport)>>,
    started: Semaphore,
    release: Semaphore,
    settlement_started: Semaphore,
    settlement_release: Semaphore,
    block_settlement: AtomicBool,
    fail_ack: AtomicBool,
    marker: Option<std::path::PathBuf>,
    settlement_marker: Option<std::path::PathBuf>,
}

impl TestModel {
    fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            accepted: Mutex::new(HashMap::new()),
            settlements: Mutex::new(Vec::new()),
            started: Semaphore::new(0),
            release: Semaphore::new(permits),
            settlement_started: Semaphore::new(0),
            settlement_release: Semaphore::new(0),
            block_settlement: AtomicBool::new(false),
            fail_ack: AtomicBool::new(false),
            marker: None,
            settlement_marker: None,
        })
    }
    async fn settle(&self, id: Uuid, report: &UsageReport) -> pi_rs_core::Result<()> {
        self.settlements.lock().unwrap().push((id, report.clone()));
        if let Some(previous) = self.accepted.lock().unwrap().insert(id, report.clone()) {
            assert_eq!(previous, *report);
        }
        if let Some(path) = &self.settlement_marker {
            let temporary = path.with_extension("tmp");
            std::fs::write(&temporary, serde_json::to_vec(&(id, report)).unwrap()).unwrap();
            std::fs::rename(temporary, path).unwrap();
        }
        self.settlement_started.add_permits(1);
        if self.block_settlement.load(Ordering::SeqCst) {
            self.settlement_release.acquire().await.unwrap().forget();
        }
        if self.fail_ack.swap(false, Ordering::SeqCst) {
            return Err(pi_rs_core::Error::Model("lost acknowledgement".into()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Model for TestModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        panic!("worker must use summary stream")
    }
    async fn stream_for_summary(
        &self,
        request: Uuid,
        session: Uuid,
        _: ModelRequest,
    ) -> pi_rs_core::Result<ModelStream> {
        self.calls.lock().unwrap().push((request, session));
        if let Some(path) = &self.marker {
            std::fs::write(path, b"running").unwrap();
        }
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("summary".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: Some(Usage {
                    input_tokens: 5,
                    output_tokens: 2,
                }),
            }),
        ])
        .boxed())
    }
    async fn record_usage(&self, id: Uuid, report: &UsageReport) -> pi_rs_core::Result<()> {
        self.settle(id, report).await
    }
    async fn record_usage_with_metadata(
        &self,
        id: Uuid,
        report: &UsageReport,
        _: &ModelMetadata,
    ) -> pi_rs_core::Result<()> {
        self.settle(id, report).await
    }
}

fn opts(model: Arc<TestModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn create(scheduler: &Scheduler, id: Uuid, session: Uuid) {
    let preparation = crate::compact::prepare(
        &[
            Message::User {
                content: "old".into(),
            },
            Message::User {
                content: "recent".into(),
            },
        ],
        "test",
        64,
        0,
        None,
    )
    .unwrap()
    .unwrap();
    SummaryStore::new(scheduler.harness.owner.store())
        .create(id, session, preparation)
        .unwrap();
}
fn state(scheduler: &Scheduler, id: Uuid) -> crate::summary_store::Snapshot {
    SummaryStore::new(scheduler.harness.owner.store())
        .get(id)
        .unwrap()
        .unwrap()
}
async fn started(model: &TestModel) {
    tokio::time::timeout(Duration::from_secs(5), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

fn placement_conversation(scheduler: &mut Scheduler) -> Uuid {
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    let (mut state, version) = scheduler.harness.load(id).unwrap();
    state.messages.extend([
        Message::User {
            content: "old".into(),
        },
        Message::User {
            content: "recent".into(),
        },
    ]);
    scheduler
        .harness
        .save(&state, Expected::Version(version))
        .unwrap();
    id
}

#[tokio::test]
async fn completed_summary_places_once_after_reopen_and_preserves_occurrence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let before = scheduler.harness.conversation(conversation).unwrap();
    let occurrence = scheduler
        .harness
        .resolved_occurrences(conversation, before.transcript.unwrap())
        .unwrap()[1]
        .1
        .clone();
    let id = Uuid::new_v4();
    assert!(
        scheduler
            .create_independent_summary(id, conversation, 64, 0)
            .unwrap()
    );
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    let after = scheduler.harness.conversation(conversation).unwrap();
    assert_eq!(after.messages.len(), 2);
    assert!(
        matches!(&after.messages[0], Message::User { content } if content.contains("<summary>\nsummary"))
    );
    assert!(matches!(&after.messages[1], Message::User { content } if content == "recent"));
    let retained = scheduler
        .harness
        .resolved_occurrences(conversation, after.transcript.unwrap())
        .unwrap()[1]
        .1
        .clone();
    assert_eq!(retained, occurrence);
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, opts(model)).unwrap();
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages
            .len(),
        2
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn placement_kill_child() {
    let Ok(dir) = std::env::var("PI_SUMMARY_PLACEMENT_KILL_DIR") else {
        return;
    };
    let dir = std::path::Path::new(&dir);
    let mut scheduler = Scheduler::open(dir.join("db"), opts(TestModel::new(1))).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = Uuid::new_v4();
    scheduler
        .create_independent_summary(id, conversation, 64, 0)
        .unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    std::fs::write(dir.join("ids"), format!("{id} {conversation}")).unwrap();
    std::fs::write(dir.join("ready"), b"ready").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_completed_worker_places_once_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::summary_worker_tests::placement_kill_child",
        ])
        .env("PI_SUMMARY_PLACEMENT_KILL_DIR", dir.path())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.path().join("ready").exists() {
        if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("summary child did not complete");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let ids = std::fs::read_to_string(dir.path().join("ids")).unwrap();
    let mut ids = ids
        .split_whitespace()
        .map(|id| Uuid::parse_str(id).unwrap());
    let id = ids.next().unwrap();
    let conversation = ids.next().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(TestModel::new(0))).unwrap();
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages
            .len(),
        2
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_context_is_terminally_stale_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = Uuid::new_v4();
    scheduler
        .create_independent_summary(id, conversation, 64, 0)
        .unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let (mut state, version) = scheduler.harness.load(conversation).unwrap();
    state.messages.push(Message::User {
        content: "new history".into(),
    });
    scheduler
        .harness
        .save(&state, Expected::Version(version))
        .unwrap();
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Stale
    );
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages,
        state.messages
    );
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, opts(model)).unwrap();
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Stale
    );
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages,
        state.messages
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn competing_results_have_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(2);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    scheduler
        .create_independent_summary(first, conversation, 64, 0)
        .unwrap();
    scheduler
        .create_independent_summary(second, conversation, 64, 0)
        .unwrap();
    scheduler.schedule_summary(first).unwrap();
    scheduler.schedule_summary(second).unwrap();
    scheduler.wait_summary(first).await.unwrap();
    scheduler.wait_summary(second).await.unwrap();
    assert_eq!(
        scheduler.place_summary(first).unwrap(),
        SummaryPlacement::Placed
    );
    let messages = scheduler
        .harness
        .conversation(conversation)
        .unwrap()
        .messages;
    assert_eq!(
        scheduler.place_summary(second).unwrap(),
        SummaryPlacement::Stale
    );
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages,
        messages
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn staged_placement_loses_to_concurrent_conversation_commit() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = Uuid::new_v4();
    scheduler
        .create_independent_summary(id, conversation, 64, 0)
        .unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let owner = scheduler.harness.owner.clone();
    let store = SummaryStore::new(owner.store());
    let snap = store.get(id).unwrap().unwrap();
    let State::Completed { result, .. } = snap.operation.state() else {
        panic!("expected completed")
    };
    let (mut candidate, version) = scheduler
        .harness
        .load_for_summary_placement(conversation)
        .unwrap();
    candidate.messages = result
        .apply_in_context(&candidate.messages, candidate.context_generation)
        .unwrap();
    candidate.context_generation = Some(Uuid::new_v4());
    let mut batch = scheduler
        .harness
        .prepare_conversation_save_inner(
            &candidate,
            Expected::Version(version),
            None,
            &[],
            &[],
            Some(result),
        )
        .unwrap();
    let (checks, mutations) = store.placement_batch(&snap, true).unwrap();
    batch
        .extend(PreparedConversationSave {
            owner,
            checks,
            mutations,
        })
        .unwrap();
    let (mut changed, version) = scheduler.harness.load(conversation).unwrap();
    changed.messages.push(Message::User {
        content: "raced".into(),
    });
    scheduler
        .harness
        .save(&changed, Expected::Version(version))
        .unwrap();
    assert!(matches!(
        scheduler.harness.commit_conversation_save(batch),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(matches!(
        state(&scheduler, id).operation.state(),
        State::Completed { .. }
    ));
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Stale
    );
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages,
        changed.messages
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn creation_rejects_a_changed_conversation_version() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let (mut state, version) = scheduler
        .harness
        .load_for_summary_placement(conversation)
        .unwrap();
    let preparation = crate::compact::prepare(&state.messages, "test", 64, 0, None)
        .unwrap()
        .unwrap()
        .bind_context(state.context_generation.unwrap());
    state.messages.push(Message::User {
        content: "raced".into(),
    });
    scheduler
        .harness
        .save(&state, Expected::Version(version))
        .unwrap();
    let id = Uuid::new_v4();
    let store = SummaryStore::new(scheduler.harness.owner.store());
    assert!(matches!(
        store.create_bound(
            id,
            conversation,
            preparation,
            Harness::key(conversation),
            version
        ),
        Err(crate::summary_store::Error::Conflict)
    ));
    assert!(store.get(id).unwrap().is_none());
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_over_limit_is_rejected_without_creating_operation() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let owner = scheduler.harness.owner.clone();
    let (mut state, version) = scheduler.harness.load(conversation).unwrap();
    let transcript = crate::transcript::Transcript::new(owner.store(), conversation);
    for _ in 0..SUMMARY_MAX_HISTORY_RECORDS {
        let (batch, cursor) = transcript
            .prepare_write(
                &serde_json::to_value(TranscriptChange::Append(vec![])).unwrap(),
                false,
            )
            .unwrap();
        owner
            .store()
            .commit(&batch.checks, &batch.mutations)
            .unwrap();
        state.transcript = Some(cursor);
    }
    state.messages.clear();
    let key = Harness::key(conversation);
    owner
        .store()
        .commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&state).unwrap(),
            }],
        )
        .unwrap();
    let id = Uuid::new_v4();
    assert!(matches!(
        scheduler.create_independent_summary(id, conversation, 64, 0),
        Err(Error::Invalid(_))
    ));
    assert!(SummaryStore::new(owner.store()).get(id).unwrap().is_none());
    drop(owner);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_executes_and_replays_lost_settlement_ack() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    model.fail_ack.store(true, Ordering::SeqCst);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model.clone())).unwrap();
    let (id, session) = (Uuid::new_v4(), Uuid::new_v4());
    create(&scheduler, id, session);
    scheduler.schedule_summary(id).unwrap();
    assert!(scheduler.wait_summary(id).await.is_err());
    assert!(matches!(
        state(&scheduler, id).operation.state(),
        State::Accounting { .. }
    ));
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Completed { .. }));
    assert!(snap.operation.attempts()[0].settled);
    {
        let calls = model.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, session);
        let settlements = model.settlements.lock().unwrap();
        assert_eq!(settlements.len(), 2);
        assert_eq!(settlements[0], settlements[1]);
        assert!(matches!(settlements[0].1, UsageReport::Measured { .. }));
    }
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_joins_running_worker_and_settles_unconfirmed() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    started(&model).await;
    scheduler.abort_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Aborted));
    assert!(snap.operation.attempts()[0].settled);
    assert_eq!(
        model.settlements.lock().unwrap()[0].1,
        UsageReport::Unconfirmed
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn separate_summaries_run_independently_and_abort_is_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model.clone())).unwrap();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    create(&scheduler, a, Uuid::new_v4());
    create(&scheduler, b, Uuid::new_v4());
    scheduler.schedule_summary(a).unwrap();
    scheduler.schedule_summary(b).unwrap();
    started(&model).await;
    started(&model).await;
    scheduler.abort_summary(a).await.unwrap();
    assert!(matches!(
        state(&scheduler, a).operation.state(),
        State::Aborted
    ));
    assert!(matches!(
        state(&scheduler, b).operation.state(),
        State::Running { .. }
    ));
    model.release.add_permits(1);
    scheduler.wait_summary(b).await.unwrap();
    assert!(matches!(
        state(&scheduler, b).operation.state(),
        State::Completed { .. }
    ));
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_during_settlement_replays_measured_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    model.block_settlement.store(true, Ordering::SeqCst);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), model.settlement_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    model.block_settlement.store(false, Ordering::SeqCst);
    scheduler.abort_summary(id).await.unwrap();
    assert!(matches!(
        state(&scheduler, id).operation.state(),
        State::Aborted
    ));
    {
        let reports = model.settlements.lock().unwrap();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0], reports[1]);
        assert!(matches!(reports[0].1, UsageReport::Measured { .. }));
    }
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupted_abort_cleanup_resumes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    model.block_settlement.store(true, Ordering::SeqCst);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    started(&model).await;
    let mut abort = Box::pin(scheduler.abort_summary(id));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            permit = model.settlement_started.acquire() => permit.unwrap().forget(),
            result = &mut abort => panic!("abort unexpectedly finished: {result:?}"),
        }
    })
    .await
    .unwrap();
    drop(abort);
    assert!(matches!(
        state(&scheduler, id).operation.state(),
        State::Aborting { .. }
    ));
    scheduler.shutdown().await.unwrap();
    model.block_settlement.store(false, Ordering::SeqCst);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Aborted));
    assert!(snap.operation.attempts()[0].settled);
    {
        let reports = model.settlements.lock().unwrap();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0], reports[1]);
    }
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_reopen_recovers_running_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    started(&model).await;
    scheduler.shutdown().await.unwrap();
    let model = TestModel::new(1);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Completed { .. }));
    assert_eq!(snap.operation.attempts().len(), 2);
    assert!(snap.operation.attempts()[0].unconfirmed);
    assert!(snap.operation.attempts().iter().all(|a| a.settled));
    assert_eq!(model.settlements.lock().unwrap().len(), 2);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn storage_failure_requires_reopen_before_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    started(&model).await;
    let inject = rusqlite::Connection::open(&path).unwrap();
    inject.execute_batch("CREATE TRIGGER fail_summary BEFORE UPDATE ON durable_store_entries BEGIN SELECT RAISE(FAIL, 'injected'); END;").unwrap();
    model.release.add_permits(1);
    assert!(scheduler.wait_summary(id).await.is_err());
    assert!(matches!(scheduler.schedule_summary(id), Err(Error::Failed)));
    inject.execute_batch("DROP TRIGGER fail_summary").unwrap();
    drop(inject);
    scheduler.shutdown().await.unwrap();
    let model = TestModel::new(1);
    let mut scheduler = Scheduler::open(&path, opts(model)).unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    assert!(matches!(
        state(&scheduler, id).operation.state(),
        State::Completed { .. }
    ));
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn killed_summary_child() {
    let Ok(dir) = std::env::var("PI_SUMMARY_WORKER_KILL_DIR") else {
        return;
    };
    let dir = std::path::Path::new(&dir);
    let id = Uuid::parse_str(&std::fs::read_to_string(dir.join("id")).unwrap()).unwrap();
    let accounting = std::env::var_os("PI_SUMMARY_WORKER_KILL_ACCOUNTING").is_some();
    let mut model = TestModel::new(usize::from(accounting));
    if accounting {
        Arc::get_mut(&mut model).unwrap().settlement_marker = Some(dir.join("accepted"));
        model.block_settlement.store(true, Ordering::SeqCst);
    } else {
        Arc::get_mut(&mut model).unwrap().marker = Some(dir.join("running"));
    }
    let mut scheduler = Scheduler::open(dir.join("db"), opts(model)).unwrap();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_scheduler_replays_accepted_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let id = Uuid::new_v4();
    std::fs::write(dir.path().join("id"), id.to_string()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::summary_worker_tests::killed_summary_child",
        ])
        .env("PI_SUMMARY_WORKER_KILL_DIR", dir.path())
        .env("PI_SUMMARY_WORKER_KILL_ACCOUNTING", "1")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.path().join("accepted").exists() {
        if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("summary child did not reach settlement");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let accepted: (Uuid, UsageReport) =
        serde_json::from_slice(&std::fs::read(dir.path().join("accepted")).unwrap()).unwrap();
    let model = TestModel::new(0);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model.clone())).unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Completed { .. }));
    assert_eq!(snap.operation.attempts().len(), 1);
    assert!(snap.operation.attempts()[0].settled);
    assert!(model.calls.lock().unwrap().is_empty());
    assert_eq!(model.settlements.lock().unwrap().as_slice(), &[accepted]);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn killed_scheduler_reopens_and_recovers_summary() {
    let dir = tempfile::tempdir().unwrap();
    let id = Uuid::new_v4();
    std::fs::write(dir.path().join("id"), id.to_string()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::summary_worker_tests::killed_summary_child",
        ])
        .env("PI_SUMMARY_WORKER_KILL_DIR", dir.path())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.path().join("running").exists() {
        if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("summary child did not enter its model call");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let model = TestModel::new(1);
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model.clone())).unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Completed { .. }));
    assert_eq!(snap.operation.attempts().len(), 2);
    assert!(snap.operation.attempts()[0].unconfirmed);
    assert!(snap.operation.attempts().iter().all(|a| a.settled));
    assert_eq!(model.settlements.lock().unwrap().len(), 2);
    scheduler.shutdown().await.unwrap();
}
