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

fn identity() -> SummaryProviderIdentity {
    SummaryProviderIdentity {
        model: "test-model-v1".into(),
        settlement: "test-settlement-v1".into(),
    }
}
fn open_manual(path: impl AsRef<std::path::Path>, options: Options) -> Result<Scheduler, Error> {
    let mut scheduler = Scheduler::open(path, options)?;
    scheduler.summary_provider = Some(identity());
    scheduler.harness.summary_provider = Some(identity());
    Ok(scheduler)
}
fn open_auto(
    path: impl AsRef<std::path::Path>,
    model: Arc<TestModel>,
    identity: SummaryProviderIdentity,
) -> Result<Scheduler, Error> {
    Scheduler::open_with_summary_recovery(
        path,
        opts(model),
        crate::extension::Registry::default(),
        None,
        identity,
    )
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
        .create_identified(id, session, preparation, identity())
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

fn request_public(scheduler: &mut Scheduler, conversation: Uuid) -> Result<Option<Uuid>, Error> {
    let id = Uuid::new_v4();
    Ok(scheduler
        .request_idle_independent_compaction(conversation, id, 0)?
        .then_some(id))
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
async fn public_idle_independent_compaction_executes_and_places_automatically() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(
        scheduler.independent_compaction_status(id).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert!(
        matches!(&scheduler.conversation(conversation).unwrap().messages[0], Message::User { content } if content.contains("<summary>\nsummary"))
    );
    assert_eq!(model.calls.lock().unwrap().len(), 1);
    assert_eq!(model.settlements.lock().unwrap().len(), 1);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_idle_compaction_becomes_stale_after_a_new_write() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    let before = scheduler.conversation(conversation).unwrap().messages;
    scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    model.release.add_permits(1);
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Stale
    );
    assert_eq!(
        scheduler.conversation(conversation).unwrap().messages,
        before
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_idle_compaction_abort_cancels_and_settles() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let before = scheduler.conversation(conversation).unwrap().messages;
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    assert_eq!(
        scheduler.abort_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Aborted
    );
    assert_eq!(
        model.settlements.lock().unwrap()[0].1,
        UsageReport::Unconfirmed
    );
    assert_eq!(
        scheduler.conversation(conversation).unwrap().messages,
        before
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn conversation_idle_wait_includes_public_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    let stop = CancellationToken::new();
    let mut wait = Box::pin(scheduler.wait_for_idle(conversation, &stop));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut wait)
            .await
            .is_err()
    );
    model.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), &mut wait)
        .await
        .unwrap()
        .unwrap();
    drop(wait);
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn conversation_abort_joins_summary_and_waits_for_usage() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    model.block_settlement.store(true, Ordering::SeqCst);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let original = scheduler.conversation(conversation).unwrap().messages;
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    let mut abort = Box::pin(scheduler.abort_and_wait(conversation));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            permit = model.settlement_started.acquire() => permit.unwrap().forget(),
            result = &mut abort => panic!("abort completed before settlement: {result:?}"),
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut abort)
            .await
            .is_err()
    );
    model.settlement_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), &mut abort)
        .await
        .unwrap()
        .unwrap();
    drop(abort);
    assert_eq!(
        scheduler.independent_compaction_status(id).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Aborted
    );
    assert_eq!(
        scheduler.conversation(conversation).unwrap().messages,
        original
    );
    assert_eq!(
        model.settlements.lock().unwrap()[0].1,
        UsageReport::Unconfirmed
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_summary_admission_invalidates_graph_abort_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let first = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    let owner = scheduler.harness.owner.clone();
    let (checks, mutations) = SummaryStore::new(owner.store())
        .prepare_owned_abort(&[conversation].into_iter().collect(), &Default::default())
        .unwrap();
    let second = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    assert!(matches!(
        owner.store().commit(&checks, &mutations),
        Err(durable_store::Error::Conflict { .. })
    ));
    scheduler.abort_and_wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(first).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    assert_eq!(
        scheduler.independent_compaction_status(second).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    drop(owner);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_idle_compaction_runs_selected_hook_and_rejects_busy_state() {
    struct HookCounter(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::extension::Hook for HookCounter {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(crate::extension::BeforeCompact::Summary(
                "hook summary".into(),
            ))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    scheduler
        .install_extension(crate::extension::Extension {
            name: "hooks".into(),
            hooks: vec![Arc::new(HookCounter(hook_calls.clone()))],
            ..Default::default()
        })
        .unwrap();
    scheduler
        .select_extensions(conversation, vec!["hooks".into()])
        .unwrap();
    let operation = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert!(
        matches!(&scheduler.conversation(conversation).unwrap().messages[0], Message::User { content } if content.contains("hook summary"))
    );
    scheduler.select_extensions(conversation, vec![]).unwrap();
    let (mut state, version) = scheduler.harness.load(conversation).unwrap();
    state.phase = Phase::ModelReady { turns: 0 };
    scheduler
        .harness
        .save(&state, Expected::Version(version))
        .unwrap();
    assert!(request_public(&mut scheduler, conversation).is_err());
    assert!(model.calls.lock().unwrap().is_empty());
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_hooks_report_errors_and_first_decision_wins() {
    type HookCalls = Arc<Mutex<Vec<(usize, Option<String>)>>>;
    struct Hook {
        calls: HookCalls,
        index: usize,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for Hook {
        async fn before_compact(
            &self,
            input: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            self.calls
                .lock()
                .unwrap()
                .push((self.index, input.instructions.clone()));
            match self.index {
                0 => Err("reported hook failure".into()),
                1 => Ok(crate::extension::BeforeCompact::Decline),
                _ => Ok(crate::extension::BeforeCompact::Summary("ignored".into())),
            }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let original = scheduler.conversation(conversation).unwrap().messages;
    let calls = Arc::new(Mutex::new(Vec::new()));
    scheduler
        .install_extension(crate::extension::Extension {
            name: "hooks".into(),
            hooks: (0..3)
                .map(|index| {
                    Arc::new(Hook {
                        calls: calls.clone(),
                        index,
                    }) as Arc<dyn crate::extension::Hook>
                })
                .collect(),
            ..Default::default()
        })
        .unwrap();
    scheduler
        .select_extensions(conversation, vec!["hooks".into()])
        .unwrap();
    let id = Uuid::new_v4();
    assert!(
        scheduler
            .request_idle_independent_compaction_with_instructions(
                conversation,
                id,
                0,
                Some("preserve paths")
            )
            .unwrap()
    );
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Declined
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            (0, Some("preserve paths".into())),
            (1, Some("preserve paths".into()))
        ]
    );
    assert!(
        scheduler
            .extension_reports()
            .unwrap()
            .iter()
            .any(|batch| batch.operation_id == id && batch.phase == "before_compact")
    );
    assert!(model.calls.lock().unwrap().is_empty());
    assert!(model.settlements.lock().unwrap().is_empty());
    assert_eq!(
        scheduler.conversation(conversation).unwrap().messages,
        original
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_hook_replays_with_the_same_durable_memo_after_reopen() {
    struct ReplayHook {
        started: Semaphore,
        release: Semaphore,
        values: Mutex<Vec<serde_json::Value>>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for ReplayHook {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            context: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            let value = context
                .memo
                .get_or_insert("token", serde_json::json!(Uuid::new_v4()))
                .await?;
            self.values.lock().unwrap().push(value);
            self.started.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|e| e.to_string())?
                .forget();
            Ok(crate::extension::BeforeCompact::Summary("from hook".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    let hook = Arc::new(ReplayHook {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
        values: Mutex::new(Vec::new()),
    });
    let extension = crate::extension::Extension {
        name: "replay".into(),
        hooks: vec![hook.clone()],
        ..Default::default()
    };
    let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
    scheduler.install_extension(extension.clone()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    scheduler
        .select_extensions(conversation, vec!["replay".into()])
        .unwrap();
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.shutdown().await.unwrap();
    let mut registry = crate::extension::Registry::default();
    registry.install(extension).unwrap();
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        opts(model.clone()),
        registry,
        None,
        identity(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    hook.release.add_permits(1);
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    {
        let values = hook.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], values[1]);
    }
    assert!(model.calls.lock().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn conversation_abort_fences_a_running_independent_hook() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct BlockHook {
        started: Semaphore,
        dropped: Arc<AtomicBool>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for BlockHook {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            let _guard = DropFlag(self.dropped.clone());
            self.started.add_permits(1);
            std::future::pending().await
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let hook = Arc::new(BlockHook {
        started: Semaphore::new(0),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    scheduler
        .install_extension(crate::extension::Extension {
            name: "block".into(),
            hooks: vec![hook.clone()],
            ..Default::default()
        })
        .unwrap();
    scheduler
        .select_extensions(conversation, vec!["block".into()])
        .unwrap();
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.abort_and_wait(conversation).await.unwrap();
    assert!(hook.dropped.load(Ordering::SeqCst));
    assert_eq!(
        scheduler.independent_compaction_status(id).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    assert!(model.calls.lock().unwrap().is_empty());
    assert!(model.settlements.lock().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_idle_compaction_rejects_a_live_conversation_run() {
    struct BusyModel {
        started: Semaphore,
        release: Semaphore,
    }
    #[async_trait::async_trait]
    impl Model for BusyModel {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        options,
        Default::default(),
        None,
        identity(),
    )
    .unwrap();
    let conversation = placement_conversation(&mut scheduler);
    scheduler.submit(conversation, None, "live".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let id = Uuid::new_v4();
    assert!(
        scheduler
            .request_idle_independent_compaction(conversation, id, 0)
            .is_err()
    );
    assert!(
        SummaryStore::new(scheduler.harness.owner.store())
            .get(id)
            .unwrap()
            .is_none()
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_idle_compaction_does_not_overwrite_a_new_live_run() {
    struct OverlapModel {
        summary_started: Semaphore,
        summary_release: Semaphore,
        run_started: Semaphore,
        run_release: Semaphore,
    }
    #[async_trait::async_trait]
    impl Model for OverlapModel {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            self.run_started.add_permits(1);
            self.run_release.acquire().await.unwrap().forget();
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
        async fn stream_for_summary(
            &self,
            _: Uuid,
            _: Uuid,
            _: ModelRequest,
        ) -> pi_rs_core::Result<ModelStream> {
            self.summary_started.add_permits(1);
            self.summary_release.acquire().await.unwrap().forget();
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("summary".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(OverlapModel {
        summary_started: Semaphore::new(0),
        summary_release: Semaphore::new(0),
        run_started: Semaphore::new(0),
        run_release: Semaphore::new(0),
    });
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        options,
        Default::default(),
        None,
        identity(),
    )
    .unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), model.summary_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler
        .submit(conversation, None, "new live input".into())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), model.run_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Stale
    );
    let live = scheduler.conversation(conversation).unwrap();
    assert!(
        live.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "new live input"))
    );
    assert!(
        live.messages
            .iter()
            .all(|m| !matches!(m, Message::User { content } if content.contains("<summary>")))
    );
    model.run_release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_idle_compaction_restarts_and_places_after_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    started(&model).await;
    scheduler.shutdown().await.unwrap();
    let recovered = TestModel::new(1);
    let mut scheduler = open_auto(&path, recovered.clone(), identity()).unwrap();
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(recovered.settlements.lock().unwrap().len(), 2);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_retry_replays_settlement_then_places_once() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    model.fail_ack.store(true, Ordering::SeqCst);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    assert!(scheduler.wait_independent_compaction(id).await.is_err());
    assert_eq!(
        scheduler.independent_compaction_status(id).unwrap(),
        IndependentCompactionStatus::Running
    );
    scheduler.resume_independent_compaction(id).unwrap();
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(model.calls.lock().unwrap().len(), 1);
    {
        let receipts = model.settlements.lock().unwrap();
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0], receipts[1]);
    }
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_placement_storage_error_fails_closed_until_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = Uuid::new_v4();
    scheduler
        .create_independent_summary_impl(id, conversation, None, 0, (true, false), None)
        .unwrap();
    crate::summary_worker::run(
        scheduler.harness.owner.clone(),
        model.clone(),
        id,
        CancellationToken::new(),
        identity(),
        None,
    )
    .await
    .unwrap();
    let inject = rusqlite::Connection::open(&path).unwrap();
    inject.execute_batch("CREATE TRIGGER fail_ticket BEFORE INSERT ON durable_store_entries WHEN substr(new.key,1,26)=CAST('durable/summary-placement/' AS BLOB) BEGIN SELECT RAISE(FAIL, 'injected'); END;").unwrap();
    scheduler.resume_independent_compaction(id).unwrap();
    assert!(scheduler.wait_independent_compaction(id).await.is_err());
    assert!(matches!(
        scheduler.independent_compaction_status(id),
        Err(Error::Failed)
    ));
    inject.execute_batch("DROP TRIGGER fail_ticket").unwrap();
    drop(inject);
    scheduler.shutdown().await.unwrap();
    let mut scheduler = open_auto(&path, TestModel::new(0), identity()).unwrap();
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_recovery_resumes_completed_and_admitted_placement() {
    for pending in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = TestModel::new(1);
        let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
        let conversation = placement_conversation(&mut scheduler);
        let id = Uuid::new_v4();
        assert!(
            scheduler
                .create_independent_summary_impl(id, conversation, None, 0, (true, false), None)
                .unwrap()
        );
        crate::summary_worker::run(
            scheduler.harness.owner.clone(),
            model.clone(),
            id,
            CancellationToken::new(),
            identity(),
            None,
        )
        .await
        .unwrap();
        if pending {
            let store = SummaryStore::new(scheduler.harness.owner.store());
            let snap = store.get(id).unwrap().unwrap();
            let key = Harness::key(conversation);
            let version = scheduler
                .harness
                .owner
                .store()
                .get(&key)
                .unwrap()
                .unwrap()
                .version;
            store.admit_placement(&snap, key, version).unwrap();
        }
        scheduler.shutdown().await.unwrap();
        let recovered = TestModel::new(0);
        let mut scheduler = open_auto(&path, recovered.clone(), identity()).unwrap();
        assert_eq!(
            scheduler.wait_independent_compaction(id).await.unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert!(recovered.calls.lock().unwrap().is_empty());
        assert!(recovered.settlements.lock().unwrap().is_empty());
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn public_abort_can_win_after_completion_or_placement_admission() {
    for pending in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let model = TestModel::new(1);
        let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
        let conversation = placement_conversation(&mut scheduler);
        let original = scheduler.conversation(conversation).unwrap().messages;
        let id = Uuid::new_v4();
        scheduler
            .create_independent_summary_impl(id, conversation, None, 0, (true, false), None)
            .unwrap();
        crate::summary_worker::run(
            scheduler.harness.owner.clone(),
            model.clone(),
            id,
            CancellationToken::new(),
            identity(),
            None,
        )
        .await
        .unwrap();
        let mut staged = None;
        if pending {
            let store = SummaryStore::new(scheduler.harness.owner.store());
            let snap = store.get(id).unwrap().unwrap();
            let key = Harness::key(conversation);
            let version = scheduler
                .harness
                .owner
                .store()
                .get(&key)
                .unwrap()
                .unwrap()
                .version;
            store.admit_placement(&snap, key, version).unwrap();
            let snap = store.get(id).unwrap().unwrap();
            let State::PlacementPending { result, .. } = snap.operation.state() else {
                panic!("expected pending placement")
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
                    owner: scheduler.harness.owner.clone(),
                    checks,
                    mutations,
                })
                .unwrap();
            staged = Some(batch);
        }
        assert_eq!(
            scheduler.abort_independent_compaction(id).await.unwrap(),
            IndependentCompactionStatus::Aborted
        );
        if let Some(batch) = staged {
            assert!(matches!(
                scheduler.harness.commit_conversation_save(batch),
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
            ));
        }
        assert_eq!(
            scheduler.conversation(conversation).unwrap().messages,
            original
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn public_kill_child() {
    let Ok(dir) = std::env::var("PI_PUBLIC_SUMMARY_KILL_DIR") else {
        return;
    };
    let dir = std::path::Path::new(&dir);
    let mut model = TestModel::new(0);
    Arc::get_mut(&mut model).unwrap().marker = Some(dir.join("started"));
    let mut scheduler = open_auto(dir.join("db"), model, identity()).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = request_public(&mut scheduler, conversation)
        .unwrap()
        .unwrap();
    std::fs::write(dir.join("ids"), format!("{id} {conversation}")).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn public_compaction_recovers_after_process_kill() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::summary_worker_tests::public_kill_child",
        ])
        .env("PI_PUBLIC_SUMMARY_KILL_DIR", dir.path())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.path().join("started").exists() || !dir.path().join("ids").exists() {
        if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("public summary child did not start");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let ids = std::fs::read_to_string(dir.path().join("ids")).unwrap();
    let mut ids = ids.split_whitespace().map(|s| Uuid::parse_str(s).unwrap());
    let id = ids.next().unwrap();
    let conversation = ids.next().unwrap();
    let model = TestModel::new(1);
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    assert_eq!(
        scheduler.wait_independent_compaction(id).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert!(
        matches!(&scheduler.conversation(conversation).unwrap().messages[0], Message::User { content } if content.contains("<summary>\nsummary"))
    );
    assert_eq!(model.settlements.lock().unwrap().len(), 2);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_summary_places_once_after_reopen_and_preserves_occurrence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(&path, opts(model)).unwrap();
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
    let mut scheduler = open_manual(dir.join("db"), opts(TestModel::new(1))).unwrap();
    let conversation = placement_conversation(&mut scheduler);
    let id = Uuid::new_v4();
    scheduler
        .create_independent_summary(id, conversation, 64, 0)
        .unwrap();
    scheduler.schedule_summary(id).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let store = SummaryStore::new(scheduler.harness.owner.store());
    let snap = store.get(id).unwrap().unwrap();
    let key = Harness::key(conversation);
    let version = scheduler
        .harness
        .owner
        .store()
        .get(&key)
        .unwrap()
        .unwrap()
        .version;
    store.admit_placement(&snap, key, version).unwrap();
    let (_, sequence) = store
        .placement_ticket(&store.get(id).unwrap().unwrap())
        .unwrap();
    std::fs::write(dir.join("sequence"), sequence.to_string()).unwrap();
    std::fs::write(dir.join("ids"), format!("{id} {conversation}")).unwrap();
    std::fs::write(dir.join("ready"), b"ready").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_pending_placement_recovers_once_after_reopen() {
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(TestModel::new(0))).unwrap();
    let pending = state(&scheduler, id);
    let (_, sequence) = SummaryStore::new(scheduler.harness.owner.store())
        .placement_ticket(&pending)
        .unwrap();
    assert_eq!(
        sequence.to_string(),
        std::fs::read_to_string(dir.path().join("sequence")).unwrap()
    );
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
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(&path, opts(model)).unwrap();
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model)).unwrap();
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model)).unwrap();
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
    let (mut candidate, version) = scheduler
        .harness
        .load_for_summary_placement(conversation)
        .unwrap();
    store
        .admit_placement(&snap, Harness::key(conversation), version)
        .unwrap();
    let snap = store.get(id).unwrap().unwrap();
    let State::PlacementPending { result, .. } = snap.operation.state() else {
        panic!("expected pending placement")
    };
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
        State::PlacementPending { .. }
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
async fn placement_ticket_orders_commits_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let key = Harness::key(conversation);
    let version = owner.store().get(&key).unwrap().unwrap().version;
    let before = owner
        .store()
        .commit_for_version(&key, version)
        .unwrap()
        .unwrap();
    store.admit_placement(&snap, key.clone(), version).unwrap();
    let pending = store.get(id).unwrap().unwrap();
    let (ticket, admitted) = store.placement_ticket(&pending).unwrap();
    assert_eq!(ticket.operation, id);
    assert_eq!(ticket.conversation, conversation);
    assert!(before < admitted);
    assert!(matches!(
        store.admit_placement(&snap, key.clone(), version),
        Err(crate::summary_store::Error::Conflict)
    ));
    scheduler
        .harness
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    let later = owner.store().get(&key).unwrap().unwrap().version;
    assert!(
        owner
            .store()
            .commit_for_version(&key, later)
            .unwrap()
            .unwrap()
            > admitted
    );
    scheduler.harness.reset(conversation, None).unwrap();
    let reset_version = owner.store().get(&key).unwrap().unwrap().version;
    assert!(
        owner
            .store()
            .commit_for_version(&key, reset_version)
            .unwrap()
            .unwrap()
            > owner
                .store()
                .commit_for_version(&key, later)
                .unwrap()
                .unwrap()
    );
    drop(owner);
    scheduler.shutdown().await.unwrap();
    let mut scheduler = open_manual(&path, opts(model)).unwrap();
    let pending = state(&scheduler, id);
    let (reopened, sequence) = SummaryStore::new(scheduler.harness.owner.store())
        .placement_ticket(&pending)
        .unwrap();
    assert_eq!(reopened, ticket);
    assert_eq!(sequence, admitted);
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Stale
    );
    let (retained, retained_sequence) = SummaryStore::new(scheduler.harness.owner.store())
        .placement_ticket(&state(&scheduler, id))
        .unwrap();
    assert_eq!((retained, retained_sequence), (ticket, admitted));
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Stale
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_placement_recovers_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let key = Harness::key(conversation);
    let version = owner.store().get(&key).unwrap().unwrap().version;
    store.admit_placement(&snap, key, version).unwrap();
    let (ticket, sequence) = store
        .placement_ticket(&store.get(id).unwrap().unwrap())
        .unwrap();
    drop(owner);
    scheduler.shutdown().await.unwrap();
    let mut scheduler = open_manual(&path, opts(model)).unwrap();
    let (recovered, recovered_sequence) = SummaryStore::new(scheduler.harness.owner.store())
        .placement_ticket(&state(&scheduler, id))
        .unwrap();
    assert_eq!((recovered, recovered_sequence), (ticket, sequence));
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Placed
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn admission_conflict_cannot_publish_a_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    let mut scheduler = open_manual(dir.path().join("db"), opts(model)).unwrap();
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
    let key = Harness::key(conversation);
    let version = owner.store().get(&key).unwrap().unwrap().version;
    scheduler.harness.reset(conversation, None).unwrap();
    assert!(matches!(
        store.admit_placement(&snap, key, version),
        Err(crate::summary_store::Error::Conflict)
    ));
    assert!(matches!(
        store.get(id).unwrap().unwrap().operation.state(),
        State::Completed { .. }
    ));
    let ticket_key = [b"durable/summary-placement/".as_slice(), id.as_bytes()].concat();
    assert!(owner.store().get(&ticket_key).unwrap().is_none());
    assert_eq!(
        scheduler.place_summary(id).unwrap(),
        SummaryPlacement::Stale
    );
    drop(owner);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_placement_fails_closed_if_ticket_is_rewritten_or_missing() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    let mut scheduler = open_manual(dir.path().join("db"), opts(model)).unwrap();
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
    let key = Harness::key(conversation);
    let version = owner.store().get(&key).unwrap().unwrap().version;
    let before = scheduler
        .harness
        .conversation(conversation)
        .unwrap()
        .messages;
    store.admit_placement(&snap, key, version).unwrap();
    let ticket_key = [b"durable/summary-placement/".as_slice(), id.as_bytes()].concat();
    let entry = owner.store().get(&ticket_key).unwrap().unwrap();
    owner
        .store()
        .commit(
            &[Check {
                key: ticket_key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key: ticket_key.clone(),
                value: entry.value,
            }],
        )
        .unwrap();
    assert!(scheduler.place_summary(id).is_err());
    let entry = owner.store().get(&ticket_key).unwrap().unwrap();
    owner
        .store()
        .commit(
            &[Check {
                key: ticket_key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Delete { key: ticket_key }],
        )
        .unwrap();
    assert!(scheduler.place_summary(id).is_err());
    assert!(matches!(
        state(&scheduler, id).operation.state(),
        State::PlacementPending { .. }
    ));
    assert_eq!(
        scheduler
            .harness
            .conversation(conversation)
            .unwrap()
            .messages,
        before
    );
    drop(owner);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn creation_rejects_a_changed_conversation_version() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(0);
    let mut scheduler = open_manual(dir.path().join("db"), opts(model)).unwrap();
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
            (Harness::key(conversation), version),
            identity(),
            (false, None),
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model)).unwrap();
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(dir.path().join("db"), opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
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
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    scheduler.schedule_summary(id).unwrap();
    started(&model).await;
    scheduler.shutdown().await.unwrap();
    let model = TestModel::new(1);
    let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
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
async fn startup_blocks_legacy_and_mismatched_provider_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let scheduler = open_manual(&path, opts(model.clone())).unwrap();
    let good = Uuid::new_v4();
    let legacy = Uuid::new_v4();
    let wrong_model = Uuid::new_v4();
    let wrong_settlement = Uuid::new_v4();
    create(&scheduler, good, Uuid::new_v4());
    let preparation = state(&scheduler, good).operation.preparation().clone();
    let store = SummaryStore::new(scheduler.harness.owner.store());
    store
        .create(legacy, Uuid::new_v4(), preparation.clone())
        .unwrap();
    store
        .create_identified(
            wrong_model,
            Uuid::new_v4(),
            preparation.clone(),
            SummaryProviderIdentity {
                model: "other-model".into(),
                settlement: identity().settlement,
            },
        )
        .unwrap();
    store
        .create_identified(
            wrong_settlement,
            Uuid::new_v4(),
            preparation,
            SummaryProviderIdentity {
                model: identity().model,
                settlement: "other-ledger".into(),
            },
        )
        .unwrap();
    let owner = Uuid::new_v4();
    for id in [legacy, wrong_model, wrong_settlement] {
        store
            .reserve(
                id,
                store.get(id).unwrap().unwrap().version,
                owner,
                Uuid::new_v4(),
            )
            .unwrap();
        store
            .start(id, store.get(id).unwrap().unwrap().version, owner)
            .unwrap();
    }
    store
        .begin_abort(legacy, store.get(legacy).unwrap().unwrap().version)
        .unwrap();
    store
        .complete(
            wrong_settlement,
            store.get(wrong_settlement).unwrap().unwrap().version,
            owner,
            crate::model::CompletedGeneration {
                content: "already generated".into(),
                calls: vec![],
                usage: Some(Usage {
                    input_tokens: 3,
                    output_tokens: 1,
                }),
                metadata: ModelMetadata::default(),
            },
        )
        .unwrap();
    let versions: Vec<_> = [legacy, wrong_model, wrong_settlement]
        .into_iter()
        .map(|id| (id, store.get(id).unwrap().unwrap().version))
        .collect();
    scheduler.shutdown().await.unwrap();
    let mut no_identity = Scheduler::open(&path, opts(model.clone())).unwrap();
    assert!(no_identity.schedule_summary(good).is_err());
    assert!(no_identity.abort_summary(good).await.is_err());
    assert!(model.calls.lock().unwrap().is_empty());
    no_identity.shutdown().await.unwrap();
    let mut scheduler = open_auto(&path, model.clone(), identity()).unwrap();
    let blocked: std::collections::HashSet<_> = scheduler
        .blocked_summary_recovery()
        .iter()
        .copied()
        .collect();
    assert_eq!(
        blocked,
        [legacy, wrong_model, wrong_settlement]
            .into_iter()
            .collect()
    );
    for (id, version) in versions {
        assert!(scheduler.schedule_summary(id).is_err());
        assert!(scheduler.abort_summary(id).await.is_err());
        assert_eq!(state(&scheduler, id).version, version);
    }
    scheduler.wait_summary(good).await.unwrap();
    assert_eq!(model.calls.lock().unwrap().len(), 1);
    assert_eq!(model.settlements.lock().unwrap().len(), 1);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn discovery_limit_prevents_partial_startup_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(200);
    let scheduler = open_manual(&path, opts(model.clone())).unwrap();
    let first = Uuid::new_v4();
    create(&scheduler, first, Uuid::new_v4());
    let preparation = state(&scheduler, first).operation.preparation().clone();
    let store = SummaryStore::new(scheduler.harness.owner.store());
    for _ in 0..crate::summary_store::MAX_DISCOVERY_OPERATIONS {
        store
            .create_identified(
                Uuid::new_v4(),
                Uuid::new_v4(),
                preparation.clone(),
                identity(),
            )
            .unwrap();
    }
    scheduler.shutdown().await.unwrap();
    assert!(open_auto(&path, model.clone(), identity()).is_err());
    assert!(model.calls.lock().unwrap().is_empty());
    assert!(model.settlements.lock().unwrap().is_empty());
}

#[tokio::test]
async fn public_admission_reserves_recovery_scan_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    let scheduler = open_manual(&path, opts(model.clone())).unwrap();
    let seed = Uuid::new_v4();
    create(&scheduler, seed, Uuid::new_v4());
    let preparation = state(&scheduler, seed).operation.preparation().clone();
    let store = SummaryStore::new(scheduler.harness.owner.store());
    for _ in 1..crate::summary_store::MAX_DISCOVERY_OPERATIONS {
        store
            .create(Uuid::new_v4(), Uuid::new_v4(), preparation.clone())
            .unwrap();
    }
    scheduler.shutdown().await.unwrap();
    let mut scheduler = open_auto(
        &path,
        model.clone(),
        SummaryProviderIdentity {
            model: "other".into(),
            settlement: "other".into(),
        },
    )
    .unwrap();
    let conversation = placement_conversation(&mut scheduler);
    assert!(request_public(&mut scheduler, conversation).is_err());
    assert_eq!(
        scheduler.blocked_summary_recovery().len(),
        crate::summary_store::MAX_DISCOVERY_OPERATIONS
    );
    assert!(model.calls.lock().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
    let scheduler = open_auto(
        &path,
        model,
        SummaryProviderIdentity {
            model: "other".into(),
            settlement: "other".into(),
        },
    )
    .unwrap();
    assert_eq!(
        scheduler.blocked_summary_recovery().len(),
        crate::summary_store::MAX_DISCOVERY_OPERATIONS
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn adapter_validation_precedes_summary_recovery() {
    struct InvalidAdapter;
    #[async_trait::async_trait]
    impl crate::model::ProviderAdapter for InvalidAdapter {
        fn id(&self) -> &str {
            ""
        }
        async fn prepare(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &ModelRequest,
        ) -> pi_rs_core::Result<serde_json::Value> {
            unreachable!()
        }
        fn validate(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &ModelRequest,
            _: &serde_json::Value,
        ) -> pi_rs_core::Result<()> {
            unreachable!()
        }
        async fn dispatch(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &ModelRequest,
            _: serde_json::Value,
        ) -> pi_rs_core::Result<ModelStream> {
            unreachable!()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(1);
    let scheduler = open_manual(&path, opts(model.clone())).unwrap();
    create(&scheduler, Uuid::new_v4(), Uuid::new_v4());
    scheduler.shutdown().await.unwrap();
    assert!(
        Scheduler::open_with_summary_recovery(
            &path,
            opts(model.clone()),
            crate::extension::Registry::default(),
            Some(Arc::new(InvalidAdapter)),
            identity()
        )
        .is_err()
    );
    assert!(model.calls.lock().unwrap().is_empty());
    assert!(model.settlements.lock().unwrap().is_empty());
}

#[tokio::test]
async fn executor_guard_rechecks_identity_before_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let model = TestModel::new(1);
    let scheduler = open_manual(dir.path().join("db"), opts(model.clone())).unwrap();
    let id = Uuid::new_v4();
    create(&scheduler, id, Uuid::new_v4());
    let before = state(&scheduler, id);
    let result = crate::summary_worker::run(
        scheduler.harness.owner.clone(),
        model.clone(),
        id,
        CancellationToken::new(),
        SummaryProviderIdentity {
            model: "different".into(),
            settlement: identity().settlement,
        },
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::summary_worker::Error::Identity)
    ));
    assert_eq!(state(&scheduler, id).version, before.version);
    assert!(model.calls.lock().unwrap().is_empty());
    assert!(model.settlements.lock().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn storage_failure_requires_reopen_before_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = TestModel::new(0);
    let mut scheduler = open_manual(&path, opts(model.clone())).unwrap();
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
    let mut scheduler = open_manual(&path, opts(model)).unwrap();
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
    let mut scheduler = open_manual(dir.join("db"), opts(model)).unwrap();
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
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
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
    let mut scheduler = open_auto(dir.path().join("db"), model.clone(), identity()).unwrap();
    scheduler.wait_summary(id).await.unwrap();
    let snap = state(&scheduler, id);
    assert!(matches!(snap.operation.state(), State::Completed { .. }));
    assert_eq!(snap.operation.attempts().len(), 2);
    assert!(snap.operation.attempts()[0].unconfirmed);
    assert!(snap.operation.attempts().iter().all(|a| a.settled));
    assert_eq!(model.settlements.lock().unwrap().len(), 2);
    scheduler.shutdown().await.unwrap();
}
