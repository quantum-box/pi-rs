use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    extension::{BeforeCompact, CompactionInput, Context, Extension, Hook, Registry},
    recovery::UncertainPolicy,
    runtime::{
        Config, Harness, IndependentCompactionStatus, Options, Scheduler, SummaryProviderIdentity,
    },
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

struct SummaryModel {
    started: Semaphore,
    release: Semaphore,
    calls: AtomicUsize,
}
#[async_trait]
impl Model for SummaryModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        panic!("unexpected generation")
    }
    async fn stream_for_summary(
        &self,
        _: Uuid,
        _: Uuid,
        _: ModelRequest,
    ) -> pi_rs_core::Result<ModelStream> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("independent checkpoint".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
struct BlockHook(Arc<Mutex<Vec<CompactionInput>>>);
#[async_trait]
impl Hook for BlockHook {
    fn supports_compaction_blocks(&self) -> bool {
        true
    }
    async fn before_compact(
        &self,
        input: &CompactionInput,
        ctx: &Context<'_>,
    ) -> Result<BeforeCompact, String> {
        self.0.lock().unwrap().push(input.clone());
        assert_eq!(
            ctx.memo
                .get_or_insert("block-hook", json!("persisted"))
                .await?,
            json!("persisted")
        );
        Ok(BeforeCompact::Pass)
    }
}
fn extension(seen: Arc<Mutex<Vec<CompactionInput>>>) -> Extension {
    Extension {
        name: "block-hook".into(),
        hooks: vec![Arc::new(BlockHook(seen))],
        ..Default::default()
    }
}
fn registry(seen: Arc<Mutex<Vec<CompactionInput>>>) -> Registry {
    let mut registry = Registry::default();
    registry.install(extension(seen)).unwrap();
    registry
}
fn options(model: Arc<SummaryModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn provider() -> SummaryProviderIdentity {
    SummaryProviderIdentity {
        model: "blocks:model:v1".into(),
        settlement: "blocks:ledger:v1".into(),
    }
}
fn seed(
    path: &std::path::Path,
    model: Arc<SummaryModel>,
    seen: Arc<Mutex<Vec<CompactionInput>>>,
) -> (Uuid, Uuid, Uuid) {
    let mut h = Harness::open_paused(path, options(model)).unwrap();
    h.install_extension(extension(seen)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    h.select_extensions(id, vec!["block-hook".into()]).unwrap();
    let old = h
        .append_user_blocks(
            id,
            vec![
                json!({"type":"text","text":"old"}),
                json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            ],
        )
        .unwrap();
    let kept = h
        .append_user_blocks(
            id,
            vec![
                json!({"type":"text","text":"keep"}),
                json!({"type":"image","data":"Ymll","mimeType":"image/png"}),
                json!({"type":"text","text":"tail","textSignature":"opaque"}),
            ],
        )
        .unwrap();
    (id, old, kept)
}
fn open(
    path: &std::path::Path,
    model: Arc<SummaryModel>,
    seen: Arc<Mutex<Vec<CompactionInput>>>,
) -> Scheduler {
    Scheduler::open_with_summary_recovery(path, options(model), registry(seen), None, provider())
        .unwrap()
}
fn model(permits: usize) -> Arc<SummaryModel> {
    Arc::new(SummaryModel {
        started: Semaphore::new(0),
        release: Semaphore::new(permits),
        calls: AtomicUsize::new(0),
    })
}
async fn started(model: &SummaryModel) {
    tokio::time::timeout(std::time::Duration::from_secs(5), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
async fn public_block_summary_hook_reopens_and_preserves_retained_occurrence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = model(0);
    let seen = Arc::new(Mutex::new(vec![]));
    let (id, old, kept) = seed(&path, model.clone(), seen.clone());
    let operation = Uuid::new_v4();
    let mut s = open(&path, model.clone(), seen.clone());
    assert!(
        s.request_idle_independent_compaction_with_instructions(id, operation, 0, Some("paths"))
            .unwrap()
    );
    started(&model).await;
    s.shutdown().await.unwrap();
    model.release.add_permits(1);
    let mut s = open(&path, model.clone(), seen.clone());
    assert_eq!(
        s.wait_independent_compaction(operation).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = s.conversation(id).unwrap();
    assert_eq!(c.content_blocks.len(), 1);
    assert_eq!(c.content_blocks[&1].message_id, kept);
    let fork = s.fork_by_entry_id(id, kept).unwrap();
    assert!(
        s.conversation(fork)
            .unwrap()
            .content_blocks
            .values()
            .any(|binding| binding.message_id == kept)
    );
    assert_eq!(
        c.content_blocks[&1].blocks,
        vec![
            json!({"type":"text","text":"keep"}),
            json!({"type":"image","data":"Ymll","mimeType":"image/png"}),
            json!({"type":"text","text":"tail","textSignature":"opaque"})
        ]
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(seen.lock().unwrap()[0].content_blocks[&0].message_id, old);
    assert_eq!(
        seen.lock().unwrap()[0].instructions.as_deref(),
        Some("paths")
    );
    s.shutdown().await.unwrap();
    let s = open(&path, model.clone(), seen);
    assert_eq!(
        s.conversation(id).unwrap().content_blocks[&1].message_id,
        kept
    );
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_block_summary_respects_reset_and_abort_fences() {
    for abort in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = model(0);
        let seen = Arc::new(Mutex::new(vec![]));
        let (id, _, kept) = seed(&path, model.clone(), seen.clone());
        let mut s = open(&path, model.clone(), seen);
        let op = Uuid::new_v4();
        assert!(s.request_idle_independent_compaction(id, op, 0).unwrap());
        started(&model).await;
        if abort {
            assert_eq!(
                s.abort_independent_compaction(op).await.unwrap(),
                IndependentCompactionStatus::Aborted
            );
            assert_eq!(
                s.conversation(id).unwrap().content_blocks[&1].message_id,
                kept
            );
        } else {
            s.reset(id, Some("new context".into())).unwrap();
            model.release.add_permits(1);
            assert_eq!(
                s.wait_independent_compaction(op).await.unwrap(),
                IndependentCompactionStatus::Stale
            );
            assert!(s.conversation(id).unwrap().content_blocks.is_empty());
        }
        s.shutdown().await.unwrap();
    }
}

struct BusyBlockAdapter {
    started: Semaphore,
    release: Arc<Semaphore>,
}
#[async_trait]
impl pi_rs_durable::model::ProviderAdapter for BusyBlockAdapter {
    fn id(&self) -> &str {
        "blocks:busy:v1"
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> pi_rs_core::Result<serde_json::Value> {
        Ok(json!({}))
    }
    fn validate(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: &serde_json::Value,
    ) -> pi_rs_core::Result<()> {
        Ok(())
    }
    async fn prepare_durable(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &pi_rs_durable::model::DurableRequest,
    ) -> pi_rs_core::Result<serde_json::Value> {
        Ok(json!({}))
    }
    fn validate_durable(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &pi_rs_durable::model::DurableRequest,
        _: &serde_json::Value,
    ) -> pi_rs_core::Result<()> {
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: serde_json::Value,
    ) -> pi_rs_core::Result<ModelStream> {
        self.started.add_permits(1);
        let release = self.release.clone();
        Ok(stream::once(async move {
            release.acquire().await.unwrap().forget();
            Ok(ModelEvent::Text("busy answer".into()))
        })
        .chain(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })]))
        .boxed())
    }
}
#[tokio::test]
async fn public_busy_block_summary_places_before_later_queued_input() {
    for queued in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = model(1);
        let seen = Arc::new(Mutex::new(vec![]));
        let adapter = Arc::new(BusyBlockAdapter {
            started: Semaphore::new(0),
            release: Arc::new(Semaphore::new(0)),
        });
        let (id, kept);
        {
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            h.install_provider_adapter(adapter.clone()).unwrap();
            h.install_extension(extension(seen.clone())).unwrap();
            id = h
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: String::new(),
                    max_output_tokens: 64,
                })
                .unwrap();
            h.select_provider_adapter(id, true).unwrap();
            h.select_extensions(id, vec!["block-hook".into()]).unwrap();
            h.append_user_blocks(id, vec![json!({"type":"text","text":"old".repeat(3000)})])
                .unwrap();
            kept = h
                .append_user_blocks(
                    id,
                    vec![
                        json!({"type":"text","text":"keep"}),
                        json!({"type":"image","data":"Ymll","mimeType":"image/png"}),
                    ],
                )
                .unwrap();
        }
        let mut s = Scheduler::open_with_summary_recovery(
            &path,
            options(model.clone()),
            registry(seen.clone()),
            Some(adapter.clone()),
            provider(),
        )
        .unwrap();
        s.submit(id, None, "latest".into()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), adapter.started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let op = Uuid::new_v4();
        assert!(s.request_busy_independent_compaction(id, op, 1500).unwrap());
        assert_eq!(
            s.wait_independent_compaction(op).await.unwrap(),
            IndependentCompactionStatus::Running
        );
        if queued {
            s.submit(id, None, "queued".into()).unwrap();
        }
        adapter.release.add_permits(2);
        s.wait(id).await.unwrap();
        assert_eq!(
            s.independent_compaction_status(op).unwrap(),
            IndependentCompactionStatus::Applied
        );
        let c = s.conversation(id).unwrap();
        assert!(
            c.content_blocks
                .values()
                .any(|b| b.message_id == kept && b.blocks[0]["text"] == "keep")
        );
        assert!(c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::Assistant { content, .. } if content == "busy answer")));
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert_eq!(c.content_blocks.len(), 1);
        if queued {
            let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("<summary>"))).unwrap();
            let later = c
                .messages
                .iter()
                .position(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"),
                )
                .unwrap();
            assert!(summary < later);
        }
        s.shutdown().await.unwrap();
    }
}

struct LegacyHook;
#[async_trait]
impl Hook for LegacyHook {}
#[tokio::test]
async fn public_block_summary_rejects_unaware_hooks_before_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = model(1);
    let seen = Arc::new(Mutex::new(vec![]));
    let (id, _, kept) = seed(&path, model.clone(), seen.clone());
    let mut s = open(&path, model.clone(), seen);
    s.install_extension(Extension {
        name: "block-hook".into(),
        hooks: vec![Arc::new(LegacyHook)],
        ..Default::default()
    })
    .unwrap();
    assert!(
        s.request_idle_independent_compaction(id, Uuid::new_v4(), 0)
            .is_err()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        s.conversation(id).unwrap().content_blocks[&1].message_id,
        kept
    );
    s.shutdown().await.unwrap();
}

struct ReplayBlockHook {
    seen: Arc<Mutex<Vec<CompactionInput>>>,
    entered: Arc<Semaphore>,
    wait: bool,
}
#[async_trait]
impl Hook for ReplayBlockHook {
    fn supports_compaction_blocks(&self) -> bool {
        true
    }
    async fn before_compact(
        &self,
        input: &CompactionInput,
        ctx: &Context<'_>,
    ) -> Result<BeforeCompact, String> {
        self.seen.lock().unwrap().push(input.clone());
        let value = ctx
            .memo
            .get_or_insert(
                "independent-replay",
                json!(if self.wait { "original" } else { "replacement" }),
            )
            .await?;
        assert_eq!(value, json!("original"));
        self.entered.add_permits(1);
        if self.wait {
            std::future::pending::<()>().await;
        }
        Ok(BeforeCompact::Pass)
    }
}
fn replay_registry(
    seen: Arc<Mutex<Vec<CompactionInput>>>,
    entered: Arc<Semaphore>,
    wait: bool,
) -> Registry {
    let mut registry = Registry::default();
    registry
        .install(Extension {
            name: "block-hook".into(),
            hooks: vec![Arc::new(ReplayBlockHook {
                seen,
                entered,
                wait,
            })],
            ..Default::default()
        })
        .unwrap();
    registry
}
#[tokio::test]
async fn public_independent_hook_recovers_block_snapshot_and_memo() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = model(1);
    let seen = Arc::new(Mutex::new(vec![]));
    let (id, old, kept) = seed(&path, model.clone(), seen.clone());
    let entered = Arc::new(Semaphore::new(0));
    let mut s = Scheduler::open_with_summary_recovery(
        &path,
        options(model.clone()),
        replay_registry(seen.clone(), entered.clone(), true),
        None,
        provider(),
    )
    .unwrap();
    let op = Uuid::new_v4();
    assert!(s.request_idle_independent_compaction(id, op, 0).unwrap());
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open_with_summary_recovery(
        &path,
        options(model.clone()),
        replay_registry(seen.clone(), entered, false),
        None,
        provider(),
    )
    .unwrap();
    assert_eq!(
        s.wait_independent_compaction(op).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(
        s.conversation(id).unwrap().content_blocks[&1].message_id,
        kept
    );
    assert!(s.extension_reports().unwrap().is_empty());
    {
        let recorded = seen.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0], recorded[1]);
        assert_eq!(recorded[0].content_blocks[&0].message_id, old);
    }
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn busy_block_summary_places_after_model_progress() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = model(0);
    let seen = Arc::new(Mutex::new(vec![]));
    let adapter = Arc::new(BusyBlockAdapter {
        started: Semaphore::new(0),
        release: Arc::new(Semaphore::new(0)),
    });
    let (id, kept);
    {
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        h.install_provider_adapter(adapter.clone()).unwrap();
        h.install_extension(extension(seen.clone())).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 64,
            })
            .unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.select_extensions(id, vec!["block-hook".into()]).unwrap();
        h.append_user_blocks(id, vec![json!({"type":"text","text":"old".repeat(3000)})])
            .unwrap();
        kept = h
            .append_user_blocks(
                id,
                vec![
                    json!({"type":"text","text":"keep"}),
                    json!({"type":"image","data":"Ymll","mimeType":"image/png"}),
                ],
            )
            .unwrap();
    }
    let mut s = Scheduler::open_with_summary_recovery(
        &path,
        options(model.clone()),
        registry(seen),
        Some(adapter.clone()),
        provider(),
    )
    .unwrap();
    s.submit(id, None, "latest".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), adapter.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let op = Uuid::new_v4();
    assert!(s.request_busy_independent_compaction(id, op, 1500).unwrap());
    started(&model).await;
    adapter.release.add_permits(1);
    s.wait(id).await.unwrap();
    model.release.add_permits(1);
    assert_eq!(
        s.wait_independent_compaction(op).await.unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = s.conversation(id).unwrap();
    assert!(
        c.content_blocks
            .values()
            .any(|b| b.message_id == kept && b.blocks[0]["text"] == "keep")
    );
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::Assistant { content, .. } if content == "busy answer")
    ));
    s.shutdown().await.unwrap();
}
