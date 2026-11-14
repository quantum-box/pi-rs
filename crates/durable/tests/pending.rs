use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Status},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let tool_done = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let event = if tool_done {
            ModelEvent::Text("done".into())
        } else {
            ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                index: 0,
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            })
        };
        let calls = if tool_done {
            vec![]
        } else {
            vec![ToolCall {
                id: "call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }]
        };
        Ok(stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct Job {
    dispatched: AtomicUsize,
    polled: AtomicUsize,
}
#[async_trait]
impl Tool for Job {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "external-1".into(),
        }
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        assert_eq!(reference, "external-1");
        if self.polled.fetch_add(1, Ordering::SeqCst) == 0 {
            Some(Outcome::Pending {
                reference: "external-1".into(),
            })
        } else {
            Some(Outcome::Succeeded("complete".into()))
        }
    }
}
fn options(job: Arc<Job>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn accepted_job_is_polled_after_reopen_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let job = Arc::new(Job {
        dispatched: AtomicUsize::new(0),
        polled: AtomicUsize::new(0),
    });
    let id;
    let attempt;
    {
        let mut h = Harness::open(&path, options(job.clone()), &cancel)
            .await
            .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.submit_and_run(id, Some("job".into()), "work".into(), &cancel)
            .await
            .unwrap();
        attempt = h.conversation(id).unwrap().tool_attempt().unwrap();
    }
    let paused = Harness::open_paused(&path, options(job.clone())).unwrap();
    assert_eq!(
        paused.conversation(id).unwrap().tool_attempt(),
        Some(attempt)
    );
    assert_eq!(job.dispatched.load(Ordering::SeqCst), 1);
    drop(paused);
    let h = Harness::open(&path, options(job.clone()), &cancel)
        .await
        .unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert_eq!(job.dispatched.load(Ordering::SeqCst), 1);
    assert_eq!(job.polled.load(Ordering::SeqCst), 2);
}

struct BlockingJob {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Tool for BlockingJob {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "external-1".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        self.started.notify_one();
        self.release.notified().await;
        Some(Outcome::Succeeded("stale".into()))
    }
}
#[tokio::test]
async fn concurrent_poll_cannot_overwrite_reconciled_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let job = Arc::new(BlockingJob {
        started: started.clone(),
        release: release.clone(),
    });
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut s = pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let (op, _) = s.blocked(id).unwrap().unwrap();
    s.reconcile(id, op, Outcome::Succeeded("confirmed".into()))
        .unwrap();
    release.notify_one();
    let _ = s.wait(id).await;
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::Tool{content,..} if content=="confirmed"))
    );
    assert!(
        !c.messages
            .iter()
            .any(|m| matches!(m,Message::Tool{content,..} if content=="stale"))
    );
    s.shutdown().await.unwrap();
}
struct EventuallyDone {
    polls: AtomicUsize,
}
#[async_trait]
impl Tool for EventuallyDone {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            Some(Outcome::Pending {
                reference: "job".into(),
            })
        } else {
            Some(Outcome::Succeeded("finished".into()))
        }
    }
}
#[tokio::test]
async fn scheduler_polls_pending_jobs_until_complete() {
    let dir = tempfile::tempdir().unwrap();
    let job = Arc::new(EventuallyDone {
        polls: AtomicUsize::new(0),
    });
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job.clone(),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut s = pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.polls.load(Ordering::SeqCst), 2);
    assert_eq!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    s.shutdown().await.unwrap();
}

struct ParkedJob {
    calls: AtomicUsize,
    polls: AtomicUsize,
    seen: tokio::sync::Semaphore,
    finish: std::sync::atomic::AtomicBool,
    panic_next: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Tool for ParkedJob {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.seen.add_permits(1);
        assert!(
            !self.panic_next.swap(false, Ordering::SeqCst),
            "poll failed"
        );
        Some(if self.finish.load(Ordering::SeqCst) {
            Outcome::Succeeded("finished".into())
        } else {
            Outcome::Pending {
                reference: "job".into(),
            }
        })
    }
}

fn parked_options(job: Arc<ParkedJob>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

async fn next_poll(job: &ParkedJob) {
    tokio::time::timeout(std::time::Duration::from_millis(500), job.seen.acquire())
        .await
        .expect("scheduled worker did not reach the parked conversation")
        .unwrap()
        .forget();
}

#[tokio::test]
async fn retries_share_one_periodic_poller_and_shutdown_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let job = Arc::new(ParkedJob {
        calls: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
        seen: tokio::sync::Semaphore::new(0),
        finish: std::sync::atomic::AtomicBool::new(false),
        panic_next: std::sync::atomic::AtomicBool::new(false),
    });
    let path = dir.path().join("db");
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(&path, parked_options(job.clone())).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    next_poll(&job).await;
    for _ in 0..8 {
        scheduler.schedule(id);
    }
    for _ in 0..8 {
        next_poll(&job).await;
    }
    // All retries are parked. Only one periodic poll should occur in the
    // next interval, even though every retry remains a potential takeover.
    tokio::time::timeout(std::time::Duration::from_secs(3), job.seen.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), job.seen.acquire())
            .await
            .is_err()
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), scheduler.shutdown())
        .await
        .unwrap()
        .unwrap();
    job.finish.store(true, Ordering::SeqCst);
    let mut reopened =
        pi_rs_durable::runtime::Scheduler::open(&path, parked_options(job.clone())).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), reopened.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.calls.load(Ordering::SeqCst), 1);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduled_retry_can_drive_a_parked_worker_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let job = Arc::new(ParkedJob {
        calls: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
        seen: tokio::sync::Semaphore::new(0),
        finish: std::sync::atomic::AtomicBool::new(false),
        panic_next: std::sync::atomic::AtomicBool::new(false),
    });
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), parked_options(job.clone()))
            .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    next_poll(&job).await;
    job.finish.store(true, Ordering::SeqCst);
    for _ in 0..8 {
        scheduler.schedule(id);
    }
    next_poll(&job).await;
    tokio::time::timeout(std::time::Duration::from_secs(3), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.calls.load(Ordering::SeqCst), 1);
    assert_eq!(job.polls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        scheduler.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn parked_worker_shutdown_and_reopen_preserves_pending_operation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let job = Arc::new(ParkedJob {
        calls: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
        seen: tokio::sync::Semaphore::new(0),
        finish: std::sync::atomic::AtomicBool::new(false),
        panic_next: std::sync::atomic::AtomicBool::new(false),
    });
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(&path, parked_options(job.clone())).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    next_poll(&job).await;
    scheduler.schedule(id);
    scheduler.shutdown().await.unwrap();
    job.finish.store(true, Ordering::SeqCst);
    let mut reopened =
        pi_rs_durable::runtime::Scheduler::open(&path, parked_options(job.clone())).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), reopened.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        reopened.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn parked_driver_clears_failure_when_it_takes_over_a_failed_retry() {
    let dir = tempfile::tempdir().unwrap();
    let job = Arc::new(ParkedJob {
        calls: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
        seen: tokio::sync::Semaphore::new(0),
        finish: std::sync::atomic::AtomicBool::new(false),
        panic_next: std::sync::atomic::AtomicBool::new(false),
    });
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), parked_options(job.clone()))
            .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let submission = scheduler.submit(id, None, "work".into()).unwrap();
    next_poll(&job).await;
    job.panic_next.store(true, Ordering::SeqCst);
    scheduler.schedule(id);
    next_poll(&job).await;
    // The original parked driver wakes and takes over the failed retry.
    tokio::time::timeout(std::time::Duration::from_secs(3), job.seen.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let cancel = CancellationToken::new();
    let wait = scheduler.wait_for_submission(id, submission, &cancel);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(250), wait)
            .await
            .is_err()
    );
    job.finish.store(true, Ordering::SeqCst);
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        scheduler.wait_for_submission(id, submission, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(job.calls.load(Ordering::SeqCst), 1);
    assert!(scheduler.shutdown().await.is_err()); // The retry panic is still joinable.
}

#[tokio::test]
async fn task_backed_tool_admits_and_resumes_after_reopen() {
    use pi_rs_durable::{
        task::{Task, TaskState},
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    use std::sync::{Mutex, mpsc};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("task-tool.db");
    let cancel = CancellationToken::new();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let handler: Arc<dyn Handler> = Arc::new(move |task: &Task| {
        assert_eq!(task.input(), &json!({}));
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!("task result")))
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let opts = || Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let id;
    let task_id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        let mut registry = Registry::default();
        registry.register_shared("job-task", true, handler.clone());
        let driver = h.start_tasks(registry).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.submit_and_run(id, None, "work".into(), &cancel)
            .await
            .unwrap();
        task_id = h
            .conversation(id)
            .unwrap()
            .tool_attempt()
            .unwrap()
            .operation_id;
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        release_tx.send(()).unwrap();
        for _ in 0..100 {
            if matches!(
                driver.get(task_id).unwrap().unwrap().state(),
                TaskState::Succeeded(_)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(matches!(
            driver.get(task_id).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
        driver.shutdown().unwrap();
    }
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert!(
        h.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content.contains("task result")))
    );
}

#[tokio::test]
async fn task_backed_tool_abort_waits_for_owned_task_cleanup() {
    use pi_rs_durable::{
        task::{Task, TaskState},
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    use std::sync::{Mutex, mpsc};
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        started_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!("must not publish")))
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), options, &cancel)
        .await
        .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler);
    registry.set_abort("job-task", |_: &Task| Ok(Step::Succeed(json!(null))));
    let driver = h.start_tasks(registry).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    let task_id = h
        .conversation(id)
        .unwrap()
        .tool_attempt()
        .unwrap()
        .operation_id;
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    h.request_abort(id).unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert!(h.conversation(id).unwrap().abort_requested);
    release_tx.send(()).unwrap();
    for _ in 0..100 {
        if driver.get(task_id).unwrap().unwrap().is_terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        driver.get(task_id).unwrap().unwrap().state(),
        &TaskState::Cancelled
    );
    h.resume_conversation(id, &cancel).await.unwrap();
    assert!(!h.conversation(id).unwrap().abort_requested);
    assert!(h.blocked(id).unwrap().is_none());
    assert!(!h.conversation(id).unwrap().messages.iter().any(
        |m| matches!(m, Message::Tool { content, .. } if content.contains("must not publish"))
    ));
    driver.shutdown().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn task_tool_kill_worker() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    use std::io::Write;
    let Ok(path) = std::env::var("PI_TASK_TOOL_KILL_DB") else {
        return;
    };
    let safe = std::env::var("PI_TASK_TOOL_KILL_UNSAFE").is_err();
    let handler: Arc<dyn Handler> = Arc::new(|_: &Task| -> Result<Step, String> {
        println!("TASK_TOOL_HANDLER_RUNNING");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        safe,
        1,
        handler.clone(),
    ));
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let cancel = CancellationToken::new();
    let mut h = Harness::open(path, opts, &cancel).await.unwrap();
    if std::env::var_os("PI_TASK_TOOL_MARKER").is_some() {
        let id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "work".into(), &cancel)
                .await
                .is_err()
        );
        println!("TASK_TOOL_MARKER_READY={id}");
        std::io::stdout().flush().unwrap();
        std::future::pending::<()>().await;
        return;
    }
    let mut registry = Registry::default();
    registry.register_shared("job-task", safe, handler);
    let _driver = h.start_tasks(registry).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    println!("TASK_TOOL_CONVERSATION={id}");
    std::io::stdout().flush().unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    std::future::pending::<()>().await;
}

#[cfg(unix)]
#[tokio::test]
async fn task_marker_crash_changed_definition_stays_blocked_on_retry() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Step},
        task_tool::TaskTool,
    };
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut process = Kill(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "task_tool_kill_worker", "--nocapture"])
            .env("PI_TASK_TOOL_KILL_DB", &path)
            .env("PI_TASK_TOOL_MARKER", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let (tx, rx) = mpsc::channel();
    let stdout = process.0.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some((_, id)) = line.split_once("TASK_TOOL_MARKER_READY=") {
                let _ = tx.send(id.parse::<uuid::Uuid>().unwrap());
                break;
            }
        }
    });
    let id = rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("unexpected")))
    });
    let opts = |version| Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(TaskTool::new(
                ToolSpec {
                    name: "job".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                },
                "job-task",
                true,
                version,
                handler.clone(),
            ))
            .register(),
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    {
        let h = Harness::open_paused(&path, opts(2)).unwrap();
        assert!(h.blocked(id).unwrap().unwrap().1.contains("identity"));
    }
    let h = Harness::open_paused(&path, opts(1)).unwrap();
    assert!(h.blocked(id).unwrap().unwrap().1.contains("identity"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn task_backed_tool_process_death_replays_task_without_readmitting() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut process = Kill(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "task_tool_kill_worker", "--nocapture"])
            .env("PI_TASK_TOOL_KILL_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut id = None;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some((_, value)) = line.split_once("TASK_TOOL_CONVERSATION=") {
                id = Some(value.parse::<uuid::Uuid>().unwrap());
            }
            if line.contains("TASK_TOOL_HANDLER_RUNNING") {
                let _ = tx.send(id.unwrap());
                break;
            }
        }
    });
    let id = rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("recovered")))
    });
    // A changed definition must be rejected before the recovered task can run;
    // restoring the original version is a valid retry.
    {
        let changed = Arc::new(TaskTool::new(
            ToolSpec {
                name: "job".into(),
                description: "".into(),
                parameters: json!({"type":"object"}),
            },
            "job-task",
            true,
            2,
            handler.clone(),
        ));
        let options = Options {
            model: Arc::new(ModelStub),
            tools: vec![changed.register()],
            restart_limit: 5,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let h = Harness::open_paused(&path, options).unwrap();
        let mut registry = Registry::default();
        registry.register_shared_versioned("job-task", 2, true, handler.clone());
        assert!(h.start_tasks(registry).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, options, &cancel).await.unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    for _ in 0..100 {
        h.resume_conversation(id, &cancel).await.unwrap();
        if matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert!(
        h.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content.contains("recovered")))
    );
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn task_backed_tool_without_driver_fails_before_admission() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let handler: Arc<dyn Handler> =
        Arc::new(|_: &Task| Ok(Step::Succeed(json!("created after retry"))));
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let opts = || Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "work".into(), &cancel)
                .await
                .is_err()
        );
    }
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    for _ in 0..100 {
        h.resume_conversation(id, &cancel).await.unwrap();
        if matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn task_backed_tool_rejects_unscoped_invocation() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Step},
        task_tool::TaskTool,
    };
    let handler: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!("unexpected"))));
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler,
    ));
    let _registration = adapter.register();
    let outcome = adapter
        .invoke(
            json!({}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: CancellationToken::new(),
            },
        )
        .await;
    assert!(matches!(outcome, Outcome::Skipped(_)));
}

#[tokio::test]
async fn task_backed_tool_scheduler_polls_to_completion() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    let dir = tempfile::tempdir().unwrap();
    let handler: Arc<dyn Handler> =
        Arc::new(|_: &Task| Ok(Step::Succeed(json!("scheduled result"))));
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        2,
        handler.clone(),
    ));
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut s = pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts).unwrap();
    let mut registry = Registry::default();
    registry.register_shared_versioned("job-task", 2, true, handler);
    let driver = s.start_tasks(registry).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    s.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn task_backed_tool_abort_preserves_already_completed_result() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let handler: Arc<dyn Handler> =
        Arc::new(|_: &Task| Ok(Step::Succeed(json!("completed before abort"))));
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    let task_id = h
        .conversation(id)
        .unwrap()
        .tool_attempt()
        .unwrap()
        .operation_id;
    for _ in 0..100 {
        if driver.get(task_id).unwrap().unwrap().is_terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(driver.get(task_id).unwrap().unwrap().is_terminal());
    h.request_abort(id).unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert!(h.conversation(id).unwrap().messages.iter().any(
        |m| matches!(m, Message::Tool { content, .. } if content.contains("completed before abort"))
    ));
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn pending_task_rejects_registry_replacement_and_accepts_same_definition() {
    use pi_rs_durable::{
        task::{Checkpoint, Task, TaskState, WaitFor},
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let handler: Arc<dyn Handler> = Arc::new(|_: &Task| {
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "wait".into(),
                data: json!(null),
            },
            condition: WaitFor::Signal("go".into()),
            children: vec![],
        })
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), options, &cancel)
        .await
        .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler.clone());
    let driver = h.start_tasks(registry).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    let task = h
        .conversation(id)
        .unwrap()
        .tool_attempt()
        .unwrap()
        .operation_id;
    for _ in 0..100 {
        if matches!(
            driver.get(task).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(matches!(
        driver.get(task).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    let mut changed = Registry::default();
    changed.register_shared_versioned("job-task", 2, true, handler.clone());
    assert!(driver.replace_registry(changed).is_err());
    let mut same = Registry::default();
    same.register_shared("job-task", true, handler.clone());
    driver.replace_registry(same).unwrap();
    assert!(matches!(
        driver.get(task).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    driver.shutdown().unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler.clone());
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let mut changed = Registry::default();
    changed.register_shared_versioned("job-task", 2, true, handler.clone());
    assert!(scheduler.replace_registry(changed).is_err());
    let mut same = Registry::default();
    same.register_shared("job-task", true, handler);
    scheduler.replace_registry(same).unwrap();
}

#[tokio::test]
async fn task_backed_tool_failure_requires_reconciliation() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let handler: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Fail("uncertain task".into())));
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        false,
        1,
        handler.clone(),
    ));
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", false, handler);
    let driver = h.start_tasks(registry).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    for _ in 0..100 {
        h.resume_conversation(id, &cancel).await.unwrap();
        if h.blocked(id)
            .unwrap()
            .is_some_and(|(_, r)| r.contains("uncertain task"))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let (operation, reason) = h.blocked(id).unwrap().unwrap();
    assert!(reason.contains("uncertain task"));
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Running
    );
    h.reconcile(id, operation, Outcome::Succeeded("reconciled".into()))
        .unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    driver.shutdown().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn task_backed_tool_process_death_unsafe_requires_reconciliation() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
        task_tool::TaskTool,
    };
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut process = Kill(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "task_tool_kill_worker", "--nocapture"])
            .env("PI_TASK_TOOL_KILL_DB", &path)
            .env("PI_TASK_TOOL_KILL_UNSAFE", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut id = None;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some((_, value)) = line.split_once("TASK_TOOL_CONVERSATION=") {
                id = Some(value.parse::<uuid::Uuid>().unwrap());
            }
            if line.contains("TASK_TOOL_HANDLER_RUNNING") {
                let _ = tx.send(id.unwrap());
                break;
            }
        }
    });
    let id = rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("unexpected replay")))
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        false,
        1,
        handler.clone(),
    ));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, options, &cancel).await.unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job-task", false, handler);
    let driver = h.start_tasks(registry).unwrap();
    for _ in 0..100 {
        h.resume_conversation(id, &cancel).await.unwrap();
        if h.blocked(id)
            .unwrap()
            .is_some_and(|(_, reason)| reason.contains("requires reconciliation"))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        h.blocked(id)
            .unwrap()
            .unwrap()
            .1
            .contains("requires reconciliation")
    );
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Running
    );
    driver.shutdown().unwrap();
}
