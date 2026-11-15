use durable_store::{Expected, Store};
use pi_rs_durable::{
    application_entry,
    document::{Address, Documents, TaskDocuments, Transaction},
    task::{
        Checkpoint, Task, TaskOwner, TaskState, WaitFor,
        scheduler::{Handler, Registry, Scheduler, Step},
    },
    task_store::TaskStore,
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use uuid::Uuid;

fn cp(phase: &str) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data: json!({}),
    }
}
fn address(name: &str) -> Address {
    Address {
        scope: "test".into(),
        name: name.into(),
        key: "".into(),
    }
}
fn parent() -> Task {
    Task::new(
        Uuid::from_u128(1),
        TaskOwner::Conversation(Uuid::from_u128(2)),
        cp("start"),
        3,
    )
    .unwrap()
    .registered("parent".into(), true)
}
fn child() -> Task {
    Task::new(
        Uuid::from_u128(3),
        TaskOwner::Task(parent().id()),
        cp("child"),
        3,
    )
    .unwrap()
    .registered("child".into(), true)
}
struct Work {
    conflict_path: Option<PathBuf>,
    calls: Arc<AtomicUsize>,
}
impl Handler for Work {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let old = docs.get(&address("write")).unwrap().unwrap();
        docs.get(&address("dependency")).unwrap();
        docs.put(
            &address("write"),
            json!({"n": 1}).as_object().unwrap().clone(),
        )
        .unwrap();
        docs.append_entry("app.first", json!(1)).unwrap();
        docs.append_entry("app.second", json!(2)).unwrap();
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = &self.conflict_path
            && call == 0
        {
            // Deterministically change a read-only dependency after it was read.
            let db = Store::open(path).unwrap();
            let mut tx = Transaction::new();
            tx.put(
                &address("dependency"),
                Expected::Missing,
                json!({"changed": true}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(&db).unwrap();
        }
        assert_eq!(old.value["n"], 0);
        Ok(Step::Wait {
            checkpoint: cp("next"),
            condition: WaitFor::Tasks(vec![child().id()]),
            children: vec![child()],
        })
    }
}
fn registry(path: Option<PathBuf>) -> Registry {
    let mut r = Registry::default();
    r.register(
        "parent",
        true,
        Work {
            conflict_path: path,
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );
    r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    r
}
fn initialize_conversation(path: &std::path::Path) {
    use durable_store::{Check, Mutation};
    let db = Store::open(path).unwrap();
    let id = Uuid::from_u128(2);
    let key = format!("runtime/conversation/{id}").into_bytes();
    let value = json!({"id": id, "config": {"model": "test", "system_prompt": "", "max_output_tokens": 1}, "messages": [], "submissions": [], "active": null, "phase": "Idle"});
    db.commit(
        &[Check {
            key: key.clone(),
            expected: Expected::Missing,
        }],
        &[Mutation::Put {
            key,
            value: serde_json::to_vec(&value).unwrap(),
        }],
    )
    .unwrap();
}
fn initialize(path: &std::path::Path) {
    initialize_conversation(path);
    let db = Store::open(path).unwrap();
    let mut tx = Transaction::new();
    tx.put(
        &address("write"),
        Expected::Missing,
        json!({"n": 0}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
}
#[test]
fn checkpoint_documents_and_child_survive_reopen_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut scheduler = Scheduler::open(&path, registry(None)).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&db);
    let entries = application_entry::list(&db, Uuid::from_u128(2)).unwrap();
    assert_eq!(
        entries.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(),
        vec!["app.first", "app.second"]
    );
    assert_ne!(entries[0].id, entries[1].id);
    assert!(
        entries
            .iter()
            .all(|e| e.author_task_id == parent().id() && e.conversation_id == Uuid::from_u128(2))
    );
    let p = tasks.load(parent().id()).unwrap().unwrap().task;
    assert_eq!(p.checkpoint(), &cp("next"));
    assert_eq!(
        p.state(),
        &TaskState::Waiting(WaitFor::Tasks(vec![child().id()]))
    );
    assert_eq!(tasks.load(child().id()).unwrap().unwrap().task, child());
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
}
#[test]
fn read_only_conflict_retries_on_next_tick_without_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut scheduler = Scheduler::open(&path, registry(Some(path.clone()))).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    let p = scheduler.get(parent().id()).unwrap().unwrap();
    assert_eq!(p.checkpoint(), &cp("start"));
    assert_eq!(p.state(), &TaskState::Ready);
    assert!(scheduler.get(child().id()).unwrap().is_none());
    assert!(
        application_entry::list(&Store::open(&path).unwrap(), Uuid::from_u128(2))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
    scheduler.tick(0).unwrap();
    assert_eq!(
        scheduler.get(parent().id()).unwrap().unwrap().checkpoint(),
        &cp("next")
    );
    assert!(scheduler.get(child().id()).unwrap().is_some());
    assert_eq!(
        application_entry::list(&Store::open(&path).unwrap(), Uuid::from_u128(2))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
}

#[test]
fn non_replay_safe_conflict_fails_without_reinvoking() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut r = Registry::default();
    r.register(
        "parent",
        false,
        Work {
            conflict_path: Some(path.clone()),
            calls: calls.clone(),
        },
    );
    r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let task = parent().registered("parent".into(), false);
    let mut scheduler = Scheduler::open(&path, r).unwrap();
    scheduler.submit(&task).unwrap();
    scheduler.tick(0).unwrap();
    assert!(
        matches!(scheduler.get(task.id()).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
    );
    assert!(scheduler.get(child().id()).unwrap().is_none());
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let mut reopened = Scheduler::open(&path, registry(None)).unwrap();
    reopened.tick(0).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
}

#[test]
fn child_creation_conflict_is_not_treated_as_document_retry() {
    use pi_rs_durable::task_store::Save;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let handler_path = path.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let mut r = Registry::default();
    r.register("parent", true, move |_: &Task| {
        handler_calls.fetch_add(1, Ordering::SeqCst);
        let db = Store::open(&handler_path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&child())]).unwrap();
        Ok(Step::Wait {
            checkpoint: cp("next"),
            condition: WaitFor::Tasks(vec![child().id()]),
            children: vec![child()],
        })
    });
    r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let mut scheduler = Scheduler::open(&path, r).unwrap();
    scheduler.submit(&parent()).unwrap();
    assert!(matches!(
        scheduler.tick(0),
        Err(pi_rs_durable::task::scheduler::Error::Document(
            pi_rs_durable::document::Error::Store(durable_store::Error::Conflict { .. })
        ))
    ));
    assert!(matches!(
        scheduler.tick(0),
        Err(pi_rs_durable::task::scheduler::Error::Poisoned)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn invocation_snapshot_read_your_writes_and_stale_rollback_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let handler_path = path.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    struct SnapshotWork {
        path: PathBuf,
        calls: Arc<AtomicUsize>,
    }
    impl Handler for SnapshotWork {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            if first {
                // The revision was captured before entering the handler, even before its first read.
                let db = Store::open(&self.path).unwrap();
                let old = Documents::new(&db).get(&address("write")).unwrap().unwrap();
                let mut tx = Transaction::new();
                tx.put(
                    &address("write"),
                    Expected::Version(old.version),
                    json!({"n": 9}).as_object().unwrap(),
                )
                .unwrap();
                tx.put(
                    &address("dependency"),
                    Expected::Missing,
                    json!({"n": 9}).as_object().unwrap(),
                )
                .unwrap();
                tx.commit(&db).unwrap();
            }
            assert_eq!(
                docs.get(&address("write")).unwrap().unwrap().value["n"],
                if first { 0 } else { 9 }
            );
            docs.put(
                &address("write"),
                json!({"n": 2}).as_object().unwrap().clone(),
            )
            .unwrap();
            assert_eq!(docs.get(&address("write")).unwrap().unwrap().value["n"], 2);
            assert_eq!(
                docs.get(&address("dependency"))
                    .unwrap()
                    .map(|d| d.value["n"].clone()),
                if first { None } else { Some(json!(9)) }
            );
            docs.put(
                &address("dependency"),
                json!({"n": 3}).as_object().unwrap().clone(),
            )
            .unwrap();
            if first {
                assert_eq!(
                    docs.get(&address("dependency")).unwrap().unwrap().version,
                    0
                );
            }
            docs.delete(&address("dependency")).unwrap();
            assert!(docs.get(&address("dependency")).unwrap().is_none());
            docs.put(
                &address("dependency"),
                json!({"n": 4}).as_object().unwrap().clone(),
            )
            .unwrap();
            Ok(Step::Continue(cp("next")))
        }
    }
    let make_registry = || {
        let mut r = Registry::default();
        r.register(
            "parent",
            true,
            SnapshotWork {
                path: handler_path.clone(),
                calls: observed.clone(),
            },
        );
        r
    };
    let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    assert_eq!(
        scheduler.get(parent().id()).unwrap().unwrap().checkpoint(),
        &cp("start")
    );
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        9
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("dependency"))
            .unwrap()
            .unwrap()
            .value["n"],
        9
    );
    drop(db);
    let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .load(parent().id())
            .unwrap()
            .unwrap()
            .task
            .checkpoint(),
        &cp("next")
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        2
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("dependency"))
            .unwrap()
            .unwrap()
            .value["n"],
        4
    );
}

#[test]
fn missing_document_create_delete_race_conflicts() {
    struct Aba(PathBuf);
    impl Handler for Aba {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            assert!(docs.get(&address("dependency")).unwrap().is_none());
            let db = Store::open(&self.0).unwrap();
            let mut tx = Transaction::new();
            tx.put(
                &address("dependency"),
                Expected::Missing,
                json!({}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(&db).unwrap();
            let version = Documents::new(&db)
                .get(&address("dependency"))
                .unwrap()
                .unwrap()
                .version;
            let mut tx = Transaction::new();
            tx.delete(&address("dependency"), version).unwrap();
            tx.commit(&db).unwrap();
            docs.get(&address("write")).unwrap();
            docs.put(
                &address("write"),
                json!({"n": 5}).as_object().unwrap().clone(),
            )
            .unwrap();
            Ok(Step::Continue(cp("next")))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut r = Registry::default();
    r.register("parent", true, Aba(path.clone()));
    let mut scheduler = Scheduler::open(&path, r).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .load(parent().id())
            .unwrap()
            .unwrap()
            .task
            .checkpoint(),
        &cp("start")
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
}

#[test]
fn failed_step_discards_staged_documents_after_reopen() {
    struct Fail;
    impl Handler for Fail {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&address("write")).unwrap();
            docs.put(
                &address("write"),
                json!({"n": 10}).as_object().unwrap().clone(),
            )
            .unwrap();
            assert_eq!(docs.get(&address("write")).unwrap().unwrap().value["n"], 10);
            docs.append_entry("app.failed", json!(null)).unwrap();
            Ok(Step::Fail("failed".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut r = Registry::default();
    r.register("parent", true, Fail);
    let mut scheduler = Scheduler::open(&path, r).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    assert!(
        application_entry::list(&db, Uuid::from_u128(2))
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        TaskStore::new(&db)
            .load(parent().id())
            .unwrap()
            .unwrap()
            .task
            .state(),
        TaskState::Failed(_)
    ));
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
}

#[test]
fn concurrent_entry_change_retries_without_partial_document_or_checkpoint() {
    use durable_store::{Check, Mutation};
    struct Racing(PathBuf, Arc<AtomicUsize>);
    impl Handler for Racing {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&address("write")).unwrap();
            docs.put(
                &address("write"),
                json!({"n": 7}).as_object().unwrap().clone(),
            )
            .unwrap();
            docs.append_entry("app.record", json!(7)).unwrap();
            if self.1.fetch_add(1, Ordering::SeqCst) == 0 {
                let db = Store::open(&self.0).unwrap();
                let key =
                    format!("durable/application-entries/{}", Uuid::from_u128(2)).into_bytes();
                db.commit(
                    &[Check {
                        key: key.clone(),
                        expected: Expected::Missing,
                    }],
                    &[Mutation::Put {
                        key,
                        value: b"[]".to_vec(),
                    }],
                )
                .unwrap();
            }
            Ok(Step::Continue(cp("next")))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut registry = Registry::default();
    registry.register(
        "parent",
        true,
        Racing(path.clone(), Arc::new(AtomicUsize::new(0))),
    );
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    let db = Store::open(&path).unwrap();
    assert!(
        application_entry::list(&db, Uuid::from_u128(2))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
    assert_eq!(
        scheduler.get(parent().id()).unwrap().unwrap().checkpoint(),
        &cp("start")
    );
    scheduler.tick(0).unwrap();
    drop(scheduler);
    assert_eq!(
        application_entry::list(&db, Uuid::from_u128(2))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        7
    );
}

#[test]
fn abort_during_handler_discards_entries_and_documents() {
    use pi_rs_durable::task_driver::Driver;
    use std::sync::{Mutex, mpsc};
    struct Gated(mpsc::Sender<()>, Mutex<mpsc::Receiver<()>>);
    impl Handler for Gated {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&address("write")).unwrap();
            docs.put(
                &address("write"),
                json!({"n": 8}).as_object().unwrap().clone(),
            )
            .unwrap();
            docs.append_entry("app.aborted", json!(8)).unwrap();
            self.0.send(()).unwrap();
            self.1.lock().unwrap().recv().unwrap();
            Ok(Step::Continue(cp("next")))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut registry = Registry::default();
    registry.register("parent", true, Gated(started_tx, Mutex::new(release_rx)));
    let scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&parent()).unwrap();
    let driver = Driver::start(scheduler);
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    driver.request_abort(parent().id()).unwrap();
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    let db = Store::open(&path).unwrap();
    assert!(
        application_entry::list(&db, Uuid::from_u128(2))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
    assert_eq!(
        TaskStore::new(&db)
            .load(parent().id())
            .unwrap()
            .unwrap()
            .task
            .checkpoint(),
        &cp("start")
    );
}

#[test]
fn append_across_steps_with_nested_authors_and_reject_reserved_kinds() {
    struct Append;
    impl Handler for Append {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            task: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            assert!(docs.append_entry("", json!(null)).is_err());
            assert!(docs.append_entry("pi.reserved", json!(null)).is_err());
            if matches!(task.owner(), TaskOwner::Task(_)) {
                docs.append_entry("app.child", json!("child")).unwrap();
                return Ok(Step::Succeed(json!(null)));
            }
            docs.append_entry("app.step", json!(task.checkpoint().phase))
                .unwrap();
            if task.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: cp("next"),
                    condition: WaitFor::Tasks(vec![child().id()]),
                    children: vec![child()],
                })
            } else {
                Ok(Step::Succeed(json!(null)))
            }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize_conversation(&path);
    let mut registry = Registry::default();
    registry.register("parent", true, Append);
    registry.register("child", true, Append);
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&parent()).unwrap();
    for _ in 0..6 {
        scheduler.tick(0).unwrap();
    }
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let entries = application_entry::list(&db, Uuid::from_u128(2)).unwrap();
    assert_eq!(
        entries.iter().map(|e| e.data.clone()).collect::<Vec<_>>(),
        vec![json!("start"), json!("child"), json!("next")]
    );
    assert!(
        application_entry::list(&db, Uuid::new_v4())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn signal_after_application_entry_remains_fail_closed() {
    use pi_rs_durable::task_store::SignalToken;
    struct EntryBeforeSignal;
    impl Handler for EntryBeforeSignal {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            task: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&address("write")).unwrap();
            docs.put(
                &address("write"),
                json!({"n":9}).as_object().unwrap().clone(),
            )
            .unwrap();
            docs.append_entry("app.record", json!(1)).unwrap();
            let token = SignalToken::new("answer", 1).unwrap();
            assert!(matches!(
                docs.signal_at(task.id(), &token, "a", json!(1), 0),
                Err(pi_rs_durable::document::Error::ReadAfterWrite)
            ));
            Ok(Step::Continue(cp("done")))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut registry = Registry::default();
    registry.register("parent", true, EntryBeforeSignal);
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let token = SignalToken::new("answer", 1).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(parent().id(), &token, "a")
            .unwrap(),
        None
    );
    assert_eq!(
        application_entry::list(&db, Uuid::from_u128(2))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        9
    );
}
