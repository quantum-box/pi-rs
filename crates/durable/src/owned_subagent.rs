//! Opt-in task-owned subagent admission. Harness abort traversal follows foreground edges.
use crate::{
    ownership_edge::{self, Edge, Mode},
    runtime::{Config, Conversation, Error, Harness, Scheduler},
    task::{Task, TaskOwner, TaskState},
    task_store,
};
use durable_store::{Check, Expected, Mutation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A key is scoped to one task and permanently binds its configuration and mode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Admission {
    pub task: Uuid,
    pub task_owner: TaskOwner,
    pub key: String,
    pub child: Uuid,
    pub config: Config,
    pub mode: Mode,
    /// Initial tool selection, bound to this reservation.
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    /// Initial submission, if this reservation was created with input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission: Option<InitialSubmission>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialSubmission {
    pub id: Uuid,
    pub content: String,
}

fn reservation_key(task: Uuid, name: &str) -> Vec<u8> {
    let mut key = format!("durable/owned-subagent/{task}/").into_bytes();
    key.extend_from_slice(name.as_bytes());
    key
}
fn child_key(id: Uuid) -> Vec<u8> {
    format!("runtime/conversation/{id}").into_bytes()
}
fn invalid() -> Error {
    Error::Invalid("invalid owned subagent admission".into())
}

impl Harness {
    /// Atomically creates an empty child, its immutable ownership edge and its
    /// reservation, while advancing the task's storage version. Retrying the
    /// same key and parameters returns the original child, including after an
    /// abort or reopen. Different parameters for the key are rejected.
    ///
    /// A storage error has an uncertain outcome: reopen before retrying. This
    /// API does not submit input, schedule, traverse, or abort the child.
    pub fn ensure_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
    ) -> Result<Admission, Error> {
        self.admit_task_owned_subagent(task, owner, name, config, mode, None, None)
    }

    /// Atomically reserve a child and admit its first input. The key permanently
    /// binds the input as well as the owner, configuration and ownership mode.
    /// A storage error has an uncertain outcome; reopen before retrying.
    pub fn submit_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
    ) -> Result<Admission, Error> {
        if content.trim().is_empty() {
            return Err(invalid());
        }
        self.admit_task_owned_subagent(task, owner, name, config, mode, Some(content), None)
    }

    /// Atomically create an owned child with its initial tool selection.
    pub fn ensure_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        self.admit_task_owned_subagent(task, owner, name, config, mode, None, allowed_tools)
    }
    /// Atomically create a restricted child and queue its first input.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        if content.trim().is_empty() {
            return Err(invalid());
        }
        self.admit_task_owned_subagent(
            task,
            owner,
            name,
            config,
            mode,
            Some(content),
            allowed_tools,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn admit_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: Option<String>,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        crate::runtime::validate_allowed_tools(&allowed_tools)?;
        if name.is_empty() || config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(invalid());
        }
        let key = reservation_key(task, name);
        loop {
            let store = self.subagent_store()?;
            if let Some(entry) = store.get(&key)? {
                let a: Admission = serde_json::from_slice(&entry.value)?;
                if a.task != task
                    || a.task_owner != owner
                    || a.key != name
                    || a.config != config
                    || a.mode != mode
                    || a.allowed_tools != allowed_tools
                    || a.submission.as_ref().map(|s| &s.content) != content.as_ref()
                    || a.task_owner == TaskOwner::Conversation(a.child)
                {
                    return Err(invalid());
                }
                let edge = store
                    .get(&ownership_edge::edge_key(a.child))?
                    .ok_or_else(invalid)?;
                let edge: Edge = serde_json::from_slice(&edge.value)?;
                let child = store.get(&child_key(a.child))?.ok_or_else(invalid)?;
                let child: Conversation = serde_json::from_slice(&child.value)?;
                if edge
                    != (Edge {
                        task,
                        task_owner: owner.clone(),
                        child: a.child,
                        mode,
                    })
                    || child.id != a.child
                {
                    return Err(invalid());
                }
                return Ok(a);
            }
            let tk = task_store::key(task);
            let snapshot = store.get(&tk)?.ok_or_else(invalid)?;
            let t: Task = serde_json::from_slice(&snapshot.value)?;
            if t.id() != task
                || t.owner() != &owner
                || t.abort_requested()
                || !matches!(
                    t.state(),
                    TaskState::Ready | TaskState::Running | TaskState::Waiting(_)
                )
            {
                return Err(invalid());
            }
            let child = Uuid::new_v4();
            if owner == TaskOwner::Conversation(child) {
                continue;
            }
            let a = Admission {
                task,
                task_owner: owner.clone(),
                key: name.into(),
                child,
                config: config.clone(),
                mode,
                allowed_tools: allowed_tools.clone(),
                submission: content.as_ref().map(|content| InitialSubmission {
                    id: Uuid::new_v4(),
                    content: content.clone(),
                }),
            };
            let edge = Edge {
                task,
                task_owner: owner.clone(),
                child,
                mode,
            };
            let ek = ownership_edge::edge_key(child);
            let mut child_state = Harness::empty_conversation(child, config.clone());
            child_state.allowed_tools = allowed_tools.clone();
            if let Some(initial) = &a.submission {
                child_state.admit_initial_input(initial.id, initial.content.clone());
            }
            let mut prepared = self.prepare_save(&child_state, Expected::Missing)?;
            let mut checks =
                ownership_edge::owner_chain_checks(store, task, child).map_err(|e| match e {
                    ownership_edge::Error::Store(e) => Error::from(e),
                    ownership_edge::Error::Json(e) => Error::from(e),
                    _ => invalid(),
                })?;
            checks.extend([
                Check {
                    key: tk.clone(),
                    expected: Expected::Version(snapshot.version),
                },
                Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: ek.clone(),
                    expected: Expected::Missing,
                },
            ]);
            let mutations = [
                Mutation::Put {
                    key: tk,
                    value: snapshot.value,
                },
                Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&a)?,
                },
                Mutation::Put {
                    key: ek,
                    value: serde_json::to_vec(&edge)?,
                },
            ];
            prepared.checks.extend(checks);
            prepared.mutations.extend(mutations);
            match store.commit(&prepared.checks, &prepared.mutations) {
                Ok(()) => return Ok(a),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        }
    }
}
impl Scheduler {
    /// Commit ownership and initial input before scheduling the child. Reopening
    /// also schedules committed children if the process stops before scheduling.
    pub fn submit_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
    ) -> Result<Admission, Error> {
        let admission = self
            .subagent_harness()
            .submit_task_owned_subagent(task, owner, name, config, mode, content)?;
        self.schedule(admission.child);
        Ok(admission)
    }

    /// Commit a restricted child and initial input before scheduling it.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        let admission = self
            .subagent_harness()
            .submit_task_owned_subagent_restricted(
                task,
                owner,
                name,
                config,
                mode,
                content,
                allowed_tools,
            )?;
        self.schedule(admission.child);
        Ok(admission)
    }
    pub fn ensure_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        self.subagent_harness()
            .ensure_task_owned_subagent_restricted(task, owner, name, config, mode, allowed_tools)
    }

    /// Reserve an owned child without scheduling it.
    pub fn ensure_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
    ) -> Result<Admission, Error> {
        self.subagent_harness()
            .ensure_task_owned_subagent(task, owner, name, config, mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ownership_edge::EdgeStore,
        recovery::UncertainPolicy,
        runtime::Options,
        task::{Checkpoint, Task},
        task_store::{Save, TaskStore},
    };
    use async_trait::async_trait;
    use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
    use std::sync::{Arc, Barrier};

    struct Stub;
    #[async_trait]
    impl Model for Stub {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
            unreachable!()
        }
    }
    fn options() -> Options {
        Options {
            model: Arc::new(Stub),
            tools: vec![],
            restart_limit: 1,
            max_turns: 1,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        }
    }
    fn setup(h: &mut Harness) -> (Uuid, TaskOwner) {
        let parent = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(parent);
        let task = Task::new(
            Uuid::new_v4(),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&task)])
            .unwrap();
        (task.id(), owner)
    }
    #[test]
    fn completing_owner_tracks_work_added_to_owned_conversation() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Scheduler, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let root = Task::new(
            Uuid::from_u128(1),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let owned = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        let mut child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(owned.child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        child.start().unwrap();
        child
            .wait(crate::task::WaitFor::DeadlineMillis(10))
            .unwrap();
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&child)])
            .unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("root")))
        });
        registry.register("child", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("child")))
        });
        let mut scheduler = Scheduler::attach(h.owner.clone(), registry).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        let late = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Conversation(owned.child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&late)])
            .unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(late.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(serde_json::json!("child"))
        );
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        scheduler.tick(10).unwrap();
        scheduler.tick(10).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(serde_json::json!("root"))
        );
    }

    #[test]
    fn abort_of_completing_owner_marks_owned_conversation_work() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Scheduler, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root = Task::new(Uuid::from_u128(1), owner.clone(), checkpoint.clone(), 3)
            .unwrap()
            .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let owned = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(owned.child),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&child)])
            .unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("root")))
        });
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
        );
        let mut scheduler = Scheduler::attach(h.owner.clone(), registry).unwrap();
        scheduler.tick(0).unwrap();
        h.request_abort_task(root.id()).unwrap();
        assert!(scheduler.get(root.id()).unwrap().unwrap().abort_requested());
        assert!(
            scheduler
                .get(child.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        assert!(h.conversation(owned.child).unwrap().abort_requested);
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        assert!(!scheduler.tick(0).unwrap());
    }

    #[test]
    fn restricted_admission_binds_selection_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let allowed = Some(vec!["read".to_string()]);
        let a = h
            .submit_task_owned_subagent_restricted(
                task,
                owner.clone(),
                "restricted",
                config(),
                Mode::Foreground,
                "work".into(),
                allowed.clone(),
            )
            .unwrap();
        assert_eq!(h.conversation(a.child).unwrap().allowed_tools, allowed);
        assert_eq!(h.conversation(a.child).unwrap().submissions.len(), 1);
        assert!(
            h.submit_task_owned_subagent_restricted(
                task,
                owner.clone(),
                "restricted",
                config(),
                Mode::Foreground,
                "work".into(),
                None
            )
            .is_err()
        );
        assert!(
            h.ensure_task_owned_subagent_restricted(
                task,
                owner.clone(),
                "bad",
                config(),
                Mode::Foreground,
                Some(vec!["x".into(), "x".into()])
            )
            .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.submit_task_owned_subagent_restricted(
                task,
                owner,
                "restricted",
                config(),
                Mode::Foreground,
                "work".into(),
                allowed
            )
            .unwrap(),
            a
        );
    }

    #[test]
    fn restricted_concurrent_admissions_share_one_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        let barrier = Arc::new(Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let mut worker = h.worker();
                let owner = owner.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    worker
                        .submit_task_owned_subagent_restricted(
                            task,
                            owner,
                            "job",
                            config(),
                            Mode::Foreground,
                            "work".into(),
                            Some(vec!["read".into()]),
                        )
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert!(results.iter().all(|a| a == &results[0]));
        let child = h.conversation(results[0].child).unwrap();
        assert_eq!(child.allowed_tools, Some(vec!["read".into()]));
        assert_eq!(child.submissions.len(), 1);
    }

    #[test]
    fn submitted_child_is_atomic_idempotent_and_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mut worker = h.worker();
                let owner = owner.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    worker
                        .submit_task_owned_subagent(
                            task,
                            owner,
                            "job",
                            config(),
                            Mode::Foreground,
                            "hello".into(),
                        )
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(results.iter().all(|a| a == &results[0]));
        let a = &results[0];
        let child = h.conversation(a.child).unwrap();
        assert_eq!(child.submissions.len(), 1);
        assert_eq!(child.submissions[0].id, a.submission.as_ref().unwrap().id);
        assert_eq!(child.submissions[0].content, "hello");
        let history = h
            .history(
                a.child,
                crate::observer::Cursor(0),
                std::num::NonZeroUsize::new(10).unwrap(),
            )
            .unwrap();
        assert_eq!(history.records.len(), 1);
        assert!(history.records[0].value.to_string().contains("hello"));
        assert_eq!(
            EdgeStore::new(h.subagent_store().unwrap())
                .children(task)
                .unwrap()
                .len(),
            1
        );
        assert!(
            h.submit_task_owned_subagent(
                task,
                owner.clone(),
                "job",
                config(),
                Mode::Foreground,
                "changed".into()
            )
            .is_err()
        );
        assert!(
            h.ensure_task_owned_subagent(task, owner.clone(), "job", config(), Mode::Foreground)
                .is_err()
        );
        h.request_abort_task(task).unwrap();
        assert!(h.conversation(a.child).unwrap().abort_requested);
        assert!(
            h.submit_task_owned_subagent(
                task,
                owner.clone(),
                "other",
                config(),
                Mode::Foreground,
                "hello".into()
            )
            .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.submit_task_owned_subagent(
                task,
                owner,
                "job",
                config(),
                Mode::Foreground,
                "hello".into()
            )
            .unwrap(),
            *a
        );
    }

    #[test]
    fn submission_races_parent_abort_without_stranding_foreground_child() {
        for _ in 0..30 {
            let dir = tempfile::tempdir().unwrap();
            let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let (task, owner) = setup(&mut h);
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let b = barrier.clone();
            let admission = std::thread::spawn(move || {
                b.wait();
                worker.submit_task_owned_subagent(
                    task,
                    owner,
                    "job",
                    config(),
                    Mode::Foreground,
                    "hello".into(),
                )
            });
            barrier.wait();
            h.request_abort_task(task).unwrap();
            if let Ok(a) = admission.join().unwrap() {
                let child = h.conversation(a.child).unwrap();
                assert!(child.abort_requested);
                assert_eq!(child.submissions.len(), 1);
            } else {
                assert!(
                    EdgeStore::new(h.subagent_store().unwrap())
                        .children(task)
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }

    #[tokio::test]
    async fn submitted_children_schedule_and_recover_across_background_boundary() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let fg = h
            .submit_task_owned_subagent(
                task,
                owner.clone(),
                "fg",
                config(),
                Mode::Foreground,
                "recover".into(),
            )
            .unwrap();
        drop(h); // committed, before any scheduler was started
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let bg = scheduler
            .submit_task_owned_subagent(
                task,
                owner,
                "bg",
                config(),
                Mode::Background,
                "background".into(),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.abort_task(task).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), scheduler.wait(fg.child))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            scheduler.conversation(bg.child).unwrap().submissions[0].status,
            crate::runtime::Status::Running
        ));
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn duplicate_admission_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let old = TaskStore::new(h.subagent_store().unwrap())
            .load(task)
            .unwrap()
            .unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mut h = h.worker();
                let b = barrier.clone();
                let owner = owner.clone();
                std::thread::spawn(move || {
                    b.wait();
                    h.ensure_task_owned_subagent(task, owner, "same", config(), Mode::Foreground)
                        .unwrap()
                })
            })
            .collect();
        let admissions: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(admissions.iter().all(|a| a == &admissions[0]));
        let store = h.subagent_store().unwrap();
        assert!(matches!(
            TaskStore::new(store).save(&[Save::Update(&old)]),
            Err(crate::task_store::Error::Store(
                durable_store::Error::Conflict { .. }
            ))
        ));
        assert_eq!(EdgeStore::new(store).children(task).unwrap().len(), 1);
        assert!(
            h.ensure_task_owned_subagent(task, owner.clone(), "same", config(), Mode::Background)
                .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.ensure_task_owned_subagent(task, owner, "same", config(), Mode::Foreground)
                .unwrap(),
            admissions[0]
        );
        assert_eq!(
            h.conversation(admissions[0].child).unwrap().id,
            admissions[0].child
        );
    }
    #[test]
    fn abort_first_never_creates_a_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        crate::task_scheduler::request_abort_subtree(
            &TaskStore::new(h.subagent_store().unwrap()),
            task,
        )
        .unwrap();
        assert!(
            h.ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground)
                .is_err()
        );
        assert!(
            EdgeStore::new(h.subagent_store().unwrap())
                .children(task)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn foreground_traversal_persists_and_skips_background() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&nested)])
            .unwrap();
        h.request_abort(parent).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.conversation(parent).unwrap().abort_requested);
        assert!(h.conversation(child).unwrap().abort_requested);
        assert!(!h.conversation(background).unwrap().abort_requested);
        let ts = TaskStore::new(h.subagent_store().unwrap());
        assert!(ts.load(task).unwrap().unwrap().task.abort_requested());
        assert!(
            ts.load(nested.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[tokio::test]
    async fn background_abort_cancels_active_children_and_waits_for_tasks() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        drop(h);
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        scheduler.submit(child, None, "active".into()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(8),
            scheduler.abort_and_wait_with_background(parent),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!matches!(
            scheduler.conversation(child).unwrap().submissions[0].status,
            crate::runtime::Status::Running
        ));
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn task_abort_traverses_foreground_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(task, owner, "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        h.request_abort_task(task).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(task)
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        assert!(h.conversation(child).unwrap().abort_requested);
    }

    #[test]
    fn driver_recovers_child_abort_when_owner_is_still_live() {
        for mode in [Mode::Foreground, Mode::Background] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let (task, owner) = setup(&mut h);
            let child = h
                .ensure_task_owned_subagent(task, owner, "child", config(), mode)
                .unwrap()
                .child;
            // A separate legacy edge has a mismatched owner. Its unmarked
            // conversation must not prevent this child's recovery.
            let unrelated = h.create_conversation(config()).unwrap();
            let corrupt = Edge {
                task,
                task_owner: TaskOwner::Task(Uuid::new_v4()),
                child: unrelated,
                mode: Mode::Foreground,
            };
            h.subagent_store()
                .unwrap()
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: ownership_edge::edge_key(unrelated),
                        value: serde_json::to_vec(&corrupt).unwrap(),
                    }],
                )
                .unwrap();
            h.request_abort(child).unwrap();
            assert!(
                !TaskStore::new(h.subagent_store().unwrap())
                    .load(task)
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
            );
            drop(h);
            let h = Harness::open_paused(&path, options()).unwrap();
            let driver = h.start_tasks(Default::default()).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
            while h.conversation(child).unwrap().abort_requested {
                assert!(
                    std::time::Instant::now() < deadline,
                    "marked child was not recovered"
                );
                std::thread::yield_now();
            }
            assert!(!driver.get(task).unwrap().unwrap().abort_requested());
            driver.shutdown().unwrap();
            drop(h);
            let reopened = Harness::open_paused(&path, options()).unwrap();
            assert!(!reopened.conversation(child).unwrap().abort_requested);
        }
    }

    #[test]
    fn driver_reports_mismatched_marked_edge_without_clearing_abort() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, _) = setup(&mut h);
        let child = h.create_conversation(config()).unwrap();
        h.request_abort(child).unwrap();
        let corrupt = Edge {
            task,
            task_owner: TaskOwner::Task(Uuid::new_v4()),
            child,
            mode: Mode::Foreground,
        };
        h.subagent_store()
            .unwrap()
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(child),
                    value: serde_json::to_vec(&corrupt).unwrap(),
                }],
            )
            .unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let driver = h.start_tasks(Default::default()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
        loop {
            if let Err(error) = driver.get(task) {
                assert!(
                    error.to_string().contains("ownership edge owner mismatch"),
                    "{error}"
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "corrupt ownership was not reported"
            );
            std::thread::yield_now();
        }
        assert!(h.conversation(child).unwrap().abort_requested);
        assert!(driver.shutdown().is_err());
        drop(h);
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(reopened.conversation(child).unwrap().abort_requested);
    }

    #[test]
    fn driver_recovers_persisted_owned_cleanup_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        h.request_abort_task(task).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let driver = h.start_tasks(Default::default()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while h.conversation(child).unwrap().abort_requested
            || !driver.get(task).unwrap().unwrap().is_terminal()
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(!h.conversation(background).unwrap().abort_requested);
        driver.shutdown().unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(!h.conversation(child).unwrap().abort_requested);
        assert!(!h.conversation(background).unwrap().abort_requested);
    }

    #[tokio::test]
    async fn owner_shutdown_drains_cleanup_before_joining_handler() {
        use crate::{
            extension,
            runtime::RuntimeOwner,
            task_scheduler::{Registry, Step},
        };
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, task_owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(task, task_owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let waiting = Task::new(
            Uuid::new_v4(),
            task_owner,
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap()
        .registered("wait-cleanup".into(), true);
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&waiting)])
            .unwrap();
        drop(h);

        // Publish a worker to the handler only after opening the owner. Hold the
        // child's worker lock so cleanup cannot win the shutdown race early.
        let worker_slot = Arc::new(std::sync::Mutex::new(None::<Harness>));
        let slot = worker_slot.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let mut registry = Registry::default();
        registry.register("wait-cleanup", true, move |_: &Task| {
            entered_tx.send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|h| !h.conversation(child).unwrap().abort_requested)
                {
                    return Ok(Step::Succeed(serde_json::Value::Null));
                }
                if Instant::now() >= deadline {
                    return Err("cleanup did not complete".into());
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let mut owner =
            RuntimeOwner::open(&path, options(), extension::Registry::default(), registry).unwrap();
        let h = owner.scheduler_mut().subagent_harness();
        let root = h.owner.conversation_controls().root();
        let guard = h
            .owner
            .conversation_controls()
            .get(child)
            .worker
            .clone()
            .lock_owned()
            .await;
        h.request_abort_task(task).unwrap();
        *worker_slot.lock().unwrap() = Some(h.worker());
        tokio::task::spawn_blocking(move || {
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        })
        .await
        .unwrap();
        let shutdown = tokio::spawn(owner.shutdown());
        tokio::time::timeout(Duration::from_secs(5), root.cancelled())
            .await
            .unwrap();
        drop(guard);
        tokio::time::timeout(Duration::from_secs(8), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(worker_slot.lock().unwrap().take());
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(matches!(
            TaskStore::new(reopened.subagent_store().unwrap())
                .load(waiting.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            crate::task::TaskState::Succeeded(_)
        ));
    }

    #[test]
    fn standalone_driver_abort_races_foreground_admission_atomically() {
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let (task, owner) = setup(&mut h);
            let driver = h.start_tasks(Default::default()).unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let b = barrier.clone();
            let admission = std::thread::spawn(move || {
                b.wait();
                worker.ensure_task_owned_subagent(task, owner, "fg", config(), Mode::Foreground)
            });
            barrier.wait();
            let abort = driver.request_abort(task);
            let admitted = admission.join().unwrap();
            assert!(abort.is_ok());
            assert!(driver.get(task).unwrap().unwrap().abort_requested());
            if let Ok(child) = admitted {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while h.conversation(child.child).unwrap().abort_requested {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
            }
            driver.shutdown().unwrap();
        }
    }

    #[tokio::test]
    async fn scheduler_cancels_active_foreground_but_not_background() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        drop(h);
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        scheduler.submit(child, None, "child".into()).unwrap();
        scheduler
            .submit(background, None, "background".into())
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.abort(parent).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), scheduler.wait(child))
            .await
            .unwrap()
            .unwrap();
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!scheduler.conversation(background).unwrap().abort_requested);
        assert!(scheduler.conversation(parent).unwrap().abort_requested);
        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn driver_abort_joins_active_owned_conversation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        drop(h);
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        scheduler.submit(child, None, "active".into()).unwrap();
        scheduler
            .submit(background, None, "active background".into())
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        driver.request_abort(task).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                if !scheduler.conversation(child).unwrap().abort_requested
                    && driver.get(task).unwrap().unwrap().is_terminal()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!scheduler.conversation(parent).unwrap().abort_requested);
        assert!(!scheduler.conversation(background).unwrap().abort_requested);
        assert!(matches!(
            scheduler.conversation(background).unwrap().submissions[0].status,
            crate::runtime::Status::Running
        ));
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_waits_for_owned_conversation_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (parent_task, owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(parent_task, owner, "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&nested)])
            .unwrap();
        let ts = TaskStore::new(h.subagent_store().unwrap());
        ts.reserve(&ts.load(parent_task).unwrap().unwrap()).unwrap();
        h.request_abort_task(parent_task).unwrap();
        drop(h); // crash with the parent handler reserved

        let mut h = Harness::open_paused(&path, options()).unwrap();
        let mut tasks = h.attach_tasks(Default::default()).unwrap();
        assert!(tasks.tick(0).unwrap()); // nested task cleanup
        assert!(tasks.get(nested.id()).unwrap().unwrap().is_terminal());
        assert!(!tasks.tick(0).unwrap()); // child conversation has not settled
        assert!(!tasks.get(parent_task).unwrap().unwrap().is_terminal());
        h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        assert!(!h.conversation(child).unwrap().abort_requested);
        assert!(tasks.tick(0).unwrap());
        assert!(tasks.get(parent_task).unwrap().unwrap().is_terminal());
    }

    #[tokio::test]
    async fn direct_legacy_cycle_abort_and_wait_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let edge = Edge {
            task,
            task_owner: TaskOwner::Conversation(parent),
            child: parent,
            mode: Mode::Foreground,
        };
        let store = h.subagent_store().unwrap();
        assert!(EdgeStore::new(store).prepare_create(&edge).is_err());
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        drop(h);
        let mut scheduler = Scheduler::open(&path, options()).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            scheduler.abort_and_wait(parent),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        assert!(!scheduler.conversation(parent).unwrap().abort_requested);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn cyclic_conversation_wait_does_not_overtake_acyclic_descendant() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let checkpoint = || Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root_task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(parent),
            checkpoint(),
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&root_task)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                root_task.id(),
                TaskOwner::Conversation(parent),
                "child",
                config(),
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(child),
            checkpoint(),
            1,
        )
        .unwrap();
        let leaf = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(nested.id()),
            checkpoint(),
            1,
        )
        .unwrap();
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&nested), Save::Create(&leaf)])
            .unwrap();
        let edge = Edge {
            task: nested.id(),
            task_owner: TaskOwner::Conversation(child),
            child: parent,
            mode: Mode::Foreground,
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        h.request_abort(parent).unwrap();
        let mut scheduler = h.attach_tasks(Default::default()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.get(leaf.id()).unwrap().unwrap().is_terminal());
        assert!(
            !scheduler
                .get(root_task.id())
                .unwrap()
                .unwrap()
                .is_terminal()
        );
        assert!(!scheduler.get(nested.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn task_only_cycle_break_keeps_acyclic_children_first_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        let task = |id, owner| {
            Task::new(
                id,
                TaskOwner::Task(owner),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap()
        };
        let store = h.subagent_store().unwrap();
        let tasks = [task(a, b), task(b, a), task(c, a)];
        let mutations: Vec<_> = tasks
            .iter()
            .map(|t| Mutation::Put {
                key: crate::task_store::key(t.id()),
                value: serde_json::to_vec(t).unwrap(),
            })
            .collect();
        store.commit(&[], &mutations).unwrap();
        h.request_abort_task(a).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let mut scheduler = h.attach_tasks(Default::default()).unwrap();
        for _ in 0..3 {
            assert!(scheduler.tick(0).unwrap());
            if !scheduler.get(c).unwrap().unwrap().is_terminal() {
                assert!(!scheduler.get(a).unwrap().unwrap().is_terminal());
                assert!(!scheduler.get(b).unwrap().unwrap().is_terminal());
            }
        }
        for id in [a, b, c] {
            assert!(scheduler.get(id).unwrap().unwrap().is_terminal());
        }
        assert!(!scheduler.tick(0).unwrap());
    }

    #[tokio::test]
    async fn cyclic_legacy_cleanup_recovers_after_racing_admission_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&nested)])
            .unwrap();
        let edge = Edge {
            task: nested.id(),
            task_owner: TaskOwner::Conversation(child),
            child: parent,
            mode: Mode::Foreground,
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();

        // A writer prepared before the abort must not overwrite the persisted
        // mark, even when the legacy graph contains a cycle.
        let stale = TaskStore::new(h.subagent_store().unwrap())
            .load(nested.id())
            .unwrap()
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut other = h.worker();
        let b = barrier.clone();
        let nested_id = nested.id();
        let admission = std::thread::spawn(move || {
            b.wait();
            other.ensure_task_owned_subagent(
                nested_id,
                TaskOwner::Conversation(child),
                "racing",
                config(),
                Mode::Foreground,
            )
        });
        let writer_harness = h.worker();
        let mut competing = stale.clone();
        let b = barrier.clone();
        let writer = std::thread::spawn(move || {
            competing.task.request_abort().unwrap();
            b.wait();
            TaskStore::new(writer_harness.subagent_store().unwrap())
                .save(&[Save::Update(&competing)])
        });
        barrier.wait();
        h.request_abort(parent).unwrap();
        assert!(matches!(
            writer.join().unwrap(),
            Ok(())
                | Err(crate::task_store::Error::Store(
                    durable_store::Error::Conflict { .. }
                ))
        ));
        assert!(matches!(
            TaskStore::new(h.subagent_store().unwrap()).save(&[Save::Update(&stale)]),
            Err(crate::task_store::Error::Store(
                durable_store::Error::Conflict { .. }
            ))
        ));
        let admitted = admission.join().unwrap().ok().map(|a| a.child);
        if let Some(id) = admitted {
            assert!(h.conversation(id).unwrap().abort_requested);
        }
        assert!(!h.conversation(background).unwrap().abort_requested);
        drop(h);

        let scheduler = Scheduler::open(&path, options()).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            scheduler.wait_for_idle(parent, &pi_rs_core::CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        assert!(driver.get(nested_id).unwrap().unwrap().is_terminal());
        assert!(!scheduler.conversation(parent).unwrap().abort_requested);
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!scheduler.conversation(background).unwrap().abort_requested);
        if let Some(id) = admitted {
            assert!(!scheduler.conversation(id).unwrap().abort_requested);
        }
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(!reopened.conversation(parent).unwrap().abort_requested);
        assert!(!reopened.conversation(child).unwrap().abort_requested);
    }

    #[test]
    fn traversal_terminates_on_ownership_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&nested)])
            .unwrap();
        let edge = Edge {
            task: nested.id(),
            task_owner: TaskOwner::Conversation(child),
            child: parent,
            mode: Mode::Foreground,
        };
        assert!(EdgeStore::new(store).prepare_create(&edge).is_err());
        // Simulate a corrupt legacy store to exercise defensive abort traversal.
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        h.request_abort(parent).unwrap();
        assert!(h.conversation(child).unwrap().abort_requested);
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(nested.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn admission_races_abort_and_retry_after_reopen() {
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let (task, owner) = setup(&mut h);
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let b = barrier.clone();
            let o = owner.clone();
            let admission = std::thread::spawn(move || {
                b.wait();
                worker.ensure_task_owned_subagent(task, o, "child", config(), Mode::Foreground)
            });
            barrier.wait();
            let abort = h.request_abort_task(task);
            let result = admission.join().unwrap();
            let store = h.subagent_store().unwrap();
            let marked = TaskStore::new(store)
                .load(task)
                .unwrap()
                .unwrap()
                .task
                .abort_requested();
            assert_eq!(marked, abort.is_ok());
            if let Ok(a) = &result {
                assert!(h.conversation(a.child).unwrap().abort_requested);
            }
            let edges = EdgeStore::new(store).children(task).unwrap();
            assert_eq!(edges.len(), usize::from(result.is_ok()));
            drop(h);
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let retry =
                h.ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground);
            match result {
                Ok(a) => assert_eq!(retry.unwrap(), a),
                Err(_) => assert!(retry.is_err()),
            }
        }
    }
}
