//! Opt-in task-owned subagent admission. This only persists ownership; it does
//! not schedule work or propagate cancellation across conversation edges.
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
            };
            let edge = Edge {
                task,
                task_owner: owner.clone(),
                child,
                mode,
            };
            let ck = child_key(child);
            let ek = ownership_edge::edge_key(child);
            let checks = [
                Check {
                    key: tk.clone(),
                    expected: Expected::Version(snapshot.version),
                },
                Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: ck.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: ek.clone(),
                    expected: Expected::Missing,
                },
            ];
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
                    key: ck,
                    value: serde_json::to_vec(&Harness::empty_conversation(child, config.clone()))?,
                },
                Mutation::Put {
                    key: ek,
                    value: serde_json::to_vec(&edge)?,
                },
            ];
            match store.commit(&checks, &mutations) {
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
    fn foreground_abort_fails_without_partial_marks_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        h.ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground)
            .unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.request_abort(parent).is_err());
        assert!(!h.conversation(parent).unwrap().abort_requested);
        let ts = TaskStore::new(h.subagent_store().unwrap());
        assert!(matches!(
            crate::task_scheduler::request_abort_subtree(&ts, task),
            Err(crate::task_scheduler::Error::ForegroundAbortUnsupported)
        ));
        assert!(!ts.load(task).unwrap().unwrap().task.abort_requested());
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
            let abort = crate::task_scheduler::request_abort_subtree(
                &TaskStore::new(h.subagent_store().unwrap()),
                task,
            );
            let result = admission.join().unwrap();
            let store = h.subagent_store().unwrap();
            let marked = TaskStore::new(store)
                .load(task)
                .unwrap()
                .unwrap()
                .task
                .abort_requested();
            assert_eq!(marked, abort.is_ok());
            assert_eq!(result.is_ok(), !marked);
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
