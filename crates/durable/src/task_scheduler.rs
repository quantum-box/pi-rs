//! Explicitly driven scheduler for generic tasks, separate from the conversation runtime.
//!
//! The scheduler holds the store's exclusive cooperative owner lock. `tick` runs
//! at most one invocation synchronously; callers periodically call it again,
//! including after signals and deadlines. Dropping between ticks is a clean stop.
//! An interrupted invocation is replayed only when both its stored and current
//! registrations permit replay. External effects cannot be made atomic with this
//! store: handlers must be pure or use destination-side idempotency/reconciliation.
//! A storage error or panic during an invocation requires dropping this scheduler
//! and reopening it; never assume an unsuccessful commit did not happen.
//!
//! Compatibility: this deliberately provides only all-settled direct-child waits,
//! one signal or one deadline per wait, and caller-driven serial polling. It does
//! not implement upstream any/fail-fast joins, combined signal/deadline waits,
//! task cancellation propagation, background children, abort handlers, or the
//! conversation scheduler integration. Live children block terminal settlement
//! and recovery of an unsafe/exhausted parent; an operator must reconcile them.
//! Unlike upstream close, an invocation cannot be interrupted by clean shutdown:
//! the caller waits for synchronous `tick` to return before dropping the owner.

use crate::task::{Checkpoint, Task, TaskOwner, TaskState, TransitionError, WaitFor};
use crate::task_store::{self, Save, Signal, TaskStore, WakeResult};
use durable_store::owner::{OwnedStore, OwnerError};
use serde_json::Value;
use std::{collections::HashMap, path::Path, sync::Arc};
use uuid::Uuid;

/// A handler computes one transition. For effects, use a stable idempotency key
/// derived from task ID and checkpoint, and reconcile uncertain results externally.
pub trait Handler: Send + Sync {
    fn run(&self, task: &Task) -> Result<Step, String>;
}
impl<F> Handler for F
where
    F: Fn(&Task) -> Result<Step, String> + Send + Sync,
{
    fn run(&self, task: &Task) -> Result<Step, String> {
        self(task)
    }
}

pub enum Step {
    Continue(Checkpoint),
    Wait {
        checkpoint: Checkpoint,
        condition: WaitFor,
        children: Vec<Task>,
    },
    Succeed(Value),
    Fail(String),
}

struct Registration {
    replay_safe: bool,
    handler: Arc<dyn Handler>,
}
#[derive(Default)]
pub struct Registry {
    entries: HashMap<String, Registration>,
}
impl Registry {
    pub fn register(
        &mut self,
        kind: impl Into<String>,
        replay_safe: bool,
        handler: impl Handler + 'static,
    ) -> bool {
        use std::collections::hash_map::Entry;
        match self.entries.entry(kind.into()) {
            Entry::Vacant(v) => {
                v.insert(Registration {
                    replay_safe,
                    handler: Arc::new(handler),
                });
                true
            }
            Entry::Occupied(_) => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Owner(#[from] OwnerError),
    #[error(transparent)]
    Store(#[from] task_store::Error),
    #[error("task transition: {0:?}")]
    Transition(TransitionError),
    #[error("unknown or mismatched task registration")]
    Registration,
    #[error("invalid ownership or child creation")]
    Ownership,
    #[error("task has live owned children")]
    LiveChildren,
    #[error("handler failed with an uncertain outcome: {0}")]
    Handler(String),
    #[error("scheduler must be reopened after an invocation or commit failure")]
    Poisoned,
}

pub struct Scheduler {
    store: OwnedStore,
    registry: Registry,
    poisoned: bool,
}
impl Scheduler {
    /// Opens exclusively and reconciles interrupted invocations. Parked tasks are
    /// untouched. Missing registrations leave their running records untouched.
    pub fn open(path: impl AsRef<Path>, registry: Registry) -> Result<Self, Error> {
        let mut scheduler = Self {
            store: OwnedStore::open(path)?,
            registry,
            poisoned: false,
        };
        scheduler.reconcile()?;
        Ok(scheduler)
    }
    fn ts(&self) -> TaskStore<'_> {
        TaskStore::new(self.store.store())
    }
    fn check(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn registered(&self, task: &Task) -> Result<&Registration, Error> {
        let entry = self
            .registry
            .entries
            .get(task.kind().ok_or(Error::Registration)?)
            .ok_or(Error::Registration)?;
        Ok(entry)
    }
    fn reconcile(&mut self) -> Result<(), Error> {
        for item in self.ts().list()? {
            if item.task.state() != &TaskState::Running {
                continue;
            }
            let reg = self.registered(&item.task)?;
            if item.task.replay_safe() && reg.replay_safe {
                let exhausted = item.task.restarts().saturating_add(1) >= item.task.restart_limit();
                if exhausted {
                    self.ensure_no_live_children(item.task.id())?;
                }
                self.ts().update(&item, |task| {
                    task.recover()?;
                    if exhausted {
                        task.exhaust_restarts()?;
                    }
                    Ok(())
                })?;
            } else {
                self.ensure_no_live_children(item.task.id())?;
                self.ts().update(&item, |t| {
                    t.fail("interrupted: execution outcome requires reconciliation".into())
                })?;
            }
        }
        Ok(())
    }
    /// Create a root task. Child creation is performed atomically by `Step::Wait`.
    pub fn submit(&self, task: &Task) -> Result<(), Error> {
        self.check()?;
        let reg = self.registered(task)?;
        if reg.replay_safe != task.replay_safe()
            || !matches!(task.owner(), TaskOwner::Conversation(_))
            || task.state() != &TaskState::Ready
        {
            return Err(Error::Ownership);
        }
        self.ts().save(&[Save::Create(task)])?;
        Ok(())
    }
    pub fn signal(&self, signal: &Signal) -> Result<(), Error> {
        self.check()?;
        self.ts().send_signal(signal)?;
        Ok(())
    }
    pub fn signal_receipt(&self, task_id: Uuid, signal_id: Uuid) -> Result<Option<Signal>, Error> {
        self.check()?;
        Ok(self.ts().signal(task_id, signal_id)?)
    }
    pub fn get(&self, id: Uuid) -> Result<Option<Task>, Error> {
        self.check()?;
        Ok(self.ts().load(id)?.map(|s| s.task))
    }
    pub fn list(&self) -> Result<Vec<Task>, Error> {
        self.check()?;
        Ok(self.ts().list()?.into_iter().map(|s| s.task).collect())
    }

    /// Reevaluate waits, then run one ready task. Returns whether progress occurred.
    /// Errors after reservation poison this instance, including handler errors.
    pub fn tick(&mut self, now_millis: i64) -> Result<bool, Error> {
        self.check()?;
        let mut progress = false;
        for snapshot in self.ts().list()? {
            if matches!(snapshot.task.state(), TaskState::Waiting(_))
                && matches!(
                    self.ts().poll_wait(&snapshot, now_millis)?,
                    WakeResult::Woken { .. }
                )
            {
                progress = true;
            }
        }
        let Some(snapshot) = self
            .ts()
            .list()?
            .into_iter()
            .find(|s| s.task.state() == &TaskState::Ready)
        else {
            return Ok(progress);
        };
        let handler = self.registered(&snapshot.task)?.handler.clone();
        self.poisoned = true;
        self.ts().reserve(&snapshot)?;
        let running = self
            .ts()
            .load(snapshot.task.id())?
            .ok_or(Error::Ownership)?;
        let step = handler.run(&running.task).map_err(Error::Handler)?;
        let mut next = running.clone();
        let mut children = Vec::new();
        match step {
            Step::Continue(checkpoint) => {
                next.task.advance(checkpoint).map_err(Error::Transition)?;
                next.task.yield_ready().map_err(Error::Transition)?;
            }
            Step::Wait {
                checkpoint,
                condition,
                children: proposed,
            } => {
                if next.task.checkpoint() != &checkpoint {
                    next.task.advance(checkpoint).map_err(Error::Transition)?;
                }
                let mut ids = std::collections::HashSet::new();
                for child in proposed {
                    if child.owner() != &TaskOwner::Task(next.task.id())
                        || child.state() != &TaskState::Ready
                        || !ids.insert(child.id())
                    {
                        return Err(Error::Ownership);
                    }
                    let reg = self.registered(&child)?;
                    if reg.replay_safe != child.replay_safe() {
                        return Err(Error::Registration);
                    }
                    children.push(child);
                }
                if let WaitFor::Tasks(ref waiting) = condition {
                    let mut seen = std::collections::HashSet::new();
                    for id in waiting {
                        if !seen.insert(*id) {
                            return Err(Error::Ownership);
                        }
                        if !children.iter().any(|c| c.id() == *id) {
                            let child = self.ts().load(*id)?.ok_or(Error::Ownership)?;
                            if child.task.owner() != &TaskOwner::Task(next.task.id()) {
                                return Err(Error::Ownership);
                            }
                        }
                    }
                }
                next.task.wait(condition).map_err(Error::Transition)?;
            }
            Step::Succeed(value) => {
                self.ensure_no_live_children(next.task.id())?;
                next.task.succeed(value).map_err(Error::Transition)?;
            }
            Step::Fail(reason) => {
                self.ensure_no_live_children(next.task.id())?;
                next.task.fail(reason).map_err(Error::Transition)?;
            }
        }
        let mut batch = vec![Save::Update(&next)];
        batch.extend(children.iter().map(Save::Create));
        self.ts().save(&batch)?;
        self.poisoned = false;
        Ok(true)
    }
    fn ensure_no_live_children(&self, id: Uuid) -> Result<(), Error> {
        if self
            .ts()
            .list()?
            .iter()
            .any(|s| s.task.owner() == &TaskOwner::Task(id) && !s.task.is_terminal())
        {
            Err(Error::LiveChildren)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn cp(phase: &str) -> Checkpoint {
        Checkpoint {
            phase: phase.into(),
            data: json!(null),
        }
    }
    fn task(id: u128, owner: TaskOwner, kind: &str, safe: bool, limit: u32) -> Task {
        Task::new(Uuid::from_u128(id), owner, cp("start"), limit)
            .unwrap()
            .registered(kind.into(), safe)
    }
    #[test]
    fn signal_timer_children_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let parent_id = Uuid::from_u128(1);
        let child_id = Uuid::from_u128(2);
        fn registry() -> Registry {
            let mut r = Registry::default();
            r.register("parent", true, |t: &Task| {
                Ok(match t.checkpoint().phase.as_str() {
                    "start" => Step::Wait {
                        checkpoint: cp("child"),
                        condition: WaitFor::Tasks(vec![Uuid::from_u128(2)]),
                        children: vec![task(2, TaskOwner::Task(t.id()), "child", true, 2)],
                    },
                    "child" => Step::Wait {
                        checkpoint: cp("signal"),
                        condition: WaitFor::Signal("ok".into()),
                        children: vec![],
                    },
                    "signal" => {
                        assert!(t.delivered_signal().is_some());
                        Step::Wait {
                            checkpoint: cp("timer"),
                            condition: WaitFor::DeadlineMillis(100),
                            children: vec![],
                        }
                    }
                    _ => Step::Succeed(json!(42)),
                })
            });
            r.register("child", true, |_t: &Task| Ok(Step::Succeed(json!("done"))));
            r
        }
        let mut s = Scheduler::open(&path, registry()).unwrap();
        s.submit(&task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            2,
        ))
        .unwrap();
        assert!(s.tick(0).unwrap());
        assert_eq!(
            s.get(child_id).unwrap().unwrap().owner(),
            &TaskOwner::Task(parent_id)
        );
        drop(s);
        let mut s = Scheduler::open(&path, registry()).unwrap();
        assert!(s.tick(0).unwrap()); // child
        assert!(s.tick(0).unwrap()); // wake parent and wait on signal
        assert!(!s.tick(0).unwrap());
        s.signal(&Signal::new(
            Uuid::from_u128(7),
            parent_id,
            "ok".into(),
            json!(1),
        ))
        .unwrap();
        assert!(s.tick(0).unwrap());
        drop(s);
        let mut s = Scheduler::open(&path, registry()).unwrap();
        assert!(!s.tick(99).unwrap());
        assert!(s.tick(100).unwrap());
        assert_eq!(
            s.get(parent_id).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!(42))
        );
    }
    #[test]
    fn interrupted_unsafe_is_not_replayed_and_safe_is_limited() {
        let dir = tempfile::tempdir().unwrap();
        for safe in [false, true] {
            let path = dir.path().join(format!("{safe}.db"));
            let mut registry = Registry::default();
            registry.register("x", safe, |_t: &Task| Err("uncertain".into()));
            let mut s = Scheduler::open(&path, registry).unwrap();
            s.submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "x",
                safe,
                1,
            ))
            .unwrap();
            assert!(matches!(s.tick(0), Err(Error::Handler(_))));
            assert!(matches!(s.tick(0), Err(Error::Poisoned)));
            drop(s);
            let mut registry = Registry::default();
            registry.register("x", safe, |_t: &Task| {
                Ok(Step::Succeed(json!("should not execute")))
            });
            let s = Scheduler::open(&path, registry).unwrap();
            assert!(matches!(
                s.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                TaskState::Failed(_)
            ));
        }
    }
    #[test]
    fn owner_lock_and_atomic_child_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut r = Registry::default();
        r.register("x", true, |t: &Task| {
            let child = task(2, TaskOwner::Task(t.id()), "x", true, 2);
            Ok(Step::Wait {
                checkpoint: cp("next"),
                condition: WaitFor::Tasks(vec![child.id()]),
                children: vec![child.clone(), child],
            })
        });
        let mut s = Scheduler::open(&path, r).unwrap();
        assert!(matches!(
            Scheduler::open(&path, Registry::default()),
            Err(Error::Owner(_))
        ));
        s.submit(&task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "x",
            true,
            2,
        ))
        .unwrap();
        assert!(matches!(s.tick(0), Err(Error::Ownership)));
        assert!(s.ts().load(Uuid::from_u128(2)).unwrap().is_none());
        assert_eq!(
            s.ts()
                .load(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .task
                .checkpoint(),
            &cp("start")
        );
    }
}
