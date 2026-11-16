//! Private storage protocol for nested-tool suspension.
//!
//! A record alone does not prove that an executor stopped, own an external job,
//! or authorize dispatch. The bounded nested-host integration fences and joins
//! the previous executor and composes transitions with root attempt and abort
//! checks. Other integrators must establish equivalent ownership guarantees.

use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

const FORMAT: u32 = 1;
const MAX_JOBS: usize = 8;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid or unsupported suspension record")]
    Invalid,
    #[error("suspension state or executor is stale")]
    Stale,
    #[error("suspension was aborted")]
    Aborted,
}

/// References are opaque and must not be interpreted, truncated, or logged.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PendingJob {
    pub operation_id: Uuid,
    pub reference: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Owner {
    Active { executor: Uuid },
    Retired { executor: Uuid },
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Record {
    format: u32,
    conversation: Uuid,
    operation: Uuid,
    suspension: Uuid,
    checkpoint: Value,
    jobs: Vec<PendingJob>,
    abort_requested: bool,
    owner: Owner,
}

/// Snapshot versions fence all transitions and every dependent commit. A
/// retained UUID alone is never a claim; a stale snapshot cannot be reused.
#[derive(Clone)]
pub(crate) struct Snapshot {
    record: Record,
    version: u64,
}

fn key(conversation: Uuid, operation: Uuid) -> Vec<u8> {
    format!("durable/nested-suspension/{conversation}/{operation}").into_bytes()
}

fn validate(record: &Record) -> Result<(), Error> {
    let mut ids = HashSet::new();
    let executor = match record.owner {
        Owner::Active { executor } | Owner::Retired { executor } => executor,
    };
    if record.format != FORMAT
        || record.conversation.is_nil()
        || record.operation.is_nil()
        || record.suspension.is_nil()
        || executor.is_nil()
        || record.jobs.is_empty()
        || record.jobs.len() > MAX_JOBS
        || record.jobs.iter().any(|job| {
            job.operation_id.is_nil()
                || job.operation_id == record.operation
                || !ids.insert(job.operation_id)
        })
    {
        return Err(Error::Invalid);
    }
    Ok(())
}

pub(crate) fn load(
    store: &Store,
    conversation: Uuid,
    operation: Uuid,
) -> Result<Option<Snapshot>, Error> {
    let Some(entry) = store.get(&key(conversation, operation))? else {
        return Ok(None);
    };
    let record: Record = serde_json::from_slice(&entry.value)?;
    validate(&record)?;
    if record.conversation != conversation || record.operation != operation {
        return Err(Error::Invalid);
    }
    Ok(Some(Snapshot {
        record,
        version: entry.version,
    }))
}

/// Prepare creation of the whole checkpoint and pending-job set as one write.
/// A caller must append the live executor and ownership checks to this batch.
pub(crate) fn prepare_create(
    conversation: Uuid,
    operation: Uuid,
    suspension: Uuid,
    executor: Uuid,
    checkpoint: Value,
    jobs: Vec<PendingJob>,
) -> Result<crate::task_store::OwnedBatch, Error> {
    let record = Record {
        format: FORMAT,
        conversation,
        operation,
        suspension,
        checkpoint,
        jobs,
        abort_requested: false,
        owner: Owner::Active { executor },
    };
    validate(&record)?;
    let key = key(conversation, operation);
    Ok(crate::task_store::OwnedBatch {
        checks: vec![Check {
            key: key.clone(),
            expected: Expected::Missing,
        }],
        mutations: vec![Mutation::Put {
            key,
            value: serde_json::to_vec(&record)?,
        }],
    })
}

/// Create an already-retired claim after the child invocation and all of its
/// local descendants have returned. The caller composes root and journal checks.
pub(crate) fn prepare_retired(
    conversation: Uuid,
    operation: Uuid,
    suspension: Uuid,
    executor: Uuid,
    checkpoint: Value,
    jobs: Vec<PendingJob>,
) -> Result<crate::task_store::OwnedBatch, Error> {
    let record = Record {
        format: FORMAT,
        conversation,
        operation,
        suspension,
        checkpoint,
        jobs,
        abort_requested: false,
        owner: Owner::Retired { executor },
    };
    validate(&record)?;
    let key = key(conversation, operation);
    Ok(crate::task_store::OwnedBatch {
        checks: vec![Check {
            key: key.clone(),
            expected: Expected::Missing,
        }],
        mutations: vec![Mutation::Put {
            key,
            value: serde_json::to_vec(&record)?,
        }],
    })
}

impl Snapshot {
    pub(crate) fn retired_check(&self) -> Result<Check, Error> {
        if self.record.abort_requested {
            return Err(Error::Aborted);
        }
        if !self.is_retired() {
            return Err(Error::Stale);
        }
        Ok(self.check())
    }
    pub(crate) fn advance_reference(
        &self,
        operation: Uuid,
        old: &str,
        new: String,
    ) -> Result<crate::task_store::OwnedBatch, Error> {
        self.retired_check()?;
        if self.record.jobs.len() != 1
            || self.record.jobs[0].operation_id != operation
            || self.record.jobs[0].reference != old
        {
            return Err(Error::Invalid);
        }
        let mut next = self.record.clone();
        next.jobs[0].reference = new;
        self.update(next)
    }
    pub(crate) fn is_retired(&self) -> bool {
        matches!(self.record.owner, Owner::Retired { .. })
    }
    pub(crate) fn executor(&self) -> Uuid {
        match self.record.owner {
            Owner::Active { executor } | Owner::Retired { executor } => executor,
        }
    }
    pub(crate) fn jobs(&self) -> &[PendingJob] {
        &self.record.jobs
    }
    pub(crate) fn checkpoint(&self) -> &Value {
        &self.record.checkpoint
    }
    pub(crate) fn suspension_id(&self) -> Uuid {
        self.record.suspension
    }
    pub(crate) fn abort_requested(&self) -> bool {
        self.record.abort_requested
    }
    fn check(&self) -> Check {
        Check {
            key: key(self.record.conversation, self.record.operation),
            expected: Expected::Version(self.version),
        }
    }
    fn update(&self, next: Record) -> Result<crate::task_store::OwnedBatch, Error> {
        validate(&next)?;
        Ok(crate::task_store::OwnedBatch {
            checks: vec![self.check()],
            mutations: vec![Mutation::Put {
                key: key(next.conversation, next.operation),
                value: serde_json::to_vec(&next)?,
            }],
        })
    }
    /// Prepare retirement only after the caller has stopped and joined this
    /// executor. This record is a fence, not proof that the executor stopped.
    pub(crate) fn retire(&self, executor: Uuid) -> Result<crate::task_store::OwnedBatch, Error> {
        if self.record.owner != (Owner::Active { executor }) {
            return Err(Error::Stale);
        }
        let mut next = self.record.clone();
        next.owner = Owner::Retired { executor };
        self.update(next)
    }
    /// Claim a retired, non-aborted suspension using a distinct executor ID.
    pub(crate) fn claim(&self, executor: Uuid) -> Result<crate::task_store::OwnedBatch, Error> {
        if self.record.abort_requested {
            return Err(Error::Aborted);
        }
        let Owner::Retired { executor: previous } = self.record.owner else {
            return Err(Error::Stale);
        };
        if executor.is_nil() || executor == previous {
            return Err(Error::Invalid);
        }
        let mut next = self.record.clone();
        next.owner = Owner::Active { executor };
        self.update(next)
    }
    /// Mark abort without discarding the job set. Abort invalidates all earlier
    /// claims, but the current owner may still be retired during cleanup.
    pub(crate) fn abort(&self) -> Result<crate::task_store::OwnedBatch, Error> {
        if self.record.abort_requested {
            return Ok(crate::task_store::OwnedBatch {
                checks: vec![self.check()],
                mutations: vec![],
            });
        }
        let mut next = self.record.clone();
        next.abort_requested = true;
        self.update(next)
    }
    /// Include this check in the *same transaction* as any claim-dependent
    /// write. It is only a storage fence, not an executor lease.
    pub(crate) fn claim_check(&self, executor: Uuid) -> Result<Check, Error> {
        if self.record.abort_requested {
            return Err(Error::Aborted);
        }
        if self.record.owner != (Owner::Active { executor }) {
            return Err(Error::Stale);
        }
        Ok(self.check())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: Uuid, reference: &str) -> PendingJob {
        PendingJob {
            operation_id: id,
            reference: reference.into(),
        }
    }
    fn commit(
        store: &Store,
        batch: crate::task_store::OwnedBatch,
    ) -> Result<(), durable_store::Error> {
        store.commit(&batch.checks, &batch.mutations)
    }
    fn setup(store: &Store) -> (Uuid, Uuid, Uuid, Vec<PendingJob>) {
        let (conversation, operation, executor) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let jobs = vec![
            job(Uuid::new_v4(), "opaque/\u{0}one"),
            job(Uuid::new_v4(), "opaque/two"),
        ];
        commit(
            store,
            prepare_create(
                conversation,
                operation,
                Uuid::new_v4(),
                executor,
                serde_json::json!({"step": 1}),
                jobs.clone(),
            )
            .unwrap(),
        )
        .unwrap();
        (conversation, operation, executor, jobs)
    }

    #[test]
    fn retired_claims_and_abort_fence_stale_writes() {
        let store = Store::memory().unwrap();
        let (conversation, operation, old, jobs) = setup(&store);
        let active = load(&store, conversation, operation).unwrap().unwrap();
        assert!(active.jobs() == jobs);
        assert_eq!(active.checkpoint(), &serde_json::json!({"step": 1}));
        assert!(!active.suspension_id().is_nil());
        let old_check = active.claim_check(old).unwrap();
        assert!(matches!(active.claim(Uuid::new_v4()), Err(Error::Stale)));
        commit(&store, active.retire(old).unwrap()).unwrap();
        assert!(store.commit(&[old_check], &[]).is_err());
        let retired = load(&store, conversation, operation).unwrap().unwrap();
        let one = Uuid::new_v4();
        let two = Uuid::new_v4();
        let first = retired.claim(one).unwrap();
        let stale = retired.claim(two).unwrap();
        commit(&store, first).unwrap();
        assert!(commit(&store, stale).is_err());
        let claimed = load(&store, conversation, operation).unwrap().unwrap();
        let claim_check = claimed.claim_check(one).unwrap();
        let stale_retire = claimed.retire(one).unwrap();
        commit(&store, claimed.abort().unwrap()).unwrap();
        let dependent = b"dependent-effect".to_vec();
        assert!(
            store
                .commit(
                    &[claim_check],
                    &[Mutation::Put {
                        key: dependent.clone(),
                        value: vec![1]
                    }]
                )
                .is_err()
        );
        assert!(store.get(&dependent).unwrap().is_none());
        assert!(commit(&store, stale_retire).is_err());
        let aborted = load(&store, conversation, operation).unwrap().unwrap();
        assert!(aborted.jobs() == jobs);
        assert!(aborted.abort_requested());
        assert!(matches!(aborted.claim_check(one), Err(Error::Aborted)));
        commit(&store, aborted.retire(one).unwrap()).unwrap();
        let retired = load(&store, conversation, operation).unwrap().unwrap();
        assert!(matches!(retired.claim(two), Err(Error::Aborted)));
    }

    #[test]
    fn creation_is_atomic_and_rejects_bad_sets_and_formats() {
        let store = Store::memory().unwrap();
        let (c, o, s, e, j) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        assert!(prepare_create(c, o, s, e, Value::Null, vec![]).is_err());
        assert!(prepare_create(c, o, s, e, Value::Null, vec![job(o, "parent")]).is_err());
        assert!(prepare_create(c, o, s, e, Value::Null, vec![job(j, "a"), job(j, "b")]).is_err());
        assert!(
            prepare_create(
                c,
                o,
                s,
                e,
                Value::Null,
                (0..9).map(|_| job(Uuid::new_v4(), "x")).collect()
            )
            .is_err()
        );
        let mut batch = prepare_create(c, o, s, e, Value::Null, vec![job(j, "a")]).unwrap();
        batch.checks.push(Check {
            key: b"missing-fence".to_vec(),
            expected: Expected::Version(1),
        });
        assert!(commit(&store, batch).is_err());
        assert!(load(&store, c, o).unwrap().is_none());
        let mut record = Record {
            format: FORMAT + 1,
            conversation: c,
            operation: o,
            suspension: s,
            checkpoint: Value::Null,
            jobs: vec![job(j, "a")],
            abort_requested: false,
            owner: Owner::Active { executor: e },
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key(c, o),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(matches!(load(&store, c, o), Err(Error::Invalid)));
        record.format = FORMAT;
        record.operation = Uuid::new_v4();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key(c, o),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(matches!(load(&store, c, o), Err(Error::Invalid)));
    }

    #[test]
    fn abort_and_claim_race_has_one_winner() {
        for claim_first in [true, false] {
            let store = Store::memory().unwrap();
            let (c, o, executor, _) = setup(&store);
            let active = load(&store, c, o).unwrap().unwrap();
            commit(&store, active.retire(executor).unwrap()).unwrap();
            let retired = load(&store, c, o).unwrap().unwrap();
            let claim = retired.claim(Uuid::new_v4()).unwrap();
            let abort = retired.abort().unwrap();
            if claim_first {
                commit(&store, claim).unwrap();
                assert!(commit(&store, abort).is_err());
                let fresh = load(&store, c, o).unwrap().unwrap();
                commit(&store, fresh.abort().unwrap()).unwrap();
            } else {
                commit(&store, abort).unwrap();
                assert!(commit(&store, claim).is_err());
            }
            assert!(load(&store, c, o).unwrap().unwrap().record.abort_requested);
        }
    }

    #[test]
    fn killed_claim_worker() {
        let Some(root) = std::env::var_os("PI_SUSPENSION_CLAIM_WORKER") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let owner = durable_store::owner::OwnedStore::open(root.join("db")).unwrap();
        let (c, o, executor, _) = setup(owner.store());
        let active = load(owner.store(), c, o).unwrap().unwrap();
        commit(owner.store(), active.retire(executor).unwrap()).unwrap();
        let retired = load(owner.store(), c, o).unwrap().unwrap();
        let claimed = Uuid::new_v4();
        commit(owner.store(), retired.claim(claimed).unwrap()).unwrap();
        std::fs::write(root.join("ready.tmp"), format!("{c}\n{o}\n{claimed}\n")).unwrap();
        std::fs::rename(root.join("ready.tmp"), root.join("ready")).unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn killed_claim_stays_fenced_across_reopen_until_retired() {
        struct Kill(std::process::Child);
        impl Drop for Kill {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut worker = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "nested_suspension::tests::killed_claim_worker"])
                .env("PI_SUSPENSION_CLAIM_WORKER", root)
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !root.join("ready").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "claim worker did not reach barrier"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        worker.0.kill().unwrap();
        worker.0.wait().unwrap();
        let contents = std::fs::read_to_string(root.join("ready")).unwrap();
        let mut values = contents.lines().map(|line| line.parse::<Uuid>().unwrap());
        let (c, o, old) = (
            values.next().unwrap(),
            values.next().unwrap(),
            values.next().unwrap(),
        );
        let owner = durable_store::owner::OwnedStore::open(root.join("db")).unwrap();
        let claimed = load(owner.store(), c, o).unwrap().unwrap();
        assert!(matches!(claimed.claim(Uuid::new_v4()), Err(Error::Stale)));
        let stale = claimed.claim_check(old).unwrap();
        // Process death was observed and joined above; storage alone cannot establish this.
        commit(owner.store(), claimed.retire(old).unwrap()).unwrap();
        assert!(owner.store().commit(&[stale], &[]).is_err());
        let retired = load(owner.store(), c, o).unwrap().unwrap();
        assert_eq!(retired.record.jobs.len(), 2);
        assert_eq!(retired.record.jobs[0].reference, "opaque/\u{0}one");
        let next = Uuid::new_v4();
        commit(owner.store(), retired.claim(next).unwrap()).unwrap();
        let current = load(owner.store(), c, o).unwrap().unwrap();
        assert!(current.claim_check(next).is_ok());
        assert!(matches!(current.claim_check(old), Err(Error::Stale)));
    }
}
