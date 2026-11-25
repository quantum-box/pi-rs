use pi_rs_durable::{
    document::{Address, TaskDocuments},
    task::{Checkpoint, Task, TaskOwner},
    task_scheduler::{Handler, PendingSettlement, Registry, Scheduler, Step, TickReservation},
    task_store::{SignalResult, SignalToken},
};
use serde_json::json;
use uuid::Uuid;

struct Staged(Address);
impl Handler for Staged {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        parent: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        docs.get(&self.0).unwrap();
        docs.put(
            &self.0,
            json!({"value": "inspect"}).as_object().unwrap().clone(),
        )
        .unwrap();
        let token = SignalToken::new("answer", 1).unwrap();
        assert_eq!(
            docs.signal_at(Uuid::from_u128(2), &token, "key", json!(7), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        let child = make_task(3, TaskOwner::Task(parent.id()), "child");
        docs.stage_keyed_child("child", "tool", child.input(), child.clone())
            .unwrap();
        Ok(Step::Succeed(json!("result")))
    }
}
fn make_task(id: u128, owner: TaskOwner, kind: &str) -> Task {
    Task::new(
        Uuid::from_u128(id),
        owner,
        Checkpoint {
            phase: "start".into(),
            data: json!({}),
        },
        3,
    )
    .unwrap()
    .registered(kind.into(), false)
}
#[test]
fn public_pending_inspection_keeps_original_outcome_and_lease() {
    let dir = tempfile::tempdir().unwrap();
    let address = Address {
        scope: "public".into(),
        name: "doc".into(),
        key: "".into(),
    };
    let mut registry = Registry::default();
    registry.register("parent", false, Staged(address.clone()));
    registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
    registry.register("receiver", false, |_: &Task| Ok(Step::Succeed(json!(null))));
    let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
    scheduler
        .submit(&make_task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
        ))
        .unwrap();
    scheduler
        .submit(&make_task(
            2,
            TaskOwner::Conversation(Uuid::from_u128(10)),
            "receiver",
        ))
        .unwrap();
    let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
        panic!("reservation")
    };
    let pending = PendingSettlement::Unprepared(invocation.execute().unwrap());
    assert_eq!(pending.task_id(), Uuid::from_u128(1));
    assert!(pending.attempt_id().is_some());
    assert_eq!(
        pending.observed_task().unwrap().unwrap().id(),
        Uuid::from_u128(1)
    );
    let completed = pending.completed().unwrap();
    assert!(
        matches!(completed.handler_result(), Some(Ok(Step::Succeed(value))) if *value == json!("result"))
    );
    assert!(completed.abort_outcome().is_none());
    let documents = completed.staged_documents().unwrap();
    assert_eq!(
        documents.writes().get(&address).unwrap().as_ref().unwrap()["value"],
        json!("inspect")
    );
    assert!(documents.signal_batch().mutations.iter().any(|mutation| matches!(mutation, durable_store::Mutation::Put { key, .. } if key.starts_with(b"durable/signal/"))));
    let child = documents.keyed_children().next().unwrap();
    assert_eq!(child.key, "child");
    assert_eq!(child.tool, "tool");
    assert_eq!(child.task.id(), Uuid::from_u128(3));
    assert!(!child.checks.is_empty());
    scheduler.try_resume_pending(pending).unwrap();
}
