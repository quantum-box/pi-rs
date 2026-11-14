use durable_store::Store;
use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_store::{Save, SignalResult as R, SignalToken, TaskStore},
};
use serde_json::json;
use uuid::Uuid;

fn task(id: Uuid) -> Task {
    Task::new(
        id,
        TaskOwner::Conversation(Uuid::new_v4()),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
}

#[test]
fn statuses_follow_task_then_key_precedence_and_survive_consumption() {
    let db = Store::memory().unwrap();
    let store = TaskStore::new(&db);
    let id = Uuid::new_v4();
    let token = SignalToken::new("answer", 1).unwrap();
    assert_eq!(
        store.deliver_signal(id, &token, "", json!(1)).unwrap(),
        R::NotFound
    );
    store.save(&[Save::Create(&task(id))]).unwrap();
    assert_eq!(
        store.deliver_signal(id, &token, "", json!(1)).unwrap(),
        R::Invalid
    );
    assert_eq!(
        store.deliver_signal(id, &token, "k", json!(1)).unwrap(),
        R::Delivered
    );
    assert_eq!(
        store.deliver_signal(id, &token, "k", json!(1)).unwrap(),
        R::Duplicate
    );
    assert_eq!(
        store.deliver_signal(id, &token, "k", json!(2)).unwrap(),
        R::Duplicate
    );
    let newer = SignalToken::new("answer", 2).unwrap();
    assert_eq!(
        store.deliver_signal(id, &newer, "k", json!(2)).unwrap(),
        R::Duplicate
    );
    store.reserve(&store.load(id).unwrap().unwrap()).unwrap();
    store
        .update(&store.load(id).unwrap().unwrap(), |t| {
            t.wait(WaitFor::TypedSignal(token.key("k").unwrap()))
        })
        .unwrap();
    store
        .poll_wait(&store.load(id).unwrap().unwrap(), 0)
        .unwrap();
    assert_eq!(
        store.deliver_signal(id, &token, "k", json!(3)).unwrap(),
        R::Duplicate
    );
    store
        .update(&store.load(id).unwrap().unwrap(), |t| t.cancel())
        .unwrap();
    assert_eq!(
        store.deliver_signal(id, &token, "k", json!(1)).unwrap(),
        R::Closed
    );
    assert_eq!(
        store.deliver_signal(id, &token, "", json!(1)).unwrap(),
        R::Closed
    );
    let aborted = Uuid::new_v4();
    let mut marked = serde_json::to_value(task(aborted)).unwrap();
    marked["abort_requested"] = json!(true);
    let marked: Task = serde_json::from_value(marked).unwrap();
    store.save(&[Save::Create(&marked)]).unwrap();
    assert_eq!(
        store
            .deliver_signal(aborted, &token, "k", json!(1))
            .unwrap(),
        R::Closed
    );
}

#[test]
fn concurrent_retries_have_one_delivery() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let db = Store::open(&path).unwrap();
    let id = Uuid::new_v4();
    TaskStore::new(&db)
        .save(&[Save::Create(&task(id))])
        .unwrap();
    let barrier = Arc::new(Barrier::new(12));
    let threads: Vec<_> = (0..12)
        .map(|n| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let db = Store::open(path).unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                barrier.wait();
                TaskStore::new(&db)
                    .deliver_signal(id, &token, "k", json!(n % 2))
                    .unwrap()
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|&&r| r == R::Delivered).count(), 1);
    assert_eq!(results.iter().filter(|&&r| r == R::Duplicate).count(), 11);
}
