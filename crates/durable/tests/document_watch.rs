use durable_store::{Expected, Store};
use pi_rs_durable::document::{Address, Documents, Transaction};
use serde_json::json;
use std::sync::{Arc, Barrier};

fn address() -> Address {
    Address {
        scope: "session".into(),
        name: "watch".into(),
        key: "".into(),
    }
}
fn put(store: &Store, n: i32) {
    let address = address();
    let expected = Documents::new(store)
        .get(&address)
        .unwrap()
        .map_or(Expected::Missing, |d| Expected::Version(d.version));
    let mut tx = Transaction::new();
    tx.put(&address, expected, json!({"n":n}).as_object().unwrap())
        .unwrap();
    tx.commit(store).unwrap();
}

#[test]
fn ordered_pages_delete_and_resume_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).unwrap();
    put(&store, 0);
    let docs = Documents::new(&store);
    let (initial, mut watch) = docs.watch(&address()).unwrap();
    assert_eq!(initial.document.unwrap().value["n"], 0);
    put(&store, 1);
    let version = docs.get(&address()).unwrap().unwrap().version;
    let mut tx = Transaction::new();
    tx.delete(&address(), version).unwrap();
    tx.commit(&store).unwrap();
    put(&store, 2);
    assert!(watch.poll(0).is_err());
    assert_eq!(watch.cursor(), initial.revision);
    let first = watch.poll(1).unwrap();
    assert_eq!(first[0].document.as_ref().unwrap().value["n"], 1);
    let cursor = watch.cursor();
    drop(store);
    let store = Store::open(&path).unwrap();
    let docs = Documents::new(&store);
    let mut watch = docs.resume_watch(&address(), cursor).unwrap();
    let remaining = watch.poll(256).unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining[0].document.is_none());
    assert_eq!(remaining[1].document.as_ref().unwrap().value["n"], 2);
    assert!(
        first[0].revision < remaining[0].revision && remaining[0].revision < remaining[1].revision
    );
    assert!(watch.poll(256).unwrap().is_empty());
    assert!(docs.resume_watch(&address(), u64::MAX).is_err());
}

#[test]
fn attach_racing_commits_never_misses_a_revision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).unwrap();
    put(&store, 0);
    let writer = Store::open(&path).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let other = barrier.clone();
    let thread = std::thread::spawn(move || {
        other.wait();
        for n in 1..=100 {
            put(&writer, n);
        }
    });
    barrier.wait();
    let (initial, mut watch) = Documents::new(&store).watch(&address()).unwrap();
    let mut numbers = vec![initial.document.unwrap().value["n"].as_i64().unwrap()];
    loop {
        let page = watch.poll(7).unwrap();
        if page.is_empty() {
            if thread.is_finished() {
                break;
            }
            std::thread::yield_now();
            continue;
        }
        numbers.extend(
            page.into_iter()
                .map(|v| v.document.unwrap().value["n"].as_i64().unwrap()),
        );
    }
    thread.join().unwrap();
    loop {
        let page = watch.poll(7).unwrap();
        if page.is_empty() {
            break;
        }
        numbers.extend(
            page.into_iter()
                .map(|v| v.document.unwrap().value["n"].as_i64().unwrap()),
        );
    }
    assert_eq!(numbers, (numbers[0]..=100).collect::<Vec<_>>());
}

#[test]
fn complete_commit_pages_survive_deletion_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.db");
    let store = Store::open(&path).unwrap();
    let a = address();
    let mut b = a.clone();
    b.key = "second".into();
    let docs = Documents::new(&store);
    let (initial, mut watch) = docs.watch_commits().unwrap();
    assert!(initial.changes.is_empty());
    let mut tx = Transaction::new();
    for item in [&a, &b] {
        tx.put(
            item,
            Expected::Missing,
            json!({"n": 1}).as_object().unwrap(),
        )
        .unwrap();
    }
    tx.commit(&store).unwrap();
    let mut tx = Transaction::new();
    tx.delete(&a, docs.get(&a).unwrap().unwrap().version)
        .unwrap();
    tx.put(
        &b,
        Expected::Version(docs.get(&b).unwrap().unwrap().version),
        json!({"n": 2}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&store).unwrap();
    assert!(watch.poll(0).is_err());
    assert_eq!(watch.cursor(), initial.revision);
    let page = watch.poll(1).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].changes.len(), 2);
    assert!(
        page[0]
            .changes
            .iter()
            .all(|(_, value)| value.as_ref().unwrap().value["n"] == 1)
    );
    let cursor = watch.cursor();
    drop(store);
    let store = Store::open(&path).unwrap();
    let docs = Documents::new(&store);
    let mut watch = docs.resume_commit_watch(cursor).unwrap();
    let page = watch.poll(1).unwrap();
    assert_eq!(page.len(), 1);
    assert!(page[0].revision > cursor);
    assert_eq!(page[0].changes.len(), 2);
    assert!(
        page[0]
            .changes
            .iter()
            .any(|(key, value)| key == &a && value.is_none())
    );
    assert!(
        page[0]
            .changes
            .iter()
            .any(|(key, value)| key == &b && value.as_ref().unwrap().value["n"] == 2)
    );
    assert!(watch.poll(1).unwrap().is_empty());
    assert!(docs.resume_commit_watch(u64::MAX).is_err());
}

#[test]
fn commit_watch_attachment_is_consistent_during_batched_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    let store = Store::open(&path).unwrap();
    let writer = Store::open(&path).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let other = barrier.clone();
    let thread = std::thread::spawn(move || {
        let a = address();
        let mut b = a.clone();
        b.key = "second".into();
        other.wait();
        for n in 0..50 {
            let docs = Documents::new(&writer);
            let mut tx = Transaction::new();
            for item in [&a, &b] {
                let expected = docs
                    .get(item)
                    .unwrap()
                    .map_or(Expected::Missing, |d| Expected::Version(d.version));
                tx.put(item, expected, json!({"n": n}).as_object().unwrap())
                    .unwrap();
            }
            tx.commit(&writer).unwrap();
        }
    });
    barrier.wait();
    let (initial, mut watch) = Documents::new(&store).watch_commits().unwrap();
    let initial_n = if initial.changes.is_empty() {
        -1
    } else {
        assert_eq!(initial.changes.len(), 2);
        let n = initial.changes[0].1.as_ref().unwrap().value["n"]
            .as_i64()
            .unwrap();
        assert_eq!(initial.changes[1].1.as_ref().unwrap().value["n"], n);
        n
    };
    thread.join().unwrap();
    let frames = watch.poll(256).unwrap();
    let mut numbers = Vec::new();
    for frame in frames {
        assert_eq!(frame.changes.len(), 2);
        let n = frame.changes[0].1.as_ref().unwrap().value["n"]
            .as_i64()
            .unwrap();
        assert_eq!(frame.changes[1].1.as_ref().unwrap().value["n"], n);
        numbers.push(n);
    }
    assert_eq!(numbers, ((initial_n + 1)..50).collect::<Vec<_>>());
}

#[test]
fn malformed_frame_does_not_advance_commit_cursor() {
    use durable_store::Mutation;
    let store = Store::memory().unwrap();
    let docs = Documents::new(&store);
    let (initial, mut watch) = docs.watch_commits().unwrap();
    store
        .commit(
            &[],
            &[Mutation::Put {
                key: b"durable/document/not-json".to_vec(),
                value: b"{}".to_vec(),
            }],
        )
        .unwrap();
    assert!(watch.poll(1).is_err());
    assert_eq!(watch.cursor(), initial.revision);
    assert!(watch.poll(1).is_err());
}

#[test]
fn operation_frames_preserve_commit_order_and_resume_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.db");
    let store = Store::open(&path).unwrap();
    put(&store, 0);
    let docs = Documents::new(&store);
    let (initial, mut first) = docs.watch(&address()).unwrap();
    let mut second = docs.resume_watch(&address(), initial.revision).unwrap();
    let writer = Store::open(&path).unwrap();
    let thread = std::thread::spawn(move || {
        for n in 1..=25 {
            put(&writer, n);
        }
    });
    let mut page = Vec::new();
    while page.len() < 7 {
        let next = first.poll_operations(7 - page.len()).unwrap();
        assert!(
            !next.is_empty() || !thread.is_finished(),
            "writer stopped before seven commits"
        );
        page.extend(next);
        std::thread::yield_now();
    }
    thread.join().unwrap();
    assert_eq!(page.len(), 7);
    for (index, frame) in page.iter().enumerate() {
        assert_eq!(frame.ops, vec![json!(["r", {"n": index as i32 + 1}])]);
        assert!(!frame.retired);
    }
    let saved = first.cursor();
    let all = second.poll_operations(256).unwrap();
    assert_eq!(all.len(), 25);
    assert_eq!(&all[..7], page.as_slice());
    drop(store);
    let store = Store::open(&path).unwrap();
    let docs = Documents::new(&store);
    let mut resumed = docs.resume_watch(&address(), saved).unwrap();
    let rest = resumed.poll_operations(256).unwrap();
    assert_eq!(rest, all[7..]);
    let version = docs.get(&address()).unwrap().unwrap().version;
    let mut tx = Transaction::new();
    tx.delete(&address(), version).unwrap();
    tx.commit(&store).unwrap();
    put(&store, 26);
    let changes = resumed.poll_operations(256).unwrap();
    assert_eq!(changes.len(), 2);
    assert!(changes[0].retired);
    assert!(changes[0].document.is_none());
    assert!(changes[0].ops.is_empty());
    assert_eq!(changes[1].ops, vec![json!(["r", {"n": 26}])]);
    assert!(!changes[1].retired);
    let cursor = resumed.cursor();
    assert!(resumed.poll_operations(0).is_err());
    assert_eq!(resumed.cursor(), cursor);
}

fn apply_chord(mut value: serde_json::Value, ops: &[serde_json::Value]) -> serde_json::Value {
    for op in ops {
        let op = op.as_array().unwrap();
        match op[0].as_str().unwrap() {
            "r" => value = op[1].clone(),
            "s" => {
                let key = op[1][0].as_str().unwrap();
                value
                    .as_object_mut()
                    .unwrap()
                    .insert(key.into(), op[2].clone());
            }
            "d" => {
                let key = op[1][0].as_str().unwrap();
                value.as_object_mut().unwrap().remove(key);
            }
            "p" => {
                let key = op[1][0].as_str().unwrap();
                let array = value[key].as_array_mut().unwrap();
                let start = op[2].as_u64().unwrap() as usize;
                let count = op[3].as_u64().unwrap() as usize;
                array.splice(start..start + count, op[4].as_array().unwrap().clone());
            }
            other => panic!("unsupported operation {other}"),
        }
    }
    value
}

#[test]
fn incremental_document_operations_apply_and_resume_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("incremental.db");
    let store = Store::open(&path).unwrap();
    let docs = Documents::new(&store);
    let (initial, mut watch) = docs.watch(&address()).unwrap();
    assert!(initial.document.is_none());
    let mut tx = Transaction::new();
    tx.put(
        &address(),
        Expected::Missing,
        json!({"stable":"x".repeat(2000), "items":[1], "remove":1})
            .as_object()
            .unwrap(),
    )
    .unwrap();
    tx.commit(&store).unwrap();
    let first = watch.poll_incremental_operations(1).unwrap();
    assert_eq!(first[0].ops[0][0], "r");
    let mut value = apply_chord(serde_json::Value::Null, &first[0].ops);
    let version = first[0].document.as_ref().unwrap().version;
    let saved = watch.cursor();
    let mut tx = Transaction::new();
    tx.put(
        &address(),
        Expected::Version(version),
        json!({"stable":"x".repeat(2000), "items":[1,2], "added":true})
            .as_object()
            .unwrap(),
    )
    .unwrap();
    tx.commit(&store).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let docs = Documents::new(&store);
    let mut watch = docs.resume_watch(&address(), saved).unwrap();
    let next = watch.poll_incremental_operations(1).unwrap();
    assert!(next[0].ops.iter().any(|op| op[0] == "p"));
    assert!(next[0].ops.iter().any(|op| op[0] == "d"));
    assert!(next[0].ops.iter().any(|op| op[0] == "s"));
    value = apply_chord(value, &next[0].ops);
    assert_eq!(value, json!(next[0].document.as_ref().unwrap().value));
    let version = next[0].document.as_ref().unwrap().version;
    let mut tx = Transaction::new();
    tx.delete(&address(), version).unwrap();
    tx.commit(&store).unwrap();
    let deleted = watch.poll_incremental_operations(1).unwrap();
    assert!(deleted[0].retired && deleted[0].ops.is_empty());
    put(&store, 7);
    let recreated = watch.poll_incremental_operations(1).unwrap();
    assert_eq!(recreated[0].ops[0][0], "r");
    assert_eq!(apply_chord(value, &recreated[0].ops), json!({"n":7}));
}
