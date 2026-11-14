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
    drop(watch);
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
