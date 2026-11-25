use durable_store::{Expected, Mutation, Store};
use pi_rs_durable::document::{Address, Documents, Transaction};
use serde_json::json;

// Existing scopes are opaque host strings, even if they resemble a future
// task ownership convention. Upgrades must not silently claim such documents.
#[test]
fn task_shaped_host_scope_remains_an_ordinary_document_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("documents.db");
    let address = Address {
        scope: "pi-rs/task/00000000-0000-0000-0000-000000000001".into(),
        name: "host-data".into(),
        key: "".into(),
    };
    let store = Store::open(&path).unwrap();
    let mut transaction = Transaction::new();
    transaction
        .put(
            &address,
            Expected::Missing,
            json!({"version": 1}).as_object().unwrap(),
        )
        .unwrap();
    transaction.commit(&store).unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    let docs = Documents::new(&store);
    let (initial, mut watch) = docs.watch(&address).unwrap();
    assert_eq!(initial.document.unwrap().value["version"], 1);
    let mut key = b"durable/document/".to_vec();
    key.extend(serde_json::to_vec(&[&address.scope, &address.name, &address.key]).unwrap());
    store
        .commit(
            &[],
            &[Mutation::Put {
                key,
                value: br#"{"version":2}"#.to_vec(),
            }],
        )
        .unwrap();
    let changes = watch.poll(10).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].document.as_ref().unwrap().value["version"], 2);
    let version = docs.get(&address).unwrap().unwrap().version;
    let mut transaction = Transaction::new();
    transaction
        .put(
            &address,
            Expected::Version(version),
            json!({"version": 3}).as_object().unwrap(),
        )
        .unwrap();
    transaction.commit(&store).unwrap();
    assert_eq!(docs.get(&address).unwrap().unwrap().value["version"], 3);
}
