#![cfg(unix)]

use durable_store::{
    owner::{OwnedStore, OwnerError},
    Mutation, Store,
};
use std::{
    fs,
    io::Read,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
fn owner_child() {
    let Ok(db) = std::env::var("DURABLE_OWNER_CHILD_DB") else {
        return;
    };
    let owner = OwnedStore::open(db).unwrap();
    owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: b"child".to_vec(),
                value: b"owned".to_vec(),
            }],
        )
        .unwrap();
    fs::write(std::env::var("DURABLE_OWNER_READY").unwrap(), b"ready").unwrap();
    let mut byte = [0];
    std::io::stdin().read_exact(&mut byte).unwrap();
    drop(owner);
}

fn start_child(db: &std::path::Path, ready: &std::path::Path) -> std::process::Child {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "owner_child", "--nocapture"])
        .env("DURABLE_OWNER_CHILD_DB", db)
        .env("DURABLE_OWNER_READY", ready)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before acquiring ownership"
        );
        assert!(Instant::now() < deadline, "child did not acquire ownership");
        thread::sleep(Duration::from_millis(10));
    }
    child
}

#[test]
fn child_owner_excludes_then_releases_and_other_stores_remain_available() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store.db");
    let mut child = start_child(&db, &dir.path().join("ready"));
    assert!(matches!(OwnedStore::open(&db), Err(OwnerError::Busy)));
    let other = OwnedStore::open(dir.path().join("other.db")).unwrap();
    // Generic store connections remain independent of scheduler ownership.
    assert_eq!(
        Store::open(&db)
            .unwrap()
            .get(b"child")
            .unwrap()
            .unwrap()
            .value,
        b"owned"
    );
    child.stdin.take().unwrap().write_all(b"x").unwrap();
    assert!(child.wait().unwrap().success());
    let owner = OwnedStore::open(&db).unwrap();
    assert!(matches!(OwnedStore::open(&db), Err(OwnerError::Busy)));
    drop(owner);
    drop(other);
    assert!(dir.path().join("store.db.scheduler.lock").exists());
    assert!(OwnedStore::open(&db).is_ok());
}

#[test]
fn killed_owner_releases_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store.db");
    let mut child = start_child(&db, &dir.path().join("ready"));
    assert!(matches!(OwnedStore::open(&db), Err(OwnerError::Busy)));
    child.kill().unwrap();
    child.wait().unwrap();
    let recovered = OwnedStore::open(&db).unwrap();
    assert_eq!(
        recovered.store().get(b"child").unwrap().unwrap().value,
        b"owned"
    );
}

#[test]
fn aliases_are_normalized_or_rejected() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    fs::create_dir(&real).unwrap();
    let link = dir.path().join("dir-link");
    symlink(&real, &link).unwrap();
    let db = real.join("db");
    let owner = OwnedStore::open(&db).unwrap();
    assert!(matches!(
        OwnedStore::open(real.join("db.scheduler.lock")),
        Err(OwnerError::InvalidPath(_))
    ));
    assert!(matches!(
        OwnedStore::open(link.join("db")),
        Err(OwnerError::Busy)
    ));
    symlink(&db, real.join("symlink-db")).unwrap();
    assert!(matches!(
        OwnedStore::open(real.join("symlink-db")),
        Err(OwnerError::InvalidPath(_))
    ));
    fs::hard_link(&db, real.join("hard-db")).unwrap();
    assert!(matches!(
        OwnedStore::open(real.join("hard-db")),
        Err(OwnerError::InvalidPath(_))
    ));
    assert!(matches!(
        OwnedStore::open(&db),
        Err(OwnerError::InvalidPath(_))
    ));
    drop(owner);
    fs::remove_file(real.join("hard-db")).unwrap();
    fs::hard_link(real.join("db.scheduler.lock"), real.join("hard-lock")).unwrap();
    assert!(matches!(
        OwnedStore::open(&db),
        Err(OwnerError::InvalidPath(_))
    ));
}

#[test]
fn task_gate_reentry_wait_and_poison_are_preserved() {
    use durable_store::controls::PollGateError;
    use std::sync::{mpsc, Arc};
    let dir = tempfile::tempdir().unwrap();
    let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
    let guard = owner.task_gate().lock().unwrap();
    assert!(matches!(
        owner.task_gate().try_lock(),
        Err(PollGateError::Reentrant)
    ));
    let worker = owner.clone();
    let (busy_tx, busy_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        assert!(matches!(
            worker.task_gate().try_lock(),
            Err(PollGateError::Busy)
        ));
        busy_tx.send(()).unwrap();
        let _guard = worker.task_gate().lock().unwrap();
        done_tx.send(()).unwrap();
    });
    busy_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(done_rx.try_recv().is_err());
    drop(guard);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    thread.join().unwrap();
    let worker = owner.clone();
    assert!(std::thread::spawn(move || {
        let _guard = worker.task_gate().lock().unwrap();
        panic!("test poison");
    })
    .join()
    .is_err());
    assert!(matches!(
        owner.task_gate().lock(),
        Err(PollGateError::Poisoned)
    ));
}
