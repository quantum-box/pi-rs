use async_trait::async_trait;
use durable_store::{Check, Expected, Mutation, Store};
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error as ModelError, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
    session_membership::{Error, SessionMembership},
};
use std::sync::Arc;

struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn setup(path: &std::path::Path) -> (Harness, uuid::Uuid, uuid::Uuid) {
    let mut h = Harness::open_paused(path, options()).unwrap();
    let config = || Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 10,
    };
    let a = h.create_conversation(config()).unwrap();
    let b = h.create_conversation(config()).unwrap();
    (h, a, b)
}
fn effect() -> Mutation {
    Mutation::Put {
        key: b"host/effect".to_vec(),
        value: b"done".to_vec(),
    }
}

#[test]
fn membership_is_explicit_scoped_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, a, b) = setup(&path);
    let store = Store::open(&path).unwrap();
    let auth = SessionMembership::new(&store);
    let s = auth.create().unwrap();
    let other = auth.create().unwrap();
    assert!(auth.authorize_pair(s, a, b).is_err());
    auth.enroll(s, a).unwrap();
    auth.enroll(other, b).unwrap();
    assert!(auth.authorize_pair(s, a, b).is_err());
    assert!(auth.enroll(s, b).is_err());
    auth.revoke(other, b).unwrap();
    auth.enroll(s, b).unwrap();
    drop(store);
    drop(h);
    let reopened = Store::open(&path).unwrap();
    let auth = SessionMembership::new(&reopened);
    auth.authorize_pair(s, a, b)
        .unwrap()
        .commit(
            &[Check {
                key: b"host/effect".to_vec(),
                expected: Expected::Missing,
            }],
            &[effect()],
        )
        .unwrap();
    assert_eq!(
        reopened.get(b"host/effect").unwrap().unwrap().value,
        b"done"
    );
    assert!(auth.authorize_pair(other, a, b).is_err());
}

#[test]
fn revoke_reenroll_and_conversation_changes_fence_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, a, b) = setup(&path);
    let store = Store::open(&path).unwrap();
    let auth = SessionMembership::new(&store);
    let s = auth.create().unwrap();
    auth.enroll(s, a).unwrap();
    auth.enroll(s, b).unwrap();
    let stale = auth.authorize_pair(s, a, b).unwrap();
    auth.revoke(s, b).unwrap();
    auth.enroll(s, b).unwrap();
    assert!(matches!(
        stale.commit(&[], &[effect()]),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(store.get(b"host/effect").unwrap().is_none());
    let stale = auth.authorize_pair(s, a, b).unwrap();
    h.submit_write(b, None, "app.test".into(), None).unwrap();
    assert!(matches!(
        stale.commit(&[], &[effect()]),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(store.get(b"host/effect").unwrap().is_none());
}

#[test]
fn proof_cannot_mutate_authority_and_checks_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (_h, a, b) = setup(&path);
    let store = Store::open(&path).unwrap();
    let auth = SessionMembership::new(&store);
    let s = auth.create().unwrap();
    auth.enroll(s, a).unwrap();
    auth.enroll(s, b).unwrap();
    let bad = Mutation::Delete {
        key: format!("durable/session-membership/v1/conversation/{b}").into_bytes(),
    };
    assert!(matches!(
        auth.authorize_pair(s, a, b)
            .unwrap()
            .commit(&[], &[effect(), bad]),
        Err(Error::ProtectedKey)
    ));
    assert!(store.get(b"host/effect").unwrap().is_none());
    assert!(matches!(
        auth.authorize_pair(s, a, b).unwrap().commit(
            &[Check {
                key: b"absent".to_vec(),
                expected: Expected::Version(1)
            }],
            &[effect()]
        ),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(store.get(b"host/effect").unwrap().is_none());
}

#[test]
fn abort_invalidates_authorization_and_prevents_new_proof() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, a, b) = setup(&path);
    let store = Store::open(&path).unwrap();
    let auth = SessionMembership::new(&store);
    let s = auth.create().unwrap();
    auth.enroll(s, a).unwrap();
    auth.enroll(s, b).unwrap();
    let stale = auth.authorize_pair(s, a, b).unwrap();
    h.request_abort(b).unwrap();
    assert!(matches!(
        stale.commit(&[], &[effect()]),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(matches!(
        auth.authorize_pair(s, a, b),
        Err(Error::InvalidConversation)
    ));
    assert!(store.get(b"host/effect").unwrap().is_none());
}

#[test]
fn session_membership_kill_child() {
    let Ok(path) = std::env::var("SESSION_MEMBERSHIP_CHILD_DB") else {
        return;
    };
    let session = std::env::var("SESSION_MEMBERSHIP_CHILD_SESSION")
        .unwrap()
        .parse()
        .unwrap();
    let conversation = std::env::var("SESSION_MEMBERSHIP_CHILD_CONVERSATION")
        .unwrap()
        .parse()
        .unwrap();
    let marker = std::env::var("SESSION_MEMBERSHIP_CHILD_MARKER").unwrap();
    let store = Store::open(path).unwrap();
    SessionMembership::new(&store)
        .revoke(session, conversation)
        .unwrap();
    std::fs::write(marker, b"revoked").unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn committed_revocation_survives_process_kill_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let marker = dir.path().join("ready");
    let (h, a, b) = setup(&path);
    let store = Store::open(&path).unwrap();
    let auth = SessionMembership::new(&store);
    let s = auth.create().unwrap();
    auth.enroll(s, a).unwrap();
    auth.enroll(s, b).unwrap();
    drop(store);
    drop(h);
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "session_membership_kill_child", "--nocapture"])
        .env("SESSION_MEMBERSHIP_CHILD_DB", &path)
        .env("SESSION_MEMBERSHIP_CHILD_SESSION", s.to_string())
        .env("SESSION_MEMBERSHIP_CHILD_CONVERSATION", b.to_string())
        .env("SESSION_MEMBERSHIP_CHILD_MARKER", &marker)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !marker.exists() {
        if std::time::Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("revocation child did not reach commit");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let reopened = Store::open(&path).unwrap();
    assert!(
        SessionMembership::new(&reopened)
            .authorize_pair(s, a, b)
            .is_err()
    );
    assert!(reopened.get(b"host/effect").unwrap().is_none());
}
