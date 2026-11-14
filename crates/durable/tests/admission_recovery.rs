use durable_store::Store;
use pi_rs_durable::{
    conversation::Conversations,
    state::{InputMode, SubmissionStatus},
};
use uuid::Uuid;

#[test]
fn child_admits_then_exits_without_closing() {
    let Ok(path) = std::env::var("PI_DURABLE_ADMISSION_DB") else {
        return;
    };
    let id = Uuid::parse_str(&std::env::var("PI_DURABLE_CONVERSATION").unwrap()).unwrap();
    let conversations = Conversations::new(Store::open(path).unwrap());
    conversations.create(id).unwrap();
    conversations
        .submit(id, Some("job".into()), "first".into(), InputMode::FollowUp)
        .unwrap();
    conversations
        .submit(
            id,
            Some("queued".into()),
            "later".into(),
            InputMode::FollowUp,
        )
        .unwrap();
    // Do not run destructors: simulate a process lost after commit but before
    // the caller receives a response.
    std::process::exit(0);
}

#[test]
fn committed_admission_survives_process_loss_and_resend() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let id = Uuid::new_v4();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("child_admits_then_exits_without_closing")
        .env("PI_DURABLE_ADMISSION_DB", &path)
        .env("PI_DURABLE_CONVERSATION", id.to_string())
        .status()
        .unwrap();
    assert!(status.success());
    let conversations = Conversations::new(Store::open(path).unwrap());
    let before = conversations.get(id).unwrap();
    let repeated = conversations
        .submit(id, Some("job".into()), "first".into(), InputMode::FollowUp)
        .unwrap();
    assert_eq!(repeated, before.submissions[0].id);
    let state = conversations.get(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.submissions[1].status, SubmissionStatus::Queued);
    assert_eq!(state.entries.len(), 1);
}
