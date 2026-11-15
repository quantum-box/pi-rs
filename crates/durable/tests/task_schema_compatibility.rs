use pi_rs_durable::task::{Checkpoint, Task, TaskOwner, TaskState, WaitFor};
use serde_json::json;
use uuid::Uuid;

#[test]
fn public_task_paths_and_legacy_serde_shape_are_stable() {
    assert_eq!(std::any::type_name::<Task>(), "pi_rs_durable::task::Task");
    let id = Uuid::from_u128(1);
    let owner = Uuid::from_u128(2);
    let old = json!({"id":id,"owner":{"Conversation":owner},"checkpoint":{"phase":"start","data":null},"state":"Ready","restarts":0,"restart_limit":4});
    let task: Task = serde_json::from_value(old).unwrap();
    assert_eq!(task.id(), id);
    assert_eq!(task.owner(), &TaskOwner::Conversation(owner));
    assert_eq!(task.state(), &TaskState::Ready);
    assert_eq!(
        task.checkpoint(),
        &Checkpoint {
            phase: "start".into(),
            data: json!(null)
        }
    );
    let expected = json!({"id":id,"owner":{"Conversation":owner},"checkpoint":{"phase":"start","data":null},"definition_version":1,"input":null,"state":"Ready","restarts":0,"restart_limit":4,"delivered_signal":null,"typed_wake":null,"kind":null,"replay_safe":false,"abort_requested":false,"background":false});
    assert_eq!(serde_json::to_value(task).unwrap(), expected);
    let wait: WaitFor =
        serde_json::from_value(json!({"TypedSignal":{"signal":"test","key":"x"}})).unwrap();
    assert_eq!(
        serde_json::to_value(wait).unwrap(),
        json!({"TypedSignal":{"signal":"test","key":"x"}})
    );
}
