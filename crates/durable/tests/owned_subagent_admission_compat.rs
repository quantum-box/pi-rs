use pi_rs_durable::{
    owned_subagent::Admission, ownership_edge::Mode, runtime::Config, task::TaskOwner,
};
use uuid::Uuid;

#[test]
fn downstream_can_construct_original_admission_literal() {
    let admission = Admission {
        task: Uuid::new_v4(),
        task_owner: TaskOwner::Conversation(Uuid::new_v4()),
        key: "child".into(),
        child: Uuid::new_v4(),
        config: Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 1,
        },
        mode: Mode::Foreground,
        allowed_tools: None,
        inherit_parent_tools: true,
        submission: None,
    };
    assert!(admission.inherit_parent_tools);
}
