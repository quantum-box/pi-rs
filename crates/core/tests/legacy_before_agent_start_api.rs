//! Source compatibility with downstream extensions built against main.
use pi_rs_core::{
    AgentStartTrigger, BeforeAgentStartEvent, BeforeAgentStartPromptOptions,
    BeforeAgentStartResult, Extension, ExtensionResult, ExtensionState,
};

struct LegacyExtension;

#[async_trait::async_trait]
impl Extension for LegacyExtension {
    fn id(&self) -> &str {
        "legacy-extension"
    }

    fn handles_before_agent_start(&self) -> bool {
        true
    }

    async fn before_agent_start(
        &self,
        event: &BeforeAgentStartEvent,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<BeforeAgentStartResult> {
        let BeforeAgentStartEvent {
            trigger,
            system_prompt,
        } = event;
        assert!(matches!(trigger, AgentStartTrigger::User { .. }));
        Ok(BeforeAgentStartResult {
            message: None,
            system_prompt: Some(format!("{system_prompt} with legacy override")),
        })
    }
}

#[tokio::test]
async fn original_before_agent_start_literals_work_through_the_new_hook() {
    let extension: &dyn Extension = &LegacyExtension;
    let event = BeforeAgentStartEvent {
        trigger: AgentStartTrigger::User {
            prompt: "question".into(),
            images: Vec::new(),
        },
        system_prompt: "base prompt".into(),
    };
    let options = BeforeAgentStartPromptOptions {
        sections: [("rules".into(), "host rules".into())]
            .into_iter()
            .collect(),
    };

    assert!(extension.handles_before_agent_start());
    let result = extension
        .before_agent_start_with_prompt_options(&event, &options, None)
        .await
        .unwrap();
    let BeforeAgentStartResult {
        message,
        system_prompt,
    } = result.result;
    assert!(message.is_none());
    assert_eq!(
        system_prompt.as_deref(),
        Some("base prompt with legacy override")
    );
    assert!(result.system_prompt_options.sections.is_empty());
}
