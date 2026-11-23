#[cfg(test)]
use pi_rs_core::ModelRequest;
use std::collections::BTreeSet;

use pi_rs_core::{Decision, Extension, ExtensionResult, ExtensionState, Message, ToolContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub struct PlanMode {
    read_only: BTreeSet<String>,
}

impl PlanMode {
    /// The host attests that these exact tools cannot cause side effects.
    pub fn new(read_only: impl IntoIterator<Item = String>) -> Self {
        Self {
            read_only: read_only.into_iter().collect(),
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum PlanState {
    #[default]
    Normal,
    Planning {
        plan: String,
    },
    Executing {
        plan: String,
    },
}

fn decode(state: Option<&ExtensionState>) -> ExtensionResult<PlanState> {
    match state {
        None => Ok(PlanState::Normal),
        Some(state) if state.version == 1 => {
            serde_json::from_value(state.data.clone()).map_err(|_| "invalid plan state".into())
        }
        Some(_) => Err("unsupported plan state version".into()),
    }
}

#[async_trait::async_trait]
impl Extension for PlanMode {
    fn id(&self) -> &str {
        "plan-mode"
    }
    fn source_is_builtin(&self) -> bool {
        true
    }

    fn commands(&self) -> &[&str] {
        &["start", "revise", "accept", "discard"]
    }

    fn command(
        &self,
        name: &str,
        argument: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        let state = decode(previous)?;
        let bounded_plan = || {
            if argument.trim().is_empty() || argument.len() > 16 * 1024 {
                Err("plan must contain 1 to 16384 bytes".to_string())
            } else {
                Ok(argument.to_owned())
            }
        };
        let next = match (name, state) {
            ("start", _) | ("revise", PlanState::Planning { .. }) => PlanState::Planning {
                plan: bounded_plan()?,
            },
            ("accept", PlanState::Planning { plan }) => PlanState::Executing { plan },
            ("discard", _) => PlanState::Normal,
            _ => return Err("invalid plan mode transition".into()),
        };
        Ok(ExtensionState {
            version: 1,
            data: serde_json::to_value(next).map_err(|error| error.to_string())?,
        })
    }

    fn context(
        &self,
        request: &mut pi_rs_core::ContextRequest<'_>,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let plan = match decode(state)? {
            PlanState::Normal => return Ok(pi_rs_core::ContextChange::Unchanged),
            PlanState::Planning { plan } => {
                request
                    .tools_mut()
                    .retain(|tool| self.read_only.contains(&tool.name));
                format!(
                    "Planning mode. Investigate with the approved read-only tools. Current plan:\n{plan}"
                )
            }
            PlanState::Executing { plan } => format!(
                "Accepted working plan. Follow the host's current authorization for each operation.\n{plan}"
            ),
        };
        let insert_at = 1.min(request.messages.len());
        request.insert_user(insert_at, Message::User { content: plan })?;
        Ok(pi_rs_core::ContextChange::Changed)
    }

    async fn before_tool(
        &self,
        name: &str,
        _: &Value,
        _: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        if matches!(decode(state)?, PlanState::Planning { .. }) && !self.read_only.contains(name) {
            return Ok(Decision::Deny(
                "plan mode only allows host-approved read-only tools".into(),
            ));
        }
        Ok(Decision::Allow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs_core::{CancellationToken, ToolSpec};
    use serde_json::json;

    #[tokio::test]
    async fn planning_filters_tools_and_rejects_unadvertised_writes() {
        let extension = PlanMode::new(["read".into()]);
        let state = extension
            .command("start", "review the changes", None)
            .unwrap();
        let mut request = ModelRequest {
            model: "fixture".into(),
            messages: vec![Message::System {
                content: "fixture".into(),
            }],
            tools: ["read", "command", "mcp_write"]
                .into_iter()
                .map(|name| ToolSpec {
                    name: name.into(),
                    description: "fixture".into(),
                    parameters: json!({"type":"object"}),
                })
                .collect(),
            max_output_tokens: 32,
        };
        let mut guarded = pi_rs_core::ContextRequest::new(&mut request);
        let access = guarded.access();
        extension.context(&mut guarded, Some(&state)).unwrap();
        assert!(!access.assistant_accessed());
        assert_eq!(access.user_edits().len(), 1);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, "read");
        let context = ToolContext {
            operation_id: uuid::Uuid::new_v4(),
            cancellation: CancellationToken::new(),
        };
        for name in ["command", "mcp_write"] {
            assert!(matches!(
                extension
                    .before_tool(name, &json!({}), &context, Some(&state))
                    .await
                    .unwrap(),
                Decision::Deny(_)
            ));
        }
        let accepted = extension.command("accept", "", Some(&state)).unwrap();
        assert_eq!(accepted.data["mode"], "executing");
    }
}
