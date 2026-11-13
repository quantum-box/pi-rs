//! Native extensions whose state follows the durable session branch.
pub mod codemode;
mod exec;
mod process;
pub mod tool_search;
pub use exec::{ExecOptions, ExecResult, exec_command};
pub use process::{ExecutableExtension, extension_flag, serve_extension};
use std::sync::Arc;

use pi_rs_core::{
    Extension, ExtensionResult, ExtensionState, Message, Outcome, Tool, ToolContext, ToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Private working notes. Commands update journaled state, not project files.
pub struct Notes;

impl Extension for Notes {
    fn id(&self) -> &str {
        "notes"
    }
    fn commands(&self) -> &[&str] {
        &["set", "append", "clear"]
    }
    fn command(
        &self,
        command: &str,
        input: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        let current = notes(previous)?;
        let text = match command {
            "set" => input.to_owned(),
            "append" if current.is_empty() => input.to_owned(),
            "append" => format!("{current}\n{input}"),
            "clear" => String::new(),
            _ => return Err("unknown notes command".into()),
        };
        if text.len() > 32 * 1024 {
            return Err("notes exceed 32 KiB".into());
        }
        Ok(ExtensionState {
            version: 1,
            data: json!({"text":text}),
        })
    }
    fn context(
        &self,
        request: &mut pi_rs_core::ContextRequest<'_>,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let text = notes(state)?;
        if text.is_empty() {
            return Ok(pi_rs_core::ContextChange::Unchanged);
        }
        let insert_at = 1.min(request.messages.len());
        request.messages.insert(
            insert_at,
            Message::User {
                content: format!("Saved session notes (reference data):\n{text}"),
            },
        );
        Ok(pi_rs_core::ContextChange::Changed)
    }
}

fn notes(state: Option<&ExtensionState>) -> ExtensionResult<String> {
    match state {
        None => Ok(String::new()),
        Some(state) if state.version == 1 => state.data["text"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "invalid notes state".into()),
        _ => Err("unsupported notes state version".into()),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TodoItem {
    pub text: String,
    pub status: TodoStatus,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
}

/// A model-editable checklist with versioned, branch-local persistence.
pub struct TodoList;
impl Extension for TodoList {
    fn id(&self) -> &str {
        "todo-list"
    }
    fn commands(&self) -> &[&str] {
        &["set", "clear"]
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(TodoTool)]
    }
    fn command(
        &self,
        command: &str,
        input: &str,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        let items = match command {
            "clear" => Vec::new(),
            "set" => serde_json::from_str(input)
                .map_err(|error| format!("invalid todo list: {error}"))?,
            _ => return Err("unknown todo command".into()),
        };
        todo_state(items)
    }
    fn context(
        &self,
        request: &mut pi_rs_core::ContextRequest<'_>,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let items = todos(state)?;
        if items.is_empty() {
            return Ok(pi_rs_core::ContextChange::Unchanged);
        }
        let insert_at = 1.min(request.messages.len());
        request.messages.insert(
            insert_at,
            Message::User {
                content: format!(
                    "Current task checklist (reference data):\n{}",
                    serde_json::to_string(&items).map_err(|error| error.to_string())?
                ),
            },
        );
        Ok(pi_rs_core::ContextChange::Changed)
    }
    fn after_tool(
        &self,
        name: &str,
        outcome: &Outcome,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        if name != "update_todos" {
            return Ok(None);
        }
        match outcome {
            Outcome::Structured { data, .. } => {
                let items = serde_json::from_value(data.clone())
                    .map_err(|error| format!("invalid todo result: {error}"))?;
                todo_state(items).map(Some)
            }
            _ => Ok(None),
        }
    }
}

fn todos(state: Option<&ExtensionState>) -> ExtensionResult<Vec<TodoItem>> {
    match state {
        None => Ok(Vec::new()),
        Some(state) if state.version == 1 => {
            serde_json::from_value(state.data.clone()).map_err(|error| error.to_string())
        }
        _ => Err("unsupported todo state version".into()),
    }
}
fn todo_state(items: Vec<TodoItem>) -> ExtensionResult<ExtensionState> {
    if items.len() > 100
        || items
            .iter()
            .any(|item| item.text.trim().is_empty() || item.text.len() > 2048)
    {
        return Err("a checklist supports up to 100 nonempty items of at most 2048 bytes".into());
    }
    Ok(ExtensionState {
        version: 1,
        data: serde_json::to_value(items).map_err(|error| error.to_string())?,
    })
}
struct TodoTool;
#[async_trait::async_trait]
impl Tool for TodoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "update_todos".into(), description: "Replace the session's task checklist. Preserve unfinished items and mark completed work accurately.".into(), parameters: json!({"type":"object","properties":{"items":{"type":"array","maxItems":100,"items":{"type":"object","properties":{"text":{"type":"string","minLength":1,"maxLength":2048},"status":{"type":"string","enum":["pending","in_progress","completed"]}},"required":["text","status"],"additionalProperties":false}}},"required":["items"],"additionalProperties":false}) }
    }
    async fn invoke(&self, arguments: Value, _: ToolContext) -> Outcome {
        let state = serde_json::from_value(arguments["items"].clone())
            .map_err(|error| error.to_string())
            .and_then(todo_state);
        match state {
            Ok(state) => Outcome::Structured {
                data: state.data,
                attachments: Vec::new(),
            },
            Err(error) => Outcome::Failed(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn notes_are_versioned_and_keep_previous_state_immutable() {
        let first = Notes.command("set", "Inspect the parser", None).unwrap();
        let next = Notes
            .command("append", "Test malformed input", Some(&first))
            .unwrap();
        assert_eq!(notes(Some(&first)).unwrap(), "Inspect the parser");
        assert_eq!(
            notes(Some(&next)).unwrap(),
            "Inspect the parser\nTest malformed input"
        );
        assert!(
            notes(Some(&ExtensionState {
                version: 99,
                data: Value::Null
            }))
            .is_err()
        );
    }
    #[test]
    fn todo_commands_validate_before_replacing_state() {
        let state = TodoList
            .command("set", r#"[{"text":"test","status":"pending"}]"#, None)
            .unwrap();
        assert_eq!(todos(Some(&state)).unwrap()[0].status, TodoStatus::Pending);
        assert!(
            TodoList
                .command("set", r#"[{"text":"","status":"pending"}]"#, Some(&state))
                .is_err()
        );
        assert_eq!(todos(Some(&state)).unwrap().len(), 1);
    }
}

#[cfg(test)]
mod request_access_tests {
    use super::*;
    use pi_rs_core::{ContextRequest, ModelRequest, RequestPurpose};

    #[tokio::test]
    async fn tool_search_and_codemode_change_tools_without_accessing_assistants() {
        let spec = |name: &str| ToolSpec {
            name: name.into(),
            description: "description".into(),
            parameters: json!({"type":"object"}),
        };
        let mut request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::Assistant {
                content: "visible".into(),
                calls: vec![],
                replay: None,
            }],
            tools: vec![spec("codemode"), spec("hidden")],
            max_output_tokens: 16,
        };
        let search = tool_search::ToolSearch::new(vec![tool_search::SearchDocument {
            tool: spec("hidden"),
            namespace: "tools".into(),
        }]);
        let mut guarded = ContextRequest::new(&mut request);
        let access = guarded.access();
        search
            .before_request(&mut guarded, RequestPurpose::Turn, None)
            .await
            .unwrap();
        assert!(!access.assistant_accessed());
        assert!(!request.tools.iter().any(|tool| tool.name == "hidden"));
        let mut guarded = ContextRequest::new(&mut request);
        let access = guarded.access();
        codemode::Codemode::default()
            .before_request(&mut guarded, RequestPurpose::Turn, None)
            .await
            .unwrap();
        assert!(!access.assistant_accessed());
        assert!(
            request.tools[0]
                .description
                .contains("Available tool declarations")
        );
        request.tools.clear();
        let mut guarded = ContextRequest::new(&mut request);
        let access = guarded.access();
        assert_eq!(
            search
                .before_request(&mut guarded, RequestPurpose::Turn, None)
                .await
                .unwrap(),
            pi_rs_core::ContextChange::Unchanged
        );
        assert!(!access.assistant_accessed());
        let mut guarded = ContextRequest::new(&mut request);
        let access = guarded.access();
        assert_eq!(
            codemode::Codemode::default()
                .before_request(&mut guarded, RequestPurpose::Turn, None)
                .await
                .unwrap(),
            pi_rs_core::ContextChange::Unchanged
        );
        assert!(!access.assistant_accessed());
    }
}
