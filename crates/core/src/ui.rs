//! Optional native interaction surface supplied by an embedding application.
use serde::{Deserialize, Serialize};

use crate::{CancellationToken, ExtensionResult};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WidgetPlacement {
    #[default]
    AboveEditor,
    BelowEditor,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiComponentSpan {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default)]
    pub bold: bool,
    #[serde(default)]
    pub italic: bool,
    #[serde(default)]
    pub underline: bool,
    #[serde(default)]
    pub inverse: bool,
}

/// Layout options for an extension's custom transcript message.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomMessageRenderOptions {
    pub expanded: bool,
    pub output_pad: usize,
    pub expand_key: Option<String>,
}

/// The extension supplies semantic theme roles. The host applies its palette
/// and wraps the rows to fit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderedCustomMessage {
    pub lines: Vec<Vec<UiComponentSpan>>,
    #[serde(default)]
    pub padding: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
}

/// Styled rows shared by the native extension renderers.
pub type RenderedExtensionRows = RenderedCustomMessage;

impl RenderedCustomMessage {
    pub(crate) fn within_limits(&self) -> bool {
        component_lines_within_limits(&self.lines)
            && self
                .background
                .as_ref()
                .is_none_or(|role| role.len() <= 128)
            && self
                .lines
                .iter()
                .flatten()
                .all(|span| span.color.as_ref().is_none_or(|color| color.len() <= 128))
    }
}

fn component_lines_within_limits(lines: &[Vec<UiComponentSpan>]) -> bool {
    lines.len() <= 256
        && lines.iter().map(Vec::len).sum::<usize>() <= 1024
        && lines
            .iter()
            .flatten()
            .map(|span| span.text.len())
            .sum::<usize>()
            <= 64 * 1024
}

/// Resolved custom-message renderers in extension load order. Hosts can use the
/// snapshot while a session runs, independently of its journal and model context.
#[derive(Clone, Default)]
pub struct CustomMessageRenderers {
    pub(crate) renderers: std::collections::BTreeMap<String, std::sync::Arc<dyn crate::Extension>>,
}

impl CustomMessageRenderers {
    pub fn types(&self) -> impl Iterator<Item = &str> {
        self.renderers.keys().map(String::as_str)
    }

    pub fn contains(&self, custom_type: &str) -> bool {
        self.renderers.contains_key(custom_type)
    }

    /// Invoke the first registered renderer for a visible message. An absent,
    /// failed, oversized or timed-out response leaves fallback rendering to
    /// the host.
    pub async fn render(
        &self,
        message: &crate::CustomMessage,
        options: &CustomMessageRenderOptions,
        timeout: std::time::Duration,
    ) -> Option<RenderedCustomMessage> {
        use futures_util::FutureExt;

        if !message.display {
            return None;
        }
        let renderer = self.renderers.get(&message.custom_type)?;
        let callback = std::panic::AssertUnwindSafe(async {
            renderer.render_custom_message(message, options).await
        })
        .catch_unwind();
        crate::runtime::time::timeout(timeout, callback)
            .await
            .ok()?
            .ok()?
            .ok()?
            .filter(RenderedCustomMessage::within_limits)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiComponentAction {
    pub key: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "method",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum UiRequest {
    Select {
        title: String,
        options: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    Confirm {
        title: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    Input {
        title: String,
        #[serde(default)]
        placeholder: Option<String>,
        #[serde(default)]
        initial_value: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    Editor {
        title: String,
        #[serde(default, rename = "prefill", alias = "initialValue")]
        initial_value: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    /// A modal native component. Return the selected action value, or cancel.
    Custom {
        title: String,
        lines: Vec<Vec<UiComponentSpan>>,
        actions: Vec<UiComponentAction>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    Notify {
        message: String,
        #[serde(default, rename = "notifyType", alias = "level")]
        level: Option<String>,
    },
    SetStatus {
        #[serde(rename = "statusKey", alias = "key")]
        key: String,
        #[serde(default, rename = "statusText", alias = "text")]
        text: Option<String>,
    },
    SetWidget {
        #[serde(rename = "widgetKey", alias = "key")]
        key: String,
        #[serde(default, rename = "widgetLines", alias = "lines")]
        lines: Option<Vec<String>>,
        #[serde(default, rename = "widgetPlacement")]
        placement: WidgetPlacement,
    },
    /// Replace the session header with styled rows. Clear it with `None`.
    SetHeader {
        #[serde(default)]
        lines: Option<Vec<Vec<UiComponentSpan>>>,
    },
    /// Replace the session footer with styled rows. Clear it with `None`.
    SetFooter {
        #[serde(default)]
        lines: Option<Vec<Vec<UiComponentSpan>>>,
    },
    SetTitle {
        title: String,
    },
    #[serde(rename = "set_editor_text", alias = "setEditorText")]
    SetEditorText {
        text: String,
    },
    GetEditorText,
}
impl UiRequest {
    pub fn timeout(&self) -> Option<u64> {
        match self {
            Self::Select { timeout, .. }
            | Self::Confirm { timeout, .. }
            | Self::Input { timeout, .. }
            | Self::Editor { timeout, .. }
            | Self::Custom { timeout, .. } => *timeout,
            _ => None,
        }
    }
    pub fn validate(&self) -> ExtensionResult<()> {
        if serde_json::to_vec(self)
            .map_err(|error| error.to_string())?
            .len()
            > 1024 * 1024
        {
            return Err("extension UI request exceeds 1 MiB".into());
        }
        if let Self::Select { options, .. } = self
            && (options.is_empty() || options.len() > 128)
        {
            return Err("extension UI selection needs 1–128 options".into());
        }
        if let Self::SetStatus { key, .. } | Self::SetWidget { key, .. } = self
            && (key.is_empty() || key.len() > 256)
        {
            return Err("extension UI key must contain 1–256 bytes".into());
        }
        if let Self::SetWidget {
            lines: Some(lines), ..
        } = self
            && lines.len() > 128
        {
            return Err("extension widgets support at most 128 lines".into());
        }
        if let Self::SetHeader { lines: Some(lines) } | Self::SetFooter { lines: Some(lines) } =
            self
            && (lines.len() > 16
                || !component_lines_within_limits(lines)
                || lines
                    .iter()
                    .flatten()
                    .any(|span| span.color.as_ref().is_some_and(|color| color.len() > 128)))
        {
            return Err("extension header or footer exceeds its rendering limits".into());
        }
        if let Self::Custom { lines, actions, .. } = self {
            if !component_lines_within_limits(lines) {
                return Err("extension component exceeds its rendering limits".into());
            }
            let mut keys = std::collections::BTreeSet::new();
            if actions.is_empty()
                || actions.len() > 64
                || actions.iter().any(|action| {
                    action.key.trim().is_empty()
                        || action.key.len() > 64
                        || action.key.chars().any(char::is_control)
                        || action.value.is_empty()
                        || action.value.len() > 1024
                        || !keys.insert(action.key.to_lowercase())
                })
            {
                return Err("extension component needs 1–64 actions with unique keys".into());
            }
        }
        Ok(())
    }
    pub fn accepts_response(&self, response: &UiResponse) -> bool {
        match (self, response) {
            (_, UiResponse::Cancelled) => true,
            (Self::Select { options, .. }, UiResponse::Text(value)) => options.contains(value),
            (Self::Confirm { .. }, UiResponse::Confirmed(_)) => true,
            (Self::Custom { actions, .. }, UiResponse::Text(value)) => {
                actions.iter().any(|action| &action.value == value)
            }
            (
                Self::Input { .. } | Self::Editor { .. } | Self::GetEditorText,
                UiResponse::Text(value),
            ) => value.len() <= 1024 * 1024,
            _ => false,
        }
    }
    pub fn needs_response(&self) -> bool {
        matches!(
            self,
            Self::Select { .. }
                | Self::Confirm { .. }
                | Self::Input { .. }
                | Self::Editor { .. }
                | Self::Custom { .. }
                | Self::GetEditorText
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UiResponse {
    Text(String),
    Confirmed(bool),
    Cancelled,
}

#[async_trait::async_trait]
pub trait ExtensionUi: Send + Sync {
    async fn request(
        &self,
        request: UiRequest,
        cancellation: CancellationToken,
    ) -> ExtensionResult<UiResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn header_footer_updates_are_bounded_and_do_not_open_prompts() {
        let rows = vec![vec![UiComponentSpan {
            text: "header".into(),
            ..Default::default()
        }]];
        let request = UiRequest::SetHeader {
            lines: Some(rows.clone()),
        };
        assert!(request.validate().is_ok());
        assert!(!request.needs_response());
        assert!(request.timeout().is_none());
        let json = serde_json::to_value(request).unwrap();
        assert_eq!(json["method"], "setHeader");
        assert_eq!(json["lines"][0][0]["text"], "header");
        let clear: UiRequest =
            serde_json::from_value(serde_json::json!({"method": "setFooter"})).unwrap();
        assert!(matches!(clear, UiRequest::SetFooter { lines: None }));
        assert!(
            UiRequest::SetFooter {
                lines: Some(vec![rows[0].clone(); 17])
            }
            .validate()
            .is_err()
        );
    }

    #[tokio::test]
    async fn first_custom_message_renderer_wins_and_hidden_messages_do_not_run() {
        struct Renderer(&'static str);
        #[async_trait::async_trait]
        impl crate::Extension for Renderer {
            fn id(&self) -> &str {
                self.0
            }
            fn custom_message_renderer_types(&self) -> Vec<&str> {
                vec!["card"]
            }
            async fn render_custom_message(
                &self,
                message: &crate::CustomMessage,
                options: &CustomMessageRenderOptions,
            ) -> ExtensionResult<Option<RenderedCustomMessage>> {
                assert!(message.display);
                let label = match message.details.as_ref().and_then(serde_json::Value::as_str) {
                    Some("fail") => return Err("renderer failure".into()),
                    Some("oversize") => "x".repeat(64 * 1024 + 1),
                    Some(value) if options.expanded => format!("{} {value}", self.0),
                    _ => self.0.into(),
                };
                Ok(Some(RenderedCustomMessage {
                    lines: vec![vec![UiComponentSpan {
                        text: label,
                        color: Some("accent".into()),
                        ..Default::default()
                    }]],
                    padding: options.output_pad,
                    background: None,
                }))
            }
        }

        let mut registry = crate::Registry::default();
        registry
            .register(std::sync::Arc::new(Renderer("first")))
            .unwrap();
        registry
            .register(std::sync::Arc::new(Renderer("second")))
            .unwrap();
        let renderers = registry.custom_message_renderers();
        assert!(renderers.contains("card"));
        assert!(!renderers.contains("unregistered"));
        let mut message = crate::CustomMessage {
            custom_type: "card".into(),
            content: crate::ContextContent::Text("model context".into()),
            display: true,
            details: Some(serde_json::json!("details")),
        };
        let options = CustomMessageRenderOptions {
            expanded: true,
            output_pad: 2,
            expand_key: Some("Ctrl+O".into()),
        };
        let timeout = std::time::Duration::from_secs(1);
        let rendered = renderers.render(&message, &options, timeout).await.unwrap();
        assert_eq!(rendered.lines[0][0].text, "first details");
        assert_eq!(rendered.lines[0][0].color.as_deref(), Some("accent"));
        assert_eq!(rendered.padding, 2);
        for details in ["fail", "oversize"] {
            message.details = Some(serde_json::json!(details));
            assert!(
                renderers
                    .render(&message, &options, timeout)
                    .await
                    .is_none()
            );
        }
        message.display = false;
        assert!(
            renderers
                .render(&message, &options, timeout)
                .await
                .is_none()
        );
    }

    #[test]
    fn rpc_fields_and_typed_responses_match_dialog_kinds() {
        let widget: UiRequest = serde_json::from_value(serde_json::json!({
            "method":"setWidget","widgetKey":"summary","widgetLines":["line"],"widgetPlacement":"belowEditor"
        })).unwrap();
        assert!(matches!(
            widget,
            UiRequest::SetWidget {
                placement: WidgetPlacement::BelowEditor,
                ..
            }
        ));
        let editor: UiRequest = serde_json::from_value(
            serde_json::json!({"method":"editor","title":"Edit","prefill":"text","timeout":20}),
        )
        .unwrap();
        assert_eq!(editor.timeout(), Some(20));
        assert!(editor.accepts_response(&UiResponse::Text("changed".into())));
        assert!(!editor.accepts_response(&UiResponse::Confirmed(true)));
        assert_eq!(
            serde_json::to_value(UiRequest::SetEditorText {
                text: "draft".into()
            })
            .unwrap()["method"],
            "set_editor_text"
        );
        let invalid = UiRequest::Select {
            title: "Select".into(),
            options: vec![],
            timeout: None,
        };
        assert!(invalid.validate().is_err());
    }
}
