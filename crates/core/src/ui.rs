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

/// The active terminal palette, without theme file paths or session data.
/// Color strings are CSS hex values, decimal ANSI palette indices, terminal
/// color names, or "default".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiRenderTheme {
    pub name: String,
    pub colors: std::collections::BTreeMap<String, String>,
    pub dimmed: Vec<String>,
}

/// Read-only layout information for a mounted header.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderRenderContext {
    pub width: u16,
    pub theme: UiRenderTheme,
}

/// A mounted header. Returning `None` uses the built-in header. Empty rows hide it.
/// The host cancels pending renders and releases this instance when it is unmounted.
#[async_trait::async_trait]
pub trait HeaderRenderer: Send + Sync {
    async fn render(
        &self,
        context: &HeaderRenderContext,
    ) -> ExtensionResult<Option<RenderedExtensionRows>>;
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

/// Styled rows returned by a custom-entry renderer.
pub type RenderedExtensionContent = RenderedCustomMessage;

/// Styled rows shared by the native extension renderers.
pub type RenderedExtensionRows = RenderedCustomMessage;

impl RenderedCustomMessage {
    /// Whether these rows fit the terminal header's limits.
    pub fn within_header_limits(&self) -> bool {
        self.lines.len() <= 16 && self.within_limits()
    }

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

/// Presentation state for a branch-local entry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomEntryRenderOptions {
    pub expanded: bool,
    pub expand_key: Option<String>,
}

/// First-registered renderers of branch-local entries. They are independent
/// of the active session and request context.
#[derive(Clone, Default)]
pub struct CustomEntryRenderers {
    pub(crate) renderers: std::collections::BTreeMap<String, std::sync::Arc<dyn crate::Extension>>,
}

impl CustomEntryRenderers {
    pub fn types(&self) -> impl Iterator<Item = &str> {
        self.renderers.keys().map(String::as_str)
    }

    pub fn contains(&self, custom_type: &str) -> bool {
        self.renderers.contains_key(custom_type)
    }

    /// Produce presentation rows for an entry. No registered renderer or an
    /// intentionally hidden entry returns `None`. Failures are returned so
    /// the host can decide whether to display a local error row.
    pub async fn render(
        &self,
        entry: &crate::CustomEntryRecord,
        options: &CustomEntryRenderOptions,
        timeout: std::time::Duration,
    ) -> ExtensionResult<Option<RenderedExtensionContent>> {
        use futures_util::FutureExt;
        let Some(renderer) = self.renderers.get(&entry.entry.custom_type) else {
            return Ok(None);
        };
        let callback = std::panic::AssertUnwindSafe(async {
            renderer.render_custom_entry(entry, options).await
        })
        .catch_unwind();
        let rendering = crate::runtime::time::timeout(timeout, callback)
            .await
            .map_err(|_| "custom entry renderer timed out")?
            .map_err(|_| "custom entry renderer panicked")??;
        if rendering
            .as_ref()
            .is_some_and(|content| !content.within_limits())
        {
            return Err("custom entry exceeds rendering limits".into());
        }
        Ok(rendering)
    }
}

/// The normal message content being rendered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MarkdownMessageType {
    User,
    Assistant,
    AssistantThinking,
}

/// Layout and message identity available to a display-only Markdown hook.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkdownTransformContext {
    pub message_type: MarkdownMessageType,
    pub is_streaming: bool,
    pub available_width: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// Markdown transformers in extension load order.
#[derive(Clone, Default)]
pub struct MarkdownTransformers {
    pub(crate) extensions: Vec<std::sync::Arc<dyn crate::Extension>>,
}

impl MarkdownTransformers {
    pub const MAX_MARKDOWN_BYTES: usize = 256 * 1024;

    pub fn is_empty(&self) -> bool {
        self.extensions.is_empty()
    }

    /// Transform Markdown for display. A failed or oversized result leaves the
    /// preceding Markdown available to the next transformer. A missing result
    /// means the host can render the original Markdown.
    pub async fn transform(
        &self,
        markdown: &str,
        context: &MarkdownTransformContext,
        timeout: std::time::Duration,
    ) -> Option<String> {
        use futures_util::FutureExt;

        if markdown.len() > Self::MAX_MARKDOWN_BYTES {
            return None;
        }
        let mut transformed: Option<String> = None;
        for extension in &self.extensions {
            let current = transformed.as_deref().unwrap_or(markdown);
            let callback = std::panic::AssertUnwindSafe(async {
                extension.transform_markdown(current, context).await
            })
            .catch_unwind();
            if let Ok(Ok(Ok(Some(next)))) = crate::runtime::time::timeout(timeout, callback).await
                && next.len() <= Self::MAX_MARKDOWN_BYTES
            {
                transformed = Some(next);
            }
        }
        transformed.filter(|text| text != markdown)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiComponentAction {
    pub key: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// A theme available from the current frontend. Embedded themes have no file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiThemeInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<std::path::PathBuf>,
}

/// Configure the indicator shown while the interactive agent is working.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkingIndicatorOptions {
    /// A single frame is static. Empty frames hide the indicator glyph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frames: Option<Vec<String>>,
    /// Omit or set to zero to use the default interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
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
    /// Mount a new instance of the calling extension's header renderer.
    /// Disable it to restore the built-in header.
    SetHeaderRenderer {
        enabled: bool,
    },
    /// Request another render after the mounted header's data changes.
    InvalidateHeaderRenderer,
    /// Replace the session footer with styled rows. Clear it with `None`.
    SetFooter {
        #[serde(default)]
        lines: Option<Vec<Vec<UiComponentSpan>>>,
    },
    /// Override the working message. Clear it with `None`.
    SetWorkingMessage {
        #[serde(default)]
        message: Option<String>,
    },
    SetWorkingVisible {
        visible: bool,
    },
    /// Override the working animation. Restore the default with `None`.
    SetWorkingIndicator {
        #[serde(default)]
        options: Option<WorkingIndicatorOptions>,
    },
    /// Change the collapsed thinking label. A blank label hides the block.
    /// Omit the label to restore the default.
    SetHiddenThinkingLabel {
        #[serde(default)]
        label: Option<String>,
    },
    SetTitle {
        title: String,
    },
    /// Paste text at the main editor cursor using the frontend's paste behavior.
    PasteToEditor {
        text: String,
    },
    #[serde(rename = "set_editor_text", alias = "setEditorText")]
    SetEditorText {
        text: String,
    },
    GetEditorText,
    /// List the frontend's built-in and loaded file themes.
    GetAllThemes,
    /// Read the terminal's current tool output expansion state.
    GetToolsExpanded,
    /// Change tool output expansion for the current terminal session.
    SetToolsExpanded {
        expanded: bool,
    },
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
        if let Self::SetWorkingMessage {
            message: Some(message),
        } = self
            && (message.len() > 4096 || message.chars().any(char::is_control))
        {
            return Err(
                "extension working message must be a single line of at most 4096 bytes".into(),
            );
        }
        if let Self::SetWorkingIndicator {
            options: Some(options),
        } = self
            && let Some(frames) = &options.frames
            && (frames.len() > 64
                || frames
                    .iter()
                    .any(|frame| frame.len() > 256 || frame.chars().any(char::is_control)))
        {
            return Err("extension working indicator supports 64 single-line frames of at most 256 bytes each".into());
        }
        if let Self::SetHiddenThinkingLabel { label: Some(label) } = self
            && (label.len() > 4096
                || label
                    .chars()
                    .any(|ch| ch.is_control() && ch != '\n' && ch != '\t'))
        {
            return Err("extension hidden thinking label exceeds its rendering limits".into());
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
            (Self::Confirm { .. } | Self::GetToolsExpanded, UiResponse::Confirmed(_)) => true,
            (Self::GetAllThemes, UiResponse::Themes(themes)) => {
                serde_json::to_vec(themes).is_ok_and(|value| value.len() <= 1024 * 1024)
            }
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
                | Self::GetAllThemes
                | Self::GetToolsExpanded
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UiResponse {
    Text(String),
    Confirmed(bool),
    Themes(Vec<UiThemeInfo>),
    Cancelled,
}

#[async_trait::async_trait]
pub trait ExtensionUi: Send + Sync {
    /// Mount (or clear) a renderer on behalf of an scoped extension callback.
    fn set_header_renderer(
        &self,
        _owner: &str,
        _renderer: Option<std::sync::Arc<dyn HeaderRenderer>>,
        _cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        Err("dynamic headers are unavailable in this UI".into())
    }

    /// Invalidate the header only if it belongs to this callback's extension.
    fn invalidate_header_renderer(
        &self,
        _owner: &str,
        _cancellation: &CancellationToken,
    ) -> ExtensionResult<()> {
        Err("dynamic headers are unavailable in this UI".into())
    }

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
    fn theme_list_uses_typed_responses_without_requiring_file_paths() {
        let wire = serde_json::json!({"method":"getAllThemes"});
        let request: UiRequest = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), wire);
        assert!(request.needs_response());

        let wire =
            serde_json::json!([{"name":"system"},{"name":"custom","path":"themes/custom.json"}]);
        let response: UiResponse = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&response).unwrap(), wire);
        assert!(request.accepts_response(&response));
        assert!(!request.accepts_response(&UiResponse::Confirmed(true)));
        assert!(!UiRequest::GetEditorText.accepts_response(&response));
    }

    #[test]
    fn paste_to_editor_uses_the_pi_request_without_opening_a_dialog() {
        let value = serde_json::json!({"method":"pasteToEditor","text":"a\r\nb"});
        let request: UiRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), value);
        assert!(request.validate().is_ok());
        assert!(!request.needs_response());
    }

    #[test]
    fn hidden_thinking_label_accepts_blank_and_reset_without_a_response() {
        for label in [
            serde_json::json!({}),
            serde_json::json!({"label":null}),
            serde_json::json!({"label":""}),
            serde_json::json!({"label":"Still thinking\nPlease wait"}),
        ] {
            let mut value = label;
            value["method"] = serde_json::json!("setHiddenThinkingLabel");
            let request: UiRequest = serde_json::from_value(value).unwrap();
            assert!(request.validate().is_ok());
            assert!(!request.needs_response());
        }

        for label in ["a".repeat(4097), "\x1b[31m".into()] {
            let request = UiRequest::SetHiddenThinkingLabel { label: Some(label) };
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn working_indicator_uses_pi_fields_and_validates_single_line_frames() {
        let value = serde_json::json!({
            "method": "setWorkingIndicator",
            "options": {"frames": ["a", "b"], "intervalMs": 250}
        });
        let request: UiRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), value);
        assert!(request.validate().is_ok());
        assert!(!request.needs_response());

        for value in [
            serde_json::json!({"method": "setWorkingIndicator", "options": {"frames": []}}),
            serde_json::json!({"method": "setWorkingIndicator"}),
            serde_json::json!({"method": "setWorkingMessage"}),
            serde_json::json!({"method": "setWorkingVisible", "visible": false}),
        ] {
            let request: UiRequest = serde_json::from_value(value).unwrap();
            assert!(request.validate().is_ok());
            assert!(!request.needs_response());
        }

        for frames in [vec!["x"; 65], vec!["\x1b[31mA"], vec!["a\nb"]] {
            let request = UiRequest::SetWorkingIndicator {
                options: Some(WorkingIndicatorOptions {
                    frames: Some(frames.into_iter().map(str::to_owned).collect()),
                    interval_ms: None,
                }),
            };
            assert!(request.validate().is_err());
        }
    }

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
    async fn markdown_transformers_chain_display_results_in_load_order() {
        struct Transformer(&'static str);

        #[async_trait::async_trait]
        impl crate::Extension for Transformer {
            fn id(&self) -> &str {
                self.0
            }

            fn handles_markdown_transform(&self) -> bool {
                true
            }

            async fn transform_markdown(
                &self,
                markdown: &str,
                context: &MarkdownTransformContext,
            ) -> ExtensionResult<Option<String>> {
                match self.0 {
                    "prefix" => Ok(Some(format!("prefix {markdown}"))),
                    "failure" => Err("expected error".into()),
                    "large" => Ok(Some("x".repeat(256 * 1024 + 1))),
                    _ => {
                        assert_eq!(context.message_type, MarkdownMessageType::AssistantThinking);
                        assert!(context.is_streaming);
                        assert_eq!(context.message_id, Some(12));
                        assert_eq!(context.timestamp.as_deref(), Some("2026-11-21T07:00:00Z"));
                        Ok(Some(format!("{markdown} [{}]", context.available_width)))
                    }
                }
            }
        }

        let mut registry = crate::Registry::default();
        assert!(registry.markdown_transformers().is_empty());
        for name in ["prefix", "failure", "large", "suffix"] {
            registry
                .register(std::sync::Arc::new(Transformer(name)))
                .unwrap();
        }
        let context = MarkdownTransformContext {
            message_type: MarkdownMessageType::AssistantThinking,
            is_streaming: true,
            available_width: 42,
            message_id: Some(12),
            timestamp: Some("2026-11-21T07:00:00Z".into()),
        };
        let rendered = registry
            .markdown_transformers()
            .transform("original", &context, std::time::Duration::from_secs(1))
            .await;
        assert_eq!(rendered.as_deref(), Some("prefix original [42]"));
    }

    #[tokio::test]
    async fn custom_entry_renderers_select_the_first_registration_and_distinguish_failure() {
        struct Renderer(&'static str);
        #[async_trait::async_trait]
        impl crate::Extension for Renderer {
            fn id(&self) -> &str {
                self.0
            }
            fn custom_entry_renderer_types(&self) -> Vec<&str> {
                vec!["card"]
            }
            async fn render_custom_entry(
                &self,
                entry: &crate::CustomEntryRecord,
                options: &CustomEntryRenderOptions,
            ) -> ExtensionResult<Option<RenderedExtensionContent>> {
                if !options.expanded {
                    return Ok(None);
                }
                let label = entry
                    .entry
                    .data
                    .as_ref()
                    .and_then(serde_json::Value::as_str)
                    .ok_or("card is missing its label")?;
                Ok(Some(RenderedExtensionContent {
                    lines: vec![vec![UiComponentSpan {
                        text: format!("{} {} {label}", self.0, entry.id),
                        ..Default::default()
                    }]],
                    ..Default::default()
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
        let renderers = registry.custom_entry_renderers();
        let mut entry = crate::CustomEntryRecord {
            id: 19,
            entry: crate::CustomEntry {
                custom_type: "card".into(),
                data: Some(serde_json::json!("label")),
            },
        };
        let deadline = std::time::Duration::from_secs(1);
        let mut options = CustomEntryRenderOptions::default();
        assert!(
            renderers
                .render(&entry, &options, deadline)
                .await
                .unwrap()
                .is_none()
        );
        options.expanded = true;
        assert_eq!(
            renderers
                .render(&entry, &options, deadline)
                .await
                .unwrap()
                .unwrap()
                .lines[0][0]
                .text,
            "first 19 label"
        );
        entry.entry.data = None;
        assert_eq!(
            renderers
                .render(&entry, &options, deadline)
                .await
                .unwrap_err(),
            "card is missing its label"
        );
        entry.entry.custom_type = "unknown".into();
        assert!(
            renderers
                .render(&entry, &options, deadline)
                .await
                .unwrap()
                .is_none()
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
