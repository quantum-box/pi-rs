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
            | Self::Editor { timeout, .. } => *timeout,
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
        Ok(())
    }
    pub fn accepts_response(&self, response: &UiResponse) -> bool {
        match (self, response) {
            (_, UiResponse::Cancelled) => true,
            (Self::Select { options, .. }, UiResponse::Text(value)) => options.contains(value),
            (Self::Confirm { .. }, UiResponse::Confirmed(_)) => true,
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
