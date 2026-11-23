use pi_rs_core::{AttachmentRef, Outcome, ToolResultPatch, ToolResultPatchV2};
use serde_json::json;

#[test]
fn structured_result_patches_preserve_content_and_attachments() {
    let original = Outcome::Structured {
        data: json!({"content":[{"type":"text","text":"note"},{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}]}),
        attachments: vec![AttachmentRef {
            id: "asset".into(),
            media_type: "image/png".into(),
        }],
    };
    let updated = ToolResultPatch {
        output: None,
        details: Some(json!({"source":"hook"})),
    }
    .apply(&original)
    .unwrap();
    let Outcome::StructuredWithDetails {
        data,
        attachments,
        details,
    } = &updated
    else {
        panic!("expected structured details")
    };
    assert_eq!(data["content"][1]["data"], "aW1hZ2U=");
    assert_eq!(attachments.len(), 1);
    assert_eq!(details["source"], "hook");
    let errored = ToolResultPatchV2 {
        output: None,
        details: None,
        is_error: Some(true),
    }
    .apply(&updated)
    .unwrap();
    assert!(errored.is_error());
    let Outcome::StructuredWithDetails {
        data,
        attachments,
        details,
    } = &errored
    else {
        panic!("expected structured error")
    };
    assert_eq!(data["content"][1]["data"], "aW1hZ2U=");
    assert_eq!(attachments.len(), 1);
    assert_eq!(details["source"], "hook");
    let replaced = ToolResultPatchV2 {
        output: Some("note".into()),
        details: None,
        is_error: None,
    }
    .apply(&errored)
    .unwrap();
    assert!(
        matches!(replaced, Outcome::TextResult { output, details: Some(details), is_error: true } if output == "note" && details["source"] == "hook")
    );
    assert!(
        matches!(original, Outcome::Structured { ref data, .. } if data.get("isError").is_none())
    );
}

#[test]
fn scalar_structured_error_patch_does_not_discard_data() {
    let original = Outcome::Structured {
        data: json!([1, 2]),
        attachments: vec![],
    };
    assert!(
        ToolResultPatchV2 {
            output: None,
            details: None,
            is_error: Some(true)
        }
        .apply(&original)
        .is_err()
    );
    assert!(matches!(original, Outcome::Structured { ref data, .. } if data == &json!([1, 2])));
}

#[tokio::test]
async fn structured_to_text_replacement_cannot_claim_unchanged_content() {
    struct Hook;
    #[async_trait::async_trait]
    impl pi_rs_core::Extension for Hook {
        fn id(&self) -> &str {
            "type-changing-hook"
        }
        async fn tool_result_update_with_presence(
            &self,
            _: &pi_rs_core::ToolCall,
            outcome: &Outcome,
            _: &pi_rs_core::ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            _: Option<std::sync::Arc<dyn pi_rs_core::ToolHost>>,
        ) -> pi_rs_core::ExtensionResult<(Option<Outcome>, Option<bool>)> {
            Ok((Some(Outcome::Succeeded(outcome.tool_content())), Some(true)))
        }
    }
    let original = Outcome::Structured {
        data: json!({"content":[{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}]}),
        attachments: vec![],
    };
    let call = pi_rs_core::ToolCall {
        id: "id".into(),
        name: "read".into(),
        arguments: "{}".into(),
    };
    let context = pi_rs_core::ToolContext {
        operation_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
        cancellation: pi_rs_core::CancellationToken::new(),
    };
    let (_, unchanged) =
        pi_rs_core::track_tool_result_presence(&Hook, &call, &original, &context, None, None)
            .await
            .unwrap();
    assert_eq!(unchanged, Some(false));
}
