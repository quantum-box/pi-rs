use pi_rs_durable::{
    extension::{Context, Extension, Hook, Memo, Registry, ToolBlockResult, ToolCall},
    images::{
        Dimensions, ImageLimits, ImageProcessor, PreparedImage, Resize, create_fit_images_extension,
    },
    runtime::{DiagnosticSeverity, ToolDiagnostic},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

struct NoMemo;
#[async_trait::async_trait]
impl Memo for NoMemo {
    async fn get(&self, _: &str) -> Result<Option<Value>, String> {
        Ok(None)
    }
    async fn get_or_insert(&self, _: &str, value: Value) -> Result<Value, String> {
        Ok(value)
    }
}
struct Processor(Mutex<Vec<ImageLimits>>);
#[async_trait::async_trait]
impl ImageProcessor for Processor {
    async fn prepare(
        &self,
        bytes: &[u8],
        _: &str,
        limits: ImageLimits,
    ) -> Result<Option<PreparedImage>, String> {
        self.0.lock().unwrap().push(limits);
        if bytes == b"bad" {
            return Ok(None);
        }
        Ok(Some(PreparedImage {
            data: "bmV3".into(),
            mime_type: "image/png".into(),
            converted_from: None,
            resized: Some(Resize {
                from: Dimensions {
                    width: 400,
                    height: 100,
                },
                to: Dimensions {
                    width: 100,
                    height: 25,
                },
            }),
        }))
    }
}
struct Observe(Arc<Mutex<Vec<Vec<Value>>>>);
#[async_trait::async_trait]
impl Hook for Observe {
    async fn after_tool_blocks(
        &self,
        _: &ToolCall,
        result: &ToolBlockResult,
        _: bool,
        _: &Context<'_>,
    ) -> Result<Option<ToolBlockResult>, String> {
        self.0.lock().unwrap().push(result.output.clone());
        Ok(None)
    }
}
struct Legacy;
#[async_trait::async_trait]
impl Hook for Legacy {
    async fn after_tool(
        &self,
        _: &ToolCall,
        value: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Ok(Some(json!(format!("{}!", value.as_str().unwrap()))))
    }
}
struct Invalid;
#[async_trait::async_trait]
impl Hook for Invalid {
    async fn after_tool_blocks(
        &self,
        _: &ToolCall,
        result: &ToolBlockResult,
        _: bool,
        _: &Context<'_>,
    ) -> Result<Option<ToolBlockResult>, String> {
        let mut changed = result.clone();
        changed.output = vec![json!({"type":"image"})];
        Ok(Some(changed))
    }
}
fn ext(name: &str, hook: Arc<dyn Hook>) -> Extension {
    Extension {
        name: name.into(),
        hooks: vec![hook],
        ..Default::default()
    }
}
fn result() -> ToolBlockResult {
    serde_json::from_value(json!({
        "output": [{"type":"text", "text":"before"}, {"type":"image", "data":"b2xk", "mimeType":"image/png"}, {"type":"image", "data":"YmFk", "mimeType":"image/png"}, {"type":"text", "text":"after"}],
        "diagnostics": [{"severity":"info", "message":"existing"}], "details": {"keep":true}
    })).unwrap()
}
fn call() -> ToolCall {
    ToolCall {
        name: "picture".into(),
        arguments: json!({}),
    }
}

#[tokio::test]
async fn hooks_fit_in_order_preserve_metadata_and_exclude_nested() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let processor = Arc::new(Processor(Mutex::new(Vec::new())));
    let mut registry = Registry::default();
    registry
        .install(ext("before", Arc::new(Observe(observations.clone()))))
        .unwrap();
    registry
        .install(create_fit_images_extension(
            processor.clone(),
            Arc::new(|snapshot| ImageLimits {
                max_width: snapshot["width"].as_u64().unwrap() as u32,
                ..ImageLimits::default()
            }),
        ))
        .unwrap();
    registry
        .install(ext("after", Arc::new(Observe(observations.clone()))))
        .unwrap();
    let selected = registry.snapshot().resolve(None);
    let snapshot = json!({"width":100});
    let ctx = Context {
        snapshot: &snapshot,
        memo: &NoMemo,
    };
    let original = result();
    let (fitted, reports) = selected
        .after_tool_blocks(&call(), original.clone(), false, &ctx)
        .await
        .unwrap();
    assert!(reports.is_empty());
    assert_eq!(
        fitted.output,
        vec![
            original.output[0].clone(),
            json!({"type":"image", "data":"bmV3", "mimeType":"image/png"}),
            original.output[3].clone()
        ]
    );
    assert_eq!(fitted.extra, original.extra);
    assert_eq!(fitted.diagnostics[0], original.diagnostics[0]);
    assert_eq!(fitted.diagnostics[1], ToolDiagnostic { severity: DiagnosticSeverity::Info, code: Some("image".into()), message: "Resized from 400x100 to 100x25. Multiply coordinates by 4.00 to map them to the original. An image (image/png) cannot be prepared for the model and was left out.".into() });
    let seen = observations.lock().unwrap();
    assert_eq!(seen[0], original.output);
    assert_eq!(seen[1], fitted.output);
    drop(seen);
    assert_eq!(
        processor.0.lock().unwrap().as_slice(),
        &[ImageLimits {
            max_width: 100,
            ..ImageLimits::default()
        }; 2]
    );
    let encoded = serde_json::to_vec(&fitted).unwrap();
    assert_eq!(
        serde_json::from_slice::<ToolBlockResult>(&encoded).unwrap(),
        fitted
    );
    let (nested, _) = selected
        .after_tool_blocks(&call(), original.clone(), true, &ctx)
        .await
        .unwrap();
    assert_eq!(nested, original);
    assert_eq!(processor.0.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn bridges_legacy_text_and_rejects_invalid_replacements() {
    let mut registry = Registry::default();
    registry.install(ext("legacy", Arc::new(Legacy))).unwrap();
    registry.install(ext("invalid", Arc::new(Invalid))).unwrap();
    let selected = registry.snapshot().resolve(None);
    let ctx = Context {
        snapshot: &Value::Null,
        memo: &NoMemo,
    };
    let mut value = result();
    value.output = vec![json!({"type":"text", "text":"hello"})];
    let (out, reports) = selected
        .after_tool_blocks(&call(), value.clone(), false, &ctx)
        .await
        .unwrap();
    assert_eq!(out.output, vec![json!({"type":"text", "text":"hello!"})]);
    assert_eq!(out.extra, value.extra);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].extension, "invalid");
    value.output = vec![json!({"type":"image"})];
    assert!(
        selected
            .after_tool_blocks(&call(), value, false, &ctx)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unchanged_images_and_nested_calls_do_not_add_diagnostics() {
    struct Identity;
    #[async_trait::async_trait]
    impl ImageProcessor for Identity {
        async fn prepare(
            &self,
            _: &[u8],
            _: &str,
            _: ImageLimits,
        ) -> Result<Option<PreparedImage>, String> {
            Ok(Some(PreparedImage {
                data: "b2xk".into(),
                mime_type: "image/png".into(),
                resized: None,
                converted_from: None,
            }))
        }
    }
    let mut registry = Registry::default();
    registry
        .install(create_fit_images_extension(
            Arc::new(Identity),
            Arc::new(|_| ImageLimits::default()),
        ))
        .unwrap();
    let selected = registry.snapshot().resolve(None);
    let ctx = Context {
        snapshot: &Value::Null,
        memo: &NoMemo,
    };
    let mut original = result();
    original.output.remove(2);
    original.output[1]["detail"] = json!("high");
    let (out, reports) = selected
        .after_tool_blocks(&call(), original.clone(), false, &ctx)
        .await
        .unwrap();
    assert!(reports.is_empty());
    assert_eq!(out, original);
    let mut invalid = original;
    invalid.extra.insert("output".into(), json!([]));
    assert!(
        selected
            .after_tool_blocks(&call(), invalid, false, &ctx)
            .await
            .is_err()
    );
}
