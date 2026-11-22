//! Rendering callbacks for extension tool calls and results.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{Extension, ExtensionResult, RenderedExtensionRows, extension::RegisteredTool};

/// Arguments and view state shared by the call and result renderers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ToolRenderContext {
    pub tool_name: String,
    pub tool_call_id: String,
    pub args: serde_json::Value,
    pub cwd: PathBuf,
    pub execution_started: bool,
    pub args_complete: bool,
    pub expanded: bool,
    pub show_images: bool,
    pub output_pad: usize,
    pub expand_key: Option<String>,
    /// Available layout width in host columns, when known.
    #[serde(default)]
    pub width: Option<usize>,
    pub result: Option<ToolRenderResult>,
}

impl ToolRenderContext {
    pub fn new(
        tool_name: impl Into<String>,
        tool_call_id: impl Into<String>,
        args: serde_json::Value,
        cwd: impl Into<PathBuf>,
    ) -> Self {
        Self {
            tool_name: tool_name.into(),
            tool_call_id: tool_call_id.into(),
            args,
            cwd: cwd.into(),
            execution_started: false,
            args_complete: false,
            expanded: false,
            show_images: false,
            output_pad: 0,
            expand_key: None,
            width: None,
            result: None,
        }
    }
}

/// A partial update or completed result as seen by the presentation host.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolRenderResult {
    pub content: serde_json::Value,
    pub details: Option<serde_json::Value>,
    pub is_partial: bool,
    pub is_error: bool,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ToolRenderShell {
    #[default]
    Default,
    #[serde(rename = "self")]
    SelfFramed,
}

/// Callback for one section of the tool display.
#[async_trait::async_trait]
pub trait ToolSectionRenderer: Send + Sync {
    async fn render(
        &self,
        context: &ToolRenderContext,
    ) -> ExtensionResult<Option<RenderedExtensionRows>>;
}

#[derive(Clone, Default)]
pub struct ToolRenderers {
    pub shell: ToolRenderShell,
    pub call: Option<Arc<dyn ToolSectionRenderer>>,
    pub result: Option<Arc<dyn ToolSectionRenderer>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderedTool {
    pub shell: ToolRenderShell,
    pub call: Option<RenderedExtensionRows>,
    pub result: Option<RenderedExtensionRows>,
}

impl ToolRenderers {
    /// Render the call before the result with a deadline for each callback.
    /// The host can fall back for any section whose callback is absent or fails.
    pub async fn render(&self, context: &ToolRenderContext, timeout: Duration) -> RenderedTool {
        async fn section(
            renderer: Option<&dyn ToolSectionRenderer>,
            context: &ToolRenderContext,
            timeout: Duration,
        ) -> Option<RenderedExtensionRows> {
            use futures_util::FutureExt;

            let renderer = renderer?;
            let callback = std::panic::AssertUnwindSafe(async { renderer.render(context).await })
                .catch_unwind();
            crate::runtime::time::timeout(timeout, callback)
                .await
                .ok()?
                .ok()?
                .ok()?
                .filter(RenderedExtensionRows::within_limits)
        }

        let call = section(self.call.as_deref(), context, timeout).await;
        let result_renderer = context.result.as_ref().and(self.result.as_deref());
        let result = section(result_renderer, context, timeout).await;
        RenderedTool {
            shell: self.shell,
            call,
            result,
        }
    }
}

/// Presentation-only snapshot. Resolve for any tool name, including historical
/// calls and tools supplied by the host.
#[derive(Clone, Default)]
pub struct ToolRendererRegistry {
    pub(crate) extensions: Vec<Arc<dyn Extension>>,
    pub(crate) tools: BTreeMap<String, Arc<RegisteredTool>>,
}

impl ToolRendererRegistry {
    /// Extensions run in registration order. Delegating past the last
    /// extension combines the registered tool's renderers with the host's
    /// defaults for any sections the tool leaves unspecified.
    pub fn resolve(
        &self,
        tool_name: &str,
        host: &dyn Fn() -> Option<ToolRenderers>,
    ) -> Option<ToolRenderers> {
        fn resolve_next(
            extensions: &[Arc<dyn Extension>],
            tool_name: &str,
            base: &dyn Fn() -> Option<ToolRenderers>,
        ) -> Option<ToolRenderers> {
            let Some((extension, remaining)) = extensions.split_first() else {
                return base();
            };
            let next = || resolve_next(remaining, tool_name, base);
            extension.resolve_tool_renderers(tool_name, &next)
        }

        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            resolve_next(&self.extensions, tool_name, &|| {
                let Some(mut owner) = self
                    .tools
                    .get(tool_name)
                    .and_then(|tool| tool.tool.tool_renderers())
                else {
                    return host();
                };
                if (owner.call.is_none() || owner.result.is_none())
                    && let Some(defaults) = host()
                {
                    owner.call = owner.call.or(defaults.call);
                    owner.result = owner.result.or(defaults.result);
                }
                Some(owner)
            })
        }))
        .unwrap_or_default()
    }
}

#[cfg(all(test, not(all(target_arch = "wasm32", target_os = "unknown"))))]
mod tool_renderer_tests {
    use super::*;
    use crate::UiComponentSpan;

    struct Label(&'static str);

    #[async_trait::async_trait]
    impl ToolSectionRenderer for Label {
        async fn render(
            &self,
            context: &ToolRenderContext,
        ) -> ExtensionResult<Option<RenderedExtensionRows>> {
            if self.0 == "broken" {
                return Err("renderer failed".into());
            }
            if self.0 == "waiting" {
                assert!(context.result.as_ref().unwrap().is_partial);
                return std::future::pending().await;
            }
            Ok(Some(RenderedExtensionRows {
                lines: vec![vec![UiComponentSpan {
                    text: format!("{}: {}", self.0, context.args["path"]),
                    color: Some("accent".into()),
                    ..Default::default()
                }]],
                ..Default::default()
            }))
        }
    }

    struct Owner;

    #[async_trait::async_trait]
    impl crate::Tool for Owner {
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: "owned".into(),
                description: "Example".into(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }

        fn tool_renderers(&self) -> Option<ToolRenderers> {
            Some(ToolRenderers {
                call: Some(Arc::new(Label("owner"))),
                ..Default::default()
            })
        }

        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            panic!("rendering must not execute the tool");
        }
    }

    #[async_trait::async_trait]
    impl crate::Extension for Owner {
        fn id(&self) -> &str {
            "owner"
        }

        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(Self)]
        }
    }

    struct Resolver(&'static str);

    #[async_trait::async_trait]
    impl crate::Extension for Resolver {
        fn id(&self) -> &str {
            self.0
        }

        fn resolve_tool_renderers(
            &self,
            name: &str,
            next: &dyn Fn() -> Option<ToolRenderers>,
        ) -> Option<ToolRenderers> {
            match (self.0, name) {
                ("first", "failed") => Some(ToolRenderers {
                    result: Some(Arc::new(Label("broken"))),
                    ..next().unwrap()
                }),
                ("first", "hidden") => None,
                ("second", "hidden") => Some(ToolRenderers::default()),
                ("second", "owned") => Some(ToolRenderers {
                    shell: ToolRenderShell::SelfFramed,
                    ..next().unwrap()
                }),
                ("second", "external") => Some(ToolRenderers {
                    result: Some(Arc::new(Label("waiting"))),
                    ..next().unwrap()
                }),
                _ => next(),
            }
        }
    }

    fn context(name: &str) -> ToolRenderContext {
        let mut context =
            ToolRenderContext::new(name, "call", serde_json::json!({"path": "example"}), ".");
        context.execution_started = true;
        context.args_complete = true;
        context.output_pad = 1;
        context.result = Some(ToolRenderResult {
            content: serde_json::json!([{"type": "text", "text": "example"}]),
            details: None,
            is_partial: true,
            is_error: false,
            duration_ms: None,
        });
        context
    }

    #[tokio::test]
    async fn tool_renderers_wrap_owner_and_host_and_keep_fallbacks_independent() {
        let mut registry = crate::Registry::default();
        registry.register(Arc::new(Resolver("first"))).unwrap();
        registry.register(Arc::new(Resolver("second"))).unwrap();
        registry.register(Arc::new(Owner)).unwrap();
        let renderers = registry.tool_renderers();
        let host = || {
            Some(ToolRenderers {
                call: Some(Arc::new(Label("host"))),
                result: Some(Arc::new(Label("host result"))),
                ..Default::default()
            })
        };
        assert!(renderers.resolve("hidden", &host).is_none());
        let timeout = Duration::from_millis(15);
        let rendered = renderers
            .resolve("owned", &host)
            .unwrap()
            .render(&context("owned"), timeout)
            .await;
        assert_eq!(rendered.shell, ToolRenderShell::SelfFramed);
        assert_eq!(
            rendered.call.unwrap().lines[0][0].text,
            "owner: \"example\""
        );
        assert_eq!(
            rendered.result.unwrap().lines[0][0].text,
            "host result: \"example\""
        );

        let rendered = renderers
            .resolve("failed", &host)
            .unwrap()
            .render(&context("failed"), timeout)
            .await;
        assert!(rendered.call.is_some());
        assert!(rendered.result.is_none());

        let mut context = context("external");
        let external = renderers.resolve("external", &host).unwrap();
        let rendered = external.render(&context, timeout).await;
        assert_eq!(rendered.call.unwrap().lines[0][0].text, "host: \"example\"");
        assert!(rendered.result.is_none());
        context.result = None;
        assert!(
            external
                .render(&context, Duration::ZERO)
                .await
                .result
                .is_none()
        );
    }

    #[tokio::test]
    async fn call_renderer_finishes_before_result_reads_shared_state() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Shared {
            call: bool,
            ready: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl ToolSectionRenderer for Shared {
            async fn render(
                &self,
                _: &ToolRenderContext,
            ) -> ExtensionResult<Option<RenderedExtensionRows>> {
                if self.call {
                    tokio::task::yield_now().await;
                    self.ready.store(true, Ordering::Relaxed);
                }
                Ok(self
                    .ready
                    .load(Ordering::Relaxed)
                    .then(RenderedExtensionRows::default))
            }
        }

        let ready = Arc::new(AtomicBool::new(false));
        let renderers = ToolRenderers {
            call: Some(Arc::new(Shared {
                call: true,
                ready: ready.clone(),
            })),
            result: Some(Arc::new(Shared { call: false, ready })),
            ..Default::default()
        };
        let rendered = renderers
            .render(&context("tool"), Duration::from_secs(1))
            .await;
        assert!(rendered.call.is_some());
        assert!(rendered.result.is_some());
    }
}
