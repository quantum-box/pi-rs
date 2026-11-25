//! Provider-facing projection helpers for durable tool results.
use crate::model::DurableToolRequest;
use pi_rs_core::{Error, ImageContent, Message, Result};
use serde_json::Value;
use std::collections::BTreeMap;

/// Host-supplied model input descriptors; unknown models retain image support.
#[derive(Clone, Default)]
pub struct ModelInputs(BTreeMap<String, crate::images::ModelImageProfile>);
impl ModelInputs {
    pub fn insert(&mut self, model: impl Into<String>, descriptor: &Value) -> Result<()> {
        let model = model.into();
        let inputs = descriptor
            .get("input")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Config("model descriptor requires input array".into()))?;
        if model.is_empty()
            || inputs
                .iter()
                .any(|v| !matches!(v.as_str(), Some("text" | "image")))
        {
            return Err(Error::Config("invalid model input descriptor".into()));
        }
        let mut limits = crate::images::ImageLimitsCatalog::default();
        limits
            .insert(model.clone(), descriptor)
            .map_err(Error::Config)?;
        let profile = crate::images::ModelImageProfile {
            supports_images: inputs.iter().any(|v| v == "image"),
            limits: limits.limits(&model),
        };
        self.0.insert(model, profile);
        Ok(())
    }
    pub fn profile(
        &self,
        adapter_id: &str,
        model: &str,
    ) -> Option<crate::images::ModelImageProfile> {
        self.0
            .get(model)
            .copied()
            .or_else(|| crate::images::builtin_model_image_profile(adapter_id, model))
    }
}
/// Wrap an existing request without assuming any legacy tool status.
pub fn wrap_durable(request: &crate::model::DurableRequest) -> crate::model::ProviderResultRequest {
    crate::model::ProviderResultRequest {
        request: crate::model::DurableToolRequest {
            request: request.clone(),
            tool_blocks: Default::default(),
        },
        tool_errors: Default::default(),
    }
}
pub fn wrap_plain(request: &pi_rs_core::ModelRequest) -> crate::model::ProviderResultRequest {
    wrap_durable(&crate::model::DurableRequest {
        request: request.clone(),
        blocks: Default::default(),
    })
}
/// Apply the same non-vision projection as the runtime to an isolated request.
/// This is idempotent when the runtime has already projected the request.
pub fn project_request(
    request: &crate::model::ProviderResultRequest,
    profile: Option<crate::images::ModelImageProfile>,
) -> crate::model::ProviderResultRequest {
    let mut request = request.clone();
    if profile.is_some_and(|p| !p.supports_images) {
        let durable = &mut request.request;
        crate::images::project_nonvision_request(
            &mut durable.request.request,
            &mut durable.request.blocks,
            &mut durable.tool_blocks,
        );
    }
    request
}
#[derive(Clone, Debug)]
pub enum Block {
    Text(String),
    Image {
        image: ImageContent,
        detail: Option<String>,
    },
}
/// Read exact blocks or the legacy string representation. Unknown metadata is
/// rejected; no provider request is allowed to discard it silently.
pub fn blocks(
    request: &DurableToolRequest,
    index: usize,
    allow_detail: bool,
) -> Result<Vec<Block>> {
    let Message::Tool { content, .. } = request
        .request
        .request
        .messages
        .get(index)
        .ok_or_else(|| Error::Config("invalid tool result index".into()))?
    else {
        return Err(Error::Config("invalid tool result".into()));
    };
    if let Some(binding) = request.tool_blocks.get(&index) {
        let mut result = Vec::new();
        for value in &binding.blocks {
            let obj = value
                .as_object()
                .ok_or_else(|| Error::Config("invalid tool block".into()))?;
            match value["type"].as_str() {
                Some("text") if obj.len() == 2 => result.push(Block::Text(
                    value["text"]
                        .as_str()
                        .ok_or_else(|| Error::Config("invalid tool text".into()))?
                        .to_owned(),
                )),
                Some("image")
                    if obj.keys().all(|k| {
                        ["type", "data", "mimeType"].contains(&k.as_str())
                            || (allow_detail && k == "detail")
                    }) =>
                {
                    let image = ImageContent {
                        data: value["data"]
                            .as_str()
                            .ok_or_else(|| Error::Config("invalid image".into()))?
                            .into(),
                        mime_type: value["mimeType"]
                            .as_str()
                            .ok_or_else(|| Error::Config("invalid image".into()))?
                            .into(),
                    };
                    image.validate()?;
                    let detail = match obj.get("detail") {
                        None => None,
                        Some(d) if matches!(d.as_str(), Some("low" | "high" | "auto")) => {
                            Some(d.as_str().unwrap().into())
                        }
                        _ => return Err(Error::Config("invalid image detail".into())),
                    };
                    result.push(Block::Image { image, detail });
                }
                _ => return Err(Error::Config("unsupported tool block metadata".into())),
            }
        }
        Ok(result)
    } else {
        let (text, images) = pi_rs_core::extract_tool_images(content.clone());
        let mut result = vec![Block::Text(text)];
        for image in images {
            image.validate()?;
            result.push(Block::Image {
                image,
                detail: None,
            });
        }
        Ok(result)
    }
}
/// Match upstream's replacement of each contiguous run of images.
pub fn omit_images(blocks: Vec<Block>, placeholder: &str) -> Vec<Block> {
    let mut result = Vec::new();
    let mut previous_placeholder = false;
    for block in blocks {
        match block {
            Block::Image { .. } => {
                if !previous_placeholder {
                    result.push(Block::Text(placeholder.into()));
                }
                previous_placeholder = true;
            }
            Block::Text(text) => {
                previous_placeholder = text == placeholder;
                result.push(Block::Text(text));
            }
        }
    }
    result
}
pub fn has_text(text: &str) -> bool {
    !text
        .trim_matches(|c: char| (c.is_whitespace() && c != '\u{0085}') || c == '\u{feff}')
        .is_empty()
}
