//! Opt-in Anthropic Messages adapter. Encoding and stream handling are adapted
//! from pi-rs-anthropic under the repository's MIT license.
mod encode;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, ModelApi, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Result, ToolCall,
    ToolCallDelta, Usage,
};
use pi_rs_durable::model::{
    DurableRequest, DurableToolRequest, ProviderAdapter, ProviderAttempt, ProviderResultRequest,
};
use reqwest::{Client, Url};
use serde_json::Value;
use std::{collections::BTreeMap, net::IpAddr, time::Duration};

const LIMIT: usize = 4 * 1024 * 1024;
const SCOPE: &str = "anthropic";

/// Explicitly installed API-key adapter. The endpoint and hooks must be trusted.
pub struct AnthropicAdapter {
    client: Client,
    endpoint: Url,
    key: String,
    model_inputs: pi_rs_durable::tool_projection::ModelInputs,
}
impl AnthropicAdapter {
    /// Override the bundled model image profile using an upstream-shaped descriptor.
    pub fn set_model_descriptor(
        &mut self,
        model: impl Into<String>,
        descriptor: &Value,
    ) -> Result<()> {
        self.model_inputs.insert(model, descriptor)
    }
    pub fn new(endpoint: &str, key: String) -> Result<Self> {
        let url =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid Anthropic endpoint".into()))?;
        let local = url
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if url.host().is_none()
            || !(url.scheme() == "https" || (url.scheme() == "http" && local))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::Config("invalid Anthropic endpoint".into()));
        }
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::Config("invalid Anthropic API key".into()));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| Error::Config("cannot initialize Anthropic client".into()))?;
        Ok(Self {
            client,
            endpoint: url,
            key,
            model_inputs: Default::default(),
        })
    }
}

fn result_payload(
    request: &ProviderResultRequest,
    profile: Option<pi_rs_durable::images::ModelImageProfile>,
) -> Result<Value> {
    use pi_rs_core::Message;
    use pi_rs_durable::tool_projection::{self, Block};
    use serde_json::json;
    request.validate_bindings()?;
    let request = tool_projection::project_request(request, profile);
    let tool = &request.request;
    let mut body = encode::encode_request_with_blocks(
        tool.request.request.clone(),
        SCOPE,
        Some(&tool.request),
    )?;
    let mut by_id = BTreeMap::new();
    let mut names = BTreeMap::new();
    for msg in &tool.request.request.messages {
        if let Message::Assistant { calls, .. } = msg {
            for call in calls {
                names.insert(call.id.clone(), call.name.clone());
            }
        }
    }
    for (index, msg) in tool.request.request.messages.iter().enumerate() {
        let Message::Tool { call_id, .. } = msg else {
            continue;
        };
        let blocks = tool_projection::blocks(tool, index, false)?;
        let status = request.tool_errors.get(&index).copied();
        let mut extras = Vec::new();
        let content_blocks = if status == Some(true)
            && blocks.iter().any(|b| matches!(b, Block::Image { .. }))
        {
            let mut images = Vec::new();
            for b in &blocks {
                if let Block::Image { image, .. } = b {
                    images.push(json!({"type":"image","source":{"type":"base64","media_type":image.mime_type,"data":image.data}}));
                }
            }
            let name = names
                .get(call_id)
                .ok_or_else(|| Error::Config("tool result has no preceding call".into()))?;
            extras.push(json!({"type":"text","text":format!("Images from the error result of tool call {call_id} ({name}):")}));
            extras.extend(images);
            tool_projection::omit_images(blocks, "(image attached after the tool results)")
        } else {
            blocks
        };
        let has_images = content_blocks
            .iter()
            .any(|b| matches!(b, Block::Image { .. }));
        let content = if !has_images {
            json!(
                content_blocks
                    .iter()
                    .filter_map(|b| match b {
                        Block::Text(text) => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        } else {
            let mut result = Vec::new();
            if !content_blocks.iter().any(|b| matches!(b, Block::Text(_))) {
                result.push(json!({"type":"text","text":"(see attached image)"}));
            }
            for b in content_blocks {
                result.push(match b { Block::Text(text) => json!({"type":"text","text":text}), Block::Image { image, .. } => json!({"type":"image","source":{"type":"base64","media_type":image.mime_type,"data":image.data}}) });
            }
            json!(result)
        };
        let mut result = json!({"type":"tool_result","tool_use_id":call_id,"content":content});
        if let Some(status) = status {
            result["is_error"] = json!(status);
        }
        if by_id.insert(call_id.clone(), (result, extras)).is_some() {
            return Err(Error::Config("ambiguous tool result call ID".into()));
        }
    }
    let mut used = std::collections::BTreeSet::new();
    for message in body["messages"]
        .as_array_mut()
        .ok_or_else(|| Error::Config("invalid messages".into()))?
    {
        let Some(blocks) = message["content"].as_array_mut() else {
            continue;
        };
        let mut out = Vec::new();
        let mut extras = Vec::new();
        for block in std::mem::take(blocks) {
            if block["type"] == "tool_result" {
                let id = block["tool_use_id"]
                    .as_str()
                    .ok_or_else(|| Error::Config("invalid tool result ID".into()))?;
                let (result, after) = by_id
                    .get(id)
                    .ok_or_else(|| Error::Config("unknown tool result".into()))?;
                if !used.insert(id.to_owned()) {
                    return Err(Error::Config("ambiguous tool result call ID".into()));
                }
                out.push(result.clone());
                extras.extend(after.iter().cloned());
            } else {
                out.append(&mut extras);
                out.push(block);
            }
        }
        out.append(&mut extras);
        *blocks = out;
    }
    if used.len() != by_id.len() {
        return Err(Error::Config("missing tool result".into()));
    }
    Ok(body)
}
#[async_trait::async_trait]
impl ProviderAdapter for AnthropicAdapter {
    fn image_profile(&self, model: &str) -> Option<pi_rs_durable::images::ModelImageProfile> {
        self.model_inputs.profile(self.id(), model)
    }
    fn supports_tool_result_metadata(&self) -> bool {
        true
    }
    async fn prepare_with_result_metadata(
        &self,
        _: &ProviderAttempt,
        request: &ProviderResultRequest,
    ) -> Result<Value> {
        result_payload(
            request,
            self.image_profile(&request.request.request.request.model),
        )
    }
    fn validate_with_result_metadata(
        &self,
        attempt: &ProviderAttempt,
        request: &ProviderResultRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate(attempt, &request.request.request.request, body)?;
        if body.get("messages")
            != result_payload(
                request,
                self.image_profile(&request.request.request.request.model),
            )?
            .get("messages")
        {
            return Err(Error::Config(
                "provider hook changed durable input or status".into(),
            ));
        }
        Ok(())
    }
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &ProviderAttempt,
        request: &DurableToolRequest,
    ) -> Result<Value> {
        result_payload(
            &ProviderResultRequest {
                request: request.clone(),
                tool_errors: Default::default(),
            },
            self.image_profile(&request.request.request.model),
        )
    }
    fn validate_with_tool_blocks(
        &self,
        attempt: &ProviderAttempt,
        request: &DurableToolRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate_with_result_metadata(
            attempt,
            &ProviderResultRequest {
                request: request.clone(),
                tool_errors: Default::default(),
            },
            body,
        )
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    async fn prepare_durable(
        &self,
        _: &ProviderAttempt,
        request: &DurableRequest,
    ) -> Result<Value> {
        result_payload(
            &pi_rs_durable::tool_projection::wrap_durable(request),
            self.image_profile(&request.request.model),
        )
    }
    fn validate_durable(
        &self,
        attempt: &ProviderAttempt,
        request: &DurableRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate(attempt, &request.request, body)?;
        if (!request.blocks.is_empty()
            || request
                .request
                .messages
                .iter()
                .any(|m| matches!(m, pi_rs_core::Message::Tool { .. })))
            && body.get("messages")
                != result_payload(
                    &pi_rs_durable::tool_projection::wrap_durable(request),
                    self.image_profile(&request.request.model),
                )?
                .get("messages")
        {
            return Err(Error::Config("provider hook changed durable input".into()));
        }
        Ok(())
    }
    fn id(&self) -> &str {
        "anthropic-messages/v1"
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value> {
        result_payload(
            &pi_rs_durable::tool_projection::wrap_plain(request),
            self.image_profile(&request.model),
        )
    }
    fn validate(&self, _: &ProviderAttempt, request: &ModelRequest, body: &Value) -> Result<()> {
        let fail = || Error::Config("invalid or unauthorized Anthropic request body".into());
        let obj = body.as_object().ok_or_else(fail)?;
        let expected = encode::encode_request(request.clone(), SCOPE).or_else(|_| {
            let mut augmented = request.clone();
            augmented.messages.push(pi_rs_core::Message::User {
                content: " ".into(),
            });
            encode::encode_request(augmented, SCOPE)
        })?;
        if obj.keys().any(|k| {
            ![
                "model",
                "messages",
                "system",
                "max_tokens",
                "stream",
                "tools",
                "temperature",
                "top_p",
                "top_k",
                "stop_sequences",
            ]
            .contains(&k.as_str())
        }) || body["model"] != request.model
            || body["stream"] != true
            || !body["messages"].is_array()
            || body["messages"].as_array().is_none_or(|messages| {
                messages.is_empty()
                    || messages.iter().any(|message| {
                        !matches!(message["role"].as_str(), Some("user" | "assistant"))
                            || !(message["content"].is_string()
                                || message["content"].as_array().is_some_and(|blocks| {
                                    !blocks.is_empty() && blocks.iter().all(Value::is_object)
                                }))
                    })
            })
            || obj
                .get("temperature")
                .is_some_and(|v| v.as_f64().is_none_or(|n| !(0.0..=1.0).contains(&n)))
            || obj
                .get("top_p")
                .is_some_and(|v| v.as_f64().is_none_or(|n| !(0.0..=1.0).contains(&n)))
            || obj.get("top_k").is_some_and(|v| v.as_u64().is_none())
            || obj
                .get("stop_sequences")
                .is_some_and(|v| v.as_array().is_none_or(|a| !a.iter().all(Value::is_string)))
            || body["max_tokens"]
                .as_u64()
                .is_none_or(|n| n == 0 || n > u64::from(request.max_output_tokens))
            || obj.get("tools") != expected.get("tools")
            || serde_json::to_vec(body)?.len() > LIMIT
        {
            return Err(fail());
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        attempt: &ProviderAttempt,
        request: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream> {
        self.validate(attempt, request, &body)?;
        let response = self
            .client
            .post(self.endpoint.clone())
            .header("x-api-key", &self.key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("Anthropic connection failed".into()))?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("Anthropic HTTP status {status}");
            return Err(
                if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
                    Error::RetryableModel(message)
                } else {
                    Error::Model(message)
                },
            );
        }
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected Anthropic SSE response".into()));
        }
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0usize;
            let mut events = response.bytes_stream().map(move |chunk| { let c = chunk.map_err(|_| Error::RetryableModel("Anthropic stream interrupted".into()))?; size = size.saturating_add(c.len()); if size > LIMIT { return Err(Error::Model("Anthropic stream exceeds size limit".into())); } Ok(c) }).eventsource();
            let mut blocks = BTreeMap::<u64, Block>::new();
            let mut started = false; let mut stop = None::<String>; let mut input = None::<u64>; let mut output = None::<u64>; let mut cached = 0u64; let mut written = 0u64;
            let mut metadata = ModelMetadata { provider_api:Some(ModelApi::AnthropicMessages), ..Default::default() };
            while let Some(event) = events.next().await {
                let event = event.map_err(|e| match e { eventsource_stream::EventStreamError::Transport(e) => e, _ => Error::RetryableModel("broken Anthropic SSE stream".into()) })?;
                let v:Value = serde_json::from_str(&event.data).map_err(|_| Error::Model("malformed Anthropic SSE event".into()))?;
                match v["type"].as_str() {
                    Some("message_start") if !started => { started = true; let m=&v["message"]; input=m["usage"]["input_tokens"].as_u64(); cached=m["usage"]["cache_read_input_tokens"].as_u64().unwrap_or(0); written=m["usage"]["cache_creation_input_tokens"].as_u64().unwrap_or(0); metadata.cached_input_tokens=Some(cached); metadata.cache_creation_input_tokens=Some(written); metadata.provider_request_id=m["id"].as_str().filter(|id| id.len()<=128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))).map(str::to_owned); }
                    Some("content_block_start") if started && stop.is_none() => { let i=index(&v)?; if blocks.len()>=256 || blocks.contains_key(&i) { Err(invalid())?; } let b=&v["content_block"]; let block=match b["type"].as_str() { Some("text") => { let t=b["text"].as_str().ok_or_else(invalid)?.to_owned(); if !t.is_empty() { yield ModelEvent::Text(t.clone()); } Block::Text { done:false } }, Some("tool_use") => { let id=field(b,"id")?.to_owned(); let name=field(b,"name")?.to_owned(); if id.is_empty() || name.is_empty() { Err(invalid())?; } let initial=b["input"].as_object().ok_or_else(invalid)?.clone(); yield ModelEvent::ToolCallDelta(ToolCallDelta { index:i,id:id.clone(),name:name.clone(),arguments: if initial.is_empty() { String::new() } else { Value::Object(initial.clone()).to_string() } }); Block::Tool { id,name,initial:Value::Object(initial), fragments:String::new(),done:false } }, _ => Err(Error::Model("unsupported Anthropic content block".into()))? }; blocks.insert(i,block); }
                    Some("content_block_delta") if started && stop.is_none() => { let i=index(&v)?; let d=&v["delta"]; match (blocks.get_mut(&i), d["type"].as_str()) { (Some(Block::Text { done:false }), Some("text_delta")) => { let t=field(d,"text")?; yield ModelEvent::Text(t.into()); }, (Some(Block::Tool { initial, fragments, done:false,.. }), Some("input_json_delta")) => { let t=field(d,"partial_json")?; if !t.is_empty() && initial.as_object().is_some_and(|v| !v.is_empty()) { Err(invalid())?; } fragments.push_str(t); yield ModelEvent::ToolCallDelta(ToolCallDelta { index:i,id:String::new(),name:String::new(),arguments:t.into() }); }, _ => Err(invalid())? } }
                    Some("content_block_stop") if started && stop.is_none() => { let b=blocks.get_mut(&index(&v)?).ok_or_else(invalid)?; match b { Block::Text {done} | Block::Tool {done,..} if !*done => *done=true, _ => Err(invalid())? } }
                    Some("message_delta") if started && stop.is_none() => { output=v["usage"]["output_tokens"].as_u64().or(output); if let Some(s)=v["delta"]["stop_reason"].as_str() { stop=Some(s.to_owned()); } }
                    Some("message_stop") if started => { let reason=match stop.as_deref() { Some("end_turn"|"stop_sequence")=>"stop", Some("max_tokens")=>"length", Some("tool_use")=>"tool_calls", _=>Err(invalid())? }; let mut calls=Vec::new(); for (_,b) in blocks { match b { Block::Text {done:true} => {}, Block::Tool {id,name,initial,fragments,done:true} => { let value=if fragments.is_empty(){ initial }else{ serde_json::from_str(&fragments).map_err(|_| invalid())? }; if !value.is_object() || calls.iter().any(|c: &ToolCall| c.id==id) || calls.len()>=64 { Err(invalid())?; } calls.push(ToolCall { id,name,arguments:value.to_string() }); }, _=>Err(invalid())? } } if (reason=="tool_calls") != !calls.is_empty() && reason!="length" { Err(invalid())?; } metadata.finish_reason=Some(reason.into()); metadata.provider_prompt_tokens=input.map(|n| n.saturating_add(cached).saturating_add(written)); yield ModelEvent::Metadata(metadata.clone()); yield ModelEvent::Complete { calls, usage:metadata.provider_prompt_tokens.zip(output).map(|(input_tokens,output_tokens)| Usage {input_tokens,output_tokens}) }; return; }
                    Some("error") => { let transient=matches!(v["error"]["type"].as_str(),Some("overloaded_error"|"api_error"|"rate_limit_error")); Err(if transient { Error::RetryableModel("Anthropic stream error".into()) } else { Error::Model("Anthropic stream error".into()) })?; }
                    Some("ping") => {}, _ => Err(invalid())?,
                }
            }
            Err(Error::RetryableModel("Anthropic stream ended before message_stop".into()))?;
        }))
    }
}
enum Block {
    Text {
        done: bool,
    },
    Tool {
        id: String,
        name: String,
        initial: Value,
        fragments: String,
        done: bool,
    },
}
fn invalid() -> Error {
    Error::Model("invalid Anthropic stream".into())
}
fn index(v: &Value) -> Result<u64> {
    v["index"].as_u64().ok_or_else(invalid)
}
fn field<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str().ok_or_else(invalid)
}
