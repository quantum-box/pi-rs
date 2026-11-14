//! Opt-in Anthropic Messages adapter. Encoding and stream handling are adapted
//! from pi-rs-anthropic under the repository's MIT license.
mod encode;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, ModelApi, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Result, ToolCall,
    ToolCallDelta, Usage,
};
use pi_rs_durable::model::{ProviderAdapter, ProviderAttempt};
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
}
impl AnthropicAdapter {
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
        })
    }
}
#[async_trait::async_trait]
impl ProviderAdapter for AnthropicAdapter {
    fn id(&self) -> &str {
        "anthropic-messages/v1"
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value> {
        encode::encode_request(request.clone(), SCOPE)
    }
    fn validate(&self, _: &ProviderAttempt, request: &ModelRequest, body: &Value) -> Result<()> {
        let fail = || Error::Config("invalid or unauthorized Anthropic request body".into());
        let obj = body.as_object().ok_or_else(fail)?;
        let expected = encode::encode_request(request.clone(), SCOPE)?;
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
