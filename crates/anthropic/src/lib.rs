//! Native Anthropic Messages and Models API integration.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    ResolvedModelIdentity, Result, ToolCall, Usage,
};
use reqwest::{Client, Response, Url, header::HeaderValue};
use serde_json::{Value, json};

pub const DEFAULT_ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub struct CatalogEntry {
    pub id: String,
    pub context_window: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub reasoning: Option<bool>,
    pub images: Option<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Progress {
    WaitingForModel,
    Thinking,
    Generating,
    PreparingTool,
}

type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct Anthropic {
    client: Client,
    endpoint: Url,
    api_key: HeaderValue,
    catalog: Arc<RwLock<BTreeMap<String, CatalogEntry>>>,
    probe_context: bool,
    progress: Option<Arc<dyn Fn(Progress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
}

impl Anthropic {
    pub fn new(endpoint: &str, api_key: &str) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid Anthropic endpoint".into()))?;
        let loopback = matches!(
            endpoint.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.path().ends_with("/messages")
        {
            return Err(Error::Config(
                "Anthropic endpoint must be HTTPS or loopback HTTP with a /messages path and no embedded credentials, query, or fragment".into(),
            ));
        }
        let mut api_key = HeaderValue::from_str(api_key)
            .ok()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| Error::Config("missing or invalid Anthropic API key".into()))?;
        api_key.set_sensitive(true);
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::Config("could not build Anthropic HTTP client".into()))?;
        Ok(Self {
            client,
            endpoint,
            api_key,
            catalog: Default::default(),
            probe_context: true,
            progress: None,
            reasoning: None,
        })
    }

    pub fn with_context_probe(mut self, enabled: bool) -> Self {
        self.probe_context = enabled;
        self
    }

    pub fn with_progress(mut self, observer: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(observer));
        self
    }

    pub fn with_reasoning(mut self, observer: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.reasoning = Some(Arc::new(observer));
        self
    }

    pub fn cached_model(&self, model: &str) -> Option<CatalogEntry> {
        self.catalog.read().ok()?.get(model).cloned()
    }

    fn models_url(&self) -> Url {
        let mut url = self.endpoint.clone();
        let base = url
            .path()
            .strip_suffix("/messages")
            .expect("validated endpoint");
        url.set_path(&format!("{base}/models"));
        url
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header("anthropic-version", API_VERSION)
            .header("x-api-key", self.api_key.clone())
    }

    /// Return paginated models in server order and retain their metadata for this client.
    pub async fn models(&self) -> Result<Vec<CatalogEntry>> {
        tokio::time::timeout(Duration::from_secs(10), self.load_models())
            .await
            .map_err(|_| Error::RetryableModel("Anthropic model listing timed out".into()))?
    }

    async fn load_models(&self) -> Result<Vec<CatalogEntry>> {
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let mut url = self.models_url();
            url.query_pairs_mut().append_pair("limit", "100");
            if let Some(cursor) = &cursor {
                url.query_pairs_mut().append_pair("after_id", cursor);
            }
            let response = send(self.authenticate(self.client.get(url))).await?;
            let page = read_json(response).await?;
            let models = page["data"]
                .as_array()
                .ok_or_else(|| Error::Model("invalid Anthropic model list".into()))?;
            for model in models {
                if let Some(model) = decode_model(model)
                    && seen.insert(model.id.clone())
                {
                    result.push(model);
                }
            }
            if page["has_more"] != true {
                self.catalog
                    .write()
                    .expect("model catalog")
                    .extend(result.iter().map(|entry| (entry.id.clone(), entry.clone())));
                return Ok(result);
            }
            let next = page["last_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| Error::Model("missing Anthropic model cursor".into()))?;
            if !cursors.insert(next.to_owned()) {
                return Err(Error::Model("repeated Anthropic model cursor".into()));
            }
            cursor = Some(next.to_owned());
        }
        Err(Error::Model(
            "Anthropic model listing exceeds page limit".into(),
        ))
    }
}

fn decode_model(model: &Value) -> Option<CatalogEntry> {
    let tokens = |key: &str| {
        model[key]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|count| *count > 0)
    };
    Some(CatalogEntry {
        id: model["id"].as_str().filter(|id| !id.is_empty())?.into(),
        context_window: tokens("max_input_tokens"),
        max_output_tokens: tokens("max_tokens"),
        reasoning: model["capabilities"]["thinking"]["supported"].as_bool(),
        images: model["capabilities"]["image_input"]["supported"].as_bool(),
    })
}

async fn send(request: reqwest::RequestBuilder) -> Result<Response> {
    let response = request
        .send()
        .await
        .map_err(|_| Error::RetryableModel("Anthropic HTTP connection failed".into()))?;
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        let message = format!("Anthropic returned HTTP {status}");
        if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
            Err(Error::RetryableModel(message))
        } else {
            Err(Error::Model(message))
        }
    }
}

async fn read_json(mut response: Response) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::RetryableModel("broken Anthropic HTTP response".into()))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(Error::Model("Anthropic response exceeds size limit".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Model("invalid Anthropic response".into()))
}

fn encode_request(request: ModelRequest) -> Result<Value> {
    let mut system = Vec::new();
    let mut turns: Vec<(&str, Vec<Value>)> = Vec::new();
    for message in request.messages {
        let (role, blocks) = match message {
            Message::System { content } => {
                system.push(content);
                continue;
            }
            Message::User { content } => ("user", text_block(content).into_iter().collect()),
            Message::UserWithImages { content, images } => {
                let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                for image in images {
                    image.validate()?;
                    blocks.push(json!({"type":"image","source":{
                        "type":"base64","media_type":image.mime_type,"data":image.data
                    }}));
                }
                ("user", blocks)
            }
            Message::Assistant { content, calls } => {
                let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                for call in calls {
                    let input = serde_json::from_str::<Value>(&call.arguments)
                        .ok()
                        .filter(Value::is_object)
                        .unwrap_or_else(|| json!({}));
                    blocks.push(
                        json!({"type":"tool_use","id":call.id,"name":call.name,"input":input}),
                    );
                }
                ("assistant", blocks)
            }
            Message::Tool { call_id, content } => (
                "user",
                vec![json!({
                    "type":"tool_result","tool_use_id":call_id,"content":content
                })],
            ),
        };
        if blocks.is_empty() {
            continue;
        }
        if let Some((previous, content)) = turns.last_mut()
            && *previous == role
        {
            content.extend(blocks);
        } else {
            turns.push((role, blocks));
        }
    }
    if turns.is_empty() {
        return Err(Error::Model(
            "Anthropic requires at least one conversation message".into(),
        ));
    }
    let messages: Vec<_> = turns
        .into_iter()
        .map(|(role, content)| json!({"role":role,"content":content}))
        .collect();
    let mut body = json!({"model":request.model,"messages":messages,"max_tokens":request.max_output_tokens,"stream":true});
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(request.tools.into_iter().map(|tool| {
            json!({"name":tool.name,"description":tool.description,"input_schema":tool.parameters})
        }).collect::<Vec<_>>());
    }
    Ok(body)
}

fn text_block(content: String) -> Option<Value> {
    (!content.is_empty()).then(|| json!({"type":"text","text":content}))
}

struct Call {
    id: String,
    name: String,
    input: Value,
    fragments: String,
    complete: bool,
}

impl Call {
    fn finish(self) -> Result<ToolCall> {
        if !self.complete {
            return Err(Error::Model(
                "Anthropic tool block ended prematurely".into(),
            ));
        }
        let input = if self.fragments.is_empty() {
            self.input
        } else {
            serde_json::from_str(&self.fragments)
                .map_err(|_| Error::Model("invalid Anthropic tool input JSON".into()))?
        };
        if !input.is_object() {
            return Err(Error::Model(
                "Anthropic tool input must be an object".into(),
            ));
        }
        Ok(ToolCall {
            id: self.id,
            name: self.name,
            arguments: input.to_string(),
        })
    }
}

#[derive(Default)]
struct StreamUsage {
    input: Option<u64>,
    output: Option<u64>,
    cached: Option<u64>,
    cache_written: Option<u64>,
}
impl StreamUsage {
    fn update(&mut self, value: &Value, metadata: &mut ModelMetadata) {
        self.input = value["input_tokens"].as_u64().or(self.input);
        self.output = value["output_tokens"].as_u64().or(self.output);
        self.cached = value["cache_read_input_tokens"].as_u64().or(self.cached);
        self.cache_written = value["cache_creation_input_tokens"]
            .as_u64()
            .or(self.cache_written);
        metadata.cached_input_tokens = self.cached;
        metadata.cache_creation_input_tokens = self.cache_written;
        metadata.provider_prompt_tokens = self.total_input();
    }
    fn total_input(&self) -> Option<u64> {
        Some(
            self.input?
                .saturating_add(self.cached.unwrap_or(0))
                .saturating_add(self.cache_written.unwrap_or(0)),
        )
    }
    fn finish(&self) -> Option<Usage> {
        Some(Usage {
            input_tokens: self.total_input()?,
            output_tokens: self.output?,
        })
    }
}

#[async_trait::async_trait]
impl Model for Anthropic {
    async fn context_window(&self, model: &str) -> Option<u32> {
        if let Some(details) = self.cached_model(model) {
            return details.context_window;
        }
        if !self.probe_context {
            return None;
        }
        let mut url = self.models_url();
        url.path_segments_mut().ok()?.push(model);
        let details = tokio::time::timeout(Duration::from_secs(2), async {
            let response = send(self.authenticate(self.client.get(url))).await.ok()?;
            decode_model(&read_json(response).await.ok()?)
        })
        .await
        .ok()
        .flatten()?;
        let window = details.context_window;
        self.catalog.write().ok()?.insert(model.into(), details);
        window
    }

    async fn stream(&self, mut request: ModelRequest) -> Result<ModelStream> {
        if let Some(limit) = self
            .cached_model(&request.model)
            .and_then(|model| model.max_output_tokens)
        {
            request.max_output_tokens = request.max_output_tokens.min(limit);
        }
        let requested_model = request.model.clone();
        let body = encode_request(request)?;
        if let Some(progress) = &self.progress {
            progress(Progress::WaitingForModel);
        }
        let response =
            send(self.authenticate(self.client.post(self.endpoint.clone()).json(&body))).await?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected an Anthropic SSE response".into()));
        }
        let progress = self.progress.clone();
        let reasoning = self.reasoning.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0_usize;
            let mut events = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|_| Error::RetryableModel("broken Anthropic HTTP stream".into()))?;
                size = size.saturating_add(chunk.len());
                if size > MAX_RESPONSE_BYTES {
                    return Err(Error::Model("Anthropic stream exceeds size limit".into()));
                }
                Ok(chunk)
            }).eventsource();
            let mut last_progress = Some(Progress::WaitingForModel);
            let mut report = |stage| {
                if last_progress != Some(stage) {
                    last_progress = Some(stage);
                    if let Some(progress) = &progress { progress(stage); }
                }
            };
            let mut calls = BTreeMap::<u64, Call>::new();
            let mut usage = StreamUsage::default();
            let mut metadata = ModelMetadata::default();
            let mut reasoning_text = String::new();
            let mut started = false;
            while let Some(event) = events.next().await {
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) => error,
                    _ => Error::RetryableModel("broken Anthropic SSE stream".into()),
                })?;
                let value: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("malformed Anthropic SSE event".into()))?;
                let previous = metadata.clone();
                match value["type"].as_str() {
                    Some("message_start") if !started => {
                        started = true;
                        let message = &value["message"];
                        metadata.provider_request_id = message["id"].as_str().filter(|id|
                            id.len() <= 128 && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                        ).map(str::to_owned);
                        metadata.resolved_model = Some(ResolvedModelIdentity {
                            provider: "anthropic".into(),
                            model: message["model"].as_str().unwrap_or(&requested_model).into(),
                        });
                        usage.update(&message["usage"], &mut metadata);
                    }
                    Some("content_block_start") if started => {
                        let block = &value["content_block"];
                        match block["type"].as_str() {
                            Some("tool_use") => {
                                let index = block_index(&value)?;
                                let id = required(&block["id"], "tool ID")?;
                                let name = required(&block["name"], "tool name")?;
                                if calls.len() >= 64 || calls.insert(index, Call {
                                    id: id.into(), name: name.into(), input: block["input"].clone(), fragments: String::new(), complete: false,
                                }).is_some() {
                                    Err(Error::Model("invalid Anthropic tool block index".into()))?;
                                }
                                report(Progress::PreparingTool);
                            }
                            Some("text") => {
                                if let Some(text) = block["text"].as_str().filter(|text| !text.is_empty()) {
                                    report(Progress::Generating);
                                    yield ModelEvent::Text(text.into());
                                }
                            }
                            _ => {}
                        }
                    }
                    Some("content_block_delta") if started => {
                        let delta = &value["delta"];
                        match delta["type"].as_str() {
                            Some("text_delta") => {
                                if let Some(text) = delta["text"].as_str().filter(|text| !text.is_empty()) {
                                    report(Progress::Generating);
                                    yield ModelEvent::Text(text.into());
                                }
                            }
                            Some("thinking_delta") => {
                                if let Some(text) = delta["thinking"].as_str().filter(|text| !text.is_empty()) {
                                    report(Progress::Thinking);
                                    reasoning_text.push_str(text);
                                    if let Some(observer) = &reasoning { observer(text); }
                                }
                            }
                            Some("input_json_delta") => {
                                let index = block_index(&value)?;
                                let call = calls.get_mut(&index).filter(|call| !call.complete)
                                    .ok_or_else(|| Error::Model("Anthropic tool input has no open block".into()))?;
                                call.fragments.push_str(delta["partial_json"].as_str().unwrap_or_default());
                            }
                            _ => {}
                        }
                    }
                    Some("content_block_stop") if started => {
                        if let Some(call) = calls.get_mut(&block_index(&value)?) {
                            call.complete = true;
                        }
                    }
                    Some("message_delta") if started => {
                        usage.update(&value["usage"], &mut metadata);
                        if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                            metadata.finish_reason = Some(match reason {
                                "end_turn" | "stop_sequence" => "stop",
                                "max_tokens" => "length",
                                "tool_use" => "tool_calls",
                                "refusal" => "content_filter",
                                _ => "other",
                            }.into());
                        }
                    }
                    Some("message_stop") if started => {
                        if !reasoning_text.is_empty() {
                            metadata.reasoning = Some(reasoning_text);
                            yield ModelEvent::Metadata(metadata.clone());
                        }
                        match metadata.finish_reason.as_deref() {
                            Some("stop" | "length") if calls.is_empty() => {}
                            Some("tool_calls") if !calls.is_empty() => {}
                            _ => Err(Error::Model("Anthropic response did not finish successfully".into()))?,
                        }
                        let calls = calls.into_values().map(Call::finish).collect::<Result<Vec<_>>>()?;
                        yield ModelEvent::Complete { calls, usage: usage.finish() };
                        return;
                    }
                    Some("error") => {
                        let kind = value["error"]["type"].as_str().unwrap_or_default();
                        Err(if matches!(kind, "overloaded_error" | "api_error" | "rate_limit_error") {
                            Error::RetryableModel("Anthropic returned a transient stream error".into())
                        } else {
                            Error::Model("Anthropic returned a stream error".into())
                        })?;
                    }
                    _ => {}
                }
                if metadata != previous {
                    yield ModelEvent::Metadata(metadata.clone());
                }
            }
            Err(Error::RetryableModel("Anthropic stream ended before message_stop".into()))?;
        }))
    }
}

fn block_index(value: &Value) -> Result<u64> {
    value["index"]
        .as_u64()
        .ok_or_else(|| Error::Model("missing Anthropic block index".into()))
}

fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Model(format!("missing Anthropic {field}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_blocks_must_close_with_an_object_before_dispatch() {
        let call = |complete, fragments: &str| Call {
            id: "toolu_fixture".into(),
            name: "read".into(),
            input: json!({}),
            fragments: fragments.into(),
            complete,
        };
        assert!(call(false, r#"{"path":"file.txt"}"#).finish().is_err());
        assert!(call(true, r#"{"path":"file.txt""#).finish().is_err());
        assert!(call(true, "[]").finish().is_err());
        let result = call(true, r#"{"path":"file.txt"}"#).finish().unwrap();
        assert_eq!(result.arguments, r#"{"path":"file.txt"}"#);
    }
}
