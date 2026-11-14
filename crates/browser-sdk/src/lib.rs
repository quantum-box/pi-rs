//! Browser host for the pi-rs agent. See the browser SDK documentation for its gateway protocol.
#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use futures_util::{StreamExt, stream};
use js_sys::{Function, Object, Reflect, Uint8Array};
use pi_rs_browser_fetch::browser::BrowserFetch;
use pi_rs_core::{
    AgentSession, Error, Extension, Limits, Model, ModelEvent, ModelRequest, ModelStream, Outcome,
    Record, Session, SessionBuilder, SessionControl, SessionStore, Tool, ToolCall, ToolContext,
    ToolSpec,
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};
use tokio::sync::mpsc;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::{JsFuture, future_to_promise, spawn_local};

// Web Locks holds an exclusive session owner across tabs and releases it on close.
#[wasm_bindgen(inline_js = r#"
export async function piAcquireSessionLock(name) {
  if (!navigator.locks) throw new Error('Web Locks unavailable');
  let signal;
  const acquired = new Promise(resolve => { signal = resolve; });
  let release;
  const finished = navigator.locks.request(name, { ifAvailable: true }, async lock => {
    if (!lock) { signal(false); return; }
    const held = new Promise(resolve => { release = resolve; });
    signal(true);
    await held;
  }).catch(() => signal(false));
  if (!await acquired) throw new Error('session already open');
  return () => { release(); return finished; };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = piAcquireSessionLock, catch)]
    fn acquire_lock(name: &str) -> Result<js_sys::Promise, JsValue>;
}

fn js_error(message: &str) -> JsValue {
    js_sys::Error::new(message).into()
}
fn storage() -> pi_rs_core::Result<web_sys::Storage> {
    web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .ok_or_else(|| Error::Session("browser storage unavailable".into()))
}
struct LocalStore {
    key: String,
    records: Vec<Record>,
}
impl SessionStore for LocalStore {
    fn load(&mut self) -> pi_rs_core::Result<Vec<Record>> {
        let value = storage()?
            .get_item(&self.key)
            .map_err(|_| Error::Session("browser storage read failed".into()))?;
        self.records = match value {
            Some(data) => serde_json::from_str(&data)?,
            None => Vec::new(),
        };
        Ok(self.records.clone())
    }
    fn append(&mut self, record: &Record) -> pi_rs_core::Result<()> {
        let mut next = self.records.clone();
        next.push(record.clone());
        let data = serde_json::to_string(&next)?;
        storage()?
            .set_item(&self.key, &data)
            .map_err(|_| Error::Session("browser storage write failed".into()))?;
        self.records = next;
        Ok(())
    }
}

struct BrowserTools;
impl Extension for BrowserTools {
    fn id(&self) -> &str {
        "browser-demo"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(AddTool)]
    }
}
struct AddTool;
#[async_trait::async_trait]
impl Tool for AddTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "add_numbers".into(),
            description: "Add two numbers together".into(),
            parameters: serde_json::json!({"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}),
        }
    }
    async fn invoke(&self, arguments: serde_json::Value, _: ToolContext) -> Outcome {
        match (arguments["a"].as_f64(), arguments["b"].as_f64()) {
            (Some(a), Some(b)) => Outcome::Succeeded((a + b).to_string()),
            _ => Outcome::Failed("expected numeric arguments".into()),
        }
    }
}

struct Gateway {
    endpoint: String,
    bearer: String,
}
#[async_trait::async_trait]
impl Model for Gateway {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        let body = serde_json::to_string(&request)?;
        let endpoint = self.endpoint.clone();
        let bearer = self.bearer.clone();
        let (tx, rx) = mpsc::channel::<pi_rs_core::Result<ModelEvent>>(8);
        spawn_local(async move {
            let headers = Object::new();
            if Reflect::set(
                &headers,
                &JsValue::from_str("content-type"),
                &JsValue::from_str("application/json"),
            )
            .is_err()
                || (!bearer.is_empty()
                    && Reflect::set(
                        &headers,
                        &JsValue::from_str("authorization"),
                        &JsValue::from_str(&format!("Bearer {bearer}")),
                    )
                    .is_err())
            {
                let _ = tx
                    .send(Err(Error::Model("invalid gateway headers".into())))
                    .await;
                return;
            }
            let fetch = match BrowserFetch::new(&endpoint, &body, &headers) {
                Ok(fetch) => fetch,
                Err(_) => {
                    let _ = tx
                        .send(Err(Error::Model("invalid gateway request".into())))
                        .await;
                    return;
                }
            };
            let work = async {
                fetch
                    .open()
                    .await
                    .map_err(|_| Error::Model("gateway request failed".into()))?;
                let mut bytes = Vec::new();
                loop {
                    let chunk = fetch
                        .read()
                        .await
                        .map_err(|_| Error::Model("gateway stream failed".into()))?;
                    if chunk.is_undefined() {
                        break;
                    }
                    bytes.extend_from_slice(&Uint8Array::new(&chunk).to_vec());
                }
                let response: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::Model("invalid gateway response".into()))?;
                let text = response
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned();
                let calls: Vec<ToolCall> = match response.get("calls") {
                    Some(calls) => serde_json::from_value(calls.clone())
                        .map_err(|_| Error::Model("invalid gateway tool calls".into()))?,
                    None => vec![],
                };
                if response.get("text").and_then(|v| v.as_str()).is_none() && calls.is_empty() {
                    return Err(Error::Model(
                        "gateway response has no text or tool calls".into(),
                    ));
                }
                Ok((text, calls))
            };
            let result = tokio::select! { result = work => result, _ = tx.closed() => { fetch.cancel(); return; } };
            match result {
                Ok((text, calls)) => {
                    if tx.send(Ok(ModelEvent::Text(text))).await.is_ok() {
                        let _ = tx
                            .send(Ok(ModelEvent::Complete { calls, usage: None }))
                            .await;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                }
            }
        });
        Ok(stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .boxed())
    }
}

#[wasm_bindgen]
pub struct BrowserSession {
    inner: Rc<SessionInner>,
}
struct SessionInner {
    session: RefCell<Option<AgentSession>>,
    control: SessionControl,
    release: Function,
    release_promise: RefCell<Option<js_sys::Promise>>,
    close_promise: RefCell<Option<js_sys::Promise>>,
    disposed: Cell<bool>,
    cancel_sequence: Cell<u64>,
}
#[wasm_bindgen]
impl BrowserSession {
    /// Opens a persisted session. `key` identifies it within this origin.
    pub async fn open(
        key: String,
        endpoint: String,
        model: String,
        bearer: String,
        timeout_ms: u32,
    ) -> Result<BrowserSession, JsValue> {
        Self::open_inner(key, endpoint, model, bearer, timeout_ms, false).await
    }
    /// Opens a demonstration session with a built-in, side-effect-free addition tool.
    pub async fn open_demo(
        key: String,
        endpoint: String,
        model: String,
        bearer: String,
        timeout_ms: u32,
    ) -> Result<BrowserSession, JsValue> {
        Self::open_inner(key, endpoint, model, bearer, timeout_ms, true).await
    }
    pub fn prompt(&self, input: String) -> js_sys::Promise {
        self.run_input(Some(input))
    }
    /// Resume a previously interrupted or settled session without adding input.
    pub fn resume(&self) -> js_sys::Promise {
        self.run_input(None)
    }
    pub fn cancel(&self) {
        self.inner
            .cancel_sequence
            .set(self.inner.cancel_sequence.get().wrapping_add(1));
        self.inner.control.abort();
    }
    /// Close an idle session and wait for its cross-tab lock to be released.
    pub fn close(&self) -> js_sys::Promise {
        if let Some(promise) = self.inner.close_promise.borrow().clone() {
            return promise;
        }
        let inner = self.inner.clone();
        let promise = future_to_promise(async move {
            let session = inner.session.borrow_mut().take();
            let Some(mut session) = session else {
                *inner.close_promise.borrow_mut() = None;
                return Err(js_error("session busy or closed"));
            };
            let close_result = session.close().await.map_err(|e| js_error(&e.to_string()));
            drop(session);
            let value = inner.release.call0(&JsValue::UNDEFINED)?;
            let released = js_sys::Promise::resolve(&value);
            *inner.release_promise.borrow_mut() = Some(released.clone());
            JsFuture::from(released).await?;
            close_result?;
            Ok(JsValue::UNDEFINED)
        });
        *self.inner.close_promise.borrow_mut() = Some(promise.clone());
        promise
    }
    pub fn messages(&self) -> Result<String, JsValue> {
        let slot = self.inner.session.borrow();
        let session = slot.as_ref().ok_or_else(|| js_error("session busy"))?;
        let messages = session.messages().map_err(|e| js_error(&e.to_string()))?;
        serde_json::to_string(&messages).map_err(|_| js_error("message serialization failed"))
    }
}
impl BrowserSession {
    async fn open_inner(
        key: String,
        endpoint: String,
        model: String,
        bearer: String,
        timeout_ms: u32,
        demo_tool: bool,
    ) -> Result<BrowserSession, JsValue> {
        if key.is_empty() || key.len() > 256 {
            return Err(js_error("invalid session key"));
        }
        let storage_key = format!("pi-rs:session:{key}");
        let release: Function = JsFuture::from(acquire_lock(&storage_key)?)
            .await?
            .dyn_into()
            .map_err(|_| js_error("session lock failed"))?;
        let result = (|| {
            let journal = Session::with_store(LocalStore {
                key: storage_key,
                records: Vec::new(),
            })
            .map_err(|e| js_error(&e.to_string()))?;
            let mut limits = Limits::default();
            if timeout_ms != 0 {
                limits.model_timeout = std::time::Duration::from_millis(timeout_ms.into());
            }
            let mut builder =
                SessionBuilder::new(Arc::new(Gateway { endpoint, bearer }), model.clone())
                    .limits(limits);
            if demo_tool {
                builder = builder
                    .extension(Arc::new(BrowserTools))
                    .map_err(|e| js_error(&e.to_string()))?;
            }
            let session = builder
                .build(journal)
                .map_err(|e| js_error(&e.to_string()))?;
            let control = session.controls();
            Ok(Self {
                inner: Rc::new(SessionInner {
                    session: RefCell::new(Some(session)),
                    control,
                    release: release.clone(),
                    release_promise: RefCell::new(None),
                    close_promise: RefCell::new(None),
                    disposed: Cell::new(false),
                    cancel_sequence: Cell::new(0),
                }),
            })
        })();
        if result.is_err()
            && let Ok(promise) = release.call0(&JsValue::UNDEFINED)
        {
            let _ = JsFuture::from(js_sys::Promise::resolve(&promise)).await;
        }
        result
    }
}
impl BrowserSession {
    fn run_input(&self, input: Option<String>) -> js_sys::Promise {
        let inner = self.inner.clone();
        let sequence = inner.cancel_sequence.get();
        future_to_promise(async move {
            if inner.disposed.get() || inner.cancel_sequence.get() != sequence {
                return Err(js_error("session cancelled"));
            }
            let mut session = inner
                .session
                .borrow_mut()
                .take()
                .ok_or_else(|| js_error("session busy or closed"))?;
            let result = match input {
                Some(input) => session.prompt(input).await,
                None => session.resume().await,
            }
            .map(JsValue::from)
            .map_err(|e| js_error(&e.to_string()));
            *inner.session.borrow_mut() = Some(session);
            result
        })
    }
}
impl Drop for BrowserSession {
    fn drop(&mut self) {
        self.inner.disposed.set(true);
        self.inner.control.abort();
    }
}
impl Drop for SessionInner {
    fn drop(&mut self) {
        if self.release_promise.borrow().is_none() {
            let _ = self.release.call0(&JsValue::UNDEFINED);
        }
    }
}
