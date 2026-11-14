//! Browser host for the pi-rs agent. See the browser SDK documentation for its gateway protocol.
#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use futures_util::{StreamExt, stream};
use js_sys::{Function, Object, Reflect, Uint8Array};
use pi_rs_browser_fetch::browser::BrowserFetch;
use pi_rs_core::{
    AgentSession, Error, Limits, Model, ModelEvent, ModelRequest, ModelStream, Record, Session,
    SessionBuilder, SessionControl, SessionStore,
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};
use tokio::sync::mpsc;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::{JsFuture, spawn_local};

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
                response
                    .get("text")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .ok_or_else(|| Error::Model("gateway response has no text".into()))
            };
            let result = tokio::select! { result = work => result, _ = tx.closed() => { fetch.cancel(); return; } };
            match result {
                Ok(text) => {
                    if tx.send(Ok(ModelEvent::Text(text))).await.is_ok() {
                        let _ = tx
                            .send(Ok(ModelEvent::Complete {
                                calls: vec![],
                                usage: None,
                            }))
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
    session: Rc<RefCell<Option<AgentSession>>>,
    control: SessionControl,
    release: Function,
    released: Cell<bool>,
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
            let session = SessionBuilder::new(Arc::new(Gateway { endpoint, bearer }), model)
                .limits(limits)
                .build(journal)
                .map_err(|e| js_error(&e.to_string()))?;
            let control = session.controls();
            Ok(Self {
                session: Rc::new(RefCell::new(Some(session))),
                control,
                release: release.clone(),
                released: Cell::new(false),
            })
        })();
        if result.is_err()
            && let Ok(promise) = release.call0(&JsValue::UNDEFINED)
        {
            let _ = JsFuture::from(js_sys::Promise::resolve(&promise)).await;
        }
        result
    }
    pub async fn prompt(&self, input: String) -> Result<String, JsValue> {
        let mut session = self
            .session
            .borrow_mut()
            .take()
            .ok_or_else(|| js_error("session busy"))?;
        let result = session
            .prompt(input)
            .await
            .map_err(|e| js_error(&e.to_string()));
        *self.session.borrow_mut() = Some(session);
        result
    }
    pub fn cancel(&self) {
        self.control.abort();
    }
    /// Close an idle session and wait for its cross-tab lock to be released.
    pub async fn close(&self) -> Result<(), JsValue> {
        if self.released.get() {
            return Ok(());
        }
        let session = self
            .session
            .borrow_mut()
            .take()
            .ok_or_else(|| js_error("session busy"))?;
        drop(session);
        self.released.set(true);
        let promise = self.release.call0(&JsValue::UNDEFINED)?;
        JsFuture::from(js_sys::Promise::resolve(&promise)).await?;
        Ok(())
    }
    pub fn messages(&self) -> Result<String, JsValue> {
        let slot = self.session.borrow();
        let session = slot.as_ref().ok_or_else(|| js_error("session busy"))?;
        let messages = session.messages().map_err(|e| js_error(&e.to_string()))?;
        serde_json::to_string(&messages).map_err(|_| js_error("message serialization failed"))
    }
}
impl Drop for BrowserSession {
    fn drop(&mut self) {
        self.control.abort();
        if !self.released.replace(true) {
            let _ = self.release.call0(&JsValue::UNDEFINED);
        }
    }
}
