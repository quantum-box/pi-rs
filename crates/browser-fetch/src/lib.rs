//! Browser Fetch transport prototype. This is not a browser SDK or agent.
#[cfg(target_arch = "wasm32")]
mod browser {
    use js_sys::{Object, Reflect, Uint8Array};
    use std::{cell::RefCell, rc::Rc};
    use wasm_bindgen::{JsCast, prelude::*};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        AbortController, Headers, ReadableStreamDefaultReader, Request, RequestCredentials,
        RequestInit, RequestRedirect, Response, Url,
    };

    const MAX_BODY: usize = 8 * 1024 * 1024;
    const MAX_RESPONSE: usize = 16 * 1024 * 1024;
    const MAX_CHUNK: usize = 1024 * 1024;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_namespace = globalThis, js_name = fetch, catch)]
        fn fetch(request: &Request) -> Result<js_sys::Promise, JsValue>;
    }

    fn error(message: &str) -> JsValue {
        js_sys::Error::new(message).into()
    }
    #[derive(PartialEq, Eq)]
    enum Phase {
        Ready,
        Opening,
        Streaming,
        Done,
        Cancelled,
        Failed,
    }
    struct State {
        phase: Phase,
        reader: Option<ReadableStreamDefaultReader>,
        reading: bool,
        total: usize,
    }

    /// One POST with bounded streaming. The host owns endpoint and headers.
    #[wasm_bindgen]
    pub struct BrowserFetch {
        request: Request,
        abort: AbortController,
        state: Rc<RefCell<State>>,
    }

    #[wasm_bindgen]
    impl BrowserFetch {
        #[wasm_bindgen(constructor)]
        pub fn new(endpoint: &str, body: &str, headers: &Object) -> Result<BrowserFetch, JsValue> {
            if body.len() > MAX_BODY {
                return Err(error("request body exceeds limit"));
            }
            let url = Url::new(endpoint).map_err(|_| error("invalid endpoint"))?;
            if !matches!(url.protocol().as_str(), "http:" | "https:")
                || !url.username().is_empty()
                || !url.password().is_empty()
            {
                return Err(error("endpoint must be HTTP(S) without URL credentials"));
            }
            let abort =
                AbortController::new().map_err(|_| error("abort controller unavailable"))?;
            let headers = Headers::new_with_record_from_str_to_str(headers)
                .map_err(|_| error("invalid request headers"))?;
            let init = RequestInit::new();
            init.set_method("POST");
            init.set_body(&JsValue::from_str(body));
            init.set_headers(&headers);
            init.set_credentials(RequestCredentials::Omit);
            init.set_redirect(RequestRedirect::Error);
            init.set_signal(Some(&abort.signal()));
            let request = Request::new_with_str_and_init(endpoint, &init)
                .map_err(|_| error("invalid request"))?;
            Ok(Self {
                request,
                abort,
                state: Rc::new(RefCell::new(State {
                    phase: Phase::Ready,
                    reader: None,
                    reading: false,
                    total: 0,
                })),
            })
        }

        /// Resolves after response headers; errors never include response bodies or credentials.
        pub async fn open(&self) -> Result<u16, JsValue> {
            {
                let mut state = self.state.borrow_mut();
                if state.phase != Phase::Ready {
                    return Err(error("request already started or closed"));
                }
                state.phase = Phase::Opening;
            }
            let result = async {
                let promise = fetch(&self.request).map_err(|_| error("fetch failed"))?;
                let response: Response = JsFuture::from(promise)
                    .await
                    .map_err(|_| error("fetch failed"))?
                    .dyn_into()
                    .map_err(|_| error("invalid fetch response"))?;
                let status = response.status();
                if !response.ok() {
                    return Err(error(&format!("HTTP status {status}")));
                }
                let body = response
                    .body()
                    .ok_or_else(|| error("response has no stream"))?;
                let reader = body
                    .get_reader()
                    .dyn_into::<ReadableStreamDefaultReader>()
                    .map_err(|_| error("response stream unavailable"))?;
                Ok((status, reader))
            }
            .await;
            let mut state = self.state.borrow_mut();
            if state.phase == Phase::Cancelled {
                return Err(error("request cancelled"));
            }
            match result {
                Ok((status, reader)) => {
                    state.reader = Some(reader);
                    state.phase = Phase::Streaming;
                    Ok(status)
                }
                Err(error) => {
                    state.phase = Phase::Failed;
                    self.abort.abort();
                    Err(error)
                }
            }
        }

        /// Returns one byte chunk, or undefined at EOF. Only one read may be pending.
        pub async fn read(&self) -> Result<JsValue, JsValue> {
            let reader = {
                let mut state = self.state.borrow_mut();
                if state.phase == Phase::Done {
                    return Ok(JsValue::UNDEFINED);
                }
                if state.phase != Phase::Streaming || state.reading {
                    return Err(error("stream unavailable or read already pending"));
                }
                state.reading = true;
                state.reader.as_ref().expect("streaming reader").clone()
            };
            let result = JsFuture::from(reader.read()).await;
            let mut state = self.state.borrow_mut();
            state.reading = false;
            if state.phase == Phase::Cancelled {
                return Err(error("request cancelled"));
            }
            let value = match result {
                Ok(value) => value,
                Err(_) => {
                    state.phase = Phase::Failed;
                    self.abort.abort();
                    return Err(error("stream read failed"));
                }
            };
            let done = Reflect::get(&value, &JsValue::from_str("done"))
                .map_err(|_| error("invalid stream result"))?
                .as_bool()
                .ok_or_else(|| error("invalid stream result"))?;
            if done {
                state.phase = Phase::Done;
                state.reader = None;
                return Ok(JsValue::UNDEFINED);
            }
            let chunk = Reflect::get(&value, &JsValue::from_str("value"))
                .map_err(|_| error("invalid stream result"))?
                .dyn_into::<Uint8Array>()
                .map_err(|_| error("invalid stream chunk"))?;
            let length = chunk.length() as usize;
            state.total = state.total.saturating_add(length);
            if length > MAX_CHUNK || state.total > MAX_RESPONSE {
                state.phase = Phase::Failed;
                self.abort.abort();
                return Err(error("response exceeds limit"));
            }
            Ok(chunk.into())
        }

        pub fn cancel(&self) {
            let mut state = self.state.borrow_mut();
            if state.phase != Phase::Done {
                state.phase = Phase::Cancelled;
                self.abort.abort();
            }
        }
    }
    impl Drop for BrowserFetch {
        fn drop(&mut self) {
            self.abort.abort();
        }
    }
}
