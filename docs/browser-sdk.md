# Browser SDK (experimental)

`crates/browser-sdk` exposes the existing Rust `AgentSession` to a web page via
`wasm-bindgen`. The exercised configuration is Chrome on a secure origin (or
localhost), with Fetch, localStorage and Web Locks. It is a deliberately narrow
text-only gateway integration, not full browser parity with the native SDK.

## Build and use

Install Rust 1.89, its `wasm32-unknown-unknown` target, and
`wasm-bindgen-cli` 0.2.130. Build from the repository root:

```sh
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' cargo +1.89.0 build --locked --manifest-path crates/browser-sdk/Cargo.toml --target wasm32-unknown-unknown
wasm-bindgen --target web --out-dir ./browser-sdk-pkg crates/browser-sdk/target/wasm32-unknown-unknown/debug/pi_rs_browser_sdk.wasm
```

Serve the generated directory from a web server and import its JavaScript module:

```js
import init, { BrowserSession } from './browser-sdk-pkg/pi_rs_browser_sdk.js';
await init();
const session = await BrowserSession.open('chat-1', gatewayUrl, 'model-name', token, 120000);
try {
  const pending = session.prompt('Hello');
  // A UI cancel button may call session.cancel() while pending is active.
  console.log(await pending);
  console.log(JSON.parse(session.messages()));
} finally {
  // Cancel and await a pending prompt before closing. close waits for lock release.
  await session.close();
  session.free();
}
```

The final argument sets the per-model timeout in milliseconds; zero uses the
native default. Only one prompt may run at a time. `cancel()` aborts the active
run and Fetch request; it does not erase committed session records. `close()`
requires an idle session and releases its origin-scoped Web Lock. Failure to
acquire the lock causes `open` to fail, so cooperating tabs cannot concurrently
mutate the same session. Call `close()` before reopening; `free()` alone cannot
wait for lock release. Page termination also releases the browser lock.

Each journal record is committed by replacing a localStorage value before the
operation returns. A later `open` validates and restores that journal. Storage
errors and corrupt records fail the operation instead of silently starting a new
session. Browser storage quotas, user deletion, private browsing, and the
browser's persistence policies apply. Other same-origin code can access this
storage; the session may contain sensitive conversation text. The bearer token
is held in memory and is not written to the journal. The application controls
the gateway URL and credentials and must arrange CORS; Fetch omits ambient
cookies and rejects redirects.

## Gateway protocol and current limits

The configured URL receives a JSON POST containing the serialized pi-rs
`ModelRequest`: `model`, `messages`, `tools`, and `max_output_tokens`. When a
nonempty token is provided the request includes `Authorization: Bearer …`.
The gateway returns a successful JSON response `{ "text": "..." }`. The
transport reads its bounded response stream, but this adapter returns the text
only after the whole JSON response arrives. The gateway is responsible for
translating to a provider's API. Its errors are intentionally reported without
response bodies or request URLs.

The browser host currently provides no built-in tools, custom tool execution,
extensions, provider-specific adapters, streaming UI events, multimodal input,
branch navigation, or native filesystem and process access. It does not expose
the complete native SDK surface. The browser Fetch adapter enforces an 8 MiB
request body and a 16 MiB response limit. Chrome integration tests use a local
mock gateway; they do not establish compatibility with a live model provider or
other browsers. Build flags select browser randomness explicitly; the same
unknown WASM target name alone does not establish browser support.
