# Experimental browser Fetch transport

`crates/browser-fetch` is an isolated transport experiment for a future browser
host. It uses the browser's Fetch and ReadableStream APIs to POST a UTF-8 body,
read bounded byte chunks, and abort either an in-flight request or response
stream. The package is deliberately independent of the native workspace while
parallel core and workspace changes are in flight.

This is **not** a browser agent, SDK session, provider adapter, or persistence
backend. Nothing in `pi-rs-core` calls it yet. In particular, the existing SDK
still does not compile for `wasm32-unknown-unknown`: filesystem capabilities,
journal storage, executor/timer and randomness integration, tool boundaries,
model protocol decoding, and the session API still need platform work. Passing
this transport test does not establish browser session support.

The caller supplies an absolute HTTP(S) URL, body and headers. URL userinfo is
rejected, ambient credentials are omitted, and redirects fail instead of
forwarding caller-supplied secrets to another endpoint. Browser CORS and the
host's authorization policy still apply; the transport does not authenticate
users, keep secrets out of JavaScript, or decide which endpoints are trusted.
Errors omit request URLs, credentials and response bodies. HTTP failures are
reported by status. Request bodies are limited to 8 MiB, individual response
chunks to 1 MiB and the total response to 16 MiB; excess data aborts the fetch.
Hosts must implement timeouts by calling `cancel()`. A stream permits one
pending read. Call `cancel()` and await pending operations before releasing the
object. There is no automatic retry, SSE decoding, session state or persistence.

To reproduce the focused tests with Rust 1.89, the installed WASM target,
`wasm-bindgen-cli` 0.2.130, Node 22+ and Chrome:

```sh
cargo +1.89.0 build --locked --offline --manifest-path crates/browser-fetch/Cargo.toml --target wasm32-unknown-unknown
wasm-bindgen --target nodejs --out-dir /tmp/pi-fetch-node crates/browser-fetch/target/wasm32-unknown-unknown/debug/pi_rs_browser_fetch.wasm
node crates/browser-fetch/tests/node-fetch.cjs /tmp/pi-fetch-node/pi_rs_browser_fetch.js
wasm-bindgen --target web --out-dir /tmp/pi-fetch-web crates/browser-fetch/target/wasm32-unknown-unknown/debug/pi_rs_browser_fetch.wasm
node crates/browser-fetch/tests/browser-fetch.cjs /tmp/pi-fetch-web /path/to/chrome
```

The Node fixture exercises real local HTTP streaming, concurrent reads, aborts,
limits and redirects. The Chrome fixture also exercises an actual cross-origin
CORS preflight, CORS rejection, streaming and abort. Neither fixture connects to
a model provider or executes a pi-rs session.
