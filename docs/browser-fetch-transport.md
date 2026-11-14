# Experimental browser Fetch transport

`crates/browser-fetch` is a transport used by the experimental
[browser SDK](browser-sdk.md). It uses Fetch and ReadableStream to POST a UTF-8
body, read bounded byte chunks, and abort an in-flight request or response
stream. The package remains independent of the native workspace.

This transport alone is not a provider adapter or persistence backend. The
browser SDK adds a text gateway and session storage; consult its documentation
for the tested browser behavior and the native SDK features it does not expose.

The caller supplies an absolute HTTP(S) URL, body and headers. URL userinfo is
rejected, ambient credentials are omitted, and redirects fail instead of
forwarding caller-supplied secrets to another endpoint. Browser CORS and the
host's authorization policy still apply; the transport does not authenticate
users, keep secrets out of JavaScript, or decide which endpoints are trusted.
Errors omit request URLs, credentials and response bodies. HTTP failures are
reported by status. Request bodies are limited to 8 MiB and total responses to
16 MiB; excess data aborts the fetch. Browser-delivered chunks are returned as
slices of at most 1 MiB, regardless of their original boundaries. The browser
may allocate an incoming chunk before its size can be checked.
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
limits, a browser-delivered chunk larger than 1 MiB, and redirects. The Chrome
fixture also exercises an actual cross-origin CORS preflight, CORS rejection,
same-origin cookie omission, streaming and abort. Dedicated CI runs both
fixtures. Neither fixture connects to a model provider or executes a pi-rs
session.
