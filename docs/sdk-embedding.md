# Embedding the Rust SDK

The Rust SDK currently targets native hosts. Tauri's Rust backend can own a
session and expose selected operations to its webview through Tauri commands;
the webview does not need to compile pi-rs to WebAssembly. The SDK does not
automatically install the workbench's tools, load its configuration, or manage
credentials for an embedding host.

## Native state and Tauri

The [native host example](../examples/native_sdk_host.rs) uses an in-memory
session and a local Ollama endpoint. Run it with a local Ollama server and the
`qwen2.5:7b` model installed:

```sh
cargo run --locked --example native_sdk_host
```

The example's offline fixture tests session reuse and that the state is
`Send + Sync`. The following is a Tauri 2 integration sketch using the same
pattern. It needs a Tauri project and its normal generated configuration; the
Tauri glue itself is not built by this repository's CI. Add `pi-rs` (with the
default `ollama` feature) and `tokio` with the `sync` feature to `src-tauri`.
Use a local path to your checkout or a pinned Git revision of this repository.

```rust
use std::sync::Arc;
use pi_rs::{AgentSession, SessionBuilder, SessionControl, ollama::Ollama};
use tokio::sync::Mutex;

struct AppState {
    session: Mutex<AgentSession>,
    control: SessionControl,
}

#[tauri::command]
async fn prompt(state: tauri::State<'_, AppState>, text: String) -> Result<String, String> {
    state.session.lock().await.prompt(text).await.map_err(|e| e.to_string())
}

#[tauri::command]
fn abort(state: tauri::State<'_, AppState>) {
    state.control.abort();
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let model = Arc::new(Ollama::new("http://127.0.0.1:11434/v1/chat/completions", None)?);
    let session = SessionBuilder::new(model, "qwen2.5:7b").in_memory()?;
    let control = session.controls();
    tauri::Builder::default()
        .manage(AppState { session: Mutex::new(session), control })
        .invoke_handler(tauri::generate_handler![prompt, abort])
        .run(tauri::generate_context!())?;
    Ok(())
}
```

Call `invoke('prompt', { text: 'Hello' })` or `invoke('abort')` from the
frontend using `@tauri-apps/api/core`. Prompt calls are serialized by the async
mutex. Abort uses a cloned control and does not wait for the prompt's lock.
For streaming, subscribe before starting a prompt with `session.subscribe()`
and forward only the events the frontend needs; slow receivers can lag, and
request/record events may contain private context. Call `close().await` when
the host can arrange asynchronous shutdown. For a durable session, replace
`in_memory()` with `open()` and a private application-owned path, and manage
the session lifecycle and reopen errors in the host.

This example registers no tools. If a host enables tools or extensions, it must
choose which frontend callers may invoke them and enforce its own workspace,
credential, and network policies. The SDK's session state is not a sandbox.
See [Tauri's state guide](https://v2.tauri.app/develop/state-management/) and
[command guide](https://v2.tauri.app/develop/calling-rust/).

## Browser WASM status

**Browser WASM is not currently supported or tested.** The target name
`wasm32-unknown-unknown` alone does not imply a browser host. The native core
has unconditional dependencies on `cap-std` and `tempfile`, uses filesystem
journals and workspace access, and uses OS-backed context fingerprinting.
`SessionBuilder::in_memory()` changes journal storage but does not remove these
dependencies from the crate. The default root feature also includes the Ollama
HTTP adapter, whose native HTTP configuration needs separate browser review.

For the locked dependency graph, `cargo tree -p pi-rs-core --target
wasm32-unknown-unknown -i getrandom@0.3.4` shows the `jsonschema`/`ahash`
path to `getrandom`. Its browser backend requires explicit feature and target
configuration; see [getrandom 0.3's WebAssembly documentation](https://docs.rs/crate/getrandom/0.3.4/source/README.md).
UUID randomness and any other resolved `getrandom` versions also need a
version-specific audit. Enabling random-number support alone does not provide
browser implementations of the filesystem, process, networking, or storage
contracts.

The public CI workflow includes a **non-gating diagnostic** running
`cargo check --locked -p pi-rs-core --target wasm32-unknown-unknown`. Its log
and job summary report the result; a successful compile would still not prove
that a browser can run an agent, persist a session, or call a provider. The
native example does not test WASM, Tauri, or a browser. Browser support needs
an explicit platform boundary, browser-safe randomness and transport, suitable
storage, and browser execution tests before it can be claimed.
