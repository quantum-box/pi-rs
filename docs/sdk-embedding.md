# Embedding the Rust SDK

The Rust SDK currently targets native hosts. Tauri's Rust backend can own a
session and expose selected operations to its webview through Tauri commands;
the webview does not need to compile pi-rs to WebAssembly. The SDK does not
automatically install the workbench's tools, load its configuration, or manage
credentials for an embedding host.

## Native quickstart

The [Ollama SDK example](../crates/ollama/examples/sdk_chat.rs) creates an
in-memory session, sends a prompt to the local server, and closes the session:

```sh
ollama pull qwen2.5:7b
cargo run --locked -p pi-rs-ollama --example sdk_chat
```

Set `PI_RS_EXAMPLE_MODEL` to use another installed model. For a browser-only
app, a native server can own the session and expose an application-specific API.

## Native state and Tauri

The [native host example](../examples/native_sdk_host.rs) demonstrates shared
session state using an offline fixture; its tests check session reuse and that
the state is `Send + Sync`. The following is a Tauri 2 integration sketch using the same
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
`in_memory()` with `open()` and a private application-owned path outside the
workspace exposed to tools, and manage session lifecycle and reopen errors in
the host.

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
The [initial public CI diagnostic](https://github.com/quantum-box/pi-rs/actions/runs/43569708117/job/132371210751)
failed with exit code 101: `errno` 0.3.14 rejects the unknown target OS. The
locked target tree reaches it through `cap-std` / `cap-primitives` → `rustix`
→ `errno`. This is the first reported failure, not an exhaustive blocker list.

For the locked dependency graph, `cargo tree -p pi-rs-core --target
wasm32-unknown-unknown -i getrandom@0.3.4` shows the `jsonschema`/`ahash`
path to `getrandom`. Its browser backend requires explicit feature and target
configuration; see [getrandom 0.3's WebAssembly documentation](https://docs.rs/crate/getrandom/0.3.4/source/README.md).
The locked `uuid` 1.29.2 enables `v4` but none of its browser randomness
backends; its [target-specific implementation](https://docs.rs/crate/uuid/1.29.2/source/src/rng.rs)
requires an explicit `js`, `rng-getrandom`, or `rng-rand` feature on this
target. These are separate dependency paths. Other resolved randomness
dependencies need their own target-specific audit. Enabling randomness alone
does not provide browser implementations of the filesystem, process,
networking, or storage contracts.

The public CI workflow includes a **non-gating diagnostic** running
`cargo check --locked -p pi-rs-core --target wasm32-unknown-unknown`. Its log
and job summary report the result; a successful compile would still not prove
that a browser can run an agent, persist a session, or call a provider. The
native example does not test WASM, Tauri, or a browser. Browser support needs
an explicit platform boundary, browser-safe randomness and transport, suitable
storage, and browser execution tests before it can be claimed.
