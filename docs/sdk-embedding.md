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
the state is `Send + Sync`. The standalone [Tauri SDK fixture](../fixtures/tauri-sdk/)
compiles real Tauri 2 command macros and managed state against `pi-rs-core`.
Its mock-runtime IPC tests invoke two prompts, an idle abort, and close, check
session reuse, and verify that close cancels active and queued prompts, including
one still starting before run registration. A separate integration test composes
the public fixture commands with an application command. Run
it on a native host with Tauri's platform
prerequisites installed:

```sh
cargo test --locked --manifest-path fixtures/tauri-sdk/Cargo.toml
```

The fixture disables Tauri's default features and does not build the Wry
webview runtime. It uses a fake model and Tauri's mock runtime: it does not test
a real webview, frontend, application lifecycle, network provider, or package.
The repository workflow runs the fixture on macOS. Linux builds may require
GTK development libraries even without Wry. The [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/)
describe the platform dependencies.

The following is a Tauri 2 integration sketch using the same pattern. It
needs a Tauri project and its normal generated configuration. Add `pi-rs` (with the
default `ollama` feature) and `tokio` with the `sync` feature to `src-tauri`.
Use a local path to your checkout or a pinned Git revision of this repository.
The fixture's `configure` installs one invoke handler and replaces any handler
already set on the builder. To integrate with other application commands, use
the fixture's public `session_state` and `commands` in one handler, as exercised
by [the composition test](../fixtures/tauri-sdk/tests/composition.rs).
Closing requests SDK shutdown before waiting for the session lock. The SDK
cancels the active run and makes later run tokens inherit cancellation; ordinary
`abort` still affects only the current run. The fixture rejects queued prompts
and waits for an admitted prompt to settle so SDK cleanup can finish. Close can
wait if startup itself never settles. This example uses an in-memory session
without extension hooks or tools; cancellation cannot undo work already begun.

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

An experimental text-only browser host uses the same Rust `AgentSession`, a
Fetch gateway, and localStorage guarded by Web Locks. Its Chrome integration
checks execution, communication, cancellation, timeout, lock exclusion and
journal restoration after page reload. See [Browser SDK](browser-sdk.md) for
build instructions, the gateway contract, storage semantics and limitations.
It does not yet provide native tools, extensions, provider adapters or the
complete SDK surface. The native Tauri fixture does not test this browser host.
