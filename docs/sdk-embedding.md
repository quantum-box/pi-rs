# Embedding the Rust SDK

The SDK is a Rust library: `pi-rs-core` owns the agent session and a model
adapter such as `pi-rs-ollama` provides inference. The
[`sdk_chat` example](../crates/ollama/examples/sdk_chat.rs) creates an
in-memory session, sends a prompt, and closes the session. Run it from this
repository with a local Ollama server:

```sh
ollama pull qwen2.5:7b
cargo run --locked -p pi-rs-ollama --example sdk_chat
```

Use `SessionBuilder::open(path)` instead of `in_memory()` to persist a session.
Keep that file in the host application's data directory, outside any workspace
that tools can modify. Construct model clients and credentials in the host;
the core SDK does not discover credentials or endpoints for you.

`SessionBuilder` starts with an empty registry. A host can register explicitly
chosen tools and extensions before building the session. `AgentSession::prompt`
requires mutable access, so a host should serialize prompts for each session.
Use `subscribe()` for owned session events and call `close().await` when the
session and its shared extensions are no longer needed.

## Tauri and web applications

For Tauri, keep the `AgentSession` in the native Rust backend and call it from
the webview through Tauri commands. Forward selected session events to the UI;
do not send model credentials to JavaScript. Store each user's session in an
appropriate application data location. The example above exercises the same
native SDK without depending on Tauri.

For a browser-only web app, a server can own the Rust session and expose an
application-specific API to the browser. Direct use of the SDK as
`wasm32-unknown-unknown` in a browser has not been verified; filesystem,
process, networking and runtime dependencies need a separate compatibility
audit. An in-memory session alone does not establish browser compatibility.

The current native API and remaining compatibility work are tracked in
[native parity](native-parity.md) and [SDK tool inventory](sdk-tool-inventory.md).
