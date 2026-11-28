# Embedding the Rust SDK

The Rust SDK currently targets native hosts. Tauri's Rust backend can own a
session and expose selected operations to its webview through Tauri commands;
the webview does not need to compile pi-rs to WebAssembly. The SDK does not
automatically install the workbench's tools, load its configuration, or manage
credentials for an embedding host.

## Host-owned model catalog

`ModelCatalogBuilder` collects already constructed clients by exact provider
and model name. Its immutable `ModelCatalog` creates a `SessionBuilder` and
resolves provider-qualified historical selections when sessions reopen or
navigate branches. This is useful when two providers use the same model name:
the SDK never guesses a provider or silently replaces an existing binding.

Use `catalog.models()` to list descriptors and `catalog.find(provider, model)`
to look up one exact pair. A registered model can have a descriptor with
`info: None`. Use `catalog.providers()` to list provider identities that have
registered models, or `catalog.find_provider(provider)` to inspect a single
provider and iterate its models. These reads expose only identities and
host-supplied metadata. They do not load config, access credentials, probe
providers, or infer a default route. Listings are ordered by the registered
identities, and lookup is case-sensitive.

A host can register the model's selectable reasoning levels with
`ModelCatalogBuilder::set_thinking_levels`. Use
`catalog.thinking_levels(provider, model)` to populate a picker in the host's
order. An empty list means the host declared no selectable levels. `None`
means that no list is available. This is the host-owned catalog equivalent
of [Pi's available thinking levels](https://github.com/earendil-works/pi/blob/db79712854e5154f48f10564c3f235abadfcefdc/packages/coding-agent/src/core/agent-session.ts#L2827-L2838).
The list describes capability metadata. The host still chooses a client
configured for the selected level.

For older journals that recorded a model name without its provider, the host
may explicitly authorize that name with `session_builder_with_legacy_model`.
Use this only when the host knows which provider the old name represents; the
ordinary `session_builder` deliberately leaves such histories unbound.

```rust,no_run
use std::sync::Arc;
use pi_rs::{CatalogModelInfo, Model, ModelCatalogBuilder};

fn session_builder(client: Arc<dyn Model>) -> pi_rs::Result<pi_rs::SessionBuilder> {
    let mut models = ModelCatalogBuilder::new();
    models.register_with_info(
        "my-provider",
        "my-model",
        client,
        CatalogModelInfo {
            display_name: Some("My model".into()),
            ..Default::default()
        },
    )?;
    models.set_thinking_levels(
        "my-provider",
        "my-model",
        ["off", "low", "high"].map(str::to_owned),
    )?;
    let catalog = models.build();
    let model = catalog
        .find("my-provider", "my-model")
        .expect("registered above");
    catalog.session_builder(model.provider, model.model)
}
```

Clients, endpoints and credentials remain the host's responsibility. Journal
identities are used only for exact lookups in this trusted snapshot; unknown
identities do not cause a network request or automatic fallback. Build a new
catalog after changing provider or model definitions, credentials, or endpoint
policy. Share the replacement snapshot for new lookups and sessions. Previously
cloned catalogs and existing sessions retain their original bindings. Hosts
needing automatic fallback can provide their own `HistoricalModelResolver`.
This provider-neutral API does not depend on Tauri or a desktop runtime.

### Reload saved keys and model routes

Native hosts using the workspace's `pi-rs-workbench` crate can rebuild a live
catalog with `AuthFileReadGrants` and `ModelClientFactory`. Prepare the exact
provider and endpoint grants and the complete set of model definitions for
each reload. Read the granted file once, then build every client from that
credential snapshot.

```rust,no_run
use pi_rs::ModelCatalog;
use pi_rs_workbench::{
    auth_factory::AuthFileReadGrants,
    model_factory::{ModelClientFactory, NativeModelDefinition},
};

fn reload<'a>(
    live: &mut ModelCatalog,
    grants: &AuthFileReadGrants,
    definitions: impl IntoIterator<Item = NativeModelDefinition<'a>>,
) -> pi_rs::Result<()> {
    let factory = ModelClientFactory::new(grants.load()?);
    let candidate = factory.catalog(definitions)?;
    *live = candidate;
    Ok(())
}
```

If the load or build fails, the previous catalog remains published. A missing
auth file yields an empty credential snapshot, so building a defined route
that requires a key fails. For a shared catalog, build the candidate on a
blocking worker, then take the host's state lock only to publish the completed
catalog. Serialize reloads or check a host-owned revision before publishing
so an older rebuild cannot overwrite a newer one.

Use the replacement catalog to start new sessions. Retained sessions, clients,
and catalog clones keep their original credentials and routes. Release or
close them when they should stop using those routes. Revoke a retired key at
the provider if it must stop working regardless of the client's lifetime.

For explicitly granted `models.json` entries, a host can use
`ModelFileReadGrants::load(&factory)` to build the candidate. That loader can
return an empty catalog for a missing file, so check any required model
identities before publishing. These file reads capture separate inputs.

OAuth acquisition and refresh stay with the host. Use `HostAuthBuilder` to
bind a host-supplied bearer credential to an exact supported route. In Tauri,
keep the grants, rebuild, and credential values in the Rust backend. Browser
Wasm clients continue to use the host-configured gateway.

## Native quickstart

The [Ollama SDK example](../crates/ollama/examples/sdk_chat.rs) creates an
in-memory session, sends a prompt to the local server, and closes the session:

```sh
ollama pull qwen2.5:7b
cargo run --locked -p pi-rs-ollama --example sdk_chat
```

Set `PI_RS_EXAMPLE_MODEL` to use another installed model. For a browser-only
app, a native server can own the session and expose an application-specific API.

## Session statistics

`AgentSession::session_stats()` reads the Pi-visible session ID, the
SDK-managed session path when available, durable totals, and current branch
context usage from one journal snapshot. The path is available for sessions
opened with the SDK or forked to a path. In-memory sessions and journals passed
to `SessionBuilder::build` have no SDK-managed path. The call returns an error
if the session is closed or the journal is busy.

Set `Limits::context_window` to include branch context usage. A missing or
zero window leaves `context_usage` absent. After compaction, the token count
can remain unknown until a fresh measured response. The snapshot makes no
provider calls. Hosts that want provider discovery can call the existing
`AgentSession::context_usage().await` explicitly.

The `totals` field uses the existing `SessionStats` accounting and native
session UUID. This includes alternate branches and compacted history. The
workbench RPC counts Pi-projected message entries and adds usage from its
native model operations, so message counts and usage totals can differ from
the SDK snapshot.

## Model cycling

Call `AgentSession::cycle_model` with the current ordered snapshot of models
your host authorizes and, optionally, its ordered scope. A nonempty scope is
filtered by exact, case-sensitive provider and model pairs. It stays in effect
even if every scoped entry is unavailable. An empty scope uses snapshot order.
Cycling wraps in either direction. If the current model is absent, forward
selects the first eligible model and backward selects the last, even when only
one is eligible. The active journal selection supplies the current identity.
An empty list or a lone current model returns `Ok(None)` without binding a
client, writing a selection, or running selection hooks.

The SDK asks the host to bind only the chosen model. It passes the requested
thinking level with the explicit scoped level first, followed by the snapshot's
per-model default, the host global default, and the current session level. The
SDK uses its implicit `off` when none is set. The binding callback should return
a client configured for the effective level and that level after applying
provider capabilities. A host can use
`ModelCatalog::get` for a prebuilt client. Hosts that use the native workbench
factory can prepare clients using `ModelClientFactory` before passing the
snapshot. The SDK performs no discovery or credential lookup during selection.
Each cycled journal selection retains its own client binding, including across
in-process forks and checkouts. Native journal records carry a binding marker,
so cycling requires a session store with atomic record metadata. When different
clients can share a provider, model, and thinking level, read the identifier
with `Session::model_binding_id_at(record_id)` and associate it with the client's
configuration in host-owned storage. Use `SessionBuilder::cycled_model_resolver_with_id`
to look up that mapping on reopen or historical checkout. Check the recorded
provider, model, and level against the host mapping before returning a client.
The opaque UUID follows the selection through native forks and checkout.
The journal stores the ID and selection details. Credentials remain with the host.

`SessionBuilder::cycled_model_resolver` supports hosts that resolve solely by
provider, model, and effective thinking level. Both callbacks apply the host's
authorization policy and can return `None` to leave the selection unresolved.
On reopen, a configured cycle resolver reauthorizes the active SDK binding,
even when the registry has a cached client for the journal. It also reauthorizes
the bindings needed to resume pending tree triggers. Reopen reserves those
bindings during authorization so a concurrent revocation prevents publishing
the clients. Other SDK bindings are resolved when their branches are selected.
A malformed or incomplete SDK binding on the active selection causes reopen to fail.
`HistoricalModelResolver` still handles ordinary provider-qualified model
history. Explicit SDK model setters keep their existing identity-based behavior.

The SDK records the chosen model and effective thinking level together in the
session journal, then delivers selection hooks with source `cycle`. A host owns
the scope and any application-wide defaults, including whether to persist the
new model to settings. See the
[pinned upstream cycling implementation](https://github.com/earendil-works/pi/blob/db79712854e5154f48f10564c3f235abadfcefdc/packages/coding-agent/src/core/agent-session.ts#L2701-L2774)
for the reference ordering and scope behavior.

## Thinking level

`AgentSession::set_thinking_level` and `cycle_thinking_level` use
`ThinkingLevelCapabilities` for the current registered provider and model.
For a legacy provider-less selection, select an explicit provider model first.
Supply unique, case-sensitive effective levels in forward cycle order. Cycling
wraps and starts at the first level if the current level is absent. An empty
list means thinking changes are unsupported. The SDK treats an unset level
as `off`. An unsupported or unchanged selection returns `Ok(None)`.

The synchronous binding callback receives the exact provider, model, and
requested level. Return a `ThinkingLevelBinding` from the host's authorized
configuration, with the client set up for the effective level after any
provider clamp. The SDK checks the returned identity against the selected
model and the effective level against the supplied capabilities. An already
effective request skips the callback. If a clamp leaves the level unchanged,
the SDK discards the returned binding and performs no selection.

A change atomically commits the effective level and its historical client
binding, then emits `thinking_level_select`. This uses the same native binding IDs
and reopen callbacks as model cycling. Capability policy, client authorization,
and any application-wide default persistence belong to the host.
[Pinned Pi thinking-level implementation](https://github.com/earendil-works/pi/blob/db79712854e5154f48f10564c3f235abadfcefdc/packages/coding-agent/src/core/agent-session.ts#L2782-L2857)

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
To use a host-configured session, construct it with `SessionBuilder` and pass it
to `session_state_from_session`. The host owns the choice and authorization of
its journal, resources, model and extensions; the IPC commands do not accept
those settings. This constructor neither reloads resources nor switches sessions.
Closing requests SDK shutdown before waiting for the session lock. The SDK
cancels the active run and makes later run tokens inherit cancellation; ordinary
`abort` still affects only the current run. The fixture rejects queued prompts
and waits for an admitted prompt to settle so SDK cleanup can finish. After SDK
close completes, the state drops its owned session and releases its journal
handle; another owner may then reopen it. Close can wait if startup itself never
settles. The following minimal sketch uses an in-memory session without extension hooks
or tools; it does not implement the fixture's supervised close lifecycle.
Cancellation cannot undo work already begun.

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

### Live tool results

Subscribe with `AgentSession::subscribe_tool_progress()` before starting work.
Direct prompts and runs started through cloned controls publish owned
`Arc<ToolProgressUpdate>` values with the tool call ID, name, argument preview,
partial result, and parent call ID for nested tools. The payload
keeps the result format supplied by the tool. Hosts can use it to update their
tool renderer while the call is running. Final results still arrive as
committed records on `AgentSession::subscribe()`.

The progress stream retains up to 32 updates per session. A slow subscription
reports `RecvError::Lagged` and can then continue with the retained updates.
Closing or dropping the session closes the stream after buffered updates are
drained. Progress is transient and reopening the journal cannot replay it.

In Tauri, create the receiver while you have access to the managed session,
then release the state lock before reading the stream. An authorized frontend
can provide a `tauri::ipc::Channel<ToolProgressUpdate>` to a host command.
The forwarding task can send `update.as_ref().clone()` through that channel
after applying the host's disclosure policy. Arguments and partial results may
contain private data. Handle lag by resuming with the newest available updates
and reconcile the display with the final committed result. Stop forwarding
when the channel disconnects or the SDK receiver closes.

This is a Rust API in `pi-rs-core` and also compiles for `wasm32-unknown-unknown`.
The JavaScript browser SDK uses a separate `wasm-bindgen` wrapper around the core
session. Its prompt and resume methods return promises for completed answers,
and its current bindings do not expose this subscription.

## Browser WASM status

The standalone `pi-rs-tool-arguments` crate isolates complete tool-argument
parsing and incremental JSON previews from the native agent and its filesystem,
process, network, and randomness dependencies. Rust embeddings can depend on
this crate directly to build those values without depending on `pi-rs-core`.
The existing `pi_rs::parse_tool_arguments` export remains available, and
`pi_rs::StreamingJson` exposes the same preview parser used by the terminal
and RPC frontends. Preview values are not final tool validation: call
`parse_tool_arguments` on the complete input before executing a tool.

CI checks this isolated crate with `cargo check --locked -p
pi-rs-tool-arguments --target wasm32-unknown-unknown`, and its parser tests run
on native targets. This is a compile check for a library component; it does not
run the component in a browser or establish browser support for the SDK.

An experimental text-only browser host uses the same Rust `AgentSession`, a
Fetch gateway, and localStorage guarded by Web Locks. Its Chrome integration
checks execution, communication, cancellation, timeout, lock exclusion and
journal restoration and resumption of an interrupted run after page reload.
See [Browser SDK](browser-sdk.md) for build instructions, the gateway contract, storage semantics and limitations.
It does not yet provide native tools, extensions, provider adapters or the
complete SDK surface. The native Tauri fixture does not test this browser host.
