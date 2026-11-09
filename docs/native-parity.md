# Pi native feature parity

The reference is Pi v1.5.0, commit
`f9a26365568127461c47a07a37dcb42c140a3ac5`.
The requested compatibility is equivalent functionality implemented in Rust.

[Reference CLI](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/docs/cli.md)
· [Reference terminal](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/docs/usage.md)
· [Reference extensions](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/docs/extensions.md)

Full parity is in progress. Each row lists the implemented surface and the
remaining acceptance work for that surface.

| Surface | Implemented in Rust | Remaining work |
| --- | --- | --- |
| Agent runtime | Streaming, ordered tools, cancellation, steering, follow-up, recovery, usage | Provider-specific thinking and multimodal event handling |
| Terminal | Default `pi` startup, Unicode multiline editor and kill ring, paste, scrolling, prompt history, Markdown and syntax highlighting, tool panels and collapse, visible thinking blocks, file completion, external editor, user keybindings, cancel, session controls | Full Markdown/table and diff layout fidelity, image display, regular terminal mode, selectors and remaining application actions |
| CLI modes | Interactive, `--print`, `--mode json`, basic `--mode rpc`, existing subcommands | Full Pi argument and output compatibility |
| Local tools | Text read with paging, write, batch exact edits, bash, grep, find, ls | Image read, PowerShell, complete output/error/truncation parity |
| Sessions | Automatic project-grouped journals, continue, explicit resume, new session, tree listing, checkout, compaction, read-only child review | Names, labels, interactive selectors, fork/clone/import, HTML/JSONL export, copy, sharing |
| Model access | Ollama and configurable OpenAI-compatible chat endpoint, bearer token | Full provider catalog, provider-native adapters, model selection, thinking controls, retry configuration |
| Authentication | Host-supplied model token | Provider login/logout, OAuth, credential storage and refresh |
| Native extensions | Rust tool, context, policy, command, state/migration and compaction hooks; standard notes/TODO/plan/review; separately built executable loading | Full Pi event and interactive UI surface, explicit hot reload, scaffolding and package distribution |
| Project resources | Explicit AGENTS.md and named skill loading, user/trusted-project settings merge and atomic user updates | Trust persistence, full resource discovery, prompt templates, complete settings UI and resource reload |
| MCP | Existing AIP read-only Linear host integration | Local CLI server configuration, discovery, authentication, resource tools, exposure controls |
| Codemode | Core tool invocation APIs | Sandboxed code execution and dynamic tool search |
| Themes | Pi theme schema, system/dark/light themes, indexed/hex/OKHSL/OKLCH colors, variables, user and trusted-project loading, `/theme`, `/reload` | Automatic system appearance notifications, live file-watch reload, theme discovery selector |
| SDK | Public Rust agent/session/model/tool/extension interfaces | High-level session builder and complete equivalents of Pi's SDK APIs |
| Packages | Cargo composition and native extension catalog | Package install/remove/update/list, dependency and resource management |

## Current RPC subset

The native JSONL interface supports `prompt`, `steer`, `follow_up`, `abort`,
`get_state`, `get_messages`, `new_session`, `set_model`, `compact`,
`get_session_stats`, and `shutdown`. Responses carry the request ID and a
success flag. Prompt acceptance is separate from completion; wait for
`agent_settled`. The interface emits native journal events plus text updates.
Additional Pi RPC commands and complete event-schema parity remain in progress.

## Verification

- Run `cargo test --locked --workspace --all-features`.
- Run `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`.
- Exercise the terminal in a real PTY, including Unicode input, a tool call,
  notes persistence, cancellation, and restoring the terminal on exit.
- Compare each remaining surface against the pinned reference before marking
  that row complete. Existing core acceptance is recorded in [acceptance.md](acceptance.md).

### Terminal and native-extension checkpoint

The current implementation passed 41 workspace tests with all features, plus
Clippy for all targets with warnings denied. A PTY check exercised Japanese
input, streamed output, a real local `read` tool call against a fixture file,
notes persistence, and terminal restoration. A separately compiled Rust
`project_notes` executable was loaded twice, retained its journaled state, and
supplied that state through its request hook to a loopback model fixture.
These checks cover the native integration paths. Live provider sign-in and the
remaining feature rows above still need their own acceptance checks.

### Faithful terminal checkpoint

The terminal now uses Pi v1.5.0 theme assets and schema, with attribution in
`crates/workbench/assets/NOTICE`. It renders Markdown, highlighted code, shaded
user/tool panels, collapsed tool output and separately styled reasoning.
`Ctrl+O` and `Ctrl+T` toggle tool output and reasoning. Reasoning received from
the provider is journaled for restoration and remains absent from ordinary
non-trace CLI output.

User settings and keybindings use the existing `~/.pi-rs` agent directory (or
`PI_RS_HOME` / `PI_CODING_AGENT_DIR`). `/theme <name>` persists the selected
theme. `/reload` currently reloads themes and keybindings. Other resource reload
and unimplemented application key actions remain in the acceptance backlog.
