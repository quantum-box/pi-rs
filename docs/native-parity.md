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
| Agent runtime | Streaming, ordered tools, cancellation, steering, follow-up, recovery, usage | Provider-specific thinking and complete multimodal event handling |
| Terminal | Default `pi` startup, Unicode multiline editor and kill ring, paste, scrolling, prompt history, Markdown and syntax highlighting, tool panels and collapse, visible thinking blocks, file completion, external editor, user keybindings, cancel, session controls | Full Markdown/table and diff layout fidelity, image display, regular terminal mode and remaining application actions |
| CLI modes | Interactive, `--print`, `--mode json`, basic `--mode rpc`, existing subcommands | Full Pi argument and output compatibility |
| Local tools | Text and image read, paging, write, batch/legacy exact edits with diff output and file mutation locks, bash, Windows PowerShell selection, grep, find, ls | Complete output/error/truncation parity, Windows runtime validation |
| Sessions | Project-grouped journals, saved working directory, names and labels, searchable resume/tree/fork selectors, independent branch clone, native JSONL import, HTML/JSONL export, clipboard copy, GitHub gist share, compaction, read-only child review | Upstream JSONL import, richer tree navigation and branch summaries, session picker rename/delete/sort, Radius sharing |
| Model access | Ollama and configurable OpenAI-compatible chat endpoint, bearer token, asynchronous searchable model selector | Full provider catalog, provider-native adapters, thinking controls, retry configuration |
| Authentication | Host-supplied model token | Provider login/logout, OAuth, credential storage and refresh |
| Native extensions | Rust tool, context, policy, command, state/migration and compaction hooks; standard notes/TODO/plan/review; separately built executable loading | Full Pi event and interactive UI surface, explicit hot reload, scaffolding and package distribution |
| Project resources | Hierarchical AGENTS/CLAUDE and override files, trust persistence and prompt, user/trusted-project settings, skills discovery and forced invocation, argument-aware prompt templates, resource reload | Settings editor UI, package resource integration, resource globs and complete validation diagnostics |
| MCP | Existing AIP read-only Linear host integration; local MCP stdio and streamable HTTP clients; user/trusted-project configuration; namespaced tools and resources; environment/command credentials; add/remove/list commands | OAuth, reconnect and live tool-list updates, full exposure/deferred-tool behavior, elicitation and task polling |
| Codemode | Core tool invocation APIs | Sandboxed code execution and dynamic tool search |
| Themes | Pi theme schema, system/dark/light themes, indexed/hex/OKHSL/OKLCH colors, variables, user and trusted-project loading, `/theme`, `/reload` | Automatic system appearance notifications, live file-watch reload |
| SDK | Public Rust agent/session/model/tool/extension interfaces | High-level session builder and complete equivalents of Pi's SDK APIs |
| Packages | Cargo composition and native extension catalog | Package install/remove/update/list, dependency and resource management |

## Current RPC subset

The native JSONL interface supports `prompt`, `steer`, `follow_up`, `abort`,
`get_state`, `get_messages`, `new_session`, `set_model`, `compact`,
`get_session_stats`, `set_session_name`, `export_html`, and `shutdown`. Responses carry the request ID and a
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

### Session checkpoint

Session names, labels and working directories are optional journal entries, so
existing v1/v2 journals continue to load. Branch copies receive a new session ID
and remapped record references, and preserve settled tool results. Managed
host-bound sessions remain protected from local import and fork.

The CLI now exposes `--name`, `--session-dir`, `--resume [file]`, `name`, `label`,
`clone`, `fork`, `export`, `import`, `copy` and `share`. The corresponding terminal
commands include searchable `/resume`, `/tree`, `/fork`, `/model` and `/theme`
selectors. Export refuses to overwrite an existing file. `share` currently uses
the authenticated GitHub CLI to create a secret gist and only uploads when that
command is invoked. Native JSONL imports preserve the complete journal. Import
of the upstream TypeScript Pi format remains unimplemented.

The checkpoint passed 63 workspace tests. Its PTY fixture exercised streamed
reasoning, a local read tool, theme and session-name persistence, native branch
cloning, HTML/JSONL exports, model discovery/selection and terminal restoration.

### Resources checkpoint

Project configuration follows a stored trust decision, with an interactive
trust selector and explicit `--trust` / `--no-trust` overrides. Context files
remain readable independently of project trust. `/skills`, `/prompts`,
`/skill:name`, prompt argument expansion and `/reload` are available.

The local CLI's coding profile can read and write absolute paths, including
personal skill files outside the working directory. Embedded users of
`CodingTools::new` retain the existing workspace-scoped profile; the CLI selects
`CodingTools::local`. A regression test covers that distinction. The resource
PTY check covered granting project trust, advertising a discovered skill and
expanding a quoted prompt argument before the model request.

### Local MCP checkpoint

`pi mcp add`, `pi mcp remove`, `pi mcp list` and `/mcp` use user-level
`mcp.json` and trusted project overrides. The native client uses rmcp 3.9 for
stdio and streamable HTTP. It exposes namespaced server tools and resource
listing/reading. Configuration can reference environment variables or bounded
credential commands. Server processes are isolated into process groups/jobs,
and the CLI closes connections after each run.

Loopback fixtures verified stdio discovery and tool execution, HTTP bearer
headers, resource reading, and the model/tool/model round trip. Credentials
were absent from captured CLI output. This checkpoint exposes enabled tools
directly; OAuth, deferred exposure and live reconnection remain in the table.

### Image and file-tool checkpoint

`read` returns bounded PNG image content for PNG, JPEG, GIF, WebP, BMP and TIFF
inputs. It preserves alpha, avoids upscaling and reduces large images before
serialization. The Ollama-compatible adapter forwards image tool results as
image content blocks in a following user message, keeping the original tool
call/result pairing. The TUI displays an image placeholder rather than base64.
Actual inline terminal-image rendering remains unimplemented.

`edit` accepts both batch and legacy single replacements, emits a unified diff,
and serializes cooperating file mutations across processes. The Windows coding
profile selects PowerShell. That platform-specific branch has not yet been
exercised on Windows.
