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
| Terminal | Default `pi` startup, Unicode multiline editor and kill ring, paste, scrolling, prompt history, Markdown, aligned tables, links and syntax highlighting, tool panels and collapse, visible thinking blocks, file completion, external editor, user keybindings, cancel, session controls | Remaining Markdown and diff layout fidelity, inline image display, regular-mode history redraw and remaining application actions |
| CLI modes | Interactive, `--print`, `--mode json`, basic `--mode rpc`, existing subcommands | Full Pi argument and output compatibility |
| Local tools | Text and image read, paging, write, batch/legacy exact edits with diff output and file mutation locks, bash, Windows PowerShell selection, grep, find, ls | Complete output/error/truncation parity, Windows runtime validation |
| Sessions | Project-grouped journals, saved working directory, names and labels, searchable resume/tree/fork selectors, independent branch clone, native and upstream JSONL import, HTML/JSONL export, clipboard copy, GitHub gist share, compaction, read-only child review | Remaining upstream message variants and display fidelity, richer tree navigation and branch summaries, session picker rename/delete/sort, Radius sharing |
| Model access | Ollama and configurable OpenAI-compatible chat endpoint, bearer token, asynchronous searchable model selector, branch-local model/thinking selection, reasoning-effort control | Full provider catalog, provider-native adapters, capability-aware thinking levels, retry configuration |
| Authentication | Host-supplied model token | Provider login/logout, OAuth, credential storage and refresh |
| Native extensions | Rust tool, context, policy, command, state/migration and compaction hooks; standard notes/TODO/plan/review; separately built executable loading, scaffolding and native package distribution | Full Pi event and interactive UI surface, explicit hot reload |
| Project resources | Hierarchical AGENTS/CLAUDE and override files, trust persistence and prompt, user/trusted-project settings, skills discovery and forced invocation, argument-aware prompt templates, package resources and reload | Complete settings editor UI, resource globs and complete validation diagnostics |
| MCP | Existing AIP read-only Linear host integration; local MCP stdio and streamable HTTP clients; user/trusted-project configuration; namespaced tools, resources, URI templates and prompts; deferred tool discovery, codemode exposure and task polling; environment/command credentials; add/remove/list commands | OAuth, reconnect and live tool-list updates, elicitation |
| Codemode | Bounded QuickJS execution, journaled native tool calls, parallel reads, transactional branch-local store, discovery, text/image output and console capture | Non-chat model invocation, complete output/image fidelity and limits |
| Themes | Pi theme schema, system/dark/light themes, indexed/hex/OKHSL/OKLCH colors, variables, user and trusted-project loading, `/theme`, `/reload`, live custom-file reload | Automatic system appearance notifications |
| SDK | Public Rust agent/session/model/tool/extension interfaces, owned session builder, controls, event subscriptions, in-memory/persistent forks, JSONL export, compaction and lifecycle shutdown | Complete resource/model/auth factories and remaining Pi SDK equivalents |
| Packages | Native Cargo/git/local install, remove, update, list, per-source build cache, dependency builds and filtered resources | Published-crate resource bundles and complete source/filter compatibility |

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

### Model-selection checkpoint

`/thinking`, Shift+Tab and `--thinking` select a reasoning effort. Selection is
persisted with the active session branch alongside the model. The local Ollama
adapter maps off to `reasoning_effort: none`, and maps higher Pi-only levels to
Ollama's accepted range. Providers still decide which efforts their models
support; capability-aware filtering awaits the provider catalog.

The model and thinking selectors accept Ctrl+S to save a startup default.
`/settings` currently edits tool-output and thinking-block visibility and saves
model/thinking defaults. `/settings-json` displays the resolved settings.

### Regular terminal checkpoint

`--tui-mode regular` (or `tuiMode` in settings) keeps completed messages in
normal terminal scrollback. The fullscreen default remains available. Both
modes passed the same PTY sequence for project trust, quoted prompt expansion,
model/thinking selection, settings, tools, exports and terminal restoration.
Changing display options does not redraw history already committed to normal
terminal scrollback. Full regular-mode redraw fidelity remains open.

Ctrl+P / Ctrl+Shift+P cycle discovered models. Custom session-action bindings
and Ctrl+X copying use the same journal operations as their slash commands.
The checkpoint passed 76 workspace tests and all-target Clippy.

### Owned SDK checkpoint

`SessionBuilder` and `AgentSession` own a model, registry and journal across
multiple prompts. `SessionControl` supports steering, follow-up input and abort
from another task. Subscribers receive bounded, owned events without blocking
the agent loop. The API also exposes extension commands, compaction, summary,
checkout and in-memory forks. Explicit asynchronous close releases shared
extensions after the last fork closes. Managed host-bound sessions continue to
use the existing host API.

SDK tests cover follow-up consumption, abort during a model stream, shared
connection lifetime and retaining an independent fork history.

### Persistent runtime checkpoint

Terminal and RPC prompts now reuse a runtime rather than rebuilding model and
MCP clients for every input. Journal locks are released while idle, so session
inspection and naming remain usable. Model/thinking changes, new sessions,
extension commands, compaction and review can use that runtime; `/reload`
rebuilds it for changed configuration.

A stateful MCP fixture retained counter values 1, 2 and 3 across prompts, a
notes command and cancellation, using one server process. A separate RPC
fixture retained its connection across `new_session`, verified idle abort,
and shut down cleanly. Fullscreen and regular-mode PTY regression checks pass.

### Native package checkpoint

`init-extension`, `install`, `remove`, `update` and `list` manage native Cargo,
git and local packages. Builds are cached, serialized per source and validated
before settings change. User and trusted-project declarations support resource
filters. Packaged extensions, skills, prompts and themes load through the
normal terminal/CLI/RPC resource paths. TypeScript/npm runtime compatibility is
outside the requested native Rust equivalence.

### System prompt checkpoint

The native default uses Pi's coding-assistant preamble and separate tool,
rule, project-context, skills and working-directory sections. The prompt names
only registered tools. Skill discovery is exposed to the model when a usable
file-reading tool is registered; manual skill invocation remains available.
Explicit `--system-prompt` content takes precedence over project SYSTEM.md.
Skill metadata is XML-escaped and relative-path guidance matches native skill
loading. Tests cover reader gating and custom-prompt precedence.

Custom theme files are now watched while the terminal is running. Valid edits
redraw the active view. Invalid edits retain the last valid colors and show one
diagnostic per changed file contents. A PTY check covers live updates and
terminal restoration. System appearance discovery still needs terminal color
query support.

Markdown tables now calculate Unicode-aware column widths, preserve alignment,
wrap cells, and use a stacked layout when a terminal cannot fit the columns.
Link destinations and strikethrough formatting are rendered. Table tests cover
Japanese text and narrow terminal widths.

The core Session and owned SDK also support no-overwrite JSONL export and
persistent branch forks. Forks receive a fresh session identity and can be
reopened independently. CLI import/export/fork now use the same atomic snapshot
publication path. Tests verify independent continuation and refusal to replace
existing files.

### Tool selection checkpoint

The CLI, TUI and RPC use the native default read/shell/edit/write selection.
`--tools`, `--exclude-tools`, `--no-tools`, `--no-builtin-tools`, and
`defaultTools` control advertised and executable tools. Allowlist patterns,
exact `+name`/`-name` changes, MCP namespace selection and exclusions are
supported. Existing legacy `read_text`/`count_lines` integrations remain
available, and `count_lines` retains its explicit command-policy flag.

MCP resource discovery now includes parameterized URI templates. Servers can
also expose prompt discovery and prompt expansion through namespaced tools.
An authenticated HTTP fixture verifies all three operations through a model
tool roundtrip. Server/tool exposure overrides apply to these synthetic tools.

A text response that reaches the provider output limit is retained with its
`length` finish reason and a visible limit notice. Incomplete tool-call
responses remain unsuccessful and are not dispatched. CLI regression tests
verify that text and provider metadata survive the output limit.

MCP tasks now persist their remote job reference before polling for a terminal
result. Polling cancellation and timeout retain that reference, and resuming
checks the accepted job without repeating its original invocation. An HTTP
fixture verifies one dispatch and two status checks. The owned SDK test also
cancels polling, exports and reopens the session, and verifies one dispatch
across both runs. Host-owned pending operations retain their reconciliation
contract.

The CLI task fixture also sends SIGINT during polling and resumes from the
saved journal in a new process. The remote task is dispatched once across both
processes. Connection identity normalizes the workspace path and includes
resolved credentials, so a resumed task cannot silently move to another
configured identity.

System prompt arguments accept literal text or existing files resolved from the
workspace. Repeatable `--append-system-prompt` values preserve their order in
both one-shot CLI and persistent RPC. Native `-e` and `-n` aliases select
extensions and session names.

### Deferred discovery checkpoint

MCP `deferred` exposure now hides tool schemas until `tool_search` loads a
match. A native BM25 ranker uses names, descriptions, schema fields and server
metadata. Selection state follows the journal branch and survives reopening.
Exact exposure overrides and wildcard matching are supported. CLI allowlists
without MCP names keep MCP discovery available without declaring all schemas.
Tools receive only their owning extension state through an optional invocation
hook; existing implementations continue through the original hook.

The core now supplies an orchestrating `ToolHost` for nested calls. Nested
operations retain individual journal records, use the same validation and
policies, and preserve uncertainty after cancellation. Tests cover denied and
invalid calls, concurrent reads and blocked replay after interruption. This is
the execution boundary for the pending codemode sandbox.

### Native codemode checkpoint

The native `codemode` tool accepts `{ "code": "..." }` and runs JavaScript in a
fresh QuickJS context. Scripts can call registered tools through `tools`, use
`searchTools`, `describeTool` and `describeNamespace`, emit `text` or `image`,
and retain JSON values with `store` / `load`. Store changes commit with a
successful tool result and follow journal branches. Failed scripts retain
partial output without committing their store. Filesystem, network and Node
APIs are absent from the script context. Memory, stack, execution time and
output have explicit bounds. Large text output is saved to a local file.

MCP tools default to codemode exposure. `--exposure direct`, `deferred`,
`codemode` and `hidden` select another mode when adding a server. The root
`autoEnableCodemode` MCP setting controls automatic activation. Explicit tool
exclusions also prevent script access. Programmatic shell calls return output,
exit status, truncation and elapsed time, including nonzero exit statuses.
Nested calls retain their own validation, policy checks and journal entries.

CLI fixtures verify file read/write and shell execution through scripts,
private-store persistence across processes, and default MCP discovery and
execution. Tests cover rollback, interruption, memory/stack limits, output
truncation and parallel script calls through the core execution host. The
native JSON code argument differs from Pi's provider-specific raw-code input.
Non-chat model APIs, complete image ordering and large-image behavior remain
open. Executable extensions still lack bidirectional host-tool invocation.

### Resource selection checkpoint

`--no-skills`, `--no-prompt-templates`, `--no-context-files`, `--no-themes`,
`--no-extensions` and `--no-mcp` control automatic resource loading. Explicit
`--skill`, `--prompt-template`, `--theme` and `--extension` entries remain
available. Skills accept file or directory paths without `--project`; the
existing named project-skill mode remains supported. Native built-ins can be
selected with `builtin:codemode`, `builtin:tool-search` and `builtin:mcp`.
These selections propagate to persistent CLI workers. The settings editor,
resource glob expansion and remaining CLI aliases still require parity work.

### Upstream session import checkpoint

The import command reads native journals and Pi v1–v3 JSONL snapshots without
modifying the source. It retains original entries alongside native events,
including branch topology, completed tool results, reasoning, usage, names,
labels, compaction boundaries and branch-local codemode store values. User and tool-result images reach the model
adapter as image content. Previously completed tools remain recorded results;
unfinished imported calls remain uncertain and require reconciliation.

Context edits are projected onto future model context while the imported raw
entries remain available. Remaining specialized message variants and system
prompt checkpoints still need parity work. Images currently appear as
attachment notices in the terminal.

### Model scope checkpoint

`--list-models [search]` lists models from the configured endpoint, with fuzzy
filtering and JSON output support. `--models` selects an ordered, deduplicated
scope using exact IDs, case-insensitive globs or fuzzy matches. The terminal
picker and cycling use that scope. Known thinking suffixes such as `:high` are
separated from the model ID, while Ollama tags such as `:4b` remain intact.
An explicit `--thinking` overrides the startup suffix. Cross-provider lookup
and capability-aware thinking still await the provider catalog.

### Context editing checkpoint

The core and owned SDK can replace or omit a user, assistant or tool-result
message from future model context. Edits are journal entries and follow the
active branch. Original messages, execution outcomes and usage remain intact.
Forks remap edit targets. Tool calls and results omitted by an edit are removed
as pairs before sending context to a provider.

Native entry points are `context-edit <id> <text>`, `context-edit <id> --delete`,
`/context-edit`, `/context-delete` and the RPC `edit_context` request with a
`target` record ID and `replacement`. Pi `context_edit` entries use the same
projection when imported, including text and image replacements.

### Multimodal input checkpoint

The CLI accepts `@file` arguments for text and supported images, using the same
image decoding and size limits as the local read tool. File text precedes the
prompt, and image bytes remain separate image content. Initial prompts passed
from a terminal now run inside the TUI and leave it available for further
conversation. Print and JSON modes remain one-shot.

The owned SDK exposes `prompt_with_images`. Idle RPC `prompt` requests accept
image blocks, including image-only input. Image-bearing input is committed as
one journal entry, restored across sessions and preserved in forks. Invalid
image encodings and oversized input are rejected before that entry is written.
Queued image input remains pending parity work, as does inline terminal image
rendering. CLI, RPC and a real terminal fixture verify the new input paths.
