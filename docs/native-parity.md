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
| Terminal | Default `pi` startup, Unicode multiline editor and kill ring, text/image clipboard paste, scrolling, prompt history, Markdown, aligned tables, links and syntax highlighting, tool panels and collapse, visible thinking blocks and model progress, file completion, external editor, user keybindings, cancel, double-Escape navigation and tree filters | Remaining Markdown and diff layout fidelity, remaining application actions and clipped-image behavior |
| CLI modes | Interactive, `--print`, `--mode json`, `--mode rpc` commands with streamed message, turn, and tool lifecycle events, existing subcommands | Full Pi argument, session response, and event compatibility |
| Local tools | Text and image read, paging, write, batch/legacy exact edits with diff output and file mutation locks, bash, Windows PowerShell selection, grep, find, ls, concurrent user shell and model execution | Complete output/error/truncation parity |
| Sessions | Project-grouped journals, saved working directory, names and labels, searchable resume/tree/fork selectors, optional model-generated branch summaries, parent-session grouping, independent branch clone, native and upstream JSONL import, HTML/JSONL export, clipboard copy, GitHub gist share, compaction, read-only child review | Remaining upstream message variants and display fidelity, remaining tree layout and navigation hooks, Radius sharing |
| Model access | Ollama, configurable OpenAI-compatible chat endpoint and native Anthropic Messages, API keys and bearer tokens, asynchronous searchable model selector, branch-local model/thinking selection, Ollama capability-aware thinking levels, agent retry configuration | Additional provider catalogs and native adapters, provider-specific thinking for other services and live provider switching |
| Authentication | Host-supplied model token | Provider login/logout, OAuth, credential storage and refresh |
| Native extensions | Rust tool, context, policy, command, state/migration and compaction hooks; standard notes/TODO/plan/review; separately built executable loading, scaffolding and native package distribution; correlated dialogs, notifications, status, editor access and positioned widgets | Remaining Pi events and custom terminal components |
| Project resources | Hierarchical AGENTS/CLAUDE and override files, trust persistence and prompt, user/trusted-project settings, skills discovery and forced invocation, argument-aware prompt templates, package resources and reload | Complete settings editor UI and remaining validation diagnostics |
| MCP | Existing AIP read-only Linear host integration; local MCP stdio and streamable HTTP clients; user/trusted-project configuration; namespaced tools, resources, URI templates and prompts; deferred tool discovery, codemode exposure and task polling; environment/command credentials; OAuth PKCE login/logout, private credential persistence and refresh; add/remove/list commands; tool catalog notifications | Interactive scope upgrades |
| Codemode | Bounded QuickJS execution, journaled native tool calls, parallel reads, transactional branch-local store, discovery, text/image output and console capture | Non-chat model invocation and remaining execution-limit fidelity |
| Themes | Pi theme schema, system/dark/light themes, indexed/hex/OKHSL/OKLCH colors, variables, user and trusted-project loading, `/theme`, `/reload`, live custom-file reload and VT appearance notifications on Unix and Windows | Real Windows Terminal acceptance and remaining palette synchronization |
| SDK | Public Rust agent/session/model/tool/extension interfaces, owned session builder, controls, event subscriptions, in-memory/persistent forks, JSONL export, compaction and lifecycle shutdown | Complete resource/model/auth factories and remaining Pi SDK equivalents |
| Packages | Native Cargo/git/local install, remove, update, list, per-source build cache, dependency builds and filtered resources | Remaining native source/filter edge cases |

## RPC support

The native JSONL interface supports `prompt`, `steer`, `follow_up`, `abort`,
`get_state`, `get_messages`, `new_session`, `set_model`, `compact`,
`get_session_stats`, `set_session_name`, `export_html`, `get_available_models`,
`cycle_model`, `set_thinking_level`, `cycle_thinking_level`,
`get_available_thinking_levels`, `switch_session`, `fork`, `clone`,
`get_fork_messages`, `get_entries`, `get_tree`, `get_last_assistant_text`, `get_commands`,
`edit_context`, `bash`, `abort_bash`, `clear_queue`, `set_steering_mode`,
`set_follow_up_mode`, `set_auto_compaction`, `set_auto_retry`, `abort_retry`, and `shutdown`. Responses carry the request ID and a
success flag. Prompt acceptance is separate from completion; wait for
`agent_settled`. The interface emits native journal events alongside Pi-style
message start, update and end events, including indexed text and reasoning
blocks, tool calls, tool results and turn boundaries. Tool-call blocks become
available when their provider response is complete. Session entry and message
snapshot schemas, incremental tool arguments, and additional session events
remain in progress.

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
theme. `/reload` reloads settings, context, skills, prompts, native extensions, MCP,
keybindings and themes. Remaining application key actions are tracked above.

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
command is invoked. Native JSONL imports preserve the complete journal. The upstream Pi JSONL import is covered in the later import checkpoint.

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
were absent from captured CLI output.

For servers implementing the 2026-07-28 protocol, set
`"protocolVersion": "2026-07-28"` on the server entry in `mcp.json` to use
`server/discover` and subscribe to tool catalog changes. Legacy stdio servers
can send tool-list notifications after the default initialize handshake.
Unchanged notified catalogs are reused for up to 30 seconds. Legacy HTTP
servers and connections without an acknowledged subscription keep polling at
model request boundaries. Loopback checks covered notifications on both
transports and changes discovered through legacy HTTP polling.

### Image and file-tool checkpoint

`read` returns bounded PNG image content for PNG, JPEG, GIF, WebP, BMP and TIFF
inputs. It preserves alpha, avoids upscaling and reduces large images before
serialization. The Ollama-compatible adapter forwards image tool results as
image content blocks in a following user message, keeping the original tool
call/result pairing. The TUI displays an image placeholder rather than base64.
Kitty and iTerm2 terminals display supported images inline.

`edit` accepts both batch and legacy single replacements, emits a unified diff,
and serializes cooperating file mutations across processes. The Windows coding
profile selects PowerShell. Windows CI exercises PowerShell output, timeout and descendant-process cleanup.

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
Display options, theme changes and terminal resizing now redraw the retained
history in regular mode. Full redraws clear the viewport and scrollback before
replaying the transcript, matching the reference renderer.

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
Non-chat model APIs and remaining execution-limit differences remain open.

### Resource selection checkpoint

`--no-skills`, `--no-prompt-templates`, `--no-context-files`, `--no-themes`,
`--no-extensions` and `--no-mcp` control automatic resource loading. Explicit
`--skill`, `--prompt-template`, `--theme` and `--extension` entries remain
available. Skills accept file or directory paths without `--project`; the
existing named project-skill mode remains supported. Native built-ins can be
selected with `builtin:codemode`, `builtin:tool-search` and `builtin:mcp`.
These selections propagate to persistent CLI workers. The settings editor,
remaining CLI aliases still require parity work.

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
CLI, RPC and a real terminal
fixture verify the new input paths.

Image-bearing steering and follow-up messages now use the same durable queue as
text input. The owned SDK exposes `steer_with_images` and
`follow_up_with_images`; RPC accepts images while a response is running.
Steering is applied before follow-ups at the next response boundary. Queued
images survive cancellation and reopening, and their IDs prevent duplicate
consumption. Existing text-only `QueuedInput` callers continue through the
extended queue representation.

Package resource filters distinguish glob patterns from exact `+path` and
`-path` overrides. Exclusions apply before exact inclusions, and exact
exclusions take precedence regardless of array order. Skill-directory paths
resolve to their SKILL.md resource. Manifest directory roots still expand to
their declared resources, and filters remain limited to that declared set.

### Resource discovery checkpoint

Native executable extensions are discovered in user and trusted-project
extension directories, and configured files or directories resolve from their
own settings directory. CLI resource paths resolve from the working directory.
Skills, prompt templates and themes use the same scoped discovery. Explicit
CLI paths take precedence, followed by project resources and user resources.
Canonical paths prevent duplicate loading through aliases.

Resource exclusions and exact inclusion overrides apply to configured roots
and discovered paths. Built-in extension overrides retain separate user and
project layers, so a project override can re-enable a user-disabled built-in.
Theme discovery uses the filtered resource list and preserves live reload.
Fixtures verify automatic and configured native extensions, trust gating,
explicit overrides, project template precedence and live theme updates.

### CLI export and transient credentials

`--export <input> [output]` exports native or imported Pi sessions to HTML and
exits. The default destination uses the input basename with `.html`. Existing
destinations are preserved. HTML exports embed validated user and tool images
as data URLs under an image-only data policy.

`--api-key` overrides the model credential for the current process, including
model discovery and persistent CLI/RPC workers. The credential remains
process-local. Regression tests verify the selected bearer header and check
that session records and command output contain no key value.

### Regular-mode redraw checkpoint

Regular mode replays committed entries when visibility settings, theme colors
or terminal dimensions change. It also restores the view after an external
editor exits. The normal append path retains existing scrollback. A PTY fixture
verifies hide/show of existing thinking content, resizing, live theme changes
and terminal restoration. History eviction keeps the pending-entry boundary
consistent, so new messages continue to appear after the transcript limit.

### Inline terminal images checkpoint

Kitty and iTerm2 image protocols render user attachments and expanded tool
results in both terminal modes. Image normalization shares the local read
implementation, preserves alpha and avoids upscaling. Kitty uploads are reused
on redraw and resize. Fullscreen exit releases owned images, while regular
mode retains them in scrollback. Unsupported terminals retain text notices.

A PTY fixture verifies both protocols, user and tool images, cached redraw,
cleanup and terminal restoration. Protocol bytes were checked without a GUI
terminal image comparison. Partially clipped image blocks are skipped, and
regular-mode images appear when the current turn commits.

### Shell output checkpoint

Shell tools merge stdout and stderr as they arrive, retain the last 2,000 lines
or 50 KiB for model-facing results, and publish complete truncated output to a
temporary file. Codemode receives up to 1 MiB, with beginning and end retained
when larger output is omitted, and a `full_output_path` for recovery. UTF-8
boundaries remain intact. Nonzero exits continue to return structured status to
scripts and errors to direct callers.

Fractional explicit timeouts retain output already received. Shell execution
has no separate default timeout, while the caller's configured tool deadline
continues to apply. A shell that exits while descendants hold its output pipes
returns after a bounded drain period. Unit and CLI fixtures cover line/byte
limits, file recovery, Unicode boundaries, timeout output and inherited pipes.

### Text paging checkpoint

Text reads now scan files with bounded memory, including files larger than
4 MiB. Pages retain complete lines and UTF-8 boundaries, report the next
`offset`, reject offsets beyond the file, and explain when a single line
exceeds the byte limit. Explicit line limits report remaining lines. Image
normalization and the separate write-size limit retain their existing bounds.

### RPC navigation checkpoint

RPC clients can discover and cycle endpoint models, set or cycle thinking,
inspect native entries and the session tree, retrieve forkable user messages,
and clone, fork or switch sessions. Selection restores with the chosen branch.
Forking returns the selected user text and starts its copy before that message.
Source journals remain intact. `get_entries` accepts a native record ID in
`since`; entries and tree nodes retain the native journal schema.

The reported thinking levels are the native selectable levels. Provider-specific
capability filtering and full upstream RPC message/event schemas remain open.

### Session reference checkpoint

`--session-id` opens or creates a named journal in the workspace's session
storage. `--session` and `--resume` resolve stored filename or native UUID
prefixes, with ambiguity checks. `--fork` reads a native or imported Pi snapshot
and creates an independent branch in the current workspace. Source reads stay
read-only and existing destinations remain protected.

The named ID is a storage alias. Native journals retain their UUID identity.
Aliases allow 1–128 ASCII letters, digits, dots, underscores and hyphens, with
an alphanumeric first and last character. Argument conflicts are checked before
opening a session. RPC restoration shares the CLI's model/scope selection logic.

### Session selector checkpoint

The resume selector supports renaming with Ctrl+R, deletion with Ctrl+D and an
explicit confirmation, and recent/fuzzy ordering with Ctrl+S. Session-specific
keybinding overrides apply inside the selector. Active and managed sessions
are protected from deletion. Renaming the active session updates its title.
Editor Ctrl+D behavior remains separate from selector controls. Threaded
parent/child grouping remains open.

### Native reload checkpoint

A rebuilt Rust extension is reloaded through `/reload`, including its changed
tool definitions. Journaled extension state remains available after the worker
and registry restart. A PTY fixture built two versions of a Rust executable,
invoked each version's tool through a model round trip, and verified that the
same saved notes reached both requests.

### Streaming search checkpoint

`grep` streams files with the native ripgrep libraries, supports surrounding
context lines, bounds long displayed lines at 500 characters, and reports match
and byte limits. Large text files remain searchable. Adjacent context ranges
are combined. Search memory is bounded at 64 MiB, and the configurable context
radius is limited to 1,000 lines.

`find` uses a 1,000-result default and reports result limits. `ls` sorts names
case-insensitively, identifies linked directories, reports entry limits and
labels empty directories. Head truncation preserves complete lines where they
fit. Fixtures cover large-file search, Unicode and literal patterns, context,
empty directory output and continuation notices.

### Published native package checkpoint

Cargo registry installs discover the compiled package source from Cargo's JSON
artifact report and retain its declared skills, prompts and themes in the Pi
package cache. Clearing Cargo's source cache does not remove those resources.
Snapshots are content-addressed and bounded to 128 MiB and 20,000 files.

Installed executables use separate generations. A failed update retains the
previous index and executable, while successful updates keep build-cache reuse.
Fixtures cover a local sparse registry, version and resource discovery, resource
use after Cargo cache removal, and a failed Rust rebuild followed by continued
use of the previous executable.

### Codemode image output checkpoint

Untruncated script output preserves interleaved text and images, including in
expanded terminal tool panels. Repeated image bytes share one saved file. Large
images can remain inline within a bounded 7 MiB result budget; omitted images
carry a visible notice and retain their file references. Other native tools keep
their existing default result limit through a source-compatible tool hook.

Image-save and full-output-save failures report diagnostics while preserving a
successful script's state commit. Truncated text retains its beginning and end,
a token-count notice and a full-output file when saving succeeds. A real PNG
fixture verifies large images from MCP through a script, ordered content,
multimodal forwarding and durable private state.

Local CLI tool deadlines are optional through `--tool-timeout-secs`. Without an
explicit deadline, scripts and their nested tools can continue beyond 30 seconds,
up to an internal ceiling of about 24 days. Embedded agents retain their existing
host-controlled deadline defaults. A CLI fixture verifies a 31-second nested
shell call and successful continuation of the script.

### Transient package checkpoint

`-e` accepts native package directories, Cargo sources and Git sources for the
current invocation. Their declared extensions, skills, prompts and themes stay
available when automatic resource loading is disabled. Package declarations
remain absent from settings. Startup and reload refresh transient packages,
retaining a cached installation if the source temporarily becomes unavailable.

Git sources accept `@tag`, `@branch` and branch names containing slashes, while
retaining existing `#ref` syntax. Transport usernames remain part of the URL.
A CLI fixture verifies pinned tags, branch selection, local changes, resource
loading with automatic discovery disabled and cached offline startup.

### Cross-workspace selector checkpoint

The resume selector switches between current-workspace and all stored sessions
with Tab. Ctrl+N filters named sessions and Ctrl+P displays their storage paths.
User keybinding overrides apply to both new named actions. Managed sessions
remain outside the selector. Session renaming and confirmed deletion retain
their existing controls.

RPC stores the working directory when creating its initial journal and restores
workspace settings and trust when switching sessions. Missing working directories
leave the previous selection intact. PTY fixtures verify both terminal modes,
remote-workspace resumption, filters, path display and terminal restoration.

The Rust 1.89 CI matrix passed formatting, all-target Clippy and workspace tests
on Linux, macOS and Windows at commit `706a249`. Subsequent changes receive the
same matrix on main.

### Native extension host calls

Native executables built with the current `serve_extension` can discover and
invoke host tools through protocol v2. Nested calls pass through the agent's
normal policies, argument validation, journal and cancellation. `ToolHost`
provides ordered batches so compatible reads can execute together. Existing
protocol-v1 executables continue to load through their original interface.

The `parallel_read` example demonstrates a Rust executable reading several files
through that host interface. A CLI fixture verifies the model-to-extension-to-host
round trip and individual journal entries. A cancellation fixture verifies that
the native process and nested shell stop before a delayed side effect. Frames
are bounded to 4 MiB. Native invocation uses the host's tool deadline, while
legacy metadata hooks retain their existing bounded execution.

### Markdown presentation checkpoint

Level-one headings use underlining, lower-level headings retain their markers,
and code blocks show language-labelled fences with indented syntax highlighting.
Wrapped list content keeps its indentation. Quotes render borders and quote
styling on every continuation line, including nested quotes and narrow terminals.
Recursive quote rendering stops at 32 levels and preserves deeper content as text.
Horizontal rules use the reference 80-column maximum.

Unit tests cover these layouts and Unicode widths. A PTY fixture verifies both
terminal modes, heading decoration, code fences, nested quotes and clean exit.

### Edit diff presentation checkpoint

Edit tool panels retain unified hunk information, show original and updated line
numbers, and highlight changed words for one-line replacements. Added, removed
and context lines use their dedicated theme colors. Tabs expand to three spaces,
and word comparison has a shared 100 ms budget per render. Large comparisons
retain line-level highlighting when that budget is exhausted.

A PTY fixture runs an actual file edit and verifies numbered output and inverse
word highlighting in fullscreen and regular mode.

### Terminal input and redraw checkpoint

Unix terminal input uses the level-triggered reader to retain input when resize
notifications arrive alongside key events. A committed PTY regression exercises
16 resize/input sequences and verifies terminal restoration. Terminal mode
checks ignore the kernel's transient `PENDIN` typeahead flag while comparing
all persistent settings.

Display updates coalesce over 16 ms. In a local 100-character input fixture,
fullscreen redraws decreased from 100 frames to two. Kitty and iTerm2 image
fixtures passed all four mode/protocol combinations on three successive runs.
Markdown, edit output, regular-history redraw and session selectors also passed
their PTY regressions.

### Clipboard attachment checkpoint

Ctrl+V pastes clipboard images or text into the terminal draft. Windows uses
Alt+V for this action. Clipboard reads run off the UI thread and report progress.
Images use the same bounded PNG preparation as local file reads, retain alpha,
and can be sent alone or with text as a prompt, steering input or follow-up.
Prompt-template and skill expansion retain draft images. Ctrl+C clears an idle
draft, including its attachments. User keybindings can override the action.

Tests cover pixel validation, alpha preservation, draft delivery and image-only
input validation. The live clipboard check exercised the empty/unavailable
clipboard error and clean terminal exit. A populated OS image clipboard has not
yet been exercised in this environment.

### Edit matching checkpoint

File edits preserve UTF-8 BOMs and the file's LF/CRLF convention. Matching first
uses exact text, then Pi-compatible Unicode normalization, quote/dash handling
and trailing-space normalization. If fuzzy matching is needed, only touched
line blocks are rewritten from the normalized view. Other line content retains
its original characters and whitespace.

Batch input accepts arrays, a single edit object or a JSON-encoded array/object.
Legacy replacements can accompany a batch. All replacements match the same
original snapshot. Ambiguous normalized matches, overlapping replacements and
no-op edits fail before publication. CLI fixtures verify these input forms,
line endings, unchanged lines and failure atomicity through real tool calls.

### Resource glob checkpoint

Explicit skill, prompt, theme and executable-extension paths accept glob and
brace patterns. Matching directories contribute their supported resources.
Configured positive patterns expand from their settings directory and coexist
with exact paths and exclusion overrides. CLI selections still load when their
automatic discovery is disabled.

A CLI fixture combines a relative workspace, `--project`, explicit skill and
native-extension globs, prompt brace expansion and settings-based patterns.
Legacy named project skills remain distinct from explicit skill paths. Empty
required patterns report an error before starting a model request.

### Argument preparation checkpoint

Rust tools can implement `prepare_arguments` before schema validation and
policy authorization. Normal and nested invocations use the same path. Raw
arguments remain in the journal. Preparation has cancellation, deadline and
size bounds, and pending-job authorization uses the prepared value too.
Native executable descriptors negotiate this hook while older binaries retain
their existing behavior. The edit tool uses it to advertise its canonical batch
schema while accepting supported legacy forms.

Tests cover normalized inputs reaching policy hooks, raw-history preservation,
a preparation timeout without dispatch, old executable descriptors, native
host calls, cancellation and codemode regression.

### Native extension UI checkpoint

Rust tools can use `ToolHost::has_ui` and the asynchronous `ToolHost::ui`
interface. Embedded applications supply an `ExtensionUi` through the registry
or `SessionBuilder::ui`. Native executable extensions use the same correlated
host protocol for select, confirm, input and multiline editor dialogs, notices,
status entries, above/below-editor widgets, window titles and editor text.
`interactive_review` is a runnable Rust example.

The TUI renders dialogs in fullscreen and regular modes. Escape cancels a
dialog, and abort or timeout removes outstanding requests. RPC forwards
`extension_ui_request` events with Pi field names and accepts
`extension_ui_response` with `value`, `confirmed` or `cancelled`. These transient
interactions are independent of durable tool outcomes.

Workspace tests and real executable fixtures cover correlated responses,
multiline editing, cancellation and request timeouts. PTY fixtures cover both
terminal modes and restoration. Existing image, Markdown, diff, session,
resource and native host-call fixtures continue to pass. Custom terminal
components and the remaining extension events are still tracked above.

### Settings validation and editing checkpoint

`/settings` offers the existing display toggles and a native multiline JSON
editor for implemented model defaults, resource lists, native packages, tool
selection, trust defaults, terminal mode and codemode settings. Ctrl+S validates
and saves, Escape discards the edit. Invalid values keep the editor open and
show the failing settings path. Unrecognized extension-owned keys survive
updates. Runtime/resource changes take effect on reload or the next startup,
and theme changes apply immediately.

The local settings editor remains open when an outstanding model response
finishes. Settings, theme and hotkey commands are handled before streaming
input is queued. Tests cover schema diagnostics and dialog error layout.
Remaining upstream settings and specialized selector behavior are still open.

A real PTY fixture covered both modes while a model response was outstanding,
then exercised invalid input, correction, cancellation and preservation of an
extension-owned setting. The existing native-dialog and resource fixtures also
passed after the settings changes.

### User shell execution checkpoint

The terminal accepts `!command` and `!!command`. Both stream output and retain
a typed shell record. The double-prefix form excludes that record from future
model context. Shell commands run directly without a model request. Nonzero
exits retain output and the exit code. Escape cancels the command and reaps its
process group or Windows job. Timeouts retain a cancelled result.

RPC supports `bash`, streamed `bash_execution_update` events correlated with
the caller's ID, `abort_bash`, and `isBashRunning` in `get_state`. One user
shell can run concurrently with the agent. RPC returns the shell result when
the command finishes. During an active run, completed shell results enter the
journal at settlement, after the assistant and tool records. `abort` and
`abort_bash` target the model run and user shell independently. In the
terminal, Escape interrupts the model first when both are active. Native and
imported Pi shell records keep context-exclusion flags.

RPC and both terminal modes passed fixtures covering streamed UTF-8 output,
nonzero exits, included/excluded model context, cancellation, process cleanup
and durable restoration. A held model response verified concurrent execution,
independent cancellation and the deferred journal append. Core tests cover
exclusion after reopening the journal, and shell tests cover partial UTF-8
chunks and timeouts.

### Clipped Kitty image checkpoint

Fullscreen Kitty rendering now places the visible portion of an image when its
upper or lower rows lie outside the viewport. Source-row rounding follows the
pinned Pi renderer. Image data remains cached across scroll and resize, and
placements are cleared independently of cached uploads. iTerm2 retains its
whole-image rendering behavior.

Unit tests and a PTY fixture cover upper/lower clipping, scrolling, cached resize
and terminal restoration. The existing Kitty/iTerm2 fixture passed both modes.
Regular-mode active-region images and iTerm2 partial-image behavior remain open.

### Package autoload checkpoint

A native package declaration with `autoload: false` starts with no resources
and applies only its explicit patterns. Project declarations in this mode apply
ordered additions/removals over a matching user package declaration. Missing or
empty per-resource arrays leave the inherited selection intact. An ordinary
project declaration still replaces the matching user selection.

Unit and CLI fixtures cover standalone explicit selection, exclusions, ordered
re-inclusion, inherited user filters and an ordinary empty-array replacement.
The existing Git revision, transient reload and offline-cache fixture passes.

### Per-model thinking defaults checkpoint

Model-specific `modelThinkingLevels` settings apply at startup and during TUI
and RPC model changes. Explicit command-line thinking and scoped-model suffixes
retain their precedence. Restoring a session for a different explicitly chosen
model uses the chosen model's default. Matching saved session selections remain
intact. The native `max` level remains valid in settings.

CLI/RPC fixtures cover default selection, explicit overrides, model scopes,
session restoration and model switching. Settings-editor regressions pass.

### Terminal suspension checkpoint

On Unix, Ctrl+Z restores normal terminal settings before suspending the
foreground process group. Continuing the job restores the TUI, draft and image
rendering. The previous SIGINT disposition is restored after continuation.
Windows and WSL use Pi's Ctrl+Q follow-up, Alt+P reverse-model and Alt+V paste
bindings. Unix suspension remains available on WSL.

A committed isolated-process-group PTY test covers resize input and suspension.
A separate fixture covers both screen modes, SIGINT while suspended, resumed
draft contents and terminal restoration on exit.

### Clipboard file-list checkpoint

File lists on the clipboard take precedence over image/text representations.
Pasting inserts one path per line into an ordinary draft and shell-quoted paths
into a `!`/`!!` command, with whitespace around adjacent text. Paths with control
characters and oversized lists are rejected before insertion. Windows uses
PowerShell quoting, and Unix uses POSIX shell quoting.

Tests cover file-list insertion, surrounding text, shell quoting, validation and
existing image/text draft behavior. A populated system file-list clipboard has
not been exercised in this environment.

### Durable queue controls checkpoint

The core session exposes pending inputs, durable queue clearing and separate
steering/follow-up modes. A mode can consume all pending messages or one per
safe boundary. Existing embedded callers retain the previous all-at-once
default. Queue removals and mode choices follow branch history and survive
reopening or forking. The owned SDK exposes these operations while idle and
counts both buffered and journaled pending inputs. Text-only queued messages
now use the same size bound as image-bearing messages.

Tests cover one-at-a-time consumption, cleared messages, historical forks and
SDK queue removal. Active controls are covered in the following checkpoint.

### Active queue controls checkpoint

The core accepts `AgentInput` messages and acknowledged queue controls while a
model connection, response stream or tool execution is waiting. The owned SDK
exposes these controls on `SessionControl` during a prompt and on `AgentSession`
while idle. Its final input drain is synchronized with late control submissions.

RPC supports queue clearing and steering/follow-up mode changes during a run.
Clearing returns the removed text and native image-bearing messages. Mode
changes update the active branch and user defaults. New native frontend sessions
default to one-at-a-time delivery. Existing core embeddings retain all-at-once
behavior unless configured otherwise.

Alt+Up restores queued messages in the TUI, with Alt+Q on Windows/WSL. Escape
restores the queue before stopping the active run. An RPC abort retains queued
messages and continues them without repeating the interrupted request first.
Compaction queue handling is covered below. Review queue handling is covered below.

RPC fixtures verify acknowledgements while HTTP/model and shell-tool execution
are pending, one-at-a-time delivery, persisted defaults on a new session and
continuation after abort. Both terminal modes pass queue-restoration checks.
Core and SDK tests cover active controls, branch history and idle operations.

### Compaction queue checkpoint

Explicit and automatic compaction accept queue controls while awaiting their
summary response. SDK and frontend callers use the same acknowledged control
channel, and late SDK controls are drained before the operation closes. Existing
callers of `Agent::summarize` keep the original interface.

SDK and RPC fixtures verify clearing pending input during a blocked summary,
cancelling that summary, and completing a later compaction without restoring the
removed message. Workspace tests and all-target Clippy pass.

### Review queue checkpoint

Independent child reviews accept parent queue controls while the reviewer is
waiting. Input and removals stay in the parent journal and do not enter the
reviewer's frozen snapshot. Cancelling and resuming a review retains its existing
identity and budget. Pending parent messages continue after a successful review
or compaction, in addition to the existing continuation after an abort.

A native worker fixture verifies parent-only queue records, prompt isolation,
review cancellation/resume and follow-up execution on the parent. Workspace
tests and all-target Clippy pass.

### Session tree navigation checkpoint

The tree selector orders records by ancestry, marks the active head and indents
forks. Alt+Left/Right folds, unfolds or moves through parent/child records.
Searching includes records inside folded branches. Shift+L edits or removes a
record label without changing its identity. Direct `/checkout` releases its
journal lock before restoring the view.

Unit tests and fullscreen/regular PTY fixtures cover branch navigation,
folding, search, labels, checkout and terminal restoration. Session and queue
selector regressions pass. Tree timestamps and richer branch summaries remain
in the outstanding session work.

### Package identity checkpoint

Cargo version suffixes and Git revisions no longer create separate configured
identities for the same source. Installing a replacement preserves resource
filters. Project and transient declarations override the corresponding user
package even when they select another revision. Removal accepts an unversioned
source and leaves cached artifacts intact.

Missing local paths retain their resolved identity for listing and removal.
Installing a missing source fails before settings change. CLI fixtures cover
version replacement, cross-scope revision overrides, inherited resource deltas,
deleted-source cleanup and the existing transient/offline behaviors.

### MCP reconnect checkpoint

Closed MCP transports reconnect before the next tool or resource request. The
connection is serialized during replacement and checked against its original
configuration and credential identity. Explicit shutdown prevents reconnection.
A failed in-flight operation is not replayed, preserving side-effect safety.
Remote task polling retains its persisted job reference across reconnection.

A stateful stdio fixture exited after each response and returned counter values
1, 2 and 3 across three prompts in one agent process, with exactly three server
starts and three mutations. Rust 1.89 workspace tests and all-target Clippy passed.
Live tool-list updates still require separate implementation and verification.

Git package identity now unifies SSH and HTTPS declarations for the same host
and repository path, including default ports and `.git` suffixes. Revision and
transport changes preserve per-package resource filters and scope precedence.
Explicit non-default ports and case-sensitive repository paths remain distinct.
Regression tests cover transport aliases, ports, revisions and missing local
sources.

### MCP form elicitation checkpoint

MCP form requests now use the active tool call's native interaction surface.
The JSON editor includes schema defaults, validates JSON and schema constraints,
and keeps invalid input available for correction. Responses retain the server's
request correlation. Cancelling the form returns cancellation, and noninteractive
hosts decline the request. The interaction binding is released after each call
or interruption. URL-based elicitation remains outstanding.

A stdio server requested a form during an actual tool call. Verification covered
defaults, malformed JSON, constraint violations, successful values, cancellation
and noninteractive decline. Existing HTTP resources, URI templates, prompts,
remote task polling and task resumption passed their regression fixtures.
Workspace tests and all-target Clippy passed on Rust 1.89.

The native `find` tool excludes dependency and Git metadata directories during
normal traversal while allowing an explicitly selected dependency root. File
search no longer silently stops after visiting 100,000 filesystem entries. Its
configured result and output limits continue to apply. Focused coding-tool tests
cover the traversal exclusions and explicit-root behavior.

### MCP live-definition checkpoint

Persistent frontends refresh MCP definitions at the start of each new prompt.
The transport and remote state stay in place. Registry replacement validates
all replacement schemas before publishing and preserves extension order, UI
bindings and tool-selection rules. Deferred search metadata and the generated
system prompt are rebuilt together with the callable tool registry.

Direct and deferred fixtures added and removed tools, then changed a surviving
tool's argument schema across three prompts. Each model request and dispatched
call used the current definition, with one server process throughout. Existing
reconnection and form-elicitation fixtures passed after the change. Workspace
tests and all-target Clippy passed on Rust 1.89. Changes during an active model
turn are applied at the next prompt boundary.

### Imported system-instruction checkpoint

Upstream system messages now become typed journal checkpoints. Resuming replays
added instruction text, ordered named-section replacements and removals, and
tool-set changes on the selected branch, including across compaction. JSON
object order is preserved during import so named sections retain their declared
order. Tool declarations resolve to current enabled native implementations and
schemas. An imported declaration cannot enable an unavailable or disabled tool.

Tests cover branch restoration, section order, replacement/removal, current tool
schemas and permission boundaries. A CLI import-and-resume fixture verified the
actual model request and retained the original source file. Workspace tests and
all-target Clippy passed on Rust 1.89, along with direct/deferred live MCP updates
and form elicitation after the serialization change. A local qwen3.5:4b CLI call
also returned the requested `OK` response.

### MCP browser elicitation checkpoint

URL-based requests require an explicit confirmation before invoking the native
browser launcher, followed by confirmation that the browser step is complete.
Declining the first prompt, cancelling completion, and noninteractive operation
return their corresponding protocol actions. Accepted URLs use HTTPS or local
loopback HTTP, and browser invocation passes the validated URL as an argument.

A stdio fixture verified all response paths and the exact launcher arguments
using an isolated replacement launcher, without opening a real browser. Form
elicitation regressions, workspace tests and all-target Clippy also passed.

### MCP OAuth checkpoint

Native HTTP MCP connections now support public-client OAuth authorization.
`pi mcp add NAME --url URL --oauth` configures a server, and `pi mcp login NAME`
starts discovery, dynamic client registration and a browser-based PKCE flow
with a loopback callback. `--client-id` selects a pre-registered public client,
and repeated `--scope` options select scopes. Pass `--local` to log in to a
project-scoped definition.
`pi mcp logout NAME` removes the saved credentials.

Credentials are written atomically in the user agent directory, with owner-only
file permissions on Unix. Refresh operations share a process-safe file lock that remains held until a
blocking write finishes, including when its async caller is cancelled.
Each successful login receives a new identifier, so old connections cannot load
credentials from a later login. A failed browser launch preserves the prior
credentials. Both expiring access tokens and rejected access tokens can refresh
through the MCP SDK. Credential values are omitted from CLI output.

Local OAuth and MCP fixtures verified dynamic registration, pre-registered
clients, PKCE verification, rejection of an incorrect callback state,
authenticated tool calls, token rotation, cross-process reuse, logout and login
replacement. Store tests cover stale connections, locking, file permissions,
failed oversized writes and recovery from a damaged store. These are fixture
checks with an isolated browser launcher. Automatic interactive scope upgrades still require implementation.

### Confidential MCP client checkpoint

`pi mcp login NAME --client-id ID --client-secret-env VARIABLE` authorizes a
pre-registered confidential client. The command line carries the environment
variable name. Its value is saved with the tokens in the private credential
file. Client secrets returned by dynamic registration are also retained.
Restored connections configure the saved client authentication and redirect
URI after the SDK verifies the authorization server issuer.

Local fixtures passed both `client_secret_basic` and `client_secret_post`,
including browser authorization, separate-process restoration, proactive and
rejected-token refresh, logout, re-login and concurrent configuration edits.
Existing public-client fixtures also passed. Store tests preserve registration
metadata across refresh and accept older files that have no registration field.
The fixtures use a replacement browser launcher and a local authorization
server. Interactive scope upgrades remain open.

### MCP client metadata registration checkpoint

`pi mcp login NAME --client-metadata-url HTTPS_URL` uses a hosted client metadata
document when the authorization server advertises support. Otherwise the
client performs dynamic registration, including persistence of any returned
client secret. Explicit client-ID and metadata-URL options are mutually
exclusive. Existing public and confidential login APIs remain available.

Local authorization fixtures verified URL-based client IDs, restoration and
refresh, invalid metadata URLs, conflicting options, and fallback to
confidential dynamic registration. The hosted document is supplied by the
caller and interpreted by the authorization server. Browser interaction was
verified with the isolated test launcher.

### Active terminal-image checkpoint

Regular terminal mode now paints images while an agent turn is still active.
Kitty committed-history and active-region placements use separate placement
IDs, and active redraws delete only the active placements. Visible-row counts
stop at image boundaries so cropped images do not cover following widgets.
The implementation follows the [Kitty placement deletion protocol](https://sw.kovidgoyal.net/kitty/graphics-protocol.html).

PTY fixtures verified blocked model turns with user and tool images for Kitty
and iTerm2, transition into committed history, draft edits, resize reuse and
terminal restoration. Fullscreen Kitty clipping and existing image fixtures
also passed. Workspace tests and all-target Clippy passed on Rust 1.89.
Partial iTerm2 images are covered by the following checkpoint.

### Clipped iTerm2 image checkpoint

iTerm2 images render their visible source rows when clipped at either viewport
edge. The renderer waits until the last visible image row before drawing, then
restores the cursor and retains the reserved row count. PNG cropping preserves
transparency and uses a bounded 4 MiB cache for repeated viewport positions.
Full committed images retain their original encoded payload.

Pixel-level unit tests verified upper/lower crops, alpha and cache reuse. PTY
fixtures decoded the actual emitted PNG data while exercising both terminal
modes, scrolling, resize and terminal restoration. Existing Kitty crop, active
image and inline-image fixtures also passed, along with workspace tests and
all-target Clippy on Rust 1.89. Further cell-metric and aspect-ratio fidelity
work remains in the terminal surface.

### Image cell-sizing checkpoint

Image cell sizing now follows the pinned Pi calculation, including small-image
upscaling, width/height bounds and Kitty's lower-distortion rounding. Default
image height follows the cell aspect ratio instead of shrinking to terminal
height. Native terminal pixel dimensions determine fractional cell sizes when
available, with the reference 9×18 fallback. Regular layouts invalidate when
cell dimensions change even if the row and column counts stay fixed.

A comparison matrix matches both reference rounding modes, including fractional
cell sizes. A PTY fixture changed pixel dimensions while preserving the character
grid and verified image placement dimensions, retained draft input, PNG reuse
and terminal restoration. Existing clipping and active-image fixtures passed
for both protocols. Workspace tests and all-target Clippy passed on Rust 1.89.
Unix terminal-reported CSI cell-size responses are covered by the following checkpoint. Windows response handling is covered by the following Windows checkpoint.

### Unix terminal-response checkpoint

The Unix input reader now separates terminal replies from keyboard input. Its
key decoder is adapted from the existing Crossterm 0.29 implementation, with
the original MIT notice in `assets/CROSSTERM-LICENSE`. Startup, resize and resume
request CSI cell dimensions. Valid replies update image layout, while malformed
replies are consumed without entering the draft.

Unix terminals can also report appearance changes through mode 2031. The
reader consumes CSI 997 reports and updates light/dark theme pairs. Fixed
themes and saved theme settings are preserved. Exit and suspension disable
notifications, and resume enables them again.

Tests cover the inherited keyboard decoder, split Unicode and replies, Alt and
control keys, literal bracketed paste and Escape handling. PTY fixtures verified
cell queries, theme-pair updates, fixed themes, invalid reports, queue controls,
settings editing, shell execution, images, resize, suspend and terminal cleanup.
Workspace tests and all-target Clippy passed on Rust 1.89.


### Windows terminal-response checkpoint

The Windows reader now handles console input records directly, preserving
Crossterm's native key, modifier, Unicode-surrogate, mouse and focus mappings.
Synthetic records without physical key information pass through the shared VT
decoder before key mapping, which preserves response ESC characters. CSI cell
size and appearance reports therefore use the same layout and theme-pair path
as Unix. Native resize records use the visible window dimensions, excluding
scrollback. Exit disables appearance reporting on both platforms.

Regression cases cover synthetic cell and appearance reports, native Enter
press/release, repeated response characters, Unicode surrogate pairs and literal bracketed paste. The input
module and its tests pass Windows-target compilation and Clippy independently.
Native Windows execution is checked by the repository's CI matrix. Interactive
verification in a real Windows Terminal remains outstanding.


### Compaction focus and RPC completion checkpoint

`/compact <instructions>`, `compact --instructions`, RPC `compact` with
`customInstructions`, and the native SDK's `summarize_with_instructions` now
pass the requested focus into the tool-free summary request. Existing summary
APIs retain their behavior. Focus text does not become a user-history entry.

RPC compaction replies after the summary checkpoint is committed, with
`summary`, `firstKeptEntryId` and `tokensBefore`. The token count uses the native
runtime's character-based estimate described in the token-estimation checkpoint. Compaction errors,
cancellation and unexpected worker exit return failed responses. The worker
stays busy until that response, and `get_state` reports compaction separately
from streaming. Queue controls retain their existing behavior.

A native SDK regression verifies the summary request and committed history.
Loopback Ollama-adapter checks exercised deferred RPC completion, Unicode
focus text, state, invalid arguments, model failure, cancellation, single
responses and atomic checkpoints. Fullscreen and regular PTY checks exercised
focused compaction and terminal restoration. The direct CLI path and existing
RPC user-shell checks also passed. These checks do not cover the remaining
automatic-compaction settings or full provider token accounting.


### Live automatic-compaction control checkpoint

`compaction.enabled` is loaded from user and trusted-project settings, defaults
to true, and is reported by RPC `get_state` as `autoCompactionEnabled`.
`set_auto_compaction` validates its boolean argument, preserves unrelated user
settings and nested compaction fields, and updates the running worker without
waiting for an active model call to finish. `/settings` exposes the same live
switch. Changes persist across restarts.

The core session exposes a shared `AutoCompactionControl`. Native SDK controls
can change it while a run is active. Forked SDK sessions copy the current value
into an independent switch. Existing `Limits` construction remains unchanged.
Disabling automatic compaction preserves context-limit checks and manual
summaries. Enabling it again permits the existing bounded automatic-summary
path to proceed.

Core and SDK regressions cover the context limit, recovery after re-enabling,
cloned controls, fork isolation and closed-session handling. Loopback
Ollama-adapter checks cover live RPC changes during a held model request,
settings persistence, field preservation, validation and manual summaries.
Both terminal layouts were exercised with a model request still running.
Unloaded-model context-window discovery
and broader provider recovery behavior remain open.


### Loaded-model context discovery checkpoint

Models can provide their effective context capacity through the optional async
`Model::context_window` method. The default remains unknown for existing model
implementations. The agent uses discovered capacity when `Limits.context_window`
is absent and preserves an explicit limit when supplied. Metadata waits honor
cancellation and the model timeout. The host budget adapter forwards this
capability to the wrapped model.

Ollama reads the matching loaded model's `context_length` from `/api/ps`,
including model-name aliases with `:latest`. Requests retain the configured
proxy prefix and bearer authentication. Responses are bounded to 1 MiB and a
250 ms timeout. Missing, malformed or unavailable metadata falls back to an
unknown capacity. Unloaded models still require an explicit limit until their
running capacity becomes available. The reported field is documented in the
[Ollama running-model API](https://github.com/ollama/ollama/blob/main/docs/api.md#list-running-models).

Core tests cover automatic compaction with discovered capacity, explicit-limit
precedence and cancellation during discovery. Loopback Ollama-adapter checks
cover an initially unloaded model, authenticated proxy paths, runtime capacity,
explicit overrides, malformed/oversized replies, unavailable endpoints and
metadata timeouts. Existing chat-only fixtures retain their inference checks
and now reject unsupported metadata requests explicitly.


### Configured compaction token policy checkpoint

Explicit `compaction.reserveTokens` and `compaction.keepRecentTokens` settings
now reach the agent. Each field resolves independently through
`compaction.modelOverrides["ollama/<model>"]`, then the ordinary setting, then
the reference defaults recorded below.
Values are validated as non-negative JavaScript-safe integers. Zero is valid,
and unrelated settings remain preserved.

A configured reserve can start compaction before the hard context limit is
reached. Manual TUI, RPC and CLI summaries use configured token retention;
CLI `--keep-turns` remains an explicit override. Configured recent-token retention initially selected complete conversation
turns. The checkpoint below adds message boundaries within a turn while
keeping tool calls with their results. A soft threshold only starts a
summary when an older prefix exists and enough request budget remains. The
existing one-compaction-per-run bound still applies. Native SDK controls expose
the same optional token settings, and forks receive an independent copy.

Within-turn boundaries and omitted-field defaults are covered by the following
checkpoints. Low-level hosts can still supply their own optional token policy.
`/settings` exposes the compaction object for editing, with reload/startup
application for token-policy changes.

Core tests cover early reserve pressure, retained recent turns and shell
output within its real conversation turn. Settings
tests cover per-field model overrides, zero, invalid numbers and malformed
model overrides. The RPC fixture exercises model changes, early compaction and
preserved user settings through the normal worker path.


### Within-turn compaction boundary checkpoint

Configured token retention can now start at a user or assistant message within
a turn. Tool-result entries are never selected alone. When trailing tool
results meet the retention target, their assistant call remains with them.
Adjacent non-context records are kept with the selected boundary, and the
existing compaction entry records that boundary for reopening and branch replay.

Context projection retains source IDs through edits and tool-pair repair.
Summary requests use the currently projected prefix, so an edit or deletion
recorded after the selected boundary still applies. For a split turn, the
summary prompt includes the initiating request and asks the model to preserve
what is needed to interpret the retained messages. Extension hooks can retain
additional whole turns, while dropping unsummarized context remains rejected.

Automatic compaction, manual RPC/TUI compaction and CLI compaction use this
boundary when token retention is configured. Explicit `--keep-turns` and the
existing SDK methods retain their turn-count behavior. The SDK also exposes
`summarize_configured` for token-based selection.

Core tests cover a trailing tool result, late context edits, source preservation,
reopening and the retained assistant/tool pair. SDK tests cover retaining an
assistant inside a turn. Loopback checks exercise automatic and manual RPC
compaction, model overrides, CLI turn-count overrides and completion responses.
Token-estimator differences remain open. Reference default values are covered
by the following checkpoint.


### Reference compaction defaults checkpoint

The local workbench now defaults to `reserveTokens = 16384` and
`keepRecentTokens = 20000`, matching Pi v1.5.0. Resolution is independent for
each field, with a model-specific value taking priority over the ordinary
setting and built-in default. Explicit zero remains zero. Missing values are
resolved in memory without rewriting user settings. The native SDK exposes
`CompactionTokens::pi_defaults()` for hosts choosing this standard policy.

The compaction settings editor displays the resolved defaults when no
compaction object exists. A manual summary with no projected prefix returns
`Nothing to compact (session too small)` before any model request. Automatic
threshold handling also checks for actual projected history before attempting
a summary, so metadata-only prefixes do not trigger empty summaries on small
context models. Explicit `--keep-turns` and SDK turn counts still select their
requested retention.

Tests cover per-field fallback, model overrides, zero values, small-session
summary rejection, and short prompts with a context smaller than the default
reserve. Loopback checks cover the CLI/RPC default path, a skipped summary,
explicit CLI override and preservation of the settings file. Both terminal
layouts display the defaults without creating a settings file.


### Character-based token estimate checkpoint

The core now estimates visible text with Pi's UTF-16-length / 4 rule and a
fixed 1,200-token image budget. User images are independent of their Base64
length. Assistant calls include their names and normalized JSON arguments.
System prompts and advertised tool schemas are included in request estimates.
Shell entries count their command and output, excluding presentation wrappers.

The same estimators drive context checks, configured retention boundaries and
RPC `tokensBefore`. Native structured tool images use the same extraction
routine as the Ollama encoder, so inline image bytes do not become text-token
estimates. The encoder retains its existing text and image output shape.

Reference comparisons matched empty/ASCII/Japanese/emoji text, user images and
tool-call examples. Tests also cover image-size independence and existing
provider image output. Loopback checks exercised configured compaction,
explicit limits, loaded-model discovery and fallback behavior. Test context
budgets were updated to continue exercising the same boundaries with the new
estimator. Provider-usage anchoring is covered below. Provider-specific reasoning
accounting remains open.


### Measured context usage checkpoint

Context estimates now use the latest visible assistant message with nonzero,
successful measured usage, then add character-based estimates for subsequent
messages. Native input totals include cached input; cache counters are not
added again. A nonzero raw provider prompt total takes precedence when supplied.
The public `ContextUsageEstimate` reports the measured portion, trailing portion
and source record as well as the total.

Later context edits, compaction, system checkpoints or model-selection changes
invalidate an older measurement. Estimation then uses the visible journal until
a new measured assistant response becomes available. Branch checkout and file
reopening reconstruct the appropriate measurement from that branch. Estimated,
zero-usage and failed responses do not create measured anchors.

Automatic compaction uses measured context when available, including cases
where a short text response consumed a large provider context. RPC
`tokensBefore` uses the same estimate. Summary requests themselves retain
prefix-specific character estimates rather than borrowing usage for the whole
conversation.

Tests cover trailing input, edits, compaction, model changes, checkout, reopening,
zero usage and cached-input accounting. Loopback Ollama checks verify early
compaction from usage, measured RPC counts and fallback counts after an edit.
Provider-specific reasoning handling and complete native prompt checkpointing
remain open.

### Directory listing limits checkpoint

Directory listings now use the reference's combined notice when both the entry
limit and 50 KB output limit are reached. The byte limit retains whole output
lines, including an entry ending exactly at 50 KB, and the limit-only notice
matches Pi's wording and punctuation. Listing
stops reading entry metadata after the requested number of successful entries,
and a file supplied as the directory reports `Not a directory`.

The boundary regression creates 301 long filenames and verifies entry-only,
byte-only and combined limits. The same cases were executed against the pinned
Pi distribution, which returned the same 254 retained lines and notices. A
second fixture verifies 256 entries ending exactly at the byte boundary. This
covers listing limits; complete locale collation, structured tool details and
other local-tool output/error parity remain open.

### Grep context and line-limit checkpoint

Grep now emits a separate context block for each match, including repeated lines
where adjacent matches overlap. The matching marker belongs to that block's
center; a neighboring match uses the context marker. Pending blocks flush at EOF,
including the final empty line when the file ends with a newline. The streaming
implementation retains a bounded context window and bounded output.

Long lines use Pi's 500 UTF-16-unit cutoff. A split surrogate is rendered as a
replacement character in native UTF-8 text. Match counting continues after the
50 KB output limit, so combined match, byte and long-line notices reflect the
same selected matches as the reference. Notice wording also matches Pi.

Regression tests cover overlapping blocks, the final selected match's context,
EOF with and without a trailing newline, surrogate boundaries, and combined
limits. Direct execution of the pinned Pi tool confirmed the context output and
the 101-line, 51,098-byte combined-limit result. A loopback CLI test also verified
that invoking the native grep tool delivers those per-match context blocks to
the next model request. Workspace tests and all-target Clippy passed. Other local-tool differences,
including structured result details and find traversal, remain open.

### Find traversal and glob checkpoint

Native `find` now includes directory matches (with a trailing slash), hidden
entries and symlinks without following symlink targets. It follows the reference
CLI's ignore-file behavior instead of unconditionally excluding `node_modules`
and `.git`. Git ignore rules also apply outside repositories. Basename patterns
use smart case; patterns containing a slash match full paths with the reference's
implicit `**/` prefix. Results use forward slashes and sorted output.

Result and byte limits use the reference CLI's combined notices and complete-line
50 KB cutoff. Tests cover directories, case-sensitive uppercase patterns,
case-insensitive lowercase patterns, path globs, ignored dependencies outside a
repository, symlinks and result-limit notices. A six-call CLI model/tool/model
round trip produced exactly the same results as the pinned Pi tool for the same
fixture. Workspace tests and all-target Clippy passed. Full structured tool details and
remaining native search edge cases still require acceptance work.

### Local file-path input checkpoint

The file tools share Pi-style input normalization for leading `@`, home-directory
paths, file URLs and Unicode space variants. Windows input also accepts drive
paths written in Git Bash, Cygwin and WSL form. Dot segments resolve before file
lookup. Existing canonical-path checks and parent traversal restrictions remain
in the workspace-scoped profile.

Only reads try the reference's filename alternatives when the exact path is
missing: screenshot AM/PM spacing, NFD Unicode, curly apostrophes and the combined
NFD/apostrophe variant. Writes and edits use the requested normalized name.
CLI attachments share the read resolver, so early canonicalization no longer
prevents these alternatives from working.

Tests cover normalization, file URLs, Windows drive forms, read-alternative
precedence, attachment resolution and the distinction between read and edit.
A five-call CLI tool round trip matched the pinned Pi read tool for the same
mention, file URL, screenshot-spacing, apostrophe and NFD inputs. Workspace
tests and all-target Clippy passed. Windows path conversion is unit-tested; interactive Windows acceptance remains
open alongside the other platform-specific checks.

### Agent-level retry checkpoint

The local workbench now uses Pi's agent retry defaults for retryable model
failures: three retries, 2-second exponential backoff and a 60-second delay cap.
`retry.enabled`, `maxRetries`, `baseDelayMs` and `maxAgentDelayMs` are validated
settings. Low-level hosts retain opt-in retry behavior through `RetryControl`;
SDK controls can configure retries or abort the active retry wait, and forks
receive independent copies of the settings.

Each attempt stays within the existing model-request bound. Failed partial
responses remain in diagnostic history and are omitted from the next request.
Usage settlement failures remain outside the retryable provider outcome, for
both completed and failed responses.
Inputs received during backoff remain queued. Retry waits are cancellable and
their control state is cleared when the waiting future is dropped.

RPC supports `set_auto_retry` and `abort_retry`, with `auto_retry_start` and
`auto_retry_end` events. The terminal displays the retry wait, attempt count and
error. Provider-internal retries and Retry-After handling, summary-specific retry
policy remain open.

Tests cover recovery, request and retry caps, queued input, wait cancellation,
dropped-wait cleanup and usage-settlement failures. Loopback RPC checks verify
503 recovery, exponential delays, aborting backoff and preserving settings when
disabling retry. A PTY check verifies the retry phase and attempt display,
recovered output and terminal restoration. The complete workspace test suite
and all-target Clippy also pass.


### Retry settings UI checkpoint

`/settings` includes an Auto-retry toggle that persists the enabled flag and
updates an already-running backend. The retry object also has a JSON editor
showing Pi defaults when no explicit object exists. Advanced edits apply on
reload/startup, as indicated by the editor title. Existing provider settings and
unrelated user keys survive a toggle.

PTY checks cover fullscreen and regular layouts, disabling retry before a 503
response, opening the retry editor and restoring the terminal. Settings tests
cover Pi defaults, explicit zero values, integral JSON numbers and invalid
retry settings. Workspace tests and all-target Clippy pass.

### RPC command discovery checkpoint

`get_commands` returns enabled extension commands, prompt templates and skills,
including source kind, descriptions when available and native source metadata.
The worker serves this read-only catalog during an active model request. A
worker startup failure produces a correlated error response. Discovery finishes
before a following request can replace its worker.

The native registry resolves duplicate names with Pi's occurrence suffixes and
avoids collisions with literal suffixed names. The same resolved names execute
through RPC prompts, print mode and the TUI before model invocation. Existing
explicit extension commands remain available. The TUI forwards unknown names
for worker validation and does not send them to the model.

Core tests cover collisions, disabled extensions and dispatch to the correct
owner. Loopback checks cover the catalog during an active model request, native
command execution, prompt and skill expansion, print mode and startup failures.
A PTY check covers native command execution and rejection of an unknown command.
Workspace tests and all-target Clippy pass. Remaining work includes complete package source identity, command
execution during an already-active model turn and further extension event/UI
parity.

### RPC session statistics checkpoint

`get_session_stats` now returns user, assistant, tool-call and tool-result counts
from the complete durable session, including alternate branches and compacted
history. Nested tool operations do not inflate model-visible tool counts.
Measured requests are counted once, and assistant or imported summary usage is
used when there is no separate measured request record. Input, output,
cache-read and cache-write totals are exposed separately without counting cached
input twice.

Import retains cached usage on assistant and tool-result messages and standalone usage,
compaction and branch-summary entries. An explicit context window also enables
`contextUsage`; after compaction, its token count remains unknown until a fresh
measured response is available. Unknown cost is returned as null; preservation of reported costs is covered
below. Live statistics are covered below. Complete message-variant counting and
provider pricing remain open.

Tests cover an imported multi-branch session with assistant, tool-result,
standalone and summary usage. A loopback RPC test covers message and tool counts,
cache accounting, summary usage deduplication and unknown post-compaction context.
The complete workspace tests and all-target Clippy pass.


### Reported session cost checkpoint

Model metadata can retain an optional USD total, preserving old journals that
omit it. Pi imports preserve the reported total for assistant and tool-result
messages, standalone usage, compaction and branch summaries. Session statistics
sum each measured request once across all branches. Missing prices, invalid
negative prices or unconfirmed usage keep the total unknown instead of implying
zero cost. An empty session has zero recorded cost.

Tests verify reported costs across branches and compaction, file reopening,
missing costs and invalid costs. The same fixture returns USD 0.9375 from both
the pinned Pi statistics implementation and the native RPC. The complete
workspace tests and all-target Clippy pass. Provider price
catalogs, automatic native pricing and detailed cost provenance remain open.

### SDK statistics checkpoint

`Session::statistics` and `AgentSession::statistics` expose the same typed,
serializable totals used by RPC, including session identity. The aggregation
lives in the core crate so embedded clients and the workbench share it.
`Session::context_usage` accepts a known window; the owned SDK's asynchronous
`context_usage` resolves an explicit window or queries the model with a timeout.

SDK tests cover measured usage and cost, session identity, fork totals,
post-compaction unknown context, explicit-window precedence, provider discovery,
closed sessions and discovery timeouts. Existing RPC statistics and cost checks,
the complete workspace tests and all-target Clippy also pass. Additional resource/model/auth factories
and the other SDK equivalents remain open.

### Live RPC session snapshots checkpoint

State, messages, statistics, entries, tree, fork candidates and last-assistant
queries can read an active run. The backend publishes committed records to an
independent snapshot, so these queries do not acquire its journal lock. Snapshot
responses are correlated and finish before a following request can replace the
worker. State responses include session identity, message count and queued-input
count while the model runs.

`Session::snapshot` reads a bounded complete-record prefix without creating,
repairing or changing its source. Normal writable opening still repairs an
incomplete tail. `Session::current_messages` exposes current projection including
pending tool calls; the settled-history API retains its existing checks.

Core tests cover incomplete tails, malformed and missing files, preservation of
writer locks on Unix and pending-tool projection. Platform file-lock rules still
apply to direct file snapshots; active RPC reads use the backend snapshot.

A loopback RPC check keeps the model response pending, queries all seven read
operations, then repeats message/statistics/tree reads while a shell tool runs.
It verifies pending calls are visible and final results appear after completion.
Existing cost, statistics, retry and command-discovery RPC checks also pass,
along with the full workspace tests and all-target Clippy.

### In-flight MCP catalog checkpoint

The agent can refresh its registry and system prompt at model-request boundaries
and refresh again before tool dispatch. The local MCP host re-lists definitions
on the established connection, atomically replaces the server extensions and
search index, and reapplies tool selection and exposure settings. This polling
also detects changes from servers that do not deliver list-change notifications.

Added tools and changed schemas are available within the same run. Removed tools
are rejected before dispatch, even when a model response was based on the older
catalog. Read-only child profiles retain their fixed capability set and do not
inherit the refresher. Refresh waits obey model timeouts and cancellation.

Core tests cover same-run advertising and dispatch updates, removal between
request and dispatch, fixed read-only profiles and cancellation. Interactive
OAuth scope upgrades and push-notification optimization remain open.

Loopback runs verify a direct tool replacement, a same-name schema change,
explicit exclusion of an added tool, and deferred discovery of a replacement
within one turn. Each run uses one MCP initialization. Existing codemode,
resource-template, prompt, authentication-header and redaction checks also pass.

The full workspace tests and all-target Clippy pass. Cancellation after a model
response also settles its unstarted tool operations before returning.

### Parent-session provenance checkpoint

RPC `new_session` retains its optional `parentSession` path in durable native
metadata. A parent reference does not add conversation messages or read or
modify the referenced file. Invalid field types are rejected before the active
session changes. New sessions without a parent remain independent.

The core exposes `parent_session` and `set_parent_session`, native JSONL export
and snapshots retain the reference, local summaries include it, and upstream
Pi header imports preserve `parentSession`. Tests cover reopening/export,
invalid RPC input, unlinked sessions and old imports without parent metadata.
A CLI RPC smoke check covers creation and persistence through normal commands.

CLI, terminal and RPC fork/clone operations record their immediate source as the
parent, replacing inherited ancestry metadata when a child is forked again.
This preserves the copied conversation while retaining the correct origin path.

### Session-name validation checkpoint

CLI `--name`, the native `name` command and RPC `set_session_name` reject empty
or whitespace-only names before changing the journal. RPC startup now applies
`--name`, matching the other execution modes. Shared core normalization collapses
runs of CR/LF to one space and trims ECMAScript whitespace, including the BOM,
while preserving U+0085. The SDK can still clear a name explicitly.

The pinned reference's in-memory session manager confirms the normalization
cases. Tests cover rejected inputs leaving existing records unchanged, Unicode
names, line-break handling, explicit clearing and reopened sessions. A CLI/RPC
smoke check verifies startup naming, error responses and a subsequent valid rename.

### Empty-label compatibility checkpoint

Empty label strings now clear the bookmark through the core, SDK and CLI paths,
matching Pi. The label projection also treats empty strings in previously saved
or imported records as removals. Whitespace-only labels retain their content,
as in the reference. Invalid targets leave the journal unchanged.

Core tests cover writes, replay of legacy empty-string entries and invalid
targets. The pinned reference confirms empty versus whitespace label behavior.

A CLI/RPC smoke check verifies removal and whitespace labels in the session tree.
The full workspace tests and all-target Clippy pass.

### Durable history timestamps checkpoint

New native journal entries carry UTC timestamps with millisecond precision.
Previously saved records without timestamps still load without synthesized
dates. Pi imports preserve source timestamps, including absent timestamps,
and JSONL export and snapshots retain them. Forks timestamp their new session
header while preserving the copied history's original timestamps.

RPC tree nodes expose `labelTimestamp` when the latest nonempty label has a known
timestamp. Clearing a label removes it, and an undated label update does not
reuse a previous change's timestamp. Core and importer tests cover these cases.

A CLI/RPC check verifies persisted UTC timestamps and label timestamp removal.
The full workspace tests and all-target Clippy pass.

### Session discovery ordering checkpoint

Resume candidates are sorted by the latest user/assistant activity rather than
file modification time. Pi imports honor the message's numeric timestamp before
the enclosing entry timestamp. Metadata-only changes do not reorder a session.
When conversation times are absent, discovery falls back to the session header
and then the file modification time.

`--continue` follows the reference's separate rule: use the most recently
modified valid session belonging to the current working directory. Newer files
from other workspaces and malformed journals are skipped. Tests cover both
ordering rules, imported message timestamps and metadata updates. A CLI smoke
check verifies continuation in a shared session directory. Full workspace tests
and all-target Clippy pass.

### Resume transcript search checkpoint

The resume picker searches session names, IDs, working directories and all
user/assistant text across saved branches. Space-separated tokens must all
match. Quoted phrases normalize whitespace. `re:` queries use a case-insensitive
native regular-expression engine with a bounded backtracking limit, including
lookaround support. Invalid expressions produce no matches. Remaining JavaScript
regular-expression edge compatibility has not been declared complete.

Name and working-directory fields support Pi's fuzzy alignment and letter/digit
swap. Message text and IDs use literal matching. Relevance sorting uses the same
match tiers and alignment scores, preserving recent order for ties. Lowercase
transcript text is cached and phrase normalization is lazy.

Workspace filtering now checks the session's recorded working directory,
including when multiple workspaces share a session directory. Session titles
use the first saved user message when no name exists. Tests compare reference
queries and scores and cover field boundaries, phrases, invalid expressions,
Unicode and shared-directory filtering. A real PTY check searches transcript
text, opens the selected session and verifies terminal restoration. Full
workspace tests and all-target Clippy pass.

### Threaded resume selector checkpoint

The resume picker defaults to parent-session groups and cycles through threaded,
recent and relevance order. Groups and siblings follow their newest descendant's
activity. Thread branches show the parent/child relationship. Search switches to
a flat relevance-sorted list in threaded mode, while recent mode preserves the
incoming order. Scope and named-only filters apply before grouping.

Missing or filtered-out parents leave their children visible as roots. Cyclic
parent references are broken for presentation without modifying stored history.
Tests cover grouping, descendant ordering, filters, search flattening and sort
cycling. PTY checks verify branch rendering and all three modes, and repeat the
transcript-search/open-session check. Full workspace tests and all-target Clippy
pass.

### Resume row presentation checkpoint

Resume rows now show message count and relative activity age, right-aligned
beside the session title. Long titles truncate by grapheme and display width,
including Japanese and combining characters. Control characters in titles
become spaces. The current session uses the accent color, named sessions use
the warning color, and selected titles are bold.

All-workspace view includes the working directory; the path toggle adds the
session file path. Home prefixes shorten to `~`. The list keeps the selected
row near its center and shows position when scrolling. Empty local results
suggest switching scope with Tab. Tests cover alignment, Unicode truncation and
the reference age units; real PTY search and threaded-selector checks pass.

Full workspace tests and all-target Clippy pass.

### Read boundary checkpoint

A final newline no longer consumes the 2,000-line truncation allowance. An
explicit 2,000-line request still reports the remaining final empty line, while
an unrestricted read or 2,001-line request preserves the complete content.
The line/byte boundary uses the reference's continuation classification.

Oversized-line guidance now uses the original requested path, the reference
KB/MB size formatting and the same bounded shell-read suggestion. Streaming
line counting and bounded retained buffers remain in use. Four normal CLI tool
calls compare their model-visible output exactly against the pinned reference,
covering terminal newlines, explicit limits, oversized lines and combined
line/byte limits. Regression tests cover the same boundaries.

Full workspace tests and all-target Clippy pass.

### Streaming UTF-8 read checkpoint

The text reader now counts decoded UTF-8 bytes throughout a line, including
replacement bytes beyond the retained prefix. The decoder preserves BOMs and
handles code points and malformed subsequences spanning read-buffer boundaries.
Displayed text remains bounded, and an oversized line's full decoded size is
reported. Size rounding follows Pi's one-decimal KB/MB values at half ties.

CLI tool calls compare exactly with the pinned reference for malformed input,
BOMs, split and incomplete sequences, offset continuation and rounding, as well
as the earlier newline and truncation boundaries. Focused tests exercise a
small streaming buffer and verify the complete decoded byte count.

Full workspace tests and all-target Clippy pass.

### Write and edit result checkpoint

The model-facing `write` and `edit` success messages now match Pi and preserve
the requested path spelling. The edit result journals its bounded unified patch
and first changed line separately from model-facing text. The terminal uses
that patch for its diff presentation, and programmatic text callers receive the
success text. Successful imported text results preserve their original details.

The native `SucceededWithDetails` outcome serializes details durably while
excluding them from the model conversation. The existing tool output limit
also applies to the serialized detail payload. Native edits retain their
existing file mutation locking, atomic replacement and line-ending behavior.

A normal CLI write/edit round trip verifies exact success text, changed bytes,
first changed line and patch against the pinned reference for a BOM/CRLF file.
The patch remains outside the subsequent model request. Remaining display and
patch edge cases are tracked under local tools and terminal parity.

Workspace tests, the imported-details regression and all-target Clippy pass.

### Numbered edit diff checkpoint

Native edit results preserve a numbered display diff alongside the unified
patch. The display diff uses old-file numbers for deletions and context,
new-file numbers for insertions, and four context lines on either side of a
change. Omitted ranges appear as ellipses. The terminal prefers the numbered
diff, preserves its column width and uses the existing word highlighting.
Previously stored unified patches still render through the existing path.

The CLI comparison checks exact numbered and unified diffs for two separate
changes in a BOM/CRLF file. A PTY check reopens the journal, renders the numbered
diff and expands the later hunk.

Workspace tests, the numbered-rendering regression and all-target Clippy pass.

### Edit diagnostics checkpoint

Missing, duplicate, overlapping, empty and unchanged replacements report the
requested path and the applicable original edit indices. Duplicate diagnostics
count every non-overlapping match in normalized text. A failed lookup is reported
before an occurrence check. The batch switches to fuzzy matching when at least
one target requires it and matches. Whitespace-only targets require an exact
match.

The local CLI compares the diagnostic text with Pi inside the native tool
failure envelope and verifies rejection before file writes.

The nine-case CLI comparison, successful edit regression, workspace tests and
all-target Clippy pass.

### CLI short aliases checkpoint

The CLI accepts Pi's `-v`, `-nt`, `-nbt`, `-xt`, `-ne`, `-ns`, `-np`, `-nc`,
`-a` and `-na` forms plus `--approve` and `--no-approve`. Approval uses the native
trust controls and their conflict validation. Version output is shared with
`--version` and `-V`.

Multi-character aliases are recognized at option positions before native
argument parsing. Option values, attached values, the trailing prompt and
arguments after `--` retain their original spelling. Tests cover clustered
short options, nested command options and literal values. A real CLI check
verifies tool filtering, resource discovery, approval and the literal prompt
delivered to a loopback model.

Workspace tests and all-target Clippy pass.

### Qualified Ollama model selection checkpoint

`--provider ollama` selects the existing Ollama adapter and validates the
provider name. The provider travels through runtime creation, discovery, the
RPC model response and worker startup. Model references accept the
`ollama/` qualifier in CLI selection, model scopes, listing filters and the
terminal model command. Other namespaces remain part of the model ID.

An available catalog gives literal model tags priority over thinking suffixes.
Direct startup without a catalog uses the existing suffix syntax and makes no
discovery request. Resolved IDs are passed literally to the hidden worker, so
a tag or namespace is interpreted only once.

CLI and RPC loopback checks cover provider validation, qualified discovery,
thinking suffixes, and literal tagged IDs through a worker. Workspace tests
and all-target Clippy pass.

### Model scope resolution checkpoint

Startup uses `enabledModels` when a CLI model scope is absent. CLI scope
patterns take precedence. Exact catalog IDs beat partial matching, including
literal brackets and thinking-like tags. A partial ID selects the latest
matching alias before dated versions. Globs can match both bare and qualified
IDs, respect slash boundaries, and retain all matches in catalog order.

Scope diagnostics warn about unmatched patterns and invalid thinking suffixes.
The resolver keeps valid candidates and falls back to normal model selection
when every pattern misses. Pattern order and the first occurrence of each
model determine cycling order and thinking settings. Listing filters match
multiple whitespace- or slash-separated terms against provider and ID.

Catalog-backed model references share the partial resolver. Direct startup
uses the literal/suffix fallback when it has no catalog. The available
catalog currently comes from the configured Ollama-compatible endpoint.

The pinned reference comparison, CLI/RPC checks, workspace tests and all-target
Clippy pass.

### Ollama model catalog presentation checkpoint

`--list-models` uses Pi's provider, model, context, max-out, thinking and images
columns. Native Ollama metadata supplies the architecture context limit and
capability flags. A running context window provides a fallback for loaded models.
Unavailable values, including max output when it is unreported, display as `?`.
JSON listing and RPC model discovery expose known fields and retain model IDs.

The adapter confirms native service metadata through `/api/ps` before looking
up selected rows through `/api/show`. Metadata calls share a two-second bound
after that probe and use bounded response buffers. A compatible relay can
return its existing ID-only catalog. Scoped discovery only looks up selected
models. [Ollama show-model details](https://docs.ollama.com/api-reference/show-model-details)

Loopback checks cover table, JSON and RPC output, unknown values, native
metadata fallback, search misses and authorization forwarding. Local Ollama
listing verifies both installed Qwen models. Full workspace tests and all-target
Clippy pass.

### Model-aware thinking controls checkpoint

Ollama discovery reads the advertised `thinking.values` alongside capability
flags. Boolean controls appear as `off` and `high`, and advertised names are
available in the terminal and RPC. Models reporting only `off` skip cycling.
Explicit selections clamp to the next supported higher standard level, then
the nearest lower one, as in Pi. Services without level metadata use the
standard fallback menu. The provider chooses its default when no level is
requested. [Ollama thinking controls](https://docs.ollama.com/capabilities/thinking)

The selected model's metadata is reused by the frontend. A resolved named
level reaches the worker and request without the legacy effort remapping.
The CLI accepts the standard thinking flags; the terminal and RPC accept
additional advertised names.

Loopback checks cover toggle, off-only and named controls, model switches,
clamping and worker transmission. A real PTY check with local Qwen verifies
the available levels, selection persistence and terminal restoration.
Workspace tests and all-target Clippy pass.

### Terminal session navigation checkpoint

With an empty draft, two interrupts within 500 ms open the session tree by
default. The `doubleEscapeAction` setting selects the tree, the fork picker,
or no action. During a running request the interrupt still cancels it. An idle
interrupt clears an unsubmitted shell command.

The tree opens with `treeFilterMode` and supports the reference's direct filter
bindings and forward/backward cycling. Its filters separate user messages,
tool results, labeled records and bookkeeping. When a filter hides the current
selection, the picker selects its nearest visible ancestor when available.
Both defaults can be changed in `/settings`. The native `all` view includes
journal bookkeeping. A PTY fixture exercised all three interrupt settings,
filter shortcuts and settings persistence, and the existing branch navigation
fixture verified folding, label editing, checkout and terminal restoration.

### Offline startup and native package update checkpoint

`pi update --extensions` reconciles configured native packages, and
`pi update <source>` still updates a single source. `--offline` and
`PI_OFFLINE=1` suppress automatic Cargo registry and Git package downloads,
including refreshes of temporary packages, and reuse the installed resources.
Uncached remote packages are skipped with a diagnostic. Local packages can
build with Cargo's offline dependency resolution.

Offline startup also skips automatic model catalog and capability probes for a
remote endpoint. Loopback Ollama discovery, explicit model listing and model
requests remain available. Remote wildcard model scopes fall back when a
catalog is unavailable.

A fixture verified the update flag, cached Git resource loading and skipped
uncached installations with a sentinel Git executable. It also verified RPC
startup without a remote catalog and model listing against local Ollama.

The tree's copy binding copies the full selected text and keeps the tree open.
Shell entries copy their command, and structured tool results copy their text
blocks. Selecting the current head is a no-op. The fork picker opens on the
most recent user message and reports when the branch contains none. A PTY
fixture exercised full Unicode clipboard text and the fork preselection.

### Branch summary checkpoint

Selecting a different tree position offers checkout without a summary, model
summarization, or summarization with custom instructions. The
`branchSummary.skipPrompt` setting selects direct checkout, and
`branchSummary.reserveTokens` controls context reserved for the summary request.
The request uses only the abandoned part of the conversation, has no tools,
and honors cancellation, agent retry settings and the configured output budget.

A single journal record attaches the completed summary at the chosen position.
It preserves the abandoned branch and does not replace retained context.
Cancelled or failed requests leave the original position active. Summaries
survive reopening, branch cloning, Pi JSONL import and HTML export. Read
and modified paths are sorted and deduplicated, including paths from prior
summaries and nested native tool calls. Native extension hooks for tree
navigation remain separate parity work.

A loopback model and PTY verified custom instructions, common-ancestor
exclusion, a tool-free request, atomic attachment, cancellation and terminal
restoration. A second fixture covers a retryable HTTP 503 and cumulative file
metadata. The journal test checks the summary's parent and branch replay.

### Interactive model scope checkpoint

`/scoped-models` opens the Ollama catalog and lets the user search, toggle and
reorder the models used by Ctrl+P. Enter toggles a model. Ctrl+A and Ctrl+X
operate on the current search results, and Ctrl+P within the picker toggles
the provider. Changes apply immediately for the running session. Ctrl+S
persists the ordered selection in `enabledModels`.

The PTY fixture verified that changing the scope leaves the settings file
alone until saving, preserves the chosen order for cycling, and selects the
first configured model after restarting. Unavailable catalog entries and
cross-provider selection remain separate work.

### Startup visibility checkpoint

`quietStartup: true` suppresses the terminal startup header and key hints.
`"header"` keeps them and suppresses the model scope details. `--verbose`
restores both. The fullscreen layout reuses the header row when quiet startup
is enabled. A PTY fixture covers all three settings and the flag override in
fullscreen and regular modes.

### Native Anthropic provider checkpoint

`--provider anthropic` or an `anthropic/<model>` reference selects the native
Messages API. The standard endpoint reads `ANTHROPIC_API_KEY`. An explicit
`--api-key` or host-provided model token can be used with a custom endpoint.
The token is excluded from output and session records. One provider is
selected per invocation.

The Rust adapter supports paginated catalog discovery, reported context
and output limits, image inputs, text and tool-call streaming, tool results
and cache-aware token usage. It translates request and stream errors into
the existing agent error categories. The journal records the selected
provider separately from the model ID and accepts older records.

A loopback fixture verified pagination, authentication, image input, a real
native read tool round trip, usage and RPC. Live account sign-in and provider switching within a running session remain open.

### Anthropic thinking checkpoint

The native Anthropic adapter reads the model's advertised thinking mode and
effort levels. It selects adaptive effort or a bounded manual thinking budget
and reports thinking progress even when the provider omits visible text.
Unsupported levels follow the shared model-aware controls.

Signed and redacted blocks are retained in the original assistant block order
for the same provider and model. Tool continuations replay those blocks along
with the tool use and result. A context edit invalidates replay when it
changes the assistant text or tool calls. Native executable context hooks
receive the portable messages and retain replay for unchanged turns.

A loopback fixture verified adaptive effort, signed and redacted block replay
through a real tool call, and replay after reopening the journal. Pi JSONL
imports also recover signed blocks from native Anthropic Messages history.
Live account verification remains separate work.
