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
| Terminal | Default `pi` startup, Unicode multiline editor and kill ring, text/image clipboard paste, scrolling, prompt history, Markdown, aligned tables, links and syntax highlighting, tool panels and collapse, visible thinking blocks, file completion, external editor, user keybindings, cancel, session controls | Remaining Markdown and diff layout fidelity, remaining application actions and clipped-image behavior |
| CLI modes | Interactive, `--print`, `--mode json`, basic `--mode rpc`, existing subcommands | Full Pi argument and output compatibility |
| Local tools | Text and image read, paging, write, batch/legacy exact edits with diff output and file mutation locks, bash, Windows PowerShell selection, grep, find, ls | Complete output/error/truncation parity |
| Sessions | Project-grouped journals, saved working directory, names and labels, searchable resume/tree/fork selectors, independent branch clone, native and upstream JSONL import, HTML/JSONL export, clipboard copy, GitHub gist share, compaction, read-only child review | Remaining upstream message variants and display fidelity, richer tree navigation and branch summaries, threaded session selectors, Radius sharing |
| Model access | Ollama and configurable OpenAI-compatible chat endpoint, bearer token, asynchronous searchable model selector, branch-local model/thinking selection, reasoning-effort control | Full provider catalog, provider-native adapters, capability-aware thinking levels, retry configuration |
| Authentication | Host-supplied model token | Provider login/logout, OAuth, credential storage and refresh |
| Native extensions | Rust tool, context, policy, command, state/migration and compaction hooks; standard notes/TODO/plan/review; separately built executable loading, scaffolding and native package distribution; correlated dialogs, notifications, status, editor access and positioned widgets | Remaining Pi events and custom terminal components |
| Project resources | Hierarchical AGENTS/CLAUDE and override files, trust persistence and prompt, user/trusted-project settings, skills discovery and forced invocation, argument-aware prompt templates, package resources and reload | Complete settings editor UI and remaining validation diagnostics |
| MCP | Existing AIP read-only Linear host integration; local MCP stdio and streamable HTTP clients; user/trusted-project configuration; namespaced tools, resources, URI templates and prompts; deferred tool discovery, codemode exposure and task polling; environment/command credentials; OAuth PKCE login/logout, private credential persistence and refresh; add/remove/list commands | In-flight tool-list updates, client metadata registrations and interactive scope upgrades |
| Codemode | Bounded QuickJS execution, journaled native tool calls, parallel reads, transactional branch-local store, discovery, text/image output and console capture | Non-chat model invocation and remaining execution-limit fidelity |
| Themes | Pi theme schema, system/dark/light themes, indexed/hex/OKHSL/OKLCH colors, variables, user and trusted-project loading, `/theme`, `/reload`, live custom-file reload | Automatic system appearance notifications |
| SDK | Public Rust agent/session/model/tool/extension interfaces, owned session builder, controls, event subscriptions, in-memory/persistent forks, JSONL export, compaction and lifecycle shutdown | Complete resource/model/auth factories and remaining Pi SDK equivalents |
| Packages | Native Cargo/git/local install, remove, update, list, per-source build cache, dependency builds and filtered resources | Remaining native source/filter edge cases |

## Current RPC subset

The native JSONL interface supports `prompt`, `steer`, `follow_up`, `abort`,
`get_state`, `get_messages`, `new_session`, `set_model`, `compact`,
`get_session_stats`, `set_session_name`, `export_html`, `get_available_models`,
`cycle_model`, `set_thinking_level`, `cycle_thinking_level`,
`get_available_thinking_levels`, `switch_session`, `fork`, `clone`,
`get_fork_messages`, `get_entries`, `get_tree`, `get_last_assistant_text`,
`edit_context`, `bash`, `abort_bash`, `clear_queue`, `set_steering_mode`,
`set_follow_up_mode`, and `shutdown`. Responses carry the request ID and a
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
were absent from captured CLI output. This checkpoint exposes enabled tools
directly; OAuth, deferred exposure and live reconnection remain in the table.

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
the caller's ID, `abort_bash`, and `isBashRunning` in `get_state`. The caller
waits for the final command response. Native and imported Pi shell records keep
context-exclusion flags. The existing journal's pending tool operations must
be resolved before a direct shell command starts.

The current frontend serializes shell and agent operations. Running a user shell
concurrently with a model response remains part of the outstanding acceptance
work. The public workbench shell API and session append API are available to
Rust embedding applications.

RPC and both terminal modes passed fixtures covering streamed UTF-8 output,
nonzero exits, included/excluded model context, cancellation, process cleanup
and durable restoration. Core tests cover exclusion after reopening the journal,
and shell tests cover partial UTF-8 chunks and timeouts.

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
server. Client metadata document registration and interactive scope upgrades
remain open.
