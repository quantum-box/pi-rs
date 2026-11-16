# Pi native feature parity

The reference is Pi v1.5.0, commit
`f9a26365568127461c47a07a37dcb42c140a3ac5`.

Automatic user and trusted-project prompt discovery scans visible Markdown files
directly in each `prompts` directory and honors its ignore files, matching Pi.
Nested or hidden prompt files can still be selected explicitly.
The requested compatibility is equivalent functionality implemented in Rust.

Markdown headings are separated from a following block by one blank line,
without adding a trailing blank line after a final heading.

[Reference CLI](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/docs/cli.md)
· [Reference terminal](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/docs/usage.md)
· [Reference extensions](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/docs/extensions.md)

Full parity is in progress. Each row lists the implemented surface and the
remaining acceptance work for that surface.

Hidden thinking runs display a themed `Thinking...` label. In fullscreen mode,
clicking a thinking run toggles it independently, including while it streams;
changing the global visibility setting clears individual overrides. Regular
mode uses the global visibility setting and does not handle transcript clicks.

Single-line extension input dialogs remove pasted line breaks and expand tabs
to four spaces, while multiline editor dialogs normalize CRLF and CR to newlines.
The native one MiB response limit applies after paste sanitization and expansion.

Visible thinking blocks render Markdown with the thinking text style and
preserve the Markdown theme's explicit colors. As in Pi, Mermaid fences in
thinking remain code rather than being transformed into diagrams.

OpenAI Responses streams both reasoning text and reasoning summary text as
thinking deltas. Completed reasoning items retain their summary when present,
otherwise their content or streamed text, in the completed turn metadata.
The native RPC stream preserves reasoning item boundaries and final text.
The additive enriched model stream carries those boundaries through the built-in
workbench and budget wrappers; third-party wrappers using the legacy stream
continue to receive deltas without item boundaries.

Chat Completions providers configured with `requiresThinkingAsText` retain
streamed reasoning and replay it as text before the answer for the same model.

Gemini streaming retains the first nonempty, bounded response ID for the turn
and reports it before completion so a handled interruption can record it.

Endpointless API keys in Pi's `auth.json` are accepted for custom providers
declared in `models.json`. The saved key takes precedence over the profile key
and is used only for endpoints declared by that profile; keys with an explicit
endpoint remain bound to that endpoint.

`--resume` and `-r` open the session selector without consuming a following
prompt. The picker uses the controlling terminal, keeping standard input and
output available to print, JSON, and RPC modes. The native `--resume=PATH`
form opens a specific session; `--session PATH` is the Pi-compatible spelling.
Invalid `--thinking` values produce a warning and leave the previous valid
selection or configured default in place. Repeated values use the last valid
selection, including when an invalid value follows it.

`httpIdleTimeoutMs` accepts Pi's numeric strings and case-insensitive `disabled`
with ECMAScript whitespace, and defaults to five minutes. It applies to model HTTP response
headers and individual body reads; `0` or `"disabled"` disables this idle
deadline. `/settings` can change it. The separate `--model-timeout-secs` bounds
the entire model operation and defaults to 2,147,483 seconds.

Model discovery and model/codemode HTTP clients, Radius OAuth and session
sharing, OpenRouter OAuth, and MCP HTTP connections honor `httpProxy`, the
HTTP(S) proxy environment variables, and `no_proxy`/`NO_PROXY` domain, wildcard,
and port exclusions. MCP OAuth discovery, explicit metadata, dynamic registration,
authorization-code exchange, refresh, login, scope upgrades, and reconnects use
the same settings. A loopback CLI test exercises the actual workbench `httpProxy`
adapter and `no_proxy` for discovery, explicit metadata and dynamic registration.
Client-level loopback tests also cover code exchange and refresh through a CONNECT
proxy and through a supplied `NoProxy` exclusion; no browser or live credentials
are used. To prevent proxy DNS exemptions from bypassing address checks,
OAuth requests and redirects to a configured proxy hostname are rejected, even
when `NO_PROXY` selects a direct connection. Embedders using the original
`HttpOptions` without proxy hostname information apply DNS filtering to proxy
hostnames too; `HttpOptionsWithProxyHosts` enables private DNS proxy hosts.
Lowercase environment variables take precedence, and explicit
environment values take precedence over the setting. Proxy URL credentials are
not displayed in the settings list. Workbench-configured reqwest proxies use
CONNECT for HTTP as well as HTTPS origins, matching Pi. Externally supplied
authenticated forward-only proxies retain forward behavior. Proxy decisions are captured per request and route-specific connection pools prevent
credentials from following a request onto another route. Caller-provided forward
proxy authorization is bound to the first route across redirects. Caller-provided proxy
authorization is removed for direct and local-socket requests, and passed only
to the selected proxy for CONNECT. It overrides configured basic authentication;
configured proxy-specific headers retain their existing precedence. HTTP/3 rejects matched proxy routes rather
than bypassing them. Other network surfaces need integration.

The `/settings` list remains open after a change, preserving its search and
selected item so several values can be changed in succession. Enter activates
the selected setting; Space also activates it when the search is empty. Closing
or saving a JSON editor returns to the list, and Escape closes the list.
When a custom autocomplete item limit is outside the presets, the next change
selects the first preset, matching Pi's settings selector.
Steering and follow-up delivery modes can be changed from this list even while
the model is streaming; the active session and the global defaults both update.
The list shows the session's selected mode when reopening an existing branch.
TUI mode changes take effect immediately, including during streaming, without
closing the settings list. Quiet startup, share destination, default project
trust and fullscreen wheel speed can also be cycled directly in the list.
The theme submenu previews the highlighted theme, restores the original on
cancel, and configures either a single theme or separate light/dark themes for
automatic terminal-appearance switching.
The per-model thinking submenu discovers models and their supported levels off
the terminal thread, saves or clears individual overrides, and preserves other
user and trusted-project settings. Changes for the active model update the
session; when an agent run is in progress, they take effect after that run.
While supported levels load, up to 1024 keyboard or paste events are retained
for the level selector; the configured picker cancellation key cancels loading
and discards that typeahead.
Saving a default model writes Pi's `defaultProvider` and literal `defaultModel`
together. If a saved model falls outside a nonempty model scope, it is added to
that scope and to a configured `enabledModels` list. Legacy qualified defaults
without `defaultProvider` are still accepted.

| Surface | Implemented in Rust | Remaining work |
| --- | --- | --- |
| Agent runtime | Streaming, ordered tools, cancellation, steering, follow-up, recovery, usage | Provider-specific thinking and complete multimodal event handling |
| Terminal | Default `pi` startup, Unicode multiline editor with word-aware wrapping, visual movement, character jumps and kill ring, text/image clipboard paste, fullscreen transcript search and semantic prompt jumps, configurable scrolling and scrollbar, mouse text selection with clipboard copy, exit transcript or resume hint, prompt history, Markdown, aligned tables, links, syntax highlighting and native Mermaid diagrams, configurable transcript padding and hardware or software cursor, optional regular-screen clear on shrink, tool panels and collapse, visible thinking blocks and model progress, file completion, external editor, user keybindings for editor and selectors, model and thinking default selection, cancel, double-Escape navigation and tree filters | Remaining Markdown and diff layout fidelity, remaining application actions and real-terminal image clipping verification |
| CLI modes | Interactive, `--print`, `--mode json`, `--mode rpc` commands with streamed message, turn, and tool lifecycle events, existing subcommands | Full Pi argument, session response, and event compatibility |
| Local tools | Text and image read, paging, write, batch/legacy exact edits with diff output and file mutation locks, configurable Bash and Windows PowerShell with session metadata and Pi-style exit and truncation text, structured read, search, listing and shell truncation details, grep, find, ls, concurrent user shell and model execution | Complete output/error/truncation parity |
| Sessions | Project-grouped journals, saved working directory, names and labels, searchable resume/tree/fork selectors and toggleable label timestamps, optional model-generated branch summaries, parent-session grouping, independent branch clone, native and upstream JSONL import, HTML/JSONL export, clipboard copy, Radius and GitHub Gist share, compaction, read-only child review | Remaining upstream message variants and display fidelity, remaining tree layout and navigation hooks, share metadata for standalone CLI exports |
| Model access | Ollama, configurable OpenAI-compatible chat endpoint, native OpenAI Responses, Anthropic Messages, Google Gemini Generate Content, Radius Pi Messages, and Amazon Bedrock Converse streaming, named profiles for those APIs, API keys and bearer tokens, searchable built-in chat model catalog, including native OpenAI, Anthropic and Gemini catalogs and the Fireworks and OpenRouter Messages models, model declarations and overrides including model-specific APIs and endpoints, cross-provider switching and scoped cycling, branch-local model/thinking selection, capability-aware thinking levels, catalog pricing and usage estimates, agent retry configuration | Additional provider catalogs and native adapters, remaining model configuration fields |
| Authentication | Host-supplied model token; masked terminal `/login` and `/logout` for native provider API keys; endpoint-bound private storage, Radius browser and device OAuth with refresh, OpenRouter browser OAuth with manual redirect fallback, ChatGPT plan browser OAuth with token refresh and account selection, Kimi Code device OAuth with refresh | OAuth and refresh for other model providers; ChatGPT live acceptance |
| Native extensions | Rust tool, prompt input, direct process execution, command-created branch-local custom entries and model-context messages, durable custom messages from native tools and supported callbacks during runs or while idle, user-shell result replacement and streaming operations, in-process shell spawn hooks, injectable model read, write, edit, shell, grep, find and ls operations, model and thinking selection, context and full-system context hooks, policy, command, state/migration, compaction, resource discovery and agent/turn lifecycle hooks; standard notes/TODO/plan/review; separately built executable loading, typed command-line flags, scaffolding and native package distribution; correlated dialogs, notifications, status, editor access, positioned widgets, and styled modal components | Remaining Pi events and selection paths, custom entries and messages from all callbacks, custom message renderers, resource provenance in other surfaces and general component render callbacks |
| Project resources | Hierarchical AGENTS/CLAUDE and override files, trust persistence and prompt, user/trusted-project settings, skills discovery and forced invocation, argument-aware prompt templates, package resources and reload | Complete settings editor UI and remaining validation diagnostics |
| MCP | Existing AIP read-only Linear host integration; local MCP stdio and streamable HTTP clients; user/trusted-project configuration; namespaced tools, resources, URI templates and prompts with per-server resource pagination; deferred tool discovery, codemode exposure and task polling; environment/command credentials; OAuth PKCE login/logout, private credential persistence, refresh, first-use consent, configured clients and interactive scope upgrades; automatic selection of configured hosted client metadata; add/remove/list commands; tool catalog notifications; per-request model-provider bearer forwarding | Live acceptance with a hosted pi-rs client identity |
| Codemode | Bounded QuickJS execution, journaled native tool calls, parallel reads, transactional branch-local store, discovery, text/image output and console capture, classifier and image model calls via System One and OpenRouter, catalog and durable usage | Additional non-chat provider APIs, complete chat-model catalog, provider retries and remaining execution-limit fidelity |
| Themes | Pi theme schema, system/dark/light themes, indexed/hex/OKHSL/OKLCH colors, variables, user and trusted-project loading, `/theme`, `/reload`, live custom-file reload and VT appearance notifications on Unix and Windows | Real Windows Terminal acceptance and remaining palette synchronization |
| SDK | Public Rust agent/session/model/tool/extension interfaces, owned session builder, host-owned exact-identity model catalog and recovery, coherent local and remote coding/read-only tool factories, host-granted system prompt and explicit-directory context resource snapshots, controls, event subscriptions, in-memory/persistent forks, JSONL export, compaction, idle custom-message turn triggering and lifecycle shutdown | Complete resource/model/auth factories and remaining Pi SDK equivalents |
| Browser SDK | Experimental text-only Rust `AgentSession` via a Fetch gateway; localStorage journal with Web Locks, cancellation, timeout, reload restoration and interrupted-run resumption tested in Chrome | Native tools and extensions, provider-specific adapters, streaming UI events, multimodal input, branch navigation and other browsers |
| Packages | Native Cargo/git/local install, remove, update, list, per-source build cache, dependency builds and filtered resources | Remaining native source/filter edge cases |

Package list emits native JSON events for every configured entry in user then
trusted-project order, including duplicates, with `scope` and `filtered` fields.
Transient `-e` sources are excluded, as in Pi's configured-package list.

Local package `file://` sources ignore URL query and fragment components and
reject malformed path escapes and encoded separators, as Pi does.

ChatGPT plan credentials respect `auth check --no-refresh` and the explicit
minimum lifetime for bearer-token export. A refreshed credential is saved
before a short lifetime is reported, preserving refresh-token rotation. In a
live check with an expired saved credential, an instrumented local build marked
its own successful refresh persistence before returning `READY` from a text
inference. The saved access and refresh values both changed and the expiry was
in the future afterward. The probe preserved credentials on authorization
failure; its successful refresh path was otherwise unchanged. New-account
browser login, other account configurations and broader live acceptance remain
open.

`pi mcp add` accepts `--cwd` for stdio servers, repeated `--header` for HTTP
servers, and `--description` for either transport. HTTP headers can be combined
with `--bearer-token-env-var` unless an Authorization header is already present.
`--env` is restricted to stdio and requires nonempty names; bearer-token
environment variables apply to HTTP servers.
HTTP `mcp add` also accepts `--oauth-client-id`, `--oauth-client-secret`,
`--oauth-callback-port`, `--oauth-client-name`, and
`--oauth-client-registration`. pi-rs requires a client ID when a client secret
is supplied and disallows mixing OAuth settings with an Authorization header.
The secret option is saved as supplied in `mcp.json`. Quote a reference such
as `'${CLIENT_SECRET}'` or use `!command` to resolve the secret during login
without storing its value in that configuration file.

Local package sources expand `~` using a nonempty `HOME` or the platform
home-directory fallback, including `~\` on Windows. On Unix an explicitly
empty `HOME` remains empty. Relative home values resolve from the source
settings scope, matching Pi.

Package resource filters expand home-relative paths and local `file://` URLs
before matching, including exact `+` and `-` overrides, as in upstream Pi.
An invalid file URL in a package settings filter reports an error when
reached against an available native or Pi manifest diagnostic candidate. For a local directory
package it stops the current declaration at that resource type, retaining
earlier resources and processing later declarations. Remote errors propagate.
Startup theme discovery uses cached candidates when available. Single-file
sources load only the selected executable and bypass package filters, as in Pi;
the native executable must pass protocol validation. Other resource filter
paths still treat invalid URLs as non-matches.
Pi-only extensions, skills and prompts are scanned only for filter diagnostics;
they are not loaded as native resources. Diagnostic scans are bounded and fall
back to native candidates on cycles, excessive traversal, unsafe ignore files,
unsupported absolute/parent globs, potentially traversed directory symlinks,
or unsupported
extglob forms. Diagnostic
extglob discovery supports bounded ASCII positive groups and a single negated
group with literal alternatives and a supported suffix; complex negation,
suffixes ending in wildcards, extglob overrides, non-ASCII
paths, and combinations with classes, braces, or globstars fall back, so errors
can still be missed in those
cases. Package resources are evaluated in settings order, with project sources
first.

Diagnostic manifest globs expand bounded numeric and same-case ASCII letter
brace ranges, including descending ranges, signed steps and padded numbers,
and include the starting directory for a trailing directory globstar. Complex,
invalid or oversized brace expansions fall back to native candidates.

Pi JSONL imports retain a UUID session ID through native export and reopen,
including for model request affinity. Pi also permits non-UUID custom IDs;
custom IDs are preserved across native export and reopen and used for
model request and cache warming affinity. A separate internal UUID continues to
identify sessions for existing SDKs and hosts; forks receive a fresh UUID and
external ID. RPC state, session statistics, resume search, share headers and
model-shell metadata project the external ID. Empty IDs omit affinity headers.
Nonempty IDs use Fetch-compatible Latin-1
header bytes and whitespace normalization. Unrepresentable or invalid values
fail before dispatch when an affinity default is needed; explicit headers win
without validating an unused default. An ID containing NUL is omitted from the
child process environment. Internal UUIDs remain accepted for native session
lookup.

Fullscreen selector lists accept mouse wheel navigation and click selection,
including settings and scoped models. Dragging does not activate an item and
mouse interaction preserves theme previews and keyboard remapping.

Input hooks identify command-line print and JSON prompts as `interactive`, as
Pi does, while RPC and extension-originated submissions retain their own source.
Pi-style `--mode json` emits the session header and agent events without an
additional native `answer` record; native `run` output retains that record.
An input hook failure is reported without discarding earlier transformations or
preventing later hooks from processing the input. Invalid replacement images
still fail validation before submission.

Known text-only chat models receive Pi's image-omission placeholders for user
and tool images. Unknown model capabilities leave image handling to the
provider, and the explicit image-blocking setting retains precedence.
Local image reads also report when the selected model cannot receive images;
the saved tool result retains the image for later use with a compatible model.

OpenAI Responses `max_output_tokens` incomplete events preserve the streamed
partial response and report the `length` finish reason. Across model providers,
tool calls in a length-limited response are never executed. When a recoverable
length stop has an older prefix to compact, native omits the attempt and tries
one compact-and-retry recovery; otherwise, it records failed tool results and
the model may request the tools again. Other incomplete reasons remain errors.
Truncated compaction and branch summaries are rejected before changing the
conversation branch.
Model-generated compaction summaries include Pi-style `read-files` and
`modified-files` sections for projected `read`, `write`, and `edit` calls in
the summarized prefix, including native nested calls attached to a visible
tool result. Modified files are excluded from the read-only list; paths are
deduplicated and sorted using JavaScript string order. The generated file lists
are retained as compaction `details` on metadata-capable stores and carried
forward from earlier model-generated compactions. Extension-provided summaries
and their details are preserved as supplied. Imported nested-call metadata not
represented as native operations is not yet included.
If appending the complete file listing would exceed the native 64 KiB summary
limit, native omits the listing from the summary but retains it in `details`
on metadata-capable stores. Generated file details are bounded to at most 4 MiB
of encoded JSON, using the actual encoded record envelope to honor the active
store's record limit; modified files are retained first. If entries do not fit, the native metadata carries
`nativeFileDetailsTruncated: true` across reopen and later compactions. Pi's
projected `details` contains only the retained paths. A legacy hook that edits a generated summary retains
the native file history across journal reopen; Pi exports still expose it as a
hook-originated compaction.
For the Cerebras Chat Completions provider, a fully received, empty HTTP 400
response is treated as context overflow, matching Pi's bodyless-error rule.
Other providers and nonempty 400 bodies retain their normal classification;
incomplete or timed-out bodies do not qualify as empty. Existing HTTP 413
handling is unchanged.
Native extensions can opt into a pre-summary compaction hook for manual,
threshold, and supported overflow compaction. A supplied summary bypasses the summary model, carries
optional details and usage into the journal, and emits the normal lifecycle
notification; cancellation leaves the branch unchanged. Executable extensions
negotiate this hook before receiving the new request. Existing post-summary
compaction hooks continue to run. The detailed compaction lifecycle callback and
negotiated executable protocol carry the `overflow` trigger; legacy lifecycle
callbacks receive `threshold` so their closed Rust enum remains source compatible.
An extension-provided summary does not consume
a model request, so threshold compaction can use it even when only the turn's
final model request remains. A declined optional hook preserves that request and
does not emit a failed-compaction event. Unlike Pi, the native hook must retain at
least the prepared recent context and uses native numeric journal IDs; cuts that
split a user message from its legacy image records or a tool call from its
result are rejected. Later legacy hook changes can retain more history, but
cannot move an extension-provided boundary forward. Native summary size and
timeout limits still apply. Custom `SessionStore` implementations that do not
support atomic record metadata reject new pre-summary extension compactions
rather than silently dropping their metadata. Legacy compaction hooks continue
to work on those stores with their previous metadata-free behavior.
An unsupported metadata append is rejected before any write, so the existing
session remains usable. When a completed model summary is recovered, usage is
settled with its original request ID and metadata before compaction is committed;
the host must make settlement idempotent per request ID. A failed settlement
leaves the summary available for retry instead of silently closing the run.
SDK `resume()` also finds a recoverable summary on the latest branch run after
reopening, and resumes that run instead of starting an unrelated one. It also
recognizes interrupted compaction markers before a summary is complete, and
recognizes a durable overflow or length stop before its marker is written. It
restores the branch model before checking the interrupted request's identity;
queued trigger turns do not overtake an unresolved compaction recovery.
Queued steering and other deferred context are held until interrupted
compaction is resolved; an unresolved request leaves them queued.
SDK recovery defers session-start callbacks until recovery succeeds; when
extensions are registered it settles known and unconfirmed recovery usage using
the restored model, then requires reconciliation before invoking them. An
interrupted hard-overflow summary request is settled as unconfirmed before that
reconciliation; it is never automatically resent.
Public startup and prompt paths also reject an unresolved recovery before
callbacks or queued triggers run. For an ambiguous silent-overflow summary,
resume retains the answer, settles any started summary request as unconfirmed
when its outcome is unknown, and records a durable skip before accepting more
work. Failed settlement leaves recovery pending for retry.
The low-level agent similarly rejects a new run or direct queued-input
delivery until the interrupted run has been resumed or reconciled.
Ordinary silent-summary model or budget failures also record the skip before
returning the answer. A saved summary's incurred usage is settled even when
the restored run budget prevents further work.
Saved summary usage is settled even when current request limits or the
auto-compaction setting prevent replay. Cancellation during a silent summary
returns cancellation and leaves the durable operation available for recovery.
Explicit overflow recovery also settles durable failed, cancelled, truncated,
empty, or oversized summary completions before deciding whether they can be
reused; uncertain work still requires reconciliation rather than a resend.
Saved silent-overflow recovery runs before checking the current auto-compaction
setting or model window. A resumed run restores request and output limits from
its journal; missing or unconfirmed usage reserves the request's full output
allowance, while a replayed reply or summary is charged once.
If cancellation arrives before silent recovery begins, it returns cancellation
without delivering queued steering or marking the saved operation complete.
Overflow recovery remains partial: when an
interrupted operation may have invoked an extension hook or sent an unconfirmed
summary request, native does not repeat the ambiguous operation. Retry-required
recovery reports that reconciliation is needed; for a completed silent-overflow
answer, native preserves the answer and skips the ambiguous compaction. An
automatic reconciliation workflow, full Pi preparation payload and all
provider overflow classifications remain gaps.

Native extensions can opt into tool execution start, update and end events,
including nested call ancestry, partial results, final outcomes and elapsed
execution time. Rust tools with a host can emit bounded progress while running;
the built-in shell tool emits bounded cumulative output updates. Other built-in
tool progress and completion-order notifications remain open.

### Meta Responses API-key checkpoint

The `meta` provider includes Pi v1.5.0's six Muse models, including their
thinking levels, image limits, token limits and prices. It uses the Responses
adapter and `META_API_KEY` on the official endpoint. Saved and configured
credentials retain precedence and remain bound to their endpoint. Existing
Pi-compatible Meta subscription OAuth credentials can supply the inference key;
when it expires or has less than five minutes remaining, the identity token is
used only at Meta's fixed key-mint endpoint, and the new 24-hour key is stored
under the auth lock. Concurrent refreshes recheck the stored expiry. OAuth
credentials are accepted only for the official Meta Responses endpoint, and
redirects are disabled for the mint request. `/login meta` starts Meta's
device flow, displays the verification URL and code, and saves credentials
only after key minting succeeds; `/login meta api-key` retains manual API-key
entry. Device and mint destinations are fixed, redirects are disabled, and a
changed credential aborts the final save. Verification links must use HTTPS;
unlike Pi, HTTP verification links are rejected. Live Meta account login,
key minting and inference have not been verified.

### xAI Responses checkpoint

The native xAI provider uses the pinned Pi Responses catalog and
`XAI_API_KEY`, with model-specific thinking levels and a provider identity
separate from OpenAI and ChatGPT. `/login xai` and `pi login xai` also support
Pi's xAI subscription device authorization, pending and slow-down polling,
Pi-compatible credentials, five-minute expiry skew, and refresh-token rotation.
The terminal's device-code prompt can open the verification URL and copy the
code on confirmation while polling continues; cancellation discards the action.
`/login xai api-key` retains manual API-key entry. OAuth credentials are accepted
only for the official xAI Responses endpoint; token requests use the fixed xAI
authorization host, disable redirects, and never print response bodies. Refresh
is serialized and persisted even if the requesting task is cancelled. The
terminal retains an in-progress credential commit when cancellation arrives.
The adapter supports streamed Responses events and tool calls through the shared
Responses implementation. Loopback fixtures do not replace live login, refresh,
or inference acceptance with an xAI subscription.

### Qwen Token Plan checkpoint

The international, China, and individual Qwen Token Plan providers expose the
pinned Pi model catalogs, regional credentials, per-model reasoning maps, and
image limits. Their chat-completions requests use Qwen's `enable_thinking`
switch, with `reasoning_effort` only where the selected model supports it.
Explicit strict-mode support emits the reference `strict: false` tool setting.
Loopback CLI coverage does not replace live Token Plan acceptance.

### Xiaomi MiMo checkpoint

Xiaomi's pay-as-you-go endpoint and its China, Amsterdam and Singapore token
plan endpoints use distinct Pi v1.5.0 catalogs and credentials. Their chat
requests use enabled/disabled thinking and replay reasoning content through
tool turns. Loopback CLI coverage does not replace live Xiaomi acceptance.

### Ant Ling API key checkpoint

Ant Ling uses the pinned Pi catalog of two Ling models and the reasoning
Ring model, its official endpoint, and `ANT_LING_API_KEY`. Ring sends its
mapped high or xhigh effort in the provider's nested `reasoning` field.
Live acceptance with an Ant Ling account remains open.

### OpenCode Zen classifiers

OpenCode Zen's two pinned Jev classifiers are discoverable through Codemode,
using its official System One endpoint and `OPENCODE_API_KEY`.

### Vercel AI Gateway checkpoint

Vercel AI Gateway exposes the 303 chat models and 14 classifier models from the
pinned Pi v1.5.0 catalog using `AI_GATEWAY_API_KEY`. Classifiers use the gateway's
System One endpoint, and Codemode supports discovery and classification with
the same endpoint-scoped credential rules as other providers. Model-specific
reasoning and empty-signature compatibility are retained. Live acceptance with
a Vercel account remains open.

### Kimi Code API key checkpoint

The native `kimi-coding` provider uses Pi's coding endpoint, `KIMI_API_KEY`,
and the six pinned Kimi chat models. Their image limits, prices, context windows,
and per-model reasoning settings remain distinct; the high-speed coding model
uses its DeepSeek-style thinking switch. Subscription OAuth supports device
login, refresh, and Pi-compatible credential storage. Live Kimi acceptance
remains open.

On terminals without OSC 8 support, Markdown links use the reference's text
fallback: link text is underlined and destinations appear in parentheses only
when they differ from the visible URL or email address. Supported terminals use
OSC 8 hyperlinks, as described in the terminal capabilities checkpoint below.

MiniMax and MiniMax CN use their separate Pi v1.5.0 Messages endpoints and
`MINIMAX_API_KEY` / `MINIMAX_CN_API_KEY` credentials. Their bundled catalogs
include M3 and M3.1-Flash. Pi also lists M2.7 on these endpoints, but MiniMax's
current regional Messages, OpenAI-compatible and standard API references list
only M3 and M3.1-Flash as supported, so direct M2.7 entries are withheld. Both
regional model-list FAQs say existing pay-as-you-go users can access M2.7 until
November 18, 2026, and that subscription requests have routed to M3.1 Flash
since October 19, 2026; their retired-model tables already list M2.7 as retired.
The references do not specify a currently supported direct M2.7 route. See the
[international model list](https://platform.minimax.io/docs/faq/models) and
[China model list](https://platform.minimax.cn/docs/faq/models).

Budgeted Messages requests use Pi's default level budgets or the selected
`thinkingBudgets` override and summarized thinking display, subject to the
native output ceiling. Overrides must be integers of at least 1,024 tokens;
only the selected level is validated. Pi's simple API treats its requested
maximum as an answer budget and adds thinking tokens (bounded by model and
context capacity). In pi-rs the adapter receives only the hard ceiling: the
smaller of the per-request limit and remaining run budget, possibly lowered by
a context extension. Expanding it would exceed a host or extension limit;
there is no separate answer budget and headroom to expand. Raising both
`--max-output-tokens` and `--output-budget` permits a larger thinking budget
within those limits.
Adaptive Messages requests also explicitly request summarized thinking, as Pi
does, including for providers whose default display differs.

The interactive `/hotkeys` view groups navigation, editing, application and
fullscreen actions. It displays effective user-configured bindings in readable
form, including multiple keys and explicitly unbound actions.

## RPC support

RPC startup ignores ordinary positional messages, as Pi does; it accepts
prompts through stdin. Both reject `@file` arguments in RPC mode, with
different diagnostic wording.

Completed tool-call JSON parsing and incremental argument previews live in the
portable `pi-rs-tool-arguments` crate, shared by the SDK and terminal/RPC
frontends. The standalone crate is compile-checked for
`wasm32-unknown-unknown`; the experimental browser SDK supports the text-only
sessions described above. Preview behavior retains the existing native parser's documented
differences from Pi's streaming parser.

RPC `extension_error` events include Pi's `extensionPath` and `error` fields
alongside the native `extension` and `message` fields. `extensionPath` is the
executable's resolved source path when available; embedded extensions without
a source path use their registered ID. This also applies to cache warming and
resource-discovery hook errors, including startup discovery.

The native JSONL interface supports `prompt`, `steer`, `follow_up`, `abort`,
`get_state`, `get_messages`, `new_session`, `set_model`, `compact`,
`get_session_stats`, `set_session_name`, `export_html`, `get_available_models`,
`cycle_model`, `set_thinking_level`, `cycle_thinking_level`,
`get_available_thinking_levels`, `switch_session`, `fork`, `clone`,
`get_fork_messages`, `get_entries`, `get_tree`, `get_last_assistant_text`, `get_commands`,
`edit_context`, `bash`, `abort_bash`, `clear_queue`, `set_steering_mode`,
`set_follow_up_mode`, `set_auto_compaction`, `set_auto_retry`, `abort_retry`, and `shutdown`. Responses carry the request ID and a
success flag. Prompt acceptance follows input and command validation. Explicitly
empty or whitespace-only text reaches input hooks before native
empty-input validation, allowing a hook to handle or replace it. Unhandled empty
input remains invalid; unlike Pi, a busy `prompt` without `streamingBehavior`
is rejected before its hook runs. A prompt that runs an extension command returns `handled`.
Idle extension commands
take precedence over valid attached images in RPC; the
attachments are not sent to the model even when the command triggers a turn.
RPC validates images before command dispatch. Unlike Pi, a command that
triggers a model turn reports `started`, and commands submitted while a turn
is active cannot yet execute immediately. A model turn returns
`started` before completion. Wait for `agent_end` or `agent_settled` for that
turn. The interface emits native journal events alongside Pi-style
message start, update and end events, including indexed text and reasoning
blocks, tool calls, tool results and turn boundaries.
`toolcall_start` includes the ID and tool name available at that point. Native
does not yet expose Pi's top-level live usage in `message_update`; provider
usage currently reaches the stream only on completion. Native retains the cumulative
`message` and `partial` snapshots as extensions; unlike Pi's compact wire
format, update size therefore grows with streamed content. Fragmented provider
IDs or names can be incomplete in `toolcall_start`; the final call is authoritative.
Failed live assistant messages include a fixed `errorMessage` based on the
native failure classification, including cancellation and retryable failures.
Unlike Pi, native live failures do not retain provider error text in the journal
or include it in that field, and historical message snapshots do not reconstruct
every failed live attempt. Imported Pi records preserve their original error text.
Other error and retry notifications retain their own diagnostics.
Tool-call deltas expose
incremental argument previews, including nested objects, arrays and partial
strings. The fullscreen and regular terminals also show the prepared tool and
its partial path, command or pattern while the model streams it. Completed
calls are still validated before execution. `get_messages` returns
Pi message roles and blocks, with a correlated partial assistant message
during streaming. Completed unsigned Anthropic responses retain bounded text
and tool-call block order across session reopen when the stored layout matches
the current message; provider continuation still uses its existing representation.
Set `includeNative: true` to include the native model projection in `nativeMessages`. The `steer` and `follow_up` commands
can queue input before the next prompt without starting the model. The same
queue accepts prompts with `streamingBehavior` during a turn, expands prompt
templates and skills, and emits `queue_update` as inputs are saved or consumed.
Successful RPC steering and follow-up mode changes omit `data`, matching Pi.
Native persists mode changes asynchronously; overlapping requests can persist
out of order, and a settings-write failure can leave the live mode changed.
Queued input is acknowledged after it is saved. The `get_entries` and
`get_tree` responses use Pi entry types, string IDs, parent links, and a leaf
ID. Session records used internally for run recovery are omitted from this
view. `get_entries` compares `since` to string IDs without coercion and uses
JavaScript string conversion in the not-found diagnostic. Set `includeNative: true` for the native entries or tree and the native
leaf ID. Newly committed entries are emitted as `entry_appended` with those
same IDs. Full malformed-JSON preview fidelity and additional session events
remain in progress.

`get_commands` identifies discovered skills and prompt templates with their
extension source, temporary scope, resolved file path, and executable base
directory. Resource provenance in other surfaces remains in progress.
Discovered resources identify inline and built-in extensions separately as
`extension:inline:<id>` and `extension:builtin:<id>`.
Extension-provided relative paths resolve from the session working directory;
home-relative paths and local file URLs are accepted. A missing resource is
reported without preventing other discovered resources from loading.

Completed tool arguments repair invalid string escapes and unescaped control
characters as Pi does. Incomplete JSON and non-object values produce a tool
error containing the received arguments. The original model response remains
in the journal; subsequent model requests use the repaired object.

When all sibling timestamps are valid, `get_tree` sorts them in chronological
order and retains append order for the same instant. This includes imported timestamps with UTC offsets and ISO year, year-month, and date forms
interpreted at UTC midnight as in Pi, including day overflow within 1–31.
For mixed invalid dates, the comparator is not transitive, so
chronological order and equal-instant append order are not guaranteed. The
native implementation reproduces Node 22/24 V8 natural runs, insertion,
run-stack collapse and galloping merges. Mixed lists of 3–10,000 entries were
compared with Node 22.23.4 and 24.22.1; other JavaScript engines and versions
may behave differently. Other JavaScript date parsing edge cases
remain open. Entries whose parent is missing or points to themselves appear as
roots.

During manual compaction, an RPC `abort` response waits for the compaction
command to finish, even if an internal agent-settled event arrives first.

Unknown RPC commands return `Unknown command: <type>` and preserve the
request ID and original type when present. Object, array and numeric types use
JavaScript string conversion in the diagnostic, including null array elements.
Non-null JSON values without a command type also return `Unknown command:
undefined`, as in Pi. Native rejects a top-level `null` as a parse response;
Pi instead attempts to access command properties on it.
RPC `set_model` rejects non-string `provider` and `modelId` fields using Pi's
diagnostic conversion. Missing `provider` still selects within the current
provider as a native extension.
RPC `set_session_name` uses Pi's diagnostics for missing, null, non-string,
and empty names.
The echoed JSON value retains Rust's parsed integer precision, which can differ
from JavaScript for integers outside the safe range; out-of-range JSON numbers
such as `1e400` are rejected by the native parser.
Malformed JSON produces a `parse` response with a
parser diagnostic. Unescaped string control characters reached by the parser
use Pi's V8 wording and UTF-16 positions, including controls after a lone
surrogate in the same string. For records up to 1 MiB, a diagnostic-only retry
also recognizes controls after a surrogate in another string when it is the
next parser error. Larger records, retry depth or number-range failures, and
other unmapped errors retain the Rust JSON parser's wording.
Model-list discovery runs independently of the RPC input loop, so a slow model
endpoint does not block cancellation or other commands. Concurrent discovery
requests are bounded and retain their individual response IDs.

RPC model values expose Pi's `input` modality list for configured models in
state, selection, cycling, and discovery responses. Discovery and selection
responses for dynamic models derive the list from a reported image capability.
Known dynamic capabilities are retained for subsequent state and cycling
responses in the same RPC session. When capability data is unavailable, the
list is omitted rather than guessed. Configured model definitions default to
text input, while partial overrides inherit the existing model's modalities.
Concurrent discoveries return their own capability data; older completions do
not overwrite newer cache results or restore models removed from a complete
catalog. Cached capabilities are scoped to the
provider, model, endpoint, adapter, profile, and credential identity. The
cache has bounded entries and scope watermarks; eviction prevents older
in-flight results from reviving discarded state. Credentials requiring
external resolution and dynamic model or discovery request headers disable
capability reuse, because their effective account cannot be safely identified
without executing them again. The native `images` field remains available for
existing clients.

`set_model` checks the exact model ID against the selected provider's
configured or discovered catalog before saving the selection. A missing
provider or model reports `Model not found: <provider>/<model>`. Existing
native clients can omit `provider` to select within the current provider.

RPC `export_html` defaults to `pi-session-<session-basename>.html` in the
invocation directory. An explicit output path selects the destination
independently of its extension and can replace a previous export. The
destination is written atomically, and the active session file is protected
from being replaced by its own export. The export uses the native renderer.

Cancelled model turns with streamed text remain visible in RPC `get_messages`
after settlement and session reopen. Marked partials have stable entries in
`get_entries` and `get_tree`; `edit_context` can replace or omit their displayed
content without adding it to model context. This display-only editing differs
from Pi's general context-edit projection. Shared Pi transcripts reimport aborted
messages as failed assistant records; resume does not treat them as completed
answers, and the agent excludes both their original and edited content from later
model requests, generated branch summaries, and local
or hosted review prompts. `conversation_at` remains a display/history snapshot;
model-bound callers use `model_conversation_at`. Compaction and tree extension
hooks receive raw journal records, which can include cancelled partials; an
enabled extension controls how it uses those records. Interrupted summaries
remain private from the RPC message projection; older
journals without a turn marker retain their prior history projection. Native
history retains text only, so interrupted thinking and incomplete tool-call
blocks are not reconstructed after reopen.

`get_last_assistant_text` and `/copy` use the active transcript. Both skip
empty aborted assistant messages, concatenate the selected message's text
blocks, and trim the result. The RPC response omits `text` when the selected
message has no text or the transcript has no eligible assistant message.

`clone` requires a visible session leaf and returns `cancelled` on success.
`fork` also returns the selected user text. Pass `includeNative: true` on
either command to receive the new `sessionFile` in the same response. The
path is also available via `get_state` after the transition.

`get_state` omits an unset session name. For `--no-session`,
`get_state` and `get_session_stats` omit the session file unless the request
sets `includeNative: true` to inspect the temporary journal.

Each accepted RPC `set_session_name` request appends a session-info entry and
emits `session_info_changed`, including when the normalized name is repeated.
During model streaming, the RPC worker routes name updates through the core
input channel. The session owner commits the change before the RPC
acknowledgment, and a subsequent state query can read it during the same turn.
Session lifecycle extensions receive the committed name change without waiting
for the model turn to finish. A rename before the first prompt also loads and
notifies explicitly selected and discovered native extensions.

The Rust SDK's cloned session controls expose `set_session_name` during
active operations. Subscribers receive `SessionInfoChanged` after the name
is committed, and lifecycle extensions are notified while the operation is
still active. The owned SDK method `set_name_with_hooks` delivers the same
notification while idle.

The message snapshot preserves imported Pi roles, timestamps, and extension
message details. Context edits replace the affected content in the snapshot.
Imported Gemini conversations carry signed text, thought, and function parts
into continued requests. Tests covered a live partial, a typed tool result,
and a resumed signed Gemini tool conversation.

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

Markdown lists preserve the reference spacing between items in loose lists
with direct text paragraphs, including nested lists, and between block-only
items separated by blank lines, including within blockquotes. Tight lists remain
compact; blank lines inside fenced code do not loosen the enclosing list. Other block-only looseness and
layout cases still need comparison.

Markdown links use OSC 8 on terminals that advertise support, with a text-and-URL
fallback on unknown terminals and screen. `PI_HYPERLINKS=1` or `0` overrides
automatic detection; tmux is probed for its `hyperlinks` terminal feature.
Empty terminal-identification environment variables do not advertise OSC 8
support or indicate a tmux session.
Bare HTTP, HTTPS and FTP URLs, `www.` links and email addresses receive link
styling and the same terminal-dependent behavior. Bare URLs stop before CJK
punctuation while explicit links retain it in their destination. Markdown
delimiters inside bare links are treated as URL text before inline formatting,
while explicit links and code spans retain their normal interpretation.
Bare email links support ASCII dot-atom and quoted local parts, including
apostrophes, with URI-sensitive characters percent-encoded in the destination.
Unlike Pi, an unsupported Unicode local part or internationalized domain is
left unlinked rather than linking a fragment that names a different recipient.
Bare strings that are both a complete email address and a `www` URL are also
left unlinked; explicit Markdown links select the intended destination.
Domain literals are not recognized as bare email links.
Wrapped link fragments are closed on every rendered line, and control characters
in destinations cannot enter terminal escape sequences. Fullscreen clicks on
visible links launch the platform handler without a shell; dragging selects text
instead, and clipped or scrolled link positions are not activated.

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

In fullscreen mode with `fullscreenCopyOnSelect` disabled, the configured
`app.message.copy` shortcut copies the active mouse selection before falling
back to the last assistant message. The fallback reads a session snapshot, so
it remains available while a model turn owns the journal lock.

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

Native SDK hosts can explicitly load skill metadata snapshots from granted files
and format the visible skills with Pi's reader hint and XML escaping. Relative
paths require an explicit absolute base; parent components and non-UTF-8 paths
are rejected so rendering cannot change the file selected by the host. Hidden
model-invocation skills remain in the snapshot for explicit use. Reads follow
symlinks and do not grant later file access; no directories, packages or settings
are discovered implicitly. Hosts can explicitly grant recursive directories;
scans read local ignore files, skip hidden entries and node_modules, and stop at
an unignored SKILL.md. Directory grants authorize enumeration and ignore-file
reads and discovered skill-file contents throughout the granted subtree, including symlink targets outside it; they
are not a sandbox. Scans bound file, entry, ignore-byte, ignore-line and depth
counts. Each file is capped at 256 KiB. Read, ignore, parse and duplicate-name
errors fail the entire load, unlike Pi's per-file warnings and continuation.
Hosts own skill invocation and must authorize each later file read independently.

Native SDK hosts can load prompt template snapshots from explicitly granted `.md`
files or direct children of explicitly granted directories. No directories,
settings, or packages are discovered implicitly. Directory scans are bounded,
non-recursive and sorted, including hidden `.md` files and symlinks to files. The
snapshot retains source paths, frontmatter description and argument hint, and
Pi-style fallback descriptions. Reads follow symlinks, are bounded, and fail
atomically on unreadable, malformed, duplicate-name or missing files; Pi reports
warnings and can continue. Expansion and automatic command registration remain
the host's responsibility.

Skill frontmatter treats non-string names as absent and falls back to the
parent directory. Only the boolean `true` disables model invocation;
other types leave a skill with a valid description available. Non-string
descriptions are rejected with a diagnostic. Prompt templates use the same
type-aware frontmatter parsing.

Project configuration follows a stored trust decision, with an interactive
trust selector and explicit `--trust` / `--no-trust` overrides. Context files
remain readable independently of project trust. `/skills`, `/prompts`,
`/skill:name`, prompt argument expansion and `/reload` are available.
As in Pi v1.5.0, the project `.agents/skills` trust check scans ancestors to
the filesystem root, excluding the global home directory. Discovery stops at the
first `.git` boundary, so an undiscovered skill above it can still prompt trust.
Explicit skill invocation sends Pi's `<skill>` block with the source location,
relative-reference directory and body. Frontmatter is removed, and optional
user text follows the block.
The TUI displays a folded skill panel with the configured tool expansion key.
User text remains visible below the panel, including when a session is reopened.
In fullscreen mode, clicking a skill panel toggles that panel independently.
HTML exports also fold the skill body and keep user text and images outside
the fold. Skill content, conversation messages, and branch and compaction
summaries render as Markdown.
Raw HTML is escaped, and Markdown images leave their alt text without loading
external image sources.
HTML exports show aborted and failed model requests and preserve any partial
answer. Imported Pi assistant errors retain their original message text.
When a Pi compaction includes a resolved system prompt, imports restore that
snapshot before replaying later system updates and resolve its tool names
against the current native registry. Earlier imports recover the snapshot
from the preserved Pi entry when reopening the journal. RPC message snapshots
include the active compaction's saved system message before the summary and
retain later system updates in transcript order.
Native compaction entries projected to Pi RPC and Radius share JSONL include
their saved system message and tool declarations. Reimport preserves the
effective section order, including sections removed and later re-added.
Pi's JavaScript object enumeration can reorder integer-like section names;
their relative order is not guaranteed when the JSONL is opened in Pi.
HTML exports fold the initial recorded system prompt and subsequent updates,
including changed sections and available, added, or removed tools. Tool
parameters expand to show types, required flags, and descriptions.
User shell executions appear in HTML exports even when excluded from model
context. The first ten output lines stay visible, with the rest folded.
Prompt-template arguments preserve literal backslashes and quoted empty
values. Positional arguments, defaults and slices use Pi's placeholder
syntax and expand only once.
Each directory contributes its first readable context file in Pi order
(`AGENTS.override.md`, `AGENTS.md`, `AGENTS.MD`, `CLAUDE.md`, `CLAUDE.MD`).
Context files are loaded from outer directories toward the workspace, and a
UTF-8 BOM at the start of a file is removed.
In a nested linked Git worktree, the worktree root's context file shadows
the main repository's context file of the same name.

The local CLI's coding profile can read and write absolute paths, including
personal skill files outside the working directory. Embedded users of
`CodingTools::new` retain the existing workspace-scoped profile; the CLI selects
`CodingTools::local`. A regression test covers that distinction. The resource
PTY check covered granting project trust, advertising a discovered skill and
expanding a quoted prompt argument before the model request.

### Local MCP checkpoint

`pi mcp list --json` emits one JSON document with server state, source,
transport, tool names, per-tool exposure overrides, and resource capability.
Invalid entries are reported in `errors`; enabled connection failures cause a
nonzero exit. An invalid trusted project override currently fails the merged
configuration as a whole, preserving pi-rs's existing override validation.
Unlike Pi, JSON transport descriptions omit stdio arguments and URL paths,
credentials, queries and fragments; failure messages are generic so these
reports do not expose configured credentials through connection errors.
Wildcard exposure matches follow pi-rs's existing sorted-pattern priority.

`pi mcp login --timeout SECONDS` bounds OAuth discovery and sign-in; the
default is 300 seconds. Once credential persistence starts, it is allowed to
finish even if the deadline passes, so its outcome can be reported reliably.

`pi mcp logout NAME --local` requires a server defined in the trusted project
configuration and removes credentials for that server URL. A partial project
override retains the global server's scope and does not satisfy `--local`.
Credentials are keyed by server name and normalized URL, so a project server
with the same name and URL as a global server shares its credentials.

`pi mcp add`, `pi mcp remove`, `pi mcp list` and `/mcp` use user-level
`mcp.json` and trusted project overrides. The native client uses rmcp 3.9 for
stdio and streamable HTTP. It exposes namespaced server tools and resource
listing/reading. Configuration can reference environment variables or bounded
credential commands. Server processes are isolated into process groups/jobs,
and the CLI closes connections after each run.

Trusted project overrides of a global MCP server reject unknown fields and
invalid values before applying any part of that override, so a typo or
`enabled: null` cannot silently enable a server.
Pi ignores invalid overrides and reports a diagnostic; native fails the
configuration load. Native additionally permits `oauth` in an override for its
existing OAuth controls, alongside Pi's `enabled`, `exposure`, and
`toolExposure`. Explicit project nulls are rejected; inherited global optional
`enabled`, `exposure`, and OAuth nulls retain native's absent-value behavior, while
non-null settings are validated. Project `auth.provider` remains forbidden.

Resource and template listings omit MCP App `ui://` entries and entries whose
MIME profile starts with `mcp-app`, along with listing icons and `_meta`, matching
pinned Pi's prefix regex. This also hides non-App profiles such as
`mcp-app-extra`; the MCP Apps specification identifies `mcp-app` itself, so this
is a known Pi-fidelity limitation rather than exact profile matching. Native
resource tools remain namespaced per server and aggregate that server's pages.

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

OAuth HTTP calls that receive a typed 403 scope challenge can request consent
in the terminal or RPC UI. After consent, the browser requests the union of
the existing and required scopes with the saved client and callback. The
connection rebinds to the new credential generation and retries once with
the original operation ID. Declining keeps the existing credentials, and a
grant with insufficient scopes leaves the previous credential file in place.
Headless runs report the required scopes for an explicit login. Initial
discovery uses the scopes granted through `pi mcp login`.

Loopback RPC and PTY checks covered consent, decline, scope union, one retry,
and credential generation changes. The existing OAuth and live catalog checks
passed with the same implementation.

### Remote model read operations

Native hosts can inject asynchronous `ReadOperations` through
`CodingTools::local(...).with_read_operations(...)` or create an independent
model-facing tool with `create_read_tool`. Access, optional image detection,
and file bytes come from the backend. Filename variants are probed in order
only when access returns an `Error::Io` with `NotFound`; other failures stop
resolution. Model read pagination, image conversion,
output limits, and cancellation remain host-owned. Backends may provide a
seekable reader through `open_file` to avoid buffering the complete file; the
local operations adapter uses this path and checks image size before decoding.
Backends that provide only `read_file` still return a complete in-memory buffer.
Dropping an operation future must cancel backend work. Blocking reader methods
must themselves return in bounded time; cancellation stops scans between reads
and is checked between image processing phases. The factory uses unrestricted Pi
path semantics and the backend enforces its own access policy. Scoped
`CodingTools::new` rejects
custom operations because local canonicalization cannot establish remote
symlink boundaries. This injection does not affect attachments or other tools.

Injected read, write, and edit tools resolve `~` using an explicitly configured
absolute backend home, never the host home. Use `CodingTools::with_remote_home`
or `RemotePathOptions` with the standalone factories. If no backend home is
configured, home-relative paths fail before invoking backend operations. Local
tools retain their existing host-home behavior. Backends remain responsible for
access control and their own path namespace.


### Remote model write operations

Native hosts can inject async `WriteOperations` through
`CodingTools::local(...).with_write_operations(...)` or create a standalone
model-facing write tool with `create_write_tool`. The backend creates parent
directories and writes content; the host validates model arguments and keeps a
mutation lock until each in-flight backend operation settles, even when the
caller drops its future. Local filesystem adapters use the canonical path and
share the local edit lock; remote locks are scoped to the backend instance and
backend-supplied resource identity within this process. Backends must map all
aliases to the same identity to serialize them. Cancellation is observed
between operations.
The backend is responsible for its access policy and must not return before its
write has settled. Scoped profiles reject custom operations because local path
checks cannot verify remote symlinks. These local locks coordinate participating
pi-rs operations; they are not a security boundary. The resolved target is a
pathname, not a pinned directory or file handle. An external process can
replace the target or a parent component after resolution and redirect an
operation under the old lock. Existing unrestricted local tools have the same
class of pathname race. Defending against hostile concurrent filesystem changes
requires handle-relative traversal, no-follow checks and a handle-relative
replacement protocol across supported platforms. `LocalWriteOperations` is a
basic Pi-style filesystem adapter. It checks file type before and after opening
and uses nonblocking Unix opens to reject FIFO substitutions; the default local
tool retains its atomic-write behavior.

### Remote model edit operations

Hosts can inject `EditOperations` using `with_edit_operations` or construct an
unrestricted model-facing tool using `create_edit_tool`. The backend provides
access, file bytes and writes; matching, validation, diffs and cancellation
checks remain host-owned. Backend reads return the whole file in memory. An
backend compare-and-replace operation can atomically reject an external change.
Injected edits require this capability by default. Hosts can explicitly enable
`allow_non_atomic_fallback` for Pi-compatible reread-then-write behavior; that
fallback leaves a race with outside writers between the check and write. The
injected local edit adapter requires this opt-in: it uses atomic file replacement
and preserves permissions, but cannot provide atomic comparison. The default
built-in local edit retains its existing behavior.
Local adapters bind operations to the canonical target used for their shared
filesystem lock, so retargeting the original symlink while waiting cannot redirect
the mutation. Remote edits and writes share a lock only when both trait objects use the same
backend `Arc` allocation and resolve aliases to the same resource identity.
Distinct backend objects have separate locks. Locks remain held until in-flight
operations settle, including after a caller drops its future. The backend must
enforce its own access policy, and scoped profiles reject custom operations.

### Remote model shell operations

Native hosts can supply streaming `BashOperations` with
`CodingTools::local(...).with_bash_operations(...)` or create an independent
model-facing bash tool with `create_bash_tool`. The configured command prefix,
spawn hook, working directory, timeout and cancellation reach the backend.
Injected remote backends receive an empty environment by default; the spawn hook
can add selected values, or `ShellConfig::with_remote_environment(true)` opts
into host environment and session metadata inheritance. The built-in local adapter
continues to inherit the local environment. Model output, progress, truncation, timeout handling and
programmatic output remain host-owned. Backends enforce their own access policy
and must stop work when cancelled or dropped. Scoped `CodingTools::new` rejects
custom operations. User-shell extension operations remain separate.

`create_local_bash_operations` exposes the local execution backend for hosts
that wrap shell operations. Backend injection and factories for grep, find and ls are still incomplete.
The local model-facing bash paths report Pi's missing-working-directory
diagnostic before attempting to spawn a command.

When both operations are installed on one `CodingTools`, its read tool can page
the host-side full-output transcripts produced by its shell tool. Independent
factories can share a session-scoped `ShellOutputStore` via
`create_bash_tool_with_shell_outputs` and `create_read_tool_with_shell_outputs`.
Only paths produced by that store bypass the read backend; do not share a store
across trust boundaries. The transcript path is not a file on the remote machine.

### Image and file-tool checkpoint

Local write and edit operations support text files larger than 4 MiB. New
files follow the process umask and edits preserve existing permissions. File
mutations run off the async runtime and check cancellation while holding the
per-file lock, including immediately before committing the replacement.
Lock acquisition also observes cancellation while another writer is active.
The built-in local `write` uses atomic replacement. Unlike Pi's in-place write,
it can replace a read-only file when the parent directory permits replacement,
requires permission to create and replace entries in that directory, and leaves
other hard links pointing at the old file. A permission preflight alone cannot
bind the check to the file that a later pathname replacement will affect.
RPC user shell commands accept empty and whitespace-only scripts. User shell
output strips terminal escape sequences and control characters before streaming
and saving the transcript.

For text reads, Pi exposes the displayed page as both text content and a
`structuredContent` string. Native agent tool-result events receive this string
through operation-correlated transient metadata, including continuation and
truncation notices. The journal, SDK observers, result hooks, direct calls and
programmatic calls retain their existing `Outcome` variants, and the public
`Notification` variants are unchanged. In-process partial patches preserve
the structured value when they only update metadata, and discard it when they
explicitly replace output, including with the same text. Custom host hooks may
opt in to reporting output-field presence; native executable extensions negotiate
that capability and forward the field-presence result across the process boundary.
A duplex host remains available for nested calls even if it cannot send custom
messages. Single-shot and duplex presence responses allow only the fixed wrapper
overhead; the underlying outcome and nonterminal frames still obey their original
limits. Older executables and opaque overrides remain conservative when field
presence is unavailable. Image reads use the same transient event path to project
Pi's image structured result with its MIME type, encoded data and displayed note;
image omission results project the note as a string. Existing image and event
size limits remain in force; attachment-reference results are not synthesized.
Structured image outcomes accept metadata-only result patches without losing
content or the transient structured value; explicit text replacement discards
both. Changing the error status of a non-object structured payload remains
unsupported.

The read tool runs file I/O and image decoding off the async runtime.
Cancellation returns promptly and stops the text scan between buffers, even
while scanning a single long line. Directory listing runs off the async
runtime and checks for cancellation during enumeration and metadata reads.
It sorts lowercased filenames with Unicode collation, using the process
locale for language-specific ordering before applying the entry limit.
On Unix, an unrestricted directory listing denied by the operating system
reports Pi's `EACCES` scandir message with the requested absolute path.
Local and injected `ls` select the locale in Node's environment order:
`LC_ALL`, `LC_MESSAGES`, then `LANG`, including an explicitly empty value.

`read` returns bounded PNG image content for PNG, JPEG, GIF, WebP, BMP and TIFF
inputs. It preserves alpha, avoids upscaling and reduces large images before
serialization. The Ollama-compatible adapter forwards image tool results as
image content blocks in a following user message, keeping the original tool
call/result pairing. The TUI displays an image placeholder rather than base64.
Kitty and iTerm2 terminals display supported images inline.

`edit` accepts both batch and legacy single replacements, emits a unified diff,
and serializes cooperating file mutations across processes. The Windows coding
profile selects PowerShell. Windows CI exercises PowerShell output, timeout and descendant-process cleanup.

`grep` and `find` run off the async executor and observe cancellation during
directory traversal. `grep` also checks cancellation between file reads, even
when a file produces no matches. Local `grep` and `ls` report the resolved
absolute path when the target is missing, and `grep` clamps a zero match limit
to one as Pi does. Grep honors requested context beyond 1,000 lines and
still applies the 50 KiB output limit with truncation metadata. Unrestricted
local `grep` also accepts match limits above 100,000. Context searches spool
match positions and accounting records to temporary storage, keeping output
and match bookkeeping memory bounded while counting every selected result.
Temporary-storage failures are reported as I/O errors. Scoped tools retain
their 100,000-match ceiling. Unrestricted local grep accepts numeric limits,
clamps them to at least one and uses Pi's numeric comparison and notices.

### Model-selection checkpoint

Native SDK hosts can construct OpenAI-compatible chat, OpenAI Responses,
ChatGPT Responses, Anthropic Messages and Gemini clients from an explicitly supplied credential snapshot,
provider identity and endpoint, then register them in a host-owned model catalog.
The factory does not perform implicit credential or model discovery, OAuth
refresh, proxy discovery, model metadata probes, or network I/O at construction. Gemini
uses a base URL and appends model-specific streaming paths. Hosts own endpoint
policy and existing clients outlive replacement of the credential snapshot.
Unauthenticated chat clients require a separate explicit factory method. Typed
clients let hosts supply provider/model configuration; the factory does not
automatically infer that configuration.


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

Native SDK hosts can expand prompt template arguments using the same Pi-style
substitution implementation as the CLI, including quoted arguments, defaults,
slices, and non-recursive insertion. The host chooses when to apply expansion.

`SessionBuilder` and `AgentSession` own a model, registry and journal across
multiple prompts. `SessionControl` supports steering, follow-up input and abort
from another task. Subscribers receive bounded, owned events without blocking
the agent loop. The API also exposes extension commands, compaction, summary,
checkout and in-memory forks. Explicit asynchronous close releases shared
extensions after the last fork closes. Managed host-bound sessions continue to
use the existing host API.

For shutdown, `SessionControl::request_shutdown` permanently cancels current and
subsequently registered runs; the owner still awaits the operation and calls
`close` to finish cleanup. Ordinary `abort` remains limited to the active run.
This is a native embedding control rather than an upstream Pi API.

SDK tests cover follow-up consumption, abort during a model stream, shared
connection lifetime and retaining an independent fork history.
The Tauri mock fixture also accepts a host-built session, including a persistent
journal and host-supplied system prompt. Its close operation survives cancellation
of the requesting IPC call, shares its result with later callers, and releases its
session after completed shutdown. It does not provide resource discovery or live
session replacement.

Owned SDK sessions expose configured extension command names, collision-resolved
invocations, descriptions, and source provenance for host command pickers. Hosts
combine these with their own built-in commands, skills and prompt templates, and
explicitly route an invocation through `invoke_registered_command`; unlike Pi,
`prompt` does not dispatch slash commands. Source paths are lossy UTF-8 display
strings. The list does not reflect temporary per-request registry refreshes.

Owned sessions restore the registered model client as well as the model name
when reopening, navigating branches, or forking. Context-window queries and
idle input hooks use that branch's client too. Register clients for historical
model names with `SessionBuilder::branch_model` or
`AgentSession::register_branch_model`; an unavailable client is reported
instead of sending to a different one. An embedding host can opt into
`SessionBuilder::historical_model_resolver` for provider-qualified selections.
The synchronous resolver receives the historical provider and model, resolves
the effective endpoint and credentials from trusted host configuration, and
returns an exact client, a different provider/model client, or unavailable.
For a fallback the SDK commits the chosen identity, clears the thinking level,
and exposes both identities through `AgentSession::model_fallback`. The SDK
rejects a fallback before committing if that provider/model already has a
different client; an existing matching client must be the same `Arc`. A host
revocation during the write prevents the entire staged client set from being
installed, and conflicting in-flight registrations are rejected before the
journal write.
The durable selection can then exist without a client and requires a new
authorized registration before dispatch. The SDK cannot verify an opaque
client's endpoint; the host must enforce endpoint-bound
credentials. Unqualified legacy selections never invoke the resolver, and
existing exact registrations take precedence. The resolver runs on session
construction and before checkout, tree navigation and forks; forks retain the
resolver for later navigation. It resolves before transition hooks and file
publication. Fallback checkout and tree navigation persist the destination and
replacement selection in one record, including for a branch summary. The
fallback clears thinking and a competing registration cannot be overwritten.
If a host revokes clients during a successful transition write, the SDK emits
the committed record and reports the successful transition, while subsequent
dispatch fails closed until an authorized client is registered. Persistent
forks resolve pending handoffs from the complete source journal before selecting
their branch and validate their complete snapshot before
publishing and retain the locked published file handle.
Failures before publication remove the temporary snapshot and do not replace a
destination. As with existing journal export, a directory-sync failure after
the atomic publish can leave the complete destination in place; callers should
inspect it before retrying.
Construction fallback commits a single native selection record even after an
imported Pi source, using a distinct timestamp so older projections retain the
change. Older journals remain readable; journals containing the new transition
records use journal version 4, which version 3 readers reject rather than
silently ignoring the fallback selection.
Hosts can also explicitly call
`set_model` or `set_model_with_hooks` to select another client. The hook-aware form installs the replacement after
recovery and before startup hooks and any turns they trigger.
If a startup-triggered turn fails after that replacement, the model-selection
notifications are still delivered before the failure is returned.
Checkout, tree navigation, and persistent fork validate the destination model
before tree hooks, changing the journal, or creating a file. Hosts can identify
the initial client with `SessionBuilder::provider`, register exact historical
identities with `provider_branch_model` or `register_provider_branch_model`,
and select them with `set_provider_model`. A provider-qualified selection uses
only its exact registration, even if a name-only client exists. Older selections
without a provider use only name-only registrations, so hosts must explicitly
resolve ambiguous legacy identities. The SDK does not discover credentials or
endpoints; the host constructs and registers clients under its own policy.
`set_model_with_hooks` uses the descriptor's provider when present, including
when replacing an unavailable startup model. Failed journal writes or a busy
journal do not replace the previously registered client.

Embedded hosts can use `coding_factories::create_coding_tools` and
`create_read_only_tools` to register Pi's ordered coding or read-only tool sets
without registering every built-in tool. The factories use the unrestricted
local profile. The corresponding `coding_tools_from` and `read_only_tools_from`
functions select from a host-configured `CodingTools`, preserving its file-tool
scope and options. A local coding set's shell runs with the host process's
permissions and is not sandboxed, including with a scoped file profile; hosts must
apply their own shell policy. For injected file operations, `injected_file_tools_from` yields
read, edit and write without a shell. It requires all three operations to use
the same backend `Arc`, which also lets injected edit and write share their
mutation lock. Hosts must ensure the backend enforces the intended access policy.
`coding_tools_from` accepts an injected shell only when read, write, edit and
non-local bash use the same backend `Arc`; hosts must ensure that all four
operations actually target the same environment and enforce its access policy.
For a working directory that exists only on the backend,
`create_remote_coding_tools` builds the set without looking up or canonicalizing
the directory on the host. Its path options configure the backend home for `~`;
without a configured home, home-relative file paths fail. Its read and shell
tools share a host-side transcript capability, as in the standalone factories.
`coding_tools_from_with_local_shell` explicitly adds a host-local, unsandboxed
shell to injected file tools and rejects a separately injected shell. Partial or
independently allocated injected backends are rejected. The read-only set rejects
custom read operations because grep, find and ls still access the local filesystem.
To use all four tools on one remote workspace, `create_remote_read_only_tools`
accepts one `Arc` whose backend implements read, grep, find, and ls operations.
Its working directory and optional backend home are absolute paths in that
backend's namespace. Local tools and injected `find`, `grep`, and `ls` share
file URL validation: malformed UTF-8, percent escapes and encoded path separators
are rejected before backend calls; query and fragment escapes are not path
escapes. On Unix encoded backslashes remain literal filename characters. On
Windows valid IDN UNC hostnames are decoded to Unicode before backend dispatch,
matching Node's `fileURLToPath` for valid hosts. For malformed punycode, Node
versions differ: Node 24 retains `xn--bad-` literally and rejects
`xn--bad-.☃`, while Node 22 decodes those hosts to `bad` and `bad.☃`.
Native follows the Node 24 behavior for these cases, including percent-encoded
ASCII or UTF-8 host input, and remains stricter than Node 22 for the mixed
non-ASCII case. Valid Unicode host normalization still applies. Backends enforce access controls and search semantics,
including matching, ignore rules, and ordering. A set is an `Extension` and can
be registered directly with `Registry`; overlapping sets have duplicate tool names.

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
Local package theme manifests are read again when resources are resolved, so
packages indexed by older versions gain their declared themes without a build.
An explicit Pi `package.json` resource list contributes themes only and prevents
previously cached native resources from being loaded. Unreadable local manifests
are skipped instead of falling back to cached resources.
When an in-progress package command is cancelled, the host requests termination
of its process group (or Windows job) while the direct child is still owned. The
host retains the child identity until inherited output pipes close, including when
the parent has already exited. This does not impose a deadline on all share
context preparation or cover processes that detach or close their inherited pipes.

### System prompt checkpoint

The native default uses Pi's coding-assistant preamble and separate tool,
rule, project-context, skills and working-directory sections. The prompt names
only registered tools. Skill discovery is exposed to the model when a usable
file-reading tool is registered; manual skill invocation remains available.
Explicit `--system-prompt` content takes precedence over project SYSTEM.md.
Skill metadata is XML-escaped and relative-path guidance matches native skill
loading. Prompt templates accept Pi string `argument-hint` frontmatter; native multi-match
completion displays the hint before the description and inserts only the command
name. Built-in and skill candidates take precedence over same-named templates. Completion
descriptions use Pi's single-line normalization, selected and muted theme roles,
and narrow-width visibility. Native completion still opens on explicit Tab and
inserts a sole match directly, rather than reproducing Pi's continuously displayed
autocomplete.
Prompt template parsing strips one leading BOM and applies ECMAScript trim to
the body after frontmatter. When no explicit description is present, the first
nonblank line uses ECMAScript whitespace rules and the first 60 UTF-16 code
units, as in Pi.
If the cutoff splits a surrogate pair, RPC `get_commands` preserves the unpaired
UTF-16 unit as a `\ud83d`-style JSON escape, as Pi does. Native display uses
U+FFFD for the unpaired unit because its strings contain valid UTF-8; actual
U+FFFD characters remain distinct. Prompt bodies and explicit descriptions are
not truncated.
Declared `SKILL.md` files with missing descriptions report both the
description problem and any invalid name diagnostics; unreadable skill files
also report their source path. Empty-description checks use ECMAScript whitespace
semantics, including U+FEFF and excluding U+0085. Tests cover these diagnostics,
reader gating and custom-prompt precedence.

Skill and prompt-template files decode malformed UTF-8 with replacement, as Pi
does, so an invalid byte does not hide an otherwise usable resource. The native
64 KiB resource limit and existing resource trust rules remain in force; source
files are not rewritten. Tests cover discovery, invocation and the size limit.
Skill invocation uses the same replacement decoding as discovery. Skill and
prompt frontmatter each strip one leading BOM and use ECMAScript whitespace
when trimming a delimited body, matching Pi.
Skill discovery keeps the selected symlink path for the skill name, displayed
location and relative references. Canonical paths are still used to deduplicate
aliases to the same file.

Custom theme files are now watched while the terminal is running. Valid edits
redraw the active view. Invalid edits retain the last valid colors and show one
diagnostic per changed file contents. A PTY check covers live updates and
terminal restoration. System appearance discovery queries terminal foreground and background colors
using OSC 10 and 11 and updates light/dark theme selection as replies arrive.
System theme accent and syntax roles use Pi's fallback ANSI palette slots.
Neutral fallback roles use terminal-default foreground with Pi's dim
attribute. The terminal palette is queried with OSC 4 and applied after all 16 colors
arrive. Requeries retain the last complete palette until a complete replacement
arrives, including after an appearance notification or incomplete reply. Color
requests are serialized and coalesced until the preceding device-attributes
(DA1) marker arrives, including when all color targets have answered. If a
refresh is waiting and DA1 does not arrive within two seconds, stale replies are
quarantined and the refresh stays queued until the old marker arrives. This
keeps stale responses out of a later query but means a terminal that never sends
DA1 cannot refresh colors during that session. After an external editor or
suspend hands terminal ownership away, further color queries are disabled for
the session: another owner may consume the old DA1 or leave an unrelated one.
If a later appearance report changes, stale colors are cleared and the reported
light/dark mode is used with the ANSI fallback. A repeated report does not discard
colors: a terminal's colors can intentionally differ from the OS appearance.
The first appearance report received during the initial color query is treated as
the startup status report, not a color change; the protocol
does not distinguish this report from a simultaneous notification.
These restrictions differ from Pi's behavior. The boundary assumes the terminal sends each query's color responses before its
DA1; arbitrary reordering or unrelated DA1 replies cannot be disambiguated,
because these protocols contain no request identifiers.
On an appearance notification, the last reported foreground, background, and
complete palette remain in effect until replacement replies arrive, as in Pi.
The reported appearance is used when no background is known.
When the background is unknown, the system theme retains ANSI-indexed colors
even if the palette is available, matching Pi's fallback. With a known background,
the system theme generates Pi's OKHSL color families and contrast-solved roles,
using a complete terminal palette when available. At interactive startup the
system theme stays grayscale until all color targets reply, the color query's
device-attributes marker arrives, or its 100 ms initial timeout, then uses the
available colors. The startup session selector does not issue color or appearance
queries: it can hand off directly to RPC, print mode, or the shell, where delayed
responses would otherwise leak as input. It uses the available fallback theme.
In interactive mode, foreground, background and complete palettes are collected
per query. A 100 ms timeout applies the partial snapshot; a later DA1 or the
final color response applies the cumulative snapshot once, matching Pi's color
aggregation while retaining the serialized DA1 safety policy described above.
Custom
OKHSL and OKLCH colors accept Pi's case-insensitive syntax and hue units,
validate component ranges, and gamut-map OKLCH colors.

Markdown tables now calculate Unicode-aware column widths, preserve alignment,
wrap cells, and use a stacked layout when a terminal cannot fit the columns.
Link destinations and strikethrough formatting are rendered. Single-tilde
pairs remain literal. Inline runs containing double tildes use the pinned Pi
Marked lexer and its strict deletion tokenizer, including escaped delimiters,
code-span crossings, list continuation indentation, table cell boundaries, and
inline or reference link destinations. The lexer is
resource bounded across the entire terminal frame, including reference-definition
preparation; oversized documents, runs or reference sets fall back without
strikethrough.
Automatically detected links in lexer output retain native recipient and
ambiguous-address guards; explicit links retain their chosen destinations.
Table tests cover Japanese text and narrow terminal widths.

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
both one-shot CLI and persistent RPC. Explicit append arguments replace
discovered `APPEND_SYSTEM.md`; an explicit base prompt likewise takes
precedence over `SYSTEM.md`. UTF-8 BOMs are removed from prompt files.
Native `-e` and `-n` aliases select
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

### Codemode OpenAI Decisions checkpoint

Codemode supports OpenAI Decisions classifier models, including the pinned
GPT-6 Luna catalog, image input, named answer conversion, usage pricing tiers,
and the Decisions-specific 504 retry exclusion. Native image validation and
endpoint-bound credentials continue to apply. Classifier usage identifies its
calculated costs as estimates with `totalFromProvider: false`.
An OpenAI key explicitly saved for `/v1/responses`, including by the provider
key UI, does not authorize `/v1/decisions`; use an endpointless Pi auth entry or
`OPENAI_API_KEY` for the official Decisions endpoint.

Codemode reserves its four concurrent model-call slots for classifier and image
inference; model discovery and resolution remain available while those slots are full.

### Native codemode checkpoint

Model catalogue search checks provider and ID argument types before dispatch,
including the sandbox's JSON conversion of omitted and non-finite values.

The native `codemode` tool accepts `{ "code": "..." }` and runs JavaScript in a
fresh QuickJS context. Scripts can call registered tools through `tools`, use
`searchTools`, `describeTool` and `describeNamespace`, emit `text` or `image`,
and retain JSON values with `store` / `load`. Store changes commit with a
successful tool result and follow journal branches. Failed scripts retain
partial output without committing their store. Filesystem, network and Node
APIs are absent from the script context. Unfiltered model discovery does not
read `auth.json`; availability checks and model calls still load credentials
and report authentication-file errors. Memory, stack and output have explicit
bounds. Execution has no default deadline; a first-line `timeout_ms`, an
explicit CLI/SDK tool timeout, or cancellation can stop it, including while
waiting for a worker. Like Pi, worker admission waits at most 60 seconds even
when execution has no deadline; a shorter script or host deadline and
cancellation stop the wait sooner. `PI_CODEMODE_MAX_WORKERS` is re-read when
admitting or releasing workers, allowing embedders to change the limit while
running. Positive integer values accept Pi's numeric forms, including whitespace,
decimal notation and radix prefixes. Reducing the limit does not interrupt
existing workers; native retains its 1,024-worker ceiling and uses the default
of two for invalid or larger values. Waiting counts toward the script deadline,
and a cancelled waiter does not release another script's worker. In the Rust
SDK, `Codemode.timeout` remains a `Duration`;
its default `Duration::MAX` means no deadline, while other values set a limit.
`Limits::default().tool_timeout` remains 30 seconds; passing `Limits` explicitly
to `SessionBuilder::limits` enforces that timeout for Codemode too. Direct
`Agent` users retain their configured timeout unless they explicitly opt out
through the registry. Nested tools retain their own limits. Large text output
is saved to a local file.

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
first-line `@options` parser accepts indented directives and zero output tokens,
rejects unsupported or invalid fields and misplaced directives, and preserves
script line numbers. The native JSON code argument differs from Pi's
provider-specific raw-code input.
Non-chat model APIs and remaining execution-limit differences remain open.

Classifier results include their API and invocation timestamp even if the
request fails early or is cancelled, as they do on successful requests.

Classifier retry delays use Pi's numeric-prefix parsing for `Retry-After` and
`Retry-After-Ms`, including decimal and exponent forms; the existing 60-second
server-delay cap still applies to the parsed value. GMT retry dates also accept
the obsolete RFC 850 form with JavaScript's two-digit year interpretation.

### Resource selection checkpoint

`--no-skills`, `--no-prompt-templates`, `--no-context-files`, `--no-themes`,
`--no-extensions` and `--no-mcp` control automatic resource loading. Explicit
`--skill`, `--prompt-template`, `--theme` and `--extension` entries remain
available. Skills accept file or directory paths without `--project`; the
existing named project-skill mode remains supported. Native built-ins can be
selected with `builtin:codemode`, `builtin:tool-search` and `builtin:mcp`.
Repeated project-approval flags follow Pi's last-option-wins behavior, including
the `-a` and `-na` aliases; flags after `--` remain prompt text.
For root help and version requests, Pi-compatible argument diagnostics are
reported before metadata; version takes precedence over help, and export takes
precedence over help. Metadata scanning follows Pi's value ownership for its
options, including values that begin with a dash. Native subcommands retain their
own Clap parsing and help, while native-only attached values and short-option
clusters continue to be accepted. For root `--export`, the last export input and first positional message select
the input and HTML output, including when help is present; native option
validation runs before writing. As a deliberate safety difference from Pi, an
export destination cannot overwrite any explicitly supplied export input, even
one replaced by a later option; aliases and symlinks are checked as well. A
missing or empty final export input is rejected
rather than falling back to an earlier input. Other invocations without root metadata keep
their existing native argument parsing.
These selections propagate to persistent CLI workers. The settings editor,
remaining CLI aliases still require parity work.

### Legacy settings migration

User and trusted-project settings independently migrate Pi's legacy
`queueMode` to `steeringMode` and the legacy `skills` object to skill paths
and `enableSkillCommands` before merging. Explicit modern keys win within
their layer; project settings retain their usual precedence and untrusted
project settings are ignored. Configured skill paths remain relative to their
own settings file. Loading does not rewrite the original files. Regression
tests exercise actual skill discovery, queue selection, precedence and trust.

### Upstream session import checkpoint

The import command reads native journals and Pi v1–v3 JSONL snapshots without
modifying the source. It retains original entries alongside native events,
including branch topology, completed tool results, reasoning, usage, names,
labels, compaction boundaries and branch-local codemode store values. User and tool-result images reach the model
adapter as image content. Previously completed tools remain recorded results;
unfinished imported calls remain uncertain and require reconciliation. When a later assistant message reuses an unfinished call ID, ambiguous results
are retained as separate, labeled historical content and neither operation is
resolved automatically. This intentionally differs from Pi’s ID-only result
pairing because it cannot establish which operation produced the result.
A source entry with a missing parent is recovered as a separate native branch;
its original parent ID remains in the retained source entry, while RPC presents
it as a root. Missing ancestors may contain unrecoverable state or tool calls;
orphan tool results and compactions involving orphaned branches are still rejected.
Forward parent references and compaction boundaries on a different branch are
also rejected. Pi instead drops a retained boundary that is not on the current
path; native import rejects it rather than silently retaining unrelated history.

Context edits are projected onto future model context while the imported raw
entries remain available. Remaining specialized message variants and system
prompt checkpoints still need parity work. Images currently appear as
attachment notices in the terminal.

### Model scope checkpoint

`--list-models [search]` lists models from available providers, with fuzzy
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
projection when imported, including text and image replacements. Assistant
replacement thinking retains its signature and redaction flag and is replayed
in block order to the original provider, API and model after tool-pair repair.
Provider replay stays private to the adapter; native and executable request
hooks and request notifications receive portable copies. Host-tracked hook
wrappers invalidate replay whenever a hook obtains general mutable access to
assistant messages, irrespective of the hook's reported status or the resulting
values. Narrow tool and system-content accessors, plus tracked insertion and
removal of user messages, retain replay for untouched assistant messages.
Tracked insertions are rejected if they would split an assistant tool call from
its results, including parallel results. Executables can
negotiate the versioned context protocol and return a payload-free unchanged
response; the host retains its original request in that case. Executables set
`context_provenance_v1` for payload-free unchanged responses. Executables also
advertising `context_safe_patch_v1` receive `context_v3`, `before_request_v3`
and `context_with_system_v3` and can return `{"change":"safe_patch","patch":...}`.
The v2 protocol supports unchanged or full replacement responses; full
replacements use `{"change":"changed","value":...}`. A safe patch can replace tools or the
leading system text and apply indexed user-message insertions or removals. The
host validates it on a detached copy before applying tracked operations; it
cannot replace or remove an assistant. An unchanged response cannot carry a
value. Full replacements and legacy protocol responses are treated as changed.
Native extension hook signatures use the tracked wrappers.
Edited Anthropic, Google and OpenAI Responses thinking requires the recorded
endpoint binding; chat-completions replay is also bound to the configured
provider. Older and imported histories without that binding omit edited thinking
rather than send it to an unverified destination. Legacy unedited chat-completions
replay without a provider binding stops with an explanatory error when its
endpoint and model match the request; editing or removing the affected assistant
context or starting a new session clears the ambiguity. Google text and tool-call signatures remain attached to their blocks.

### Multimodal input checkpoint

The CLI accepts `@file` arguments for text and supported images, using the same
image decoding and size limits as the local read tool. File text precedes the
prompt, empty files are skipped, UTF-8 BOMs are stripped from text files, and
malformed UTF-8 in text attachments is decoded with replacement characters.
Attachment file tags contain the literal resolved filename, including `&`,
quotes and angle brackets, matching Pi's model-visible prompt text. Native
canonicalizes paths for local reads, so a symlink or Windows canonical path
can still differ from Pi's lexically resolved filename.
For CLI attachments, image detection does not infer a format from the file
extension. Malformed PNG headers and animated PNGs detected within the first
4,100 bytes are treated as text. JPEG, GIF, WebP and BMP content detection
also follows Pi; unsupported image formats are treated as text.
Images are referenced by a single-line file tag and sent as separate image
content. The TUI, print and JSON modes send separate positional messages as
successive turns in the same session. Attachments belong to the first turn.
The TUI remains available for input after the startup turns. Text output
prints the final answer, and JSON emits each turn. Piped stdin is decoded as
UTF-8 with replacement, trimmed, and concatenated before the attachment text
and initial prompt, without an added separator.

Print and JSON modes also accept an empty invocation. Text mode emits the
last assistant message's text blocks if it is the final message of the
selected session. JSON mode emits the session header without starting a turn
and takes precedence over `--print`.

When the final text-mode answer reaches the provider's output limit, the CLI
prints the partial answer and exits with status 1. An earlier truncated turn
does not determine the exit status of a later successful answer. JSON mode
reports the stop reason in its message events.

The owned SDK exposes `prompt_with_images`. Idle RPC `prompt` requests accept
image blocks, including image-only input. Image-bearing input is committed as
one journal entry, restored across sessions and preserved in forks. Invalid
image encodings and oversized input are rejected before that entry is written.
CLI, RPC and a real terminal
fixture verify the new input paths.

RPC user `message_start` and `message_end` events and reconstructed native user
messages use Pi's ordered content blocks: a text block first, including for
image-only input, followed by image blocks. Unedited imported Pi messages retain
their original representation; native model-facing messages are unchanged.

### Multimodal tool event checkpoint

Live JSON/RPC tool completion events and reconstructed RPC tool messages
preserve the order of native text and image content blocks. Details remain
separate; extra native structured fields and attachment references are retained
as `nativeData` and `nativeAttachments`; additional legacy envelope fields are
preserved as `nativeEnvelope`. `structuredContent` is also exposed directly
on the projected result even when content cannot be projected. Image blocks
retain their MIME types in event content; the separate provider-facing image
guard still applies. Malformed, unknown or oversized blocks are kept with their
original indexes in `nativeRejectedBlocks`, without moving
supported blocks out of order. Plain text that happens to look like JSON and
results with no recognized blocks retain their previous representation.
The native provider-facing tool envelope is unchanged. Focused tests cover
interleaved images, metadata, plain text and imported tool results.

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

Interactive `/export <path>.jsonl` writes the complete native journal, including
messages and other records on branches that are not currently selected. It is
not an upstream Pi branch-only JSONL export; review the entire file before
sharing it. Selected-branch content can itself include sensitive information.
Core and SDK JSONL exports also publish complete native journals.

`--export <input> [output]` exports native or imported Pi sessions to HTML and
exits. The default destination is `pi-session-<input-basename>.html` in the
invocation directory. An explicit destination can use any extension and replace
an existing output file. The source session is protected from replacement.
HTML exports embed validated user and tool images as data URLs under an
image-only data policy.

`--api-key` overrides the model credential for the current process, including
model discovery and persistent CLI/RPC workers. The credential remains
process-local. Regression tests verify the selected bearer header and check
that session records and command output contain no key value.

### Repeated CLI modes and session references

Repeated `--mode`, `--tui-mode`, `--session`, `--session-id` and `--fork`
options use their last value, as in Pi v1.5.0. Invalid mode values are still
rejected even when a later value is valid. Native session-option conflicts and
session ID validation remain in force, and arguments after `--` remain prompt
text. Parser regressions cover these cases; a CLI fixture verifies that only
the last named session is created.

Repeated `--tools` and `-t` selections use the last value. Each selection is
checked for mixed names and modifiers or wildcard modifiers, including an
invalid selection followed by a valid one.

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
Empty terminal-identification environment variables do not enable image
protocols or imply an active tmux session, matching Pi's detection behavior.
`PI_IMAGE_PROTOCOL` is case-insensitive, and `TERM=xterm-kitty` alone does not
override another terminal's image protocol.

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
returns after both pipes close or 100 ms without further output. Output from
an active descendant resets that idle period. Unlike Pi, pi-rs also caps the
post-exit drain at two seconds to bound continuously writing descendants. Unit and CLI fixtures cover line/byte
limits, file recovery, Unicode boundaries, timeout output and inherited pipes.

### Text paging checkpoint

Text reads now scan files with bounded memory, including files larger than
4 MiB. Pages retain complete lines and UTF-8 boundaries, report the next
`offset`, reject offsets beyond the file, and explain when a single line
exceeds the byte limit. Explicit line limits report remaining lines. Image
normalization and the separate write-size limit retain their existing bounds.

### Image read omission checkpoint

Image reads that cannot be decoded, converted or resized return Pi's text
notice explaining why the image was omitted, including for injected read
operations. Programmatic callers receive that text rather than an image.
Unsupported inline formats such as BMP are converted to PNG even with image
resizing disabled; conversion and resize failures have distinct notices.
Conversion preserves image orientation; resizing operates on decoded pixels
without applying the source byte limit to an intermediate encoding. Supported
inline formats pass through without decoding when resizing is disabled.
The native 20 MiB source limit, cancellation and filesystem errors remain
separate failures. When an injected backend only implements `read_file`, its
whole response is buffered before the 20 MiB check; bounded transfer requires
the optional streaming `open_file` operation. The 6 MiB limit when resizing
is disabled still applies to the source and converted output.

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
and creates an independent session in the current workspace. For histories with
only inert messages and metadata, all branches are retained as in Pi. Histories
with executable operations, queued work or other unverified records retain the
existing selected-branch copy, including its prior handling of pending queues
and recovery. Source reads stay read-only and existing destinations remain protected.

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
and byte limits. Large text files remain searchable. Overlapping context ranges
are repeated for each match. Search memory is bounded at 64 MiB, and the configurable context
radius is limited to 1,000 lines.

`find` uses a 1,000-result default and reports result limits. `ls` sorts names
case-insensitively, identifies linked directories, reports entry limits and
labels empty directories. Head truncation preserves complete lines where they
fit. Fixtures cover large-file search, Unicode and literal patterns, context,
empty directory output and continuation notices.

Single-file `grep` preserves the requested file name in its output when the
path is a symbolic link, while workspace boundary checks still use the resolved
target. For explicitly named binary files, NUL bytes are treated as line
breaks while searching, matching Pi's ripgrep invocation; directory traversal
continues to skip binary files. Context still comes from the original file, as
in Pi. Focused checks compare both linked and ordinary files, including context
lines.

Unrestricted `grep` applies path-containing glob overrides to the absolute
search paths passed to ripgrep by Pi. Basename globs, negated globs, and
explicit file arguments retain ripgrep's matching behavior. Directory results
use Pi's slash-normalized relative paths and its basename fallback for paths
starting with `..`; a single-file result retains the requested basename.
The scoped native profile intentionally keeps its workspace-relative glob
interpretation. On Unix, scoped `grep` preserves literal backslashes in file
names so its results can be passed to `read`; unrestricted Pi-compatible grep
normalizes them as Pi does. Both profiles normalize Windows path separators.
Offline fixtures compare these cases with Pi's pinned ripgrep invocation and
exercise `grep`-to-`read` for a Unix filename containing a backslash.

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
Code blocks are separated from following blocks; syntax tokens use the active
theme's syntax roles, and unknown languages use its plain code-block color.
Paragraphs are separated from following code blocks as well. TypeScript and
TSX fences use Syntect's JavaScript grammar as a fallback, so shared syntax is
highlighted but TypeScript-specific tokens can differ from Pi's highlighter.
Empty fenced blocks retain one indented content row.
Wrapped list content keeps its indentation. Quotes render borders and quote
styling on every continuation line, including nested quotes and narrow terminals.
User messages also render Markdown, retaining their original ordered and
unordered list markers and escaped punctuation. Task-list markers and wrapped
continuations use Pi's bracketed form and indentation.
Recursive quote rendering stops at 32 levels and preserves deeper content as text.
Horizontal rules use the reference 80-column maximum.
Top-level horizontal rules separate following blocks with one blank line and
do not add a trailing blank line at the end of the rendered message.
Consecutive paragraphs retain a single separating blank line, including inside
block quotes; hard line breaks do not add paragraph spacing.
Top-level tables are separated from a preceding rendered block by a blank line.
Inline and display LaTeX use a native port of the pinned Pi terminal renderer,
including fractions, operators, scripts and matrix environments. Math can occur
in list items and quotes; unsupported or incomplete expressions retain their
source. Oversized or deeply nested expressions and exhausted rendering budgets
fall back to source text. These limits are intentional native safeguards and
may preserve an expression that Pi would render. A pinned reference fixture
checks 3,474 expression renderings; Markdown tests cover delimiters, code,
links, list indentation, streaming and narrow display widths.

Unit tests cover these layouts and Unicode widths. A PTY fixture verifies both
terminal modes, heading decoration, code fences, nested quotes and clean exit.

### Edit diff presentation checkpoint

Edit tool panels retain unified hunk information, show original and updated line
numbers, and highlight changed words for one-line replacements. Added, removed
and context lines use their dedicated theme colors. Tabs expand to three spaces,
and word comparison follows Pi's `diffWords` token grouping and whitespace
deduplication, including the first changed fragment's indentation treatment.
Numbered diff gutters use JavaScript's whitespace and line-terminator rules,
including its treatment of BOM and U+0085.
Comparison has a shared 100 ms budget per render; oversized or unfinished
comparisons retain line-level highlighting. The Rust word-diff implementation
derives from jsdiff 8.0.4; its BSD license is in
`crates/workbench/src/ui/diff/JSDIFF-LICENSE.txt`.

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
its original characters and whitespace. Fuzzy trailing-space matching follows
ECMAScript `trimEnd`: U+FEFF is trimmed, while U+0085 is retained. A missing
U+0085 in an edit target therefore cannot cause a fuzzy match and file rewrite.
Local and injected edit regressions verify that this mismatch leaves the file
or backend untouched.

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
Widgets appear in registration order within each position; replacing or moving
a widget places it last at its destination, as in Pi.
String-array widgets show at most ten input lines and a truncation notice, as in
Pi. Native widgets retain the existing 64-widget and 128-line request limits;
their text styling and fixed fullscreen placement remain to be matched.
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

Like Pi, user and trusted-project `settings.json` files may start with a UTF-8
BOM when loaded or updated. Malformed or unreadable user and trusted-project
settings are reported and the other scope remains available. Settings updates
do not overwrite malformed files. Invalid implemented setting values remain
errors; updates preserve unknown keys. On reload, a failed scope retains its
previous in-memory values while valid changes in the other scope take effect.
When a worker restarts, it reads valid external settings changes as before;
the frontend keeps its own snapshot until it reloads or changes sessions.

`defaultTools` follows Pi's layered selection: project modifiers extend the
user selection, while project plain-name lists replace it. Mixed plain names
and ordered modifiers in settings resolve before CLI tool selection.

The interactive `/debug` command writes `pi-debug.log` in the agent directory,
including styled transcript and widget lines, terminal dimensions, and the
current Pi-format session messages. It is available while a response is
streaming, and the slash-command completion includes it.

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

A native package declaration with `autoload: false` writes only paths matched
by its ordered patterns. A project delta is evaluated before each matching user
declaration, and its explicit enabled or disabled paths take precedence.
Unmatched paths can be supplied by the user declarations. The delta resolves
against the first usable cached user source, falling back to the first declared
source when none is cached. This native fallback can apply a project delta to a
later revision after an earlier revision fails to install.
An ordinary project declaration replaces the matching user selections. Native Git and
Cargo caches remain per source revision; for layered declarations, matching
relative resource paths share first-writer precedence across those caches.
Resources present only in another cached revision can therefore remain visible.

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
Windows uses Ctrl+Z for undo; WSL uses Alt+Z so Ctrl+Z remains suspend.
Both use Ctrl+F for fullscreen transcript search and their prompt-jump
defaults omit the redundant Ctrl+Shift variants, matching Pi.
Transcript search normalizes ECMAScript whitespace in queries and rendered
graphemes, including U+FEFF. U+0085 is retained in search-only input and
rendered-text metadata, so it remains distinct from both whitespace and an
absent character. Its zero-width matches can be navigated, although they have
no visible cell to highlight. Terminal output still removes U+0085 and other
unsafe control characters.

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
The active branch is listed first at each fork and marked along its path.
Branch connectors and gutters are rebuilt from the visible tree after filtering,
so descendants attach to their nearest visible ancestor. Deep trees pan
horizontally to keep the selected entry visible while retaining the selection
gutter; clipping skips partially visible wide characters at either boundary
while preserving theme styling.
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

Package updates reconcile both user and trusted-project declarations even when
the project shadows a different revision of the same source. Exact duplicate
sources share a cache and are updated once; transient command-line packages are
refreshed through startup rather than persisted update.

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
sources. Pi treats the first `@` in a Git repository path as a ref separator.
For compatibility with existing native declarations, pi-rs retains its prior
last-`@` interpretation when several occur and warns during installation. Use
the native `#ref` separator to disambiguate, for example
`git:https://host/team/repo#feature@v2` or
`git:https://host/org@team/repo#stable`. Both forms preserve repository identity
and resource filters. URL userinfo is not treated as a ref, and credential-helper
requirements remain unchanged. Package diagnostics and list output redact URL
userinfo, including before a configured source is validated; saved declarations
and internal identity matching are not rewritten.

An explicit native package resource manifest limits loading to its declared
resource types, including when a declared array is empty. A manifest containing
only metadata retains automatic directory and native executable discovery.
Cached local, Git and Cargo package resources are filtered against the current
cached-root manifest before loading. A present `pi` value must be an object, and
every resource field must be a string array. Local and Git manifests are checked
before their Cargo build. A Cargo manifest without Pi metadata falls back to
`package.json` resource declarations; competing present manifests are validated.
Registry packages are different: `cargo install` builds
the crate (including build scripts) before pi-rs obtains and validates its source
manifest. Installing a registry package therefore requires trusting its build;
validating before that build needs a separate source acquisition and resolution
design that preserves Cargo's selected version, registry and integrity checks.

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
checks with an isolated browser launcher. Interactive scope upgrades for typed
403 challenges are described in the MCP checkpoint above.

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
server. The MCP checkpoint above describes interactive upgrades for typed 403
scope challenges.

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


Recognized OSC 10/11 color replies and OSC 4 replies with a complete palette
header remain buffered across input timeouts, matching Pi's slow-terminal
behavior. BEL or ST terminates the response; CAN, SUB or a new escape sequence
abandons an incomplete response. Ambiguous Alt+] prefixes recover as keyboard
input after 50 ms without a new fragment, matching Pi's sequence timeout. For bounded memory, pi-rs discards an OSC color payload over
256 bytes until its terminator or cancellation, an intentional native limit.
An unterminated recognized reply can absorb ordinary text until it ends or is
cancelled; timeout alone cannot distinguish a delayed reply from that text.

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

Native request budgeting and configured retention retain the original estimator;
Pi-compatible session reporting uses a separate projection-based estimate.
Native structured tool images in request estimates use the same extraction
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

For native budgeting, later context edits, compaction, system checkpoints or
model-selection changes invalidate an older measurement. The Pi-compatible
session report instead follows the Pi projection and its invalidation rules.
Branch checkout and file
reopening reconstruct the appropriate measurement from that branch. Estimated,
zero-usage and failed responses do not create measured anchors.

Automatic compaction uses the native measured estimate when available, including
cases where a short text response consumed a large provider context. Summary requests retain
prefix-specific character estimates rather than borrowing usage for the whole
conversation. The compaction record and RPC response report Pi-compatible
`tokensBefore`, independently of the native retention budget.

Native request budgeting uses adapter-specific message estimates. Anthropic tool
results count their original text envelope; Bedrock and other adapters that
extract tool images use a fixed image allowance instead of counting encoded
image bytes. Bedrock also accepts user images. Matching provider replay blocks,
including reasoning and signatures, are included only when the adapter actually
replays them. Bedrock freezes inspectable SDK credentials or bearer tokens,
region and endpoint before reusing a measured baseline; opaque SDK clients or
custom transports fall back to fresh estimates. A measured baseline is combined with provider-aware estimates of subsequent
messages and any additional replay content. Request hooks that change the
model, messages or tools invalidate the measured baseline; that fact is recorded
with the request so it remains effective after reopening. A fingerprint also
requires the effective adapter identity, image policy, model, system messages
and tool schemas to match the measured request. Secret-bearing adapter and
header identities use a persistent HMAC key protected by owner-only file
permissions on Unix and user-scoped DPAPI on Windows. If protected key storage
is unavailable, the measured baseline is not reused. Deferred workbench providers
resolve credentials and headers before the capacity check and use the same
resolved adapter for dispatch; the AIP budget wrapper preserves preparation and
its budget settlement. Radius also freezes the catalog-selected route
for that request, so replay is counted only when that route will replay it.
Adapters that cannot prove a stable identity and older requests without the
markers use a fresh estimate. The estimator remains a heuristic rather than a
provider tokenizer or a strict upper bound.

Tests cover trailing input, edits, compaction, model changes, checkout, reopening,
zero usage and cached-input accounting. Loopback Ollama checks verify early
compaction from usage, measured RPC counts and fallback counts after an edit.
Complete native prompt checkpointing remains open.

### Tool argument token estimates

Pi estimates assistant tool-call arguments using JavaScript `JSON.stringify`
after parsing them. Native estimates now count JavaScript binary64 number
rounding and exponent formatting as well as UTF-16 string units, including in
imported assistant messages. To retain the correct binary64 value when imported
JSON is first read, the shared `serde_json` parser enables `float_roundtrip`.
This also changes decimal floating-point parsing throughout binaries that use the
core crate, including model configuration, provider responses, tool arguments,
and reserialized provider requests. Pi likewise parses JSON decimals as
JavaScript numbers. Native integer values retain their existing exact integer
representation, even above 2^53, and other native consumers may observe more
accurate decimal rounding. Previously persisted rounded values cannot recover
their original decimal representation. Regressions cover exponent cutoffs,
integers beyond 2^53, negative zero, nested values and escaped Unicode against
Node 24, as well as tool execution and an Anthropic loopback request. Inputs
that Rust's JSON parser rejects, such as non-finite numeric overflow, retain the
existing raw-input fallback, so complete JavaScript JSON parsing and
token-estimator parity remain open.

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

### Shell partial-line notice checkpoint

Completed local and injected remote model shell calls expose Pi's separate
`structuredContent` in tool events: bounded full output, truncation state,
exit code, rounded wall time and the full-output path when truncated. Text sent
to the model remains the display result; nonzero exits retain their error flag
and truncation details. Timeout and cancellation remain error results without
structured content. A text-replacing extension patch drops the now-stale
structured value; metadata-only in-process patches preserve it. Executables
advertising `structured_text_result` receive the structured outcome and
preserve that distinction through the returned outcome. Older executable
protocols receive a compatible text result; any returned replacement drops
the structured value because those protocols cannot distinguish a same-text
replacement from a metadata-only update. Legacy failure replacements retain
existing error details.
Duplex native-extension frames have a symmetric 16 MiB bound so escaped
structured shell output can cross the process boundary; oversized frames are
rejected, and the separate shell-stream protocol retains its 4 MiB bound.
Single-shot responses use the same 16 MiB allowance for negotiated structured
result hooks and retain the 4 MiB bound for other requests. Duplex call batches
are checked against the existing 4,096-call limit before their arguments are
materialized. A streaming preflight measures each argument's compact JSON size
against the existing 1 MiB limit. Oversized calls are skipped individually;
other calls remain ordered and participate in the shared nested-call budget.
Shell-progress and SDK host wrappers forward prevalidated positions; the
duplex host proxy advertises this support only when its underlying host opts in
to charging every position against its ordered call budget. A peer or custom
host without that capability rejects a batch containing an oversized call
rather than bypassing its call budget. The default in-process `ToolHost`
implementation applies the same fail-closed rule to direct callers.

For a byte-truncated final shell line, the displayed line size follows Pi's
streaming accumulator: a trailing newline resets the current-line byte count
to zero, while an unterminated line retains its UTF-8 byte count. The retained
tail and truncation metadata are unchanged. A regression covers both cases
with chunked multibyte output; other local-tool parity gaps remain open.

### Grep context and line-limit checkpoint

Invalid regular expressions in unrestricted local `grep` include the `rg:`
prefix used by Pi's ripgrep diagnostic. Unsupported look-around and
backreferences include ripgrep's PCRE2 suggestion.

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

### Local image reads

Local `read` preserves supported image bytes and MIME types when they fit Pi's
default image limits, and applies model-specific limits when configured. Its
text result reports the MIME type and, when dimensions change, the coordinate
scale back to the original image. The local read tool allows enough result
space for Pi's default inline image payload; scoped hosted tools retain their
existing tighter output bound.

### Local file-path input checkpoint

The file tools share Pi-style input normalization for leading `@`, home-directory
paths, file URLs and Unicode space variants. Windows input also accepts drive
paths written in Git Bash, Cygwin and WSL form, with case-insensitive `mnt` and
`cygdrive` prefixes as in Pi. Dot segments resolve before file
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
history. Message counts use Pi entry types and roles, including after import.
Hook and system messages contribute to the total without counting as user
messages, and standalone custom entries are excluded. Nested tool operations
do not inflate model-visible tool counts.
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


### Imported extension entries

Pi JSONL custom entries and custom messages retain their type, optional data
and details, display flag, and ordered text/image content in the native journal.
Imported custom messages remain model context and support branch-local context
edits. RPC session entries keep the original Pi representation without
duplicating the converted journal records. Codemode store entries also continue
to restore their branch-local state.
Legacy `custom` and `hookMessage` message records likewise retain their display
metadata and enter model context without being rendered as user prompts.
Imported message-form branch summaries use Pi's summary context framing while
retaining their original RPC representation and branch reference.
Message-form compaction summaries are displayed as summaries and framed for
model context without performing another compaction or removing earlier entries.

Imported failed or aborted assistant messages retain their original transcript
and failure status. Incomplete tool calls in those messages are not executed,
and their partial output is excluded from subsequent model requests, matching
Pi's treatment of unsuccessful responses.

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

### Imported session names

Session and owned SDK name reads trim ECMAScript whitespace from the latest
stored name, as Pi does, including names imported from external JSONL. A
whitespace-only name clears the title without falling back to an older name;
reading preserves internal line breaks and does not rewrite the journal.

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
OAuth upgrades for typed 403 scope challenges are described above;
push-notification optimization remains open.

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

Selected-branch forks resolve labels across the source journal, including
updates and clears made on other branches, and retain the latest label timestamp.
Imported Pi label entries are removed from the copied path and retained labels
are recreated; compaction boundaries and retained record metadata are remapped.
New Pi imports record conversion provenance. Forking rejects ambiguous legacy
ownership or inconsistent provenance when it cannot safely reconstruct the
selected history.
Interactive and startup branch forks retain imported conversion provenance and
compaction metadata when saving and reopening the fork.

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
regular-expression edge compatibility has not been declared complete. In
patterns without bracket expressions or explicit native dotall flags, `.`
excludes JavaScript's four line terminators in both regex engines. JavaScript
UTF-16 code-unit matching for supplementary characters remains different.
In patterns without bracket expressions that the linear regex engine accepts,
`\d`, `\D`, `\w`, `\W`, `\b`, and `\B` use JavaScript's ASCII classes
and word boundaries. Single-shorthand bracket expressions such as `[\d]`,
`[^\w]`, and `[\D]` also use those ASCII classes, including their negations;
shorthands outside complex brackets are translated independently by the linear
engine. Patterns without brackets or word boundaries that require
lookaround or backreferences use ASCII digit and word shorthand classes in the
bounded engine. Advanced patterns with brackets or word boundaries retain the
engine's previous Unicode classes and boundaries. Mixed bracket expressions and
other Unicode case folding differences remain open. The native engine matches
Unicode scalar values rather than JavaScript's non-Unicode UTF-16 code units:
for example, `^[\D]$` matches an emoji natively but not in Pi.

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

### Missing read path diagnostics

An unrestricted local `read` of a missing path now reports the resolved path
in Pi's `ENOENT: no such file or directory, access '…'` diagnostic. This also
identifies the requested file when a parent directory is missing. On Unix, a
non-directory parent reports Pi's `ENOTDIR` access diagnostic with the requested
path. An inaccessible file or parent reports the requested path in Pi's
`EACCES` access diagnostic. Other I/O errors and restricted workspace path
checks retain their existing behavior. A directory read reports Pi's Unix
`EISDIR` diagnostic, and an out-of-range offset preserves Pi's message without
a configuration-error prefix.

### Write and edit result checkpoint

The model-facing `write` and `edit` success messages now match Pi and preserve
the requested path spelling. On Unix, unrestricted local `edit` checks read and
write access before replacing the file and reports Pi's `EACCES` diagnostic.
On Windows, a read-only file attribute is rejected before matching edits.
As with Pi, this check does not prevent external permission or pathname changes
afterward. The edit result journals its bounded unified patch
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
diff, preserves each line’s original gutter (including empty line numbers),
and uses the existing word highlighting for single-line changes. Added, removed
and context rows use the corresponding Pi theme roles.
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
argument parsing. At the top level, built-in and extension flags can follow or
appear between prompt arguments. Use `--` before prompt arguments that look
like flags. Option values and prompt arguments retain their original spelling.
Tests cover clustered short options, nested command options and literal values. A real CLI check
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

Model cycling leaves the current selection intact for lists with fewer than
two choices. If the current model is outside the list, forward and backward
cycling start from the first slot, as in Pi.

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
An explicit null in `thinkingLevelMap` excludes that level, including
`off` for models that require reasoning. The terminal shows the new level
after cycling or reports that the model does not support thinking.
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

### Google thinking budgets checkpoint

Gemini 2.5 token-budget thinking uses Pi's model-specific defaults or the
selected `thinkingBudgets` setting, independently of `maxOutputTokens`.
Turning thinking off sends a zero budget when the model supports it. For a
discrete-level model that cannot disable thinking, an omitted level uses its
lowest supported mapped level without requesting thought output. Model
thinking-level mappings are resolved before selecting a budget or discrete
Google effort, including MINIMAL for Pro models. Bundled Google models retain
Pi v1.5.0 thinking maps, including unsupported levels and disabled fallbacks.
Configured reasoning models outside known families use the selected budget, or
-1 for an enabled level and zero when disabled. The configured budgets also enter
the protected request identity, so changing them invalidates a prior context
estimate. A loopback fixture checks the configured budget on the actual request;
live Google service limits and acceptance have not been verified.

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

An Anthropic fallback marker before output is accepted; a fallback after any
text, thinking, or tool block fails the attempt, as in Pi.

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

Native Anthropic streams expose individual text and thinking block boundaries,
and show redacted thinking as `[Reasoning redacted]` while retaining its opaque
payload for provider replay. Live Pi RPC thinking-start partial messages include
the redacted flag without a synthetic delta. Unlike Pi, they omit the opaque
thinking signature. Legacy model and agent observers continue to receive the
placeholder delta. RPC `get_messages` reconstructs signed Anthropic block order
from validated replay, retains the redacted flag, and omits opaque signatures
from history. Unsigned text-only responses are still collapsed into one text block.
Unlike Pi, the native agent has separate 1 MiB cumulative budgets for reasoning
deltas and provider-supplied finalized reasoning. When the finalized budget is
exceeded, block-end notifications contain only that block's observed deltas;
the response can still complete and retain its provider replay. Provider metadata
and replay are not covered by these notification budgets.

The native Anthropic adapter reads the model's advertised thinking mode and
effort levels. It selects adaptive effort or a bounded manual thinking budget
and reports thinking progress even when the provider omits visible text.
Unsupported levels follow the shared model-aware controls.

Signed and redacted blocks are retained in the original assistant block order
for the same provider and model. Tool continuations replay those blocks along
with the tool use and result. Assistant context edits replace the original
replay with the edited block sequence, preserving signed and redacted thinking
and excluding tool calls removed by repair. Native executable context hooks
receive the portable messages and retain replay for unchanged turns.

A loopback fixture verified adaptive effort, signed and redacted block replay
through a real tool call, and replay after reopening the journal. Pi JSONL
imports also recover signed blocks from native Anthropic Messages history.
Live account verification remains separate work.

### Live provider selection checkpoint

The terminal model picker searches the available Ollama, Anthropic, and Google catalogs.
Use `/model anthropic/<id>` or `/model ollama/<id>` to select a model directly,
or send the provider and model ID with RPC `set_model`. The worker reconnects
for the next prompt and keeps the current session. Reopening a session
restores the recorded provider when its connection is available and the CLI
has no explicit provider, model, or endpoint override.

`--models` and `enabledModels` accept ordered model patterns across providers,
including provider-qualified globs and per-model thinking suffixes.
`/scoped-models` edits this order, and Ctrl+P and RPC `cycle_model` follow it.
RPC `get_available_models` returns the available catalogs independently of
the cycling scope. Loopback tests exercised all three providers in one session
and a PTY verified provider toggling, reordering, and persistence.

User-level `models.json` supports `providers.ollama`, `providers.anthropic`, and `providers.google`
connection profiles with `baseUrl`, `api`, and `apiKey`. Profile keys can be
literal strings or refer to environment variables. The active provider's
`--endpoint` takes precedence over its profile. A runtime key stays with its
original provider and endpoint. A profile key is used only for that profile's
endpoint. Named custom providers, per-model overrides and credential
commands remain in progress.

Loopback RPC and PTY checks switched providers in one session, verified
conversation continuity and credential isolation between the endpoints,
and reopened a session with its saved provider.

### Provider API-key authentication checkpoint

Native SDK hosts may build immutable in-memory credential snapshots keyed by exact
provider and endpoint strings. Secrets have redacted debug output; clients must be
constructed explicitly by the host, which remains responsible for endpoint and
redirect authorization. Replacing a snapshot does not revoke existing copies or
clients. Unlike Pi's auth storage, this focused API performs no file, environment,
command, OAuth, or automatic credential discovery.

Use `/login` to select a provider or `/login <provider>` to open its masked
input directly. `/logout` removes a saved key. Keys are saved atomically in
the user agent directory's `auth.json`, bound to the endpoint selected when
the key was saved. Pi-style API-key entries without an endpoint apply to the
provider's default endpoint. The runtime override takes precedence, followed
by a saved key, a matching `models.json` profile, and the provider's environment
credential. `/reload` refreshes the saved-key view.

Both terminal modes passed key masking, invalid input, cancellation, restart,
and logout checks against a loopback Anthropic endpoint. The saved file used
mode 0600 on Unix, and the transcript and terminal output excluded the key.

### Google Gemini checkpoint

Use `--provider google --model <id>` or `--model google/<id>`. Google uses
the Generate Content REST API. Its `--endpoint` or `models.json` `baseUrl`
is an API base such as `https://generativelanguage.googleapis.com/v1beta`.
The official endpoint can use `GEMINI_API_KEY`. Custom endpoints can use a
matching profile key, saved key, or explicit `--api-key`.

The adapter discovers chat models through the paginated Models API and
streams text, thought summaries, and tool calls. It encodes image inputs and
tool results, records measured usage including thought tokens, and preserves
ordered signed response parts for the same model across tools and journal
reopening. Gemini 2.5 and newer model families use their budget or level
controls. A truncated response keeps its text and skips tool dispatch.

Loopback checks covered these paths and a PTY showed thinking, generation,
and the final answer. [Generate Content API](https://ai.google.dev/api/generate-content?hl=en)
· [Thought signatures](https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures?hl=en)

### Interactive MCP sign-in checkpoint

When a configured OAuth server requires sign-in, the terminal or RPC frontend
can confirm browser authorization before the first prompt uses that server.
The terminal also offers sign-in through `/mcp`. Accepted credentials use
the existing private store, and the tool catalog is available to the current
turn after authorization. Declining continues without the server. Print mode
and `pi mcp list` keep their noninteractive behavior.

Loopback checks covered rejection without a browser launch, initial consent
through both terminal and RPC, tool execution during the same turn, and
credential reuse in a fresh process. HTTP servers can also trigger sign-in
with a Bearer challenge when OAuth has not been configured explicitly. A
fixture exercised challenge-specific metadata and scope, and confirmed the
credential was usable after restart without editing `mcp.json`.

The `oauth` field also accepts an object with a pre-registered client,
environment or command-backed secret, explicit scopes, a fixed loopback
callback port and path, and an authorization metadata URL. Dynamic
registration accepts `clientName`. A hosted `clientMetadataUrl` can be used
when the server supports it. The explicit `cimd` mode requires this URL
for the Rust client identity. Existing boolean OAuth settings still work.

### Native prompt input hooks

Rust extensions opt in with `Extension::handles_input` and implement
`Extension::input`. The hook receives the original text, image attachments,
input source, and the extension's saved branch state. Hooks run in
registration order before skill and template expansion. `InputAction::Handled`
ends submission without a model turn. `InputAction::Transform` can change the
text and optionally replace the images. Leaving the images unspecified keeps
the previous attachments.

Terminal, RPC, print and the SDK's prompt methods use this path. Terminal
and RPC queues also run the hook and expand templates before saving input.
The hook receives the delivery behavior during an active turn. Queued hooks
run independently of cancellation and dialog replies. The executable
protocol advertises the hook during discovery and continues to load
extensions built without that capability.

### Model declarations and overrides

A supported provider's `models.json` profile can declare models by ID and
display name. The catalog merges declared and discovered IDs, and still
offers declared models if discovery fails. Display names can be selected
directly or matched by scoped model patterns. Overrides apply by ID without
adding entries to the catalog.

Declared reasoning and image capabilities appear in model discovery.
`contextWindow` supplies the context limit unless the CLI overrides it.
`maxTokens` caps the output allowance of each request. The terminal's model
picker reloads the file, and a key supplied for the original connection
stays bound to its endpoint when the file changes.

Loopback checks covered aliases, model overrides, unavailable discovery,
actual request limits, and updated configuration in a real terminal picker.

### Compatible model sampling

The chat-completions and OpenAI Responses adapters apply `samplingParams` and the effective
thinking level's `samplingParamsByThinkingLevel`. Model overrides merge
per level and per field. `thinkingLevelMap` can disable unsupported levels
and send an explicit value for a supported level. Request fields owned by the
agent, including the conversation, tool definitions, streaming and token
limits, are protected from sampling overrides.
This is a deliberate difference from Pi v1.5.0, which permits arbitrary sampling
fields to replace managed request fields. Pi-rs rejects such fields when configuring
the affected Responses model, preserving its streaming, conversation, tool, and
output-limit guarantees without blocking other models from loading. Responses
sampling can still set `tool_choice`, `parallel_tool_calls`, and `max_tool_calls`.

The loopback fixture exercised level clamping, the actual request payload,
and clearing the previous model's sampling configuration when switching
models on the same connection.

### Named model providers

A custom provider in `models.json` uses its own ID, base URL, models and
credentials. Its `api` selects one of the installed adapters
(`openai-completions`, `anthropic-messages`, `google-generative-ai`, or
`pi-messages`).
Select it with `--provider`, a qualified `--model provider/id`, RPC
`set_model`, or the terminal's model picker. Named providers participate
in scopes, saved model selection and the masked API key controls.

The RPC and terminal fixtures used two providers with the same model ID
and different local endpoints and authentication schemes. They covered
switching, credentials bound to each connection, journal restoration,
and provider identity in the transcript.

### Provider and model request headers

The compatible adapters apply provider headers, then declared model headers
and model overrides. Header names match case-insensitively. A provider can
enable `authHeader` to also send its key as a bearer token. Header values
can refer to environment variables, and credentials stay bound to the
configured connection. The terminal reloads these settings when opening
the model picker and replaces its worker when the profile changes.

Loopback checks exercised native and custom authentication headers, model
override order, endpoint isolation, redacted errors and a live terminal
worker reload.

### Session sharing destinations

`/share` and `pi share` honor `sessionShareDestination`. The default
`auto` uses Radius when a Radius API key is available and otherwise
creates a secret Gist. Explicit `radius` requires a Radius credential,
and a failed upload never sends the session to the other destination.
Radius receives the current branch as Pi v3 JSONL with organization
visibility. Gist returns both the Pi viewer link and the Gist URL. The
terminal shows upload progress and accepts Escape to cancel.

Radius accepts `RADIUS_API_KEY` and a saved OAuth credential for the
gateway. The `PI_RS_RADIUS_SHARE_URL` environment override can point to a
configured artifact endpoint. Native journals do not yet record the complete
system prompt and tool schemas for the viewer's `pi.share` metadata.

### Streamed tool call arguments

Chat-completions, Anthropic Messages and Gemini model streams expose tool
call fragments before the response completes. RPC emits `toolcall_start`
and `toolcall_delta` with stable assistant content indexes, then
`toolcall_end` with the completed call. JSON trace mode emits the native
`tool_call_delta` records. The Rust SDK exposes
the same transient fragments through its event subscription. Partial
arguments populate the snapshot once the accumulated JSON is a valid
object. Dispatch still uses the completed, journaled assistant response.

A loopback server held the final argument fragment until the RPC client
observed the first one. The fixture covered stable indexes after
reasoning, final arguments and tool output, and abort before completion.

### Radius OAuth

`pi login radius` signs in through a local browser callback with PKCE
and state validation. Use `--device-code` to sign in from another
device. The terminal's `/login radius` offers both methods and Escape
cancels the flow. `pi logout radius` and `/logout radius` clear the
gateway's saved credentials.

Credentials are private and bound to the configured `PI_RADIUS_GATEWAY`
origin. Radius sharing refreshes expiring credentials, serializes
concurrent refreshes and saves rotated refresh tokens. A local OAuth
fixture exercised both login methods, a rejected callback, cancellation,
concurrent refresh, endpoint isolation, authenticated share and logout.
Bearer-token export checks its requested minimum validity after refresh; a
token that falls short is persisted before the command fails, preserving any
rotated refresh token. Ordinary model, MCP and sharing requests accept a
successfully refreshed token without imposing that export requirement.
Normal Radius access refreshes credentials within five minutes of expiry,
matching the upstream OAuth resolver's minimum refresh window.
Before sending a refresh, native writes and syncs a keyless pending entry
containing no token; on Unix it also syncs the parent directory. The outcome
after power loss depends on the platform and filesystem, and equivalent
directory durability has not been verified on Windows. If the exchange fails, returns no
replacement refresh token, or its outcome is otherwise unknown, native fails
closed and requires a new Radius login instead of retrying the old token. This
is an intentional difference from upstream; it can require login even if no
request reached the server. A pending entry also prevents `auto` sharing from
silently falling back to Gist, while an explicit Radius API key still takes
precedence. A saved replacement whose requested lifetime is too short remains
saved as described above.

The pending representation is readable by pinned Pi v1.5.0 as a keyless
API-key entry, so that version's locked OAuth resolver does not reuse the old
refresh token. It does not protect tokens already copied into another process,
non-cooperating writers, storage rollback, or a compromised auth lock. After
a process crash leaves `auth.json.lock`, stop all Pi and pi-rs processes before
removing that stale lock directory from the agent directory; then sign in to
Radius again or explicitly log out. Never remove a lock held by a live process.
If a caller cancels while a Radius refresh is queued at the refresh barrier,
the exchange is discarded. Once admitted, it continues through credential
persistence even if the caller is cancelled. Admission can precede the actual
network request; this does not provide a separate refresh broker or a bounded
standalone-share shutdown.
The [pinned upstream OAuth resolver](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/ai/src/auth/resolve.ts)
also completes and persists an admitted refresh despite caller cancellation.
Upstream admits it under the credential-store lock and bounds the exchange to
15 seconds; pi-rs uses its refresh barrier and a 30-second request timeout.

### Pi Messages connections

A named provider with `api: "pi-messages"` posts Pi transcript context
and request options to the gateway's `/messages` endpoint. It streams
text, thinking and tool input, reports provider usage and response
identity, and carries signed response blocks forward on the same
connection. Changing endpoints excludes the opaque replay data.

A loopback fixture covered tool calls and results, streamed reasoning,
cache and cost accounting, signed history across process restarts and
credential and signature isolation on endpoint changes.

### Radius model discovery

The `radius` provider loads the gateway's authenticated model catalog
and uses its model endpoint on the same origin. The catalog supplies
display names, context and output limits, image support and thinking
levels. It can use a saved Radius OAuth credential, `RADIUS_API_KEY`,
or an explicitly configured model key. The in-process catalog is scoped
to the credential used to fetch it. A cross-origin model endpoint is
rejected before any credentials are sent there.

Local acceptance covered both authentication methods, model filtering,
inference against the catalog route, the advertised output cap and
rejection of an unrelated endpoint. The built-in gateway ships the
public chat model metadata from Pi 1.5.0. An authenticated catalog
replaces that baseline and is cached privately for the gateway and
credential. Offline startup and transient discovery failures can use
the matching cache. Successful empty catalogs remain authoritative.
[Pi AI 1.5.0 distribution](https://www.npmjs.com/package/@earendil-works/pi-ai/v/1.5.0)

### Single-shot JSON event stream

`--mode json` emits the Pi session header followed by agent, turn,
message and tool lifecycle events. It shares the RPC projection for
text, reasoning and fragmented tool arguments. Existing native record
metadata and the final `answer` remain available. The full request
trace stays behind `--trace`.

Visible reasoning is a transient model and SDK event. Print mode still
writes the answer as text. Loopback tests covered the JSON header,
reasoning before text, tool events, settlement, and the legacy answer.

### MCP provider authentication

An HTTP server in user `mcp.json` can set
`"auth": {"provider": "radius"}` (or name another installed provider).
The transport resolves that provider's bearer on each request. Updated
stored keys and Radius OAuth refreshes apply without an MCP credential
copy. A server using provider authentication signs in through the
provider's login controls.

Forwarding is limited to the configured HTTPS or loopback endpoint.
Project MCP files cannot request forwarding. The local fixture covered
key rotation on an existing connection, a Radius refresh, refusal of
project forwarding and a redirect that received no token.

### Command-backed model credentials

User model profiles can use `!command` for an API key or a request
header, including a model override. Endpoint-bound keys in `auth.json`
also support commands and environment references. Commands from
`auth.json` cache their first result for the process lifetime, including
failures. Model profile keys and headers resolve at request time. The
workbench bounds command execution and output. A key entered through
the masked terminal login stays literal.

The RPC fixture kept the stored key stable while refreshing the header
from its command on each request. A separate check verified that failed
stored commands stay unresolved, and endpoint overrides receive only
their explicitly supplied credentials.

### Queued SDK input hooks

Rust SDK controls run the same registered input hooks as direct prompts.
A hook can handle or transform steering and follow-ups, preserving
attachments unless it replaces them. Active submissions include their
delivery mode. Queue controls keep their order with submissions while
cancellation remains independent of input preparation.

The SDK tests cover handled and transformed queued input, image
attachments, fork and checkout state, delivery mode during a run and
clearing the resulting queue.

### Active-run native custom messages

A native tool or supported extension callback can call `ToolHost::send_message`
with `Steer`, `FollowUp`, `NextTurn`, `TriggerTurn`, or `AfterTurn`. These
correspond to Pi's default steering, `deliverAs: "followUp"`,
`deliverAs: "nextTurn"`, `triggerTurn: true`, and `triggerTurn: false`,
respectively. `TriggerTurn` queues steering during an active run. The last mode adds context after the
current assistant turn and its tool results without itself starting another
model request. `SessionControl` exposes the same active-run delivery modes;
`AgentSession::queue_custom_message_next_turn` also works while idle.

Messages are journaled before the host acknowledges them. Their type, content,
visibility and details survive reopening, and a single delivery record both
consumes the pending item and adds it to context. Steering and follow-ups share
FIFO order and queue modes with user input. Clearing the queue removes those
two modes but retains next-turn messages. Executable extensions negotiate the
host-call capability and report it unavailable with older hosts.

The SDK supplies a message host to initial input hooks and to `SessionControl`
input hooks while idle or during a run. Idle host sends are acknowledged after
journaling, and trigger messages start a run through the shared session owner.
Directly awaited control calls from an idle input callback use a bounded
callback-scoped child lane. To await a control call in a spawned task, capture
`SessionControl::scoped()` inside the callback and move the returned control
into that task. Scoped controls and message hosts expire when their individual
extension callback finishes, including initial-prompt input hooks. Host scopes
are bound to their session owner, so nested sessions cannot write to the
caller’s journal. Ordinary control clones used by independently spawned tasks
enter the normal FIFO queue.
Provider-request and context-with-system hooks and the normal scheduler's tool-call and tool-result hooks also receive a host.
Lifecycle hosts are available for active `agent_start`, `turn_start`, `turn_end`,
and ordinary and truncated-tool execution events; settled `agent_end` and
`agent_settled`; and SDK-owned session and model-selection notifications,
including active control rename, pre-navigation `session_before_tree` and
post-navigation `session_tree` notifications. SDK and workbench transition
checks, workbench resource discovery, and user-bash replacement, operation
selection, and streamed execution hooks also service messages through the
session owner. Streamed output and message calls share the negotiated process
channel. A workbench discovery or shell callback can schedule a triggered run
before the next user operation. Idle callbacks append messages without starting
a model turn, except `NextTurn`, which is queued, and
`TriggerTurn`, which starts a separate run. A trigger's queued message UUID is
also its run ID. Reopening a session resumes an interrupted triggered run with
that same ID and does not redeliver its message or repeat a recorded answer.
Reentrant sends are serviced by the journal owner while the callback waits.
Separately built extensions negotiate the individual callback capabilities.
Messages queued by `session_before_tree` are session-wide: they survive a
checkout, reopening, and an exported fork, and are consumed once across branch
switches. If a tree-triggered run was started on another branch, recovery uses
a journaled temporary handoff to resume there, then restores the selected branch
on success, failure, or cancellation. Reopening repairs an interrupted handoff.
The current branch's operations must be settled before the handoff. Branch
extension state is refreshed at both boundaries. If the originating branch
selected a different model, the host must register its model instance with
`SessionBuilder::branch_model`, `AgentSession::register_branch_model`, or
`Registry::register_branch_model`; otherwise recovery reports an error without
running the wrong model. The workbench resolves recorded provider/model selections
from its current configuration and registers them per session for recovery,
including each model's effective output and context limits and per-model
compaction tokens. Hosts that register provider models can supply their limits
and compaction tokens alongside the client; otherwise the current agent limits
and live session compaction settings apply. Handoff-specific tokens do not alter
the selected branch's live compaction settings. Cross-provider recovery requires an exact provider/model registration; absent
credentials or configuration leave the trigger pending with an error. Journals
do not distinguish two runtime instances with the same provider and model name. Immediate idle messages remain on the branch where
they were sent.

Focused tests cover queue and tool-result ordering, callback reentrancy,
initial and streaming input, provider-request delivery, session start and
settled boundaries, process protocol metadata, and reopening before the next
prompt. Tests also cover multiple triggers, idle and active delivery, waiting and
undelivered cancelled runs, replayed `agent_start` callbacks, separate SDK run
brackets, and restarts before run creation, before and after message delivery,
and after the answer was recorded. During a run, `agent_start`, `turn_start`,
`turn_end`, `before_request`, and `context_with_system` callbacks can call
`send_message_with_key`. Keys are scoped by run, extension, callback phase and
turn; request hooks use a journaled invocation ID and distinguish abandoned
preparations and completed attempts. Recovered
live and recovered tool-result hooks scope keys by operation and extension. The extension
must supply a stable key unique for each logical send within that callback.
Identical keyed replays reuse the journal entry even when callback order
changes; reusing a key with different content fails the run instead of retrying
indefinitely. Executable extensions negotiate keyed-message support.
The journal records completion of turn-end callbacks and application of deferred
context. Recovery reruns an unfinished callback before returning an existing
answer or advancing the tool turn, then applies deferred context once. Journal
version 3 identifies turns that use these boundaries. New records appended to a
version 1 or 2 journal use at least version 3; earlier records retain their
original version. An older assistant record
without a completion marker is treated as already observed: older journals
cannot distinguish a completed callback from one interrupted before completion.
Before a new run starts, unfinished earlier turn boundaries are resolved and
their deferred messages are applied; legacy deferred messages are applied
before the next request without replaying the legacy callback.

Exactly-once recovery is not possible for unkeyed callback sends when a process
stops between sends or the callback takes a different path. Ordinary Pi-style
unkeyed sends remain accepted on recovery; re-executing a callback can append a
message again. Extensions that can supply stable keys may use them for
recovery-safe delivery and must not switch an interrupted run's sends from
unkeyed to keyed: older journals do not contain enough information to match
those identities. Journals produced by earlier versions of this draft did not
scope keyed sends by request invocation, turn, or recovered tool operation; interrupted runs from
those versions cannot be migrated without potentially duplicating a send.
Automatic trigger
runs retain their own SDK run events and do not replace the initiating run's
result.

The pinned Pi [`_prepareRun` implementation](https://github.com/badlogic/pi-mono/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/src/core/agent-session.ts#L1941-L1982)
places the starting user or custom message before pending `nextTurn` messages,
even when the latter were queued earlier. The native scheduler follows that
order; it is covered for both user and custom starting messages. Host-aware
`before_tool` policies can opt in with
`handles_before_tool_host`; separately built extensions negotiate this support.
Model-generated summary compaction, synchronous SDK `compact()`, and the
asynchronous `compact_with_hooks()` SDK alternative run host-aware before/after
hooks. Synchronous SDK `compact()` blocks its caller while a separate runtime
services those hooks and triggered turns; callbacks that need work scheduled on
the caller's blocked executor should use `compact_with_hooks()` instead. Legacy
synchronous compaction hooks keep their signatures but execute on that worker
thread when called through SDK `compact()`. Cooperative asynchronous hooks are
bounded by the tool timeout; a legacy hook that blocks its worker thread cannot
be preempted. Tasks spawned on the private runtime do not outlive the call;
extensions needing longer-lived tasks should use the async API. Queue controls
submitted reentrantly during compaction hooks are serviced by the session owner.
Async `session_compact` and
`session_compact_failed` notifications can send through the session owner.
Manual preparation failures also emit `session_compact_failed`; its error
message identifies manual versus automatic threshold compaction, and aborted
events omit the error message. Native compaction retains its existing
preflight, error types, and extension replacement behavior. Manual and threshold
compaction accept extension-provided summaries before model generation.
Supported overflow recovery also invokes the hook; ambiguous interrupted
requests or hooks are not automatically repeated.
For host-bound workbench sessions, resource discovery
uses the state-only API and session transitions require the owning host. The
state-only `Registry::discover_resources` and `session_transition_cancelled`
APIs do not have a session owner; hosts that need message delivery use their
session-owned counterparts.
Discovery callbacks invoked through `Registry::discover_resources_in_session`,
and transition callbacks invoked through `session_transition_cancelled_in_session`,
can also use `ToolHost::append_entry` to persist branch-local custom entries.
Both in-process and executable extensions receive this capability. The callback
is acknowledged after the entry is committed; the entry does not enter model
context.
Idle session start, shutdown, and name-change notifications delivered through
the session-owned lifecycle API also support custom entries. Single-command
workbench startup and shutdown use this path. The owned SDK also exposes entry
access to scoped startup and idle input callbacks through their session owner.
Persistent workers and other callback types do not yet expose it.
These callbacks can read active-branch custom entries, optionally filtered
by type, including entries appended earlier in the callback. Responses over
8 MiB are rejected. An executable
request already dispatched when its callback ends can still commit; queued
requests are rejected after closure.
A trigger requested by a session shutdown callback remains queued for the next
session start because the closing session cannot start another run.
Command effects retain their existing return-value mechanism; custom message
renderers are unsupported. The default terminal rendering uses Pi's `[type]`
label, custom-message theme and Markdown, joining text blocks with newlines;
both immediate and delivered queued messages appear live and on session reopen.
Hidden messages and their details remain undisplayed. Native additionally keeps
its existing inline-image display for image blocks when graphics are enabled.

### Rust extension lifecycle observation

Rust extensions can opt into `agent_start`, `agent_end`, `turn_start`
and `turn_end`. Completed turn events contain the journaled assistant
and tool messages with their record IDs. Model retries stay within the
same turn. Agent end events include the durable run result, including
failure and cancellation. A bounded observing hook reports errors
without changing the model or tool result.

The native executable protocol advertises lifecycle support during
discovery. An independently built Rust extension verified turn order
across a retry and a tool call, successful and failed runs, and a
non-fatal callback failure.

### Native modal components

A Rust extension can request a custom modal component with styled
text spans, theme color roles and key actions. The terminal wraps and
scrolls the content in both display modes. Extensions receive the
selected action value and can open a new view with updated content.
Escape and Ctrl+C cancel the current component. RPC clients receive
the same component payload and submit a declared action value.

An independently compiled native extension verified scrolling, key
actions, component updates, cancellation and terminal restoration in
both display modes, as well as the correlated RPC responses.


### Radius share presentation metadata

Terminal `/share` appends Pi's `pi.share` entry with the session's system prompt
and active tool schemas to Radius uploads. Idle and in-flight shares use the
worker's session state; request-only `context_with_system` transformations do
not replace the shared prompt, matching Pi's `session.state` export. Pi v1.5.0
exposes interactive `/share`; the standalone `pi share` command is a pi-rs
extension. Standalone share keeps the journal lock without implicitly repairing
or creating the journal. An incomplete journal or pending handoff recovery is
reported before upload; an explicit `--name` still updates the session name.
The snapshot is matched to the session and head before upload, and a changed
session requires a retry. Idle refreshes accept a disk snapshot only when it
extends the worker's retained journal, preventing a delayed read from rewinding
its entry projection. If a request's journal entry and context are not yet
paired, the upload proceeds without metadata. Refreshed runtime prompts and
tool catalogs remain selected after the run settles; summary requests do not replace
the active context. The metadata is not written to the journal, and the 128 MiB share limit includes
it. Gist sharing continues to export HTML. When runtime metadata is unavailable, including missing model credentials,
startup failure, pending project trust, or standalone `pi share`, Radius uploads
continue without metadata rather than reconstructing it from stale settings.
Idle journal reads run on a detached thread, while active shares use the in-memory
snapshot; only one read can be outstanding. Journal preparation runs outside worker
locks and validates the captured version before publication. Transient streaming
events do not invalidate it, and prompt and tool metadata are retained by
reference while the worker locks are held. Preparing metadata does not block
the terminal event loop; extension dialogs remain interactive. PTY and loopback checks covered idle and
in-flight sharing; live Radius acceptance remains open.

### Native Codemode model operations

Codemode lists classifier and image entries using `models.getModelsOfType`,
`models.getAvailableOfType`, and `models.getModelOfType`. Scripts invoke System
One classifiers with `models.classify` and OpenRouter image models with
`models.generateImages`. Model credentials remain in the native host and
provider responses enter the same journal as other nested tool calls. Native
`models.json` also accepts configured entries with `type: "classifier"` or
`type: "image"` for supported endpoints.
Cloudflare Workers AI System One classifiers include Pi's five pinned catalog
entries and support the `/run` request and response envelopes. The official
endpoint uses an explicit account ID from stored credential metadata or
`CLOUDFLARE_ACCOUNT_ID`; Pi does not discover accounts over the network.
Endpoint-scoped credentials supply a stored account only when it reconstructs
their exact official classifier endpoint.
Configured official URLs can include `{CLOUDFLARE_ACCOUNT_ID}` or a concrete
account ID; placeholder expansion is restricted to the official account path.
Custom endpoints retain endpoint-scoped credential and header rules.
If a Cloudflare provider has no base URL and its configured models all name
the same endpoint, provider credentials and headers bind to that endpoint;
ambiguous model endpoints do not inherit them.
Codemode validates classifier and image model arguments before dispatch: invalid
model references and malformed contexts reject the script promise, while an
actual provider failure remains a structured model result. Classifier state
must be an object, and native request validation enforces that independently.
The configured model is resolved before context validation, without loading
credentials or making a provider request for the lookup.
Unknown and wrong-type errors identify the provider and model. Contexts are
validated after JSON serialization, so sparse array slots are rejected as null.
Classifier and image usage is included in the script result, terminal token
total, and RPC session statistics. The TypeSafe and OpenRouter non-chat
catalogues are projected from the published
[Pi AI v1.5.0 package](https://www.npmjs.com/package/@earendil-works/pi-ai/v/1.5.0).

### OpenRouter image result checkpoint

Codemode's non-chat model catalogue exposes declared image input limits and
classifier compatibility metadata. The pinned OpenRouter image entries retain
Pi's image resize profiles, while model request headers remain private.
Native descriptors also omit endpoint URLs, which can contain private path
components; unlike Pi, scripts cannot inspect `baseUrl` in these entries.

Image results record their API and creation timestamp even when request
validation fails. Response IDs and measured usage survive malformed choices,
and accepted text and images survive an error in a later image block. Empty
choices succeed. Usage estimates identify their total as locally calculated.
Non-data URLs and structurally incomplete data URLs are ignored. Unlike Pi,
pi-rs validates generated image MIME types and base64 before exposing them and
retains its response-size limit. Provider-level operation headers are bound to
the provider endpoint, including for classifiers; model headers apply to their
own endpoint. No live OpenRouter account was used.

### Chat model connection and compatibility

Codemode's chat-model catalogue includes declared input modalities, reasoning
capability, thinking-level mappings and maximum output tokens when that
metadata is available.

Configured chat models can select their own endpoint and request headers.
Startup credentials remain bound to the original provider and endpoint after
a model switch. Provider, model, and override compatibility settings can
select the token-limit field, message roles, streaming usage and finish
handling, and provider-specific thinking fields. Textual assistant reasoning
is replayed only to the model and endpoint that produced it when the receiving
adapter requires that field.

For Chat Completions, `compat.vllmPriority` supplies the numeric top-level
`priority` request field, including zero and negative values. Custom model settings
override provider settings; provider settings override bundled model defaults.
Model overrides apply afterward, and explicit
`samplingParams.priority` takes final precedence. Focused loopback tests cover
these configuration paths; no live vLLM server was used.

For reasoning-enabled Chat Completions models, `compat.thinkingTokenBudgetField`
selects `thinking_token_budget`, `thinking_budget`, or `thinking_budget_tokens`;
`supportsThinkingTokenBudget: true` selects `thinking_token_budget` by default.
Custom model compatibility overrides provider values, while provider settings
override bundled model defaults. The selected thinking level uses
Pi's default or configured `thinkingBudgets`, capped to leave 1,024 tokens for
the answer within the host's output limit. Off and non-reasoning models omit the
field. Discovered Ollama reasoning capability is carried through the RPC and
interactive worker to the request, even when discovery omits explicit thinking
levels. Chat-template `thinking.budget` variables remain unsupported. The request
field is covered by loopback tests; no live compatible server was used.

Loopback RPC checks exercised model and endpoint switching, header and key
isolation, reasoning replay, stream completion and reported usage and cost.

The bundled chat catalog includes 506 chat-completions entries for Cerebras,
DeepSeek, Fireworks, Groq, Hugging Face, Moonshot AI and Moonshot AI CN,
OpenRouter, Together, Z.AI, and Z.AI Coding CN. It preserves their context,
output, image and thinking capabilities from the pinned Pi AI release.
User models and overrides take precedence. The CLI lists available providers
together, and `--provider` can restrict the list to one. Built-in environment
keys and endpoint-bound saved credentials stay off user-configured relays.
Baseten and NVIDIA's pinned chat-completions catalogs are also available,
including model-specific compatibility, NVIDIA polling headers and Baseten's
thinking template. Catalog entries that use other upstream APIs still need
their own adapters.

### OpenRouter browser authorization

`/login openrouter` and `pi login openrouter` use OpenRouter's browser
authorization with S256 PKCE and a unique loopback callback. The terminal
dialog also accepts a pasted redirect URL or code. The returned key is
stored in the private auth file and is available only to the official
OpenRouter chat, classification, and image endpoints. `/logout openrouter`
removes the saved key. `/login openrouter api-key` opens the existing masked
API key editor. Browser startup, input masking and cancellation were
verified in both terminal modes. A loopback fixture exercised code exchange,
and credential tests verified official-route isolation.

The authorization contract follows
[OpenRouter's OAuth PKCE documentation](https://openrouter.ai/docs/guides/overview/auth/oauth).

OpenRouter chat also preserves structured `reasoning_details` across tool
continuations and session restarts. Text and summary fragments are combined
into their logical blocks, and encrypted blocks stay separate. Replay is
bound to the original endpoint and model. A loopback RPC check exercised
restart, encrypted-only thinking and a switch to a different model endpoint.

### Terminal progress indicator

The `terminal.showTerminalProgress` setting and the `/settings` toggle
publish the same OSC 9;4 indeterminate indicator as Pi while a model
request or compaction runs. A one-second keepalive maintains it in
terminals that clear stale indicators. Completion and terminal teardown
clear the indicator. The settings toggle edits only the user-level member
of the terminal settings object.

`terminal.showImages` and `terminal.imageWidthCells` control inline terminal
graphics. Both terminal modes apply changes from `/settings` to images
already in the transcript. Hiding graphics preserves textual tool results
and leaves model image delivery intact. A PTY check exercised visibility,
width changes, and existing user and tool images.

### Terminal autocomplete

Tab opens a selectable list when more than one file or command matches. The
selection replaces only the current word, preserving text to the right of
the cursor. Tab and Enter confirm, and Escape keeps the original draft.
`autocompleteMaxVisible` sets the row limit. `enableSkillCommands` controls
skill suggestions while preserving manual skill invocation and discovery.

### Model image delivery setting

`images.blockImages` replaces images with a text placeholder in outbound
chat context. Image data remains available in the local transcript and
journal. Codemode rejects image-bearing classifier and image-generation
inputs while the setting is on. The `/settings` toggle takes effect on the
next request. A PTY and loopback check covered local display, user and tool
image delivery, a live setting change, and blocked Codemode calls.

### Skill and prompt discovery

Skill directories recognize `SKILL.md` roots and direct Markdown skill
files, skip hidden and dependency directories, and honor local ignore
files. A declared root takes precedence over nested skills. Name and
description diagnostics preserve otherwise loadable skills. Malformed
prompt templates report their path while other templates remain usable.
Prompt descriptions without frontmatter use a bounded first-line preview.
Missing explicit prompt template paths are silently skipped, while native glob
patterns that match no files remain errors.

Native `grep` now applies inclusion and exclusion globs with the same
walker semantics as ripgrep, including workspace-relative patterns. Explicit
file targets still work with exclusion globs. Native `find` honors
`.fdignore` and omits Git metadata directories while retaining ordinary
hidden files and dependency directories.
Unrestricted local `find` follows Pi's fd-line trimming for terminal
ECMAScript whitespace, so a returned filename ending in such whitespace may
not name the original file. Newlines and carriage returns in a filename also
split it into separate output lines. Scoped `find` preserves its filename spelling.
[fd manual](https://github.com/sharkdp/fd/blob/master/doc/fd.1)

### CLI credential checks

`pi auth check` reports configured credential readiness for a provider
or model. It supports JSON output and a read-only `--no-refresh` check.
`print-api-key` and `print-bearer-token` explicitly resolve their own
credential type. Model-specific endpoints are respected. Radius bearer
tokens use the normal refresh path and accept `--min-expiry`. Kimi bearer
exports follow the pinned Pi resolver: after refresh, an explicit minimum is
checked with a five-minute floor, even when a shorter minimum was requested.
The rotated credential is saved before that check. The default
check output contains only status and metadata.

pi-rs does not automatically remove stale `auth.json.lock` directories.
After a worker admits a protected OAuth refresh, pi-rs retains its worker
refresh barrier until the auth lock is released, including on errors. A worker
terminated while holding the auth lock but waiting to enter that barrier can
still leave a stale directory, as can a crash or external termination.
To recover one safely, stop **all** Pi and pi-rs processes that can access
that credential store, including background or waiting processes on other
hosts sharing the filesystem. Once they have exited, either remove the stale
directory or run a single Pi process to recover it; wait for recovery to
finish before restarting any other process.

This restriction matters even when no process currently holds the lock. Pi's
`proper-lockfile` can observe a stale directory, pause, and later remove by
path after another process has replaced it with a live pi-rs lock. Concurrent
Pi stale recovery can therefore still compromise a pi-rs credential refresh
and lose a rotated token. pi-rs cannot serialize an already-running Pi
reclaimer with a separate lock or a path identity check; a complete fix
must prevent Pi's delayed removal from deleting a live lock. The current
protocol provides no such guarantee.

One possible defense to investigate is publishing a prepopulated lock
directory with an atomic no-replace operation, so a delayed `rmdir` fails while
the directory is held. That approach would change crash recovery and depends
on operating-system and filesystem behavior; it has not been implemented or
validated and is not protection in the current release.

Loopback checks covered expired and refreshed Radius credentials,
model-specific key selection, OpenRouter's OAuth key type, and invalid
configuration.

Native packages accept local `file:` URLs, including encoded spaces and
Unicode, and recognize HTTP, HTTPS, SSH, and Git repository transports.
Default transport ports and revision selectors are excluded from repository
identity. A local install and removal check verified that a file URL and its
equivalent path resolve to the same package.

### Native extension flags

Executable extensions can declare typed boolean and string flags through
`Extension::cli_flags`. Callbacks read the effective values through
`pi_rs_extensions::extension_flag`. Specify custom options before the
prompt, such as `--native-plan --native-target "東京"`. The terminal and
RPC forward the same flags to the worker. The extension's default applies
when a flag is omitted, and unknown or conflicting flags return errors.
Top-level `--help` lists flags from enabled, already-installed extensions,
including explicitly selected executables.

An independently built Rust extension verified default values, argument
validation, Unicode strings, and access from context and tool callbacks
in direct, RPC, and terminal runs.

### MCP client metadata selection

Set `mcpClientMetadataBaseUrl` in user settings to the HTTPS directory
hosting the application's Client ID Metadata Documents. pi-rs selects
`client.json` when the authorization server returns its issuer in
authorization responses. Otherwise it selects `<server-id>/client.json`
with a matching server-specific loopback callback. The ID is the first
nine SHA-256 bytes of the canonical MCP URL encoded as base64url.
Explicit client IDs and documents take precedence. Servers that lack
public metadata support use dynamic registration in auto mode.

A loopback OAuth fixture covered both document paths, dynamic registration,
and precedence. The hosted documents are supplied by the configured
application identity. [MCP authorization specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization#client-id-metadata-documents)

`images.autoResize` controls CLI file attachments, clipboard image
encoding and the native `read` tool. With resizing disabled, supported
files keep their original format and bytes within the input limit, and
clipboard images keep their dimensions. The `/settings` toggle applies
to subsequent reads. CLI and PTY checks compared bytes and dimensions at
the receiving model fixture.

### Native Anthropic and Gemini offline catalog checkpoint

The Pi v1.5.0 catalog now supplies 17 Anthropic and 23 Gemini chat models
with reasoning and image capabilities and token limits.
The model picker and CLI can list them offline. Automatic environment keys
are bound to the standard service endpoints, including the port and path.

### Model-specific chat API checkpoint

A native connection profile can declare the API per model. A model can
inherit the profile base URL or supply its own base URL. The CLI, terminal,
RPC model selection and Codemode catalog use the model's effective API.
A loopback session verified switching between Chat Completions and Messages,
endpoint-bound keys and headers, and route-bound signed Messages replay.

### Fireworks and OpenRouter Messages catalogs checkpoint

The offline catalog includes 19 Fireworks and 19 OpenRouter models using
Messages alongside the existing Chat Completions models. The official
Messages routes use bearer authentication. Environment credentials and
imported provider-wide keys or OpenRouter OAuth work on the official routes.
Explicitly stored keys remain bound to their saved endpoints. Loopback tests
covered adaptive thinking, effort mapping and replay with Fireworks empty
signatures and OpenRouter signed blocks. Additional cache warming and
mid-conversation options remain to be matched.

### Terminal output padding checkpoint

The Pi `outputPad` setting controls the horizontal margin of user and
assistant messages, thinking, tool panels, and inline images. It defaults
to one cell and can switch to zero from `/settings` in either terminal
mode. Existing transcript entries, stream content and exit replay use
the same layout. PTY checks covered changing the setting in both modes
and repainting existing Kitty placements at the new column.

### Terminal cursor checkpoint

The Pi `showHardwareCursor` setting and `PI_HARDWARE_CURSOR=1` fallback
select the native cursor or a reverse-video software caret. Text input,
dialog editors, and transcript search keep the real cursor positioned
for the terminal while the software caret is active. The setting updates
from `/settings` in both modes. PTY checks covered a wide Unicode character,
configuration precedence, live toggling, and exit restoration.

### Regular terminal redraw checkpoint

The optional `terminal.clearOnShrink` setting replays the transcript and
clears stale scrollback rows when the regular screen shrinks. It defaults
to off, skips modal overlays, and can be changed from `/settings`. The
fullscreen renderer already redraws the entire alternate screen. PTY
checks compared both settings and both terminal modes, including the
committed conversation and the edited multiline draft.

### Native terminal Mermaid checkpoint

The Pi `markdown.mermaid` setting supports `streaming`, `final`, and
`off` in both terminal modes. The renderer uses native Rust from the
implementation that inspired Pi's TypeScript renderer. It applies the
active theme to borders, text, arrows, and edge labels. [Pinned renderer source](https://github.com/xai-org/grok-build/blob/2bdd1d6a6369de0e8c68132ea4539e9abd9e14a8/crates/codegen/xai-grok-markdown/src/mermaid.rs)

Flowcharts, sequence, state, class, and ER diagrams render inline. Narrow
terminals and unsupported diagrams retain the Markdown fence. Streaming
can show a readable prefix while the last diagram line is incomplete.
Completed output retains source and a warning when that last line failed.
The journal, print output, and model context retain the original Markdown.
Partially accepted flowchart statements retain the source and report the first
warning and the count of additional warnings after streaming; during streaming
the readable portion remains visible. Like Pi v1.5.0’s
`grok-mermaid` parser, a class name immediately followed by an undirected link
without whitespace can consume part of that link and render an incomplete graph.

### Responses session routing checkpoint

API-key Responses requests in the workbench send Pi's session routing
headers. The default format sends `session-id` and `x-client-request-id`;
OpenRouter sends `x-session-id`, and a model or provider can select the
OpenAI format without `session-id`. Explicit headers take precedence, and
summary requests omit automatic routing headers. The existing native
`sendSessionAffinityHeaders` override can disable them.

The standalone API-key Responses adapter also adds session routing headers
through the session-aware model interface. SDK hosts can select an OpenAI,
OpenAI without session ID, OpenRouter, or disabled format. ChatGPT sign-in
uses a separate provider path and remains outside this checkpoint.

### Model session affinity checkpoint

The model adapter receives the durable session UUID for each agent
request. OpenRouter uses `x-session-id`. Fireworks follows the pinned
Messages or Chat Completions compatibility settings. Named profiles
and models can disable automatic affinity or select another format.
Explicit request headers retain precedence. Loopback checks verified
both APIs, command-resolved credentials, journal reopening, distinct
sessions, and header overrides.

### Chat tool strict-mode capabilities

The bundled Pi v1.5.0 Chat Completions entries retain their per-model
`supportsStrictMode` flags. Models declaring support send `strict: false` on
ordinary function tools; models declaring no support omit the field. Built-in
model flags override built-in provider defaults. An explicit provider setting
overrides bundled model flags, and a model override takes precedence over both.
User-defined models use their own compatibility settings over provider settings.
Nested `chatTemplateArgs` merge by key across these layers.

### Chat catalog pricing checkpoint

The bundled chat entries retain the input, output, cache-read, and
cache-write prices from Pi v1.5.0. JSON listings, RPC, and Codemode expose
these rates. A measured response without a provider total gets an
estimate in the journal and session statistics. An explicit provider
total takes precedence. Unmeasured responses remain unpriced. Model
overrides can replace individual price fields. Loopback checks covered
Chat Completions, native Anthropic Messages, Gemini, and RPC statistics.

### OpenRouter reported usage checkpoint

Both OpenRouter chat transports use complete reported usage totals. For
bring-your-own-key requests, the total adds the gateway charge and the
upstream inference charge. Separate prompt and completion charges can
supply the upstream total. Server-tool charges already included by the
gateway are counted once. Incomplete or malformed accounting falls back
to the catalog estimate when measured usage exists.

### Tiered rates and usage detail checkpoint

JSON catalog surfaces include the pinned input-length pricing tiers.
Estimates select the highest threshold strictly below total input tokens
and apply the separate one-hour Anthropic cache-write rate. The journal
and RPC retain the component costs, the one-hour token count, and whether
the total came from the provider. Existing journal records can omit these
fields. Loopback checks covered tier boundaries, nested cache usage,
provider totals, and the detailed RPC projection.

### Durable model API and Gemini route checkpoint

Native model metadata records the API used for each completed request.
RPC history uses that API even when models share a provider or the turn
contains no signed reasoning. Older journals retain the prior inference
path. Named Gemini connections bind signed parts to their provider,
endpoint, and model. Loopback tests covered Chat Completions, Messages,
and Generate Content within one profile, resuming the original Gemini
route, and changing the endpoint without forwarding its old signature.

### Per-model-only API configuration checkpoint

Custom providers with a shared base URL can declare the API on each
model. The catalog chooses the first declared chat API for provider
discovery, while each model uses its own adapter and route. When the
provider omits the API, a chat model without its own API is rejected.

### Per-model image resize checkpoint

The 394 bundled models with resize limits retain their width, height,
base64 budget, and JPEG quality. Custom model overrides merge individual
limits. CLI attachments, clipboard paste, and `read` apply the profile
before recording new images. In-budget files keep their original bytes.
Resized transparent images retain alpha. A same-endpoint model change
updates the active reader profile. The existing `images.autoResize`
setting governs model-specific resizing. Raw RPC images and pending
drafts use the active profile before submission, including queued
steering and follow-up inputs.

### Queued image preparation checkpoint

New RPC and TUI prompt images are prepared before the worker accepts
them, using the active model. Loopback checks covered a raw RPC prompt,
a follow-up queued during streaming, the journaled resized bytes, and
rejection of invalid image data before any model request.

### Chat Completions prompt cache keys

Chat Completions models honor `promptCacheKeyMode`: `enabled` uses the
session key, `disabled` omits the automatic key, and `auto` uses it for
OpenAI endpoints or supported long retention. Supported long retention also
sends the 24-hour retention request. Provider and model overrides merge, and
summary requests omit automatic keys and retention. Explicit sampling fields
retain precedence. Native Ollama keeps long retention disabled by default as a
pi-rs extension; its explicit compatibility setting can enable it.

### Responses instruction role

Responses requests use `developer` for models marked as reasoning-capable
unless `supportsDeveloperRole` is false. Other models use `system`. Provider
and model overrides merge for that compatibility setting. When capabilities
are unavailable, the standalone adapter defaults to `system`; SDK hosts can
explicitly select the developer role. This is also the fallback for unknown
workbench models, whose upstream capabilities cannot be inferred.

### Responses prompt cache key configuration

Responses model compatibility accepts `promptCacheKeyMode` as `auto`,
`enabled`, or `disabled`, with model settings overriding provider settings.
`auto` and `enabled` use the session-derived cache key when caching is active;
`disabled` omits the automatic key. An explicit sampling field can still
supply a key, matching the upstream request merge order. The setting does not
disable session routing headers or prompt caching itself.

### Responses cache retention checkpoint

For API-key Responses models, `PI_CACHE_RETENTION=long` requests Pi's
24-hour retention, or the 30-minute explicit cache option when the model
supports `supportsExplicitPromptCacheMode`. Provider and model compatibility
overrides can disable long retention. Summary requests remove configured cache
fields and, for explicit-mode models, request explicit caching with no
breakpoints. ChatGPT plan requests omit these API-specific options. Responses requests use the active session ID as their prompt cache key,
clamped to 64 Unicode characters, and omit it for summary requests. Pi’s
Responses session-routing headers remain unsupported.

### Native Messages cache control checkpoint

The workbench marks the system prompt, last supported conversation block,
and last tool definition with an ephemeral cache breakpoint.
`PI_CACHE_RETENTION=long` requests a one-hour breakpoint when supported.
Fireworks model compatibility controls retention and tool markers.
Compaction and branch summaries use the uncached model path. Loopback
checks covered ordinary Messages requests, Fireworks, and model-generated
compaction. Prompt warming remains.

### Compatible chat cache control checkpoint

OpenRouter Anthropic-compatible chat models and profiles declaring
`cacheControlFormat: "anthropic"` mark the instruction text, last
applicable conversation text, and last tool. Short and one-hour retention
respect the model and provider compatibility settings. Other chat models
keep their standard request shape. Compaction omits automatic cache
markers and session-affinity headers. Loopback checks covered default
OpenRouter routing, configured short retention, and compacted history.

A configured Anthropic `reasoning: true` declaration supplies fallback thinking
levels when model discovery is unavailable or omits thinking capability. When the Models API returns a
capability, its supported modes and levels drive both selection and requests;
explicit adaptive or thinking-level-map overrides remain authoritative. Other
discovered model metadata, including output limits, is retained with overrides.
Automatic thinking discovery for configured Anthropic Messages-compatible models
is restricted to the Anthropic provider; other gateways retain their declared
capabilities without an additional Models API probe.

### Native Anthropic authorization header checkpoint

Pi-compatible Anthropic subscription credentials already present in
`auth.json` are bound to the official Messages endpoint and refreshed under
the auth lock. OAuth requests use Pi's subscription headers, identity and
tool-name mapping. To avoid ambiguous dispatch, native rejects tool names
that differ only in case. `/login anthropic` supports browser and copy-code
authorization; `pi login anthropic --copy-code` enables the headless flow.
The browser callback binds and advertises IPv4 loopback and uses a state
independent of the PKCE verifier, unlike upstream's `localhost` callback and
shared state/verifier. Live subscription login and acceptance remain open.
Native token exchanges have a 30-second HTTP timeout; upstream bounds stored
OAuth refresh to 15 seconds.
Opaque Anthropic replay is shared between API-key and OAuth authentication
at the official endpoint, while custom endpoints use separate replay scopes;
legacy custom-endpoint replay without a matching scope falls back to the
ordinary message and tool-call representation.

The standard Anthropic Messages endpoint accepts an explicitly supplied
`ANTHROPIC_AUTH_TOKEN` as an Authorization bearer credential. Stored and
configured keys retain precedence. The header token takes priority over
`ANTHROPIC_API_KEY` on the standard endpoint. Custom URLs and ports do
not inherit it. Auth status remains masked and uses Pi's API credential
classification. Offline and loopback checks covered priority, endpoint
isolation, and explicit proxy keys.

### Published input limits and cache lifetimes checkpoint

The bundled Anthropic and Gemini catalogs retain Pi's declared request
byte and image count limits. Anthropic also exposes the short and long
prompt cache lifetimes. JSON model listings, RPC, and Codemode include
the metadata. Custom declarations merge individual limit, resize, and
lifetime fields. Release checks covered the offline catalogs, nested
overrides, invalid values, and the existing image resize and cost paths.

### Cache miss notices checkpoint

`showCacheMissNotices` is available from `/settings` and defaults off.
Both terminal modes derive notices from measured, completed chat turns
during a run and when reopening a session. Model switches and recorded
idle gaps appear in the notice. `/session` totals re-billed tokens and
estimated cost even when notices are disabled. Compaction and branch
summaries reset the comparison baseline. Failed requests and summary
usage stay out of the conversation cache comparison.

### Imported cache refresh checkpoint

Imported Pi session entries with `kind: "cache_warm"` preserve their
cache refresh notice, optional note, and billed cost. They also reset
the cache comparison baseline to the measured prompt at the refresh
timestamp. The original usage remains part of session billing. An
offline session import verified display and totals without duplicate
billing or an additional conversation message.

### Idle prompt cache refresh checkpoint

With the user setting `cacheWarming: "idle"`, a persistent terminal or
RPC worker can refresh an eligible Anthropic Messages prompt after the
run settles. The last confirmed request is replayed with a one-token
output cap when its published TTL and rates indicate at least $0.05 in
expected savings. The worker checks the same session head and setting
before dispatch, uses the original affinity and tool definitions, and
stops on a changed context or the 30-minute limit. A completed cache miss
retains its billed usage and schedules another decision on the normal cadence,
measured from the refresh request's start to protect the cache lifetime.
Failed refreshes and local model timeouts stop warming without
recording usage because the provider may still be processing the request. Pi
waits for the refresh to settle and also retries errors.
Refresh usage is journaled and projected as a Pi usage entry. Response
content stays out of the conversation. This checkpoint covers idle
requests with thinking off. Streaming warming and extension decisions
remain.

### Streaming prompt cache refresh checkpoint

Persistent terminal and RPC workers default to warming eligible caches
while the agent is waiting for the model or running tools. The worker
records measured refresh usage against the current model request through
the agent's journal writer. It cancels stale refreshes when context or
the request changes. In idle mode the same timer or in-flight refresh
can continue after the run settles. The terminal settings picker cycles
through streaming, idle, and off.

Anthropic models with published adaptive thinking can reuse their exact
thinking options. Budgeted thinking is skipped because the one-token
refresh cap would change its request. Loopback checks cover streaming,
tool execution, adaptive thinking, and the transition to idle. Extension
decisions and detailed warming status remain.

### Cache warming extension decision checkpoint

Native Rust extensions can opt into `cache_warming_decision` through the
embedded trait or executable extension protocol. Each handler receives
the original action, phase, model, measured prompt size, and estimated
economics with its own extension state. The last override takes effect.
A failed handler leaves the current decision in place. Successful forced
refreshes retain the override note in the journal, terminal, and RPC.
Legacy executable descriptions opt out by default.

### Cache warming status checkpoint

The terminal `/session` command reports the selected warming mode,
time until the next decision, an in-flight refresh, and the reason a
refresh stopped. When pricing is available it shows the expected savings,
refresh cost, cache miss penalty, and any extension override. The command
can read the journal during model and tool execution and does not become
a queued prompt. Changing the active journal head invalidates an idle
status for the previous head.

### Compaction and branch summary billing checkpoint

Measured summary usage and cost stay attached to compaction and branch
summary entries, including when a fork omits the original model request.
The terminal shows the billed tokens and cost when cache notices are
enabled. RPC session entries expose the cache breakdown and provider
total. Older native journals recover usage details from the adjacent
model completion when available. Imported Pi usage remains intact and
session statistics count each completed summary once.

### Foldable compaction and branch summaries checkpoint

Both terminal modes render compaction and branch summaries as themed,
foldable Markdown cards. The configured tool-expansion key opens or
closes all cards. Fullscreen clicks toggle one card and drags retain
text selection. Generated summary deltas stay out of the normal
assistant transcript. New compactions record their pre-compaction
context size, and imported sessions retain Pi's `tokensBefore`.
Loopback and PTY checks covered live compaction and branch summaries,
imported journals, custom bindings, individual clicks, and billing.

### Anthropic input transformation diagnostics checkpoint

Anthropic Messages responses retain bounded input transformation diagnostics
in the assistant's journal metadata and RPC message. A final SSE array
supersedes the initial array, including when it is empty. Pi JSONL imports
retain the diagnostic timestamp and transformation type, path, and reason.
With `showCacheMissNotices` enabled, both terminal modes warn when the
number of dropped thinking blocks increases compared to the previous
assistant response on the active branch. Reopening restores that baseline.

### Tool-result extension hooks

Native in-process and executable extensions can opt in to a `tool_result`
hook. After a tool returns a final result, hooks run in extension order and
can replace the outcome before it is committed and included in model context.
Later hooks see the preceding replacement. Unexecuted, unresolved, and pending
operations are never transformed, preserving recovery and reconciliation.
Hook failures are reported and leave the latest known outcome intact. Esc and
the tool timeout bound hook execution. Output limits still apply to replacements.

### Session tree extension checkpoint

Embedded and executable Rust extensions can opt into tree navigation.
The pre-navigation hook receives the selected entry, the current leaf,
their common ancestor, the abandoned entries, and the requested summary
options. A veto keeps the branch unchanged. The last returned decision
can supply a summary with details and usage, override the model summary
instructions, or label the selected entry or new summary.

The terminal runs the hooks for both summarized and unsummarized
navigation, including when the confirmation prompt is disabled. The
post-navigation lifecycle event reports the committed leaf and summary
origin. Extension-supplied details and origin survive journal reload,
fork, and RPC session projection.

### Owned SDK tree navigation checkpoint

`AgentSession::navigate_tree` runs the native tree extension hooks and
returns the selected user's editor text and any committed summary entry.
Callers can request a summary, replace its instructions, and attach a
label through `TreeNavigationOptions`. The same options reach the
pre-navigation extension hook. Branch-local input extension state and
model selection are restored after navigation. Subscribers receive the
committed records, and `SessionControl::abort` cancels a pending summary.

### Session lifecycle extension checkpoint

Native extensions that opt into session lifecycle events receive
`session_start` after runtime configuration and `session_shutdown` before
cleanup. This covers single-command model runs, including errors, and
persistent workers. When a worker attaches to another journal, it
delivers shutdown to the previous journal's extension state and start
to the destination's state, with both journal paths available across
the two events. A loaded terminal runtime restarts for new sessions,
forks, resumes, and reloads, and RPC session switches use the same
transition path. Both runtimes receive the corresponding reason.
Previously built extensions receive the reasons their descriptors support.

Renaming the active session in the terminal or RPC emits
`session_info_changed` to the loaded extension runtime with the
normalized name. The terminal resume selector uses the same notification
when it renames the active session. Pending name notifications are
delivered before an orderly worker shutdown. Executables built before
this event continue to receive their supported session events.

SDK subscribers receive `SessionEvent::SessionInfoChanged` after the
journal record. Embedders can use `AgentSession::set_name_with_hooks`
to also await the native extension notification. Both forms report the
normalized name, including a cleared name.
The first model operation, tree navigation, or hook-aware rename
delivers the start hook. An embedder can call `AgentSession::start`
explicitly. An owned fork reports the fork reason and the source path
when the SDK opened the source file. Closing a started
session delivers its shutdown hook before releasing registry resources.

### Session transition extension hooks

Native extensions can cancel new sessions, resumes, and forks through
`session_before_switch` and `session_before_fork`. When native extensions
are configured, the terminal and RPC start a worker for the first
transition even before a model request. A veto keeps the active journal and skips creating
the destination. Fork hooks distinguish the selected entry from the
position immediately before it. The owned SDK exposes the same fork
hook through `AgentSession::fork_with_hooks`.

### Tool-call argument mutation checkpoint

Native extensions can modify validated tool arguments in load order before
execution; subsequent hooks and the tool receive the changed value. The
executable extension protocol advertises this capability and falls back to
the existing policy hook for older executables. As in Pi, mutations are not
revalidated against the tool schema. Argument size limits still apply.

### Full-system context diagnostics checkpoint

When a `context_with_system` handler removes the leading system message, the
host reports a non-fatal extension error for that handler. Later handlers and
the provider still receive its transformed context, matching Pi v1.5.0. A
handler that restores and then removes the leading system message is reported
again.

RPC and JSON modes forward transient `tool_execution_update` events, including
the tool arguments, partial result, and parent call ID when present. Updates
are delivered during execution even when no extension subscribes to progress.

### Legacy session compaction import

Pi v1 session imports translate `firstKeptEntryIndex` compaction boundaries
using the original JSONL entry order, including the header. The original entry
is retained for inspection, while the imported context and forks retain the
correct post-compaction messages. A boundary at the compaction itself, or at a
later unassigned v1 entry, retains no earlier messages. Other legacy session variants remain subject
to the session import limitations above.

### Amazon Bedrock ConverseStream

The `amazon-bedrock` provider uses the AWS SDK's standard region and credential
chain, including `AWS_PROFILE`, shared AWS configuration, temporary role
credentials, and credential refresh. It also accepts `AWS_BEARER_TOKEN_BEDROCK`
or a configured API key as a Bedrock bearer token. The optional library feature
is `bedrock`.

The bundled Pi 1.5.0 chat model catalog provides offline model selection and
metadata, including pricing. It is a static snapshot, not an account-specific
list of enabled models. The runtime uses the configured AWS region rather than
pinning the catalog's default endpoint.

Text, user images, structured tool-result images, tool calls and results,
streamed reasoning, stop reasons, and input/output/cache usage are supported.
Redacted Bedrock reasoning emits Pi's fixed placeholder once per block while
retaining the opaque payload for replay.
Signed and redacted reasoning is replayed in its original order only when the
provider, model, text and tool calls still match, including the journal's
normalization of malformed arguments. For application inference profile ARNs
without a model family in the ID, set the model `name` in `models.json` (for
example, `Claude Sonnet`) to enable Claude-specific signature and error-image
handling. Images from Claude error results follow the grouped tool results.
Tool-free summary requests retain tool history as text and pass structured
images as image blocks. The SDK chooses the endpoint and signs requests or uses the
bearer token. No AWS resources are created.

Claude and GPT-family models support the Pi reasoning levels and configured
thinking-level mappings. Claude uses adaptive thinking on the supported newer
families and budgeted thinking on earlier models; GPT and GPT-OSS use their
respective effort fields, including explicit mappings such as GPT `off` to
`none`. The output limit supplied by the host remains a hard
ceiling. Budgeted Claude thinking reserves at least 1,024 tokens for the answer
and requires an output limit of at least 2,048; unlike Pi's simple API, this
adapter does not increase the host's output budget automatically. Reasoning
options for other model families are not sent, matching Pi's Bedrock adapter.
For budgeted Claude models, Pi's `thinkingBudgets` settings override the
minimal/low/medium/high defaults; xhigh and max use the high budget. Bedrock
requires integer budgets of at least 1,024; only the selected budget is
validated. The adapter clamps it to
leave 1,024 tokens for the answer within the host's output limit. Adaptive
models use effort instead and ignore token budgets. Embedders can also choose
summarized or omitted thinking display and disable interleaved thinking for
budgeted Claude through the Bedrock builder. GovCloud omits display; adaptive
thinking does not use the interleaved beta toggle.
Claude 5 models that default to adaptive thinking receive an explicit disabled
setting when thinking is off, where supported. Fable 5, Mythos 5 and Opus 5.5
cannot disable thinking; unset uses adaptive thinking with the model's default effort, explicit off
returns a configuration error, and summaries use adaptive thinking at low
effort. The output limit applies to thinking and answer combined, so thinking
can reduce the space available for a summary. Sonnet 5.5 uses `between_tools`
for tool-free summaries; with tools, explicit off fails and summaries use low
adaptive effort because thinking can still occur between tool calls.

For supported Claude models, the provider adds default five-minute cache
checkpoints after the system content and at the end of the final user message.
Set `PI_CACHE_RETENTION=long` to request one-hour checkpoints on models that
support that TTL; otherwise the provider uses five-minute checkpoints.
Workbench model and provider compatibility can explicitly override one-hour
support, and library callers can use `with_cache_retention` and
`with_long_cache_support`. Set
`AWS_BEDROCK_FORCE_CACHE=1` when the model ID and configured name cannot identify
a supported model; forcing checkpoints alone does not enable one-hour TTL.
Tool-free summaries disable checkpoints and optional reasoning so
that their smaller output limit remains usable. Cache usage includes
one-hour write details when Bedrock reports them. Model support and minimum
cacheable lengths are determined by Bedrock.

Configured provider and model request headers are applied before SigV4 signing;
`authorization`, `host`, and `x-amz-*` cannot override SDK authentication or
routing. Library callers can set custom headers on `Bedrock`. Effective custom
headers are included in the protected prepared-request identity. Custom values
are marked sensitive in SDK traces and redacted from service diagnostics.
During requests and stream reads with custom values, SDK and HTTP transport
events are suppressed because signing and error traces can contain unredacted
headers or echoed response frames; application tracing remains enabled.

Not yet supported: non-image multimodal inputs, workbench-specific thinking display controls, tool
choice/strict sampling, guardrails, prompt management ARNs, live account-specific
model discovery, and workbench proxy/idle-timeout settings. Custom model endpoints in workbench configuration are rejected;
callers embedding the adapter can pass a configured AWS SDK client, including a
test endpoint, through `Bedrock::new`.

### External editor result

The external-editor command follows Pi's configured, `VISUAL`, `EDITOR`, then
platform-default precedence, ignoring blank configured values and empty
environment values. On successful return it decodes UTF-8 with replacement,
strips one leading BOM and removes exactly one final LF. Failed editors leave
the draft unchanged. As in Pi, the temporary draft is closed before the editor
launches, allowing editors that require exclusive file access. Native shell-style
command splitting remains supported.

### Editor kill ring checkpoint

Typing now follows Pi's undo grouping: consecutive non-whitespace characters
form one undo unit, and each typed whitespace starts a new unit with the word
that follows it. Cursor movement, edits, newlines and undo break the typing
group; paste and programmatic insertion remain atomic. This applies to the
main editor, extension dialogs and transcript search. Focused tests cover
Unicode, repeated spaces, navigation, multiline input and paste boundaries.

Consecutive editor kill commands now combine their removed text in deletion
order, including newline joins. Cursor movement or another editing action
starts a separate kill. Yank-pop rotates the ring and is undoable, matching
Pi's editor behavior. Focused editor tests cover both directions, multiline
text, interrupted kills and yank cycling; other editor parity gaps remain open. Word navigation and word deletion use ICU word
boundaries and Pi’s ASCII punctuation rules, and move across a newline one
line at a time. As intentional fixes to pinned Pi, backward word operations
advance past a trailing Hebrew apostrophe and preserve combining marks on
punctuation graphemes. Native editing retains literal pasted text rather than Pi’s
atomic paste markers.

### Session tree branch navigation checkpoint

Tree fold and jump keys now follow visible branch segments rather than folding
or stepping through every record. Filtered-out intermediate records attach to
nearest visible ancestors for navigation; changing the search clears folds.
Focused selector tests cover linear chains, branches, hidden records, search,
and fold/unfold behavior. Other tree layout and navigation differences remain.

### Sparse custom-message import checkpoint

Pi custom-message entries with missing or null content now import as empty
content blocks, matching its session-context projection. RPC messages report
empty blocks while retained source entries preserve whether content was missing
or null. Tests cover export and reopen, branch-local edits, metadata and invalid
non-null content. Other specialized message variants remain open.

### RPC JSON diagnostics

Common truncated RPC JSON records report Pi/V8 wording for incomplete arrays,
initial object property names, unfinished strings with complete escapes
(including lone surrogate escapes), dangling backslashes, incomplete Unicode
escapes, invalid escape characters or Unicode digits, and missing values. Offsets
and columns count UTF-16 units; CR, LF and CRLF are counted as line breaks.
For records up to 1 MiB, a bounded diagnostic-only retry identifies a raw
control after a surrogate in another string if no other parse error occurs first.
Other malformed JSON retains the native parser's diagnostics; full
malformed-input parity remains open.

### Shell execution transcript checkpoint

Imported and live Pi-style shell executions display their output and completion
status separately from the command, instead of showing the model-context
`Ran ...` wrapper and Markdown fences in the terminal. Cancellation, nonzero
exit status and truncation paths are included in the display output; the native
excluded-from-context label is retained. Model context and journal data retain
their existing framing and values. The terminal continues to use its generic
tool styling and preview limits, so a long output may hide its status until expanded.
Shell transcript rendering strips terminal escape sequences and normalizes line
endings, including for imported and extension-provided output; journal and
model-context text retain the original bytes.

### Compaction retention token estimate

Token-based compaction retention uses the projected Pi message estimate,
including assistant thinking and structured tool-result blocks, when choosing
a cut point or complete turns. Context edits replace the original estimate.
A branch-local index shares reasoning and latest edits across message estimates;
a 2,500-turn regression covers long histories and context usage.
The separate native model-request safety budget retains its existing
conservative estimate. Other token-estimator differences remain open.

### Model listing search checkpoint

`--list-models` searches provider and model IDs with Pi's tokenized fuzzy
matching, including the ASCII letter/number swap (for example `p2` matches
`nova-2p`). Display names alone do not match. The native JSON listing remains
available; model scope and picker selection retain their existing behavior.

### Messages quota retry classification

Anthropic-compatible Messages requests inspect bounded retryable HTTP error
responses and stream error fields for Pi's account, quota and billing markers.
Structured JSON classification uses error message, code and type fields, excluding
request identifiers and other metadata. Those failures
stop agent retries; transient rate limits remain retryable. Response bodies are
not surfaced or logged. Classification falls back to the existing retry behavior
when the marker lies beyond 64 KiB or the response cannot be read within two seconds.
Complete error fields preceding a truncated later JSON field remain classifiable;
unrelated metadata, incomplete numeric values and undelimited numbers are ignored.
Known credentials and configured header values are redacted before error fields
are classified, including for stream errors.
Other provider adapters still need equivalent quota classification.

### Bedrock quota retry classification

Bedrock service and stream exceptions stop agent retries when their redacted
error message or code contains Pi's account quota or billing exhaustion
markers, even if AWS categorizes the exception as transient throttling. The model receives a
fixed limit message; ordinary throttling remains retryable even if its request
ID contains a marker. Classification scans the full SDK error message with a
fixed-size rolling window and redacts secrets before matching. This uses bounded
additional memory; scan time depends on message length and configured secrets.
AWS SDK retries before an exception reaches
the agent remain governed by the configured SDK policy.

Pi's non-chat System One provider-level policy retries transient HTTP status
codes including 429 without classifying the response body; native keeps that
separate policy.

### Provider quota retry classification

Google Gemini, OpenAI-compatible chat, OpenAI Responses and Pi Messages requests
recognize permanent account, billing and quota exhaustion in structured error
message, code and type fields for retryable HTTP statuses and stream errors.
Recognized transient stream errors, including Gemini errors without a numeric
code, remain retryable after permanent-limit checks. OpenAI response codes are
classified only after known credentials are masked.
JSON metadata such as request IDs does not affect classification. Known request
credentials, Bearer tokens and conventional authentication-header values are
masked before classification, even when they equal quota or transient terms.
Other configured header values are masked
when distinguishable from those terms; ambiguous header values remain
classifiable because an echo cannot be distinguished from a provider error.
Diagnostics remain fixed and do not include provider bodies. Transient stream
matching follows the pinned Pi patterns. OpenAI subscription
codes retain their explicit error precedence. Plain-text errors are also supported. HTTP body inspection is limited to 64 KiB and two
seconds. Complete error fields preceding a truncated later JSON field remain
classifiable; incomplete values and unrelated metadata are ignored. Failed or
timed-out inspection falls back to the existing transient classification. Provider error bodies are not logged or
returned. Bedrock typed SDK errors and non-chat operations retain their separate
policies.

Native SDK hosts can inspect the configured base system prompt with
`AgentSession::configured_system_prompt`. Forks copy that value. This accessor
does not run extension hooks and is not Pi's effective `systemPrompt`; hooks may
modify individual requests after the configured prompt is read.

SDK hosts can read a session's selected provider with `model_provider()`
alongside `model_name()`, including after model switches and branch checkout.
Provider-less legacy selections return `None`; an unavailable historical model
returns an error rather than the builder default. A busy journal or an
unreconciled direct journal selection or revoked client binding also returns an
error. This is a point-in-time check, not a guarantee of later availability.

### Native SDK theme snapshots

`pi_rs_workbench::theme_resources::ThemeReadGrants` loads an ordered snapshot
from explicitly granted absolute files without parent components. It shares schema validation and color
resolution with the interactive frontend and exposes each name, source path and
resolved color roles without selecting a theme or probing the terminal. Reads
are synchronous, bounded, follow symlinks, and are not a filesystem sandbox.
A grant includes the symlink target. Reloading creates an independent snapshot.
Missing, invalid, duplicate-name, oversized or excessive inputs fail atomically;
variable chains have a depth limit. Unlike Pi's resource loader, this API does
not discover directories, packages, settings or built-in themes, detect terminal
capabilities, or return per-file warnings and deduplicate collisions. Hosts choose
when to apply a snapshot, and revoking a grant does not erase an existing one.

### Native SDK prompt resource factory

`pi_rs_workbench::prompt_resources::PromptResourceFactory` exposes the system
prompt resource part of Pi's `DefaultResourceLoader` to native hosts. Each
`load` accepts current `PromptReadGrants`: an optional working directory for
`.pi/SYSTEM.md` and `.pi/APPEND_SYSTEM.md`, and an optional agent directory for
the corresponding global files. Nothing is read without a grant. As in the
[pinned loader](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/coding-agent/src/core/resource-loader.ts),
project files take precedence independently for each category, and explicit
system or append values replace discovery. File values have a UTF-8 BOM removed
and carry their source path. An empty system prompt uses the host default; an
explicit empty append list suppresses discovery. `ExplicitPromptResourceFactory`
also accepts unambiguous literal and file inputs for each category. File inputs
require a matching current grant for the absolute path, reject parent components,
retain their source path, and fail atomically on missing or unreadable files.
Explicit inputs are limited to 128 entries and 4 MiB of resolved text per
snapshot. File grants follow symlinks and are not a sandbox.

The host must obtain and reevaluate its own permissions and choose the paths;
grants permit ordinary filesystem reads including symlink targets and are not a
sandbox. Loading is synchronous, so GUI hosts should run it off their UI thread.
The returned snapshot is independent of later loads: apply it to a
`SessionBuilder`, or compose and explicitly set an idle session's system prompt.
Revoking a grant affects future loads, not snapshots or sessions already given
content. A failed load returns an error without modifying an earlier snapshot.

This is a focused native API, not a complete Pi resource loader. It does not
load skills, prompt templates, extensions, context files, themes, or settings;
it does not interpret an explicit string as a filename; hosts opt into file
inputs explicitly. Unlike Pi's fallback
for unreadable prompt files, invalid UTF-8, oversized files, and read failures
are errors. Unix file opens are nonblocking and validate the opened handle before
reading, so a FIFO is rejected. The host supplies the default prompt and any Pi-specific context
formatting; `compose` joins nonempty appendices with blank lines. Existing CLI
loading remains independent.

### Local image read cancellation

Local `read` passes its cancellation token through image detection, decoding,
resizing and conversion. Cancelling a read returns `Operation aborted` and
allows the blocking worker to stop at the image processing cancellation checks,
matching Pi's aborted read behavior. The restricted no-profile image path also checks cancellation after decoding,
thumbnailing and each PNG encoding attempt. Individual image operations remain
synchronous and are interrupted at their checkpoints.

SDK hosts can load ordered context-file snapshots from explicitly granted directories.
Each directory uses Pi’s context filename precedence; repeated selected paths are
loaded once. Hosts choose directories and
when to apply a snapshot. No ancestor or global discovery occurs implicitly. Reads
follow symlinks, fail atomically on errors, and use the prompt resource size limit.
Unlike Pi, unreadable higher-priority files fail rather than falling through.
Hosts that have verified a nested linked worktree can opt into Pi's matching-name
shadowing by supplying both explicitly granted roots. The selected main-root
entry is suppressed when the nested worktree has the same selected filename;
separate directory aliases remain available for global context. Hosts identify
the global role explicitly when it shares the main-root path. The host verifies
the repository relationship, and no Git metadata or additional paths are read.

The SDK model catalog can enumerate its host-registered provider/model identities
in deterministic order for host model pickers. This lists the immutable catalog
snapshot, not Pi's discovered model registry or a live credential availability
check. The host remains responsible for rebuilding it when authorization changes.
Hosts can optionally attach display, API, context, output, reasoning, image and
cost metadata to an exact identity. These values are descriptive and do not
configure the model client, enforce capabilities, or perform discovery.

### Explicit skill file diagnostics checkpoint

Explicit skill files must end in `.md`; other files produce a diagnostic while
the remaining skills continue loading. The check uses the supplied filename
before following symlinks, matching Pi.
Missing explicit skill paths also warn and leave other resources available.
Unmatched explicit glob patterns remain errors as a native extension.
For missing paths, glob metacharacters outside the working-directory prefix are
interpreted as patterns; this preserves native glob precedence for ambiguous paths.
