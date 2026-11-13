# pi-rs

An embeddable Rust agent loop with streaming model responses, tools, an
append-only session journal, and native Rust extensions. The workbench offers
an interactive terminal, print and JSON output, and a JSONL control interface.

## Interactive terminal

```sh
ollama pull qwen2.5:7b
cargo run --locked -p pi-rs-workbench --bin pi -- --model qwen2.5:7b
```

Or install the local command with
`cargo install --locked --path crates/workbench --bin pi`, then run `pi`.
Sessions are saved under `~/.pi-rs/sessions/`, grouped by working directory.
`PI_RS_HOME` changes the storage root. `pi --continue` reopens the latest session
for the working directory, and `pi --session <path>` opens an explicit session.

Enter sends a prompt. Shift+Enter adds a line, Alt+Enter queues a follow-up,
Escape cancels the current operation, and PageUp/PageDown scroll the transcript.
The status bar animates while waiting and shows elapsed time, confirmed model
loading status when local Ollama reports it, thinking activity, and answer
generation. Token totals are labeled as settled usage and update on completion.
`/help` lists the available commands, including `/model`, `/new`, `/resume`,
`/compact`, `/tree`, `/checkout`, `/plan`, `/review`, `/notes`, and `/todo`.
The default CLI and interactive profile enables `read`, `write`, `edit`, and
`bash` (`powershell` on Windows). Shell commands run with the launching user's
permissions. `--coding-tools` additionally enables `grep`, `find`, and `ls`.

```sh
pi --print 'Explain this project'
pi --mode json 'Explain this project'
pi --mode rpc
```

The existing `pi-rs ... run <prompt>` interface retains its JSONL output.
Use `--tools read,grep` for an allowlist, `--tools=+grep,-bash` to adjust the
defaults, `--exclude-tools "mcp__*"` to exclude matching tools, or `--no-tools`
to start without tools. `--no-builtin-tools` retains extension tools. The
current RPC commands are documented in
[the parity checklist](docs/native-parity.md).

The model connection has a five-minute HTTP idle timeout by default; receiving
data resets it. Use `/settings` to change it, or set `httpIdleTimeoutMs` in
`settings.json` to a number of milliseconds or `"disabled"`. For example,
`{"httpIdleTimeoutMs":"disabled"}` removes the HTTP idle deadline.
`--model-timeout-secs` separately sets a deadline for the complete operation.

To use an OpenAI API key and API billing, set `OPENAI_API_KEY` and select the
`openai` provider, for example `pi --provider openai --model gpt-4.1`.

To use eligible ChatGPT plan usage, sign in with your ChatGPT account and
authorize plan usage in the browser:

```sh
pi login openai-chatgpt
pi --provider openai-chatgpt
```

Inside the terminal, `/login openai-chatgpt` starts the same flow, and
`/logout openai-chatgpt` signs out. Available models and usage depend on the
account and workspace. Manage app usage in [ChatGPT Settings](https://chatgpt.com/settings/usage).
Use `pi login openai-chatgpt --new-account` to add another account or workspace,
`pi login openai-chatgpt --list-accounts` to list registrations, and
`pi login openai-chatgpt --account <client-id>` to select one. The terminal's
`/login` menu also offers saved accounts and a new-account option.

For Amazon Bedrock, use `/login amazon-bedrock` in the terminal to choose a
bearer token, a named AWS profile, or the existing AWS credential chain. The
choice is saved in Pi's `auth.json`; `/logout amazon-bedrock` removes it. Named
profiles are resolved by the AWS SDK without changing the process environment.
An explicitly selected profile or credential chain ignores the ambient
`AWS_BEARER_TOKEN_BEDROCK` token. This intentionally differs from the pinned Pi
v1.5 behavior, which gives that token priority. Without an explicit choice, the
ambient token retains priority.

To use a Kimi Code subscription, run `pi login kimi-coding` and complete the
device authorization, then select `--provider kimi-coding`. In the terminal,
`/login kimi-coding` and `/logout kimi-coding` sign in and out. Tokens are
shared with Pi through `auth.json` and refreshed automatically. To enter an
API key instead, use `/login kimi-coding api-key` or `KIMI_API_KEY`.

## Native extensions

The terminal enables notes and a TODO checklist, alongside plan mode and branch
review. Their state is stored in the session journal and follows its branches.

```text
/notes Keep the public API compatible
/todo [{"text":"Inspect the parser","status":"in_progress"}]
/plan Investigate before making changes
/plan accept
/review Check the parser changes
```

For one-shot use, enable a bundled extension explicitly:

```sh
pi --session /tmp/example-session.jsonl --extension notes \
  extension notes set 'Keep the public API compatible'
```

See [the native extension guide](docs/native-extensions.md) for the catalog,
configuration, and the Rust API for writing and registering another extension.
[The parity checklist](docs/native-parity.md) tracks the remaining Pi v1.5.0
features and distinguishes implemented behavior from planned work.

## Run locally

Requires Rust 1.89 or newer, Ollama, and a local model that supports tools. The
workbench's fixed command tool uses `/usr/bin/wc` on macOS or Linux.

```sh
ollama pull qwen2.5:7b
cargo build --locked --release -p pi-rs-workbench

state="$(mktemp -d)"
workspace="$(mktemp -d)"
printf 'XR-314\n' > "$workspace/stock.txt"
target/release/pi-rs --session "$state/session.jsonl" --workspace "$workspace" \
  --model qwen2.5:7b run 'Use read_text on stock.txt and give me the code.'
```

Keep the session outside the workspace exposed to tools. Repeat the model,
workspace, and extension options when reopening it. `inspect` lists journal
record IDs and tool statuses. `checkout <record-id>` selects a settled branch
point. `compact --keep-turns 1` generates a summary without tools and retains
the latest turn. Supply `--summary <text>` to use your own summary. A following
`run` receives the compressed context. `resume` continues an unfinished turn
and returns the stored answer if the turn already completed.

- `--project .` loads trusted `AGENTS.md` files. Pass a workspace-relative
  directory to include nested instructions. `--skill harness-extension`
  loads the corresponding `.pi/skills/<name>/SKILL.md` explicitly.
- `read_text` reads a bounded UTF-8 file. `count_lines` runs a fixed command
  with an empty environment and requires `--allow-commands`.
  `--orders <collection-url>` adds `lookup_order` using a read-only
  `GET <collection-url>/<id>`. Authentication is supplied separately via
  `PI_RS_MODEL_TOKEN` or `PI_RS_ORDERS_TOKEN`.
- `--stdin-control` on `run` or `resume` accepts
  `{"type":"steer","content":"..."}`, `{"type":"followup","content":"..."}`,
  and `{"type":"cancel"}` as JSON lines. Optional UUID `id` deduplicates
  a resend. An `input_queued` event acknowledges durable receipt. Steering
  enters before the next model call. Follow-up waits for the current turn
  to finish. Ctrl-C also cancels.
- Stdout uses JSONL. The default stream includes record IDs, statuses, tool
  names, usage, and the final answer. Use `--trace` in a trusted workspace to
  include request context, tool arguments and outputs, and streamed text.
  The journal contains conversation data and is created with mode `0600`
  on Unix. Keep journals and trace output in private storage.

An operation dispatched before a crash, cancellation, or timeout can have an
unknown outcome. Find its ID with `inspect`, verify its outcome with the
service that owns the effect, then use
`reconcile <operation-id> <succeeded|failed> <evidence>` and `resume`.
The harness leaves dispatch decisions to the host and requires a settled
history before branching. See the [execution and extension contract](docs/extensions.md)
for lifecycle and storage details.

## Develop and evaluate

```sh
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

The [workbench extension crate](crates/workbench/src/lib.rs) provides tool
registration, a command policy, and a versioned context extension.
[Evaluation and adoption](docs/evaluation.md) records the pinned Pi comparison
and the boundaries with AIP, Field, and Workspace Runtime.

### Execution contracts

- `--context-window <estimated-tokens>` enables one automatic compaction
  attempt per run. The newest request is preserved verbatim. Older history
  is summarized under the same output budget.
- `--run-id <uuid>` lets a host correlate an initial request and an explicit
  `resume`. Duplicate initial requests fail before model or tool execution.
- Standard output is JSONL. Standard error carries errors. Exit 0 is success,
  1 is a runtime error (including operations awaiting reconciliation), 2 is
  CLI input error, and 130 is cancellation. Broken output pipes do not stop
  durable writes. Use the journal to determine the final state.
- See [architecture](docs/architecture.md) and the
  [extension contract](docs/extensions.md) for provider-neutral embedding,
  plan mode, and branch review.

[Expanded Pi comparison](docs/expanded-evaluation.md) covers synthetic code
repair, writes, compaction, and branch review.
