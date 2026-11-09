# pi-rs

An embeddable Rust agent loop with streaming model responses, tools, an
append-only session journal, and extensions registered at build time. The
workbench CLI exercises the library with Ollama and three representative tools.

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
