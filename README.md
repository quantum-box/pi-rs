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
  `{"type":"followup","content":"..."}` and `{"type":"cancel"}` as JSON lines.
  The durable user record acknowledges an accepted follow-up. Follow-ups enter
  context between complete tool batches or model turns. Ctrl-C also cancels.
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
