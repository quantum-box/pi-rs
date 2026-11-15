# Durable Anthropic Messages adapter

This standalone MIT-licensed crate is opt-in. Add it as a path dependency, construct `AnthropicAdapter::new(endpoint, api_key)`, install it with `Harness::install_provider_adapter(Arc::new(adapter))`, and enable it for a conversation with `select_provider_adapter(id, true)`. Install the same adapter identity before reopening a selected conversation. No provider is registered automatically. Run `cargo test --manifest-path crates/durable-anthropic/Cargo.toml` independently.

The message encoder and SSE interpretation are adapted from `crates/anthropic/src/lib.rs` under the repository MIT license. Text, images, tool results, streamed text and function calls, token usage, and `max_tokens` truncation are supported. Incomplete or malformed streams fail rather than return partial results. A `length` finish reason tells the durable runtime not to execute partial calls.

The API key is held by the adapter, excluded from hook payloads and errors, and attached as `x-api-key` only after validation. The endpoint must be HTTPS or numeric-loopback HTTP, without URL credentials, query or fragment. The host must trust the configured endpoint and hooks: HTTPS can target arbitrary hosts and hooks can inspect or replace conversation input. Proxies and redirects are disabled. Requests and raw response streams are limited to 4 MiB; connect and read inactivity timeouts apply. Cancellation drops the opening future or response stream, but cannot guarantee remote execution stopped.

Validation fixes the model, streaming, tool definitions and output token ceiling, and only permits a small allowlist of top-level fields. Hooks may modify messages and allowed options; basic message shape and sampling option types and ranges are checked. Tool blocks with both nonempty initial input and streamed JSON are rejected as ambiguous. This is not a general authorization or cost policy; hosts must control hooks and provider-side budgets.

Extended thinking, signed reasoning replay, citations, server-side tools, OAuth and bearer credentials, custom headers, proxies, model discovery, automatic retry, provider diagnostics, and non-streaming responses are unsupported. Unsupported response content blocks fail closed. The adapter has no configuration for cache controls or thinking, and hook-provided thinking is rejected. HTTP errors expose status only; provider response bodies are not reported. This independent adapter can drift from the ordinary provider and upstream pi.

The adapter consumes durable tool-result status and model image profiles. A host
can call `set_model_descriptor(model, descriptor)` with an upstream-shaped
`input` array; this overrides the bundled profile (if any), and must be
reapplied after reopening. For a known text-only profile the runtime projects
images to upstream placeholder text on a request copy; stored blocks remain
unchanged. Unknown profiles retain the image-capable behavior. The adapter
checks the projected body after provider hooks. Metadata without a provable
occurrence binding fails closed; missing legacy metadata remains unknown.

The Messages API receives `is_error` for known outcomes. As in upstream,
images in an error result are replaced by text in the result and attached after
all tool results in that user turn; successful images stay in the result.
Unknown legacy status leaves `is_error` absent.
