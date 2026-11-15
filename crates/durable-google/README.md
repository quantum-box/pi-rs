# Durable Google Gemini adapter

This standalone MIT-licensed crate is opt-in. Add it as a path dependency, construct `GoogleAdapter::new(base_url, api_key)`, install it with `Harness::install_provider_adapter(Arc::new(adapter))`, and select its `google-generative-ai/v1` identity for a conversation. Install the same identity when reopening it. No provider is registered automatically. Run the local-only tests with `cargo test --manifest-path crates/durable-google/Cargo.toml`.

The request encoder and SSE decoder are adapted from `crates/google` under the repository license. They support text, image inputs, function calls and results, thought text, usage, and signed replay parts scoped to the Google model. A `MAX_TOKENS` response is marked `length` and its partial calls are discarded. Missing finish reasons, blocked prompts, stream errors, malformed chunks, partial-argument calls, duplicate call IDs and candidates after a finish reason fail rather than complete. Unlike the local Google provider, partial-argument calls are rejected instead of being interpreted as empty calls. The adapter never performs model discovery or calls a real endpoint during tests.

Hooks may replace conversation contents and system instructions. Validation checks supported conversation part shapes and restricts top-level fields, preserves the original tool declarations and tool configuration, fixes candidate count to one, and bounds output tokens and request size. The model is selected from the original request and is not part of the hook body. Dispatch validates again and sends that exact JSON value. Trusted hooks and host-side budget and authorization policies remain necessary: validation does not establish that modified conversation content is safe.

The API key stays in a sensitive HTTP header and is absent from prepared bodies and error messages. Base URLs must use HTTPS or numeric loopback HTTP and cannot contain credentials, query, or fragment. The host must trust its configured HTTPS endpoint. Redirects and proxies are disabled, responses are bounded to 4 MiB, and connect and read idle timeouts apply. Cancellation drops the request or stream; remote execution may already have occurred.

Unlike the full provider, this adapter does not support catalog lookup, model limit discovery, custom provider identities, custom headers, proxies, progress observers, or a thinking configuration API. Hook-supplied sampling, thinking, safety settings and other extra generation options are rejected. It supports only streaming GenerateContent with API-key authentication, not OAuth, Vertex AI, Live API, hosted tools or other Gemini APIs. Usage is limited to the counters handled by the existing provider. Independent copied protocol code may drift from pi-rs and upstream pi.

By default, durable tool-result blocks follow upstream pi's
[Google conversion at c3cc916](https://github.com/earendil-works/pi/blob/c3cc91632d0a1950fb122605b190233f2aa0006e/packages/ai/src/api/google-shared.ts):
text blocks are joined with newlines, and images are attached to the function
response for Gemini 3+ or unrecognized model IDs, or placed in an immediately
following user message for older Gemini models. Whitespace-only text becomes
`(see attached image)` when images exist, otherwise empty text. This loses
text/image interleaving only in the provider request; persisted blocks remain
unchanged. Hosts may call `set_model_descriptor(model, &json!({"input":
["text", "image"]}))` before installing the adapter. A descriptor whose input
contains only `text` omits tool-result images as upstream does; unknown models
retain the historical image-capable default. Descriptors are host configuration,
not persisted, and must be supplied again on reopen. The durable runtime passes
committed tool error status to this adapter, which emits `error` rather than
`output`. Legacy results without metadata use `output`; ambiguous reused call
IDs without provable occurrence bindings fail closed. New text-only results are
also stored as durable blocks to bind their status, so the existing tool-block
compaction and hook opt-in requirements apply. Legacy status without an occurrence binding fails closed rather than being
guessed.

For explicit order preservation, construct
`GoogleAdapter::new_ordered_tool_blocks(base_url, api_key)` and select its
`google-generative-ai-ordered-tool-blocks/v1` identity. Its ordered array uses
[Google image references](https://ai.google.dev/gemini-api/docs/generate-content/function-calling#multimodal)
instead of the upstream representation. It requires recognized Gemini 3+
models for images, supports PNG, JPEG and WebP, and rejects empty result arrays.
The separate identity prevents silently changing the mode on reopen. Fake HTTP
tests verify the body but not real provider acceptance or model interpretation.

Provider hooks cannot modify result status, associated messages or tool blocks;
the adapter validates the complete projected contents before dispatch. The
status-aware route also covers plain text tool results. Metadata is only passed
to adapters that explicitly opt in to `ProviderResultRequest`; other adapters
retain their earlier behavior.
