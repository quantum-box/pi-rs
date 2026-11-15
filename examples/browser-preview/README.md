# Browser SDK model demo

This experimental demo offers a chat UI built with AI Elements and runs the pi-rs `AgentSession` as WebAssembly and a Qwen3
model in a browser Web Worker using WebGPU. Choose 0.6B (Transformers.js) or
1.7B (WebLLM). The model can request the side-effect-free `add_numbers` tool;
pi executes the call in WASM and returns the result to the model. The UI shows
the call and its result. Model output is displayed after generation completes.

From the repository root, install Rust 1.89, its `wasm32-unknown-unknown`
target, Node.js 22+, and wasm-bindgen-cli 0.2.130, then run:

```sh
bash examples/browser-preview/build.sh
python3 -m http.server 8768 --bind 127.0.0.1 --directory examples/browser-preview/dist
```

Open http://127.0.0.1:8768/ in a WebGPU-capable browser. On first use the demo
loads JavaScript runtimes from jsDelivr and model files from Hugging Face or
WebLLM's model library; downloads can exceed 1 GB. Browser caching, storage
quotas, GPU support, memory requirements, and speed vary. Those third-party
scripts execute in the model worker and can access the conversation passed to
it. Do not use sensitive data unless you trust those dependencies. Subsequent
inference runs in the browser. This is an experiment, not a general-purpose
coding agent: its only built-in demo tool adds two numbers; filesystem and
shell access are not provided.

The preview also contains optional Tachyon Model Gateway and localhost Ollama
adapters. Tachyon requires the user's token and tenant access; the Ollama path
may require browser and server origin permissions. Credentials are kept in page
memory and are not included in the session journal.

## OpenAI API

Choose OpenAI in the connection settings, enter your existing API key in the
password field, and select a model (default `gpt-6.1-terra`). Requests go directly
from the browser to `https://api.openai.com/v1/responses`; API charges
apply. The key stays in page memory and is cleared on navigation or reload.
Conversation history is stored in browser local storage, without the key.
The OpenAI session includes the `add_numbers` demonstration tool. Ask the
agent to add two numbers using the tool to inspect its call and result. The
chat does not provide filesystem or shell tools.

The UI uses React, Vite, Tailwind and installed AI Elements components. To
rebuild the UI after editing it, run `npm ci && npm run build` in this directory.
`build.sh` builds both the UI and the browser SDK from the repository root.
