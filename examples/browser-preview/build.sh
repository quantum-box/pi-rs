#!/usr/bin/env bash
set -euo pipefail
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.89.0
  export PATH="$HOME/.cargo/bin:$PATH"
fi
rustup toolchain install 1.89.0 --profile minimal
rustup target add --toolchain 1.89.0 wasm32-unknown-unknown
if ! command -v wasm-bindgen >/dev/null 2>&1 || [ "$(wasm-bindgen --version)" != 'wasm-bindgen 0.2.130' ]; then
  cargo +1.89.0 install wasm-bindgen-cli --version 0.2.130 --locked
fi
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' cargo +1.89.0 build --locked --manifest-path crates/browser-sdk/Cargo.toml --target wasm32-unknown-unknown
mkdir -p examples/browser-preview/dist/pkg
cp examples/browser-preview/index.html examples/browser-preview/mock-response.json examples/browser-preview/dist/
wasm-bindgen --target web --out-dir examples/browser-preview/dist/pkg crates/browser-sdk/target/wasm32-unknown-unknown/debug/pi_rs_browser_sdk.wasm
