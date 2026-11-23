// Run against a checked-out Pi v1.5.0 with its runtime dependencies installed:
// PI_RS_UPSTREAM_PI_ROOT=/path/to/pi node --experimental-strip-types crates/workbench/tests/radius_pending_pi_compat.mjs
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = process.env.PI_RS_UPSTREAM_PI_ROOT;
if (!root) throw new Error("PI_RS_UPSTREAM_PI_ROOT must point to the pinned Pi checkout");
const revision = execFileSync("git", ["-C", root, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
assert.equal(revision, "f9a26365568127461c47a07a37dcb42c140a3ac5", "expected pinned Pi v1.5.0");
const load = (path) => import(pathToFileURL(resolve(root, path)).href);
const { AuthStorage, ReadOnlyAuthStorage } = await load("packages/coding-agent/src/core/auth-storage.ts");
const { resolveProviderAuth, refreshStoredOAuthCredential } = await load("packages/ai/src/auth/resolve.ts");
const { envApiKeyAuth } = await load("packages/ai/src/auth/helpers.ts");
const dir = mkdtempSync(join(tmpdir(), "pi-radius-pending-"));
try {
    const file = join(dir, "auth.json");
    const original = { type: "oauth", access: "old", refresh: "old-refresh", expires: 0 };
    writeFileSync(file, JSON.stringify({ radius: original, other: { type: "api_key", key: "keep" } }));
    const store = AuthStorage.create(file);
    assert.equal((await store.read("radius")).type, "oauth");
    // This shape must match radius_auth::tests::pending_entry_matches_upstream_interop_fixture.
    const pending = { type: "api_key", _piRsRefreshPending: { attempt: "fixture", gateway: "https://radius.pi.dev/" } };
    const replacement = join(dir, "replacement.json");
    writeFileSync(replacement, JSON.stringify({ radius: pending, other: { type: "api_key", key: "keep" } }));
    renameSync(replacement, file);
    let refreshCalls = 0;
    const oauth = { name: "Radius", refresh: async (credential) => { refreshCalls++; return credential; }, toAuth: async (credential) => ({ apiKey: credential.access }) };
    const result = await refreshStoredOAuthCredential(store, "radius", oauth, () => true, new AbortController().signal);
    assert.equal(result, undefined);
    assert.equal(refreshCalls, 0);
    const readonly = new ReadOnlyAuthStorage(file);
    assert.equal((await readonly.read("radius")).type, "api_key");
    const provider = { id: "radius", auth: { apiKey: envApiKeyAuth("Radius", ["RADIUS_API_KEY"]), oauth } };
    const context = { env: async () => undefined, fileExists: async () => false };
    assert.equal(await resolveProviderAuth(provider, readonly, context), undefined);
    assert.equal((await readonly.read("other")).key, "keep");
    assert.equal(refreshCalls, 0);
    console.log("Pinned Pi accepts the pending entry without refreshing the old token");
} finally {
    rmSync(dir, { recursive: true, force: true });
}
