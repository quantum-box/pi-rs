import { execFile } from "node:child_process";
import { createServer } from "node:http";
import { chmod, copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { promisify } from "node:util";
import assert from "node:assert/strict";

const exec = promisify(execFile);
const project = process.cwd();
const root = await mkdtemp(join(tmpdir(), "pi-rs-extension-cycle-"));
const workspace = join(root, "workspace");
await mkdir(workspace);
const history = join(root, "session.jsonl");
const staged = join(root, "candidate");
const previous = join(root, "previous");
const target = resolve(process.env.CARGO_TARGET_DIR ?? "target");
const compiled = join(target, "release", "pi-rs");
const effects = new Set();
const metrics = {};
const evidence = {};

async function measure(name, operation) {
  const start = performance.now();
  try {
    return await operation();
  } finally {
    metrics[name] = Math.round(performance.now() - start);
  }
}

function events(result) {
  return result.stdout.trim().split("\n").filter(Boolean).map(JSON.parse);
}

const server = createServer(async (request, response) => {
  try {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks));
    if (request.url === "/fixture/submit_mock_order") {
      if (body.id !== "QO-19" || !request.headers["idempotency-key"]) {
        response.writeHead(422).end();
        return;
      }
      effects.add(request.headers["idempotency-key"]);
      response.end("submitted QO-19");
      return;
    }
    if (request.url !== "/v1/chat/completions") {
      response.writeHead(404).end();
      return;
    }
    const user = body.messages.findLast((message) => message.role === "user");
    const afterTool = body.messages.at(-1).role === "tool";
    const calls = user.content === "Submit now." && !afterTool &&
      body.tools?.some((tool) => tool.function.name === "submit_mock_order");
    const remembered = body.messages.some((message) => message.role === "user" && message.content.includes("RZ-73"));
    const delta = calls
      ? { tool_calls: [{ index: 0, id: "fixture-call", function: { name: "submit_mock_order", arguments: '{"id":"QO-19"}' } }] }
      : { content: afterTool ? "SUBMITTED" : remembered ? "RZ-73" : "MISSING" };
    const frames = [
      { choices: [{ index: 0, delta, finish_reason: null }] },
      { choices: [{ index: 0, delta: {}, finish_reason: calls ? "tool_calls" : "stop" }] },
      { choices: [], usage: { prompt_tokens: 40, completion_tokens: 10 } },
    ];
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(frames.map((frame) => `data: ${JSON.stringify(frame)}\n\n`).join("") + "data: [DONE]\n\n");
  } catch {
    response.writeHead(500).end();
  }
});
await new Promise((done) => server.listen(0, "127.0.0.1", done));
const endpoint = `http://127.0.0.1:${server.address().port}`;
const build = (...flags) => exec("cargo", ["build", "--locked", "--release", "-p", "pi-rs-workbench", ...flags], {
  cwd: project, env: { ...process.env, CARGO_TARGET_DIR: target }, timeout: 120_000,
});
const cli = (binary, session, args) => exec(binary, [
  "--session", session, "--workspace", workspace, "--endpoint", `${endpoint}/v1/chat/completions`, ...args,
], { timeout: 30_000 });
const answer = (result) => events(result).find((event) => event.type === "answer")?.content;

try {
  await measure("previous_build_ms", () => build());
  await copyFile(compiled, previous);
  await chmod(previous, 0o700);
  assert.equal(answer(await cli(previous, history, ["run", "Keep the original verification code RZ-73."])), "RZ-73");
  await measure("failed_build_ms", () => assert.rejects(build("--features", "nonexistent-evaluation-extension"), /nonexistent-evaluation-extension/));
  assert.equal(answer(await cli(previous, history, ["resume"])), "RZ-73");
  const candidateStart = performance.now();
  await measure("candidate_build_ms", () => build("--features", "evaluation-fixtures"));
  await copyFile(compiled, staged);
  await chmod(staged, 0o700);

  await measure("candidate_contract_check_ms", async () => {
    const check = join(root, "candidate-check.jsonl");
    await assert.rejects(cli(staged, check, ["--fixtures", "http://example.org/fixture/", "run", "Submit now."]), /loopback/);
    assert.equal(answer(await cli(staged, check, ["--fixtures", `${endpoint}/fixture/`, "run", "Submit now."])), "SUBMITTED");
    assert.equal(effects.size, 1);
  });
  effects.clear();

  await measure("activate_and_continue_ms", async () => {
    assert.equal(answer(await cli(staged, history, ["--fixtures", `${endpoint}/fixture/`, "run", "Submit now."])), "SUBMITTED");
    assert.equal(effects.size, 1);
    assert.equal(answer(await cli(staged, history, ["--fixtures", `${endpoint}/fixture/`, "run", "What verification code were we using before?"])), "RZ-73");
  });
  metrics.stage_to_use_ms = Math.round(performance.now() - candidateStart);
  const before = (await readFile(history, "utf8")).trimEnd().split("\n").map(JSON.parse);
  const state = before.findLast((record) => record.event.states?.["evaluation-fixtures"]);
  assert.equal(state.event.states["evaluation-fixtures"].data, 1);

  const incompatible = join(root, "incompatible.jsonl");
  const last = before.at(-1);
  const bad = { version: last.version, id: last.id + 1, parent: last.id,
    event: { type: "extension_states", states: { "evaluation-fixtures": { version: 99, data: 1 } } } };
  await writeFile(incompatible, before.map((record) => JSON.stringify(record)).join("\n") + "\n" + JSON.stringify(bad) + "\n");
  await assert.rejects(cli(staged, incompatible, ["--fixtures", `${endpoint}/fixture/`, "run", "Should be rejected."]), /unsupported fixture state/);

  await measure("rollback_and_continue_ms", async () => {
    assert.equal(answer(await cli(previous, history, ["run", "Verify the original code after rollback."])), "RZ-73");
  });
  const after = (await readFile(history, "utf8")).trimEnd().split("\n").map(JSON.parse);
  assert.deepEqual(after.slice(0, before.length), before);
  assert.equal(effects.size, 1);
  evidence.failed_build_left_previous_usable = true;
  evidence.incompatible_state_rejected = true;
  evidence.old_binary_preserved_extension_state = true;
  evidence.operation_count_after_rollback = effects.size;
  evidence.resumed_original_code = "RZ-73";
  console.log(JSON.stringify({ ...metrics, ...evidence }, null, 2));
} finally {
  await new Promise((done) => server.close(done));
  await rm(root, { recursive: true, force: true });
}
