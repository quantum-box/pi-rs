import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir, arch, release } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { promisify } from "node:util";

const exec = promisify(execFile);
const root = await mkdtemp(join(tmpdir(), "pi-rs-comparison-"));
const workspace = join(root, "workspace");
const systemFile = join(root, "system.txt");
const ollama = process.env.OLLAMA_URL ?? "http://127.0.0.1:11434";
const model = process.env.EVAL_MODEL ?? "qwen2.5:7b";
const expectedDigest = process.env.EVAL_MODEL_DIGEST ?? (model === "qwen2.5:7b"
  ? "845dbda0ea48ed749caafd9e6037047aa19acfcfd82e704d7ca97d631a0b697e"
  : undefined);
const rust = resolve(process.env.PI_RS_BIN ?? "target/release/pi-rs");
const trials = Number(process.env.EVAL_TRIALS ?? 3);
const maxRequests = 6;
const maxTokens = 512;
const timeout = 60_000;
const system = "You are a workspace assistant. Use the requested tools and return only the requested answer.";
const tasks = [
  { name: "read", prompt: "Use read_text on stock.txt. Reply with only the code in that file.", answer: "XR-314", tools: ["read_text"] },
  { name: "count", prompt: "Use count_lines on manifest.txt. Reply with only the number returned by the tool.", answer: "7", tools: ["count_lines"] },
  { name: "order", prompt: "Use lookup_order for order QO-19 and count_lines on manifest.txt. Reply only as carrier|eta|count using the tool results.", answer: "Sable|Friday|7", tools: ["count_lines", "lookup_order"] },
];

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  }
  return value;
}
function digest(value) {
  return createHash("sha256").update(JSON.stringify(stable(value))).digest("hex");
}
function contextSignature(body) {
  // Tool call IDs are generated anew. Replace them with their ordinal while preserving linkage.
  const ids = new Map();
  const messages = body.messages.map((message) => {
    const normalized = { role: message.role, content: message.content ?? "" };
    if (Array.isArray(normalized.content) && normalized.content.every((part) => part.type === "text")) {
      normalized.content = normalized.content.map((part) => part.text).join("");
    }
    if (message.tool_calls) {
      normalized.tool_calls = message.tool_calls.map((call) => {
        ids.set(call.id, ids.size);
        return { ordinal: ids.get(call.id), name: call.function.name, args: JSON.parse(call.function.arguments) };
      });
    }
    if (message.tool_call_id) normalized.tool_call = ids.get(message.tool_call_id);
    return normalized;
  });
  return {
    messages: digest(messages), tools: digest(body.tools),
    system: digest(messages.filter((message) => message.role === "system")),
    user: digest(messages.filter((message) => message.role === "user")),
  };
}

let active;
const server = createServer(async (request, response) => {
  try {
    if (request.method === "GET" && request.url === "/orders/QO-19") {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ carrier: "Sable", eta: "Friday", order: "QO-19" }));
      return;
    }
    if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
      response.writeHead(404).end();
      return;
    }
    const trial = active;
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks));
    if (!trial || trial.requests.length >= maxRequests) {
      response.writeHead(429).end();
      return;
    }
    if (body.model !== model || body.stream !== true) throw new Error("unexpected model or stream mode");
    const requestInfo = contextSignature(body);
    trial.requests.push(requestInfo);
    const upstream = await fetch(`${ollama}/v1/chat/completions`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ...body, max_tokens: maxTokens, stream_options: { include_usage: true },
        temperature: 0, seed: 123,
      }),
      signal: AbortSignal.timeout(timeout),
    });
    response.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" });
    const decoder = new TextDecoder();
    let pending = "";
    for await (const chunk of upstream.body) {
      response.write(chunk);
      pending += decoder.decode(chunk, { stream: true });
      const lines = pending.split("\n");
      pending = lines.pop();
      for (const line of lines) {
        if (!line.startsWith("data: ") || line.trimEnd() === "data: [DONE]") continue;
        const payload = JSON.parse(line.slice(6));
        if (payload.usage) {
          requestInfo.input_tokens = payload.usage.prompt_tokens;
          requestInfo.output_tokens = payload.usage.completion_tokens;
        }
      }
    }
    response.end();
  } catch (error) {
    if (!response.headersSent) response.writeHead(502);
    response.end(String(error.message));
  }
});
await new Promise((done) => server.listen(0, "127.0.0.1", done));
const endpoint = `http://127.0.0.1:${server.address().port}`;

async function run(harness, task, trial) {
  const agentDir = join(root, `${harness}-${task.name}-${trial}`);
  await mkdir(agentDir, { mode: 0o700 });
  const commands = {
    rust: [rust, ["--session", join(agentDir, "history.jsonl"), "--workspace", workspace,
      "--endpoint", `${endpoint}/v1/chat/completions`, "--model", model,
      "--system-prompt", systemFile, "--orders", `${endpoint}/orders`, "--allow-commands",
      "--max-requests", String(maxRequests), "--max-output-tokens", String(maxTokens),
      "--output-budget", String(maxRequests * maxTokens), "run", task.prompt]],
    pi: [process.execPath, [fileURLToPath(new URL("./pi.mjs", import.meta.url)), workspace, agentDir, systemFile,
      endpoint, model, String(maxTokens), task.prompt]],
  };
  const [file, args] = commands[harness];
  active = { requests: [] };
  let answer, tools, error;
  const start = performance.now();
  try {
    const { stdout } = await exec(file, args, {
      cwd: workspace, timeout, maxBuffer: 1024 * 1024,
      env: { PATH: process.env.PATH, HOME: agentDir, PI_CODING_AGENT_DIR: agentDir, PI_TELEMETRY: "off" },
    });
    const events = stdout.trim().split("\n").filter((line) => line.startsWith("{")).map(JSON.parse);
    if (harness === "rust") {
      answer = events.find((event) => event.type === "answer")?.content;
      const succeeded = new Set(events.filter((event) => event.event === "tool_finished" && event.status === "succeeded")
        .map((event) => event.operation_id));
      tools = events.filter((event) => event.event === "assistant").flatMap((event) => event.tools)
        .filter((tool) => succeeded.has(tool.operation_id)).map((tool) => tool.name);
    } else {
      const result = events.at(-1);
      answer = result?.answer;
      tools = result?.toolResults?.map((tool) => tool.name);
    }
  } catch (failure) {
    error = failure.stderr?.trim() || failure.message;
  }
  const requests = active.requests;
  active = undefined;
  return {
    trial, harness, task: task.name,
    success: answer?.trim() === task.answer && task.tools.every((name) => tools?.includes(name)),
    elapsed_ms: Math.round(performance.now() - start),
    input_tokens: requests.length && requests.every((info) => info.input_tokens != null) ? requests.reduce((n, info) => n + info.input_tokens, 0) : null,
    output_tokens: requests.length && requests.every((info) => info.output_tokens != null) ? requests.reduce((n, info) => n + info.output_tokens, 0) : null,
    model_api_cost_usd: 0, human_interventions: 0,
    answer: answer ?? null, tools: tools ?? [], requests, ...(error ? { error } : {}),
  };
}

try {
  if (!Number.isSafeInteger(trials) || trials < 1) throw new Error("EVAL_TRIALS must be a positive integer");
  await mkdir(workspace);
  await Promise.all([
    writeFile(join(workspace, "stock.txt"), "XR-314"),
    writeFile(join(workspace, "manifest.txt"), "a\nb\nc\nd\ne\nf\ng\n"),
    writeFile(systemFile, system),
  ]);
  const version = await (await fetch(`${ollama}/api/version`)).json();
  const tags = await (await fetch(`${ollama}/api/tags`)).json();
  const tag = tags.models.find((tag) => tag.name === model);
  if (!tag) throw new Error(`pull model ${model} before running the comparison`);
  if (!expectedDigest || tag.digest !== expectedDigest) throw new Error("unexpected model digest");
  // Warm the model once. Trial timing includes harness startup and session persistence.
  const warm = await fetch(`${ollama}/api/chat`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ model, messages: [{ role: "user", content: "OK" }], stream: false, think: false, options: { num_predict: 1 } }),
  });
  if (!warm.ok) throw new Error("failed to warm model");
  await warm.text();
  const rows = [];
  for (let trial = 1; trial <= trials; trial++) {
    for (const task of tasks) {
      const order = (trial + tasks.indexOf(task)) % 2 ? ["rust", "pi"] : ["pi", "rust"];
      for (const harness of order) {
        const row = await run(harness, task, trial);
        rows.push(row);
        console.error(`${trial} ${task.name} ${harness}: ${row.success ? "pass" : "fail"} (${row.elapsed_ms} ms)${row.error ? ` ${row.error}` : ""}`);
      }
    }
  }
  const parity = rows.filter((row) => row.harness === "rust").map((row) => {
    const pi = rows.find((other) => other.trial === row.trial && other.task === row.task && other.harness === "pi");
    const [r, p] = [row.requests[0], pi.requests[0]];
    return {
      trial: row.trial, task: row.task,
      initial_prompt_match: !!r && !!p && r.messages === p.messages,
      initial_tools_match: !!r && !!p && r.tools === p.tools,
      all_contexts_match: row.requests.length > 0 && row.requests.length === pi.requests.length
        && row.requests.every((request, index) => request.messages === pi.requests[index].messages && request.tools === pi.requests[index].tools),
    };
  });
  console.log(JSON.stringify({
    pi_version: "1.5.0", pi_commit: "f9a26365568127461c47a07a37dcb42c140a3ac5",
    ollama: version.version, model, model_digest: tag.digest, arch: arch(), os_release: release(),
    max_requests: maxRequests, max_output_tokens_per_request: maxTokens, timeout_ms: timeout, trials, rows, parity,
  }, null, 2));
  if (parity.some((pair) => !pair.initial_prompt_match || !pair.initial_tools_match)) process.exitCode = 1;
} finally {
  await new Promise((done) => server.close(done));
  await rm(root, { recursive: true, force: true });
}
