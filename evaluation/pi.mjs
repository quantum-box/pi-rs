import { readFile, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const [workspace, agentDir, systemFile, endpoint, modelId, maxOutputTokens, taskJson] = process.argv.slice(2);
const task = JSON.parse(taskJson);
const systemPrompt = await readFile(systemFile, "utf8");
const modelsPath = resolve(agentDir, "models.json");
await writeFile(modelsPath, JSON.stringify({
  providers: {
    local: {
      baseUrl: `${endpoint}/v1`,
      api: "openai-completions",
      apiKey: "local-ollama",
      compat: {
        supportsStore: false, supportsDeveloperRole: false, supportsReasoningEffort: false,
        maxTokensField: "max_tokens", requiresToolResultName: false, supportsStrictMode: false,
      },
      models: [{ id: modelId, contextWindow: 4096, maxTokens: Number(maxOutputTokens), reasoning: false }],
    },
  },
}));
const modelRuntime = await ModelRuntime.create({ modelsPath, authPath: resolve(agentDir, "auth.json") });
const [model] = await modelRuntime.getAvailable("local");
if (!model) throw new Error("local model unavailable");

const settingsManager = SettingsManager.inMemory({ compaction: { keepRecentTokens: 100 } });
settingsManager.setCompactionEnabled(false);
settingsManager.setRetryEnabled(false);
const resourceLoader = new DefaultResourceLoader({
  cwd: workspace, agentDir, settingsManager, systemPrompt,
  extensionFactories: [(pi) => pi.on("before_agent_start", async () => ({ systemPrompt }))],
  noExtensions: true, noSkills: true, noPromptTemplates: true, noContextFiles: true, noThemes: true,
});
await resourceLoader.reload();

const exec = promisify(execFile);
const toolResults = [];
function tool(name, description, arg, operation) {
  return {
    name, label: name, description,
    parameters: Type.Object({ [arg]: Type.String({ minLength: 1 }) }, { additionalProperties: false }),
    executionMode: "sequential",
    async execute(callId, args, signal) {
      const content = await operation(args[arg], signal, callId);
      toolResults.push({ name, content });
      return { content: [{ type: "text", text: content }], details: {} };
    },
  };
}
function fixture(name) {
  if (!["stock.txt", "manifest.txt", "buggy.js"].includes(name)) throw new Error("expected an evaluation fixture");
  return resolve(workspace, name);
}
const customTools = [
  tool("count_lines", "Count newline characters in a workspace file using wc -l.", "path", async (name, signal) => {
    const { stdout } = await exec("/usr/bin/wc", ["-l", fixture(name)], { env: {}, signal });
    return stdout.trim().split(/\s+/)[0];
  }),
  tool("lookup_order", "Retrieve an order by its exact order ID.", "id", async (id, signal) => {
    const result = await fetch(`${endpoint}/orders/${encodeURIComponent(id)}`, { signal });
    if (!result.ok) throw new Error(`orders returned HTTP ${result.status}`);
    return JSON.stringify(await result.json());
  }),
  tool("read_text", "Read the complete UTF-8 contents of a workspace text file.", "path",
    (name, signal) => readFile(fixture(name), { encoding: "utf8", signal })),
];
if (task.mutating) {
  const mutation = (name, description, arg) => tool(name, description, arg, async (value, signal, id) => {
    const response = await fetch(`${endpoint}/fixture/${name}`, {
      method: "POST", headers: { "content-type": "application/json", "idempotency-key": id },
      body: JSON.stringify({ [arg]: value }), signal,
    });
    if (!response.ok) throw new Error(`fixture returned HTTP ${response.status}`);
    return response.text();
  });
  customTools.splice(2, 0, mutation("patch_fixture",
    "Replace the return expression of the discount function in the synthetic buggy.js fixture.", "expression"));
  customTools.push(mutation("submit_mock_order", "Submit an order to the synthetic evaluation store only.", "id"));
}
const { session } = await createAgentSession({
  cwd: workspace, agentDir, model, modelRuntime, resourceLoader, settingsManager,
  sessionManager: SessionManager.create(workspace, agentDir),
  tools: customTools.map(({ name }) => name), customTools, thinkingLevel: "off",
});
try {
  if (task.seed) {
    await session.prompt(task.seed);
    const anchor = session.sessionManager.getLeafId();
    await session.prompt(task.bridge);
    if (task.scenario === "compact") {
      await session.compact("Preserve the original release verification code verbatim.");
    } else if (task.scenario === "review") {
      await session.navigateTree(anchor, { summarize: false });
    }
  }
  await session.prompt(task.prompt);
  const last = session.messages.findLast((message) => message.role === "assistant");
  const error = last?.stopReason === "error" ? last.errorMessage ?? "provider error" : undefined;
  console.log(JSON.stringify({ answer: session.getLastAssistantText(), toolResults, error }));
} finally {
  session.dispose();
}
