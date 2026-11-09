import { readFile, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const [workspace, agentDir, systemFile, endpoint, modelId, maxOutputTokens, prompt] = process.argv.slice(2);
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

const settingsManager = SettingsManager.inMemory();
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
    async execute(_callId, args, signal) {
      const content = await operation(args[arg], signal);
      toolResults.push({ name, content });
      return { content: [{ type: "text", text: content }], details: {} };
    },
  };
}
function fixture(name) {
  if (!["stock.txt", "manifest.txt"].includes(name)) throw new Error("expected an evaluation fixture");
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
const { session } = await createAgentSession({
  cwd: workspace, agentDir, model, modelRuntime, resourceLoader, settingsManager,
  sessionManager: SessionManager.create(workspace, agentDir),
  tools: customTools.map(({ name }) => name), customTools, thinkingLevel: "off",
});
try {
  await session.prompt(prompt);
  console.log(JSON.stringify({ answer: session.getLastAssistantText(), toolResults }));
} finally {
  session.dispose();
}
