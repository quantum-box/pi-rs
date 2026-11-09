import { readFile } from "node:fs/promises";

const inputPath = process.argv[2];
if (!inputPath) throw new Error("Pass the synthetic cases exported by the AIP reference acceptance test.");
const input = JSON.parse(await readFile(inputPath, "utf8"));
const server = process.env.OLLAMA_URL ?? "http://127.0.0.1:11434";
const model = "qwen2.5:7b";
const expectedDigest = "845dbda0ea48ed749caafd9e6037047aa19acfcfd82e704d7ca97d631a0b697e";
const options = { seed: 123, temperature: 0, num_predict: 64, num_ctx: 4096 };

async function api(path, body) {
  const response = await fetch(new URL(path, server), {
    ...(body && { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!response.ok) throw new Error(`Ollama ${path} returned ${response.status}`);
  return response.json();
}

const { version } = await api("/api/version");
const { models } = await api("/api/tags");
const digest = models.find((entry) => entry.model === model)?.digest;
if (digest !== expectedDigest) throw new Error("Install the pinned model used by this comparison.");

const trials = [];
for (let iteration = 1; iteration <= 3; iteration++) {
  const modes = iteration % 2 === 1 ? ["disabled", "enabled"] : ["enabled", "disabled"];
  for (const mode of modes) {
    const result = await api("/api/chat", { model, messages: input[mode], stream: false, options });
    const answer = result.message.content.trim();
    const { prompt_eval_count: inputTokens, eval_count: outputTokens } = result;
    if (result.done_reason !== "stop" || !Number.isInteger(inputTokens) || !Number.isInteger(outputTokens)) {
      throw new Error(`Incomplete comparison response for ${mode} trial ${iteration}`);
    }
    trials.push({ iteration, mode, answer, passed: answer === input.answer, inputTokens, outputTokens });
  }
}

console.log(JSON.stringify({ version, model, digest, options, expectedAnswer: input.answer, trials }, null, 2));
