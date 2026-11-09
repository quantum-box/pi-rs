import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const source = process.argv[2] ?? new URL("./expanded-results.json", import.meta.url);
const results = JSON.parse(await readFile(source, "utf8"));
const tasks = [...new Set(results.rows.map((row) => row.task))];
const settings = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex");

function metrics(row) {
  const known = [row.input_tokens, row.output_tokens, row.cached_input_tokens].every(Number.isSafeInteger)
    && row.cached_input_tokens <= row.input_tokens;
  return {
    trial_id: `${row.harness}-${row.task}-${row.trial}`,
    ...(known ? { tokens: {
      input_uncached: row.input_tokens - row.cached_input_tokens,
      cache_read_input: row.cached_input_tokens, cache_write_input: 0,
      output: row.output_tokens, reasoning_output: 0,
    } } : {}),
    token_measurement: known ? "actual" : "missing",
    unknown_calls: known ? 0 : Math.max(1, row.requests.filter((request) => request.input_tokens == null).length),
    analysis_tokens: 0,
    cost: { reference: { basis: "reference", nanodollars: 0, price_version: "local-no-model-api-fee" } },
    wall_time_ms: row.elapsed_ms,
    quality: {
      gate: row.task === "code_fix" ? "tests" : "fixture",
      verdict: row.success ? "passed" : row.failure_kind === "execution" ? "not_verified" : "failed",
      completed: row.failure_kind !== "execution",
      rework: 0,
    },
  };
}

const requests = tasks.map((task) => {
  const parity = results.parity.filter((pair) => pair.task === task);
  if (!parity.length || !parity.every((pair) => pair.initial_prompt_match && pair.initial_tools_match)) {
    throw new Error(`unpaired initial conditions for ${task}`);
  }
  const equivalent = parity.every((pair) => pair.all_contexts_match);
  const conditions = (harness) => ({
    task,
    model: `${results.model}@${results.model_digest}`,
    settings_hash: settings([task, results.max_requests, results.max_output_tokens_per_request, results.timeout_ms, equivalent ? "matched-wire-context" : harness]),
    cache_state: "warm",
    path: "local_client",
    client: "other",
  });
  const trials = (harness) => results.rows.filter((row) => row.task === task && row.harness === harness).map(metrics);
  return {
    layer: "controlled_task",
    policy: "pol_pi_rs_evaluation",
    policy_version: 1,
    baseline_conditions: conditions("pi"),
    candidate_conditions: conditions("rust"),
    baseline_trials: trials("pi"),
    candidate_trials: trials("rust"),
    replay: { isolated_environment: true, data: "synthetic", side_effects: "mocked", billing: "evaluation_budget" },
  };
});
console.log(JSON.stringify(requests, null, 2));
