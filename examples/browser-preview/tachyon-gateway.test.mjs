import test from 'node:test';
import assert from 'node:assert/strict';
import { createGatewayFetch } from './tachyon-gateway.mjs';

const origin = 'https://preview.example';
function request(messages = [{ role: 'user', content: 'hello' }], signal) {
  return new Request(`${origin}/demo-gateway`, { method: 'POST', body: JSON.stringify({ model: 'other-model', max_output_tokens: 4096, messages }), signal });
}

test('forwards text to the fixed Luna model and converts the gateway reply', async () => {
  let called;
  const gateway = createGatewayFetch({ origin, getToken: () => ' token ', onError: assert.fail, fetchImpl: async (url, options) => {
    called = { url, options };
    return Response.json({ choices: [{ message: { content: 'hello from Luna' } }] });
  }});
  const response = await gateway(request([{ role: 'assistant', content: 'earlier', calls: [] }, { role: 'user', content: 'hello' }]));
  assert.deepEqual(await response.json(), { text: 'hello from Luna' });
  assert.equal(called.url, 'https://api.n1.tachy.one/v1/ai/chat/completions');
  assert.equal(called.options.headers.authorization, 'Bearer token');
  assert.equal(called.options.headers['x-operator-id'], 'tn_01kxz0ytmhnab5vh53011cwctj');
  assert.equal(called.options.credentials, 'omit');
  assert.equal(called.options.redirect, 'error');
  assert.deepEqual(JSON.parse(called.options.body), { model: 'openai/gpt-6-luna', messages: [{role:'assistant',content:'earlier'},{role:'user',content:'hello'}], stream: false, max_completion_tokens: 1024 });
});

test('reports gateway errors without exposing response content', async () => {
  let message = '';
  const gateway = createGatewayFetch({ origin, getToken: () => 'token', onError: value => { message = value; }, fetchImpl: async () => new Response('private', { status: 401 }) });
  const response = await gateway(request());
  assert.equal(response.status, 401);
  assert.match(message, /HTTP 401/);
  assert.ok(!message.includes('private'));
});

test('forwards cancellation and does not dispatch without a token', async () => {
  let errorMessage = '';
  let called = false;
  const gateway = createGatewayFetch({ origin, getToken: () => '', onError: value => { errorMessage = value; }, fetchImpl: async () => { called = true; } });
  await assert.rejects(gateway(request()));
  assert.equal(called, false);
  assert.match(errorMessage, /トークン/);
  const controller = new AbortController();
  const aborting = createGatewayFetch({ origin, getToken: () => 'token', onError: assert.fail, fetchImpl: async (_url, options) => new Promise((resolve, reject) => {
    options.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true });
  }) });
  const pending = aborting(request(undefined, controller.signal));
  await new Promise(resolve => setTimeout(resolve, 0));
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
});
