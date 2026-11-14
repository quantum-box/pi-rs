import test from 'node:test';
import assert from 'node:assert/strict';
import { createGatewayFetch } from './tachyon-gateway.mjs';

const origin = 'https://preview.example';
function request(messages = [{ role: 'user', content: 'hello' }], signal) {
  return new Request(`${origin}/demo-gateway`, { method: 'POST', body: JSON.stringify({ model: 'other-model', max_output_tokens: 4096, messages }), signal });
}

test('forwards text to the fixed Luna model and converts the gateway reply', async () => {
  let called;
  const deltas = [];
  const gateway = createGatewayFetch({ origin, getToken: () => ' token ', onError: assert.fail, onDelta: delta => deltas.push(delta), fetchImpl: async (url, options) => {
    called = { url, options };
    const body = 'data: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"from Luna\"}}]}\n\ndata: [DONE]\n\n';
    return new Response(new ReadableStream({ start(controller) { const bytes = new TextEncoder().encode(body); for (let i = 0; i < bytes.length; i += 7) controller.enqueue(bytes.slice(i, i + 7)); controller.close(); } }));
  }});
  const response = await gateway(request([{ role: 'assistant', content: 'earlier', calls: [] }, { role: 'user', content: 'hello' }]));
  assert.deepEqual(await response.json(), { text: 'hello from Luna' });
  assert.deepEqual(deltas, ['hello ', 'from Luna']);
  assert.equal(called.url, 'https://api.n1.tachy.one/v1/ai/chat/completions');
  assert.equal(called.options.headers.authorization, 'Bearer token');
  assert.equal(called.options.headers['x-operator-id'], 'tn_01kxz0ytmhnab5vh53011cwctj');
  assert.equal(called.options.credentials, 'omit');
  assert.equal(called.options.redirect, 'error');
  assert.deepEqual(JSON.parse(called.options.body), { model: 'openai/gpt-6-luna', messages: [{role:'assistant',content:'earlier'},{role:'user',content:'hello'}], stream: true });
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


test('emits text before the response completes and rejects an incomplete stream', async () => {
  let stream, received = '';
  const gateway = createGatewayFetch({ origin, getToken: () => 'token', onError: () => {}, onDelta: delta => { received += delta; }, fetchImpl: async () => new Response(new ReadableStream({ start(controller) { stream = controller; } })) });
  const pending = gateway(request());
  await new Promise(resolve => setTimeout(resolve, 0));
  stream.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"途中"}}]}\n\n'));
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.equal(received, '途中');
  stream.close();
  await assert.rejects(pending, /before completion/);
});
