// Adapts the browser SDK's text gateway contract to Tachyon Model Gateway.
// Credentials stay in the caller's browser and are sent only to the fixed API.
const API_URL = 'https://api.n1.tachy.one/v1/ai/chat/completions';
const OPERATOR_ID = 'tn_01kxz0ytmhnab5vh53011cwctj';
const MODEL = 'openai/gpt-6-luna';
const MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

async function readStream(response, onDelta) {
  if (!response.body) throw new Error('応答ストリームがありません');
  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let buffer = '', text = '', total = 0, doneMarker = false;
  function event(block) {
    const data = block.split(/\r\n|\r|\n/).filter(line => line.startsWith('data:')).map(line => line.slice(5).replace(/^ /, '')).join('\n');
    if (!data) return;
    if (data === '[DONE]') { doneMarker = true; return; }
    const chunk = JSON.parse(data);
    if (chunk.error) throw new Error('Gateway stream error');
    const delta = chunk.choices?.[0]?.delta?.content;
    if (typeof delta === 'string' && delta) { text += delta; onDelta(delta); }
  }
  try {
    while (!doneMarker) {
      const part = await reader.read();
      if (part.done) break;
      total += part.value.byteLength;
      if (total > MAX_RESPONSE_BYTES) throw new Error('応答が大きすぎます');
      buffer += decoder.decode(part.value, { stream: true });
      let match;
      while ((match = /\r\n\r\n|\n\n|\r\r/.exec(buffer))) {
        event(buffer.slice(0, match.index));
        buffer = buffer.slice(match.index + match[0].length);
        if (doneMarker) break;
      }
    }
    if (!doneMarker) throw new Error('Gateway stream ended before completion');
    return text;
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  } finally { reader.releaseLock(); }
}

export function createGatewayFetch({ fetchImpl, getToken, onError, onDelta = () => {}, origin }) {
  return async function gatewayFetch(request, options) {
    if (!(request instanceof Request) || new URL(request.url).origin !== origin || new URL(request.url).pathname !== '/demo-gateway') {
      return fetchImpl(request, options);
    }
    const token = getToken().trim();
    if (!token) {
      onError('Tachyon アクセストークンを入力してください。');
      throw new Error('Tachyon authentication required');
    }
    const source = await request.clone().json();
    const messages = source.messages.map(message => {
      if (!['system', 'user', 'assistant'].includes(message.role) || typeof message.content !== 'string') {
        throw new Error('このプレビューはテキスト会話のみ対応しています');
      }
      return { role: message.role, content: message.content };
    });
    try {
      const response = await fetchImpl(API_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${token}`, 'x-operator-id': OPERATOR_ID },
        body: JSON.stringify({ model: MODEL, messages, stream: true }),
        signal: request.signal,
        credentials: 'omit',
        redirect: 'error',
      });
      if (!response.ok) {
        onError(`Tachyon Gateway がエラーを返しました（HTTP ${response.status}）。トークン、権限、利用枠を確認してください。`);
        return new Response('', { status: response.status });
      }
      const text = await readStream(response, onDelta);
      return new Response(JSON.stringify({ text }), { status: 200, headers: { 'content-type': 'application/json' } });
    } catch (error) {
      if (error?.name !== 'AbortError') onError('Tachyon Gateway に接続できませんでした。');
      throw error;
    }
  };
}
