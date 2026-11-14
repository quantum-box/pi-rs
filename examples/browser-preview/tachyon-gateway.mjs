// Adapts the browser SDK's text gateway contract to Tachyon Model Gateway.
// Credentials stay in the caller's browser and are sent only to the fixed API.
const API_URL = 'https://api.n1.tachy.one/v1/ai/chat/completions';
const OPERATOR_ID = 'tn_01kxz0ytmhnab5vh53011cwctj';
const MODEL = 'openai/gpt-6-luna';
const MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

async function boundedJson(response) {
  if (!response.body) throw new Error('応答ストリームがありません');
  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_RESPONSE_BYTES) throw new Error('応答が大きすぎます');
      chunks.push(value);
    }
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  }
  const data = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { data.set(chunk, offset); offset += chunk.byteLength; }
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(data));
}

export function createGatewayFetch({ fetchImpl, getToken, onError, origin }) {
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
        body: JSON.stringify({ model: MODEL, messages, stream: false, max_completion_tokens: Math.min(source.max_output_tokens || 1024, 1024) }),
        signal: request.signal,
        credentials: 'omit',
        redirect: 'error',
      });
      if (!response.ok) {
        onError(`Tachyon Gateway がエラーを返しました（HTTP ${response.status}）。トークン、権限、利用枠を確認してください。`);
        return new Response('', { status: response.status });
      }
      const data = await boundedJson(response);
      const content = data.choices?.[0]?.message?.content;
      const text = typeof content === 'string' ? content : Array.isArray(content) ? content.filter(part => part.type === 'text' && typeof part.text === 'string').map(part => part.text).join('') : null;
      if (text === null) throw new Error('モデル応答にテキストがありません');
      return new Response(JSON.stringify({ text }), { status: 200, headers: { 'content-type': 'application/json' } });
    } catch (error) {
      if (error?.name !== 'AbortError') onError('Tachyon Gateway に接続できませんでした。');
      throw error;
    }
  };
}
