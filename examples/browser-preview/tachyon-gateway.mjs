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

export function createGatewayFetch({ fetchImpl, getToken, getProvider = () => 'tachyon', getLocalModel = () => '', getBrowserModel = () => '0.6b', onError, onDelta = () => {}, onProgress = () => {}, origin }) {
  let browserWorker, loadedBrowserModel;
  return async function gatewayFetch(request, options) {
    if (!(request instanceof Request) || new URL(request.url).origin !== origin || new URL(request.url).pathname !== '/demo-gateway') {
      return fetchImpl(request, options);
    }
    const provider = getProvider();
    const local = provider === 'ollama';
    const browser = provider === 'browser';
    if (!local && !browser && provider !== 'tachyon') throw new Error('unknown provider');
    const token = local || browser ? '' : getToken().trim();
    const localModel = local ? getLocalModel().trim() : '';
    if (local && !localModel) { onError('Ollama のモデル名を入力してください。'); throw new Error('Ollama model required'); }
    if (!local && !browser && !token) {
      onError('Tachyon アクセストークンを入力してください。');
      throw new Error('Tachyon authentication required');
    }
    const source = await request.clone().json();
    if (browser) {
      return new Promise((resolve, reject) => {
        const selectedModel = getBrowserModel();
        if (!['0.6b', '1.7b'].includes(selectedModel)) return reject(new Error('invalid browser model'));
        if (browserWorker && loadedBrowserModel !== selectedModel) { browserWorker.terminate(); browserWorker = null; }
        loadedBrowserModel = selectedModel;
        const worker = browserWorker ||= new Worker('./browser-model-worker.mjs', {type:'module'});
        const abort = () => { worker.terminate(); browserWorker = null; reject(new DOMException('Aborted', 'AbortError')); };
        if (request.signal.aborted) return abort();
        request.signal.addEventListener('abort', abort, {once:true});
        worker.onerror = () => { request.signal.removeEventListener('abort', abort); worker.terminate(); browserWorker = null; onError('ブラウザ内モデルを起動できませんでした。'); reject(new Error('browser model failed')); };
        worker.onmessage = ({data}) => {
          if (data.type === 'progress') { onProgress(data.progress); return; }
          if (data.type === 'ready') return;
          request.signal.removeEventListener('abort', abort);
          if (data.type === 'error') { worker.terminate(); browserWorker = null; onError(data.message); reject(new Error(data.message)); return; }
          resolve(Response.json({text:data.text, calls:data.calls}));
        };
        worker.postMessage({messages:source.messages, tools:source.tools, model:selectedModel});
      });
    }
    const messages = source.messages.map(message => {
      if (!['system', 'user', 'assistant'].includes(message.role) || typeof message.content !== 'string') {
        throw new Error('このプレビューはテキスト会話のみ対応しています');
      }
      return { role: message.role, content: message.content };
    });
    try {
      const response = await fetchImpl(local ? 'http://127.0.0.1:11434/v1/chat/completions' : API_URL, {
        method: 'POST',
        headers: local ? { 'content-type': 'application/json' } : { 'content-type': 'application/json', authorization: `Bearer ${token}`, 'x-operator-id': OPERATOR_ID },
        body: JSON.stringify({ model: local ? localModel : MODEL, messages, stream: true }),
        signal: request.signal,
        credentials: 'omit',
        redirect: 'error',
      });
      if (!response.ok) {
        onError(local ? `Ollama がエラーを返しました（HTTP ${response.status}）。モデル名とサーバーを確認してください。` : `Tachyon Gateway がエラーを返しました（HTTP ${response.status}）。トークン、権限、利用枠を確認してください。`);
        return new Response('', { status: response.status });
      }
      const text = await readStream(response, onDelta);
      return new Response(JSON.stringify({ text }), { status: 200, headers: { 'content-type': 'application/json' } });
    } catch (error) {
      if (error?.name !== 'AbortError') onError(local ? 'Ollama に接続できませんでした。起動状態とブラウザの接続許可を確認してください。' : 'Tachyon Gateway に接続できませんでした。');
      throw error;
    }
  };
}
