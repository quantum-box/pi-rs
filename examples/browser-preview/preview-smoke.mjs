// Exercise the built site and its real worker without downloading a model.
import http from 'node:http';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';

const dist = path.resolve(process.argv[2] || 'examples/browser-preview/dist');
const chrome = process.argv[3] || process.env.CHROME_BIN;
if (!chrome) throw Error('Provide the Chrome executable as the second argument or CHROME_BIN');
const requests = [];
const site = http.createServer(async (req, res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  requests.push(pathname);
  const file = path.resolve(dist, '.' + (pathname === '/' ? '/index.html' : pathname));
  if (!file.startsWith(dist + path.sep)) { res.writeHead(403).end(); return; }
  try {
    const body = await readFile(file);
    const type = {'.html':'text/html', '.js':'text/javascript', '.mjs':'text/javascript', '.wasm':'application/wasm', '.css':'text/css'}[path.extname(file)] || 'application/octet-stream';
    res.writeHead(200, {'content-type': type}).end(body);
  } catch { res.writeHead(404).end(); }
});
site.listen(0, '127.0.0.1'); await once(site, 'listening');
const profile = await mkdtemp(path.join(tmpdir(), 'pi-preview-smoke-'));
let child, socket;
try {
  const args = ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', `--user-data-dir=${profile}`, '--remote-debugging-port=0', 'about:blank'];
  if (process.env.PI_FETCH_CHROME_NO_SANDBOX === '1') args.unshift('--no-sandbox');
  child = spawn(chrome, args, {stdio:['ignore','ignore','pipe']});
  let stderr = '';
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Chrome debugging endpoint timeout')), 10000);
    child.stderr.on('data', chunk => { stderr += chunk; const match = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/); if (match) { clearTimeout(timer); resolve(match[1]); } });
    child.on('exit', () => { clearTimeout(timer); reject(Error('Chrome exited: ' + stderr)); });
  });
  socket = new WebSocket(endpoint); await once(socket, 'open');
  let next = 0, workerAttached = false, stubbedModules = 0;
  const pending = new Map();
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++next;
    const timer = setTimeout(() => { pending.delete(id); reject(Error(`Chrome command timed out: ${method}`)); }, 10000);
    pending.set(id, {resolve: result => { clearTimeout(timer); resolve(result); }, reject: error => { clearTimeout(timer); reject(error); }});
    socket.send(JSON.stringify({id, method, params, sessionId}));
  });
  let eventError;
  socket.addEventListener('message', event => {
    const msg = JSON.parse(event.data);
    if (msg.id && pending.has(msg.id)) {
      const p = pending.get(msg.id); pending.delete(msg.id);
      msg.error ? p.reject(Error(msg.error.message)) : p.resolve(msg.result);
    } else if (msg.method === 'Target.attachedToTarget' && msg.params.targetInfo.type === 'worker') {
      workerAttached = true;
      const id = msg.params.sessionId;
      send('Runtime.runIfWaitingForDebugger', {}, id).catch(error => { eventError = error; });
    } else if (msg.method === 'Fetch.requestPaused') {
      // Substitute only the external model runtimes. The deployed worker itself
      // is loaded and executed unchanged from dist.
      const url = msg.params.request.url;
      const source = url.startsWith('https://cdn.jsdelivr.net/')
        ? 'export const pipeline = () => { throw Error("unexpected pipeline"); };'
        : 'export async function CreateMLCEngine() { return {chat:{completions:{create:async () => ({choices:[{message:{content:"smoke model response"}}]})}}}; }';
      stubbedModules++;
      send('Fetch.fulfillRequest', {requestId:msg.params.requestId, responseCode:200, responseHeaders:[{name:'Content-Type',value:'text/javascript'},{name:'Access-Control-Allow-Origin',value:'*'}], body:Buffer.from(source).toString('base64')}, msg.sessionId).catch(error => { eventError = error; });
    }
  });
  const target = await send('Target.createTarget', {url:'about:blank'});
  const {sessionId} = await send('Target.attachToTarget', {targetId:target.targetId, flatten:true});
  await send('Fetch.enable', {patterns:[{urlPattern:'https://cdn.jsdelivr.net/*'}, {urlPattern:'https://esm.run/*'}]}, sessionId);
  await send('Target.setAutoAttach', {autoAttach:true, waitForDebuggerOnStart:true, flatten:true}, sessionId);
  await send('Page.navigate', {url:`http://127.0.0.1:${site.address().port}/`}, sessionId);
  const evaluate = async expression => {
    const result = await send('Runtime.evaluate', {expression, returnByValue:true, awaitPromise:true}, sessionId);
    if (result.exceptionDetails) throw Error(result.exceptionDetails.text);
    return result.result.value;
  };
  async function waitFor(expression, description) {
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
      if (eventError) throw eventError;
      if (await evaluate(expression)) return;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw Error(`Timed out waiting for ${description}: ${await evaluate('document.body?.innerText')}`);
  }
  await waitFor(`document.body?.innerText.includes('接続準備完了') && !document.querySelector('textarea')?.disabled`, 'preview initialization');
  await evaluate(`(() => { const select = document.querySelector('.settings select'); select.value = 'browser'; select.dispatchEvent(new Event('change', {bubbles:true})); })()`);
  await waitFor(`document.body?.innerText.includes('Qwen3 1.7B') && !document.querySelector('textarea')?.disabled`, 'browser session');
  await evaluate(`(() => { const input = document.querySelector('textarea'); Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set.call(input, 'smoke test'); input.dispatchEvent(new Event('input', {bubbles:true})); })()`);
  await waitFor(`!document.querySelector('button[aria-label="送信"]')?.disabled`, 'enabled send button');
  await evaluate(`document.querySelector('button[aria-label="送信"]').click()`);
  await waitFor(`document.body?.innerText.includes('smoke model response')`, 'worker response in preview');
  for (const name of ['/', '/pkg/pi_rs_browser_sdk.js', '/pkg/pi_rs_browser_sdk_bg.wasm', '/browser-model-worker.mjs']) {
    if (!requests.includes(name)) throw Error(`Preview did not request ${name}`);
  }
  if (!workerAttached || stubbedModules !== 2) throw Error(`Worker or model imports missing: ${workerAttached}, ${stubbedModules}`);
  console.log('preview smoke passed (HTML, WASM initialization, browser session, worker and rendered response)');
} finally {
  socket?.close();
  if (child && child.exitCode === null) { child.kill('SIGKILL'); await once(child, 'close'); }
  site.closeAllConnections(); site.close();
  await rm(profile, {recursive:true, force:true});
}
