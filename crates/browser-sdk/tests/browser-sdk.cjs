// Run with: node browser-sdk.cjs <wasm-bindgen web output dir> <Chrome executable>
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
(async () => {
  const dir = process.argv[2];
  const chrome = process.argv[3];
  if (!dir || !chrome) throw Error('expected bindings directory and Chrome path');
  const requests = [];
  let aborted = false;
  const api = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'authorization,content-type');
    res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
    if (req.method === 'OPTIONS') { res.writeHead(204).end(); return; }
    let body = ''; req.on('data', b => body += b); req.on('end', () => {
      const request = JSON.parse(body); request._auth = req.headers.authorization; requests.push(request);
      const last = request.messages.filter(m => m.role === 'user').at(-1)?.content;
      if (last === 'fail me') { res.writeHead(403).end('private'); }
      else if (last === 'cancel me' || last === 'timeout me') {
        res.on('close', () => { if (!res.writableEnded) aborted = true; });
        setTimeout(() => { if (!res.destroyed) res.end(JSON.stringify({text:'too late'})); }, 3000);
      } else {
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({text: last === 'first' ? 'answer one' : 'answer two'}));
      }
    });
  });
  api.listen(0, '127.0.0.1'); await once(api, 'listening');
  const origin = `http://127.0.0.1:${api.address().port}`;
  const html = `<!doctype html><body>RUNNING<script type="module">
import init, {BrowserSession} from '/pkg/pi_rs_browser_sdk.js';
const assert = (x, msg) => { if (!x) throw Error(msg); };
const reject = async (p, part) => { let err; try { await p; } catch(e) { err = e; } assert(err && String(err).includes(part), String(err)); };
try {
  await init(); const base = ${JSON.stringify(origin)};
  if (sessionStorage.getItem('reloaded')) {
    const restored = await BrowserSession.open('test', base + '/gateway', 'test-model', 'secret', 1500);
    assert(JSON.parse(restored.messages()).some(m => m.content === 'after timeout'), 'restored after page reload');
    await restored.close(); restored.free();
    document.body.textContent = 'PASS';
  } else {
  localStorage.setItem('pi-rs:session:bad', 'invalid');
  await reject(BrowserSession.open('bad', base + '/gateway', 'test-model', '', 1500), 'expected value');
  localStorage.removeItem('pi-rs:session:bad');
  let recovered = await BrowserSession.open('bad', base + '/gateway', 'test-model', '', 1500); await recovered.close(); recovered.free();
  let session = await BrowserSession.open('test', base + '/gateway', 'test-model', 'secret', 1500);
  await reject(BrowserSession.open('test', base + '/gateway', 'test-model', '', 1500), 'already open');
  assert(await session.prompt('first') === 'answer one', 'first answer');
  let messages = JSON.parse(session.messages());
  assert(messages.some(m => m.content === 'first') && messages.some(m => m.content === 'answer one'), 'saved messages');
  assert(!localStorage.getItem('pi-rs:session:test').includes('secret'), 'token persisted in session');
  await session.close(); session.free();
  session = await BrowserSession.open('test', base + '/gateway', 'test-model', 'secret', 1500);
  messages = JSON.parse(session.messages());
  assert(messages.some(m => m.content === 'first') && messages.some(m => m.content === 'answer one'), 'restored messages');
  assert(await session.prompt('second') === 'answer two', 'second answer');
  const immediate = session.prompt('never sent'); session.cancel();
  await reject(immediate, 'cancel');
  const pending = session.prompt('cancel me');
  while (!(await (await fetch('/cancel-started')).json())) await new Promise(r => setTimeout(r, 20));
  await reject(session.close(), 'busy');
  session.cancel();
  await reject(pending, 'cancel');
  assert(await session.prompt('after cancel') === 'answer two', 'run after cancel');
  await reject(session.prompt('timeout me'), 'timed out');
  assert(await session.prompt('after timeout') === 'answer two', 'run after timeout');
  await reject(session.prompt('fail me'), 'gateway request failed');
  assert(await session.prompt('after failure') === 'answer two', 'run after gateway failure');
  const doomed = session.prompt('cancel me');
  await new Promise(r => setTimeout(r, 30));
  session.free();
  await reject(doomed, 'cancel');
  session = null;
  const lockDeadline = Date.now() + 3000;
  while (Date.now() < lockDeadline) {
    try { session = await BrowserSession.open('test', base + '/gateway', 'test-model', 'secret', 1500); break; }
    catch (e) { if (!String(e).includes('already open')) throw e; await new Promise(r => setTimeout(r, 20)); }
  }
  assert(session !== null, 'lock released after free');
  await Promise.all([session.close(), session.close()]); session.free();
  sessionStorage.setItem('reloaded', 'yes');
  location.reload();
  }
} catch(e) { document.body.textContent = 'FAIL ' + String(e) + ' ' + (e.stack || ''); }
</script></body>`;
  const site = http.createServer((req, res) => {
    if (req.url === '/cancel-started') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(requests.some(r => r.messages.some(m => m.content === 'cancel me')))); return; }
    if (req.url === '/') { res.setHeader('content-type', 'text/html'); res.end(html); return; }
    const name = path.basename(req.url);
    const file = req.url.includes('/snippets/') ? path.join(dir, req.url.slice('/pkg/'.length)) : path.join(dir, name);
    if (!['pi_rs_browser_sdk.js', 'pi_rs_browser_sdk_bg.wasm', 'inline0.js'].includes(name)) { res.writeHead(404).end(); return; }
    res.setHeader('content-type', name.endsWith('.wasm') ? 'application/wasm' : 'text/javascript');
    res.end(fs.readFileSync(file));
  });
  site.listen(0, '127.0.0.1'); await once(site, 'listening');
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-browser-fetch-'));
  try {
    const args = ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', `--user-data-dir=${profile}`, '--remote-debugging-port=0', 'about:blank'];
    // GitHub-hosted Ubuntu runners cannot create Chrome's user namespace sandbox.
    if (process.env.PI_FETCH_CHROME_NO_SANDBOX === '1') args.unshift('--no-sandbox');
    const child = spawn(chrome, args, {stdio:['ignore','ignore','pipe']});
    let stderr = '';
    const endpoint = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(Error('Chrome debugging endpoint timeout')), 10000);
      child.stderr.on('data', chunk => {
        stderr += chunk;
        const match = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/);
        if (match) { clearTimeout(timer); resolve(match[1]); }
      });
      child.on('exit', () => { clearTimeout(timer); reject(Error('Chrome exited: ' + stderr)); });
    });
    try {
      const socket = new WebSocket(endpoint); await once(socket, 'open');
      let next = 0; const pending = new Map();
      socket.addEventListener('message', event => { const message = JSON.parse(event.data); if (message.id && pending.has(message.id)) { const {resolve, reject} = pending.get(message.id); pending.delete(message.id); message.error ? reject(Error(message.error.message)) : resolve(message.result); } });
      const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => { const id = ++next; pending.set(id, {resolve, reject}); socket.send(JSON.stringify({id, method, params, sessionId})); });
      const target = await send('Target.createTarget', {url: `http://127.0.0.1:${site.address().port}/`});
      const {sessionId} = await send('Target.attachToTarget', {targetId:target.targetId, flatten:true});
      let text = ''; const deadline = Date.now() + 15000;
      while (Date.now() < deadline) {
        const response = await send('Runtime.evaluate', {expression:'document.body?.textContent || ""', returnByValue:true}, sessionId);
        text = response.result.value || '';
        if (text === 'PASS' || text.startsWith('FAIL ')) break;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      socket.close();
      if (text !== 'PASS') throw Error(`browser failed: ${text}`);
      if (requests.some(r => r.messages.some(m => m.content === 'never sent'))) throw Error('pre-start cancellation dispatched a request');
      if (requests.some(r => r._auth !== 'Bearer secret')) throw Error('gateway authorization header missing');
      if (requests.length < 4 || !requests[1].messages.some(m => m.content === 'first') || !requests[1].messages.some(m => m.content === 'answer one')) throw Error('restored context was not sent to gateway');
      if (!aborted) throw Error('cancel did not abort network request');
    } finally { child.kill('SIGKILL'); await once(child, 'close'); }
    console.log('browser SDK Chrome integration passed (restore, lock, cancellation and Fetch)');
  } finally { api.closeAllConnections(); site.closeAllConnections(); api.close(); site.close(); fs.rmSync(profile, {recursive:true, force:true}); }
})().catch(e => { console.error(e); process.exitCode = 1; });
