// Run with: node browser-fetch.cjs <wasm-bindgen web output dir> <Chrome executable>
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
  let redirects = 0;
  let cookieSeen = false;
  const api = http.createServer((req, res) => {
    if (req.url !== '/no-cors') {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Headers', 'authorization,content-type');
      res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
    }
    if (req.method === 'OPTIONS') { res.writeHead(204).end(); return; }
    if (req.url === '/redirect') { res.writeHead(302, { Location: '/redirect-target' }).end(); return; }
    if (req.url === '/redirect-target') { redirects++; res.end(); return; }
    if (req.url === '/ok' && req.headers.cookie) cookieSeen = true;
    if (req.url === '/slow-read') { res.writeHead(200); res.write('a'); setTimeout(() => res.end('late'), 1000); return; }
    if (req.url === '/wait') { setTimeout(() => res.end('late'), 1000); return; }
    if (req.url === '/fail') { res.writeHead(403).end('private body'); return; }
    res.writeHead(200); res.write('a'); setTimeout(() => res.end('b'), 30);
  });
  api.listen(0, '127.0.0.1'); await once(api, 'listening');
  const origin = `http://127.0.0.1:${api.address().port}`;
  const html = `<!doctype html><body>RUNNING<script type="module">
import init, {BrowserFetch} from '/pkg/pi_rs_browser_fetch.js';
const assert = (x, msg) => { if (!x) throw Error(msg); };
const reject = async (p, part) => { try { await p; throw Error('unexpected success'); } catch(e) { assert(String(e).includes(part), String(e)); } };
try {
  document.cookie = 'pi_fetch_test=secret; SameSite=Lax';
  await init(); const base = ${JSON.stringify(origin)};
  const request = new BrowserFetch(base + '/ok', '{}', {authorization:'Bearer private'});
  assert(await request.open() === 200, 'status'); let out = ''; for (;;) { const b = await request.read(); if (b === undefined) break; out += new TextDecoder().decode(b); }
  assert(out === 'ab', 'stream'); request.free();
  const cors = new BrowserFetch(base + '/no-cors', '', {}); await reject(cors.open(), 'fetch failed'); cors.free();
  const redirect = new BrowserFetch(base + '/redirect', '', {authorization:'Bearer private'}); await reject(redirect.open(), 'fetch failed'); redirect.free();
  const fail = new BrowserFetch(base + '/fail', '', {}); await reject(fail.open(), 'HTTP status 403'); fail.free();
  const reading = new BrowserFetch(base + '/slow-read', '', {}); await reading.open(); await reading.read(); const next = reading.read(); setTimeout(() => reading.cancel(), 20); await reject(next, 'cancelled'); reading.free();
  const slow = new BrowserFetch(base + '/wait', '', {}); const p = slow.open(); setTimeout(() => slow.cancel(), 20); await reject(p, 'cancelled'); slow.free();
  document.body.textContent = 'PASS';
} catch(e) { document.body.textContent = 'FAIL ' + String(e) + ' ' + (e.stack || ''); }
</script></body>`;
  const site = http.createServer((req, res) => {
    if (req.url === '/') { res.setHeader('content-type', 'text/html'); res.end(html); return; }
    const name = path.basename(req.url);
    if (!['pi_rs_browser_fetch.js', 'pi_rs_browser_fetch_bg.wasm'].includes(name)) { res.writeHead(404).end(); return; }
    res.setHeader('content-type', name.endsWith('.wasm') ? 'application/wasm' : 'text/javascript');
    res.end(fs.readFileSync(path.join(dir, name)));
  });
  site.listen(0, '127.0.0.1'); await once(site, 'listening');
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-browser-fetch-'));
  try {
    const child = spawn(chrome, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', `--user-data-dir=${profile}`, '--remote-debugging-port=0', 'about:blank'], {stdio:['ignore','ignore','pipe']});
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
      if (text !== 'PASS' || redirects !== 0 || cookieSeen) throw Error(`browser failed: ${text} redirects=${redirects} cookieSeen=${cookieSeen}`);
    } finally { child.kill('SIGKILL'); await once(child, 'close'); }
    console.log('browser-fetch Chrome integration passed (including CORS)');
  } finally { api.closeAllConnections(); site.closeAllConnections(); api.close(); site.close(); fs.rmSync(profile, {recursive:true, force:true}); }
})().catch(e => { console.error(e); process.exitCode = 1; });
