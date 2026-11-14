// Integration tests use real Fetch and a local server; they do not test browser CORS.
const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');
const { BrowserFetch } = require(process.argv[2]);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
(async () => {
  let redirects = 0;
  const server = http.createServer((req, res) => {
    if (req.url === '/redirect') { res.writeHead(302, { Location: '/secret-target' }).end(); return; }
    if (req.url === '/secret-target') { redirects++; res.end(); return; }
    if (req.url === '/slow-open') { setTimeout(() => res.end('late'), 1500); return; }
    if (req.url === '/error') { res.writeHead(401).end('private-response-body'); return; }
    if (req.url === '/slow-read') { res.writeHead(200); res.write('one'); setTimeout(() => res.end('late'), 1500); return; }
    if (req.url === '/large') { res.writeHead(200); for (let i = 0; i < 260; i++) res.write(Buffer.alloc(65536)); res.end(); return; }
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('one'); setTimeout(() => res.end('two'), 20);
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const root = `http://127.0.0.1:${server.address().port}`;
  try {
    assert.throws(() => new BrowserFetch('http://user:pass@127.0.0.1/', '', {}), /without URL credentials/);
    assert.throws(() => new BrowserFetch('file:///tmp/x', '', {}), /HTTP/);
    assert.throws(() => new BrowserFetch(root, 'é'.repeat(4 * 1024 * 1024 + 1), {}), /body exceeds limit/);
    const stream = new BrowserFetch(root, 'body', { authorization: 'Bearer secret' });
    assert.equal(await stream.open(), 200);
    let data = ''; for (;;) { const value = await stream.read(); if (value === undefined) break; data += Buffer.from(value).toString(); }
    assert.equal(data, 'onetwo'); assert.equal(await stream.read(), undefined); stream.free();
    const redirect = new BrowserFetch(root + '/redirect', '', { authorization: 'Bearer secret' });
    await assert.rejects(redirect.open(), /fetch failed/); assert.equal(redirects, 0); redirect.free();
    const bad = new BrowserFetch(root + '/error', '', {});
    await assert.rejects(bad.open(), e => /HTTP status 401/.test(e.message) && !e.message.includes('private-response-body')); bad.free();
    const pending = new BrowserFetch(root + '/slow-open', '', {});
    const opening = pending.open(); await delay(30); pending.cancel(); await assert.rejects(opening, /cancelled/); pending.free();
    const reader = new BrowserFetch(root + '/slow-read', '', {}); await reader.open(); await reader.read();
    const waiting = reader.read(); await assert.rejects(reader.read(), /already pending/); reader.cancel(); await assert.rejects(waiting, /cancelled/); reader.free();
    const large = new BrowserFetch(root + '/large', '', {}); await large.open(); await assert.rejects(async () => { while (await large.read() !== undefined) {} }, /exceeds limit/); large.free();
    console.log('browser-fetch Node integration passed');
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
})().catch(error => { console.error(error); process.exitCode = 1; });
