import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import net from 'node:net';
import { stopChild, waitForSocketOpen } from './smoke-lifecycle.mjs';

class FakeSocket extends EventTarget { readyState = 0; }

test('WebSocket opening succeeds and removes listeners', async () => {
  const socket = new FakeSocket();
  const opened = waitForSocketOpen(socket, 100);
  socket.dispatchEvent(new Event('open'));
  await opened;
  socket.dispatchEvent(new Event('error'));
});

test('WebSocket error, early close, and timeout reject promptly', async () => {
  for (const [event, message] of [['error', /connection failed/], ['close', /closed before opening/], [null, /open timeout/]]) {
    const socket = new FakeSocket();
    const opened = waitForSocketOpen(socket, 20);
    if (event) socket.dispatchEvent(new Event(event));
    await assert.rejects(opened, message);
  }
  const closed = new FakeSocket(); closed.readyState = 3;
  await assert.rejects(waitForSocketOpen(closed, 20), /closed before opening/);
});

test('real refused WebSocket rejects', async () => {
  const server = net.createServer(); server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  await assert.rejects(waitForSocketOpen(new WebSocket(`ws://127.0.0.1:${port}`), 1000), /connection failed|closed before opening/);
});

test('stops a running child and accepts a child already closed by signal', async () => {
  const running = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)']);
  await once(running, 'spawn');
  await stopChild(running, 2000);
  assert.equal(running.signalCode, 'SIGKILL');
  // The close event has already happened when this is called again.
  await stopChild(running, 20);
});

test('shutdown has a deadline even if close never arrives', async () => {
  const child = new EventEmitter();
  child.exitCode = null; child.signalCode = null; child.kill = () => true;
  await assert.rejects(stopChild(child, 20), /shutdown timeout/);
  assert.equal(child.listenerCount('close'), 0);
  assert.equal(child.listenerCount('error'), 0);
});
