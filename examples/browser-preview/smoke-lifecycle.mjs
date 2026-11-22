// Keep browser failures bounded, including when a child already exited by signal.
export function waitForSocketOpen(socket, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let timer;
    function finish(error) {
      clearTimeout(timer);
      socket.removeEventListener('open', opened);
      socket.removeEventListener('error', failed);
      socket.removeEventListener('close', closed);
      error ? reject(error) : resolve();
    }
    const opened = () => finish();
    const failed = () => finish(Error('Chrome WebSocket connection failed'));
    const closed = () => finish(Error('Chrome WebSocket closed before opening'));
    socket.addEventListener('open', opened);
    socket.addEventListener('error', failed);
    socket.addEventListener('close', closed);
    timer = setTimeout(() => finish(Error('Chrome WebSocket open timeout')), timeoutMs);
    // Account for a socket that settled before listeners were attached.
    if (socket.readyState === 1) opened();
    else if (socket.readyState >= 2) closed();
  });
}

export function stopChild(child, timeoutMs = 10000) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve, reject) => {
    let timer;
    function finish(error) {
      clearTimeout(timer);
      child.removeListener('close', closed);
      child.removeListener('error', failed);
      error ? reject(error) : resolve();
    }
    const closed = () => finish();
    const failed = error => finish(error);
    child.once('close', closed);
    child.once('error', failed);
    timer = setTimeout(() => finish(Error('Chrome shutdown timeout')), timeoutMs);
    // Recheck after subscribing so an already emitted close cannot be missed.
    if (child.exitCode !== null || child.signalCode !== null) closed();
    else {
      try { child.kill('SIGKILL'); } catch (error) { finish(error); }
    }
  });
}
