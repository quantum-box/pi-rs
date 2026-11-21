"""Terminal and model-wire fixture for the MCP OAuth manager."""

import fcntl
import http.server
import json
import os
import pty
import re
import select
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import time
import urllib.parse
from pathlib import Path

class Chat(http.server.BaseHTTPRequestHandler):
    offered = []
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.offered.append([tool["function"]["name"] for tool in request.get("tools", [])])
        event = {"choices": [{"index": 0, "delta": {"content": f"fixture response {len(self.offered)}"}, "finish_reason": "stop"}]}
        body = f"data: {json.dumps(event)}\n\ndata: [DONE]\n\n".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

class OAuth(http.server.BaseHTTPRequestHandler):
    token_requests = 0
    bearer_requests = 0
    disconnected = False

    @property
    def base(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def reply(self, code, response=None, headers=None):
        body = json.dumps(response).encode() if response is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        issuer = self.base + "/issuer"
        if self.path == "/protected-resource":
            self.reply(200, {"resource": self.base + "/mcp", "authorization_servers": [issuer]})
        elif self.path == "/.well-known/oauth-authorization-server/issuer":
            self.reply(200, {
                "issuer": issuer, "authorization_endpoint": self.base + "/authorize",
                "token_endpoint":self.base+"/token",
                "response_types_supported":["code"], "grant_types_supported":["authorization_code"],
                "token_endpoint_auth_methods_supported":["none","client_secret_post"],
                "code_challenge_methods_supported":["S256"],
            })
        else:
            self.reply(405)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/token":
            assert b"code=fixture-code" in body and b"code_verifier=" in body
            type(self).token_requests += 1
            self.reply(200, {"access_token":"fixture-access","token_type":"Bearer","expires_in":3600})
            return
        assert self.path == "/mcp"
        if self.disconnected:
            self.reply(503, {"error": "temporarily_unavailable"})
            return
        if self.headers.get("Authorization") != "Bearer fixture-access":
            if self.headers.get("X-Fixture-Route") != "configured":
                self.reply(401)
                return
            metadata = self.base + "/protected-resource"
            self.reply(
                401,
                {"error": "invalid_token"},
                {"WWW-Authenticate": f'Bearer resource_metadata="{metadata}"'},
            )
            return
        type(self).bearer_requests += 1
        req = json.loads(body)
        if req["method"] == "initialize":
            result = {
                "protocolVersion": req["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fixture", "version": "1"},
            }
        elif req["method"] == "tools/list":
            result = {"tools":[{"name":"managed","inputSchema":{"type":"object"}}]}
        else:
            self.reply(202)
            return
        self.reply(200, {"jsonrpc":"2.0","id":req["id"],"result":result})

    def log_message(self, format, *args):
        pass

with (
    tempfile.TemporaryDirectory() as folder,
    http.server.ThreadingHTTPServer(("127.0.0.1", 0), Chat) as chat,
    http.server.ThreadingHTTPServer(("127.0.0.1", 0), OAuth) as oauth,
):
    for server in [chat, oauth]:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    root = Path(folder)
    home = root / "home"
    home.mkdir()
    browser = root / "browser"
    browser.mkdir()
    launcher = r"""#!/usr/bin/env python3
import os, pathlib, socket, sys, urllib.parse
capture = pathlib.Path(os.environ["PI_TEST_MCP_BROWSER_URL"])
capture.write_text(sys.argv[-1])
if capture.with_suffix(".manual").exists():
    sys.exit(1)
args = urllib.parse.parse_qs(urllib.parse.urlsplit(sys.argv[-1]).query)
callback = args["redirect_uri"][0]
parts = urllib.parse.urlsplit(callback)
assert parts.hostname in ("127.0.0.1", "localhost")
query = urllib.parse.urlencode({"state":args["state"][0],"code":"fixture-code"})
# Exit after opening the callback, as the desktop browser launcher does.
# The Rust process begins reading the redirect once that launcher exits.
with socket.create_connection((parts.hostname, parts.port), timeout=5) as redirect:
    request = f"GET {parts.path}?{query} HTTP/1.1\r\nHost: {parts.netloc}\r\nConnection: close\r\n\r\n"
    redirect.sendall(request.encode())
"""
    for program in ("open", "xdg-open"):
        executable = browser/program
        executable.write_text(launcher)
        executable.chmod(0o755)
    base = f"http://127.0.0.1:{oauth.server_port}"
    config = home/"mcp.json"
    config.write_text(json.dumps({"mcpServers": {"fixture": {
        "url": base + "/mcp",
        "oauth": {"clientId": "fixture-client"},
        "headers": {"X-Fixture-Route": "configured"},
        "exposure": "direct",
    }}}))
    saved = config.read_bytes()
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 28, 125, 0, 0))
    endpoint = f"http://127.0.0.1:{chat.server_port}/v1/chat/completions"
    process = subprocess.Popen([
        sys.argv[1], "--endpoint", endpoint, "--model", "fixture",
        "--offline", "--no-trust", "--tui-mode", "fullscreen",
    ],
        cwd=root,
        env={
            **os.environ,
            "PI_RS_HOME": str(home),
            "TERM": "xterm-256color",
            "PATH": str(browser) + os.pathsep + os.environ.get("PATH", ""),
            "PI_TEST_MCP_BROWSER_URL": str(root / "authorization-url"),
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        },
        stdin=slave,
        stdout=slave,
        stderr=slave,
        start_new_session=True,
    )
    os.close(slave)

    def expect(*patterns):
        data = b""
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if select.select([master], [], [], .15)[0]:
                try:
                    data += os.read(master, 65536)
                except OSError:
                    break
            text = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", data).decode(errors="replace")
            if all(pattern in text for pattern in patterns):
                return
        raise AssertionError((patterns, data[-4500:].decode(errors="replace")))
    def send(keys):
        os.write(master, keys)
    def manager():
        send(b"/mcp\r")
        expect("MCP servers", "fixture")
        send(b"\r")
        expect("MCP server · fixture", "Exposure")
    def close_manager():
        send(b"\x1b")
        expect("MCP servers")
        send(b"\x1b")
        expect("Enter to send")
        time.sleep(.15)
    def prompt(words):
        previous = len(Chat.offered)
        send(words.encode() + b"\r")
        expect(f"fixture response {previous + 1}")
        time.sleep(.7)
        assert len(Chat.offered) == previous + 1, Chat.offered
        return {tool for tool in Chat.offered[-1] if tool.startswith("mcp__")}
    try:
        expect("Enter to send")
        manager()
        send(b"i")
        expect("Signed in", "Sign out", "Tools · 1")
        assert OAuth.token_requests == 1 and OAuth.bearer_requests >= 2
        close_manager()
        assert prompt("Signed in prompt") == {"mcp__fixture__managed"}, Chat.offered
        manager()
        send(b"o")
        expect("Signed out", "needs sign-in", "Tools · 0")
        close_manager()
        assert prompt("Signed out prompt") == set(), Chat.offered
        assert config.read_bytes() == saved
        capture = root / "authorization-url"
        capture.unlink()
        capture.with_suffix(".manual").touch()
        manager()
        send(b"i")
        expect("Open this authorization URL", "Paste redirect URL")
        send(b"\r")
        expect("Paste the browser redirect URL")
        deadline = time.monotonic() + 5
        while not capture.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        authorization = urllib.parse.urlsplit(capture.read_text())
        parameters = urllib.parse.parse_qs(authorization.query)
        callback = parameters["redirect_uri"][0]
        for state in ["fixture-wrong-state", parameters["state"][0]]:
            redirect = callback + "?" + urllib.parse.urlencode({"state": state, "code": "fixture-code"})
            send(redirect.encode() + b"\r")
            if state == "fixture-wrong-state":
                expect("Invalid redirect URL")
                assert OAuth.token_requests == 1
        expect("Signed in", "Tools · 1")
        assert OAuth.token_requests == 2
        OAuth.disconnected = True
        send(b"r")
        expect("Status · disconnected", "Tools · 0")
    finally:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)
        os.close(master)
        chat.shutdown()
        oauth.shutdown()
