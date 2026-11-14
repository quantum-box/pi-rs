# HTTP CONNECT transport investigation (work in progress)

Reference: Pi v1.5.0 `f9a26365568127461c47a07a37dcb42c140a3ac5`,
`packages/coding-agent/src/core/http-dispatcher.ts`: Undici's
`EnvHttpProxyAgent` uses `proxyTunnel: true`. This applies CONNECT to plain
HTTP as well as HTTPS for requests using that dispatcher. Bedrock uses its own
AWS HTTP handler and must be assessed separately.

## Constraints

* Model and other workbench clients use reqwest 0.12.28; MCP transport and
  OAuth use reqwest 0.13.5. Public SDK APIs accept `reqwest::Proxy` values.
* Both reqwest versions choose CONNECT only for HTTPS destinations. Their
  `connector_layer` wraps a service using private URI/connection fields, so a
  downstream layer cannot safely replace the stream or change its proxy flag.
* HTTP forward proxy authentication and custom proxy headers are injected into
  the origin request by reqwest. Authenticated dynamic `Proxy::custom` matches
  use CONNECT even without the opt-in because callback results can change
  between request construction and connection. This intentionally changes
  legacy custom proxies that require authenticated forward requests.
* MCP OAuth enforces URL and redirect checks and uses a DNS resolver that
  exempts configured proxy hosts. Any transport replacement must preserve the
  checks on every redirect and avoid treating an origin as a proxy host.
* Keep HTTPS proxy and origin TLS verification, `NO_PROXY` selection, proxy
  credentials, read timeouts, cancellation, streaming, and existing SDK APIs.

## Candidate approach

The worktree contains an unvalidated prototype patch to both locked reqwest versions, adding an
opt-in tunnel policy rather than changing defaults for public SDK callers.
In the proxied HTTP connection branch use the existing hyper-util `Tunnel`
with the same proxy TLS connector and proxy headers used for HTTPS; return a
connection marked non-proxy so hyper sends origin-form requests. Suppress the
forward-proxy header injection for this policy. Wire the policy only through
workbench-configured clients, retaining existing externally supplied Proxy
semantics. A maintained fork or upstream contribution is preferable to silently
changing all reqwest users through a global patch. Investigate how to expose
the option through both versions and MCP/rmcp without a public API break.

An alternative custom hyper transport would require adapting all streaming,
redirect and MCP OAuth call sites and reproducing their security checks. A
localhost forwarding shim is not acceptable without an explicit security
design: it changes resolver observations and can leak proxy credentials.

## Acceptance checks

* HTTP loopback proxy requires CONNECT and then verifies origin-form request;
  verify proxy credentials appear only in CONNECT. Tests for the reqwest 0.12
  and reqwest 0.13 paths live in `http_proxy.rs`.
* Equivalent reqwest 0.13/MCP test including reconnect and OAuth metadata,
  registration and redirect handling.
* HTTPS target and HTTPS proxy preserve certificate validation and tunnel
  authentication; test TLS failures and no credential leakage.
* `NO_PROXY` host/domain/port cases bypass CONNECT without sending proxy
  credentials, including redirects between bypassed and proxied hosts.
* OAuth private-address and proxy-host restrictions remain enforced for direct,
  proxied and redirected targets. Preserve bounded response bodies.
* Exercise streaming, cancellation, timeout and malformed/rejected CONNECT;
  ensure no unbounded background tasks and no replay of request bodies.
* Model loopback and MCP transport fixtures, followed by cross-platform CI.

The prototype is pending public CI and independent review. The two reqwest
sources retain their MIT and Apache-2.0 license files. Vendoring is a temporary
maintenance burden: every security update and dependency upgrade must rebase
and revalidate both small patches. Prefer upstreaming this opt-in API and
removing the forks when released; until then monitor advisories for both pinned
versions and update them together with their regression tests. This document
does not claim acceptance of untested transport or OAuth behavior.
