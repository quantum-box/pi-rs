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
* HTTP forward proxy authentication and custom proxy headers must only reach
  the selected proxy. A dynamic callback can change between request creation
  and connection, so those operations must use the same route snapshot.
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

Independent review found that the previous forced-tunnel approach changed a
nonmatching dynamic rule followed by a static authenticated forward-only proxy.
Active loopback regressions cover this in both versions. The current prototype
selects a route per hop and uses it for headers, connector and a bounded
route-specific pool. Its network tests and Rustls configurations need public CI
and independent review before asserting compatibility or security.

### Candidate request-scoped routing

The redirect service wraps `HyperService`, so a routing service immediately
inside it can evaluate proxy rules once for each redirect hop. The selected
immutable route must be used both to prepare forward-proxy headers and to
construct the connector. Separate hyper clients/pools per route are required:
hyper-util keys its legacy pool by destination origin, so sharing a pool
between direct and proxied routes can reuse the wrong connection. The route key
must distinguish the proxy authority, scheme, authentication and custom
headers without logging or persisting secrets. It must also distinguish direct,
forward and tunnel connections. Concurrent requests need independent snapshots;
eviction cannot invalidate a client currently in use.

`ConnectorBuilder` owns the TLS, DNS, timeout and connector-layer configuration,
and hyper's legacy builder is cloneable. The prototype shares a single layered
connector across route-specific clients and retains at most 32 routes. The
selected route travels in the opaque connector request, keeping shared layer
state while isolating connection pools. It removes
`Proxy-Authorization` for direct and tunneled dynamic routes, including when a
caller explicitly supplied that header. Cache keys currently retain credential
and proxy-header bytes in memory until eviction; they are not logged or written
to disk. This retention and its security tradeoff require review. HTTP/3,
local transports, custom connector layers, cancellation, failed connection
setup and route changes on redirect still need targeted verification.

The standalone `experiments/proxy_route_pool.rs` models exact route matching,
bounded LRU eviction and in-flight client ownership. Its three std-only tests
pass for concurrent selection, different credentials, redirect-hop selection,
reuse and eviction. It does not use either reqwest version, open sockets, or
prove the security of the transport. It intentionally retains raw secret bytes
in memory; a production implementation still needs a retention and zeroization
design and must preserve exact, collision-free route identity.

The prototype is pending public CI and independent review. The two reqwest
sources retain their MIT and Apache-2.0 license files. Vendoring is a temporary
maintenance burden: every security update and dependency upgrade must rebase
and revalidate both small patches. Prefer upstreaming this opt-in API and
removing the forks when released; until then monitor advisories for both pinned
versions and update them together with their regression tests. This document
does not claim acceptance of untested transport or OAuth behavior.

Static and dynamic proxy rules now share per-hop route selection. Local sockets
bypass proxy headers, and direct and tunneled requests discard caller-provided
proxy authorization; configure CONNECT credentials on the proxy. HTTP/3 fails
closed when a proxy matches, because its connector does not implement proxy
routing. These changes are pending public CI and independent review.

Caller-provided forward proxy authorization is bound to the first route in a
redirect chain. A change of route removes that authorization before applying
any credentials configured for the newly selected proxy; the same route keeps
the caller header. This behavior is pending public CI and independent review.
