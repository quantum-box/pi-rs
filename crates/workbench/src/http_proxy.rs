//! Pi-compatible proxy selection for workbench model HTTP clients.
use pi_rs::{Error, Result};
use reqwest::{Proxy, Url};

fn environment(lower: &str, upper: &str) -> Option<String> {
    std::env::var(lower)
        .ok()
        .or_else(|| std::env::var(upper).ok())
}

pub(super) fn configured(settings: &crate::config::Settings) -> Result<Vec<Proxy>> {
    let (http, https, excluded) = settings_values(settings);
    proxies(http.as_deref(), https.as_deref(), &excluded)
}

fn settings_values(settings: &crate::config::Settings) -> (Option<String>, Option<String>, String) {
    let fallback = settings
        .string("httpProxy")
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let http = environment("http_proxy", "HTTP_PROXY").or_else(|| fallback.map(str::to_owned));
    let https = environment("https_proxy", "HTTPS_PROXY")
        .or_else(|| fallback.map(str::to_owned))
        .or_else(|| http.clone());
    let excluded = environment("no_proxy", "NO_PROXY").unwrap_or_default();
    (http, https, excluded)
}

fn parse_proxy(value: Option<&str>) -> Result<Option<Url>> {
    let parse = |value: Option<&str>| -> Result<Option<Url>> {
        value
            .filter(|value| !value.is_empty())
            .map(|value| {
                let url = Url::parse(value)
                    .map_err(|_| Error::Config("Invalid HTTP proxy URL".into()))?;
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(Error::Config("Invalid HTTP proxy URL".into()));
                }
                Ok(url)
            })
            .transpose()
    };
    parse(value)
}

fn proxies(http: Option<&str>, https: Option<&str>, excluded: &str) -> Result<Vec<Proxy>> {
    let http = parse_proxy(http)?;
    let https = parse_proxy(https)?;
    let excluded = excluded.to_owned();
    if http.is_none() && https.is_none() {
        return Ok(Vec::new());
    }
    Ok(vec![
        Proxy::custom(move |url| {
            if bypasses_proxy(url, &excluded) {
                return None;
            }
            match url.scheme() {
                "http" => http.clone(),
                "https" => https.clone(),
                _ => None,
            }
        })
        .tunnel_http(true),
    ])
}

/// Build a Smithy transport that applies the proxy decision to each request URL.
pub(super) fn bedrock_client(
    settings: &crate::config::Settings,
    provider_env: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<
    Option<(
        aws_smithy_runtime_api::client::http::SharedHttpClient,
        aws_smithy_runtime_api::client::http::SharedHttpClient,
    )>,
> {
    use aws_smithy_http_client::{Connector, proxy::ProxyConfig, tls};
    use aws_smithy_runtime_api::client::http::{SharedHttpConnector, http_client_fn};
    let env = |lower, upper| {
        provider_env
            .and_then(|env| env.get(lower))
            .and_then(serde_json::Value::as_str)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                provider_env
                    .and_then(|env| env.get(upper))
                    .and_then(serde_json::Value::as_str)
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned)
            })
            .or_else(|| {
                std::env::var(lower)
                    .ok()
                    .filter(|v| !v.is_empty())
                    .or_else(|| std::env::var(upper).ok().filter(|v| !v.is_empty()))
            })
    };
    let all = env("all_proxy", "ALL_PROXY");
    let fallback = settings
        .string("httpProxy")
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let http = env("http_proxy", "HTTP_PROXY")
        .or_else(|| all.clone())
        .or_else(|| fallback.map(str::to_owned));
    let https = env("https_proxy", "HTTPS_PROXY")
        .or(all)
        .or_else(|| fallback.map(str::to_owned));
    let excluded = env("no_proxy", "NO_PROXY").unwrap_or_default();
    // Match the container credential endpoint itself, not its origin: STS or
    // other SDK services may be configured on the same host and port.
    let container_endpoint =
        if let Ok(relative) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            let base = Url::parse("http://169.254.170.2/").expect("valid ECS endpoint");
            base.join(&relative)
                .ok()
                .filter(|url| url.origin() == base.origin())
        } else {
            std::env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI")
                .ok()
                .and_then(|value| Url::parse(&value).ok())
        };
    let http = parse_proxy(http.as_deref())?;
    let https = parse_proxy(https.as_deref())?;
    if http.is_none() && https.is_none() {
        return Ok(None);
    }
    let proxy = |value: Option<Url>| -> Result<Option<ProxyConfig>> {
        value
            .map(|url| {
                ProxyConfig::all(url.as_str())
                    .map_err(|_| Error::Config("Invalid HTTP proxy URL".into()))
            })
            .transpose()
    };
    let http = proxy(http)?;
    let https = proxy(https)?;
    let make_client = |credential_traffic: bool| {
        let http = http.clone();
        let https = https.clone();
        let excluded = excluded.clone();
        let container_endpoint = container_endpoint.clone();
        http_client_fn(move |settings, _components| {
            let connector = |config: ProxyConfig| {
                SharedHttpConnector::new(
                    Connector::builder()
                        .connector_settings(settings.clone())
                        .proxy_config(config)
                        .tls_provider(tls::Provider::Rustls(
                            tls::rustls_provider::CryptoMode::AwsLc,
                        ))
                        .build(),
                )
            };
            let direct = connector(ProxyConfig::disabled());
            let http = http.clone().map(connector);
            let https = https.clone().map(connector);
            SharedHttpConnector::new(BedrockConnector {
                direct,
                http,
                https,
                excluded: excluded.clone(),
                container_endpoint: container_endpoint.clone(),
                credential_traffic,
            })
        })
    };
    Ok(Some((make_client(false), make_client(true))))
}

struct BedrockConnector {
    direct: aws_smithy_runtime_api::client::http::SharedHttpConnector,
    http: Option<aws_smithy_runtime_api::client::http::SharedHttpConnector>,
    https: Option<aws_smithy_runtime_api::client::http::SharedHttpConnector>,
    excluded: String,
    container_endpoint: Option<Url>,
    credential_traffic: bool,
}
impl std::fmt::Debug for BedrockConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BedrockConnector")
    }
}
impl aws_smithy_runtime_api::client::http::HttpConnector for BedrockConnector {
    fn call(
        &self,
        request: aws_smithy_runtime_api::client::orchestrator::HttpRequest,
    ) -> aws_smithy_runtime_api::client::http::HttpConnectorFuture {
        let Ok(url) = Url::parse(request.uri()) else {
            use aws_smithy_runtime_api::client::{
                http::HttpConnectorFuture, result::ConnectorError,
            };
            return HttpConnectorFuture::new(async {
                Err(ConnectorError::user(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid AWS request URL",
                ))))
            });
        };
        // The SDK also permits an IMDS endpoint in an AWS profile, which is
        // not available in the process environment. Recognize IMDSv2's token
        // and authenticated metadata requests so those remain direct too.
        let imds_request = (request.method() == "PUT"
            && url.path().ends_with("/latest/api/token")
            && request
                .headers()
                .get("x-aws-ec2-metadata-token-ttl-seconds")
                .is_some())
            || (request.method() == "GET"
                && url.path().contains("/latest/meta-data/")
                && request.headers().get("x-aws-ec2-metadata-token").is_some());
        let container_request = request.method() == "GET"
            && self
                .container_endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint == &url);
        let metadata = imds_request || container_request;
        let proxy = if (self.credential_traffic && metadata)
            || bedrock_bypasses_proxy(&url, &self.excluded)
        {
            None
        } else {
            match url.scheme() {
                "http" => self.http.as_ref(),
                "https" => self.https.as_ref(),
                _ => None,
            }
        };
        proxy.unwrap_or(&self.direct).call(request)
    }
}

fn bedrock_bypasses_proxy(url: &Url, excluded: &str) -> bool {
    // Pi treats both *.example.com and *example.com as domain suffixes.
    let normalized = excluded
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(|entry| {
            entry
                .strip_prefix('*')
                .filter(|tail| !tail.is_empty() && !tail.starts_with(':'))
                .map_or_else(
                    || entry.to_owned(),
                    |tail| {
                        if tail.starts_with('.') {
                            tail.to_owned()
                        } else {
                            format!(".{tail}")
                        }
                    },
                )
        })
        .collect::<Vec<_>>()
        .join(",");
    bypasses_proxy(url, &normalized)
}

pub(super) fn configured_mcp(
    settings: &crate::config::Settings,
) -> Result<pi_rs_workbench::mcp::HttpOptionsWithProxyHosts> {
    let (http, https, excluded) = settings_values(settings);
    let http = parse_proxy(http.as_deref())?;
    let https = parse_proxy(https.as_deref())?;
    let proxy_hosts = [http.as_ref(), https.as_ref()]
        .into_iter()
        .flatten()
        .filter_map(|url| url.host_str().map(str::to_owned))
        .collect();
    let mut proxies = Vec::new();
    if http.is_some() || https.is_some() {
        proxies.push(
            reqwest_mcp::Proxy::custom(move |url| {
                if bypasses_proxy(url, &excluded) {
                    return None;
                }
                match url.scheme() {
                    "http" => http.clone(),
                    "https" => https.clone(),
                    _ => None,
                }
            })
            .tunnel_http(true),
        );
    }
    Ok(pi_rs_workbench::mcp::HttpOptionsWithProxyHosts {
        http: pi_rs_workbench::mcp::HttpOptions {
            proxies,
            idle_timeout: settings.http_idle_timeout(),
        },
        proxy_hosts,
    })
}

fn bypasses_proxy(url: &Url, excluded: &str) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host
        .trim_matches(['[', ']'])
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let port = url.port_or_known_default();
    excluded
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .any(|entry| {
            let (name, required_port) = if let Some(rest) = entry.strip_prefix('[') {
                match rest.split_once(']') {
                    Some((name, "")) => (name, None),
                    Some((name, suffix)) => {
                        match suffix.strip_prefix(':').and_then(|p| p.parse::<u16>().ok()) {
                            Some(port) => (name, Some(port)),
                            None => (entry, None),
                        }
                    }
                    None => (entry, None),
                }
            } else if entry.matches(':').count() == 1 {
                match entry
                    .rsplit_once(':')
                    .and_then(|(name, port)| port.parse::<u16>().ok().map(|port| (name, port)))
                {
                    Some((name, port)) => (name, Some(port)),
                    None => (entry, None),
                }
            } else {
                (entry, None)
            };
            if required_port.is_some_and(|required| Some(required) != port) {
                return false;
            }
            if name == "*" {
                return true;
            }
            let wildcard = name.starts_with("*.");
            let name = name
                .strip_prefix("*.")
                .or_else(|| name.strip_prefix('.'))
                .unwrap_or(name)
                .trim_end_matches('.')
                .to_ascii_lowercase();
            (!wildcard && host == name)
                || host
                    .strip_suffix(&name)
                    .is_some_and(|prefix| prefix.ends_with('.'))
        })
}

#[cfg(test)]
pub(super) async fn test_proxy(
    response: &'static str,
) -> (Proxy, tokio::task::JoinHandle<(String, String, Vec<u8>)>) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::all(format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let mut socket = BufReader::new(socket);
        let mut first = String::new();
        socket.read_line(&mut first).await.unwrap();
        let mut headers = String::new();
        let mut size = 0;
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                size = value.trim().parse().unwrap();
            }
            headers.push_str(&line);
        }
        let mut body = vec![0; size];
        socket.read_exact(&mut body).await.unwrap();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        (first, headers, body)
    });
    (proxy, task)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! route_pool_loopback {
        ($test:ident, $client:ident) => {
            #[tokio::test]
            async fn $test() {
                use std::sync::{
                    Arc,
                    atomic::{AtomicUsize, Ordering},
                };
                use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
                use tokio::net::{TcpListener, TcpStream};
                use tokio::time::{Duration, timeout};
                #[derive(Clone)]
                struct CountingLayer(Arc<AtomicUsize>);
                impl<S> tower::Layer<S> for CountingLayer {
                    type Service = S;
                    fn layer(&self, service: S) -> S {
                        self.0.fetch_add(1, Ordering::SeqCst);
                        service
                    }
                }
                async fn request(socket: &mut BufReader<TcpStream>) -> String {
                    let mut result = String::new();
                    loop {
                        let mut line = String::new();
                        timeout(Duration::from_secs(5), socket.read_line(&mut line))
                            .await
                            .unwrap()
                            .unwrap();
                        if line == "\r\n" || line.is_empty() {
                            break;
                        }
                        result.push_str(&line);
                    }
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                    result
                }
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = calls.clone();
                let proxy = $client::Proxy::custom(move |_| {
                    let password = if count.fetch_add(1, Ordering::SeqCst) < 2 {
                        "one"
                    } else {
                        "two"
                    };
                    Some(format!("http://user:{password}@{address}"))
                });
                let server = tokio::spawn(async move {
                    let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                    let mut socket = BufReader::new(socket);
                    let first = request(&mut socket).await;
                    let second = request(&mut socket).await;
                    let (other, _) = timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                    let third = request(&mut BufReader::new(other)).await;
                    (first, second, third)
                });
                let layers = Arc::new(AtomicUsize::new(0));
                let client = $client::Client::builder()
                    .no_proxy()
                    .proxy(proxy)
                    .connector_layer(CountingLayer(layers.clone()))
                    .build()
                    .unwrap();
                for _ in 0..3 {
                    let response = timeout(
                        Duration::from_secs(5),
                        client.get("http://origin.invalid/test").send(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    assert!(response.status().is_success());
                }
                let (first, second, third) = server.await.unwrap();
                let auth = |request: &str| {
                    request
                        .lines()
                        .find(|line| {
                            line.to_ascii_lowercase()
                                .starts_with("proxy-authorization:")
                        })
                        .unwrap()
                        .to_owned()
                };
                assert_eq!(auth(&first), auth(&second));
                assert_ne!(auth(&first), auth(&third));
                assert_eq!(calls.load(Ordering::SeqCst), 3);
                assert_eq!(layers.load(Ordering::SeqCst), 1);
            }
        };
    }
    route_pool_loopback!(
        route_pool_reuses_matching_proxy_and_isolates_credentials,
        reqwest
    );
    route_pool_loopback!(
        mcp_route_pool_reuses_matching_proxy_and_isolates_credentials,
        reqwest_mcp
    );

    macro_rules! concurrent_routes_loopback {
        ($test:ident, $client:ident) => {
            #[tokio::test]
            async fn $test() {
                use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
                use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
                use tokio::net::TcpListener;
                use tokio::time::{Duration, timeout};
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = calls.clone();
                let proxy = $client::Proxy::custom(move |_| {
                    let id = count.fetch_add(1, Ordering::SeqCst);
                    Some(format!("http://user:secret{id}@{address}"))
                });
                let server = tokio::spawn(async move {
                    let mut workers = Vec::new();
                    for _ in 0..8 {
                        let (socket, _) = timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
                        workers.push(tokio::spawn(async move {
                            let mut socket = BufReader::new(socket);
                            let mut authorization = String::new();
                            loop {
                                let mut line = String::new();
                                timeout(Duration::from_secs(5), socket.read_line(&mut line)).await.unwrap().unwrap();
                                if line == "\r\n" || line.is_empty() { break; }
                                if line.to_ascii_lowercase().starts_with("proxy-authorization:") { authorization = line; }
                            }
                            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                            authorization
                        }));
                    }
                    let mut headers = std::collections::HashSet::new();
                    for worker in workers { headers.insert(worker.await.unwrap()); }
                    headers
                });
                let client = $client::Client::builder().no_proxy().proxy(proxy).build().unwrap();
                let mut workers = Vec::new();
                for _ in 0..8 {
                    let client = client.clone();
                    workers.push(tokio::spawn(async move {
                        timeout(Duration::from_secs(5), client.get("http://origin.invalid/test").send()).await.unwrap().unwrap().status()
                    }));
                }
                for worker in workers { assert!(worker.await.unwrap().is_success()); }
                let headers = server.await.unwrap();
                assert_eq!(headers.len(), 8);
                assert!(!headers.contains(""));
                assert_eq!(calls.load(Ordering::SeqCst), 8);
            }
        };
    }
    concurrent_routes_loopback!(concurrent_proxy_credentials_are_isolated, reqwest);
    concurrent_routes_loopback!(mcp_concurrent_proxy_credentials_are_isolated, reqwest_mcp);

    #[tokio::test]
    async fn nonmatching_custom_rule_keeps_authenticated_forward_proxy() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut first = String::new();
            timeout(Duration::from_secs(5), socket.read_line(&mut first))
                .await
                .unwrap()
                .unwrap();
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                headers.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            (first, headers)
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(reqwest::Proxy::custom(|_| None::<String>))
            .proxy(
                reqwest::Proxy::all(format!("http://{address}"))
                    .unwrap()
                    .basic_auth("user", "pass"),
            )
            .build()
            .unwrap();
        let response = timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid/test").send(),
        )
        .await;
        let (first, headers) = server.await.unwrap();
        assert!(
            first.starts_with("GET http://origin.invalid/test HTTP/1.1"),
            "{first}"
        );
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("proxy-authorization: basic dxnlcjpwyxnz")
        );
        assert!(response.unwrap().unwrap().status().is_success());
    }

    #[tokio::test]
    async fn mcp_nonmatching_custom_rule_keeps_authenticated_forward_proxy() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut first = String::new();
            timeout(Duration::from_secs(5), socket.read_line(&mut first))
                .await
                .unwrap()
                .unwrap();
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                headers.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            (first, headers)
        });
        let client = reqwest_mcp::Client::builder()
            .no_proxy()
            .proxy(reqwest_mcp::Proxy::custom(|_| None::<String>))
            .proxy(
                reqwest_mcp::Proxy::all(format!("http://{address}"))
                    .unwrap()
                    .basic_auth("user", "pass"),
            )
            .build()
            .unwrap();
        let response = timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid/test").send(),
        )
        .await;
        let (first, headers) = server.await.unwrap();
        assert!(
            first.starts_with("GET http://origin.invalid/test HTTP/1.1"),
            "{first}"
        );
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("proxy-authorization: basic dxnlcjpwyxnz")
        );
        assert!(response.unwrap().unwrap().status().is_success());
    }

    #[tokio::test]
    async fn stateful_proxy_callback_does_not_leak_credentials_to_direct_origin() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = origin.local_addr().unwrap();
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://user:secret@{}", proxy.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let proxy_config = reqwest::Proxy::custom(move |_| {
            (counter.fetch_add(1, Ordering::SeqCst) == 0).then(|| proxy_url.clone())
        });
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), proxy.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut forwarded = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                forwarded.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let (socket, _) = timeout(Duration::from_secs(5), origin.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut direct = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                direct.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            (forwarded, direct)
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy_config)
            .build()
            .unwrap();
        for _ in 0..2 {
            let response = timeout(
                Duration::from_secs(5),
                client.get(format!("http://{target}/test")).send(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(response.status().is_success());
        }
        let (forwarded, direct) = server.await.unwrap();
        assert!(forwarded.starts_with(&format!("GET http://{target}/test HTTP/1.1\r\n")));
        assert!(
            forwarded
                .to_ascii_lowercase()
                .contains("proxy-authorization: basic dxnlcjpzzwnyzxq=")
        );
        assert!(direct.starts_with("GET /test HTTP/1.1\r\n"));
        assert!(!direct.to_ascii_lowercase().contains("proxy-authorization:"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn mcp_stateful_proxy_callback_does_not_leak_credentials_to_direct_origin() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = origin.local_addr().unwrap();
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://user:secret@{}", proxy.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let proxy_config = reqwest_mcp::Proxy::custom(move |_| {
            (counter.fetch_add(1, Ordering::SeqCst) == 0).then(|| proxy_url.clone())
        });
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), proxy.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut forwarded = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                forwarded.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let (socket, _) = timeout(Duration::from_secs(5), origin.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut direct = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                direct.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            (forwarded, direct)
        });
        let client = reqwest_mcp::Client::builder()
            .no_proxy()
            .proxy(proxy_config)
            .build()
            .unwrap();
        for _ in 0..2 {
            let response = timeout(
                Duration::from_secs(5),
                client.get(format!("http://{target}/test")).send(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(response.status().is_success());
        }
        let (forwarded, direct) = server.await.unwrap();
        assert!(forwarded.starts_with(&format!("GET http://{target}/test HTTP/1.1\r\n")));
        assert!(
            forwarded
                .to_ascii_lowercase()
                .contains("proxy-authorization: basic dxnlcjpzzwnyzxq=")
        );
        assert!(direct.starts_with("GET /test HTTP/1.1\r\n"));
        assert!(!direct.to_ascii_lowercase().contains("proxy-authorization:"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http_proxy_connects_before_request_without_leaking_proxy_credentials() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!(
            "http://proxy-user:proxy-pass@{}",
            listener.local_addr().unwrap()
        );
        let proxy = proxies(Some(&proxy_url), None, "").unwrap().pop().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut first = String::new();
            timeout(Duration::from_secs(5), socket.read_line(&mut first))
                .await
                .unwrap()
                .unwrap();
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                timeout(Duration::from_secs(5), socket.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                headers.push_str(&line);
            }
            if !first.starts_with("CONNECT ") {
                socket.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                return (first, headers, String::new());
            }
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut origin = String::new();
            loop {
                let mut line = String::new();
                timeout(Duration::from_secs(5), socket.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                origin.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
            (first, headers, origin)
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .proxy(
                reqwest::Proxy::all("http://127.0.0.1:1")
                    .unwrap()
                    .basic_auth("later", "secret"),
            )
            .build()
            .unwrap();
        let response = timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid:8080/test").send(),
        )
        .await
        .unwrap()
        .unwrap();
        let (first, headers, origin) = server.await.unwrap();
        assert_eq!(first, "CONNECT origin.invalid:8080 HTTP/1.1\r\n");
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("proxy-authorization: basic chjvehktdxnlcjpwcm94es1wyxnz")
        );
        assert!(origin.starts_with("GET /test HTTP/1.1\r\n"));
        assert!(!origin.to_ascii_lowercase().contains("proxy-authorization:"));
        assert!(response.status().is_success());
    }

    #[tokio::test]
    async fn mcp_http_proxy_uses_connect() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = reqwest_mcp::Proxy::all(format!("http://{}", listener.local_addr().unwrap()))
            .unwrap()
            .tunnel_http(true);
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut first = String::new();
            timeout(Duration::from_secs(5), socket.read_line(&mut first))
                .await
                .unwrap()
                .unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            first
        });
        let client = reqwest_mcp::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .build()
            .unwrap();
        let _ = timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid:8080/mcp").send(),
        )
        .await
        .unwrap();
        assert_eq!(
            server.await.unwrap(),
            "CONNECT origin.invalid:8080 HTTP/1.1\r\n"
        );
    }

    #[tokio::test]
    async fn https_still_uses_authenticated_connect() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let proxy = proxies(None, Some(&format!("http://user:pass@{address}")), "")
            .unwrap()
            .pop()
            .unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                timeout(Duration::from_secs(5), socket.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                request.push_str(&line);
            }
            socket
                .write_all(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            request
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .build()
            .unwrap();
        let _ = timeout(
            Duration::from_secs(5),
            client.get("https://origin.invalid/test").send(),
        )
        .await
        .unwrap();
        let request = server.await.unwrap();
        assert!(request.starts_with("CONNECT origin.invalid:443 HTTP/1.1\r\n"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("proxy-authorization: basic dxnlcjpwyxnz")
        );
    }

    #[tokio::test]
    async fn https_tunnel_preserves_origin_tls_verification() {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::sync::Arc;
        use std::time::Duration;
        for valid_name in [true, false] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let proxy = proxies(
                Some(&format!(
                    "http://user:pass@{}",
                    listener.local_addr().unwrap()
                )),
                Some(&format!(
                    "http://user:pass@{}",
                    listener.local_addr().unwrap()
                )),
                "",
            )
            .unwrap()
            .pop()
            .unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut first = String::new();
                reader.read_line(&mut first).unwrap();
                assert!(first.starts_with("CONNECT "));
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                drop(reader);
                socket
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .unwrap();
                let cert = rustls::pki_types::CertificateDer::from(
                    include_bytes!("../tests/fixtures/bedrock-proxy-tls/server.der").to_vec(),
                );
                let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        include_bytes!("../tests/fixtures/bedrock-proxy-tls/server-key.der")
                            .to_vec(),
                    ),
                );
                let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                    rustls::crypto::aws_lc_rs::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .unwrap();
                let mut tls = rustls::StreamOwned::new(
                    rustls::ServerConnection::new(Arc::new(config)).unwrap(),
                    socket,
                );
                let mut request = [0u8; 4096];
                let result = tls.read(&mut request);
                if valid_name {
                    let n = result.unwrap();
                    let request = String::from_utf8_lossy(&request[..n]);
                    assert!(request.starts_with("GET /test HTTP/1.1"));
                    assert!(
                        !request
                            .to_ascii_lowercase()
                            .contains("proxy-authorization:")
                    );
                    tls.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                } else {
                    assert!(result.is_err());
                }
            });
            let cert = reqwest::Certificate::from_pem(include_bytes!(
                "../tests/fixtures/bedrock-proxy-tls/ca.pem"
            ))
            .unwrap();
            let client = reqwest::Client::builder()
                .no_proxy()
                .proxy(proxy)
                .tls_built_in_root_certs(false)
                .add_root_certificate(cert)
                .build()
                .unwrap();
            let host = if valid_name { "127.0.0.1" } else { "localhost" };
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.get(format!("https://{host}:8443/test")).send(),
            )
            .await
            .unwrap();
            assert_eq!(result.is_ok(), valid_name);
            server.join().unwrap();
        }
    }

    #[tokio::test]
    async fn http_origin_uses_tls_proxy_and_keeps_auth_out_of_tunnel() {
        use std::io::{BufRead, BufReader, Write};
        use std::sync::Arc;
        use std::time::Duration;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_url = format!("https://user:pass@{}", listener.local_addr().unwrap());
        let proxy = proxies(Some(&proxy_url), None, "").unwrap().pop().unwrap();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let cert = rustls::pki_types::CertificateDer::from(
                include_bytes!("../tests/fixtures/bedrock-proxy-tls/server.der").to_vec(),
            );
            let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    include_bytes!("../tests/fixtures/bedrock-proxy-tls/server-key.der").to_vec(),
                ),
            );
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
            let tls = rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(config)).unwrap(),
                socket,
            );
            let mut tls = BufReader::new(tls);
            let mut connect = String::new();
            loop {
                let mut line = String::new();
                tls.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                connect.push_str(&line);
            }
            assert!(connect.starts_with("CONNECT origin.invalid:80 HTTP/1.1\r\n"));
            assert!(
                connect
                    .to_ascii_lowercase()
                    .contains("proxy-authorization: basic dxnlcjpwyxnz")
            );
            tls.get_mut()
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .unwrap();
            let mut origin = String::new();
            loop {
                let mut line = String::new();
                tls.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                origin.push_str(&line);
            }
            assert!(origin.starts_with("GET /test HTTP/1.1\r\n"));
            assert!(!origin.to_ascii_lowercase().contains("proxy-authorization:"));
            tls.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        let cert = reqwest::Certificate::from_pem(include_bytes!(
            "../tests/fixtures/bedrock-proxy-tls/ca.pem"
        ))
        .unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .tls_built_in_root_certs(false)
            .add_root_certificate(cert)
            .build()
            .unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid/test").send(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.status().is_success());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn mcp_http_origin_uses_tls_proxy_and_keeps_auth_out_of_tunnel() {
        use std::io::{BufRead, BufReader, Write};
        use std::sync::Arc;
        use std::time::Duration;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_url = format!("https://user:pass@{}", listener.local_addr().unwrap());
        let proxy = reqwest_mcp::Proxy::all(&proxy_url)
            .unwrap()
            .tunnel_http(true);
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let cert = rustls::pki_types::CertificateDer::from(
                include_bytes!("../tests/fixtures/bedrock-proxy-tls/server.der").to_vec(),
            );
            let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    include_bytes!("../tests/fixtures/bedrock-proxy-tls/server-key.der").to_vec(),
                ),
            );
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
            let tls = rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(config)).unwrap(),
                socket,
            );
            let mut tls = BufReader::new(tls);
            let mut connect = String::new();
            loop {
                let mut line = String::new();
                tls.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                connect.push_str(&line);
            }
            assert!(connect.starts_with("CONNECT origin.invalid:80 HTTP/1.1\r\n"));
            assert!(
                connect
                    .to_ascii_lowercase()
                    .contains("proxy-authorization: basic dxnlcjpwyxnz")
            );
            tls.get_mut()
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .unwrap();
            let mut origin = String::new();
            loop {
                let mut line = String::new();
                tls.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                origin.push_str(&line);
            }
            assert!(origin.starts_with("GET /test HTTP/1.1\r\n"));
            assert!(!origin.to_ascii_lowercase().contains("proxy-authorization:"));
            tls.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        let cert = reqwest_mcp::Certificate::from_pem(include_bytes!(
            "../tests/fixtures/bedrock-proxy-tls/ca.pem"
        ))
        .unwrap();
        let client = reqwest_mcp::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .tls_certs_only([cert])
            .build()
            .unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid/test").send(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.status().is_success());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn bypassed_http_request_does_not_receive_proxy_credentials() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let proxy = proxies(
            Some("http://user:pass@127.0.0.1:1"),
            None,
            &address.to_string(),
        )
        .unwrap()
        .pop()
        .unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                timeout(Duration::from_secs(5), socket.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                request.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            request
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .build()
            .unwrap();
        let response = timeout(
            Duration::from_secs(5),
            client.get(format!("http://{address}/test")).send(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.status().is_success());
        let request = server.await.unwrap();
        assert!(request.starts_with("GET /test HTTP/1.1\r\n"));
        assert!(
            !request
                .to_ascii_lowercase()
                .contains("proxy-authorization:")
        );
    }

    #[tokio::test]
    async fn redirect_from_proxy_to_no_proxy_does_not_leak_auth() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::time::{Duration, timeout};
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = origin.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://user:pass@{}", listener.local_addr().unwrap());
        let proxy = proxies(Some(&proxy_url), None, &address.to_string())
            .unwrap()
            .pop()
            .unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut connect = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                connect.push_str(&line);
            }
            assert!(connect.starts_with("CONNECT origin.invalid:80 HTTP/1.1\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            socket.write_all(format!("HTTP/1.1 302 Found\r\nLocation: http://{address}/end\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            let (socket, _) = timeout(Duration::from_secs(5), origin.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                request.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            request
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .build()
            .unwrap();
        let response = timeout(
            Duration::from_secs(5),
            client.get("http://origin.invalid/start").send(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.status().is_success());
        let request = server.await.unwrap();
        assert!(request.starts_with("GET /end HTTP/1.1\r\n"));
        assert!(
            !request
                .to_ascii_lowercase()
                .contains("proxy-authorization:")
        );
    }
    #[test]
    fn bypass_rules_match_domains_ports_wildcards_and_ipv6() {
        let url = |url| Url::parse(url).unwrap();
        assert!(bypasses_proxy(
            &url("https://api.example.com"),
            ".example.com"
        ));
        assert!(bypasses_proxy(&url("https://example.com"), ".example.com"));
        assert!(!bypasses_proxy(
            &url("https://example.com"),
            "*.example.com"
        ));
        assert!(bypasses_proxy(
            &url("https://api.example.com"),
            "*.example.com"
        ));
        assert!(!bypasses_proxy(
            &url("https://example.com"),
            "example.com:80"
        ));
        assert!(bypasses_proxy(&url("https://[::1]:8443"), "[::1]:8443"));
        assert!(!bypasses_proxy(&url("https://[::1]:443"), "[::1]:8443"));
        assert!(bypasses_proxy(&url("http://a.test:80"), "x, *:80"));
        assert!(!bypasses_proxy(
            &url("http://badexample.com"),
            "example.com"
        ));
    }
    #[test]
    fn bedrock_wildcard_bypass_includes_apex_like_pi() {
        let apex = Url::parse("https://example.com").unwrap();
        let child = Url::parse("https://api.example.com").unwrap();
        assert!(bedrock_bypasses_proxy(&apex, "*.example.com"));
        assert!(bedrock_bypasses_proxy(&child, "*.example.com"));
        assert!(bedrock_bypasses_proxy(&apex, "*example.com"));
        assert!(bedrock_bypasses_proxy(&child, "*example.com"));
        assert!(!bedrock_bypasses_proxy(
            &Url::parse("https://badexample.com").unwrap(),
            "*example.com"
        ));
        assert!(bedrock_bypasses_proxy(&apex, "*"));
        assert!(bedrock_bypasses_proxy(&apex, "*:443"));
        assert!(!bedrock_bypasses_proxy(&apex, "*:80"));
        assert!(bedrock_bypasses_proxy(
            &Url::parse("http://example.com:8080").unwrap(),
            "other.test, *:8080"
        ));
        assert!(!bedrock_bypasses_proxy(
            &Url::parse("http://example.com:8081").unwrap(),
            "other.test, *:8080"
        ));
        assert!(!bedrock_bypasses_proxy(&apex, "*.example.com:80"));
    }

    #[test]
    fn invalid_proxy_does_not_expose_credentials() {
        let error = match proxies(Some("bad://user:secret@example.com"), None, "") {
            Ok(_) => panic!("invalid scheme accepted"),
            Err(error) => error,
        };
        assert!(!error.to_string().contains("secret"));
    }
}
