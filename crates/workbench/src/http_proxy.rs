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
    Ok(vec![Proxy::custom(move |url| {
        if bypasses_proxy(url, &excluded) {
            return None;
        }
        match url.scheme() {
            "http" => http.clone(),
            "https" => https.clone(),
            _ => None,
        }
    })])
}

pub(super) fn configured_mcp(
    settings: &crate::config::Settings,
) -> Result<pi_rs_workbench::mcp::HttpOptions> {
    let (http, https, excluded) = settings_values(settings);
    let http = parse_proxy(http.as_deref())?;
    let https = parse_proxy(https.as_deref())?;
    let mut proxies = Vec::new();
    if http.is_some() || https.is_some() {
        proxies.push(reqwest_mcp::Proxy::custom(move |url| {
            if bypasses_proxy(url, &excluded) {
                return None;
            }
            match url.scheme() {
                "http" => http.clone(),
                "https" => https.clone(),
                _ => None,
            }
        }));
    }
    Ok(pi_rs_workbench::mcp::HttpOptions {
        proxies,
        idle_timeout: settings.http_idle_timeout(),
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
    fn invalid_proxy_does_not_expose_credentials() {
        let error = match proxies(Some("bad://user:secret@example.com"), None, "") {
            Ok(_) => panic!("invalid scheme accepted"),
            Err(error) => error,
        };
        assert!(!error.to_string().contains("secret"));
    }
}
