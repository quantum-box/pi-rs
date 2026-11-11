//! Pi-compatible proxy selection for workbench model HTTP clients.
use pi_rs::{Error, Result};
use reqwest::{Proxy, Url};

fn environment(lower: &str, upper: &str) -> Option<String> {
    std::env::var(lower)
        .ok()
        .or_else(|| std::env::var(upper).ok())
}

pub(super) fn configured(settings: &crate::config::Settings) -> Result<Vec<Proxy>> {
    let fallback = settings
        .string("httpProxy")
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let http = environment("http_proxy", "HTTP_PROXY").or_else(|| fallback.map(str::to_owned));
    let https = environment("https_proxy", "HTTPS_PROXY")
        .or_else(|| fallback.map(str::to_owned))
        .or_else(|| http.clone());
    let excluded = environment("no_proxy", "NO_PROXY").unwrap_or_default();
    proxies(http.as_deref(), https.as_deref(), &excluded)
}

fn proxies(http: Option<&str>, https: Option<&str>, excluded: &str) -> Result<Vec<Proxy>> {
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
    let http = parse(http)?;
    let https = parse(https)?;
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
