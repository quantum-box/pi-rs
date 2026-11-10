//! Validate loopback OAuth redirects before handing a code to a provider.
use std::time::Duration;

use pi_rs::{Error, Result};
use reqwest::Url;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct Callback {
    pub url: Url,
    stream: TcpStream,
}

impl Callback {
    pub fn code(&self) -> Option<String> {
        self.url
            .query_pairs()
            .find_map(|(key, value)| (key == "code").then(|| value.into_owned()))
    }

    pub async fn reply(mut self, success: bool) {
        let (status, message) = if success {
            ("200 OK", "Signed in. You may close this tab.")
        } else {
            ("400 Bad Request", "Authorization failed.")
        };
        reply(&mut self.stream, status, message).await;
    }
}

pub async fn receive(listener: &TcpListener, callback: &Url, state: &str) -> Result<Callback> {
    receive_matching(listener, callback, Some(state)).await
}

/// Match an unguessable callback path for providers that do not return OAuth state.
pub async fn receive_path(listener: &TcpListener, callback: &Url) -> Result<Callback> {
    receive_matching(listener, callback, None).await
}

async fn receive_matching(
    listener: &TcpListener,
    callback: &Url,
    state: Option<&str>,
) -> Result<Callback> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        let read = async {
            while request.len() <= 8192 && !request.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0_u8; 1024];
                let count = stream.read(&mut chunk).await?;
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
            }
            Ok::<_, std::io::Error>(())
        };
        if !tokio::time::timeout(Duration::from_secs(3), read)
            .await
            .is_ok_and(|result| result.is_ok())
        {
            continue;
        }
        if let Some(url) = redirect(&request, callback, state) {
            return Ok(Callback { url, stream });
        }
        reply(&mut stream, "400 Bad Request", "Invalid callback.").await;
    }
}

fn redirect(request: &[u8], callback: &Url, state: Option<&str>) -> Option<Url> {
    if request.len() > 8192 || !request.windows(4).any(|part| part == b"\r\n\r\n") {
        return None;
    }
    let target = std::str::from_utf8(request)
        .ok()?
        .lines()
        .next()?
        .strip_prefix("GET ")?
        .strip_suffix(" HTTP/1.1")?;
    if !target.starts_with('/') || target.starts_with("//") {
        return None;
    }
    let redirect = callback.join(target).ok()?;
    if redirect.origin() != callback.origin()
        || redirect.path() != callback.path()
        || redirect.fragment().is_some()
    {
        return None;
    }
    let mut states = redirect
        .query_pairs()
        .filter_map(|(key, value)| (key == "state").then_some(value));
    if states.next().as_deref() != state || states.next().is_some() {
        return None;
    }
    let responses = redirect
        .query_pairs()
        .filter(|(key, _)| key == "code" || key == "error")
        .count();
    (responses == 1).then_some(redirect)
}

async fn reply(stream: &mut TcpStream, status: &str, message: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{message}",
        message.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

pub async fn open_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = tokio::process::Command::new("open");
        command.arg("--").arg(url);
        command
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = tokio::process::Command::new("rundll32");
        command.arg("url.dll,FileProtocolHandler").arg(url);
        command
    };
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let mut command = {
        let mut command = tokio::process::Command::new("xdg-open");
        command.arg(url);
        command
    };
    command
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let status = tokio::time::timeout(Duration::from_secs(10), command.status())
        .await
        .map_err(|_| Error::Config("browser launch timed out".into()))?
        .map_err(|_| Error::Config("could not launch browser".into()))?;
    if !status.success() {
        return Err(Error::Config("could not launch browser".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_the_callback_origin_path_state_and_result() {
        let callback = Url::parse("http://127.0.0.1:1456/oauth/callback").unwrap();
        let request = |target: &str| format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        let accepted = request("/oauth/callback?state=expected&code=browser-code");
        assert!(redirect(accepted.as_bytes(), &callback, Some("expected")).is_some());
        for target in [
            "/oauth/callback?state=wrong&code=browser-code",
            "/oauth/callback?state=expected&state=expected&code=browser-code",
            "/oauth/callback?state=expected&code=one&code=two",
            "/oauth/callback?state=expected&code=one&error=access_denied",
            "/unrelated?state=expected&code=browser-code",
            "//localhost:1456/oauth/callback?state=expected&code=browser-code",
        ] {
            assert!(redirect(request(target).as_bytes(), &callback, Some("expected")).is_none());
        }
    }

    #[test]
    fn stateless_callbacks_require_the_unique_path() {
        let callback = Url::parse("http://127.0.0.1:12345/oauth/callback/random-id").unwrap();
        let request = |path: &str| format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(
            redirect(
                request("/oauth/callback/random-id?code=good").as_bytes(),
                &callback,
                None
            )
            .is_some()
        );
        for path in [
            "/oauth/callback?code=good",
            "/oauth/callback/other-id?code=good",
            "/oauth/callback/random-id?code=good&state=unexpected",
            "/oauth/callback/random-id?code=good&code=duplicate",
        ] {
            assert!(
                redirect(request(path).as_bytes(), &callback, None).is_none(),
                "{path}"
            );
        }
    }
}
