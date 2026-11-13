use serde_json::json;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::time::{Duration, Instant};

// Some hosted runners open idle connections to local listeners. Read each
// connection independently so one idle peer cannot block the real request.
fn accept_proxy_request(listener: TcpListener) -> (TcpStream, BufReader<TcpStream>, String) {
    // Windows duplicates may not retain the original listener’s nonblocking mode.
    // Without this, accept can block after a reader has received the request.
    listener.set_nonblocking(true).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "proxy request not received before deadline"
        );
        if let Ok(request) = receiver.try_recv() {
            return request;
        }
        match listener.accept() {
            Ok((socket, _)) => {
                let sender = sender.clone();
                std::thread::spawn(move || {
                    // Accepted sockets can inherit O_NONBLOCK on macOS.
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    let mut reader = BufReader::new(socket.try_clone().unwrap());
                    let mut first = String::new();
                    if reader.read_line(&mut first).is_ok() && !first.is_empty() {
                        let _ = sender.send((socket, reader, first));
                    }
                });
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("proxy request not received: {error}"),
        }
    }
}

#[test]
fn bedrock_proxy_and_no_proxy_use_effective_request_url() {
    for mode in [
        "env",
        "setting",
        "all",
        "empty-lowercase",
        "bypass",
        "port-mismatch",
        "https",
    ] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        let target_addr = target.local_addr().unwrap();
        let listener = if mode == "bypass" { target } else { proxy };
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = accept_proxy_request(listener);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                headers.push_str(&line);
            }
            if mode == "https" {
                assert!(first.starts_with("CONNECT "), "{first}");
                assert!(
                    headers
                        .to_ascii_lowercase()
                        .contains("proxy-authorization: basic dxnlcjpwyxnz"),
                    "{headers}"
                );
            } else if mode == "bypass" {
                assert!(first.starts_with("POST /model/"), "{first}");
                assert!(
                    !headers.to_ascii_lowercase().contains("proxy-authorization"),
                    "{headers}"
                );
            } else {
                assert!(
                    first.starts_with(&format!("POST http://{target_addr}/model/")),
                    "{first}"
                );
                assert!(
                    headers.to_ascii_lowercase().contains(
                        "proxy-authorization: basic dXNlcjpwYXNz"
                            .to_ascii_lowercase()
                            .as_str()
                    ),
                    "{headers}"
                );
            }
            write!(
                socket,
                "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let proxy_url = format!("http://user:pass@{proxy_addr}");
        let settings = if mode == "setting" {
            json!({"httpProxy":proxy_url,"retry":{"enabled":false}})
        } else {
            json!({"retry":{"enabled":false}})
        };
        std::fs::write(home.join("settings.json"), settings.to_string()).unwrap();
        let endpoint = format!(
            "{}://{target_addr}",
            if mode == "https" { "https" } else { "http" }
        );
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("AWS_REGION", "us-east-1")
            .env("AWS_MAX_ATTEMPTS", "1")
            .env("AWS_ACCESS_KEY_ID", "TESTACCESS")
            .env("AWS_SECRET_ACCESS_KEY", "TESTSECRET")
            .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", endpoint)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
            .env("AWS_CONFIG_FILE", root.path().join("missing-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                root.path().join("missing-credentials"),
            );
        for key in [
            "http_proxy",
            "HTTP_PROXY",
            "https_proxy",
            "HTTPS_PROXY",
            "all_proxy",
            "ALL_PROXY",
            "no_proxy",
            "NO_PROXY",
            "AWS_PROFILE",
            "AWS_SESSION_TOKEN",
            "AWS_BEARER_TOKEN_BEDROCK",
        ] {
            command.env_remove(key);
        }
        if mode == "all" {
            command.env("all_proxy", &proxy_url);
        } else if mode == "empty-lowercase" {
            command.env("http_proxy", "").env("HTTP_PROXY", &proxy_url);
        } else if mode != "setting" {
            command
                .env("http_proxy", &proxy_url)
                .env("https_proxy", &proxy_url);
        }
        if mode == "bypass" {
            command.env("no_proxy", format!("127.0.0.1:{}", target_addr.port()));
        } else if mode == "port-mismatch" {
            let other_port = if target_addr.port() == 80 { 81 } else { 80 };
            command.env("no_proxy", format!("127.0.0.1:{other_port}"));
        }
        let output = command
            .args([
                "--no-session",
                "--no-mcp",
                "--no-trust",
                "--no-tools",
                "--offline",
                "--provider",
                "amazon-bedrock",
                "--model",
                "amazon.nova-2-lite-v1:0",
                "--print",
                "hello",
            ])
            .output()
            .unwrap();
        server.join().unwrap_or_else(|_| {
            panic!(
                "{mode}: no fixture request; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("user:pass"));
    }
}
