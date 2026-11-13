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
        "credential-env",
        "credential-profile",
        "credential-bypass",
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
        let listener = if matches!(mode, "bypass" | "credential-bypass") {
            target
        } else {
            proxy
        };
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
            } else if matches!(mode, "bypass" | "credential-bypass") {
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
        if mode.starts_with("credential-") {
            let mut env = json!({"HTTP_PROXY":proxy_url});
            if mode == "credential-bypass" {
                env["NO_PROXY"] = json!(format!("127.0.0.1:{}", target_addr.port()));
            }
            if mode == "credential-profile" {
                env["AWS_PROFILE"] = json!("selected");
            }
            let mut entry = json!({"type":"api_key", "env":env});
            if mode != "credential-profile" {
                entry["key"] = json!("saved-token");
            }
            std::fs::write(
                home.join("auth.json"),
                json!({"amazon-bedrock":entry}).to_string(),
            )
            .unwrap();
        }

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
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        ] {
            command.env_remove(key);
        }
        if mode.starts_with("credential-") {
            command
                .env("http_proxy", "http://127.0.0.1:9")
                .env("NO_PROXY", "not-matching.invalid");
            if mode == "credential-profile" {
                let credentials = root.path().join("credentials");
                std::fs::write(&credentials, "[selected]\naws_access_key_id=PROFILEKEY\naws_secret_access_key=PROFILESECRET\n").unwrap();
                command.env("AWS_SHARED_CREDENTIALS_FILE", credentials);
            }
        } else if mode == "all" {
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

#[test]
fn selected_assume_role_profile_uses_proxy_for_sts_and_bedrock() {
    use std::io::Read;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("auth.json"),
        json!({"amazon-bedrock":{"type":"api_key","env":{"AWS_PROFILE":"selected"}}}).to_string(),
    )
    .unwrap();
    std::fs::write(
        home.join("settings.json"),
        json!({"retry":{"enabled":false}}).to_string(),
    )
    .unwrap();
    let credentials = root.path().join("credentials");
    let config = root.path().join("config");
    std::fs::write(
        &credentials,
        "[source]\naws_access_key_id=SOURCEKEY\naws_secret_access_key=SOURCESECRET\n",
    )
    .unwrap();
    std::fs::write(&config, "[profile selected]\nregion=us-west-2\nrole_arn=arn:aws:iam::123456789012:role/test\nsource_profile=source\n").unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let sts_target = TcpListener::bind("127.0.0.1:0").unwrap();
    let model_target = TcpListener::bind("127.0.0.1:0").unwrap();
    let sts_url = format!("http://{}", sts_target.local_addr().unwrap());
    let model_url = format!("http://{}", model_target.local_addr().unwrap());
    let expected_sts = sts_url.clone();
    let expected_model = model_url.clone();
    let server = std::thread::spawn(move || {
        let mut seen_sts = false;
        let mut seen_model = false;
        for _ in 0..2 {
            let (mut socket, mut reader, first) = accept_proxy_request(proxy.try_clone().unwrap());
            let mut headers = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                headers.push_str(&line);
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            if first.starts_with(&format!("POST {expected_sts}/")) {
                assert!(headers.contains("Credential=SOURCEKEY/"), "{headers}");
                assert!(String::from_utf8_lossy(&body).contains("Action=AssumeRole"));
                let body = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><AssumeRoleResult><Credentials><AccessKeyId>ASSUMEDKEY</AccessKeyId><SecretAccessKey>ASSUMEDSECRET</SecretAccessKey><SessionToken>SESSION</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>fixture</RequestId></ResponseMetadata></AssumeRoleResponse>";
                write!(socket, "HTTP/1.1 200 OK\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).unwrap();
                seen_sts = true;
            } else {
                assert!(
                    first.starts_with(&format!("POST {expected_model}/model/")),
                    "{first}"
                );
                assert!(headers.contains("Credential=ASSUMEDKEY/"), "{headers}");
                write!(
                    socket,
                    "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                )
                .unwrap();
                seen_model = true;
            }
        }
        assert!(seen_sts && seen_model);
    });
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    command
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("AWS_SHARED_CREDENTIALS_FILE", credentials)
        .env("AWS_CONFIG_FILE", config)
        .env("AWS_ENDPOINT_URL_STS", sts_url)
        .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", model_url)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
        .env("AWS_MAX_ATTEMPTS", "1")
        .env("http_proxy", &proxy_url)
        .env_remove("AWS_REGION")
        .env_remove("AWS_DEFAULT_REGION")
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_PROFILE")
        .env_remove("AWS_BEARER_TOKEN_BEDROCK")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_EC2_METADATA_SERVICE_ENDPOINT")
        .env_remove("AWS_CONTAINER_CREDENTIALS_FULL_URI")
        .env_remove("no_proxy")
        .env_remove("NO_PROXY");
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
            "fixture request missing: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(!output.status.success());
}

#[test]
fn instance_metadata_bypasses_proxy_but_bedrock_uses_it() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("auth.json"),
        json!({"amazon-bedrock":{"type":"api_key"}}).to_string(),
    )
    .unwrap();
    std::fs::write(
        home.join("settings.json"),
        json!({"retry":{"enabled":false}}).to_string(),
    )
    .unwrap();
    let metadata = TcpListener::bind("127.0.0.1:0").unwrap();
    let metadata_url = format!("http://{}", metadata.local_addr().unwrap());
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    let target_url = format!("http://{}", target.local_addr().unwrap());
    let metadata_task = std::thread::spawn(move || {
        for index in 0..3 {
            let (mut socket, mut reader, first) =
                accept_proxy_request(metadata.try_clone().unwrap());
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let body = match index {
                0 => {
                    assert!(first.starts_with("PUT /latest/api/token"), "{first}");
                    "fixture-token".to_owned()
                }
                1 => {
                    assert!(
                        first.starts_with("GET /latest/meta-data/iam/security-credentials/"),
                        "{first}"
                    );
                    "fixture-role".to_owned()
                }
                _ => {
                    assert!(first.contains("fixture-role"), "{first}");
                    json!({"Code":"Success","Type":"AWS-HMAC","AccessKeyId":"IMDSKEY","SecretAccessKey":"IMDSSECRET","Token":"SESSION","Expiration":"2099-01-01T00:00:00Z","LastUpdated":"2026-01-01T00:00:00Z"}).to_string()
                }
            };
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nx-aws-ec2-metadata-token-ttl-seconds: 21600\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        }
    });
    let expected = target_url.clone();
    let proxy_task = std::thread::spawn(move || {
        let (mut socket, mut reader, first) = accept_proxy_request(proxy);
        assert!(
            first.starts_with(&format!("POST {expected}/model/")),
            "{first}"
        );
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            headers.push_str(&line);
        }
        assert!(headers.contains("Credential=IMDSKEY/"), "{headers}");
        write!(
            socket,
            "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        )
        .unwrap();
    });
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    command
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_MAX_ATTEMPTS", "1")
        .env("AWS_EC2_METADATA_SERVICE_ENDPOINT", metadata_url)
        .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", target_url)
        .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
        .env("AWS_CONFIG_FILE", root.path().join("missing-config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            root.path().join("missing-credentials"),
        )
        .env("http_proxy", proxy_url);
    for key in [
        "AWS_EC2_METADATA_DISABLED",
        "AWS_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_BEARER_TOKEN_BEDROCK",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "no_proxy",
        "NO_PROXY",
    ] {
        command.env_remove(key);
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
    metadata_task.join().unwrap_or_else(|_| {
        panic!(
            "IMDS fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    proxy_task.join().unwrap_or_else(|_| {
        panic!(
            "proxy fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(!output.status.success());
}

#[test]
fn https_connect_validates_tls_and_keeps_proxy_auth_out_of_origin_request() {
    use std::io::Read;
    use std::sync::Arc;
    for valid_name in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(
            home.join("settings.json"),
            json!({"retry":{"enabled":false}}).to_string(),
        )
        .unwrap();
        let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_url = format!("http://user:pass@{}", proxy.local_addr().unwrap());
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = if valid_name { "127.0.0.1" } else { "localhost" };
        let target_url = format!("https://{host}:{}", target.local_addr().unwrap().port());
        let server = std::thread::spawn(move || {
            let (mut socket, mut reader, first) = accept_proxy_request(proxy);
            assert!(first.starts_with("CONNECT "), "{first}");
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                headers.push_str(&line);
            }
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("proxy-authorization: basic dxnlcjpwyxnz")
            );
            drop(reader);
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let cert = rustls::pki_types::CertificateDer::from(
                include_bytes!("fixtures/bedrock-proxy-tls/server.der").to_vec(),
            );
            let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    include_bytes!("fixtures/bedrock-proxy-tls/server-key.der").to_vec(),
                ),
            );
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let config = rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .unwrap();
            let connection = rustls::ServerConnection::new(Arc::new(config)).unwrap();
            let mut tls = rustls::StreamOwned::new(connection, socket);
            let mut bytes = [0u8; 8192];
            let result = tls.read(&mut bytes);
            if valid_name {
                let count = result.expect("trusted TLS handshake and HTTP request");
                let request = String::from_utf8_lossy(&bytes[..count]);
                assert!(
                    request.starts_with("POST /model/") || request.starts_with("POST https://"),
                    "{request}"
                );
                assert!(!request.to_ascii_lowercase().contains("proxy-authorization"));
                tls.write_all(
                    b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .unwrap();
            } else {
                assert!(result.is_err(), "wrong hostname must fail TLS validation");
            }
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("AWS_REGION", "us-east-1")
            .env("AWS_MAX_ATTEMPTS", "1")
            .env("AWS_ACCESS_KEY_ID", "TESTACCESS")
            .env("AWS_SECRET_ACCESS_KEY", "TESTSECRET")
            .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", target_url)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
            .env("AWS_CONFIG_FILE", root.path().join("missing-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                root.path().join("missing-credentials"),
            )
            .env(
                "SSL_CERT_FILE",
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/bedrock-proxy-tls/ca.pem"
                ),
            )
            .env("https_proxy", proxy_url);
        for key in [
            "SSL_CERT_DIR",
            "http_proxy",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "all_proxy",
            "ALL_PROXY",
            "no_proxy",
            "NO_PROXY",
            "AWS_PROFILE",
            "AWS_SESSION_TOKEN",
            "AWS_BEARER_TOKEN_BEDROCK",
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        ] {
            command.env_remove(key);
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
                "TLS fixture failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
        assert!(!output.status.success());
    }
}
