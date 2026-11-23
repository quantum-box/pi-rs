use serde_json::json;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::time::{Duration, Instant};

#[test]
fn pending_radius_stops_auto_share_but_explicit_api_key_still_works() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let session = root.path().join("session.jsonl");
    drop(pi_rs::Session::open(&session).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let endpoint = format!("{origin}v1/artifacts");
    let auth = json!({"radius":{"type":"api_key","_piRsRefreshPending":{"attempt":"fixture","gateway":origin}},"other":{"type":"api_key","key":"keep"}}).to_string();
    std::fs::write(home.join("auth.json"), &auth).unwrap();
    let run = |key: Option<&str>| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pi"));
        cmd.current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("PI_RADIUS_GATEWAY", &origin)
            .env("PI_RS_RADIUS_SHARE_URL", &endpoint)
            .env_remove("RADIUS_API_KEY")
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--provider",
                "ollama",
                "--model",
                "fixture",
                "--endpoint",
                "http://127.0.0.1:9",
            ])
            .arg("--session")
            .arg(&session)
            .arg("share");
        if let Some(key) = key {
            cmd.env("RADIUS_API_KEY", key);
        }
        cmd
    };
    let failed = run(None).output().unwrap();
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("unknown outcome"));
    assert!(listener.accept().is_err());
    assert_eq!(
        std::fs::read_to_string(home.join("auth.json")).unwrap(),
        auth
    );

    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(error) => panic!("share request not received: {error}"),
            }
        };
        // macOS can inherit O_NONBLOCK from the listener on accepted sockets.
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut headers = String::new();
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
            headers.push_str(&line.to_ascii_lowercase());
        }
        assert!(headers.contains("authorization: bearer explicit-fixture"));
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes).unwrap();
        let response = r#"{"artifact":{"canonical_url":"https://example.com/shared"}}"#;
        write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).unwrap();
    });
    let succeeded = run(Some("explicit-fixture")).output().unwrap();
    server.join().unwrap();
    assert!(
        succeeded.status.success(),
        "{}",
        String::from_utf8_lossy(&succeeded.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(home.join("auth.json")).unwrap(),
        auth
    );
}

#[cfg(unix)]
#[test]
fn standalone_share_finishes_admitted_radius_refresh_on_interrupt() {
    use std::os::unix::fs::PermissionsExt;
    use std::process::Stdio;

    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    // Test both preparation via MCP and authorization of the artifact upload.
    for (mcp, fail_save) in [(false, false), (true, false), (true, true)] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir(&home).unwrap();
        let session = root.path().join("session.jsonl");
        drop(pi_rs::Session::open(&session).unwrap());
        let journal_before = std::fs::read(&session).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let auth = home.join("auth.json");
        std::fs::write(
            &auth,
            json!({
                "radius": {"type":"oauth","gateway":origin,"access":"old","refresh":"old-refresh","expires":0},
                "other": {"type":"api_key","key":"keep"}
            })
            .to_string(),
        )
        .unwrap();
        if mcp {
            std::fs::write(
                home.join("mcp.json"),
                json!({
                    "mcpServers": {
                        "fixture": {"url": format!("{origin}mcp"), "auth":{"provider":"radius"}}
                    }
                })
                .to_string(),
            )
            .unwrap();
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("PI_RADIUS_GATEWAY", &origin)
            .env_remove("PI_RS_RADIUS_SHARE_URL")
            .env_remove("RADIUS_API_KEY")
            .env_remove("PI_RS_MODEL_TOKEN")
            .args([
                "--offline",
                "--no-trust",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--provider",
                "ollama",
                "--model",
                "fixture",
            ])
            .arg("--session")
            .arg(&session)
            .arg("share")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = FixtureChild(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    assert!(
                        child.0.try_wait().unwrap().is_none(),
                        "pi exited before OAuth"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("OAuth request not received: {error}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut first = String::new();
        reader.read_line(&mut first).unwrap();
        assert!(first.starts_with("POST /v1/oauth/token "), "{first}");
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                break;
            }
            if let Some((key, value)) = line.split_once(':')
                && key.eq_ignore_ascii_case("content-length")
            {
                content_length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        assert!(String::from_utf8_lossy(&body).contains("refresh_token=old-refresh"));

        let pending: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&auth).unwrap()).unwrap();
        assert!(pending["radius"]["_piRsRefreshPending"].is_object());
        assert!(pending["radius"]["refresh"].is_null());

        // On the save-failure case, let MCP surface the result under normal
        // execution. The broker must prevent a metadata upload even if rmcp
        // reports only a generic authentication error.
        if fail_save {
            std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o500)).unwrap();
        } else {
            assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGINT) }, 0);
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "pi abandoned an admitted refresh"
            );
        }
        let response = r#"{"access_token":"new","refresh_token":"rotated","expires_in":3600}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "pi did not finish");
            std::thread::sleep(Duration::from_millis(10));
        };
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut stderr = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        let after: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&auth).unwrap()).unwrap();
        assert_eq!(after["other"]["key"], "keep");
        if fail_save {
            assert_eq!(status.code(), Some(1), "{stderr}");
            assert!(stderr.contains("could not be durably saved"), "{stderr}");
            assert!(after["radius"]["_piRsRefreshPending"].is_object());
        } else {
            assert_eq!(status.code(), Some(130), "{stderr}");
            assert_eq!(after["radius"]["refresh"], "rotated");
            assert!(after["radius"]["_piRsRefreshPending"].is_null());
            assert!(!home.join("auth.json.lock").exists());
        }
        assert_eq!(std::fs::read(&session).unwrap(), journal_before);
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "cancelled or failed refresh proceeded to an HTTP request"
        );
    }
}

#[cfg(unix)]
struct FixtureChild(std::process::Child);

#[cfg(unix)]
impl Drop for FixtureChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
