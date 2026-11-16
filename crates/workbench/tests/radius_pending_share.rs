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
        // macOS may inherit the listener's nonblocking flag on accept.
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
