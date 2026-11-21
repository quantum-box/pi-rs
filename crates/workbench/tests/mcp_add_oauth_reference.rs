#![cfg(any(target_os = "linux", target_os = "macos"))]

use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command,
    time::{Instant, timeout_at},
};

const SECRET: &str = "fixture-secret-7x9-private";
const REFERENCE: &str = "${PI_RS_OAUTH_REFERENCE_TEST_SECRET}";

#[tokio::test]
async fn add_reference_is_resolved_only_for_oauth_token_exchange() {
    run_reference_login(Scope::Global, false, false).await;
}

#[tokio::test]
async fn trusted_project_login_uses_its_configured_oauth_client() {
    run_reference_login(Scope::Project, false, false).await;
}

#[tokio::test]
async fn changed_project_authentication_does_not_replace_existing_credentials() {
    run_reference_login(Scope::Project, true, false).await;
}

#[tokio::test]
async fn local_login_uses_trusted_project_oauth_client() {
    run_reference_login(Scope::Project, false, true).await;
}

#[tokio::test]
async fn trusted_project_login_uses_a_partial_oauth_override() {
    run_reference_login(Scope::Override, false, false).await;
}

#[tokio::test]
async fn login_with_implicit_oauth_does_not_rewrite_configuration() {
    run_reference_login(Scope::Implicit, false, false).await;
}

#[derive(Clone, Copy)]
enum Scope {
    Global,
    Project,
    Override,
    Implicit,
}

async fn run_reference_login(scope: Scope, change_during_login: bool, local: bool) {
    let project_scope = matches!(scope, Scope::Project | Scope::Override);
    let root = tempfile::tempdir().unwrap();
    let browser_dir = root.path().join("bin");
    std::fs::create_dir(&browser_dir).unwrap();
    let browser = browser_dir.join(if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    });
    // The launcher only records its final argument. The test drives the loopback callback.
    std::fs::write(
        &browser,
        "#!/bin/sh\nfor arg do url=$arg; done\nprintf '%s' \"$url\" > \"$PI_TEST_BROWSER_URL.tmp\"\n/bin/mv \"$PI_TEST_BROWSER_URL.tmp\" \"$PI_TEST_BROWSER_URL\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&browser, std::fs::Permissions::from_mode(0o700)).unwrap();
    let browser_url = root.path().join("browser-url");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    let fixture_base = base.clone();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = timeout_at(deadline, listener.accept())
                .await
                .expect("fixture accept deadline")
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut first = String::new();
            timeout_at(deadline, socket.read_line(&mut first))
                .await
                .unwrap()
                .unwrap();
            let mut length = 0;
            loop {
                let mut line = String::new();
                timeout_at(deadline, socket.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!line.is_empty(), "incomplete request headers");
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            assert!(length < 65536);
            let mut body = vec![0; length];
            timeout_at(deadline, socket.read_exact(&mut body))
                .await
                .unwrap()
                .unwrap();
            let token = first.starts_with("POST /token ");
            let metadata = first.contains("/.well-known/oauth-authorization-server")
                || first.contains("/.well-known/openid-configuration");
            let (status, response) = if metadata {
                let issuer = if first.contains("oauth-authorization-server/mcp") {
                    format!("{fixture_base}/mcp")
                } else {
                    fixture_base.clone()
                };
                ("200 OK", json!({"issuer":issuer,"authorization_endpoint":format!("{fixture_base}/authorize"),"token_endpoint":format!("{fixture_base}/token"),"response_types_supported":["code"],"token_endpoint_auth_methods_supported":["client_secret_post"],"code_challenge_methods_supported":["S256"]}).to_string())
            } else if token {
                ("200 OK", json!({"access_token":"fixture-access-token","token_type":"Bearer","expires_in":3600}).to_string())
            } else {
                ("404 Not Found", "{}".to_owned())
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len()
            );
            timeout_at(deadline, socket.write_all(response.as_bytes()))
                .await
                .unwrap()
                .unwrap();
            if token {
                return body;
            }
        }
    });
    let mut path = std::ffi::OsString::from(browser_dir.as_os_str());
    path.push(":");
    path.push(std::env::var_os("PATH").unwrap_or_default());
    let make_command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .env("PATH", &path)
            .env("PI_TEST_BROWSER_URL", &browser_url)
            .env("PI_RS_OAUTH_REFERENCE_TEST_SECRET", SECRET)
            .env_remove("HTTP_PROXY")
            .env_remove("http_proxy")
            .env_remove("HTTPS_PROXY")
            .env_remove("https_proxy")
            .env_remove("ALL_PROXY")
            .env_remove("all_proxy")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    };
    let added = timeout_at(
        deadline,
        make_command()
            .args([
                "mcp",
                "add",
                "fixture",
                "--url",
                &format!("{base}/mcp"),
                "--oauth-client-id",
                "fixture-client",
                "--oauth-client-secret",
                REFERENCE,
            ])
            .output(),
    )
    .await
    .expect("add deadline")
    .unwrap();
    assert!(
        added.status.success(),
        "add failed: {}",
        String::from_utf8_lossy(&added.stderr).replace(SECRET, "[redacted]")
    );
    let global_path = root.path().join("mcp.json");
    let config_path = if project_scope {
        let project_dir = root.path().join(".pi");
        std::fs::create_dir(&project_dir).unwrap();
        let project_path = project_dir.join("mcp.json");
        if matches!(scope, Scope::Override) {
            let mut config: Value =
                serde_json::from_slice(&std::fs::read(&global_path).unwrap()).unwrap();
            let oauth = config["mcpServers"]["fixture"]
                .as_object_mut()
                .unwrap()
                .remove("oauth")
                .unwrap();
            config["mcpServers"]["fixture"]["oauth"] = json!({"clientId": "other-client"});
            std::fs::write(&global_path, config.to_string()).unwrap();
            std::fs::write(
                &project_path,
                json!({"mcpServers": {"fixture": {"oauth": oauth}}}).to_string(),
            )
            .unwrap();
        } else {
            std::fs::copy(&global_path, &project_path).unwrap();
            std::fs::write(&global_path, json!({"mcpServers":{"fixture":{"url":"https://global.example/mcp","auth":{"provider":"fixture"}}}}).to_string()).unwrap();
        }
        project_path
    } else {
        global_path.clone()
    };
    if matches!(scope, Scope::Implicit) {
        std::fs::write(
            &global_path,
            json!({"mcpServers":{"fixture":{"url":format!("{base}/mcp")}}}).to_string(),
        )
        .unwrap();
    }
    let global_before = std::fs::read(&global_path).unwrap();
    let before = std::fs::read(&config_path).unwrap();
    if !matches!(scope, Scope::Implicit) {
        let config: Value = serde_json::from_slice(&before).unwrap();
        assert_eq!(
            config["mcpServers"]["fixture"]["oauth"]["clientSecret"],
            REFERENCE
        );
    }
    let credential_path = {
        use sha2::{Digest, Sha256};
        let digest =
            Sha256::digest(serde_json::to_vec(&("fixture", format!("{base}/mcp"))).unwrap());
        let directory = root.path().join("mcp-auth");
        let path = directory.join(format!("{digest:x}.json"));
        if change_during_login {
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(&path, "old credentials").unwrap();
        }
        path
    };
    let callback_browser_url = browser_url.clone();
    let callback_config_path = config_path.clone();
    let browser_task = tokio::spawn(async move {
        let url = loop {
            if let Ok(url) = tokio::fs::read_to_string(&callback_browser_url).await
                && !url.is_empty()
            {
                break url;
            }
            assert!(Instant::now() < deadline, "browser launch deadline");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let authorization = reqwest::Url::parse(&url).unwrap();
        let redirect = authorization
            .query_pairs()
            .find(|(k, _)| k == "redirect_uri")
            .unwrap()
            .1
            .into_owned();
        let state = authorization
            .query_pairs()
            .find(|(k, _)| k == "state")
            .unwrap()
            .1
            .into_owned();
        let mut callback = reqwest::Url::parse(&redirect).unwrap();
        assert_eq!(callback.host_str(), Some("127.0.0.1"));
        callback
            .query_pairs_mut()
            .append_pair("code", "fixture-code")
            .append_pair("state", &state);
        if change_during_login {
            let mut config: Value =
                serde_json::from_slice(&std::fs::read(&callback_config_path).unwrap()).unwrap();
            config["mcpServers"]["fixture"]["url"] = json!("https://changed.example/mcp");
            std::fs::write(&callback_config_path, config.to_string()).unwrap();
        }
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = timeout_at(deadline, client.get(callback).send())
            .await
            .expect("callback deadline")
            .unwrap();
        assert_eq!(response.status().is_success(), !change_during_login);
    });
    let mut command = make_command();
    if project_scope {
        command.arg("--trust");
    }
    command.args(["mcp", "login", "fixture"]);
    if local {
        command.arg("--local");
    }
    if matches!(scope, Scope::Implicit) {
        command.args([
            "--client-id",
            "fixture-client",
            "--client-secret-env",
            "PI_RS_OAUTH_REFERENCE_TEST_SECRET",
        ]);
    }
    let login = timeout_at(deadline, command.output())
        .await
        .expect("login deadline")
        .unwrap();
    assert_eq!(
        login.status.success(),
        !change_during_login,
        "unexpected login result: {}",
        String::from_utf8_lossy(&login.stderr).replace(SECRET, "[redacted]")
    );
    if change_during_login {
        assert_eq!(
            std::fs::read_to_string(&credential_path).unwrap(),
            "old credentials"
        );
    } else {
        let saved: Value =
            serde_json::from_slice(&std::fs::read(&credential_path).unwrap()).unwrap();
        assert_eq!(
            saved.pointer("/credentials/token_response/access_token"),
            Some(&json!("fixture-access-token"))
        );
    }
    if !change_during_login {
        // OAuth credentials are the only published login state.
        assert_eq!(std::fs::read(&config_path).unwrap(), before);
    } else {
        let mut expected: Value = serde_json::from_slice(&before).unwrap();
        expected["mcpServers"]["fixture"]["url"] = json!("https://changed.example/mcp");
        let actual: Value = serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
        assert_eq!(actual, expected);
    }
    if project_scope {
        assert_eq!(std::fs::read(&global_path).unwrap(), global_before);
    }
    timeout_at(deadline, browser_task).await.unwrap().unwrap();
    let token_body = timeout_at(deadline, server).await.unwrap().unwrap();
    let form = reqwest::Url::parse(&format!(
        "http://localhost/?{}",
        String::from_utf8(token_body).unwrap()
    ))
    .unwrap();
    assert!(
        form.query_pairs()
            .any(|(k, v)| k == "client_secret" && v == SECRET)
    );
    assert!(
        form.query_pairs()
            .any(|(k, v)| k == "client_id" && v == "fixture-client")
    );
    for bytes in [
        &added.stdout,
        &added.stderr,
        &login.stdout,
        &login.stderr,
        &before,
        &std::fs::read(config_path).unwrap(),
    ] {
        assert!(
            !bytes
                .windows(SECRET.len())
                .any(|window| window == SECRET.as_bytes()),
            "secret leaked"
        );
    }
}
