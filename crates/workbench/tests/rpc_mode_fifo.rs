use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

struct Rpc {
    child: Child,
    input: std::process::ChildStdin,
    events: mpsc::Receiver<Value>,
}
impl Rpc {
    fn start(root: &std::path::Path) -> Self {
        Self::start_with_args(root, &[])
    }
    fn start_with_args(root: &std::path::Path, extra: &[&str]) -> Self {
        Self::start_inner(root, true, extra)
    }
    fn start_inner(root: &std::path::Path, no_session: bool, extra: &[&str]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command.current_dir(root).env("PI_RS_HOME", root);
        if no_session {
            command.arg("--no-session");
        }
        let mut child = command
            .args(["--no-trust", "--no-mcp", "--mode", "rpc"])
            .args(extra)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, events) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout)
                .lines()
                .map_while(std::result::Result::ok)
            {
                if let Ok(event) = serde_json::from_str(&line)
                    && send.send(event).is_err()
                {
                    break;
                }
            }
        });
        Self {
            child,
            input,
            events,
        }
    }
    fn send(&mut self, value: Value) {
        writeln!(self.input, "{value}").unwrap();
        self.input.flush().unwrap();
    }
    fn next(&self) -> Value {
        self.events.recv_timeout(Duration::from_secs(20)).unwrap()
    }
}

#[test]
fn switching_to_session_without_mode_records_preserves_configured_state() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("settings.json"),
        r#"{"steeringMode":"all","followUpMode":"one-at-a-time"}"#,
    )
    .unwrap();
    let initial = dir.path().join("initial.jsonl");
    let target = dir.path().join("target.jsonl");
    for path in [&initial, &target] {
        let mut session = pi_rs::Session::open(path).unwrap();
        session.set_working_directory(dir.path()).unwrap();
        assert!(
            !session
                .branch()
                .iter()
                .any(|record| matches!(record.event, pi_rs::Event::QueueModeSelected { .. }))
        );
    }
    let mut rpc = Rpc::start_inner(
        dir.path(),
        false,
        &["--offline", "--session", initial.to_str().unwrap()],
    );
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    loop {
        let event = rpc.next();
        if event["id"] == "mode" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"switch_session","id":"switch","sessionPath":target}));
    loop {
        let event = rpc.next();
        if event["id"] == "switch" {
            assert_eq!(event["success"], true, "{event}");
            assert_eq!(event["data"]["cancelled"], false, "{event}");
            break;
        }
    }
    let switched = pi_rs::Session::snapshot(&target).unwrap();
    assert!(
        !switched
            .branch()
            .iter()
            .any(|record| matches!(record.event, pi_rs::Event::QueueModeSelected { .. }))
    );
    rpc.send(json!({"type":"get_state","id":"state"}));
    loop {
        let event = rpc.next();
        if event["id"] == "state" {
            assert_eq!(event["success"], true, "{event}");
            assert_eq!(event["data"]["steeringMode"], "all", "{event}");
            assert_eq!(event["data"]["followUpMode"], "one-at-a-time", "{event}");
            break;
        }
    }
}
impl Drop for Rpc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn mode_rejections_overtake_blocked_writes_and_capacity_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut rpc = Rpc::start(dir.path());

    // Each state response confirms the preceding live application while the
    // held settings lock keeps all successful mode responses pending.
    for index in 0..32 {
        let mode = if index == 31 { "one-at-a-time" } else { "all" };
        rpc.send(json!({"type":"set_steering_mode","id":index,"mode":mode}));
        rpc.send(json!({"type":"get_state","id":format!("barrier-{index}")}));
        loop {
            let event = rpc.next();
            assert!(
                !event["id"].is_u64(),
                "write unexpectedly finished: {event}"
            );
            if event["id"] == format!("barrier-{index}") {
                assert_eq!(event["success"], true, "{event}");
                assert_eq!(event["data"]["steeringMode"], mode, "{event}");
                break;
            }
        }
    }

    rpc.send(json!({"type":"set_follow_up_mode","id":"invalid","mode":"invalid"}));
    rpc.send(json!({"type":"set_steering_mode","id":"overflow","mode":"all"}));
    let mut rejected = Vec::new();
    while rejected.len() < 2 {
        let event = rpc.next();
        assert!(
            !event["id"].is_u64(),
            "write unexpectedly finished: {event}"
        );
        match event["id"].as_str() {
            Some("invalid") => {
                assert_eq!(event["success"], false, "{event}");
                assert!(
                    event["error"].as_str().unwrap().contains("unknown variant"),
                    "{event}"
                );
                rejected.push("invalid");
            }
            Some("overflow") => {
                assert_eq!(event["success"], false, "{event}");
                assert_eq!(
                    event["error"], "configuration: queue control channel is full or closed",
                    "{event}"
                );
                rejected.push("overflow");
            }
            _ => {}
        }
    }
    assert_eq!(rejected, ["invalid", "overflow"]);
    rpc.send(json!({"type":"get_state","id":"after-reject"}));
    loop {
        let event = rpc.next();
        assert!(
            !event["id"].is_u64(),
            "write unexpectedly finished: {event}"
        );
        if event["id"] == "after-reject" {
            assert_eq!(event["data"]["steeringMode"], "one-at-a-time", "{event}");
            assert_eq!(event["data"]["followUpMode"], "one-at-a-time", "{event}");
            break;
        }
    }

    lock.unlock().unwrap();
    let mut written = Vec::new();
    while written.len() < 32 {
        let event = rpc.next();
        if let Some(id) = event["id"].as_u64() {
            assert_eq!(event["success"], true, "{event}");
            written.push(id);
        }
    }
    assert_eq!(written, (0..32).collect::<Vec<_>>());
    let settings: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["steeringMode"], "one-at-a-time");
    rpc.send(json!({"type":"set_steering_mode","id":"retry","mode":"all"}));
    loop {
        let event = rpc.next();
        if event["id"] == "retry" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    let settings: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["steeringMode"], "all");
}

#[test]
fn mode_writes_are_fifo_and_shutdown_drains_without_blocking_abort() {
    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","id":"same","mode":"all"}));
    rpc.send(json!({"type":"set_steering_mode","id":"same","mode":"one-at-a-time"}));
    rpc.send(json!({"type":"set_follow_up_mode","id":"follow","mode":"all"}));
    rpc.send(json!({"type":"get_state","id":"state"}));
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let event = rpc.next();
        if event["id"] == "state" {
            assert_eq!(event["data"]["steeringMode"], "one-at-a-time", "{event}");
            assert_eq!(event["data"]["followUpMode"], "all", "{event}");
            break;
        }
        events.push(event);
    }
    assert!(
        events
            .iter()
            .all(|event| event["id"] != "same" && event["id"] != "follow"),
        "{events:?}"
    );
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    rpc.send(json!({"type":"abort","id":"abort"}));
    loop {
        let event = rpc.next();
        if event["id"] == "abort" {
            assert_eq!(event["success"], true);
            break;
        }
        events.push(event);
    }
    assert!(events.iter().all(|event| event["id"] != "stop"));
    lock.unlock().unwrap();
    let mut responses = Vec::new();
    loop {
        let event = rpc.next();
        if event["type"] == "response" {
            if event["id"] == "stop" {
                assert_eq!(event["success"], true);
                break;
            }
            if event["id"] == "same" || event["id"] == "follow" {
                assert_eq!(event["success"], true, "{event}");
                responses.push(event["command"].as_str().unwrap().to_owned());
            }
        }
    }
    assert_eq!(
        responses,
        [
            "set_steering_mode",
            "set_steering_mode",
            "set_follow_up_mode"
        ]
    );
    let settings: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["steeringMode"], "one-at-a-time");
    assert_eq!(settings["followUpMode"], "all");
}

#[test]
fn failed_settings_write_reports_error_but_preserves_applied_cache() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("settings.lock")).unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    loop {
        let event = rpc.next();
        if event["id"] == "mode" {
            assert_eq!(event["success"], false, "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"get_state","id":"state"}));
    loop {
        let event = rpc.next();
        if event["id"] == "state" {
            assert_eq!(event["data"]["steeringMode"], "all", "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    loop {
        let event = rpc.next();
        if event["id"] == "stop" {
            assert_eq!(event["success"], true);
            break;
        }
    }
}

#[cfg(unix)]
#[test]
fn shutdown_drains_mode_behind_gated_hook_while_abort_is_serviced() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("hook.py");
    std::fs::write(
        &script,
        r##"#!/usr/bin/env python3
import json, os, sys, time
r=json.load(sys.stdin)
if r['type']=='describe':
    result={'id':'gate','dependencies':[],'commands':[],'tools':[],'input_hook':True}
elif r['type']=='input':
    root=os.environ['PI_RS_HOME']
    open(os.path.join(root,'hook-entered'),'w').close()
    while not os.path.exists(os.path.join(root,'hook-release')): time.sleep(0.01)
    result={'action':'handled'}
else: result=None
print(json.dumps({'version':1,'response':{'Ok':result}}))
"##,
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(dir.path())
        .env("PI_RS_HOME", dir.path())
        .args([
            "--no-session",
            "--no-trust",
            "--no-mcp",
            "--mode",
            "rpc",
            "--extension",
        ])
        .arg(&script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let input = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, events) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout)
            .lines()
            .map_while(std::result::Result::ok)
        {
            if let Ok(event) = serde_json::from_str(&line)
                && send.send(event).is_err()
            {
                break;
            }
        }
    });
    let mut rpc = Rpc {
        child,
        input,
        events,
    };
    rpc.send(json!({"type":"steer","id":"steer","message":"hold"}));
    let deadline = Instant::now() + Duration::from_secs(20);
    while !dir.path().join("hook-entered").exists() {
        assert!(Instant::now() < deadline, "hook did not start");
        std::thread::sleep(Duration::from_millis(10));
    }
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    rpc.send(json!({"type":"abort","id":"abort"}));
    loop {
        let event = rpc.next();
        if event["id"] == "abort" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    std::fs::write(dir.path().join("hook-release"), "").unwrap();
    let mut seen = Vec::new();
    loop {
        let event = rpc.next();
        if event["type"] == "response" {
            assert_eq!(event["success"], true, "{event}");
            seen.push(event["id"].as_str().unwrap_or_default().to_owned());
            if event["id"] == "stop" {
                break;
            }
        }
    }
    assert_eq!(seen, ["steer", "mode", "stop"]);
}

#[test]
fn persisted_mode_remains_visible_after_new_ephemeral_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    loop {
        let event = rpc.next();
        if event["id"] == "mode" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"new_session","id":"new"}));
    loop {
        let event = rpc.next();
        if event["id"] == "new" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"get_state","id":"state"}));
    loop {
        let event = rpc.next();
        if event["id"] == "state" {
            assert_eq!(event["data"]["steeringMode"], "all", "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"set_steering_mode","id":"after-restart","mode":"one-at-a-time"}));
    loop {
        let event = rpc.next();
        if event["id"] == "after-restart" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    loop {
        let event = rpc.next();
        if event["id"] == "stop" {
            break;
        }
    }
}

#[test]
fn shutdown_waits_for_earlier_deferred_modes_and_rejects_later_work() {
    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","id":"one","mode":"all"}));
    rpc.send(json!({"type":"set_steering_mode","id":"two","mode":"one-at-a-time"}));
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    rpc.send(json!({"type":"set_follow_up_mode","id":"late","mode":"all"}));
    rpc.send(json!({"type":"abort","id":"abort"}));
    let mut late = false;
    loop {
        let event = rpc.next();
        if event["id"] == "late" {
            assert_eq!(event["success"], false, "{event}");
            late = true;
        }
        if event["id"] == "abort" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    assert!(late);
    lock.unlock().unwrap();
    let mut completed = Vec::new();
    loop {
        let event = rpc.next();
        if event["type"] == "response"
            && ["one", "two", "stop"].contains(&event["id"].as_str().unwrap_or_default())
        {
            assert_eq!(event["success"], true, "{event}");
            completed.push(event["id"].as_str().unwrap().to_owned());
            if event["id"] == "stop" {
                break;
            }
        }
    }
    assert_eq!(completed, ["one", "two", "stop"]);
    let settings: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["steeringMode"], "one-at-a-time");
    assert!(settings.get("followUpMode").is_none());
}

#[test]
fn live_session_name_does_not_wait_for_mode_settings_write() {
    use std::io::Read;
    use std::net::TcpListener;

    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (release, waiting) = mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        loop {
            let (mut socket, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let is_post = line.starts_with("POST ");
            let mut length = 0;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            if !is_post {
                write!(
                    socket,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                continue;
            }
            reader.read_exact(&mut vec![0; length]).unwrap();
            let chunk = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"working\"},\"finish_reason\":null}]}\n\n";
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n", chunk.len(), chunk).unwrap();
            socket.flush().unwrap();
            let _ = waiting.recv_timeout(Duration::from_secs(30));
            break;
        }
    });
    let mut rpc = Rpc::start_with_args(
        dir.path(),
        &["--offline", "--no-skills", "--endpoint", &endpoint],
    );
    rpc.send(json!({"type":"prompt","id":"prompt","message":"hello"}));
    loop {
        let event = rpc.next();
        if event["type"] == "text_delta" || event["assistantMessageEvent"]["type"] == "text_delta" {
            break;
        }
        assert_ne!(
            event["type"], "agent_settled",
            "model did not remain active: {event}"
        );
    }
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    rpc.send(json!({"type":"get_state","id":"applied"}));
    loop {
        let event = rpc.next();
        if event["id"] == "applied" {
            assert_eq!(event["data"]["steeringMode"], "all");
            assert_eq!(event["data"]["isStreaming"], true);
            break;
        }
    }
    rpc.send(json!({"type":"set_session_name","id":"name","name":"live name"}));
    loop {
        let event = rpc.next();
        assert_ne!(event["id"], "mode", "mode write completed while lock held");
        if event["id"] == "name" {
            assert_eq!(event["success"], true, "{event}");
            break;
        }
    }
    rpc.send(json!({"type":"abort","id":"abort"}));
    loop {
        let event = rpc.next();
        if event["id"] == "abort" {
            assert_eq!(event["success"], true);
            break;
        }
    }
    let _ = release.send(());
    server.join().unwrap();
    lock.unlock().unwrap();
    loop {
        let event = rpc.next();
        if event["id"] == "mode" {
            assert_eq!(event["success"], true);
            break;
        }
    }
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    loop {
        if rpc.next()["id"] == "stop" {
            break;
        }
    }
}

#[test]
fn session_transition_waits_for_mode_persistence_and_keeps_abort_live() {
    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    rpc.send(json!({"type":"get_state","id":"applied"}));
    loop {
        let event = rpc.next();
        if event["id"] == "applied" {
            assert_eq!(event["data"]["steeringMode"], "all");
            break;
        }
    }
    rpc.send(json!({"type":"new_session","id":"new"}));
    rpc.send(json!({"type":"abort","id":"abort"}));
    loop {
        let event = rpc.next();
        assert_ne!(event["id"], "new", "transition must wait: {event}");
        if event["id"] == "abort" {
            assert_eq!(event["success"], true);
            break;
        }
    }
    lock.unlock().unwrap();
    let mut completed = Vec::new();
    while completed.len() < 2 {
        let event = rpc.next();
        if ["mode", "new"].contains(&event["id"].as_str().unwrap_or_default()) {
            assert_eq!(event["success"], true, "{event}");
            completed.push(event["id"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(completed, ["mode", "new"]);
    rpc.send(json!({"type":"get_state","id":"state"}));
    loop {
        let event = rpc.next();
        if event["id"] == "state" {
            assert_eq!(event["data"]["steeringMode"], "all");
            break;
        }
    }
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    loop {
        if rpc.next()["id"] == "stop" {
            break;
        }
    }
}

#[test]
fn settings_wait_behind_mode_without_blocking_abort_or_later_commands() {
    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    rpc.send(json!({"type":"get_state","id":"applied"}));
    loop {
        let event = rpc.next();
        if event["id"] == "applied" {
            assert_eq!(event["data"]["steeringMode"], "all");
            break;
        }
    }
    rpc.send(json!({"type":"set_auto_retry","id":"setting","enabled":false}));
    rpc.send(json!({"type":"get_state","id":"later"}));
    rpc.send(json!({"type":"abort_retry","id":"abort-retry"}));
    rpc.send(json!({"type":"abort","id":"abort"}));
    let mut cancellations = Vec::new();
    while cancellations.len() < 2 {
        let event = rpc.next();
        assert_ne!(event["id"], "setting", "{event}");
        assert_ne!(event["id"], "later", "{event}");
        if matches!(event["id"].as_str(), Some("abort" | "abort-retry")) {
            assert_eq!(event["success"], true);
            cancellations.push(event["id"].as_str().unwrap().to_owned());
        }
    }
    lock.unlock().unwrap();
    let mut ids = Vec::new();
    while ids.len() < 3 {
        let event = rpc.next();
        if ["mode", "setting", "later"].contains(&event["id"].as_str().unwrap_or_default()) {
            assert_eq!(event["success"], true, "{event}");
            ids.push(event["id"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(ids[0], "mode");
    assert!(ids.contains(&"setting".to_owned()) && ids.contains(&"later".to_owned()));
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    loop {
        if rpc.next()["id"] == "stop" {
            break;
        }
    }
}

#[test]
fn mode_responses_preserve_missing_null_and_duplicate_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut rpc = Rpc::start(dir.path());
    rpc.send(json!({"type":"set_steering_mode","mode":"all"}));
    rpc.send(json!({"type":"set_steering_mode","id":null,"mode":"one-at-a-time"}));
    rpc.send(json!({"type":"set_follow_up_mode","id":"same","mode":"all"}));
    rpc.send(json!({"type":"set_follow_up_mode","id":"same","mode":"one-at-a-time"}));
    let mut responses = Vec::new();
    while responses.len() < 4 {
        let event = rpc.next();
        if event["type"] == "response"
            && matches!(
                event["command"].as_str(),
                Some("set_steering_mode" | "set_follow_up_mode")
            )
        {
            assert_eq!(event["success"], true, "{event}");
            responses.push(event);
        }
    }
    assert!(responses[0].get("id").is_none());
    assert_eq!(responses[1].get("id"), Some(&Value::Null));
    assert_eq!(responses[2]["id"], "same");
    assert_eq!(responses[3]["id"], "same");
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    loop {
        if rpc.next()["id"] == "stop" {
            break;
        }
    }
}

#[cfg(unix)]
#[test]
fn ui_reply_is_serviced_before_pending_shutdown_drains() {
    use std::io::Read;
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("settings.lock"))
        .unwrap();
    lock.lock().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    std::thread::spawn(move || {
        loop {
            let (mut socket, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let is_post = line.starts_with("POST ");
            let mut length = 0;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            if !is_post {
                write!(
                    socket,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                continue;
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let events = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-ui\",\"type\":\"function\",\"function\":{\"name\":\"ui_gate\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                "data: [DONE]\n\n"
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
            break;
        }
    });
    let script = dir.path().join("ui.py");
    std::fs::write(&script, r##"#!/usr/bin/env python3
import json, os, sys
if '--pi-rs-extension-v1' in sys.argv:
    r=json.load(sys.stdin)
    d={'id':'ui-gate','dependencies':[],'commands':[],'host_calls':True,'tools':[{'spec':{'name':'ui_gate','description':'Request confirmation','parameters':{'type':'object'}},'execution':'sequential','transactional':True}]}
    result=d if r['type']=='describe' else r.get('request') if r['type']=='before_request' else {'allow':True,'reason':None} if r['type']=='before_tool' else None
    print(json.dumps({'version':1,'response':{'Ok':result}}))
else:
    frame=json.loads(sys.stdin.readline()); r=frame['request']
    if r['type']=='invoke':
        print(json.dumps({'type':'ui','request':{'method':'confirm','title':'Ready','message':'Continue?'}}),flush=True)
        reply=json.loads(sys.stdin.readline())
        with open(os.path.join(os.environ['PI_RS_HOME'],'ui-reply'),'w') as f: json.dump(reply,f)
        result={'outcome':{'status':'succeeded','detail':'confirmed'}}
    else: result=None
    print(json.dumps({'type':'complete','response':{'Ok':result}}),flush=True)
"##).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(dir.path())
        .env("PI_RS_HOME", dir.path())
        .args([
            "--no-session",
            "--no-trust",
            "--no-mcp",
            "--offline",
            "--no-skills",
            "--mode",
            "rpc",
            "--endpoint",
        ])
        .arg(&endpoint)
        .arg("--extension")
        .arg(&script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let input = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, events) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout)
            .lines()
            .map_while(std::result::Result::ok)
        {
            if let Ok(event) = serde_json::from_str(&line)
                && send.send(event).is_err()
            {
                break;
            }
        }
    });
    let mut rpc = Rpc {
        child,
        input,
        events,
    };
    rpc.send(json!({"type":"prompt","id":"prompt","message":"hello"}));
    let ui = loop {
        let event = rpc.next();
        assert_ne!(
            event["type"], "error",
            "agent failed before UI request: {event}"
        );
        assert_ne!(
            event["type"], "agent_settled",
            "agent stopped before UI request: {event}"
        );
        if event["type"] == "extension_ui_request" {
            break event;
        }
    };
    rpc.send(json!({"type":"set_steering_mode","id":"mode","mode":"all"}));
    rpc.send(json!({"type":"shutdown","id":"stop"}));
    rpc.send(json!({"type":"extension_ui_response","id":ui["id"],"confirmed":true}));
    let deadline = Instant::now() + Duration::from_secs(20);
    while !dir.path().join("ui-reply").exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    rpc.send(json!({"type":"abort","id":"abort"}));
    lock.unlock().unwrap();
    let mut seen = Vec::new();
    loop {
        let event = rpc.next();
        if event["type"] == "response" {
            if event["id"] != "prompt" {
                assert_eq!(event["success"], true, "{event}");
            }
            seen.push(event["id"].as_str().unwrap_or_default().to_owned());
            if event["id"] == "stop" {
                break;
            }
        }
    }
    assert!(seen.contains(&"abort".into()));
    assert!(seen.contains(&"mode".into()));
    assert_eq!(seen.last().map(String::as_str), Some("stop"));
    let reply: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("ui-reply")).unwrap()).unwrap();
    assert_eq!(reply, json!({"Ok":true}));
}
