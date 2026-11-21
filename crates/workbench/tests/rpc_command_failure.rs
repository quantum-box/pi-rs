#![cfg(unix)]
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn rpc_distinguishes_callback_failure_from_transport_and_migration() {
    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("fixture.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys
r=json.load(sys.stdin)
if r['type']=='describe': result={'Ok':{'id':'fixture','dependencies':[],'commands':['fail','not-dispatched','transport','typed-transport','invalid','malformed','badversion','badresult','good'],'tools':[],'command_effects':True,'command_failure_classification_v1':not os.environ.get('PI_RS_TEST_LEGACY_COMMAND'),'input_hook':True}}
elif r['type']=='migrate' and os.path.exists(os.path.join(os.environ['PI_RS_HOME'],'fail-migration')): result={'Err':'migration failure'}
elif r['type']=='input':
    with open(os.path.join(os.environ['PI_RS_HOME'],'input-hook'), 'a') as log: log.write('input\n')
    result={'Ok':{'action':'continue'}}
elif r['type']=='command_effects_classified_v1' and r['name']=='fail':
    with open(os.path.join(os.environ['PI_RS_HOME'],'callback'), 'a') as log: log.write('fail\n')
    result={'Ok':{'Err':{'Callback':'callback failure'}}}
elif r['type']=='command_effects_classified_v1' and r['name']=='transport': sys.exit(7)
elif r['type']=='command_effects_classified_v1' and r['name']=='typed-transport': result={'Ok':{'Err':{'Transport':'transport failure'}}}
elif r['type']=='command_effects_classified_v1' and r['name']=='not-dispatched': result={'Err':'unknown extension command'}
elif r['type']=='command_effects_classified_v1' and r['name']=='malformed': print('not json'); sys.exit(0)
elif r['type']=='command_effects_classified_v1' and r['name']=='badversion': print(json.dumps({'version':99,'response':{'Err':'callback failure'}})); sys.exit(0)
elif r['type']=='command_effects_classified_v1' and r['name']=='badresult': result={'Ok':'not effects'}
elif r['type']=='command_effects_classified_v1' and r['name']=='invalid': result={'Ok':{'Ok':{'actions':[{'type':'message','message':{'customType':'x','display':False,'content':[{'type':'image','mimeType':'image/png','data':'!!!!'}]}}]}}}
elif r['type']=='command_effects_classified_v1': result={'Ok':{'Ok':{'actions':[]}}}
elif r['type']=='command_effects': result={'Err':'legacy failure'}
else: result={'Ok':None}
print(json.dumps({'version':1,'response':result}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .env_remove("PI_RS_TEST_LEGACY_COMMAND")
            .args([
                "--mode",
                "rpc",
                "--no-session",
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--extension",
            ])
            .arg(&extension)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut input = child.0.stdin.take().unwrap();
    let stdout = child.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if send
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let mut request = |id: &str, message: &str| -> Vec<Value> {
        writeln!(
            input,
            "{}",
            json!({"type":"prompt","id":id,"message":message})
        )
        .unwrap();
        input.flush().unwrap();
        let mut events = Vec::new();
        loop {
            let event = receive
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|error| panic!("{id}: {error}; events={events:?}"));
            let finished = event["type"] == "agent_settled";
            events.push(event);
            if finished {
                return events;
            }
        }
    };
    let events = request("callback", "/fail");
    let error = events
        .iter()
        .find(|event| event["type"] == "extension_error")
        .unwrap();
    assert_eq!(error["event"], "command");
    assert_eq!(error["extensionPath"], "command:fail");
    assert_eq!(
        error["nativeExtensionPath"],
        extension.canonicalize().unwrap().to_str().unwrap()
    );
    assert_eq!(error["extension"], "fixture");
    assert_eq!(error["error"], "callback failure");
    let response = events
        .iter()
        .find(|event| event["id"] == "callback")
        .unwrap();
    assert_eq!(response["success"], true);
    assert_eq!(response["data"]["disposition"], "handled");
    assert_eq!(
        events
            .iter()
            .filter(|event| event["id"] == "callback")
            .count(),
        1
    );
    assert!(
        events
            .iter()
            .all(|event| event["type"] != "agent_start" && event["type"] != "agent_end")
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("callback")).unwrap(),
        "fail\n"
    );
    assert!(!root.path().join("input-hook").exists());
    for name in [
        "not-dispatched",
        "typed-transport",
        "transport",
        "malformed",
        "badversion",
        "badresult",
    ] {
        let events = request(name, &format!("/{name}"));
        assert!(
            events
                .iter()
                .all(|event| event["type"] != "extension_error"),
            "{events:?}"
        );
        assert_eq!(
            events.iter().find(|event| event["id"] == name).unwrap()["success"],
            false,
            "{events:?}"
        );
    }
    let events = request("invalid", "/invalid");
    assert!(
        events
            .iter()
            .all(|event| event["type"] != "extension_error")
    );
    assert_eq!(
        events
            .iter()
            .find(|event| event["id"] == "invalid")
            .unwrap()["success"],
        false
    );
    std::fs::write(root.path().join("fail-migration"), "").unwrap();
    let events = request("migration", "/good");
    assert!(
        events
            .iter()
            .all(|event| event["type"] != "extension_error")
    );
    let response = events
        .iter()
        .find(|event| event["id"] == "migration")
        .unwrap();
    assert_eq!(response["success"], false);
    assert!(
        response["error"]
            .as_str()
            .unwrap()
            .contains("migration failure")
    );
    std::fs::remove_file(root.path().join("fail-migration")).unwrap();
    let events = request("after", "/good");
    assert_eq!(
        events.iter().find(|event| event["id"] == "after").unwrap()["success"],
        true
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();

    let mut legacy = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path())
            .env("PI_RS_TEST_LEGACY_COMMAND", "1")
            .args([
                "--mode",
                "rpc",
                "--no-session",
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--extension",
            ])
            .arg(&extension)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut input = legacy.0.stdin.take().unwrap();
    let output = legacy.0.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if sender
                .send(serde_json::from_str::<Value>(&line).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    writeln!(
        input,
        "{}",
        json!({"type":"prompt","id":"legacy","message":"/fail"})
    )
    .unwrap();
    input.flush().unwrap();
    let mut events = Vec::new();
    loop {
        let event = receiver.recv_timeout(Duration::from_secs(30)).unwrap();
        let done = event["type"] == "agent_settled";
        events.push(event);
        if done {
            break;
        }
    }
    assert!(
        events
            .iter()
            .all(|event| event["type"] != "extension_error"),
        "{events:?}"
    );
    let response = events.iter().find(|event| event["id"] == "legacy").unwrap();
    assert_eq!(response["success"], false, "{events:?}");
    assert!(
        response["error"]
            .as_str()
            .unwrap()
            .contains("legacy failure")
    );
}
