#![cfg(feature = "text-cli")]

use std::{
    io::{BufRead, BufReader, Cursor, Read, Write},
    net::TcpListener,
    process::{Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use base64::Engine;
use serde_json::{Value, json};

#[test]
fn persists_text_conversation_and_rejects_a_changed_provider_route() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let (finish_first_tx, finish_first_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut bodies = Vec::new();
        for (index, answer) in [
            "first answer",
            "second answer",
            "third answer",
            "partial before abort",
            "first attempt only",
            "replacement answer",
            "saved before failure",
            "queue base answer",
            "follow-up answer",
            "queued partial before failure",
            "retained after failure",
            "new prompt answer",
            "transient rate limit",
            "image prompt answer",
        ]
        .into_iter()
        .enumerate()
        {
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "provider request timed out");
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept provider request: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&mut socket);
            let mut length = 0;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            bodies.push(serde_json::from_slice::<Value>(&body).unwrap());
            if index == 12 {
                let rate_limit = r#"{"error":{"code":"rate_limit_exceeded"}}"#;
                write!(socket, "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rate_limit}", rate_limit.len()).unwrap();
                continue;
            }
            let delta = serde_json::json!({
                "type": "response.output_text.delta", "output_index": 0, "delta": answer
            });
            let completed = serde_json::json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "output": [{
                        "type": "message", "role": "assistant",
                        "content": [{"type": "output_text", "text": answer}]
                    }],
                    "usage": {"input_tokens": 1, "output_tokens": 1}
                }
            });
            let partial = format!("data: {delta}\n\n");
            let final_frame = match index {
                4 => serde_json::json!({"type":"error","status":503}),
                6 | 9 => serde_json::json!({"type":"error","status":400}),
                _ => completed,
            };
            let final_event = format!("data: {final_frame}\n\n");
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{partial}",
                partial.len() + final_event.len(),
            ).unwrap();
            if matches!(index, 0 | 2 | 3 | 4 | 7) {
                socket.flush().unwrap();
                finish_first_rx
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
            let result = socket.write_all(final_event.as_bytes());
            if index != 3 {
                result.unwrap();
            }
        }
        bodies
    });

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("durable.sqlite");
    let command = |url: &str, args: &[&str]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi-rs-durable"));
        command
            .env("OPENAI_API_KEY", "fixture-key")
            .arg("--db")
            .arg(&db)
            .args(["--endpoint", url])
            .args(args);
        command
    };
    let mut first_run = command(&endpoint, &["--model", "fixture", "first request"])
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(first_run.stdout.take().unwrap());
    let mut first_frames = Vec::new();
    let mut saw_progress = false;
    loop {
        let mut line = String::new();
        if stdout.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let frame: Value = serde_json::from_str(&line).unwrap();
        if frame["type"] == "progress" && !saw_progress {
            assert_eq!(frame["text"], "first answer");
            saw_progress = true;
            finish_first_tx.send(()).unwrap();
        }
        first_frames.push(frame);
    }
    let output = first_run.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(saw_progress);
    let id = first_frames[0]["id"].as_str().unwrap();
    assert_eq!(first_frames.last().unwrap()["text"], "first answer");
    let wrong_route = command(
        "http://127.0.0.1:1/v1/responses",
        &["--conversation", id, "wrong route"],
    )
    .arg("--json")
    .output()
    .unwrap();
    assert!(!wrong_route.status.success());
    assert!(String::from_utf8_lossy(&wrong_route.stderr).contains("different provider route"));
    let second = frames(
        command(&endpoint, &["--conversation", id, "second request"])
            .arg("--json")
            .output()
            .unwrap(),
    );
    assert_eq!(second[0]["id"], id);
    assert_eq!(second.last().unwrap()["text"], "second answer");

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(stdin, r#"{{"type":"get_state","id":"status"}}"#).unwrap();
    writeln!(stdin, r#"{{"type":"get_messages","id":"history"}}"#).unwrap();
    writeln!(
        stdin,
        r#"{{"type":"prompt","id":"run","message":"third request"}}"#
    )
    .unwrap();
    drop(stdin);
    let stdout = BufReader::new(rpc.stdout.take().unwrap());
    let mut saw_rpc_progress = false;
    let rpc_frames: Vec<Value> = stdout
        .lines()
        .map(|line| {
            let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
            if frame["assistantMessageEvent"]["type"] == "text_delta" && !saw_rpc_progress {
                assert_eq!(frame["assistantMessageEvent"]["delta"], "third answer");
                saw_rpc_progress = true;
                finish_first_tx.send(()).unwrap();
            }
            frame
        })
        .collect();
    let output = rpc.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(saw_rpc_progress);
    assert_eq!(rpc_frames[0]["id"], "status");
    assert_eq!(rpc_frames[0]["data"]["sessionId"], id);
    assert_eq!(rpc_frames[1]["id"], "history");
    assert_eq!(
        rpc_frames[1]["data"]["messages"].as_array().unwrap().len(),
        4
    );
    assert_eq!(rpc_frames[2]["id"], "run");
    assert_eq!(rpc_frames[2]["success"], true);
    assert_eq!(rpc_frames[2]["data"]["disposition"], "started");
    let end = rpc_frames.last().unwrap();
    assert_eq!(end["type"], "agent_end");
    assert_eq!(end["success"], true);
    assert_eq!(end["messages"][1]["content"][0]["text"], "third answer");

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(
        stdin,
        r#"{{"type":"prompt","id":"cancel-test","message":"cancel this request"}}"#
    )
    .unwrap();
    let stdout = BufReader::new(rpc.stdout.take().unwrap());
    let mut sent_abort = false;
    let mut ended = false;
    let mut acknowledged = false;
    for line in stdout.lines() {
        let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
        if frame["assistantMessageEvent"]["type"] == "text_delta" && !sent_abort {
            assert_eq!(
                frame["assistantMessageEvent"]["delta"],
                "partial before abort"
            );
            sent_abort = true;
            writeln!(stdin, r#"{{"type":"abort","id":"cancel"}}"#).unwrap();
        } else if frame["id"] == "cancel" {
            assert!(ended, "abort must respond after the active turn ends");
            assert_eq!(frame["success"], true);
            finish_first_tx.send(()).unwrap();
            acknowledged = true;
            break;
        } else if frame["type"] == "agent_end" {
            assert_eq!(frame["success"], false);
            ended = true;
        }
    }
    drop(stdin);
    let output = rpc.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(sent_abort && ended && acknowledged);

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(
        stdin,
        r#"{{"type":"prompt","message":"retry this request"}}"#
    )
    .unwrap();
    let stdout = BufReader::new(rpc.stdout.take().unwrap());
    let mut reason_and_text = Vec::new();
    let mut saw_completion = false;
    let mut observed = String::new();
    for line in stdout.lines() {
        let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
        if frame["type"] == "message_start" && frame["message"]["role"] == "assistant" {
            observed.clear();
        }
        let event = &frame["assistantMessageEvent"];
        if event["type"] == "text_delta" {
            let delta = event["delta"].as_str().unwrap();
            if delta == "first attempt only" {
                finish_first_tx.send(()).unwrap();
            }
            observed.push_str(delta);
        }
        if frame["type"] == "message_end" && frame["message"]["role"] == "assistant" {
            assert_eq!(frame["message"]["content"][0]["text"], observed);
            reason_and_text.push((
                frame["message"]["stopReason"].as_str().unwrap().to_owned(),
                observed.clone(),
            ));
        }
        if frame["type"] == "agent_end" {
            if saw_completion {
                assert_eq!(frame["success"], false);
                break;
            }
            assert_eq!(frame["success"], true);
            saw_completion = true;
            writeln!(
                stdin,
                r#"{{"type":"prompt","message":"fail this request"}}"#
            )
            .unwrap();
        }
    }
    drop(stdin);
    let output = rpc.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        reason_and_text,
        [
            ("error".into(), "first attempt only".into()),
            ("stop".into(), "replacement answer".into()),
            ("error".into(), "saved before failure".into()),
        ]
    );

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(stdin, r#"{{"type":"prompt","message":"queue base"}}"#).unwrap();
    let mut png = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 2)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    let image_data = base64::engine::general_purpose::STANDARD.encode(png.into_inner());
    let image = json!({"type":"image", "mimeType":"image/png", "data":image_data});
    let mut ends = Vec::new();
    let mut sent = false;
    let mut released = false;
    for line in BufReader::new(rpc.stdout.take().unwrap()).lines() {
        let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
        if !sent && frame["assistantMessageEvent"]["delta"] == "queue base answer" {
            writeln!(
                stdin,
                "{}",
                json!({"type":"steer","id":"steer","message":"discard steer","images":[image]})
            )
            .unwrap();
            for request in [
                r#"{"type":"prompt","id":"original-follow","message":"discard follow-up","streamingBehavior":"followUp"}"#,
                r#"{"type":"get_state","id":"pending"}"#,
                r#"{"type":"clear_queue","id":"clear","includeContent":true}"#,
                r#"{"type":"set_follow_up_mode","id":"mode","mode":"all"}"#,
                r#"{"type":"get_state","id":"modes"}"#,
                r#"{"type":"set_follow_up_mode","mode":"one-at-a-time"}"#,
            ] {
                writeln!(stdin, "{request}").unwrap();
            }
            writeln!(
                stdin,
                "{}",
                json!({
                    "type":"follow_up", "message":"deliver after base", "images":[image],
                })
            )
            .unwrap();
            writeln!(
                stdin,
                r#"{{"type":"follow_up","message":"fail between polls"}}"#
            )
            .unwrap();
            writeln!(
                stdin,
                r#"{{"type":"follow_up","id":"after","message":"resume retained input"}}"#
            )
            .unwrap();
            sent = true;
        }
        match frame["id"].as_str() {
            Some("steer" | "original-follow") => {
                assert_eq!(frame["data"]["disposition"], "queued");
            }
            Some("pending") => assert_eq!(frame["data"]["pendingMessageCount"], 2),
            Some("clear") => {
                assert_eq!(frame["data"]["inputs"][0]["content"][1], image);
                assert_eq!(
                    frame["data"]["steering"],
                    serde_json::json!(["discard steer"])
                );
                assert_eq!(
                    frame["data"]["followUp"],
                    serde_json::json!(["discard follow-up"])
                );
            }
            Some("mode") => assert_eq!(frame["success"], true),
            Some("modes") => {
                assert_eq!(frame["data"]["followUpMode"], "all");
                assert_eq!(frame["data"]["pendingMessageCount"], 0);
            }
            Some("resume") => assert_eq!(frame["data"]["disposition"], "started"),
            Some("after") => {
                assert_eq!(frame["data"]["disposition"], "queued");
                finish_first_tx.send(()).unwrap();
                released = true;
            }
            _ => {}
        }
        if frame["type"] == "agent_end" {
            assert_eq!(frame["success"], ends.len() != 2);
            if ends.len() == 1 {
                assert_eq!(frame["messages"][0]["content"][1], image);
            }
            if ends.len() == 2 {
                assert_eq!(
                    frame["messages"][0]["content"][0]["text"],
                    "fail between polls"
                );
                writeln!(
                    stdin,
                    r#"{{"type":"prompt","id":"resume","message":"new prompt resumes queue"}}"#
                )
                .unwrap();
            }
            ends.push(
                frame["messages"][1]["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
            if ends.len() == 5 {
                break;
            }
        }
    }
    drop(stdin);
    let status = rpc.wait_with_output().unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(sent && released);
    assert_eq!(
        ends,
        [
            "queue base answer",
            "follow-up answer",
            "queued partial before failure",
            "retained after failure",
            "new prompt answer",
        ]
    );

    let mut rpc = command(&endpoint, &["--conversation", id, "--rpc"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = rpc.stdin.take().unwrap();
    writeln!(stdin, "{}", json!({"type":"get_messages","id":"reopen"})).unwrap();
    writeln!(
        stdin,
        "{}",
        json!({
            "type":"prompt","id":"image-prompt","message":"inspect PNG","images":[image]
        })
    )
    .unwrap();
    drop(stdin);
    let frames = frames(rpc.wait_with_output().unwrap());
    let saved = frames[0]["data"]["messages"].as_array().unwrap();
    assert!(saved.iter().any(|entry| {
        entry["content"][0]["text"] == "deliver after base" && entry["content"][1] == image
    }));
    assert_eq!(frames[1]["data"]["disposition"], "started");
    let end = frames.last().unwrap();
    assert_eq!(end["messages"][0]["content"][1], image);
    assert_eq!(
        end["messages"][1]["content"][0]["text"],
        "image prompt answer"
    );

    let bodies = worker.join().unwrap();
    assert_eq!(bodies[0]["model"], "fixture");
    let input = bodies[1]["input"].to_string();
    assert!(input.contains("first request"));
    assert!(input.contains("first answer"));
    assert!(input.contains("second request"));
    assert!(!input.contains("wrong route"));
    let third = bodies[2]["input"].to_string();
    assert!(third.contains("second answer"));
    assert!(third.contains("third request"));
    let follow_up = bodies[8]["input"].to_string();
    assert!(follow_up.contains("deliver after base"));
    assert!(!follow_up.contains("discard steer"));
    assert!(!follow_up.contains("discard follow-up"));
    assert!(
        bodies[10]["input"]
            .to_string()
            .contains("resume retained input")
    );
    assert!(
        bodies[11]["input"]
            .to_string()
            .contains("new prompt resumes queue")
    );
    let image_url = format!("data:image/png;base64,{image_data}");
    for index in [8, 12, 13] {
        let wire = bodies[index]["input"].as_array().unwrap();
        assert!(wire.iter().any(|message| {
            message["content"][1]["type"] == "input_image"
                && message["content"][1]["image_url"] == image_url
        }));
    }
    assert_eq!(bodies[12]["model"], "fixture");
}

#[test]
fn rpc_session_name_is_sanitized_and_retained_on_reopen() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("durable.sqlite");
    let rpc = |args: &[&str], requests: &[Value]| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_pi-rs-durable"))
            .env("OPENAI_API_KEY", "fixture-key")
            .arg("--db")
            .arg(&db)
            .args(["--endpoint", &endpoint, "--rpc"])
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = process.stdin.take().unwrap();
        for request in requests {
            writeln!(input, "{request}").unwrap();
        }
        drop(input);
        frames(process.wait_with_output().unwrap())
    };
    let responses = rpc(
        &["--model", "fixture"],
        &[
            json!({"type":"get_state"}),
            json!({"id":"empty","type":"get_tree"}),
            json!({"type":"set_session_name","name":"  Test\r\nTitle  "}),
            json!({"type":"set_session_name","name":"   "}),
            json!({"type":"get_state"}),
            json!({"id":"history","type":"get_entries"}),
            json!({"type":"get_entries","since":"unknown"}),
            json!({"id":"tree","type":"get_tree"}),
        ],
    );
    assert_eq!(responses.len(), 8);
    assert!(responses[0]["data"].get("sessionName").is_none());
    assert_eq!(responses[1]["data"], json!({"tree": [], "leafId": null}));
    assert_eq!(responses[2]["success"], true);
    assert_eq!(responses[3]["success"], false);
    assert_eq!(responses[4]["data"]["sessionName"], "Test Title");
    let history = &responses[5]["data"];
    let first = &history["entries"][0];
    let leaf = history["leafId"].as_str().unwrap();
    assert_eq!(first["id"], leaf);
    assert_eq!(first["type"], "session_info");
    assert_eq!(first["parentId"], Value::Null);
    assert_eq!(first["name"], "Test Title");
    assert_eq!(responses[6]["error"], "Entry not found: unknown");
    assert_eq!(responses[7]["id"], "tree");
    assert_eq!(
        responses[7]["data"]["tree"],
        json!([{"entry": first, "children": []}])
    );
    assert_eq!(responses[7]["data"]["leafId"], leaf);

    let id = responses[4]["data"]["sessionId"].as_str().unwrap();
    let reopened = rpc(
        &["--conversation", id],
        &[
            json!({"type":"get_state"}),
            json!({"type":"set_session_name","name":"Updated"}),
            json!({"type":"get_entries","since":leaf}),
            json!({"type":"get_tree"}),
        ],
    );
    assert_eq!(reopened[0]["data"]["sessionName"], "Test Title");
    assert_eq!(reopened[0]["data"]["messageCount"], 0);
    let new_entry = &reopened[2]["data"]["entries"][0];
    assert_eq!(reopened[2]["data"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(new_entry["parentId"], leaf);
    assert_eq!(new_entry["name"], "Updated");
    assert_eq!(
        reopened[3]["data"]["tree"][0]["children"][0]["entry"],
        *new_entry
    );
    let forked = rpc(
        &["--conversation", id],
        &[
            json!({"type":"set_session_label","target":leaf,"label":"earlier"}),
            json!({"type":"fork_at_entry","entryId":leaf}),
            json!({"type":"get_state"}),
            json!({"type":"get_tree"}),
        ],
    );
    let child = forked[1]["data"]["sessionId"].as_str().unwrap();
    assert_ne!(child, id);
    assert_eq!(forked[2]["data"]["sessionId"], child);
    assert_eq!(forked[2]["data"]["sessionName"], "Test Title");
    assert_eq!(forked[3]["data"]["tree"][0]["label"], "earlier");
    let parent = rpc(&["--conversation", id], &[json!({"type":"get_state"})]);
    assert_eq!(parent[0]["data"]["sessionName"], "Updated");
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn coding_tool_calls_are_committed_and_reopened_through_rpc() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let provider = thread::spawn(move || {
        for step in 0..2 {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "provider request timed out");
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept provider request: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&mut socket);
            let mut length = None;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = Some(value.trim().parse().unwrap());
                }
            }
            let mut body = vec![0; length.unwrap()];
            reader.read_exact(&mut body).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            let events = if step == 0 {
                let tools = request["tools"].as_array().unwrap();
                assert_eq!(tools.len(), 4);
                assert!(tools.iter().any(|tool| tool["name"] == "read"));
                let calls = [
                    ("fixture-call", "fixture.txt"),
                    ("missing-call", "missing.txt"),
                ]
                .map(|(call_id, path)| {
                    json!({
                        "type":"function_call", "call_id":call_id, "name":"read",
                        "arguments":json!({"path":path}).to_string()
                    })
                });
                let mut events = Vec::new();
                for (index, call) in calls.iter().enumerate() {
                    events.extend([
                        json!({"type":"response.output_item.added","output_index":index,"item":call}),
                        json!({"type":"response.function_call_arguments.delta","output_index":index,"delta":call["arguments"]}),
                    ]);
                }
                events.push(json!({"type":"response.completed","response":{"status":"completed","output":calls,"usage":{"input_tokens":1,"output_tokens":1}}}));
                events
            } else {
                assert!(request["input"].as_array().unwrap().iter().any(|item| {
                    item["type"] == "function_call_output"
                        && item["call_id"] == "fixture-call"
                        && item["output"].to_string().contains("durable-file-content")
                }));
                vec![
                    json!({"type":"response.output_text.delta","output_index":0,"delta":"File read."}),
                    json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"File read."}]}],"usage":{"input_tokens":1,"output_tokens":1}}}),
                ]
            };
            let response = events
                .into_iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect::<String>();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("durable.sqlite");
    std::fs::write(dir.path().join("fixture.txt"), "durable-file-content").unwrap();
    let other = tempfile::tempdir().unwrap();
    let rpc = |workspace: &std::path::Path, args: &[&str], requests: &[Value]| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_pi-rs-durable"))
            .env("OPENAI_API_KEY", "fixture-key")
            .args(["--endpoint", &endpoint, "--rpc", "--db"])
            .arg(&db)
            .arg("--coding-tools")
            .arg(workspace)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = process.stdin.take().unwrap();
        for request in requests {
            writeln!(input, "{request}").unwrap();
        }
        drop(input);
        process.wait_with_output().unwrap()
    };
    let events = frames(rpc(
        dir.path(),
        &["--model", "fixture"],
        &[
            json!({"type":"get_state"}),
            json!({"type":"prompt", "message":"Read the fixture"}),
        ],
    ));
    let id = events[0]["data"]["sessionId"].as_str().unwrap();
    let start = events
        .iter()
        .position(|event| {
            event["type"] == "tool_execution_start" && event["toolCallId"] == "fixture-call"
        })
        .unwrap_or_else(|| panic!("missing tool event: {events:#?}"));
    let end = events
        .iter()
        .position(|event| {
            event["type"] == "tool_execution_end" && event["toolCallId"] == "fixture-call"
        })
        .unwrap_or_else(|| panic!("missing tool event: {events:#?}"));
    assert!(start < end);
    assert_eq!(events[start]["toolName"], "read");
    assert_eq!(events[end]["isError"], false);
    assert!(
        events[end]["result"]["content"]
            .to_string()
            .contains("durable-file-content")
    );
    let failed = events
        .iter()
        .position(|event| {
            event["type"] == "tool_execution_end" && event["toolCallId"] == "missing-call"
        })
        .expect("missing file should produce a tool result");
    assert_eq!(events[failed]["isError"], true);
    let failed_outcome: pi_rs_core::Outcome =
        serde_json::from_value(events[failed]["durableOutcome"].clone()).unwrap();
    assert!(failed_outcome.is_error());
    let tool_turns: Vec<_> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event["type"] == "turn_end" && event["message"]["stopReason"] == "toolUse"
        })
        .collect();
    let [(turn_index, turn)] = tool_turns.as_slice() else {
        panic!("expected exactly one completed tool-use turn: {tool_turns:#?}");
    };
    assert!(*turn_index > end.max(failed));
    let results = turn["toolResults"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    for execution_end in [end, failed] {
        let call = &events[execution_end]["toolCallId"];
        let result = results
            .iter()
            .find(|result| &result["toolCallId"] == call)
            .unwrap();
        let lifecycle: Vec<_> = events
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                event["message"]["role"] == "toolResult" && &event["message"]["toolCallId"] == call
            })
            .collect();
        let [(start_index, start_event), (end_index, end_event)] = lifecycle.as_slice() else {
            panic!("expected one start and one end for {call}: {lifecycle:#?}");
        };
        assert!(
            execution_end < *start_index && *start_index < *end_index && end_index < turn_index
        );
        assert_eq!(start_event["type"], "message_start");
        assert_eq!(end_event["type"], "message_end");
        assert_eq!(&start_event["message"], result);
        assert_eq!(&end_event["message"], result);
    }
    assert!(
        results
            .iter()
            .any(|result| result["toolCallId"] == "fixture-call" && result["isError"] == false)
    );
    assert!(
        results
            .iter()
            .any(|result| result["toolCallId"] == "missing-call" && result["isError"] == true)
    );
    assert_eq!(events.last().unwrap()["type"], "agent_end");
    assert_eq!(events.last().unwrap()["success"], true);
    let reopened = frames(rpc(
        dir.path(),
        &["--conversation", id],
        &[json!({"type":"get_messages"})],
    ));
    assert!(
        reopened[0]["data"]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["role"] == "toolResult"
                    && message["durableInvocation"] == events[start]["durableInvocation"]
                    && message["content"]
                        .to_string()
                        .contains("durable-file-content")
            })
    );
    let failed_result = reopened[0]["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["toolCallId"] == "missing-call")
        .expect("the failed result should also reopen");
    assert_eq!(failed_result["isError"], true);
    assert_eq!(
        failed_result["durableOutcome"],
        events[failed]["durableOutcome"]
    );
    let wrong_workspace = rpc(other.path(), &["--conversation", id], &[]);
    assert!(!wrong_workspace.status.success());
    assert!(String::from_utf8_lossy(&wrong_workspace.stderr).contains("coding tools workspace"));
    provider.join().unwrap();
}

fn frames(output: Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
