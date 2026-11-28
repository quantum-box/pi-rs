#![cfg(unix)]

use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// A socket accepted from a nonblocking listener can itself be nonblocking on
// macOS. Normalize it before using timeouts for the fixture's gate protocol.
fn prepare_gate(socket: &std::net::TcpStream) {
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
}

#[test]
fn gate_waits_for_delayed_bytes_even_if_accepted_socket_was_nonblocking() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut socket, _) = listener.accept().unwrap();
    // Simulate platforms where accept inherits the listener's O_NONBLOCK.
    socket.set_nonblocking(true).unwrap();
    prepare_gate(&socket);
    let (sent, received) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut byte = [0];
        let result = socket.read_exact(&mut byte).map(|()| byte);
        sent.send(result).unwrap();
    });
    assert!(matches!(
        received.recv_timeout(Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    client.write_all(b"S").unwrap();
    assert_eq!(
        received
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        *b"S"
    );
    reader.join().unwrap();
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct BoundedProcess {
    child: Child,
    master: Option<File>,
}
impl Drop for BoundedProcess {
    fn drop(&mut self) {
        drop(self.master.take());
        let _ = self.child.kill();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                result => {
                    if !std::thread::panicking() {
                        panic!("terminal child did not exit after kill: {result:?}");
                    }
                    return;
                }
            }
        }
    }
}

fn settings_row_shows(text: &str, label: &str, value: &str) -> bool {
    static COLORS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let colors = COLORS.get_or_init(|| regex::Regex::new(r"\x1b\[[0-9;]*m").unwrap());
    let text = colors.replace_all(text, "");
    text.match_indices(label).any(|(index, _)| {
        let tail = &text[index + label.len()..];
        tail.starts_with(' ')
            && tail
                .trim_start_matches(' ')
                .strip_prefix(value)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with([' ', '\x1b']))
    })
}

fn output_until(master: &mut File, predicate: impl Fn(&str) -> bool) {
    output_until_with_timeout(master, Duration::from_secs(10), predicate);
}

fn output_until_with_timeout(
    master: &mut File,
    timeout: Duration,
    predicate: impl Fn(&str) -> bool,
) {
    let deadline = Instant::now() + timeout;
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    while Instant::now() < deadline {
        match master.read(&mut buffer) {
            Ok(count) => output.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => panic!("terminal read failed: {error}"),
        }
        if predicate(&String::from_utf8_lossy(&output)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "terminal did not render expected input: {}",
        String::from_utf8_lossy(&output)
    );
}

fn spawn_auth_login(root: &std::path::Path, args: &[&str]) -> (File, File, Process) {
    let mut master_fd = -1;
    let mut slave_fd = -1;
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        0
    );
    let master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root)
            .env("PI_RS_HOME", root)
            .env("TERM", "xterm-256color")
            .args(["auth", "login"])
            .args(args)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()))
            .spawn()
            .unwrap(),
    );
    (master, slave, process)
}

#[test]
fn auth_login_uses_a_hidden_prompt_and_saves_the_selected_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let models = serde_json::json!({"providers":{"relay":{
        "name":"Fixture Relay", "api":"openai-completions",
        "baseUrl":"https://example.test/v1", "models":[{"id":"chat"}]
    }}});
    std::fs::write(root.path().join("models.json"), models.to_string()).unwrap();
    let (mut master, slave, mut process) =
        spawn_auth_login(root.path(), &["--api-key", "  fIXTURE Relay  "]);
    output_until(&mut master, |text| {
        text.contains("Enter Fixture Relay API key: ")
    });
    master.write_all(b"private-fixture-value\r").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("private-fixture-value"), "credential echoed");
        text.contains("Saved API key for Fixture Relay.")
    });
    assert!(process.0.wait().unwrap().success());
    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("auth.json")).unwrap()).unwrap();
    assert_eq!(auth["relay"]["type"], "api_key");
    assert_eq!(auth["relay"]["key"], "private-fixture-value");
    assert_eq!(
        auth["relay"]["endpoint"],
        "https://example.test/v1/chat/completions"
    );
    let mut settings = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), settings.as_mut_ptr()) },
        0
    );
    let settings = unsafe { settings.assume_init() };
    assert_ne!(settings.c_lflag & libc::ECHO, 0);
}

#[test]
fn auth_login_sigint_exits_while_waiting_for_browser_authorization() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, _slave, mut process) =
        spawn_auth_login(root.path(), &["--no-browser", "openrouter"]);
    output_until(&mut master, |text| {
        text.contains("Continue in your browser: https://openrouter.ai/auth")
    });
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGINT) },
        0
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert_eq!(status.code(), Some(130));
            break;
        }
        assert!(
            Instant::now() < deadline,
            "sign-in did not exit after SIGINT"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!root.path().join("auth.json").exists());
}

#[test]
fn bug_report_completion_consent_and_branch_export_in_both_modes() {
    use std::os::unix::fs::PermissionsExt;

    for mode in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(
            home.join("auth.json"),
            br#"{"relay":{"type":"api_key","key":"test-credential-value"}}"#,
        )
        .unwrap();
        std::fs::write(
            home.join("settings.json"),
            br#"{"defaultModel":"test-settings-value"}"#,
        )
        .unwrap();
        let session_path = root.path().join("session.jsonl");
        let saved_workspace = root.path().join("saved-workspace");
        std::fs::create_dir(&saved_workspace).unwrap();
        let mut session = pi_rs::Session::open(&session_path).unwrap();
        session.set_working_directory(&saved_workspace).unwrap();
        let anchor = session.head();
        session
            .append_user_with_images("inactive branch fixture".into(), vec![], None)
            .unwrap();
        session.checkout(anchor).unwrap();
        session
            .append_user_with_images("active branch fixture".into(), vec![], None)
            .unwrap();
        let session_id = session.session_id();
        drop(session);

        let mut master_fd = -1;
        let mut slave_fd = -1;
        let mut size = libc::winsize {
            ws_row: 32,
            ws_col: 120,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::addr_of_mut!(size),
                )
            },
            0
        );
        let mut master = unsafe { File::from_raw_fd(master_fd) };
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let _process = Process(
            Command::new(env!("CARGO_BIN_EXE_pi"))
                .current_dir(root.path())
                .env("PI_RS_HOME", &home)
                .env("PI_RS_BUG_TEST_SECRET", "test-environment-value")
                .env("TERM", "xterm-256color")
                .args([
                    "--no-trust",
                    "--no-mcp",
                    "--no-extensions",
                    "--offline",
                    "--provider",
                    "ollama",
                    "--model",
                    "fixture",
                    "--tui-mode",
                    mode,
                    "--session",
                ])
                .arg(&session_path)
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave))
                .spawn()
                .unwrap(),
        );
        let archives = || {
            std::fs::read_dir(root.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "zip"))
                .collect::<Vec<_>>()
        };
        output_until(&mut master, |text| text.contains("Enter to send"));
        let journal = std::fs::read(&session_path).unwrap();
        master.write_all(b"/bu\t").unwrap();
        output_until(&mut master, |text| text.contains("/bug"));
        master.write_all(b" cancelled description\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Metadata and diagnostics only")
        });
        assert!(archives().is_empty());
        master.write_all(b"\x1b").unwrap();
        output_until(&mut master, |text| text.contains("Bug report cancelled"));
        assert!(archives().is_empty());

        master.write_all(b"/bug example description\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Include current-branch transcript")
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| text.contains("Bug report exported to:"));
        let paths = archives();
        assert_eq!(paths.len(), 1);
        let path = &paths[0];
        assert_eq!(path.metadata().unwrap().permissions().mode() & 0o077, 0);
        let mut archive = zip::ZipArchive::new(File::open(path).unwrap()).unwrap();
        assert_eq!(archive.len(), 2);
        assert!(archive.by_name("session.jsonl").is_err());
        let mut metadata = String::new();
        archive
            .by_name("report.json")
            .unwrap()
            .read_to_string(&mut metadata)
            .unwrap();
        let report: serde_json::Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(report["description"], "example description");
        assert_eq!(report["sessionId"], session_id.to_string());
        assert_eq!(report["transcriptIncluded"], false);
        let mut diagnostics = String::new();
        archive
            .by_name("diagnostics.json")
            .unwrap()
            .read_to_string(&mut diagnostics)
            .unwrap();
        let details: serde_json::Value = serde_json::from_str(&diagnostics).unwrap();
        assert_eq!(details["userMessages"], 1);
        for marker in [
            "active branch fixture",
            "inactive branch fixture",
            "test-credential-value",
            "test-settings-value",
            "test-environment-value",
            "cancelled description",
        ] {
            assert!(!metadata.contains(marker) && !diagnostics.contains(marker));
        }
        drop(archive);
        std::fs::remove_file(path).unwrap();

        master.write_all(b"/bug\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Include current-branch transcript")
        });
        master.write_all(b"\x1b[B\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Bug report exported to:") && !archives().is_empty()
        });
        let paths = archives();
        assert_eq!(paths.len(), 1);
        let mut archive = zip::ZipArchive::new(File::open(&paths[0]).unwrap()).unwrap();
        assert_eq!(archive.len(), 3);
        let report: serde_json::Value =
            serde_json::from_reader(archive.by_name("report.json").unwrap()).unwrap();
        assert!(report["description"].is_null());
        assert_eq!(report["transcriptIncluded"], true);
        let mut file = archive.by_name("session.jsonl").unwrap();
        assert_eq!(file.unix_mode().unwrap() & 0o777, 0o600);
        let mut transcript = String::new();
        file.read_to_string(&mut transcript).unwrap();
        let records = transcript
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let branch = pi_rs::Session::from_serialized_records(records).unwrap();
        assert_eq!(branch.current_messages().len(), 1);
        assert!(transcript.contains("active branch fixture"));
        assert!(!transcript.contains("inactive branch fixture"));
        assert_eq!(std::fs::read(&session_path).unwrap(), journal);
        assert!(!saved_workspace.read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|extension| extension == "zip")
        }));
    }
}

#[test]
fn typing_redraws_in_a_synchronized_frame() {
    for mode in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let mut master_fd = -1;
        let mut slave_fd = -1;
        let mut size = libc::winsize {
            ws_row: 32,
            ws_col: 110,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::addr_of_mut!(size),
                )
            },
            0
        );
        let mut master = unsafe { File::from_raw_fd(master_fd) };
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let _process = Process(
            Command::new(env!("CARGO_BIN_EXE_pi"))
                .current_dir(root.path())
                .env("PI_RS_HOME", root.path().join("home"))
                .env("TERM", "xterm-256color")
                .args(["--no-trust", "--no-mcp", "--offline", "--tui-mode", mode])
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave))
                .spawn()
                .unwrap(),
        );
        output_until(&mut master, |text| text.contains("Enter to send"));
        master.write_all(b"flicker check").unwrap();
        output_until(&mut master, |text| {
            text.split("\x1b[?2026h").skip(1).any(|frame| {
                frame.split_once("\x1b[?2026l").is_some_and(|(body, _)| {
                    body.contains("flicker check")
                        && body.contains(if mode == "fullscreen" {
                            "\x1b[2J"
                        } else {
                            "\x1b[J"
                        })
                })
            })
        });
    }
}

fn fullscreen_widget_terminal(
    widgets: &[(&str, Vec<String>)],
) -> (tempfile::TempDir, File, Process) {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    std::fs::write(
        home.join("settings.json"),
        r#"{"fullscreenScrollbar":"always","showHardwareCursor":true}"#,
    )
    .unwrap();
    let session_path = root.path().join("session.jsonl");
    let mut session = pi_rs::Session::open(&session_path).unwrap();
    for index in 0..80 {
        session
            .append_user_with_images(format!("history-row-{index:02}"), vec![], None)
            .unwrap();
    }
    drop(session);

    let extension = root.path().join("widgets.py");
    std::fs::write(
        &extension,
        r#"#!/usr/bin/env python3
import json, os, sys
if '--pi-rs-extension-v1' in sys.argv:
    request = json.load(sys.stdin)
    result = {'id':'widgets-fixture','dependencies':[],'commands':[],'tools':[],
              'lifecycle_hook':True,'session_lifecycle_hook':True,'lifecycle_host_calls':True} if request['type'] == 'describe' else None
    print(json.dumps({'version':1,'response':{'Ok':result}}))
else:
    event = json.loads(sys.stdin.readline())['request']['event']
    if event['type'] == 'session_start':
        for key, (placement, lines) in enumerate(json.loads(os.environ['PI_TEST_WIDGETS'])):
            print(json.dumps({'type':'ui','request':{'method':'setWidget','widgetKey':str(key),
                  'widgetPlacement':placement,'widgetLines':lines}}), flush=True)
            assert json.loads(sys.stdin.readline()) == {'Ok':None}
    print(json.dumps({'type':'complete','response':{'Ok':None}}), flush=True)
"#,
    )
    .unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();

    let (master, process) = spawn_capability_test_terminal(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("PI_TEST_WIDGETS", serde_json::to_string(widgets).unwrap())
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--tui-mode",
                "fullscreen",
                "--model",
                "fixture",
                "--session",
            ])
            .arg(&session_path)
            .arg("--extension")
            .arg(&extension),
    );
    (root, master, process)
}

fn wait_for_widget_frame(master: &mut File, predicate: impl Fn(&str) -> bool) {
    output_until(master, |text| {
        text.split("\x1b[?2026h")
            .skip(1)
            .filter_map(|frame| frame.split_once("\x1b[?2026l").map(|(body, _)| body))
            .any(&predicate)
    });
}

fn resize_widget_terminal(master: &File, process: &Process, rows: u16, columns: u16) {
    let size = libc::winsize {
        ws_row: rows,
        ws_col: columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &size) },
        0
    );
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGWINCH) },
        0
    );
}

fn tall_widget(label: &str) -> Vec<String> {
    std::iter::once(label.to_owned())
        .chain((0..9).map(|i| format!("{label}-filler-{i}")))
        .collect()
}

#[test]
fn fullscreen_extension_widgets_stay_with_editor_when_scrolling_and_resizing() {
    let (_root, mut master, process) = fullscreen_widget_terminal(&[
        ("aboveEditor", tall_widget("dock-above")),
        ("belowEditor", vec!["dock-below".into()]),
    ]);
    let assert_dock = |frame: &str, draft: &str| {
        let above = frame.find("dock-above").expect("above-editor widget");
        let editor = frame.find(draft).expect("editor draft");
        let below = frame.find("dock-below").expect("below-editor widget");
        assert!(
            above < editor && editor < below,
            "unexpected dock: {frame:?}"
        );
    };
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("dock-below") && frame.contains("history-row-79")
    });

    master.write_all(b"dock-draft").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        if !frame.contains("history-row-79") || !frame.contains("dock-draft") {
            return false;
        }
        assert_dock(frame, "dock-draft");
        true
    });
    master.write_all(b"\x1b[5~").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        if !frame.contains("history-row-")
            || frame.contains("history-row-79")
            || !frame.contains("dock-draft")
        {
            return false;
        }
        assert_dock(frame, "dock-draft");
        true
    });

    // Ctrl+Shift+F, followed by text that exists only in the fixed widget.
    master.write_all(b"\x1b[102;6udock-above").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("dock-above-filler-0") && frame.contains("No matches")
    });
    master.write_all(b"\x1b").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("dock-draft") && !frame.contains("No matches")
    });

    resize_widget_terminal(&master, &process, 9, 72);
    master.write_all(b"-short").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        if !frame.contains("dock-draft-short") || frame.contains("dock-above-filler-8") {
            return false;
        }
        assert_dock(frame, "dock-draft-short");
        // The scroll thumb occupies the one remaining transcript row.
        assert!(frame.contains("┃") && !frame.contains("│"), "{frame:?}");
        true
    });
}

#[test]
fn fullscreen_extension_widgets_yield_to_search_on_a_short_terminal() {
    let (_root, mut master, process) = fullscreen_widget_terminal(&[
        ("aboveEditor", tall_widget("dock-above")),
        ("belowEditor", tall_widget("dock-below")),
    ]);
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("dock-above") && frame.contains("dock-below")
    });
    resize_widget_terminal(&master, &process, 9, 72);
    master.write_all(b"preserved-draft").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("\x1b[9;1H")
            && !frame.contains("\x1b[10;1H")
            && frame.contains("preserved-draft")
            && frame.contains("dock-below")
    });

    master.write_all(b"\x1b[102;6u").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("Find in transcript")
            && frame.contains("preserved-draft")
            && frame.contains("\x1b[?25h")
    });
    master.write_all(b"history-row-79").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains(" 1/1 ")
            && frame.contains("history-row-79")
            && frame.contains("preserved-draft")
    });
    master.write_all(b"x").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("history-row-79x") && frame.contains("No matches")
    });
    master.write_all(b"\x1b").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("preserved-draft")
            && frame.contains("dock-below")
            && !frame.contains("No matches")
    });
}

#[test]
fn fullscreen_extension_widgets_yield_to_settings_pagination() {
    let (_root, mut master, _process) = fullscreen_widget_terminal(&[
        ("aboveEditor", tall_widget("above-first")),
        ("aboveEditor", tall_widget("above-second")),
        ("belowEditor", tall_widget("below-first")),
        ("belowEditor", tall_widget("below-second")),
    ]);
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("above-first") && frame.contains("below-second-filler-8")
    });
    master.write_all(b"/settings\r").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        if !frame.contains("Settings") || !frame.contains("(1/") {
            return false;
        }
        assert!(
            frame.contains("Auto-compact") && frame.contains("Editor padding"),
            "{frame:?}"
        );
        true
    });
    master.write_all(b"\x1b[6~").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("Settings") && frame.contains("(11/")
    });
    master.write_all(b"\x1b[5~").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("Settings") && frame.contains("(1/")
    });
    master.write_all(b"\x1b").unwrap();
    wait_for_widget_frame(&mut master, |frame| {
        frame.contains("above-first") && frame.contains("below-second-filler-8")
    });
}

#[test]
fn terminal_appearance_notifications_follow_the_saved_theme_selection() {
    let root = tempfile::tempdir().unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .args(["--offline", "--no-trust", "--no-mcp", "--use-theme", "dark"])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b[?2031h"));
        assert!(!text.contains("\x1b[?996n"));
        text.contains("\x1b]11;?\x07") && text.contains("Enter to send")
    });
    master.write_all(b"\x1b[?1;2c/reload\r").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b[?2031h"));
        assert!(!text.contains("\x1b[?996n"));
        text.contains("Reloaded settings") && text.contains("\x1b]11;?\x07")
    });
    master.write_all(b"\x1b[?1;2c/theme light/dark\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("\x1b[?2031h\x1b[?996n") && text.contains("\x1b]11;?\x07")
    });
    master.write_all(b"/theme light\r").unwrap();
    output_until(&mut master, |text| text.contains("\x1b[?2031l"));
    master.write_all(b"/theme system\r").unwrap();
    output_until(&mut master, |text| text.contains("\x1b[?2031h\x1b[?996n"));
    master.write_all(b"/quit\r").unwrap();
    output_until(&mut master, |text| text.contains("\x1b[?2031l"));
}

#[test]
fn reload_applies_saved_theme_changes_without_overriding_cli_selection() {
    for explicit_theme in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let settings = home.join("settings.json");
        std::fs::write(&settings, br#"{"theme":"dark"}"#).unwrap();
        let mut master_fd = -1;
        let mut slave_fd = -1;
        let mut size = libc::winsize {
            ws_row: 32,
            ws_col: 110,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::addr_of_mut!(size),
                )
            },
            0
        );
        let mut master = unsafe { File::from_raw_fd(master_fd) };
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
        assert_eq!(
            unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--tui-mode",
                "fullscreen",
            ]);
        if explicit_theme {
            command.args(["--use-theme", "dark"]);
        }
        let _process = Process(
            command
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave))
                .spawn()
                .unwrap(),
        );

        output_until(&mut master, |text| {
            assert!(!text.contains("\x1b[?2031h"));
            text.contains("Enter to send") && text.contains("\x1b]11;?\x07")
        });
        std::fs::write(&settings, br#"{"theme":"system"}"#).unwrap();
        master.write_all(b"\x1b[?1;2c/reload\r").unwrap();
        output_until(&mut master, |text| {
            let queries = text.matches("\x1b]11;?\x07").count();
            assert!(queries <= 1);
            if explicit_theme {
                assert!(!text.contains("\x1b[?2031h"));
            }
            text.contains("Reloaded settings")
                && queries == 1
                && (explicit_theme || text.contains("\x1b[?2031h"))
        });
        if !explicit_theme {
            master.write_all(b"\x1b[?997;2n").unwrap();
        }
        master.write_all(b"\x1b[?1;2c/settings\r").unwrap();
        output_until(&mut master, |text| {
            assert!(!text.contains("\x1b]11;?\x07"));
            text.contains("Auto-compact")
        });
        master.write_all(b"Theme").unwrap();
        let expected = if explicit_theme { "dark" } else { "system" };
        output_until(&mut master, |text| {
            settings_row_shows(text, "Theme", expected)
        });
    }
}

#[test]
fn terminal_color_and_hyperlink_capabilities_apply_at_startup_and_reload_in_both_modes() {
    const RGB: &str = "\x1b[38;2;255;0;0m";
    const INDEXED: &str = "\x1b[38;5;196m";
    const RGB_BG: &str = "\x1b[48;2;128;128;128m";
    const INDEXED_BG: &str = "\x1b[48;5;244m";
    const TRANSCRIPT: &str = "committed terminal fixture";
    const URL: &str = "https://example.com/terminal";
    let osc_link = format!("\x1b]8;;{URL}\x1b\\");
    for mode in ["regular", "fullscreen"] {
        for (configured, env, before, after) in
            [(false, "1", INDEXED, RGB), (true, "0", RGB, INDEXED)]
        {
            let root = tempfile::tempdir().unwrap();
            let home = root.path().join("home");
            std::fs::create_dir_all(home.join("themes")).unwrap();
            let session_path = root.path().join("session.jsonl");
            pi_rs::Session::open(&session_path)
                .unwrap()
                .append_user_with_images(format!("{TRANSCRIPT} {URL}"), vec![], None)
                .unwrap();
            let settings = home.join("settings.json");
            std::fs::write(
                &settings,
                serde_json::json!({"theme":"color-fixture","terminal":{"trueColor":configured,"hyperlinks":configured}})
                    .to_string(),
            )
            .unwrap();
            let mut theme: serde_json::Value =
                serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
            theme["name"] = serde_json::json!("color-fixture");
            theme["colors"]["border"] = serde_json::json!("#ff0000");
            theme["colors"]["userMessageBg"] = serde_json::json!("#808080");
            std::fs::write(home.join("themes/color-fixture.json"), theme.to_string()).unwrap();

            let (mut master, _process) = spawn_capability_test_terminal(
                Command::new(env!("CARGO_BIN_EXE_pi"))
                    .current_dir(root.path())
                    .env("PI_RS_HOME", &home)
                    .env("TERM", "xterm-256color")
                    .env("PI_TRUE_COLOR", env)
                    .env("PI_HYPERLINKS", env)
                    .env_remove("NO_COLOR")
                    .args([
                        "--offline",
                        "--no-trust",
                        "--no-mcp",
                        "--no-extensions",
                        "--tui-mode",
                        mode,
                        "--session",
                    ])
                    .arg(&session_path),
            );
            output_until(&mut master, |text| {
                assert!(
                    !text.contains(after),
                    "unexpected color mode before reload in {mode}"
                );
                if before == INDEXED {
                    assert!(!text.contains("\x1b[38;2;") && !text.contains("\x1b[48;2;"));
                }
                let background = if configured { RGB_BG } else { INDEXED_BG };
                let ready = text.contains("Enter to send")
                    && text.contains(TRANSCRIPT)
                    && text.contains(before)
                    && text.contains(background)
                    && text.contains(URL);
                if ready {
                    assert_eq!(text.contains(&osc_link), configured, "{mode}");
                }
                ready
            });
            std::fs::write(
                &settings,
                br#"{"theme":"color-fixture","terminal":{"trueColor":"auto","hyperlinks":"auto"}}"#,
            )
            .unwrap();
            master.write_all(b"/reload\r").unwrap();
            output_until(&mut master, |text| {
                let background = if configured { INDEXED_BG } else { RGB_BG };
                let ready = text.contains("Reloaded settings")
                    && text.contains(TRANSCRIPT)
                    && text.contains(after)
                    && text.contains(background)
                    && text.contains(URL);
                if ready {
                    assert_eq!(text.contains(&osc_link), !configured, "{mode}");
                }
                ready
            });
        }
    }
}

fn spawn_capability_test_terminal(command: &mut Command) -> (File, Process) {
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let process = Process(
        command
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    (master, process)
}

#[test]
fn terminal_hyperlink_capabilities_reprobe_tmux_after_override() {
    use std::os::unix::fs::PermissionsExt;

    const LABEL: &str = "tmux link fixture";
    const URL: &str = "https://example.com/tmux-link";
    let osc_link = format!("\x1b]8;;{URL}\x1b\\");
    let fallback = format!(" ({URL})");

    for mode in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        std::fs::create_dir(&home).unwrap();
        let settings = home.join("settings.json");
        let features = root.path().join("features");
        let probe_log = root.path().join("probe-log");
        let tmux = root.path().join("tmux");
        std::fs::write(&probe_log, "").unwrap();
        std::fs::write(
            &tmux,
            "#!/bin/sh\nprintf 'probe\\n' >> \"$PI_TEST_TMUX_PROBE_LOG\"\nIFS= read -r features < \"$PI_TEST_TMUX_FEATURES\"\nprintf '%s\\n' \"$features\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&tmux, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(&features, "RGB\n").unwrap();
        std::fs::write(
            &settings,
            br#"{"terminal":{"hyperlinks":null,"trueColor":null}}"#,
        )
        .unwrap();
        let session_path = root.path().join("session.jsonl");
        pi_rs::Session::open(&session_path)
            .unwrap()
            .append_user_with_images(format!("[{LABEL}]({URL})"), vec![], None)
            .unwrap();
        let path = std::env::join_paths(
            std::iter::once(root.path().to_owned()).chain(
                std::env::var_os("PATH")
                    .iter()
                    .flat_map(std::env::split_paths),
            ),
        )
        .unwrap();
        let (mut master, _process) = spawn_capability_test_terminal(
            Command::new(env!("CARGO_BIN_EXE_pi"))
                .current_dir(root.path())
                .env("PI_RS_HOME", &home)
                .env("TERM", "xterm-256color")
                .env("TMUX", "fixture")
                .env("PATH", path)
                .env("PI_TEST_TMUX_FEATURES", &features)
                .env("PI_TEST_TMUX_PROBE_LOG", &probe_log)
                .env_remove("PI_HYPERLINKS")
                .args([
                    "--offline",
                    "--no-trust",
                    "--no-mcp",
                    "--no-extensions",
                    "--tui-mode",
                    mode,
                    "--session",
                ])
                .arg(&session_path),
        );
        let probes = || std::fs::read_to_string(&probe_log).unwrap().lines().count();
        output_until(&mut master, |text| {
            text.contains("Enter to send") && text.contains(LABEL) && text.contains(&fallback)
        });
        assert_eq!(probes(), 1, "{mode}");

        std::fs::write(&settings, br#"{"terminal":{"hyperlinks":true}}"#).unwrap();
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Reloaded settings") && text.contains(&osc_link)
        });
        assert_eq!(probes(), 1, "{mode}");

        // Force a visual change on the next automatic probe. Regular mode
        // retains the existing link when explicit and detected support agree.
        std::fs::write(&settings, br#"{"terminal":{"hyperlinks":false}}"#).unwrap();
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            text.split("\x1b[?2026h").skip(1).any(|frame| {
                frame.split_once("\x1b[?2026l").is_some_and(|(body, _)| {
                    body.contains("Reloaded settings")
                        && body.contains(&fallback)
                        && !body.contains(&osc_link)
                })
            })
        });
        assert_eq!(probes(), 1, "{mode}");

        std::fs::write(&features, "RGB,hyperlinks\n").unwrap();
        std::fs::write(&settings, br#"{"terminal":{"hyperlinks":"auto"}}"#).unwrap();
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Reloaded settings") && text.contains(&osc_link) && probes() == 2
        });
        master.write_all(b"cached probe fixture").unwrap();
        output_until(&mut master, |text| text.contains("cached probe fixture"));
        assert_eq!(probes(), 2, "{mode}");
    }
}

const KITTY_IMAGE: &str = "\x1b_Ga=t,";
const ITERM_IMAGE: &str = "\x1b]1337;File=inline=1;";
const IMAGE_TRANSCRIPT: &str = "saved image fixture";

fn completed_frame(text: &str, check: impl Fn(&str) -> bool) -> bool {
    text.split("\x1b[?2026h")
        .skip(1)
        .filter_map(|frame| frame.split_once("\x1b[?2026l"))
        .any(|(frame, _)| check(frame))
}

fn write_image_settings(root: &std::path::Path, images: serde_json::Value, show_images: bool) {
    let value = serde_json::json!({
        "theme": "dark",
        "terminal": {"images": images, "showImages": show_images},
    });
    std::fs::write(root.join("home/settings.json"), value.to_string()).unwrap();
}

fn spawn_image_terminal(
    root: &std::path::Path,
    mode: &str,
    protocol: &str,
    attach_image: bool,
) -> (File, File, Process, libc::winsize) {
    use base64::Engine;

    let home = root.join("home");
    std::fs::create_dir_all(&home).unwrap();
    write_image_settings(root, serde_json::json!(protocol), true);
    let path = root.join("session.jsonl");
    let mut attachments = Vec::new();
    if attach_image {
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(201, 101)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        attachments.push(pi_rs::ImageContent {
            data: base64::engine::general_purpose::STANDARD.encode(png.into_inner()),
            mime_type: "image/png".into(),
        });
    }
    pi_rs::Session::open(&path)
        .unwrap()
        .append_user_with_images(IMAGE_TRANSCRIPT.into(), attachments, None)
        .unwrap();

    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 42,
        ws_col: 110,
        ws_xpixel: 990,
        ws_ypixel: 756,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root)
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .env("PI_IMAGE_PROTOCOL", "iterm2")
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--tui-mode",
                mode,
                "--session",
            ])
            .arg(path)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()))
            .spawn()
            .unwrap(),
    );
    (master, slave, process, size)
}

#[test]
fn terminal_image_protocol_applies_at_startup_reload_and_resize_in_both_modes() {
    const DELETE_KITTY: &str = "\x1b_Ga=d,d=I,";
    const QUERY_CELL: &str = "\x1b[16t";
    for mode in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let (mut master, slave, process, mut size) =
            spawn_image_terminal(root.path(), mode, "kitty", true);
        output_until(&mut master, |text| {
            assert!(!text.contains(ITERM_IMAGE), "{mode}");
            text.contains(IMAGE_TRANSCRIPT)
                && text.contains(KITTY_IMAGE)
                && text.contains(",c=60,r=15,")
                && text.contains("Enter to send")
        });

        write_image_settings(root.path(), serde_json::json!("iterm2"), true);
        master.write_all(b"\x1b[?1;2c/reload\r").unwrap();
        output_until(&mut master, |text| {
            let Some((before, after)) = text.split_once(ITERM_IMAGE) else {
                return false;
            };
            assert!(before.contains(DELETE_KITTY), "{mode}");
            after.contains(";width=60;height=16;") && text.contains("Reloaded settings")
        });

        size.ws_col = 100;
        size.ws_xpixel = 900;
        assert_eq!(
            unsafe { libc::ioctl(slave.as_raw_fd(), libc::TIOCSWINSZ, &size) },
            0
        );
        assert_eq!(
            unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGWINCH) },
            0
        );
        output_until(&mut master, |text| {
            text.split_once(QUERY_CELL)
                .is_some_and(|(_, frame)| frame.contains(ITERM_IMAGE))
        });

        write_image_settings(root.path(), serde_json::json!(false), true);
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            text.contains(if mode == "regular" {
                "\x1b[3J"
            } else {
                "\x1b[2J"
            }) && completed_frame(text, |frame| {
                frame.contains("Reloaded settings")
                    && frame.contains(IMAGE_TRANSCRIPT)
                    && (mode != "regular" || frame.contains("\x1b[3J"))
                    && !frame.contains(ITERM_IMAGE)
                    && !frame.contains(KITTY_IMAGE)
            })
        });

        write_image_settings(root.path(), serde_json::json!("auto"), false);
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            let Some((_, frame)) = text.split_once(QUERY_CELL) else {
                return false;
            };
            completed_frame(frame, |frame| {
                frame.contains("Reloaded settings")
                    && !frame.contains(ITERM_IMAGE)
                    && !frame.contains(KITTY_IMAGE)
            })
        });

        write_image_settings(root.path(), serde_json::json!("auto"), true);
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            text.contains(ITERM_IMAGE) && text.contains("Reloaded settings")
        });
    }
}

#[test]
fn terminal_image_protocol_switch_preserves_regular_scrollback_without_images() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, _slave, _process, _) =
        spawn_image_terminal(root.path(), "regular", "iterm2", false);
    output_until(&mut master, |text| {
        text.contains(IMAGE_TRANSCRIPT) && text.contains("Enter to send")
    });
    write_image_settings(root.path(), serde_json::json!("kitty"), true);
    master.write_all(b"/reload\r").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b[3J"), "{text}");
        completed_frame(text, |frame| frame.contains("Reloaded settings"))
    });
}

fn switch_image_terminal_mode(master: &mut File, mode: &str, check: impl Fn(&str) -> bool) {
    let (previous, control) = if mode == "regular" {
        ("fullscreen", "\x1b[?1049l")
    } else {
        ("regular", "\x1b[?1049h")
    };
    master.write_all(b"/settings\r").unwrap();
    output_until(master, |text| text.contains("Settings"));
    master.write_all(b"TUI mode").unwrap();
    output_until(master, |text| {
        settings_row_shows(text, "TUI mode", previous)
    });
    master.write_all(b"\r").unwrap();
    output_until(master, |text| {
        text.split_once(control).is_some_and(|(_, restored)| {
            settings_row_shows(restored, "TUI mode", mode) && check(restored)
        })
    });
    master.write_all(b"\x03").unwrap();
    output_until(master, |text| {
        completed_frame(text, |frame| {
            frame.contains("0 tokens") && !settings_row_shows(frame, "TUI mode", mode)
        })
    });
}

#[test]
fn terminal_image_visibility_reflows_the_regular_screen_after_fullscreen_reload() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, _slave, _process, _) =
        spawn_image_terminal(root.path(), "regular", "iterm2", true);
    output_until(&mut master, |text| {
        text.contains(ITERM_IMAGE) && text.contains("Enter to send")
    });

    for show_images in [false, true] {
        switch_image_terminal_mode(&mut master, "fullscreen", |_| true);
        write_image_settings(root.path(), serde_json::json!("iterm2"), show_images);
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |text| {
            completed_frame(text, |frame| {
                frame.contains("Reloaded settings") && frame.contains(ITERM_IMAGE) == show_images
            })
        });
        switch_image_terminal_mode(&mut master, "regular", |text| {
            completed_frame(text, |frame| {
                frame.contains("\x1b[3J")
                    && frame.contains(IMAGE_TRANSCRIPT)
                    && frame.contains(ITERM_IMAGE) == show_images
            })
        });
    }
}

fn start_blocked_image_reload(root: &std::path::Path, master: &mut File) {
    use std::os::unix::fs::PermissionsExt;

    let package = root.join("reload-package.sh");
    std::fs::write(
        &package,
        r#"#!/bin/sh
: > "$PI_RS_HOME/probe-started"
for i in $(seq 1 100); do
    if [ -f "$PI_RS_HOME/probe-release" ]; then
        printf '%s\n' '{"version":1,"response":{"Ok":{"id":"probe","dependencies":[],"commands":[],"tools":[]}}}'
        exit 0
    fi
    sleep .1
done
exit 1
"#,
    )
    .unwrap();
    std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o755)).unwrap();
    let home = root.join("home");
    std::fs::write(
        home.join("settings.json"),
        serde_json::json!({"theme":"dark","terminal":{"images":"kitty"},"packages":[package]})
            .to_string(),
    )
    .unwrap();
    master.write_all(b"/reload\r").unwrap();
}

#[test]
fn terminal_image_exit_replay_uses_settings_from_an_interrupted_reload() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, _slave, process, _) =
        spawn_image_terminal(root.path(), "fullscreen", "iterm2", true);
    output_until(&mut master, |text| {
        text.contains(ITERM_IMAGE) && text.contains("Enter to send")
    });
    start_blocked_image_reload(root.path(), &mut master);
    let home = root.path().join("home");
    output_until(&mut master, |_| home.join("probe-started").exists());
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    output_until(&mut master, |text| {
        let Some((_, replay)) = text.split_once("\x1b[?1049l") else {
            return false;
        };
        assert!(!replay.contains(ITERM_IMAGE), "{replay}");
        replay.contains(IMAGE_TRANSCRIPT) && replay.contains(KITTY_IMAGE)
    });
    std::fs::File::create(home.join("probe-release")).unwrap();
}

#[test]
fn terminal_image_interrupt_during_reload_keeps_regular_transcript() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, slave, mut process, _) =
        spawn_image_terminal(root.path(), "regular", "iterm2", true);
    output_until(&mut master, |text| {
        completed_frame(text, |frame| {
            frame.contains(IMAGE_TRANSCRIPT) && frame.contains(ITERM_IMAGE)
        })
    });
    let terminal_flags = || {
        let mut settings = std::mem::MaybeUninit::<libc::termios>::uninit();
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), settings.as_mut_ptr()) },
            0
        );
        unsafe { settings.assume_init().c_lflag }
    };
    let cooked_flags = libc::ICANON | libc::ECHO;
    assert_eq!(terminal_flags() & cooked_flags, 0);

    start_blocked_image_reload(root.path(), &mut master);
    let home = root.path().join("home");
    let erased = std::cell::Cell::new(false);
    let record_erase = |text: &str| {
        erased.set(erased.get() || text.contains("\x1b[2J") || text.contains("\x1b[3J"));
    };
    output_until(&mut master, |text| {
        record_erase(text);
        home.join("probe-started").exists()
    });
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    output_until(&mut master, |text| {
        record_erase(text);
        text.contains("Resume this session with pi --session")
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "pi did not exit during package preparation"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    std::fs::File::create(home.join("probe-release")).unwrap();
    assert_eq!(status.code(), Some(130));
    assert_eq!(terminal_flags() & cooked_flags, cooked_flags);
    assert!(
        !erased.get(),
        "interrupted reload erased the regular transcript"
    );
}

#[test]
fn initial_appearance_report_keeps_late_color_replies() {
    let root = tempfile::tempdir().unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 || libc::ioctl(slave_fd, libc::TIOCSCTTY as libc::c_ulong, 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .env("TERM", "xterm-256color")
        .env("COLORTERM", "truecolor")
        .env_remove("PI_TRUE_COLOR")
        .env_remove("NO_COLOR")
        .args([
            "--no-trust",
            "--no-mcp",
            "--offline",
            "--use-theme",
            "system",
        ])
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave))
        .spawn()
        .unwrap();
    // The command retains copies of the slave descriptors. Drop them and let
    // the PTY master close before the child guard waits, including on panic.
    drop(command);
    let mut process = BoundedProcess {
        child,
        master: Some(unsafe { File::from_raw_fd(master_fd) }),
    };
    let master = process.master.as_mut().unwrap();
    output_until_with_timeout(master, Duration::from_secs(30), |text| {
        assert!(!text.contains("\x1b[38;2;"));
        text.contains("\x1b]11;?\x07") && text.contains("Enter to send")
    });
    // No DA1: the first appearance report is delayed past the 100 ms
    // initial fallback and arrives after the foreground reply.
    master.write_all(b"\x1b]10;rgb:ffff/ffff/ffff\x07").unwrap();
    std::thread::sleep(Duration::from_millis(160));
    // Discard already-rendered startup frames before observing the response.
    let mut pre_response = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        match master.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => pre_response.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => panic!("terminal read failed: {error}"),
        }
    }
    assert!(!String::from_utf8_lossy(&pre_response).contains("\x1b[38;2;"));
    master
        .write_all(b"\x1b[?997;1n\x1b]11;rgb:0000/0000/0000\x07")
        .unwrap();
    // Pi applies the accumulated late result when its query reaches DA1.
    master.write_all(b"\x1b[?1;2c").unwrap();
    output_until(master, |text| text.contains("\x1b[38;2;"));
    master.write_all(b"\x04").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut drain = [0; 8192];
        let _ = master.read(&mut drain);
        if let Some(status) = process.child.try_wait().unwrap() {
            assert!(
                status.success(),
                "terminal child exited unsuccessfully: {status}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child failed to exit after Ctrl-D"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn settings_picker_can_be_operated_with_the_mouse() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/settings\r").unwrap();
    output_until(&mut master, |text| text.contains("Auto-compact"));
    // Header row, picker title, then the first setting (SGR mouse uses 1-based coordinates).
    master.write_all(b"\x1b[<0;2;3M\x1b[<0;2;3m").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Auto-compact", "false")
    });
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["compaction"]["enabled"], false);
    master.write_all(b"Warnings\r").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Anthropic extra usage", "true")
    });
    master.write_all(b" ").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Anthropic extra usage", "false")
    });
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Warnings", "configure")
    });
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["warnings"]["anthropicExtraUsage"], false);
    assert_eq!(settings["compaction"]["enabled"], false);

    master.write_all(b"\x15Notices\r").unwrap();
    output_until(&mut master, |text| {
        [
            "Cache misses",
            "Cache warming",
            "Compaction costs",
            "Reasoning drops",
        ]
        .into_iter()
        .all(|label| settings_row_shows(text, label, "false"))
    });
    master.write_all(b" ").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Cache misses", "true")
    });
    master.write_all(b"\x1b[B\x1b[B\x1b[B\r").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Reasoning drops", "true")
    });
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Notices", "configure")
    });
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    assert_eq!(
        settings["notices"],
        serde_json::json!({"cacheMisses":true,"reasoningDrops":true})
    );
    assert_eq!(settings["warnings"]["anthropicExtraUsage"], false);
}

#[test]
fn project_settings_do_not_prevent_live_compaction_and_retry_toggles() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(root.path().join(".pi")).unwrap();
    std::fs::write(
        home.join("models.json"),
        r#"{"providers":{"openai":{"models":[{"id":"second"}]}}}"#,
    )
    .unwrap();
    std::fs::write(
        home.join("settings.json"),
        r#"{"compaction":{"enabled":false},"retry":{"enabled":false}}"#,
    )
    .unwrap();
    std::fs::write(
        root.path().join(".pi/settings.json"),
        r#"{"compaction":{"enabled":true},"retry":{"enabled":true}}"#,
    )
    .unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("OPENAI_API_KEY", "test-key")
            .env("TERM", "xterm-256color")
            .args([
                "--trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--context-window",
                "32",
                "--max-output-tokens",
                "128",
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    for (label, key, enabled, disabled) in [
        ("Auto-compact", "compaction", "true", "false"),
        ("Auto-retry", "retry", "on", "off"),
    ] {
        master.write_all(b"/settings\r").unwrap();
        output_until(&mut master, |text| text.contains("Auto-compact"));
        master.write_all(label.as_bytes()).unwrap();
        output_until(&mut master, |text| settings_row_shows(text, label, enabled));
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, label, disabled)
        });
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
        assert_eq!(saved[key]["enabled"], false);
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| settings_row_shows(text, label, enabled));
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
        assert_eq!(saved[key]["enabled"], true);
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, label, disabled)
        });
        master.write_all(b"\x1b").unwrap();
        output_until(&mut master, |text| text.contains("Enter to send"));
    }
    master.write_all(b"/model openai/second\r").unwrap();
    output_until(&mut master, |text| text.contains("Model: second"));
    master.write_all(b"test worker restart\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("automatic compaction is disabled")
    });
}

#[test]
fn editor_component_edits_then_restores_native_keys_in_both_modes() {
    use std::os::unix::fs::PermissionsExt;

    for mode in ["regular", "fullscreen"] {
        let directory = tempfile::tempdir().unwrap();
        let session_path = directory.path().join("session.jsonl");
        let extension = directory.path().join("editor-component.py");
        std::fs::write(&extension, r#"#!/usr/bin/env python3
import json, sys
if '--pi-rs-extension-v1' in sys.argv:
    json.load(sys.stdin)
    descriptor = {'id':'editor-component','dependencies':[],'commands':[],'tools':[],'lifecycle_hook':True,'session_lifecycle_hook':True,'lifecycle_host_calls':True}
    print(json.dumps({'version':1,'response':{'Ok':descriptor}}))
else:
    event = json.loads(sys.stdin.readline())['request']['event']
    def ui(method, **fields):
        print(json.dumps({'type':'ui','request':{'method':method,**fields}}), flush=True)
        assert json.loads(sys.stdin.readline()) == {'Ok':None}
    if event['type'] == 'session_start':
        ui('setEditorComponent', component={'placeholder':'Editor component ready','keybindings':[{'key':'f6','action':'deleteCharBackward'}]})
    if event['type'] == 'session_info_changed':
        ui('setEditorComponent', component=None)
        ui('notify', message='Default editor restored after ' + event['name'])
    print(json.dumps({'type':'complete','response':{'Ok':None}}), flush=True)
"#).unwrap();
        std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (mut master, mut slave) = (-1, -1);
        let mut size = libc::winsize {
            ws_row: 32,
            ws_col: 110,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::addr_of_mut!(size),
                )
            },
            0
        );
        let mut master = unsafe { File::from_raw_fd(master) };
        let slave = unsafe { File::from_raw_fd(slave) };
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let mut process = Process(
            Command::new(env!("CARGO_BIN_EXE_pi"))
                .current_dir(directory.path())
                .env("PI_RS_HOME", directory.path().join("home"))
                .env("TERM", "xterm-256color")
                .args(["--no-trust", "--no-mcp", "--offline", "--tui-mode", mode])
                .arg("--session")
                .arg(&session_path)
                .arg("--extension")
                .arg(&extension)
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave))
                .spawn()
                .unwrap(),
        );
        output_until(&mut master, |text| text.contains("component ready"));
        master
            .write_all(b"\x1b[?1;2c/name custom-keptX\x1b[17~\r")
            .unwrap();
        output_until(&mut master, |text| {
            text.contains("Default editor restored after custom-kept")
        });
        assert_eq!(
            pi_rs::Session::snapshot(&session_path).unwrap().name(),
            Some("custom-kept"),
            "{mode}"
        );
        master.write_all(b"/name plain-keptX\x1b[17~\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Default editor restored after plain-keptX")
        });
        assert_eq!(
            pi_rs::Session::snapshot(&session_path).unwrap().name(),
            Some("plain-keptX"),
            "{mode}"
        );
        master.write_all(b"/qui\t").unwrap();
        output_until(&mut master, |text| text.contains("/quit"));
        master.write_all(b"\r").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success(), "{mode}: {status}");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{mode}: completed quit command did not exit"
            );
            let mut output = [0; 8192];
            let _ = master.read(&mut output);
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

#[test]
fn delayed_color_reply_does_not_type_tail_or_open_editor() {
    use std::os::unix::fs::PermissionsExt;
    fn wait_for(master: &mut File, expected: &str, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        let mut bytes = Vec::new();
        let mut buffer = [0; 8192];
        while Instant::now() < deadline {
            match master.read(&mut buffer) {
                Ok(count) => bytes.extend_from_slice(&buffer[..count]),
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
                Err(error) => panic!("terminal read failed: {error}"),
            }
            let text = String::from_utf8_lossy(&bytes);
            if text.contains(expected) {
                return text.into_owned();
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!(
            "terminal did not render {expected}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let root = tempfile::tempdir().unwrap();
    let editor = root.path().join("editor.sh");
    let marker = root.path().join("editor-opened");
    std::fs::write(&editor, b"#!/bin/sh\ntouch \"$PI_TEST_EDITOR_MARKER\"\n").unwrap();
    std::fs::set_permissions(&editor, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    // Pi needs the slave on stdio but must not retain a copy of the master,
    // which the test closes to release any blocked terminal write.
    for fd in [master_fd, slave_fd] {
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) },
            0
        );
    }
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 || libc::ioctl(slave_fd, libc::TIOCSCTTY as libc::c_ulong, 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut process = BoundedProcess {
        child: command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .env("EDITOR", &editor)
            .env("PI_TEST_EDITOR_MARKER", &marker)
            .env_remove("VISUAL")
            .args(["--no-trust", "--no-mcp", "--offline"])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
        master: Some(master),
    };
    let master = process.master.as_mut().unwrap();
    wait_for(master, "Enter to send", Duration::from_secs(30));
    master.write_all(b"\x1b]11;rgb:0000/").unwrap();
    std::thread::sleep(Duration::from_millis(650));
    master.write_all(b"0000/0000\x07typed-after-color").unwrap();
    let text = wait_for(master, "typed-after-color", Duration::from_secs(10));
    assert!(
        !text.contains("0000/0000"),
        "OSC tail reached the editor: {text}"
    );
    assert!(!marker.exists(), "OSC BEL opened the external editor");
}

#[test]
fn debug_command_and_global_shortcut_write_session_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args(["--no-trust", "--no-mcp", "--offline", "--model", "fixture"])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/debug\r").unwrap();
    output_until(&mut master, |text| text.contains("Debug log written"));
    let path = home.join("pi-debug.log");
    let output = std::fs::read_to_string(&path).unwrap();
    assert!(output.contains("Terminal: 110x32"));
    assert!(output.contains("=== All rendered lines with visible widths ==="));
    assert!(output.contains("=== Agent messages (JSONL) ==="));
    std::fs::remove_file(&path).unwrap();
    master.write_all(b"/changelog\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("What's New") && text.contains("Added native MCP")
    });
    master.write_all(b"/settings\r").unwrap();
    output_until(&mut master, |text| text.contains("Settings"));
    // Kitty CSI-u for Shift+Ctrl+D, delivered while the settings picker has focus.
    master.write_all(b"\x1b[100;6u").unwrap();
    output_until(&mut master, |_| {
        std::fs::read_to_string(&path)
            .is_ok_and(|snapshot| snapshot.contains("=== Agent messages (JSONL) ==="))
    });
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| text.contains("Enter to send"));
}

#[test]
fn startup_extension_lists_and_reads_named_theme_palettes() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let theme_path = root.path().join("explicit-theme.json");
    let mut theme: serde_json::Value =
        serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
    theme["name"] = serde_json::json!("startup:fixture-theme");
    theme["colors"]["accent"] = serde_json::json!("#123456");
    std::fs::write(&theme_path, format!("\u{feff}{theme}")).unwrap();
    let project_themes = root.path().join(".pi/themes");
    std::fs::create_dir_all(&project_themes).unwrap();
    theme["name"] = serde_json::json!("untrusted-project-theme");
    std::fs::write(project_themes.join("project.json"), theme.to_string()).unwrap();
    let extension = root.path().join("theme-catalog");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys
if '--pi-rs-extension-v1' in sys.argv:
    request = json.load(sys.stdin)
    result = {'id':'theme-catalog','dependencies':[],'commands':[],'tools':[],'lifecycle_hook':True,'session_lifecycle_hook':True,'lifecycle_host_calls':True} if request['type'] == 'describe' else None
    print(json.dumps({'version':1,'response':{'Ok':result}}))
else:
    frame = json.loads(sys.stdin.readline())
    event = frame['request']['event']
    def active():
        print(json.dumps({'type':'ui','request':{'method':'getActiveTheme'}}), flush=True)
        return json.loads(sys.stdin.readline())['Ok']
    def notify(message):
        print(json.dumps({'type':'ui','request':{'method':'notify','message':message}}), flush=True)
        assert json.loads(sys.stdin.readline()) == {'Ok':None}
    if event['type'] == 'session_start' and event['reason'] == 'new':
        assert active()['name'] == 'light'
        notify('Rebound theme palette verified')
    elif event['type'] == 'session_start':
        print(json.dumps({'type':'ui','request':{'method':'getAllThemes'}}), flush=True)
        response = json.loads(sys.stdin.readline())['Ok']
        assert response[0] == {'name':'system'}, response
        expected = os.path.realpath(os.environ['PI_TEST_THEME_PATH'])
        assert {'name':'startup:fixture-theme','path':expected} in response, response
        def lookup(name):
            print(json.dumps({'type':'ui','request':{'method':'getTheme','name':name}}), flush=True)
            return json.loads(sys.stdin.readline())['Ok']
        selected = lookup('startup:fixture-theme')
        assert selected['path'] == expected and selected['name'] == 'startup:fixture-theme', selected
        assert selected['colors']['accent'] == {'kind':'rgb','r':18,'g':52,'b':86}, selected
        light = lookup('light')
        assert light['name'] == 'light' and 'path' not in light and light['colors'], light
        system = lookup('system')
        assert system['name'] == 'system' and 'path' not in system and system['colors'], system
        assert lookup('untrusted-project-theme') is None
        assert lookup(expected) is None
        assert lookup('light/dark') is None
        def select(theme):
            print(json.dumps({'type':'ui','request':{'method':'setTheme','theme':theme}}), flush=True)
            return json.loads(sys.stdin.readline())['Ok']
        assert select('startup:fixture-theme') == {'success':True}
        assert active() == selected
        settings_path = os.path.join(os.environ['PI_RS_HOME'], 'settings.json')
        def settings_bytes():
            with open(settings_path, 'rb') as file:
                return file.read()
        for missing in ['untrusted-project-theme', 'light/dark', expected, 'Startup:fixture-theme']:
            assert select('light') == {'success':True}
            assert active() == light
            saved = settings_bytes()
            result = select(missing)
            assert result['success'] is False and result['error'], result
            fallback = active()
            assert fallback['name'] == 'system' and 'path' not in fallback, fallback
            assert settings_bytes() == saved
        assert select('light') == {'success':True}
        saved = settings_bytes()
        with open(settings_path, 'wb') as file:
            file.write(b'{')
        try:
            result = select('startup:fixture-theme')
            assert result['success'] is False and result['error'], result
            assert active() == light
            assert settings_bytes() == b'{'
        finally:
            with open(settings_path, 'wb') as file:
                file.write(saved)
        assert select(selected) == {'success':True}
        palette = active()
        assert palette['name'] == selected['name'], palette
        assert palette['colors'] == selected['colors'], palette
        assert 'path' not in palette, palette
        notify('Startup theme palettes verified')
    print(json.dumps({'type':'complete','response':{'Ok':None}}), flush=True)
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--tui-mode",
                "fullscreen",
            ])
            .arg("--theme")
            .arg(&theme_path)
            .args(["--use-theme", "startup:fixture-theme"])
            .arg("--extension")
            .arg(&extension)
            .env("PI_RS_HOME", root.path().join("home"))
            .env("PI_TEST_THEME_PATH", &theme_path)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| {
        text.contains("Startup theme palettes verified")
    });
    master.write_all(b"/new\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Rebound theme palette verified")
    });
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("home/settings.json")).unwrap())
            .unwrap();
    assert_eq!(settings["theme"], "light");
}

fn start_theme_extension(root: &std::path::Path, args: &[&str], handler: &str) -> (File, Process) {
    use std::os::unix::fs::PermissionsExt;

    let home = root.join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("settings.json"), br#"{"theme":"dark"}"#).unwrap();
    std::fs::write(
        home.join("models.json"),
        br#"{"providers":{"ollama":{"models":[{"id":"fixture","reasoning":true},{"id":"alternate","reasoning":true}]}}}"#,
    )
    .unwrap();
    let extension = root.join("theme-extension.py");
    let fixture = r#"#!/usr/bin/env python3
import json, sys
if '--pi-rs-extension-v1' in sys.argv:
    request = json.load(sys.stdin)
    result = {'id':'theme-extension','dependencies':[],'commands':[],'tools':[],'lifecycle_hook':True,'lifecycle_host_calls':True,'session_lifecycle_hook':True,'model_selection_hook':True} if request['type'] == 'describe' else None
    if request['type'] == 'render_header':
        context = request['context']
        result = {'lines': [[{'text': f"Mounted header {context['width']} {context['theme']['name']}"}]]}
    print(json.dumps({'version':1,'response':{'Ok':result}}))
    sys.exit(0)
event = json.loads(sys.stdin.readline())['request']['event']
def ui(method, **fields):
    print(json.dumps({'type':'ui','request':{'method':method,**fields}}), flush=True)
    response = json.loads(sys.stdin.readline())
    assert 'Ok' in response, response
    return response['Ok']
def active():
    return ui('getActiveTheme')['name']
def notify(message):
    assert ui('notify', message=message) is None
"#;
    std::fs::write(
        &extension,
        format!("{fixture}{handler}\nprint(json.dumps({{'type':'complete','response':{{'Ok':None}}}}), flush=True)\n"),
    )
    .unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();

    let (mut master, mut slave) = (-1, -1);
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master) };
    let slave = unsafe { File::from_raw_fd(slave) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root)
            .env("PI_RS_HOME", home)
            .env("TERM", "xterm-256color")
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--model",
                "fixture",
                "--thinking",
                "off",
            ])
            .arg("--extension")
            .arg(extension)
            .args(args)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    master.write_all(b"\x1b[?1;2c").unwrap();
    (master, process)
}

#[test]
fn theme_requests_finish_during_selection_callbacks() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, _process) = start_theme_extension(
        root.path(),
        &[],
        r#"
if event['type'] == 'session_start':
    notify('Theme selection fixture ready')
elif event['type'] == 'thinking_level_select' and event['level'] == 'low':
    assert active() == 'dark'
    assert ui('getTheme', name='system')['name'] == 'system'
    assert ui('setTheme', theme='light') == {'success':True}
    assert active() == 'light'
    notify('Selection callback finished with light theme')
elif event['type'] == 'model_select':
    assert active() == 'light'
    assert ui('setTheme', theme='dark') == {'success':True}
    assert active() == 'dark'
    notify('Model callback finished with dark theme')
"#,
    );
    output_until(&mut master, |text| {
        text.contains("Theme selection fixture ready")
    });
    master.write_all(b"/thinking low\r").unwrap();
    output_until_with_timeout(&mut master, Duration::from_secs(5), |text| {
        assert!(!text.contains("timed out"), "{text}");
        text.contains("Selection callback finished with light theme")
    });
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("home/settings.json")).unwrap())
            .unwrap();
    assert_eq!(settings["theme"], "light");
    master.write_all(b"/model ollama/alternate\r").unwrap();
    output_until_with_timeout(&mut master, Duration::from_secs(5), |text| {
        assert!(!text.contains("timed out"), "{text}");
        text.contains("Model callback finished with dark theme")
    });
}

#[test]
fn mounted_header_redraws_after_extension_theme_selection_in_both_modes() {
    for mode in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let (mut master, _process) = start_theme_extension(
            root.path(),
            &["--tui-mode", mode],
            r#"
if event['type'] == 'session_start':
    assert active() == 'dark'
    assert ui('setHeaderRenderer', enabled=True) is None
elif event['type'] == 'thinking_level_select' and event['level'] == 'low':
    assert ui('setTheme', theme='light') == {'success':True}
    assert active() == 'light'
"#,
        );
        output_until(&mut master, |text| text.contains("Mounted header 110 dark"));
        master.write_all(b"/thinking low\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Mounted header 110 light")
        });
    }
}

#[test]
fn final_selection_callback_can_read_but_cannot_change_theme() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, mut process) = start_theme_extension(
        root.path(),
        &[],
        r#"
import os, pathlib
if event['type'] == 'session_start':
    notify('Final selection fixture ready')
elif event['type'] == 'thinking_level_select':
    result = [active(), ui('getTheme', name='system')['name'], ui('setTheme', theme='light'), active()]
    path = pathlib.Path(os.environ['PI_RS_HOME']) / 'final-selection.json'
    path.write_text(json.dumps(result))
"#,
    );
    output_until(&mut master, |text| {
        text.contains("Final selection fixture ready")
    });
    master
        .write_all(b"!echo shell-$(printf waiting); sleep 20\r")
        .unwrap();
    output_until(&mut master, |text| text.contains("shell-waiting"));
    // Shift-Tab queues a thinking change while the shell keeps the worker busy.
    master.write_all(b"\x1b[Z").unwrap();
    output_until(&mut master, |text| text.contains("Thinking level:"));
    let home = root.path().join("home");
    let result = home.join("final-selection.json");
    assert!(!result.exists());
    master.write_all(b"/quit\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "interactive process failed: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "final selection did not settle");
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        std::thread::sleep(Duration::from_millis(5));
    }
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(result).unwrap()).unwrap();
    assert_eq!(
        result,
        serde_json::json!(["dark", "system", {"success":false,"error":"UI not available"}, "dark"])
    );
    let settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
    assert_eq!(settings["theme"], "dark");
}

#[test]
fn reload_discards_extension_palette_if_saved_theme_is_missing() {
    let root = tempfile::tempdir().unwrap();
    let (mut master, _process) = start_theme_extension(
        root.path(),
        &[],
        r#"
if event['type'] == 'session_start' and event['reason'] == 'startup':
    palette = ui('getTheme', name='light')
    palette['name'] = 'local-palette'
    assert ui('setTheme', theme=palette) == {'success':True}
    assert active() == 'local-palette'
    notify('Local palette installed')
elif event['type'] == 'session_start' and event['reason'] == 'reload':
    assert active() == 'system', active()
    notify('Reload replaced the local palette with system')
"#,
    );
    output_until(&mut master, |text| text.contains("Local palette installed"));
    std::fs::write(
        root.path().join("home/settings.json"),
        br#"{"theme":"missing-theme"}"#,
    )
    .unwrap();
    master.write_all(b"/reload\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Reload replaced the local palette with system")
    });
}

#[test]
fn reload_reads_saved_theme_after_named_extension_selection() {
    for cli_theme in [None, Some("system")] {
        let root = tempfile::tempdir().unwrap();
        let args = cli_theme.map(|name| ["--use-theme", name]);
        let (mut master, _process) = start_theme_extension(
            root.path(),
            args.as_ref().map_or(&[], |args| &args[..]),
            r#"
if event['type'] == 'session_start' and event['reason'] == 'startup':
    assert ui('setTheme', theme='light') == {'success':True}
    assert active() == 'light'
    notify('Named extension theme installed')
elif event['type'] == 'session_start' and event['reason'] == 'reload':
    notify('Theme after reload is ' + active())
"#,
        );
        output_until(&mut master, |text| {
            text.contains("Named extension theme installed")
        });
        std::fs::write(
            root.path().join("home/settings.json"),
            br#"{"theme":"dark"}"#,
        )
        .unwrap();
        master.write_all(b"/reload\r").unwrap();
        let expected = format!("Theme after reload is {}", cli_theme.unwrap_or("dark"));
        output_until(&mut master, |text| text.contains(&expected));
    }
}

#[test]
fn named_theme_lookup_and_selection_share_snapshot_until_reload() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("theme.json");
    let mut theme: serde_json::Value =
        serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
    theme["name"] = serde_json::json!("original");
    std::fs::write(&path, theme.to_string()).unwrap();
    let (mut master, _process) = start_theme_extension(
        root.path(),
        &["--theme", path.to_str().unwrap()],
        r#"
if event['type'] == 'session_start' and event['reason'] == 'startup':
    assert ui('getTheme', name='original')['name'] == 'original'
    notify('Original theme snapshot ready')
elif event['type'] == 'session_info_changed':
    assert ui('getTheme', name='renamed') is None
    assert ui('setTheme', theme='renamed')['success'] is False
    original = ui('getTheme', name='original')
    assert ui('setTheme', theme='original') == {'success':True}
    assert ui('getActiveTheme') == original
    notify('Lookup and selection used the original snapshot')
elif event['type'] == 'session_start' and event['reason'] == 'reload':
    assert ui('getTheme', name='original') is None
    renamed = ui('getTheme', name='renamed')
    assert ui('setTheme', theme='renamed') == {'success':True}
    assert ui('getActiveTheme') == renamed
    notify('Lookup and selection used the refreshed snapshot')
"#,
    );
    output_until(&mut master, |text| {
        text.contains("Original theme snapshot ready")
    });
    theme["name"] = serde_json::json!("renamed");
    theme["colors"]["accent"] = serde_json::json!("#123456");
    std::fs::write(&path, theme.to_string()).unwrap();
    master.write_all(b"/name refresh-test\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Lookup and selection used the original snapshot")
    });
    master.write_all(b"/reload\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Lookup and selection used the refreshed snapshot")
    });
}

#[test]
fn extension_tool_renderer_displays_resumed_result_and_updates_on_expand() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let session = root.path().join("session.jsonl");
    let source = root.path().join("pi-session.jsonl");
    std::fs::write(
        &source,
        r#"{"type":"session","version":3}
{"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"lookup","arguments":{"path":"example"}}]}}
{"type":"message","id":"r","parentId":"a","message":{"role":"toolResult","toolCallId":"call","toolName":"lookup","content":[{"type":"text","text":"original result"}],"isError":false}}
"#,
    )
    .unwrap();

    let seed = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--offline", "--no-mcp", "--no-trust", "--session"])
        .arg(&session)
        .arg("import")
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        seed.status.success(),
        "{}",
        String::from_utf8_lossy(&seed.stderr)
    );

    let extension = root.path().join("renderer.py");
    std::fs::write(
        &extension,
        r#"#!/usr/bin/env python3
import json, sys
request = json.load(sys.stdin)
result = None
if request["type"] == "describe":
    result = {"id":"renderer", "commands":[], "dependencies":[], "tools":[{
        "spec":{"name":"lookup","description":"Lookup","parameters":{"type":"object"}},
        "execution":"sequential", "renderers":{"shell":"default","call":True,"result":True}
    }]}
elif request["type"] == "render_tool_section":
    context = request["context"]
    if request["section"] == "call":
        text = "rendered call " + context["args"]["path"]
    else:
        original = context["result"]["content"][0]["text"]
        text = ("expanded " if context["expanded"] else "collapsed ") + original
    result = {"lines":[[{"text":text,"color":"accent"}]]}
print(json.dumps({"version":1,"response":{"Ok":result}}))
"#,
    )
    .unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o700)).unwrap();

    let (mut master_fd, mut slave_fd) = (-1, -1);
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--offline",
                "--no-mcp",
                "--no-trust",
                "--model",
                "fixture",
                "--tui-mode",
                "fullscreen",
                "--session",
            ])
            .arg(&session)
            .arg("--extension")
            .arg(&extension)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until_with_timeout(&mut master, Duration::from_secs(15), |text| {
        text.contains("rendered call example") && text.contains("collapsed original result")
    });
    master.write_all(b"\x0f").unwrap();
    output_until(&mut master, |text| {
        text.contains("expanded original result")
    });
}

#[test]
fn delayed_thinking_callback_preserves_the_next_selection() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let session_path = root.path().join("session.jsonl");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("models.json"),
        br#"{"providers":{"ollama":{"models":[{"id":"fixture","reasoning":true,"thinkingLevelMap":{"xhigh":"xhigh"}}]}}}"#,
    ).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let extension = root.path().join("selection.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, socket, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'selection', 'dependencies':[], 'commands':[], 'tools':[], 'lifecycle_hook':True, 'model_selection_hook':True}
elif request['type'] == 'lifecycle':
    event = request['event']
    if event.get('type') == 'thinking_level_select' and event.get('level') == 'low':
        host, port = os.environ['PI_RS_TEST_SELECTION_GATE'].rsplit(':', 1)
        with socket.create_connection((host, int(port)), timeout=10) as gate:
            gate.sendall(b'S')
            if gate.recv(1) != b'G':
                raise RuntimeError('selection gate was not released')
    with open(os.path.join(os.environ['PI_RS_HOME'], 'events.jsonl'), 'a') as log:
        log.write(json.dumps(event) + '\n')
    result = None
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (mut master_fd, mut slave_fd) = (-1, -1);
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .env(
                "PI_RS_TEST_SELECTION_GATE",
                listener.local_addr().unwrap().to_string(),
            )
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--thinking",
                "off",
                "--session",
            ])
            .arg(&session_path)
            .arg("--extension")
            .arg(&extension)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/thinking low\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut gate = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(error) if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline => {
                let mut output = [0; 8192];
                let _ = master.read(&mut output);
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("first selection callback did not start: {error}"),
        }
    };
    prepare_gate(&gate);
    let mut started = [0];
    gate.read_exact(&mut started).unwrap();
    assert_eq!(&started, b"S");
    // Queue the second selection while the first callback is still held.
    master.write_all(b"/thinking high\r").unwrap();
    assert!(!home.join("events.jsonl").exists());
    let session = pi_rs::Session::snapshot(&session_path).unwrap();
    assert!(
        session
            .model_selection()
            .is_some_and(|(_, level)| level.as_deref() == Some("low")),
        "the UI must not apply the queued selection before the callback completes"
    );
    gate.write_all(b"G").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let events = loop {
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        let events: Vec<serde_json::Value> = std::fs::read_to_string(home.join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        if events.len() >= 2 {
            break events;
        }
        assert!(
            Instant::now() < deadline,
            "selection callbacks did not complete: {events:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        events,
        vec![
            serde_json::json!({"type":"thinking_level_select", "level":"low", "previousLevel":"off"}),
            serde_json::json!({"type":"thinking_level_select", "level":"high", "previousLevel":"low"}),
        ]
    );
    let session = pi_rs::Session::snapshot(&session_path).unwrap();
    assert!(
        session
            .model_selection()
            .is_some_and(|(_, level)| level.as_deref() == Some("high"))
    );
}

#[test]
fn thinking_command_notifies_native_extension_from_terminal() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("models.json"),
        br#"{"providers":{"ollama":{"models":[{"id":"fixture","reasoning":true,"thinkingLevelMap":{"xhigh":"xhigh"}}]}}}"#,
    )
    .unwrap();
    let extension = root.path().join("selection.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'selection', 'dependencies':[], 'commands':[], 'tools':[], 'lifecycle_hook':True, 'model_selection_hook':True}
elif request['type'] == 'lifecycle':
    with open(os.path.join(os.environ['PI_RS_HOME'], 'events.jsonl'), 'a') as log:
        log.write(json.dumps(request['event']) + '\n')
    result = None
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--thinking",
                "off",
                "--extension",
            ])
            .arg(&extension)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/thinking low\r").unwrap();
    let path = home.join("events.jsonl");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut terminal_output = Vec::new();
    let mut buffer = [0; 8192];
    while std::fs::metadata(&path).map_or(true, |metadata| metadata.len() == 0)
        && Instant::now() < deadline
    {
        match master.read(&mut buffer) {
            Ok(count) => terminal_output.extend_from_slice(&buffer[..count]),
            Err(error)
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
            Err(error) => panic!("terminal read failed: {error}"),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let contents = std::fs::read_to_string(path).unwrap_or_else(|error| {
        panic!(
            "extension did not receive selection: {error}; terminal output: {}",
            String::from_utf8_lossy(&terminal_output)
        )
    });
    let event: serde_json::Value = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
    assert_eq!(
        event,
        serde_json::json!({"type":"thinking_level_select", "level":"low", "previousLevel":"off"})
    );
}

#[test]
fn follow_up_submits_the_selected_command_with_a_remapped_key() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    std::fs::write(
        home.join("keybindings.json"),
        br#"{"app.message.followUp":"ctrl+q"}"#,
    )
    .unwrap();
    let (mut master, _process) = spawn_capability_test_terminal(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", home)
            .env("TERM", "xterm-256color")
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-session",
            ]),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/re\t").unwrap();
    output_until(&mut master, |text| {
        text.contains("/reload") && text.contains("/resume")
    });
    master.write_all(b"\x11").unwrap(); // Ctrl+Q
    output_until(&mut master, |text| text.contains("Reloaded settings"));
}

#[test]
fn follow_up_confirms_a_file_completion_and_leaves_the_draft_editable() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("follow-up-a.txt"), "").unwrap();
    std::fs::write(root.path().join("follow-up-b.txt"), "").unwrap();
    let (mut master, _process) = spawn_capability_test_terminal(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .args([
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-session",
            ]),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"check @follow-up-\t").unwrap();
    output_until(&mut master, |text| {
        text.contains("@follow-up-a.txt") && text.contains("@follow-up-b.txt")
    });
    master.write_all(b"\x1b\r!").unwrap(); // Alt+Enter, then keep typing
    output_until(&mut master, |text| text.contains("check @follow-up-a.txt!"));
}

#[test]
fn autocomplete_settings_cycle_starts_at_first_preset_for_custom_value() {
    for mode in ["regular", "fullscreen"] {
        check_numeric_setting_cycle(
            mode,
            r#"{"autocompleteMaxVisible":4}"#,
            "Autocomplete max items",
            "autocompleteMaxVisible",
            "4",
            &[3, 5, 7],
        );
    }
}

#[test]
fn settings_treats_integer_equivalent_editor_padding_as_the_same_preset() {
    for mode in ["regular", "fullscreen"] {
        check_numeric_setting_cycle(
            mode,
            r#"{"editorPaddingX":2.0}"#,
            "Editor padding",
            "editorPaddingX",
            "2",
            &[3, 0],
        );
    }
}

fn check_numeric_setting_cycle(
    mode: &str,
    settings: &str,
    label: &str,
    key: &str,
    initial_value: &str,
    expected: &[u64],
) {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("settings.json"), settings).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--no-skills",
                "--no-extensions",
                "--no-context-files",
                "--offline",
                "--model",
                "fixture",
                "--tui-mode",
                mode,
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    for name in ["sample-a", "sample-b"] {
        std::fs::File::create(root.path().join(name)).unwrap();
    }
    let file_draft = format!("look at {}/sample-\t", root.path().display());
    master.write_all(file_draft.as_bytes()).unwrap();
    output_until(&mut master, |text| {
        text.contains("Autocomplete") && text.contains("sample-b")
    });
    master.write_all(b"\r-draft").unwrap();
    output_until(&mut master, |text| text.contains("sample-a-draft"));

    master.write_all(b"\x15/se\t").unwrap();
    output_until(&mut master, |text| {
        text.contains("Autocomplete") && text.contains("/settings")
    });
    master.write_all(b"\x1b[B\t suffix").unwrap();
    output_until(&mut master, |text| text.contains("/settings suffix"));

    master.write_all(b"\x15/se\t").unwrap();
    output_until(&mut master, |text| {
        text.contains("Autocomplete") && text.contains("/settings")
    });
    master.write_all(b"\x1b[B\r").unwrap();
    output_until(&mut master, |text| text.contains("Auto-compact"));
    master.write_all(label.as_bytes()).unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, label, initial_value)
    });
    for expected in expected {
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, label, &expected.to_string())
        });
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap();
        assert_eq!(saved[key], *expected);
    }
}

#[test]
fn settings_stays_open_after_changes_and_editor_cancel() {
    for mode in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let session_path = root.path().join("session.jsonl");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let home = root.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(home.join("models.json"), br#"{"providers":{"ollama":{"models":[{"id":"test-fixture","reasoning":true,"thinkingLevelMap":{"xhigh":"xhigh"}}]}}}"#).unwrap();
        let project_settings = root.path().join(".pi/settings.json");
        let project = br#"{"externalEditor":"project-editor"}"#;
        std::fs::create_dir(project_settings.parent().unwrap()).unwrap();
        std::fs::write(&project_settings, project).unwrap();
        listener.set_nonblocking(true).unwrap();
        let (finish, finished) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let mut finished = Some(finished);
            let mut streaming: Option<std::thread::JoinHandle<()>> = None;
            let deadline = Instant::now() + Duration::from_secs(60);
            while Instant::now() < deadline
                && streaming.as_ref().is_none_or(|task| !task.is_finished())
            {
                let (mut socket, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut method = String::new();
                if reader.read_line(&mut method).is_err() || method.is_empty() {
                    continue;
                }
                let mut size = 0;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        size = value.trim().parse().unwrap();
                    }
                }
                if method.starts_with("GET ") {
                    let body = br#"{"data":[{"id":"test-fixture"}]}"#;
                    let _ = write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(body);
                    continue;
                }
                let mut body = vec![0; size];
                if reader.read_exact(&mut body).is_err() {
                    continue;
                }
                let Some(finished) = finished.take() else {
                    continue;
                };
                streaming = Some(std::thread::spawn(move || {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"live-stream-marker\"},\"finish_reason\":null}]}\n\n").unwrap();
                    socket.flush().unwrap();
                    let _ = finished.recv_timeout(Duration::from_secs(45));
                    let _ = socket.write_all(b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n");
                }));
            }
            if let Some(task) = streaming {
                task.join().unwrap();
            }
        });
        let mut master_fd = -1;
        let mut slave_fd = -1;
        let mut size = libc::winsize {
            ws_row: 32,
            ws_col: 110,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::addr_of_mut!(size),
                )
            },
            0
        );
        let mut master = unsafe { File::from_raw_fd(master_fd) };
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let _process = Process(
            Command::new(env!("CARGO_BIN_EXE_pi"))
                .args([
                    "--workspace",
                    root.path().to_str().unwrap(),
                    "--session",
                    session_path.to_str().unwrap(),
                    "--trust",
                    "--no-mcp",
                    "--offline",
                    "--model",
                    "test-fixture",
                    "--endpoint",
                    &endpoint,
                    "--tui-mode",
                    mode,
                ])
                .env("PI_RS_HOME", root.path().join("home"))
                .env("TERM", "xterm-256color")
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave))
                .spawn()
                .unwrap(),
        );
        output_until(&mut master, |text| text.contains("Enter to send"));
        master.write_all(b"hello\r").unwrap();
        output_until(&mut master, |text| text.contains("live-stream-marker"));
        master.write_all(b"/settings\r").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, "Auto-compact", "true")
        });
        master.write_all(b" ").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, "Auto-compact", "false")
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, "Auto-compact", "true")
        });
        for (setting, first, second) in [
            ("Transport", "sse", Some("websocket")),
            ("HTTP idle timeout", "disabled", Some("30 sec")),
            ("Steering mode", "all", None),
            ("Follow-up mode", "all", None),
        ] {
            master.write_all(setting.as_bytes()).unwrap();
            master.write_all(b"\r").unwrap();
            output_until(&mut master, |text| settings_row_shows(text, setting, first));
            if let Some(second) = second {
                master.write_all(b"\r").unwrap();
                output_until(&mut master, |text| {
                    settings_row_shows(text, setting, second)
                });
            }
            master.write_all(&vec![0x7f; setting.len()]).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(session) = pi_rs::Session::snapshot(&session_path)
                && session.queue_mode(pi_rs::InputKind::Steer) == pi_rs::QueueMode::All
                && session.queue_mode(pi_rs::InputKind::FollowUp) == pi_rs::QueueMode::All
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "queue mode changes were not applied to the session"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        // Fix the mistyped search in place before activating its result.
        master
            .write_all(b"TUU mode\x01\x1b[C\x1b[C\x1b[3~I\x1b[F\r")
            .unwrap();
        let changed = if mode == "regular" {
            "fullscreen"
        } else {
            "regular"
        };
        output_until(&mut master, |text| {
            settings_row_shows(text, "TUI mode", changed)
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            settings_row_shows(text, "TUI mode", mode)
        });
        master
            .write_all(b"\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f")
            .unwrap();
        let thinking_label = "Default thinking level per model";
        master.write_all(thinking_label.as_bytes()).unwrap();
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| text.contains("Thinking: select model"));
        master.write_all(b"ollama/test-fixture\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Thinking level · ollama/test-fixture") && text.contains("high")
        });
        master.write_all(b"high\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("ollama/test-fixture · high")
        });
        master.write_all(b"\x03").unwrap();
        output_until(&mut master, |text| {
            text.contains("Default thinking level per model") && text.contains("1 configured")
        });
        master.write_all(&vec![0x7f; thinking_label.len()]).unwrap();
        master.write_all(b"Edit shellPath\r").unwrap();
        output_until(&mut master, |text| text.contains("Edit shellPath as JSON"));
        master.write_all(b"\x1b").unwrap();
        output_until(&mut master, |text| {
            text.contains("Edit shellPath") && text.contains("to change")
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| text.contains("Edit shellPath as JSON"));
        master.write_all(b"\x13").unwrap();
        output_until(&mut master, |text| {
            text.contains("Edit shellPath") && text.contains("to change")
        });
        master.write_all(b"\x15Edit externalEditor\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Edit externalEditor as JSON")
                && text.contains("overridden by the project")
        });
        master.write_all(b"\x13").unwrap();
        output_until(&mut master, |text| {
            text.contains("Edit externalEditor") && text.contains("to change")
        });
        let settings: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("home/settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["transport"], "websocket");
        assert_eq!(settings["httpIdleTimeoutMs"], 30_000);
        assert_eq!(settings["steeringMode"], "all");
        assert_eq!(settings["followUpMode"], "all");
        assert_eq!(
            settings["modelThinkingLevels"]["ollama/test-fixture"],
            "high"
        );
        assert_eq!(settings["shellPath"], "");
        assert_eq!(settings["externalEditor"], "");
        assert_eq!(std::fs::read(project_settings).unwrap(), project);
        finish.send(()).unwrap();
        server.join().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(session) = pi_rs::Session::snapshot(&session_path)
                && session.model_selection().is_some_and(|(model, level)| {
                    model == "test-fixture" && level.as_deref() == Some("high")
                })
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "thinking level was not saved after streaming completed: {}",
                std::fs::read_to_string(&session_path).unwrap_or_default()
            );
            let mut buffer = [0; 8192];
            let _ = master.read(&mut buffer);
            std::thread::sleep(Duration::from_millis(20));
        }
        master.write_all(b"\x1b/quit\r").unwrap();
    }
}

#[test]
fn resize_and_suspend_preserve_input_and_restore_terminal() {
    exercise_terminal_suspend("fullscreen", SuspendTrigger::Key);
}

#[test]
fn external_suspend_restores_the_terminal_and_preserves_the_draft() {
    for mode in ["regular", "fullscreen"] {
        exercise_terminal_suspend(mode, SuspendTrigger::Signal);
    }
}

#[test]
fn external_suspend_interrupts_the_wait_during_reload_without_losing_preparation() {
    exercise_terminal_suspend("fullscreen", SuspendTrigger::Reload);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SuspendTrigger {
    Key,
    Signal,
    Reload,
}

fn exercise_terminal_suspend(mode: &str, trigger: SuspendTrigger) {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 30,
        ws_col: 100,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // openpty initializes both descriptors only when it returns zero.
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), original.as_mut_ptr()) },
        0
    );
    let original = unsafe { original.assume_init() };
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    command.process_group(0);
    // The test owns this child job, so suspension cannot stop the test runner.
    unsafe {
        command.pre_exec(|| {
            libc::signal(libc::SIGTSTP, libc::SIG_DFL);
            Ok(())
        });
    }
    let mut process = Process(
        command
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--tui-mode",
                mode,
            ])
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    for round in 0..16 {
        size.ws_col = 90 + round;
        assert_eq!(
            unsafe { libc::ioctl(slave.as_raw_fd(), libc::TIOCSWINSZ, &size) },
            0
        );
        assert_eq!(
            unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGWINCH) },
            0
        );
        let marker = format!("resize-marker-{round}");
        master.write_all(marker.as_bytes()).unwrap();
        output_until(&mut master, |text| text.contains(&marker));
        master.write_all(b"\x15").unwrap();
    }
    if trigger == SuspendTrigger::Reload {
        use std::os::unix::fs::PermissionsExt;
        let package = root.path().join("slow-package.sh");
        std::fs::write(
            &package,
            r#"#!/bin/sh
: > "$PI_RS_HOME/probe-started"
for i in $(seq 1 160); do
    if [ -f "$PI_RS_HOME/probe-release" ]; then
        printf '%s\n' '{"version":1,"response":{"Ok":{"id":"probe","dependencies":[],"commands":[],"tools":[]}}}'
        exit 0
    fi
    sleep .1
done
exit 1
"#,
        )
        .unwrap();
        std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[package]}).to_string(),
        )
        .unwrap();
        master.write_all(b"/reload\r").unwrap();
        output_until(&mut master, |_| home.join("probe-started").exists());
    } else {
        master.write_all(b"resume-draft").unwrap();
        output_until(&mut master, |text| text.contains("resume-draft"));
    }
    if trigger == SuspendTrigger::Key {
        master.write_all(b"\x1a").unwrap();
    } else {
        assert_eq!(
            unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGTSTP) },
            0
        );
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stopped = false;
    let mut before_stop = Vec::new();
    let mut buffer = [0; 8192];
    while Instant::now() < deadline {
        while let Ok(count @ 1..) = master.read(&mut buffer) {
            before_stop.extend_from_slice(&buffer[..count]);
        }
        let mut status = 0;
        let observed = unsafe {
            libc::waitpid(
                process.0.id() as libc::pid_t,
                &mut status,
                libc::WNOHANG | libc::WUNTRACED,
            )
        };
        if observed > 0 {
            assert!(libc::WIFSTOPPED(status));
            assert_eq!(libc::WSTOPSIG(status), libc::SIGTSTP);
            stopped = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(stopped, "foreground job did not suspend");
    while let Ok(count @ 1..) = master.read(&mut buffer) {
        before_stop.extend_from_slice(&buffer[..count]);
    }
    let before_stop = String::from_utf8_lossy(&before_stop);
    assert!(before_stop.contains("\x1b[?2004l"), "{before_stop:?}");
    if mode == "fullscreen" {
        assert!(before_stop.contains("\x1b[?1049l"), "{before_stop:?}");
        assert!(before_stop.contains("\x1b[?1006l"), "{before_stop:?}");
    }
    let mut paused = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), paused.as_mut_ptr()) },
        0
    );
    let paused = unsafe { paused.assume_init() };
    assert_eq!(
        paused.c_lflag & !libc::PENDIN,
        original.c_lflag & !libc::PENDIN
    );
    assert_eq!(
        unsafe { libc::kill(-(process.0.id() as libc::pid_t), libc::SIGCONT) },
        0
    );
    let expected_after_resume = if trigger == SuspendTrigger::Reload {
        std::fs::File::create(home.join("probe-release")).unwrap();
        "Reloaded settings"
    } else {
        "resume-draft"
    };
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b]11;?"));
        text.contains(expected_after_resume) && text.contains("\x1b[>4;2m")
    });
    // An old or foreign DA1 and late OSC must not launch another query after
    // the shell owned the terminal, and ordinary input must still be usable.
    master
        .write_all(b"\x1b[?1;2c\x1b]11;rgb:ffff/ffff/ffff\x07-after-resume")
        .unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b]11;?"));
        text.contains("-after-resume")
    });
    master.write_all(b"\x15/quit\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut buffer = [0; 8192];
        let _ = master.read(&mut buffer);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        process
            .0
            .try_wait()
            .unwrap()
            .is_some_and(|status| status.success())
    );
    let mut restored = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), restored.as_mut_ptr()) },
        0
    );
    let restored = unsafe { restored.assume_init() };
    // PENDIN is a kernel typeahead flag, not a persistent terminal mode.
    assert_eq!(
        original.c_lflag & !libc::PENDIN,
        restored.c_lflag & !libc::PENDIN
    );
    assert_eq!(original.c_iflag, restored.c_iflag);
    assert_eq!(original.c_oflag, restored.c_oflag);
    assert_eq!(original.c_cflag, restored.c_cflag);
    assert_eq!(original.c_cc, restored.c_cc);
}

#[test]
fn resume_picker_and_runtime_skip_malformed_theme_before_journal_update() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let sessions = root.path().join("sessions");
    let seed = Command::new(env!("CARGO_BIN_EXE_pi"))
        .current_dir(root.path())
        .env("PI_RS_HOME", &home)
        .args(["--offline", "--no-mcp", "--no-trust", "--session-dir"])
        .arg(&sessions)
        .args(["--session-id", "target", "name", "Resume Fixture"])
        .output()
        .unwrap();
    assert!(
        seed.status.success(),
        "{}",
        String::from_utf8_lossy(&seed.stderr)
    );
    let journal = sessions.join("target.jsonl");
    let original = std::fs::read(&journal).unwrap();
    let bad = root.path().join("broken.json");
    let good = root.path().join("good.json");
    std::fs::write(&bad, "not JSON").unwrap();
    let mut theme: serde_json::Value =
        serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
    theme["name"] = serde_json::json!("fixture-theme");
    std::fs::write(&good, theme.to_string()).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 || libc::ioctl(slave_fd, libc::TIOCSCTTY as libc::c_ulong, 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut process = Process(
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--offline",
                "--no-mcp",
                "--no-trust",
                "--no-extensions",
                "--tui-mode",
                "fullscreen",
                "--session-dir",
            ])
            .arg(&sessions)
            .args([
                "--resume",
                "--name",
                "Resumed Fixture",
                "--use-theme",
                "fixture-theme",
                "--theme",
            ])
            .arg(&bad)
            .arg("--theme")
            .arg(&good)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Resume session"));
    assert_eq!(std::fs::read(&journal).unwrap(), original);
    master.write_all(b"Resume Fixture").unwrap();
    output_until(&mut master, |text| text.contains("Resume Fixture"));
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/theme fixture-theme\r").unwrap();
    output_until(&mut master, |_| {
        std::fs::read(home.join("settings.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .is_some_and(|settings| settings["theme"] == "fixture-theme")
    });
    master.write_all(b"/quit\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut buf = [0; 8192];
        let _ = master.read(&mut buf);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        process
            .0
            .try_wait()
            .unwrap()
            .is_some_and(|status| status.success())
    );
    assert_eq!(
        pi_rs::Session::open(&journal).unwrap().name(),
        Some("Resumed Fixture")
    );
    assert!(std::fs::read(&journal).unwrap().starts_with(&original));
}

#[test]
fn rpc_resume_uses_controlling_terminal_without_corrupting_protocol() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let mut seed = Command::new(env!("CARGO_BIN_EXE_pi"))
        .args([
            "--workspace",
            root.path().to_str().unwrap(),
            "--session-id",
            "target",
            "--mode",
            "rpc",
            "--no-mcp",
        ])
        .env("PI_RS_HOME", &home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    seed.stdin.take().unwrap().write_all(b"{\"id\":\"name\",\"type\":\"set_session_name\",\"name\":\"Resume Fixture\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n").unwrap();
    assert!(seed.wait_with_output().unwrap().status.success());
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 30,
        ws_col: 100,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let mut slave = unsafe { File::from_raw_fd(slave_fd) };
    let mut initial_termios = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave_fd, initial_termios.as_mut_ptr()) },
        0
    );
    let initial_termios = unsafe { initial_termios.assume_init() };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 || libc::ioctl(slave_fd, libc::TIOCSCTTY as libc::c_ulong, 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut process = Process(
        command
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--resume",
                "--mode",
                "rpc",
                "--no-mcp",
            ])
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .env_remove("NO_COLOR")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b]10;?"));
        assert!(!text.contains("\x1b]11;?"));
        assert!(!text.contains("\x1b]4;"));
        assert!(!text.contains("\x1b[c"));
        text.contains("Resume session")
    });
    master.write_all(b"Resume Fixture").unwrap();
    output_until(&mut master, |text| {
        text.contains("Resume session") && text.contains("Resume Fixture")
    });
    // Queue input for the next terminal owner in the same write as selection.
    master.write_all(b"\rnext shell command\n").unwrap();
    output_until(&mut master, |text| text.contains("\x1b[?1049l"));
    // The alternate-screen exit is emitted before raw mode is restored.
    // Wait for terminal handoff before reading as the next owner.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut current = std::mem::MaybeUninit::<libc::termios>::uninit();
        assert_eq!(
            unsafe { libc::tcgetattr(slave_fd, current.as_mut_ptr()) },
            0
        );
        let current = unsafe { current.assume_init() };
        // PENDIN is a transient kernel typeahead flag on macOS and Linux.
        if current.c_lflag & !libc::PENDIN == initial_termios.c_lflag & !libc::PENDIN
            && current.c_iflag == initial_termios.c_iflag
            && current.c_oflag == initial_termios.c_oflag
            && current.c_cflag == initial_termios.c_cflag
            && current.c_cc == initial_termios.c_cc
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminal was not restored: lflag={:#x}/{:#x}, iflag={:#x}/{:#x}, oflag={:#x}/{:#x}, cflag={:#x}/{:#x}, cc={:?}/{:?}",
            current.c_lflag,
            initial_termios.c_lflag,
            current.c_iflag,
            initial_termios.c_iflag,
            current.c_oflag,
            initial_termios.c_oflag,
            current.c_cflag,
            initial_termios.c_cflag,
            current.c_cc,
            initial_termios.c_cc,
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let flags = unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let expected = b"next shell command\n";
    let mut queued = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while queued.len() < expected.len() {
        let mut buffer = [0; 128];
        match slave.read(&mut buffer) {
            Ok(0) => panic!("terminal closed before handoff input"),
            Ok(count) => queued.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => panic!("next owner's terminal input: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "incomplete handoff input: {queued:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(queued, expected);
    process
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(
            b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n",
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut buffer = [0; 8192];
        let _ = master.read(&mut buffer);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        process
            .0
            .try_wait()
            .unwrap()
            .is_some_and(|status| status.success())
    );
    let mut output = String::new();
    process
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    let events: Vec<serde_json::Value> = output
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let state = events.iter().find(|event| event["id"] == "state").unwrap();
    assert!(
        state["data"]["sessionFile"]
            .as_str()
            .unwrap()
            .ends_with("/target.jsonl")
    );
}

#[test]
fn resume_selector_escape_cancels_with_redirected_stdio() {
    let root = tempfile::tempdir().unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 30,
        ws_col: 100,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let _slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 || libc::ioctl(slave_fd, libc::TIOCSCTTY as libc::c_ulong, 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut process = Process(
        command
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--resume",
                "--print",
                "must not run",
                "--no-mcp",
            ])
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Resume session"));
    master.write_all(b"\x1b").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut terminal_output = Vec::new();
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut buffer = [0; 8192];
        if let Ok(count) = master.read(&mut buffer) {
            terminal_output.extend_from_slice(&buffer[..count]);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        process
            .0
            .try_wait()
            .unwrap()
            .is_some_and(|status| status.success())
    );
    let mut buffer = [0; 8192];
    if let Ok(count) = master.read(&mut buffer) {
        terminal_output.extend_from_slice(&buffer[..count]);
    }
    let terminal_output = String::from_utf8_lossy(&terminal_output);
    assert!(!terminal_output.contains("\x1b]10;?"));
    assert!(!terminal_output.contains("\x1b]11;?"));
    assert!(!terminal_output.contains("\x1b[c"));
    let mut output = String::new();
    process
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    assert!(output.is_empty(), "unexpected stdout: {output}");
}

#[test]
fn settings_switches_terminal_modes_and_restores_input() {
    for initial in ["regular", "fullscreen"] {
        let root = tempfile::tempdir().unwrap();
        let mut master_fd = -1;
        let mut slave_fd = -1;
        let mut size = libc::winsize {
            ws_row: 32,
            ws_col: 110,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::addr_of_mut!(size),
                )
            },
            0
        );
        let mut master = unsafe { File::from_raw_fd(master_fd) };
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let _process = Process(
            Command::new(env!("CARGO_BIN_EXE_pi"))
                .args([
                    "--workspace",
                    root.path().to_str().unwrap(),
                    "--no-trust",
                    "--no-mcp",
                    "--offline",
                    "--tui-mode",
                    initial,
                ])
                .env("PI_RS_HOME", root.path().join("home"))
                .env("TERM", "xterm-256color")
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave))
                .spawn()
                .unwrap(),
        );
        output_until(&mut master, |text| text.contains("Enter to send"));
        master.write_all(b"/settings\r").unwrap();
        output_until(&mut master, |text| text.contains("Settings"));
        master.write_all(b"TUI mode").unwrap();
        output_until(&mut master, |text| text.contains("TUI mode"));
        master.write_all(b"\r").unwrap();
        let (control, changed) = if initial == "regular" {
            ("\u{1b}[?1049h", "fullscreen")
        } else {
            ("\u{1b}[?1049l", "regular")
        };
        output_until(&mut master, |text| {
            text.contains(control) && settings_row_shows(text, "TUI mode", changed)
        });
        master.write_all(b"\r").unwrap();
        let control = if initial == "regular" {
            "\u{1b}[?1049l"
        } else {
            "\u{1b}[?1049h"
        };
        output_until(&mut master, |text| {
            text.contains(control) && settings_row_shows(text, "TUI mode", initial)
        });
        let settings: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("home/settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["tuiMode"], initial);
        master.write_all(b"\x03").unwrap();
        output_until(&mut master, |text| text.contains("0 tokens"));
        if initial == "regular" {
            for (label, key, expected) in [
                ("Quiet startup", "quietStartup", serde_json::json!(true)),
                (
                    "Collapse changelog",
                    "collapseChangelog",
                    serde_json::json!(true),
                ),
                (
                    "Session share destination",
                    "sessionShareDestination",
                    serde_json::json!("gist"),
                ),
                (
                    "Default project trust",
                    "defaultProjectTrust",
                    serde_json::json!("always"),
                ),
                (
                    "Fullscreen wheel scrolling",
                    "fullscreenWheelScrollLines",
                    serde_json::json!(1),
                ),
                (
                    "Mermaid diagrams",
                    "markdown.mermaid",
                    serde_json::json!("off"),
                ),
            ] {
                master.write_all(b"/settings\r").unwrap();
                output_until(&mut master, |text| text.contains("Settings"));
                master.write_all(label.as_bytes()).unwrap();
                output_until(&mut master, |text| text.contains(label));
                master.write_all(b"\r").unwrap();
                let displayed = if key == "defaultProjectTrust" {
                    "Always trust".into()
                } else {
                    expected
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| expected.to_string())
                };
                output_until(&mut master, |text| {
                    settings_row_shows(text, label, &displayed)
                });
                let settings: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(root.path().join("home/settings.json")).unwrap(),
                )
                .unwrap();
                let actual = key.split('.').fold(&settings, |value, part| &value[part]);
                assert_eq!(*actual, expected);
                master.write_all(b"\x03").unwrap();
                output_until(&mut master, |text| text.contains("0 tokens"));
            }
        }
        master.write_all(b"draft survives").unwrap();
        output_until(&mut master, |text| text.contains("draft survives"));
        let mut previous = None;
        for (key, notice) in [
            (b"\x0f", "Tool output: expanded"),
            (b"\x14", "Thinking blocks: hidden"),
            (b"\x0f", "Tool output: collapsed"),
            (b"\x14", "Thinking blocks: visible"),
        ] {
            master.write_all(key).unwrap();
            output_until(&mut master, |text| {
                text.contains(notice)
                    && text.contains("draft survives")
                    && previous.is_none_or(|previous| !text.contains(previous))
            });
            previous = Some(notice);
        }
        let settings: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("home/settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["hideThinkingBlock"], false);
    }
}

#[test]
fn settings_theme_preview_cancel_and_automatic_selection() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("settings.json"), br#"{"theme":"dark"}"#).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--tui-mode",
                "fullscreen",
            ])
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| {
        text.contains("Enter to send") && text.contains("\x1b]11;?\x07")
    });
    master.write_all(b"\x1b[?1;2c/settings\r").unwrap();
    output_until(&mut master, |text| text.contains("Auto-compact"));
    master.write_all(b"Theme\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Automatic · follow terminal appearance")
    });
    master.write_all(b"light").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b]11;?\x07"));
        text.contains("Theme · light")
    });
    let read_theme = || -> String {
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(home.join("settings.json")).unwrap(),
        )
        .unwrap()["theme"]
            .as_str()
            .unwrap()
            .into()
    };
    assert_eq!(read_theme(), "dark");
    master.write_all(b"\x03").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Theme", "dark") && text.contains("to change")
    });
    assert_eq!(read_theme(), "dark");
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Automatic · follow terminal appearance")
    });
    master.write_all(b"light\r").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b[?2031h"));
        text.contains("\x1b]11;?\x07")
            && settings_row_shows(text, "Theme", "light")
            && text.contains("to change")
    });
    assert_eq!(read_theme(), "light");
    master.write_all(b"\x1b[?1;2c\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Automatic · follow terminal appearance")
    });
    master.write_all(b"light\r").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("\x1b[?2031h"));
        text.contains("\x1b]11;?\x07") && settings_row_shows(text, "Theme", "light")
    });
    master.write_all(b"\x1b[?1;2c").unwrap();
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Automatic · follow terminal appearance")
    });
    master.write_all(b"\x1b[H\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Automatic theme") && text.contains("Light theme · light")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Light theme") && text.contains("system")
    });
    master.write_all(b"system\r").unwrap();
    output_until(&mut master, |text| text.contains("Light theme · system"));
    master.write_all(b"\x1b[B\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Dark theme") && text.contains("dark")
    });
    master.write_all(b"dark\r").unwrap();
    output_until(&mut master, |text| text.contains("Dark theme · dark"));
    master.write_all(b"\x1b[B\x1b[B\r").unwrap();
    output_until(&mut master, |text| {
        settings_row_shows(text, "Theme", "system/dark") && text.contains("to change")
    });
    assert_eq!(read_theme(), "system/dark");
}

#[test]
fn settings_choose_and_clear_per_model_thinking() {
    check_per_model_thinking_settings(false, false, false);
}

#[test]
fn project_per_model_thinking_preserves_live_edits_and_disk_precedence() {
    check_per_model_thinking_settings(true, false, false);
}

#[test]
fn clearing_per_model_thinking_uses_unqualified_model_fallback() {
    check_per_model_thinking_settings(false, true, false);
}

#[test]
fn delayed_per_model_thinking_callback_preserves_next_selection() {
    check_per_model_thinking_settings(false, true, true);
}

fn check_per_model_thinking_settings(project_override: bool, model_fallback: bool, delayed: bool) {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let session_path = root.path().join("session.jsonl");
    std::fs::create_dir_all(&home).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let extension = root.path().join("selection.py");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, socket, sys
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'selection', 'dependencies':[], 'commands':[], 'tools':[], 'lifecycle_hook':True, 'model_selection_hook':True, 'session_lifecycle_hook':bool(os.environ.get('PI_RS_TEST_SELECTION_GATE'))}
elif request['type'] == 'lifecycle':
    event = request['event']
    address = os.environ.get('PI_RS_TEST_SELECTION_GATE')
    if address and event.get('type') == 'session_start':
        host, port = address.rsplit(':', 1)
        with socket.create_connection((host, int(port)), timeout=15) as gate:
            gate.sendall(b'R')
            if gate.recv(1) != b'G':
                raise RuntimeError('startup gate was not released')
        print(json.dumps({'version':1, 'response':{'Ok':None}}))
        sys.exit(0)
    if address and os.environ.get('PI_RS_TEST_DELAY_SELECTION') and event.get('type') == 'thinking_level_select' and event.get('level') == 'high':
        host, port = address.rsplit(':', 1)
        with socket.create_connection((host, int(port)), timeout=10) as gate:
            gate.sendall(b'S')
            if gate.recv(1) != b'G':
                raise RuntimeError('selection gate was not released')
    with open(os.path.join(os.environ['PI_RS_HOME'], 'events.jsonl'), 'a') as log:
        log.write(json.dumps(event) + '\n')
    result = None
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let initial_settings = if model_fallback {
        serde_json::json!({"defaultThinkingLevel":"medium", "modelThinkingLevels":{"fixture-model":"low"}})
    } else {
        serde_json::json!({"defaultThinkingLevel":"low"})
    };
    std::fs::write(home.join("settings.json"), initial_settings.to_string()).unwrap();
    if project_override {
        std::fs::create_dir_all(root.path().join(".pi")).unwrap();
        std::fs::write(
            root.path().join(".pi/settings.json"),
            br#"{"modelThinkingLevels":{"ollama/fixture-model":"low"}}"#,
        )
        .unwrap();
    }
    std::fs::write(home.join("models.json"), br#"{"providers":{"ollama":{"models":[{"id":"fixture-model","reasoning":true,"thinkingLevelMap":{"xhigh":"xhigh"}},{"id":"plain-model","reasoning":false}]}}}"#).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    command.env(
        "PI_RS_TEST_SELECTION_GATE",
        listener.local_addr().unwrap().to_string(),
    );
    if delayed {
        command.env("PI_RS_TEST_DELAY_SELECTION", "1");
    } else {
        command.env_remove("PI_RS_TEST_DELAY_SELECTION");
    }
    let _process = Process(
        command
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--session",
                session_path.to_str().unwrap(),
                "--trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture-model",
                "--endpoint",
                "http://127.0.0.1:1/v1/chat/completions",
                "--tui-mode",
                "fullscreen",
            ])
            .arg("--extension")
            .arg(&extension)
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut output = Vec::new();
        let mut startup = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    let mut chunk = [0; 8192];
                    if let Ok(count) = master.read(&mut chunk) {
                        output.extend_from_slice(&chunk[..count]);
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!(
                    "extension session startup did not reach gate: {error}; terminal: {}",
                    String::from_utf8_lossy(&output)
                ),
            }
        };
        prepare_gate(&startup);
        let mut ready = [0];
        startup.read_exact(&mut ready).unwrap();
        assert_eq!(&ready, b"R");
        startup.write_all(b"G").unwrap();
    }
    master.write_all(b"/settings\r").unwrap();
    output_until(&mut master, |text| text.contains("Auto-compact"));
    master
        .write_all(b"Default thinking level per model\r")
        .unwrap();
    output_until(&mut master, |text| text.contains("Thinking: select model"));
    master.write_all(b"ollama/fixture-model\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Thinking level · ollama/fixture-model") && text.contains("high")
    });
    master.write_all(b"high\r").unwrap();
    let displayed = "high";
    if !delayed {
        output_until(&mut master, |text| {
            text.contains(&format!("ollama/fixture-model · {displayed}"))
        });
    }
    let read_settings = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap()
    };
    let wait_level = |master: &mut File, expected: &str| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(session) = pi_rs::Session::snapshot(&session_path)
                && session.model_selection().is_some_and(|(model, level)| {
                    model == "fixture-model" && level.as_deref() == Some(expected)
                })
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "thinking level was not applied to the session"
            );
            // A full PTY can block the UI before it persists the selection.
            let mut output = [0; 8192];
            let _ = master.read(&mut output);
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    wait_level(&mut master, displayed);
    assert_eq!(
        read_settings()["modelThinkingLevels"]["ollama/fixture-model"],
        "high"
    );
    let wait_notifications = |master: &mut File, count: usize| {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut terminal = Vec::new();
        loop {
            // A full PTY output buffer can prevent the UI dispatching events.
            let mut output = [0; 8192];
            if let Ok(count) = master.read(&mut output) {
                terminal.extend_from_slice(&output[..count]);
            }
            let entries: Vec<serde_json::Value> =
                std::fs::read_to_string(home.join("events.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .filter_map(|line| serde_json::from_str(line).ok())
                    .collect();
            if entries.len() >= count {
                return entries;
            }
            assert!(
                Instant::now() < deadline,
                "selection events were not delivered: {entries:?}; terminal: {}",
                String::from_utf8_lossy(&terminal)
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    if delayed {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut observed_output = Vec::new();
        let mut gate = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    let mut output = [0; 8192];
                    if let Ok(count) = master.read(&mut output) {
                        observed_output.extend_from_slice(&output[..count]);
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!(
                    "first per-model callback did not start: {error}; terminal: {}; events: {}",
                    String::from_utf8_lossy(&observed_output),
                    std::fs::read_to_string(home.join("events.jsonl")).unwrap_or_default()
                ),
            }
        };
        prepare_gate(&gate);
        let mut started = [0];
        gate.read_exact(&mut started).unwrap();
        assert_eq!(&started, b"S");
        // Queue the complete second selection while the first callback is held.
        master.write_all(b"\rClear\r").unwrap();
        assert!(!home.join("events.jsonl").exists());
        assert_eq!(
            read_settings()["modelThinkingLevels"]["ollama/fixture-model"],
            "high"
        );
        gate.write_all(b"G").unwrap();
    }
    let notifications = wait_notifications(&mut master, if delayed { 2 } else { 1 });
    assert_eq!(
        notifications[0],
        serde_json::json!({"type":"thinking_level_select", "level":"high", "previousLevel":"low"})
    );
    if !delayed {
        master.write_all(b"\x03").unwrap();
        output_until(&mut master, |text| {
            text.contains("Default thinking level per model")
                && text.contains(if model_fallback {
                    "2 configured"
                } else {
                    "1 configured"
                })
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| text.contains("Thinking: select model"));
        master.write_all(b"ollama/fixture-model").unwrap();
        let reopened = if project_override { "low" } else { "high" };
        output_until(&mut master, |text| {
            text.contains(&format!("ollama/fixture-model · {reopened}"))
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("Clear override · use default")
        });
        master.write_all(b"Clear\r").unwrap();
        output_until(&mut master, |text| text.contains("Thinking: select model"));
    }
    if model_fallback {
        assert_eq!(
            read_settings()["modelThinkingLevels"],
            serde_json::json!({"fixture-model":"low"})
        );
    } else {
        assert!(read_settings().get("modelThinkingLevels").is_none());
    }
    wait_level(&mut master, "low");
    let notifications = wait_notifications(&mut master, 2);
    assert_eq!(
        notifications[0],
        serde_json::json!({"type":"thinking_level_select", "level":"high", "previousLevel":"low"})
    );
    assert_eq!(
        notifications[1],
        serde_json::json!({"type":"thinking_level_select", "level":"low", "previousLevel":"high"})
    );
    master
        .write_all(&vec![0x7f; "ollama/fixture-model".len()])
        .unwrap();
    master.write_all(b"ollama/plain-model\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Thinking level · ollama/plain-model") && text.contains("off")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("ollama/plain-model · off")
    });
    assert_eq!(
        read_settings()["modelThinkingLevels"]["ollama/plain-model"],
        "off"
    );
    wait_level(&mut master, "low");
    master.write_all(b"\x03").unwrap();
    output_until(&mut master, |text| {
        text.contains("Default thinking level per model")
            && text.contains(if project_override || model_fallback {
                "2 configured"
            } else {
                "1 configured"
            })
    });
}

#[test]
fn thinking_model_discovery_can_be_cancelled_while_server_is_slow() {
    let root = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let (accepted, connected) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break Some(socket),
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(_) => break None,
            }
        };
        if let Some(socket) = socket {
            let _ = accepted.send(());
            let _ = gate.recv_timeout(Duration::from_secs(10));
            drop(socket);
        }
    });
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--endpoint",
                &endpoint,
                "--tui-mode",
                "fullscreen",
            ])
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/settings\r").unwrap();
    output_until(&mut master, |text| text.contains("Auto-compact"));
    master
        .write_all(b"Default thinking level per model\r")
        .unwrap();
    output_until(&mut master, |text| text.contains("Loading thinking models"));
    connected.recv_timeout(Duration::from_secs(5)).unwrap();
    let start = Instant::now();
    master.write_all(b"\x03").unwrap();
    output_until(&mut master, |text| {
        text.contains("Default thinking level per model") && text.contains("to change")
    });
    assert!(start.elapsed() < Duration::from_secs(3));
    release.send(()).unwrap();
    server.join().unwrap();
}

#[test]
fn save_default_model_persists_provider_and_extends_scope() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("settings.json"), br#"{"defaultProvider":"anthropic","defaultModel":"old-model","enabledModels":["ollama/first"],"modelThinkingLevels":{"ollama/first":"high"}}"#).unwrap();
    std::fs::write(home.join("models.json"), br#"{"providers":{"ollama":{"baseUrl":"http://127.0.0.1:1/v1","models":[{"id":"first","name":"First Fixture","reasoning":true,"thinkingLevelMap":{"high":"high"}},{"id":"second","reasoning":false}]}}}"#).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .args([
                "--workspace",
                root.path().to_str().unwrap(),
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--provider",
                "ollama",
                "--model",
                "first",
                "--endpoint",
                "http://127.0.0.1:1/v1/chat/completions",
                "--tui-mode",
                "fullscreen",
            ])
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/settings\r").unwrap();
    output_until(&mut master, |text| text.contains("Auto-compact"));
    master.write_all(b"Save default model\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Save default model") && text.contains("to change")
    });
    let read_settings = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap()).unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while read_settings()["defaultProvider"] != "ollama" {
        assert!(Instant::now() < deadline, "default model was not saved");
        let mut buffer = [0; 8192];
        let _ = master.read(&mut buffer);
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(read_settings()["defaultProvider"], "ollama");
    assert_eq!(read_settings()["defaultModel"], "first");
    master.write_all(b"\x03").unwrap();
    output_until(&mut master, |text| text.contains("0 tokens"));
    master.write_all(b"/model\r").unwrap();
    output_until(&mut master, |text| text.contains("Select model"));
    master.write_all(b"ollama/second").unwrap();
    output_until(&mut master, |text| text.contains("ollama/second"));
    master.write_all(b"\x13").unwrap();
    output_until(&mut master, |text| {
        text.contains("Default model: ollama/second")
    });
    let saved = read_settings();
    assert_eq!(saved["defaultProvider"], "ollama");
    assert_eq!(saved["defaultModel"], "second");
    assert_eq!(
        saved["enabledModels"],
        serde_json::json!(["ollama/first", "ollama/second"])
    );
    master.write_all(b"\x10").unwrap();
    output_until(&mut master, |text| {
        text.contains("Switched to First Fixture (thinking: high)")
    });
    master.write_all(b"\x1b[Z").unwrap();
    output_until(&mut master, |text| {
        text.contains("Thinking level: off") && !text.contains("Switched to First Fixture")
    });
    master.write_all(b"\x10").unwrap();
    output_until(&mut master, |text| {
        text.contains("Switched to second") && !text.contains("Thinking level: off")
    });
    let mut restart = Command::new(env!("CARGO_BIN_EXE_pi"))
        .env("PI_RS_HOME", &home)
        .args([
            "--workspace",
            root.path().to_str().unwrap(),
            "--no-trust",
            "--no-session",
            "--no-mcp",
            "--offline",
            "--mode",
            "rpc",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    restart
        .stdin
        .take()
        .unwrap()
        .write_all(
            b"{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"stop\",\"type\":\"shutdown\"}\n",
        )
        .unwrap();
    let output = restart.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let state = events.iter().find(|event| event["id"] == "state").unwrap();
    assert_eq!(state["data"]["model"]["provider"], "ollama");
    assert_eq!(state["data"]["model"]["id"], "second");
}

#[test]
fn exit_during_shell_restores_terminal_and_drains_late_key_release() {
    let root = tempfile::tempdir().unwrap();
    let (mut master_fd, mut slave_fd) = (-1, -1);
    let mut size = libc::winsize {
        ws_row: 30,
        ws_col: 100,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), original.as_mut_ptr()) },
        0
    );
    let original = unsafe { original.assume_init() };
    #[cfg(debug_assertions)]
    let barrier_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    #[cfg(debug_assertions)]
    barrier_listener.set_nonblocking(true).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    #[cfg(debug_assertions)]
    command.env(
        "PI_RS_TEST_INPUT_DRAIN_BARRIER",
        barrier_listener.local_addr().unwrap().to_string(),
    );
    let mut process = Process(
        command
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--offline",
                "--model",
                "fixture",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master
        .write_all(b"!echo shell-$(printf running); sleep 10\r")
        .unwrap();
    output_until(&mut master, |text| text.contains("shell-running"));
    master.write_all(b"\x03\x03").unwrap();
    output_until(&mut master, |text| text.contains("\x1b[<u"));
    #[cfg(debug_assertions)]
    let mut barrier = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match barrier_listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::yield_now();
                }
                Err(error) => panic!("terminal drain barrier: {error}"),
            }
        }
    };
    #[cfg(debug_assertions)]
    {
        prepare_gate(&barrier);
        let mut ready = [0];
        barrier.read_exact(&mut ready).unwrap();
        assert_eq!(&ready, b"D");
    }
    master.write_all(b"\x1b[99;5:3u").unwrap();
    #[cfg(debug_assertions)]
    barrier.write_all(b"C").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut state = std::mem::MaybeUninit::<libc::termios>::uninit();
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), state.as_mut_ptr()) },
            0
        );
        if unsafe { state.assume_init() }.c_lflag & libc::ICANON == original.c_lflag & libc::ICANON
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminal remained in raw mode during worker shutdown"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let flags = unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    // Canonical input is released only after a newline, so complete the line
    // to reveal any escape bytes that the shutdown left for the parent shell.
    master.write_all(b"\n").unwrap();
    let mut remaining = [0; 64];
    let mut slave = slave;
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match slave.read(&mut remaining) {
            Ok(count) if count > 0 => {
                assert_eq!(
                    &remaining[..count],
                    b"\n",
                    "late key release leaked to terminal input"
                );
                break;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            other => panic!("terminal input read failed: {other:?}"),
        }
        assert!(
            Instant::now() < deadline,
            "canonical newline was not delivered"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "interactive process failed: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "worker shutdown did not finish");
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn reload_preserves_broken_settings_in_restarted_worker() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let skill = root.path().join("special/SKILL.md");
    let previous = root.path().join("previous/SKILL.md");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
    std::fs::create_dir_all(previous.parent().unwrap()).unwrap();
    std::fs::write(
        &previous,
        "---\nname: previous-fixture-skill\ndescription: previous-skill-description\n---\nbody",
    )
    .unwrap();
    std::fs::write(
        &skill,
        "---\nname: special-fixture-skill\ndescription: unique-reload-skill-description\n---\nbody",
    )
    .unwrap();
    let settings = home.join("settings.json");
    std::fs::write(
        &settings,
        serde_json::json!({"skills":[previous]}).to_string(),
    )
    .unwrap();
    std::fs::write(
        home.join("models.json"),
        br#"{"providers":{"ollama":{"models":[{"id":"test-fixture"}]}}}"#,
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (sender, receiver) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut requests = 0;
        while Instant::now() < deadline {
            let (mut socket, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("{error}"),
            };
            prepare_gate(&socket);
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut request = String::new();
            if reader.read_line(&mut request).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            if request.starts_with("GET ") {
                let body = br#"{"data":[{"id":"test-fixture"}]}"#;
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                socket.write_all(body).unwrap();
                continue;
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            sender
                .send(String::from_utf8_lossy(&body).into_owned())
                .unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n").unwrap();
            requests += 1;
            if requests == 2 {
                return;
            }
        }
    });
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--offline",
                "--coding-tools",
                "--provider",
                "ollama",
                "--model",
                "test-fixture",
                "--endpoint",
                &endpoint,
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    fn wait_terminal(
        master: &mut File,
        label: &str,
        initial: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut output = initial.as_bytes().to_vec();
        let mut buffer = [0; 8192];
        loop {
            match master.read(&mut buffer) {
                Ok(count) => output.extend_from_slice(&buffer[..count]),
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
                Err(error) => panic!("{label}: terminal read failed: {error}"),
            }
            let text = String::from_utf8_lossy(&output);
            if predicate(&text) {
                return text.into_owned();
            }
            assert!(
                Instant::now() < deadline,
                "{label}: timed out; terminal: {text}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn wait_request(
        master: &mut File,
        receiver: &std::sync::mpsc::Receiver<String>,
        label: &str,
    ) -> (String, String) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut output = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            if let Ok(body) = receiver.try_recv() {
                return (body, String::from_utf8_lossy(&output).into_owned());
            }
            match master.read(&mut buffer) {
                Ok(count) => output.extend_from_slice(&buffer[..count]),
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
                Err(error) => panic!("{label}: terminal read failed: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "{label}: timed out; terminal: {}",
                String::from_utf8_lossy(&output)
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    wait_terminal(&mut master, "startup", "", |text| {
        text.contains("Enter to send")
    });
    std::fs::write(&settings, serde_json::json!({"skills":[skill]}).to_string()).unwrap();
    master.write_all(b"first request\r").unwrap();
    let (first, observed) = wait_request(&mut master, &receiver, "first request");
    assert!(first.contains("unique-reload-skill-description"), "{first}");
    let idle = |text: &str| {
        let frame = text.rsplit("\x1b[?2026h").next().unwrap_or_default();
        frame.contains("done") && frame.contains("\x1b[?2026l") && !frame.contains("Escで取消")
    };
    if !idle(&observed) {
        wait_terminal(&mut master, "first response idle", &observed, idle);
    }
    std::fs::write(&settings, "{").unwrap();
    master.write_all(b"/reload\r").unwrap();
    loop {
        let output = wait_terminal(&mut master, "reload", "", |text| {
            text.contains("Reloaded settings")
                || text.contains("Wait for the shell command to finish")
        });
        if output.contains("Reloaded settings") {
            break;
        }
        master.write_all(b"\r").unwrap();
    }
    master.write_all(b"hello\r").unwrap();
    let (body, _) = wait_request(&mut master, &receiver, "second request");
    assert!(body.contains("unique-reload-skill-description"), "{body}");
    server.join().unwrap();
}

#[test]
fn reload_waits_for_auth_lock_without_closing_terminal() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    let lock = home.join("auth.json.lock");
    std::fs::create_dir(&lock).unwrap();
    master.write_all(b"/reload\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Reloading provider credentials")
    });
    master.write_all(b"reload-still-responsive").unwrap();
    output_until(&mut master, |text| text.contains("reload-still-responsive"));
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| {
        text.contains("Provider credential reload cancelled")
    });
    assert!(process.0.try_wait().unwrap().is_none());
    std::fs::remove_dir(lock).unwrap();
    master.write_all(b"\x15/reload\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Provider credentials reloaded")
    });
    assert!(process.0.try_wait().unwrap().is_none());
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGINT) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        process
            .0
            .try_wait()
            .unwrap()
            .and_then(|status| status.code()),
        Some(130)
    );
}

#[test]
fn sigint_cancels_interactive_startup_model_discovery() {
    use std::net::TcpListener;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut bytes = [0; 4096];
        assert!(socket.read(&mut bytes).unwrap() > 0);
        started_tx.send(()).unwrap();
        let _ = release_rx.recv();
    });
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let _master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .env_remove("HTTP_PROXY")
            .env_remove("http_proxy")
            .env_remove("ALL_PROXY")
            .env_remove("all_proxy")
            .args([
                "--no-trust",
                "--no-mcp",
                "--provider",
                "ollama",
                "--endpoint",
                &endpoint,
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    started_rx.recv_timeout(Duration::from_secs(15)).unwrap();
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGINT) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = process.0.try_wait().unwrap();
    let _ = release_tx.send(());
    server.join().unwrap();
    assert_eq!(status.and_then(|status| status.code()), Some(130));
}

#[test]
fn external_sigint_interrupts_stalled_interactive_transition() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let extension = root.path().join("slow-transition.py");
    let marker = home.join("transition-started");
    std::fs::write(&extension, r##"#!/usr/bin/env python3
import json, os, sys, time
request = json.load(sys.stdin)
if request['type'] == 'describe':
    result = {'id':'slow-transition', 'dependencies':[], 'commands':[], 'tools':[], 'session_transition_hook':True}
elif request['type'] == 'before_session_transition':
    with open(os.path.join(os.environ['PI_RS_HOME'], 'transition-started'), 'w') as f: f.write(str(os.getpid()))
    time.sleep(30)
    result = False
else:
    result = None
print(json.dumps({'version':1, 'response':{'Ok':result}}))
"##).unwrap();
    std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--tui-mode",
                "fullscreen",
                "--extension",
            ])
            .arg(extension)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/new\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !marker.exists() && process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        // Keep the PTY flowing while the worker starts; a full output buffer
        // can otherwise prevent the UI from dispatching the command.
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(marker.exists(), "transition hook did not start");
    let extension_pid: i32 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGINT) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(4);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        process
            .0
            .try_wait()
            .unwrap()
            .and_then(|status| status.code()),
        Some(130)
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while unsafe { libc::kill(extension_pid, 0) } == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_ne!(
        unsafe { libc::kill(extension_pid, 0) },
        0,
        "transition extension survived shutdown"
    );
}

#[test]
fn sigint_restores_terminal_before_waiting_for_refresh_barrier() {
    use std::net::TcpListener;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let auth = home.join("auth.json");
    std::fs::write(&auth, serde_json::json!({"kimi-coding":{"type":"oauth","access":"old","refresh":"old-refresh","expires":0}}).to_string()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let host = format!("http://{}", listener.local_addr().unwrap());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut data = [0u8; 4096];
        assert!(socket.read(&mut data).unwrap() > 0);
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        let body = r#"{"access_token":"new","refresh_token":"rotated","expires_in":3600}"#;
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), original.as_mut_ptr()) },
        0
    );
    let original = unsafe { original.assume_init() };
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .env("KIMI_CODE_OAUTH_HOST", host)
            .env_remove("KIMI_API_KEY")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--provider",
                "kimi-coding",
                "--model",
                "kimi-for-coding",
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"hello\r").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if started_rx.try_recv().is_ok() {
            break;
        }
        let mut output = [0; 8192];
        let _ = master.read(&mut output);
        assert!(Instant::now() < deadline, "refresh request did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGINT) },
        0
    );
    let signalled = Instant::now();
    let deadline = signalled + Duration::from_secs(2);
    let mut output = Vec::new();
    let mut restored = false;
    while Instant::now() < deadline {
        let mut buffer = [0; 8192];
        if let Ok(n) = master.read(&mut buffer) {
            output.extend_from_slice(&buffer[..n]);
        }
        let mut current = std::mem::MaybeUninit::<libc::termios>::uninit();
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), current.as_mut_ptr()) },
            0
        );
        let current = unsafe { current.assume_init() };
        if current.c_lflag & !libc::PENDIN == original.c_lflag & !libc::PENDIN
            && current.c_iflag == original.c_iflag
            && current.c_oflag == original.c_oflag
            && current.c_cflag == original.c_cflag
            && current.c_cc == original.c_cc
            && output
                .windows(b"\x1b[?1049l".len())
                .any(|chunk| chunk == b"\x1b[?1049l")
        {
            restored = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    while signalled.elapsed() < Duration::from_millis(2500)
        && process.0.try_wait().unwrap().is_none()
    {
        let mut data = [0; 8192];
        let _ = master.read(&mut data);
        std::thread::sleep(Duration::from_millis(20));
    }
    let still_waiting = process.0.try_wait().unwrap().is_none();
    release_tx.send(()).unwrap();
    server.join().unwrap();
    let deadline = Instant::now() + Duration::from_secs(6);
    while process.0.try_wait().unwrap().is_none() && Instant::now() < deadline {
        let mut data = [0; 8192];
        let _ = master.read(&mut data);
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = process.0.try_wait().unwrap();
    assert!(
        restored,
        "terminal was not restored before the refresh completed"
    );
    assert!(still_waiting, "process did not wait for the refresh");
    assert_eq!(status.and_then(|status| status.code()), Some(130));
    let stored: serde_json::Value = serde_json::from_slice(&std::fs::read(auth).unwrap()).unwrap();
    assert_eq!(stored["kimi-coding"]["refresh"], "rotated");
}

#[test]
fn bedrock_login_selects_profile_chain_and_logout() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/login amazon-bedrock\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Existing AWS credential chain")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| text.contains("amazon-bedrock API key"));
    master.write_all(b"fixture-bearer\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("API key saved for amazon-bedrock")
    });
    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("auth.json")).unwrap()).unwrap();
    assert_eq!(auth["amazon-bedrock"]["key"], "fixture-bearer");
    master.write_all(b"/login amazon-bedrock\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Existing AWS credential chain")
    });
    master.write_all(b"\x1b[B\r").unwrap();
    output_until(&mut master, |text| text.contains("Enter AWS profile name"));
    master.write_all(b"my-profile\r").unwrap();
    output_until(&mut master, |text| text.contains("AWS profile saved"));
    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("auth.json")).unwrap()).unwrap();
    assert_eq!(auth["amazon-bedrock"]["env"]["AWS_PROFILE"], "my-profile");
    master.write_all(b"/login amazon-bedrock\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Existing AWS credential chain")
    });
    master.write_all(b"\x1b[B\x1b[B\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Configure AWS credentials")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Using existing AWS credential chain")
    });
    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("auth.json")).unwrap()).unwrap();
    assert_eq!(
        auth["amazon-bedrock"],
        serde_json::json!({"type":"api_key"})
    );
    master.write_all(b"/logout amazon-bedrock\r").unwrap();
    output_until(&mut master, |text| text.contains("Removed saved API key"));
    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("auth.json")).unwrap()).unwrap();
    assert!(auth.get("amazon-bedrock").is_none());
}

#[test]
fn tree_navigation_during_streaming_restores_the_queue_before_checkout() {
    for mode in ["regular", "fullscreen"] {
        check_streaming_tree_navigation(mode);
    }
}

fn check_streaming_tree_navigation(mode: &str) {
    let root = tempfile::tempdir().unwrap();
    let session_path = root.path().join("session.jsonl");
    let mut session = pi_rs::Session::open(&session_path).unwrap();
    session
        .append_user_with_images("historical anchor".into(), vec![], None)
        .unwrap();
    let anchor = session.head();
    drop(session);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    listener.set_nonblocking(true).unwrap();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            let (mut socket, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if matches!(
                        released.try_recv(),
                        Err(std::sync::mpsc::TryRecvError::Disconnected)
                    ) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("fixture accept failed: {error}"),
            };
            prepare_gate(&socket);
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut method = String::new();
            reader.read_line(&mut method).unwrap();
            let mut size = 0;
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap() == 0 || header == "\r\n" {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    size = value.trim().parse().unwrap();
                }
            }
            if method.starts_with("GET ") {
                let body = r#"{"data":[{"id":"test-fixture"}]}"#;
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                continue;
            }
            let mut body = vec![0; size];
            reader.read_exact(&mut body).unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"live-stream-marker\"}}]}\n\n").unwrap();
            let _ = released.recv_timeout(Duration::from_secs(45));
            return;
        }
    });

    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", root.path().join("home"))
            .env("TERM", "xterm-256color")
            .args([
                "--session",
                session_path.to_str().unwrap(),
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-skills",
                "--no-extensions",
                "--model",
                "test-fixture",
                "--endpoint",
                &endpoint,
                "--tui-mode",
                mode,
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );

    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"active prompt\r").unwrap();
    output_until(&mut master, |text| text.contains("live-stream-marker"));
    master.write_all(b"retained draft\r").unwrap();
    // The terminal can redraw while the worker saves the input. Keep
    // draining the PTY so a full output buffer cannot stall that redraw.
    output_until(&mut master, |_| {
        pi_rs::Session::snapshot(&session_path).is_ok_and(|session| {
            session
                .queued_inputs()
                .iter()
                .any(|message| message.input.content == "retained draft")
        })
    });

    master.write_all(b"/tree\r").unwrap();
    output_until(&mut master, |text| text.contains("Session tree"));
    master.write_all(b"historical anchorL").unwrap();
    output_until(&mut master, |text| text.contains("Label record"));
    master.write_all(b"stream-bookmark\r").unwrap();
    output_until(&mut master, |text| text.contains("[stream-bookmark]"));
    let labeled = pi_rs::Session::snapshot(&session_path).unwrap();
    assert_eq!(
        labeled.labels().get(&anchor).map(String::as_str),
        Some("stream-bookmark")
    );
    assert!(
        !labeled.queued_inputs().is_empty(),
        "editing a label keeps queued input"
    );

    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| text.contains("Summarize branch?"));
    let session = pi_rs::Session::snapshot(&session_path).unwrap();
    assert!(session.branch().iter().any(|record| matches!(&record.event,
        pi_rs::Event::User { content, .. } if content == "active prompt"
    )));
    assert!(
        !session.queued_inputs().is_empty(),
        "opening the selector keeps queued input"
    );

    master.write_all(b"\r").unwrap();
    output_until_with_timeout(&mut master, Duration::from_secs(20), |text| {
        text.contains("Navigated to selected point") && text.contains("retained draft")
    });
    let session = pi_rs::Session::snapshot(&session_path).unwrap();
    assert!(session.queued_inputs().is_empty());
    assert!(
        !session.branch().iter().any(|record| matches!(&record.event,
            pi_rs::Event::User { content, .. } if content == "active prompt"
        ))
    );
    drop(release);
    server.join().unwrap();
}

#[test]
fn vertex_login_saves_service_account_configuration() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let credentials_file = root.path().join("service-account.json");
    std::fs::write(&credentials_file, "{}").unwrap();
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .args([
                "--no-trust",
                "--no-mcp",
                "--offline",
                "--model",
                "fixture",
                "--tui-mode",
                "fullscreen",
            ])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/login google-vertex\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Service account credentials file")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| text.contains("google-vertex API key"));
    master.write_all(b"fixture-key\r").unwrap();
    output_until(&mut master, |text| {
        assert!(!text.contains("fixture-key"));
        text.contains("API key saved for google-vertex")
    });
    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("auth.json")).unwrap()).unwrap();
    assert_eq!(auth["google-vertex"]["key"], "fixture-key");
    master.write_all(b"/login google-vertex\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Service account credentials file")
    });
    master.write_all(b"\x1b[B\x1b[B\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Enter Google Cloud project ID")
    });
    master.write_all(b"project-id\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Enter Google Cloud location")
    });
    master.write_all(b"us-central1\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Enter service account credentials file path")
    });
    master.write_all(b"service-account.json\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Google Vertex AI credentials saved")
    });

    let auth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("auth.json")).unwrap()).unwrap();
    let entry = &auth["google-vertex"];
    let saved_file = std::path::Path::new(
        entry["env"]["GOOGLE_APPLICATION_CREDENTIALS"]
            .as_str()
            .unwrap(),
    );
    assert!(saved_file.is_absolute());
    assert_eq!(
        saved_file.canonicalize().unwrap(),
        credentials_file.canonicalize().unwrap()
    );
    assert_eq!(
        *entry,
        serde_json::json!({
            "type": "api_key",
            "env": {
                "GOOGLE_CLOUD_PROJECT": "project-id",
                "GOOGLE_CLOUD_LOCATION": "us-central1",
                "GOOGLE_APPLICATION_CREDENTIALS": saved_file,
            },
        })
    );
}

#[test]
fn external_editor_ctrl_c_does_not_cancel_active_backend_shell() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let editor = root.path().join("editor");
    std::fs::write(
        &editor,
        "#!/bin/sh\ntrap 'printf done > \"$PI_RS_HOME/editor-done\"; exit 130' INT\nprintf 'EDITOR_READY\\n'\nwhile :; do sleep 1; done\n",
    )
    .unwrap();
    std::fs::set_permissions(&editor, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (mut master_fd, mut slave_fd) = (-1, -1);
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    // Declare the process guard first so the PTY master closes before the
    // guard waits, including on assertion failure.
    let mut process: Process;
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    command
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .env("EDITOR", &editor)
        .env("TERM", "xterm-256color")
        .args([
            "--no-trust",
            "--no-mcp",
            "--no-extensions",
            "--offline",
            "--model",
            "fixture",
        ])
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as libc::c_ulong, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    process = Process(command.spawn().unwrap());
    output_until(&mut master, |text| text.contains("Enter to send"));
    master
        .write_all(b"!echo WORKER_ACTIVE; while [ ! -f allow-worker ]; do sleep .1; done; echo survived > worker-marker\r")
        .unwrap();
    output_until(&mut master, |text| text.contains("WORKER_ACTIVE"));
    master.write_all(&[7]).unwrap();
    output_until(&mut master, |text| text.contains("EDITOR_READY"));
    master.write_all(&[3]).unwrap();
    let editor_done = root.path().join("home/editor-done");
    let deadline = Instant::now() + Duration::from_secs(6);
    while !editor_done.exists() && Instant::now() < deadline {
        let mut bytes = [0; 8192];
        let _ = master.read(&mut bytes);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        editor_done.exists(),
        "editor did not observe terminal Ctrl+C"
    );
    std::thread::sleep(Duration::from_millis(200));
    std::fs::write(root.path().join("allow-worker"), "go").unwrap();
    let deadline = Instant::now() + Duration::from_secs(6);
    while !root.path().join("worker-marker").exists() && Instant::now() < deadline {
        let mut bytes = [0; 8192];
        let _ = master.read(&mut bytes);
        assert!(process.0.try_wait().unwrap().is_none(), "frontend exited");
        std::thread::sleep(Duration::from_millis(20));
    }
    let survived = root.path().join("worker-marker").exists();
    // Closing the PTY before waiting prevents terminal teardown from blocking
    // the test's process guard on platforms that drain an open master.
    drop(master);
    let _ = process.0.kill();
    let _ = process.0.wait();
    assert!(survived, "editor Ctrl+C cancelled active worker operation");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn extension_editor_timeout_disconnects_reparented_terminal_reader() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let extension = root.path().join("editor-extension.py");
    let editor = root.path().join("editor.py");
    std::fs::write(
        &extension,
        r#"#!/usr/bin/env python3
import json, pathlib, sys
if '--pi-rs-extension-v1' in sys.argv:
    request = json.load(sys.stdin)
    result = {
        'id':'editor-fixture','dependencies':[],'commands':[],'host_calls':True,
        'tools':[{'spec':{'name':'ask_editor','description':'Open an editor',
                          'parameters':{'type':'object'}},
                  'execution':'sequential','transactional':True}]
    } if request['type'] == 'describe' else None
    if request['type'] == 'before_request':
        result = request['request']
    elif request['type'] == 'before_tool':
        result = {'allow':True,'reason':None}
    print(json.dumps({'version':1,'response':{'Ok':result}}))
else:
    frame = json.loads(sys.stdin.readline())
    assert frame['request']['type'] == 'invoke'
    print(json.dumps({'type':'ui','request':{
        'method':'editor','title':'EDIT EXTENSION DRAFT','prefill':'original','timeout':6000
    }}), flush=True)
    response = json.loads(sys.stdin.readline())
    pathlib.Path('ui-reply').write_text(json.dumps(response))
    result = {'outcome':{'status':'succeeded','detail':'finished'}}
    print(json.dumps({'type':'complete','response':{'Ok':result}}), flush=True)
"#,
    )
    .unwrap();
    std::fs::write(
        &editor,
        r#"#!/usr/bin/env python3
import errno, os, pathlib, signal, sys
try:
    os.fstat(77)
except OSError as error:
    assert error.errno == errno.EBADF
else:
    raise RuntimeError('editor inherited an extra real terminal descriptor')
child = os.fork()
if child:
    os.waitpid(child, 0)
    sys.exit(0)
signal.signal(signal.SIGHUP, signal.SIG_IGN)
signal.alarm(20)
# Escape the editor's session, but keep an inherited tty descriptor.
os.setsid()
assert os.isatty(0)
size = os.get_terminal_size()
print(f'EDITOR READY {size.columns}x{size.lines}', flush=True)
assert sys.stdin.readline() == 'editor-input\n'
size = os.get_terminal_size()
print(f'EDITOR WAITING {size.columns}x{size.lines}', flush=True)
try:
    line = sys.stdin.readline()
except OSError:
    line = ''
pathlib.Path('editor-end').write_text(line if line else 'disconnected')
"#,
    )
    .unwrap();
    for path in [&extension, &editor] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut sent_call = false;
        while Instant::now() < deadline {
            let (mut socket, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("fixture model accept failed: {error}"),
            };
            prepare_gate(&socket);
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
                    length = value.trim().parse().unwrap();
                }
            }
            if !is_post {
                socket
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                continue;
            }
            reader.read_exact(&mut vec![0; length]).unwrap();
            let events = if sent_call {
                concat!(
                    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Fixture finished\"},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n",
                )
            } else {
                concat!(
                    "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"editor-call\",\"type\":\"function\",\"function\":{\"name\":\"ask_editor\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
                    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                    "data: [DONE]\n\n",
                )
            };
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
            if sent_call {
                return;
            }
            sent_call = true;
        }
        panic!("model fixture timed out");
    });

    let (mut master_fd, mut slave_fd) = (-1, -1);
    let mut size = libc::winsize {
        ws_row: 32,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let mut baseline = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::tcgetattr(slave_fd, &mut baseline) }, 0);
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) },
        0
    );
    for fd in [master_fd, slave_fd] {
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) },
            0
        );
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_pi"));
    command
        .current_dir(root.path())
        .env("PI_RS_HOME", root.path().join("home"))
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env("VISUAL", &editor)
        .env("TERM", "xterm-256color")
        .args([
            "--no-session",
            "--no-trust",
            "--no-mcp",
            "--offline",
            "--model",
            "fixture",
            "--endpoint",
            &endpoint,
            "--extension",
        ])
        .arg(extension)
        .stdin(slave.try_clone().unwrap())
        .stdout(slave.try_clone().unwrap())
        .stderr(slave.try_clone().unwrap());
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0
                || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0
                || libc::dup2(0, 77) < 0
            {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut process = BoundedProcess {
        child: command.spawn().unwrap(),
        master: Some(master),
    };
    let master = process.master.as_mut().unwrap();
    output_until(master, |text| text.contains("Enter to send"));
    master.write_all(b"exercise editor fixture\r").unwrap();
    output_until(master, |text| text.contains("EDIT EXTENSION DRAFT"));
    master.write_all(&[7]).unwrap();
    output_until(master, |text| text.contains("EDITOR READY 110x32"));
    size.ws_row = 34;
    size.ws_col = 112;
    assert_eq!(
        unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &size) },
        0
    );
    // The relay copies window size before forwarding each input buffer.
    master.write_all(b"editor-input\r").unwrap();
    output_until(master, |text| text.contains("EDITOR WAITING 112x34"));

    output_until(master, |text| text.contains("Fixture finished"));
    server.join().unwrap();
    let reply: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("ui-reply")).unwrap()).unwrap();
    assert_eq!(reply, serde_json::json!({"Ok":null}));
    master.write_all(b"frontend-kept-input").unwrap();
    output_until(master, |text| text.contains("frontend-kept-input"));

    let marker = root.path().join("editor-end");
    let deadline = Instant::now() + Duration::from_secs(3);
    while !marker.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "disconnected");
    // A normal frontend shutdown must still restore the pre-editor tty modes.
    assert_eq!(
        unsafe { libc::kill(process.child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut bytes = [0; 8192];
        let _ = master.read(&mut bytes);
        if process.child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "frontend did not shut down");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Darwin revokes the old slave descriptor when its session leader exits.
    let mut restored = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::tcgetattr(master.as_raw_fd(), &mut restored) },
        0
    );
    assert_eq!(restored.c_iflag, baseline.c_iflag);
    assert_eq!(restored.c_oflag, baseline.c_oflag);
    assert_eq!(restored.c_lflag, baseline.c_lflag);
    assert_eq!(restored.c_cc, baseline.c_cc);
}

#[test]
fn mcp_inspector_lists_servers_and_announced_tool_exposure() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let project = root.path().join(".pi");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&project).unwrap();
    let server = root.path().join("mcp-fixture.py");
    let tool_name = root.path().join("tool-name");
    std::fs::write(&tool_name, "fixture_tool").unwrap();
    std::fs::write(
        &server,
        r#"
import json, pathlib, sys
name = pathlib.Path(sys.argv[1]).read_text()
if name == "fail":
    sys.exit(1)
for line in sys.stdin:
    request = json.loads(line)
    if request["method"] == "initialize":
        result = {
            "protocolVersion": request["params"]["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "fixture", "version": "1"},
        }
    elif request["method"] == "tools/list":
        result = {"tools": [{"name": name, "inputSchema": {"type": "object"}}]}
    else:
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
"#,
    )
    .unwrap();
    std::fs::write(
        home.join("mcp.json"),
        r#"{"mcpServers":{"retired":{"enabled":false,"command":"unused"}}}"#,
    )
    .unwrap();
    std::fs::write(
        project.join("mcp.json"),
        serde_json::json!({"mcpServers":{"ready":{"command":"python3","args":[server, tool_name],"description":"Fixture server description","toolExposure":{"fixture_tool":"hidden"}}}}).to_string(),
    )
    .unwrap();

    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
        ws_row: 25,
        ws_col: 110,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(size),
            )
        },
        0
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_pi"))
            .current_dir(root.path())
            .args(["--trust", "--offline", "--tui-mode", "fullscreen"])
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave))
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Enter to send"));
    master.write_all(b"/mcp\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("MCP servers")
            && text.contains("ready · connected · 1 tool · codemode · project")
            && text.contains("retired · disabled · 0 tools · codemode · global")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("MCP server · ready") && text.contains("Fixture server description")
    });
    master.write_all(b"t").unwrap();
    output_until(&mut master, |text| {
        text.contains("MCP tools · ready") && text.contains("fixture_tool · hidden")
    });
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| text.contains("MCP server · ready"));

    std::fs::write(&tool_name, "replacement_tool").unwrap();
    master.write_all(b"r").unwrap();
    output_until(&mut master, |text| text.contains("Reconnected"));
    master.write_all(b"t").unwrap();
    output_until(&mut master, |text| {
        text.contains("replacement_tool · codemode") && !text.contains("fixture_tool · hidden")
    });
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| text.contains("MCP server · ready"));

    std::fs::write(&tool_name, "fail").unwrap();
    master.write_all(b"r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Status · disconnected") && text.contains("Tools · 0")
    });
    std::fs::write(&tool_name, "fixture_tool").unwrap();
    master.write_all(b"r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Reconnected") && text.contains("Tools · 1")
    });

    master.write_all(b"x").unwrap();
    output_until(&mut master, |text| {
        text.contains("Exposure of ready") && text.contains("codemode ✓")
    });
    master.write_all(b"\x1b[B\x1b[B\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("Exposure updated") && text.contains("Exposure · direct")
    });
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(project.join("mcp.json")).unwrap()).unwrap();
    assert_eq!(saved["mcpServers"]["ready"]["exposure"], "direct");
    assert_eq!(
        saved["mcpServers"]["ready"]["toolExposure"]["fixture_tool"],
        "hidden"
    );
    master.write_all(b"t").unwrap();
    output_until(&mut master, |text| text.contains("fixture_tool · hidden"));
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| text.contains("MCP server · ready"));

    for enabled in [false, true] {
        master.write_all(b"e").unwrap();
        output_until(&mut master, |text| {
            if enabled {
                text.contains("Enabled") && text.contains("Tools · 1")
            } else {
                text.contains("Disabled") && text.contains("Tools · 0")
            }
        });
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(project.join("mcp.json")).unwrap()).unwrap();
        assert_eq!(saved["mcpServers"]["ready"]["enabled"], enabled);
        assert_eq!(
            saved["mcpServers"]["ready"]["toolExposure"]["fixture_tool"],
            "hidden"
        );
    }

    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| text.contains("MCP servers"));
    master.write_all(b"\x1b").unwrap();
    output_until(&mut master, |text| text.contains("Enter to send"));

    // The inspector reports the applied configuration while the running
    // connection still uses it, even after mcp.json changes on disk.
    std::fs::write(project.join("mcp.json"), r#"{"mcpServers":{}}"#).unwrap();
    master.write_all(b"/mcp\r").unwrap();
    output_until(&mut master, |text| {
        text.contains("ready · connected · 1 tool")
    });
    master.write_all(b"\r").unwrap();
    output_until(&mut master, |text| text.contains("MCP server · ready"));
    master.write_all(b"t").unwrap();
    output_until(&mut master, |text| text.contains("fixture_tool · hidden"));
}

#[test]
fn mcp_manager_handles_loopback_oauth_sign_in_and_sign_out() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mcp_manager_oauth.py");
    let output = Command::new("python3")
        .arg(fixture)
        .arg(env!("CARGO_BIN_EXE_pi"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
