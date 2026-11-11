#![cfg(unix)]

use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn output_until(master: &mut File, predicate: impl Fn(&str) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
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
        let (finish, finished) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let mut socket = loop {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut method = String::new();
                let mut size = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if method.is_empty() {
                        method = line.clone();
                    }
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        size = value.trim().parse().unwrap();
                    }
                }
                if method.starts_with("GET ") {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    continue;
                }
                let mut body = vec![0; size];
                reader.read_exact(&mut body).unwrap();
                break socket;
            };
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"live-stream-marker\"},\"finish_reason\":null}]}\n\n").unwrap();
            socket.flush().unwrap();
            let _ = finished.recv_timeout(Duration::from_secs(30));
            let _ = socket.write_all(b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n");
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
                    &mut size,
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
                    "--no-trust",
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
        output_until(&mut master, |text| text.contains("HTTP idle timeout"));
        master.write_all(b" ").unwrap();
        output_until(&mut master, |text| {
            text.contains("HTTP idle timeout") && text.contains("disabled")
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            text.contains("HTTP idle timeout") && text.contains("30 sec")
        });
        master.write_all(b"\x1b[B\r").unwrap();
        output_until(&mut master, |text| text.contains("Steering mode · all"));
        master.write_all(b"\x1b[B\r").unwrap();
        output_until(&mut master, |text| text.contains("Follow-up mode · all"));
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
        master.write_all(b"TUI mode\r").unwrap();
        let changed = if mode == "regular" {
            "fullscreen"
        } else {
            "regular"
        };
        output_until(&mut master, |text| {
            text.contains(&format!("TUI mode · {changed}"))
        });
        master.write_all(b"\r").unwrap();
        output_until(&mut master, |text| {
            text.contains(&format!("TUI mode · {mode}"))
        });
        master
            .write_all(b"\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f")
            .unwrap();
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
        let settings: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("home/settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["httpIdleTimeoutMs"], 30_000);
        assert_eq!(settings["steeringMode"], "all");
        assert_eq!(settings["followUpMode"], "all");
        assert_eq!(settings["shellPath"], "");
        finish.send(()).unwrap();
        server.join().unwrap();
        master.write_all(b"\x1b/quit\r").unwrap();
    }
}

#[test]
fn resize_and_suspend_preserve_input_and_restore_terminal() {
    let root = tempfile::tempdir().unwrap();
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
            ])
            .env("PI_RS_HOME", root.path().join("home"))
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
    master.write_all(b"resume-draft").unwrap();
    output_until(&mut master, |text| text.contains("resume-draft"));
    master.write_all(b"\x1a").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stopped = false;
    while Instant::now() < deadline {
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
        let mut buffer = [0; 8192];
        let _ = master.read(&mut buffer);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(stopped, "foreground job did not suspend");
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
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    assert_eq!(
        unsafe { libc::kill(-(process.0.id() as libc::pid_t), libc::SIGCONT) },
        0
    );
    output_until(&mut master, |text| text.contains("resume-draft"));
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
                &mut size,
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
                "--mode",
                "rpc",
                "--no-mcp",
            ])
            .env("PI_RS_HOME", &home)
            .env("TERM", "xterm-256color")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    output_until(&mut master, |text| text.contains("Resume session"));
    master.write_all(b"Resume Fixture").unwrap();
    output_until(&mut master, |text| {
        text.contains("Resume session") && text.contains("Resume Fixture")
    });
    master.write_all(b"\r").unwrap();
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
                &mut size,
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
                    &mut size,
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
            text.contains(control) && text.contains(&format!("TUI mode · {changed}"))
        });
        master.write_all(b"\r").unwrap();
        let control = if initial == "regular" {
            "\u{1b}[?1049l"
        } else {
            "\u{1b}[?1049h"
        };
        output_until(&mut master, |text| {
            text.contains(control) && text.contains(&format!("TUI mode · {initial}"))
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
                    "Mermaid rendering",
                    "markdown.mermaid",
                    serde_json::json!("off"),
                ),
            ] {
                master.write_all(b"/settings\r").unwrap();
                output_until(&mut master, |text| text.contains("Settings"));
                master.write_all(label.as_bytes()).unwrap();
                output_until(&mut master, |text| text.contains(label));
                master.write_all(b"\r").unwrap();
                let displayed = expected
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| expected.to_string());
                output_until(&mut master, |text| {
                    text.contains(&format!("{label} · {displayed}"))
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
    }
}
