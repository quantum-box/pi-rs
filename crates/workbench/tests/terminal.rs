#![cfg(unix)]

use std::fs::File;
use std::io::{ErrorKind, Read, Write};
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
