//! Terminal transport for extension editors that can be cancelled by the backend.
//! The editor gets a separate controlling terminal. Pi owns the only master
//! descriptor and drops it before it returns the user's terminal to the TUI.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::process::Command;

const BUFFER_SIZE: usize = 4096;

pub(super) struct EditorPty {
    master: File,
    input: PendingBytes,
    output: PendingBytes,
    hung_up: bool,
}

impl EditorPty {
    pub(super) fn attach(
        command: &mut Command,
        terminal: Option<(&File, &libc::termios)>,
    ) -> io::Result<Self> {
        let (master, slave) = open_pty()?;
        let inherited = inherited_descriptors()?;
        let fd = slave.as_raw_fd();
        if let Some((tty, baseline)) = terminal {
            check(unsafe { libc::tcsetattr(fd, libc::TCSANOW, baseline) })?;
            copy_size(tty, &master)?;
        }
        command
            .stdin(slave.try_clone()?)
            .stdout(slave.try_clone()?)
            .stderr(slave);
        // All three standard descriptors are already connected to the slave
        // before pre_exec runs. Neither terminal nor the master survives exec.
        unsafe {
            command.pre_exec(move || {
                mark_close_on_exec(&inherited)?;
                check(libc::setsid())?;
                check(libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY as _, 0))?;
                Ok(())
            });
        }
        Ok(Self {
            master,
            input: PendingBytes::default(),
            output: PendingBytes::default(),
            hung_up: false,
        })
    }

    /// Transfer at most one buffer in each direction per call. A stalled
    /// terminal or editor must leave the frontend able to process cancellation.
    /// Returns true when no more output is immediately available.
    pub(super) fn relay(&mut self, mut tty: &File, read_input: bool) -> io::Result<bool> {
        if !self.hung_up {
            copy_size(tty, &self.master)?;
        }

        let mut drained = true;
        if self.output.is_empty() && !self.hung_up {
            match self.output.read_from(&mut self.master) {
                Ok(Some(0)) => self.hung_up = true,
                Ok(Some(_)) => drained = false,
                Ok(None) => {}
                Err(error) if error.raw_os_error() == Some(libc::EIO) => self.hung_up = true,
                Err(error) => return Err(error),
            }
        }
        self.output.write_to(&mut tty)?;
        if read_input && !self.hung_up {
            if self.input.is_empty() && self.input.read_from(&mut tty)? == Some(0) {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            match self.input.write_to(&mut self.master) {
                Ok(()) => {}
                Err(error) if error.raw_os_error() == Some(libc::EIO) => self.hung_up = true,
                Err(error) => return Err(error),
            }
        }
        Ok(drained && self.output.is_empty())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_pty() -> io::Result<(File, File)> {
    let flags = libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC;
    let fd = check(unsafe { libc::posix_openpt(flags) })?;
    let master = unsafe { File::from_raw_fd(fd) };
    check(unsafe { libc::grantpt(fd) })?;
    check(unsafe { libc::unlockpt(fd) })?;
    check(unsafe { libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) })?;
    #[cfg(target_os = "linux")]
    let slave_fd = check(unsafe { libc::ioctl(fd, libc::TIOCGPTPEER, flags) })?;
    #[cfg(target_os = "macos")]
    let slave_fd = {
        let mut name = [0 as libc::c_char; 128];
        check(unsafe { libc::ioctl(fd, libc::TIOCPTYGNAME.into(), name.as_mut_ptr()) })?;
        check(unsafe { libc::open(name.as_ptr(), flags) })?
    };
    Ok((master, unsafe { File::from_raw_fd(slave_fd) }))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn open_pty() -> io::Result<(File, File)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "isolated extension editors require Linux or macOS",
    ))
}

fn inherited_descriptors() -> io::Result<Vec<libc::c_int>> {
    #[cfg(target_os = "linux")]
    let directory = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let directory = "/dev/fd";
    let mut descriptors = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let name = entry?.file_name();
        if let Some(fd) = name.to_str().and_then(|name| name.parse().ok())
            && fd > libc::STDERR_FILENO
        {
            descriptors.push(fd);
        }
    }
    Ok(descriptors)
}

fn mark_close_on_exec(inherited: &[libc::c_int]) -> io::Result<()> {
    // Preserve std::process's exec-error pipe until exec. File descriptors Pi
    // opens during startup already use close-on-exec, but callers can pass
    // extra descriptors, including a copy of the user's terminal.
    #[cfg(target_os = "linux")]
    if unsafe {
        libc::syscall(
            libc::SYS_close_range,
            3 as libc::c_uint,
            libc::c_uint::MAX,
            libc::CLOSE_RANGE_CLOEXEC,
        )
    } == 0
    {
        return Ok(());
    }
    for &fd in inherited {
        if let Err(error) = check(unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) })
            && error.raw_os_error() != Some(libc::EBADF)
        {
            return Err(error);
        }
    }
    Ok(())
}

fn copy_size(from: &File, to: &File) -> io::Result<()> {
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    check(unsafe { libc::ioctl(from.as_raw_fd(), libc::TIOCGWINSZ, &mut size) })?;
    check(unsafe { libc::ioctl(to.as_raw_fd(), libc::TIOCSWINSZ, &size) })?;
    Ok(())
}

fn check(value: libc::c_int) -> io::Result<libc::c_int> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

struct PendingBytes {
    bytes: [u8; BUFFER_SIZE],
    start: usize,
    end: usize,
}

impl Default for PendingBytes {
    fn default() -> Self {
        Self {
            bytes: [0; BUFFER_SIZE],
            start: 0,
            end: 0,
        }
    }
}

impl PendingBytes {
    fn is_empty(&self) -> bool {
        self.start == self.end
    }

    fn read_from(&mut self, from: &mut impl Read) -> io::Result<Option<usize>> {
        debug_assert!(self.is_empty());
        match retry_later(from.read(&mut self.bytes))? {
            Some(count) => {
                self.start = 0;
                self.end = count;
                Ok(Some(count))
            }
            None => Ok(None),
        }
    }

    fn write_to(&mut self, to: &mut impl Write) -> io::Result<()> {
        if !self.is_empty()
            && let Some(count) = retry_later(to.write(&self.bytes[self.start..self.end]))?
        {
            self.start += count;
        }
        Ok(())
    }
}

fn retry_later<T>(result: io::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
