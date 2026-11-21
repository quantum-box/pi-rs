use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::time::Duration;

use super::{InputEvent, decoder::Decoder};

pub struct Reader {
    file: File,
    decoder: Decoder,
    size: Option<(u16, u16, u16, u16)>,
    read_one: bool,
}
impl Reader {
    pub fn new() -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            // A terminal can report readiness even when a blocking read would
            // wait for another byte; keep escape-key timeouts responsive.
            .custom_flags(libc::O_NONBLOCK)
            .open("/dev/tty")
            .or_else(|_| {
                let descriptor =
                    unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_DUPFD_CLOEXEC, 3) };
                if descriptor < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(unsafe { File::from_raw_fd(descriptor) })
                }
            })?;
        Ok(Self {
            file,
            decoder: Decoder::default(),
            size: terminal_size(),
            read_one: false,
        })
    }

    // Avoid pulling input intended for the next terminal owner into our decoder.
    pub fn new_for_handoff() -> io::Result<Self> {
        let mut reader = Self::new()?;
        reader.read_for_handoff(true);
        Ok(reader)
    }

    pub fn read_for_handoff(&mut self, enabled: bool) {
        self.read_one = enabled;
    }

    pub fn drain(&mut self, max: Duration, idle: Duration) -> io::Result<()> {
        // Read bytes directly: an incomplete escape sequence is still activity,
        // even when the event decoder cannot yet produce an event.
        super::drain_until_idle(max, idle, |timeout| self.discard_input(timeout))
    }
    fn discard_input(&mut self, timeout: Duration) -> io::Result<bool> {
        let mut descriptor = libc::pollfd {
            fd: self.file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = timeout.as_millis().clamp(1, i32::MAX as u128) as i32;
        let ready = unsafe { libc::poll(&mut descriptor, 1, millis) };
        if ready < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(false)
            } else {
                Err(error)
            };
        }
        if ready == 0 {
            return Ok(false);
        }
        let mut bytes = [0_u8; 1024];
        match self.file.read(&mut bytes) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "terminal input closed",
            )),
            Ok(_) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    pub fn next(&mut self, timeout: Duration) -> io::Result<Option<InputEvent>> {
        self.decoder.flush_escape();
        if let Some(event) = self.decoder.next() {
            return Ok(Some(event));
        }
        let size = terminal_size();
        if size != self.size {
            self.size = size;
            if let Some((columns, rows, _, _)) = size {
                return Ok(Some(InputEvent::Event(crossterm::event::Event::Resize(
                    columns, rows,
                ))));
            }
        }
        let timeout = match (self.decoder.escape_started, self.decoder.escape_timeout()) {
            (Some(started), Some(delay)) => timeout.min(delay.saturating_sub(started.elapsed())),
            _ => timeout,
        };
        let mut descriptor = libc::pollfd {
            fd: self.file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let milliseconds = if timeout.is_zero() {
            0
        } else {
            timeout.as_millis().clamp(1, i32::MAX as u128) as i32
        };
        let result = unsafe { libc::poll(&mut descriptor, 1, milliseconds) };
        if result < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(None)
            } else {
                Err(error)
            };
        }
        if result > 0 {
            let mut buffer = [0_u8; 1024];
            let length = if self.read_one { 1 } else { buffer.len() };
            let count = match self.file.read(&mut buffer[..length]) {
                Ok(count) => count,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "terminal input closed",
                ));
            }
            self.decoder.push(&buffer[..count]);
        }
        self.decoder.flush_escape();
        Ok(self.decoder.next())
    }
}
fn terminal_size() -> Option<(u16, u16, u16, u16)> {
    crossterm::terminal::window_size()
        .ok()
        .map(|size| (size.columns, size.rows, size.width, size.height))
        .or_else(|| {
            crossterm::terminal::size()
                .ok()
                .map(|(columns, rows)| (columns, rows, 0, 0))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::IntoRawFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn drain_consumes_fragmented_late_input_until_quiet() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut input = Reader {
            file: unsafe { File::from_raw_fd(reader.into_raw_fd()) },
            decoder: Decoder::default(),
            size: None,
            read_one: false,
        };
        // Force the raw reader to make multiple reads, with an incomplete
        // escape sequence crossing its 1024-byte buffer boundary.
        writer.write_all(&[b'x'; 1023]).unwrap();
        writer.write_all(b"\x1b[").unwrap();
        writer.write_all(b"99;5:3u").unwrap();
        let start = std::time::Instant::now();
        let elapsed = std::cell::Cell::new(Duration::ZERO);
        let mut reads = 0;
        super::super::drain_until_idle_with_clock(
            Duration::from_secs(1),
            Duration::from_millis(20),
            || start + elapsed.get(),
            |timeout| {
                let consumed = input.discard_input(Duration::ZERO)?;
                if consumed {
                    reads += 1;
                } else {
                    elapsed.set(elapsed.get() + timeout);
                }
                Ok(consumed)
            },
        )
        .unwrap();
        assert!(reads >= 2);
        assert_eq!(elapsed.get(), Duration::from_millis(20));
        let mut bytes = [0; 32];
        assert_eq!(
            input.file.read(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
