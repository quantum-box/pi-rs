use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd};
use std::time::{Duration, Instant};

use super::{InputEvent, escape_delay, parse::parse_event};

#[derive(Default)]
struct Decoder {
    buffer: Vec<u8>,
    events: VecDeque<InputEvent>,
    escape_started: Option<Instant>,
}
impl Decoder {
    fn push(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.buffer.push(*byte);
            if self.buffer == b"\x1b" {
                self.escape_started = Some(Instant::now());
            }
            match parse_event(&self.buffer, true) {
                Ok(Some(event)) => {
                    self.events.push_back(event);
                    self.buffer.clear();
                    self.escape_started = None;
                }
                Err(_) => {
                    self.buffer.clear();
                    self.escape_started = None;
                }
                Ok(None) => {}
            }
        }
    }
    fn flush_escape(&mut self) {
        if self.buffer == b"\x1b"
            && self
                .escape_started
                .is_some_and(|start| start.elapsed() >= escape_delay())
        {
            self.events
                .push_back(InputEvent::Event(crossterm::event::Event::Key(
                    crossterm::event::KeyCode::Esc.into(),
                )));
            self.buffer.clear();
            self.escape_started = None;
        }
    }
    fn next(&mut self) -> Option<InputEvent> {
        while let Some(event) = self.events.pop_front() {
            match event {
                InputEvent::CursorPosition(column, row) => {
                    let _ = (column, row);
                }
                InputEvent::KeyboardEnhancementFlags(flags) => {
                    let _ = flags;
                }
                InputEvent::PrimaryDeviceAttributes => {}
                event => return Some(event),
            }
        }
        None
    }
}

pub struct Reader {
    file: File,
    decoder: Decoder,
    size: Option<(u16, u16, u16, u16)>,
}
impl Reader {
    pub fn new() -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open("/dev/tty")
            .or_else(|_| {
                let descriptor = unsafe { libc::dup(libc::STDIN_FILENO) };
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
        })
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
        let timeout = self
            .decoder
            .escape_started
            .filter(|_| self.decoder.buffer == b"\x1b")
            .map_or(timeout, |start| {
                timeout.min(escape_delay().saturating_sub(start.elapsed()))
            });
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
            let count = match self.file.read(&mut buffer) {
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
    use crossterm::event::{Event, KeyCode, KeyModifiers};

    #[test]
    fn split_cell_reports_preserve_surrounding_unicode_and_keys() {
        let mut decoder = Decoder::default();
        let text = "界".as_bytes();
        decoder.push(&text[..1]);
        assert!(decoder.next().is_none());
        decoder.push(&text[1..]);
        decoder.push(b"\x1b[6;18;");
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('界'))
        );
        assert!(decoder.next().is_none());
        decoder.push(b"9tx\x1bp\x03");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::CellSize {
                width: 9,
                height: 18
            })
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('p') && key.modifiers == KeyModifiers::ALT)
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('c') && key.modifiers == KeyModifiers::CONTROL)
        );
        assert!(decoder.next().is_none());
    }

    #[test]
    fn appearance_reports_are_separate_from_user_keys() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b[?997;");
        assert!(decoder.next().is_none());
        decoder.push(b"2na\x1b[?997;1n\x1b[?997;3nb");
        assert_eq!(decoder.next(), Some(InputEvent::Appearance { dark: false }));
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('a'))
        );
        assert_eq!(decoder.next(), Some(InputEvent::Appearance { dark: true }));
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('b'))
        );
        assert!(decoder.next().is_none());
    }

    #[test]
    fn pasted_terminal_sequences_stay_literal_and_escape_is_delayed() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b[200~literal\x1b[6;18;9t\x1b[201~");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::Event(Event::Paste(
                "literal\x1b[6;18;9t".into()
            )))
        );
        decoder.push(b"\x1b");
        assert!(decoder.next().is_none());
        decoder.escape_started = Some(Instant::now() - escape_delay());
        decoder.flush_escape();
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Esc)
        );
        decoder.push(b"\x1b[6;18;9;3tx");
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        assert!(decoder.next().is_none());
    }
}
