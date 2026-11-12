//! Native input events, including terminal replies that are not keyboard input.
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
pub enum InputEvent {
    Event(crossterm::event::Event),
    #[cfg(any(unix, windows))]
    CellSize {
        width: u16,
        height: u16,
    },
    #[cfg(any(unix, windows))]
    Appearance {
        dark: bool,
    },
    #[cfg(any(unix, windows))]
    TerminalColorInvalid {
        target: u8,
    },
    #[cfg(any(unix, windows))]
    TerminalPalette {
        index: u8,
        red: u8,
        green: u8,
        blue: u8,
    },
    #[cfg(any(unix, windows))]
    TerminalColor {
        foreground: bool,
        red: u8,
        green: u8,
        blue: u8,
    },
    #[cfg(any(unix, windows))]
    CursorPosition(u16, u16),
    #[cfg(any(unix, windows))]
    KeyboardEnhancementFlags(crossterm::event::KeyboardEnhancementFlags),
    #[cfg(any(unix, windows))]
    PrimaryDeviceAttributes,
}

#[cfg(any(unix, windows))]
mod decoder;
#[cfg(any(unix, windows))]
mod parse;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
mod windows_keys;
#[cfg(windows)]
pub use windows::Reader;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Reader;

#[cfg(not(any(unix, windows)))]
pub struct Reader;
#[cfg(not(any(unix, windows)))]
impl Reader {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self)
    }
    pub fn drain(&mut self, max: Duration, idle: Duration) -> std::io::Result<()> {
        drain_until_idle(max, idle, |timeout| {
            self.next(timeout).map(|event| event.is_some())
        })
    }
    pub fn next(&mut self, timeout: Duration) -> std::io::Result<Option<InputEvent>> {
        if crossterm::event::poll(timeout)? {
            crossterm::event::read().map(InputEvent::Event).map(Some)
        } else {
            Ok(None)
        }
    }
}

#[cfg(any(unix, windows))]
fn escape_delay() -> Duration {
    Duration::from_millis(25)
}

// Keep raw mode active until input has been quiet, but bound shutdown even if
// a terminal continuously sends events. `read` must wait at most its timeout.
fn drain_until_idle(
    max: Duration,
    idle: Duration,
    mut read: impl FnMut(Duration) -> std::io::Result<bool>,
) -> std::io::Result<()> {
    let started = Instant::now();
    let mut last_data = started;
    loop {
        let now = Instant::now();
        let remaining = max.saturating_sub(now.duration_since(started));
        let quiet = idle.saturating_sub(now.duration_since(last_data));
        if remaining.is_zero() || quiet.is_zero() {
            return Ok(());
        }
        if read(remaining.min(quiet))? {
            last_data = Instant::now();
        }
    }
}

#[cfg(any(windows, test))]
fn drain_console_records(
    max: Duration,
    idle: Duration,
    mut wait: impl FnMut(Duration) -> std::io::Result<bool>,
    mut discard: impl FnMut() -> std::io::Result<()>,
) -> std::io::Result<()> {
    drain_until_idle(max, idle, |timeout| {
        if wait(timeout)? {
            discard()?;
            Ok(true)
        } else {
            Ok(false)
        }
    })
}

#[cfg(test)]
mod drain_tests {
    use super::*;

    #[test]
    fn drain_counts_incomplete_vt_records_as_activity() {
        use std::sync::mpsc;
        let (sender, receiver) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            sender.send(b'\x1b').unwrap();
            std::thread::sleep(Duration::from_millis(35));
            sender.send(b'[').unwrap();
            std::thread::sleep(Duration::from_millis(35));
            sender.send(b'9').unwrap();
        });
        let pending = std::cell::Cell::new(None);
        let mut discarded = Vec::new();
        drain_console_records(
            Duration::from_secs(1),
            Duration::from_millis(50),
            |timeout| match receiver.recv_timeout(timeout) {
                Ok(byte) => {
                    pending.set(Some(byte));
                    Ok(true)
                }
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(false),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Model a quiet console, not an immediately readable EOF.
                    std::thread::sleep(timeout);
                    Ok(false)
                }
            },
            || {
                discarded.push(pending.take().unwrap());
                Ok(())
            },
        )
        .unwrap();
        producer.join().unwrap();
        assert_eq!(discarded, b"\x1b[9");
    }

    #[test]
    fn drain_stops_at_deadline_during_continuous_input() {
        let start = Instant::now();
        let mut reads = 0;
        drain_until_idle(
            Duration::from_millis(40),
            Duration::from_millis(30),
            |timeout| {
                std::thread::sleep(timeout.min(Duration::from_millis(5)));
                reads += 1;
                Ok(true)
            },
        )
        .unwrap();
        assert!(reads > 1);
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}
