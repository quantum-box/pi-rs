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
#[cfg(not(windows))]
fn drain_until_idle(
    max: Duration,
    idle: Duration,
    read: impl FnMut(Duration) -> std::io::Result<bool>,
) -> std::io::Result<()> {
    drain_until_idle_with_clock(max, idle, Instant::now, read)
}

fn drain_until_idle_with_clock(
    max: Duration,
    idle: Duration,
    mut clock: impl FnMut() -> Instant,
    mut read: impl FnMut(Duration) -> std::io::Result<bool>,
) -> std::io::Result<()> {
    let started = clock();
    let mut last_data = started;
    loop {
        let now = clock();
        let remaining = max.saturating_sub(now.duration_since(started));
        let quiet = idle.saturating_sub(now.duration_since(last_data));
        if remaining.is_zero() || quiet.is_zero() {
            return Ok(());
        }
        if read(remaining.min(quiet))? {
            last_data = clock();
        }
    }
}

#[cfg(windows)]
fn drain_console_records(
    max: Duration,
    idle: Duration,
    wait: impl FnMut(Duration) -> std::io::Result<bool>,
    discard: impl FnMut() -> std::io::Result<()>,
) -> std::io::Result<()> {
    drain_console_records_with_clock(max, idle, Instant::now, wait, discard)
}

#[cfg(any(windows, test))]
fn drain_console_records_with_clock(
    max: Duration,
    idle: Duration,
    clock: impl FnMut() -> Instant,
    mut wait: impl FnMut(Duration) -> std::io::Result<bool>,
    mut discard: impl FnMut() -> std::io::Result<()>,
) -> std::io::Result<()> {
    drain_until_idle_with_clock(max, idle, clock, |timeout| {
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
        let start = Instant::now();
        let elapsed = std::cell::Cell::new(Duration::ZERO);
        let pending = std::cell::Cell::new(None);
        let mut records = std::collections::VecDeque::from([
            (Duration::from_millis(20), b'\x1b'),
            (Duration::from_millis(55), b'['),
            (Duration::from_millis(90), b'9'),
        ]);
        let mut discarded = Vec::new();
        drain_console_records_with_clock(
            Duration::from_secs(1),
            Duration::from_millis(50),
            || start + elapsed.get(),
            |timeout| {
                let until = elapsed.get() + timeout;
                if records
                    .front()
                    .is_some_and(|(arrival, _)| *arrival <= until)
                {
                    let (arrival, byte) = records.pop_front().unwrap();
                    elapsed.set(arrival);
                    pending.set(Some(byte));
                    Ok(true)
                } else {
                    elapsed.set(until);
                    Ok(false)
                }
            },
            || {
                discarded.push(pending.take().unwrap());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(discarded, b"\x1b[9");
        assert!(records.is_empty());
        assert_eq!(elapsed.get(), Duration::from_millis(140));
    }

    #[test]
    fn drain_extends_quiet_period_after_each_fragment() {
        let start = Instant::now();
        let elapsed = std::cell::Cell::new(Duration::ZERO);
        let mut arrivals =
            std::collections::VecDeque::from([20, 55, 90].map(Duration::from_millis));
        let mut waits = Vec::new();
        drain_until_idle_with_clock(
            Duration::from_secs(1),
            Duration::from_millis(50),
            || start + elapsed.get(),
            |timeout| {
                waits.push(timeout);
                let until = elapsed.get() + timeout;
                if arrivals.front().is_some_and(|arrival| *arrival <= until) {
                    elapsed.set(arrivals.pop_front().unwrap());
                    Ok(true)
                } else {
                    elapsed.set(until);
                    Ok(false)
                }
            },
        )
        .unwrap();
        assert!(arrivals.is_empty());
        assert_eq!(waits, vec![Duration::from_millis(50); 4]);
        assert_eq!(elapsed.get(), Duration::from_millis(140));
    }

    #[test]
    fn drain_stops_at_deadline_during_continuous_input() {
        let start = Instant::now();
        let elapsed = std::cell::Cell::new(Duration::ZERO);
        let mut waits = Vec::new();
        drain_until_idle_with_clock(
            Duration::from_millis(40),
            Duration::from_millis(30),
            || start + elapsed.get(),
            |timeout| {
                waits.push(timeout);
                elapsed.set(elapsed.get() + timeout.min(Duration::from_millis(10)));
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(waits, [30, 30, 20, 10].map(Duration::from_millis));
        assert_eq!(elapsed.get(), Duration::from_millis(40));
    }
}
