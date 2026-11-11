//! Native input events, including terminal replies that are not keyboard input.
use std::time::Duration;

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
