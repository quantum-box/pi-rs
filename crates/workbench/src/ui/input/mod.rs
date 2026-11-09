//! Native input events, including terminal replies that are not keyboard input.
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub enum InputEvent {
    Event(crossterm::event::Event),
    #[cfg(unix)]
    CellSize {
        width: u16,
        height: u16,
    },
    #[cfg(unix)]
    Appearance {
        dark: bool,
    },
    #[cfg(unix)]
    CursorPosition(u16, u16),
    #[cfg(unix)]
    KeyboardEnhancementFlags(crossterm::event::KeyboardEnhancementFlags),
    #[cfg(unix)]
    PrimaryDeviceAttributes,
}

#[cfg(unix)]
mod parse;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Reader;

#[cfg(not(unix))]
pub struct Reader;
#[cfg(not(unix))]
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

#[cfg(unix)]
fn escape_delay() -> Duration {
    Duration::from_millis(25)
}
