//! Windows console input with VT replies decoded before ordinary key handling.
use std::io;
use std::time::{Duration, Instant};

use crossterm::event::Event;
use crossterm_winapi::{Console, Handle, InputRecord, KeyEventRecord};
use winapi::shared::winerror::WAIT_TIMEOUT;
use winapi::um::{
    synchapi::WaitForSingleObject,
    winbase::{WAIT_FAILED, WAIT_OBJECT_0},
    wincon::{
        LEFT_ALT_PRESSED, LEFT_CTRL_PRESSED, RIGHT_ALT_PRESSED, RIGHT_CTRL_PRESSED, SHIFT_PRESSED,
    },
};

use super::windows_keys::{
    MouseButtonsPressed, handle_key_event, handle_mouse_event, handle_surrogate,
};
use super::{InputEvent, decoder::Decoder, escape_delay};

pub struct Reader {
    console: Console,
    decoder: Decoder,
    surrogate: Option<u16>,
    reply_surrogate: Option<u16>,
    mouse_buttons: MouseButtonsPressed,
}
impl Reader {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            console: Console::from(Handle::current_in_handle()?),
            decoder: Decoder::default(),
            surrogate: None,
            reply_surrogate: None,
            mouse_buttons: MouseButtonsPressed::default(),
        })
    }

    pub fn drain(&mut self, max: Duration, idle: Duration) -> io::Result<()> {
        super::drain_until_idle(max, idle, |timeout| {
            self.next(timeout).map(|event| event.is_some())
        })
    }

    pub fn next(&mut self, timeout: Duration) -> io::Result<Option<InputEvent>> {
        let started = Instant::now();
        loop {
            self.decoder.flush_escape();
            if let Some(event) = self.decoder.next() {
                return Ok(Some(event));
            }
            let mut remaining = timeout.saturating_sub(started.elapsed());
            if (self.decoder.buffer == b"\x1b"
                || (self.decoder.buffer.starts_with(b"\x1b]")
                    && self.decoder.buffer.len() < 5
                    && !self.decoder.buffer.starts_with(b"\x1b]4;")))
                && let Some(started) = self.decoder.escape_started
            {
                remaining = remaining.min(escape_delay().saturating_sub(started.elapsed()));
            }
            let handle = Handle::current_in_handle()?;
            // Round up sub-millisecond waits and keep INFINITE reserved.
            let millis =
                remaining.as_millis() + u128::from(remaining.subsec_nanos() % 1_000_000 != 0);
            let status = unsafe {
                WaitForSingleObject(*handle, millis.min(u128::from(u32::MAX - 1)) as u32)
            };
            match status {
                WAIT_OBJECT_0 => self.read_record()?,
                WAIT_TIMEOUT => {
                    self.decoder.flush_escape();
                    return Ok(self.decoder.next());
                }
                WAIT_FAILED => return Err(io::Error::last_os_error()),
                _ => return Err(io::Error::other("unexpected console wait result")),
            }
            if started.elapsed() >= timeout {
                self.decoder.flush_escape();
                return Ok(self.decoder.next());
            }
        }
    }

    fn read_record(&mut self) -> io::Result<()> {
        let event = match self.console.read_single_input_event()? {
            InputRecord::KeyEvent(record) => decode_key(
                record,
                &mut self.decoder,
                &mut self.surrogate,
                &mut self.reply_surrogate,
            ),
            InputRecord::MouseEvent(record) => {
                let event = handle_mouse_event(record, &self.mouse_buttons);
                self.mouse_buttons = MouseButtonsPressed {
                    left: record.button_state.left_button(),
                    right: record.button_state.right_button(),
                    middle: record.button_state.middle_button(),
                };
                event
            }
            InputRecord::WindowBufferSizeEvent(_) => {
                // Buffer dimensions can include scrollback. The UI needs the
                // visible console window, just like a terminal resize on Unix.
                let (columns, rows) = crossterm::terminal::size()?;
                Some(Event::Resize(columns, rows))
            }
            InputRecord::FocusEvent(record) => Some(if record.set_focus {
                Event::FocusGained
            } else {
                Event::FocusLost
            }),
            _ => None,
        };
        if let Some(event) = event {
            self.decoder.events.push_back(InputEvent::Event(event));
        }
        Ok(())
    }
}

fn decode_key(
    record: KeyEventRecord,
    decoder: &mut Decoder,
    surrogate: &mut Option<u16>,
    reply_surrogate: &mut Option<u16>,
) -> Option<Event> {
    // Console hosts inject VT replies without physical key/scancode
    // information. Crossterm's key mapper drops their ESC character.
    if record.virtual_key_code == 0
        && record.virtual_scan_code == 0
        && !record.control_key_state.has_state(
            LEFT_ALT_PRESSED
                | RIGHT_ALT_PRESSED
                | LEFT_CTRL_PRESSED
                | RIGHT_CTRL_PRESSED
                | SHIFT_PRESSED,
        )
    {
        if record.key_down && record.u_char != 0 {
            for _ in 0..record.repeat_count.max(1) {
                let ch = if (0xD800..=0xDFFF).contains(&record.u_char) {
                    handle_surrogate(reply_surrogate, record.u_char)
                } else {
                    *reply_surrogate = None;
                    char::from_u32(u32::from(record.u_char))
                };
                if let Some(ch) = ch {
                    let mut bytes = [0; 4];
                    decoder.push(ch.encode_utf8(&mut bytes).as_bytes());
                }
            }
        }
        None
    } else {
        handle_key_event(record, surrogate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEventKind};
    use winapi::um::wincon::{INPUT_RECORD, KEY_EVENT};

    fn key(value: u16, virtual_key: u16) -> KeyEventRecord {
        // Zero flags and scan code are valid console-record fields.
        let mut raw: INPUT_RECORD = unsafe { std::mem::zeroed() };
        raw.EventType = KEY_EVENT;
        let record = unsafe { raw.Event.KeyEvent_mut() };
        record.bKeyDown = 1;
        record.wRepeatCount = 1;
        record.wVirtualKeyCode = virtual_key;
        unsafe { *record.uChar.UnicodeChar_mut() = value };
        let InputRecord::KeyEvent(record) = InputRecord::from(raw) else {
            unreachable!()
        };
        record
    }

    #[test]
    fn console_replies_are_separate_from_physical_keys() {
        let mut decoder = Decoder::default();
        let mut surrogate = None;
        let mut reply_surrogate = None;
        for value in "\x1b[6;18;9t\x1b[?997;2n".encode_utf16() {
            assert!(
                decode_key(
                    key(value, 0),
                    &mut decoder,
                    &mut surrogate,
                    &mut reply_surrogate
                )
                .is_none()
            );
        }
        assert_eq!(
            decoder.next(),
            Some(InputEvent::CellSize {
                width: 9,
                height: 18
            })
        );
        assert_eq!(decoder.next(), Some(InputEvent::Appearance { dark: false }));
        assert!(decoder.next().is_none());
        let event = decode_key(
            key(13, 13),
            &mut decoder,
            &mut surrogate,
            &mut reply_surrogate,
        );
        assert!(
            matches!(event, Some(Event::Key(key)) if key.code == KeyCode::Enter && key.kind == KeyEventKind::Press)
        );
        let mut release = key(13, 13);
        release.key_down = false;
        assert!(
            matches!(decode_key(release, &mut decoder, &mut surrogate, &mut reply_surrogate), Some(Event::Key(key)) if key.kind == KeyEventKind::Release)
        );
    }

    #[test]
    fn repeated_console_characters_preserve_response_numbers() {
        let mut decoder = Decoder::default();
        let mut surrogate = None;
        let mut reply_surrogate = None;
        for value in "\x1b[?97;2n".encode_utf16() {
            let mut record = key(value, 0);
            if value == u16::from(b'9') {
                record.repeat_count = 2;
            }
            decode_key(record, &mut decoder, &mut surrogate, &mut reply_surrogate);
        }
        assert_eq!(decoder.next(), Some(InputEvent::Appearance { dark: false }));
        assert!(decoder.next().is_none());
    }

    #[test]
    fn synthetic_unicode_and_bracketed_paste_survive() {
        let mut decoder = Decoder::default();
        let mut surrogate = None;
        let mut reply_surrogate = None;
        for value in "界🦀\x1b[200~literal\x1b[6;18;9t\x1b[201~".encode_utf16() {
            decode_key(
                key(value, 0),
                &mut decoder,
                &mut surrogate,
                &mut reply_surrogate,
            );
        }
        for expected in ['界', '🦀'] {
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char(expected))
            );
        }
        assert_eq!(
            decoder.next(),
            Some(InputEvent::Event(Event::Paste(
                "literal\x1b[6;18;9t".into()
            )))
        );
        assert!(decoder.next().is_none());
    }
}
