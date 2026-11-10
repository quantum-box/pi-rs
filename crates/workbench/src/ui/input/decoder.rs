use std::collections::VecDeque;
use std::time::Instant;

use super::{InputEvent, escape_delay, parse::parse_event};

#[derive(Default)]
pub(super) struct Decoder {
    pub(super) buffer: Vec<u8>,
    pub(super) events: VecDeque<InputEvent>,
    pub(super) escape_started: Option<Instant>,
}
impl Decoder {
    pub(super) fn push(&mut self, bytes: &[u8]) {
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
    pub(super) fn flush_escape(&mut self) {
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
    pub(super) fn next(&mut self) -> Option<InputEvent> {
        while let Some(event) = self.events.pop_front() {
            match event {
                InputEvent::CursorPosition(column, row) => {
                    let _ = (column, row);
                }
                event => return Some(event),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{Event, KeyCode, KeyModifiers};

    #[test]
    fn preserves_shift_with_kitty_alternate_keys_and_xterm_modified_keys() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b[?7u\x1b[?1;2c\x1b[102:70;6u\x1b[27;6;70~\x1b[27;2;13~");
        assert!(
            matches!(decoder.next(), Some(InputEvent::KeyboardEnhancementFlags(flags)) if flags.bits() == 7)
        );
        assert_eq!(decoder.next(), Some(InputEvent::PrimaryDeviceAttributes));
        for _ in 0..2 {
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key)))
                if key.code == KeyCode::Char('F') && key.modifiers == (KeyModifiers::CONTROL | KeyModifiers::SHIFT))
            );
        }
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key)))
            if key.code == KeyCode::Enter && key.modifiers == KeyModifiers::SHIFT)
        );
    }

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
