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
            if self.buffer.starts_with(b"\x1b]") {
                if self.buffer.len() > 256 {
                    self.buffer.clear();
                    self.escape_started = None;
                    continue;
                }
                let foreground = b"\x1b]10;";
                let background = b"\x1b]11;";
                let palette = b"\x1b]4;";
                if foreground.starts_with(&self.buffer)
                    || background.starts_with(&self.buffer)
                    || palette.starts_with(&self.buffer)
                {
                    continue;
                }
                if self.buffer.starts_with(foreground)
                    || self.buffer.starts_with(background)
                    || self.buffer.starts_with(palette)
                {
                    if self.buffer.last() == Some(&7) || self.buffer.ends_with(b"\x1b\\") {
                        if self.buffer.starts_with(palette) {
                            if let Some((index, red, green, blue)) =
                                parse_palette_color(&self.buffer)
                            {
                                self.events.push_back(InputEvent::TerminalPalette {
                                    index,
                                    red,
                                    green,
                                    blue,
                                });
                            }
                        } else if let Some((red, green, blue)) = parse_terminal_color(&self.buffer)
                        {
                            self.events.push_back(InputEvent::TerminalColor {
                                foreground: self.buffer.starts_with(foreground),
                                red,
                                green,
                                blue,
                            });
                        }
                        self.buffer.clear();
                        self.escape_started = None;
                    }
                    continue;
                }
                let remaining = self.buffer.split_off(2);
                self.buffer.clear();
                self.escape_started = None;
                self.events
                    .push_back(InputEvent::Event(crossterm::event::Event::Key(
                        crossterm::event::KeyEvent::new(
                            crossterm::event::KeyCode::Char(']'),
                            crossterm::event::KeyModifiers::ALT,
                        ),
                    )));
                self.push(&remaining);
                continue;
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
        if (self.buffer.starts_with(b"\x1b]10;")
            || self.buffer.starts_with(b"\x1b]11;")
            || self.buffer.starts_with(b"\x1b]4;"))
            && self
                .escape_started
                .is_some_and(|start| start.elapsed() >= std::time::Duration::from_millis(500))
        {
            self.buffer.clear();
            self.escape_started = None;
            return;
        }
        if self
            .escape_started
            .is_none_or(|start| start.elapsed() < escape_delay())
        {
            return;
        }
        if self.buffer == b"\x1b" {
            self.events
                .push_back(InputEvent::Event(crossterm::event::Event::Key(
                    crossterm::event::KeyCode::Esc.into(),
                )));
            self.buffer.clear();
            self.escape_started = None;
        } else if self.buffer.starts_with(b"\x1b]")
            && self.buffer.len() < 5
            && !self.buffer.starts_with(b"\x1b]4;")
        {
            let remaining = self.buffer.split_off(2);
            self.buffer.clear();
            self.escape_started = None;
            self.events
                .push_back(InputEvent::Event(crossterm::event::Event::Key(
                    crossterm::event::KeyEvent::new(
                        crossterm::event::KeyCode::Char(']'),
                        crossterm::event::KeyModifiers::ALT,
                    ),
                )));
            self.push(&remaining);
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

fn parse_terminal_color(buffer: &[u8]) -> Option<(u8, u8, u8)> {
    let bytes = buffer
        .strip_prefix(b"\x1b]11;")
        .or_else(|| buffer.strip_prefix(b"\x1b]10;"))?;
    parse_color_value(bytes)
}

fn parse_palette_color(buffer: &[u8]) -> Option<(u8, u8, u8, u8)> {
    let value = buffer.strip_prefix(b"\x1b]4;")?;
    let split = value.iter().position(|byte| *byte == b';')?;
    let digits = &value[..split];
    if digits.is_empty() || digits.len() > 3 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let index: u8 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    if index >= 16 {
        return None;
    }
    let (red, green, blue) = parse_color_value(&value[split + 1..])?;
    Some((index, red, green, blue))
}

fn parse_color_value(bytes: &[u8]) -> Option<(u8, u8, u8)> {
    let bytes = bytes
        .strip_suffix(b"\x07")
        .or_else(|| bytes.strip_suffix(b"\x1b\\"))?;
    let value = std::str::from_utf8(bytes).ok()?.trim();
    let channels: Vec<&str> = if let Some(hex) = value.strip_prefix('#') {
        match hex.len() {
            6 => vec![hex.get(0..2)?, hex.get(2..4)?, hex.get(4..6)?],
            12 => vec![hex.get(0..4)?, hex.get(4..8)?, hex.get(8..12)?],
            _ => return None,
        }
    } else {
        let prefix = value
            .get(..5)
            .filter(|prefix| prefix.eq_ignore_ascii_case("rgba:"))
            .map(|_| 5)
            .or_else(|| {
                value
                    .get(..4)
                    .filter(|prefix| prefix.eq_ignore_ascii_case("rgb:"))
                    .map(|_| 4)
            })
            .unwrap_or(0);
        let value = &value[prefix..];
        value.split('/').take(3).collect()
    };
    if channels.len() != 3 {
        return None;
    }
    let channel = |text: &str| {
        if text.is_empty() || text.len() > 8 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let value = u32::from_str_radix(text, 16).ok()? as f64;
        let maximum = (16_f64).powi(text.len() as i32) - 1.0;
        Some((value / maximum * 255.0).round() as u8)
    };
    Some((
        channel(channels[0])?,
        channel(channels[1])?,
        channel(channels[2])?,
    ))
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
    fn background_replies_survive_fragmentation_and_alt_bracket_stays_a_key() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]11;rgb:ffff/0000/");
        assert!(decoder.next().is_none());
        decoder.push(b"8080\x1b\\a");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalColor {
                foreground: false,
                red: 255,
                green: 0,
                blue: 128
            })
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('a'))
        );
        decoder.push(b"\x1b]10;rgb:ffff/eeee/dddd\x07");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalColor {
                foreground: true,
                red: 255,
                green: 238,
                blue: 221
            })
        );
        decoder.push(b"\x1b]11;#aabbcc\x07");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalColor {
                foreground: false,
                red: 170,
                green: 187,
                blue: 204
            })
        );
        decoder.push(b"\x1b]x");
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char(']') && key.modifiers == KeyModifiers::ALT)
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        decoder.push(b"\x1b]");
        decoder.escape_started = Some(Instant::now() - escape_delay());
        decoder.flush_escape();
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char(']') && key.modifiers == KeyModifiers::ALT)
        );
    }

    #[test]
    fn palette_replies_are_decoded_without_leaking_into_keyboard_input() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]4;1");
        decoder.push(b"3;rgb:ffff/8080/0000\x1b\\x");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalPalette {
                index: 13,
                red: 255,
                green: 128,
                blue: 0
            })
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        decoder.push(b"\x1b]4;256;#ffffff\x07y");
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('y'))
        );
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
