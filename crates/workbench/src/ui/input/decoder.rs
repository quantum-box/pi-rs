use std::collections::VecDeque;
use std::time::Instant;

use super::{InputEvent, escape_delay, parse::parse_event};

#[derive(Default)]
pub(super) struct Decoder {
    pub(super) buffer: Vec<u8>,
    pub(super) events: VecDeque<InputEvent>,
    pub(super) escape_started: Option<Instant>,
    discarding_osc: bool,
    discarded_osc_target: Option<u8>,
    discarded_osc_escape: bool,
    recovering_osc_escape: bool,
}
impl Decoder {
    pub(super) fn push(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if self.discarding_osc {
                if matches!(*byte, 0x18 | 0x1a) {
                    self.discarding_osc = false;
                    self.discarded_osc_escape = false;
                    self.discarded_osc_target = None;
                } else if self.discarded_osc_escape && *byte != b'\\' {
                    self.discarding_osc = false;
                    self.discarded_osc_escape = false;
                    self.discarded_osc_target = None;
                    self.restart_after_interrupted_osc(*byte);
                } else if *byte == 7 || (self.discarded_osc_escape && *byte == b'\\') {
                    if let Some(target) = self.discarded_osc_target.take() {
                        self.events
                            .push_back(InputEvent::TerminalColorInvalid { target });
                    }
                    self.discarding_osc = false;
                    self.discarded_osc_escape = false;
                } else if *byte == 0x1b {
                    self.discarded_osc_escape = true;
                }
                continue;
            }
            if self.recovering_osc_escape {
                if *byte == 0x1b {
                    self.events
                        .push_back(InputEvent::Event(crossterm::event::Event::Key(
                            crossterm::event::KeyCode::Esc.into(),
                        )));
                    self.escape_started = Some(Instant::now());
                    continue;
                }
                self.recovering_osc_escape = false;
            }
            if color_reply_header_complete(&self.buffer) {
                if matches!(*byte, 0x18 | 0x1a) {
                    self.buffer.clear();
                    self.escape_started = None;
                    continue;
                }
                if self.buffer.last() == Some(&0x1b) && *byte != b'\\' {
                    self.buffer.clear();
                    self.escape_started = None;
                    self.restart_after_interrupted_osc(*byte);
                    continue;
                }
            }
            // Ambiguous Alt+] / OSC headers use an inactivity timeout. Each
            // fragment renews the deadline until the reply header is complete.
            if self.buffer == b"\x1b"
                || (self.buffer.starts_with(b"\x1b]") && !color_reply_header_complete(&self.buffer))
            {
                self.escape_started = Some(Instant::now());
            }
            self.buffer.push(*byte);
            if self.buffer == b"\x1b" {
                self.escape_started = Some(Instant::now());
            }
            if self.buffer.starts_with(b"\x1b]") {
                if self.buffer.len() > 256 {
                    // Keep memory bounded without letting the remaining OSC
                    // payload or its BEL terminator become user keystrokes.
                    self.discarded_osc_target = color_reply_target(&self.buffer);
                    let ended_with_st = self.buffer.ends_with(b"\x1b\\");
                    self.discarded_osc_escape = *byte == 0x1b;
                    self.discarding_osc = true;
                    self.buffer.clear();
                    self.escape_started = None;
                    if *byte == 7 || ended_with_st {
                        if let Some(target) = self.discarded_osc_target.take() {
                            self.events
                                .push_back(InputEvent::TerminalColorInvalid { target });
                        }
                        self.discarding_osc = false;
                    }
                    continue;
                }
                let foreground = b"\x1b]10;";
                let background = b"\x1b]11;";
                let palette = b"\x1b]4;";
                if foreground.starts_with(&self.buffer)
                    || background.starts_with(&self.buffer)
                    || palette.starts_with(&self.buffer)
                    || palette_header_incomplete(&self.buffer)
                {
                    continue;
                }
                if color_reply_header_complete(&self.buffer) {
                    if self.buffer.last() == Some(&7) || self.buffer.ends_with(b"\x1b\\") {
                        if self.buffer.starts_with(palette) {
                            if let Some(index) = palette_index(&self.buffer) {
                                if let Some((_, red, green, blue)) =
                                    parse_palette_color(&self.buffer)
                                {
                                    self.events.push_back(InputEvent::TerminalPalette {
                                        index,
                                        red,
                                        green,
                                        blue,
                                    });
                                } else {
                                    self.events.push_back(InputEvent::TerminalColorInvalid {
                                        target: index,
                                    });
                                }
                            }
                        } else {
                            let foreground = self.buffer.starts_with(foreground);
                            if let Some((red, green, blue)) = parse_terminal_color(&self.buffer) {
                                self.events.push_back(InputEvent::TerminalColor {
                                    foreground,
                                    red,
                                    green,
                                    blue,
                                });
                            } else {
                                self.events.push_back(InputEvent::TerminalColorInvalid {
                                    target: if foreground { 16 } else { 17 },
                                });
                            }
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
    fn restart_after_interrupted_osc(&mut self, next: u8) {
        if next == 0x1b {
            // Keep the second ESC as a possible new CSI/OSC introducer.
            self.events
                .push_back(InputEvent::Event(crossterm::event::Event::Key(
                    crossterm::event::KeyCode::Esc.into(),
                )));
            self.push(&[0x1b]);
            self.recovering_osc_escape = true;
        } else {
            self.push(&[0x1b, next]);
        }
    }

    pub(super) fn flush_escape(&mut self) {
        // A recognized terminal response remains buffered until it ends,
        // even over a slow connection. Its body must not become keystrokes.
        if color_reply_header_complete(&self.buffer) || self.discarding_osc {
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
            self.recovering_osc_escape = false;
        } else if self.buffer.starts_with(b"\x1b]") {
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
    pub(super) fn waits_for_escape_timeout(&self) -> bool {
        self.buffer == b"\x1b"
            || (self.buffer.starts_with(b"\x1b]") && !color_reply_header_complete(&self.buffer))
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

fn color_reply_header_complete(buffer: &[u8]) -> bool {
    if buffer.starts_with(b"\x1b]10;") || buffer.starts_with(b"\x1b]11;") {
        return true;
    }
    let Some(rest) = buffer.strip_prefix(b"\x1b]4;") else {
        return false;
    };
    let Some(end) = rest.iter().position(|byte| *byte == b';') else {
        return false;
    };
    (1..=3).contains(&end) && rest[..end].iter().all(u8::is_ascii_digit)
}

fn palette_header_incomplete(buffer: &[u8]) -> bool {
    buffer
        .strip_prefix(b"\x1b]4;")
        .is_some_and(|rest| rest.len() <= 3 && rest.iter().all(u8::is_ascii_digit))
}

fn color_reply_target(buffer: &[u8]) -> Option<u8> {
    if buffer.starts_with(b"\x1b]10;") {
        Some(16)
    } else if buffer.starts_with(b"\x1b]11;") {
        Some(17)
    } else {
        palette_index(buffer)
    }
}

fn parse_terminal_color(buffer: &[u8]) -> Option<(u8, u8, u8)> {
    let bytes = buffer
        .strip_prefix(b"\x1b]11;")
        .or_else(|| buffer.strip_prefix(b"\x1b]10;"))?;
    parse_color_value(bytes)
}

fn palette_index(buffer: &[u8]) -> Option<u8> {
    let value = buffer.strip_prefix(b"\x1b]4;")?;
    let split = value.iter().position(|byte| *byte == b';')?;
    let digits = &value[..split];
    if digits.is_empty() || digits.len() > 3 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let index: u8 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    (index < 16).then_some(index)
}

fn parse_palette_color(buffer: &[u8]) -> Option<(u8, u8, u8, u8)> {
    let index = palette_index(buffer)?;
    let value = buffer.strip_prefix(b"\x1b]4;")?;
    let split = value.iter().position(|byte| *byte == b';')?;
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
    fn delayed_color_replies_remain_buffered_after_timeouts() {
        for (prefix, tail, expected) in [
            (
                &b"\x1b]10;rgb:ffff/"[..],
                &b"0000/8080\x07x"[..],
                InputEvent::TerminalColor {
                    foreground: true,
                    red: 255,
                    green: 0,
                    blue: 128,
                },
            ),
            (
                &b"\x1b]11;rgb:ffff/"[..],
                &b"0000/8080\x1b\\x"[..],
                InputEvent::TerminalColor {
                    foreground: false,
                    red: 255,
                    green: 0,
                    blue: 128,
                },
            ),
            (
                &b"\x1b]4;1;rgb:ffff/"[..],
                &b"0000/8080\x07x"[..],
                InputEvent::TerminalPalette {
                    index: 1,
                    red: 255,
                    green: 0,
                    blue: 128,
                },
            ),
        ] {
            let mut decoder = Decoder::default();
            decoder.push(prefix);
            decoder.escape_started = Some(Instant::now() - std::time::Duration::from_secs(5));
            decoder.flush_escape();
            decoder.flush_escape();
            assert!(decoder.next().is_none());
            decoder.push(tail);
            assert_eq!(decoder.next(), Some(expected));
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
            );
            assert!(decoder.next().is_none());
        }
    }

    #[test]
    fn fragmented_palette_header_renews_its_inactivity_deadline() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]4;");
        let older = Instant::now() - std::time::Duration::from_millis(10);
        decoder.escape_started = Some(older);
        decoder.flush_escape();
        assert!(decoder.next().is_none());
        decoder.push(b"1");
        assert!(decoder.escape_started.unwrap() > older);
        decoder.escape_started = Some(Instant::now() - std::time::Duration::from_millis(10));
        decoder.flush_escape();
        assert!(decoder.next().is_none());
        decoder.push(b";rgb:ffff/0000/8080\x07x");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalPalette {
                index: 1,
                red: 255,
                green: 0,
                blue: 128
            })
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        assert!(decoder.next().is_none());
    }

    #[test]
    fn incomplete_palette_header_recovers_alt_input() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]4;12");
        decoder.escape_started = Some(Instant::now() - escape_delay());
        decoder.flush_escape();
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char(']') && key.modifiers == KeyModifiers::ALT)
        );
        for ch in ['4', ';', '1', '2'] {
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char(ch))
            );
        }
        assert!(decoder.next().is_none());
    }

    #[test]
    fn cancelled_or_interrupted_color_reply_recovers_following_input() {
        for cancel in [0x18, 0x1a] {
            let mut decoder = Decoder::default();
            decoder.push(b"\x1b]10;rgb:ffff/");
            decoder.push(&[cancel, b'x']);
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
            );
            assert!(decoder.next().is_none());
        }
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]10;rgb:ffff/\x1b");
        decoder.push(b"[Ax");
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Up)
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        assert!(decoder.next().is_none());
    }

    #[test]
    fn interrupted_reply_keeps_final_escape_as_new_sequence() {
        for oversized in [false, true] {
            for count in 2..=5 {
                for split in [false, true] {
                    for next in [&b"]11;rgb:ffff/0000/8080\x07"[..], &b"[A"[..]] {
                        let mut decoder = Decoder::default();
                        decoder.push(b"\x1b]10;rgb:");
                        if oversized {
                            decoder.push(&[b'a'; 300]);
                        }
                        let mut input = vec![0x1b; count];
                        input.extend_from_slice(next);
                        input.push(b'x');
                        if split {
                            for byte in input {
                                decoder.push(&[byte]);
                            }
                        } else {
                            decoder.push(&input);
                        }
                        for _ in 1..count {
                            assert!(
                                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Esc)
                            );
                        }
                        if next.starts_with(b"]") {
                            assert_eq!(
                                decoder.next(),
                                Some(InputEvent::TerminalColor {
                                    foreground: false,
                                    red: 255,
                                    green: 0,
                                    blue: 128
                                })
                            );
                        } else {
                            assert!(
                                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Up)
                            );
                        }
                        assert!(
                            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
                        );
                        assert!(decoder.next().is_none());
                    }
                }
            }
        }
    }

    #[test]
    fn escape_bel_after_oversized_reply_matches_short_reply() {
        let mut event_sequences = Vec::new();
        for oversized in [false, true] {
            let mut decoder = Decoder::default();
            decoder.push(b"\x1b]10;rgb:");
            if oversized {
                decoder.push(&[b'a'; 300]);
            }
            decoder.push(b"\x1b\x07x");
            let mut events = Vec::new();
            while let Some(event) = decoder.next() {
                events.push(event);
            }
            event_sequences.push(events);
        }
        assert_eq!(event_sequences[0], event_sequences[1]);
        assert!(!event_sequences[0].is_empty());
    }

    #[test]
    fn oversized_color_reply_is_discarded_until_terminator() {
        for terminator in [&b"\x07"[..], &b"\x1b\\"[..]] {
            let mut decoder = Decoder::default();
            decoder.push(b"\x1b]10;");
            decoder.push(&[b'a'; 4096]);
            assert!(decoder.buffer.len() <= 256);
            decoder.escape_started = Some(Instant::now() - std::time::Duration::from_secs(5));
            decoder.flush_escape();
            assert!(decoder.next().is_none());
            for byte in terminator {
                decoder.push(&[*byte]);
            }
            decoder.push(b"x");
            assert_eq!(
                decoder.next(),
                Some(InputEvent::TerminalColorInvalid { target: 16 })
            );
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
            );
            assert!(decoder.next().is_none());
        }
        // An ST whose final byte is exactly the first byte beyond the limit.
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]10;");
        decoder.push(&[b'a'; 250]);
        decoder.push(b"\x1b\\x");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalColorInvalid { target: 16 })
        );
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
    }

    #[test]
    fn oversized_reply_recovers_after_cancel_or_new_sequence() {
        for cancel in [0x18, 0x1a] {
            let mut decoder = Decoder::default();
            decoder.push(b"\x1b]4;3;");
            decoder.push(&[b'a'; 300]);
            decoder.push(&[cancel, b'x']);
            assert!(
                matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
            );
            assert!(decoder.next().is_none());
        }
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]4;3;");
        decoder.push(&[b'a'; 300]);
        decoder.push(b"\x1b]11;rgb:ffff/0000/8080\x07x");
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
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
        );
        assert!(decoder.next().is_none());
    }

    #[test]
    fn recognized_header_does_not_schedule_an_escape_timeout() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]4;12");
        assert!(decoder.waits_for_escape_timeout());
        decoder.push(b";rgb:");
        assert!(!decoder.waits_for_escape_timeout());
        decoder.push(b"\x1b");
        assert!(!decoder.waits_for_escape_timeout());
        decoder.push(b"\\");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::TerminalColorInvalid { target: 12 })
        );
    }

    #[test]
    fn bracketed_paste_keeps_color_reply_sequences_literal() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b[200~a\x1b]10;rgb:ffff/0000/8080\x07b\x1b[201~");
        assert_eq!(
            decoder.next(),
            Some(InputEvent::Event(Event::Paste(
                "a\x1b]10;rgb:ffff/0000/8080\x07b".into()
            )))
        );
        assert!(decoder.next().is_none());
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
    fn malformed_color_values_are_counted_without_becoming_keyboard_input() {
        let mut decoder = Decoder::default();
        decoder.push(b"\x1b]10;bogus\x07\x1b]11;?\x1b\\\x1b]4;7;bad\x07x");
        for target in [16, 17, 7] {
            assert_eq!(
                decoder.next(),
                Some(InputEvent::TerminalColorInvalid { target })
            );
        }
        assert!(
            matches!(decoder.next(), Some(InputEvent::Event(Event::Key(key))) if key.code == KeyCode::Char('x'))
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
