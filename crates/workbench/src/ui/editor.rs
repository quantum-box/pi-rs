//! Grapheme-safe editor with line movement, a kill ring, and undo snapshots.
use std::borrow::Cow;
use std::ops::Range;
use std::sync::LazyLock;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::editor_layout::Layout;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

static PASTE_CTRL_LETTER: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\x1b\[([0-9]+);5u").expect("paste Ctrl-letter pattern"));

const WORD_PUNCTUATION: &str = "(){}[]<>.,;:'\"!?+-=*/\\|&%^$#@~`";

fn word_segments(text: &str) -> Vec<(usize, usize, bool)> {
    let segmenter = icu_segmenter::WordSegmenter::new_dictionary(Default::default());
    let mut start = 0;
    let mut segments = Vec::new();
    for (end, kind) in segmenter.segment_str(text).iter_with_word_type() {
        if end > start {
            // ICU4X can classify a Hebrew segment ending in an ASCII
            // apostrophe as non-word while Intl.Segmenter marks it word-like.
            // Do not promote arbitrary alphabetic segments: a ZWJ emoji
            // sequence can include letters while remaining non-word.
            let segment = &text[start..end];
            let hebrew_apostrophe = segment.ends_with('\'')
                && segment
                    .chars()
                    .any(|c| ('\u{0590}'..='\u{05ff}').contains(&c) && c.is_alphabetic())
                && !segment.contains('\u{200d}');
            let word = kind.is_word_like() || hebrew_apostrophe;
            segments.push((start, end, word));
        }
        start = end;
    }
    segments
}

fn word_space(text: &str) -> bool {
    text.chars()
        .any(|c| (c.is_whitespace() && c != '\u{0085}') || c == '\u{feff}')
}

fn word_boundary(text: &str, backward: bool) -> usize {
    let mut segments = word_segments(text);
    if backward {
        while segments
            .last()
            .is_some_and(|&(a, b, _)| word_space(&text[a..b]))
        {
            segments.pop();
        }
        let Some(&(start, end, word)) = segments.last() else {
            return 0;
        };
        if word {
            text[start..end]
                .grapheme_indices(true)
                .rev()
                .find(|&(_, grapheme)| grapheme.chars().any(|c| WORD_PUNCTUATION.contains(c)))
                .map_or(start, |(i, grapheme)| {
                    let after = start + i + grapheme.len();
                    if after == end { start + i } else { after }
                })
        } else {
            segments
                .iter()
                .rev()
                .take_while(|&&(a, b, word)| !word && !word_space(&text[a..b]))
                .last()
                .map_or(start, |&(a, _, _)| a)
        }
    } else {
        let mut iter = segments.into_iter().peekable();
        let mut end = 0;
        while iter
            .peek()
            .is_some_and(|&(a, b, _)| word_space(&text[a..b]))
        {
            end = iter.next().unwrap().1;
        }
        let Some((start, finish, word)) = iter.next() else {
            return end;
        };
        if word {
            text[start..finish]
                .char_indices()
                .find(|&(_, c)| WORD_PUNCTUATION.contains(c))
                .map_or(finish, |(i, _)| start + i)
        } else {
            end = finish;
            while iter
                .peek()
                .is_some_and(|&(a, b, word)| !word && !word_space(&text[a..b]))
            {
                end = iter.next().unwrap().1;
            }
            end
        }
    }
}

#[derive(Clone)]
struct CollapsedPaste {
    source: Range<usize>,
    marker: String,
}

// Draft consumers use the original text. Only layout and navigation use markers.
struct DraftDisplay<'a> {
    text: Cow<'a, str>,
    ranges: Vec<(Range<usize>, Range<usize>)>,
}

impl DraftDisplay<'_> {
    fn display_offset(&self, original: usize) -> usize {
        let mut source_end = 0;
        let mut display_end = 0;
        for (source, display) in &self.ranges {
            if original < source.end {
                break;
            }
            source_end = source.end;
            display_end = display.end;
        }
        display_end + original - source_end
    }

    fn original_offset(&self, offset: usize, forward: bool) -> usize {
        let mut source_end = 0;
        let mut display_end = 0;
        for (source, display) in &self.ranges {
            if offset <= display.start {
                break;
            }
            if offset < display.end {
                return if forward { source.end } else { source.start };
            }
            source_end = source.end;
            display_end = display.end;
        }
        source_end + offset - display_end
    }
}

#[derive(Clone)]
struct Snapshot {
    text: String,
    cursor: usize,
    collapsed: Vec<CollapsedPaste>,
    paste_count: usize,
}
#[derive(Default)]
pub struct Editor {
    pub text: String,
    pub cursor: usize,
    undo: Vec<Snapshot>,
    collapsed: Vec<CollapsedPaste>,
    paste_count: usize,
    typing_word: bool,
    kills: Vec<String>,
    last_kill: bool,
    yank: Option<(Range<usize>, usize)>,
    preferred_column: Option<usize>,
    scroll_offset: usize,
    jump_forward: Option<bool>,
}
enum InputMode {
    Text,
    TranscriptSearch,
    SettingsSearch,
}

impl Editor {
    fn remember(&mut self) {
        self.undo.push(Snapshot {
            text: self.text.clone(),
            cursor: self.cursor,
            collapsed: self.collapsed.clone(),
            paste_count: self.paste_count,
        });
        if self.undo.len() > 100 {
            self.undo.remove(0);
        }
        self.yank = None;
        self.last_kill = false;
        self.preferred_column = None;
        self.jump_forward = None;
        self.typing_word = false;
    }
    pub fn replace(&mut self, text: String) {
        self.remember();
        self.text = text;
        self.cursor = self.text.len();
        self.collapsed.clear();
        self.paste_count = 0;
    }

    /// Keep source ranges attached to their original text as the draft changes.
    fn edit(&mut self, range: Range<usize>, replacement: &str) {
        let delta = replacement.len() as isize - range.len() as isize;
        self.collapsed.retain_mut(|paste| {
            if paste.source.start >= range.end {
                paste.source.start = paste.source.start.saturating_add_signed(delta);
                paste.source.end = paste.source.end.saturating_add_signed(delta);
                true
            } else {
                paste.source.end <= range.start
            }
        });
        self.text.replace_range(range, replacement);
    }

    fn display(&self) -> DraftDisplay<'_> {
        let mut visible_pastes = self
            .collapsed
            .iter()
            .filter(|paste| {
                // A cursor moved programmatically into pasted text can edit it directly.
                !(paste.source.start < self.cursor && self.cursor < paste.source.end)
            })
            .peekable();
        if visible_pastes.peek().is_none() {
            return DraftDisplay {
                text: Cow::Borrowed(&self.text),
                ranges: Vec::new(),
            };
        }
        let mut text = String::new();
        let mut ranges = Vec::new();
        let mut previous_end = 0;
        for paste in visible_pastes {
            text.push_str(&self.text[previous_end..paste.source.start]);
            let start = text.len();
            text.push_str(&paste.marker);
            ranges.push((paste.source.clone(), start..text.len()));
            previous_end = paste.source.end;
        }
        text.push_str(&self.text[previous_end..]);
        DraftDisplay {
            text: Cow::Owned(text),
            ranges,
        }
    }

    pub fn completion_prefix(&self) -> Option<(usize, &str)> {
        let boundary = self
            .collapsed
            .iter()
            .rev()
            .find(|paste| paste.source.end <= self.cursor)
            .map_or(0, |paste| paste.source.end);
        if boundary > 0 && boundary == self.cursor {
            return None;
        }
        Some((boundary, &self.text[boundary..self.cursor]))
    }

    pub fn visible_neighbors(&self) -> (Option<char>, Option<char>) {
        let before = self
            .collapsed
            .iter()
            .find(|paste| paste.source.end == self.cursor)
            .map_or_else(
                || self.text[..self.cursor].chars().next_back(),
                |paste| paste.marker.chars().next_back(),
            );
        let after = self
            .collapsed
            .iter()
            .find(|paste| paste.source.start == self.cursor)
            .map_or_else(
                || self.text[self.cursor..].chars().next(),
                |paste| paste.marker.chars().next(),
            );
        (before, after)
    }

    /// Paste at the current cursor as one undoable edit.
    pub fn paste(&mut self, text: &str) {
        // tmux can encode literal Ctrl-letter bytes inside bracketed paste.
        let decoded = PASTE_CTRL_LETTER.replace_all(text, |capture: &regex::Captures<'_>| {
            match capture[1].parse::<u8>() {
                Ok(byte) if byte.is_ascii_alphabetic() => String::from(char::from(byte & 0x1f)),
                _ => capture[0].to_owned(),
            }
        });
        let normalized = decoded.replace("\r\n", "\n").replace('\r', "\n");
        let mut text = super::render::clean(&normalized);
        if text.starts_with(['/', '~', '.'])
            && self
                .visible_neighbors()
                .0
                .is_some_and(|previous| previous.is_ascii_alphanumeric() || previous == '_')
        {
            text.insert(0, ' ');
        }
        if text.is_empty() {
            return;
        }
        let lines = text.bytes().filter(|byte| *byte == b'\n').count() + 1;
        let chars = text.encode_utf16().count();
        let start = self.cursor;
        self.insert_clean(&text);
        if lines > 10 || chars > 1000 {
            self.paste_count += 1;
            let count = self.paste_count;
            let amount = if lines > 10 {
                format!("+{lines} lines")
            } else {
                format!("{chars} chars")
            };
            let at = self
                .collapsed
                .partition_point(|paste| paste.source.start < start);
            self.collapsed.insert(
                at,
                CollapsedPaste {
                    source: start..self.cursor,
                    marker: format!("[paste #{count} {amount}]"),
                },
            );
        }
    }

    pub fn insert(&mut self, text: &str) {
        self.insert_clean(&super::render::clean(text));
    }

    fn insert_clean(&mut self, text: &str) {
        self.replace_range(self.cursor..self.cursor, text);
    }

    pub fn replace_range(&mut self, range: Range<usize>, text: &str) {
        self.remember();
        let cursor = range.start + text.len();
        self.edit(range, text);
        self.cursor = cursor;
    }

    // Only transcript search retains U+0085 internally. Rendering remains
    // sanitized by Span; all other control characters stay excluded.
    pub(super) fn insert_search(&mut self, text: &str) {
        let text: String = text
            .chars()
            .filter(|character| !character.is_control() || *character == '\u{85}')
            .collect();
        self.insert_clean(&text);
    }
    pub(super) fn insert_settings_search(&mut self, text: &str) {
        // Pi's single-line input removes CR/LF and expands tabs.
        // Retain the other input characters for matching and sanitize at render.
        self.insert_clean(&text.replace(['\r', '\n'], "").replace('\t', "    "));
    }
    /// Insert a typed character, grouping consecutive typing as Pi does.
    /// Paste and programmatic insertion continue to use the atomic `insert`.
    pub fn type_character(&mut self, character: char) {
        self.type_character_text(character, &super::render::clean(&character.to_string()));
    }
    pub(super) fn type_settings_search(&mut self, character: char) {
        let mut utf8 = [0; 4];
        let text = match character {
            '\r' | '\n' => return,
            '\t' => "    ",
            _ => character.encode_utf8(&mut utf8),
        };
        self.type_character_text(character, text);
    }

    fn type_character_text(&mut self, character: char, text: &str) {
        if text.is_empty() {
            return;
        }
        // JavaScript's /\s/ includes BOM but not U+0085.
        let whitespace =
            (character.is_whitespace() && character != '\u{0085}') || character == '\u{feff}';
        if whitespace || !self.typing_word {
            self.remember();
        } else {
            self.yank = None;
            self.last_kill = false;
            self.preferred_column = None;
            self.jump_forward = None;
        }
        self.edit(self.cursor..self.cursor, text);
        self.cursor += text.len();
        self.typing_word = true;
    }
    pub fn left(&mut self) {
        self.typing_word = false;
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let next = display.text[..offset]
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(index, _)| index);
        self.cursor = display.original_offset(next, false);
    }
    pub fn right(&mut self) {
        self.typing_word = false;
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let next = offset
            + display.text[offset..]
                .graphemes(true)
                .next()
                .map_or(0, str::len);
        self.cursor = display.original_offset(next, true);
    }
    pub fn line_start(&self) -> usize {
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let start = display.text[..offset]
            .rfind('\n')
            .map_or(0, |index| index + 1);
        display.original_offset(start, false)
    }
    fn line_end(&self) -> usize {
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let end = display.text[offset..]
            .find('\n')
            .map_or(display.text.len(), |index| offset + index);
        display.original_offset(end, true)
    }
    fn word_left(&mut self) {
        self.typing_word = false;
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let start = display.text[..offset]
            .rfind('\n')
            .map_or(0, |index| index + 1);
        let next = if offset == start {
            offset.saturating_sub(1)
        } else {
            start + word_boundary(&display.text[start..offset], true)
        };
        self.cursor = display.original_offset(next, false);
    }
    fn word_right(&mut self) {
        self.typing_word = false;
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let end = display.text[offset..]
            .find('\n')
            .map_or(display.text.len(), |index| offset + index);
        let next = if offset == end {
            (end + 1).min(display.text.len())
        } else {
            offset + word_boundary(&display.text[offset..end], false)
        };
        self.cursor = display.original_offset(next, true);
    }
    fn remove(&mut self, range: Range<usize>, kill: Option<bool>) {
        if range.is_empty() {
            return;
        }
        let accumulate = self.last_kill;
        self.remember();
        if let Some(backward) = kill {
            let removed = &self.text[range.clone()];
            if accumulate && !self.kills.is_empty() {
                if backward {
                    self.kills[0].insert_str(0, removed);
                } else {
                    self.kills[0].push_str(removed);
                }
            } else {
                self.kills.insert(0, removed.into());
                self.kills.truncate(30);
            }
            self.last_kill = true;
        }
        self.edit(range.clone(), "");
        self.cursor = range.start;
    }
    pub fn backspace(&mut self) {
        let end = self.cursor;
        self.left();
        let start = self.cursor;
        self.cursor = end;
        self.remove(start..end, None);
    }
    pub fn delete(&mut self) {
        let start = self.cursor;
        self.right();
        let end = self.cursor;
        self.cursor = start;
        self.remove(start..end, None);
    }
    pub fn take(&mut self) -> String {
        self.remember();
        self.cursor = 0;
        self.collapsed.clear();
        self.paste_count = 0;
        std::mem::take(&mut self.text)
    }
    /// Returns false at the first/last logical line so the caller can browse history.
    pub fn vertical(&mut self, down: bool) -> bool {
        self.typing_word = false;
        self.last_kill = false;
        self.yank = None;
        let start = self.line_start();
        let end = self.line_end();
        let target = if down {
            if end == self.text.len() {
                return false;
            }
            end + 1
        } else {
            if start == 0 {
                return false;
            }
            self.text[..start - 1]
                .rfind('\n')
                .map_or(0, |index| index + 1)
        };
        let column = UnicodeWidthStr::width(&self.text[start..self.cursor]);
        let target_end = target
            + self.text[target..]
                .find('\n')
                .unwrap_or(self.text.len() - target);
        let mut used = 0;
        self.cursor = target;
        for grapheme in self.text[target..target_end].graphemes(true) {
            used += UnicodeWidthStr::width(grapheme);
            if used > column {
                break;
            }
            self.cursor += grapheme.len();
        }
        true
    }
    pub fn visible_rows(&mut self, width: usize, height: usize) -> (Vec<String>, usize, usize) {
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let layout = Layout::new(&display.text, width);
        let row = layout.row(offset);
        let column = layout.column(&display.text, row, offset).min(width - 1);
        let scroll_offset = self
            .scroll_offset
            .clamp(row.saturating_sub(height - 1), row)
            .min(layout.len().saturating_sub(height));
        let lines = layout
            .visible(&display.text, scroll_offset, height)
            .map(str::to_owned)
            .collect();
        self.scroll_offset = scroll_offset;
        (lines, row - scroll_offset, column)
    }

    /// False at a viewport boundary, where the caller may switch to history navigation.
    pub fn visual_move(&mut self, width: usize, delta: isize) -> bool {
        self.typing_word = false;
        self.last_kill = false;
        self.yank = None;
        let display = self.display();
        let offset = display.display_offset(self.cursor);
        let layout = Layout::new(&display.text, width);
        let row = layout.row(offset);
        let target = row.saturating_add_signed(delta).min(layout.len() - 1);
        if row == target {
            return false;
        }
        let column = self
            .preferred_column
            .unwrap_or_else(|| layout.column(&display.text, row, offset));
        let target = layout.cursor_at(&display.text, target, column);
        self.cursor = display.original_offset(target, delta > 0);
        self.preferred_column = Some(column);
        true
    }

    /// Arm a character jump, or consume the next printable key as its case-sensitive target.
    pub fn jump_key(&mut self, action: &str, key: KeyEvent) -> bool {
        let direction = match action {
            "tui.editor.jumpForward" => Some(true),
            "tui.editor.jumpBackward" => Some(false),
            _ => None,
        };
        if let Some(direction) = direction {
            self.typing_word = false;
            self.last_kill = false;
            self.yank = None;
            self.jump_forward = if self.jump_forward.is_some() {
                None
            } else {
                Some(direction)
            };
            return true;
        }
        let Some(forward) = self.jump_forward.take() else {
            return false;
        };
        self.typing_word = false;
        if let KeyCode::Char(character) = key.code
            && !character.is_control()
            && !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER)
        {
            let display = self.display();
            let cursor = display.display_offset(self.cursor);
            let mut glyphs = display.text.grapheme_indices(true);
            let matched = if forward {
                glyphs.find(|(offset, glyph)| *offset > cursor && glyph.contains(character))
            } else {
                glyphs.rfind(|(offset, glyph)| *offset < cursor && glyph.contains(character))
            };
            if let Some((offset, _)) = matched {
                self.cursor = display.original_offset(offset, forward);
            }
            self.preferred_column = None;
            self.yank = None;
            return true;
        }
        false
    }

    pub fn continued_line(&mut self) -> bool {
        if self.visible_neighbors().0 != Some('\\') {
            return false;
        }
        self.remember();
        self.edit(self.cursor - 1..self.cursor, "\n");
        true
    }

    pub fn action(&mut self, action: &str) -> bool {
        self.action_inner(action, InputMode::Text)
    }

    pub(super) fn search_action(&mut self, action: &str) -> bool {
        self.action_inner(action, InputMode::TranscriptSearch)
    }

    pub(super) fn settings_search_action(&mut self, action: &str) -> bool {
        self.action_inner(action, InputMode::SettingsSearch)
    }

    fn action_inner(&mut self, action: &str, mode: InputMode) -> bool {
        if !matches!(
            action,
            "tui.editor.deleteWordBackward"
                | "tui.editor.deleteWordForward"
                | "tui.editor.deleteToLineStart"
                | "tui.editor.deleteToLineEnd"
        ) {
            self.last_kill = false;
        }
        match action {
            "tui.editor.cursorLeft" => self.left(),
            "tui.editor.cursorRight" => self.right(),
            "tui.editor.cursorWordLeft" => self.word_left(),
            "tui.editor.cursorWordRight" => self.word_right(),
            "tui.editor.cursorLineStart" => self.cursor = self.line_start(),
            "tui.editor.cursorLineEnd" => self.cursor = self.line_end(),
            "tui.editor.deleteCharBackward" => self.backspace(),
            "tui.editor.deleteCharForward" => self.delete(),
            "tui.editor.deleteWordBackward" => {
                let end = self.cursor;
                if self.cursor == self.line_start() && self.cursor > 0 {
                    self.left();
                } else {
                    let line_start = self.line_start();
                    self.word_left();
                    self.cursor = self.cursor.max(line_start);
                }
                let start = self.cursor;
                self.cursor = end;
                self.remove(start..end, Some(true));
            }
            "tui.editor.deleteWordForward" => {
                let start = self.cursor;
                let line_end = self.line_end();
                if self.cursor == line_end && self.cursor < self.text.len() {
                    self.right();
                } else {
                    self.word_right();
                    self.cursor = self.cursor.min(line_end);
                }
                let end = self.cursor;
                self.cursor = start;
                self.remove(start..end, Some(false));
            }
            "tui.editor.deleteToLineStart" => {
                let start = self.line_start();
                self.remove(
                    if start == self.cursor {
                        start.saturating_sub(1)
                    } else {
                        start
                    }..self.cursor,
                    Some(true),
                );
            }
            "tui.editor.deleteToLineEnd" => {
                let end = self.line_end();
                self.remove(
                    self.cursor..if end == self.cursor && end < self.text.len() {
                        end + 1
                    } else {
                        end
                    },
                    Some(false),
                );
            }
            "tui.editor.yank" => {
                if let Some(text) = self.kills.first().cloned() {
                    let start = self.cursor;
                    match mode {
                        InputMode::Text => self.insert(&text),
                        InputMode::TranscriptSearch => self.insert_search(&text),
                        InputMode::SettingsSearch => self.insert_settings_search(&text),
                    }
                    self.yank = Some((start..self.cursor, 0));
                }
            }
            "tui.editor.yankPop" => {
                if let Some((range, _)) = self.yank.take().filter(|_| self.kills.len() > 1) {
                    self.remember();
                    self.kills.rotate_left(1);
                    let replacement = self.kills[0].clone();
                    self.edit(range.clone(), &replacement);
                    self.cursor = range.start + replacement.len();
                    self.yank = Some((range.start..self.cursor, 0));
                }
            }
            "tui.editor.undo" => {
                if let Some(snapshot) = self.undo.pop() {
                    self.text = snapshot.text;
                    self.cursor = snapshot.cursor;
                    self.collapsed = snapshot.collapsed;
                    self.paste_count = snapshot.paste_count;
                    self.yank = None;
                }
            }
            "tui.input.newLine" => self.insert("\n"),
            _ => return false,
        }
        self.typing_word = false;
        self.preferred_column = None;
        if !matches!(action, "tui.editor.yank" | "tui.editor.yankPop") {
            self.yank = None;
        }
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn type_text(editor: &mut Editor, text: &str) {
        for character in text.chars() {
            // The interactive loop first tries a keybinding for each key.
            assert!(!editor.action(""));
            editor.type_character(character);
        }
    }

    #[test]
    fn paste_collapses_long_draft_visually_and_keeps_source_for_reads_and_submit() {
        let mut editor = Editor::default();
        editor.replace("左右".into());
        editor.cursor = "左".len();
        let paste = std::iter::repeat_n("私", 11)
            .collect::<Vec<_>>()
            .join("\r\n");
        let normalized = paste.replace("\r\n", "\n");

        editor.paste(&paste);
        let (rows, _, column) = editor.visible_rows(80, 6);
        assert_eq!(rows, ["左[paste #1 +11 lines]右"]);
        assert_eq!(column, UnicodeWidthStr::width("左[paste #1 +11 lines]"));
        assert_eq!(editor.text, format!("左{normalized}右"));
        editor.left();
        assert_eq!(editor.cursor, "左".len());
        editor.right();
        assert_eq!(editor.cursor, "左".len() + normalized.len());
        assert_eq!(editor.take(), format!("左{normalized}右"));
        assert!(editor.text.is_empty());
        editor.action("tui.editor.undo");
        assert_eq!(editor.visible_rows(80, 6).0, ["左[paste #1 +11 lines]右"]);
    }

    #[test]
    fn paste_tokens_survive_neighboring_edits_undo_and_literal_marker_text() {
        let mut editor = Editor::default();
        editor.replace("beforeafter".into());
        editor.cursor = "before".len();
        let paste = "🙂".repeat(501);
        editor.paste(&paste);
        let end = editor.cursor;
        let marker = "[paste #1 1002 chars]";
        assert_eq!(
            editor.visible_rows(80, 6).0,
            [format!("before{marker}after")]
        );

        editor.replace_range(0..6, "pre");
        editor.left();
        editor.insert(marker);
        assert_eq!(
            editor.visible_rows(80, 6).0,
            [format!("pr{marker}e{marker}after")]
        );
        assert_eq!(editor.text, format!("pr{marker}e{paste}after"));

        editor.action("tui.editor.undo");
        editor.action("tui.editor.undo");
        assert_eq!(editor.cursor, end);
        editor.backspace();
        assert_eq!(editor.text, "beforeafter");
        editor.action("tui.editor.undo");
        assert_eq!(
            editor.visible_rows(80, 6).0,
            [format!("before{marker}after")]
        );
        editor.action("tui.editor.undo");
        assert_eq!(editor.visible_rows(80, 6).0, ["beforeafter"]);
    }

    #[test]
    fn paste_editing_uses_marker_boundaries_instead_of_hidden_characters() {
        let mut editor = Editor::default();
        editor.paste(&format!("{}\\", "z".repeat(1001)));
        assert!(!editor.continued_line());
        assert_eq!(editor.text.as_bytes().last(), Some(&b'\\'));

        editor.cursor = 0;
        assert!(editor.jump_key(
            "tui.editor.jumpForward",
            KeyEvent::new(KeyCode::Char(']'), KeyModifiers::CONTROL)
        ));
        assert!(editor.jump_key("", KeyEvent::new(KeyCode::Char('z'), KeyModifiers::NONE)));
        assert_eq!(editor.cursor, 0);

        editor.right();
        let paste_end = editor.cursor;
        assert_eq!(editor.visible_neighbors().0, Some(']'));
        editor.paste("/tmp");
        assert_eq!(&editor.text[paste_end..], "/tmp");
    }

    #[test]
    fn paste_normalizes_line_endings_and_tabs_and_separates_paths() {
        let mut editor = Editor::default();
        editor.insert("see");
        editor.paste("/tmp/file\r\t♡\r\nnext\x1b[106;5ulast\x1b");
        assert_eq!(editor.text, "see /tmp/file\n    ♡\nnext\nlast");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "see");
    }

    #[test]
    fn word_navigation_matches_punctuation_and_line_boundaries() {
        let mut editor = Editor::default();
        editor.replace("foo...bar/baz\n次の行".into());
        editor.cursor = 0;
        for position in [3, 6, 9, 10, 13, 14] {
            editor.action("tui.editor.cursorWordRight");
            assert_eq!(editor.cursor, position);
        }
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(editor.cursor, 13);
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(editor.cursor, 10);
    }

    #[test]
    fn word_navigation_handles_cjk_and_combining_text() {
        let mut editor = Editor::default();
        editor.replace("你好世界 test e\u{301}lan".into());
        editor.cursor = 0;
        editor.action("tui.editor.cursorWordRight");
        assert_eq!(&editor.text[..editor.cursor], "你好");
        editor.action("tui.editor.cursorWordRight");
        assert_eq!(&editor.text[..editor.cursor], "你好世界");
        editor.cursor = editor.text.len();
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(&editor.text[editor.cursor..], "e\u{301}lan");
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "你好世界 e\u{301}lan");
    }

    #[test]
    fn non_word_numeric_symbols_move_and_delete_with_adjacent_emoji() {
        let mut editor = Editor::default();
        editor.replace("😀²abc".into());
        editor.cursor = 0;
        editor.action("tui.editor.cursorWordRight");
        assert_eq!(editor.cursor, "😀²".len());
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(editor.cursor, 0);
        editor.action("tui.editor.deleteWordForward");
        assert_eq!(editor.text, "abc");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "😀²abc");
        editor.cursor = "😀²".len();
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "abc");

        editor.replace("שלום test".into());
        editor.cursor = 0;
        editor.action("tui.editor.cursorWordRight");
        assert_eq!(editor.cursor, "שלום".len());
    }

    #[test]
    fn zwj_emoji_segments_remain_non_word_with_hebrew_fallback() {
        let mut editor = Editor::default();
        for text in ["a‍😀!b", "א‍😀!b"] {
            editor.replace(text.into());
            editor.cursor = 0;
            editor.action("tui.editor.cursorWordRight");
            assert_eq!(&editor.text[..editor.cursor], &text[..text.len() - 1]);
            editor.action("tui.editor.cursorWordLeft");
            assert_eq!(editor.cursor, 0);
            editor.action("tui.editor.deleteWordForward");
            assert_eq!(editor.text, "b");
            editor.action("tui.editor.undo");
            editor.cursor = text.len() - 1;
            editor.action("tui.editor.deleteWordBackward");
            assert_eq!(editor.text, "b");
        }
        editor.replace("אב'".into());
        editor.cursor = 0;
        editor.action("tui.editor.cursorWordRight");
        assert_eq!(&editor.text[..editor.cursor], "אב");
    }

    #[test]
    fn backward_word_actions_advance_and_preserve_punctuation_graphemes() {
        let mut editor = Editor::default();
        editor.replace("אב'".into());
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(&editor.text[..editor.cursor], "אב");
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(editor.cursor, 0);
        editor.cursor = editor.text.len();
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "אב");
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "");

        editor.replace("a.\u{301}b".into());
        editor.action("tui.editor.cursorWordLeft");
        assert_eq!(&editor.text[..editor.cursor], "a.\u{301}");
        editor.cursor = editor.text.len();
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "a.\u{301}");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "a.\u{301}b");
    }

    #[test]
    fn word_deletion_uses_punctuation_and_preserves_undo() {
        let mut editor = Editor::default();
        editor.replace("foo.bar".into());
        editor.cursor = editor.text.len();
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "foo.");
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "foo");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "foo.bar");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "foo");
    }

    #[test]
    fn typing_undo_groups_words_and_individual_spaces() {
        let mut editor = Editor::default();
        type_text(&mut editor, "hello world  ");
        for expected in ["hello world ", "hello world", "hello", ""] {
            assert!(editor.action("tui.editor.undo"));
            assert_eq!(editor.text, expected);
            assert_eq!(editor.cursor, expected.len());
        }
    }

    #[test]
    fn typing_undo_respects_newlines_movement_and_atomic_insertion() {
        let mut editor = Editor::default();
        type_text(&mut editor, "hello");
        editor.action("tui.input.newLine");
        type_text(&mut editor, "world");
        for expected in ["hello\n", "hello", ""] {
            editor.action("tui.editor.undo");
            assert_eq!(editor.text, expected);
        }

        type_text(&mut editor, "abc");
        editor.action("tui.editor.cursorLeft");
        type_text(&mut editor, "XY");
        assert_eq!(editor.text, "abXYc");
        editor.action("tui.editor.undo");
        assert_eq!((&*editor.text, editor.cursor), ("abc", 2));
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "");

        type_text(&mut editor, "before");
        editor.insert(" pasted text");
        type_text(&mut editor, "after");
        for expected in ["before pasted text", "before", ""] {
            editor.action("tui.editor.undo");
            assert_eq!(editor.text, expected);
        }
    }

    #[test]
    fn typing_undo_preserves_unicode_and_resets_after_navigation() {
        let mut editor = Editor::default();
        type_text(&mut editor, "日本\u{00a0}e\u{301}👩\u{200d}💻");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "日本");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "");

        type_text(&mut editor, "ab");
        assert!(!editor.visual_move(80, -1));
        type_text(&mut editor, "c");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "ab");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "");
    }
    #[test]
    fn consecutive_kills_accumulate_in_direction_and_across_lines() {
        let mut editor = Editor::default();
        editor.insert("one two\n三 four");
        for _ in 0..4 {
            editor.action("tui.editor.deleteWordBackward");
        }
        assert_eq!(editor.text, "one ");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "one two\n三 four");

        editor.action("tui.editor.cursorLineStart");
        editor.action("tui.editor.deleteToLineStart");
        editor.action("tui.editor.deleteToLineStart");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "one two\n三 four");

        editor.cursor = 0;
        editor.action("tui.editor.cursorLineStart");
        editor.action("tui.editor.deleteToLineEnd");
        editor.action("tui.editor.deleteToLineEnd");
        editor.action("tui.editor.deleteToLineEnd");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "one two\n三 four");
    }

    #[test]
    fn kill_ring_limits_separate_entries_but_accumulates_consecutive_kills() {
        let mut editor = Editor::default();
        for index in 0..35 {
            editor.replace(format!("entry{index}"));
            editor.action("tui.editor.deleteToLineStart");
        }
        assert_eq!(editor.kills.len(), 30);
        assert_eq!(editor.kills.first().unwrap(), "entry34");
        assert_eq!(editor.kills.last().unwrap(), "entry5");

        editor.replace("joined tail".into());
        editor.action("tui.editor.deleteWordBackward");
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.kills.len(), 30);
        assert_eq!(editor.kills.first().unwrap(), "joined tail");
        assert_eq!(editor.kills.last().unwrap(), "entry6");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "joined tail");
    }

    #[test]
    fn undo_word_deletion_restores_original_cursor_in_both_directions() {
        for (action, cursor, result) in [
            (
                "tui.editor.deleteWordBackward",
                "first\n".len(),
                "firstsecond",
            ),
            ("tui.editor.deleteWordForward", "first".len(), "firstsecond"),
            (
                "tui.editor.deleteWordBackward",
                "first\nsecond".len(),
                "first\n",
            ),
            ("tui.editor.deleteWordForward", 0, "\nsecond"),
        ] {
            let mut editor = Editor::default();
            editor.insert("first\nsecond");
            editor.cursor = cursor;
            editor.action(action);
            assert_eq!(editor.text, result, "{action}");
            editor.action("tui.editor.undo");
            assert_eq!(editor.text, "first\nsecond", "{action}");
            assert_eq!(editor.cursor, cursor, "{action}");
        }
    }

    #[test]
    fn undo_character_deletion_restores_original_cursor() {
        for (action, cursor) in [
            ("tui.editor.deleteCharBackward", 1),
            ("tui.editor.deleteCharForward", 0),
        ] {
            let mut editor = Editor::default();
            editor.insert("ab");
            editor.cursor = cursor;
            editor.action(action);
            editor.action("tui.editor.undo");
            assert_eq!(editor.text, "ab");
            assert_eq!(editor.cursor, cursor, "{action}");
        }
    }

    #[test]
    fn interrupted_kills_rotate_yanks_and_yank_pop_is_undoable() {
        let mut editor = Editor::default();
        editor.insert("alpha beta gamma");
        editor.action("tui.editor.deleteWordBackward");
        editor.action("tui.editor.cursorLeft");
        editor.action("tui.editor.cursorRight");
        editor.action("tui.editor.deleteWordBackward");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "alpha beta ");
        editor.action("tui.editor.yankPop");
        assert_eq!(editor.text, "alpha gamma");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "alpha beta ");
        // Pi rotates the ring itself, so a later yank uses the rotated entry.
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "alpha beta gamma");
    }

    #[test]
    fn jumps_across_lines_without_inserting_the_target() {
        let mut editor = Editor::default();
        editor.insert("最初\n東京 京");
        let key = KeyEvent::new(KeyCode::Char('京'), KeyModifiers::NONE);
        editor.jump_key("tui.editor.jumpBackward", key);
        assert!(editor.jump_key("", key));
        assert_eq!(editor.cursor, "最初\n東京 ".len());
        editor.jump_key("tui.editor.jumpBackward", key);
        assert!(editor.jump_key("", key));
        assert_eq!(editor.cursor, "最初\n東".len());
        editor.jump_key("tui.editor.jumpForward", key);
        editor.jump_key("", key);
        assert_eq!(editor.cursor, "最初\n東京 ".len());
        assert_eq!(editor.text, "最初\n東京 京");

        editor.insert("\\");
        assert!(editor.continued_line());
        assert_eq!(editor.text, "最初\n東京 \n京");
        assert!(editor.action("tui.editor.undo"));
        assert_eq!(editor.text, "最初\n東京 \\京");
    }

    #[test]
    fn kill_yank_undo_and_multiline_movement_preserve_unicode() {
        let mut editor = Editor::default();
        editor.insert("日本\nhello world");
        editor.action("tui.editor.deleteWordBackward");
        assert_eq!(editor.text, "日本\nhello ");
        editor.action("tui.editor.yank");
        assert_eq!(editor.text, "日本\nhello world");
        editor.action("tui.editor.undo");
        assert_eq!(editor.text, "日本\nhello ");
        assert!(editor.vertical(false));
        assert_eq!(editor.cursor, "日本".len());
        editor.action("tui.editor.cursorLineStart");
        assert_eq!(editor.cursor, 0);
    }
}
