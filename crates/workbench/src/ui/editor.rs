//! Grapheme-safe editor with line movement, a kill ring, and undo snapshots.
use std::ops::Range;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::editor_layout::Layout;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Clone)]
struct Snapshot {
    text: String,
    cursor: usize,
}
#[derive(Default)]
pub struct Editor {
    pub text: String,
    pub cursor: usize,
    undo: Vec<Snapshot>,
    kills: Vec<String>,
    last_kill: bool,
    yank: Option<(Range<usize>, usize)>,
    preferred_column: Option<usize>,
    scroll_offset: usize,
    jump_forward: Option<bool>,
}
impl Editor {
    fn remember(&mut self) {
        self.undo.push(Snapshot {
            text: self.text.clone(),
            cursor: self.cursor,
        });
        if self.undo.len() > 100 {
            self.undo.remove(0);
        }
        self.yank = None;
        self.last_kill = false;
        self.preferred_column = None;
        self.jump_forward = None;
    }
    pub fn replace(&mut self, text: String) {
        self.remember();
        self.text = text;
        self.cursor = self.text.len();
    }
    pub fn insert(&mut self, text: &str) {
        self.remember();
        let text = super::render::clean(text);
        self.text.insert_str(self.cursor, &text);
        self.cursor += text.len();
    }
    pub fn left(&mut self) {
        self.cursor = self.text[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(index, _)| index);
    }
    pub fn right(&mut self) {
        self.cursor += self.text[self.cursor..]
            .graphemes(true)
            .next()
            .map_or(0, str::len);
    }
    pub fn line_start(&self) -> usize {
        self.text[..self.cursor]
            .rfind('\n')
            .map_or(0, |index| index + 1)
    }
    fn line_end(&self) -> usize {
        self.cursor
            + self.text[self.cursor..]
                .find('\n')
                .unwrap_or(self.text.len() - self.cursor)
    }
    fn word_left(&mut self) {
        while self.cursor > 0
            && self.text[..self.cursor]
                .chars()
                .next_back()
                .is_some_and(char::is_whitespace)
        {
            self.left();
        }
        while self.cursor > 0
            && self.text[..self.cursor]
                .chars()
                .next_back()
                .is_some_and(|c| !c.is_whitespace())
        {
            self.left();
        }
    }
    fn word_right(&mut self) {
        while self.cursor < self.text.len()
            && self.text[self.cursor..]
                .chars()
                .next()
                .is_some_and(char::is_whitespace)
        {
            self.right();
        }
        while self.cursor < self.text.len()
            && self.text[self.cursor..]
                .chars()
                .next()
                .is_some_and(|c| !c.is_whitespace())
        {
            self.right();
        }
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
            }
            self.last_kill = true;
        }
        self.text.replace_range(range.clone(), "");
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
        std::mem::take(&mut self.text)
    }
    /// Returns false at the first/last logical line so the caller can browse history.
    pub fn vertical(&mut self, down: bool) -> bool {
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
        let layout = Layout::new(&self.text, width);
        let row = layout.row(self.cursor);
        let column = layout.column(&self.text, row, self.cursor).min(width - 1);
        self.scroll_offset = self
            .scroll_offset
            .clamp(row.saturating_sub(height - 1), row)
            .min(layout.len().saturating_sub(height));
        let lines = layout
            .visible(&self.text, self.scroll_offset, height)
            .map(str::to_owned)
            .collect();
        (lines, row - self.scroll_offset, column)
    }

    /// False at a viewport boundary, where the caller may switch to history navigation.
    pub fn visual_move(&mut self, width: usize, delta: isize) -> bool {
        self.last_kill = false;
        self.yank = None;
        let layout = Layout::new(&self.text, width);
        let row = layout.row(self.cursor);
        let target = row.saturating_add_signed(delta).min(layout.len() - 1);
        if row == target {
            return false;
        }
        let column = *self
            .preferred_column
            .get_or_insert_with(|| layout.column(&self.text, row, self.cursor));
        self.cursor = layout.cursor_at(&self.text, target, column);
        self.yank = None;
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
        if let KeyCode::Char(character) = key.code
            && !character.is_control()
            && !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER)
        {
            let mut glyphs = self.text.grapheme_indices(true);
            let matched = if forward {
                glyphs.find(|(offset, glyph)| *offset > self.cursor && glyph.contains(character))
            } else {
                glyphs.rfind(|(offset, glyph)| *offset < self.cursor && glyph.contains(character))
            };
            if let Some((offset, _)) = matched {
                self.cursor = offset;
            }
            self.preferred_column = None;
            self.yank = None;
            return true;
        }
        false
    }

    pub fn continued_line(&mut self) -> bool {
        if !self.text[..self.cursor].ends_with('\\') {
            return false;
        }
        self.remember();
        self.text.replace_range(self.cursor - 1..self.cursor, "\n");
        true
    }

    pub fn action(&mut self, action: &str) -> bool {
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
                    self.insert(&text);
                    self.yank = Some((start..self.cursor, 0));
                }
            }
            "tui.editor.yankPop" => {
                if let Some((range, _)) = self.yank.take().filter(|_| self.kills.len() > 1) {
                    self.remember();
                    self.kills.rotate_left(1);
                    self.text.replace_range(range.clone(), &self.kills[0]);
                    self.cursor = range.start + self.kills[0].len();
                    self.yank = Some((range.start..self.cursor, 0));
                }
            }
            "tui.editor.undo" => {
                if let Some(snapshot) = self.undo.pop() {
                    self.text = snapshot.text;
                    self.cursor = snapshot.cursor;
                    self.yank = None;
                }
            }
            "tui.input.newLine" => self.insert("\n"),
            _ => return false,
        }
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
