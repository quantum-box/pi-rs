//! Rendered editor rows mapped back to the original UTF-8 byte positions.
use std::ops::Range;

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

pub struct Layout {
    rows: Vec<Range<usize>>,
}

impl Layout {
    pub fn new(text: &str, width: usize) -> Self {
        let mut rows = Vec::new();
        let mut offset = 0;
        for line in text.split('\n') {
            let mut start = 0;
            let mut used = 0;
            let mut opportunity = None;
            let mut glyphs = line.grapheme_indices(true).peekable();
            while let Some((index, glyph)) = glyphs.next() {
                let size = display_width(glyph);
                if used + size > width && start < index {
                    let (end, preceding) = opportunity
                        .filter(|(_, preceding)| used - preceding + size <= width)
                        .unwrap_or((index, used));
                    rows.push(offset + start..offset + end);
                    start = end;
                    used -= preceding;
                    opportunity = None;
                }
                used += size;
                if let Some(&(next_index, next)) = glyphs.peek()
                    && !next.chars().all(char::is_whitespace)
                    && (glyph.chars().all(char::is_whitespace) || cjk(glyph) || cjk(next))
                {
                    opportunity = Some((next_index, used));
                }
            }
            rows.push(offset + start..offset + line.len());
            offset += line.len() + 1;
        }
        Self { rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn row(&self, cursor: usize) -> usize {
        self.rows
            .partition_point(|row| row.start <= cursor)
            .saturating_sub(1)
    }

    pub fn column(&self, text: &str, row: usize, cursor: usize) -> usize {
        display_width(&text[self.rows[row].start..cursor])
    }

    pub fn cursor_at(&self, text: &str, row: usize, column: usize) -> usize {
        let range = &self.rows[row];
        let wraps = self
            .rows
            .get(row + 1)
            .is_some_and(|next| next.start == range.end);
        let mut used = 0;
        let mut cursor = range.start;
        for (offset, glyph) in text[range.clone()].grapheme_indices(true) {
            used += display_width(glyph);
            let end = range.start + offset + glyph.len();
            if used > column || (wraps && end == range.end) {
                break;
            }
            cursor = end;
        }
        cursor
    }

    pub fn visible<'a>(
        &'a self,
        text: &'a str,
        start: usize,
        height: usize,
    ) -> impl Iterator<Item = &'a str> {
        self.rows
            .iter()
            .skip(start)
            .take(height)
            .map(|range| &text[range.clone()])
    }
}

fn display_width(text: &str) -> usize {
    if text.contains('\t') {
        return text.split('\t').map(UnicodeWidthStr::width).sum::<usize>()
            + text.bytes().filter(|byte| *byte == b'\t').count() * 4;
    }
    text.width()
}

fn cjk(glyph: &str) -> bool {
    glyph.starts_with(|c: char| matches!(c as u32, 0x2e80..=0x9fff | 0xac00..=0xd7ff | 0xf900..=0xfaff | 0x20000..=0x323af))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_at_words_and_keeps_utf8_cursor_positions_in_the_visual_row() {
        let text = "abc defghi 日本語\n\nend";
        let layout = Layout::new(text, 8);
        assert_eq!(
            layout.visible(text, 0, 10).collect::<Vec<_>>(),
            ["abc ", "defghi ", "日本語", "", "end"]
        );
        assert_eq!(layout.row(4), 1);
        let japanese = text.find("日本").unwrap();
        assert_eq!(layout.row(japanese), 2);
        assert_eq!(layout.cursor_at(text, 2, 3), japanese + '日'.len_utf8());
        assert_eq!(layout.cursor_at(text, 0, 9), 3);
        assert_eq!(layout.column(text, 2, japanese + '日'.len_utf8()), 2);
    }
}
