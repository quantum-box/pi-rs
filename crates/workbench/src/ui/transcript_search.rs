//! Literal, case-insensitive search over the rendered fullscreen transcript.
use std::ops::Range;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::render::{Line, Span};
use super::{editor::Editor, keys::Keys, theme::Theme};

#[derive(Debug, PartialEq, Eq)]
struct Segment {
    row: usize,
    bytes: Range<usize>,
}

type Match = Vec<Segment>;

#[derive(Default)]
pub struct Search {
    editor: Editor,
    rows: Vec<String>,
    query: String,
    matches: Vec<Match>,
    selected: usize,
    direction: Option<bool>,
}

impl Search {
    pub fn input(&mut self, key: KeyEvent, keys: &Keys) {
        let action = keys.action(key, false).unwrap_or("");
        if action == "tui.input.newLine" || self.editor.action(action) {
            return;
        }
        if let KeyCode::Char(character) = key.code
            && !character.is_control()
            && !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER)
            && self.editor.text.len() + character.len_utf8() <= 4096
        {
            self.editor.type_character(character);
        }
    }

    pub fn paste(&mut self, text: &str) {
        let remaining = 4096usize.saturating_sub(self.editor.text.len());
        let mut end = remaining.min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let text = &text[..end];
        self.editor
            .insert(&text.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    pub fn overlay(&self, theme: &Theme, keys: &Keys, width: usize) -> (Vec<Line>, usize, usize) {
        let border = theme.color("border");
        if width < 4 {
            return (vec![Line::plain("⌕", border)], 0, 0);
        }
        let inner = width - 2;
        let result = self.result();
        let result = if !result.is_empty() && result.width() + 4 < inner {
            format!(" {result} ")
        } else {
            String::new()
        };
        let input_width = inner - result.width();
        let cursor_prefix = &self.editor.text[..self.editor.cursor];
        let mut cursor = 0;
        let mut offset = cursor_prefix.len();
        for (start, glyph) in cursor_prefix.grapheme_indices(true).rev() {
            let size = glyph.width();
            if cursor + size >= input_width {
                break;
            }
            cursor += size;
            offset = start;
        }
        let input = if self.editor.text.is_empty() {
            "Find in transcript"
        } else {
            &self.editor.text[offset..]
        };
        let input = super::render::wrap(
            &[Span::themed(
                input,
                theme,
                if self.editor.text.is_empty() {
                    "dim"
                } else {
                    "text"
                },
            )],
            input_width,
        )
        .into_iter()
        .next()
        .expect("rendered input line");
        let used = input
            .spans
            .iter()
            .map(|span| span.text.width())
            .sum::<usize>();
        let mut middle = Line::plain("│", border);
        middle.spans.extend(input.spans);
        middle
            .spans
            .push(Span::new(" ".repeat(input_width - used), border));
        middle.spans.push(Span::themed(result, theme, "muted"));
        middle.spans.push(Span::new("│", border));
        let controls = format!(
            " ↑ {} · ↓ {} ",
            keys.hint("tui.altScreen.searchPrevious"),
            keys.hint("tui.altScreen.searchNext")
        );
        let controls = if controls.width() < inner {
            controls
        } else {
            String::new()
        };
        (
            vec![
                Line::plain(&format!("┌{}┐", "─".repeat(inner)), border),
                middle,
                Line::plain(
                    &format!("└{}{controls}┘", "─".repeat(inner - controls.width())),
                    border,
                ),
            ],
            1,
            cursor + 1,
        )
    }

    pub fn navigate(&mut self, forward: bool) {
        self.direction = Some(forward);
    }

    /// Return the row to reveal after a query change or explicit navigation.
    pub fn update(&mut self, lines: &[Line], top: usize) -> Option<Range<usize>> {
        let query = self
            .editor
            .text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let query_changed = query != self.query;
        let source_changed = self.rows.len() != lines.len()
            || self.rows.iter().zip(lines).any(|(text, line)| {
                text.bytes()
                    .ne(line.spans.iter().flat_map(|span| span.text.bytes()))
            });
        let previous = self.matches.get(self.selected);
        let anchor = previous.map_or(top, |found| found[0].row);

        if source_changed {
            self.rows = lines
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
        }
        if source_changed || query_changed {
            let matches = find_matches(&self.rows, &query);
            self.selected = (!query_changed)
                .then(|| matches.iter().position(|found| Some(found) == previous))
                .flatten()
                .unwrap_or_else(|| {
                    let index = matches.partition_point(|found| found[0].row < anchor);
                    if index == matches.len() { 0 } else { index }
                });
            self.matches = matches;
            self.query = query;
        }
        let direction = self.direction.take();
        if self.matches.is_empty() {
            return None;
        }
        if let Some(forward) = direction {
            let count = self.matches.len();
            self.selected = (self.selected + if forward { 1 } else { count - 1 }) % count;
        }
        (query_changed || direction.is_some()).then(|| {
            let found = &self.matches[self.selected];
            found[0].row..found.last().expect("nonempty match").row + 1
        })
    }

    pub fn result(&self) -> String {
        if self.query.is_empty() {
            String::new()
        } else if self.matches.is_empty() {
            "No matches".into()
        } else {
            format!("{}/{}", self.selected + 1, self.matches.len())
        }
    }

    pub fn highlight(&self, lines: &mut [Line], visible: Range<usize>, theme: &Theme) {
        for (index, found) in self.matches.iter().enumerate() {
            let current = index == self.selected;
            for segment in found
                .iter()
                .filter(|segment| visible.contains(&segment.row))
            {
                lines[segment.row].style_bytes(segment.bytes.clone(), |span| {
                    span.background = Some(theme.color("searchMatchBg"));
                    span.color = theme.color("searchMatchText");
                    span.dim = theme.is_dim("searchMatchText");
                    span.inverse |= current;
                    span.bold |= current;
                    span.underline |= !current;
                });
            }
        }
    }
}

struct Source {
    corpus: Range<usize>,
    row: usize,
    offset: usize,
}

fn find_matches(rows: &[String], query: &str) -> Vec<Match> {
    if query.is_empty() {
        return vec![];
    }
    let mut corpus = String::new();
    let mut source = Vec::new();
    for (row, text) in rows.iter().enumerate() {
        let mut offset = 0;
        for word in text.split_whitespace() {
            offset += text[offset..].find(word).expect("word from source line");
            if !corpus.is_empty() {
                corpus.push(' ');
            }
            let start = corpus.len();
            corpus.push_str(word);
            source.push(Source {
                corpus: start..corpus.len(),
                row,
                offset,
            });
            offset += word.len();
        }
    }
    let pattern = regex::RegexBuilder::new(&regex::escape(query))
        .case_insensitive(true)
        .build();
    let Ok(pattern) = pattern else {
        return vec![];
    };
    let mut first = 0;
    pattern
        .find_iter(&corpus)
        .map(|found| {
            while source[first].corpus.end <= found.start() {
                first += 1;
            }
            source[first..]
                .iter()
                .take_while(|part| part.corpus.start < found.end())
                .map(|part| {
                    let start = part.offset + found.start().saturating_sub(part.corpus.start);
                    let end = part.offset + found.end().min(part.corpus.end) - part.corpus.start;
                    // Expand a partial match inside a combining sequence to its whole glyph.
                    let text = &rows[part.row];
                    let bytes = if text.is_ascii() {
                        start..end
                    } else {
                        let mut glyphs = text
                            .grapheme_indices(true)
                            .skip_while(|(offset, glyph)| offset + glyph.len() <= start);
                        let (first, glyph) = glyphs.next().expect("match intersects a grapheme");
                        let last = glyphs
                            .take_while(|(offset, _)| *offset < end)
                            .last()
                            .unwrap_or((first, glyph));
                        first..last.0 + last.1.len()
                    };
                    Segment {
                        row: part.row,
                        bytes,
                    }
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::style::Color;

    #[test]
    fn finds_a_literal_across_wraps_and_highlights_entire_graphemes() {
        let mut lines = [
            Line::plain("  A. Café\u{301}", Color::Reset),
            Line::plain("   東京  ", Color::Reset),
            Line::plain("A. Café\u{301} 東京", Color::Reset),
        ];
        let mut search = Search::default();
        search.editor.replace("a. café\u{301}  東京".into());
        assert_eq!(search.update(&lines, 1), Some(2..3));
        assert_eq!(search.result(), "2/2");
        search.navigate(true);
        assert_eq!(search.update(&lines, 2), Some(0..2));
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        search.highlight(&mut lines, 0..2, &theme);
        assert!(
            lines[0]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "Café\u{301}")
        );
        assert!(
            lines[1]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "東京")
        );
        assert!(lines[2].spans.iter().all(|span| !span.inverse));
    }
}
