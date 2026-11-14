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
        if action == "tui.input.newLine" || self.editor.search_action(action) {
            return;
        }
        if let KeyCode::Char(character) = key.code
            && (!character.is_control() || character == '\u{85}')
            && !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER)
            && self.editor.text.len() + character.len_utf8() <= 4096
        {
            if character == '\u{85}' {
                self.editor.insert_search("\u{85}");
            } else {
                self.editor.type_character(character);
            }
        }
    }

    pub fn paste(&mut self, text: &str) {
        let remaining = 4096usize.saturating_sub(self.editor.text.len());
        // Match the single-line input's paste behavior; normalize the complete
        // query during search so pasted boundary spaces are not discarded. Cap
        // after sanitization and tab expansion, without splitting UTF-8.
        let mut sanitized = String::with_capacity(remaining.min(text.len()));
        for character in text.chars() {
            if character == '\t' {
                let count = 4.min(remaining.saturating_sub(sanitized.len()));
                sanitized.extend(std::iter::repeat_n(' ', count));
                if count < 4 {
                    break;
                }
            } else if character.is_control() && character != '\u{85}' {
                continue;
            } else if character.len_utf8() <= remaining.saturating_sub(sanitized.len()) {
                sanitized.push(character);
            } else {
                break;
            }
        }
        self.editor.insert_search(&sanitized);
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
            let size = super::render::search_display_width(glyph);
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
        let query = normalize_whitespace(&self.editor.text);
        let query_changed = query != self.query;
        let source_changed = self.rows.len() != lines.len()
            || self.rows.iter().zip(lines).any(|(text, line)| {
                text.bytes()
                    .ne(line.spans.iter().flat_map(|span| span.searchable().bytes()))
            });
        let previous = self.matches.get(self.selected);
        let anchor = previous.map_or(top, |found| found[0].row);

        if source_changed {
            self.rows = lines
                .iter()
                .map(|line| line.spans.iter().map(|span| span.searchable()).collect())
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

// ECMAScript's \s includes U+FEFF and excludes U+0085, unlike Rust's
// Unicode White_Space predicate. Pi uses this rule for its search query and
// for deciding whether a rendered grapheme separates corpus runs.
fn is_js_whitespace(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

fn normalize_whitespace(value: &str) -> String {
    value
        .split(is_js_whitespace)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn find_matches(rows: &[String], query: &str) -> Vec<Match> {
    if query.is_empty() {
        return vec![];
    }
    let mut corpus = String::new();
    let mut source = Vec::new();
    let controls: Vec<Vec<usize>> = rows
        .iter()
        .map(|row| {
            row.match_indices('\u{85}')
                .map(|(offset, _)| offset)
                .collect()
        })
        .collect();
    for (row, text) in rows.iter().enumerate() {
        let mut append = |offset: usize, word: &str| {
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
        };
        if text.is_ascii() {
            let mut offset = 0;
            for word in text.split(is_js_whitespace).filter(|word| !word.is_empty()) {
                offset += text[offset..].find(word).expect("word from source line");
                append(offset, word);
                offset += word.len();
            }
        } else {
            let mut start = None;
            for (offset, glyph) in text.grapheme_indices(true) {
                if glyph.chars().all(is_js_whitespace) {
                    if let Some(start) = start.take() {
                        append(start, &text[start..offset]);
                    }
                } else if start.is_none() {
                    start = Some(offset);
                }
            }
            if let Some(start) = start {
                append(start, &text[start..]);
            }
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
                    // Search-only U+0085 contributes no bytes to the terminal
                    // span. Empty ranges still identify a navigable row.
                    let display_offset = |offset: usize| {
                        offset
                            - controls[part.row].partition_point(|position| *position < offset)
                                * '\u{85}'.len_utf8()
                    };
                    Segment {
                        row: part.row,
                        bytes: display_offset(bytes.start)..display_offset(bytes.end),
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
    fn search_uses_ecmascript_whitespace_for_queries_and_corpus() {
        assert_eq!(normalize_whitespace("  a\u{feff}b  "), "a b");
        assert_eq!(normalize_whitespace("a\u{85}b"), "a\u{85}b");
        let rows = vec!["a\u{feff}b".into(), "a\u{85}b".into()];
        let matches = find_matches(&rows, &normalize_whitespace("a b"));
        assert_eq!(matches.len(), 1);
        assert!(matches[0].iter().all(|segment| segment.row == 0));
        let matches = find_matches(&rows, &normalize_whitespace("a\u{85}b"));
        assert_eq!(matches.len(), 1);
        assert!(matches[0].iter().all(|segment| segment.row == 1));
        let mut search = Search::default();
        search.paste("a\u{feff}b\u{85}c");
        assert_eq!(search.editor.text, "a\u{feff}b\u{85}c");
        let rendered = Line::plain("a\u{85}b", Color::Reset);
        assert_eq!(rendered.spans[0].text, "ab");
        assert_eq!(rendered.spans[0].searchable(), "a\u{85}b");
    }

    #[test]
    fn pasted_separator_is_preserved_between_existing_query_text() {
        let mut search = Search::default();
        search.editor.replace("ab".into());
        search.editor.cursor = 1;
        search.paste("\u{feff}");
        assert_eq!(search.editor.text, "a\u{feff}b");
        let lines = [Line::plain("a b", Color::Reset)];
        assert_eq!(search.update(&lines, 0), Some(0..1));
        assert_eq!(search.query, "a b");
        assert_eq!(search.result(), "1/1");
    }

    #[test]
    fn search_only_control_is_navigable_and_never_drawn() {
        let mut lines = [
            Line::plain("a\u{85}b", Color::Reset),
            Line::plain("x\u{85}y", Color::Reset),
        ];
        let mut search = Search::default();
        search.paste("\u{85}");
        assert_eq!(search.update(&lines, 0), Some(0..1));
        assert_eq!(search.result(), "1/2");
        assert_eq!(search.matches[0][0].bytes, 1..1);
        search.navigate(true);
        assert_eq!(search.update(&lines, 0), Some(1..2));
        assert_eq!(search.result(), "2/2");
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        search.highlight(&mut lines, 0..2, &theme);
        let mut bytes = Vec::new();
        for line in &lines {
            line.draw(&mut bytes, 20).unwrap();
        }
        assert!(!bytes.windows(2).any(|pair| pair == "\u{85}".as_bytes()));
        let (overlay, _, _) = search.overlay(&theme, &Keys::default(), 30);
        let mut bytes = Vec::new();
        for line in overlay {
            line.draw(&mut bytes, 30).unwrap();
        }
        assert!(!bytes.windows(2).any(|pair| pair == "\u{85}".as_bytes()));
    }

    #[test]
    fn typed_control_and_sanitized_tool_output_keep_search_identity() {
        use super::super::render::{Entry, TranscriptOptions};
        let theme = Theme::system();
        let entry = Entry::Tool {
            id: "test".into(),
            name: "bash".into(),
            arguments: "{}".into(),
            output: "\x1b[31ma\u{85}b\x1b[0m".into(),
            failed: false,
            finished: true,
            parts: vec![],
            numbered_diff: false,
        };
        let lines = entry.render(&theme, 80, true, true, TranscriptOptions::default());
        let mut search = Search::default();
        search.input(
            KeyEvent::new(KeyCode::Char('\u{85}'), KeyModifiers::NONE),
            &Keys::default(),
        );
        search.update(&lines, 0);
        assert_eq!(search.result(), "1/1");
        let mut joined = Search::default();
        joined.paste("ab");
        joined.update(&lines, 0);
        assert_eq!(joined.result(), "No matches");
        let mut bytes = Vec::new();
        for line in lines {
            line.draw(&mut bytes, 80).unwrap();
        }
        assert!(!bytes.windows(2).any(|pair| pair == "\u{85}".as_bytes()));
    }

    #[test]
    fn search_cache_tracks_invisible_source_changes() {
        let mut search = Search::default();
        search.paste("\u{85}");
        let first = [Line::plain("a\u{85}b", Color::Reset)];
        assert_eq!(search.update(&first, 0), Some(0..1));
        let second = [Line::plain("ab", Color::Reset)];
        search.update(&second, 0);
        assert_eq!(search.result(), "No matches");
        search.update(&first, 0);
        assert_eq!(search.result(), "1/1");
    }

    #[test]
    fn search_kill_yank_and_undo_preserve_control() {
        let mut search = Search::default();
        search.paste("a\u{85}b");
        assert!(search.editor.search_action("tui.editor.deleteToLineStart"));
        assert_eq!(search.editor.text, "");
        assert!(search.editor.search_action("tui.editor.yank"));
        assert_eq!(search.editor.text, "a\u{85}b");
        assert!(search.editor.search_action("tui.editor.undo"));
        assert_eq!(search.editor.text, "");
    }

    #[test]
    fn search_metadata_survives_highlight_splits_and_rerender() {
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        for source in ["\u{85}é\u{85}b\u{85}", "a\u{85}\u{85}b"] {
            let mut lines = [Line::plain(source, Color::Reset)];
            let mut search = Search::default();
            search.paste(if source.starts_with('a') { "a" } else { "é" });
            search.update(&lines, 0);
            search.highlight(&mut lines, 0..1, &theme);
            let raw: String = lines[0].spans.iter().map(Span::searchable).collect();
            assert_eq!(raw, source);
            let mut control_search = Search::default();
            control_search.paste("\u{85}");
            control_search.update(&lines, 0);
            assert_eq!(
                control_search.matches.len(),
                source.matches('\u{85}').count()
            );
            let mut bytes = Vec::new();
            lines[0].draw(&mut bytes, 20).unwrap();
            assert!(!bytes.windows(2).any(|pair| pair == "\u{85}".as_bytes()));
        }
    }

    #[test]
    fn search_tracks_control_through_markdown_and_wrapping() {
        use super::super::render::{Entry, TranscriptOptions};
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        for content in [
            "**a\u{85}b**",
            "`a\u{85}b`",
            "[a\u{85}b](https://x.example)",
            "```\na\u{85}b\n```",
            "```rust\nlet a = \"a\u{85}b\";\n```",
            "| a\u{85}b |\n| --- |\n| value |",
        ] {
            let entry = Entry::Assistant(content.into());
            let mut lines = entry.render(&theme, 40, false, false, TranscriptOptions::default());
            let mut search = Search::default();
            search.paste("a\u{85}b");
            assert!(search.update(&lines, 0).is_some(), "{content:?}");
            assert_eq!(search.result(), "1/1", "{content:?}");
            let len = lines.len();
            search.highlight(&mut lines, 0..len, &theme);
            let mut output = Vec::new();
            for line in lines {
                line.draw(&mut output, 40).unwrap();
            }
            assert!(!output.windows(2).any(|pair| pair == "\u{85}".as_bytes()));
        }
        let span = Span::new("a\u{85}b", Color::Reset);
        let lines = super::super::render::wrap(&[span], 1);
        let mut search = Search::default();
        search.paste("\u{85}");
        assert!(search.update(&lines, 0).is_some());
        assert_eq!(search.result(), "1/1");
    }

    #[test]
    fn invisible_control_does_not_consume_wrap_or_cursor_columns() {
        let theme = Theme::system();
        let lines = super::super::render::wrap(&[Span::new("a\u{85}b", Color::Reset)], 2);
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0]
                .spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>(),
            "ab"
        );
        let mut search = Search::default();
        search.paste("a\u{85}b");
        assert_eq!(search.update(&lines, 0), Some(0..1));
        assert_eq!(search.result(), "1/1");
        let (overlay, row, column) = search.overlay(&theme, &Keys::default(), 8);
        assert_eq!((row, column), (1, 3));
        let (narrow, row, column) = search.overlay(&theme, &Keys::default(), 4);
        assert_eq!((row, column), (1, 2));
        assert!(narrow[1].spans.iter().any(|span| span.text == "b"));
        let mut output = Vec::new();
        for line in lines.iter().chain(&overlay).chain(&narrow) {
            line.draw(&mut output, 8).unwrap();
        }
        assert!(!output.windows(2).any(|pair| pair == "\u{85}".as_bytes()));
    }

    #[test]
    fn pasted_tabs_and_utf8_respect_the_remaining_input_limit() {
        let mut search = Search::default();
        search.paste(&"\t".repeat(4096));
        assert_eq!(search.editor.text.len(), 4096);
        assert!(search.editor.text.bytes().all(|byte| byte == b' '));

        let mut search = Search::default();
        search.editor.replace("x".repeat(4094));
        search.paste("\tmore");
        assert_eq!(search.editor.text.len(), 4096);
        assert!(search.editor.text.ends_with("  "));

        let mut search = Search::default();
        search.editor.replace("x".repeat(4095));
        search.paste("éz");
        assert_eq!(search.editor.text.len(), 4095);
    }

    #[test]
    fn whitespace_with_combining_mark_remains_part_of_a_grapheme() {
        let rows = vec!["a \u{301}b".into()];
        assert_eq!(find_matches(&rows, "a \u{301}b").len(), 1);
        assert!(find_matches(&rows, "a b").is_empty());
    }

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
