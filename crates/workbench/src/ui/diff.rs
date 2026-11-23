//! Render native unified edit results with line numbers and word-level changes.
use super::{
    render::{Line, Span, wrap, wrap_words},
    theme::Theme,
};
mod word_diff;
use word_diff::Tag;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Text,
    Context,
    Removed,
    Added,
}
struct Row {
    kind: Kind,
    number: Option<usize>,
    text: String,
    explicit_prefix: Option<String>,
}
impl Row {
    fn color(&self) -> &'static str {
        match self.kind {
            Kind::Removed => "toolDiffRemoved",
            Kind::Added => "toolDiffAdded",
            _ => "toolDiffContext",
        }
    }
    fn prefix(&self, digits: usize) -> String {
        if let Some(prefix) = &self.explicit_prefix {
            return prefix.clone();
        }
        let marker = match self.kind {
            Kind::Removed => '-',
            Kind::Added => '+',
            _ => ' ',
        };
        self.number
            .map_or_else(String::new, |number| format!("{marker}{number:>digits$} "))
    }
}
fn rows(text: &str) -> Vec<Row> {
    let mut position = None;
    text.split('\n')
        .map(|line| {
            if line.starts_with("@@ ") {
                let mut ranges = line.split_whitespace().skip(1);
                let start = |part: &str| part.get(1..)?.split(',').next()?.parse::<usize>().ok();
                position = ranges
                    .next()
                    .and_then(start)
                    .zip(ranges.next().and_then(start));
            }
            let kind = match (position, line.as_bytes().first()) {
                (Some(_), Some(b'-')) => Kind::Removed,
                (Some(_), Some(b'+')) => Kind::Added,
                (Some(_), Some(b' ')) => Kind::Context,
                _ => Kind::Text,
            };
            let number = position.as_mut().and_then(|(old, new)| match kind {
                Kind::Removed => {
                    let number = *old;
                    *old = old.saturating_add(1);
                    Some(number)
                }
                Kind::Added => {
                    let number = *new;
                    *new = new.saturating_add(1);
                    Some(number)
                }
                Kind::Context => {
                    let number = *new;
                    *old = old.saturating_add(1);
                    *new = new.saturating_add(1);
                    Some(number)
                }
                Kind::Text => None,
            });
            Row {
                kind,
                number,
                explicit_prefix: None,
                text: if number.is_some() { &line[1..] } else { line }.replace('\t', "   "),
            }
        })
        .collect()
}
fn changed_span(text: &str, row: &Row, theme: &Theme, changed: bool, first: bool) -> Vec<Span> {
    if !changed {
        return vec![Span::themed(text, theme, row.color())];
    }
    let trimmed = if first {
        text.trim_start_matches(word_diff::ws)
    } else {
        text
    };
    let mut spans = vec![Span::themed(
        &text[..text.len() - trimmed.len()],
        theme,
        row.color(),
    )];
    let mut span = Span::themed(trimmed, theme, row.color());
    span.inverse = true;
    spans.push(span);
    spans
}
pub fn render(text: &str, theme: &Theme, width: usize) -> Vec<Line> {
    let rows = rows(text);
    let digits = rows
        .iter()
        .filter_map(|row| row.number)
        .max()
        .unwrap_or(1)
        .to_string()
        .len();
    render_rows(&rows, digits, theme, width, wrap)
}

pub fn render_numbered(text: &str, theme: &Theme, width: usize) -> Vec<Line> {
    // Pi parses the display gutter on each line independently, including empty
    // line numbers. Preserve that gutter instead of normalizing its padding.
    // JavaScript's \s includes the BOM and excludes U+0085; its dot also
    // excludes CR and Unicode line separators.
    static DIFF_LINE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(concat!(
            r"^([+\-\x{0009}-\x{000D}\x{0020}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}])",
            r"([\x{0009}-\x{000D}\x{0020}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}]*[0-9]*)",
            r"[\x{0009}-\x{000D}\x{0020}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}]",
            r"([^\r\n\x{2028}\x{2029}]*)$"
        )).unwrap()
    });
    let rows: Vec<_> = text
        .split('\n')
        .map(|line| {
            if let Some(captures) = DIFF_LINE.captures(line) {
                let marker = &captures[1];
                let kind = match marker {
                    "-" => Kind::Removed,
                    "+" => Kind::Added,
                    _ => Kind::Context,
                };
                let marker = if kind == Kind::Context { " " } else { marker };
                return Row {
                    kind,
                    number: None,
                    explicit_prefix: Some(format!("{marker}{} ", &captures[2])),
                    text: captures[3].replace('\t', "   "),
                };
            }
            Row {
                kind: Kind::Text,
                number: None,
                explicit_prefix: None,
                text: line.into(),
            }
        })
        .collect();
    render_rows(&rows, 1, theme, width, wrap_words)
}

fn render_rows(
    rows: &[Row],
    digits: usize,
    theme: &Theme,
    width: usize,
    wrap_row: fn(&[Span], usize) -> Vec<Line>,
) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut index = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    while index < rows.len() {
        let row = &rows[index];
        let paired = std::time::Instant::now() < deadline
            && row.kind == Kind::Removed
            && rows
                .get(index + 1)
                .is_some_and(|row| row.kind == Kind::Added)
            && (index == 0 || rows[index - 1].kind != Kind::Removed)
            && rows
                .get(index + 2)
                .is_none_or(|row| row.kind != Kind::Added);
        let changes = paired
            .then(|| word_diff::diff(&row.text, &rows[index + 1].text, deadline))
            .flatten();
        if let Some(changes) = changes {
            let added = &rows[index + 1];
            let mut removed_spans = vec![Span::themed(row.prefix(digits), theme, row.color())];
            let mut added_spans = vec![Span::themed(added.prefix(digits), theme, added.color())];
            let (mut first_removed, mut first_added) = (true, true);
            for change in changes {
                if change.tag != Tag::Insert {
                    removed_spans.extend(changed_span(
                        &change.text,
                        row,
                        theme,
                        change.tag == Tag::Delete,
                        first_removed,
                    ));
                    if change.tag == Tag::Delete {
                        first_removed = false;
                    }
                }
                if change.tag != Tag::Delete {
                    added_spans.extend(changed_span(
                        &change.text,
                        added,
                        theme,
                        change.tag == Tag::Insert,
                        first_added,
                    ));
                    if change.tag == Tag::Insert {
                        first_added = false;
                    }
                }
            }
            lines.extend(wrap_row(&removed_spans, width));
            lines.extend(wrap_row(&added_spans, width));
            index += 2;
        } else {
            lines.extend(wrap_row(
                &[Span::themed(
                    format!("{}{}", row.prefix(digits), row.text),
                    theme,
                    row.color(),
                )],
                width,
            ));
            index += 1;
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numbered_diff_wraps_words_and_keeps_highlighting_across_rows() {
        let theme = Theme::system();
        let text = |line: &Line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        };
        for (source, width, expected) in [
            ("-1 red blue", 8, vec!["-1 red", "blue"]),
            ("+1 red chartreuse", 8, vec!["+1 red", "chartreu", "se"]),
            ("-9     alpha  beta", 8, vec!["-9", "alpha", "beta"]),
            ("+1 abc 日本語 test", 9, vec!["+1 abc 日", "本語 test"]),
            ("+1 🧑‍💻 after", 8, vec!["+1 🧑‍💻", "after"]),
        ] {
            let rows = render_numbered(source, &theme, width);
            assert_eq!(rows.iter().map(text).collect::<Vec<_>>(), expected);
        }
        let rows = render_numbered(
            "-1 first oldword suffix\n+1 first newword suffix",
            &theme,
            10,
        );
        assert_eq!(
            rows.iter().map(text).collect::<Vec<_>>(),
            [
                "-1 first", "oldword", "suffix", "+1 first", "newword", "suffix"
            ]
        );
        assert!(
            rows[1]
                .spans
                .iter()
                .all(|span| span.inverse && span.color == theme.color("toolDiffRemoved"))
        );
        assert!(
            rows[4]
                .spans
                .iter()
                .all(|span| span.inverse && span.color == theme.color("toolDiffAdded"))
        );
        assert!(rows[5].spans.iter().all(|span| !span.inverse));
    }

    #[test]
    fn grouped_changes_match_pi_whitespace_highlighting() {
        let lines = render_numbered("-1 A x B y\n+1 A  c B  d", &Theme::system(), 80);
        let highlighted = |line: &Line| {
            line.spans
                .iter()
                .filter(|s| s.inverse)
                .map(|s| s.text.as_str())
                .collect::<String>()
        };
        assert_eq!(highlighted(&lines[0]), "xy");
        assert_eq!(highlighted(&lines[1]), "c d");
        assert_eq!(
            lines[1]
                .spans
                .iter()
                .map(|s| s.text.as_str())
                .collect::<String>(),
            "+1 A  c B  d"
        );

        let lines = render_numbered("-1 abc def\n+1 xyz ghi", &Theme::system(), 80);
        assert_eq!(highlighted(&lines[0]), "abc def");
        assert_eq!(highlighted(&lines[1]), "xyz ghi");
    }
    #[test]
    fn numbered_edits_keep_explicit_padding_and_word_highlighting() {
        let theme = Theme::system();
        let lines = render_numbered(
            "     ...\n-  6 old value\n+  6 new value\n  10 unchanged",
            &theme,
            80,
        );
        let text = |line: &Line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        };
        assert_eq!(text(&lines[0]), "     ...");
        assert_eq!(text(&lines[1]), "-  6 old value");
        assert_eq!(text(&lines[2]), "+  6 new value");
        assert!(
            lines[2]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "new")
        );
    }

    #[test]
    fn numbered_gutters_and_unnumbered_changes_follow_pi() {
        for source in [
            include_str!("../../assets/themes/dark.json"),
            include_str!("../../assets/themes/light.json"),
        ] {
            let theme = Theme::parse(source, None).unwrap();
            let lines = render_numbered(
                "-  6 old value\n+6 new value\n  10 unchanged\n- before\n+ after\n     ...\nplain",
                &theme,
                80,
            );
            let text = |line: &Line| {
                line.spans
                    .iter()
                    .map(|s| s.text.as_str())
                    .collect::<String>()
            };
            assert_eq!(
                lines.iter().map(text).collect::<Vec<_>>(),
                [
                    "-  6 old value",
                    "+6 new value",
                    "  10 unchanged",
                    "- before",
                    "+ after",
                    "     ...",
                    "plain",
                ]
            );
            for (index, role, changed) in [
                (0, "toolDiffRemoved", "old"),
                (1, "toolDiffAdded", "new"),
                (3, "toolDiffRemoved", "before"),
                (4, "toolDiffAdded", "after"),
            ] {
                assert!(
                    lines[index]
                        .spans
                        .iter()
                        .all(|s| s.color == theme.color(role))
                );
                assert!(
                    lines[index]
                        .spans
                        .iter()
                        .any(|s| s.inverse && s.text == changed)
                );
            }
            assert!(
                lines[5]
                    .spans
                    .iter()
                    .all(|s| s.color == theme.color("toolDiffContext"))
            );
        }
    }

    #[test]
    fn numbered_diff_uses_ecmascript_whitespace_and_line_terminators() {
        let theme = Theme::parse(include_str!("../../assets/themes/dark.json"), None).unwrap();
        let lines = render_numbered(
            "-\u{feff}1 old\n+\u{feff}1 new\n-\u{85}1 old\n+\u{85}1 new\n-1 old\rinside\n+1 new\rinside",
            &theme,
            80,
        );
        assert!(
            lines[0]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "old")
        );
        assert!(
            lines[1]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "new")
        );
        for line in &lines[2..] {
            assert!(
                line.spans
                    .iter()
                    .all(|span| !span.inverse && span.color == theme.color("toolDiffContext"))
            );
        }
    }

    #[test]
    fn unnumbered_multi_line_changes_are_not_paired() {
        let lines = render_numbered("- first\n- second\n+ replacement", &Theme::system(), 80);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .all(|span| !span.inverse)
        );
    }

    #[test]
    fn unified_edits_show_line_numbers_and_changed_words() {
        let theme = Theme::system();
        let lines = render(
            "--- before\n+++ after\n@@ -10,2 +10,2 @@\n-let old = 1;\n+let new = 1;\n unchanged\n",
            &theme,
            80,
        );
        let text = |line: &Line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        };
        assert_eq!(text(&lines[3]), "-10 let old = 1;");
        assert_eq!(text(&lines[4]), "+10 let new = 1;");
        assert_eq!(text(&lines[5]), " 11 unchanged");
        assert!(
            lines[3]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "old")
        );
        assert!(
            lines[4]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "new")
        );
        assert!(
            lines[0]
                .spans
                .iter()
                .all(|span| span.color == theme.color("toolDiffContext"))
        );
    }
    #[test]
    fn unicode_and_tabs_fit_narrow_columns_and_multiple_hunks_reset_numbers() {
        use unicode_width::UnicodeWidthStr;
        let lines = render(
            "@@ -1 +1 @@\n-\t日本語\n+\t日本人\n@@ -20 +21 @@\n last",
            &Theme::system(),
            8,
        );
        assert!(lines.iter().all(|line| {
            line.spans
                .iter()
                .map(|span| UnicodeWidthStr::width(span.text.as_str()))
                .sum::<usize>()
                <= 8
        }));
        let text = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains(" 21 last"));
        assert!(!text.contains('\t'));
    }
}
