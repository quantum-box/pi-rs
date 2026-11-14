//! Render native unified edit results with line numbers and word-level changes.
use super::{
    render::{Line, Span, wrap},
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
    render_rows(&rows, digits, theme, width)
}

pub fn render_numbered(text: &str, theme: &Theme, width: usize) -> Vec<Line> {
    let mut digits = 1;
    let rows: Vec<_> = text
        .split('\n')
        .map(|line| {
            if let Some((marker, body)) = line.split_at_checked(1)
                && matches!(marker, "-" | "+" | " ")
            {
                let trimmed = body.trim_start_matches(' ');
                if let Some((number, text)) = trimmed.split_once(' ')
                    && let Ok(number_value) = number.parse::<usize>()
                {
                    let kind = match marker {
                        "-" => Kind::Removed,
                        "+" => Kind::Added,
                        _ => Kind::Context,
                    };
                    digits = digits.max(body.len() - trimmed.len() + number.len());
                    return Row {
                        kind,
                        number: Some(number_value),
                        text: text.replace('\t', "   "),
                    };
                }
            }
            Row {
                kind: Kind::Text,
                number: None,
                text: line.into(),
            }
        })
        .collect();
    render_rows(&rows, digits, theme, width)
}

fn render_rows(rows: &[Row], digits: usize, theme: &Theme, width: usize) -> Vec<Line> {
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
            lines.extend(wrap(&removed_spans, width));
            lines.extend(wrap(&added_spans, width));
            index += 2;
        } else {
            lines.extend(wrap(
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
