//! Render native unified edit results with line numbers and word-level changes.
use super::{
    render::{Line, Span, wrap},
    theme::Theme,
};
use similar::{ChangeTag, TextDiff};

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
fn changed_span(text: &str, row: &Row, theme: &Theme, changed: bool) -> Vec<Span> {
    if !changed {
        return vec![Span::new(text, theme.color(row.color()))];
    }
    let trimmed = text.trim_start();
    let mut spans = vec![Span::new(
        &text[..text.len() - trimmed.len()],
        theme.color(row.color()),
    )];
    let mut span = Span::new(trimmed, theme.color(row.color()));
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
        if paired {
            let added = &rows[index + 1];
            let mut removed_spans = vec![Span::new(row.prefix(digits), theme.color(row.color()))];
            let mut added_spans = vec![Span::new(added.prefix(digits), theme.color(added.color()))];
            for change in TextDiff::configure()
                .deadline(deadline)
                .diff_words(&row.text, &added.text)
                .iter_all_changes()
            {
                if change.tag() != ChangeTag::Insert {
                    removed_spans.extend(changed_span(
                        change.value(),
                        row,
                        theme,
                        change.tag() == ChangeTag::Delete,
                    ));
                }
                if change.tag() != ChangeTag::Delete {
                    added_spans.extend(changed_span(
                        change.value(),
                        added,
                        theme,
                        change.tag() == ChangeTag::Insert,
                    ));
                }
            }
            lines.extend(wrap(&removed_spans, width));
            lines.extend(wrap(&added_spans, width));
            index += 2;
        } else {
            lines.extend(wrap(
                &[Span::new(
                    format!("{}{}", row.prefix(digits), row.text),
                    theme.color(row.color()),
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
