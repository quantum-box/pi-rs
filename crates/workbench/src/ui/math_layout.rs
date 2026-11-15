//! Native port of Pi v1.5.0 terminal math layout (`packages/tui/src/latex.ts`).
//! Copyright (c) 2025 Mario Zechner; MIT, see assets/PI-LICENSE.
use unicode_width::UnicodeWidthStr;

pub(super) const MARKER_START: char = '\u{f0000}';
pub(super) const MARKER_END: char = '\u{f0001}';
pub(super) const PROTECTED_SPACE: char = '\u{f0002}';
const MAX_LAYOUT_WORK: usize = 2 * 1024 * 1024;

struct Budget(usize);
impl Budget {
    fn charge(&mut self, amount: usize) -> Option<()> {
        self.0 = self.0.checked_sub(amount)?;
        Some(())
    }
    fn area(&mut self, rows: usize, columns: usize) -> Option<()> {
        self.charge(rows.checked_mul(columns.max(1))?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LayoutNode {
    Fraction {
        numerator: String,
        denominator: String,
    },
    Operator {
        operator: String,
        lower: Option<String>,
        upper: Option<String>,
    },
    Script {
        lower: Option<String>,
        upper: Option<String>,
    },
    Matrix {
        lines: Vec<String>,
        baseline: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Layout {
    pub lines: Vec<String>,
    pub width: usize,
    pub baseline: usize,
}

pub(super) fn marker(index: usize) -> String {
    format!("{MARKER_START}{index}{MARKER_END}")
}

fn width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}
fn padded(line: &str, cells: usize, centered: bool) -> String {
    let padding = cells.saturating_sub(width(line));
    let left = if centered { padding / 2 } else { 0 };
    format!("{}{line}{}", " ".repeat(left), " ".repeat(padding - left))
}
fn text_layout(text: String) -> Layout {
    Layout {
        width: width(&text),
        lines: vec![text],
        baseline: 0,
    }
}
fn join(layouts: &[Layout], budget: &mut Budget) -> Option<Layout> {
    if layouts.is_empty() {
        return Some(text_layout(String::new()));
    }
    let baseline = layouts.iter().map(|item| item.baseline).max().unwrap_or(0);
    let below = layouts
        .iter()
        .map(|item| item.lines.len().saturating_sub(item.baseline + 1))
        .max()
        .unwrap_or(0);
    let rows = baseline.checked_add(below)?.checked_add(1)?;
    let columns = layouts.iter().try_fold(0usize, |sum, item| {
        let bytes = item.lines.iter().map(String::len).max().unwrap_or(0);
        sum.checked_add(item.width.max(bytes))
    })?;
    budget.area(rows, columns)?;
    let mut lines = Vec::with_capacity(rows);
    for row in 0..=baseline + below {
        let mut line = String::new();
        for item in layouts {
            let source_row = row
                .checked_add(item.baseline)
                .and_then(|r| r.checked_sub(baseline));
            match source_row.and_then(|r| item.lines.get(r)) {
                Some(text) => line.push_str(&padded(text, item.width, false)),
                None => line.push_str(&" ".repeat(item.width)),
            }
        }
        lines.push(line.trim_end().to_owned());
    }
    Some(Layout {
        lines,
        width: layouts.iter().map(|item| item.width).sum(),
        baseline,
    })
}

fn node_layout(
    node: &LayoutNode,
    nodes: &[LayoutNode],
    depth: usize,
    budget: &mut Budget,
) -> Option<Layout> {
    match node {
        LayoutNode::Fraction {
            numerator,
            denominator,
        } => {
            let numerator = render_at(numerator, nodes, depth + 1, budget)?;
            let denominator = render_at(denominator, nodes, depth + 1, budget)?;
            let content_width = numerator.width.max(denominator.width).max(1);
            let total = content_width + 2;
            let baseline = numerator.lines.len();
            budget.area(
                numerator
                    .lines
                    .len()
                    .checked_add(denominator.lines.len())?
                    .checked_add(1)?,
                total.checked_mul(4)?,
            )?;
            let mut lines: Vec<_> = numerator
                .lines
                .iter()
                .map(|line| padded(line, total, true))
                .collect();
            lines.push(format!(" {} ", "─".repeat(content_width)));
            lines.extend(
                denominator
                    .lines
                    .iter()
                    .map(|line| padded(line, total, true)),
            );
            Some(Layout {
                lines,
                width: total,
                baseline,
            })
        }
        LayoutNode::Operator {
            operator,
            lower,
            upper,
        } => {
            let content_width = width(operator)
                .max(lower.as_deref().map_or(0, width))
                .max(upper.as_deref().map_or(0, width));
            budget.area(
                1 + usize::from(lower.is_some()) + usize::from(upper.is_some()),
                content_width.checked_mul(4)?.checked_add(1)?,
            )?;
            let mut lines = Vec::new();
            if let Some(text) = upper {
                lines.push(format!("{} ", padded(text, content_width, true)));
            }
            lines.push(format!("{} ", padded(operator, content_width, true)));
            if let Some(text) = lower {
                lines.push(format!("{} ", padded(text, content_width, true)));
            }
            Some(Layout {
                lines,
                width: content_width + 1,
                baseline: usize::from(upper.is_some()),
            })
        }
        LayoutNode::Script { lower, upper } => {
            let upper = match upper {
                Some(text) => Some(render_at(text, nodes, depth + 1, budget)?),
                None => None,
            };
            let lower = match lower {
                Some(text) => Some(render_at(text, nodes, depth + 1, budget)?),
                None => None,
            };
            let cells = upper
                .as_ref()
                .map_or(0, |layout| layout.width)
                .max(lower.as_ref().map_or(0, |layout| layout.width));
            let baseline = upper.as_ref().map_or(0, |layout| layout.lines.len());
            let rows = baseline
                .checked_add(lower.as_ref().map_or(0, |layout| layout.lines.len()))?
                .checked_add(1)?;
            budget.area(rows, cells.checked_mul(4)?)?;
            let mut lines = Vec::new();
            if let Some(layout) = upper {
                lines.extend(layout.lines.iter().map(|line| padded(line, cells, false)));
            }
            lines.push(" ".repeat(cells));
            if let Some(layout) = lower {
                lines.extend(layout.lines.iter().map(|line| padded(line, cells, false)));
            }
            Some(Layout {
                lines,
                width: cells,
                baseline,
            })
        }
        LayoutNode::Matrix { lines, baseline } => {
            let cells = lines
                .iter()
                .map(|line| width(line.as_str()))
                .max()
                .unwrap_or(0);
            let max_bytes = lines.iter().map(String::len).max().unwrap_or(0);
            budget.area(lines.len(), cells.checked_add(max_bytes)?)?;
            Some(Layout {
                lines: lines
                    .iter()
                    .map(|line| padded(line, cells, false))
                    .collect(),
                width: cells,
                baseline: *baseline,
            })
        }
    }
}

// Returns valid marker spans, matching Pi's digit-only marker regex. Invalid
// references remain literal and do not affect text slicing.
fn next_marker(line: &str, from: usize) -> Option<(usize, usize, usize)> {
    let mut search = from;
    while let Some(offset) = line[search..].find(MARKER_START) {
        let start = search + offset;
        let digits = start + MARKER_START.len_utf8();
        let digit_len = line[digits..]
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        let after_digits = digits + digit_len;
        if digit_len > 0
            && line[after_digits..].starts_with(MARKER_END)
            && let Ok(index) = line[digits..after_digits].parse()
        {
            return Some((start, after_digits + MARKER_END.len_utf8(), index));
        }
        search = digits;
    }
    None
}

fn render_at(
    source: &str,
    nodes: &[LayoutNode],
    depth: usize,
    budget: &mut Budget,
) -> Option<Layout> {
    if depth > 64 {
        return None;
    }
    budget.charge(source.len().checked_add(1)?)?;
    let mut rendered = Vec::new();
    let mut first_baseline = 0;
    for source_line in source.split('\n') {
        let mut layouts = Vec::new();
        let mut position = 0;
        let mut search = 0;
        let mut previous: Option<&LayoutNode> = None;
        while let Some((start, end, index)) = next_marker(source_line, search) {
            search = end;
            let Some(node) = nodes.get(index) else {
                continue;
            };
            if start > position {
                let slice = &source_line[position..start];
                let trimmed = if previous.is_some() {
                    slice.trim_start()
                } else {
                    slice
                }
                .trim_end();
                let leading = matches!(previous, Some(LayoutNode::Matrix { .. }))
                    && slice.starts_with(char::is_whitespace);
                let trailing = matches!(node, LayoutNode::Matrix { .. })
                    && slice.ends_with(char::is_whitespace);
                let text = if !trimmed.is_empty() {
                    format!(
                        "{}{trimmed}{}",
                        if leading { " " } else { "" },
                        if trailing { " " } else { "" }
                    )
                } else if leading || trailing {
                    " ".into()
                } else {
                    String::new()
                };
                layouts.push(text_layout(text));
            }
            layouts.push(node_layout(node, nodes, depth, budget)?);
            position = end;
            previous = Some(node);
        }
        if position < source_line.len() {
            let slice = &source_line[position..];
            let trimmed = if previous.is_some() {
                slice.trim_start()
            } else {
                slice
            };
            let leading = matches!(previous, Some(LayoutNode::Matrix { .. }))
                && slice.starts_with(char::is_whitespace);
            layouts.push(text_layout(format!(
                "{}{trimmed}",
                if leading { " " } else { "" }
            )));
        }
        let line = join(&layouts, budget)?;
        if rendered.is_empty() {
            first_baseline = line.baseline;
        }
        rendered.extend(line.lines);
    }
    let cells = rendered
        .iter()
        .map(|line| width(line.as_str()))
        .max()
        .unwrap_or(0);
    Some(Layout {
        lines: rendered,
        width: cells,
        baseline: first_baseline,
    })
}

pub(super) fn render(source: &str, nodes: &[LayoutNode]) -> Option<Layout> {
    render_at(source, nodes, 0, &mut Budget(MAX_LAYOUT_WORK))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fraction_aligns_on_the_bar_with_surrounding_text() {
        let nodes = vec![LayoutNode::Fraction {
            numerator: "0.1 lux".into(),
            denominator: "100 lm/W".into(),
        }];
        let layout = render(&format!("E ≈ {}", marker(0)), &nodes).unwrap();
        assert_eq!(
            layout.lines,
            ["    0.1 lux", "E ≈ ────────", "    100 lm/W"]
        );
    }
    #[test]
    fn matrix_keeps_spaces_and_baseline() {
        let nodes = vec![LayoutNode::Matrix {
            lines: vec!["⎛ π │ 0   ⎞".into(), "⎝ 0 │ 1/π ⎠".into()],
            baseline: 0,
        }];
        let layout = render(&format!("A = {}.", marker(0)), &nodes).unwrap();
        assert_eq!(layout.lines, ["A = ⎛ π │ 0   ⎞.", "    ⎝ 0 │ 1/π ⎠"]);
    }
    #[test]
    fn invalid_and_recursive_markers_are_bounded() {
        assert_eq!(
            render(&format!("a{}b", marker(4)), &[]).unwrap().lines,
            [format!("a{}b", marker(4))]
        );
        let nodes = vec![LayoutNode::Fraction {
            numerator: marker(0),
            denominator: "1".into(),
        }];
        assert!(render(&marker(0), &nodes).is_none());
        let mut nodes = vec![LayoutNode::Fraction {
            numerator: "a".into(),
            denominator: "b".into(),
        }];
        for index in 0..24 {
            nodes.push(LayoutNode::Fraction {
                numerator: marker(index),
                denominator: marker(index),
            });
        }
        assert!(render(&marker(24), &nodes).is_none());
    }

    #[test]
    fn operator_limits_and_scripts_align_around_their_baseline() {
        let nodes = vec![
            LayoutNode::Operator {
                operator: "lim".into(),
                lower: Some("x→0".into()),
                upper: None,
            },
            LayoutNode::Fraction {
                numerator: "a".into(),
                denominator: "b".into(),
            },
            LayoutNode::Script {
                lower: Some("i".into()),
                upper: Some("n".into()),
            },
        ];
        let operator = render(&marker(0), &nodes).unwrap();
        assert_eq!(operator.lines, ["lim", "x→0"]);
        assert_eq!(operator.baseline, 0);
        let combined = render(&format!("{}{}", marker(0), marker(1)), &nodes).unwrap();
        assert_eq!(combined.lines, ["     a", "lim  ─", "x→0  b"]);
        let script = render(&format!("x{}", marker(2)), &nodes).unwrap();
        assert_eq!(script.lines, [" n", "x", " i"]);
    }
}
