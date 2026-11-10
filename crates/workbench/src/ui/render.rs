//! Styled, terminal-safe transcript rendering shared by all themes.
use std::io::Write;
use std::ops::Range;
use std::sync::LazyLock;

use crossterm::{
    queue,
    style::{
        Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    },
};
use pulldown_cmark::{Alignment, CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use syntect::{easy::HighlightLines, highlighting::ThemeSet, parsing::SyntaxSet};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::theme::Theme;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span {
    pub text: String,
    pub color: Color,
    pub background: Option<Color>,
    pub bold: bool,
    pub italic: bool,
    pub underline: bool,
    pub inverse: bool,
    pub strikethrough: bool,
}
impl Span {
    pub(super) fn new(text: impl Into<String>, color: Color) -> Self {
        Self {
            text: clean(&text.into()),
            color,
            background: None,
            bold: false,
            italic: false,
            underline: false,
            inverse: false,
            strikethrough: false,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Line {
    pub spans: Vec<Span>,
    pub background: Option<Color>,
    pub image: Option<super::images::Placement>,
}
impl Line {
    pub fn plain(text: &str, color: Color) -> Self {
        Self {
            spans: vec![Span::new(text, color)],
            background: None,
            image: None,
        }
    }
    pub fn style_bytes(&mut self, bytes: Range<usize>, style: impl Fn(&mut Span)) {
        let mut offset = 0;
        self.spans = std::mem::take(&mut self.spans)
            .into_iter()
            .flat_map(|span| {
                let start = bytes.start.saturating_sub(offset);
                let end = bytes.end.saturating_sub(offset).min(span.text.len());
                offset += span.text.len();
                if start >= end {
                    return vec![span];
                }
                let mut pieces = Vec::with_capacity(3);
                for (range, matched) in [
                    (0..start, false),
                    (start..end, true),
                    (end..span.text.len(), false),
                ] {
                    if !range.is_empty() {
                        let mut piece = Span {
                            text: span.text[range].into(),
                            ..span.clone()
                        };
                        if matched {
                            style(&mut piece);
                        }
                        pieces.push(piece);
                    }
                }
                pieces
            })
            .collect();
    }

    pub fn mark_cursor(&mut self, column: usize) {
        let mut used = 0;
        let mut offset = 0;
        let mut cursor = None;
        'spans: for span in &self.spans {
            for (start, grapheme) in span.text.grapheme_indices(true) {
                let size = UnicodeWidthStr::width(grapheme);
                if column < used + size {
                    cursor = Some(offset + start..offset + start + grapheme.len());
                    break 'spans;
                }
                used += size;
            }
            offset += span.text.len();
        }
        if let Some(bytes) = cursor {
            self.style_bytes(bytes, |span| span.inverse = !span.inverse);
        } else {
            if column > used {
                self.spans
                    .push(Span::new(" ".repeat(column - used), Color::Reset));
            }
            self.spans.push(Span {
                inverse: true,
                ..Span::new(" ", Color::Reset)
            });
        }
    }

    pub fn draw(&self, out: &mut impl Write, width: usize) -> std::io::Result<()> {
        queue!(
            out,
            SetAttribute(Attribute::Reset),
            SetBackgroundColor(self.background.unwrap_or(Color::Reset))
        )?;
        let mut used = 0;
        for span in &self.spans {
            queue!(
                out,
                SetForegroundColor(span.color),
                SetBackgroundColor(span.background.or(self.background).unwrap_or(Color::Reset)),
                SetAttribute(if span.bold {
                    Attribute::Bold
                } else {
                    Attribute::NormalIntensity
                }),
                SetAttribute(if span.italic {
                    Attribute::Italic
                } else {
                    Attribute::NoItalic
                }),
                SetAttribute(if span.inverse {
                    Attribute::Reverse
                } else {
                    Attribute::NoReverse
                }),
                SetAttribute(if span.underline {
                    Attribute::Underlined
                } else {
                    Attribute::NoUnderline
                }),
                SetAttribute(if span.strikethrough {
                    Attribute::CrossedOut
                } else {
                    Attribute::NotCrossedOut
                })
            )?;
            for grapheme in span.text.graphemes(true) {
                let size = UnicodeWidthStr::width(grapheme);
                if used + size > width {
                    break;
                }
                queue!(out, Print(grapheme))?;
                used += size;
            }
        }
        queue!(
            out,
            SetAttribute(Attribute::Reset),
            SetBackgroundColor(self.background.unwrap_or(Color::Reset)),
            Print(" ".repeat(width.saturating_sub(used))),
            ResetColor
        )
    }
}

#[derive(Clone)]
pub enum ToolPart {
    Text(String),
    Image(std::sync::Arc<super::images::InlineImage>),
}

#[derive(Clone)]
pub enum Entry {
    Notice(String),
    User(String),
    Assistant(String),
    Thinking(String),
    Image(std::sync::Arc<super::images::InlineImage>),
    Tool {
        id: String,
        name: String,
        arguments: String,
        output: String,
        failed: bool,
        finished: bool,
        parts: Vec<ToolPart>,
        numbered_diff: bool,
    },
}
impl Entry {
    pub fn render(
        &self,
        theme: &Theme,
        width: usize,
        expand_tools: bool,
        show_thinking: bool,
        image_width: Option<usize>,
        output_pad: usize,
    ) -> Vec<Line> {
        let padding = output_pad.min(width.saturating_sub(1) / 2);
        let inner = width.saturating_sub(2 * padding).max(1);
        let mut lines = match self {
            Self::Notice(text) => return wrap(&[Span::new(text, theme.color("muted"))], width),
            Self::User(text) => {
                let mut lines = wrap(&[Span::new(text, theme.color("userMessageText"))], inner);
                for line in &mut lines {
                    line.background = Some(theme.color("userMessageBg"));
                }
                lines.push(Line::default());
                lines
            }
            Self::Assistant(text) => {
                let mut lines = markdown(text, theme, inner);
                lines.push(Line::default());
                lines
            }
            Self::Image(image) => {
                return image_width
                    .map_or_else(Vec::new, |limit| image.lines(width, limit, padding));
            }
            Self::Thinking(text) => {
                if !show_thinking {
                    return vec![];
                }
                let mut span = Span::new(text, theme.color("thinkingText"));
                span.italic = true;
                let mut lines = wrap(&[span], inner);
                lines.push(Line::default());
                lines
            }
            Self::Tool {
                name,
                arguments,
                output,
                failed,
                finished,
                parts,
                numbered_diff,
                ..
            } => {
                let role = if *failed {
                    "toolErrorBg"
                } else if *finished {
                    "toolSuccessBg"
                } else {
                    "toolPendingBg"
                };
                let arguments = serde_json::from_str::<serde_json::Value>(arguments).ok();
                let detail = arguments
                    .as_ref()
                    .and_then(|value| {
                        value
                            .get("path")
                            .or_else(|| value.get("command"))
                            .or_else(|| value.get("pattern"))
                    })
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let mut title = Span::new(format!("{name} {detail}"), theme.color("toolTitle"));
                title.bold = true;
                let mut lines = wrap(&[title], inner);
                let output_lines: Vec<_> = output.lines().collect();
                let limit = if expand_tools { output_lines.len() } else { 10 };
                let render_text = |text: &str, lines: &mut Vec<Line>| {
                    if *numbered_diff {
                        lines.extend(super::diff::render_numbered(text, theme, inner));
                    } else if name == "edit" {
                        lines.extend(super::diff::render(text, theme, inner));
                    } else {
                        for text in text.split('\n') {
                            lines
                                .extend(wrap(&[Span::new(text, theme.color("toolOutput"))], inner));
                        }
                    }
                };
                if expand_tools
                    && !parts.is_empty()
                    && let Some(limit) = image_width
                {
                    for part in parts {
                        match part {
                            ToolPart::Text(text) => render_text(text, &mut lines),
                            ToolPart::Image(image) => {
                                lines.extend(image.lines(width, limit, padding))
                            }
                        }
                    }
                } else {
                    let visible = output_lines
                        .iter()
                        .take(limit)
                        .copied()
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !visible.is_empty() {
                        render_text(&visible, &mut lines);
                    }
                }
                if output_lines.len() > limit {
                    lines.push(Line::plain(
                        &format!(
                            "… {} more lines (ctrl+o to expand)",
                            output_lines.len() - limit
                        ),
                        theme.color("dim"),
                    ));
                }
                for line in &mut lines {
                    line.background = Some(theme.color(role));
                }
                lines.push(Line::default());
                lines
            }
        };
        if padding != 0 {
            for line in &mut lines {
                if !line.spans.is_empty() {
                    line.spans.insert(0, Span::new(" ", Color::Reset));
                }
            }
        }
        lines
    }
}

pub fn clean(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .collect::<String>()
        .replace('\t', "    ")
}
pub fn wrap(spans: &[Span], width: usize) -> Vec<Line> {
    let width = width.max(1);
    let mut lines = vec![Line::default()];
    let mut used = 0;
    for span in spans {
        let mut fragment = span.clone();
        fragment.text.clear();
        for grapheme in span.text.graphemes(true) {
            let size = UnicodeWidthStr::width(grapheme);
            if grapheme == "\n" || (used + size > width && used > 0) {
                lines.last_mut().unwrap().spans.push(fragment.clone());
                fragment.text.clear();
                lines.push(Line::default());
                used = 0;
                if grapheme == "\n" {
                    continue;
                }
            }
            if size <= width {
                fragment.text.push_str(grapheme);
                used += size;
            }
        }
        lines.last_mut().unwrap().spans.push(fragment);
    }
    lines
}

static SYNTAXES: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);
static SYNTAX_THEMES: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

#[derive(Default)]
struct Table {
    alignments: Vec<Alignment>,
    rows: Vec<Vec<Vec<Span>>>,
    row: Vec<Vec<Span>>,
}
impl Table {
    fn finish_row(&mut self, heading: bool) {
        if heading {
            for span in self.row.iter_mut().flatten() {
                span.bold = true;
            }
        }
        self.rows.push(std::mem::take(&mut self.row));
    }
    fn render(self, theme: &Theme, width: usize) -> Vec<Line> {
        let columns = self.alignments.len();
        if columns == 0 {
            return Vec::new();
        }
        let minimum: Vec<usize> = (0..columns)
            .map(|column| {
                self.rows
                    .iter()
                    .filter_map(|row| row.get(column))
                    .flatten()
                    .flat_map(|span| span.text.graphemes(true))
                    .map(UnicodeWidthStr::width)
                    .max()
                    .unwrap_or(1)
                    .max(1)
            })
            .collect();
        if width < columns * 3 + 1 + minimum.iter().sum::<usize>() {
            let mut lines = Vec::new();
            for row in self.rows {
                for cell in row {
                    lines.extend(wrap(&cell, width));
                }
                lines.push(Line::default());
            }
            return lines;
        }
        let available = width - columns * 3 - 1;
        let natural: Vec<usize> = (0..columns)
            .map(|column| {
                self.rows
                    .iter()
                    .filter_map(|row| row.get(column))
                    .map(|cell| {
                        cell.iter()
                            .map(|span| UnicodeWidthStr::width(span.text.as_str()))
                            .sum()
                    })
                    .max()
                    .unwrap_or(1)
                    .max(1)
            })
            .collect();
        let mut widths = minimum;
        for _ in widths.iter().sum::<usize>()..available {
            let Some(column) = (0..columns)
                .filter(|&column| widths[column] < natural[column])
                .min_by_key(|&column| widths[column])
            else {
                break;
            };
            widths[column] += 1;
        }
        let border = |left: &str, middle: &str, right: &str| {
            Line::plain(
                &format!(
                    "{left}{}{right}",
                    widths
                        .iter()
                        .map(|width| "─".repeat(width + 2))
                        .collect::<Vec<_>>()
                        .join(middle)
                ),
                theme.color("borderMuted"),
            )
        };
        let mut lines = vec![border("┌", "┬", "┐")];
        for (index, row) in self.rows.into_iter().enumerate() {
            let cells: Vec<_> = widths
                .iter()
                .enumerate()
                .map(|(column, width)| {
                    wrap(row.get(column).map(Vec::as_slice).unwrap_or(&[]), *width)
                })
                .collect();
            for line_index in 0..cells.iter().map(Vec::len).max().unwrap_or(1) {
                let mut line = Line::plain("│", theme.color("borderMuted"));
                for (column, cell) in cells.iter().enumerate() {
                    let spans = cell
                        .get(line_index)
                        .map(|line| line.spans.as_slice())
                        .unwrap_or(&[]);
                    let used: usize = spans
                        .iter()
                        .map(|span| UnicodeWidthStr::width(span.text.as_str()))
                        .sum();
                    let padding = widths[column].saturating_sub(used);
                    let left = match self.alignments[column] {
                        Alignment::Right => padding,
                        Alignment::Center => padding / 2,
                        _ => 0,
                    };
                    line.spans
                        .push(Span::new(" ".repeat(left + 1), theme.color("text")));
                    line.spans.extend_from_slice(spans);
                    line.spans.push(Span::new(
                        format!("{}│", " ".repeat(padding - left + 1)),
                        theme.color("borderMuted"),
                    ));
                }
                lines.push(line);
            }
            if index == 0 {
                lines.push(border("├", "┼", "┤"));
            }
        }
        lines.push(border("└", "┴", "┘"));
        lines
    }
}

pub fn markdown(text: &str, theme: &Theme, width: usize) -> Vec<Line> {
    markdown_at_depth(text, theme, width, 0)
}
fn markdown_at_depth(text: &str, theme: &Theme, width: usize, depth: usize) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut bold = 0usize;
    let mut italic = 0usize;
    let mut strikethrough = 0usize;
    let mut heading = 0usize;
    let mut quote_skip = 0usize;
    let mut code: Option<String> = None;
    let mut code_text = String::new();
    let mut lists: Vec<Option<u64>> = Vec::new();
    let mut item_prefixes = Vec::new();
    let mut table: Option<Table> = None;
    let mut links = Vec::new();
    for (event, range) in Parser::new_ext(
        text,
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    )
    .into_offset_iter()
    {
        if quote_skip > 0 {
            match event {
                Event::Start(Tag::BlockQuote(_)) => quote_skip += 1,
                Event::End(TagEnd::BlockQuote(_)) => quote_skip -= 1,
                _ => {}
            }
            continue;
        }
        match event {
            Event::Start(Tag::Table(alignments)) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                table = Some(Table {
                    alignments,
                    ..Table::default()
                });
            }
            Event::End(TagEnd::TableCell) => {
                if let Some(table) = &mut table {
                    table.row.push(std::mem::take(&mut spans));
                }
            }
            Event::End(TagEnd::TableHead | TagEnd::TableRow) => {
                if let Some(table) = &mut table {
                    table.finish_row(matches!(event, Event::End(TagEnd::TableHead)));
                }
            }
            Event::End(TagEnd::Table) => {
                if let Some(table) = table.take() {
                    lines.extend(table.render(theme, width));
                }
            }
            Event::Start(Tag::Link { dest_url, .. }) => links.push(dest_url.into_string()),
            Event::End(TagEnd::Link) => {
                if let Some(url) = links.pop() {
                    spans.push(Span::new(format!(" ({url})"), theme.color("mdLinkUrl")));
                }
            }
            Event::Start(Tag::Strikethrough) => strikethrough += 1,
            Event::End(TagEnd::Strikethrough) => strikethrough = strikethrough.saturating_sub(1),
            Event::Start(Tag::Strong) => bold += 1,
            Event::End(TagEnd::Strong) => bold = bold.saturating_sub(1),
            Event::Start(Tag::Emphasis) => italic += 1,
            Event::End(TagEnd::Emphasis) => italic = italic.saturating_sub(1),
            Event::Start(Tag::Heading { level, .. }) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                heading = level as usize;
                if heading >= 3 {
                    let mut prefix = Span::new(
                        format!("{} ", "#".repeat(heading)),
                        theme.color("mdHeading"),
                    );
                    prefix.bold = true;
                    spans.push(prefix);
                }
            }
            Event::End(TagEnd::Heading(_)) => {
                heading = 0;
                flush(&mut spans, &mut lines, width, item_prefixes.last());
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                code = Some(match kind {
                    CodeBlockKind::Fenced(language) => {
                        language.split_whitespace().next().unwrap_or("").to_owned()
                    }
                    CodeBlockKind::Indented => String::new(),
                });
                lines.extend(wrap(
                    &[Span::new(
                        format!("```{}", code.as_deref().unwrap_or_default()),
                        theme.color("mdCodeBlockBorder"),
                    )],
                    width,
                ));
            }
            Event::End(TagEnd::CodeBlock) => {
                if let Some(language) = code.take() {
                    let syntax = SYNTAXES
                        .find_syntax_by_token(&language)
                        .unwrap_or_else(|| SYNTAXES.find_syntax_plain_text());
                    let syntax_theme = &SYNTAX_THEMES.themes[if theme.name == "light" {
                        "InspiredGitHub"
                    } else {
                        "base16-ocean.dark"
                    }];
                    let mut highlighter = HighlightLines::new(syntax, syntax_theme);
                    for line in code_text.lines() {
                        let pieces = highlighter
                            .highlight_line(line, &SYNTAXES)
                            .unwrap_or_default()
                            .into_iter()
                            .map(|(style, text)| {
                                Span::new(
                                    text,
                                    Color::Rgb {
                                        r: style.foreground.r,
                                        g: style.foreground.g,
                                        b: style.foreground.b,
                                    },
                                )
                            })
                            .collect::<Vec<_>>();
                        let indent = width.saturating_sub(1).min(2);
                        for mut line in wrap(&pieces, width.saturating_sub(indent)) {
                            line.spans.insert(
                                0,
                                Span::new(" ".repeat(indent), theme.color("mdCodeBlock")),
                            );
                            lines.push(line);
                        }
                    }
                    lines.extend(wrap(
                        &[Span::new("```", theme.color("mdCodeBlockBorder"))],
                        width,
                    ));
                    code_text.clear();
                }
            }
            Event::Start(Tag::List(start)) => lists.push(start),
            Event::End(TagEnd::List(_)) => {
                lists.pop();
            }
            Event::Start(Tag::Item) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                let prefix = match lists.last_mut() {
                    Some(Some(index)) => {
                        let prefix = format!("{index}. ");
                        *index += 1;
                        prefix
                    }
                    _ => "• ".into(),
                };
                let prefix = Span::new(
                    format!("{}{prefix}", "  ".repeat(lists.len().saturating_sub(1))),
                    theme.color("mdListBullet"),
                );
                item_prefixes.push(prefix.clone());
                spans.push(prefix);
            }
            Event::Start(Tag::BlockQuote(_)) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                let body = text[range]
                    .lines()
                    .map(|line| {
                        let trimmed = line.trim_start_matches(' ');
                        if line.len() - trimmed.len() <= 3 {
                            trimmed
                                .strip_prefix('>')
                                .map(|line| line.strip_prefix(' ').unwrap_or(line))
                                .unwrap_or(line)
                        } else {
                            line
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let bordered = width >= 3;
                let content_width = width.saturating_sub(if bordered { 2 } else { 0 });
                let quoted = if depth < 32 {
                    markdown_at_depth(&body, theme, content_width, depth + 1)
                } else {
                    wrap(&[Span::new(body, theme.color("mdQuote"))], content_width)
                };
                for mut line in quoted {
                    for span in &mut line.spans {
                        span.italic = true;
                        if span.color == theme.color("text") {
                            span.color = theme.color("mdQuote");
                        }
                    }
                    if bordered {
                        line.spans
                            .insert(0, Span::new("│ ", theme.color("mdQuoteBorder")));
                    }
                    lines.push(line);
                }
                quote_skip = 1;
            }
            Event::Text(text) | Event::Html(text) | Event::InlineHtml(text) => {
                if code.is_some() {
                    code_text.push_str(&text);
                } else {
                    let mut span = Span::new(
                        text,
                        theme.color(if heading > 0 {
                            "mdHeading"
                        } else if !links.is_empty() {
                            "mdLink"
                        } else {
                            "text"
                        }),
                    );
                    span.bold = bold > 0 || heading > 0;
                    span.underline = heading == 1;
                    span.italic = italic > 0;
                    span.strikethrough = strikethrough > 0;
                    spans.push(span);
                }
            }
            Event::Code(text) => {
                let mut span = Span::new(text, theme.color("mdCode"));
                span.underline = heading == 1;
                span.bold = heading > 0;
                spans.push(span);
            }
            Event::SoftBreak => spans.push(Span::new(" ", theme.color("text"))),
            Event::End(TagEnd::Item) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                item_prefixes.pop();
            }
            Event::HardBreak | Event::End(TagEnd::Paragraph) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last())
            }
            Event::Rule => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                lines.push(Line::plain(&"─".repeat(width.min(80)), theme.color("mdHr")));
            }
            Event::TaskListMarker(checked) => spans.push(Span::new(
                if checked { "☑ " } else { "☐ " },
                theme.color("mdListBullet"),
            )),
            _ => {}
        }
    }
    flush(&mut spans, &mut lines, width, item_prefixes.last());
    lines
}
fn flush(spans: &mut Vec<Span>, lines: &mut Vec<Line>, width: usize, prefix: Option<&Span>) {
    if spans.is_empty() {
        return;
    }
    if let Some(prefix) = prefix {
        let has_marker = spans.first() == Some(prefix);
        let indent = UnicodeWidthStr::width(prefix.text.as_str()).min(width.saturating_sub(1));
        let body = if has_marker {
            &spans[1..]
        } else {
            spans.as_slice()
        };
        for (index, mut line) in wrap(body, width.saturating_sub(indent))
            .into_iter()
            .enumerate()
        {
            let mut marker = prefix.clone();
            marker.text = if index == 0 && has_marker {
                marker.text.chars().take(indent).collect()
            } else {
                " ".repeat(indent)
            };
            line.spans.insert(0, marker);
            lines.push(line);
        }
    } else {
        lines.extend(wrap(spans, width));
    }
    spans.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn list_continuations_keep_indent_and_deep_quotes_are_bounded() {
        let theme = Theme::system();
        let lines = markdown("- abcdefghijkl", &theme, 8);
        let text = |line: &Line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        };
        assert_eq!(text(&lines[0]), "• abcdef");
        assert_eq!(text(&lines[1]), "  ghijkl");
        let deep = format!("{}text", "> ".repeat(128));
        let lines = markdown(&deep, &theme, 1);
        assert!(!lines.is_empty());
        assert!(lines.len() <= deep.len());
        assert!(
            lines
                .iter()
                .all(|line| UnicodeWidthStr::width(text(line).as_str()) <= 1)
        );
    }

    #[test]
    fn reference_headings_fences_and_quote_continuations() {
        let theme = Theme::system();
        let lines = markdown(
            "# Top\n\n### Third\n\n```rust\nlet x = 1;\n```\n\n> A long quoted sentence wraps.\n>\n> > nested quote\n",
            &theme,
            16,
        );
        let texts: Vec<_> = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>()
            })
            .collect();
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.text == "Top" && span.underline)
        );
        assert!(texts.iter().any(|line| line == "### Third"));
        assert!(texts.iter().any(|line| line == "```rust"));
        assert!(texts.iter().any(|line| line == "  let x = 1;"));
        assert!(texts.iter().any(|line| line == "```"));
        let quote_lines: Vec<_> = texts.iter().filter(|line| line.starts_with("│ ")).collect();
        assert!(quote_lines.len() >= 3, "{texts:?}");
        assert!(
            quote_lines.iter().any(|line| line.starts_with("│ │ ")),
            "{texts:?}"
        );
        assert!(
            texts
                .iter()
                .all(|line| UnicodeWidthStr::width(line.as_str()) <= 16)
        );
        for width in 1..=3 {
            let narrow = markdown("> > > nested quote", &theme, width);
            assert!(narrow.len() < 20);
            assert!(narrow.iter().all(|line| {
                line.spans
                    .iter()
                    .map(|span| UnicodeWidthStr::width(span.text.as_str()))
                    .sum::<usize>()
                    <= width
            }));
        }
        assert_eq!(
            markdown("---", &theme, 120)[0].spans[0].text.len() / "─".len(),
            80
        );
    }

    #[test]
    fn expanded_tools_keep_text_and_images_in_sequence() {
        use base64::Engine;
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::RgbaImage::from_pixel(1, 1, image::Rgba([255, 0, 0, 255]))
            .write_to(&mut bytes, image::ImageFormat::Png)
            .unwrap();
        let image = super::super::images::InlineImage::decode(&pi_rs::ImageContent {
            data: base64::engine::general_purpose::STANDARD.encode(bytes.into_inner()),
            mime_type: "image/png".into(),
        })
        .unwrap();
        let entry = Entry::Tool {
            id: "test".into(),
            name: "codemode".into(),
            arguments: "{}".into(),
            output: "before\n[image]\nafter".into(),
            failed: false,
            finished: true,
            numbered_diff: false,
            parts: vec![
                ToolPart::Text("before".into()),
                ToolPart::Image(image),
                ToolPart::Text("after".into()),
            ],
        };
        let lines = entry.render(&Theme::system(), 80, true, true, Some(60), 1);
        let before = lines
            .iter()
            .position(|line| line.spans.iter().any(|span| span.text.contains("before")))
            .unwrap();
        let image = lines.iter().position(|line| line.image.is_some()).unwrap();
        let after = lines
            .iter()
            .position(|line| line.spans.iter().any(|span| span.text.contains("after")))
            .unwrap();
        assert!(before < image && image < after);
        assert!(
            entry
                .render(&Theme::system(), 80, false, true, Some(60), 1)
                .iter()
                .all(|line| line.image.is_none())
        );
    }

    #[test]
    fn tables_align_unicode_cells_and_wrap_at_terminal_width() {
        let source = "| Name | Count |\n| :--- | ---: |\n| 日本語 | 12 |\n| long description | 3 |";
        for width in [5, 20, 50] {
            let lines = markdown(source, &Theme::system(), width);
            assert!(lines.iter().all(|line| {
                line.spans
                    .iter()
                    .map(|span| UnicodeWidthStr::width(span.text.as_str()))
                    .sum::<usize>()
                    <= width
            }));
            let text = lines
                .iter()
                .flat_map(|line| line.spans.iter().map(|span| span.text.as_str()))
                .collect::<String>();
            assert!(text.contains("12"));
            assert!(text.contains("日本"));
        }
        let lines = markdown("[Documentation](https://example.com)", &Theme::system(), 80);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.text.contains("https://example.com"))
        );
    }
    #[test]
    fn wraps_styles_without_splitting_graphemes_or_allowing_escapes() {
        let lines = wrap(&[Span::new("日本👩‍💻\x1b[31m", Color::Green)], 4);
        assert_eq!(lines[0].spans[0].text, "日本");
        assert!(
            lines
                .iter()
                .flat_map(|l| &l.spans)
                .all(|s| !s.text.contains('\x1b'))
        );
    }
    #[test]
    fn markdown_renders_headings_lists_and_highlighted_code() {
        let lines = markdown(
            "# Title\n\n**bold**\n\n- item\n\n```rust\nlet x = 1;\n```",
            &Theme::system(),
            80,
        );
        assert!(
            lines
                .iter()
                .flat_map(|l| &l.spans)
                .any(|s| s.bold && s.text == "Title")
        );
        assert!(
            lines
                .iter()
                .flat_map(|l| &l.spans)
                .any(|s| s.text.contains('•'))
        );
        assert!(
            lines
                .iter()
                .flat_map(|l| &l.spans)
                .any(|s| s.text.contains("let"))
        );
    }
}
