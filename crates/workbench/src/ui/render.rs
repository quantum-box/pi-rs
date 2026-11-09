//! Styled, terminal-safe transcript rendering shared by all themes.
use std::io::Write;
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
    pub bold: bool,
    pub italic: bool,
    pub strikethrough: bool,
}
impl Span {
    fn new(text: impl Into<String>, color: Color) -> Self {
        Self {
            text: clean(&text.into()),
            color,
            bold: false,
            italic: false,
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
            Print(" ".repeat(width.saturating_sub(used))),
            ResetColor
        )
    }
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
        images: Vec<std::sync::Arc<super::images::InlineImage>>,
    },
}
impl Entry {
    pub fn render(
        &self,
        theme: &Theme,
        width: usize,
        expand_tools: bool,
        show_thinking: bool,
    ) -> Vec<Line> {
        let inner = width.saturating_sub(2).max(1);
        match self {
            Self::Notice(text) => wrap(&[Span::new(text, theme.color("muted"))], width),
            Self::User(text) => {
                let mut lines = wrap(&[Span::new(text, theme.color("userMessageText"))], inner);
                for line in &mut lines {
                    line.background = Some(theme.color("userMessageBg"));
                    line.spans
                        .insert(0, Span::new(" ", theme.color("userMessageText")));
                }
                lines.push(Line::default());
                lines
            }
            Self::Assistant(text) => {
                let mut lines = markdown(text, theme, width);
                lines.push(Line::default());
                lines
            }
            Self::Image(image) => image.lines(width),
            Self::Thinking(text) => {
                if !show_thinking {
                    return vec![];
                }
                let mut span = Span::new(text, theme.color("thinkingText"));
                span.italic = true;
                let mut lines = wrap(&[span], width);
                lines.push(Line::default());
                lines
            }
            Self::Tool {
                name,
                arguments,
                output,
                failed,
                finished,
                images,
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
                for text in output_lines.iter().take(limit) {
                    let color = if name == "edit" && text.starts_with('+') {
                        "toolDiffAdded"
                    } else if name == "edit" && text.starts_with('-') {
                        "toolDiffRemoved"
                    } else {
                        "toolOutput"
                    };
                    lines.extend(wrap(&[Span::new(*text, theme.color(color))], inner));
                }
                if expand_tools {
                    for image in images {
                        lines.extend(image.lines(inner));
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
                    line.spans
                        .insert(0, Span::new(" ", theme.color("toolOutput")));
                }
                lines.push(Line::default());
                lines
            }
        }
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
    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut bold = 0usize;
    let mut italic = 0usize;
    let mut strikethrough = 0usize;
    let mut heading = false;
    let mut code: Option<String> = None;
    let mut code_text = String::new();
    let mut lists: Vec<Option<u64>> = Vec::new();
    let mut table: Option<Table> = None;
    let mut links = Vec::new();
    for event in Parser::new_ext(
        text,
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    ) {
        match event {
            Event::Start(Tag::Table(alignments)) => {
                flush(&mut spans, &mut lines, width);
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
            Event::Start(Tag::Heading { .. }) => heading = true,
            Event::End(TagEnd::Heading(_)) => {
                heading = false;
                flush(&mut spans, &mut lines, width);
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                flush(&mut spans, &mut lines, width);
                code = Some(match kind {
                    CodeBlockKind::Fenced(language) => {
                        language.split_whitespace().next().unwrap_or("").to_owned()
                    }
                    CodeBlockKind::Indented => String::new(),
                });
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
                        lines.extend(wrap(&pieces, width));
                    }
                    code_text.clear();
                }
            }
            Event::Start(Tag::List(start)) => lists.push(start),
            Event::End(TagEnd::List(_)) => {
                lists.pop();
            }
            Event::Start(Tag::Item) => {
                flush(&mut spans, &mut lines, width);
                let prefix = match lists.last_mut() {
                    Some(Some(index)) => {
                        let prefix = format!("{index}. ");
                        *index += 1;
                        prefix
                    }
                    _ => "• ".into(),
                };
                spans.push(Span::new(
                    format!("{}{prefix}", "  ".repeat(lists.len().saturating_sub(1))),
                    theme.color("mdListBullet"),
                ));
            }
            Event::Start(Tag::BlockQuote(_)) => {
                spans.push(Span::new("│ ", theme.color("mdQuoteBorder")))
            }
            Event::Text(text) | Event::Html(text) | Event::InlineHtml(text) => {
                if code.is_some() {
                    code_text.push_str(&text);
                } else {
                    let mut span = Span::new(
                        text,
                        theme.color(if heading {
                            "mdHeading"
                        } else if !links.is_empty() {
                            "mdLink"
                        } else {
                            "text"
                        }),
                    );
                    span.bold = bold > 0 || heading;
                    span.italic = italic > 0;
                    span.strikethrough = strikethrough > 0;
                    spans.push(span);
                }
            }
            Event::Code(text) => spans.push(Span::new(text, theme.color("mdCode"))),
            Event::SoftBreak => spans.push(Span::new(" ", theme.color("text"))),
            Event::HardBreak | Event::End(TagEnd::Paragraph | TagEnd::Item) => {
                flush(&mut spans, &mut lines, width)
            }
            Event::Rule => {
                flush(&mut spans, &mut lines, width);
                lines.push(Line::plain(&"─".repeat(width), theme.color("mdHr")));
            }
            Event::TaskListMarker(checked) => spans.push(Span::new(
                if checked { "☑ " } else { "☐ " },
                theme.color("mdListBullet"),
            )),
            _ => {}
        }
    }
    flush(&mut spans, &mut lines, width);
    lines
}
fn flush(spans: &mut Vec<Span>, lines: &mut Vec<Line>, width: usize) {
    if !spans.is_empty() {
        lines.extend(wrap(spans, width));
        spans.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
