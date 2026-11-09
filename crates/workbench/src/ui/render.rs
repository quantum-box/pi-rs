//! Styled, terminal-safe transcript rendering shared by all themes.
use std::io::Write;
use std::sync::LazyLock;

use crossterm::{
    queue,
    style::{
        Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    },
};
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
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
}
impl Span {
    fn new(text: impl Into<String>, color: Color) -> Self {
        Self {
            text: clean(&text.into()),
            color,
            bold: false,
            italic: false,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Line {
    pub spans: Vec<Span>,
    pub background: Option<Color>,
}
impl Line {
    pub fn plain(text: &str, color: Color) -> Self {
        Self {
            spans: vec![Span::new(text, color)],
            background: None,
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
    Tool {
        id: String,
        name: String,
        arguments: String,
        output: String,
        failed: bool,
        finished: bool,
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

pub fn markdown(text: &str, theme: &Theme, width: usize) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut bold = 0usize;
    let mut italic = 0usize;
    let mut heading = false;
    let mut code: Option<String> = None;
    let mut code_text = String::new();
    let mut lists: Vec<Option<u64>> = Vec::new();
    for event in Parser::new_ext(
        text,
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    ) {
        match event {
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
                        theme.color(if heading { "mdHeading" } else { "text" }),
                    );
                    span.bold = bold > 0 || heading;
                    span.italic = italic > 0;
                    spans.push(span);
                }
            }
            Event::Code(text) => spans.push(Span::new(text, theme.color("mdCode"))),
            Event::SoftBreak => spans.push(Span::new(" ", theme.color("text"))),
            Event::HardBreak | Event::End(TagEnd::Paragraph | TagEnd::Item | TagEnd::TableRow) => {
                flush(&mut spans, &mut lines, width)
            }
            Event::End(TagEnd::TableCell) => {
                spans.push(Span::new(" │ ", theme.color("borderMuted")))
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
