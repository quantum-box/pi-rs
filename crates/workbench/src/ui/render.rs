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
    pub dim: bool,
    pub italic: bool,
    pub underline: bool,
    pub inverse: bool,
    pub strikethrough: bool,
    pub hyperlink: Option<String>,
}
impl Span {
    pub(super) fn new(text: impl Into<String>, color: Color) -> Self {
        Self {
            text: clean(&text.into()),
            color,
            background: None,
            bold: false,
            dim: false,
            italic: false,
            underline: false,
            inverse: false,
            strikethrough: false,
            hyperlink: None,
        }
    }
    pub(super) fn themed(text: impl Into<String>, theme: &Theme, role: &str) -> Self {
        let mut span = Self::new(text, theme.color(role));
        span.dim = theme.is_dim(role);
        span
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
    pub fn themed(text: &str, theme: &Theme, role: &str) -> Self {
        Self {
            spans: vec![Span::themed(text, theme, role)],
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
            if let Some(url) = &span.hyperlink {
                queue!(out, Print(format!("\x1b]8;;{url}\x1b\\")))?;
            }
            queue!(
                out,
                SetForegroundColor(span.color),
                SetBackgroundColor(span.background.or(self.background).unwrap_or(Color::Reset)),
                SetAttribute(Attribute::NormalIntensity),
                SetAttribute(if span.bold {
                    Attribute::Bold
                } else {
                    Attribute::NormalIntensity
                }),
                SetAttribute(if span.dim {
                    Attribute::Dim
                } else if span.bold {
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
            if span.hyperlink.is_some() {
                queue!(out, Print("\x1b]8;;\x1b\\"))?;
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
pub enum SummaryKind {
    Compaction { tokens_before: u64 },
    Branch,
}

#[derive(Clone)]
pub enum Entry {
    Notice(String),
    Warning(String),
    User(String),
    Skill {
        text: String,
        expanded: Option<bool>,
    },
    Assistant(String),
    Thinking(String),
    Summary {
        id: u64,
        kind: SummaryKind,
        text: String,
        expanded: Option<bool>,
    },
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
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct TranscriptOptions {
    pub image_width: Option<usize>,
    pub padding: usize,
    pub mermaid: super::mermaid::Mermaid,
    pub expand_key: Option<crossterm::event::KeyEvent>,
}

impl Entry {
    pub fn user(text: String) -> Self {
        if crate::resources::skill_invocation(&text).is_some() {
            Self::Skill {
                text,
                expanded: None,
            }
        } else {
            Self::User(text)
        }
    }

    pub fn render(
        &self,
        theme: &Theme,
        width: usize,
        expand_tools: bool,
        show_thinking: bool,
        options: TranscriptOptions,
    ) -> Vec<Line> {
        let padding = options.padding.min(width.saturating_sub(1) / 2);
        let inner = width.saturating_sub(2 * padding).max(1);
        let mut lines = match self {
            Self::Notice(text) => return wrap(&[Span::themed(text, theme, "muted")], width),
            Self::Warning(text) => {
                return wrap(&[Span::themed(text, theme, "warning")], width);
            }
            Self::User(text) => user_lines(text, theme, inner, options),
            Self::Skill { text, expanded } => {
                let (name, body, message) =
                    crate::resources::skill_invocation(text).expect("skill invocation");

                let mut label = Span::themed("[skill]", theme, "customMessageLabel");
                label.bold = true;
                let color = theme.color("customMessageText");
                let mut lines = vec![Line::default()];
                if expanded.unwrap_or(expand_tools) {
                    lines.extend(wrap(&[label], inner));
                    let markdown = format!("**{name}**\n\n{body}");
                    let mut body = markdown_at_depth(
                        &markdown,
                        theme,
                        inner,
                        0,
                        options.mermaid,
                        *HYPERLINKS,
                        false,
                    );
                    for span in body.iter_mut().flat_map(|line| &mut line.spans) {
                        if span.color == theme.color("text") && !span.dim {
                            span.color = color;
                            span.dim = theme.is_dim("customMessageText");
                        }
                    }
                    lines.extend(body);
                } else {
                    let mut title = vec![
                        label,
                        Span::themed(format!(" {name}"), theme, "customMessageText"),
                    ];
                    if options.expand_key.is_some() {
                        let key = super::keys::binding_hint(options.expand_key);
                        title.push(Span::themed(format!(" ({key} to expand)"), theme, "dim"));
                    }
                    lines.extend(wrap(&title, inner));
                }
                lines.push(Line::default());
                for line in &mut lines {
                    line.background = Some(theme.color("customMessageBg"));
                }
                lines.push(Line::default());
                if let Some(message) = message {
                    lines.extend(user_lines(message, theme, inner, options));
                }
                lines
            }
            Self::Assistant(text) => {
                let mut lines =
                    markdown_at_depth(text, theme, inner, 0, options.mermaid, *HYPERLINKS, false);
                lines.push(Line::default());
                lines
            }
            Self::Summary {
                kind,
                text,
                expanded,
                ..
            } => {
                let (label, title) = match kind {
                    SummaryKind::Compaction { tokens_before } => {
                        let mut count = tokens_before.to_string();
                        let mut offset = count.len();
                        while offset > 3 {
                            offset -= 3;
                            count.insert(offset, ',');
                        }
                        ("[compaction]", format!("Compacted from {count} tokens"))
                    }
                    SummaryKind::Branch => ("[branch]", "Branch summary".into()),
                };
                let color = theme.color("customMessageText");
                let mut heading = Span::themed(label, theme, "customMessageLabel");
                heading.bold = true;
                let mut lines = vec![Line::default()];
                lines.extend(wrap(&[heading], inner));
                lines.push(Line::default());
                if expanded.unwrap_or(expand_tools) {
                    let markdown = format!("**{title}**\n\n{text}");
                    let mut body = markdown_at_depth(
                        &markdown,
                        theme,
                        inner,
                        0,
                        options.mermaid,
                        *HYPERLINKS,
                        false,
                    );
                    for span in body.iter_mut().flat_map(|line| &mut line.spans) {
                        if span.color == theme.color("text") && !span.dim {
                            span.color = color;
                            span.dim = theme.is_dim("customMessageText");
                        }
                    }
                    lines.extend(body);
                } else {
                    let mut hint = vec![Span::themed(title, theme, "customMessageText")];
                    if options.expand_key.is_some() {
                        hint.extend([
                            Span::themed(" (", theme, "customMessageText"),
                            Span::themed(
                                super::keys::binding_hint(options.expand_key),
                                theme,
                                "dim",
                            ),
                            Span::themed(" to expand)", theme, "customMessageText"),
                        ]);
                    }
                    lines.extend(wrap(&hint, inner));
                }
                lines.push(Line::default());
                for line in &mut lines {
                    line.background = Some(theme.color("customMessageBg"));
                }
                lines.push(Line::default());
                lines
            }
            Self::Image(image) => {
                return options
                    .image_width
                    .map_or_else(Vec::new, |limit| image.lines(width, limit, padding));
            }
            Self::Thinking(text) => {
                if !show_thinking {
                    return vec![];
                }
                let mut span = Span::themed(text, theme, "thinkingText");
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
                let mut title = Span::themed(format!("{name} {detail}"), theme, "toolTitle");
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
                            lines.extend(wrap(&[Span::themed(text, theme, "toolOutput")], inner));
                        }
                    }
                };
                if expand_tools
                    && !parts.is_empty()
                    && let Some(limit) = options.image_width
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
                    lines.push(Line::themed(
                        &format!(
                            "… {} more lines (ctrl+o to expand)",
                            output_lines.len() - limit
                        ),
                        theme,
                        "dim",
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

fn detect_hyperlinks(get: impl Fn(&str) -> Option<String>, tmux: impl Fn() -> bool) -> bool {
    match get("PI_HYPERLINKS").as_deref() {
        Some("1") => return true,
        Some("0") => return false,
        _ => {}
    }
    let term = get("TERM").unwrap_or_default().to_lowercase();
    if get("TMUX").is_some() || term.starts_with("tmux") {
        return tmux();
    }
    if term.starts_with("screen") {
        return false;
    }
    let program = get("TERM_PROGRAM").unwrap_or_default().to_lowercase();
    if matches!(
        program.as_str(),
        "herdr"
            | "rex"
            | "kitty"
            | "ghostty"
            | "wezterm"
            | "warpterminal"
            | "iterm.app"
            | "alacritty"
            | "vscode"
            | "zed"
    ) {
        return true;
    }
    if program == "orca" && get("ORCA_IMAGE_PROTOCOL").as_deref() == Some("kitty") {
        return true;
    }
    term.contains("ghostty")
        || [
            "KITTY_WINDOW_ID",
            "GHOSTTY_RESOURCES_DIR",
            "WEZTERM_PANE",
            "WARP_SESSION_ID",
            "WARP_TERMINAL_SESSION_UUID",
            "ITERM_SESSION_ID",
            "WT_SESSION",
        ]
        .iter()
        .any(|name| get(name).is_some())
}

fn tmux_hyperlinks() -> bool {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    let Ok(mut child) = Command::new("tmux")
        .args(["display-message", "-p", "#{client_termfeatures}"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
    else {
        return false;
    };
    let deadline = Instant::now() + Duration::from_millis(250);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut value = String::new();
                if status.success()
                    && let Some(mut output) = child.stdout.take()
                    && output.read_to_string(&mut value).is_ok()
                {
                    return value
                        .split(',')
                        .any(|feature| feature.trim() == "hyperlinks");
                }
                return false;
            }
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

static HYPERLINKS: LazyLock<bool> =
    LazyLock::new(|| detect_hyperlinks(|name| std::env::var(name).ok(), tmux_hyperlinks));

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
            Line::themed(
                &format!(
                    "{left}{}{right}",
                    widths
                        .iter()
                        .map(|width| "─".repeat(width + 2))
                        .collect::<Vec<_>>()
                        .join(middle)
                ),
                theme,
                "borderMuted",
            )
        };
        let mut lines = vec![border("┌", "┬", "┐")];
        let row_count = self.rows.len();
        for (index, row) in self.rows.into_iter().enumerate() {
            let cells: Vec<_> = widths
                .iter()
                .enumerate()
                .map(|(column, width)| {
                    wrap(row.get(column).map(Vec::as_slice).unwrap_or(&[]), *width)
                })
                .collect();
            for line_index in 0..cells.iter().map(Vec::len).max().unwrap_or(1) {
                let mut line = Line::themed("│", theme, "borderMuted");
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
                        .push(Span::themed(" ".repeat(left + 1), theme, "text"));
                    line.spans.extend_from_slice(spans);
                    line.spans.push(Span::themed(
                        format!("{}│", " ".repeat(padding - left + 1)),
                        theme,
                        "borderMuted",
                    ));
                }
                lines.push(line);
            }
            if index == 0 || index + 1 < row_count {
                lines.push(border("├", "┼", "┤"));
            }
        }
        lines.push(border("└", "┴", "┘"));
        lines
    }
}

fn user_lines(text: &str, theme: &Theme, width: usize, options: TranscriptOptions) -> Vec<Line> {
    let mut lines = markdown_at_depth(text, theme, width, 0, options.mermaid, *HYPERLINKS, true);
    for line in &mut lines {
        line.background = Some(theme.color("userMessageBg"));
        for span in &mut line.spans {
            if span.color == theme.color("text") && !span.dim {
                span.color = theme.color("userMessageText");
                span.dim = theme.is_dim("userMessageText");
            }
        }
    }
    lines.push(Line::default());
    lines
}

#[cfg(test)]
fn markdown(text: &str, theme: &Theme, width: usize) -> Vec<Line> {
    markdown_at_depth(
        text,
        theme,
        width,
        0,
        super::mermaid::Mermaid::Off,
        false,
        false,
    )
}

fn safe_hyperlink(url: &str) -> bool {
    !url.is_empty() && !url.chars().any(char::is_control)
}

fn original_list_marker(source: &str, ordered: bool) -> Option<String> {
    let line = source.trim_start_matches(' ');
    let bytes = line.as_bytes();
    let end = if ordered {
        let digits = bytes
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digits == 0 || digits > 9 || !matches!(bytes.get(digits), Some(b'.' | b')')) {
            return None;
        }
        digits + 1
    } else if matches!(bytes.first(), Some(b'-' | b'+' | b'*')) {
        1
    } else {
        return None;
    };
    if bytes
        .get(end)
        .is_some_and(|byte| !byte.is_ascii_whitespace())
    {
        return None;
    }
    Some(format!("{} ", &line[..end]))
}

// Pulldown-cmark emits paragraph events inside loose list items, but does not
// expose the list's looseness directly. Record it before rendering so every
// pair of items receives the same spacing, including the first pair.
fn loose_list_offsets(text: &str, options: Options) -> std::collections::HashSet<usize> {
    let mut loose = std::collections::HashSet::new();
    let mut lists = Vec::new();
    let mut quote_depth = 0usize;
    for (event, range) in Parser::new_ext(text, options).into_offset_iter() {
        match event {
            Event::Start(Tag::List(_)) => lists.push((range.start, quote_depth)),
            Event::End(TagEnd::List(_)) => {
                lists.pop();
            }
            Event::Start(Tag::BlockQuote(_)) => quote_depth += 1,
            Event::End(TagEnd::BlockQuote(_)) => quote_depth -= 1,
            Event::Start(Tag::Paragraph) => {
                if let Some(&(offset, list_quote_depth)) = lists.last()
                    && quote_depth == list_quote_depth
                {
                    loose.insert(offset);
                }
            }
            _ => {}
        }
    }
    loose
}

fn markdown_at_depth(
    text: &str,
    theme: &Theme,
    width: usize,
    depth: usize,
    mermaid: super::mermaid::Mermaid,
    hyperlinks: bool,
    preserve_source: bool,
) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut bold = 0usize;
    let mut italic = 0usize;
    let mut strikethrough = 0usize;
    let mut heading = 0usize;
    let mut quote_skip = 0usize;
    let mut code: Option<String> = None;
    let mut code_text = String::new();
    let mut lists: Vec<(Option<u64>, bool, bool)> = Vec::new();
    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let loose_lists = loose_list_offsets(text, options);
    let mut item_prefixes = Vec::new();
    let mut table: Option<Table> = None;
    let mut links = Vec::new();
    let mut previous_paragraph_end = None;
    let mut last_heading_separator = None;
    for (event, range) in Parser::new_ext(text, options).into_offset_iter() {
        if quote_skip > 0 {
            match event {
                Event::Start(Tag::BlockQuote(_)) => quote_skip += 1,
                Event::End(TagEnd::BlockQuote(_)) => quote_skip -= 1,
                _ => {}
            }
            continue;
        }
        match event {
            Event::Start(Tag::Paragraph) if lists.is_empty() => {
                if let Some(end) = previous_paragraph_end.take()
                    && end <= range.start
                    && text[end..range.start].trim().is_empty()
                {
                    lines.push(Line::default());
                }
            }
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
            Event::Start(Tag::Link { dest_url, .. }) => {
                links.push((dest_url.into_string(), String::new()));
            }
            Event::End(TagEnd::Link) => {
                if let Some((url, label)) = links.pop() {
                    let comparable_url = url.strip_prefix("mailto:").unwrap_or(&url);
                    if (!hyperlinks || !safe_hyperlink(&url))
                        && label != url
                        && label != comparable_url
                    {
                        spans.push(Span::themed(format!(" ({url})"), theme, "mdLinkUrl"));
                    }
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
                previous_paragraph_end = None;
                heading = level as usize;
                if heading >= 3 {
                    let mut prefix =
                        Span::themed(format!("{} ", "#".repeat(heading)), theme, "mdHeading");
                    prefix.bold = true;
                    spans.push(prefix);
                }
            }
            Event::End(TagEnd::Heading(_)) => {
                heading = 0;
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                if lists.is_empty() {
                    last_heading_separator = Some(lines.len());
                    lines.push(Line::default());
                }
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                code = Some(match kind {
                    CodeBlockKind::Fenced(language) => {
                        language.split_whitespace().next().unwrap_or("").to_owned()
                    }
                    CodeBlockKind::Indented => String::new(),
                });
            }
            Event::End(TagEnd::CodeBlock) => {
                if let Some(language) = code.take() {
                    let (art, warning) = if depth == 0
                        && lists.is_empty()
                        && language.eq_ignore_ascii_case("mermaid")
                    {
                        super::mermaid::render(&code_text, theme, width, mermaid)
                    } else {
                        (None, None)
                    };
                    if let Some(art) = art {
                        lines.extend(art);
                        code_text.clear();
                        continue;
                    }
                    lines.extend(wrap(
                        &[Span::themed(
                            format!("```{language}"),
                            theme,
                            "mdCodeBlockBorder",
                        )],
                        width,
                    ));
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
                            line.spans
                                .insert(0, Span::themed(" ".repeat(indent), theme, "mdCodeBlock"));
                            lines.push(line);
                        }
                    }
                    lines.extend(wrap(
                        &[Span::themed("```", theme, "mdCodeBlockBorder")],
                        width,
                    ));
                    if let Some(warning) = warning {
                        lines.extend(wrap(&[Span::themed(warning, theme, "warning")], width));
                    }
                    code_text.clear();
                }
            }
            Event::Start(Tag::List(start)) => {
                lists.push((start, loose_lists.contains(&range.start), false));
            }
            Event::End(TagEnd::List(_)) => {
                lists.pop();
            }
            Event::Start(Tag::Item) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                let ordered = matches!(lists.last(), Some((Some(_), _, _)));
                if let Some((_, loose, seen)) = lists.last_mut() {
                    if *loose && *seen {
                        lines.push(Line::default());
                    }
                    *seen = true;
                }
                let default_prefix = match lists.last_mut() {
                    Some((Some(index), _, _)) => {
                        let prefix = format!("{index}. ");
                        *index += 1;
                        prefix
                    }
                    _ => "- ".into(),
                };
                let prefix = if preserve_source {
                    original_list_marker(&text[range.clone()], ordered).unwrap_or(default_prefix)
                } else {
                    default_prefix
                };
                let prefix = Span::themed(
                    format!("{}{prefix}", "    ".repeat(lists.len().saturating_sub(1))),
                    theme,
                    "mdListBullet",
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
                    markdown_at_depth(
                        &body,
                        theme,
                        content_width,
                        depth + 1,
                        mermaid,
                        hyperlinks,
                        preserve_source,
                    )
                } else {
                    wrap(&[Span::themed(body, theme, "mdQuote")], content_width)
                };
                for mut line in quoted {
                    for span in &mut line.spans {
                        span.italic = true;
                        if span.color == theme.color("text") && !span.dim {
                            span.color = theme.color("mdQuote");
                            span.dim = theme.is_dim("mdQuote");
                        }
                    }
                    if bordered {
                        line.spans
                            .insert(0, Span::themed("│ ", theme, "mdQuoteBorder"));
                    }
                    lines.push(line);
                }
                quote_skip = 1;
            }
            Event::Text(content) | Event::Html(content) | Event::InlineHtml(content) => {
                if code.is_some() {
                    code_text.push_str(&content);
                } else {
                    let preceding = text.as_bytes()[..range.start]
                        .iter()
                        .rev()
                        .take_while(|byte| **byte == b'\\')
                        .count();
                    let displayed = if preserve_source && preceding % 2 == 1 {
                        &text[range.start - 1..range.end]
                    } else if preserve_source && text[range.clone()].starts_with('\\') {
                        &text[range]
                    } else {
                        content.as_ref()
                    };
                    if let Some((_, label)) = links.last_mut() {
                        label.push_str(displayed);
                    }
                    let mut span = Span::themed(
                        displayed,
                        theme,
                        if heading > 0 {
                            "mdHeading"
                        } else if !links.is_empty() {
                            "mdLink"
                        } else {
                            "text"
                        },
                    );
                    span.bold = bold > 0 || heading > 0;
                    span.underline = heading == 1 || !links.is_empty();
                    span.hyperlink = links.last().and_then(|(url, _)| {
                        (hyperlinks && safe_hyperlink(url)).then(|| url.clone())
                    });
                    span.italic = italic > 0;
                    span.strikethrough = strikethrough > 0;
                    spans.push(span);
                }
            }
            Event::Code(text) => {
                if let Some((_, label)) = links.last_mut() {
                    label.push_str(&text);
                }
                let mut span = Span::themed(text, theme, "mdCode");
                span.underline = heading == 1 || !links.is_empty();
                span.hyperlink = links
                    .last()
                    .and_then(|(url, _)| (hyperlinks && safe_hyperlink(url)).then(|| url.clone()));
                span.bold = heading > 0;
                spans.push(span);
            }
            Event::SoftBreak => {
                if let Some((_, label)) = links.last_mut() {
                    label.push(' ');
                }
                let mut span = Span::themed(" ", theme, "text");
                span.hyperlink = links
                    .last()
                    .and_then(|(url, _)| (hyperlinks && safe_hyperlink(url)).then(|| url.clone()));
                spans.push(span);
            }
            Event::End(TagEnd::Item) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                item_prefixes.pop();
            }
            Event::End(TagEnd::Paragraph) => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                if lists.is_empty() {
                    previous_paragraph_end = Some(range.end);
                }
            }
            Event::HardBreak => flush(&mut spans, &mut lines, width, item_prefixes.last()),
            Event::Rule => {
                flush(&mut spans, &mut lines, width, item_prefixes.last());
                lines.push(Line::themed(&"─".repeat(width.min(80)), theme, "mdHr"));
            }
            Event::TaskListMarker(checked) => {
                let marker = if checked { "[x] " } else { "[ ] " };
                if let Some(prefix) = item_prefixes.last_mut() {
                    if spans.first() == Some(prefix) {
                        spans[0].text.push_str(marker);
                    }
                    prefix.text.push_str(marker);
                } else {
                    spans.push(Span::themed(marker, theme, "mdListBullet"));
                }
            }
            _ => {}
        }
    }
    flush(&mut spans, &mut lines, width, item_prefixes.last());
    // Source after a heading can consist solely of non-rendered link definitions.
    if last_heading_separator == lines.len().checked_sub(1) && last_heading_separator.is_some() {
        lines.pop();
    }
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
    fn markdown_separates_headings_from_following_blocks() {
        for (source, expected) in [
            ("# Hello\nworld", vec!["Hello", "", "world"]),
            ("# Hello\n\nworld", vec!["Hello", "", "world"]),
            ("# Hello\n## Next", vec!["Hello", "", "Next"]),
            ("# Hello\n\n", vec!["Hello"]),
        ] {
            let rendered: Vec<String> = markdown(source, &Theme::system(), 80)
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            assert_eq!(rendered, expected, "{source:?}");
        }
    }
    #[test]
    fn markdown_heading_ignores_nonrendered_reference_definitions() {
        for (source, expected) in [
            ("# Hello\n\n[ref]: https://example.com", vec!["Hello"]),
            (
                "# Hello\n\n[ref]: https://example.com\n\nworld",
                vec!["Hello", "", "world"],
            ),
            (
                "# First\n\n## Last\n\n[ref]: https://example.com",
                vec!["First", "", "Last"],
            ),
        ] {
            let rendered: Vec<String> = markdown(source, &Theme::system(), 80)
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            assert_eq!(rendered, expected, "{source:?}");
        }
    }
    #[test]
    fn markdown_keeps_blank_lines_between_paragraphs() {
        let theme = Theme::system();
        for source in [
            "first\n\nsecond",
            "first\n\n\nsecond",
            "> first\n>\n> second",
        ] {
            let lines = markdown(source, &theme, 80);
            let rendered: Vec<String> = lines
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            if source.starts_with('>') {
                assert_eq!(rendered, ["│ first", "│ ", "│ second"]);
            } else {
                assert_eq!(rendered, ["first", "", "second"]);
            }
        }
        let lines = markdown("first  \nsecond", &theme, 80);
        assert_eq!(lines.len(), 2);
    }
    #[test]
    fn markdown_spaces_loose_list_items_at_each_depth() {
        let theme = Theme::system();
        for (source, expected) in [
            (
                "- first\n\n- second\n- third",
                vec!["- first", "", "- second", "", "- third"],
            ),
            ("- first\n- second", vec!["- first", "- second"]),
            (
                "- outer\n  - one\n\n  - two\n- next",
                vec!["- outer", "    - one", "", "    - two", "- next"],
            ),
            ("- > quote\n- next", vec!["- ", "│ quote", "- next"]),
        ] {
            let actual: Vec<String> = markdown(source, &theme, 80)
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            assert_eq!(actual, expected, "{source}");
        }
    }

    #[test]
    fn system_dim_roles_survive_wrapping_and_emit_dim_attributes() {
        let system = Theme::system();
        let spans = [
            Span::themed("muted text", &system, "muted"),
            Span::themed("normal", &system, "text"),
        ];
        let lines = wrap(&spans, 6);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.dim && span.text.contains("muted"))
        );
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| !span.dim && !span.text.is_empty())
        );
        let mut output = Vec::new();
        lines[0].draw(&mut output, 6).unwrap();
        assert!(String::from_utf8(output).unwrap().contains("\x1b[2m"));
        let custom = Theme::parse(include_str!("../../assets/themes/dark.json"), None).unwrap();
        assert!(!Span::themed("muted", &custom, "muted").dim);
    }

    #[test]
    fn skill_invocation_hides_the_body_and_keeps_user_text_visible() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let entry = Entry::user(
            "<skill name=\"fixture\" location=\"/tmp/SKILL.md\">\n**skill body**\n</skill>\n\nUser request".into(),
        );
        let theme = Theme::system();
        let options = TranscriptOptions {
            expand_key: Some(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::CONTROL)),
            ..TranscriptOptions::default()
        };
        let collapsed = entry.render(&theme, 80, false, true, options);
        let text: String = collapsed
            .iter()
            .flat_map(|line| &line.spans)
            .map(|span| span.text.as_str())
            .collect();
        assert!(text.contains("[skill] fixture (Ctrl+G to expand)"));
        assert!(!text.contains("skill body"));
        assert!(collapsed.iter().any(|line| {
            line.background == Some(theme.color("userMessageBg"))
                && line.spans.iter().any(|span| span.text == "User request")
        }));
        let expanded = entry.render(&theme, 80, true, true, options);
        assert!(
            expanded
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.bold && span.text == "skill body")
        );
    }

    #[test]
    fn summaries_fold_the_markdown_and_use_the_configured_expand_key() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let options = TranscriptOptions {
            padding: 1,
            expand_key: Some(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::CONTROL)),
            ..TranscriptOptions::default()
        };
        let mut entry = Entry::Summary {
            id: 7,
            kind: SummaryKind::Compaction {
                tokens_before: 12_345,
            },
            text: "**recovered context**".into(),
            expanded: None,
        };
        let theme = Theme::system();
        let render = |entry: &Entry, expand| entry.render(&theme, 72, expand, true, options);
        let folded = render(&entry, false);
        let text: String = folded
            .iter()
            .flat_map(|line| &line.spans)
            .map(|span| span.text.as_str())
            .collect();
        assert!(text.contains("Compacted from 12,345 tokens (Ctrl+G to expand)"));
        assert!(!text.contains("recovered context"));
        assert!(
            folded
                .iter()
                .any(|line| line.background == Some(theme.color("customMessageBg")))
        );
        if let Entry::Summary { expanded, .. } = &mut entry {
            *expanded = Some(true);
        }
        assert!(
            render(&entry, false)
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.bold && span.text == "recovered context")
        );
    }

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
        assert_eq!(text(&lines[0]), "- abcdef");
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
    fn user_markdown_preserves_list_markers_and_wraps_tasks() {
        let theme = Theme::system();
        let options = TranscriptOptions::default();
        let lines = user_lines("+ **bold**\n+ [x] abcdefgh", &theme, 10, options);
        let rendered: Vec<String> = lines
            .iter()
            .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
            .collect();
        assert_eq!(rendered[0], "+ bold");
        assert!(
            lines[0]
                .spans
                .iter()
                .any(|span| span.text == "bold" && span.bold)
        );
        assert_eq!(rendered[1], "+ [x] abcd");
        assert_eq!(rendered[2], "      efgh");
        assert!(
            lines[..3]
                .iter()
                .all(|line| line.background == Some(theme.color("userMessageBg")))
        );
        let assistant = markdown("- [ ] pending\n  - child", &theme, 40);
        let rendered: Vec<String> = assistant
            .iter()
            .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
            .collect();
        assert_eq!(rendered, ["- [ ] pending", "    - child"]);
        let escaped = user_lines(r"\*literal\*", &theme, 40, options);
        let text: String = escaped
            .iter()
            .flat_map(|line| &line.spans)
            .map(|span| span.text.as_str())
            .collect();
        assert_eq!(text, r"\*literal\*");
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
        let lines = entry.render(
            &Theme::system(),
            80,
            true,
            true,
            TranscriptOptions {
                image_width: Some(60),
                padding: 1,
                ..Default::default()
            },
        );
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
                .render(
                    &Theme::system(),
                    80,
                    false,
                    true,
                    TranscriptOptions {
                        image_width: Some(60),
                        padding: 1,
                        ..Default::default()
                    }
                )
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
    fn markdown_tables_separate_data_rows_after_wrapped_cells() {
        let source = "| Name | Count |\n| :--- | ---: |\n| long description | 12 |\n| 日本語 | 3 |\n| last | 40 |";
        let lines = markdown(source, &Theme::system(), 20);
        let text: Vec<String> = lines
            .iter()
            .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
            .collect();
        let dividers: Vec<_> = text
            .iter()
            .enumerate()
            .filter(|(_, line)| line.starts_with('├'))
            .map(|(index, _)| index)
            .collect();
        assert_eq!(dividers.len(), 3, "{text:#?}");
        assert!(
            text[dividers[0] + 1..dividers[1]].len() > 1,
            "first row should wrap: {text:#?}"
        );
        assert!(
            text[dividers[1] + 1..dividers[2]]
                .iter()
                .any(|line| line.contains("日本")),
            "{text:#?}"
        );
        assert!(text[dividers[2] + 1].contains("last"), "{text:#?}");
        assert!(text.last().unwrap().starts_with('└'), "{text:#?}");
        assert!(
            text.iter()
                .all(|line| UnicodeWidthStr::width(line.as_str()) <= 20)
        );
        for source in ["| H |\n| - |", "| H |\n| - |\n| one |"] {
            let lines = markdown(source, &Theme::system(), 30);
            assert_eq!(
                lines
                    .iter()
                    .filter(|line| line
                        .spans
                        .iter()
                        .map(|span| span.text.as_str())
                        .collect::<String>()
                        .starts_with('├'))
                    .count(),
                1,
                "{source}"
            );
        }
    }

    #[test]
    fn markdown_links_do_not_duplicate_their_visible_destination() {
        for (source, expected) in [
            ("<https://example.com>", "https://example.com"),
            (
                "[https://example.com](https://example.com)",
                "https://example.com",
            ),
            ("<hello@example.com>", "hello@example.com"),
            (
                "[**docs**](https://example.com)",
                "docs (https://example.com)",
            ),
        ] {
            let lines = markdown(source, &Theme::system(), 100);
            let spans = lines
                .iter()
                .flat_map(|line| &line.spans)
                .collect::<Vec<_>>();
            assert_eq!(
                spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>(),
                expected
            );
            assert!(spans[0].underline, "{source}");
        }
    }

    #[test]
    fn osc8_links_wrap_and_close_before_padding() {
        let lines = markdown_at_depth(
            "[**documentation**](https://example.com)",
            &Theme::system(),
            6,
            0,
            super::super::mermaid::Mermaid::Off,
            true,
            false,
        );
        assert!(lines.len() >= 2);
        assert_eq!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .map(|s| s.text.as_str())
                .collect::<String>(),
            "documentation"
        );
        for line in lines
            .iter()
            .filter(|line| line.spans.iter().any(|span| !span.text.is_empty()))
        {
            let mut bytes = Vec::new();
            line.draw(&mut bytes, 6).unwrap();
            let output = String::from_utf8(bytes).unwrap();
            assert!(output.contains("\x1b]8;;https://example.com\x1b\\"));
            assert!(output.contains("\x1b]8;;\x1b\\"));
            assert!(!output.contains(" (https://example.com)"));
        }
        assert!(!safe_hyperlink("https://example.com/\x1b]0;bad"));
    }

    #[test]
    fn detects_hyperlinks_conservatively_with_explicit_override() {
        let detect = |values: &[(&str, &str)], tmux: bool| {
            detect_hyperlinks(
                |key| {
                    values
                        .iter()
                        .find(|(name, _)| *name == key)
                        .map(|(_, value)| (*value).to_owned())
                },
                || tmux,
            )
        };
        assert!(detect(&[("TERM_PROGRAM", "iTerm.app")], false));
        assert!(!detect(
            &[("TERM", "screen-256color"), ("TERM_PROGRAM", "iTerm.app")],
            true
        ));
        assert!(!detect(
            &[("TMUX", "1"), ("TERM_PROGRAM", "iTerm.app")],
            false
        ));
        assert!(detect(&[("TMUX", "1")], true));
        assert!(detect(&[("TERM", "screen"), ("PI_HYPERLINKS", "1")], false));
        assert!(!detect(
            &[("TERM_PROGRAM", "kitty"), ("PI_HYPERLINKS", "0")],
            true
        ));
        assert!(!detect(&[], false));
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
        assert!(lines.iter().flat_map(|l| &l.spans).any(|s| s.text == "- "));
        assert!(
            lines
                .iter()
                .flat_map(|l| &l.spans)
                .any(|s| s.text.contains("let"))
        );
    }
}
