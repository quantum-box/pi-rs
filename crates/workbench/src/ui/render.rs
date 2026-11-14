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
use syntect::{
    easy::HighlightLines,
    highlighting::{Color as SyntaxColor, StyleModifier, Theme as SyntaxTheme, ThemeItem},
    parsing::SyntaxSet,
};
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
// Syntect's synthetic colors identify semantic roles; the active Pi theme is
// applied afterwards so indexed terminal colors and dim styling survive.
const SYNTAX_ROLES: &[(&str, &str)] = &[
    ("comment, punctuation.definition.comment", "syntaxComment"),
    (
        "keyword, storage.modifier, storage.type.function, storage.type.js",
        "syntaxKeyword",
    ),
    ("entity.name.function, support.function", "syntaxFunction"),
    ("variable, entity.other.attribute-name", "syntaxVariable"),
    ("string, punctuation.definition.string", "syntaxString"),
    ("constant.numeric, constant.language", "syntaxNumber"),
    (
        "storage.type, entity.name.type, entity.name.class, support.type, support.class",
        "syntaxType",
    ),
    ("keyword.operator", "syntaxOperator"),
    ("punctuation", "syntaxPunctuation"),
    ("markup.inserted", "toolDiffAdded"),
    ("markup.deleted", "toolDiffRemoved"),
];
static SYNTAX_THEME: LazyLock<SyntaxTheme> = LazyLock::new(|| {
    let mut theme = SyntaxTheme::default();
    theme.settings.foreground = Some(SyntaxColor {
        r: 0,
        g: 0,
        b: 0,
        a: 255,
    });
    theme.scopes = SYNTAX_ROLES
        .iter()
        .enumerate()
        .map(|(index, (scope, _))| ThemeItem {
            scope: scope.parse().expect("valid syntax scope"),
            style: StyleModifier {
                foreground: Some(SyntaxColor {
                    r: (index + 1) as u8,
                    g: 0,
                    b: 0,
                    a: 255,
                }),
                ..StyleModifier::default()
            },
        })
        .collect();
    theme
});

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
        // Keep whole words where the columns can all accommodate them, as Pi does.
        // Join styled spans before measuring so emphasis inside a word does not
        // create an artificial break. Very long tokens are still allowed to wrap.
        let word_minimum: Vec<usize> = (0..columns)
            .map(|column| {
                let longest = self
                    .rows
                    .iter()
                    .filter_map(|row| row.get(column))
                    .map(|cell| {
                        let text: String = cell.iter().map(|span| span.text.as_str()).collect();
                        text.split_whitespace()
                            .map(UnicodeWidthStr::width)
                            .max()
                            .unwrap_or(1)
                            .min(30)
                    })
                    .max()
                    .unwrap_or(1);
                minimum[column].max(longest)
            })
            .collect();
        let mut widths = if word_minimum.iter().sum::<usize>() <= available {
            word_minimum
        } else {
            minimum
        };
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

// Parser formatting can split a local part across text events. Do not link a
// trailing fragment if the original source immediately continues the address.
fn has_email_prefix(text: &str, start: usize, lower_bound: usize) -> bool {
    let mut has_content = false;
    for c in text[lower_bound..start].chars().rev() {
        if c.is_alphanumeric()
            || unicode_normalization::char::is_combining_mark(c)
            || "!#$%&'*+-/=?^_`{|}~.\"\\’".contains(c)
        {
            has_content |= !matches!(c, '*' | '_' | '~');
        } else {
            break;
        }
    }
    has_content
}

// Pi's GFM tokenizer links bare URLs and emails, but stops URLs at CJK
// punctuation. Explicit Markdown links are handled separately by the parser.
fn bare_links(text: &str) -> Vec<(Range<usize>, Option<String>)> {
    static EMAIL: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"(?:"(?:[\x20-\x21\x23-\x5b\x5d-\x7e]|\\[\x20-\x7e])*"|[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*)@[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]*[a-zA-Z0-9])+"#).unwrap()
    });
    static URL: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?:(?i:https?://|ftp://)|www\.)(?:[a-zA-Z0-9-]+\.?)+[^\s<]*").unwrap()
    });
    static CJK_PUNCT: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"[\p{P}&&[\p{scx=Han}\p{scx=Hiragana}\p{scx=Katakana}\p{scx=Hangul}\p{scx=Bopomofo}]]|[，．：；！？（）［］｛｝“”‘’…—]").unwrap()
    });
    let mut links = Vec::new();
    let mut offset = 0;
    let mut next_web = None;
    let mut next_mail = None;
    loop {
        if next_web.is_none_or(|cached: Option<regex::Match<'_>>| {
            cached.is_some_and(|m| m.start() < offset)
        }) {
            next_web = Some(URL.find_at(text, offset));
        }
        if next_mail.is_none_or(|cached: Option<regex::Match<'_>>| {
            cached.is_some_and(|m| m.start() < offset)
        }) {
            next_mail = Some(EMAIL.find_at(text, offset));
        }
        let web = next_web.flatten();
        let mail = next_mail.flatten();
        let (found, email, ambiguous) = match (web, mail) {
            (None, None) => break,
            (Some(web), None) => (web, false, false),
            (None, Some(mail)) => (mail, true, false),
            (Some(web), Some(mail)) if web.start() < mail.start() => (web, false, false),
            (Some(web), Some(mail)) if mail.start() < web.start() => (mail, true, false),
            (Some(web), Some(mail)) => {
                // A complete mailbox can also be a valid www URL. Pi picks
                // HTTP, but silently choosing either destination is unsafe.
                let web_end = CJK_PUNCT
                    .find(web.as_str())
                    .map_or(web.end(), |p| web.start() + p.start());
                let trailing = text.get(mail.end()..web_end);
                let ambiguous = web.as_str().starts_with("www.")
                    && trailing.is_some_and(|tail| {
                        tail.chars().all(|c| {
                            matches!(
                                c,
                                '?' | '!'
                                    | '.'
                                    | ','
                                    | ':'
                                    | ';'
                                    | '*'
                                    | '_'
                                    | '\''
                                    | '"'
                                    | '~'
                                    | ')'
                            )
                        })
                    });
                if ambiguous {
                    (mail, true, true)
                } else {
                    (web, false, false)
                }
            }
        };
        let start = found.start();
        let mut end = found.end();
        let candidate = &text[start..end];
        // Marked can match only the suffix after an omitted local-part
        // character (e.g. o'connor@example.com). Never link a different
        // recipient; leave the whole address as text instead.
        if email
            && text[..start].chars().next_back().is_some_and(|c| {
                c.is_alphanumeric()
                    || unicode_normalization::char::is_combining_mark(c)
                    || matches!(
                        c,
                        '.' | '"'
                            | '\\'
                            | '!'
                            | '#'
                            | '$'
                            | '%'
                            | '&'
                            | '\''
                            | '*'
                            | '/'
                            | '='
                            | '?'
                            | '^'
                            | '`'
                            | '{'
                            | '|'
                            | '}'
                            | '~'
                            | '’'
                    )
            })
        {
            offset = found.end();
            continue;
        }
        if !email {
            if let Some(punctuation) = CJK_PUNCT.find(candidate) {
                end = start + punctuation.start();
            }
            // An unmatched opening parenthesis is outside marked's URL token.
            // Stop there before trimming any punctuation immediately before it.
            let mut scan = start;
            while let Some(relative) = text[scan..end].find('(') {
                let open = scan + relative;
                if let Some(close) = text[open + 1..end].find(')') {
                    scan = open + 1 + close + 1;
                } else {
                    end = open;
                    break;
                }
            }
            // Marked's backpedal excludes trailing punctuation, while keeping
            // a closing parenthesis that belongs to a balanced URL path.
            let value = &text[start..end];
            let opens = value.bytes().filter(|b| *b == b'(').count();
            let mut closes = value.bytes().filter(|b| *b == b')').count();
            loop {
                let value = &text[start..end];
                if value.ends_with(';')
                    && let Some(amp) = value.rfind('&')
                    && value[amp + 1..value.len() - 1]
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric())
                    && amp + 2 < value.len()
                {
                    end = start + amp;
                    continue;
                }
                let Some(last) = value.chars().last() else {
                    break;
                };
                let remove = matches!(
                    last,
                    '?' | '!' | '.' | ',' | ':' | ';' | '*' | '_' | '\'' | '"' | '~'
                ) || (last == ')' && closes > opens);
                if !remove {
                    break;
                }
                if last == ')' {
                    closes -= 1;
                }
                end -= last.len_utf8();
            }
        } else {
            let suffix = &text[end..];
            let domain_continues = |c: char| {
                c.is_alphanumeric()
                    || unicode_normalization::char::is_combining_mark(c)
                    || matches!(c, '-' | '_')
            };
            if suffix.chars().next().is_some_and(domain_continues)
                || suffix
                    .strip_prefix(['.', '。', '．', '｡'])
                    .and_then(|s| s.chars().next())
                    .is_some_and(domain_continues)
            {
                offset = found.end();
                continue;
            }
        }
        if end > start {
            let label = &text[start..end];
            let href = if email {
                let (local, domain) = label.rsplit_once('@').expect("email candidate");
                let mut encoded = String::new();
                for b in local.bytes() {
                    if b.is_ascii_alphanumeric() || b"-._~!$'()*+;".contains(&b) {
                        encoded.push(char::from(b));
                    } else {
                        use std::fmt::Write as _;
                        write!(encoded, "%{b:02X}").expect("write to String");
                    }
                }
                format!("mailto:{encoded}@{domain}")
            } else if label.starts_with("www.") {
                format!("http://{label}")
            } else {
                label.to_owned()
            };
            links.push((start..end, (!ambiguous).then_some(href)));
        }
        offset = end.max(start + 1);
    }
    links
}

// Marked identifies bare links before parsing inline formatting. Protect the
// ASCII delimiters within candidates so pulldown-cmark makes the same choice.
// Byte lengths remain unchanged, allowing all parser offsets to index `text`.
fn protect_bare_links(text: &str, options: Options) -> String {
    let mut plain_ranges = Vec::new();
    let mut segments = Vec::new();
    let mut after_table = 0;
    let mut protected = 0usize;
    let mut formatting = Vec::new();
    for (event, range) in Parser::new_ext(text, options).into_offset_iter() {
        match event {
            Event::Start(Tag::Table(_)) => {
                segments.push((after_table..range.start, false));
                after_table = range.end;
            }
            Event::Start(Tag::TableCell) => segments.push((range, true)),
            Event::Start(tag @ (Tag::Emphasis | Tag::Strong | Tag::Strikethrough)) => {
                let marker = if matches!(tag, Tag::Emphasis) { 1 } else { 2 };
                formatting.push(range.end.saturating_sub(marker));
            }
            Event::End(TagEnd::Emphasis | TagEnd::Strong | TagEnd::Strikethrough) => {
                formatting.pop();
            }
            Event::Start(Tag::Link { .. } | Tag::Image { .. } | Tag::CodeBlock(_)) => {
                protected += 1
            }
            Event::End(TagEnd::Link | TagEnd::Image | TagEnd::CodeBlock) => {
                protected = protected.saturating_sub(1)
            }
            Event::Text(ref value)
                if protected == 0 && text.get(range.clone()) == Some(value.as_ref()) =>
            {
                plain_ranges.push((range, formatting.iter().copied().min()))
            }
            _ => {}
        }
    }
    segments.push((after_table..text.len(), false));
    let mut bytes = text.as_bytes().to_vec();
    let mut eligible = 0;
    for (segment, in_table) in segments {
        for (mut range, _) in bare_links(&text[segment.clone()]) {
            range.start += segment.start;
            range.end += segment.start;
            while eligible < plain_ranges.len() && plain_ranges[eligible].0.end <= range.start {
                eligible += 1;
            }
            // Markdown delimiters may also be valid email local-part
            // characters. When the parser has identified them as opening
            // formatting, begin the address at the first text inside it.
            if eligible < plain_ranges.len()
                && range.start < plain_ranges[eligible].0.start
                && plain_ranges[eligible].0.start < range.end
                && text[range.clone()].contains('@')
                && text[range.start..plain_ranges[eligible].0.start]
                    .bytes()
                    .all(|byte| matches!(byte, b'*' | b'_' | b'~'))
            {
                range.start = plain_ranges[eligible].0.start;
            }
            if eligible == plain_ranges.len() || !plain_ranges[eligible].0.contains(&range.start) {
                continue;
            }
            if let Some(end) = plain_ranges[eligible].1 {
                range.end = range.end.min(end);
            }
            // Escaped constructs keep their source rendering and do not gain a link.
            let preceding = text.as_bytes()[..range.start]
                .iter()
                .rev()
                .take_while(|b| **b == b'\\')
                .count();
            if preceding % 2 != 0 {
                continue;
            }
            let mut index = range.start;
            while index < range.end {
                if in_table
                    && bytes[index] == b'\\'
                    && index + 1 < range.end
                    && bytes[index + 1] == b'|'
                {
                    bytes[index] = b'x';
                    bytes[index + 1] = b'x';
                    index += 2;
                    continue;
                }
                if matches!(
                    bytes[index],
                    b'*' | b'_' | b'~' | b'`' | b'[' | b']' | b'\\' | b'&'
                ) {
                    bytes[index] = b'x';
                }
                index += 1;
            }
        }
    }
    String::from_utf8(bytes).expect("only ASCII markdown delimiters are replaced")
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
    let mut table_cell_start = None;
    let mut links = Vec::new();
    let mut previous_paragraph_end = None;
    let mut last_heading_separator = None;
    let mut last_code_separator = None;
    let protected_text = protect_bare_links(text, options);
    let mut events = Parser::new_ext(&protected_text, options)
        .into_offset_iter()
        .peekable();
    while let Some((event, mut range)) = events.next() {
        if quote_skip > 0 {
            match event {
                Event::Start(Tag::BlockQuote(_)) => quote_skip += 1,
                Event::End(TagEnd::BlockQuote(_)) => quote_skip -= 1,
                _ => {}
            }
            continue;
        }
        let is_text = matches!(&event, Event::Text(_));
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
            Event::Start(Tag::TableCell) => table_cell_start = Some(range.start),
            Event::End(TagEnd::TableCell) => {
                if let Some(table) = &mut table {
                    table.row.push(std::mem::take(&mut spans));
                }
                table_cell_start = None;
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
                if lists.is_empty()
                    && let Some(end) = previous_paragraph_end.take()
                    && end <= range.start
                    && text[end..range.start].trim().is_empty()
                {
                    lines.push(Line::default());
                }
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
                    // Syntect's bundled grammars omit TypeScript and TSX. Use
                    // its JavaScript grammar for their shared syntax.
                    let syntax_language = if matches!(
                        language.to_ascii_lowercase().as_str(),
                        "ts" | "typescript" | "tsx"
                    ) {
                        "javascript"
                    } else {
                        &language
                    };
                    let syntax = SYNTAXES.find_syntax_by_token(syntax_language);
                    let mut highlighter =
                        syntax.map(|syntax| HighlightLines::new(syntax, &SYNTAX_THEME));
                    for line in code_text
                        .split_inclusive('\n')
                        .chain(code_text.is_empty().then_some(""))
                    {
                        let display_line = line.strip_suffix('\n').unwrap_or(line);
                        let terminated;
                        let input = if line.ends_with('\n') {
                            line
                        } else {
                            terminated = format!("{line}\n");
                            &terminated
                        };
                        let pieces = highlighter
                            .as_mut()
                            .and_then(|highlighter| {
                                highlighter.highlight_line(input, &SYNTAXES).ok()
                            })
                            .map(|pieces| {
                                pieces
                                    .into_iter()
                                    .map(|(style, text)| {
                                        let role = usize::from(style.foreground.r)
                                            .checked_sub(1)
                                            .and_then(|index| SYNTAX_ROLES.get(index))
                                            .map_or("text", |(_, role)| *role);
                                        Span::themed(
                                            text.strip_suffix('\n').unwrap_or(text),
                                            theme,
                                            role,
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_else(|| {
                                vec![Span::themed(display_line, theme, "mdCodeBlock")]
                            });
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
                    if lists.is_empty() {
                        last_code_separator = Some(lines.len());
                        lines.push(Line::default());
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
                    let mut merged = content.into_string();
                    if is_text
                        && links.is_empty()
                        && protected_text.get(range.clone()) == Some(merged.as_str())
                    {
                        while let Some((Event::Text(next), next_range)) = events.peek() {
                            if next_range.start != range.end
                                || protected_text.get(next_range.clone()) != Some(next.as_ref())
                            {
                                break;
                            }
                            merged.push_str(next);
                            range.end = next_range.end;
                            events.next();
                        }
                        if protected_text[range.clone()] != text[range.clone()] {
                            merged = text[range.clone()].to_owned();
                        }
                    }
                    let preceding = text.as_bytes()[..range.start]
                        .iter()
                        .rev()
                        .take_while(|byte| **byte == b'\\')
                        .count();
                    let displayed = if preserve_source && preceding % 2 == 1 {
                        &text[range.start - 1..range.end]
                    } else if preserve_source && text[range.clone()].starts_with('\\') {
                        &text[range.clone()]
                    } else {
                        merged.as_str()
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
                    if is_text
                        && links.is_empty()
                        && preceding % 2 == 0
                        && displayed == &text[range.clone()]
                    {
                        let mut cursor = 0;
                        for (link_range, url) in bare_links(displayed) {
                            let Some(mut url) = url else {
                                let mut plain = span.clone();
                                plain.text = clean(&displayed[cursor..link_range.end]);
                                spans.push(plain);
                                cursor = link_range.end;
                                continue;
                            };
                            if url.starts_with("mailto:")
                                && has_email_prefix(
                                    text,
                                    range.start + link_range.start,
                                    table_cell_start.unwrap_or(0),
                                )
                            {
                                continue;
                            }
                            if cursor < link_range.start {
                                let mut plain = span.clone();
                                plain.text = clean(&displayed[cursor..link_range.start]);
                                spans.push(plain);
                            }
                            let raw_label = &displayed[link_range.clone()];
                            let label = if table.is_some() {
                                url = url.replace("\\|", "|");
                                raw_label.replace("\\|", "|")
                            } else {
                                raw_label.to_owned()
                            };
                            let mut link = span.clone();
                            link.text = clean(&label);
                            link.color = theme.color("mdLink");
                            link.dim = theme.is_dim("mdLink");
                            link.underline = true;
                            link.hyperlink =
                                (hyperlinks && safe_hyperlink(&url)).then(|| url.clone());
                            spans.push(link);
                            if (!hyperlinks || !safe_hyperlink(&url))
                                && label != url
                                && !url.starts_with("mailto:")
                            {
                                spans.push(Span::themed(format!(" ({url})"), theme, "mdLinkUrl"));
                            }
                            cursor = link_range.end;
                        }
                        if cursor < displayed.len() {
                            span.text = clean(&displayed[cursor..]);
                            spans.push(span);
                        }
                    } else {
                        spans.push(span);
                    }
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
    if last_code_separator == lines.len().checked_sub(1) && last_code_separator.is_some() {
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
    fn empty_fenced_code_has_one_indented_content_row() {
        for language in ["", "rust", "unknown-language"] {
            for content in ["", "\n"] {
                let source = format!("before\n\n```{language}\n{content}```\n\nafter");
                let theme = Theme::system();
                let lines = markdown(&source, &theme, 80);
                let rendered: Vec<String> = lines
                    .iter()
                    .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                    .collect();
                assert_eq!(
                    rendered,
                    [
                        "before",
                        "",
                        &format!("```{language}"),
                        "  ",
                        "```",
                        "",
                        "after"
                    ],
                    "{source:?}"
                );
                assert_eq!(lines[3].spans[0].color, theme.color("mdCodeBlock"));
            }
        }
    }
    #[test]
    fn fenced_code_separates_following_blocks_without_trailing_blank_line() {
        for source in ["```\ncode\n```\nnext", "```\ncode\n```\n\nnext"] {
            let lines: Vec<String> = markdown(source, &Theme::system(), 80)
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            assert_eq!(lines, ["```", "  code", "```", "", "next"]);
        }
        let lines = markdown("```\ncode\n```", &Theme::system(), 80);
        assert_eq!(lines.len(), 3);
        for source in [
            "hello this is text\n```\ncode block\n```\nmore text",
            "hello this is text\n\n```\ncode block\n```\n\nmore text",
        ] {
            let lines: Vec<String> = markdown(source, &Theme::system(), 80)
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            assert_eq!(
                lines,
                [
                    "hello this is text",
                    "",
                    "```",
                    "  code block",
                    "```",
                    "",
                    "more text"
                ]
            );
        }
    }

    #[test]
    fn fenced_code_preserves_line_states_and_typescript_fallback() {
        let mut definition: serde_json::Value =
            serde_json::from_str(include_str!("../../assets/themes/dark.json")).unwrap();
        definition["name"] = "custom".into();
        definition["colors"]["syntaxKeyword"] = "#123456".into();
        definition["colors"]["syntaxComment"] = "#654321".into();
        definition["colors"]["mdCodeBlock"] = "#abcdef".into();
        let theme = Theme::parse(&definition.to_string(), None).unwrap();
        for language in ["javascript", "ts", "typescript", "tsx", "TypeScript"] {
            let source = format!(
                "```{language}\n// comment\nconst value = 1;\n\nconst jsx = <div>ok</div>;\n```"
            );
            let lines = markdown(&source, &theme, 80);
            let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
            assert!(
                spans.iter().any(|span| span.text.contains("comment")
                    && span.color == theme.color("syntaxComment")),
                "{language}"
            );
            assert!(
                spans.iter().any(|span| span.text.contains("const")
                    && span.color == theme.color("syntaxKeyword")),
                "{language}: {spans:?}"
            );
            let rendered: Vec<String> = lines
                .iter()
                .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
                .collect();
            assert_eq!(
                rendered,
                [
                    format!("```{language}"),
                    "  // comment".into(),
                    "  const value = 1;".into(),
                    "  ".into(),
                    "  const jsx = <div>ok</div>;".into(),
                    "```".into()
                ]
            );
        }
        let lines = markdown(
            "```python\n# comment\ndef hello():\n    pass\n```",
            &theme,
            80,
        );
        assert!(lines.iter().flat_map(|line| &line.spans).any(|span| span.text.contains("def") && span.color == theme.color("syntaxKeyword")));
    }
    #[test]
    fn fenced_code_uses_active_theme_roles_and_plain_fallback() {
        let mut definition: serde_json::Value =
            serde_json::from_str(include_str!("../../assets/themes/dark.json")).unwrap();
        definition["name"] = "custom".into();
        definition["colors"]["syntaxKeyword"] = "#123456".into();
        definition["colors"]["syntaxComment"] = "#654321".into();
        definition["colors"]["mdCodeBlock"] = "#abcdef".into();
        let theme = Theme::parse(&definition.to_string(), None).unwrap();
        let lines = markdown(
            "```rust\nfn main() {\n  // hello\n}\n```\n\n```unknown-language\nplain code\n```",
            &theme,
            80,
        );
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert!(
            spans
                .iter()
                .any(|span| span.text.contains("fn") && span.color == theme.color("syntaxKeyword")),
            "{spans:?}"
        );
        assert!(
            spans
                .iter()
                .any(|span| span.text.contains("hello")
                    && span.color == theme.color("syntaxComment"))
        );
        assert!(spans.iter().any(
            |span| span.text.contains("plain code") && span.color == theme.color("mdCodeBlock")
        ));

        definition["colors"]["syntaxKeyword"] = "#ff0000".into();
        let changed = Theme::parse(&definition.to_string(), None).unwrap();
        let lines = markdown("```rust\nfn main() {}\n```", &changed, 80);
        assert!(
            lines.iter().flat_map(|line| &line.spans).any(
                |span| span.text.contains("fn") && span.color == changed.color("syntaxKeyword")
            )
        );
        let system = Theme::system();
        let lines = markdown("```rust\n// comment\n```", &system, 80);
        assert!(lines.iter().flat_map(|line| &line.spans).any(|span| {
            span.text.contains("comment")
                && span.color == system.color("syntaxComment")
                && span.dim == system.is_dim("syntaxComment")
        }));
    }
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
    fn markdown_table_keeps_longest_words_when_columns_fit() {
        let source = "| Column One | Column Two With Detail |\n| --- | --- |\n| super**califra**gil short | otherword |\n| small | tiny |";
        let lines = markdown(source, &Theme::system(), 32);
        let text: Vec<String> = lines
            .iter()
            .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
            .collect();
        assert!(
            text.iter()
                .any(|line| line.contains("supercalifragil") && line.contains("otherword")),
            "{text:#?}"
        );
        assert!(
            text.iter()
                .all(|line| UnicodeWidthStr::width(line.as_str()) <= 32)
        );
        let unicode =
            "| Header | Other Column Name |\n| --- | --- |\n| 日本**語文**字列 | example |";
        let lines = markdown(unicode, &Theme::system(), 32);
        assert!(lines.iter().any(|line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
                .contains("日本語文字列")
        }));
        // When words cannot all fit, preserve the width bound and all content.
        let lines = markdown(
            "| A | B |\n| - | - |\n| abcdefghijk | xyzabcdefgh |",
            &Theme::system(),
            12,
        );
        let text: Vec<String> = lines
            .iter()
            .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
            .collect();
        assert!(
            text.iter()
                .all(|line| UnicodeWidthStr::width(line.as_str()) <= 12)
        );
        assert!(text.iter().any(|line| line.starts_with('│')));
        let mut cells = [String::new(), String::new()];
        for line in text
            .iter()
            .skip_while(|line| !line.starts_with('├'))
            .skip(1)
        {
            if line.starts_with('└') {
                break;
            }
            for (cell, part) in cells.iter_mut().zip(line.split('│').skip(1)) {
                cell.push_str(part.trim());
            }
        }
        assert_eq!(cells, ["abcdefghijk", "xyzabcdefgh"]);
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
    fn bare_links_do_not_change_adjacent_table_cells() {
        let source = "| A | B | C |\n|---|---|---|\n|https://example.com|**important**|[docs](https://other.example)|\n|https://one.example|https://two.example|end|";
        let lines = markdown_at_depth(
            source,
            &Theme::system(),
            120,
            0,
            super::super::mermaid::Mermaid::Off,
            true,
            false,
        );
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("https://example.com"))
        );
        assert!(
            spans.iter().any(|s| s.bold && s.text.contains("important")),
            "{spans:?}"
        );
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("https://other.example"))
        );
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("https://one.example"))
        );
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("https://two.example"))
        );
        let source = "| A | B |\n|---|---|\n|https://example.com/a\\|b|keep|";
        let lines = markdown_at_depth(
            source,
            &Theme::system(),
            120,
            0,
            super::super::mermaid::Mermaid::Off,
            true,
            false,
        );
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert!(
            spans
                .iter()
                .any(|s| s.text.contains("https://example.com/a|b")
                    && s.hyperlink.as_deref() == Some("https://example.com/a|b")),
            "{spans:?}"
        );
        assert!(spans.iter().any(|s| s.text.contains("keep")), "{spans:?}");
        let source = "| A | B |\n|---|---|\n|user@example.com|keep|";
        let lines = markdown_at_depth(
            source,
            &Theme::system(),
            100,
            0,
            super::super::mermaid::Mermaid::Off,
            true,
            false,
        );
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("mailto:user@example.com")),
            "{spans:?}"
        );
        assert!(spans.iter().any(|s| s.text.contains("keep")));
    }

    #[test]
    fn long_link_lists_keep_every_target() {
        for (item, expected) in [
            ("user@example.com", "mailto:user@example.com"),
            ("https://example.com", "https://example.com"),
        ] {
            let source = vec![item; 512].join(" ");
            let links = bare_links(&source);
            assert_eq!(links.len(), 512);
            assert!(
                links
                    .iter()
                    .all(|(_, target)| target.as_deref() == Some(expected))
            );
        }
    }

    #[test]
    fn bare_url_keeps_enclosing_emphasis_separate_from_suffix() {
        for (source, target, bold) in [
            ("**https://example.com**suffix", "https://example.com", true),
            ("*https://example.com*suffix", "https://example.com", false),
        ] {
            for hyperlinks in [false, true] {
                let lines = markdown_at_depth(
                    source,
                    &Theme::system(),
                    100,
                    0,
                    super::super::mermaid::Mermaid::Off,
                    hyperlinks,
                    false,
                );
                let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
                assert!(
                    spans.iter().any(|s| s.text == target
                        && s.underline
                        && s.bold == bold
                        && (!hyperlinks || s.hyperlink.as_deref() == Some(target))),
                    "{source}: {spans:?}"
                );
                assert!(
                    spans.iter().any(|s| s.text == "suffix"
                        && !s.bold
                        && !s.italic
                        && s.hyperlink.is_none()),
                    "{source}: {spans:?}"
                );
            }
        }
    }

    #[test]
    fn bare_emails_preserve_complete_local_parts() {
        let theme = Theme::system();
        for (address, target) in [
            ("o'connor@example.com", "mailto:o'connor@example.com"),
            ("foo!bar@example.com", "mailto:foo!bar@example.com"),
            ("foo%bar@example.com", "mailto:foo%25bar@example.com"),
            ("foo/bar@example.com", "mailto:foo%2Fbar@example.com"),
            (
                "a!#$%&'*+-/=?^_`{|}~.z@example.com",
                "mailto:a!%23$%25%26'*+-%2F%3D%3F%5E_%60%7B%7C%7D~.z@example.com",
            ),
            (
                "\"quoted local\"@example.com",
                "mailto:%22quoted%20local%22@example.com",
            ),
            ("\"a@b\"@example.com", "mailto:%22a%40b%22@example.com"),
            ("\"a,b\"@example.com", "mailto:%22a%2Cb%22@example.com"),
            (
                "\"a\\\"b\"@example.com",
                "mailto:%22a%5C%22b%22@example.com",
            ),
        ] {
            for hyperlinks in [false, true] {
                let lines = markdown_at_depth(
                    address,
                    &theme,
                    160,
                    0,
                    super::super::mermaid::Mermaid::Off,
                    hyperlinks,
                    false,
                );
                let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
                assert_eq!(
                    spans.iter().map(|s| s.text.as_str()).collect::<String>(),
                    address,
                    "{address}: {spans:?}"
                );
                assert!(
                    spans.iter().any(|s| s.underline
                        && if hyperlinks {
                            s.hyperlink.as_deref() == Some(target)
                        } else {
                            s.hyperlink.is_none()
                        }),
                    "{address}: {spans:?}"
                );
            }
        }
        for address in [
            "o’connor@example.com",
            "éconnor@example.com",
            "e\u{301}connor@example.com",
            "foo..bar@example.com",
            "é*foo*bar@example.com",
            "user@example.comé",
            "user@example.com.中国",
            "user@example.com。evil",
            "user@example.com．evil",
            "user@example.com｡evil",
        ] {
            let lines = markdown_at_depth(
                address,
                &theme,
                100,
                0,
                super::super::mermaid::Mermaid::Off,
                true,
                false,
            );
            assert!(
                lines
                    .iter()
                    .flat_map(|line| &line.spans)
                    .all(|span| span.hyperlink.is_none()),
                "{address}: {lines:?}"
            );
        }
        for (source, target) in [
            (
                "**first.last+tag@example.com**",
                "mailto:first.last+tag@example.com",
            ),
            ("**o'co*nor@example.com**", "mailto:o'co*nor@example.com"),
            ("**o'*co*nor@example.com**", "mailto:o'*co*nor@example.com"),
        ] {
            let lines = markdown_at_depth(
                source,
                &theme,
                100,
                0,
                super::super::mermaid::Mermaid::Off,
                true,
                false,
            );
            assert!(
                lines
                    .iter()
                    .flat_map(|line| &line.spans)
                    .any(|span| span.bold && span.hyperlink.as_deref() == Some(target)),
                "{source}: {lines:?}"
            );
        }
    }

    #[test]
    fn ambiguous_www_email_candidates_are_not_linked() {
        for source in [
            "www.user?tag@example.com",
            "www.user#tag@example.com",
            "www.user/tag@example.com",
            "www.foo@example.com",
            "www.example.com/a@other.example",
            "www.example.com?to=user@other.example",
            "www.example.com#user@other.example",
            "www.user?tag@example.com.",
            "www.user?tag@example.com。后文",
        ] {
            for hyperlinks in [false, true] {
                let lines = markdown_at_depth(
                    source,
                    &Theme::system(),
                    100,
                    0,
                    super::super::mermaid::Mermaid::Off,
                    hyperlinks,
                    false,
                );
                let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
                assert_eq!(
                    spans
                        .iter()
                        .map(|span| span.text.as_str())
                        .collect::<String>(),
                    source
                );
                assert!(
                    spans
                        .iter()
                        .all(|span| span.hyperlink.is_none() && !span.underline),
                    "{source}: {spans:?}"
                );
            }
        }
        for (source, target) in [
            (
                "www.example.com/a@other.example/more",
                "http://www.example.com/a@other.example/more",
            ),
            (
                "[website](http://www.user?tag@example.com)",
                "http://www.user?tag@example.com",
            ),
            (
                "<mailto:www.user?tag@example.com>",
                "mailto:www.user?tag@example.com",
            ),
        ] {
            let lines = markdown_at_depth(
                source,
                &Theme::system(),
                100,
                0,
                super::super::mermaid::Mermaid::Off,
                true,
                false,
            );
            assert!(
                lines
                    .iter()
                    .flat_map(|line| &line.spans)
                    .any(|span| span.hyperlink.as_deref() == Some(target)),
                "{source}: {lines:?}"
            );
        }
    }

    #[test]
    fn bare_links_keep_ampersands_and_inline_markdown_delimiters() {
        let theme = Theme::system();
        let render = |source: &str, hyperlinks| {
            markdown_at_depth(
                source,
                &theme,
                160,
                0,
                super::super::mermaid::Mermaid::Off,
                hyperlinks,
                false,
            )
        };
        for (source, href) in [
            ("https://example.com/path&", "https://example.com/path&"),
            (
                "https://example.com/foo*bar*",
                "https://example.com/foo*bar",
            ),
            (
                "https://example.com/foo~~bar~~",
                "https://example.com/foo~~bar",
            ),
            (
                "https://example.com/?a=1&amp;b=2",
                "https://example.com/?a=1&amp;b=2",
            ),
            ("https://example.com/path&amp;", "https://example.com/path"),
        ] {
            let lines = render(source, true);
            let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
            assert!(
                spans
                    .iter()
                    .any(|span| span.hyperlink.as_deref() == Some(href)),
                "{source}: {spans:?}"
            );
            let fallback = render(source, false);
            let fallback: Vec<_> = fallback.iter().flat_map(|line| &line.spans).collect();
            assert!(
                fallback
                    .iter()
                    .any(|span| span.underline && span.text == href),
                "{source}: {fallback:?}"
            );
        }
        let lines = render("**https://example.com/foo*bar***", true);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.bold
                    && span.hyperlink.as_deref() == Some("https://example.com/foo*bar"))
        );
        let lines = render(
            "`https://example.com/foo*bar*` [https://example.com/*foo*](https://other.example)",
            true,
        );
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert!(
            spans
                .iter()
                .any(|s| s.text == "https://example.com/foo*bar*" && s.hyperlink.is_none())
        );
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("https://other.example"))
        );
        assert!(
            spans
                .iter()
                .all(|s| s.hyperlink.as_deref() != Some("https://example.com/*foo*"))
        );
    }

    #[test]
    fn markdown_bare_links_preserve_cjk_boundaries_and_styles() {
        let theme = Theme::system();
        let render = |source: &str, hyperlinks| {
            markdown_at_depth(
                source,
                &theme,
                160,
                0,
                super::super::mermaid::Mermaid::Off,
                hyperlinks,
                false,
            )
        };
        for punctuation in "。，；：！？、“”‘’（）【】《》「」『』…—·・〈〉［］｛｝．｡｢｣､･".chars()
        {
            let source = format!("https://example.com/中文?q=搜索#片段{punctuation}后文");
            let lines = render(&source, true);
            let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
            assert!(
                spans.iter().any(|span| span.hyperlink.as_deref()
                    == Some("https://example.com/中文?q=搜索#片段")),
                "{source}: {spans:?}"
            );
            assert_eq!(
                spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>(),
                source
            );
            assert!(
                spans
                    .iter()
                    .any(|span| span.text.contains(punctuation) && span.hyperlink.is_none())
            );
        }
        for (source, expected) in [
            (
                "https://example.com/a_(b).，后文",
                "https://example.com/a_(b)",
            ),
            (
                "https://example.com/path(unclosed",
                "https://example.com/path",
            ),
            ("www.example.com/path", "http://www.example.com/path"),
            ("user@example.com", "mailto:user@example.com"),
            (
                "https://example.com/テストー한글",
                "https://example.com/テストー한글",
            ),
        ] {
            let lines = render(source, true);
            assert!(
                lines
                    .iter()
                    .flat_map(|line| &line.spans)
                    .any(|span| span.underline && span.hyperlink.as_deref() == Some(expected)),
                "{source}: {lines:?}"
            );
        }
        let lines = render("Contact user@example.com and https://example.com.", false);
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert_eq!(
            spans.iter().map(|s| s.text.as_str()).collect::<String>(),
            "Contact user@example.com and https://example.com."
        );
        assert!(
            spans
                .iter()
                .any(|s| s.underline && s.text == "user@example.com")
        );
        assert!(spans.iter().all(|s| s.hyperlink.is_none()));
        let lines = render("www.example.com", false);
        assert_eq!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .map(|s| s.text.as_str())
                .collect::<String>(),
            "www.example.com (http://www.example.com)"
        );
        let lines = render("https://a.example，https://b.example", true);
        let targets: Vec<_> = lines
            .iter()
            .flat_map(|line| &line.spans)
            .filter_map(|span| span.hyperlink.as_deref())
            .collect();
        assert_eq!(targets, ["https://a.example", "https://b.example"]);
        let lines = render(
            "`https://example.com` [link](https://example.com/中文，路径)",
            true,
        );
        let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
        assert!(
            spans
                .iter()
                .any(|s| s.text == "https://example.com" && s.hyperlink.is_none())
        );
        assert!(
            spans
                .iter()
                .any(|s| s.hyperlink.as_deref() == Some("https://example.com/中文，路径"))
        );
        let lines = render("https://example.com/\u{1b}]malicious", true);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .all(|span| !span.text.contains('\u{1b}') && span.hyperlink.is_none())
        );
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
