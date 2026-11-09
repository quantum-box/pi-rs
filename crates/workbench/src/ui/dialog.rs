//! Native extension dialogs use the same editor and keybindings as the prompt.
use super::{
    editor::Editor,
    keys::Keys,
    render::{Line, Span, wrap},
    theme::Theme,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use pi_rs::{UiRequest, UiResponse};
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

pub struct Dialog {
    pub id: Uuid,
    request: UiRequest,
    editor: Editor,
    selected: usize,
}
impl Dialog {
    pub fn new(id: Uuid, request: UiRequest) -> Self {
        let mut editor = Editor::default();
        if let UiRequest::Input { initial_value, .. } | UiRequest::Editor { initial_value, .. } =
            &request
        {
            editor.replace(initial_value.clone().unwrap_or_default());
        }
        Self {
            id,
            request,
            editor,
            selected: 0,
        }
    }
    pub fn paste(&mut self, text: &str) {
        if matches!(
            self.request,
            UiRequest::Input { .. } | UiRequest::Editor { .. }
        ) && self.editor.text.len().saturating_add(text.len()) <= 1024 * 1024
        {
            self.editor.insert(text);
        }
    }
    pub fn input(&mut self, key: KeyEvent, keys: &Keys) -> Option<UiResponse> {
        if key.code == KeyCode::Esc
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            return Some(UiResponse::Cancelled);
        }
        match &self.request {
            UiRequest::Select { options, .. } => match key.code {
                KeyCode::Up => self.selected = self.selected.saturating_sub(1),
                KeyCode::Down => {
                    self.selected = (self.selected + 1).min(options.len().saturating_sub(1))
                }
                KeyCode::Enter => return options.get(self.selected).cloned().map(UiResponse::Text),
                _ => {}
            },
            UiRequest::Confirm { .. } => match key.code {
                KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down | KeyCode::Tab => {
                    self.selected = 1 - self.selected
                }
                KeyCode::Enter => return Some(UiResponse::Confirmed(self.selected == 0)),
                KeyCode::Char('y') => return Some(UiResponse::Confirmed(true)),
                KeyCode::Char('n') => return Some(UiResponse::Confirmed(false)),
                _ => {}
            },
            UiRequest::Input { .. } | UiRequest::Editor { .. } => {
                let multiline = matches!(self.request, UiRequest::Editor { .. });
                if (key.code == KeyCode::Enter
                    && (!multiline || key.modifiers.contains(KeyModifiers::CONTROL)))
                    || (key.code == KeyCode::Char('s')
                        && key.modifiers.contains(KeyModifiers::CONTROL))
                {
                    return Some(UiResponse::Text(self.editor.text.clone()));
                }
                let action = keys.action(key, self.editor.text.is_empty()).unwrap_or("");
                if action == "tui.editor.cursorUp" {
                    self.editor.vertical(false);
                    return None;
                }
                if action == "tui.editor.cursorDown" {
                    self.editor.vertical(true);
                    return None;
                }
                if multiline && key.code == KeyCode::Enter {
                    self.editor.insert("\n");
                } else if !self.editor.action(action)
                    && let KeyCode::Char(character) = key.code
                    && !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER)
                    && self.editor.text.len() < 1024 * 1024
                {
                    self.editor.insert(&character.to_string());
                }
            }
            _ => {}
        }
        None
    }
    fn title(&self) -> &str {
        match &self.request {
            UiRequest::Select { title, .. }
            | UiRequest::Confirm { title, .. }
            | UiRequest::Input { title, .. }
            | UiRequest::Editor { title, .. } => title,
            _ => "Extension",
        }
    }
    pub fn cursor(&self, width: usize, height: usize) -> Option<(usize, usize)> {
        if !matches!(
            self.request,
            UiRequest::Input { .. } | UiRequest::Editor { .. }
        ) {
            return None;
        }
        let prefix = wrap(
            &[Span::new(
                &self.editor.text[..self.editor.cursor],
                crossterm::style::Color::Reset,
            )],
            width,
        );
        let offset = prefix.len().saturating_sub(height.saturating_sub(2).max(1));
        let column = prefix
            .last()
            .map_or(0, |line| {
                line.spans
                    .iter()
                    .map(|span| UnicodeWidthStr::width(span.text.as_str()))
                    .sum::<usize>()
            })
            .min(width.saturating_sub(1));
        Some((
            1 + prefix.len().saturating_sub(1).saturating_sub(offset),
            column,
        ))
    }
    pub fn render(&self, theme: &Theme, width: usize, height: usize) -> Vec<Line> {
        let mut lines = vec![Line::plain(self.title(), theme.color("accent"))];
        let available = height.saturating_sub(2).max(1);
        match &self.request {
            UiRequest::Select { options, .. } => {
                let start = self.selected.saturating_sub(available.saturating_sub(1));
                for (index, option) in options.iter().enumerate().skip(start).take(available) {
                    let mut line = Line::plain(
                        &format!(
                            "{} {}",
                            if index == self.selected { "›" } else { " " },
                            option
                        ),
                        theme.color("text"),
                    );
                    if index == self.selected {
                        line.background = Some(theme.color("selectedBg"));
                    }
                    lines.push(line);
                }
            }
            UiRequest::Confirm { message, .. } => {
                lines.extend(
                    wrap(&[Span::new(message, theme.color("text"))], width)
                        .into_iter()
                        .take(available.saturating_sub(1)),
                );
                lines.push(Line::plain(
                    if self.selected == 0 {
                        "› Yes    No"
                    } else {
                        "  Yes  › No"
                    },
                    theme.color("text"),
                ));
            }
            UiRequest::Input { placeholder, .. } if self.editor.text.is_empty() => {
                lines.push(Line::plain(
                    placeholder.as_deref().unwrap_or(""),
                    theme.color("dim"),
                ));
            }
            UiRequest::Input { .. } | UiRequest::Editor { .. } => {
                let content = wrap(&[Span::new(&self.editor.text, theme.color("text"))], width);
                let prefix = wrap(
                    &[Span::new(
                        &self.editor.text[..self.editor.cursor],
                        theme.color("text"),
                    )],
                    width,
                );
                let start = prefix.len().saturating_sub(available);
                lines.extend(content.into_iter().skip(start).take(available));
            }
            _ => {}
        }
        lines.push(Line::plain(
            if matches!(self.request, UiRequest::Editor { .. }) {
                "Ctrl+S save · Esc cancel"
            } else {
                "Enter confirm · Esc cancel"
            },
            theme.color("dim"),
        ));
        lines.truncate(height);
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialogs_accept_selection_text_multiline_and_cancellation() {
        let keys = Keys::default();
        let mut select = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Select {
                title: "Select".into(),
                options: vec!["one".into(), "two".into()],
                timeout: None,
            },
        );
        select.input(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), &keys);
        assert_eq!(
            select.input(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &keys),
            Some(UiResponse::Text("two".into()))
        );
        let mut editor = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Editor {
                title: "Edit".into(),
                initial_value: Some("first".into()),
                timeout: None,
            },
        );
        editor.input(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &keys);
        editor.paste("second");
        assert_eq!(
            editor.input(
                KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
                &keys
            ),
            Some(UiResponse::Text("first\nsecond".into()))
        );
        assert_eq!(
            editor.input(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &keys),
            Some(UiResponse::Cancelled)
        );
    }
}
