//! Native extension dialogs use the same editor and keybindings as the prompt.
use super::{
    editor::Editor,
    keys::{self, Keys},
    render::{Line, Span, wrap},
    theme::Theme,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use pi_rs::{Error, Result, UiRequest, UiResponse};
use std::borrow::Cow;
use std::cell::Cell;
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

pub struct Dialog {
    pub id: Uuid,
    request: UiRequest,
    editor: Editor,
    selected: usize,
    scroll_max: Cell<usize>,
    error: Option<String>,
    external_error: Option<String>,
    masked: bool,
}
impl Dialog {
    pub fn validate_keys(request: &UiRequest) -> Result<()> {
        if let UiRequest::Custom { actions, .. } = request {
            let mut seen = Vec::with_capacity(actions.len());
            for action in actions {
                let key = keys::parse(&action.key)?;
                if key.code == KeyCode::Esc
                    || key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    || seen.contains(&key)
                {
                    return Err(Error::Config(
                        "extension component key is reserved or repeated".into(),
                    ));
                }
                seen.push(key);
            }
        }
        Ok(())
    }

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
            scroll_max: Cell::new(0),
            error: None,
            external_error: None,
            masked: false,
        }
    }
    pub fn secret(title: String) -> Self {
        let mut dialog = Self::new(
            Uuid::new_v4(),
            UiRequest::Input {
                title,
                placeholder: Some("Paste or enter your API key".into()),
                initial_value: None,
                timeout: None,
            },
        );
        dialog.masked = true;
        dialog
    }

    fn visible<'a>(&self, text: &'a str) -> Cow<'a, str> {
        if self.masked {
            Cow::Owned("•".repeat(text.chars().count()))
        } else {
            Cow::Borrowed(text)
        }
    }
    pub fn set_error(&mut self, error: String) {
        self.error = Some(error);
    }
    pub fn external_editor_requested(&self, key: KeyEvent, keys: &Keys) -> bool {
        matches!(self.request, UiRequest::Editor { .. })
            && !keys.matches("tui.select.cancel", key)
            && keys.matches("app.editor.external", key)
    }
    pub fn external_edit(&mut self, run: impl FnOnce(&mut Editor) -> Result<()>) {
        if !matches!(self.request, UiRequest::Editor { .. }) {
            return;
        }
        let mut draft = Editor::default();
        draft.replace(self.editor.text.clone());
        match run(&mut draft) {
            Ok(())
                if self
                    .request
                    .accepts_response(&UiResponse::Text(draft.text.clone())) =>
            {
                self.editor.replace(draft.text);
                self.external_error = None;
            }
            Ok(()) => {
                self.external_error =
                    Some("External editor result exceeds 1 MiB; draft unchanged".into())
            }
            Err(Error::Config(message)) => self.external_error = Some(message),
            Err(error) => self.external_error = Some(error.to_string()),
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
        let cancel = if matches!(self.request, UiRequest::Custom { .. }) {
            key.code == KeyCode::Esc
                || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        } else {
            keys.matches("tui.select.cancel", key)
        };
        if cancel {
            return Some(UiResponse::Cancelled);
        }
        let up = keys.matches("tui.select.up", key);
        let down = keys.matches("tui.select.down", key);
        let confirm = keys.matches("tui.select.confirm", key);
        match &self.request {
            UiRequest::Select { options, .. } => {
                if confirm {
                    return options.get(self.selected).cloned().map(UiResponse::Text);
                }
                let count = options.len().max(1);
                if up {
                    self.selected = self.selected.checked_sub(1).unwrap_or(count - 1);
                } else if down {
                    self.selected = (self.selected + 1) % count;
                } else if keys.matches("tui.select.pageUp", key) {
                    self.selected = self.selected.saturating_sub(8);
                } else if keys.matches("tui.select.pageDown", key) {
                    self.selected = (self.selected + 8).min(count - 1);
                }
            }
            UiRequest::Confirm { .. } => {
                if confirm {
                    return Some(UiResponse::Confirmed(self.selected == 0));
                }
                if up
                    || down
                    || matches!(key.code, KeyCode::Left | KeyCode::Right)
                    || keys.matches("tui.input.tab", key)
                {
                    self.selected = 1 - self.selected;
                } else if key.modifiers.is_empty() {
                    match key.code {
                        KeyCode::Char('y') => return Some(UiResponse::Confirmed(true)),
                        KeyCode::Char('n') => return Some(UiResponse::Confirmed(false)),
                        _ => {}
                    }
                }
            }
            UiRequest::Custom { actions, .. } => {
                let normalized = keys::normalize(key);
                if let Some(action) = actions.iter().find(|action| {
                    keys::parse(&action.key).is_ok_and(|binding| {
                        binding.code == normalized.code && binding.modifiers == normalized.modifiers
                    })
                }) {
                    return Some(UiResponse::Text(action.value.clone()));
                }
                let max = self.scroll_max.get();
                let position = self.selected.min(max);
                self.selected = match key.code {
                    KeyCode::Up => position.saturating_sub(1),
                    KeyCode::Down => (position + 1).min(max),
                    KeyCode::PageUp => position.saturating_sub(8),
                    KeyCode::PageDown => (position + 8).min(max),
                    KeyCode::Home => 0,
                    KeyCode::End => max,
                    _ => position,
                };
            }
            UiRequest::Input { .. } | UiRequest::Editor { .. } => {
                let multiline = matches!(self.request, UiRequest::Editor { .. });
                if (!multiline && confirm)
                    || (multiline
                        && key.code == KeyCode::Enter
                        && key.modifiers.contains(KeyModifiers::CONTROL))
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
                    self.editor.type_character(character);
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
            | UiRequest::Editor { title, .. }
            | UiRequest::Custom { title, .. } => title,
            _ => "Extension",
        }
    }
    fn heading(&self) -> &str {
        self.error.as_deref().unwrap_or_else(|| self.title())
    }
    fn heading_height(&self, width: usize, height: usize) -> usize {
        wrap(
            &[Span::new(self.heading(), crossterm::style::Color::Reset)],
            width,
        )
        .len()
        .min(height.saturating_sub(2).clamp(1, 4))
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
                self.visible(&self.editor.text[..self.editor.cursor])
                    .as_ref(),
                crossterm::style::Color::Reset,
            )],
            width,
        );
        let heading_height = self.heading_height(width, height);
        let offset = prefix.len().saturating_sub(
            height
                .saturating_sub(heading_height + 1 + usize::from(self.external_error.is_some()))
                .max(1),
        );
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
            heading_height + prefix.len().saturating_sub(1).saturating_sub(offset),
            column,
        ))
    }
    pub fn render(&self, theme: &Theme, width: usize, height: usize, keys: &Keys) -> Vec<Line> {
        let mut lines = wrap(&[Span::themed(self.heading(), theme, "accent")], width);
        lines.truncate(self.heading_height(width, height));
        let available = height
            .saturating_sub(lines.len() + 1 + usize::from(self.external_error.is_some()))
            .max(1);
        match &self.request {
            UiRequest::Select { options, .. } => {
                let start = self.selected.saturating_sub(available.saturating_sub(1));
                for (index, option) in options.iter().enumerate().skip(start).take(available) {
                    let mut line = Line::themed(
                        &format!(
                            "{} {}",
                            if index == self.selected { "›" } else { " " },
                            option
                        ),
                        theme,
                        "text",
                    );
                    if index == self.selected {
                        line.background = Some(theme.color("selectedBg"));
                    }
                    lines.push(line);
                }
            }
            UiRequest::Confirm { message, .. } => {
                lines.extend(
                    wrap(&[Span::themed(message, theme, "text")], width)
                        .into_iter()
                        .take(available.saturating_sub(1)),
                );
                lines.push(Line::themed(
                    if self.selected == 0 {
                        "› Yes    No"
                    } else {
                        "  Yes  › No"
                    },
                    theme,
                    "text",
                ));
            }
            UiRequest::Custom { lines: body, .. } => {
                let rows: Vec<_> = body
                    .iter()
                    .flat_map(|line| {
                        let spans: Vec<_> = line
                            .iter()
                            .map(|span| {
                                let mut rendered = Span::themed(
                                    &span.text,
                                    theme,
                                    span.color.as_deref().unwrap_or("text"),
                                );
                                rendered.bold = span.bold;
                                rendered.italic = span.italic;
                                rendered.underline = span.underline;
                                rendered.inverse = span.inverse;
                                rendered
                            })
                            .collect();
                        wrap(&spans, width)
                    })
                    .collect();
                let max = rows.len().saturating_sub(available);
                self.scroll_max.set(max);
                lines.extend(
                    rows.into_iter()
                        .skip(self.selected.min(max))
                        .take(available),
                );
            }
            UiRequest::Input { placeholder, .. } if self.editor.text.is_empty() => {
                lines.push(Line::themed(
                    placeholder.as_deref().unwrap_or(""),
                    theme,
                    "dim",
                ));
            }
            UiRequest::Input { .. } | UiRequest::Editor { .. } => {
                let content = wrap(
                    &[Span::themed(
                        self.visible(&self.editor.text).as_ref(),
                        theme,
                        "text",
                    )],
                    width,
                );
                let prefix = wrap(
                    &[Span::themed(
                        self.visible(&self.editor.text[..self.editor.cursor])
                            .as_ref(),
                        theme,
                        "text",
                    )],
                    width,
                );
                let start = prefix.len().saturating_sub(available);
                lines.extend(content.into_iter().skip(start).take(available));
            }
            _ => {}
        }
        let hint = match &self.request {
            UiRequest::Editor { .. } => Cow::Owned(format!(
                "Ctrl+S save · {} external editor · {} cancel",
                keys.hint("app.editor.external"),
                keys.hint("tui.select.cancel")
            )),
            UiRequest::Custom { actions, .. } => {
                let keys = actions
                    .iter()
                    .take(4)
                    .map(|action| {
                        format!(
                            "{} {}",
                            action.key,
                            action.label.as_deref().unwrap_or(&action.value)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" · ");
                Cow::Owned(format!("{keys} · ↑/↓ scroll · Esc cancel"))
            }
            _ => Cow::Borrowed("Enter confirm · Esc cancel"),
        };
        if let Some(error) = &self.external_error {
            lines.extend(
                wrap(&[Span::themed(error, theme, "warning")], width)
                    .into_iter()
                    .take(1),
            );
        }
        lines.push(Line::themed(&hint, theme, "dim"));
        lines.truncate(height);
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_editor_shortcut_is_scoped_remappable_and_cancel_first() {
        let mut keys = Keys::default();
        let editor = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Editor {
                title: "Edit".into(),
                initial_value: None,
                timeout: None,
            },
        );
        let input = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Input {
                title: "Secret".into(),
                placeholder: None,
                initial_value: None,
                timeout: None,
            },
        );
        let ctrl_g = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::CONTROL);
        assert!(editor.external_editor_requested(ctrl_g, &keys));
        assert!(!input.external_editor_requested(ctrl_g, &keys));
        keys.set_test_binding("app.editor.external", "ctrl+e");
        assert!(!editor.external_editor_requested(ctrl_g, &keys));
        assert!(editor.external_editor_requested(
            KeyEvent::new(KeyCode::Char('e'), KeyModifiers::CONTROL),
            &keys
        ));
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        let lines = editor.render(&theme, 100, 8, &keys);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.text.contains("Ctrl+E external editor"))
        );
        keys.set_test_binding("app.editor.external", "escape");
        assert!(
            !editor
                .external_editor_requested(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &keys)
        );
    }

    #[test]
    fn external_editor_failure_and_oversize_keep_dialog_open_and_draft() {
        let keys = Keys::default();
        let mut dialog = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Editor {
                title: "Edit".into(),
                initial_value: Some("original".into()),
                timeout: None,
            },
        );
        dialog.external_edit(|draft| {
            draft.replace("discarded".into());
            Err(Error::Config("editor failed".into()))
        });
        assert_eq!(dialog.editor.text, "original");
        assert!(
            dialog
                .external_error
                .as_deref()
                .unwrap()
                .contains("editor failed")
        );
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        let lines = dialog.render(&theme, 100, 8, &keys);
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.text.contains("editor failed"))
        );
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| span.text.contains("external editor"))
        );
        dialog.external_edit(|draft| {
            draft.replace("x".repeat(1024 * 1024 + 1));
            Ok(())
        });
        assert_eq!(dialog.editor.text, "original");
        assert!(dialog.external_error.as_deref().unwrap().contains("1 MiB"));
        dialog.external_edit(|draft| {
            draft.replace("edited".into());
            Ok(())
        });
        assert!(dialog.external_error.is_none());
        assert_eq!(
            dialog.input(
                KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
                &keys
            ),
            Some(UiResponse::Text("edited".into()))
        );
        assert_eq!(
            dialog.input(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &keys),
            Some(UiResponse::Cancelled)
        );
    }
    #[test]
    fn editor_dialog_coalesces_typed_words_but_not_paste() {
        let keys = Keys::default();
        let mut dialog = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Editor {
                title: "Edit".into(),
                initial_value: None,
                timeout: None,
            },
        );
        for character in "hello world".chars() {
            dialog.input(
                KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE),
                &keys,
            );
        }
        let undo = [
            KeyEvent::new(KeyCode::Char('-'), KeyModifiers::CONTROL),
            KeyEvent::new(KeyCode::Char('z'), KeyModifiers::CONTROL),
            KeyEvent::new(KeyCode::Char('z'), KeyModifiers::ALT),
        ]
        .into_iter()
        .find(|key| keys.matches("tui.editor.undo", *key))
        .unwrap();
        dialog.input(undo, &keys);
        assert_eq!(dialog.editor.text, "hello");
        dialog.paste(" pasted");
        dialog.input(undo, &keys);
        assert_eq!(dialog.editor.text, "hello");
        dialog.input(undo, &keys);
        assert_eq!(dialog.editor.text, "");
    }
    #[test]
    fn component_keys_reject_cancel_bindings_and_alias_collisions() {
        let request = |keys: &[&str]| UiRequest::Custom {
            title: "Component".into(),
            lines: vec![],
            actions: keys
                .iter()
                .map(|key| pi_rs::UiComponentAction {
                    key: (*key).into(),
                    value: "action".into(),
                    label: None,
                })
                .collect(),
            timeout: None,
        };
        assert!(Dialog::validate_keys(&request(&["ctrl+s", "enter"])).is_ok());
        assert!(Dialog::validate_keys(&request(&["escape"])).is_err());
        assert!(Dialog::validate_keys(&request(&["ctrl+shift+c"])).is_err());
        assert!(Dialog::validate_keys(&request(&["enter", "return"])).is_err());
    }

    #[test]
    fn error_headers_wrap_without_hiding_the_editor_cursor() {
        let mut dialog = Dialog::new(
            Uuid::new_v4(),
            UiRequest::Editor {
                title: "Edit setting".into(),
                initial_value: Some("first\nsecond".into()),
                timeout: None,
            },
        );
        dialog.set_error("Invalid value for this setting".into());
        let theme = Theme::load("dark", &[], std::path::Path::new("."), false, false).unwrap();
        let lines = dialog.render(&theme, 12, 8, &Keys::default());
        let (row, column) = dialog.cursor(12, 8).unwrap();
        assert!(row >= 2 && row < lines.len());
        assert_eq!(column, 6);
        assert!(lines.len() <= 8);
    }

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
