//! Searchable keyboard selectors for session and model navigation.
use super::{render::Line, theme::Theme};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::PathBuf;

#[derive(Clone)]
pub enum Selection {
    Resume(PathBuf),
    RenameSession(PathBuf, String),
    DeleteSession(PathBuf),
    Checkout(u64),
    Fork(u64),
    Model(String),
    Theme(String),
    Trust(bool),
    Thinking(String),
    Setting(String, serde_json::Value),
}
pub struct Item {
    pub label: String,
    pub selection: Selection,
}
pub struct Picker {
    pub title: String,
    items: Vec<Item>,
    query: String,
    selected: usize,
    active_session: Option<PathBuf>,
    session_action: Option<SessionAction>,
    session_error: Option<String>,
    relevance_sort: bool,
    all_workspaces: bool,
    named_only: bool,
    show_path: bool,
    named_sessions: std::collections::HashSet<PathBuf>,
}
#[derive(Clone)]
enum SessionAction {
    Rename(PathBuf),
    Delete(PathBuf),
}
pub enum Input {
    Continue,
    Cancel,
    Select(Selection),
    Save(Selection),
}
impl Picker {
    pub fn new(title: &str, items: Vec<Item>) -> Self {
        Self {
            title: title.into(),
            items,
            query: String::new(),
            selected: 0,
            active_session: None,
            session_action: None,
            session_error: None,
            relevance_sort: false,
            all_workspaces: false,
            named_only: false,
            show_path: false,
            named_sessions: Default::default(),
        }
    }
    pub fn sessions(
        items: Vec<Item>,
        active: PathBuf,
        named: std::collections::HashSet<PathBuf>,
    ) -> Self {
        Self {
            active_session: Some(active),
            named_sessions: named,
            ..Self::new("Resume session", items)
        }
    }
    fn matches(&self) -> Vec<&Item> {
        let query = self.query.to_lowercase();
        let mut matches: Vec<_> = self
            .items
            .iter()
            .filter(|item| {
                if let (Some(active), Selection::Resume(path)) =
                    (&self.active_session, &item.selection)
                    && ((!self.all_workspaces && path.parent() != active.parent())
                        || (self.named_only && !self.named_sessions.contains(path)))
                {
                    return false;
                }
                let label = item.label.to_lowercase();
                if self.active_session.is_some() {
                    crate::model_selection::fuzzy(&query, &label)
                } else {
                    label.contains(&query)
                }
            })
            .collect();
        if self.relevance_sort && !query.is_empty() {
            matches.sort_by_key(|item| {
                let label = item.label.to_lowercase();
                (label.find(&query).unwrap_or(usize::MAX), label.len())
            });
        }
        matches
    }
    pub fn input(&mut self, key: KeyEvent, keys: &super::keys::Keys) -> Input {
        if let Some(action) = self.session_action.clone() {
            if key.code == KeyCode::Esc {
                self.session_action = None;
                if matches!(action, SessionAction::Rename(_)) {
                    self.query.clear();
                }
                return Input::Continue;
            }
            match action {
                SessionAction::Delete(path) => {
                    return if key.code == KeyCode::Enter {
                        Input::Select(Selection::DeleteSession(path))
                    } else {
                        Input::Continue
                    };
                }
                SessionAction::Rename(path) if key.code == KeyCode::Enter => {
                    return Input::Select(Selection::RenameSession(path, self.query.clone()));
                }
                SessionAction::Rename(_) => {}
            }
        } else if self.active_session.is_some() {
            self.session_error = None;
            if key.code == KeyCode::Tab {
                self.all_workspaces = !self.all_workspaces;
                self.selected = 0;
                return Input::Continue;
            }
            if keys.matches("app.session.toggleNamedFilter", key) {
                self.named_only = !self.named_only;
                self.selected = 0;
                return Input::Continue;
            }
            if keys.matches("app.session.togglePath", key) {
                self.show_path = !self.show_path;
                return Input::Continue;
            }
            if keys.matches("app.session.toggleSort", key) {
                self.relevance_sort = !self.relevance_sort;
                self.selected = 0;
                return Input::Continue;
            }
            let selected = self
                .matches()
                .get(self.selected)
                .map(|item| item.selection.clone());
            if let Some(Selection::Resume(path)) = selected {
                if keys.matches("app.session.rename", key) {
                    self.session_action = Some(SessionAction::Rename(path));
                    self.query.clear();
                    return Input::Continue;
                }
                if keys.matches("app.session.delete", key)
                    || (self.query.is_empty() && keys.matches("app.session.deleteNoninvasive", key))
                {
                    if path.canonicalize().ok()
                        == self
                            .active_session
                            .as_ref()
                            .and_then(|path| path.canonicalize().ok())
                    {
                        self.session_error = Some("Cannot delete the active session".into());
                    } else {
                        self.session_action = Some(SessionAction::Delete(path));
                    }
                    return Input::Continue;
                }
            }
        }
        let count = self.matches().len();
        match key.code {
            KeyCode::Esc => return Input::Cancel,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Input::Cancel;
            }
            KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(item) = self.matches().get(self.selected) {
                    return Input::Save(item.selection.clone());
                }
            }
            KeyCode::Enter => {
                if let Some(item) = self.matches().get(self.selected) {
                    return Input::Select(item.selection.clone());
                }
            }
            KeyCode::Up => self.selected = self.selected.saturating_sub(1),
            KeyCode::Down => self.selected = (self.selected + 1).min(count.saturating_sub(1)),
            KeyCode::PageUp => self.selected = self.selected.saturating_sub(10),
            KeyCode::PageDown => self.selected = (self.selected + 10).min(count.saturating_sub(1)),
            KeyCode::Home => self.selected = 0,
            KeyCode::End => self.selected = count.saturating_sub(1),
            KeyCode::Backspace => {
                self.query.pop();
                self.selected = 0;
            }
            KeyCode::Char(character)
                if !key.modifiers.intersects(
                    KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                ) =>
            {
                self.query.push(character);
                self.selected = 0;
            }
            _ => {}
        }
        Input::Continue
    }
    pub fn render(&self, theme: &Theme, height: usize) -> Vec<Line> {
        if let Some(action) = &self.session_action {
            let (title, hint) = match action {
                SessionAction::Rename(_) => (
                    format!("Rename session · {}", self.query),
                    "Type the new name · Enter to save · Esc to cancel",
                ),
                SessionAction::Delete(path) => (
                    format!(
                        "Delete {}?",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    "Enter to delete · Esc to cancel",
                ),
            };
            return vec![
                Line::plain(&title, theme.color("accent")),
                Line::plain(hint, theme.color("dim")),
            ];
        }
        let mut lines = vec![Line::plain(
            &format!("{} · {}", self.title, self.query),
            theme.color("accent"),
        )];
        let items = self.matches();
        let available = height
            .saturating_sub(
                3 + usize::from(self.session_error.is_some())
                    + usize::from(self.active_session.is_some()),
            )
            .max(1);
        let start = self.selected.saturating_sub(available - 1);
        for (index, item) in items.iter().enumerate().skip(start).take(available) {
            let mut line = Line::plain(
                &format!(
                    "{} {}",
                    if index == self.selected { "›" } else { " " },
                    if self.show_path {
                        match &item.selection {
                            Selection::Resume(path) => {
                                format!("{} · {}", item.label, path.display())
                            }
                            _ => item.label.clone(),
                        }
                    } else {
                        item.label.clone()
                    }
                ),
                theme.color("text"),
            );
            if index == self.selected {
                line.background = Some(theme.color("selectedBg"));
            }
            lines.push(line);
        }
        if items.is_empty() {
            lines.push(Line::plain("No matches", theme.color("dim")));
        }
        if let Some(error) = &self.session_error {
            lines.push(Line::plain(error, theme.color("error")));
        }
        if self.active_session.is_some() {
            lines.push(Line::plain(
                &format!(
                    "{} · {} · Tab scope · Ctrl+N named · Ctrl+P paths",
                    if self.all_workspaces {
                        "All workspaces"
                    } else {
                        "Current workspace"
                    },
                    if self.named_only {
                        "Named sessions"
                    } else {
                        "All sessions"
                    }
                ),
                theme.color("dim"),
            ));
        }
        let hint = if self.active_session.is_some() {
            if self.relevance_sort {
                "Fuzzy order · Ctrl+S recent · Ctrl+R rename · Ctrl+D delete · Esc cancel"
            } else {
                "Recent order · Ctrl+S fuzzy · Ctrl+R rename · Ctrl+D delete · Esc cancel"
            }
        } else {
            "Type to search · ↑↓ to select · Enter to open · Esc to cancel"
        };
        lines.push(Line::plain(hint, theme.color("dim")));
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_scope_and_name_filters_compose_and_reset_selection() {
        let local = PathBuf::from("one/local.jsonl");
        let remote = PathBuf::from("two/remote.jsonl");
        let mut picker = Picker::sessions(
            vec![
                Item {
                    label: "local".into(),
                    selection: Selection::Resume(local.clone()),
                },
                Item {
                    label: "remote".into(),
                    selection: Selection::Resume(remote.clone()),
                },
            ],
            local.clone(),
            std::collections::HashSet::from([remote]),
        );
        let keys = super::super::keys::Keys::load().unwrap();
        assert_eq!(picker.matches().len(), 1);
        picker.input(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), &keys);
        assert_eq!(picker.matches().len(), 2);
        picker.input(
            KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL),
            &keys,
        );
        assert_eq!(picker.matches().len(), 1);
        assert_eq!(picker.matches()[0].label, "remote");
        picker.input(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), &keys);
        assert!(picker.matches().is_empty());
    }
    #[test]
    fn cancelling_deletion_retains_the_search_and_selection() {
        let path = PathBuf::from("victim.jsonl");
        let mut picker = Picker::sessions(
            vec![Item {
                label: "Victim Session".into(),
                selection: Selection::Resume(path.clone()),
            }],
            PathBuf::from("active.jsonl"),
            Default::default(),
        );
        picker.query = "Victim".into();
        picker.session_action = Some(SessionAction::Delete(path.clone()));
        assert!(matches!(
            picker.input(
                KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
                &super::super::keys::Keys::default()
            ),
            Input::Continue
        ));
        assert_eq!(picker.query, "Victim");
        assert!(picker.session_action.is_none());
        assert!(
            matches!(&picker.matches()[0].selection, Selection::Resume(selected) if *selected == path)
        );
    }
}
