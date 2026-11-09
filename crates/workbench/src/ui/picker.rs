//! Searchable keyboard selectors for session and model navigation.
use super::{render::Line, theme::Theme};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::PathBuf;

#[derive(Clone)]
pub enum Selection {
    Resume(PathBuf),
    Checkout(u64),
    Fork(u64),
    Model(String),
    Theme(String),
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
}
pub enum Input {
    Continue,
    Cancel,
    Select(Selection),
}
impl Picker {
    pub fn new(title: &str, items: Vec<Item>) -> Self {
        Self {
            title: title.into(),
            items,
            query: String::new(),
            selected: 0,
        }
    }
    fn matches(&self) -> Vec<&Item> {
        let query = self.query.to_lowercase();
        self.items
            .iter()
            .filter(|item| item.label.to_lowercase().contains(&query))
            .collect()
    }
    pub fn input(&mut self, key: KeyEvent) -> Input {
        let count = self.matches().len();
        match key.code {
            KeyCode::Esc => return Input::Cancel,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Input::Cancel;
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
        let mut lines = vec![Line::plain(
            &format!("{} · {}", self.title, self.query),
            theme.color("accent"),
        )];
        let items = self.matches();
        let available = height.saturating_sub(3).max(1);
        let start = self.selected.saturating_sub(available - 1);
        for (index, item) in items.iter().enumerate().skip(start).take(available) {
            let mut line = Line::plain(
                &format!(
                    "{} {}",
                    if index == self.selected { "›" } else { " " },
                    item.label
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
        lines.push(Line::plain(
            "Type to search · ↑↓ to select · Enter to open · Esc to cancel",
            theme.color("dim"),
        ));
        lines
    }
}
