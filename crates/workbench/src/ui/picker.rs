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
    Label(u64, String),
    Fork(u64),
    Model(String),
    Theme(String),
    Trust(bool),
    Thinking(String),
    Setting(String, serde_json::Value),
    EditSetting(String),
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
    session_sort: SessionSort,
    all_workspaces: bool,
    named_only: bool,
    show_path: bool,
    named_sessions: std::collections::HashSet<PathBuf>,
    session_documents: std::collections::BTreeMap<PathBuf, super::session_search::Document>,
    tree: Option<TreeState>,
}
#[derive(Clone, Copy, PartialEq)]
enum SessionSort {
    Threaded,
    Recent,
    Relevance,
}
#[derive(Clone)]
enum SessionAction {
    Rename(PathBuf),
    Delete(PathBuf),
    Label(u64),
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
            session_sort: SessionSort::Threaded,
            all_workspaces: false,
            named_only: false,
            show_path: false,
            named_sessions: Default::default(),
            session_documents: Default::default(),
            tree: None,
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
    pub fn set_session_documents(
        &mut self,
        documents: std::collections::BTreeMap<PathBuf, super::session_search::Document>,
    ) {
        self.session_documents = documents;
    }
    pub fn tree(
        items: Vec<Item>,
        parents: std::collections::BTreeMap<u64, Option<u64>>,
        labels: std::collections::BTreeMap<u64, String>,
        head: u64,
    ) -> Self {
        let tree = TreeState::new(parents, labels, head);
        let mut items: std::collections::BTreeMap<_, _> = items
            .into_iter()
            .filter_map(|item| {
                if let Selection::Checkout(id) = item.selection {
                    Some((id, item))
                } else {
                    None
                }
            })
            .collect();
        let ordered = tree
            .order
            .iter()
            .filter_map(|id| items.remove(id))
            .collect();
        let mut picker = Self {
            tree: Some(tree),
            ..Self::new("Session tree", ordered)
        };
        picker.focus_record(head);
        picker
    }
    pub fn focus_record(&mut self, id: u64) {
        if let Some(index) = self
            .matches()
            .iter()
            .position(|item| matches!(item.selection, Selection::Checkout(value) if value == id))
        {
            self.selected = index;
        }
    }
    fn matches(&self) -> Vec<&Item> {
        let query = self.query.to_lowercase();
        let session_query = super::session_search::Query::new(&self.query);
        let mut matches: Vec<_> = self
            .items
            .iter()
            .filter(|item| {
                if let (Some(active), Selection::Resume(path)) =
                    (&self.active_session, &item.selection)
                    && ((!self.all_workspaces
                        && !self
                            .session_documents
                            .get(path)
                            .map_or(path.parent() == active.parent(), |document| document.local))
                        || (self.named_only && !self.named_sessions.contains(path)))
                {
                    return false;
                }
                if query.is_empty()
                    && let (Some(tree), Selection::Checkout(id)) = (&self.tree, &item.selection)
                    && !tree.visible.contains(id)
                {
                    return false;
                }
                if let Selection::Resume(path) = &item.selection
                    && let Some(document) = self.session_documents.get(path)
                {
                    return session_query.score(document).is_some();
                }
                let label = item.label.to_lowercase();
                if self.active_session.is_some() {
                    crate::model_selection::fuzzy(&query, &label)
                } else {
                    label.contains(&query)
                }
            })
            .collect();
        if self.session_sort != SessionSort::Recent && !query.trim().is_empty() {
            matches.sort_by_key(|item| {
                if let Selection::Resume(path) = &item.selection
                    && let Some(document) = self.session_documents.get(path)
                {
                    return (session_query.score(document).unwrap_or(i64::MAX), 0);
                }
                let label = item.label.to_lowercase();
                (
                    label
                        .find(&query)
                        .map_or(i64::MAX, |position| position as i64),
                    label.len(),
                )
            });
        }
        if self.active_session.is_some()
            && self.session_sort == SessionSort::Threaded
            && query.trim().is_empty()
            && let Some(rows) = self.session_layout(&matches)
        {
            return rows.into_iter().map(|row| matches[row.index]).collect();
        }
        matches
    }
    fn session_layout(&self, items: &[&Item]) -> Option<Vec<super::session_threads::Row>> {
        let entries: Option<Vec<_>> = items
            .iter()
            .map(|item| {
                let Selection::Resume(path) = &item.selection else {
                    return None;
                };
                Some((
                    path.clone(),
                    self.session_documents
                        .get(path)
                        .and_then(|document| document.parent.clone()),
                ))
            })
            .collect();
        let (paths, parents): (Vec<_>, Vec<_>) = entries?.into_iter().unzip();
        Some(super::session_threads::layout(&paths, &parents))
    }
    pub fn input(&mut self, key: KeyEvent, keys: &super::keys::Keys) -> Input {
        if let Some(action) = self.session_action.clone() {
            if key.code == KeyCode::Esc {
                self.session_action = None;
                if matches!(action, SessionAction::Rename(_) | SessionAction::Label(_)) {
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
                SessionAction::Label(id) if key.code == KeyCode::Enter => {
                    return Input::Select(Selection::Label(id, self.query.clone()));
                }
                SessionAction::Rename(_) | SessionAction::Label(_) => {}
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
                self.session_sort = match self.session_sort {
                    SessionSort::Threaded => SessionSort::Recent,
                    SessionSort::Recent => SessionSort::Relevance,
                    SessionSort::Relevance => SessionSort::Threaded,
                };
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
        if self.session_action.is_none() && self.tree.is_some() {
            let selected =
                self.matches()
                    .get(self.selected)
                    .and_then(|item| match item.selection {
                        Selection::Checkout(id) => Some(id),
                        _ => None,
                    });
            if let Some(id) = selected {
                if keys.matches("app.tree.editLabel", key) {
                    self.query = self
                        .tree
                        .as_ref()
                        .and_then(|tree| tree.labels.get(&id))
                        .cloned()
                        .unwrap_or_default();
                    self.session_action = Some(SessionAction::Label(id));
                    return Input::Continue;
                }
                if keys.matches("app.tree.foldOrUp", key) {
                    let tree = self.tree.as_mut().expect("tree picker");
                    let target = if tree
                        .children
                        .get(&id)
                        .is_some_and(|children| !children.is_empty())
                        && tree.collapsed.insert(id)
                    {
                        tree.rebuild_visible();
                        id
                    } else {
                        tree.parents.get(&id).copied().flatten().unwrap_or(id)
                    };
                    self.focus_record(target);
                    return Input::Continue;
                }
                if keys.matches("app.tree.unfoldOrDown", key) {
                    let tree = self.tree.as_mut().expect("tree picker");
                    let target = if tree.collapsed.remove(&id) {
                        tree.rebuild_visible();
                        id
                    } else {
                        tree.children
                            .get(&id)
                            .and_then(|children| children.first())
                            .copied()
                            .unwrap_or(id)
                    };
                    self.focus_record(target);
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
                SessionAction::Label(id) => (
                    format!("Label record {id} · {}", self.query),
                    "Type the label · Enter to save · Empty to remove · Esc to cancel",
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
        let mut prefixes = vec![String::new(); items.len()];
        if self.active_session.is_some()
            && self.session_sort == SessionSort::Threaded
            && self.query.trim().is_empty()
            && let Some(rows) = self.session_layout(&items)
        {
            for row in rows {
                prefixes[row.index] = row.prefix;
            }
        }
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
                    "{} {}{}",
                    if index == self.selected { "›" } else { " " },
                    prefixes[index],
                    if self.show_path {
                        match &item.selection {
                            Selection::Resume(path) => {
                                format!("{} · {}", item.label, path.display())
                            }
                            _ => item.label.clone(),
                        }
                    } else if let (Some(tree), Selection::Checkout(id)) =
                        (&self.tree, &item.selection)
                    {
                        format!(
                            "{}{}{} {}",
                            "  ".repeat(tree.depths.get(id).copied().unwrap_or(0).min(12)),
                            if tree.collapsed.contains(id) {
                                "▸"
                            } else if tree
                                .children
                                .get(id)
                                .is_some_and(|children| !children.is_empty())
                            {
                                "▾"
                            } else {
                                "·"
                            },
                            if *id == tree.head { "●" } else { " " },
                            item.label
                        )
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
            match self.session_sort {
                SessionSort::Threaded => {
                    "Threaded · Ctrl+S recent · Ctrl+R rename · Ctrl+D delete · Esc cancel"
                }
                SessionSort::Recent => {
                    "Recent · Ctrl+S relevance · Ctrl+R rename · Ctrl+D delete · Esc cancel"
                }
                SessionSort::Relevance => {
                    "Relevance · Ctrl+S threaded · Ctrl+R rename · Ctrl+D delete · Esc cancel"
                }
            }
        } else if self.tree.is_some() {
            "Type to search · Alt+←/→ fold/navigate · Shift+L label · Enter checkout · Esc cancel"
        } else {
            "Type to search · ↑↓ to select · Enter to open · Esc to cancel"
        };
        lines.push(Line::plain(hint, theme.color("dim")));
        lines
    }
}

struct TreeState {
    parents: std::collections::BTreeMap<u64, Option<u64>>,
    children: std::collections::BTreeMap<u64, Vec<u64>>,
    labels: std::collections::BTreeMap<u64, String>,
    roots: Vec<u64>,
    order: Vec<u64>,
    depths: std::collections::BTreeMap<u64, usize>,
    collapsed: std::collections::BTreeSet<u64>,
    visible: std::collections::BTreeSet<u64>,
    head: u64,
}
impl TreeState {
    fn new(
        parents: std::collections::BTreeMap<u64, Option<u64>>,
        labels: std::collections::BTreeMap<u64, String>,
        head: u64,
    ) -> Self {
        let mut children = std::collections::BTreeMap::<_, Vec<_>>::new();
        let mut roots = Vec::new();
        for (&id, parent) in &parents {
            if let Some(parent) = parent.filter(|parent| parents.contains_key(parent)) {
                children.entry(parent).or_default().push(id);
            } else {
                roots.push(id);
            }
        }
        let mut tree = Self {
            parents,
            children,
            labels,
            roots,
            order: vec![],
            depths: Default::default(),
            collapsed: Default::default(),
            visible: Default::default(),
            head,
        };
        let mut pending: Vec<_> = tree.roots.iter().rev().map(|id| (*id, 0usize)).collect();
        while let Some((id, depth)) = pending.pop() {
            tree.order.push(id);
            tree.depths.insert(id, depth);
            if let Some(children) = tree.children.get(&id) {
                let next_depth = depth + usize::from(children.len() > 1);
                pending.extend(children.iter().rev().map(|child| (*child, next_depth)));
            }
        }
        tree.rebuild_visible();
        tree
    }
    fn rebuild_visible(&mut self) {
        self.visible.clear();
        let mut pending = self.roots.clone();
        while let Some(id) = pending.pop() {
            self.visible.insert(id);
            if !self.collapsed.contains(&id)
                && let Some(children) = self.children.get(&id)
            {
                pending.extend(children);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tree_folding_search_navigation_and_labels_preserve_record_identity() {
        let parents = [
            (1, None),
            (2, Some(1)),
            (3, Some(2)),
            (4, Some(1)),
            (5, Some(4)),
        ]
        .into_iter()
        .collect();
        let items = (1..=5)
            .map(|id| Item {
                label: format!("node {id}"),
                selection: Selection::Checkout(id),
            })
            .collect();
        let mut picker = Picker::tree(
            items,
            parents,
            [(1, "root".into())].into_iter().collect(),
            5,
        );
        let keys = super::super::keys::Keys::load().unwrap();
        assert_eq!(picker.selected, 4);
        picker.focus_record(1);
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        assert_eq!(picker.matches().len(), 1);
        picker.query = "3".into();
        assert_eq!(picker.matches()[0].label, "node 3");
        picker.query.clear();
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT), &keys);
        assert_eq!(picker.matches().len(), 5);
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT), &keys);
        assert!(matches!(
            picker.matches()[picker.selected].selection,
            Selection::Checkout(2)
        ));
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        assert!(matches!(
            picker.matches()[picker.selected].selection,
            Selection::Checkout(1)
        ));
        picker.input(
            KeyEvent::new(KeyCode::Char('L'), KeyModifiers::SHIFT),
            &keys,
        );
        assert_eq!(picker.query, "root");
        picker.query = "updated".into();
        assert!(
            matches!(picker.input(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &keys), Input::Select(Selection::Label(1, value)) if value == "updated")
        );
    }

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
    fn session_threads_group_parents_and_cycle_through_reference_sort_modes() {
        let parent = PathBuf::from("parent.jsonl");
        let child = PathBuf::from("child.jsonl");
        let other = PathBuf::from("other.jsonl");
        let paths = [child.clone(), other.clone(), parent.clone()];
        let mut picker = Picker::sessions(
            paths
                .iter()
                .map(|path| Item {
                    label: path.to_string_lossy().into_owned(),
                    selection: Selection::Resume(path.clone()),
                })
                .collect(),
            parent.clone(),
            std::collections::HashSet::from([child.clone()]),
        );
        let mut documents = std::collections::BTreeMap::new();
        for path in &paths {
            let mut document = super::super::session_search::Document::new(
                path.to_string_lossy().into_owned(),
                String::new(),
                String::new(),
                "needle".into(),
                true,
            );
            if *path == child {
                document.parent = Some(parent.clone());
            }
            documents.insert(path.clone(), document);
        }
        picker.set_session_documents(documents);
        let selected = |picker: &Picker| {
            picker
                .matches()
                .into_iter()
                .map(|item| match &item.selection {
                    Selection::Resume(path) => path.clone(),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            selected(&picker),
            vec![parent.clone(), child.clone(), other.clone()]
        );
        picker.query = "needle".into();
        assert_eq!(selected(&picker), paths);
        picker.query.clear();
        let keys = super::super::keys::Keys::load().unwrap();
        picker.input(
            KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
            &keys,
        );
        assert_eq!(selected(&picker), paths);
        picker.input(
            KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
            &keys,
        );
        assert!(picker.session_sort == SessionSort::Relevance);
        picker.input(
            KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
            &keys,
        );
        assert!(picker.session_sort == SessionSort::Threaded);
        picker.named_only = true;
        assert_eq!(selected(&picker), vec![child]);
    }

    #[test]
    fn session_search_uses_body_and_workspace_metadata_in_shared_directories() {
        let local = PathBuf::from("shared/local.jsonl");
        let remote = PathBuf::from("shared/remote.jsonl");
        let mut picker = Picker::sessions(
            vec![
                Item {
                    label: "Visible title".into(),
                    selection: Selection::Resume(local.clone()),
                },
                Item {
                    label: "Other title".into(),
                    selection: Selection::Resume(remote.clone()),
                },
            ],
            local.clone(),
            Default::default(),
        );
        picker.set_session_documents(std::collections::BTreeMap::from([
            (
                local.clone(),
                super::super::session_search::Document::new(
                    "Visible title".into(),
                    "a".into(),
                    "/one".into(),
                    "hidden transcript needle".into(),
                    true,
                ),
            ),
            (
                remote,
                super::super::session_search::Document::new(
                    "Other title".into(),
                    "b".into(),
                    "/two".into(),
                    "needle".into(),
                    false,
                ),
            ),
        ]));
        picker.query = "needle".into();
        assert_eq!(picker.matches().len(), 1);
        assert!(
            matches!(&picker.matches()[0].selection, Selection::Resume(path) if *path == local)
        );
        picker.all_workspaces = true;
        assert_eq!(picker.matches().len(), 2);
        picker.query = "re:transcript.*needle".into();
        assert_eq!(picker.matches().len(), 1);
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
