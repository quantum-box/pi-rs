//! Searchable keyboard selectors for session and model navigation.
use super::{keys::Keys, render::Line, theme::Theme};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::PathBuf;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Clone)]
pub enum Selection {
    Resume(PathBuf),
    RenameSession(PathBuf, String),
    DeleteSession(PathBuf),
    Checkout(u64),
    CheckoutWithoutSummary(u64),
    CheckoutWithSummary(u64),
    EditBranchSummary(u64),
    Label(u64, String),
    Fork(u64),
    Model(String),
    LoginProvider(super::super::Provider),
    LoginOpenRouter,
    LogoutProvider(super::super::Provider),
    RadiusMethods,
    LoginRadius(crate::radius_auth::Method),
    LogoutRadius,
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
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TreeEntryKind {
    User,
    Tool,
    Message,
    Bookkeeping,
    Hidden,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TreeFilter {
    Default,
    NoTools,
    UserOnly,
    LabeledOnly,
    All,
}

impl TreeFilter {
    const MODES: [(Self, &'static str, &'static str); 5] = [
        (Self::Default, "default", "app.tree.filter.default"),
        (Self::NoTools, "no-tools", "app.tree.filter.noTools"),
        (Self::UserOnly, "user-only", "app.tree.filter.userOnly"),
        (
            Self::LabeledOnly,
            "labeled-only",
            "app.tree.filter.labeledOnly",
        ),
        (Self::All, "all", "app.tree.filter.all"),
    ];

    pub fn parse(value: &str) -> Self {
        Self::MODES
            .iter()
            .find(|(_, name, _)| *name == value)
            .map_or(Self::Default, |(mode, _, _)| *mode)
    }

    pub fn next(self) -> Self {
        self.cycle(1)
    }

    pub fn name(self) -> &'static str {
        Self::MODES[self.index()].1
    }

    fn index(self) -> usize {
        Self::MODES
            .iter()
            .position(|(mode, _, _)| *mode == self)
            .expect("tree filter")
    }

    fn cycle(self, offset: usize) -> Self {
        Self::MODES[(self.index() + offset) % Self::MODES.len()].0
    }

    fn filter_for_key(self, key: KeyEvent, keys: &Keys) -> Option<Self> {
        if keys.matches("app.tree.filter.cycleForward", key) {
            return Some(self.next());
        }
        if keys.matches("app.tree.filter.cycleBackward", key) {
            return Some(self.cycle(Self::MODES.len() - 1));
        }
        Self::MODES
            .iter()
            .find(|(_, _, action)| keys.matches(action, key))
            .map(|(mode, _, _)| if self == *mode { Self::Default } else { *mode })
    }
}

pub struct Picker {
    pub title: String,
    items: Vec<Item>,
    query: String,
    selected: usize,
    page_size: std::cell::Cell<usize>,
    active_session: Option<PathBuf>,
    session_action: Option<SessionAction>,
    feedback: Option<Result<String, String>>,
    session_sort: SessionSort,
    all_workspaces: bool,
    named_only: bool,
    show_path: bool,
    named_sessions: std::collections::HashSet<PathBuf>,
    session_documents: std::collections::BTreeMap<PathBuf, super::session_search::Document>,
    tree: Option<TreeState>,
    model_scope: Option<ModelScope>,
}

struct ModelScope {
    provider: String,
    catalog: Vec<String>,
    enabled: Vec<String>,
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
    CopyTreeRecord(u64),
    UpdateModelScope { save: bool },
}
impl Picker {
    pub fn new(title: &str, items: Vec<Item>) -> Self {
        Self {
            title: title.into(),
            items,
            query: String::new(),
            selected: 0,
            page_size: std::cell::Cell::new(10),
            active_session: None,
            session_action: None,
            feedback: None,
            session_sort: SessionSort::Threaded,
            all_workspaces: false,
            named_only: false,
            show_path: false,
            named_sessions: Default::default(),
            session_documents: Default::default(),
            tree: None,
            model_scope: None,
        }
    }
    pub fn scoped_models(provider: &str, catalog: Vec<String>, mut enabled: Vec<String>) -> Self {
        let available: std::collections::HashSet<_> = catalog.iter().collect();
        enabled.retain(|model| available.contains(model));
        let mut picker = Self {
            model_scope: Some(ModelScope {
                provider: provider.to_owned(),
                catalog,
                enabled,
            }),
            ..Self::new("Model Configuration", vec![])
        };
        picker.refresh_model_scope(None);
        picker
    }

    pub fn model_scope(&self) -> Option<(&[String], &[String])> {
        self.model_scope
            .as_ref()
            .map(|scope| (scope.catalog.as_slice(), scope.enabled.as_slice()))
    }

    fn refresh_model_scope(&mut self, selected: Option<&str>) {
        let scope = self.model_scope.as_ref().expect("model scope picker");
        let enabled: std::collections::HashSet<_> = scope.enabled.iter().collect();
        self.items = scope
            .enabled
            .iter()
            .chain(
                scope
                    .catalog
                    .iter()
                    .filter(|model| !enabled.contains(model)),
            )
            .map(|model| {
                let (provider, id) = model
                    .split_once('/')
                    .unwrap_or((&scope.provider, model.as_str()));
                Item {
                    label: format!(
                        "{} {id} [{provider}]",
                        if enabled.contains(model) { "✓" } else { " " }
                    ),
                    selection: Selection::Model(model.clone()),
                }
            })
            .collect();
        let matches = self.matches();
        self.selected = selected
            .and_then(|selected| {
                matches.iter().position(
                    |item| matches!(&item.selection, Selection::Model(model) if model == selected),
                )
            })
            .unwrap_or_else(|| self.selected.min(matches.len().saturating_sub(1)));
    }

    fn model_scope_input(&mut self, key: KeyEvent, keys: &Keys) -> Option<Input> {
        if keys.matches("tui.select.cancel", key)
            && key.code == KeyCode::Char('c')
            && key.modifiers.contains(KeyModifiers::CONTROL)
            && !self.query.is_empty()
        {
            self.query.clear();
            self.selected = 0;
            return Some(Input::Continue);
        }
        if keys.matches("app.models.save", key) {
            return Some(Input::UpdateModelScope { save: true });
        }
        let toggle = keys.matches("tui.select.confirm", key);
        let enable = keys.matches("app.models.enableAll", key);
        let clear = keys.matches("app.models.clearAll", key);
        let provider = keys.matches("app.models.toggleProvider", key);
        let up = keys.matches("app.models.reorderUp", key);
        let down = keys.matches("app.models.reorderDown", key);
        if !(toggle || enable || clear || provider || up || down) {
            return None;
        }
        let visible: Vec<_> = self
            .matches()
            .into_iter()
            .filter_map(|item| match &item.selection {
                Selection::Model(model) => Some(model.clone()),
                _ => None,
            })
            .collect();
        let selected = visible.get(self.selected);
        let scope = self.model_scope.as_mut().expect("model scope picker");
        if up || down {
            if let Some(index) =
                selected.and_then(|model| scope.enabled.iter().position(|id| id == model))
            {
                let neighbor = if up {
                    index.checked_sub(1)
                } else {
                    Some(index + 1)
                };
                if let Some(neighbor) = neighbor.filter(|index| *index < scope.enabled.len()) {
                    scope.enabled.swap(index, neighbor);
                }
            }
        } else {
            let targets = if toggle {
                selected.into_iter().cloned().collect()
            } else if provider {
                selected.map_or_else(Vec::new, |model| {
                    let selected_provider = model
                        .split_once('/')
                        .map_or(scope.provider.as_str(), |(provider, _)| provider);
                    scope
                        .catalog
                        .iter()
                        .filter(|model| {
                            model
                                .split_once('/')
                                .map_or(scope.provider.as_str(), |(provider, _)| provider)
                                == selected_provider
                        })
                        .cloned()
                        .collect()
                })
            } else {
                visible.clone()
            };
            let mut enabled: std::collections::HashSet<_> = scope.enabled.iter().cloned().collect();
            if clear
                || ((provider || toggle) && targets.iter().all(|model| enabled.contains(model)))
            {
                let targets: std::collections::HashSet<_> = targets.into_iter().collect();
                scope.enabled.retain(|model| !targets.contains(model));
            } else {
                scope.enabled.extend(
                    targets
                        .into_iter()
                        .filter(|model| enabled.insert(model.clone())),
                );
            }
        }
        self.refresh_model_scope(selected.map(String::as_str));
        Some(Input::UpdateModelScope { save: false })
    }

    pub fn show_feedback(&mut self, feedback: Result<String, String>) {
        self.feedback = Some(feedback);
    }

    pub fn select_last(&mut self) {
        self.selected = self.matches().len().saturating_sub(1);
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
        entries: std::collections::BTreeMap<u64, (Option<u64>, TreeEntryKind)>,
        labels: std::collections::BTreeMap<u64, String>,
        head: u64,
        filter: TreeFilter,
    ) -> Self {
        let tree = TreeState::new(entries, labels, head, filter);
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
    pub fn set_label_timestamps(&mut self, timestamps: std::collections::BTreeMap<u64, String>) {
        if let Some(tree) = &mut self.tree {
            tree.label_timestamps = timestamps;
        }
    }
    pub fn focus_thinking(&mut self, current: &str) {
        if let Some(index) = self.items.iter().position(
            |item| matches!(&item.selection, Selection::Thinking(level) if level == current),
        ) {
            self.selected = index;
        }
    }
    pub fn focus_record(&mut self, id: u64) {
        let visible: std::collections::HashMap<_, _> = self
            .matches()
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item.selection {
                Selection::Checkout(id) => Some((id, index)),
                _ => None,
            })
            .collect();
        let mut target = Some(id);
        while let Some(id) = target {
            if let Some(&index) = visible.get(&id) {
                self.selected = index;
                return;
            }
            target = self
                .tree
                .as_ref()
                .and_then(|tree| tree.parents.get(&id).copied().flatten());
        }
        self.selected = visible.len().saturating_sub(1);
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
                if let (Some(tree), Selection::Checkout(id)) = (&self.tree, &item.selection)
                    && (!tree.passes_filter(*id)
                        || (query.is_empty() && !tree.visible.contains(id)))
                {
                    return false;
                }
                if let Selection::Resume(path) = &item.selection
                    && let Some(document) = self.session_documents.get(path)
                {
                    return session_query.score(document).is_some();
                }
                let label = item.label.to_lowercase();
                if self.active_session.is_some() || self.model_scope.is_some() {
                    crate::model_selection::fuzzy(&query, &label)
                } else {
                    label.contains(&query)
                }
            })
            .collect();
        if self.tree.is_none()
            && self.model_scope.is_none()
            && self.session_sort != SessionSort::Recent
            && !query.trim().is_empty()
        {
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
    pub fn input(&mut self, key: KeyEvent, keys: &Keys) -> Input {
        self.feedback = None;
        if self.model_scope.is_some()
            && let Some(input) = self.model_scope_input(key, keys)
        {
            return input;
        }
        let cancel = keys.matches("tui.select.cancel", key);
        let confirm = keys.matches("tui.select.confirm", key);
        if let Some(action) = self.session_action.clone() {
            if cancel {
                self.session_action = None;
                if matches!(action, SessionAction::Rename(_) | SessionAction::Label(_)) {
                    self.query.clear();
                }
                return Input::Continue;
            }
            match action {
                SessionAction::Delete(path) => {
                    return if confirm {
                        Input::Select(Selection::DeleteSession(path))
                    } else {
                        Input::Continue
                    };
                }
                SessionAction::Rename(path) if confirm => {
                    return Input::Select(Selection::RenameSession(path, self.query.clone()));
                }
                SessionAction::Label(id) if confirm => {
                    return Input::Select(Selection::Label(id, self.query.clone()));
                }
                SessionAction::Rename(_) | SessionAction::Label(_) => {}
            }
        } else if self.active_session.is_some() {
            if keys.matches("tui.input.tab", key) {
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
                        self.show_feedback(Err("Cannot delete the active session".into()));
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
            let tree = self.tree.as_mut().expect("tree picker");
            if keys.matches("app.tree.toggleLabelTimestamp", key) {
                tree.show_label_timestamps = !tree.show_label_timestamps;
                return Input::Continue;
            }
            if let Some(filter) = tree.filter.filter_for_key(key, keys) {
                tree.filter = filter;
                tree.collapsed.clear();
                tree.rebuild_visible();
                let focus = selected.unwrap_or(tree.head);
                self.focus_record(focus);
                return Input::Continue;
            }
            if let Some(id) = selected {
                if keys.matches("app.message.copy", key) {
                    return Input::CopyTreeRecord(id);
                }
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
        let matches = self.matches();
        let count = matches.len();
        if let Some(item) = matches.get(self.selected) {
            let save = match &item.selection {
                Selection::Thinking(_) => keys.matches("app.thinking.save", key),
                Selection::Model(_) => keys.matches("app.models.save", key),
                _ => false,
            };
            if save {
                return Input::Save(item.selection.clone());
            }
            if confirm {
                return Input::Select(item.selection.clone());
            }
        }
        if cancel {
            if let Some(tree) = &mut self.tree
                && !self.query.is_empty()
            {
                self.query.clear();
                tree.collapsed.clear();
                tree.rebuild_visible();
                let head = tree.head;
                self.focus_record(head);
                return Input::Continue;
            }
            return Input::Cancel;
        }
        let tree = self.tree.is_some();
        if keys.matches("tui.select.up", key) {
            self.selected = self
                .selected
                .checked_sub(1)
                .unwrap_or(count.saturating_sub(1));
            return Input::Continue;
        }
        if keys.matches("tui.select.down", key) {
            self.selected = (self.selected + 1) % count.max(1);
            return Input::Continue;
        }
        if keys.matches("tui.select.pageUp", key)
            || (tree && keys.matches("tui.editor.cursorLeft", key))
        {
            self.selected = self.selected.saturating_sub(self.page_size.get());
            return Input::Continue;
        }
        if keys.matches("tui.select.pageDown", key)
            || (tree && keys.matches("tui.editor.cursorRight", key))
        {
            self.selected = (self.selected + self.page_size.get()).min(count.saturating_sub(1));
            return Input::Continue;
        }
        if keys.matches("tui.editor.deleteCharBackward", key) {
            self.query.pop();
            self.selected = 0;
            return Input::Continue;
        }
        match key.code {
            KeyCode::Home => self.selected = 0,
            KeyCode::End => self.selected = count.saturating_sub(1),
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
    fn session_line(
        &self,
        item: &Item,
        prefix: &str,
        selected: bool,
        theme: &Theme,
        width: usize,
    ) -> Line {
        let Selection::Resume(path) = &item.selection else {
            unreachable!("session row");
        };
        let document = self.session_documents.get(path).expect("session document");
        let age = document
            .activity
            .map(|time| {
                session_age(
                    std::time::SystemTime::now()
                        .duration_since(time)
                        .unwrap_or_default(),
                )
            })
            .unwrap_or_else(|| "?".into());
        let mut right = format!("{} {age}", document.message_count);
        if self.all_workspaces && !document.cwd().is_empty() {
            right = format!(
                "{} {right}",
                short_path(std::path::Path::new(document.cwd()))
            );
        }
        if self.show_path {
            right = format!("{} {right}", short_path(path));
        }
        let title: String = item
            .label
            .chars()
            .map(|character| {
                if character <= '\u{1f}' || character == '\u{7f}' {
                    ' '
                } else {
                    character
                }
            })
            .collect();
        let title = truncate(
            title.trim(),
            width
                .saturating_sub(2 + prefix.width() + right.width() + 2)
                .max(10),
        );
        let active = self.active_session.as_ref().is_some_and(|active| {
            active
                .canonicalize()
                .ok()
                .zip(path.canonicalize().ok())
                .is_some_and(|(active, path)| active == path)
        });
        let color = if active {
            "accent"
        } else if self.named_sessions.contains(path) {
            "warning"
        } else {
            "text"
        };
        let mut title_span = super::render::Span::new(title.clone(), theme.color(color));
        title_span.bold = selected;
        let spacing = width
            .saturating_sub(2 + prefix.width() + title.width() + right.width())
            .max(1);
        Line {
            spans: vec![
                super::render::Span::new(if selected { "› " } else { "  " }, theme.color("accent")),
                super::render::Span::new(prefix, theme.color("dim")),
                title_span,
                super::render::Span::new(
                    format!("{}{right}", " ".repeat(spacing)),
                    theme.color("dim"),
                ),
            ],
            background: selected.then(|| theme.color("selectedBg")),
            image: None,
        }
    }

    pub fn render(&self, theme: &Theme, width: usize, height: usize, keys: &Keys) -> Vec<Line> {
        let hint = |action| keys.hint(action);
        if let Some(action) = &self.session_action {
            let confirm = hint("tui.select.confirm");
            let cancel = hint("tui.select.cancel");
            let (title, hint) = match action {
                SessionAction::Rename(_) => (
                    format!("Rename session · {}", self.query),
                    format!("Type the new name · {confirm} to save · {cancel} to cancel"),
                ),
                SessionAction::Label(id) => (
                    format!("Label record {id} · {}", self.query),
                    format!(
                        "Type the label · {confirm} to save · Empty to remove · {cancel} to cancel"
                    ),
                ),
                SessionAction::Delete(path) => (
                    format!(
                        "Delete {}?",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    format!("{confirm} to delete · {cancel} to cancel"),
                ),
            };
            return vec![
                Line::plain(&title, theme.color("accent")),
                Line::plain(&hint, theme.color("dim")),
            ];
        }
        let filter = self
            .tree
            .as_ref()
            .map_or_else(String::new, |tree| format!(" · {}", tree.filter.name()));
        let mut lines = vec![Line::plain(
            &format!("{}{filter} · {}", self.title, self.query),
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
        let mut available = height
            .saturating_sub(
                3 + usize::from(self.feedback.is_some())
                    + usize::from(self.active_session.is_some()),
            )
            .max(1);
        let scrolling = self.active_session.is_some() && items.len() > available;
        if scrolling {
            available = available.saturating_sub(1).max(1);
        }
        self.page_size.set(available);
        let start = if self.active_session.is_some() {
            self.selected
                .saturating_sub(available / 2)
                .min(items.len().saturating_sub(available))
        } else {
            self.selected.saturating_sub(available - 1)
        };
        for (index, item) in items.iter().enumerate().skip(start).take(available) {
            if let Selection::Resume(path) = &item.selection
                && self.session_documents.contains_key(path)
            {
                lines.push(self.session_line(
                    item,
                    &prefixes[index],
                    index == self.selected,
                    theme,
                    width,
                ));
                continue;
            }
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
                            "{}{}{} {}{}",
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
                            if tree.show_label_timestamps {
                                tree.label_timestamps
                                    .get(id)
                                    .map_or_else(String::new, |timestamp| {
                                        format!("{} ", format_label_timestamp(timestamp))
                                    })
                            } else {
                                String::new()
                            },
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
        if scrolling {
            lines.push(Line::plain(
                &format!("  ({}/{})", self.selected + 1, items.len()),
                theme.color("dim"),
            ));
        }
        if items.is_empty() {
            let message =
                if self.active_session.is_some() && self.query.is_empty() && !self.all_workspaces {
                    "No sessions in current folder. Press Tab to view all."
                } else {
                    "No matches"
                };
            lines.push(Line::plain(message, theme.color("dim")));
        }
        if let Some(feedback) = &self.feedback {
            let (message, role) = match feedback {
                Ok(message) => (message, "dim"),
                Err(message) => (message, "error"),
            };
            lines.push(Line::plain(message, theme.color(role)));
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
            let (sort, next) = match self.session_sort {
                SessionSort::Threaded => ("Threaded", "recent"),
                SessionSort::Recent => ("Recent", "relevance"),
                SessionSort::Relevance => ("Relevance", "threaded"),
            };
            format!(
                "{sort} · {} {next} · {} rename · {} delete · {} cancel",
                hint("app.session.toggleSort"),
                hint("app.session.rename"),
                hint("app.session.delete"),
                hint("tui.select.cancel"),
            )
        } else if self.model_scope.is_some() {
            format!(
                "{} toggle · {}/{} reorder · {} enable · {} clear · {} save",
                hint("tui.select.confirm"),
                hint("app.models.reorderUp"),
                hint("app.models.reorderDown"),
                hint("app.models.enableAll"),
                hint("app.models.clearAll"),
                hint("app.models.save"),
            )
        } else if self.tree.is_some() {
            format!(
                "{} filter · {}/{} navigate · {} label · {} timestamps · {} copy · {} checkout",
                hint("app.tree.filter.cycleForward"),
                hint("app.tree.foldOrUp"),
                hint("app.tree.unfoldOrDown"),
                hint("app.tree.editLabel"),
                hint("app.tree.toggleLabelTimestamp"),
                hint("app.message.copy"),
                hint("tui.select.confirm"),
            )
        } else {
            let save = self.items.first().and_then(|item| match item.selection {
                Selection::Thinking(_) => Some("app.thinking.save"),
                Selection::Model(_) => Some("app.models.save"),
                _ => None,
            });
            format!(
                "Type to search · {}/{} to select · {} to open · {}{} to cancel",
                hint("tui.select.up"),
                hint("tui.select.down"),
                hint("tui.select.confirm"),
                save.map_or_else(String::new, |action| format!(
                    "{} to set as default · ",
                    hint(action)
                )),
                hint("tui.select.cancel"),
            )
        };
        lines.push(Line::plain(&hint, theme.color("dim")));
        lines
    }
}

fn format_label_timestamp(timestamp: &str) -> String {
    use chrono::{Datelike, Local};
    let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(timestamp) else {
        return timestamp.to_owned();
    };
    let date = timestamp.with_timezone(&Local);
    let today = Local::now().date_naive();
    let format = if date.date_naive() == today {
        "%H:%M"
    } else if date.year() == today.year() {
        "%-m/%-d %H:%M"
    } else {
        "%y/%-m/%-d %H:%M"
    };
    date.format(format).to_string()
}

struct TreeState {
    kinds: std::collections::BTreeMap<u64, TreeEntryKind>,
    filter: TreeFilter,
    parents: std::collections::BTreeMap<u64, Option<u64>>,
    children: std::collections::BTreeMap<u64, Vec<u64>>,
    labels: std::collections::BTreeMap<u64, String>,
    label_timestamps: std::collections::BTreeMap<u64, String>,
    show_label_timestamps: bool,
    roots: Vec<u64>,
    order: Vec<u64>,
    depths: std::collections::BTreeMap<u64, usize>,
    collapsed: std::collections::BTreeSet<u64>,
    visible: std::collections::BTreeSet<u64>,
    head: u64,
}
impl TreeState {
    fn new(
        entries: std::collections::BTreeMap<u64, (Option<u64>, TreeEntryKind)>,
        labels: std::collections::BTreeMap<u64, String>,
        head: u64,
        filter: TreeFilter,
    ) -> Self {
        let (parents, kinds): (std::collections::BTreeMap<_, _>, _) = entries
            .into_iter()
            .map(|(id, (parent, kind))| ((id, parent), (id, kind)))
            .unzip();
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
            kinds,
            filter,
            parents,
            children,
            labels,
            label_timestamps: Default::default(),
            show_label_timestamps: false,
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
    fn passes_filter(&self, id: u64) -> bool {
        let kind = self.kinds[&id];
        if kind == TreeEntryKind::Hidden {
            return false;
        }
        match self.filter {
            TreeFilter::Default => kind != TreeEntryKind::Bookkeeping,
            TreeFilter::NoTools => {
                !matches!(kind, TreeEntryKind::Tool | TreeEntryKind::Bookkeeping)
            }
            TreeFilter::UserOnly => kind == TreeEntryKind::User,
            TreeFilter::LabeledOnly => self.labels.contains_key(&id),
            TreeFilter::All => true,
        }
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

fn session_age(age: std::time::Duration) -> String {
    let minutes = age.as_secs() / 60;
    let hours = minutes / 60;
    let days = hours / 24;
    if minutes < 1 {
        "now".into()
    } else if minutes < 60 {
        format!("{minutes}m")
    } else if hours < 24 {
        format!("{hours}h")
    } else if days < 7 {
        format!("{days}d")
    } else if days < 30 {
        format!("{}w", days / 7)
    } else if days < 365 {
        format!("{}mo", days / 30)
    } else {
        format!("{}y", days / 365)
    }
}
fn short_path(path: &std::path::Path) -> String {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    if let Some(home) = home
        && let Ok(relative) = path.strip_prefix(&home)
    {
        return std::path::Path::new("~")
            .join(relative)
            .display()
            .to_string();
    }
    path.display().to_string()
}
fn truncate(text: &str, width: usize) -> String {
    if text.width() <= width {
        return text.into();
    }
    let mut result = String::new();
    let mut used = 0;
    for grapheme in text.graphemes(true) {
        if used + grapheme.width() > width.saturating_sub(1) {
            break;
        }
        used += grapheme.width();
        result.push_str(grapheme);
    }
    result.push('…');
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tree_folding_search_navigation_and_labels_preserve_record_identity() {
        let entries = [
            (1, None),
            (2, Some(1)),
            (3, Some(2)),
            (4, Some(1)),
            (5, Some(4)),
        ]
        .into_iter()
        .map(|(id, parent)| (id, (parent, TreeEntryKind::Message)))
        .collect();
        let items = (1..=5)
            .map(|id| Item {
                label: format!("node {id}"),
                selection: Selection::Checkout(id),
            })
            .collect();
        let mut picker = Picker::tree(
            items,
            entries,
            [(1, "root".into())].into_iter().collect(),
            5,
            TreeFilter::Default,
        );
        let keys = Keys::default();
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
    fn tree_filters_preserve_the_nearest_visible_ancestor() {
        use TreeEntryKind::{Bookkeeping, Hidden, Message, Tool, User};

        let entries = [Bookkeeping, User, Hidden, Tool, Message]
            .into_iter()
            .enumerate()
            .map(|(i, kind)| (i as u64 + 1, ((i > 0).then_some(i as u64), kind)))
            .collect();
        let items = (1..=5)
            .map(|id| Item {
                label: format!("node {id}"),
                selection: Selection::Checkout(id),
            })
            .collect();
        let mut picker = Picker::tree(
            items,
            entries,
            [(2, "labeled".into())].into_iter().collect(),
            5,
            TreeFilter::NoTools,
        );
        let keys = Keys::default();
        let rows = |picker: &Picker| {
            picker
                .matches()
                .iter()
                .filter_map(|item| match item.selection {
                    Selection::Checkout(id) => Some(id),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(rows(&picker), [2, 5]);
        assert_eq!(picker.selected, 1);
        let control = |character| KeyEvent::new(KeyCode::Char(character), KeyModifiers::CONTROL);
        picker.input(control('o'), &keys);
        assert_eq!(rows(&picker), [2]);
        assert_eq!(picker.selected, 0);
        picker.input(control('o'), &keys);
        assert_eq!(rows(&picker), [2]);
        picker.input(control('a'), &keys);
        assert_eq!(rows(&picker), [1, 2, 4, 5]);
        assert_eq!(picker.selected, 1);
        picker.input(control('d'), &keys);
        assert_eq!(rows(&picker), [2, 4, 5]);
    }

    #[test]
    fn scoped_models_reorder_and_filter_the_enabled_set() {
        let catalog = ["alpha", "beta", "gamma"].map(str::to_owned).to_vec();
        let mut picker = Picker::scoped_models("ollama", catalog.clone(), catalog);
        let keys = Keys::default();
        picker.input(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &keys);
        assert_eq!(picker.model_scope().unwrap().1, ["beta", "gamma"]);
        picker.query = "gam".into();
        picker.selected = 0;
        picker.input(KeyEvent::new(KeyCode::Up, KeyModifiers::ALT), &keys);
        assert_eq!(picker.model_scope().unwrap().1, ["gamma", "beta"]);
        assert!(matches!(
            picker.input(
                KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
                &keys
            ),
            Input::UpdateModelScope { save: true }
        ));
        picker.input(
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::CONTROL),
            &keys,
        );
        assert_eq!(picker.model_scope().unwrap().1, ["beta"]);
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
        let keys = Keys::default();
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
    fn session_rows_align_metadata_and_style_active_unicode_titles() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("active.jsonl");
        std::fs::write(&path, "").unwrap();
        let mut document = super::super::session_search::Document::new(
            "named".into(),
            "id".into(),
            String::new(),
            String::new(),
            true,
        );
        document.message_count = 12;
        document.activity =
            Some(std::time::SystemTime::now() - std::time::Duration::from_secs(7200));
        let mut picker = Picker::sessions(
            vec![Item {
                label: format!("  日本語\n{}", "長い会話".repeat(30)),
                selection: Selection::Resume(path.clone()),
            }],
            path.clone(),
            Default::default(),
        );
        picker.set_session_documents(std::collections::BTreeMap::from([(path, document)]));
        let theme = Theme::system();
        let row = picker.session_line(&picker.items[0], "   └─ ", true, &theme, 60);
        let text = row
            .spans
            .iter()
            .map(|span| span.text.as_str())
            .collect::<String>();
        assert_eq!(text.width(), 60);
        assert!(text.ends_with("12 2h"));
        assert!(text.contains('…'));
        assert!(!text.contains('\n'));
        assert_eq!(row.spans[2].color, theme.color("accent"));
        assert!(row.spans[2].bold);
    }

    #[test]
    fn session_age_uses_reference_units() {
        for (seconds, expected) in [
            (0, "now"),
            (59, "now"),
            (60, "1m"),
            (3600, "1h"),
            (86400, "1d"),
            (604800, "1w"),
            (2592000, "1mo"),
            (31536000, "1y"),
        ] {
            assert_eq!(
                session_age(std::time::Duration::from_secs(seconds)),
                expected
            );
        }
        assert_eq!(truncate("日本語", 4), "日…");
        assert_eq!(truncate("e\u{301}xyz", 3), "e\u{301}x…");
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
        let keys = Keys::default();
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
                &Keys::default()
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
