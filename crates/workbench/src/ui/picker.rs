//! Searchable keyboard selectors for session and model navigation.
use super::{
    editor::Editor,
    keys::Keys,
    render::{Line, Span},
    theme::Theme,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEventKind};
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
    Fork(u64),
    Model(String),
    Completion {
        range: std::ops::Range<usize>,
        text: String,
        description: Option<String>,
    },
    LoginProvider(super::super::Provider),
    VertexApiKey,
    VertexAdc {
        service_account: bool,
    },
    BedrockBearer,
    BedrockProfile,
    BedrockChain,
    LoginOpenRouter,
    AnthropicMethods,
    LoginAnthropic(crate::anthropic_auth::LoginMethod),
    LoginKimi,
    LoginMeta,
    LoginXai,
    LoginCodex,
    LoginCodexDevice,
    LoginCopilot,
    LoginChatGpt,
    LoginChatGptNew,
    LoginChatGptAccount(String),
    LogoutChatGpt,
    LogoutProvider(super::super::Provider),
    RadiusMethods,
    LoginRadius(crate::radius_auth::Method),
    LogoutRadius,
    Theme(String),
    ThemeSettings(ThemeSettingsAction),
    ThinkingModel(String),
    ThinkingOverride(Option<String>),
    Trust(bool),
    Thinking(String),
    Setting(String, serde_json::Value),
    SaveDefaultModel,
    EditSetting(String),
}
#[derive(Clone)]
pub enum ThemeSettingsAction {
    Automatic,
    Single,
    Light,
    Dark,
    Apply,
    Pick(String),
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

/// The heading and body carry independent theme roles in the session tree.
pub struct TreeDescription {
    pub heading: String,
    pub heading_role: &'static str,
    pub body: String,
    pub body_role: &'static str,
}

impl TreeDescription {
    fn spans(&self, theme: &Theme, selected: bool) -> [super::render::Span; 2] {
        let mut heading = super::render::Span::themed(&self.heading, theme, self.heading_role);
        let mut body = super::render::Span::themed(&self.body, theme, self.body_role);
        heading.bold = selected;
        body.bold = selected;
        [heading, body]
    }
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
    query: Editor,
    selected: usize,
    page_size: std::cell::Cell<usize>,
    mouse_layout: std::cell::Cell<(usize, usize, usize)>,
    mouse_pressed: Option<usize>,
    completion_limit: Option<usize>,
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
    Label(u64, String),
}
pub enum Input {
    Continue,
    Cancel,
    Select(Selection),
    Save(Selection),
    CopyTreeRecord(u64),
    SetTreeLabel(u64, String),
    UpdateModelScope { save: bool },
}
pub enum MouseInput {
    Ignored,
    Changed,
    Activate,
}
impl Picker {
    pub fn new(title: &str, items: Vec<Item>) -> Self {
        Self {
            title: title.into(),
            items,
            query: Editor::default(),
            selected: 0,
            page_size: std::cell::Cell::new(10),
            mouse_layout: std::cell::Cell::new((0, 0, 0)),
            mouse_pressed: None,
            completion_limit: None,
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
    pub fn completion(
        candidates: Vec<(String, Option<String>)>,
        range: std::ops::Range<usize>,
        limit: usize,
    ) -> Self {
        let items = candidates
            .into_iter()
            .map(|(text, description)| Item {
                label: text.clone(),
                selection: Selection::Completion {
                    range: range.clone(),
                    text,
                    description,
                },
            })
            .collect();
        Self {
            completion_limit: Some(limit),
            ..Self::new("Autocomplete", items)
        }
    }

    pub fn paste(&mut self, text: &str) {
        if self.title == "Warnings" {
            return;
        }
        let tree_focus = self.tree_filter_focus();
        if self.title == "Settings" {
            self.query.insert_settings_search(text);
        } else {
            self.query
                .text
                .extend(text.chars().filter(|character| !character.is_control()));
        }
        self.apply_search_focus(tree_focus);
    }

    fn tree_filter_focus(&self) -> Option<u64> {
        let tree = self.tree.as_ref()?;
        if self.session_action.is_some() {
            return None;
        }
        self.matches()
            .get(self.selected)
            .and_then(|item| match item.selection {
                Selection::Checkout(id) => Some(id),
                _ => None,
            })
            .or(Some(tree.last_filter_focus))
    }

    fn apply_search_focus(&mut self, focus: Option<u64>) {
        if let Some(id) = focus {
            let tree = self.tree.as_mut().expect("tree search");
            tree.collapsed.clear();
            tree.rebuild_visible();
            self.focus_record(id);
        } else {
            self.selected = 0;
        }
    }

    /// Handle pointer input in the rendered selector body, with row zero at its title.
    pub fn mouse(&mut self, kind: MouseEventKind, row: usize) -> MouseInput {
        let (start, visible, height) = self.mouse_layout.get();
        if self.session_action.is_some() || row >= height {
            self.mouse_pressed = None;
            return MouseInput::Ignored;
        }
        match kind {
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                self.mouse_pressed = None;
                let count = self.matches().len();
                if kind == MouseEventKind::ScrollUp {
                    self.selected = self.selected.saturating_sub(1);
                } else {
                    self.selected = (self.selected + 1).min(count.saturating_sub(1));
                }
                MouseInput::Changed
            }
            MouseEventKind::Down(MouseButton::Left)
                if row > 0 && row <= visible && start + row - 1 < self.matches().len() =>
            {
                self.selected = start + row - 1;
                self.mouse_pressed = Some(self.selected);
                MouseInput::Changed
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                self.mouse_pressed = None;
                MouseInput::Changed
            }
            MouseEventKind::Up(MouseButton::Left) => {
                if let Some(index) = self
                    .mouse_pressed
                    .take()
                    .filter(|index| *index < self.matches().len())
                {
                    self.selected = index;
                    MouseInput::Activate
                } else {
                    MouseInput::Ignored
                }
            }
            _ => MouseInput::Ignored,
        }
    }

    pub fn clear_mouse_press(&mut self) {
        self.mouse_pressed = None;
    }

    pub fn selected(&self) -> Option<Selection> {
        self.matches()
            .get(self.selected)
            .map(|item| item.selection.clone())
    }

    pub fn focus_theme(&mut self, name: &str) {
        if let Some(index) = self.matches().iter().position(|item| matches!(&item.selection, Selection::ThemeSettings(ThemeSettingsAction::Pick(value)) if value == name)) {
            self.selected = index;
        }
    }

    pub fn refresh_thinking_models(&mut self, items: Vec<Item>) {
        let selected = match self.selected() {
            Some(Selection::ThinkingModel(name)) => Some(name),
            _ => None,
        };
        self.items = items;
        self.selected = selected.and_then(|name| self.matches().iter().position(|item| matches!(&item.selection, Selection::ThinkingModel(model) if model == &name))).unwrap_or(0);
    }

    pub fn focus_thinking_model(&mut self, model: &str) {
        if let Some(index) = self.matches().iter().position(
            |item| matches!(&item.selection, Selection::ThinkingModel(name) if name == model),
        ) {
            self.selected = index;
        }
    }

    pub fn focus_thinking_level(&mut self, level: Option<&str>) {
        if let Some(index) = self.matches().iter().position(|item| matches!(&item.selection, Selection::ThinkingOverride(value) if value.as_deref() == level)) { self.selected = index; }
    }

    pub fn refresh_settings(&mut self, items: Vec<Item>) {
        let selected = self
            .matches()
            .get(self.selected)
            .and_then(|item| match &item.selection {
                Selection::Setting(name, _) => Some((false, name.clone())),
                Selection::EditSetting(name) => Some((true, name.clone())),
                Selection::SaveDefaultModel => Some((false, "__save_default_model".into())),
                _ => None,
            });
        self.items = items;
        self.selected = selected
            .and_then(|(edit, name)| {
                self.matches()
                    .iter()
                    .position(|item| match &item.selection {
                        Selection::Setting(item_name, _) => !edit && *item_name == name,
                        Selection::EditSetting(item_name) => edit && *item_name == name,
                        Selection::SaveDefaultModel => !edit && name == "__save_default_model",
                        _ => false,
                    })
            })
            .unwrap_or(0);
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

    fn model_scope_input(&mut self, key: KeyEvent, keys: &Keys, activate: bool) -> Option<Input> {
        if keys.matches("tui.select.cancel", key)
            && key.code == KeyCode::Char('c')
            && key.modifiers.contains(KeyModifiers::CONTROL)
            && !self.query.text.is_empty()
        {
            self.query.text.clear();
            self.selected = 0;
            return Some(Input::Continue);
        }
        if keys.matches("app.models.save", key) {
            return Some(Input::UpdateModelScope { save: true });
        }
        let toggle = activate || keys.matches("tui.select.confirm", key);
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
    pub fn set_tree_descriptions(
        &mut self,
        descriptions: std::collections::BTreeMap<u64, TreeDescription>,
    ) {
        if let Some(tree) = &mut self.tree {
            tree.descriptions = descriptions;
        }
    }

    pub fn set_label_timestamps(&mut self, timestamps: std::collections::BTreeMap<u64, String>) {
        if let Some(tree) = &mut self.tree {
            tree.label_timestamps = timestamps;
        }
    }

    pub fn track_tree_label(&mut self, request: uuid::Uuid, record: u64) {
        if let Some(tree) = &mut self.tree {
            tree.pending_label = Some((request, record));
        }
    }

    pub fn take_tree_label(&mut self, request: uuid::Uuid) -> Option<u64> {
        self.tree
            .as_mut()?
            .pending_label
            .take_if(|(id, _)| *id == request)
            .map(|(_, record)| record)
    }

    pub fn cancel_pending_tree_label(&mut self) {
        if let Some(tree) = &mut self.tree
            && tree.pending_label.take().is_some()
        {
            self.show_feedback(Err(
                "The active worker stopped before confirming the label".into()
            ));
        }
    }

    pub fn refresh_tree(&mut self, mut replacement: Self, record: u64) {
        let (Some(previous), Some(next)) = (&mut self.tree, &mut replacement.tree) else {
            return;
        };
        next.filter = previous.filter;
        next.collapsed = std::mem::take(&mut previous.collapsed);
        next.show_label_timestamps = previous.show_label_timestamps;
        next.rebuild_visible();
        replacement.query = std::mem::take(&mut self.query);
        replacement.session_action = self.session_action.take();
        replacement.focus_record(record);
        *self = replacement;
    }
    pub fn focus_thinking(&mut self, current: &str) {
        if let Some(index) = self.items.iter().position(
            |item| matches!(&item.selection, Selection::Thinking(level) if level == current),
        ) {
            self.selected = index;
        }
    }
    pub fn focus_record(&mut self, id: u64) {
        if let Some(tree) = &mut self.tree {
            tree.last_filter_focus = id;
        }
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
        let query = self.query.text.to_lowercase();
        let tree_tokens: Vec<_> = if self.tree.is_some() {
            query.split_whitespace().collect()
        } else {
            Vec::new()
        };
        let session_query = super::session_search::Query::new(&self.query.text);
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
                if let (Some(tree), Selection::Checkout(id)) = (&self.tree, &item.selection) {
                    if !tree.passes_filter(*id) || !tree.visible.contains(id) {
                        return false;
                    }
                    if tree_tokens.is_empty() {
                        return true;
                    }
                    let text = item.label.to_lowercase();
                    let label = tree.labels.get(id).map(|label| label.to_lowercase());
                    return tree_tokens.iter().all(|token| {
                        text.contains(token)
                            || label.as_ref().is_some_and(|label| label.contains(token))
                    });
                }
                if let Selection::Resume(path) = &item.selection
                    && let Some(document) = self.session_documents.get(path)
                {
                    return session_query.score(document).is_some();
                }
                let label = match &item.selection {
                    Selection::Completion { text, .. } => text.to_lowercase(),
                    _ => item.label.to_lowercase(),
                };
                if self.title == "Settings" {
                    settings_match_score(&query, settings_label_parts(&label).0).is_some()
                } else if self.active_session.is_some()
                    || self.model_scope.is_some()
                    || self.title == "Thinking: select model"
                {
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
                let label = match &item.selection {
                    Selection::Completion { text, .. } => text.to_lowercase(),
                    _ => item.label.to_lowercase(),
                };
                if self.title == "Settings" {
                    let name = label.split(" · ").next().unwrap_or(&label);
                    return (settings_match_score(&query, name).unwrap_or(i64::MAX), 0);
                }
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
        self.input_inner(key, keys, false)
    }

    pub fn activate_mouse_selection(&mut self, keys: &Keys) -> Input {
        self.input_inner(
            KeyEvent::new(KeyCode::F(255), KeyModifiers::NONE),
            keys,
            true,
        )
    }

    fn input_inner(&mut self, key: KeyEvent, keys: &Keys, activate: bool) -> Input {
        self.feedback = None;
        if self.model_scope.is_some()
            && let Some(input) = self.model_scope_input(key, keys, activate)
        {
            return input;
        }
        let cancel = keys.matches("tui.select.cancel", key);
        let confirm = activate
            || keys.matches("tui.select.confirm", key)
            || (matches!(self.title.as_str(), "Settings" | "Warnings")
                && self.query.text.is_empty()
                && key.code == KeyCode::Char(' ')
                && key.modifiers == KeyModifiers::NONE)
            || (self.completion_limit.is_some() && keys.matches("tui.input.tab", key));
        if let Some(action) = self.session_action.clone() {
            if cancel {
                self.session_action = None;
                match action {
                    SessionAction::Rename(_) => self.query.text.clear(),
                    SessionAction::Label(id, search) => {
                        self.query.text = search;
                        self.focus_record(id);
                    }
                    SessionAction::Delete(_) => {}
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
                    return Input::Select(Selection::RenameSession(path, self.query.text.clone()));
                }
                SessionAction::Label(id, search) if confirm => {
                    let label = std::mem::replace(&mut self.query.text, search);
                    self.session_action = None;
                    self.focus_record(id);
                    return Input::SetTreeLabel(id, label);
                }
                SessionAction::Rename(_) | SessionAction::Label(..) => {}
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
                    self.query.text.clear();
                    return Input::Continue;
                }
                if keys.matches("app.session.delete", key)
                    || (self.query.text.is_empty()
                        && keys.matches("app.session.deleteNoninvasive", key))
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
            let visible: Vec<u64> = self
                .matches()
                .iter()
                .filter_map(|item| match item.selection {
                    Selection::Checkout(id) => Some(id),
                    _ => None,
                })
                .collect();
            let selected = visible.get(self.selected).copied();
            let tree = self.tree.as_mut().expect("tree picker");
            if keys.matches("app.tree.toggleLabelTimestamp", key) {
                tree.show_label_timestamps = !tree.show_label_timestamps;
                return Input::Continue;
            }
            if let Some(filter) = tree.filter.filter_for_key(key, keys) {
                tree.filter = filter;
                tree.collapsed.clear();
                tree.rebuild_visible();
                let focus = selected.unwrap_or(tree.last_filter_focus);
                self.focus_record(focus);
                return Input::Continue;
            }
            if let Some(id) = selected {
                if keys.matches("app.message.copy", key) {
                    return Input::CopyTreeRecord(id);
                }
                if keys.matches("app.tree.editLabel", key) {
                    let label = tree.labels.get(&id).cloned().unwrap_or_default();
                    let search = std::mem::replace(&mut self.query.text, label);
                    self.session_action = Some(SessionAction::Label(id, search));
                    return Input::Continue;
                }
                if keys.matches("app.tree.foldOrUp", key) {
                    let tree = self.tree.as_mut().expect("tree picker");
                    let (parents, children) = tree.navigation_graph(&visible);
                    let parent = parents.get(&id).copied().flatten();
                    let foldable = children.get(&id).is_some_and(|list| !list.is_empty())
                        && parent.is_none_or(|parent| {
                            children.get(&parent).is_some_and(|list| list.len() > 1)
                        });
                    let target = if foldable && tree.collapsed.insert(id) {
                        tree.rebuild_visible();
                        id
                    } else {
                        let mut current = id;
                        while let Some(parent) = parents.get(&current).copied().flatten() {
                            if children.get(&parent).is_some_and(|list| list.len() > 1)
                                && current != id
                            {
                                break;
                            }
                            current = parent;
                        }
                        current
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
                        let (_, children) = tree.navigation_graph(&visible);
                        let mut current = id;
                        while let Some(list) =
                            children.get(&current).filter(|list| !list.is_empty())
                        {
                            current = list[0];
                            if list.len() > 1 {
                                break;
                            }
                        }
                        current
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
            if !self.query.text.is_empty()
                && let Some(focus) = self.tree_filter_focus()
            {
                self.query.text.clear();
                self.apply_search_focus(Some(focus));
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
        if self.title == "Settings"
            && let Some(action) = keys.editor_action(key)
        {
            let previous = self.query.text.clone();
            if self.query.settings_search_action(action) {
                if self.query.text != previous {
                    self.selected = 0;
                }
                return Input::Continue;
            }
        }
        if keys.matches("tui.editor.deleteCharBackward", key) {
            if let Some((index, _)) = self.query.text.grapheme_indices(true).next_back() {
                let focus = self.tree_filter_focus();
                self.query.text.truncate(index);
                self.apply_search_focus(focus);
            } else if self.tree.is_none() {
                self.selected = 0;
            }
            return Input::Continue;
        }
        if keys.matches("tui.editor.deleteWordBackward", key) {
            let focus = self.tree_filter_focus();
            self.query
                .text
                .truncate(previous_word_start(&self.query.text));
            self.apply_search_focus(focus);
            return Input::Continue;
        }
        match key.code {
            KeyCode::Home if self.title != "Settings" => self.selected = 0,
            KeyCode::End if self.title != "Settings" => self.selected = count.saturating_sub(1),
            KeyCode::Char(character)
                if self.title != "Warnings"
                    && !key.modifiers.intersects(
                        KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                    ) =>
            {
                let focus = self.tree_filter_focus();
                if self.title == "Settings" {
                    self.query.type_settings_search(character);
                } else {
                    self.query.text.push(character);
                }
                self.apply_search_focus(focus);
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
        let mut title_span = super::render::Span::themed(title.clone(), theme, color);
        title_span.bold = selected;
        let spacing = width
            .saturating_sub(2 + prefix.width() + title.width() + right.width())
            .max(1);
        Line {
            spans: vec![
                super::render::Span::themed(if selected { "› " } else { "  " }, theme, "accent"),
                super::render::Span::themed(prefix, theme, "dim"),
                title_span,
                super::render::Span::themed(
                    format!("{}{right}", " ".repeat(spacing)),
                    theme,
                    "dim",
                ),
            ],
            background: selected.then(|| theme.color("selectedBg")),
            image: None,
        }
    }

    fn settings_heading(&self, prefix: &str, theme: &Theme, width: usize) -> Line {
        let query = super::render::clean(&self.query.text);
        let available = width.saturating_sub(prefix.width());
        if available == 0 {
            return Line::themed(prefix, theme, "accent");
        }
        let before_cursor = &self.query.text[..self.query.cursor.min(self.query.text.len())];
        let cursor = super::render::clean(before_cursor).width();
        let scroll_width = available.saturating_sub(usize::from(cursor == query.width()));
        let first_column = cursor
            .saturating_sub(scroll_width / 2)
            .min(query.width().saturating_sub(scroll_width));
        let mut start_column = 0;
        let mut first_byte = 0;
        for grapheme in query.graphemes(true) {
            if start_column >= first_column {
                break;
            }
            start_column += grapheme.width();
            first_byte += grapheme.len();
        }
        let visible = clip_completion(&query[first_byte..], available);
        let mut line = Line::themed(&format!("{prefix}{visible}"), theme, "accent");
        line.mark_cursor(prefix.width() + cursor.saturating_sub(start_column));
        line
    }

    pub fn render(&self, theme: &Theme, width: usize, height: usize, keys: &Keys) -> Vec<Line> {
        self.render_in_terminal(theme, width, height, height, keys)
    }

    pub fn render_in_terminal(
        &self,
        theme: &Theme,
        width: usize,
        height: usize,
        terminal_height: usize,
        keys: &Keys,
    ) -> Vec<Line> {
        let hint = |action| keys.hint(action);
        if let Some(action) = &self.session_action {
            let confirm = hint("tui.select.confirm");
            let cancel = hint("tui.select.cancel");
            let (title, hint) = match action {
                SessionAction::Rename(_) => (
                    format!("Rename session · {}", self.query.text),
                    format!("Type the new name · {confirm} to save · {cancel} to cancel"),
                ),
                SessionAction::Label(id, _) => (
                    format!("Label record {id} · {}", self.query.text),
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
                Line::themed(&title, theme, "accent"),
                Line::themed(&hint, theme, "dim"),
            ];
        }
        let filter = self
            .tree
            .as_ref()
            .map_or_else(String::new, |tree| format!(" · {}", tree.filter.name()));
        let settings = matches!(self.title.as_str(), "Settings" | "Warnings");
        let prefix = format!("{}{filter} · ", self.title);
        let mut lines = vec![match self.title.as_str() {
            "Settings" => self.settings_heading(&prefix, theme, width),
            "Warnings" => Line::themed(&self.title, theme, "accent"),
            _ => Line::themed(&format!("{prefix}{}", self.query.text), theme, "accent"),
        }];
        let items = self.matches();
        let description = if settings {
            items
                .get(self.selected)
                .and_then(|item| setting_description(&item.selection))
        } else {
            None
        };
        let description_lines = description.map_or_else(Vec::new, |description| {
            if width < 4 {
                return Vec::new();
            }
            let content_width = width.saturating_sub(4).max(1);
            let layout = super::editor_layout::Layout::new(description, content_width);
            let visible = layout.len().min(height.saturating_sub(4));
            let truncated = visible < layout.len();
            layout
                .visible(description, 0, visible)
                .enumerate()
                .map(|(index, line)| {
                    let line = line.trim_end();
                    let text = if truncated && index + 1 == visible {
                        format!(
                            "{}…",
                            clip_completion(line, content_width.saturating_sub(1))
                        )
                    } else {
                        line.to_owned()
                    };
                    Line::themed(&format!("  {text}"), theme, "dim")
                })
                .collect::<Vec<_>>()
        });
        let tree_layout = self.tree.as_ref().map(|tree| {
            let visible: Vec<_> = items
                .iter()
                .filter_map(|item| match item.selection {
                    Selection::Checkout(id) => Some(id),
                    _ => None,
                })
                .collect();
            tree.layout(&visible)
        });
        let mut prefixes = vec![String::new(); items.len()];
        if self.active_session.is_some()
            && self.session_sort == SessionSort::Threaded
            && self.query.text.trim().is_empty()
            && let Some(rows) = self.session_layout(&items)
        {
            for row in rows {
                prefixes[row.index] = row.prefix;
            }
        }
        let mut available = height
            .saturating_sub(
                3 + usize::from(self.feedback.is_some())
                    + usize::from(self.active_session.is_some())
                    + description_lines.len(),
            )
            .max(1);
        if let Some(limit) = self.completion_limit {
            available = available.min(limit);
        }
        if self.tree.is_some() {
            available = available.min((terminal_height / 2).max(5));
        }
        const SETTINGS_MAX_ROWS: usize = 10;
        let scrolling = (settings && items.len() > available.min(SETTINGS_MAX_ROWS))
            || (self.active_session.is_some() && items.len() > available);
        if scrolling {
            available = available.saturating_sub(1).max(1);
        }
        if settings {
            available = available.min(SETTINGS_MAX_ROWS);
        }
        self.page_size.set(available);
        let start = if settings || self.tree.is_some() || self.active_session.is_some() {
            self.selected
                .saturating_sub(available / 2)
                .min(items.len().saturating_sub(available))
        } else {
            self.selected.saturating_sub(available - 1)
        };
        self.mouse_layout.set((
            start,
            items.len().saturating_sub(start).min(available),
            height,
        ));
        let tree_scroll = if let (Some(tree), Some(layout)) = (&self.tree, &tree_layout) {
            let viewport = width.saturating_sub(2);
            let mut max_body = 0usize;
            let mut selected_anchor = None;
            for (index, item) in items.iter().enumerate().skip(start).take(available) {
                let Selection::Checkout(id) = item.selection else {
                    continue;
                };
                let Some(prefix) = layout.rows.get(&id) else {
                    continue;
                };
                let marker = if tree.active_path.contains(&id) { 2 } else { 0 };
                let anchor = prefix.width().saturating_add(marker);
                let text_width = tree
                    .description_spans(id, item, theme, index == self.selected)
                    .iter()
                    .map(|span| span.text.width())
                    .sum::<usize>();
                max_body = max_body.max(anchor.saturating_add(text_width));
                if index == self.selected {
                    selected_anchor = Some(anchor);
                }
            }
            let max_scroll = max_body.saturating_sub(viewport);
            let min_content = (viewport / 3).clamp(4, 20);
            selected_anchor
                .filter(|&anchor| anchor > viewport.saturating_sub(min_content))
                .map_or(0, |anchor| {
                    anchor
                        .saturating_sub((viewport / 4).clamp(2, 12))
                        .min(max_scroll)
                })
        } else {
            0
        };
        let completion_column = items
            .iter()
            .map(|item| item.label.width() + 2)
            .max()
            .unwrap_or(12)
            .clamp(12, 32);
        let settings_column = matches!(self.title.as_str(), "Settings" | "Warnings").then(|| {
            self.items
                .iter()
                .map(|item| settings_label_parts(&item.label).0.width())
                .max()
                .unwrap_or(0)
                .min(36)
        });
        for (index, item) in items.iter().enumerate().skip(start).take(available) {
            if let Some(column) = settings_column {
                lines.push(render_settings_row(
                    &item.label,
                    index == self.selected,
                    theme,
                    width,
                    column,
                ));
                continue;
            }
            if let (Some(tree), Some(layout), Selection::Checkout(id)) =
                (&self.tree, &tree_layout, &item.selection)
            {
                let selected = index == self.selected;
                let prefix = &layout.rows[id];
                let viewport = width.saturating_sub(2);
                let prefix_text = prefix.slice(&layout.gutters, tree_scroll, viewport);
                let mut suffix = vec![super::render::Span::themed(
                    if tree.active_path.contains(id) {
                        "• "
                    } else {
                        ""
                    },
                    theme,
                    "accent",
                )];
                suffix.extend(tree.description_spans(*id, item, theme, selected));
                let mut spans = vec![super::render::Span::themed(
                    if selected { "› " } else { "  " },
                    theme,
                    "accent",
                )];
                let prefix_columns = prefix_text.width();
                spans.push(super::render::Span::themed(prefix_text, theme, "dim"));
                spans.extend(slice_tree_spans(
                    &suffix,
                    tree_scroll.saturating_sub(prefix.width()),
                    viewport.saturating_sub(prefix_columns),
                ));
                lines.push(Line {
                    spans,
                    background: selected.then(|| theme.color("selectedBg")),
                    image: None,
                });
                continue;
            }
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
            if let Selection::Completion {
                text, description, ..
            } = &item.selection
            {
                let selected = index == self.selected;
                let role = if selected { "accent" } else { "text" };
                let prefix = if selected { "› " } else { "  " };
                let mut spans = vec![super::render::Span::themed(prefix, theme, role)];
                let normalized = description.as_deref().map(normalize_completion_description);
                let column = completion_column.min(width.saturating_sub(6)).max(1);
                let primary = clip_completion(text, column.saturating_sub(2).max(1));
                let spacing = column.saturating_sub(primary.width()).max(1);
                let remaining = width.saturating_sub(2 + primary.width() + spacing + 2);
                if width > 40
                    && remaining > 10
                    && normalized.as_ref().is_some_and(|s| !s.is_empty())
                {
                    spans.push(super::render::Span::themed(primary, theme, role));
                    let desc = format!(
                        "{}{}",
                        " ".repeat(spacing),
                        clip_completion(normalized.as_deref().unwrap_or_default(), remaining)
                    );
                    spans.push(super::render::Span::themed(
                        desc,
                        theme,
                        if selected { "accent" } else { "muted" },
                    ));
                } else {
                    spans.push(super::render::Span::themed(
                        clip_completion(text, width.saturating_sub(4)),
                        theme,
                        role,
                    ));
                }
                lines.push(Line {
                    spans,
                    background: selected.then(|| theme.color("selectedBg")),
                    image: None,
                });
                continue;
            }
            let mut line = Line::themed(
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
                    } else {
                        item.label.clone()
                    }
                ),
                theme,
                "text",
            );
            if index == self.selected {
                line.background = Some(theme.color("selectedBg"));
            }
            lines.push(line);
        }
        if items.is_empty() {
            let message = if self.active_session.is_some()
                && self.query.text.is_empty()
                && !self.all_workspaces
            {
                "No sessions in current folder. Press Tab to view all."
            } else if settings {
                "No matching settings"
            } else if self.tree.is_some() {
                "  No entries found"
            } else {
                "No matches"
            };
            lines.push(Line::themed(message, theme, "dim"));
        }
        if scrolling || self.tree.is_some() {
            let position = (self.selected + 1).min(items.len());
            lines.push(Line::themed(
                &format!("  ({position}/{})", items.len()),
                theme,
                "dim",
            ));
        }
        lines.extend(description_lines);
        if let Some(feedback) = &self.feedback {
            let (message, role) = match feedback {
                Ok(message) => (message, "dim"),
                Err(message) => (message, "error"),
            };
            lines.push(Line::themed(message, theme, role));
        }
        if self.active_session.is_some() {
            lines.push(Line::themed(
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
                theme,
                "dim",
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
        } else if self.completion_limit.is_some() {
            format!(
                "{}/{} to select · {}/{} to insert · {} to cancel",
                hint("tui.select.up"),
                hint("tui.select.down"),
                hint("tui.input.tab"),
                hint("tui.select.confirm"),
                hint("tui.select.cancel"),
            )
        } else if self.title == "Warnings" {
            format!(
                "{}/Space to change · {} to go back",
                hint("tui.select.confirm"),
                hint("tui.select.cancel"),
            )
        } else if self.title == "Settings" {
            format!(
                "Type to search · {}/{} to select · {}/Space to change · {} to close",
                hint("tui.select.up"),
                hint("tui.select.down"),
                hint("tui.select.confirm"),
                hint("tui.select.cancel"),
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
        lines.push(Line::themed(&hint, theme, "dim"));
        lines
    }
}

fn settings_label_parts(label: &str) -> (&str, &str) {
    label.split_once(" · ").unwrap_or((label, ""))
}

fn render_settings_row(
    label: &str,
    selected: bool,
    theme: &Theme,
    width: usize,
    column: usize,
) -> Line {
    let (label, value) = settings_label_parts(label);
    let prefix = clip_completion(if selected { "→ " } else { "  " }, width);
    let available = width.saturating_sub(prefix.width());
    let label_width = label.width();
    let aligned_padding = column.saturating_sub(label_width) + 2;
    let padding = if label_width + aligned_padding + value.width() <= available {
        aligned_padding
    } else {
        2
    };
    let label = clip_completion(&format!("{label}{}", " ".repeat(padding)), available);
    let value_width = available.saturating_sub(label.width());
    Line {
        spans: vec![
            Span::themed(prefix, theme, "accent"),
            Span::themed(label, theme, if selected { "accent" } else { "text" }),
            Span::themed(
                clip_completion(value, value_width),
                theme,
                if selected { "accent" } else { "muted" },
            ),
        ],
        ..Line::default()
    }
}

// Pi's settings list explains the selected option below the list. Keep
// descriptions keyed by the action so searching and refreshing do not detach
// them from the setting they describe.
fn setting_description(selection: &Selection) -> Option<&'static str> {
    if let Selection::EditSetting(key) = selection
        && !matches!(key.as_str(), "theme" | "modelThinkingLevels" | "warnings")
    {
        return Some("Edit the saved setting as JSON; changes apply on reload or startup.");
    }
    let key = match selection {
        Selection::Setting(key, _) | Selection::EditSetting(key) => key.as_str(),
        Selection::SaveDefaultModel => {
            return Some("Save the current provider and model as the default for new sessions.");
        }
        _ => return None,
    };
    Some(match key {
        "httpIdleTimeoutMs" => {
            "Maximum idle gap while waiting for HTTP headers or body chunks. Disable it for models that pause longer."
        }
        "steeringMode" => {
            "Choose whether steering messages are delivered one at a time or all at once while the agent is running."
        }
        "followUpMode" => {
            "Choose whether queued follow-up messages are delivered one at a time or all at once after the agent stops."
        }
        "theme" => "Choose a color theme for the interface.",
        "modelThinkingLevels" => "Override the default thinking level for specific models.",
        "warnings" => "Enable or disable individual warnings.",
        "warnings.anthropicExtraUsage" => {
            "Warn when Anthropic subscription auth may use paid extra usage."
        }
        "expandTools" => "Expand or collapse tool output in the transcript.",
        "hideThinkingBlock" => "Hide thinking blocks in assistant responses.",
        "showCacheMissNotices" => {
            "Show transcript notices for cache costs and provider recovery diagnostics."
        }
        "cacheWarming" => {
            "Keep prompt caches warm while streaming, also while idle, or turn cache warming off."
        }
        "defaultThinkingLevel" => {
            "Save the current thinking level as the default for new sessions."
        }
        "autoCompactionEnabled" => "Automatically compact context when it gets too large.",
        "autoRetryEnabled" => "Automatically retry supported transient errors.",
        "images.autoResize" => "Resize large images before sending them to a model.",
        "images.blockImages" => "Prevent images from being sent to model providers.",
        "terminal.showImages" => "Render supported images inline in the terminal.",
        "terminal.imageWidthCells" => "Set the preferred width of inline images in terminal cells.",
        "tuiMode" => "Choose fullscreen or regular mode; regular mode uses terminal scrollback.",
        "quietStartup" => "Control startup output; header keeps only the startup header.",
        "sessionShareDestination" => "Choose where to upload sessions shared with /share.",
        "defaultProjectTrust" => "Choose the fallback when no saved project trust decision exists.",
        "fullscreenWheelScrollLines" => {
            "Set lines per mouse-wheel event in fullscreen mode; auto adapts to fast scrolling where needed."
        }
        "autocompleteMaxVisible" => "Set the maximum number of visible autocomplete items.",
        "enableSkillCommands" => "Register skills as /skill:name commands.",
        "terminal.showTerminalProgress" => "Show progress indicators in the terminal tab bar.",
        "fullscreenExitOutput" => {
            "Print the transcript or a resume hint when exiting fullscreen mode."
        }
        "fullscreenScrollbar" => "Choose when the fullscreen scrollbar is visible.",
        "fullscreenCopyOnSelect" => "Automatically copy selected text in fullscreen mode.",
        "editorPaddingX" => "Set horizontal padding for the input editor.",
        "markdown.mermaid" => "Render Mermaid code blocks as Unicode diagrams.",
        "terminal.clearOnShrink" => {
            "Clear empty terminal rows when content shrinks; this can cause flicker."
        }
        "showHardwareCursor" => "Use the terminal cursor instead of the drawn cursor.",
        "outputPad" => "Set horizontal padding for messages, tool output, and command output.",
        "doubleEscapeAction" => {
            "Choose what happens when Escape is pressed twice with an empty editor."
        }
        "treeFilterMode" => "Choose the default filter when opening /tree.",
        _ => return None,
    })
}

fn previous_word_start(text: &str) -> usize {
    let segmenter = icu_segmenter::WordSegmenter::new_dictionary(Default::default());
    let mut segments = Vec::new();
    let mut start = 0;
    for (end, word_type) in segmenter.segment_str(text).iter_with_word_type() {
        if end > start {
            // ICU4X can classify Hebrew word segments as None even when
            // Intl.Segmenter reports them as word-like.
            let word_like =
                word_type.is_word_like() || text[start..end].chars().any(char::is_alphanumeric);
            segments.push((start, end, word_like));
        }
        start = end;
    }
    while segments
        .last()
        .is_some_and(|&(start, end, _)| text[start..end].chars().all(char::is_whitespace))
    {
        segments.pop();
    }
    let Some(&(start, end, word_like)) = segments.last() else {
        return 0;
    };
    if word_like {
        text[start..end]
            .char_indices()
            .rev()
            .find(|(_, c)| "(){}[]<>.,;:'\"!?+-=*/\\|&%^$#@~`".contains(*c))
            .map_or(start, |(i, c)| {
                let after = start + i + c.len_utf8();
                // A word-like segment can end in punctuation (e.g. Hebrew
                // apostrophe). Deleting from its end would leave it stuck.
                if after == end { start + i } else { after }
            })
    } else {
        segments
            .iter()
            .rev()
            .take_while(|&&(start, end, word_like)| {
                !word_like && !text[start..end].chars().all(char::is_whitespace)
            })
            .last()
            .map_or(start, |&(start, _, _)| start)
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
    descriptions: std::collections::BTreeMap<u64, TreeDescription>,
    label_timestamps: std::collections::BTreeMap<u64, String>,
    show_label_timestamps: bool,
    pending_label: Option<(uuid::Uuid, u64)>,
    last_filter_focus: u64,
    roots: Vec<u64>,
    order: Vec<u64>,
    active_path: std::collections::BTreeSet<u64>,
    collapsed: std::collections::BTreeSet<u64>,
    visible: std::collections::BTreeSet<u64>,
}

struct TreeGutter {
    level: usize,
    show: bool,
    parent: Option<usize>,
}

struct TreePrefix {
    indent: usize,
    connector: Option<(bool, char)>,
    folded_root: bool,
    gutters: Option<usize>,
}
struct TreeLayout {
    rows: std::collections::BTreeMap<u64, TreePrefix>,
    gutters: Vec<TreeGutter>,
}
impl TreePrefix {
    fn width(&self) -> usize {
        self.indent
            .saturating_mul(3)
            .saturating_add(2 * usize::from(self.folded_root))
    }
    fn slice(&self, gutters: &[TreeGutter], start: usize, width: usize) -> String {
        let end = start.saturating_add(width).min(self.width());
        if start >= end {
            return String::new();
        }
        let first = start / 3;
        let last = end.min(self.indent * 3).div_ceil(3);
        let mut visible = std::collections::BTreeMap::new();
        let mut gutter = self.gutters;
        while let Some(index) = gutter {
            let item = &gutters[index];
            if item.level >= first && item.level < last {
                visible.entry(item.level).or_insert(item.show);
            }
            gutter = item.parent;
        }
        let mut text = String::new();
        for position in start..end {
            if position >= self.indent * 3 {
                text.push(if position == self.indent * 3 {
                    '⊞'
                } else {
                    ' '
                });
                continue;
            }
            let level = position / 3;
            let offset = position % 3;
            let ch = if let Some(show) = visible.get(&level) {
                if offset == 0 && *show { '│' } else { ' ' }
            } else if level + 1 == self.indent {
                match (self.connector, offset) {
                    (Some((last, _)), 0) => {
                        if last {
                            '└'
                        } else {
                            '├'
                        }
                    }
                    (Some((_, marker)), 1) => marker,
                    _ => ' ',
                }
            } else {
                ' '
            };
            text.push(ch);
        }
        text
    }
}

// Clip styled body spans by terminal columns without splitting graphemes.
fn slice_tree_spans(
    spans: &[super::render::Span],
    start: usize,
    width: usize,
) -> Vec<super::render::Span> {
    if width == 0 {
        return Vec::new();
    }
    let end = start.saturating_add(width);
    let mut column = 0usize;
    let mut result = Vec::new();
    for span in spans {
        let mut piece = span.clone();
        piece.text.clear();
        for grapheme in span.text.graphemes(true) {
            let next = column.saturating_add(grapheme.width());
            if column >= start && column < end && next <= end {
                piece.text.push_str(grapheme);
            }
            column = next;
            if column >= end {
                break;
            }
        }
        if !piece.text.is_empty() {
            result.push(piece);
        }
        if column >= end {
            break;
        }
    }
    result
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
        let mut active_path = std::collections::BTreeSet::new();
        let mut current = Some(head);
        while let Some(id) = current {
            if !active_path.insert(id) {
                break;
            }
            current = parents.get(&id).copied().flatten();
        }
        roots.sort_by_key(|id| !active_path.contains(id));
        for siblings in children.values_mut() {
            siblings.sort_by_key(|id| !active_path.contains(id));
        }
        let mut tree = Self {
            kinds,
            filter,
            parents,
            children,
            labels,
            descriptions: Default::default(),
            label_timestamps: Default::default(),
            show_label_timestamps: false,
            pending_label: None,
            last_filter_focus: head,
            roots,
            order: vec![],
            active_path,
            collapsed: Default::default(),
            visible: Default::default(),
        };
        let mut pending: Vec<_> = tree.roots.iter().rev().copied().collect();
        while let Some(id) = pending.pop() {
            tree.order.push(id);
            if let Some(children) = tree.children.get(&id) {
                pending.extend(children.iter().rev().copied());
            }
        }
        tree.rebuild_visible();
        tree
    }
    fn navigation_graph(
        &self,
        visible: &[u64],
    ) -> (
        std::collections::BTreeMap<u64, Option<u64>>,
        std::collections::BTreeMap<u64, Vec<u64>>,
    ) {
        let ids: std::collections::BTreeSet<_> = visible.iter().copied().collect();
        let mut parents = std::collections::BTreeMap::new();
        let mut children = std::collections::BTreeMap::<u64, Vec<u64>>::new();
        for &id in visible {
            let mut parent = self.parents.get(&id).copied().flatten();
            while let Some(candidate) = parent {
                if ids.contains(&candidate) {
                    break;
                }
                parent = self.parents.get(&candidate).copied().flatten();
            }
            parents.insert(id, parent);
            if let Some(parent) = parent {
                children.entry(parent).or_default().push(id);
            }
        }
        (parents, children)
    }

    // Filtering can hide branch points. Rebuild the layout from visible
    // ancestors, as the navigation graph already does for branch jumps.
    fn layout(&self, visible: &[u64]) -> TreeLayout {
        let (parents, children) = self.navigation_graph(visible);
        let roots: Vec<_> = visible
            .iter()
            .copied()
            .filter(|id| parents[id].is_none())
            .collect();
        let multiple_roots = roots.len() > 1;
        // id, indent, follows a branch, has connector, last sibling, gutters, virtual root
        let mut pending: Vec<_> = roots
            .iter()
            .enumerate()
            .rev()
            .map(|(index, &id)| {
                (
                    id,
                    usize::from(multiple_roots),
                    multiple_roots,
                    multiple_roots,
                    index + 1 == roots.len(),
                    None::<usize>,
                    multiple_roots,
                )
            })
            .collect();
        let mut result = std::collections::BTreeMap::new();
        let mut gutter_arena = Vec::new();
        while let Some((id, indent, branched, connector, last, gutters, virtual_root)) =
            pending.pop()
        {
            let display_indent = indent.saturating_sub(usize::from(multiple_roots));
            let show_connector = connector && !virtual_root;
            let connector_pos = display_indent.saturating_sub(1);
            let foldable = children.get(&id).is_some_and(|items| !items.is_empty())
                && parents[&id].is_none_or(|parent| {
                    children.get(&parent).is_some_and(|items| items.len() > 1)
                });
            let marker = if self.collapsed.contains(&id) {
                '⊞'
            } else if foldable {
                '⊟'
            } else {
                '─'
            };
            result.insert(
                id,
                TreePrefix {
                    indent: display_indent,
                    connector: show_connector.then_some((last, marker)),
                    folded_root: self.collapsed.contains(&id) && !show_connector,
                    gutters,
                },
            );
            let descendants = children.get(&id).map_or(&[][..], Vec::as_slice);
            let branches = descendants.len() > 1;
            let next_indent = indent + usize::from(branches || (branched && indent > 0));
            let next_gutters = if show_connector {
                let index = gutter_arena.len();
                gutter_arena.push(TreeGutter {
                    level: connector_pos,
                    show: !last,
                    parent: gutters,
                });
                Some(index)
            } else {
                gutters
            };
            for (index, &child) in descendants.iter().enumerate().rev() {
                pending.push((
                    child,
                    next_indent,
                    branches,
                    branches,
                    index + 1 == descendants.len(),
                    next_gutters,
                    false,
                ));
            }
        }
        TreeLayout {
            rows: result,
            gutters: gutter_arena,
        }
    }

    fn description_spans(
        &self,
        id: u64,
        item: &Item,
        theme: &Theme,
        selected: bool,
    ) -> Vec<super::render::Span> {
        use super::render::Span;

        let mut spans = Vec::new();
        if let Some(label) = self.labels.get(&id).filter(|label| !label.is_empty()) {
            spans.push(Span::themed(format!("[{label}] "), theme, "warning"));
            if self.show_label_timestamps
                && let Some(timestamp) = self.label_timestamps.get(&id)
            {
                let timestamp = format_label_timestamp(timestamp);
                spans.push(Span::themed(format!("{timestamp} "), theme, "muted"));
            }
        }
        if let Some(description) = self.descriptions.get(&id) {
            spans.extend(description.spans(theme, selected));
        } else {
            spans.push(Span::themed(&item.label, theme, "text"));
        }
        spans
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
fn settings_match_score(query: &str, label: &str) -> Option<i64> {
    query
        .split(|ch| ch == '/' || super::session_search::is_whitespace(ch))
        .filter(|token| !token.is_empty())
        .try_fold(0_i64, |sum, token| {
            super::session_search::fuzzy_score(token, label).map(|score| sum.saturating_add(score))
        })
}

fn normalize_completion_description(text: &str) -> String {
    let mut result = String::new();
    let mut in_newline = false;
    for ch in text.chars() {
        if matches!(ch, '\r' | '\n') {
            if !in_newline {
                result.push(' ');
            }
            in_newline = true;
        } else {
            result.push(ch);
            in_newline = false;
        }
    }
    result
        .trim_matches(|ch: char| (ch.is_whitespace() && ch != '\u{0085}') || ch == '\u{feff}')
        .to_owned()
}

fn clip_completion(text: &str, width: usize) -> String {
    let mut result = String::new();
    let mut used = 0;
    for grapheme in text.graphemes(true) {
        let next = grapheme.width();
        if used + next > width {
            break;
        }
        result.push_str(grapheme);
        used += next;
    }
    result
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
    fn completion_description_follows_pi_width_and_theme_rules() {
        let picker = Picker::completion(
            vec![
                ("/audit".into(), Some("  <file>\r\n\n[focus]  ".into())),
                (
                    "/author".into(),
                    Some("second description that is long enough".into()),
                ),
            ],
            0..3,
            10,
        );
        let theme = Theme::system();
        let keys = Keys::default();
        let wide = picker.render(&theme, 80, 10, &keys);
        let selected = &wide[1];
        assert!(selected.spans.iter().any(
            |span| span.text.contains("<file> [focus]") && span.color == theme.color("accent")
        ));
        let other = &wide[2];
        assert!(
            other
                .spans
                .iter()
                .any(|span| span.text.contains("second description")
                    && span.color == theme.color("muted"))
        );
        assert!(
            selected
                .spans
                .iter()
                .all(|span| !span.text.contains(['\r', '\n']))
        );
        let narrow = picker.render(&theme, 40, 10, &keys);
        for line in &narrow[1..3] {
            let text = line
                .spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>();
            assert!(!text.contains("file") && !text.contains("description"));
            assert!(text.width() <= 40);
        }
        let clipped = picker.render(&theme, 43, 10, &keys);
        assert!(
            clipped[2]
                .spans
                .iter()
                .map(|span| span.text.width())
                .sum::<usize>()
                <= 43
        );
        assert_eq!(
            normalize_completion_description("\u{feff}a\r\n\nb\u{feff}"),
            "a b"
        );
    }

    #[test]
    fn completion_search_matches_inserted_text_not_description() {
        let mut picker = Picker::completion(
            vec![
                ("/audit".into(), Some("<path> — Inspect changes".into())),
                ("/author".into(), None),
            ],
            0..3,
            10,
        );
        picker.paste("changes");
        assert!(picker.matches().is_empty());
        picker.query.text.clear();
        picker.paste("audit");
        assert_eq!(picker.matches().len(), 1);
    }

    #[test]
    fn search_deletes_words_at_unicode_and_punctuation_boundaries() {
        let mut picker = Picker::new("Choose", vec![]);
        let keys = Keys::default();
        picker.paste("alpha café-file  ");
        let delete_word = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL);
        picker.input(delete_word, &keys);
        assert_eq!(picker.query.text, "alpha café-");
        picker.input(delete_word, &keys);
        assert_eq!(picker.query.text, "alpha café");
        picker.input(delete_word, &keys);
        assert_eq!(picker.query.text, "alpha ");
        picker.paste("e\u{301}");
        picker.input(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE), &keys);
        assert_eq!(picker.query.text, "alpha ");
    }

    #[test]
    fn search_deletes_dictionary_segmented_words() {
        let keys = Keys::default();
        let delete = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL);
        for (query, expected, next) in [
            ("東京大学", "東京", ""),
            ("中华人民共和国", "中华人民", "中华"),
            ("ภาษาไทย", "ภาษา", ""),
        ] {
            let mut picker = Picker::new("Choose", vec![]);
            picker.paste(query);
            picker.input(delete, &keys);
            assert_eq!(picker.query.text, expected, "first deletion from {query}");
            picker.input(delete, &keys);
            assert_eq!(picker.query.text, next, "second deletion from {query}");
        }
    }

    #[test]
    fn search_word_deletion_progresses_past_hebrew_apostrophes() {
        let keys = Keys::default();
        let delete = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL);
        for (query, expected) in [
            ("אב'", vec!["אב", ""]),
            ("אב'ג", vec!["אב'", "אב", ""]),
            ("אב'  ", vec!["אב", ""]),
        ] {
            let mut picker = Picker::new("Choose", vec![]);
            picker.paste(query);
            for remaining in expected {
                let previous_len = picker.query.text.len();
                picker.input(delete, &keys);
                assert_eq!(
                    picker.query.text, remaining,
                    "deleting from {query:?} at length {previous_len}"
                );
                assert!(picker.query.text.len() < previous_len);
            }
            picker.input(delete, &keys);
            assert!(picker.query.text.is_empty());
        }
    }

    #[test]
    fn session_search_word_deletion_does_not_start_session_deletion() {
        let mut picker = Picker::sessions(
            vec![Item {
                label: "first second".into(),
                selection: Selection::Resume("other".into()),
            }],
            "active".into(),
            Default::default(),
        );
        picker.paste("first second");
        assert_eq!(picker.matches().len(), 1);
        let keys = Keys::default();
        picker.input(KeyEvent::new(KeyCode::Backspace, KeyModifiers::ALT), &keys);
        assert_eq!(picker.query.text, "first ");
        assert!(picker.session_action.is_none());
    }

    #[test]
    fn mouse_selects_and_activates_visible_items_and_wheel_clamps() {
        let items = (0..8)
            .map(|id| Item {
                label: format!("item {id}"),
                selection: Selection::Checkout(id),
            })
            .collect();
        let mut picker = Picker::new("Choose", items);
        let keys = Keys::default();
        picker.render(&Theme::system(), 80, 7, &keys);
        assert!(matches!(
            picker.mouse(MouseEventKind::Down(MouseButton::Left), 3),
            MouseInput::Changed
        ));
        assert_eq!(picker.selected, 2);
        assert!(matches!(
            picker.mouse(MouseEventKind::Up(MouseButton::Left), 3),
            MouseInput::Activate
        ));
        assert!(matches!(
            picker.activate_mouse_selection(&keys),
            Input::Select(Selection::Checkout(2))
        ));
        assert!(matches!(
            picker.mouse(MouseEventKind::ScrollUp, 0),
            MouseInput::Changed
        ));
        assert_eq!(picker.selected, 1);
        picker.mouse(MouseEventKind::ScrollUp, 0);
        picker.mouse(MouseEventKind::ScrollUp, 0);
        assert_eq!(picker.selected, 0);
        picker.mouse(MouseEventKind::Down(MouseButton::Left), 1);
        picker.mouse(MouseEventKind::Drag(MouseButton::Left), 2);
        assert!(matches!(
            picker.mouse(MouseEventKind::Up(MouseButton::Left), 2),
            MouseInput::Ignored
        ));
        assert!(matches!(
            picker.mouse(MouseEventKind::Down(MouseButton::Left), 6),
            MouseInput::Ignored
        ));
    }

    #[test]
    fn mouse_activation_toggles_scoped_models() {
        let mut picker =
            Picker::scoped_models("ollama", vec!["alpha".into()], vec!["alpha".into()]);
        let keys = Keys::default();
        picker.render(&Theme::system(), 80, 8, &keys);
        picker.mouse(MouseEventKind::Down(MouseButton::Left), 1);
        assert!(matches!(
            picker.mouse(MouseEventKind::Up(MouseButton::Left), 1),
            MouseInput::Activate
        ));
        assert!(matches!(
            picker.activate_mouse_selection(&keys),
            Input::UpdateModelScope { save: false }
        ));
        assert!(picker.model_scope().unwrap().1.is_empty());
    }

    #[test]
    fn settings_align_values_and_use_pi_theme_roles() {
        let item = |label: &str| Item {
            label: label.into(),
            selection: Selection::EditSetting("theme".into()),
        };
        let mut picker = Picker::new(
            "Settings",
            vec![item("Theme · dark"), item("Long setting · on")],
        );
        let theme = Theme::system();
        let keys = Keys::default();
        let lines = picker.render(&theme, 80, 8, &keys);
        let plain = |line: &Line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        };
        assert_eq!(plain(&lines[1]), "→ Theme         dark");
        assert_eq!(plain(&lines[2]), "  Long setting  on");
        assert_eq!(lines[1].spans[1].color, theme.color("accent"));
        assert_eq!(lines[1].spans[2].color, theme.color("accent"));
        assert_eq!(lines[2].spans[1].color, theme.color("text"));
        assert_eq!(lines[2].spans[2].color, theme.color("muted"));
        assert!(lines[1].background.is_none());
        picker.paste("Theme");
        let lines = picker.render(&theme, 80, 8, &keys);
        assert_eq!(plain(&lines[1]), "→ Theme         dark");
        let compact = picker.render(&theme, 16, 8, &keys);
        assert_eq!(plain(&compact[1]), "→ Theme  dark");
        let exact_width = picker.render(&theme, 20, 8, &keys);
        assert_eq!(plain(&exact_width[1]), "→ Theme         dark");
        for width in 0..20 {
            let lines = picker.render(&theme, width, 8, &keys);
            assert!(plain(&lines[1]).width() <= width);
        }
    }

    #[test]
    fn settings_describe_selected_item_within_the_available_height() {
        let item = |label: &str, key: &str| Item {
            label: label.into(),
            selection: Selection::Setting(key.into(), serde_json::json!(true)),
        };
        let mut picker = Picker::new(
            "Settings",
            vec![
                item("HTTP idle timeout", "httpIdleTimeoutMs"),
                item("Block images", "images.blockImages"),
            ],
        );
        let theme = Theme::system();
        let keys = Keys::default();
        let lines = picker.render(&theme, 80, 8, &keys);
        assert!(lines.iter().any(|line| {
            line.spans.iter().any(|span| {
                span.text.contains("Maximum idle gap") && span.dim == theme.is_dim("dim")
            })
        }));
        picker.paste("Block images");
        let lines = picker.render(&theme, 24, 6, &keys);
        let text = lines
            .iter()
            .flat_map(|line| line.spans.iter().map(|span| span.text.as_str()))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(text.contains("Prevent images"), "{text}");
        assert!(!text.contains("Maximum idle gap"), "{text}");
        assert!(text.contains('…'), "truncation should be visible: {text}");
        assert!(lines.len() <= 6, "{} lines: {text}", lines.len());
        for width in 1..4 {
            let tiny = picker.render(&theme, width, 6, &keys);
            assert!(
                !tiny
                    .iter()
                    .flat_map(|line| &line.spans)
                    .any(|span| span.text.contains("Prevent images"))
            );
        }
        let description = lines
            .iter()
            .find(|line| {
                line.spans
                    .iter()
                    .any(|span| span.text.contains("Prevent images"))
            })
            .unwrap();
        assert!(
            description
                .spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
                .width()
                <= 24
        );
    }

    #[test]
    fn settings_refresh_preserves_search_and_selected_setting() {
        let item = |label: &str, value| Item {
            label: format!("{label} · {value}"),
            selection: Selection::Setting(label.into(), serde_json::json!(value)),
        };
        let mut picker = Picker::new("Settings", vec![item("HTTP timeout", 5), item("Other", 1)]);
        picker.paste("ht to");
        assert_eq!(picker.matches().len(), 1);
        picker.refresh_settings(vec![item("Other", 1), item("HTTP timeout", 30)]);
        assert!(
            matches!(&picker.matches()[picker.selected].selection, Selection::Setting(name, value) if name == "HTTP timeout" && *value == 30)
        );
        assert_eq!(picker.query.text, "ht to");
    }

    #[test]
    fn settings_search_can_edit_at_the_cursor_without_moving_the_selection() {
        let settings = ["Quiet startup", "HTTP idle timeout", "Model timeout"]
            .into_iter()
            .map(|label| Item {
                label: format!("{label} · value"),
                selection: Selection::Setting(label.into(), serde_json::json!(true)),
            })
            .collect();
        let mut picker = Picker::new("Settings", settings);
        let keys = Keys::default();
        let key = |code| KeyEvent::new(code, KeyModifiers::NONE);

        let empty = picker.render(&Theme::system(), 80, 20, &keys);
        assert!(
            empty[0]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == " ")
        );
        picker.paste("ttp timeout");
        picker.input(key(KeyCode::Home), &keys);
        picker.input(key(KeyCode::Char('h')), &keys);
        assert_eq!(picker.query.text, "http timeout");
        assert_eq!(picker.query.cursor, 1);
        assert_eq!(
            picker.matches()[picker.selected].label,
            "HTTP idle timeout · value"
        );
        let lines = picker.render(&Theme::system(), 80, 20, &keys);
        assert!(
            lines[0]
                .spans
                .iter()
                .any(|span| span.inverse && span.text == "t")
        );

        picker.input(key(KeyCode::Delete), &keys);
        assert_eq!(picker.query.text, "htp timeout");
        picker.paste("t");
        picker.input(key(KeyCode::End), &keys);
        assert_eq!(picker.query.text, "http timeout");
        assert_eq!(picker.query.cursor, picker.query.text.len());

        picker.paste(" avec une recherche plus longue 界");
        let narrow = picker.render(&Theme::system(), 27, 20, &keys);
        assert!(narrow[0].spans.iter().any(|span| span.inverse));
        assert!(
            narrow[0]
                .spans
                .iter()
                .map(|span| span.text.width())
                .sum::<usize>()
                <= 27
        );
    }

    #[test]
    fn settings_search_prefers_editor_bindings_over_app_shortcuts() {
        let mut picker = Picker::new("Settings", vec![]);
        let mut keys = Keys::default();
        keys.set_test_binding("tui.editor.cursorLeft", "ctrl+l");
        picker.paste("hte");
        picker.input(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL),
            &keys,
        );
        picker.input(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE), &keys);
        assert_eq!(picker.query.text, "htre");
    }

    #[test]
    fn settings_search_yank_preserves_the_original_query() {
        let mut picker = Picker::new("Settings", vec![]);
        let keys = Keys::default();
        picker.paste("http\u{1b}timeout");
        picker.input(
            KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL),
            &keys,
        );
        assert!(picker.query.text.is_empty());
        picker.input(
            KeyEvent::new(KeyCode::Char('y'), KeyModifiers::CONTROL),
            &keys,
        );
        assert_eq!(picker.query.text, "http\u{1b}timeout");
    }

    #[test]
    fn settings_search_matches_control_characters_without_rendering_them() {
        let mut picker = Picker::new("Settings", vec![]);
        picker.paste("http");
        picker.input(
            KeyEvent::new(KeyCode::Char('\u{85}'), KeyModifiers::NONE),
            &Keys::default(),
        );
        assert_eq!(picker.query.text, "http\u{85}");
        let heading = picker.settings_heading("Settings · ", &Theme::system(), 80);
        assert!(
            heading
                .spans
                .iter()
                .all(|span| !span.text.contains('\u{85}'))
        );
    }

    #[test]
    fn settings_search_uses_pi_token_order_and_scores() {
        let item = |name: &str| Item {
            label: format!("{name} · value"),
            selection: Selection::Setting(name.into(), serde_json::json!(true)),
        };
        let mut picker = Picker::new(
            "Settings",
            [
                "HTTP timeout",
                "Timeout HTTP",
                "HTTP response timeout",
                "Unrelated",
            ]
            .into_iter()
            .map(item)
            .collect(),
        );
        for query in ["timeout/http", "timeout http", "ht\u{feff}time"] {
            picker.query.text = query.into();
            let matches = picker.matches();
            let names: Vec<_> = matches.iter().map(|item| item.label.as_str()).collect();
            assert_eq!(
                names,
                [
                    "Timeout HTTP · value",
                    "HTTP timeout · value",
                    "HTTP response timeout · value"
                ],
                "{query:?}"
            );
        }
        picker.query.text = "http\u{85}timeout".into();
        assert!(picker.matches().is_empty());
        picker.query.text = "//".into();
        assert_eq!(picker.matches().len(), 4);
    }

    #[test]
    fn settings_scroll_window_stays_centered_and_keeps_mouse_rows_aligned() {
        let items = (0..19)
            .map(|index| Item {
                label: format!("Setting {index:02}"),
                selection: Selection::EditSetting(format!("key{index}")),
            })
            .collect();
        let mut picker = Picker::new("Settings", items);
        let theme = Theme::system();
        let keys = Keys::default();
        let contains = |lines: &[Line], value: &str| {
            lines
                .iter()
                .any(|line| line.spans.iter().any(|span| span.text.contains(value)))
        };
        for (selected, first) in [(0, 0), (8, 3), (18, 9)] {
            picker.selected = selected;
            let lines = picker.render(&theme, 80, 32, &keys);
            assert!(contains(&lines, format!("({}/19)", selected + 1).as_str()));
            assert_eq!(picker.mouse_layout.get(), (first, 10, 32));
            for index in 0..19 {
                assert_eq!(
                    contains(&lines, format!("Setting {index:02}").as_str()),
                    (first..first + 10).contains(&index)
                );
            }
        }
        let lines = picker.render(&theme, 80, 8, &keys);
        assert!(lines.len() <= 8);
        assert!(contains(&lines, "Setting 18"));
        picker.paste("missing");
        let lines = picker.render(&theme, 80, 32, &keys);
        assert!(contains(&lines, "No matching settings"));
    }

    #[test]
    fn settings_paste_preserves_filter_semantics_and_sanitizes_display() {
        let mut picker = Picker::new(
            "Settings",
            vec![Item {
                label: "HTTP timeout · 30".into(),
                selection: Selection::Setting("HTTP timeout".into(), serde_json::json!(30)),
            }],
        );
        picker.paste("time\r\nout\thttp");
        assert_eq!(picker.query.text, "timeout    http");
        assert_eq!(picker.matches().len(), 1);

        picker.query.replace(String::new());
        picker.paste("http\u{85}timeout");
        assert_eq!(picker.query.text, "http\u{85}timeout");
        assert!(picker.matches().is_empty());

        picker.paste("\u{1b}]8;;https://example.invalid\u{7}");
        let lines = picker.render(&Theme::system(), 80, 8, &Keys::default());
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .all(|span| { span.text.chars().all(|character| !character.is_control()) })
        );
    }
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
        assert_eq!(picker.selected, 2);
        picker.focus_record(1);
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        assert_eq!(picker.matches().len(), 1);
        picker.paste("3");
        assert_eq!(picker.matches()[0].label, "node 3");
        picker.query.text.clear();
        picker.focus_record(1);
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT), &keys);
        assert_eq!(picker.matches().len(), 5);
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT), &keys);
        assert!(matches!(
            picker.matches()[picker.selected].selection,
            Selection::Checkout(4)
        ));
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        assert!(matches!(
            picker.matches()[picker.selected].selection,
            Selection::Checkout(1)
        ));
        let edit_label = KeyEvent::new(KeyCode::Char('L'), KeyModifiers::SHIFT);
        let escape = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        let confirm = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
        picker.paste("node 1");
        picker.input(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL),
            &keys,
        );
        picker.input(
            KeyEvent::new(KeyCode::Char('T'), KeyModifiers::SHIFT),
            &keys,
        );
        picker.input(edit_label, &keys);
        assert_eq!(picker.query.text, "root");
        assert!(matches!(picker.input(escape, &keys), Input::Continue));
        assert_eq!(picker.query.text, "node 1");
        assert!(matches!(picker.selected(), Some(Selection::Checkout(1))));
        picker.input(edit_label, &keys);
        picker.query.text = "updated".into();
        assert!(
            matches!(picker.input(confirm, &keys), Input::SetTreeLabel(1, value) if value == "updated")
        );
        assert_eq!(picker.query.text, "node 1");
        assert!(matches!(picker.selected(), Some(Selection::Checkout(1))));
        let tree = picker.tree.as_ref().unwrap();
        assert_eq!(tree.filter.name(), "labeled-only");
        assert!(tree.show_label_timestamps);
    }

    #[test]
    fn tree_layout_prioritizes_active_branch_and_reflows_filtered_nodes() {
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
        let mut picker = Picker::tree(items, entries, Default::default(), 5, TreeFilter::Default);
        let tree = picker.tree.as_ref().unwrap();
        assert_eq!(tree.order, [1, 4, 5, 2, 3]);
        let layout = tree.layout(&tree.order);
        let text = |layout: &TreeLayout, id| {
            layout.rows[&id].slice(&layout.gutters, 0, layout.rows[&id].width())
        };
        assert_eq!(text(&layout, 1), "");
        assert_eq!(text(&layout, 4), "├⊟ ");
        assert_eq!(text(&layout, 5), "│     ");
        assert_eq!(text(&layout, 2), "└⊟ ");
        assert_eq!(text(&tree.layout(&[1, 5]), 5), "");
        let layout = tree.layout(&[1, 5, 2, 3]);
        assert_eq!(text(&layout, 5), "├─ ");
        assert_eq!(text(&layout, 2), "└⊟ ");
        let lines = picker.render(&Theme::system(), 80, 20, &Keys::default());
        let text: Vec<String> = lines
            .iter()
            .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
            .collect();
        assert!(text.iter().any(|line| line.contains("├⊟ • node 4")));
        assert!(text.iter().any(|line| line.contains("└⊟ node 2")));
        picker.focus_record(1);
        picker.input(
            KeyEvent::new(KeyCode::Right, KeyModifiers::ALT),
            &Keys::default(),
        );
        assert!(matches!(
            picker.matches()[picker.selected].selection,
            Selection::Checkout(4)
        ));
    }

    #[test]
    fn tree_active_root_precedes_other_roots() {
        let entries = [(1, None), (2, Some(1)), (3, None), (4, Some(3))]
            .into_iter()
            .map(|(id, parent)| (id, (parent, TreeEntryKind::Message)))
            .collect();
        let tree = TreeState::new(entries, Default::default(), 4, TreeFilter::Default);
        assert_eq!(tree.order, [3, 4, 1, 2]);
        let layout = tree.layout(&tree.order);
        assert_eq!(
            layout.rows[&3].slice(&layout.gutters, 0, layout.rows[&3].width()),
            ""
        );
        assert_eq!(
            layout.rows[&1].slice(&layout.gutters, 0, layout.rows[&1].width()),
            ""
        );
        assert_eq!(
            layout.rows[&4].slice(&layout.gutters, 0, layout.rows[&4].width()),
            "   "
        );
    }

    #[test]
    fn tree_viewport_centers_selection_and_pages_by_terminal_height() {
        let entries = (1..=40)
            .map(|id| (id, ((id > 1).then_some(id - 1), TreeEntryKind::User)))
            .collect();
        let items = (1..=40)
            .map(|id| Item {
                label: format!("viewport-entry-{id:02}"),
                selection: Selection::Checkout(id),
            })
            .collect();
        let mut picker = Picker::tree(items, entries, Default::default(), 40, TreeFilter::Default);
        let theme = Theme::system();
        let keys = Keys::default();

        for (key, expected, first) in [
            (None, 40, 27),
            (Some(KeyCode::PageUp), 26, 19),
            (Some(KeyCode::PageDown), 40, 27),
        ] {
            if let Some(key) = key {
                picker.input(KeyEvent::new(key, KeyModifiers::NONE), &keys);
            }
            let rows = picker.render_in_terminal(&theme, 80, 22, 28, &keys);
            assert_eq!(picker.mouse_layout.get(), (first - 1, 14, 22));
            assert_eq!(picker.selected + 1, expected);
            assert_eq!(rows[15].spans[0].text, format!("  ({expected}/40)"));
            let selected = rows
                .iter()
                .find(|line| line.spans.first().is_some_and(|span| span.text == "› "))
                .unwrap();
            assert!(
                selected
                    .spans
                    .iter()
                    .any(|span| span.text.contains(&format!("viewport-entry-{expected:02}")))
            );
        }

        picker.render_in_terminal(&theme, 80, 10, 12, &keys);
        assert_eq!(picker.page_size.get(), 6);
        picker.render_in_terminal(&theme, 80, 8, 9, &keys);
        assert_eq!(picker.page_size.get(), 5);
        picker.render_in_terminal(&theme, 80, 7, 9, &keys);
        assert_eq!(picker.page_size.get(), 4);
    }

    #[test]
    fn tree_viewport_tracks_deep_selection_and_branch_navigation() {
        let mut entries = std::collections::BTreeMap::new();
        entries.insert(1, (None, TreeEntryKind::Message));
        let mut parent = 1;
        for depth in 1..=16 {
            let child = depth * 2;
            entries.insert(child, (Some(parent), TreeEntryKind::Message));
            entries.insert(child + 1, (Some(parent), TreeEntryKind::Message));
            parent = child;
        }
        let items = entries
            .keys()
            .map(|id| Item {
                label: format!("node-{id}-日本語"),
                selection: Selection::Checkout(*id),
            })
            .collect();
        let mut picker = Picker::tree(
            items,
            entries,
            Default::default(),
            parent,
            TreeFilter::Default,
        );
        let theme = Theme::system();
        let keys = Keys::default();
        let rows = picker.render(&theme, 24, 50, &keys);
        let selected = rows
            .iter()
            .find(|line| line.spans.first().is_some_and(|span| span.text == "› "))
            .unwrap();
        let text: String = selected
            .spans
            .iter()
            .map(|span| span.text.as_str())
            .collect();
        assert!(text.contains("node-32"), "{text:?}");
        assert!(text.contains('├') || text.contains('└'), "{text:?}");
        assert_eq!(selected.background, Some(theme.color("selectedBg")));
        assert!(
            rows.iter()
                .skip(1)
                .take(picker.page_size.get())
                .all(|line| {
                    line.spans
                        .iter()
                        .map(|span| span.text.width())
                        .sum::<usize>()
                        <= 24
                })
        );
        let tree = picker.tree.as_ref().unwrap();
        assert!(tree.layout(&tree.order).rows[&parent].width() > 36);

        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        assert_ne!(picker.selected, 0);
        picker.focus_record(1);
        let rows = picker.render(&theme, 24, 50, &keys);
        let selected = rows
            .iter()
            .find(|line| line.spans.first().is_some_and(|span| span.text == "› "))
            .unwrap();
        let text: String = selected
            .spans
            .iter()
            .map(|span| span.text.as_str())
            .collect();
        assert!(text.starts_with("› • node-1"), "{text:?}");
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT), &keys);
        assert_eq!(picker.matches().len(), 1);
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT), &keys);
        assert_eq!(picker.matches().len(), 33);
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT), &keys);
        assert!(matches!(
            picker.matches()[picker.selected].selection,
            Selection::Checkout(2)
        ));
        picker.focus_record(1);
        picker.render(&theme, 8, 8, &keys);
        let page = picker.page_size.get();
        picker.input(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE), &keys);
        assert_eq!(picker.selected, page);
        let rows = picker.render(&theme, 8, 8, &keys);
        let selected = rows
            .iter()
            .find(|line| line.spans.first().is_some_and(|span| span.text == "› "))
            .unwrap();
        assert!(
            selected
                .spans
                .iter()
                .map(|span| span.text.width())
                .sum::<usize>()
                <= 8
        );
        picker.input(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE), &keys);
        assert_eq!(picker.selected, 0);
    }

    #[test]
    fn tree_viewport_clips_wide_graphemes_without_losing_style() {
        let mut span =
            super::super::render::Span::new("a日本e\u{301}z", crossterm::style::Color::Red);
        span.bold = true;
        let sliced = slice_tree_spans(&[span], 2, 4);
        assert_eq!(
            sliced.iter().map(|s| s.text.as_str()).collect::<String>(),
            "本e\u{301}"
        );
        assert!(
            sliced
                .iter()
                .all(|s| s.bold && s.color == crossterm::style::Color::Red)
        );
        assert!(slice_tree_spans(&sliced, 0, 0).is_empty());
        let first = super::super::render::Span::new("日", crossterm::style::Color::Blue);
        let mut second = super::super::render::Span::new("本abc", crossterm::style::Color::Green);
        second.bold = true;
        let sliced = slice_tree_spans(&[first, second], 1, 4);
        assert_eq!(
            sliced.iter().map(|s| s.text.as_str()).collect::<String>(),
            "本a"
        );
        assert!(
            sliced
                .iter()
                .all(|s| s.bold && s.color == crossterm::style::Color::Green)
        );
        let span = super::super::render::Span::new("a日b", crossterm::style::Color::Red);
        assert_eq!(
            slice_tree_spans(&[span], 0, 2)
                .iter()
                .map(|s| s.text.as_str())
                .collect::<String>(),
            "a"
        );
    }

    #[test]
    fn tree_navigation_uses_visible_branch_segments() {
        use TreeEntryKind::{Bookkeeping, Message};
        let entries = [
            (1, None, Message),
            (2, Some(1), Bookkeeping),
            (3, Some(2), Message),
            (4, Some(3), Message),
            (5, Some(4), Message),
            (6, Some(3), Message),
            (7, Some(6), Message),
        ]
        .into_iter()
        .map(|(id, parent, kind)| (id, (parent, kind)))
        .collect();
        let items = (1..=7)
            .map(|id| Item {
                label: format!("node {id}"),
                selection: Selection::Checkout(id),
            })
            .collect();
        let mut picker = Picker::tree(items, entries, Default::default(), 1, TreeFilter::Default);
        let keys = Keys::default();
        let left = KeyEvent::new(KeyCode::Left, KeyModifiers::ALT);
        let right = KeyEvent::new(KeyCode::Right, KeyModifiers::ALT);
        let selected = |picker: &Picker| match picker.matches()[picker.selected].selection {
            Selection::Checkout(id) => id,
            _ => unreachable!(),
        };
        picker.input(right, &keys);
        assert_eq!(
            selected(&picker),
            4,
            "down skips the single-child chain and enters the first branch"
        );
        picker.input(right, &keys);
        assert_eq!(selected(&picker), 5);
        picker.input(left, &keys);
        assert_eq!(selected(&picker), 4);
        picker.input(left, &keys);
        assert_eq!(
            picker.matches().len(),
            5,
            "a branch segment start folds its descendants"
        );
        picker.input(right, &keys);
        assert_eq!(picker.matches().len(), 6);
        picker.input(left, &keys);
        picker.input(left, &keys);
        assert_eq!(
            selected(&picker),
            1,
            "up from a folded segment reaches the visible root"
        );

        picker.paste("node");
        picker.focus_record(3);
        picker.input(left, &keys);
        assert_eq!(
            selected(&picker),
            1,
            "a linear node is navigated instead of folded"
        );
        picker.focus_record(4);
        picker.input(left, &keys);
        assert!(
            !picker
                .matches()
                .iter()
                .any(|item| matches!(item.selection, Selection::Checkout(5))),
            "folding also applies while searching"
        );
        picker.input(KeyEvent::new(KeyCode::Char('5'), KeyModifiers::NONE), &keys);
        assert!(
            picker.tree.as_ref().unwrap().collapsed.is_empty(),
            "editing search clears folds"
        );
    }

    #[test]
    fn tree_search_keeps_selection_through_empty_results_and_escape() {
        let entries = [
            (1, (None, TreeEntryKind::User)),
            (2, (Some(1), TreeEntryKind::Message)),
            (3, (Some(2), TreeEntryKind::User)),
        ]
        .into_iter()
        .collect();
        let items = ["shared alpha", "shared beta", "shared gamma"]
            .into_iter()
            .enumerate()
            .map(|(index, label)| Item {
                label: label.into(),
                selection: Selection::Checkout(index as u64 + 1),
            })
            .collect();
        let labels = [(2, "checkpoint".into())].into_iter().collect();
        let mut picker = Picker::tree(items, entries, labels, 3, TreeFilter::Default);
        let keys = Keys::default();
        let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
        let selected = |picker: &Picker| {
            picker
                .matches()
                .get(picker.selected)
                .map(|item| match item.selection {
                    Selection::Checkout(id) => id,
                    _ => unreachable!(),
                })
        };

        picker.input(key(KeyCode::Up), &keys);
        picker.paste("SHARED");
        assert_eq!(selected(&picker), Some(2));
        picker.input(key(KeyCode::Char('x')), &keys);
        assert_eq!(selected(&picker), None);
        picker.input(key(KeyCode::Backspace), &keys);
        assert_eq!(selected(&picker), Some(2));
        picker.paste(" checkpoint beta");
        assert_eq!(selected(&picker), Some(2));
        picker.paste(" absent");
        assert_eq!(selected(&picker), None);
        assert!(matches!(
            picker.input(key(KeyCode::Esc), &keys),
            Input::Continue
        ));
        assert_eq!(selected(&picker), Some(2));
        assert!(matches!(
            picker.input(key(KeyCode::Esc), &keys),
            Input::Cancel
        ));

        picker.focus_record(3);
        picker.paste("beta");
        assert_eq!(
            selected(&picker),
            Some(2),
            "fall back to the visible ancestor"
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
        picker.query.text = "gam".into();
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
        picker.query.text = "needle".into();
        assert_eq!(selected(&picker), paths);
        picker.query.text.clear();
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
        picker.query.text = "needle".into();
        assert_eq!(picker.matches().len(), 1);
        assert!(
            matches!(&picker.matches()[0].selection, Selection::Resume(path) if *path == local)
        );
        picker.all_workspaces = true;
        assert_eq!(picker.matches().len(), 2);
        picker.query.text = "re:transcript.*needle".into();
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
        picker.query.text = "Victim".into();
        picker.session_action = Some(SessionAction::Delete(path.clone()));
        assert!(matches!(
            picker.input(
                KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
                &Keys::default()
            ),
            Input::Continue
        ));
        assert_eq!(picker.query.text, "Victim");
        assert!(picker.session_action.is_none());
        assert!(
            matches!(&picker.matches()[0].selection, Selection::Resume(selected) if *selected == path)
        );
    }
}
