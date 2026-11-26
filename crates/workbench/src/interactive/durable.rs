//! The terminal view over the Durable companion's line-delimited RPC.
use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{BufRead, BufReader, Read},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, TryRecvError},
};

use clap::Parser;
use uuid::Uuid;

use super::*;
use crate::ui::input::{InputEvent, Reader};
use crate::ui::picker::TreeEntryKind;

// The companion counts the whole UTF-8 RPC line, including its newline.
const MAX_RPC_REQUEST_BYTES: usize = 4 * 1024 * 1024;

struct Companion {
    child: Child,
    input: Option<ChildStdin>,
    events: Receiver<Result<Value>>,
}

impl Companion {
    fn spawn(executable: &Path, arguments: &[OsString]) -> Result<Self> {
        let mut child = Command::new(executable)
            .args(arguments)
            .arg("--rpc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().expect("piped Durable stdin");
        let stdout = child.stdout.take().expect("piped Durable stdout");
        let stderr = child.stderr.take().expect("piped Durable stderr");
        let (sender, events) = mpsc::sync_channel(32);
        let errors = sender.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let event = line
                    .map_err(Error::from)
                    .and_then(|line| serde_json::from_str(&line).map_err(Error::from));
                let stop = event.is_err();
                if sender.send(event).is_err() || stop {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            let mut stream = BufReader::new(stderr);
            let mut captured = Vec::new();
            if stream
                .by_ref()
                .take(8192)
                .read_to_end(&mut captured)
                .is_ok()
            {
                let _ = std::io::copy(&mut stream, &mut std::io::sink());
            }
            if !captured.is_empty() {
                let message = String::from_utf8_lossy(&captured);
                let _ = errors.send(Err(Error::Config(message.trim().to_owned())));
            }
        });
        Ok(Self {
            child,
            input: Some(input),
            events,
        })
    }

    fn send(&mut self, request: Value) -> Result<()> {
        self.send_json(&serde_json::to_vec(&request)?)
    }

    fn send_json(&mut self, request: &[u8]) -> Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| Error::Config("Durable companion input is closed".into()))?;
        input.write_all(request)?;
        writeln!(input)?;
        input.flush()?;
        Ok(())
    }
}

impl Drop for Companion {
    fn drop(&mut self) {
        drop(self.input.take());
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                _ => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const FOREGROUND_REFRESH: Duration = Duration::from_millis(250);

struct ForegroundViewer {
    child: Uuid,
    dialog: Uuid,
    request: Option<Uuid>,
    last_refresh: Option<Instant>,
    stopped: bool,
}

#[derive(Default)]
struct Conversation {
    active: bool,
    submissions: BTreeMap<Uuid, pi_rs::QueuedMessage>,
    tree: Option<TerminalTree>,
    tree_request: Option<Uuid>,
    fork_pending: bool,
    foreground_children: Vec<Uuid>,
    foreground_choices: Option<Vec<Uuid>>,
    foreground_viewer: Option<ForegroundViewer>,
}

impl Conversation {
    fn busy(&self) -> bool {
        self.active || self.fork_pending || !self.submissions.is_empty()
    }

    fn event(&mut self, view: &mut View, args: &mut Cli, event: Value) {
        match event["type"].as_str() {
            Some("response") => {
                let id = entry_id(&event["id"]);
                let command = event["command"].as_str();
                if command == Some("get_foreground_subagent") {
                    self.receive_foreground_view(view, id, &event);
                    return;
                }
                if command == Some("get_entries") {
                    if id.is_some() && id == self.tree_request {
                        self.tree_request = None;
                        self.show_tree(view, args, &event, None);
                    }
                    return;
                }
                if command == Some("set_session_label") {
                    let focus = id.and_then(|id| view.picker.as_mut()?.take_tree_label(id));
                    if let Some(focus) = focus {
                        self.show_tree(view, args, &event, Some(focus));
                    }
                    return;
                }
                if command == Some("fork_at_entry") {
                    self.fork_pending = false;
                    if event["success"] == true {
                        self.tree = None;
                        view.picker = None;
                        view.streaming.clear();
                        view.session_name = event["data"]["sessionId"].as_str().map(str::to_owned);
                        if let Some(model) = event["data"]["model"]["id"].as_str() {
                            args.model = model.to_owned();
                        }
                        restore_messages(view, &event["data"]);
                        view.append("Forked to new session");
                        view.viewport.follow();
                        return;
                    }
                    if let Some(picker) = view.picker.as_mut() {
                        picker.show_feedback(Err(event["error"]
                            .as_str()
                            .unwrap_or("Durable fork failed")
                            .into()));
                        return;
                    }
                }
                let pending = id.and_then(|id| self.submissions.remove(&id));
                if event["success"] != true {
                    if let Some(pending) = pending {
                        restore_draft(view, pending);
                    }
                    view.append(event["error"].as_str().unwrap_or("Durable request failed"));
                    return;
                }
                match event["command"].as_str() {
                    Some("get_state") => {
                        self.active = event["data"]["isStreaming"] == true;
                        if let Some(model) = event["data"]["model"]["id"].as_str() {
                            args.model = model.to_owned();
                        }
                        if let Some(session) = event["data"]["sessionId"].as_str()
                            && view.session_name.as_deref() != Some(session)
                        {
                            view.session_name = Some(session.to_owned());
                            append_session_reference(view);
                        }
                    }
                    Some("get_messages") => {
                        restore_messages(view, &event["data"]);
                    }
                    Some("prompt") if event["data"]["disposition"] == "started" => {
                        self.active = true;
                    }
                    Some("clear_queue") => restore_queue(view, &event["data"]),
                    _ => {}
                }
            }
            Some("agent_start") => self.active = true,
            Some("agent_end") => {
                self.active = false;
                self.foreground_children.clear();
            }
            Some("durable_foreground_subagents") => {
                self.foreground_children = event["conversationIds"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(entry_id)
                    .take(16)
                    .collect();
            }
            Some("tool_execution_start") => {
                show_tool(view, &event["durableInvocation"], &Value::Null);
            }
            Some("tool_execution_update") => view.update_tool_progress(&event),
            Some("tool_execution_end") => {
                show_tool(view, &event["durableInvocation"], &event["durableOutcome"]);
            }
            Some("message_start") if event["message"]["role"] == "user" => {
                append_message(view, &event["message"]);
                view.viewport.follow();
            }
            Some("message_update") if event["message"]["role"] == "assistant" => {
                view.streaming = message_text(&event["message"]);
            }
            Some("message_end") if event["message"]["role"] == "assistant" => {
                view.streaming.clear();
                append_message(view, &event["message"]);
                if let Some(error) = event["message"]["errorMessage"].as_str() {
                    view.append(error);
                }
            }
            _ => {}
        }
    }

    fn choose_foreground(&mut self, view: &mut View) {
        self.tree = None;
        self.tree_request = None;
        let children = self.foreground_children.clone();
        let mut items = vec![Item {
            label: "main".into(),
            selection: Selection::Checkout(0),
        }];
        items.extend(children.iter().enumerate().map(|(index, child)| Item {
            label: format!("Subagent {child}"),
            selection: Selection::Checkout(index as u64 + 1),
        }));
        view.picker = Some(Picker::new("Conversations", items));
        self.foreground_choices = Some(children);
    }

    fn pick_foreground(&mut self, view: &mut View, key: Option<KeyEvent>, keys: &Keys) -> bool {
        let Some(choices) = self.foreground_choices.as_ref() else {
            return false;
        };
        let Some(picker) = view.picker.as_mut() else {
            return false;
        };
        let input = match key {
            Some(key) => picker.input(key, keys),
            None => picker.activate_mouse_selection(keys),
        };
        match input {
            Input::Cancel | Input::Select(Selection::Checkout(0)) => {
                view.picker = None;
                self.foreground_choices = None;
            }
            Input::Select(Selection::Checkout(row)) => {
                let child = choices.get((row - 1) as usize).copied();
                if let Some(child) = child.filter(|child| self.foreground_children.contains(child))
                {
                    view.picker = None;
                    self.foreground_choices = None;
                    self.open_foreground_view(view, child);
                } else {
                    picker.show_feedback(Err("Subagent is no longer running".into()));
                }
            }
            _ => {}
        }
        true
    }

    fn open_foreground_view(&mut self, view: &mut View, child: Uuid) {
        let dialog = Uuid::new_v4();
        view.dialogs.push_front(crate::ui::dialog::Dialog::new(
            dialog,
            pi_rs::UiRequest::Custom {
                title: format!("Subagent {child}"),
                lines: vec![foreground_line("Loading conversation", "muted")],
                actions: vec![],
                timeout: None,
            },
        ));
        self.foreground_viewer = Some(ForegroundViewer {
            child,
            dialog,
            request: None,
            last_refresh: None,
            stopped: false,
        });
    }

    fn foreground_input(&mut self, view: &mut View, key: KeyEvent, keys: &Keys) -> bool {
        let Some(viewer) = &self.foreground_viewer else {
            return false;
        };
        if let Some(dialog) = view
            .dialog_mut()
            .filter(|dialog| dialog.id == viewer.dialog)
            && dialog.input(key, keys).is_some()
        {
            view.dialogs.pop_front();
            self.foreground_viewer = None;
        }
        true
    }

    fn refresh_foreground_view(&mut self, send: impl FnOnce(Value) -> Result<()>) -> Result<()> {
        let Some(viewer) = self.foreground_viewer.as_mut() else {
            return Ok(());
        };
        if viewer.stopped
            || viewer.request.is_some()
            || !self.foreground_children.contains(&viewer.child)
            || viewer
                .last_refresh
                .is_some_and(|at| at.elapsed() < FOREGROUND_REFRESH)
        {
            return Ok(());
        }
        let id = Uuid::new_v4();
        send(json!({"type": "get_foreground_subagent", "conversationId": viewer.child, "id": id}))?;
        viewer.request = Some(id);
        viewer.last_refresh = Some(Instant::now());
        Ok(())
    }

    fn receive_foreground_view(&mut self, view: &mut View, id: Option<Uuid>, event: &Value) {
        let Some(viewer) = self.foreground_viewer.as_mut() else {
            return;
        };
        if id.is_none() || id != viewer.request {
            return;
        }
        viewer.request = None;
        let Some(dialog) = view
            .dialog_mut()
            .filter(|dialog| dialog.id == viewer.dialog)
        else {
            return;
        };
        if event["success"] == true
            && entry_id(&event["data"]["conversationId"]) == Some(viewer.child)
        {
            dialog.refresh_custom_lines(foreground_transcript(&event["data"]));
        } else {
            viewer.stopped = true;
            dialog.set_error(
                event["error"]
                    .as_str()
                    .unwrap_or("Subagent view unavailable")
                    .into(),
            );
        }
    }

    fn submit(
        &mut self,
        view: &mut View,
        follow_up: bool,
        send: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<()> {
        if view.clipboard.is_some() || self.fork_pending {
            return Ok(());
        }
        let text = &view.editor.text;
        if text.trim().is_empty() && view.draft_images.is_empty() {
            return Ok(());
        }
        let busy = self.busy();
        let id = Uuid::new_v4();
        let images: Vec<_> = view
            .draft_images
            .iter()
            .map(|image| json!({"type":"image", "mimeType":image.mime_type, "data":image.data}))
            .collect();
        let mut request = json!({"type":"prompt", "id":id, "message":text, "images":images});
        if busy {
            request["streamingBehavior"] = json!(if follow_up { "followUp" } else { "steer" });
        }
        let encoded = serde_json::to_vec(&request)?;
        if encoded.len() >= MAX_RPC_REQUEST_BYTES {
            view.append("Durable request exceeds 4 MiB. Shorten the draft or remove an image.");
            return Ok(());
        }
        send(&encoded)?;
        let input = view.draft_message(text);
        view.editor.take();
        view.history_index = None;
        view.draft_images.clear();
        self.submissions.insert(id, input);
        Ok(())
    }

    fn request_tree(&mut self, worker: &mut Companion) -> Result<()> {
        if self.tree_request.is_none() && !self.fork_pending {
            let id = Uuid::new_v4();
            worker.send(json!({"type":"get_entries","id":id}))?;
            self.tree_request = Some(id);
        }
        Ok(())
    }

    fn show_tree(&mut self, view: &mut View, args: &Cli, response: &Value, focus: Option<u64>) {
        let filter = TreeFilter::parse(args.settings.string("treeFilterMode").unwrap_or("default"));
        let result = if response["success"] == true {
            TerminalTree::from_rpc(&response["data"], filter)
        } else {
            Err(Error::Config(
                response["error"]
                    .as_str()
                    .unwrap_or("Session tree unavailable")
                    .into(),
            ))
        };
        match result {
            Ok(Some((picker, tree))) => {
                self.tree = Some(tree);
                if let (Some(focus), Some(old_picker)) = (focus, view.picker.as_mut()) {
                    old_picker.refresh_tree(picker, focus);
                } else {
                    view.picker = Some(picker);
                }
            }
            Ok(None) => view.append("No entries in session"),
            Err(error) => {
                if let Some(picker) = view.picker.as_mut() {
                    picker.show_feedback(Err(error.to_string()));
                } else {
                    view.append(&error.to_string());
                }
            }
        }
    }

    fn pick_tree(
        &mut self,
        view: &mut View,
        worker: &mut Companion,
        key: Option<KeyEvent>,
        keys: &Keys,
    ) -> Result<bool> {
        let Some(tree) = &self.tree else {
            return Ok(false);
        };
        let Some(mut picker) = view.picker.take() else {
            return Ok(false);
        };
        if self.fork_pending {
            view.picker = Some(picker);
            return Ok(true);
        }
        let input = match key {
            Some(key) => picker.input(key, keys),
            None => picker.activate_mouse_selection(keys),
        };
        match input {
            Input::Cancel => {
                self.tree = None;
                return Ok(true);
            }
            Input::Select(Selection::Checkout(record)) => {
                if let Some(row) = tree.row(record) {
                    if Some(row.id) == tree.leaf {
                        self.tree = None;
                        view.append("Already at this point");
                        return Ok(true);
                    } else if self.busy() {
                        picker
                            .show_feedback(Err("Wait for the current turn before forking".into()));
                    } else {
                        worker.send(json!({"type":"fork_at_entry","entryId":row.id}))?;
                        self.fork_pending = true;
                    }
                }
            }
            Input::CopyTreeRecord(record) => {
                if let Some(row) = tree.row(record) {
                    picker.show_feedback(
                        sessions::copy_text(&row.text)
                            .map(|()| "Copied entry".into())
                            .map_err(|error| error.to_string()),
                    );
                }
            }
            Input::SetTreeLabel(record, label) => {
                if let Some(row) = tree.row(record) {
                    let request = Uuid::new_v4();
                    worker.send(json!({
                        "type":"set_session_label","target":row.id,"label":label,"id":request
                    }))?;
                    picker.track_tree_label(request, record);
                }
            }
            _ => {}
        }
        view.picker = Some(picker);
        Ok(true)
    }
}

fn foreground_line(text: &str, color: &str) -> Vec<pi_rs::UiComponentSpan> {
    vec![pi_rs::UiComponentSpan {
        text: text.into(),
        color: Some(color.into()),
        ..Default::default()
    }]
}

fn foreground_transcript(data: &Value) -> Vec<Vec<pi_rs::UiComponentSpan>> {
    let mut lines = Vec::new();
    for message in data["messages"].as_array().into_iter().flatten() {
        let (label, color) = match message["role"].as_str() {
            Some("user" | "user_with_images") => ("User", "userMessageText"),
            Some("assistant") => ("Assistant", "text"),
            Some("tool") => ("Tool", "toolOutput"),
            _ => continue,
        };
        let Some(text) = message["content"].as_str().filter(|text| !text.is_empty()) else {
            continue;
        };
        lines.push(foreground_line(label, "muted"));
        lines.extend(text.lines().map(|line| foreground_line(line, color)));
    }
    for (pointer, label, color) in [
        ("/live/partial/text", "Assistant", "text"),
        ("/toolProgress/partial/text", "Tool", "toolOutput"),
    ] {
        if let Some(text) = data
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            lines.push(foreground_line(label, "muted"));
            lines.extend(text.lines().map(|line| foreground_line(line, color)));
        }
    }
    if lines.is_empty() {
        lines.push(foreground_line("Waiting for output", "muted"));
    }
    lines
}

fn entry_id(value: &Value) -> Option<Uuid> {
    Uuid::parse_str(value.as_str()?).ok()
}

fn restore_messages(view: &mut View, data: &Value) {
    if let Some(messages) = data["messages"].as_array() {
        view.graphics.reset();
        view.regular.committed = 0;
        view.regular.redraw = true;
        view.lines.clear();
        view.clear_markdown_renderings();
        append_session_reference(view);
        for message in messages {
            append_message(view, message);
        }
    }
}

struct TerminalTree {
    rows: Vec<TreeRow>,
    leaf: Option<Uuid>,
}

struct TreeRow {
    id: Uuid,
    text: String,
}

impl TerminalTree {
    fn row(&self, record: u64) -> Option<&TreeRow> {
        self.rows.get(usize::try_from(record.checked_sub(1)?).ok()?)
    }

    fn from_rpc(data: &Value, filter: TreeFilter) -> Result<Option<(Picker, Self)>> {
        use session_tree::{tree_description, tree_preview};

        let invalid = || Error::Config("Invalid Durable session history".into());
        let entries = data["entries"].as_array().ok_or_else(invalid)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let mut ids = BTreeMap::new();
        let mut rows = Vec::with_capacity(entries.len());
        let mut parents = BTreeMap::new();
        let mut labels = BTreeMap::new();
        let mut timestamps = BTreeMap::new();
        let mut descriptions = BTreeMap::new();
        let mut items = Vec::with_capacity(entries.len());
        for entry in entries {
            let id = entry_id(&entry["id"]).ok_or_else(invalid)?;
            let record = rows.len() as u64 + 1;
            let parent = entry_id(&entry["parentId"]).and_then(|id| ids.get(&id).copied());
            if ids.insert(id, record).is_some() {
                return Err(invalid());
            }
            let message = &entry["message"];
            let (kind, heading, role, text) = match entry["type"].as_str() {
                Some("message") => {
                    let (kind, heading, role) = match message["role"].as_str() {
                        Some("user") => (TreeEntryKind::User, "user: ", "accent"),
                        Some("assistant") => (TreeEntryKind::Message, "assistant: ", "success"),
                        Some("toolResult") => (TreeEntryKind::Tool, "[tool]: ", "muted"),
                        _ => (TreeEntryKind::Message, "[system]: ", "dim"),
                    };
                    (kind, heading.to_owned(), role, message_text(message))
                }
                Some("compaction") => (
                    TreeEntryKind::Message,
                    "[compaction]: ".into(),
                    "borderAccent",
                    entry["summary"].as_str().unwrap_or_default().to_owned(),
                ),
                Some("session_info") => (
                    TreeEntryKind::Bookkeeping,
                    "[title]: ".into(),
                    "dim",
                    entry["name"].as_str().unwrap_or_default().to_owned(),
                ),
                Some("label") => {
                    let value = entry["label"].as_str();
                    if let Some(target) =
                        entry_id(&entry["targetId"]).and_then(|id| ids.get(&id).copied())
                    {
                        if let Some(label) = value {
                            labels.insert(target, label.to_owned());
                            if let Some(timestamp) = entry["timestamp"].as_str() {
                                timestamps.insert(target, timestamp.to_owned());
                            }
                        } else {
                            labels.remove(&target);
                            timestamps.remove(&target);
                        }
                    }
                    (
                        TreeEntryKind::Bookkeeping,
                        "[label]: ".into(),
                        "dim",
                        value.unwrap_or_default().to_owned(),
                    )
                }
                Some("custom") => (
                    TreeEntryKind::Bookkeeping,
                    format!(
                        "[custom: {}]: ",
                        entry["customType"].as_str().unwrap_or_default()
                    ),
                    "dim",
                    entry.get("data").map(Value::to_string).unwrap_or_default(),
                ),
                _ => (
                    TreeEntryKind::Bookkeeping,
                    "[entry]".into(),
                    "dim",
                    String::new(),
                ),
            };
            let description = tree_description(heading, role, tree_preview(&text, 200));
            items.push(Item {
                label: format!("{id} {}{}", description.heading, description.body),
                selection: Selection::Checkout(record),
            });
            parents.insert(record, (parent, kind));
            descriptions.insert(record, description);
            rows.push(TreeRow { id, text });
        }
        let leaf = entry_id(&data["leafId"]);
        let head = leaf
            .and_then(|id| ids.get(&id).copied())
            .unwrap_or_default();
        let mut picker = Picker::tree(items, parents, labels, head, filter);
        picker.title = "Session tree (Enter to fork)".into();
        picker.set_tree_descriptions(descriptions);
        picker.set_label_timestamps(timestamps);
        Ok(Some((picker, Self { rows, leaf })))
    }
}

fn append_session_reference(view: &mut View) {
    if let Some(session) = &view.session_name {
        view.append(&format!("Durable session ID {session}"));
    }
}

fn message_text(message: &Value) -> String {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "text")
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn append_message(view: &mut View, message: &Value) {
    let text = message_text(message);
    match message["role"].as_str() {
        Some("user") => {
            view.remember_prompt(&text);
            view.append_entry(Entry::User(text.into()));
            let attachments = message_images(message);
            if !attachments.is_empty() {
                view.append(&format!("{} attached image(s)", attachments.len()));
                view.add_images(&attachments);
            }
        }
        Some("assistant") => {
            if !text.is_empty() {
                view.append_entry(Entry::Assistant(text.into()));
            }
            if let Some(invocations) = message["durableInvocations"].as_array() {
                for invocation in invocations {
                    show_tool(view, invocation, &Value::Null);
                }
            }
        }
        Some("toolResult") => {
            show_tool(
                view,
                &message["durableInvocation"],
                &message["durableOutcome"],
            );
        }
        _ => {}
    }
}

fn show_tool(view: &mut View, invocation: &Value, outcome: &Value) {
    let Ok(invocation) = serde_json::from_value::<pi_rs::Invocation>(invocation.clone()) else {
        return;
    };
    let operation = invocation.operation_id.to_string();
    if !view
        .lines
        .iter()
        .rev()
        .any(|row| matches!(row, Entry::Tool { id, .. } if id == &operation))
    {
        view.add_tool(&invocation, None);
    }
    if let Ok(outcome) = serde_json::from_value::<pi_rs::Outcome>(outcome.clone()) {
        view.finish_tool(&operation, &outcome, None);
    }
}

fn message_images(message: &Value) -> Vec<pi_rs::ImageContent> {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "image")
        .filter_map(|block| serde_json::from_value(block.clone()).ok())
        .collect()
}

fn restore_queue(view: &mut View, data: &Value) {
    let mut draft = view.draft_message("");
    draft.images.clear();
    let text = if let Some(inputs) = data["inputs"].as_array() {
        draft.images = inputs.iter().flat_map(message_images).collect();
        inputs.iter().map(message_text).collect::<Vec<_>>()
    } else {
        [&data["steering"], &data["followUp"]]
            .into_iter()
            .filter_map(Value::as_array)
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    };
    if !text.is_empty() {
        draft.input.content = text.join("\n\n");
        restore_draft(view, draft);
    }
}

fn restore_draft(view: &mut View, mut input: pi_rs::QueuedMessage) {
    let existing = view.editor.take();
    let content = [input.input.content, existing]
        .into_iter()
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    view.editor.replace(content);
    input.images.append(&mut view.draft_images);
    view.draft_images = input.images;
}

pub(crate) fn run(executable: &Path, options: &[OsString]) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(Error::Config(
            "the Durable terminal interface requires a terminal".into(),
        ));
    }
    let mut args = Cli::try_parse_from(["pi", "--no-session"])
        .map_err(|error| Error::Config(error.to_string()))?;
    args.workspace = std::env::current_dir()?;
    args.provider = Provider::Custom {
        id: "openai".into(),
        adapter: crate::providers::Adapter::OpenAiResponses,
    };
    args.settings = crate::config::Settings::load(&args.workspace, false)?;
    args.tui_mode = if args.settings.string("tuiMode") == Some("regular") {
        Some(TuiMode::Regular)
    } else {
        Some(TuiMode::Fullscreen)
    };
    let mut worker = Companion::spawn(executable, options)?;
    worker.send(json!({"type":"get_state"}))?;
    worker.send(json!({"type":"get_messages"}))?;
    let mut view = View::default();
    let mut conversation = Conversation::default();
    #[cfg(any(unix, windows))]
    let _pending_colors = crate::ui::theme::PendingTerminalColors::new();
    let mut theme = load_resolved_theme(
        args.theme_setting().unwrap_or("system"),
        &args.theme_paths,
        &args.workspace,
        false,
    )
    .unwrap_or_else(|_| Theme::system());
    let keys = Keys::load()?;
    let mut watcher = ThemeWatcher::new(&theme);
    let mut terminal = Terminal::enter(
        args.tui_mode,
        view.graphics.cleanup(),
        #[cfg(any(unix, windows))]
        None,
        #[cfg(any(unix, windows))]
        follows_terminal_appearance(args.theme_setting()),
    )?;
    let mut input = Reader::new()?;
    if view.graphics.enabled() {
        images::query_cell_size()?;
    }
    let mut dirty = true;
    let mut last_theme_check = Instant::now();
    loop {
        loop {
            match worker.events.try_recv() {
                Ok(event) => conversation.event(&mut view, &mut args, event?),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    let status = worker.child.wait()?;
                    return Err(Error::Config(format!(
                        "Durable companion exited ({status})"
                    )));
                }
            }
            dirty = true;
        }
        conversation.refresh_foreground_view(|request| worker.send(request))?;
        dirty |= view.poll_clipboard();
        if last_theme_check.elapsed() >= Duration::from_millis(500) {
            dirty |= watcher.poll(&mut theme)?;
            last_theme_check = Instant::now();
        }
        terminal.update_progress(args.settings.terminal_progress() && conversation.busy())?;
        if dirty {
            view.draw(&args, &theme, conversation.busy(), &keys)?;
        }
        dirty = true;
        match input.next(Duration::from_millis(32))? {
            Some(InputEvent::Event(Event::Key(key))) if key.kind != KeyEventKind::Release => {
                if conversation.foreground_input(&mut view, key, &keys)
                    || conversation.pick_foreground(&mut view, Some(key), &keys)
                {
                    continue;
                }
                if key.code == KeyCode::Esc && conversation.tree_request.take().is_some() {
                    continue;
                }
                if conversation.pick_tree(&mut view, &mut worker, Some(key), &keys)? {
                    continue;
                }
                if let Some(action) = keys.viewport_action(key) {
                    view.viewport.navigate(action);
                    view.scrollbar_activity = Some(Instant::now());
                    continue;
                }
                let action = keys
                    .action(
                        key,
                        view.editor.text.is_empty() && view.draft_images.is_empty(),
                    )
                    .unwrap_or("");
                if view.editor.jump_key(action, key)
                    || view.editor.action(action)
                    || view.editor_vertical_action(action)
                {
                    continue;
                }
                match action {
                    "app.exit" => return Ok(()),
                    "app.clear" => {
                        view.editor.take();
                        view.draft_images.clear();
                    }
                    "app.tools.expand" => view.set_expand_tools(!view.expand_tools),
                    "app.session.tree" => conversation.request_tree(&mut worker)?,
                    "app.clipboard.pasteImage" => view.paste_clipboard(&args),
                    "app.message.dequeue" => {
                        worker.send(json!({"type":"clear_queue","includeContent":true}))?
                    }
                    "app.interrupt" if conversation.busy() => {
                        worker.send(json!({"type":"clear_queue","includeContent":true}))?;
                        worker.send(json!({"type":"abort"}))?;
                    }
                    "tui.input.submit" if view.editor.continued_line() => {}
                    "tui.input.submit"
                        if view.editor.text.trim() == "/tree" && view.draft_images.is_empty() =>
                    {
                        view.editor.take();
                        conversation.request_tree(&mut worker)?;
                    }
                    "tui.input.submit"
                        if view.editor.text.trim() == "/agents" && view.draft_images.is_empty() =>
                    {
                        view.editor.take();
                        conversation.choose_foreground(&mut view);
                    }
                    "tui.input.submit" | "app.message.followUp" => {
                        conversation.submit(
                            &mut view,
                            action == "app.message.followUp",
                            |request| worker.send_json(request),
                        )?;
                    }
                    _ => {
                        if let KeyCode::Char(character) = key.code
                            && !key.modifiers.intersects(
                                KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                            )
                        {
                            view.editor.type_character(character);
                        }
                    }
                }
            }
            Some(InputEvent::Event(Event::Mouse(mouse)))
                if args.tui_mode != Some(TuiMode::Regular) && view.picker.is_some() =>
            {
                let picker = view.picker.as_mut().expect("checked above");
                if usize::from(mouse.column) < view.fullscreen_width
                    && let Some(row) =
                        usize::from(mouse.row).checked_sub(view.fullscreen_header_rows)
                {
                    if matches!(picker.mouse(mouse.kind, row), MouseInput::Activate)
                        && !conversation.pick_foreground(&mut view, None, &keys)
                    {
                        conversation.pick_tree(&mut view, &mut worker, None, &keys)?;
                    }
                } else {
                    picker.clear_mouse_press();
                }
            }
            Some(InputEvent::Event(Event::Paste(text))) => {
                if conversation.foreground_viewer.is_some() {
                    continue;
                }
                if let Some(picker) = &mut view.picker {
                    picker.paste(&text);
                } else {
                    view.paste_editor_text(&text);
                }
            }
            Some(InputEvent::Event(Event::Resize(_, _))) => (),
            #[cfg(any(unix, windows))]
            Some(InputEvent::CellSize { width, height }) => {
                images::set_cell_dimensions(width, height);
            }
            #[cfg(unix)]
            Some(InputEvent::KeyboardEnhancementFlags(_)) => {
                terminal.keyboard_reply(true)?;
            }
            #[cfg(any(unix, windows))]
            Some(InputEvent::PrimaryDeviceAttributes) => {
                finish_initial_terminal_colors(&mut theme);
                #[cfg(unix)]
                terminal.keyboard_reply(false)?;
            }
            #[cfg(any(unix, windows))]
            Some(InputEvent::Appearance { dark }) if terminal.appearance_notifications => {
                let (changed, _) = handle_terminal_appearance(
                    &mut terminal.colors,
                    &mut theme,
                    dark,
                    &mut std::io::stdout(),
                )?;
                if changed {
                    refresh_terminal_theme_selection(&args, &mut view, &mut theme);
                }
            }
            #[cfg(any(unix, windows))]
            Some(InputEvent::TerminalColor {
                foreground,
                red,
                green,
                blue,
            }) => {
                if let Some(snapshot) = terminal.colors.record_color(
                    if foreground { 16 } else { 17 },
                    [red, green, blue],
                    &mut std::io::stdout(),
                )? {
                    let (changed, _) = apply_terminal_color_snapshot(snapshot, &mut theme);
                    if changed {
                        refresh_terminal_theme_selection(&args, &mut view, &mut theme);
                    }
                }
                settle_complete_colors(&terminal.colors, &mut theme);
            }
            _ => dirty = false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_overlay_follows_the_child_and_preserves_the_parent_draft() {
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        let keys = Keys::default();
        let mut view = View::default();
        let mut conversation = Conversation::default();
        let child = Uuid::from_u128(1);
        view.editor.replace("keep this draft".into());
        conversation.event(
            &mut view,
            &mut args,
            json!({"type":"durable_foreground_subagents", "conversationIds":[child]}),
        );
        conversation.choose_foreground(&mut view);
        let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
        assert!(conversation.pick_foreground(&mut view, Some(key(KeyCode::Down)), &keys));
        assert!(conversation.pick_foreground(&mut view, Some(key(KeyCode::Enter)), &keys));
        let mut request = None;
        conversation
            .refresh_foreground_view(|frame| {
                request = Some(frame);
                Ok(())
            })
            .unwrap();
        let request = request.unwrap();
        assert_eq!(request["conversationId"], json!(child));
        assert_eq!(request["type"], "get_foreground_subagent");
        conversation
            .refresh_foreground_view(|_| panic!("request is still pending"))
            .unwrap();
        let response = |id, text| {
            json!({
                "type":"response","command":"get_foreground_subagent","success":true,"id":id,
                "data":{"conversationId":child,"messages":[{"role":"user","content":"child prompt"}],
                "live":{"state":"running","partial":{"text":text}}}
            })
        };
        conversation.event(&mut view, &mut args, response(json!(Uuid::nil()), "stale"));
        conversation.event(
            &mut view,
            &mut args,
            response(request["id"].clone(), "live output"),
        );
        let drawn = view
            .dialog()
            .unwrap()
            .render(&Theme::system(), 100, 16, &keys)
            .into_iter()
            .flat_map(|line| line.spans)
            .map(|span| span.text)
            .collect::<String>();
        assert!(drawn.contains("child prompt"));
        assert!(drawn.contains("live output"));
        assert!(!drawn.contains("stale"));

        conversation.event(&mut view, &mut args, json!({
            "type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"parent answer"}]}
        }));
        conversation.event(&mut view, &mut args, json!({"type":"agent_end"}));
        conversation
            .refresh_foreground_view(|_| panic!("parent tool has finished"))
            .unwrap();
        assert!(conversation.foreground_input(&mut view, key(KeyCode::Esc), &keys));
        assert!(view.dialog().is_none());
        assert_eq!(view.editor.text, "keep this draft");
        assert!(matches!(view.lines.last(), Some(Entry::Assistant(_))));
    }

    #[test]
    fn durable_tree_projects_stable_entries_into_the_native_picker() {
        let first = Uuid::from_u128(1);
        let last = Uuid::from_u128(2);
        let response = json!({
            "leafId":last,
            "entries":[
                {"id":first,"parentId":null,"type":"message","message":{
                    "role":"user","content":[{"type":"text","text":"Earlier prompt"}]
                }},
                {"id":last,"parentId":first,"type":"label","targetId":first,"label":"bookmark"}
            ]
        });
        let (picker, tree) = TerminalTree::from_rpc(&response, TreeFilter::LabeledOnly)
            .unwrap()
            .unwrap();
        assert_eq!(tree.leaf, Some(last));
        assert_eq!(tree.row(1).unwrap().id, first);
        assert_eq!(tree.row(1).unwrap().text, "Earlier prompt");
        assert!(matches!(picker.selected(), Some(Selection::Checkout(1))));
    }

    #[test]
    fn oversized_prompt_preserves_the_entire_draft_and_never_writes_to_rpc() {
        let mut view = View::default();
        let mut conversation = Conversation::default();
        // JSON escaping doubles these bytes even though the editor text fits.
        let text = "\\".repeat(MAX_RPC_REQUEST_BYTES / 2);
        let image = pi_rs::ImageContent {
            data: "YQ==".into(),
            mime_type: "image/png".into(),
        };
        view.editor.replace(text.clone());
        view.editor.cursor = 2;
        view.draft_images.push(image.clone());
        conversation
            .submit(&mut view, false, |_| panic!("oversized prompt was sent"))
            .unwrap();
        assert_eq!(view.editor.text, text);
        assert_eq!(view.editor.cursor, 2);
        assert_eq!(view.draft_images, [image]);
        assert!(conversation.submissions.is_empty());
        assert!(matches!(&view.lines[..], [Entry::Notice(text)] if text.contains("4 MiB")));

        view.editor.replace("shortened".into());
        conversation
            .submit(&mut view, false, |request| {
                let request: Value = serde_json::from_slice(request)?;
                assert_eq!(request["message"], "shortened");
                assert_eq!(request["images"][0]["data"], "YQ==");
                Ok(())
            })
            .unwrap();
        assert!(view.editor.text.is_empty());
        assert!(view.draft_images.is_empty());
        assert_eq!(conversation.submissions.len(), 1);
    }

    #[test]
    fn prompt_limit_counts_the_wire_newline() {
        let mut view = View::default();
        let prefix_bytes = serde_json::to_vec(
            &json!({"type":"prompt", "id":Uuid::nil(), "message":"", "images":[]}),
        )
        .unwrap()
        .len();
        let mut conversation = Conversation::default();
        view.editor
            .replace("a".repeat(MAX_RPC_REQUEST_BYTES - prefix_bytes));
        conversation
            .submit(&mut view, false, |_| panic!("oversized prompt was sent"))
            .unwrap();
        assert!(conversation.submissions.is_empty());

        view.editor
            .replace("a".repeat(MAX_RPC_REQUEST_BYTES - prefix_bytes - 1));
        conversation
            .submit(&mut view, false, |request| {
                assert_eq!(request.len() + 1, MAX_RPC_REQUEST_BYTES);
                Ok(())
            })
            .unwrap();
        assert_eq!(conversation.submissions.len(), 1);
    }

    #[test]
    fn durable_session_id_survives_history_loading_with_the_header_hidden() {
        let session = Uuid::new_v4().to_string();
        let settings_dir = tempfile::tempdir().unwrap();
        let path = settings_dir.path().join("settings.json");
        std::fs::write(&path, r#"{"quietStartup":true}"#).unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.tui_mode = Some(TuiMode::Regular);
        args.settings =
            crate::config::Settings::load_from_paths(settings_dir.path(), false, &path).unwrap();
        assert!(!show_startup_header(&args));

        let mut conversation = Conversation::default();
        let mut view = View::default();
        let state = json!({
            "type":"response","command":"get_state","success":true,
            "data":{"sessionId":session}
        });
        conversation.event(&mut view, &mut args, state.clone());
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"response","command":"get_messages","success":true,"data":{"messages":[
                    {"role":"user","content":[{"type":"text","text":"saved"}]}
                ]}
            }),
        );
        conversation.event(&mut view, &mut args, state);
        assert!(matches!(
            &view.lines[..],
            [Entry::Notice(text), Entry::User(_)] if text == &format!("Durable session ID {session}")
        ));
    }

    #[test]
    fn durable_fork_replaces_the_session_reference_and_redraws_regular_history() {
        let original = Uuid::from_u128(1).to_string();
        let fork = Uuid::from_u128(2).to_string();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.tui_mode = Some(TuiMode::Regular);
        let mut view = View::default();
        let mut conversation = Conversation::default();
        view.session_name = Some(original.clone());
        append_session_reference(&mut view);
        view.regular.committed = 1;
        view.regular.redraw = false;
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"response","command":"fork_at_entry","success":true,
                "data":{"sessionId":fork,"messages":[]}
            }),
        );
        assert_eq!(view.session_name.as_deref(), Some(fork.as_str()));
        assert_eq!(view.regular.committed, 0);
        assert!(view.regular.redraw);
        assert!(matches!(
            &view.lines[..],
            [Entry::Notice(reference), Entry::Notice(_)]
                if reference == &format!("Durable session ID {fork}")
        ));
    }

    #[test]
    fn durable_tool_cards_use_the_native_result_renderer_without_duplicates() {
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        let mut view = View::default();
        let mut conversation = Conversation::default();
        let invocation = pi_rs::Invocation {
            operation_id: Uuid::new_v4(),
            call: pi_rs::ToolCall {
                id: "call".into(),
                name: "edit".into(),
                arguments: r#"{"path":"file"}"#.into(),
            },
        };
        let outcome = pi_rs::Outcome::StructuredWithDetails {
            data: json!({"content":[{"type":"text","text":"edited"}], "isError":false}),
            attachments: Vec::new(),
            details: json!({"diff":"- old\n+ new"}),
        };
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_end",
                "message":{"role":"assistant","content":[], "durableInvocations":[invocation]}
            }),
        );
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"tool_execution_start","durableInvocation":invocation
            }),
        );
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"tool_execution_update","toolCallId":"call","toolName":"edit",
                "partialResult":{"content":[{"type":"text","text":"working"}]}
            }),
        );
        assert!(
            matches!(&view.lines[..], [Entry::Tool {output, finished: false, ..}] if output == "working")
        );
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"tool_execution_end","durableInvocation":invocation,"durableOutcome":outcome
            }),
        );
        assert!(
            matches!(&view.lines[..], [Entry::Tool {output, numbered_diff: true, finished: true, ..}] if output == "- old\n+ new")
        );
    }

    #[test]
    fn durable_messages_share_the_native_transcript_and_restore_rejected_input() {
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        let mut view = View::default();
        let mut conversation = Conversation::default();
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_start",
                "message":{"role":"user","content":[{"type":"text","text":"first"}]}
            }),
        );
        assert!(matches!(&view.lines[..], [Entry::User(_)]));
        view.editor_width = 80;
        assert!(view.editor_vertical_action("tui.editor.cursorUp"));
        assert_eq!(view.editor.text, "first");
        assert!(view.editor_vertical_action("tui.editor.cursorDown"));
        assert!(view.editor.text.is_empty());

        view.editor.replace("first line\nsecond line".into());
        let last_row_cursor = view.editor.cursor;
        assert!(view.editor_vertical_action("tui.editor.cursorUp"));
        assert!(view.editor.cursor < "first line\n".len());
        assert!(view.editor_vertical_action("tui.editor.cursorDown"));
        assert_eq!(view.editor.cursor, last_row_cursor);
        view.editor.take();
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_update",
                "message":{"role":"assistant","content":[{"type":"text","text":"live"}]}
            }),
        );
        assert_eq!(view.streaming, "live");
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_end",
                "message":{"role":"assistant","content":[{"type":"text","text":"final"}]}
            }),
        );
        assert!(view.streaming.is_empty());
        assert!(matches!(
            &view.lines[..],
            [Entry::User(_), Entry::Assistant(_)]
        ));
        let id = Uuid::new_v4();
        conversation
            .submissions
            .insert(id, view.draft_message("rejected"));
        view.editor.replace("later".into());
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"response","command":"prompt","id":id,"success":false,"error":"unavailable"
            }),
        );
        assert_eq!(view.editor.take(), "rejected\n\nlater");
        assert!(conversation.submissions.is_empty());
        assert!(matches!(view.lines.last(), Some(Entry::Notice(text)) if text == "unavailable"));
    }
}
