//! Pi action identifiers and user keybinding overrides.
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use pi_rs::{Error, Result};
use serde_json::Value;
use std::collections::BTreeMap;

pub fn windows_keybindings() -> bool {
    cfg!(windows)
        || (cfg!(target_os = "linux")
            && (std::env::var_os("WSL_DISTRO_NAME").is_some()
                || std::env::var_os("WSL_INTEROP").is_some()))
}

pub struct Keys {
    bindings: BTreeMap<String, Vec<KeyEvent>>,
}
impl Keys {
    fn defaults() -> Result<BTreeMap<String, Vec<KeyEvent>>> {
        Self::defaults_for(cfg!(windows), windows_keybindings())
    }

    fn defaults_for(windows: bool, windows_keys: bool) -> Result<BTreeMap<String, Vec<KeyEvent>>> {
        let defaults: &[(&str, &[&str])] = &[
            ("tui.editor.cursorUp", &["up"]),
            ("tui.editor.cursorDown", &["down"]),
            ("tui.editor.cursorLeft", &["left", "ctrl+b"]),
            ("tui.editor.cursorRight", &["right", "ctrl+f"]),
            (
                "tui.editor.cursorWordLeft",
                &["alt+left", "ctrl+left", "alt+b"],
            ),
            (
                "tui.editor.cursorWordRight",
                &["alt+right", "ctrl+right", "alt+f"],
            ),
            ("tui.editor.cursorLineStart", &["home", "ctrl+a"]),
            ("tui.editor.cursorLineEnd", &["end", "ctrl+e"]),
            ("tui.editor.jumpForward", &["ctrl+]"]),
            ("tui.editor.jumpBackward", &["ctrl+alt+]"]),
            ("tui.editor.pageUp", &["pageup", "ctrl+pageup"]),
            ("tui.editor.pageDown", &["pagedown", "ctrl+pagedown"]),
            ("tui.editor.deleteCharBackward", &["backspace"]),
            ("tui.editor.deleteCharForward", &["delete", "ctrl+d"]),
            (
                "tui.editor.deleteWordBackward",
                &["ctrl+w", "alt+backspace"],
            ),
            ("tui.editor.deleteWordForward", &["alt+d", "alt+delete"]),
            ("tui.editor.deleteToLineStart", &["ctrl+u"]),
            ("tui.editor.deleteToLineEnd", &["ctrl+k"]),
            ("tui.editor.yank", &["ctrl+y"]),
            ("tui.editor.yankPop", &["alt+y"]),
            (
                "tui.editor.undo",
                if windows {
                    &["ctrl+z"]
                } else if windows_keys {
                    &["alt+z"]
                } else {
                    &["ctrl+-"]
                },
            ),
            ("tui.input.newLine", &["shift+enter", "ctrl+j"]),
            ("tui.input.submit", &["enter"]),
            ("tui.input.tab", &["tab"]),
            ("tui.input.copy", &["ctrl+c"]),
            ("tui.select.up", &["up"]),
            ("tui.select.down", &["down"]),
            ("tui.select.pageUp", &["pageup"]),
            ("tui.select.pageDown", &["pagedown"]),
            ("tui.select.confirm", &["enter"]),
            ("tui.select.cancel", &["escape", "ctrl+c"]),
            ("tui.altScreen.pageUp", &["pageup"]),
            ("tui.altScreen.pageDown", &["pagedown"]),
            ("tui.altScreen.halfPageUp", &[]),
            ("tui.altScreen.halfPageDown", &[]),
            ("tui.altScreen.lineUp", &[]),
            ("tui.altScreen.lineDown", &[]),
            (
                "tui.altScreen.previousPrompt",
                if windows_keys {
                    &["ctrl+up"]
                } else {
                    &["ctrl+shift+up", "ctrl+up"]
                },
            ),
            (
                "tui.altScreen.nextPrompt",
                if windows_keys {
                    &["ctrl+down"]
                } else {
                    &["ctrl+shift+down", "ctrl+down"]
                },
            ),
            (
                "tui.altScreen.search",
                if windows_keys {
                    &["ctrl+f"]
                } else {
                    &["ctrl+shift+f"]
                },
            ),
            ("tui.altScreen.searchNext", &["enter", "ctrl+g"]),
            (
                "tui.altScreen.searchPrevious",
                &["shift+enter", "ctrl+shift+g"],
            ),
            ("tui.altScreen.searchClose", &["escape"]),
            ("tui.altScreen.top", &["ctrl+home"]),
            ("tui.altScreen.bottom", &["ctrl+end"]),
            ("app.interrupt", &["escape"]),
            ("app.suspend", if windows { &[] } else { &["ctrl+z"] }),
            ("app.clear", &["ctrl+c"]),
            ("app.exit", &["ctrl+d"]),
            ("app.editor.external", &["ctrl+g"]),
            ("app.tools.expand", &["ctrl+o"]),
            ("app.thinking.toggle", &["ctrl+t"]),
            ("app.thinking.cycle", &["shift+tab"]),
            ("app.thinking.save", &["ctrl+s"]),
            (
                "app.message.followUp",
                if windows_keys {
                    &["ctrl+q"]
                } else {
                    &["alt+enter"]
                },
            ),
            (
                "app.message.dequeue",
                if windows_keys {
                    &["alt+q"]
                } else {
                    &["alt+up"]
                },
            ),
            ("app.model.select", &["ctrl+l"]),
            ("app.model.cycleForward", &["ctrl+p"]),
            (
                "app.model.cycleBackward",
                if windows_keys {
                    &["alt+p"]
                } else {
                    &["ctrl+shift+p"]
                },
            ),
            ("app.message.copy", &["ctrl+x"]),
            (
                "app.clipboard.pasteImage",
                if windows_keys {
                    &["alt+v"]
                } else {
                    &["ctrl+v"]
                },
            ),
            ("app.tree.foldOrUp", &["alt+left", "ctrl+left"]),
            ("app.tree.unfoldOrDown", &["alt+right", "ctrl+right"]),
            ("app.tree.editLabel", &["shift+l"]),
            ("app.tree.toggleLabelTimestamp", &["shift+t"]),
            ("app.tree.filter.default", &["ctrl+d"]),
            ("app.tree.filter.noTools", &["ctrl+t"]),
            ("app.tree.filter.userOnly", &["ctrl+u"]),
            ("app.tree.filter.labeledOnly", &["ctrl+l"]),
            ("app.tree.filter.all", &["ctrl+a"]),
            ("app.tree.filter.cycleForward", &["ctrl+o"]),
            ("app.tree.filter.cycleBackward", &["ctrl+shift+o"]),
            ("app.models.save", &["ctrl+s"]),
            ("app.models.enableAll", &["ctrl+a"]),
            ("app.models.clearAll", &["ctrl+x"]),
            ("app.models.toggleProvider", &["ctrl+p"]),
            ("app.models.reorderUp", &["alt+up"]),
            ("app.models.reorderDown", &["alt+down"]),
            ("app.session.rename", &["ctrl+r"]),
            ("app.session.delete", &["ctrl+d"]),
            ("app.session.deleteNoninvasive", &["ctrl+backspace"]),
            ("app.session.toggleSort", &["ctrl+s"]),
            ("app.session.toggleNamedFilter", &["ctrl+n"]),
            ("app.session.togglePath", &["ctrl+p"]),
        ];
        let mut bindings = BTreeMap::new();
        for (action, keys) in defaults {
            bindings.insert(
                (*action).into(),
                keys.iter()
                    .map(|key| parse(key))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(bindings)
    }
    pub fn load() -> Result<Self> {
        let mut bindings = Self::defaults()?;
        let custom =
            crate::config::read_object(&crate::config::agent_dir().join("keybindings.json"))?;
        for (action, value) in custom.as_object().expect("object") {
            if action == "$schema" {
                continue;
            }
            let values = match value {
                Value::String(key) => vec![key.as_str()],
                Value::Array(keys) => keys
                    .iter()
                    .map(|v| {
                        v.as_str().ok_or_else(|| {
                            Error::Config(format!("keybinding {action} must contain strings"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                _ => {
                    return Err(Error::Config(format!(
                        "keybinding {action} must be a string or array"
                    )));
                }
            };
            bindings.insert(
                action.clone(),
                values.into_iter().map(parse).collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(Self { bindings })
    }
    pub fn matches(&self, action: &str, key: KeyEvent) -> bool {
        let key = normalize(key);
        self.bindings.get(action).is_some_and(|keys| {
            keys.iter()
                .any(|binding| binding.code == key.code && binding.modifiers == key.modifiers)
        })
    }
    pub fn action(&self, key: KeyEvent, empty: bool) -> Option<&str> {
        let key = normalize(key);
        // Exit shares Ctrl+D with forward delete, and applies only to an empty editor.
        self.bindings
            .iter()
            .find(|(action, keys)| {
                if action.starts_with("app.tree.")
                    || action.starts_with("app.models.")
                    || action.starts_with("tui.select.")
                    || action.starts_with("tui.altScreen.")
                    || matches!(
                        action.as_str(),
                        "tui.input.copy"
                            | "app.thinking.save"
                            | "app.session.rename"
                            | "app.session.delete"
                            | "app.session.deleteNoninvasive"
                            | "app.session.toggleSort"
                            | "app.session.toggleNamedFilter"
                            | "app.session.togglePath"
                    )
                {
                    return false;
                }
                if (action.as_str() == "app.exit") != empty
                    && matches!(action.as_str(), "app.exit" | "tui.editor.deleteCharForward")
                {
                    return false;
                }
                keys.iter()
                    .any(|binding| binding.code == key.code && binding.modifiers == key.modifiers)
            })
            .map(|(action, _)| action.as_str())
    }
    pub fn viewport_action(&self, key: KeyEvent) -> Option<&'static str> {
        [
            "tui.altScreen.pageUp",
            "tui.altScreen.pageDown",
            "tui.altScreen.halfPageUp",
            "tui.altScreen.halfPageDown",
            "tui.altScreen.lineUp",
            "tui.altScreen.lineDown",
            "tui.altScreen.previousPrompt",
            "tui.altScreen.nextPrompt",
            "tui.altScreen.top",
            "tui.altScreen.bottom",
        ]
        .into_iter()
        .find(|action| self.matches(action, key))
    }

    pub fn first_binding(&self, action: &str) -> Option<KeyEvent> {
        self.bindings
            .get(action)
            .and_then(|keys| keys.first())
            .copied()
    }

    pub fn hint(&self, action: &str) -> String {
        binding_hint(self.first_binding(action))
    }
    pub fn describe(&self) -> String {
        let sections: &[(&str, &[(&str, &str)])] = &[
            (
                "Navigation",
                &[
                    ("tui.editor.cursorUp", "Move up / browse previous history"),
                    ("tui.editor.cursorDown", "Move down / browse next history"),
                    ("tui.editor.cursorLeft", "Move left"),
                    ("tui.editor.cursorRight", "Move right"),
                    ("tui.editor.cursorWordLeft", "Move back a word"),
                    ("tui.editor.cursorWordRight", "Move forward a word"),
                    ("tui.editor.cursorLineStart", "Start of line"),
                    ("tui.editor.cursorLineEnd", "End of line"),
                    ("tui.editor.jumpForward", "Jump forward to character"),
                    ("tui.editor.jumpBackward", "Jump backward to character"),
                    ("tui.editor.pageUp", "Move editor up a page"),
                    ("tui.editor.pageDown", "Move editor down a page"),
                    ("tui.editor.historyPrevious", "Previous history entry"),
                    ("tui.editor.historyNext", "Next history entry"),
                ],
            ),
            (
                "Editing",
                &[
                    ("tui.input.submit", "Send message"),
                    ("tui.input.newLine", "New line"),
                    ("tui.editor.deleteCharBackward", "Delete character backward"),
                    ("tui.editor.deleteCharForward", "Delete character forward"),
                    ("tui.editor.deleteWordBackward", "Delete word backward"),
                    ("tui.editor.deleteWordForward", "Delete word forward"),
                    ("tui.editor.deleteToLineStart", "Delete to start of line"),
                    ("tui.editor.deleteToLineEnd", "Delete to end of line"),
                    ("tui.editor.yank", "Paste last deleted text"),
                    ("tui.editor.yankPop", "Cycle deleted text"),
                    ("tui.editor.undo", "Undo"),
                    ("tui.input.tab", "Complete path or accept autocomplete"),
                ],
            ),
            (
                "Application",
                &[
                    ("app.interrupt", "Cancel or abort"),
                    ("app.clear", "Clear editor"),
                    ("app.exit", "Exit when editor is empty"),
                    ("app.suspend", "Suspend to background"),
                    ("app.thinking.cycle", "Cycle thinking level"),
                    ("app.thinking.save", "Save thinking level"),
                    ("app.model.cycleForward", "Cycle to next model"),
                    ("app.model.cycleBackward", "Cycle to previous model"),
                    ("app.model.select", "Open model selector"),
                    ("app.tools.expand", "Toggle tool output"),
                    ("app.thinking.toggle", "Toggle thinking blocks"),
                    ("app.editor.external", "Open external editor"),
                    (
                        "app.message.copy",
                        "Copy selection or last assistant message",
                    ),
                    ("app.message.followUp", "Queue follow-up message"),
                    ("app.message.dequeue", "Restore queued messages"),
                    ("app.clipboard.pasteImage", "Paste files, images, or text"),
                    ("app.session.new", "Start a new session"),
                    ("app.session.tree", "Open session tree"),
                    ("app.session.fork", "Fork current session"),
                    ("app.session.resume", "Resume a session"),
                ],
            ),
            (
                "Fullscreen",
                &[
                    ("tui.altScreen.pageUp", "Scroll up a page"),
                    ("tui.altScreen.pageDown", "Scroll down a page"),
                    ("tui.altScreen.previousPrompt", "Previous prompt"),
                    ("tui.altScreen.nextPrompt", "Next prompt"),
                    ("tui.altScreen.search", "Search transcript"),
                    ("tui.altScreen.top", "Scroll to top"),
                    ("tui.altScreen.bottom", "Scroll to bottom"),
                ],
            ),
        ];
        let mut lines = vec!["Keyboard Shortcuts".to_owned()];
        let mut included = std::collections::BTreeSet::new();
        for (heading, actions) in sections {
            lines.push(format!("\n{heading}"));
            for (action, description) in *actions {
                included.insert(*action);
                let keys = self.bindings.get(*action).map_or_else(
                    || "—".to_owned(),
                    |keys| {
                        if keys.is_empty() {
                            "—".to_owned()
                        } else {
                            keys.iter()
                                .map(|key| binding_hint(Some(*key)))
                                .collect::<Vec<_>>()
                                .join(", ")
                        }
                    },
                );
                lines.push(format!("{keys}  {description}"));
            }
        }
        let other: Vec<_> = self
            .bindings
            .iter()
            .filter(|(name, _)| !included.contains(name.as_str()))
            .collect();
        if !other.is_empty() {
            lines.push("\nOther bindings".into());
            for (name, keys) in other {
                let keys = if keys.is_empty() {
                    "—".to_owned()
                } else {
                    keys.iter()
                        .map(|key| binding_hint(Some(*key)))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                lines.push(format!("{keys}  {name}"));
            }
        }
        lines.join("\n")
    }
}
pub(super) fn binding_hint(key: Option<KeyEvent>) -> String {
    let Some(key) = key else {
        return "—".into();
    };
    let mut label = String::new();
    for (modifier, prefix) in [
        (KeyModifiers::CONTROL, "Ctrl+"),
        (KeyModifiers::ALT, "Alt+"),
        (KeyModifiers::SHIFT, "Shift+"),
        (KeyModifiers::SUPER, "Super+"),
    ] {
        if key.modifiers.contains(modifier) {
            label.push_str(prefix);
        }
    }
    match key.code {
        KeyCode::Char(' ') => label.push_str("Space"),
        KeyCode::Char(character) => label.push(character.to_ascii_uppercase()),
        KeyCode::Up => label.push('↑'),
        KeyCode::Down => label.push('↓'),
        KeyCode::Left => label.push('←'),
        KeyCode::Right => label.push('→'),
        code => label.push_str(&format!("{code:?}")),
    }
    label
}

pub(super) fn normalize(mut key: KeyEvent) -> KeyEvent {
    if key.code == KeyCode::BackTab {
        key.code = KeyCode::Tab;
        key.modifiers |= KeyModifiers::SHIFT;
    }
    if let KeyCode::Char(character) = key.code {
        key.code = KeyCode::Char(character.to_ascii_lowercase());
    }
    key
}

pub(super) fn parse(text: &str) -> Result<KeyEvent> {
    let lower = text.to_lowercase();
    let mut rest = lower.as_str();
    let mut modifiers = KeyModifiers::NONE;
    while let Some((prefix, suffix)) = rest.split_once('+') {
        let modifier = match prefix {
            "ctrl" => KeyModifiers::CONTROL,
            "alt" => KeyModifiers::ALT,
            "shift" => KeyModifiers::SHIFT,
            "super" => KeyModifiers::SUPER,
            _ => break,
        };
        modifiers |= modifier;
        rest = suffix;
    }
    let code = match rest {
        "escape" | "esc" => KeyCode::Esc,
        "enter" | "return" => KeyCode::Enter,
        "tab" => KeyCode::Tab,
        "space" => KeyCode::Char(' '),
        "backspace" => KeyCode::Backspace,
        "delete" => KeyCode::Delete,
        "insert" => KeyCode::Insert,
        "home" => KeyCode::Home,
        "end" => KeyCode::End,
        "clear" => KeyCode::Null,
        "pageup" => KeyCode::PageUp,
        "pagedown" => KeyCode::PageDown,
        "up" => KeyCode::Up,
        "down" => KeyCode::Down,
        "left" => KeyCode::Left,
        "right" => KeyCode::Right,
        _ if rest.starts_with('f') && rest.len() > 1 => KeyCode::F(
            rest[1..]
                .parse::<u8>()
                .ok()
                .filter(|n| (1..=12).contains(n))
                .ok_or_else(|| Error::Config(format!("invalid key {text:?}")))?,
        ),
        _ if rest.chars().count() == 1 => KeyCode::Char(rest.chars().next().unwrap()),
        _ => return Err(Error::Config(format!("invalid key {text:?}"))),
    };
    Ok(KeyEvent::new(code, modifiers))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_combined_modifiers_and_literal_plus() {
        assert_eq!(
            parse("alt+ctrl+x").unwrap(),
            KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::ALT | KeyModifiers::CONTROL
            )
        );
        assert_eq!(parse("ctrl++").unwrap().code, KeyCode::Char('+'));
        assert!(parse("f99").is_err());
    }

    #[test]
    fn platform_defaults_match_windows_wsl_and_other_terminals() {
        for (windows, windows_keys, undo, search, suspend) in [
            (true, true, "ctrl+z", "ctrl+f", None),
            (false, true, "alt+z", "ctrl+f", Some("ctrl+z")),
            (false, false, "ctrl+-", "ctrl+shift+f", Some("ctrl+z")),
        ] {
            let keys = Keys {
                bindings: Keys::defaults_for(windows, windows_keys).unwrap(),
            };
            assert!(keys.matches("tui.editor.undo", parse(undo).unwrap()));
            assert!(keys.matches("tui.altScreen.search", parse(search).unwrap()));
            assert_eq!(
                keys.first_binding("app.suspend"),
                suspend.map(|key| parse(key).unwrap())
            );
            assert_eq!(
                keys.bindings["tui.altScreen.previousPrompt"].len(),
                if windows_keys { 1 } else { 2 }
            );
            assert_eq!(
                keys.bindings["tui.altScreen.nextPrompt"].len(),
                if windows_keys { 1 } else { 2 }
            );
        }
    }

    #[test]
    fn hotkeys_show_readable_effective_bindings_and_unbound_actions() {
        let mut keys = Keys::default();
        keys.bindings.insert(
            "app.model.select".into(),
            vec![parse("alt+m").unwrap(), parse("ctrl+l").unwrap()],
        );
        keys.bindings.insert("app.message.copy".into(), Vec::new());
        let text = keys.describe();
        assert!(text.contains("Alt+M, Ctrl+L  Open model selector"));
        assert!(text.contains("—  Copy selection or last assistant message"));
        assert!(text.contains("Shift+Enter, Ctrl+J  New line"));
        assert!(text.contains("Ctrl+T  app.tree.filter.noTools"));
        assert!(!text.contains("KeyEvent"));
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    #[test]
    fn session_bindings_remain_local_to_the_picker() {
        let keys = Keys {
            bindings: [
                ("app.session.delete".into(), vec![parse("ctrl+d").unwrap()]),
                ("app.exit".into(), vec![parse("ctrl+d").unwrap()]),
                (
                    "tui.editor.deleteCharForward".into(),
                    vec![parse("ctrl+d").unwrap()],
                ),
            ]
            .into(),
        };
        let key = parse("ctrl+d").unwrap();
        assert!(keys.matches("app.session.delete", key));
        assert_eq!(keys.action(key, true), Some("app.exit"));
        assert_eq!(
            keys.action(key, false),
            Some("tui.editor.deleteCharForward")
        );
    }
}

#[cfg(test)]
impl Default for Keys {
    fn default() -> Self {
        Self {
            bindings: Self::defaults().expect("valid built-in keybindings"),
        }
    }
}
