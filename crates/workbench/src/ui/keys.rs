//! Pi action identifiers and user keybinding overrides.
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use pi_rs::{Error, Result};
use serde_json::Value;
use std::collections::BTreeMap;

pub struct Keys {
    bindings: BTreeMap<String, Vec<KeyEvent>>,
}
impl Keys {
    pub fn load() -> Result<Self> {
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
            ("tui.editor.undo", &["ctrl+-"]),
            ("tui.input.newLine", &["shift+enter", "ctrl+j"]),
            ("tui.input.submit", &["enter"]),
            ("tui.input.tab", &["tab"]),
            ("tui.altScreen.pageUp", &["pageup"]),
            ("tui.altScreen.pageDown", &["pagedown"]),
            ("tui.altScreen.top", &["ctrl+home"]),
            ("tui.altScreen.bottom", &["ctrl+end"]),
            ("app.interrupt", &["escape"]),
            ("app.clear", &["ctrl+c"]),
            ("app.exit", &["ctrl+d"]),
            ("app.editor.external", &["ctrl+g"]),
            ("app.tools.expand", &["ctrl+o"]),
            ("app.thinking.toggle", &["ctrl+t"]),
            ("app.message.followUp", &["alt+enter"]),
            ("app.model.select", &["ctrl+l"]),
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
    pub fn action(&self, mut key: KeyEvent, empty: bool) -> Option<&str> {
        if key.code == KeyCode::BackTab {
            key.code = KeyCode::Tab;
            key.modifiers |= KeyModifiers::SHIFT;
        }
        if let KeyCode::Char(character) = key.code {
            key.code = KeyCode::Char(character.to_ascii_lowercase());
        }
        // Exit shares Ctrl+D with forward delete, and applies only to an empty editor.
        self.bindings
            .iter()
            .find(|(action, keys)| {
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
    pub fn describe(&self) -> String {
        self.bindings
            .iter()
            .filter(|(_, keys)| !keys.is_empty())
            .map(|(name, keys)| {
                format!(
                    "{name}  {}",
                    keys.iter()
                        .map(|key| format!("{key:?}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
fn parse(text: &str) -> Result<KeyEvent> {
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
}
