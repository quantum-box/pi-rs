//! Pi theme roles and color formats, rendered without embedded escape strings.
use crossterm::style::Color;
use palette::{FromColor, Okhsl, Oklch, Srgb};
use pi_rs::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};

static APPEARANCE: AtomicU8 = AtomicU8::new(0);

#[cfg(any(unix, windows))]
pub fn set_appearance(dark: bool) -> bool {
    let value = if dark { 1 } else { 2 };
    APPEARANCE.swap(value, Ordering::Relaxed) != value
}

fn colorfgbg_is_dark(value: &str) -> Option<bool> {
    let background = value
        .rsplit(';')
        .next()?
        .trim_matches(|ch: char| (ch.is_whitespace() && ch != '\u{0085}') || ch == '\u{feff}');
    if !(1..=2).contains(&background.len()) || !background.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let index = background.parse::<u8>().ok()?;
    (index <= 15).then_some(index <= 6 || index == 8)
}

#[derive(Clone, PartialEq, Eq)]
pub struct Theme {
    pub name: String,
    pub path: Option<PathBuf>,
    colors: BTreeMap<String, Color>,
}
/// Watches the selected custom file while retaining the last valid theme.
pub struct ThemeWatcher {
    path: Option<PathBuf>,
    fingerprint: Option<Vec<u8>>,
}
impl ThemeWatcher {
    pub fn new(theme: &Theme) -> Self {
        Self {
            path: theme.path.clone(),
            fingerprint: theme
                .path
                .as_ref()
                .and_then(|path| std::fs::read(path).ok()),
        }
    }
    pub fn poll(&mut self, theme: &mut Theme) -> Result<bool> {
        if self.path != theme.path {
            *self = Self::new(theme);
            return Ok(false);
        }
        let Some(path) = &self.path else {
            return Ok(false);
        };
        let contents = std::fs::read(path);
        let fingerprint = contents.as_ref().ok().cloned();
        if fingerprint == self.fingerprint {
            return Ok(false);
        }
        self.fingerprint = fingerprint;
        let text =
            String::from_utf8(contents?).map_err(|error| Error::Config(error.to_string()))?;
        let next = Theme::parse(&text, Some(path.clone()))?;
        *theme = next;
        Ok(true)
    }
}
#[derive(Deserialize)]
struct Definition {
    name: String,
    #[serde(default)]
    vars: BTreeMap<String, Value>,
    colors: BTreeMap<String, Value>,
}
impl Theme {
    pub fn load(
        name: &str,
        extra: &[PathBuf],
        workspace: &Path,
        trusted: bool,
        discover: bool,
    ) -> Result<Self> {
        let dark = match APPEARANCE.load(Ordering::Relaxed) {
            1 => true,
            2 => false,
            _ => std::env::var("COLORFGBG")
                .ok()
                .and_then(|value| colorfgbg_is_dark(&value))
                .unwrap_or(true),
        };
        let name = if let Some((light, dark_name)) = name.split_once('/') {
            if dark { dark_name } else { light }
        } else {
            name
        };
        match name {
            "system" => Ok(Self::system()),
            "dark" => Self::parse(include_str!("../../assets/themes/dark.json"), None),
            "light" => Self::parse(include_str!("../../assets/themes/light.json"), None),
            _ => {
                let mut paths = vec![
                    crate::config::agent_dir()
                        .join("themes")
                        .join(format!("{name}.json")),
                ];
                if !discover {
                    paths.clear();
                }
                if trusted && discover {
                    paths.push(workspace.join(".pi/themes").join(format!("{name}.json")));
                }
                for path in extra {
                    paths.push(if path.is_dir() {
                        path.join(format!("{name}.json"))
                    } else {
                        path.clone()
                    });
                }
                for path in paths {
                    if path.is_file() {
                        let text = std::fs::read_to_string(&path)?;
                        let theme = Self::parse(&text, Some(path))?;
                        if theme.name == name {
                            return Ok(theme);
                        }
                    }
                }
                Err(Error::Config(format!("theme {name:?} was not found")))
            }
        }
    }
    pub fn discover(
        extra: &[PathBuf],
        workspace: &Path,
        trusted: bool,
        discover: bool,
    ) -> Vec<String> {
        let mut names = vec!["system".into(), "dark".into(), "light".into()];
        let mut paths = extra.to_vec();
        if discover {
            paths.push(crate::config::agent_dir().join("themes"));
        }
        if trusted && discover {
            paths.push(workspace.join(".pi/themes"));
        }
        for path in paths {
            let files = if path.is_dir() {
                std::fs::read_dir(path)
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
                    .collect::<Vec<_>>()
            } else {
                vec![path]
            };
            for file in files {
                if let Ok(text) = std::fs::read_to_string(&file)
                    && let Ok(theme) = Self::parse(&text, Some(file))
                {
                    names.push(theme.name);
                }
            }
        }
        names.sort();
        names.dedup();
        names
    }

    pub fn parse(text: &str, path: Option<PathBuf>) -> Result<Self> {
        let value: Value = serde_json::from_str(text)?;
        let schema: Value = serde_json::from_str(include_str!("../../assets/theme.schema.json"))?;
        let validator =
            jsonschema::validator_for(&schema).map_err(|error| Error::Config(error.to_string()))?;
        if let Err(error) = validator.validate(&value) {
            return Err(Error::Config(format!("invalid theme: {error}")));
        }
        let definition: Definition = serde_json::from_value(value)?;
        if definition.name == "system" {
            return Err(Error::Config("system is a reserved theme name".into()));
        }
        let mut colors = BTreeMap::new();
        for (role, value) in &definition.colors {
            colors.insert(
                role.clone(),
                resolve(value, &definition.vars, &mut BTreeSet::new())?,
            );
        }
        for (role, fallback) in [
            ("scrollbarTrack", "muted"),
            ("scrollbarThumb", "text"),
            ("searchMatchBg", "selectedBg"),
            ("searchMatchText", "text"),
            ("thinkingMax", "thinkingXhigh"),
        ] {
            if !colors.contains_key(role) {
                colors.insert(
                    role.into(),
                    colors.get(fallback).copied().unwrap_or(Color::Reset),
                );
            }
        }
        Ok(Self {
            name: definition.name,
            path,
            colors,
        })
    }
    pub fn color(&self, role: &str) -> Color {
        self.colors.get(role).copied().unwrap_or(Color::Reset)
    }
    pub fn system() -> Self {
        let roles = [
            ("accent", Color::Cyan),
            ("border", Color::DarkGrey),
            ("borderAccent", Color::Cyan),
            ("borderMuted", Color::DarkGrey),
            ("text", Color::Reset),
            ("muted", Color::Grey),
            ("dim", Color::DarkGrey),
            ("success", Color::Green),
            ("warning", Color::Yellow),
            ("error", Color::Red),
            ("thinkingText", Color::DarkGrey),
            ("mdHeading", Color::Yellow),
            ("mdLink", Color::Blue),
            ("mdLinkUrl", Color::DarkGrey),
            ("mdCode", Color::Magenta),
            ("mdCodeBlock", Color::Green),
            ("mdQuote", Color::Grey),
            ("mdQuoteBorder", Color::DarkGrey),
            ("mdHr", Color::DarkGrey),
            ("mdListBullet", Color::Cyan),
            ("toolTitle", Color::Reset),
            ("toolOutput", Color::Grey),
            ("toolDiffAdded", Color::Green),
            ("toolDiffRemoved", Color::Red),
            ("toolDiffContext", Color::Grey),
            ("thinkingOff", Color::DarkGrey),
            ("thinkingMinimal", Color::DarkBlue),
            ("thinkingLow", Color::Blue),
            ("thinkingMedium", Color::Magenta),
            ("thinkingHigh", Color::DarkMagenta),
            ("thinkingXhigh", Color::Red),
            ("thinkingMax", Color::Yellow),
            ("bashMode", Color::Green),
        ];
        Self {
            name: "system".into(),
            path: None,
            colors: roles
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        }
    }
}

fn resolve(
    value: &Value,
    variables: &BTreeMap<String, Value>,
    seen: &mut BTreeSet<String>,
) -> Result<Color> {
    if let Some(index) = value.as_u64() {
        return u8::try_from(index)
            .map(Color::AnsiValue)
            .map_err(|_| Error::Config("color index must be 0..255".into()));
    }
    let text = value
        .as_str()
        .ok_or_else(|| Error::Config("invalid color value".into()))?;
    if text.is_empty() {
        return Ok(Color::Reset);
    }
    if let Some(hex) = text.strip_prefix('#') {
        let hex = if hex.len() == 3 {
            hex.chars().flat_map(|c| [c, c]).collect::<String>()
        } else {
            hex.into()
        };
        if hex.len() != 6 {
            return Err(Error::Config("hex color needs 3 or 6 digits".into()));
        }
        let number =
            u32::from_str_radix(&hex, 16).map_err(|_| Error::Config("invalid hex color".into()))?;
        return Ok(Color::Rgb {
            r: (number >> 16) as u8,
            g: (number >> 8) as u8,
            b: number as u8,
        });
    }
    if text.starts_with("okhsl(") || text.starts_with("oklch(") {
        let (kind, args) = text.split_once('(').expect("color function");
        let args = args
            .strip_suffix(')')
            .ok_or_else(|| Error::Config("unclosed color function".into()))?;
        let components: Vec<_> = args
            .split_whitespace()
            .map(|part| {
                let percent = part.ends_with('%');
                let number = part
                    .trim_end_matches('%')
                    .parse::<f32>()
                    .map_err(|_| Error::Config("invalid color component".into()))?;
                if !number.is_finite() {
                    return Err(Error::Config("color components must be finite".into()));
                }
                Ok(if percent { number / 100.0 } else { number })
            })
            .collect::<Result<_>>()?;
        let [a, b, c] = components.as_slice() else {
            return Err(Error::Config(
                "color function needs three components".into(),
            ));
        };
        let rgb: Srgb<f32> = if kind == "okhsl" {
            Srgb::from_color(Okhsl::new(*a, *b, *c))
        } else {
            Srgb::from_color(Oklch::new(*a, *b, *c))
        };
        let rgb: Srgb<u8> = rgb.into_format();
        return Ok(Color::Rgb {
            r: rgb.red,
            g: rgb.green,
            b: rgb.blue,
        });
    }
    if !seen.insert(text.into()) {
        return Err(Error::Config(format!("circular color variable {text:?}")));
    }
    let variable = variables
        .get(text)
        .ok_or_else(|| Error::Config(format!("missing color variable {text:?}")))?;
    let result = resolve(variable, variables, seen);
    seen.remove(text);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn colorfgbg_classifies_pi_terminal_palette_indexes() {
        for index in 0..=15 {
            assert_eq!(
                colorfgbg_is_dark(&format!("15; {index} ")),
                Some(index <= 6 || index == 8)
            );
        }
        for value in ["", "7;16", "7;-1", "7;007", "7;９", "7;\u{0085}7"] {
            assert_eq!(colorfgbg_is_dark(value), None, "{value:?}");
        }
        assert_eq!(colorfgbg_is_dark("7;\u{feff}7\u{feff}"), Some(false));
    }

    #[test]
    fn file_watch_applies_valid_changes_and_retains_last_valid_theme() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("theme.json");
        let original = include_str!("../../assets/themes/dark.json");
        std::fs::write(&path, original).unwrap();
        let mut theme = Theme::parse(original, Some(path.clone())).unwrap();
        let mut watcher = ThemeWatcher::new(&theme);
        assert!(!watcher.poll(&mut theme).unwrap());
        let updated = original.replace("\"name\": \"dark\"", "\"name\": \"custom\"");
        std::fs::write(&path, updated).unwrap();
        assert!(watcher.poll(&mut theme).unwrap());
        assert_eq!(theme.name, "custom");
        std::fs::write(&path, "{").unwrap();
        assert!(watcher.poll(&mut theme).is_err());
        assert_eq!(theme.name, "custom");
        assert!(!watcher.poll(&mut theme).unwrap());
        std::fs::write(&path, original).unwrap();
        assert!(watcher.poll(&mut theme).unwrap());
        assert_eq!(theme.name, "dark");
    }
    #[test]
    fn upstream_themes_validate_and_resolve_all_color_formats() {
        for source in [
            include_str!("../../assets/themes/dark.json"),
            include_str!("../../assets/themes/light.json"),
        ] {
            let theme = Theme::parse(source, None).unwrap();
            assert!(matches!(theme.color("accent"), Color::Rgb { .. }));
            assert_ne!(theme.color("toolErrorBg"), theme.color("toolSuccessBg"));
        }
        assert_eq!(
            resolve(
                &Value::String("#0af".into()),
                &BTreeMap::new(),
                &mut BTreeSet::new()
            )
            .unwrap(),
            Color::Rgb {
                r: 0,
                g: 170,
                b: 255
            }
        );
    }
    #[test]
    fn color_cycles_are_rejected() {
        let vars = [
            ("a".into(), Value::String("b".into())),
            ("b".into(), Value::String("a".into())),
        ]
        .into();
        assert!(resolve(&Value::String("a".into()), &vars, &mut BTreeSet::new()).is_err());
    }
}
