//! Pi theme roles and color formats, rendered without embedded escape strings.
use crossterm::style::Color;
use palette::convert::FromColorUnclamped;
use palette::{FromColor, LinSrgb, Okhsl, Oklab, Oklch, Srgb};
use pi_rs::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

static APPEARANCE: AtomicU8 = AtomicU8::new(0);
static TERMINAL_BACKGROUND: AtomicU32 = AtomicU32::new(0);
static TERMINAL_FOREGROUND: AtomicU32 = AtomicU32::new(0);
static TERMINAL_PALETTE: [AtomicU32; 16] = [const { AtomicU32::new(0) }; 16];

#[cfg(any(unix, windows))]
pub fn reset_terminal_background() {
    TERMINAL_BACKGROUND.store(0, Ordering::Relaxed);
    TERMINAL_FOREGROUND.store(0, Ordering::Relaxed);
    clear_terminal_palette();
}

#[cfg(any(unix, windows))]
pub fn clear_terminal_palette() {
    for entry in &TERMINAL_PALETTE {
        entry.store(0, Ordering::Relaxed);
    }
}

#[cfg(any(unix, windows))]
pub fn set_terminal_palette(index: u8, red: u8, green: u8, blue: u8) -> bool {
    let Some(entry) = TERMINAL_PALETTE.get(index as usize) else {
        return false;
    };
    let value = 0x1000000 | (u32::from(red) << 16) | (u32::from(green) << 8) | u32::from(blue);
    let changed = entry.swap(value, Ordering::Relaxed) != value;
    changed
        && TERMINAL_PALETTE
            .iter()
            .all(|color| color.load(Ordering::Relaxed) != 0)
}

fn unpack_terminal_color(value: u32) -> Option<[u8; 3]> {
    (value & 0x1000000 != 0).then_some([(value >> 16) as u8, (value >> 8) as u8, value as u8])
}

fn terminal_is_dark(background: [u8; 3], foreground: Option<[u8; 3]>) -> bool {
    let color = |rgb: [u8; 3]| {
        Srgb::new(
            rgb[0] as f64 / 255.0,
            rgb[1] as f64 / 255.0,
            rgb[2] as f64 / 255.0,
        )
    };
    let linear = color(background).into_linear();
    let luminance = 0.2126 * linear.red + 0.7152 * linear.green + 0.0722 * linear.blue;
    let white_contrast = 1.05 / (luminance + 0.05);
    let black_contrast = (luminance + 0.05) / 0.05;
    if let Some(foreground) = foreground {
        let foreground_lightness = Oklab::from_color(color(foreground).into_linear()).l;
        let background_lightness = Oklab::from_color(linear).l;
        if (foreground_lightness - background_lightness).abs() > 0.05 {
            let dark = foreground_lightness > background_lightness;
            if (if dark { white_contrast } else { black_contrast }) >= 4.5 {
                return dark;
            }
        }
    }
    white_contrast >= black_contrast
}

#[cfg(any(unix, windows))]
pub fn set_terminal_color(foreground: bool, red: u8, green: u8, blue: u8) -> bool {
    let value = 0x1000000 | (u32::from(red) << 16) | (u32::from(green) << 8) | u32::from(blue);
    if foreground {
        &TERMINAL_FOREGROUND
    } else {
        &TERMINAL_BACKGROUND
    }
    .store(value, Ordering::Relaxed);
    let Some(background) = unpack_terminal_color(TERMINAL_BACKGROUND.load(Ordering::Relaxed))
    else {
        return false;
    };
    let foreground = unpack_terminal_color(TERMINAL_FOREGROUND.load(Ordering::Relaxed));
    let appearance = if terminal_is_dark(background, foreground) {
        1
    } else {
        2
    };
    APPEARANCE.swap(appearance, Ordering::Relaxed) != appearance
}

#[cfg(any(unix, windows))]
pub fn set_appearance(dark: bool) -> bool {
    if unpack_terminal_color(TERMINAL_BACKGROUND.load(Ordering::Relaxed)).is_some() {
        return false;
    }
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
        let name = if let Some((light, dark_name)) = parse_auto_theme_setting(name) {
            if dark { dark_name } else { light }
        } else if name.contains('/') {
            return Err(Error::Config("invalid automatic theme setting".into()));
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
    pub fn is_dim(&self, role: &str) -> bool {
        self.name == "system"
            && matches!(
                role,
                "customMessageText"
                    | "muted"
                    | "dim"
                    | "thinkingText"
                    | "toolOutput"
                    | "mdLinkUrl"
                    | "mdQuote"
                    | "mdQuoteBorder"
                    | "mdHr"
                    | "mdCodeBlockBorder"
                    | "toolDiffContext"
                    | "syntaxComment"
                    | "scrollbarTrack"
                    | "scrollbarThumb"
                    | "searchMatchText"
                    | "borderMuted"
                    | "thinkingOff"
            )
    }
    pub fn system() -> Self {
        let background = unpack_terminal_color(TERMINAL_BACKGROUND.load(Ordering::Relaxed));
        let palette: Option<Vec<_>> = TERMINAL_PALETTE
            .iter()
            .map(|value| unpack_terminal_color(value.load(Ordering::Relaxed)))
            .collect();
        Self::system_with_palette(background, palette.as_deref())
    }

    fn system_with_palette(background: Option<[u8; 3]>, palette: Option<&[[u8; 3]]>) -> Self {
        // Pi uses the terminal's ANSI slots when no palette has been reported.
        let roles = [
            ("accent", 5),
            ("border", 4),
            ("borderAccent", 5),
            ("success", 2),
            ("error", 1),
            ("warning", 3),
            ("mdHeading", 3),
            ("mdLink", 4),
            ("mdCode", 5),
            ("mdCodeBlock", 2),
            ("mdListBullet", 5),
            ("customMessageLabel", 5),
            ("toolDiffAdded", 2),
            ("toolDiffRemoved", 1),
            ("bashMode", 2),
            ("syntaxKeyword", 4),
            ("syntaxFunction", 3),
            ("syntaxVariable", 6),
            ("syntaxString", 2),
            ("syntaxNumber", 5),
            ("syntaxType", 5),
            ("thinkingMinimal", 4),
            ("thinkingLow", 4),
            ("thinkingMedium", 6),
            ("thinkingHigh", 5),
            ("thinkingXhigh", 13),
            ("thinkingMax", 1),
        ];
        // Without a reported background Pi leaves colors as ANSI indices, even if
        // a palette was reported. The terminal can then render its own colors.
        let palette = background
            .and(palette)
            .filter(|palette| palette.len() == 16);
        Self {
            name: "system".into(),
            path: None,
            colors: roles
                .into_iter()
                .map(|(key, index)| {
                    let color = palette
                        .map(|palette| {
                            let [r, g, b] = palette[index as usize];
                            Color::Rgb { r, g, b }
                        })
                        .unwrap_or(Color::AnsiValue(index));
                    (key.into(), color)
                })
                .collect(),
        }
    }
}

pub fn parse_auto_theme_setting(value: &str) -> Option<(&str, &str)> {
    let (light, dark) = value.split_once('/')?;
    let (light, dark) = (light.trim(), dark.trim());
    (!light.is_empty() && !dark.is_empty() && !dark.contains('/')).then_some((light, dark))
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
    if let Some((kind, _)) = text.split_once('(')
        && (kind.eq_ignore_ascii_case("okhsl") || kind.eq_ignore_ascii_case("oklch"))
    {
        return parse_function_color(text, kind.eq_ignore_ascii_case("okhsl"));
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

fn parse_function_color(text: &str, is_hsl: bool) -> Result<Color> {
    use std::sync::OnceLock;
    static HSL: OnceLock<regex::Regex> = OnceLock::new();
    static LCH: OnceLock<regex::Regex> = OnceLock::new();
    let number = r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:e[+-]?[0-9]+)?";
    let pattern = if is_hsl { &HSL } else { &LCH };
    let pattern = pattern.get_or_init(|| {
        let expression = if is_hsl {
            format!(r"(?i)^okhsl\(\s*({number})(?:deg)?\s+({number})(%)?\s+({number})(%)?\s*\)$")
        } else {
            format!(r"(?i)^oklch\(\s*({number})(%)?\s+({number})\s+({number})(?:deg)?\s*\)$")
        };
        regex::Regex::new(&expression).expect("valid color pattern")
    });
    let invalid = || Error::Config(format!("invalid color value: {text}"));
    let captures = pattern.captures(text).ok_or_else(invalid)?;
    let number = |index: usize| -> Result<f64> {
        let value = captures[index].parse::<f64>().map_err(|_| invalid())?;
        if value.is_finite() {
            Ok(value)
        } else {
            Err(invalid())
        }
    };
    let rgb: Srgb<f64> = if is_hsl {
        let hue = number(1)?;
        let saturation = number(2)?
            / if captures.get(3).is_some() {
                100.0
            } else {
                1.0
            };
        let lightness = number(4)?
            / if captures.get(5).is_some() {
                100.0
            } else {
                1.0
            };
        if !(0.0..=1.0).contains(&saturation) || !(0.0..=1.0).contains(&lightness) {
            return Err(invalid());
        }
        Srgb::from_color(Okhsl::new(hue, saturation, lightness))
    } else {
        let lightness = number(1)?
            / if captures.get(2).is_some() {
                100.0
            } else {
                1.0
            };
        let chroma = number(3)?;
        let hue = number(4)?;
        if !(0.0..=1.0).contains(&lightness) || chroma < 0.0 {
            return Err(invalid());
        }
        let at_chroma = |chroma| LinSrgb::from_color_unclamped(Oklch::new(lightness, chroma, hue));
        let in_gamut = |rgb: LinSrgb<f64>| {
            [rgb.red, rgb.green, rgb.blue]
                .into_iter()
                .all(|channel| (-1e-7..=1.0 + 1e-7).contains(&channel))
        };
        let direct = at_chroma(chroma);
        let linear = if in_gamut(direct) {
            direct
        } else {
            let mut linear = at_chroma(0.0);
            let (mut low, mut high) = (0.0, chroma);
            for _ in 0..20 {
                let middle = (low + high) / 2.0;
                let candidate = at_chroma(middle);
                if in_gamut(candidate) {
                    low = middle;
                    linear = candidate;
                } else {
                    high = middle;
                }
            }
            linear
        };
        Srgb::from_linear(linear)
    };
    let rgb: Srgb<u8> = rgb.into_format();
    Ok(Color::Rgb {
        r: rgb.red,
        g: rgb.green,
        b: rgb.blue,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn automatic_theme_setting_requires_two_nonempty_names() {
        assert_eq!(
            super::parse_auto_theme_setting(" light / dark "),
            Some(("light", "dark"))
        );
        for invalid in ["light", "/dark", "light/", "light/dark/extra", " / dark"] {
            assert!(super::parse_auto_theme_setting(invalid).is_none());
        }
    }
    use super::*;
    #[test]
    fn terminal_appearance_uses_foreground_direction_when_readable() {
        assert!(terminal_is_dark([117, 117, 117], Some([255, 255, 255])));
        assert!(!terminal_is_dark([117, 117, 117], Some([0, 0, 0])));
        assert!(!terminal_is_dark([255, 255, 255], Some([255, 255, 255])));
        assert!(terminal_is_dark([0, 0, 0], Some([0, 0, 0])));
    }

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
    fn system_fallback_uses_pi_terminal_palette_slots() {
        let theme = Theme::system();
        for (token, index) in [
            ("accent", 5),
            ("border", 4),
            ("success", 2),
            ("error", 1),
            ("syntaxString", 2),
            ("syntaxNumber", 5),
            ("thinkingXhigh", 13),
        ] {
            assert_eq!(theme.color(token), Color::AnsiValue(index), "{token}");
        }
        for token in ["text", "toolTitle", "selectedBg", "toolErrorBg"] {
            assert_eq!(theme.color(token), Color::Reset, "{token}");
        }
    }

    #[test]
    fn system_without_background_keeps_indexed_colors_even_with_palette() {
        let palette = [[17, 34, 51]; 16];
        let theme = Theme::system_with_palette(None, Some(&palette));
        assert_eq!(theme.color("accent"), Color::AnsiValue(5));
        assert_eq!(theme.color("text"), Color::Reset);
        assert!(theme.is_dim("muted"));

        let theme = Theme::system_with_palette(Some([0, 0, 0]), Some(&palette));
        assert_eq!(
            theme.color("accent"),
            Color::Rgb {
                r: 17,
                g: 34,
                b: 51
            }
        );
        let theme = Theme::system_with_palette(Some([0, 0, 0]), Some(&palette[..15]));
        assert_eq!(theme.color("accent"), Color::AnsiValue(5));
    }

    #[test]
    fn color_functions_accept_pi_syntax_and_validate_ranges() {
        let parse = |text: &str| {
            resolve(
                &Value::String(text.into()),
                &BTreeMap::new(),
                &mut BTreeSet::new(),
            )
        };
        assert_eq!(
            parse("OKHSL(250deg 60% 55%)").unwrap(),
            parse("okhsl(250 0.6 0.55)").unwrap()
        );
        assert_eq!(
            parse("OKLCH(62% 0.1 200deg)").unwrap(),
            parse("oklch(.62 0.1 200)").unwrap()
        );
        assert_eq!(
            parse("oklch(100% 0.3 150)").unwrap(),
            Color::Rgb {
                r: 255,
                g: 255,
                b: 255
            }
        );
        for invalid in [
            "okhsl(1 160% 55%)",
            "okhsl(1 -0.1 0.5)",
            "oklch(50% -0.1 0)",
            "oklch(150% 0 0)",
            "oklch(.5 20% 0)",
            "okhsl(2% .5 .5)",
        ] {
            assert!(parse(invalid).is_err(), "{invalid}");
        }
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
