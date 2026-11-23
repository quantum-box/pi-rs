//! Pi theme roles and color formats, rendered without embedded escape strings.
use crossterm::style::Color;
use palette::Srgb;
use pi_rs::{Error, Result};
#[cfg(test)]
use pi_rs_workbench::theme_parser::resolve;
#[cfg(test)]
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};

static APPEARANCE: AtomicU8 = AtomicU8::new(0);
static TERMINAL_BACKGROUND: AtomicU32 = AtomicU32::new(0);
static TERMINAL_FOREGROUND: AtomicU32 = AtomicU32::new(0);
static TERMINAL_COLORS_PENDING: AtomicBool = AtomicBool::new(false);
static TERMINAL_PALETTE: std::sync::Mutex<PaletteState> =
    std::sync::Mutex::new(PaletteState::new());

#[cfg(any(unix, windows))]
pub fn mark_terminal_colors_pending() {
    TERMINAL_COLORS_PENDING.store(true, Ordering::Relaxed);
}

#[cfg(any(unix, windows))]
pub fn finish_terminal_colors_pending() -> bool {
    TERMINAL_COLORS_PENDING.swap(false, Ordering::Relaxed)
}

#[cfg(any(unix, windows))]
pub struct PendingTerminalColors;

#[cfg(any(unix, windows))]
impl PendingTerminalColors {
    pub fn new() -> Self {
        mark_terminal_colors_pending();
        Self
    }
}

#[cfg(any(unix, windows))]
impl Drop for PendingTerminalColors {
    fn drop(&mut self) {
        finish_terminal_colors_pending();
    }
}

struct PaletteState {
    current: Option<[[u8; 3]; 16]>,
    pending: [Option<[u8; 3]>; 16],
}

impl PaletteState {
    const fn new() -> Self {
        Self {
            current: None,
            pending: [None; 16],
        }
    }
    fn begin(&mut self) {
        self.pending = [None; 16];
    }
    fn record(&mut self, index: u8, color: [u8; 3]) -> bool {
        let Some(slot) = self.pending.get_mut(index as usize) else {
            return false;
        };
        *slot = Some(color);
        let Some(colors) = self.pending.iter().copied().collect::<Option<Vec<_>>>() else {
            return false;
        };
        let palette: [[u8; 3]; 16] = colors.try_into().expect("sixteen palette colors");
        let changed = self.current != Some(palette);
        self.current = Some(palette);
        changed
    }
}

#[cfg(any(unix, windows))]
pub fn reset_terminal_background() {
    invalidate_terminal_default_colors();
    clear_terminal_palette();
}

#[cfg(any(unix, windows))]
pub fn invalidate_terminal_default_colors() {
    TERMINAL_BACKGROUND.store(0, Ordering::Relaxed);
    TERMINAL_FOREGROUND.store(0, Ordering::Relaxed);
}

#[cfg(any(unix, windows))]
pub fn clear_terminal_palette() {
    *TERMINAL_PALETTE.lock().expect("terminal palette") = PaletteState::new();
}

#[cfg(any(unix, windows))]
pub fn begin_terminal_palette_query() {
    TERMINAL_PALETTE.lock().expect("terminal palette").begin();
}

#[cfg(any(unix, windows))]
pub fn set_terminal_palette(index: u8, red: u8, green: u8, blue: u8) -> bool {
    TERMINAL_PALETTE
        .lock()
        .expect("terminal palette")
        .record(index, [red, green, blue])
}

fn unpack_terminal_color(value: u32) -> Option<[u8; 3]> {
    (value & 0x1000000 != 0).then_some([(value >> 16) as u8, (value >> 8) as u8, value as u8])
}

pub(super) fn terminal_is_dark(background: [u8; 3], foreground: Option<[u8; 3]>) -> bool {
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
        let foreground_lightness = super::system_theme_color::rgb_to_lab(foreground)[0];
        let background_lightness = super::system_theme_color::rgb_to_lab(background)[0];
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
    dim: BTreeSet<String>,
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
        let parsed = pi_rs_workbench::theme_parser::parse_theme(text)?;
        Ok(Self {
            name: parsed.name,
            path,
            colors: parsed.colors,
            dim: BTreeSet::new(),
        })
    }
    pub fn color(&self, role: &str) -> Color {
        self.colors.get(role).copied().unwrap_or(Color::Reset)
    }
    pub fn is_dim(&self, role: &str) -> bool {
        self.dim.contains(role)
    }
    pub fn system() -> Self {
        if TERMINAL_COLORS_PENDING.load(Ordering::Relaxed) {
            let generated = super::system_theme::generate(None, None, None, 0.0, None);
            return Self {
                name: "system".into(),
                path: None,
                colors: generated.colors,
                dim: generated.dim,
            };
        }
        let background = unpack_terminal_color(TERMINAL_BACKGROUND.load(Ordering::Relaxed));
        let foreground = unpack_terminal_color(TERMINAL_FOREGROUND.load(Ordering::Relaxed));
        let palette = TERMINAL_PALETTE.lock().expect("terminal palette").current;
        Self::system_generated(
            background,
            foreground,
            palette.as_ref().map(|p| p.as_slice()),
        )
    }

    fn system_generated(
        background: Option<[u8; 3]>,
        foreground: Option<[u8; 3]>,
        palette: Option<&[[u8; 3]]>,
    ) -> Self {
        let generated = super::system_theme::generate(background, foreground, palette, 1.0, None);
        Self {
            name: "system".into(),
            path: None,
            colors: generated.colors,
            dim: generated.dim,
        }
    }

    #[cfg(test)]
    fn system_with_palette(background: Option<[u8; 3]>, palette: Option<&[[u8; 3]]>) -> Self {
        Self::system_generated(background, None, palette)
    }
}

pub fn parse_auto_theme_setting(value: &str) -> Option<(&str, &str)> {
    let (light, dark) = value.split_once('/')?;
    let (light, dark) = (light.trim(), dark.trim());
    (!light.is_empty() && !dark.is_empty() && !dark.contains('/')).then_some((light, dark))
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

    #[cfg(any(unix, windows))]
    #[test]
    fn appearance_changes_theme_pair_while_requery_is_incomplete() {
        const CHILD: &str = "PI_RS_THEME_APPEARANCE_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ui::theme::tests::appearance_changes_theme_pair_while_requery_is_incomplete",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        reset_terminal_background();
        set_terminal_color(false, 0, 0, 0);
        begin_terminal_palette_query();
        for index in 0..16 {
            set_terminal_palette(index, index, 10, 20);
        }
        let saved = TERMINAL_PALETTE.lock().unwrap().current;
        let theme = |setting| {
            Theme::load(setting, &[], std::path::Path::new("."), false, false)
                .unwrap()
                .name
        };
        assert_eq!(theme("light/dark"), "dark");
        invalidate_terminal_default_colors();
        assert!(set_appearance(false));
        begin_terminal_palette_query();
        for index in 0..8 {
            assert!(!set_terminal_palette(index, index, 30, 40));
        }
        assert_eq!(theme("light/dark"), "light");
        assert_eq!(theme("dark"), "dark");
        assert_eq!(TERMINAL_PALETTE.lock().unwrap().current, saved);
        // A foreground reply without a fresh background must not restore stale appearance.
        assert!(!set_terminal_color(true, 0, 0, 0));
        assert_eq!(theme("light/dark"), "light");
        assert!(!set_terminal_color(false, 255, 255, 255));
        assert_eq!(theme("light/dark"), "light");
        assert_eq!(TERMINAL_PALETTE.lock().unwrap().current, saved);
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
    #[cfg(any(unix, windows))]
    fn system_theme_stays_grayscale_until_color_query_settles() {
        const CHILD: &str = "PI_RS_PENDING_SYSTEM_THEME_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ui::theme::tests::system_theme_stays_grayscale_until_color_query_settles",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        reset_terminal_background();
        mark_terminal_colors_pending();
        assert_eq!(Theme::system().color("accent"), Color::Reset);
        set_terminal_color(false, 0, 0, 0);
        assert_eq!(Theme::system().color("accent"), Color::Reset);
        assert!(finish_terminal_colors_pending());
        assert!(!finish_terminal_colors_pending());
        assert!(matches!(Theme::system().color("accent"), Color::Rgb { .. }));
    }

    #[test]
    fn system_without_background_keeps_indexed_colors_even_with_palette() {
        let palette = [[17, 34, 51]; 16];
        let theme = Theme::system_with_palette(None, Some(&palette));
        assert_eq!(theme.color("accent"), Color::AnsiValue(5));
        assert_eq!(theme.color("text"), Color::Reset);
        assert!(theme.is_dim("muted"));

        let theme = Theme::system_with_palette(Some([0, 0, 0]), Some(&palette));
        assert!(matches!(theme.color("accent"), Color::Rgb { .. }));
        assert!(!theme.is_dim("muted"));
        let theme = Theme::system_with_palette(Some([0, 0, 0]), Some(&palette[..15]));
        assert!(matches!(theme.color("accent"), Color::Rgb { .. }));
    }

    #[test]
    fn system_theme_uses_reported_foreground_and_generated_dim_flags() {
        let bg = Some([0, 0, 0]);
        let bright = Theme::system_generated(bg, Some([255, 255, 255]), None);
        let weak = Theme::system_generated(bg, Some([40, 40, 40]), None);
        assert_eq!(bright.color("text"), Color::Reset);
        assert!(matches!(weak.color("text"), Color::Rgb { .. }));
        assert!(!bright.is_dim("muted"));
        assert!(Theme::system_generated(None, None, None).is_dim("muted"));
    }

    #[test]
    fn palette_queries_publish_only_complete_snapshots_and_keep_last_valid_colors() {
        let mut state = PaletteState::new();
        for index in (0..16).rev() {
            assert_eq!(state.record(index, [index, 10, 20]), index == 0);
        }
        let previous = state.current.unwrap();
        assert!(matches!(
            Theme::system_with_palette(Some([0, 0, 0]), Some(&previous)).color("accent"),
            Color::Rgb { .. }
        ));

        state.begin();
        for index in 0..15 {
            assert!(!state.record(index, [index, 30, 40]));
            assert_eq!(state.current, Some(previous));
        }
        // A timed-out or incomplete query must not contaminate the next one.
        state.begin();
        for index in 0..15 {
            assert!(!state.record(index, [index, 30, 40]));
        }
        assert!(state.record(15, [15, 30, 40]));
        let updated = state.current.unwrap();
        assert_ne!(
            Theme::system_with_palette(Some([0, 0, 0]), Some(&updated)).color("accent"),
            Theme::system_with_palette(Some([0, 0, 0]), Some(&previous)).color("accent")
        );
        state.begin();
        for index in 0..16 {
            assert!(!state.record(index, [index, 30, 40]));
        }
        assert!(!state.record(16, [1, 2, 3]));
        assert_eq!(state.current, Some(updated));
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
