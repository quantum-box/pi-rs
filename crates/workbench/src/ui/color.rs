use std::sync::LazyLock;

use crossterm::style::Color;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColorMode {
    TrueColor,
    Indexed,
}

impl ColorMode {
    pub fn resolve(configured: Option<bool>) -> Self {
        static DETECTED: LazyLock<bool> =
            LazyLock::new(|| detect_true_color(|key| std::env::var(key).ok(), cfg!(windows)));
        Self::from_true_color(configured.unwrap_or_else(|| *DETECTED))
    }

    fn from_true_color(enabled: bool) -> Self {
        if enabled {
            Self::TrueColor
        } else {
            Self::Indexed
        }
    }

    pub fn encode(self, color: Color) -> Color {
        match (self, color) {
            (Self::Indexed, Color::Rgb { r, g, b }) => Color::AnsiValue(rgb_to_ansi256([r, g, b])),
            _ => color,
        }
    }
}

fn detect_true_color(get: impl Fn(&str) -> Option<String>, windows: bool) -> bool {
    match get("PI_TRUE_COLOR").as_deref() {
        Some("1") => return true,
        Some("0") => return false,
        _ => {}
    }
    let lower = |key| get(key).unwrap_or_default().to_ascii_lowercase();
    let present = |key| get(key).is_some_and(|value| !value.is_empty());
    let term = lower("TERM");
    let program = lower("TERM_PROGRAM");
    let hint =
        matches!(lower("COLORTERM").as_str(), "truecolor" | "24bit") || term.ends_with("-direct");
    if present("TMUX")
        || term.starts_with("tmux")
        || term.starts_with("screen")
        || program == "herdr"
    {
        return hint;
    }
    hint || windows
        || matches!(
            program.as_str(),
            "rex"
                | "kitty"
                | "ghostty"
                | "wezterm"
                | "warpterminal"
                | "iterm.app"
                | "alacritty"
                | "vscode"
                | "zed"
        )
        || (program == "orca" && get("ORCA_IMAGE_PROTOCOL").as_deref() == Some("kitty"))
        || term.contains("ghostty")
        || term.starts_with("alacritty")
        || lower("TERMINAL_EMULATOR") == "jetbrains-jediterm"
        || [
            "KITTY_WINDOW_ID",
            "GHOSTTY_RESOURCES_DIR",
            "WEZTERM_PANE",
            "WARP_SESSION_ID",
            "WARP_TERMINAL_SESSION_UUID",
            "ITERM_SESSION_ID",
            "WT_SESSION",
            "ALACRITTY_WINDOW_ID",
        ]
        .into_iter()
        .any(present)
}

fn rgb_to_ansi256(rgb: [u8; 3]) -> u8 {
    const CUBE: [u8; 6] = [0, 95, 135, 175, 215, 255];
    const GRAYS: [u8; 24] = [
        8, 18, 28, 38, 48, 58, 68, 78, 88, 98, 108, 118, 128, 138, 148, 158, 168, 178, 188, 198,
        208, 218, 228, 238,
    ];
    let [r, g, b] = rgb.map(|component| closest(&CUBE, component));
    let cube_index = 16 + 36 * r + 6 * g + b;
    let [red, green, blue] = rgb;
    let spread = red.max(green).max(blue) - red.min(green).min(blue);
    // Pi only considers the grayscale ramp for near-neutral colors.
    if spread < 10 {
        let gray = weighted(rgb.map(f64::from)).round() as u8;
        let offset = closest(&GRAYS, gray);
        let cube = [r, g, b].map(|index| CUBE[index as usize]);
        if distance(rgb, [GRAYS[offset as usize]; 3]) < distance(rgb, cube) {
            return 232 + offset;
        }
    }
    cube_index
}

fn closest(values: &[u8], target: u8) -> u8 {
    values
        .iter()
        .enumerate()
        .min_by_key(|(_, value)| target.abs_diff(**value))
        .map_or(0, |(index, _)| index as u8)
}

fn weighted([red, green, blue]: [f64; 3]) -> f64 {
    0.299 * red + 0.587 * green + 0.114 * blue
}

fn distance([r, g, b]: [u8; 3], [other_r, other_g, other_b]: [u8; 3]) -> f64 {
    weighted([
        f64::from(r.abs_diff(other_r)).powi(2),
        f64::from(g.abs_diff(other_g)).powi(2),
        f64::from(b.abs_diff(other_b)).powi(2),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn true_color_auto_and_env_follow_pi_precedence() {
        let detect = |values: &[(&str, &str)], windows| {
            detect_true_color(
                |key| {
                    values
                        .iter()
                        .find(|(name, _)| *name == key)
                        .map(|(_, value)| value.to_string())
                },
                windows,
            )
        };
        assert!(!detect(&[("TERM", "xterm-256color")], false));
        assert!(detect(&[("TERM", "XTERM-DIRECT")], false));
        assert!(detect(&[("COLORTERM", "24Bit")], false));
        assert!(detect(&[("TERM_PROGRAM", "WezTerm")], false));
        assert!(detect(
            &[("TERMINAL_EMULATOR", "JetBrains-JediTerm")],
            false
        ));
        assert!(!detect(&[("TERM_PROGRAM", "orca")], false));
        assert!(detect(
            &[("TERM_PROGRAM", "orca"), ("ORCA_IMAGE_PROTOCOL", "kitty")],
            false
        ));
        assert!(detect(&[], true));
        for multiplexed in [
            ("TERM", "screen"),
            ("TMUX", "/tmp/tmux-socket"),
            ("TERM_PROGRAM", "herdr"),
        ] {
            assert!(!detect(&[multiplexed, ("KITTY_WINDOW_ID", "1")], false));
            assert!(detect(&[multiplexed, ("COLORTERM", "TRUECOLOR")], false));
        }
        assert!(!detect(
            &[("PI_TRUE_COLOR", "0"), ("TERM_PROGRAM", "kitty")],
            false
        ));
        assert!(detect(&[("PI_TRUE_COLOR", "1"), ("TERM", "screen")], false));
        assert!(!detect(&[("PI_TRUE_COLOR", "true")], false));
        assert!(detect(
            &[("PI_TRUE_COLOR", " 0"), ("COLORTERM", "truecolor")],
            false
        ));
        assert_eq!(ColorMode::resolve(Some(false)), ColorMode::Indexed);
        assert_eq!(ColorMode::resolve(Some(true)), ColorMode::TrueColor);
    }

    #[test]
    fn quantizes_rgb_with_pi_cube_and_gray_rules() {
        for (rgb, index) in [
            ([0, 0, 0], 16),
            ([255, 255, 255], 231),
            ([255, 0, 0], 196),
            ([128, 128, 128], 244),
            ([48, 48, 57], 236),
            ([48, 48, 58], 59),
            ([115, 0, 0], 52),
            ([95, 95, 95], 59),
        ] {
            assert_eq!(rgb_to_ansi256(rgb), index, "{rgb:?}");
        }
        for mode in [ColorMode::TrueColor, ColorMode::Indexed] {
            for color in [Color::Reset, Color::Blue, Color::AnsiValue(123)] {
                assert_eq!(mode.encode(color), color);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn color_mode_encodes_only_at_the_render_boundary() {
        use crate::ui::render::{Line, Span};
        if std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty()) {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ui::color::tests::color_mode_encodes_only_at_the_render_boundary",
                ])
                .env_remove("NO_COLOR")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let red = Color::Rgb { r: 255, g: 0, b: 0 };
        let gray = Color::Rgb {
            r: 128,
            g: 128,
            b: 128,
        };
        let mut line = Line::plain("r", red);
        line.background = Some(gray);
        let mut indexed = Span::new("i", Color::AnsiValue(123));
        indexed.background = Some(red);
        line.spans.push(indexed);
        for (mode, fg, background, span_background) in [
            (ColorMode::Indexed, "38;5;196", "48;5;244", "48;5;196"),
            (
                ColorMode::TrueColor,
                "38;2;255;0;0",
                "48;2;128;128;128",
                "48;2;255;0;0",
            ),
        ] {
            let mut output = Vec::new();
            line.draw(&mut output, 3, mode).unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains(&format!("\x1b[{fg}m")));
            assert!(output.ends_with(&format!("\x1b[0m\x1b[{background}m \x1b[0m")));
            assert!(output.contains(&format!("\x1b[38;5;123m\x1b[{span_background}m")));
            if mode == ColorMode::Indexed {
                assert!(!output.contains("\x1b[38;2;"));
                assert!(!output.contains("\x1b[48;2;"));
            }
        }
        assert_eq!(line.spans[0].color, red);
        assert_eq!(line.background, Some(gray));
    }
}
