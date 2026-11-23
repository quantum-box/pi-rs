//! Port of Pi v1.5.0's system-theme recipe and contrast solver.
//! Upstream copyright and license: `system_theme_pi_license.txt`.
use super::system_theme_color as math;
use super::system_theme_data::{FAMILIES, LEVELS, TOKENS};
use crossterm::style::Color;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy)]
pub(super) struct Family {
    pub hue: f64,
    pub min: f64,
    pub max: f64,
    pub slot: u8,
}
#[derive(Clone, Copy)]
pub(super) struct Curve {
    pub coefficients: [f64; 6],
    pub reachable: [f64; 2],
}
type Rgb = [u8; 3];
#[derive(Clone, Copy)]
struct Source {
    h: f64,
    s: f64,
    l: f64,
    c: f64,
}
struct Rule {
    token: &'static str,
    surfaces: Vec<&'static str>,
    level: &'static str,
}

const PANELS: &[&str] = &[
    "userMessageBg",
    "toolPendingBg",
    "toolSuccessBg",
    "toolErrorBg",
    "selectedBg",
    "searchMatchBg",
    "customMessageBg",
];
const FOREGROUND: &[&str] = &["text", "userMessageText", "toolTitle"];
fn family(name: &str) -> Family {
    FAMILIES.iter().find(|(n, _)| *n == name).unwrap().1
}
fn token_family(token: &str) -> Family {
    family(TOKENS.iter().find(|(t, _)| *t == token).unwrap().1)
}
fn slot(token: &str, family: Family) -> u8 {
    match token {
        "syntaxString" => 2,
        "syntaxNumber" => 5,
        "searchMatchBg" => 3,
        _ => family.slot,
    }
}
fn rules() -> Vec<Rule> {
    let mut r = Vec::new();
    let mut add = |tokens: &[&'static str], surfaces: &[&'static str], level| {
        for &token in tokens {
            r.push(Rule {
                token,
                surfaces: surfaces.to_vec(),
                level,
            });
        }
    };
    add(PANELS, &["background"], "panel");
    add(&["text"], &["background"], "text");
    add(&["text"], &["selectedBg"], "textOnPanel");
    add(&["userMessageText"], &["userMessageBg"], "textOnPanel");
    add(
        &["toolTitle"],
        &["toolPendingBg", "toolSuccessBg", "toolErrorBg"],
        "textOnPanel",
    );
    add(
        &["accent", "success", "error", "warning"],
        &[
            "background",
            "selectedBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &["muted"],
        &[
            "background",
            "selectedBg",
            "customMessageBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &["dim"],
        &[
            "background",
            "selectedBg",
            "customMessageBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "subtle",
    );
    add(&["thinkingText"], &["background"], "readable");
    add(
        &["customMessageText"],
        &[
            "customMessageBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &["customMessageLabel"],
        &[
            "background",
            "customMessageBg",
            "selectedBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &["toolOutput"],
        &[
            "background",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &[
            "mdHeading",
            "mdLink",
            "mdLinkUrl",
            "mdCode",
            "mdQuote",
            "mdCodeBlockBorder",
            "mdListBullet",
        ],
        &["background", "userMessageBg", "customMessageBg"],
        "readable",
    );
    add(
        &["mdCodeBlock"],
        &[
            "background",
            "userMessageBg",
            "customMessageBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &["toolDiffAdded", "toolDiffRemoved", "toolDiffContext"],
        &[
            "background",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(
        &[
            "syntaxComment",
            "syntaxKeyword",
            "syntaxFunction",
            "syntaxVariable",
            "syntaxString",
            "syntaxNumber",
            "syntaxType",
            "syntaxOperator",
            "syntaxPunctuation",
        ],
        &[
            "background",
            "userMessageBg",
            "customMessageBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(&["searchMatchText"], &["searchMatchBg"], "readable");
    add(
        &["bashMode", "border", "borderAccent"],
        &["background"],
        "readable",
    );
    add(&["borderMuted"], &["background"], "subtle");
    add(
        &["mdQuoteBorder", "mdHr"],
        &[
            "background",
            "userMessageBg",
            "customMessageBg",
            "toolPendingBg",
            "toolSuccessBg",
            "toolErrorBg",
        ],
        "readable",
    );
    add(&["scrollbarTrack"], &["background"], "track");
    add(&["scrollbarThumb"], &["scrollbarTrack"], "thumb");
    for (token, level) in [
        "thinkingOff",
        "thinkingMinimal",
        "thinkingLow",
        "thinkingMedium",
        "thinkingHigh",
        "thinkingXhigh",
        "thinkingMax",
    ]
    .into_iter()
    .zip([
        "thinking0",
        "thinking1",
        "thinking2",
        "thinking3",
        "thinking4",
        "thinking5",
        "thinking6",
    ]) {
        add(&[token], &["background"], level);
    }
    r
}
fn lightness(c: Rgb) -> f64 {
    math::rgb_to_lab(c)[0]
}
fn hsl_lightness(x: f64) -> f64 {
    math::hsl_lightness(x)
}
fn source(c: Rgb) -> Source {
    let [h, s, l] = math::rgb_to_hsl(c);
    let lab = math::rgb_to_lab(c);
    Source {
        h,
        s,
        l,
        c: lab[1].hypot(lab[2]),
    }
}
fn hsl(h: f64, s: f64, l: f64) -> Rgb {
    math::hsl_to_rgb(h, s.clamp(0., 1.), l.clamp(0., 1.))
}
fn bell(l: f64) -> f64 {
    let gaussian = |x: f64| (-((x - 0.5).powi(2)) / (2.0 * 0.25_f64.powi(2))).exp();
    (gaussian(l) - gaussian(0.)) / (1. - gaussian(0.))
}
fn sat_curve(f: Family, l: f64) -> f64 {
    let floor = if f.max > 0. { f.min / f.max } else { 1. };
    floor + (1. - floor) * bell(l)
}
// Retain the palette hue while limiting chroma to the source color and its falloff.
fn anchored(src: Source, f: Family, l: f64, sat: f64) -> Rgb {
    let anchor = sat_curve(f, src.l);
    let fall = if anchor > 0. {
        (sat_curve(f, l) / anchor).min(1.)
    } else {
        1.
    };
    let color = hsl(src.h, src.s * fall * sat, l);
    let cap = src.c * fall * sat;
    let lab = math::rgb_to_lab(color);
    let c = lab[1].hypot(lab[2]);
    if c <= cap {
        color
    } else {
        math::lch_to_rgb(lab[0], cap, src.h)
    }
}
fn contrast(a: Rgb, b: Rgb) -> f64 {
    let lum = |c: Rgb| {
        let linear = |channel: u8| {
            let value = channel as f64 / 255.0;
            if value <= 0.04045 {
                value / 12.92
            } else {
                ((value + 0.055) / 1.055).powf(2.4)
            }
        };
        0.2126 * linear(c[0]) + 0.7152 * linear(c[1]) + 0.0722 * linear(c[2])
    };
    let x = lum(a);
    let y = lum(b);
    (x.max(y) + 0.05) / (x.min(y) + 0.05)
}
fn level_target(level: &str, dark: bool, l: f64) -> Option<f64> {
    let curve = LEVELS.iter().find(|(n, _)| *n == level).unwrap().1[if dark { 0 } else { 1 }];
    if l < curve.reachable[0] || l > curve.reachable[1] {
        return None;
    }
    Some(
        curve
            .coefficients
            .iter()
            .enumerate()
            .map(|(i, c)| c * l.powi(i as i32))
            .sum(),
    )
}
// First compress strong levels toward the readable floor, then toward the surface.
fn target(level: &str, dark: bool, l: f64, t: f64) -> Option<f64> {
    let reached = level_target(level, dark, l);
    if reached.is_none() && t == 0. {
        return None;
    }
    let extreme = if dark { 1. } else { 0. };
    let d = reached.unwrap_or(extreme) - l;
    let floor =
        level_target(if dark { "readable" } else { "subtle" }, dark, l).unwrap_or(extreme) - l;
    let compressed = if d.abs() > floor.abs() {
        d - (d - floor) * t.min(1.)
    } else {
        d
    };
    Some(l + compressed * (1. - (t - 1.).max(0.)))
}
// Surfaces must be solved before the colors drawn on them.
fn order(rules: &[Rule]) -> Vec<&'static str> {
    fn visit(t: &'static str, r: &[Rule], out: &mut Vec<&'static str>) {
        if out.contains(&t) {
            return;
        }
        for rule in r.iter().filter(|r| r.token == t) {
            for &s in &rule.surfaces {
                if s != "background" {
                    visit(s, r, out)
                }
            }
        }
        out.push(t);
    }
    let mut out = Vec::new();
    for r in rules {
        visit(r.token, rules, &mut out)
    }
    out
}
fn with_text_contrast(color: Rgb, surfaces: &[Rgb], dark: bool) -> Rgb {
    let meets = |c| surfaces.iter().all(|s| contrast(c, *s) >= 4.5);
    if meets(color) {
        return color;
    }
    let src = source(color);
    let extreme = if dark { 1. } else { 0. };
    let at = |l| hsl(src.h, src.s, l);
    if !meets(at(extreme)) {
        return at(extreme);
    }
    let (mut low, mut high) = (src.l, extreme);
    for _ in 0..20 {
        let mid = (low + high) / 2.;
        if meets(at(mid)) {
            high = mid
        } else {
            low = mid
        }
    }
    at(high)
}

pub(super) struct Generated {
    pub colors: BTreeMap<String, Color>,
    pub dim: BTreeSet<String>,
    // Kept as part of the upstream result; the renderer currently stores appearance separately.
    #[allow(dead_code)]
    pub dark: Option<bool>,
}
pub(super) fn generate(
    background: Option<Rgb>,
    foreground: Option<Rgb>,
    palette: Option<&[Rgb]>,
    saturation: f64,
    hint: Option<bool>,
) -> Generated {
    let saturation = saturation.clamp(0., 1.);
    let Some(background) = background else {
        let mut colors = BTreeMap::new();
        let mut dim = BTreeSet::new();
        for &(token, fname) in TOKENS {
            let f = family(fname);
            let color = if PANELS.contains(&token) || fname == "neutral" || saturation == 0. {
                Color::Reset
            } else {
                Color::AnsiValue(slot(token, f))
            };
            if fname == "neutral" && !PANELS.contains(&token) && !FOREGROUND.contains(&token) {
                dim.insert(token.into());
            }
            colors.insert(token.into(), color);
        }
        return Generated {
            colors,
            dim,
            dark: hint,
        };
    };
    let dark = super::theme::terminal_is_dark(background, foreground);
    let bg_l = lightness(background);
    let palette = palette
        .filter(|p| p.len() == 16)
        .map(|p| p.iter().copied().map(source).collect::<Vec<_>>());
    let paint = |token: &str, l: f64| {
        let l = hsl_lightness(l).clamp(0., 1.);
        let f = token_family(token);
        match &palette {
            Some(p) => anchored(p[slot(token, f) as usize], f, l, saturation),
            None => hsl(f.hue, (f.min + (f.max - f.min) * bell(l)) * saturation, l),
        }
    };
    let rules = rules();
    let order = order(&rules);
    let extreme = if dark { [255, 255, 255] } else { [0, 0, 0] };
    // Cap panel lightness so extreme body text can still achieve 4.5:1.
    let solve = |t: f64| -> Option<BTreeMap<&str, Rgb>> {
        let mut colors = BTreeMap::new();
        colors.insert("background", background);
        for &token in &order {
            let mut targets = Vec::new();
            for rule in rules.iter().filter(|r| r.token == token) {
                for surface in &rule.surfaces {
                    let l = lightness(*colors.get(surface).unwrap_or(&background));
                    let value = target(rule.level, dark, l, t)?;
                    if !(0. ..=1.).contains(&value) {
                        return None;
                    }
                    targets.push(value);
                }
            }
            let l = targets
                .into_iter()
                .reduce(|a, b| if dark { a.max(b) } else { a.min(b) })
                .unwrap();
            let mut color = paint(token, l);
            if PANELS.contains(&token) && contrast(extreme, color) < 4.5 {
                let (mut low, mut high) = (bg_l, l);
                for _ in 0..20 {
                    let mid = (low + high) / 2.;
                    if contrast(extreme, paint(token, mid)) >= 4.5 {
                        low = mid
                    } else {
                        high = mid
                    }
                }
                color = paint(token, low);
            }
            colors.insert(token, color);
        }
        Some(colors)
    };
    let mut relaxation = 0.;
    let mut solved = solve(0.);
    if solved.is_none() {
        // Mid-gray can make the unrelaxed levels unreachable; find the least relaxation that fits.
        let (mut low, mut high) = (0., 2.);
        solved = solve(high);
        for _ in 0..20 {
            let mid = (low + high) / 2.;
            if let Some(c) = solve(mid) {
                high = mid;
                solved = Some(c)
            } else {
                low = mid
            }
        }
        relaxation = high;
    }
    let solved = solved.unwrap();
    let mut result = BTreeMap::new();
    for &(token, _) in TOKENS {
        let [r, g, b] = solved[token];
        result.insert(token.into(), Color::Rgb { r, g, b });
    }
    for &token in FOREGROUND {
        let surfaces: Vec<Rgb> = rules
            .iter()
            .filter(|r| r.token == token)
            .flat_map(|r| {
                r.surfaces
                    .iter()
                    .map(|s| *solved.get(s).unwrap_or(&background))
            })
            .collect();
        let mut text = solved[token];
        if let Some(foreground) = foreground {
            let targets: Option<Vec<f64>> = surfaces
                .iter()
                .map(|s| {
                    target("emphasis", dark, lightness(*s), relaxation)
                        .filter(|v| (0. ..=1.).contains(v))
                })
                .collect();
            if let Some(targets) = targets {
                let needed = targets
                    .into_iter()
                    .reduce(|a, b| if dark { a.max(b) } else { a.min(b) })
                    .unwrap();
                let fl = lightness(foreground);
                if if dark { fl >= needed } else { fl <= needed } {
                    result.insert(token.into(), Color::Reset);
                    continue;
                }
                text = anchored(
                    source(foreground),
                    family("neutral"),
                    hsl_lightness(needed),
                    saturation,
                );
            }
        }
        let [r, g, b] = with_text_contrast(text, &surfaces, dark);
        result.insert(token.into(), Color::Rgb { r, g, b });
    }
    Generated {
        colors: result,
        dim: BTreeSet::new(),
        dark: Some(dark),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    fn rgb_value(v: &Value) -> Rgb {
        [
            v["r"].as_u64().unwrap() as u8,
            v["g"].as_u64().unwrap() as u8,
            v["b"].as_u64().unwrap() as u8,
        ]
    }
    #[test]
    fn matches_upstream_fixtures() {
        let cases: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/system-theme/upstream-v1.5.0.json"
        ))
        .unwrap();
        let mut failures = Vec::new();
        for case in cases.as_array().unwrap() {
            let input = &case["input"];
            let output = &case["output"];
            let palette = input["palette"]
                .as_array()
                .map(|a| a.iter().map(rgb_value).collect::<Vec<_>>());
            let generated = generate(
                input.get("background").map(rgb_value),
                input.get("foreground").map(rgb_value),
                palette.as_deref(),
                input["saturation"].as_f64().unwrap_or(1.),
                input["appearanceHint"].as_str().map(|s| s == "dark"),
            );
            assert_eq!(
                generated.colors.len(),
                output["colors"].as_object().unwrap().len(),
                "{} role count",
                case["name"]
            );
            for (token, expected) in output["colors"].as_object().unwrap() {
                let actual = generated.colors[token];
                let expected = if let Some(n) = expected.as_u64() {
                    Color::AnsiValue(n as u8)
                } else {
                    let s = expected.as_str().unwrap();
                    if s.is_empty() {
                        Color::Reset
                    } else {
                        Color::Rgb {
                            r: u8::from_str_radix(&s[1..3], 16).unwrap(),
                            g: u8::from_str_radix(&s[3..5], 16).unwrap(),
                            b: u8::from_str_radix(&s[5..7], 16).unwrap(),
                        }
                    }
                };
                if actual != expected {
                    failures.push(format!(
                        "{} {token}: {actual:?} != {expected:?}",
                        case["name"]
                    ));
                }
            }
            let dim: BTreeSet<String> = output["dim"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().into())
                .collect();
            assert_eq!(generated.dim, dim, "{} dim", case["name"]);
            assert_eq!(
                generated.dark,
                output
                    .get("appearance")
                    .and_then(Value::as_str)
                    .map(|s| s == "dark"),
                "{} appearance",
                case["name"]
            );
        }
        assert!(
            failures.is_empty(),
            "{} mismatches:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
    #[test]
    fn foreground_is_used_only_when_readable() {
        let bright = generate(Some([0, 0, 0]), Some([255, 255, 255]), None, 1.0, None);
        let weak = generate(Some([0, 0, 0]), Some([40, 40, 40]), None, 1.0, None);
        assert_eq!(bright.colors["text"], Color::Reset);
        let Color::Rgb { r, g, b } = weak.colors["text"] else {
            panic!("weak foreground must be adjusted")
        };
        assert!(contrast([r, g, b], [0, 0, 0]) >= 4.5);
        assert!(weak.dim.is_empty());
    }

    #[test]
    fn partial_palette_uses_family_hues() {
        let background = Some([30, 30, 30]);
        let partial = [[255, 0, 0]; 15];
        let plain = generate(background, None, None, 1.0, None);
        let incomplete = generate(background, None, Some(&partial), 1.0, None);
        assert_eq!(plain.colors, incomplete.colors);
    }
}
