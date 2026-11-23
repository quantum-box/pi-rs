//! Shared pure parser for file-based themes.
use crossterm::style::Color;
use palette::convert::FromColorUnclamped;
use palette::{FromColor, LinSrgb, Okhsl, Oklch, Srgb};
use pi_rs::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
struct Definition {
    name: String,
    #[serde(default)]
    vars: BTreeMap<String, Value>,
    colors: BTreeMap<String, Value>,
}
/// Parsed theme data shared by the SDK and interactive frontend.
#[doc(hidden)]
pub struct ParsedTheme {
    pub name: String,
    pub colors: BTreeMap<String, Color>,
}

/// Parse and resolve a Pi theme without filesystem or terminal access.
#[doc(hidden)]
pub fn parse_theme(text: &str) -> Result<ParsedTheme> {
    let value: Value = serde_json::from_str(text)?;
    let schema: Value = serde_json::from_str(include_str!("../assets/theme.schema.json"))?;
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
    Ok(ParsedTheme {
        name: definition.name,
        colors,
    })
}

#[doc(hidden)]
pub fn resolve(
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
