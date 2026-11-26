//! Native package resource matching shared by the CLI and bounded SDK discovery.
use std::path::Path;

use crate::package_manifest::{resource_glob, skill_directory};

/// A native resource pattern after the caller has resolved its path syntax.
/// Matching is lexical and performs no filesystem or environment lookups.
pub struct ResourceMatcher<'a> {
    pattern: &'a str,
    exact: bool,
    directory_roots: bool,
    negated: bool,
    glob: Option<globset::GlobMatcher>,
    positive_extglob: Option<regex::Regex>,
    negative_extglob: Option<fancy_regex::Regex>,
    subtree: Option<String>,
}

impl<'a> ResourceMatcher<'a> {
    pub fn new(pattern: &'a str, exact: bool, directory_roots: bool) -> Self {
        let disabled = exact || (!directory_roots && pattern.starts_with('#'));
        let (pattern, negated) = if !exact && !directory_roots {
            let target = pattern.trim_start_matches('!');
            (target, (pattern.len() - target.len()) % 2 == 1)
        } else {
            (pattern, false)
        };
        let pattern = if exact {
            pattern
        } else {
            pattern.trim_start_matches("./")
        };
        Self {
            pattern,
            exact,
            directory_roots,
            negated,
            glob: (!disabled)
                .then(|| {
                    resource_glob(pattern, !directory_roots)
                        .ok()
                        .map(|glob| glob.compile_matcher())
                })
                .flatten(),
            positive_extglob: (!disabled)
                .then(|| extglob_regex(pattern).flatten())
                .flatten(),
            negative_extglob: (!disabled && !directory_roots)
                .then(|| negative_extglob_regex(pattern))
                .flatten(),
            subtree: (!exact && directory_roots && !pattern.contains(['*', '?', '[', '{']))
                .then(|| format!("{pattern}/")),
        }
    }

    pub fn matches(&self, path: &Path, root: &Path) -> bool {
        let path = dunce::simplified(path);
        let root = dunce::simplified(root);
        let normalize = |path: &Path| path.to_string_lossy().replace('\\', "/");
        let mut candidates = vec![
            normalize(path),
            normalize(path.strip_prefix(root).unwrap_or(path)),
        ];
        if let Some(parent) = skill_directory(path) {
            candidates.push(normalize(parent));
            candidates.push(normalize(parent.strip_prefix(root).unwrap_or(parent)));
            if !self.exact
                && let Some(name) = parent.file_name()
            {
                candidates.push(name.to_string_lossy().into_owned());
            }
        }
        if self.exact {
            return candidates.iter().any(|candidate| candidate == self.pattern);
        }
        if let Some(name) = path.file_name() {
            candidates.push(name.to_string_lossy().into_owned());
        }

        candidates
            .iter()
            .any(|candidate| self.matches_candidate(candidate))
    }

    fn matches_candidate(&self, candidate: &str) -> bool {
        // The extglob translators cover ASCII paths without hidden components.
        // Other spellings keep using the native glob matcher.
        if candidate.is_ascii() && !candidate.split('/').any(|part| part.starts_with('.')) {
            let matched = if let Some(regex) = &self.negative_extglob {
                regex.is_match(candidate).ok()
            } else {
                self.positive_extglob
                    .as_ref()
                    .map(|regex| regex.is_match(candidate))
            };
            if let Some(matched) = matched {
                return matched != self.negated;
            }
        }
        let Some(glob) = &self.glob else {
            return false;
        };
        let matched = glob.is_match(candidate)
            && (self.directory_roots || matches_hidden_filter_segments(candidate, self.pattern));
        matched != self.negated
            || self
                .subtree
                .as_ref()
                .is_some_and(|prefix| candidate.starts_with(prefix))
    }
}

pub fn has_extglob(pattern: &str) -> bool {
    pattern
        .as_bytes()
        .windows(2)
        .any(|w| matches!(w[0], b'@' | b'?' | b'*' | b'+' | b'!') && w[1] == b'(')
}

// Minimatch requires a leading dot in the pattern segment that matches a
// hidden component. A globstar can skip zero segments or consume visible ones.
fn matches_hidden_filter_segments(candidate: &str, pattern: &str) -> bool {
    if !candidate.split('/').any(|segment| segment.starts_with('.')) {
        return true;
    }
    if pattern.contains('{') {
        // Keep the existing glob match for brace combinations outside the
        // bounded expansion. A leading dot can come from any alternative.
        return expand_diagnostic_braces(pattern).is_none_or(|variants| {
            variants
                .iter()
                .any(|variant| matches_hidden_filter_segments(candidate, variant))
        });
    }
    let components: Vec<_> = candidate.split('/').collect();
    let mut matched = vec![false; components.len() + 1];
    matched[0] = true;
    for segment in pattern.split('/') {
        if segment == "**" {
            for (index, component) in components.iter().enumerate() {
                matched[index + 1] |= matched[index] && !component.starts_with('.');
            }
            continue;
        }
        let Ok(glob) = resource_glob(segment, true) else {
            return false;
        };
        let matcher = glob.compile_matcher();
        let allows_dot = segment.starts_with('.') || segment.starts_with("[.]");
        for (index, component) in components.iter().enumerate().rev() {
            matched[index + 1] = matched[index]
                && (allows_dot || !component.starts_with('.'))
                && matcher.is_match(component);
        }
        matched[0] = false;
    }
    matched[components.len()]
}

pub const MAX_EXTGLOB_PATTERN: usize = 1024;
pub const MAX_EXTGLOB_DEPTH: usize = 16;
pub const NEGATIVE_BACKTRACK_LIMIT: usize = 2_000;
const MAX_BRACE_VARIANTS: usize = 128;
const MAX_BRACE_PATTERN_BYTES: usize = 65_536;
const MAX_BRACE_INPUT: usize = 4_096;
pub const MAX_BRACE_DEPTH: usize = 16;

// Expand only syntax whose Node semantics we can establish. Bounds apply to
// the whole expansion, before any filesystem traversal or matcher compilation.
pub fn expand_diagnostic_braces(pattern: &str) -> Option<Vec<String>> {
    fn expand(pattern: &str, depth: usize, out: &mut Vec<String>, bytes: &mut usize) -> Option<()> {
        if depth > MAX_BRACE_DEPTH || pattern.len() > MAX_BRACE_INPUT {
            return None;
        }
        let Some(start) = pattern.find('{') else {
            if pattern.contains('}') || out.len() >= MAX_BRACE_VARIANTS {
                return None;
            }
            *bytes = bytes.checked_add(pattern.len())?;
            if *bytes > MAX_BRACE_PATTERN_BYTES {
                return None;
            }
            out.push(pattern.to_owned());
            return Some(());
        };
        let mut level = 0usize;
        let mut end = None;
        let mut separators = Vec::new();
        for (i, ch) in pattern.char_indices().skip_while(|(i, _)| *i < start) {
            match ch {
                '{' => {
                    level += 1;
                    if level > MAX_BRACE_DEPTH {
                        return None;
                    }
                }
                '}' => {
                    level = level.checked_sub(1)?;
                    if level == 0 {
                        end = Some(i);
                        break;
                    }
                }
                ',' if level == 1 => separators.push(i),
                _ => {}
            }
        }
        let end = end?;
        let body = &pattern[start + 1..end];
        let mut alternatives = Vec::new();
        if !separators.is_empty() {
            let mut begin = start + 1;
            for stop in separators.into_iter().chain(std::iter::once(end)) {
                let part = &pattern[begin..stop];
                if part.is_empty() {
                    return None;
                }
                alternatives.push(part.to_owned());
                begin = stop + 1;
            }
        } else {
            let parts: Vec<_> = body.split("..").collect();
            if !(2..=3).contains(&parts.len()) {
                return None;
            }
            let numeric = |s: &str| {
                !s.is_empty()
                    && s.trim_start_matches('-')
                        .bytes()
                        .all(|b| b.is_ascii_digit())
                    && s != "-"
                    && !s.starts_with("--")
            };
            let step = if parts.len() == 3 {
                if !numeric(parts[2]) {
                    return None;
                }
                parts[2].parse::<i64>().ok()?.checked_abs()?.max(1)
            } else {
                1
            };
            let (first, last, width, alphabetic) = if numeric(parts[0]) && numeric(parts[1]) {
                let padded = parts.iter().any(|s| {
                    let digits = s.strip_prefix('-').unwrap_or(s);
                    digits.len() > 1 && digits.starts_with('0')
                });
                (
                    parts[0].parse::<i64>().ok()?,
                    parts[1].parse::<i64>().ok()?,
                    if padded {
                        parts[0].len().max(parts[1].len())
                    } else {
                        0
                    },
                    false,
                )
            } else if parts[0].len() == 1
                && parts[1].len() == 1
                && ((parts[0].as_bytes()[0].is_ascii_lowercase()
                    && parts[1].as_bytes()[0].is_ascii_lowercase())
                    || (parts[0].as_bytes()[0].is_ascii_uppercase()
                        && parts[1].as_bytes()[0].is_ascii_uppercase()))
            {
                (
                    parts[0].as_bytes()[0] as i64,
                    parts[1].as_bytes()[0] as i64,
                    0,
                    true,
                )
            } else {
                return None;
            };
            // JavaScript range expansion uses Number; do not infer results
            // outside its exact integer range.
            if [first, last, step]
                .iter()
                .any(|n| i128::from(*n).abs() > 9_007_199_254_740_991)
            {
                return None;
            }
            let distance = (i128::from(last) - i128::from(first)).unsigned_abs();
            if distance / step as u128 + 1 > MAX_BRACE_VARIANTS as u128 {
                return None;
            }
            let mut value = first;
            loop {
                alternatives.push(if alphabetic {
                    char::from(value as u8).to_string()
                } else if width > 0 {
                    if value < 0 {
                        format!("-{:0width$}", value.unsigned_abs(), width = width - 1)
                    } else {
                        format!("{value:0width$}")
                    }
                } else {
                    value.to_string()
                });
                let Some(next) = (if first <= last {
                    value.checked_add(step)
                } else {
                    value.checked_sub(step)
                }) else {
                    break;
                };
                if (first <= last && next > last) || (first > last && next < last) {
                    break;
                }
                value = next;
            }
        }
        for alternative in alternatives {
            expand(
                &format!(
                    "{}{}{}",
                    &pattern[..start],
                    alternative,
                    &pattern[end + 1..]
                ),
                depth + 1,
                out,
                bytes,
            )?;
        }
        Some(())
    }
    if pattern.contains(['[', ']']) || has_extglob(pattern) {
        return None;
    }
    let mut out = Vec::new();
    let mut bytes = 0;
    expand(pattern, 0, &mut out, &mut bytes)?;
    Some(out)
}

// One top-level negation with literal alternatives and a simple component
// suffix. Anything more complex must keep using the diagnostic fallback.
pub fn negative_extglob_regex(pattern: &str) -> Option<fancy_regex::Regex> {
    if pattern.len() > MAX_EXTGLOB_PATTERN
        || !pattern.is_ascii()
        || pattern.contains(['[', ']', '{', '}'])
        || pattern.contains("**")
    {
        return None;
    }
    let start = pattern.find("!(")?;
    if has_extglob(&pattern[..start]) {
        return None;
    }
    let end = start + 2 + pattern[start + 2..].find(')')?;
    let alternatives = &pattern[start + 2..end];
    if alternatives.split('|').any(|s| {
        s.is_empty()
            || !s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    }) {
        return None;
    }
    let after = &pattern[end + 1..];
    if has_extglob(after) || after.contains(['(', ')', '|']) {
        return None;
    }
    let suffix_end = after.find('/').unwrap_or(after.len());
    let (suffix, rest) = after.split_at(suffix_end);
    // Node's minimatch handles a terminal wildcard after !() differently
    // from the suffix-aware lookahead below (including an empty match).
    if suffix.ends_with(['*', '?']) {
        return None;
    }
    fn simple(pattern: &str) -> Option<String> {
        let mut result = String::new();
        for c in pattern.chars() {
            match c {
                '*' => result.push_str("[^/]*?"),
                '?' => result.push_str("[^/]"),
                '(' | ')' | '|' => return None,
                _ => result.push_str(&regex::escape(&c.to_string())),
            }
        }
        Some(result)
    }
    let prefix = simple(&pattern[..start])?;
    let suffix = simple(suffix)?;
    let rest = simple(rest)?;
    let alternatives = alternatives
        .split('|')
        .map(regex::escape)
        .collect::<Vec<_>>()
        .join("|");
    let expression =
        format!(r"\A{prefix}(?!(?:{alternatives}){suffix}(?:/|\z))[^/]*?{suffix}{rest}\z");
    fancy_regex::RegexBuilder::new(&expression)
        .backtrack_limit(NEGATIVE_BACKTRACK_LIMIT)
        .delegate_size_limit(1024 * 1024)
        .delegate_dfa_size_limit(1024 * 1024)
        .build()
        .ok()
}

// This deliberately supports a bounded subset. In particular negated extglobs,
// classes, braces and globstars require Node-specific semantics; falling back
// is preferable to reporting a partial or incorrectly empty candidate set.
pub fn extglob_regex(pattern: &str) -> Option<Option<regex::Regex>> {
    let bytes = pattern.as_bytes();
    if !has_extglob(pattern) {
        return Some(None);
    }
    if bytes.len() > MAX_EXTGLOB_PATTERN
        || !pattern.is_ascii()
        || pattern.contains(['[', ']', '{', '}'])
        || pattern.contains("**")
    {
        return None;
    }
    fn sequence(bytes: &[u8], pos: &mut usize, depth: usize) -> Option<String> {
        if depth > MAX_EXTGLOB_DEPTH {
            return None;
        }
        let mut out = String::new();
        while *pos < bytes.len() {
            let ch = bytes[*pos];
            if depth > 0 && matches!(ch, b'|' | b')') {
                break;
            }
            if matches!(ch, b'@' | b'?' | b'*' | b'+' | b'!') && bytes.get(*pos + 1) == Some(&b'(')
            {
                if ch == b'!' {
                    return None;
                }
                *pos += 2;
                let mut alternatives = Vec::new();
                loop {
                    alternatives.push(sequence(bytes, pos, depth + 1)?);
                    match bytes.get(*pos) {
                        Some(b'|') => *pos += 1,
                        Some(b')') => {
                            *pos += 1;
                            break;
                        }
                        _ => return None,
                    }
                }
                let quantifier = match ch {
                    b'@' => "",
                    b'?' => "?",
                    b'*' => "*",
                    b'+' => "+",
                    _ => return None,
                };
                out.push_str(&format!("(?:{}){quantifier}", alternatives.join("|")));
            } else {
                // Bare parentheses inside an extglob are parsed differently by
                // Node's matcher; do not infer a partial interpretation.
                if depth > 0 && ch == b'(' {
                    return None;
                }
                *pos += 1;
                match ch {
                    b'*' => out.push_str("[^/]*"),
                    b'?' => out.push_str("[^/]"),
                    b'/' if depth > 0 => return None,
                    _ => out.push_str(&regex::escape(&(ch as char).to_string())),
                }
            }
        }
        Some(out)
    }
    let mut pos = 0;
    let expression = sequence(bytes, &mut pos, 0)?;
    if pos != bytes.len() {
        return None;
    }
    let regex = regex::RegexBuilder::new(&format!("\\A{expression}\\z"))
        .size_limit(1024 * 1024)
        .build()
        .ok()?;
    Some(Some(regex))
}
