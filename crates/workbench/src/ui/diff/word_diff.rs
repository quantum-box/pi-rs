//! Word diff compatible with jsdiff 8.0.4's default diffWords behavior.
//! Keep this bounded: an unfinished comparison falls back to line-level color.
//! Derived from jsdiff by Kevin Decker and contributors; see JSDIFF-LICENSE.txt.
use std::{collections::HashMap, rc::Rc, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Tag {
    Equal,
    Delete,
    Insert,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Change {
    pub tag: Tag,
    pub text: String,
}
#[derive(Clone)]
struct Component {
    tag: Tag,
    count: usize,
    prev: Option<Rc<Component>>,
}
#[derive(Clone)]
struct Path {
    old: isize,
    last: Option<Rc<Component>>,
}

// ECMAScript WhiteSpace and LineTerminator, used by jsdiff's /\s/ and trim.
pub(super) fn ws(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
fn word(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || c == '_'
        || matches!(c as u32, 0xad | 0xc0..=0xd6 | 0xd8..=0xf6 | 0xf8..=0x2c6 | 0x2c8..=0x2d7 | 0x2de..=0x2ff | 0x1e00..=0x1eff)
}
fn tokenize(s: &str, deadline: Instant) -> Option<Vec<String>> {
    let mut parts: Vec<&str> = Vec::new();
    let mut chars = s.char_indices().peekable();
    while let Some((start, c)) = chars.next() {
        if Instant::now() >= deadline {
            return None;
        }
        let kind = if ws(c) {
            0
        } else if word(c) {
            1
        } else {
            2
        };
        let mut end = start + c.len_utf8();
        if kind != 2 {
            while let Some(&(i, ch)) = chars.peek() {
                if (kind == 0 && ws(ch)) || (kind == 1 && word(ch)) {
                    chars.next();
                    end = i + ch.len_utf8();
                } else {
                    break;
                }
            }
        }
        parts.push(&s[start..end]);
        if parts.len() > 4096 {
            return None;
        }
    }
    let mut tokens: Vec<String> = Vec::new();
    let mut prev: Option<&str> = None;
    for part in parts {
        if part.chars().next().is_some_and(ws) {
            if prev.is_none() {
                tokens.push(part.into());
            } else {
                tokens.last_mut().unwrap().push_str(part);
            }
        } else if prev.is_some_and(|p| p.chars().next().is_some_and(ws)) {
            let p = prev.unwrap();
            if tokens.last().is_some_and(|t| t == p) {
                tokens.last_mut().unwrap().push_str(part);
            } else {
                tokens.push(format!("{p}{part}"));
            }
        } else {
            tokens.push(part.into());
        }
        prev = Some(part);
    }
    Some(tokens)
}
fn add(path: &Path, tag: Tag, count: usize, old_inc: isize) -> Path {
    let (count, prev) = match &path.last {
        Some(last) if last.tag == tag => (count + last.count, last.prev.clone()),
        _ => (count, path.last.clone()),
    };
    Path {
        old: path.old + old_inc,
        last: Some(Rc::new(Component { tag, count, prev })),
    }
}
fn common(
    path: &mut Path,
    diagonal: isize,
    old: &[String],
    new: &[String],
    deadline: Instant,
) -> Option<isize> {
    let mut n = path.old - diagonal;
    let mut count = 0;
    while (path.old + 1) < old.len() as isize
        && n + 1 < new.len() as isize
        && old[(path.old + 1) as usize].trim_matches(ws) == new[(n + 1) as usize].trim_matches(ws)
    {
        if Instant::now() >= deadline {
            return None;
        }
        path.old += 1;
        n += 1;
        count += 1;
    }
    if count > 0 {
        *path = add(path, Tag::Equal, count, 0);
    }
    Some(n)
}
fn joined(tokens: &[String]) -> String {
    let mut out = String::new();
    for (i, t) in tokens.iter().enumerate() {
        out.push_str(if i == 0 { t } else { t.trim_start_matches(ws) });
    }
    out
}
fn make_changes(path: Path, old: &[String], new: &[String]) -> Vec<Change> {
    let mut comps = Vec::new();
    let mut node = path.last;
    while let Some(c) = node {
        comps.push((c.tag, c.count));
        node = c.prev.clone();
    }
    comps.reverse();
    let (mut o, mut n) = (0, 0);
    let mut result = Vec::new();
    for (tag, count) in comps {
        let text = if tag == Tag::Delete {
            let v = joined(&old[o..o + count]);
            o += count;
            v
        } else {
            let v = joined(&new[n..n + count]);
            n += count;
            if tag == Tag::Equal {
                o += count;
            }
            v
        };
        result.push(Change { tag, text });
    }
    result
}
fn prefix(s: &str) -> &str {
    &s[..s.len() - s.trim_start_matches(ws).len()]
}
fn suffix(s: &str) -> &str {
    &s[s.trim_end_matches(ws).len()..]
}
fn common_prefix<'a>(a: &'a str, b: &str) -> &'a str {
    let mut len = 0;
    for (x, y) in a.chars().zip(b.chars()) {
        if x != y {
            break;
        }
        len += x.len_utf8();
    }
    &a[..len]
}
fn common_suffix<'a>(a: &'a str, b: &str) -> &'a str {
    let mut len = 0;
    for (x, y) in a.chars().rev().zip(b.chars().rev()) {
        if x != y {
            break;
        }
        len += x.len_utf8();
    }
    &a[a.len() - len..]
}
fn replace_prefix(s: &mut String, old: &str, new: &str) {
    debug_assert!(s.starts_with(old));
    s.replace_range(..old.len(), new);
}
fn replace_suffix(s: &mut String, old: &str, new: &str) {
    debug_assert!(s.ends_with(old));
    s.replace_range(s.len() - old.len().., new);
}
fn overlap(a: &str, b: &str) -> usize {
    // KMP keeps long whitespace runs linear and matches whole Unicode scalars.
    let pattern: Vec<char> = b.chars().collect();
    if pattern.is_empty() {
        return 0;
    }
    let mut failure = vec![0; pattern.len()];
    let mut k = 0;
    for i in 1..pattern.len() {
        while k > 0 && pattern[i] != pattern[k] {
            k = failure[k - 1];
        }
        if pattern[i] == pattern[k] {
            k += 1;
        }
        failure[i] = k;
    }
    k = 0;
    for c in a.chars() {
        while k > 0 && (k == pattern.len() || c != pattern[k]) {
            k = failure[k - 1];
        }
        if c == pattern[k] {
            k += 1;
        }
    }
    pattern[..k].iter().map(|c| c.len_utf8()).sum()
}
fn dedupe(
    changes: &mut [Change],
    start: Option<usize>,
    del: Option<usize>,
    ins: Option<usize>,
    end: Option<usize>,
) {
    match (del, ins) {
        (Some(d), Some(i)) => {
            let (op, os, np, ns) = (
                prefix(&changes[d].text).to_owned(),
                suffix(&changes[d].text).to_owned(),
                prefix(&changes[i].text).to_owned(),
                suffix(&changes[i].text).to_owned(),
            );
            if let Some(k) = start {
                let p = common_prefix(&op, &np).to_owned();
                replace_suffix(&mut changes[k].text, &np, &p);
                replace_prefix(&mut changes[d].text, &p, "");
                replace_prefix(&mut changes[i].text, &p, "");
            }
            if let Some(k) = end {
                let s = common_suffix(&os, &ns).to_owned();
                replace_prefix(&mut changes[k].text, &ns, &s);
                replace_suffix(&mut changes[d].text, &s, "");
                replace_suffix(&mut changes[i].text, &s, "");
            }
        }
        (None, Some(i)) => {
            if start.is_some() {
                let p = prefix(&changes[i].text).len();
                changes[i].text.drain(..p);
            }
            if let Some(k) = end {
                let p = prefix(&changes[k].text).len();
                changes[k].text.drain(..p);
            }
        }
        (Some(d), None) => match (start, end) {
            (Some(a), Some(b)) => {
                let full = prefix(&changes[b].text).to_owned();
                let p = common_prefix(&full, prefix(&changes[d].text)).to_owned();
                replace_prefix(&mut changes[d].text, &p, "");
                let s = common_suffix(&full[p.len()..], suffix(&changes[d].text)).to_owned();
                replace_suffix(&mut changes[d].text, &s, "");
                replace_prefix(&mut changes[b].text, &full, &s);
                replace_suffix(&mut changes[a].text, &full, &full[..full.len() - s.len()]);
            }
            (None, Some(b)) => {
                let n = overlap(suffix(&changes[d].text), prefix(&changes[b].text));
                let len = changes[d].text.len();
                changes[d].text.truncate(len - n);
            }
            (Some(a), None) => {
                let n = overlap(suffix(&changes[a].text), prefix(&changes[d].text));
                changes[d].text.drain(..n);
            }
            _ => {}
        },
        _ => {}
    }
}
fn clean(changes: &mut [Change]) {
    let (mut keep, mut del, mut ins) = (None, None, None);
    for idx in 0..changes.len() {
        match changes[idx].tag {
            Tag::Delete => del = Some(idx),
            Tag::Insert => ins = Some(idx),
            Tag::Equal => {
                if del.is_some() || ins.is_some() {
                    dedupe(changes, keep, del, ins, Some(idx));
                }
                keep = Some(idx);
                del = None;
                ins = None;
            }
        }
    }
    if del.is_some() || ins.is_some() {
        dedupe(changes, keep, del, ins, None);
    }
}
pub(super) fn diff(old_text: &str, new_text: &str, deadline: Instant) -> Option<Vec<Change>> {
    if old_text.len().saturating_add(new_text.len()) > 131_072 {
        return None;
    }
    let old = tokenize(old_text, deadline)?;
    let new = tokenize(new_text, deadline)?;
    let mut best: HashMap<isize, Path> = HashMap::new();
    let mut initial = Path {
        old: -1,
        last: None,
    };
    let n = common(&mut initial, 0, &old, &new, deadline)?;
    if initial.old + 1 >= old.len() as isize && n + 1 >= new.len() as isize {
        return Some(make_changes(initial, &old, &new));
    }
    best.insert(0, initial);
    let (mut min, mut max) = (isize::MIN, isize::MAX);
    for length in 1..=old.len() + new.len() {
        let length = length as isize;
        let mut diagonal = (-length).max(min);
        let end = length.min(max);
        while diagonal <= end {
            if Instant::now() >= deadline {
                return None;
            }
            let remove = best.remove(&(diagonal - 1));
            let add_path = best.get(&(diagonal + 1)).cloned();
            let can_add = add_path.as_ref().is_some_and(|p| {
                let pos = p.old - diagonal;
                pos >= 0 && pos < new.len() as isize
            });
            let can_remove = remove
                .as_ref()
                .is_some_and(|p| p.old + 1 < old.len() as isize);
            if !can_add && !can_remove {
                best.remove(&diagonal);
                diagonal += 2;
                continue;
            }
            let mut path = if !can_remove
                || (can_add && remove.as_ref().unwrap().old < add_path.as_ref().unwrap().old)
            {
                add(add_path.as_ref().unwrap(), Tag::Insert, 1, 0)
            } else {
                add(remove.as_ref().unwrap(), Tag::Delete, 1, 1)
            };
            let n = common(&mut path, diagonal, &old, &new, deadline)?;
            if path.old + 1 >= old.len() as isize && n + 1 >= new.len() as isize {
                let mut result = make_changes(path, &old, &new);
                clean(&mut result);
                return Some(result);
            }
            if path.old + 1 >= old.len() as isize {
                max = max.min(diagonal - 1);
            }
            if n + 1 >= new.len() as isize {
                min = min.max(diagonal + 1);
            }
            best.insert(diagonal, path);
            diagonal += 2;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn matches_jsdiff_8_0_4_fixtures() {
        use Tag::{Delete as D, Equal as E, Insert as I};
        let cases: &[(&str, &str, &[(Tag, &str)])] = &[
            (
                "A x B y",
                "A  c B  d",
                &[
                    (E, "A "),
                    (D, "x"),
                    (I, " c"),
                    (E, " B "),
                    (D, "y"),
                    (I, " d"),
                ],
            ),
            ("abc def", "xyz ghi", &[(D, "abc def"), (I, "xyz ghi")]),
            (
                "foo bar baz",
                "foo baz",
                &[(E, "foo "), (D, "bar "), (E, "baz")],
            ),
            (
                "foo baz",
                "foo bar baz",
                &[(E, "foo "), (I, "bar "), (E, "baz")],
            ),
            ("  one", "   two", &[(D, "  one"), (I, "   two")]),
            (
                "é中 👩‍💻!",
                "é中 🐈 !",
                &[(E, "é中 "), (D, "👩‍💻"), (I, "🐈 "), (E, "!")],
            ),
            ("foo  bar", "foo   bar", &[(E, "foo   bar")]),
            (
                "a a b",
                "a b a",
                &[(E, "a "), (D, "a "), (E, "b "), (I, "a")],
            ),
            ("  ", " ", &[(E, " ")]),
            ("a", "", &[(D, "a")]),
            ("", "b", &[(I, "b")]),
        ];
        for (old, new, expected) in cases {
            let actual = diff(old, new, Instant::now() + Duration::from_secs(1)).unwrap();
            let actual: Vec<_> = actual.iter().map(|c| (c.tag, c.text.as_str())).collect();
            assert_eq!(&actual, expected, "{old:?} -> {new:?}");
        }
    }
    #[test]
    fn deadline_and_input_limits_fall_back() {
        assert!(diff("a", "b", Instant::now()).is_none());
        assert!(
            diff(
                &"a".repeat(131_073),
                "b",
                Instant::now() + Duration::from_secs(1)
            )
            .is_none()
        );
    }
}
