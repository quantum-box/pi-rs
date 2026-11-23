//! Pi-compatible recognition of Markdown math delimiters, based on
//! packages/tui/src/components/markdown.ts at Pi v1.5.0 (MIT, see PI-LICENSE).
//!
//! The caller must invoke these at a Markdown token boundary, so escaped
//! delimiters and fenced or inline code remain owned by the Markdown lexer.
use std::ops::Range;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct MathToken {
    pub raw_len: usize,
    pub text: Range<usize>,
    pub pending: bool,
}

fn escaped(source: &str, index: usize) -> bool {
    source.as_bytes()[..index]
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'\\')
        .count()
        % 2
        == 1
}

fn closing(source: &str, delimiter: &str, start: usize) -> Option<usize> {
    let mut pos = start;
    while let Some(offset) = source[pos..].find(delimiter) {
        let at = pos + offset;
        if !escaped(source, at) {
            return Some(at);
        }
        pos = at + delimiter.len();
    }
    None
}

fn looks_like_math(source: &str) -> bool {
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' && chars.peek().is_some_and(char::is_ascii_alphabetic) {
            return true;
        }
        if "_^=+*/<>()[]|±≤≥≠≈∈→⇒∞∫∑√-".contains(ch) {
            return true;
        }
    }
    false
}

fn identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

// JavaScript's `\s` includes BOM and excludes U+0085.
fn js_whitespace(ch: char) -> bool {
    (ch.is_whitespace() && ch != '\u{85}') || ch == '\u{feff}'
}

fn looks_like_shell_pair(content: &str, following: &str) -> bool {
    let mut chars = content.chars().peekable();
    if !chars
        .peek()
        .is_some_and(|c| c.is_ascii_uppercase() || *c == '_')
    {
        return false;
    }
    while chars
        .peek()
        .is_some_and(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
    {
        chars.next();
    }
    let suffix = chars.next();
    let valid = suffix.is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_' && !js_whitespace(c))
        && chars.next().is_none();
    valid && following.chars().next().is_some_and(identifier_start)
}

pub(super) fn inline(source: &str) -> Option<MathToken> {
    let (open, close) = if source.starts_with("$$") {
        ("$$", "$$")
    } else if source.starts_with("\\(") {
        ("\\(", "\\)")
    } else if source.starts_with("\\[") {
        ("\\[", "\\]")
    } else if source.starts_with('$') && !source[1..].starts_with(js_whitespace) {
        ("$", "$")
    } else {
        return None;
    };
    let start = open.len();
    let Some(end) = closing(source, close, start) else {
        return (open.starts_with('\\') || looks_like_math(&source[start..])).then_some(
            MathToken {
                raw_len: source.len(),
                text: start..source.len(),
                pending: true,
            },
        );
    };
    let content = &source[start..end];
    let following = &source[end + close.len()..];
    if open == "$"
        && (content.ends_with(js_whitespace)
            || following.starts_with(|c: char| c.is_ascii_digit())
            || looks_like_shell_pair(content, following)
            || content.contains('`'))
    {
        return None;
    }
    if content.is_empty() || content.contains('\n') {
        return None;
    }
    Some(MathToken {
        raw_len: end + close.len(),
        text: start..end,
        pending: false,
    })
}

fn table_delimiter(line: &str) -> bool {
    let indent = line.bytes().take_while(|b| *b == b' ').count();
    if indent > 3 {
        return false;
    }
    let line = line[indent..].trim_matches([' ', '\t']);
    let line = line.strip_prefix('|').unwrap_or(line);
    let line = line.strip_suffix('|').unwrap_or(line);
    if line.is_empty() {
        return false;
    }
    line.split('|').all(|cell| {
        let cell = cell.trim_matches([' ', '\t']);
        let cell = cell.strip_prefix(':').unwrap_or(cell);
        let cell = cell.strip_suffix(':').unwrap_or(cell);
        !cell.is_empty() && cell.bytes().all(|b| b == b'-')
    })
}

fn contains_table(source: &str) -> bool {
    let mut lines = source.split('\n');
    let Some(mut header) = lines.next() else {
        return false;
    };
    for delimiter in lines {
        if (header.contains('|') || delimiter.contains('|')) && table_delimiter(delimiter) {
            return true;
        }
        header = delimiter;
    }
    false
}

/// Recognize display math at a block boundary. The returned content range is
/// untrimmed so callers can preserve the exact source on fallback.
pub(super) fn block(source: &str) -> Option<MathToken> {
    let indent = source.bytes().take_while(|b| *b == b' ').count();
    if indent > 3 {
        return None;
    }
    let rest = &source[indent..];
    let (open, close) = if rest.starts_with("$$") {
        ("$$", "$$")
    } else if rest.starts_with("\\[") {
        ("\\[", "\\]")
    } else {
        return None;
    };
    let mut start = indent + open.len();
    while source
        .as_bytes()
        .get(start)
        .is_some_and(|b| matches!(b, b' ' | b'\t'))
    {
        start += 1;
    }
    if source.as_bytes().get(start) == Some(&b'\n') {
        start += 1;
    }
    let mut search = start;
    while let Some(offset) = source[search..].find(close) {
        let end = search + offset;
        let mut after = end + close.len();
        while source
            .as_bytes()
            .get(after)
            .is_some_and(|b| matches!(b, b' ' | b'\t'))
        {
            after += 1;
        }
        if after == source.len() || source.as_bytes().get(after) == Some(&b'\n') {
            let content = &source[start..end];
            if !content.is_empty() && !contains_table(content) {
                return Some(MathToken {
                    raw_len: after + usize::from(after < source.len()),
                    text: start..end,
                    pending: false,
                });
            }
            break;
        }
        search = end + close.len();
    }
    let content = &source[start..];
    if !content.is_empty()
        && !contains_table(content)
        && (open == "\\[" || looks_like_math(content))
    {
        Some(MathToken {
            raw_len: source.len(),
            text: start..source.len(),
            pending: true,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inline_delimiters_and_currency_follow_pi() {
        for (source, expected) in [
            ("$x-y$ rest", Some("x-y")),
            ("\\(s \\to ∞\\)", Some("s \\to ∞")),
            ("$5 and $10", None),
            ("$HOME/$USER", None),
            ("$x $", None),
            ("$`x`$", None),
        ] {
            let token = inline(source);
            assert_eq!(
                token.as_ref().map(|t| &source[t.text.clone()]),
                expected,
                "{source}"
            );
        }
        assert!(inline("$x+").unwrap().pending);
        assert!(inline("\\(abc").unwrap().pending);
        assert!(inline("$HOME").is_none());
    }
    #[test]
    fn display_math_does_not_consume_tables() {
        let source = "  \\[\nx^2\n\\]\nafter";
        let token = block(source).unwrap();
        assert_eq!(&source[token.text], "x^2\n");
        assert!(!token.pending);
        assert!(block("$$\nx^2\n| value |\n| --- |\n| x |\n$$").is_none());
        assert!(block("$$\nx^2").unwrap().pending);
        assert!(block("$$\nordinary prose").is_none());
        assert!(block("    $$x$$").is_none());
    }
}
