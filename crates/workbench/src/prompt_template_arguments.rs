//! Pi-compatible prompt template argument parsing and substitution.

use std::str::Chars;

fn is_prompt_space(character: char) -> bool {
    (character.is_whitespace() && character != '\u{0085}') || character == '\u{feff}'
}

fn parse_arguments(arguments: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = None;
    let mut remaining = arguments.chars();
    let mut unmatched = [false; 2];
    while let Some(character) = remaining.next() {
        if current.is_none() && matches!(character, '\'' | '"') {
            let unmatched = &mut unmatched[usize::from(character == '"')];
            if !*unmatched {
                let mut lookahead = remaining.clone();
                if let Some(value) = read_quoted(&mut lookahead, character) {
                    current = Some(value);
                    remaining = lookahead;
                    continue;
                }
                // Keep the opener literal. With no unescaped closer in this
                // suffix, later openers of the same kind cannot close either.
                *unmatched = true;
            }
        }
        if is_prompt_space(character) {
            args.extend(current.take());
        } else {
            current.get_or_insert_with(String::new).push(character);
        }
    }
    args.extend(current);
    args
}

fn read_quoted(remaining: &mut Chars<'_>, delimiter: char) -> Option<String> {
    let mut value = String::new();
    while let Some(character) = remaining.next() {
        if character == delimiter {
            return Some(value);
        }
        if character == '\\' && remaining.as_str().starts_with(delimiter) {
            remaining.next();
            value.push(delimiter);
        } else {
            value.push(character);
        }
    }
    None
}

/// Expand Pi prompt template placeholders using quoted command arguments.
/// Inserted argument text is not recursively expanded.
pub fn expand_arguments(body: &str, arguments: &str) -> String {
    static PLACEHOLDER: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let expression = PLACEHOLDER.get_or_init(|| {
        regex::Regex::new(r"\$\{([0-9]+|ARGUMENTS|@):-([^}]*)\}|\$\{@:([0-9]+)(?::([0-9]+))?\}|\$(ARGUMENTS|@|[0-9]+)")
            .expect("Pi argument placeholders")
    });
    let args = parse_arguments(arguments);
    let all = args.join(" ");
    let value = |key: &str| match key {
        "@" | "ARGUMENTS" => all.as_str(),
        _ => key
            .parse::<usize>()
            .ok()
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| args.get(index))
            .map_or("", String::as_str),
    };
    expression
        .replace_all(body, |captures: &regex::Captures<'_>| {
            if let Some(target) = captures.get(1) {
                let found = value(target.as_str());
                return if found.is_empty() {
                    captures[2].to_owned()
                } else {
                    found.to_owned()
                };
            }
            if let Some(start) = captures.get(3) {
                let start = start
                    .as_str()
                    .parse::<usize>()
                    .unwrap_or(usize::MAX)
                    .saturating_sub(1)
                    .min(args.len());
                let length = captures
                    .get(4)
                    .and_then(|length| length.as_str().parse::<usize>().ok())
                    .unwrap_or(usize::MAX);
                return args[start..start.saturating_add(length).min(args.len())].join(" ");
            }
            value(&captures[5]).to_owned()
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::parse_arguments;

    #[test]
    fn prompt_template_arguments_match_upstream_quoting() {
        let cases: &[(&str, &[&str])] = &[
            (
                "why doesn't the user's cache work",
                &["why", "doesn't", "the", "user's", "cache", "work"],
            ),
            (r#"prefix"a b" suffix"#, &["prefix\"a", "b\"", "suffix"]),
            (
                r#""say \"hi\"" 'it\'s here' next"#,
                &["say \"hi\"", "it's here", "next"],
            ),
            (
                r#""C:\dir\file" 'a\"b' \"literal"#,
                &[r"C:\dir\file", r#"a\"b"#, r#"\"literal"#],
            ),
            ("'tis the season", &["'tis", "the", "season"]),
            ("say \"hello world", &["say", "\"hello", "world"]),
            ("a '", &["a", "'"]),
            (r#""abc\""#, &[r#""abc\""#]),
            (r#"'first "二 words" last"#, &["'first", "二 words", "last"]),
            (r#"'first \'second tail"#, &["'first", r"\'second", "tail"]),
        ];
        for &(input, expected) in cases {
            assert_eq!(parse_arguments(input), expected, "{input:?}");
        }
    }

    #[test]
    fn prompt_template_arguments_match_upstream_whitespace_and_unicode() {
        let cases: &[(&str, &[&str])] = &[
            ("  a\n\n\tb  c  ", &["a", "b", "c"]),
            ("a '' \"  \" c", &["a", "", "  ", "c"]),
            ("\"líné\n日本語\" 😀 café", &["líné\n日本語", "😀", "café"]),
            (
                "\u{feff}日本語\u{a0}😀\u{2003}café\u{0085}x\u{2028}end",
                &["日本語", "😀", "café\u{0085}x", "end"],
            ),
        ];
        for &(input, expected) in cases {
            assert_eq!(parse_arguments(input), expected, "{input:?}");
        }
    }
}
