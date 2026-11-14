//! Session-search fields and matching rules shared by the resume picker.
use std::sync::OnceLock;

pub struct Document {
    pub local: bool,
    pub message_count: u64,
    pub activity: Option<std::time::SystemTime>,
    pub parent: Option<std::path::PathBuf>,
    fields: [String; 4],
    lower: [String; 4],
    normalized: OnceLock<[String; 4]>,
}
impl Document {
    pub fn from_session(
        session: &pi_rs::Session,
        cwd: String,
        messages: String,
        local: bool,
    ) -> Self {
        Self::new(
            session.name().unwrap_or_default().into(),
            session.external_session_id(),
            cwd,
            messages,
            local,
        )
    }
    pub fn cwd(&self) -> &str {
        &self.fields[2]
    }

    pub fn new(name: String, id: String, cwd: String, messages: String, local: bool) -> Self {
        let fields = [name, id, cwd, messages];
        let lower = fields.each_ref().map(|field| field.to_lowercase());
        Self {
            local,
            message_count: 0,
            activity: None,
            parent: None,
            fields,
            lower,
            normalized: OnceLock::new(),
        }
    }
}

pub enum Token {
    Fuzzy(String),
    Phrase(String),
}
pub enum Query {
    Tokens(Vec<Token>),
    Regex(SearchRegex),
    Invalid,
}
pub enum SearchRegex {
    Linear(regex::Regex),
    Fancy(fancy_regex::Regex),
}
impl SearchRegex {
    fn is_match(&self, text: &str) -> bool {
        match self {
            Self::Linear(regex) => regex.is_match(text),
            Self::Fancy(regex) => regex.is_match(text).unwrap_or(false),
        }
    }
}
impl Query {
    pub fn new(query: &str) -> Self {
        let query = query.trim_matches(is_whitespace);
        if let Some(pattern) = query.strip_prefix("re:") {
            if pattern.trim_matches(is_whitespace).is_empty() {
                return Self::Invalid;
            }
            let pattern = pattern.trim_matches(is_whitespace);
            let linear = format!("(?i){}", ascii_regex_classes(pattern));
            if let Ok(regex) = regex::Regex::new(&linear) {
                return Self::Regex(SearchRegex::Linear(regex));
            }
            // Keep advanced patterns on their original bounded implementation.
            // Expanded lookarounds can exhaust its limit on long fields.
            return fancy_regex::RegexBuilder::new(&format!("(?i){pattern}"))
                .backtrack_limit(100_000)
                .build()
                .map_or(Self::Invalid, |regex| {
                    Self::Regex(SearchRegex::Fancy(regex))
                });
        }
        let mut tokens = Vec::new();
        let mut buffer = String::new();
        let mut quoted = false;
        for character in query.chars() {
            if character == '"' || (!quoted && is_whitespace(character)) {
                let value = buffer.trim_matches(is_whitespace).to_lowercase();
                if !value.is_empty() {
                    tokens.push(if quoted {
                        Token::Phrase(normalize(&value))
                    } else {
                        Token::Fuzzy(value)
                    });
                }
                buffer.clear();
                if character == '"' {
                    quoted = !quoted;
                }
            } else {
                buffer.push(character);
            }
        }
        if quoted {
            return Self::Tokens(
                query
                    .split(is_whitespace)
                    .filter(|part| !part.is_empty())
                    .map(|value| Token::Fuzzy(value.to_lowercase()))
                    .collect(),
            );
        }
        if !buffer.trim_matches(is_whitespace).is_empty() {
            tokens.push(Token::Fuzzy(
                buffer.trim_matches(is_whitespace).to_lowercase(),
            ));
        }
        Self::Tokens(tokens)
    }

    pub fn score(&self, document: &Document) -> Option<i64> {
        const TEXT: i64 = 10_000_000;
        const FUZZY: i64 = 20_000_000;
        match self {
            Self::Invalid => None,
            Self::Regex(regex) => document
                .fields
                .iter()
                .enumerate()
                .find(|(_, field)| regex.is_match(field))
                .map(|(index, _)| if index == 0 { 0 } else { TEXT }),
            Self::Tokens(tokens) => {
                let mut total = 0;
                for token in tokens {
                    let (value, fields) = match token {
                        Token::Fuzzy(value) => (value, &document.lower),
                        Token::Phrase(value) => (
                            value,
                            document.normalized.get_or_init(|| {
                                document.lower.each_ref().map(|field| normalize(field))
                            }),
                        ),
                    };
                    let score = if fields[0].contains(value) {
                        fuzzy_score(value, &fields[0]).unwrap_or(0)
                    } else if fields[1..].iter().any(|field| field.contains(value)) {
                        TEXT
                    } else if matches!(token, Token::Fuzzy(_)) {
                        [0, 2]
                            .into_iter()
                            .filter_map(|index| fuzzy_score(value, &fields[index]))
                            .min()?
                            + FUZZY
                    } else {
                        return None;
                    };
                    total += score;
                }
                Some(total)
            }
        }
    }
}
// JavaScript's non-Unicode regular expressions use ASCII for these classes.
// Translate single-shorthand bracket expressions too. More complex bracket
// expressions remain untouched; their union and negation need a JS parser.
fn ascii_regex_classes(pattern: &str) -> String {
    let mut result = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '[' {
            let mut probe = chars.clone();
            let negated = probe.peek() == Some(&'^');
            if negated {
                probe.next();
            }
            let shorthand = match (probe.next(), probe.next(), probe.next()) {
                (Some('\\'), Some(kind @ ('d' | 'D' | 'w' | 'W')), Some(']')) => Some(kind),
                _ => None,
            };
            if let Some(kind) = shorthand {
                let positive = matches!(kind, 'd' | 'w') != negated;
                let word = matches!(kind, 'w' | 'W');
                result.push_str(match (word, positive) {
                    (false, true) => "(?-i:[0-9])",
                    (false, false) => "(?-i:[^0-9])",
                    (true, true) => "(?-i:[A-Za-z0-9_])",
                    (true, false) => "(?-i:[^A-Za-z0-9_])",
                });
                chars = probe;
            } else {
                result.push('[');
                let mut escaped = false;
                for next in chars.by_ref() {
                    result.push(next);
                    if escaped {
                        escaped = false;
                    } else if next == '\\' {
                        escaped = true;
                    } else if next == ']' {
                        break;
                    }
                }
            }
        } else if character == '\\' {
            let Some(next) = chars.next() else {
                result.push(character);
                break;
            };
            let replacement = match next {
                'd' => Some("(?-i:[0-9])"),
                'D' => Some("(?-i:[^0-9])"),
                'w' => Some("(?-i:[A-Za-z0-9_])"),
                'W' => Some("(?-i:[^A-Za-z0-9_])"),
                'b' => Some(r"(?-u:\b)"),
                'B' => Some(r"(?-u:\B)"),
                _ => None,
            };
            if let Some(replacement) = replacement {
                result.push_str(replacement);
            } else {
                result.push(character);
                result.push(next);
            }
        } else {
            result.push(character);
        }
    }
    result
}
fn is_whitespace(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
fn normalize(value: &str) -> String {
    value
        .split(is_whitespace)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn fuzzy_score(query: &str, text: &str) -> Option<i64> {
    let query: Vec<_> = query.encode_utf16().collect();
    let text: Vec<_> = text.encode_utf16().collect();
    if let Some(score) = alignment_score(&query, &text) {
        return Some(score);
    }
    let split = query.iter().position(|character| {
        (u16::from(b'0')..=u16::from(b'9')).contains(character)
            != (u16::from(b'0')..=u16::from(b'9')).contains(&query[0])
    })?;
    let (first, second) = query.split_at(split);
    let letters = |part: &[u16]| {
        part.iter()
            .all(|character| (u16::from(b'a')..=u16::from(b'z')).contains(character))
    };
    let digits = |part: &[u16]| {
        part.iter()
            .all(|character| (u16::from(b'0')..=u16::from(b'9')).contains(character))
    };
    if !((letters(first) && digits(second)) || (digits(first) && letters(second))) {
        return None;
    }
    let swapped: Vec<_> = second.iter().chain(first).copied().collect();
    alignment_score(&swapped, &text).map(|score| score + 50)
}
fn alignment_score(query: &[u16], text: &[u16]) -> Option<i64> {
    if query.is_empty() {
        return Some(0);
    }
    let mut positions = Vec::with_capacity(query.len());
    let mut start = 0;
    for character in query {
        let position = text
            .get(start..)?
            .iter()
            .position(|value| value == character)?
            + start;
        positions.push(position);
        start = position + 1;
    }
    let mut score = position_score(text, &positions);
    for (start, window) in text.windows(query.len()).enumerate() {
        if window == query {
            let positions: Vec<_> = (start..start + query.len()).collect();
            score = score.min(position_score(text, &positions));
        }
    }
    Some(score - if query == text { 1000 } else { 0 })
}
fn position_score(text: &[u16], positions: &[usize]) -> i64 {
    let mut score = 0;
    let mut previous = None;
    let mut consecutive = 0;
    for &position in positions {
        if previous.map_or(position == 0, |previous| previous + 1 == position) {
            consecutive += 1;
            score -= consecutive * 50;
        } else {
            consecutive = 0;
            if let Some(previous) = previous {
                score += (position - previous - 1) as i64 * 20;
            }
        }
        if position == 0
            || char::from_u32(u32::from(text[position - 1]))
                .is_some_and(|character| is_whitespace(character) || "-_./:".contains(character))
        {
            score -= 100;
        }
        score += position as i64;
        previous = Some(position);
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_shorthand_classes_match_javascript_ascii_rules() {
        let doc = |name: &str| {
            Document::new(
                name.into(),
                String::new(),
                String::new(),
                String::new(),
                true,
            )
        };
        for (pattern, yes, no) in [
            (r"re:^\d+$", "123", "١٢٣"),
            (r"re:^\D+$", "١٢٣", "123"),
            (r"re:^\w+$", "A_2", "K"),
            (r"re:^\W+$", "K", "A_2"),
        ] {
            assert!(
                Query::new(pattern).score(&doc(yes)).is_some(),
                "{pattern} {yes}"
            );
            assert!(
                Query::new(pattern).score(&doc(no)).is_none(),
                "{pattern} {no}"
            );
        }
        assert!(Query::new(r"re:^\\d$").score(&doc(r"\d")).is_some());
        assert_eq!(ascii_regex_classes(r"[\d]"), r"(?-i:[0-9])");
    }

    #[test]
    fn regex_single_shorthand_brackets_use_javascript_ascii_classes() {
        let doc = |name: &str| {
            Document::new(
                name.into(),
                String::new(),
                String::new(),
                String::new(),
                true,
            )
        };
        for (pattern, yes, no) in [
            (r"re:^[\d]+$", "123", "١٢٣"),
            (r"re:^[^\d]+$", "١٢٣", "123"),
            (r"re:^[\D]+$", "١٢٣", "123"),
            (r"re:^[^\D]+$", "123", "١٢٣"),
            (r"re:^[\w]+$", "A_2", "K"),
            (r"re:^[^\w]+$", "K", "A"),
            (r"re:^[\W]+$", "K", "A"),
            (r"re:^[^\W]+$", "A_2", "K"),
            (r"re:^[a-z]+\d$", "abc1", "abc١"),
        ] {
            assert!(
                Query::new(pattern).score(&doc(yes)).is_some(),
                "{pattern} {yes}"
            );
            assert!(
                Query::new(pattern).score(&doc(no)).is_none(),
                "{pattern} {no}"
            );
        }
        assert_eq!(ascii_regex_classes(r"[a\d]"), r"[a\d]");
        assert_eq!(ascii_regex_classes(r"\[\d\]"), r"\[(?-i:[0-9])\]");
    }

    #[test]
    fn regex_word_boundaries_use_ascii_with_non_ascii_neighbours() {
        let doc = |name: &str| {
            Document::new(
                name.into(),
                String::new(),
                String::new(),
                String::new(),
                true,
            )
        };
        for text in ["Kfoo", "fooá", "Kfooá", "foo"] {
            assert!(
                Query::new(r"re:\b\w+\b").score(&doc(text)).is_some(),
                "{text}"
            );
        }
        for text in ["á", "K", ""] {
            assert!(
                Query::new(r"re:\b\w+\b").score(&doc(text)).is_none(),
                "{text}"
            );
        }
        for text in ["Kfooá", "foo", "a_b"] {
            assert!(
                Query::new(r"re:\B\w+\B").score(&doc(text)).is_some(),
                "{text}"
            );
        }
        for text in ["K", "á", "a"] {
            assert!(
                Query::new(r"re:\B\w+\B").score(&doc(text)).is_none(),
                "{text}"
            );
        }
        for (pattern, text, expected) in [
            (r"re:^K\bfoo\bá$", "Kfooá", true),
            (r"re:^K\bfoo\bá$", "Kfooxá", false),
            (r"re:^K\Bfoo\Bá$", "Kfooá", false),
            (r"re:^K\B!$", "K!", true),
            (r"re:^\Bfoo\b$", "foo", false),
            (r"re:^f\Boo\b$", "foo", true),
        ] {
            assert_eq!(
                Query::new(pattern).score(&doc(text)).is_some(),
                expected,
                "{pattern} {text}"
            );
        }
        assert!(Query::new(r"re:^\\b$").score(&doc(r"\b")).is_some());
        assert_eq!(ascii_regex_classes(r"[\b]\w"), r"[\b](?-i:[A-Za-z0-9_])");
    }

    #[test]
    fn regex_word_boundary_finds_match_after_long_session_text() {
        let text = format!("{} missing", "a".repeat(50_000));
        let document = Document::new(String::new(), String::new(), String::new(), text, true);
        assert!(Query::new(r"re:\bmissing\b").score(&document).is_some());
        assert!(
            Query::new(r"re:\bmissing\b(?=$)")
                .score(&document)
                .is_some()
        );
        let nonmatching = format!("{} missingx", "a".repeat(50_000));
        let document = Document::new(
            String::new(),
            String::new(),
            String::new(),
            nonmatching,
            true,
        );
        assert!(Query::new(r"re:\bmissing\b").score(&document).is_none());
    }

    #[test]
    fn imported_external_identity_is_searchable_after_reopen_and_fork_changes_it() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("source.jsonl");
        std::fs::write(
            &input,
            "{\"type\":\"session\",\"version\":3,\"id\":\"legacy-search-id\"}\n",
        )
        .unwrap();
        let imported = crate::session_import::read(&input).unwrap();
        let exported = dir.path().join("export.jsonl");
        imported.export_jsonl(&exported).unwrap();
        let reopened = crate::session_import::read(&exported).unwrap();
        let document = Document::from_session(&reopened, String::new(), String::new(), true);
        assert!(Query::new("legacy-search-id").score(&document).is_some());
        let fork =
            pi_rs::Session::from_records(reopened.fork_records(reopened.head()).unwrap()).unwrap();
        let document = Document::from_session(&fork, String::new(), String::new(), true);
        assert!(Query::new("legacy-search-id").score(&document).is_none());
        assert!(
            Query::new(&fork.session_id().to_string())
                .score(&document)
                .is_some()
        );
    }

    fn document() -> Document {
        Document::new(
            "Planning Rust".into(),
            "abc-123".into(),
            "/work/project".into(),
            "Earlier turn timeout\n retry 日本語 task".into(),
            true,
        )
    }

    #[test]
    fn reference_queries_match_the_same_fields_and_scores() {
        let document = document();
        for (query, score) in [
            ("rust", Some(-358)),
            ("planning timeout", Some(9_998_128)),
            ("\"timeout retry\"", Some(10_000_000)),
            ("re:timeout(?=\\s+retry)", Some(10_000_000)),
            ("pln", Some(19_999_774)),
            ("abc", Some(10_000_000)),
            ("rust unknown", None),
            ("re:[", None),
            ("\"unclosed", None),
        ] {
            assert_eq!(Query::new(query).score(&document), score, "{query}");
        }
    }

    #[test]
    fn body_tokens_are_literal_and_do_not_cross_field_boundaries() {
        let document = Document::new(
            "Alpha".into(),
            "123".into(),
            "/work".into(),
            "separate conversation".into(),
            true,
        );
        assert!(Query::new("scv").score(&document).is_none());
        assert!(Query::new("\"alpha 123\"").score(&document).is_none());
        assert!(Query::new("alpha conversation").score(&document).is_some());
        assert!(Query::new("re:^Alpha$").score(&document).is_some());
    }

    #[test]
    fn numeric_letter_swaps_and_unicode_follow_reference_alignment() {
        assert_eq!(
            fuzzy_score("4o", "GPT-o4"),
            fuzzy_score("o4", "GPT-o4").map(|score| score + 50)
        );
        assert!(fuzzy_score("日本", "計画 日本語").is_some());
        assert_eq!(Query::new("   ").score(&document()), Some(0));
    }
}
