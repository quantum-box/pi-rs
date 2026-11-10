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
    Regex(fancy_regex::Regex),
    Invalid,
}
impl Query {
    pub fn new(query: &str) -> Self {
        let query = query.trim_matches(is_whitespace);
        if let Some(pattern) = query.strip_prefix("re:") {
            if pattern.trim_matches(is_whitespace).is_empty() {
                return Self::Invalid;
            }
            return fancy_regex::RegexBuilder::new(&format!(
                "(?i){}",
                pattern.trim_matches(is_whitespace)
            ))
            .backtrack_limit(100_000)
            .build()
            .map_or(Self::Invalid, Self::Regex);
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
                .find(|(_, field)| regex.is_match(field).unwrap_or(false))
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
