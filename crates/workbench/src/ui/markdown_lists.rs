//! Preserve Marked's ordered list starts inside Markdown list items.
use std::{borrow::Cow, ops::Range};

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

use super::strike::{frame_expired, item_indent};

const MAX_SCAN_BYTES: usize = 1024 * 1024;
const MAX_PASSES: usize = 32;

fn ordered_marker(line: &str) -> Option<Range<usize>> {
    let start = line.bytes().take_while(|&b| b == b' ').count();
    let end = start + line[start..].bytes().take_while(u8::is_ascii_digit).count();
    if !(1..=9).contains(&(end - start))
        || !matches!(line.as_bytes().get(end), Some(b'.' | b')'))
        || line.as_bytes().get(end + 1) != Some(&b' ')
        || line[end + 1..].trim().is_empty()
    {
        return None;
    }
    Some(start..end)
}

pub(super) fn source_ordered_start(line: &str) -> Option<u64> {
    line.get(ordered_marker(line)?)?.parse().ok()
}

// Pulldown provides byte ranges after the containers' source markers. A
// block quote may start on the same physical line as a containing list item.
fn quote_prefix(line: &str) -> (usize, usize) {
    let mut end = 0;
    let mut count = 0;
    for (index, byte) in line.bytes().enumerate() {
        if byte == b'>' {
            end = index + 1;
            count += 1;
        }
    }
    if count > 0 && line.as_bytes().get(end) == Some(&b' ') {
        end += 1;
    }
    (end, count)
}

fn item_column(source: &str, start: usize, quotes: usize) -> Option<usize> {
    let bol = source[..start].rfind('\n').map_or(0, |index| index + 1);
    let prefix = &source[bol..start];
    let (after_quote, path_quotes) = quote_prefix(prefix);
    (path_quotes == quotes)
        .then(|| item_indent(source, start))
        .flatten()
        .map(|indent| prefix.len() - after_quote + indent)
}

fn adjacent_marker(
    source: &str,
    bol: usize,
    marker: usize,
    parent: usize,
    quotes: usize,
) -> Option<Range<usize>> {
    let prefix = source.get(bol..marker)?;
    let (after_quote, path_quotes) = quote_prefix(prefix);
    let column = prefix.len() - after_quote;
    if path_quotes != quotes || !(parent..parent + 4).contains(&column) {
        return None;
    }
    let start = bol + after_quote;
    let line = source.get(start..)?.split('\n').next()?;
    let digits = ordered_marker(line)?;
    if digits.start != column || line[digits.clone()].parse::<u64>().ok()? == 1 {
        return None;
    }
    Some(start + digits.start..start + digits.end)
}

// Marked lexes list item contents outside its top-level paragraph rule. In
// that context a nested ordered list can interrupt a line of text at any
// starting number. Pulldown reserves this interruption for a start of one.
// Give the parser a width-preserving marker and use the original number when
// rendering. Verify the replacement became a list marker before accepting it.
pub(super) fn normalize_nested_ordered(source: &str) -> Cow<'_, str> {
    if source.len().saturating_mul(2) > MAX_SCAN_BYTES
        || !source.split('\n').skip(1).any(|line| {
            let start = line
                .bytes()
                .take_while(|&byte| matches!(byte, b' ' | b'>'))
                .count();
            start > 0 && ordered_marker(&line[start..]).is_some()
        })
    {
        return Cow::Borrowed(source);
    }
    // Display math is recognized outside Pulldown's inline event stream.
    // Do not turn apparent list markers inside a math expression into blocks.
    let math_mask = if source.contains("$$") || source.contains("\\[") {
        super::math_blocks::reference_source(source)
    } else {
        Cow::Borrowed(source)
    };
    if math_mask.len() != source.len() {
        return Cow::Borrowed(source);
    }
    let options = Options::ENABLE_TABLES | Options::ENABLE_TASKLISTS;
    let mut parsed = Cow::Borrowed(source);
    for _ in 0..MAX_PASSES.min(MAX_SCAN_BYTES / (2 * source.len().max(1))) {
        if frame_expired() {
            break;
        }
        let mut items = Vec::new();
        let mut quotes = 0;
        let mut line_start = None;
        let mut changes = Vec::new();
        for (event, range) in Parser::new_ext(&parsed, options).into_offset_iter() {
            let previous_break = line_start.take();
            match event {
                Event::Start(Tag::BlockQuote(_)) => quotes += 1,
                Event::End(TagEnd::BlockQuote(_)) => quotes -= 1,
                Event::Start(Tag::Item) => items.push(item_column(&parsed, range.start, quotes)),
                Event::End(TagEnd::Item) => {
                    items.pop();
                }
                Event::HardBreak | Event::SoftBreak => line_start = Some(range.end),
                Event::Text(_) => {
                    if let Some((bol, Some(parent))) = previous_break.zip(items.last().copied())
                        && let Some(digits) =
                            adjacent_marker(&parsed, bol, range.start, parent, quotes)
                    {
                        changes.push(digits);
                    }
                }
                Event::Code(_) => {
                    let Some(Some(parent)) = items.last().copied() else {
                        continue;
                    };
                    // Block markers take precedence over a multiline inline
                    // code span that would otherwise consume the next line.
                    for (offset, _) in parsed[range.clone()].match_indices('\n') {
                        let offset = range.start + offset + 1;
                        let marker = offset
                            + parsed[offset..]
                                .bytes()
                                .take_while(|&byte| matches!(byte, b' ' | b'>'))
                                .count();
                        if let Some(digits) =
                            adjacent_marker(&parsed, offset, marker, parent, quotes)
                        {
                            changes.push(digits);
                        }
                    }
                }
                _ => {}
            }
        }
        changes.retain(|digits| math_mask.as_bytes()[digits.start].is_ascii_digit());
        if changes.is_empty() || frame_expired() {
            break;
        }

        let mut bytes = parsed.as_bytes().to_vec();
        for digits in &changes {
            bytes[digits.clone()].fill(b'0');
            bytes[digits.end - 1] = b'1';
        }
        let candidate = String::from_utf8(bytes).expect("only ASCII digits changed");
        let markers: std::collections::BTreeSet<_> = Parser::new_ext(&candidate, options)
            .into_offset_iter()
            .filter_map(|(event, range)| {
                if matches!(event, Event::Start(Tag::Item)) {
                    let spaces = candidate[range.start..]
                        .bytes()
                        .take_while(|&b| b == b' ')
                        .count();
                    Some(range.start + spaces)
                } else {
                    None
                }
            })
            .collect();
        if changes
            .iter()
            .any(|digits| !markers.contains(&digits.start))
        {
            break;
        }
        parsed = Cow::Owned(candidate);
    }
    parsed
}
