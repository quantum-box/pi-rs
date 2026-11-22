//! Work-in-progress source-gap classification for Markdown block spacing.
//! The caller must provide normalized LF source and already identified
//! reference-definition ranges; no rendered output is changed by this module.
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use std::borrow::Cow;
use std::ops::Range;
use std::time::{Duration, Instant};

#[cfg(not(test))]
const SCAN_TIME: Duration = Duration::from_millis(20);
#[cfg(test)]
const SCAN_TIME: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
enum GapKind {
    Space,
    Definition,
}

#[derive(Debug, PartialEq, Eq)]
struct GapToken {
    kind: GapKind,
    range: Range<usize>,
}

/// Classify whitespace and reference definitions in a gap between blocks.
/// A single newline is absorbed by the previous token in Marked; longer
/// whitespace runs remain explicit space tokens. Return None if the caller's
/// ranges are invalid or the gap contains unclassified source.
fn classify_gap(
    source: &str,
    definitions: &[Range<usize>],
    mut previous: bool,
) -> Option<Vec<GapToken>> {
    let mut output = Vec::new();
    let mut cursor = 0;
    for definition in definitions
        .iter()
        .cloned()
        .chain(std::iter::once(source.len()..source.len()))
    {
        if definition.start < cursor
            || definition.end < definition.start
            || definition.end > source.len()
        {
            return None;
        }
        let whitespace = source.get(cursor..definition.start)?;
        if !whitespace
            .bytes()
            .all(|b| matches!(b, b' ' | b'\t' | b'\n'))
        {
            return None;
        }
        // Pi expands tabs to three spaces before lexing.
        let normalized_len = whitespace
            .bytes()
            .map(|b| if b == b'\t' { 3 } else { 1 })
            .sum::<usize>();
        if !whitespace.is_empty() && (normalized_len > 1 || !previous) {
            output.push(GapToken {
                kind: GapKind::Space,
                range: cursor..definition.start,
            });
            previous = true;
        }
        if definition.start < definition.end {
            source.get(definition.clone())?;
            output.push(GapToken {
                kind: GapKind::Definition,
                range: definition.clone(),
            });
            previous = true;
        }
        cursor = definition.end;
    }
    Some(output)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BlockFlavor {
    Paragraph,
    List,
    Html,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BlockTokenKind {
    Block(BlockFlavor),
    Math,
    Space,
    Definition,
}

#[derive(Debug)]
pub(super) struct BlockToken {
    pub kind: BlockTokenKind,
    pub range: Range<usize>,
    pub math_content: Option<Range<usize>>,
    pub pending: bool,
}

#[derive(Debug)]
struct SourceBlock {
    range: Range<usize>,
    fenced_unclosed: bool,
    kind: BlockTokenKind,
    math_content: Option<Range<usize>>,
    pending: bool,
}

pub(super) fn include_line_indent(source: &str, start: usize) -> usize {
    let line = source[..start].rfind('\n').map_or(0, |index| index + 1);
    if source[line..start].bytes().all(|byte| byte == b' ') {
        line
    } else {
        start
    }
}

pub(super) const INLINE_COMMENT_PREFIX: char = '\u{1}';

// Pulldown lets an indented comment interrupt a paragraph. Marked treats it
// as inline HTML. Give the continuation a text prefix in the parser input
// while retaining source offsets for indentation and rendered spans.
pub(super) fn inline_indented_comments(source: &str) -> Cow<'_, str> {
    if source.len() > 256 * 1024
        || source.contains(INLINE_COMMENT_PREFIX)
        || !source.contains("<!--")
        || !source.contains("\n ")
    {
        return Cow::Borrowed(source);
    }
    let mut rewritten = None;
    let mut depth = 0;
    let mut paragraph_end = None;
    let options = Options::ENABLE_TABLES | Options::ENABLE_TASKLISTS;
    for (event, range) in Parser::new_ext(source, options).into_offset_iter() {
        if super::strike::frame_expired() {
            return Cow::Borrowed(source);
        }
        match event {
            Event::Start(tag) => {
                if depth == 0
                    && let Some(end) = paragraph_end.take()
                    && matches!(tag, Tag::HtmlBlock)
                    && end < range.start
                    && source.as_bytes()[end..range.start]
                        .iter()
                        .all(|&byte| byte == b' ')
                    && source[range.start..].starts_with("<!--")
                {
                    rewritten.get_or_insert_with(|| source.as_bytes().to_vec())[end] =
                        INLINE_COMMENT_PREFIX as u8;
                    paragraph_end = Some(range.end);
                }
                depth += 1;
            }
            Event::End(tag) => {
                depth -= 1;
                if depth == 0 && tag == TagEnd::Paragraph {
                    paragraph_end = Some(range.end);
                }
            }
            _ if depth == 0 => paragraph_end = None,
            _ => {}
        }
    }
    match rewritten {
        Some(bytes) => Cow::Owned(String::from_utf8(bytes).expect("only ASCII spaces changed")),
        None => Cow::Borrowed(source),
    }
}

fn parse_blocks(
    source: &str,
    base: usize,
    options: Options,
    deadline: Instant,
) -> Option<Vec<SourceBlock>> {
    let mut blocks = Vec::new();
    let mut depth = 0usize;
    for (event, local) in Parser::new_ext(source, options).into_offset_iter() {
        if blocks.len() > 512 || Instant::now() >= deadline || super::strike::frame_expired() {
            return None;
        }
        match event {
            Event::Start(tag) => {
                if depth == 0 {
                    // Marked consumes indentation with these block markers.
                    // A space token here would add a spurious blank line.
                    let start = if matches!(
                        tag,
                        Tag::Paragraph
                            | Tag::Heading { .. }
                            | Tag::BlockQuote(_)
                            | Tag::CodeBlock(_)
                            | Tag::HtmlBlock
                    ) {
                        include_line_indent(source, local.start)
                    } else {
                        local.start
                    };
                    let fenced_unclosed = matches!(tag, Tag::CodeBlock(CodeBlockKind::Fenced(_)))
                        && unclosed_fence(&source[local.clone()]);
                    let flavor = match tag {
                        Tag::Paragraph => BlockFlavor::Paragraph,
                        Tag::List(_) => BlockFlavor::List,
                        Tag::HtmlBlock => BlockFlavor::Html,
                        _ => BlockFlavor::Other,
                    };
                    blocks.push(SourceBlock {
                        range: base + start..base + local.end,
                        fenced_unclosed,
                        kind: BlockTokenKind::Block(flavor),
                        math_content: None,
                        pending: false,
                    });
                }
                depth += 1;
            }
            Event::End(_) => depth = depth.checked_sub(1)?,
            _ if depth == 0 => {
                let start = if matches!(event, Event::Rule) {
                    include_line_indent(source, local.start)
                } else {
                    local.start
                };
                blocks.push(SourceBlock {
                    range: base + start..base + local.end,
                    fenced_unclosed: false,
                    kind: if matches!(event, Event::Html(_) | Event::InlineHtml(_)) {
                        BlockTokenKind::Block(BlockFlavor::Html)
                    } else {
                        BlockTokenKind::Block(BlockFlavor::Other)
                    },
                    math_content: None,
                    pending: false,
                });
            }
            _ => {}
        }
    }
    Some(blocks)
}

pub(super) fn unclosed_fence(source: &str) -> bool {
    let Some(first) = source.lines().next() else {
        return false;
    };
    let opening = first.trim_start_matches(' ');
    let Some(marker) = opening.bytes().next().filter(|b| matches!(b, b'`' | b'~')) else {
        return false;
    };
    let count = opening.bytes().take_while(|b| *b == marker).count();
    if count < 3 {
        return false;
    }
    !source.lines().skip(1).any(|line| {
        let line = line.trim_start_matches(' ');
        let n = line.bytes().take_while(|b| *b == marker).count();
        n >= count && line[n..].trim_matches([' ', '\t', '\r']).is_empty()
    })
}

/// Build a bounded top-level block/gap inventory. This is not yet connected to
/// rendering: its token boundaries must first match the pinned oracle.
pub(super) fn block_inventory(source: &str) -> Option<Vec<BlockToken>> {
    if source.len() > 256 * 1024 || source.contains('\r') {
        return None;
    }
    let options = Options::ENABLE_TABLES | Options::ENABLE_TASKLISTS;
    let deadline = Instant::now() + SCAN_TIME;
    let mut blocks = parse_blocks(source, 0, options, deadline)?;
    let maths = super::math_blocks::top_level(source);
    if maths.len() > 64 || maths.len().saturating_mul(blocks.len()) > 4096 {
        return None;
    }
    let has_math = !maths.is_empty();
    for math in maths {
        if Instant::now() >= deadline || super::strike::frame_expired() {
            return None;
        }
        let mut next = Vec::new();
        for block in blocks {
            if next.len() > 512 || Instant::now() >= deadline || super::strike::frame_expired() {
                return None;
            }
            if block.range.end <= math.source.start || block.range.start >= math.source.end {
                next.push(block);
            } else {
                if block.range.start < math.source.start {
                    next.push(SourceBlock {
                        range: block.range.start..math.source.start,
                        fenced_unclosed: false,
                        kind: block.kind,
                        math_content: None,
                        pending: false,
                    });
                }
                if block.range.end > math.source.end {
                    next.push(SourceBlock {
                        range: math.source.end..block.range.end,
                        fenced_unclosed: false,
                        kind: block.kind,
                        math_content: None,
                        pending: false,
                    });
                }
            }
        }
        next.push(SourceBlock {
            range: math.source,
            fenced_unclosed: false,
            kind: BlockTokenKind::Math,
            math_content: Some(math.content),
            pending: math.pending,
        });
        next.sort_unstable_by_key(|block| block.range.start);
        blocks = next;
    }
    if has_math {
        // A definition directly following display math can be part of a
        // pulldown paragraph before the math split. Parse each remaining
        // fragment as a fresh block context so it is recognized correctly.
        let mut reparsed = Vec::new();
        for block in blocks {
            if reparsed.len() > 512 || Instant::now() >= deadline || super::strike::frame_expired()
            {
                return None;
            }
            if block.kind == BlockTokenKind::Math {
                reparsed.push(block);
            } else {
                reparsed.extend(parse_blocks(
                    &source[block.range.clone()],
                    block.range.start,
                    options,
                    deadline,
                )?);
            }
        }
        reparsed.sort_unstable_by_key(|block| block.range.start);
        blocks = reparsed;
    }
    for block in &mut blocks {
        if block.kind != BlockTokenKind::Math && !block.fenced_unclosed {
            let raw = &source[block.range.clone()];
            let content_end = raw
                .trim_end_matches(|c: char| c.is_ascii_whitespace())
                .len();
            if let Some(newline) = raw[content_end..].find('\n') {
                // Spaces preceding the first newline still belong to the
                // block (and can encode a Markdown hard break).
                block.range.end = block.range.start + content_end + newline;
            }
            while let Some(offset) = source[block.range.start..block.range.end].find('\n') {
                let line = &source[block.range.start..block.range.start + offset];
                if !line.bytes().all(|byte| matches!(byte, b' ' | b'\t')) {
                    break;
                }
                block.range.start += offset + 1;
            }
        }
    }
    blocks.retain(|block| block.range.start < block.range.end);
    let mut result = Vec::new();
    let mut cursor = 0;
    let mut scan_work = 0usize;
    let mut seen_definitions = std::collections::HashSet::new();
    let reference_source = super::math_blocks::reference_source(source);
    let document_parser = Parser::new_ext(&reference_source, options);
    for block in blocks.iter().map(Some).chain(std::iter::once(None)) {
        if result.len() > 1024 || Instant::now() >= deadline || super::strike::frame_expired() {
            return None;
        }
        let end = block.map_or(source.len(), |block| block.range.start);
        if end < cursor {
            return None;
        }
        let gap = &source[cursor..end];
        let mut definitions = Vec::new();
        let mut duplicate_definitions = Vec::new();
        let mut from = 0;
        loop {
            if Instant::now() >= deadline || super::strike::frame_expired() {
                return None;
            }
            scan_work = scan_work.saturating_add(gap.len() - from);
            if scan_work > 1024 * 1024 || definitions.len() > 128 {
                return None;
            }
            let parser = Parser::new_ext(&gap[from..], options);
            let earliest = parser
                .reference_definitions()
                .iter()
                .map(|(label, def)| (label.to_string(), def.span.clone()))
                .min_by_key(|(_, span)| span.start);
            let Some((label, span)) = earliest else {
                break;
            };
            let start = from + span.start;
            let earlier_in_document = document_parser
                .reference_definitions()
                .get(&label)
                .is_some_and(|definition| {
                    definition.span.start < cursor + start
                        && !blocks.iter().any(|block| {
                            block.kind == BlockTokenKind::Math
                                && block.range.start <= definition.span.start
                                && definition.span.end <= block.range.end
                        })
                });
            let duplicate =
                earlier_in_document || !seen_definitions.insert(unicase::UniCase::new(label));
            let mut finish = from + span.end;
            while finish > start && gap.as_bytes()[finish - 1] == b'\n' {
                finish -= 1;
            }
            if finish <= start {
                return None;
            }
            definitions.push(start..finish);
            if duplicate {
                duplicate_definitions.push(start..finish);
            }
            from = finish;
        }
        for token in classify_gap(gap, &definitions, !result.is_empty())? {
            // Marked consumes duplicate definitions without emitting a def
            // token; whitespace on either side remains independently visible.
            if token.kind == GapKind::Definition && duplicate_definitions.contains(&token.range) {
                continue;
            }
            result.push(BlockToken {
                kind: match token.kind {
                    GapKind::Space => BlockTokenKind::Space,
                    GapKind::Definition => BlockTokenKind::Definition,
                },
                range: cursor + token.range.start..cursor + token.range.end,
                math_content: None,
                pending: false,
            });
        }
        if let Some(block) = block {
            result.push(BlockToken {
                kind: block.kind,
                range: block.range.clone(),
                math_content: block.math_content.clone(),
                pending: block.pending,
            });
            cursor = block.range.end;
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_rejects_excessive_block_work() {
        assert!(block_inventory(&"# heading\n\n".repeat(2000)).is_none());
        assert!(block_inventory(&"$$x$$\n\n".repeat(2000)).is_none());
        assert!(block_inventory(&"paragraph\n\n".repeat(33)).is_some());
        let definitions = (0..2000)
            .map(|index| format!("[ref{index}]: https://example.org/{index}\n"))
            .collect::<String>();
        assert!(block_inventory(&definitions).is_none());
    }

    #[test]
    fn reference_and_space_gaps_match_pinned_pi_tokens() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/markdown/pi-v150-trailing.json"
        ))
        .unwrap();
        let mut checked = 0;
        for case in fixture["cases"].as_array().unwrap() {
            let tokens = case["tokens"].as_array().unwrap();
            // Marked removes duplicate definitions from token raw text, so
            // their synthetic concatenation is not an actual source gap.
            if tokens
                .iter()
                .map(|t| t["raw"].as_str().unwrap())
                .collect::<String>()
                != case["source"].as_str().unwrap()
            {
                continue;
            }
            let mut index = 0;
            while index < tokens.len() {
                if !matches!(tokens[index]["type"].as_str(), Some("space" | "def")) {
                    index += 1;
                    continue;
                }
                let first = index;
                let mut gap = String::new();
                let mut definitions = Vec::new();
                let mut expected = Vec::new();
                while index < tokens.len() {
                    let kind = tokens[index]["type"].as_str().unwrap();
                    if !matches!(kind, "space" | "def") {
                        break;
                    }
                    let raw = tokens[index]["raw"].as_str().unwrap();
                    let start = gap.len();
                    gap.push_str(raw);
                    if kind == "def" {
                        let end = start + raw.trim_end_matches('\n').len();
                        definitions.push(start..end);
                        expected.push(GapKind::Definition);
                    } else {
                        expected.push(GapKind::Space);
                    }
                    index += 1;
                }
                let actual: Vec<_> = classify_gap(&gap, &definitions, first > 0)
                    .unwrap()
                    .into_iter()
                    .map(|token| token.kind)
                    .collect();
                assert_eq!(actual, expected, "source={:?}, gap={gap:?}", case["source"]);
                checked += 1;
            }
        }
        assert!(
            checked >= 40,
            "oracle did not exercise enough block gaps: {checked}"
        );
    }

    #[test]
    fn top_level_inventory_matches_pinned_pi() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/markdown/pi-v150-trailing.json"
        ))
        .unwrap();
        let mut failures = Vec::new();
        for case in fixture["cases"].as_array().unwrap() {
            let source = case["source"].as_str().unwrap();
            let expected: Vec<_> = case["tokens"]
                .as_array()
                .unwrap()
                .iter()
                .map(|token| match token["type"].as_str().unwrap() {
                    "space" => BlockTokenKind::Space,
                    "def" => BlockTokenKind::Definition,
                    "latexBlock" => BlockTokenKind::Math,
                    _ => BlockTokenKind::Block(BlockFlavor::Other),
                })
                .collect();
            // The renderer normalizes line endings before it inventories blocks.
            let normalized = source.replace("\r\n", "\n").replace('\r', "\n");
            let actual = block_inventory(&normalized).map(|tokens| {
                tokens
                    .into_iter()
                    .map(|token| match token.kind {
                        BlockTokenKind::Block(_) => BlockTokenKind::Block(BlockFlavor::Other),
                        other => other,
                    })
                    .collect::<Vec<_>>()
            });
            if actual.as_ref() != Some(&expected) {
                failures.push(format!(
                    "{source:?}: expected={expected:?}, actual={actual:?}"
                ));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
