//! Locate display-math blocks without treating code, tables or nested Markdown
//! containers as top-level math. Nested containers are handled in their own
//! rendering context.
use super::math_tokenizer::{self, MathToken};
use pulldown_cmark::{Event, Options, Parser, Tag};
use std::borrow::Cow;
use std::ops::Range;
use std::time::{Duration, Instant};

#[cfg(not(test))]
const SCAN_TIME: Duration = Duration::from_millis(20);
// Tests run in parallel and can be descheduled between the clock checks.
#[cfg(test)]
const SCAN_TIME: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
pub(super) struct MathBlock {
    pub source: Range<usize>,
    pub content: Range<usize>,
    pub pending: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ListMathBlock {
    pub source: Range<usize>,
    pub expression: String,
    pub raw: String,
    pub pending: bool,
}

/// Find display blocks after applying Marked's list-item continuation rules.
/// Offsets point back into the original document, including expanded tabs.
pub(super) fn list_blocks(source: &str) -> Vec<ListMathBlock> {
    try_list_blocks(source).unwrap_or_default()
}

fn try_list_blocks(source: &str) -> Option<Vec<ListMathBlock>> {
    if !source.contains("$$") && !source.contains("\\[") {
        return Some(Vec::new());
    }
    if source.len() > 256 * 1024 {
        return None;
    }
    let deadline = Instant::now() + SCAN_TIME;
    let mut result = Vec::new();
    let mut quote_depth = 0usize;
    let mut examined = 0usize;
    for (event, range) in
        Parser::new_ext(source, Options::ENABLE_TABLES | Options::ENABLE_TASKLISTS)
            .into_offset_iter()
    {
        if Instant::now() >= deadline {
            return None;
        }
        match event {
            Event::Start(Tag::BlockQuote(_)) => quote_depth += 1,
            Event::End(pulldown_cmark::TagEnd::BlockQuote(_)) => quote_depth -= 1,
            Event::Start(Tag::Item) if quote_depth == 0 => {
                examined = examined.saturating_add(range.len());
                if examined > 1024 * 1024 || result.len() > 64 {
                    return None;
                }
                let Some(indent) = super::strike::item_indent(source, range.start) else {
                    continue;
                };
                let start = range.start + indent;
                let Some(body) = source.get(start..range.end) else {
                    continue;
                };
                let (normalized, offsets) = super::strike::normalize_continuations(
                    body,
                    &[super::strike::Container::Item(indent)],
                )?;
                for block in try_top_level(&normalized)? {
                    let source_start = start + offsets[block.source.start];
                    let mut source_end = start + offsets[block.source.end];
                    if normalized[block.source.clone()].ends_with('\n') {
                        while source_end > source_start
                            && matches!(source.as_bytes()[source_end - 1], b' ' | b'\t')
                        {
                            source_end -= 1;
                        }
                        if source.as_bytes().get(source_end.wrapping_sub(1)) == Some(&b'\n') {
                            source_end -= 1;
                        }
                    }
                    result.push(ListMathBlock {
                        source: source_start..source_end,
                        expression: normalized[block.content].to_owned(),
                        raw: normalized[block.source].to_owned(),
                        pending: block.pending,
                    });
                    if result.len() > 64 {
                        return None;
                    }
                }
            }
            _ => {}
        }
    }
    result.sort_unstable_by_key(|block| block.source.start);
    result.dedup_by(|a, b| a.source == b.source);
    Some(result)
}

/// Mask display math before resolving document-wide references.
/// If any scan exceeds its budget, leave references unresolved rather than
/// allowing an unverified math definition to choose a destination.
pub(super) fn reference_source(source: &str) -> Cow<'_, str> {
    let Some(top) = try_top_level(source) else {
        return Cow::Borrowed("");
    };
    let mut ranges: Vec<_> = top.into_iter().map(|block| block.source).collect();
    let Some(lists) = try_list_blocks(source) else {
        return Cow::Borrowed("");
    };
    ranges.extend(lists.into_iter().map(|block| block.source));
    let Some(quotes) = quote_blocks(source) else {
        return Cow::Borrowed("");
    };
    ranges.extend(quotes);
    if ranges.is_empty() {
        return Cow::Borrowed(source);
    }
    let mut bytes = source.as_bytes().to_vec();
    for range in ranges {
        for byte in &mut bytes[range] {
            if !matches!(*byte, b'\n' | b'\r') {
                *byte = b' ';
            }
        }
    }
    Cow::Owned(String::from_utf8(bytes).expect("math masks preserve UTF-8"))
}

/// Return byte ranges of display math inside blockquotes, or None on a limit.
pub(super) fn quote_blocks(source: &str) -> Option<Vec<Range<usize>>> {
    const MAX_BYTES: usize = 256 * 1024;
    const MAX_WORK: usize = 1024 * 1024;
    const MAX_DEPTH: usize = 32;
    const MAX_RANGES: usize = 64;

    fn visit(
        source: &str,
        offsets: &[usize],
        in_quote: bool,
        depth: usize,
        work: &mut usize,
        deadline: Instant,
        result: &mut Vec<Range<usize>>,
    ) -> Option<()> {
        *work = work.checked_add(source.len())?;
        if depth > MAX_DEPTH || *work > MAX_WORK || Instant::now() >= deadline {
            return None;
        }
        let math = if in_quote {
            try_top_level(source)?
        } else {
            Vec::new()
        };
        if Instant::now() >= deadline {
            return None;
        }
        for block in &math {
            let start = *offsets.get(block.source.start)?;
            // A boundary at the next line can point past its stripped prefix.
            let end = offsets
                .get(block.source.end.checked_sub(1)?)?
                .checked_add(1)?;
            result.push(start..end);
            if result.len() > MAX_RANGES {
                return None;
            }
        }
        let mut container_depth = 0usize;
        for (event, range) in
            Parser::new_ext(source, Options::ENABLE_TABLES | Options::ENABLE_TASKLISTS)
                .into_offset_iter()
        {
            if Instant::now() >= deadline {
                return None;
            }
            match event {
                Event::Start(Tag::BlockQuote(_) | Tag::Item) => {
                    if container_depth == 0
                        && !math
                            .iter()
                            .any(|b| b.source.start <= range.start && range.start < b.source.end)
                    {
                        let (body, local_offsets, quote) = match event {
                            Event::Start(Tag::BlockQuote(_)) => {
                                let body = source.get(range.clone())?;
                                let mut normalized = String::with_capacity(body.len());
                                let mut map = Vec::with_capacity(body.len() + 1);
                                let mut at = 0;
                                for line in body.split_inclusive('\n') {
                                    let spaces = line.bytes().take_while(|b| *b == b' ').count();
                                    let prefix = if spaces <= 3
                                        && line.as_bytes().get(spaces) == Some(&b'>')
                                    {
                                        let mut n = spaces + 1;
                                        if matches!(line.as_bytes().get(n), Some(b' ' | b'\t')) {
                                            n += 1;
                                        }
                                        n
                                    } else {
                                        0
                                    };
                                    normalized.push_str(&line[prefix..]);
                                    map.extend((at + prefix)..(at + line.len()));
                                    at += line.len();
                                }
                                map.push(body.len());
                                (normalized, map, true)
                            }
                            _ => {
                                let Some(indent) = super::strike::item_indent(source, range.start)
                                else {
                                    container_depth += 1;
                                    continue;
                                };
                                let start = range.start + indent;
                                let body = source.get(start..range.end)?;
                                let (normalized, map) = super::strike::normalize_continuations(
                                    body,
                                    &[super::strike::Container::Item(indent)],
                                )?;
                                let map = map.into_iter().map(|i| i + indent).collect();
                                (normalized, map, in_quote)
                            }
                        };
                        let mapped: Vec<usize> = local_offsets
                            .into_iter()
                            .map(|i| offsets.get(range.start + i).copied())
                            .collect::<Option<_>>()?;
                        visit(
                            &body,
                            &mapped,
                            in_quote || quote,
                            depth + 1,
                            work,
                            deadline,
                            result,
                        )?;
                    }
                    container_depth += 1;
                }
                Event::End(
                    pulldown_cmark::TagEnd::BlockQuote(_) | pulldown_cmark::TagEnd::Item,
                ) => container_depth = container_depth.checked_sub(1)?,
                _ => {}
            }
        }
        Some(())
    }
    if !source.contains("$$") && !source.contains("\\[") {
        return Some(Vec::new());
    }
    if source.len() > MAX_BYTES {
        return None;
    }
    let offsets: Vec<usize> = (0..=source.len()).collect();
    let mut result = Vec::new();
    visit(
        source,
        &offsets,
        false,
        0,
        &mut 0,
        Instant::now() + SCAN_TIME,
        &mut result,
    )?;
    result.sort_unstable_by_key(|range| range.start);
    result.dedup();
    Some(result)
}

pub(super) fn top_level(source: &str) -> Vec<MathBlock> {
    try_top_level(source).unwrap_or_default()
}

fn try_top_level(source: &str) -> Option<Vec<MathBlock>> {
    if !source.contains("$$") && !source.contains("\\[") {
        return Some(Vec::new());
    }
    if source.len() > 256 * 1024 {
        return None;
    }
    let deadline = Instant::now() + SCAN_TIME;
    let parser = Parser::new_ext(source, Options::ENABLE_TABLES);
    let mut protected: Vec<Range<usize>> = parser
        .reference_definitions()
        .iter()
        .map(|(_, def)| def.span.clone())
        .collect();
    for (event, range) in parser.into_offset_iter() {
        match event {
            Event::Start(
                Tag::CodeBlock(_)
                | Tag::HtmlBlock
                | Tag::Table(_)
                | Tag::BlockQuote(_)
                | Tag::List(_)
                | Tag::Link { .. }
                | Tag::Image { .. },
            )
            | Event::Code(_)
            | Event::Html(_)
            | Event::InlineHtml(_) => protected.push(range),
            _ => {}
        }
    }
    protected.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<Range<usize>> = Vec::new();
    for range in protected {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    let mut out = Vec::new();
    let mut line = 0;
    let mut blocked = 0;
    while line < source.len() {
        if Instant::now() >= deadline {
            return None;
        }
        while blocked < merged.len() && merged[blocked].end <= line {
            blocked += 1;
        }
        let limit = source[line..]
            .find('\n')
            .map_or(source.len(), |offset| line + offset + 1);
        let opener = line
            + source[line..limit]
                .bytes()
                .take_while(|byte| *byte == b' ')
                .count();
        // Inline Markdown syntax inside a display expression belongs to the
        // math block. Only a container covering its opener can shield it.
        let overlaps = merged
            .get(blocked)
            .is_some_and(|range| range.start <= opener && range.end > opener);
        if !overlaps
            && let Some(MathToken {
                raw_len,
                text,
                pending,
            }) = math_tokenizer::block(&source[line..])
        {
            let end = line + raw_len;
            out.push(MathBlock {
                source: line..end,
                content: line + text.start..line + text.end,
                pending,
            });
            line = end;
        } else {
            line = limit;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finds_display_blocks_without_consuming_code_or_tables() {
        let source = "before\n\\[\nx^2\n\\]\nafter\n\n```text\n$$x$$\n```\n\n| h |\n|---|\n| $$y$$ |\n\n$$z^2$$\n";
        let blocks = top_level(source);
        assert_eq!(blocks.len(), 2);
        assert_eq!(&source[blocks[0].content.clone()], "x^2\n");
        assert_eq!(&source[blocks[1].content.clone()], "z^2");
        assert!(blocks.iter().all(|b| !b.pending));
    }
    #[test]
    fn retains_incomplete_source_and_lets_tables_be_parsed() {
        assert!(top_level("$$\nx^2\n| h |\n|---|\n| y |").is_empty());
        let blocks = top_level("\\[\nx^2");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].pending);
        assert!(top_level("- $$x$$\n").is_empty());
        assert!(top_level("> $$x$$\n").is_empty());
    }

    #[test]
    fn finds_display_math_in_list_items_with_source_offsets() {
        let inline = "- item\n  $$\\frac{1}{2}$$\n  after";
        let found = list_blocks(inline);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(&inline[found[0].source.clone()], "$$\\frac{1}{2}$$");
        let source = "- item\n  $$\n  \\frac{1}{2}\n  $$\n  after\n- $$x^2$$\n";
        let blocks = list_blocks(source);
        assert_eq!(blocks.len(), 2, "{blocks:?}");
        assert_eq!(blocks[0].expression, "\\frac{1}{2}\n");
        assert_eq!(
            &source[blocks[0].source.clone()],
            "$$\n  \\frac{1}{2}\n  $$"
        );
        assert_eq!(blocks[1].expression, "x^2");
        assert_eq!(&source[blocks[1].source.clone()], "$$x^2$$");
    }
    #[test]
    fn quote_math_ranges_cover_fake_reference_only() {
        let source = "[x][r]\n\n> $$\n>\n> [r]: /fake\n>\n> $$\n\n[r]: /real";
        let ranges = quote_blocks(source).unwrap();
        assert_eq!(ranges.len(), 1, "{ranges:?}");
        assert_eq!(&source[ranges[0].clone()], "$$\n>\n> [r]: /fake\n>\n> $$\n");
        assert!(!ranges[0].contains(&source.find("[r]: /real").unwrap()));
    }

    #[test]
    fn quote_math_handles_nested_lists_quotes_and_unicode_offsets() {
        let source = "é\n\n- item\n  > > $$\n  > > [r]: /fake\n  > > $$\n\n> - item\n>   \\[\n>   x^2\n>   \\]\n";
        let ranges = quote_blocks(source).unwrap();
        assert_eq!(ranges.len(), 2, "{ranges:?}");
        assert!(source[ranges[0].clone()].contains("[r]: /fake"));
        assert!(source[ranges[0].clone()].starts_with("$$"));
        assert!(source[ranges[1].clone()].starts_with("\\["));
        assert!(source[ranges[1].clone()].contains("x^2"));
    }

    #[test]
    fn quote_math_ignores_fenced_code_and_obeys_limits() {
        let source = "> ```\n> $$\n> [r]: /fake\n> $$\n> ```\n\n```\n> $$x$$\n```\n\n> $$x^2$$\n";
        let ranges = quote_blocks(source).unwrap();
        assert_eq!(ranges.len(), 1, "{ranges:?}");
        assert_eq!(&source[ranges[0].clone()], "$$x^2$$\n");
        assert!(quote_blocks(&format!("{}$$x$$", "> ".repeat(40))).is_none());
        assert!(quote_blocks(&format!("{}\n> $$x$$", "x".repeat(256 * 1024))).is_none());
        let many = format!(
            "[x][r]\n\n{}> $$\n>\n> [r]: /fake\n>\n> $$\n\n[r]: /real",
            "> $$x$$\n\n".repeat(65)
        );
        assert!(quote_blocks(&many).is_none());
        assert!(
            Parser::new_ext(&reference_source(&many), Options::empty())
                .reference_definitions()
                .iter()
                .next()
                .is_none()
        );
        let many_lists = format!(
            "[x][r]\n\n{}- $$\n\n  [r]: /fake\n\n  $$\n\n[r]: /real",
            "- $$x$$\n\n".repeat(65)
        );
        assert!(try_list_blocks(&many_lists).is_none());
        assert!(reference_source(&many_lists).is_empty());
    }
}
