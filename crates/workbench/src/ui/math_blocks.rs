//! Locate display-math blocks without treating code, tables or nested Markdown
//! containers as top-level math. Nested containers are handled in their own
//! rendering context.
use super::math_tokenizer::{self, MathToken};
use pulldown_cmark::{Event, Options, Parser, Tag};
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
    if source.len() > 256 * 1024 || (!source.contains("$$") && !source.contains("\\[")) {
        return Vec::new();
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
            return Vec::new();
        }
        match event {
            Event::Start(Tag::BlockQuote(_)) => quote_depth += 1,
            Event::End(pulldown_cmark::TagEnd::BlockQuote(_)) => quote_depth -= 1,
            Event::Start(Tag::Item) if quote_depth == 0 => {
                examined = examined.saturating_add(range.len());
                if examined > 1024 * 1024 || result.len() > 64 {
                    return Vec::new();
                }
                let Some(indent) = super::strike::item_indent(source, range.start) else {
                    continue;
                };
                let start = range.start + indent;
                let Some(body) = source.get(start..range.end) else {
                    continue;
                };
                let Some((normalized, offsets)) = super::strike::normalize_continuations(
                    body,
                    &[super::strike::Container::Item(indent)],
                ) else {
                    return Vec::new();
                };
                for block in top_level(&normalized) {
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
                }
            }
            _ => {}
        }
    }
    result.sort_unstable_by_key(|block| block.source.start);
    result.dedup_by(|a, b| a.source == b.source);
    result
}

pub(super) fn top_level(source: &str) -> Vec<MathBlock> {
    if source.len() > 256 * 1024 || (!source.contains("$$") && !source.contains("\\[")) {
        return Vec::new();
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
            return Vec::new();
        }
        while blocked < merged.len() && merged[blocked].end <= line {
            blocked += 1;
        }
        let limit = source[line..]
            .find('\n')
            .map_or(source.len(), |offset| line + offset + 1);
        let overlaps = merged
            .get(blocked)
            .is_some_and(|range| range.start < limit && range.end > line);
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
    out
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
}
