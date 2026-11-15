//! Locate display-math blocks without treating code, tables or nested Markdown
//! containers as top-level math. Nested containers are handled in their own
//! rendering context.
use super::math_tokenizer::{self, MathToken};
use pulldown_cmark::{Event, Options, Parser, Tag};
use std::ops::Range;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct MathBlock {
    pub source: Range<usize>,
    pub content: Range<usize>,
    pub pending: bool,
}

pub(super) fn top_level(source: &str) -> Vec<MathBlock> {
    if source.len() > 256 * 1024 || (!source.contains("$$") && !source.contains("\\[")) {
        return Vec::new();
    }
    let deadline = Instant::now() + Duration::from_millis(20);
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
}
