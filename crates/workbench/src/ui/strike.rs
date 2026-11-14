//! Pinned Pi/Marked inline tokens, without rewriting source bytes or link URLs.
//!
//! Pulldown continues to parse blocks. Runs containing double tildes use the
//! actual pinned Marked inline lexer because its deletion precedence can end
//! inside a code span, emphasis span, or link destination parsed by pulldown.
use std::cell::{Cell, RefCell};
use std::ops::Range;
use std::time::{Duration, Instant};

use pulldown_cmark::{CowStr, Event, LinkType, Options, Parser, RefDefs, Tag, TagEnd};
use rquickjs::{Context, Function, Runtime};
use serde_json::{Value, json};

type Spanned = (Event<'static>, Range<usize>);

const FRAME_TIME: Duration = Duration::from_millis(20);
const MAX_DOCUMENT_BYTES: usize = 256 * 1024;
const MAX_RUN_BYTES: usize = 64 * 1024;
const MAX_REFS: usize = 256;
const MAX_REF_BYTES: usize = 64 * 1024;
const MAX_TOKEN_BYTES: usize = 512 * 1024;

thread_local! {
    static FRAME_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

pub(crate) struct FrameBudget(Option<Instant>);

pub(crate) fn frame_budget() -> FrameBudget {
    FrameBudget(FRAME_DEADLINE.with(|cell| cell.replace(Some(Instant::now() + FRAME_TIME))))
}

fn frame_expired() -> bool {
    FRAME_DEADLINE.with(|cell| {
        cell.get()
            .is_some_and(|deadline| Instant::now() >= deadline)
    })
}

impl Drop for FrameBudget {
    fn drop(&mut self) {
        FRAME_DEADLINE.with(|cell| cell.set(self.0));
    }
}

struct InlineLexer {
    runtime: Runtime,
    context: Context,
}
impl InlineLexer {
    fn new() -> rquickjs::Result<Self> {
        let runtime = Runtime::new()?;
        runtime.set_memory_limit(128 * 1024 * 1024);
        runtime.set_max_stack_size(1024 * 1024);
        let context = Context::full(&runtime)?;
        context.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../assets/marked-inline/marked.js"))?;
            ctx.eval::<(), _>(include_str!("../../assets/marked-inline/cjk.js"))?;
            ctx.eval::<(), _>(include_str!("../../assets/marked-inline/pi-inline.js"))?;
            Ok::<_, rquickjs::Error>(())
        })?;
        Ok(Self { runtime, context })
    }
    fn tokens(&self, text: &str, refs: &RefDefs<'_>) -> Option<Vec<Value>> {
        let deadline = FRAME_DEADLINE
            .with(|cell| cell.get())
            .unwrap_or_else(|| Instant::now() + FRAME_TIME);
        if text.len() > MAX_RUN_BYTES || Instant::now() >= deadline {
            return None;
        }
        let mut links = serde_json::Map::new();
        let mut bytes = 0usize;
        for (name, def) in refs.iter() {
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(def.dest.len())
                .saturating_add(def.title.as_ref().map_or(0, |title| title.len()));
            if links.len() >= MAX_REFS || bytes > MAX_REF_BYTES || Instant::now() >= deadline {
                return None;
            }
            links.insert(
                name.split(|ch: char| (ch.is_whitespace() && ch != '\u{0085}') || ch == '\u{feff}')
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_lowercase(),
                json!({"href": def.dest.as_ref(), "title": def.title.as_deref()}),
            );
        }
        let links = serde_json::to_string(&links).ok()?;
        if Instant::now() >= deadline {
            return None;
        }
        self.runtime
            .set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)));
        let result = self.context.with(|ctx| {
            let function: Function<'_> = ctx.globals().get("piInlineTokens")?;
            function.call::<_, String>((text, links))
        });
        self.runtime.set_interrupt_handler(None);
        let result = result.ok()?;
        if result.len() > MAX_TOKEN_BYTES || Instant::now() >= deadline {
            return None;
        }
        serde_json::from_str(&result).ok()
    }
}
thread_local! {
    static LEXER: RefCell<Option<InlineLexer>> = RefCell::new(InlineLexer::new().ok());
}

#[derive(Debug)]
enum Node {
    Leaf(Spanned),
    Group {
        start: Spanned,
        children: Vec<Node>,
        end: Spanned,
    },
}
impl Node {
    fn range(&self) -> Range<usize> {
        match self {
            Self::Leaf((_, range)) => range.clone(),
            Self::Group { start, .. } => start.1.clone(),
        }
    }
    fn inline(&self) -> bool {
        match self {
            Self::Leaf((event, _)) => matches!(
                event,
                Event::Text(_)
                    | Event::Code(_)
                    | Event::InlineHtml(_)
                    | Event::SoftBreak
                    | Event::HardBreak
                    | Event::InlineMath(_)
            ),
            Self::Group {
                start: (event, _), ..
            } => matches!(
                event,
                Event::Start(Tag::Emphasis | Tag::Strong | Tag::Link { .. } | Tag::Image { .. })
            ),
        }
    }
    fn flatten(self, events: &mut Vec<Spanned>) {
        match self {
            Self::Leaf(event) => events.push(event),
            Self::Group {
                start,
                children,
                end,
            } => {
                events.push(start);
                for child in children {
                    child.flatten(events);
                }
                events.push(end);
            }
        }
    }
}
fn tree(events: impl IntoIterator<Item = Spanned>) -> Vec<Node> {
    let mut roots = Vec::new();
    let mut stack: Vec<(Spanned, Vec<Node>)> = Vec::new();
    for item in events {
        match item.0 {
            Event::Start(_) => stack.push((item, Vec::new())),
            Event::End(_) => {
                if let Some((start, children)) = stack.pop() {
                    let node = Node::Group {
                        start,
                        children,
                        end: item,
                    };
                    if let Some((_, children)) = stack.last_mut() {
                        children.push(node);
                    } else {
                        roots.push(node);
                    }
                }
            }
            _ => {
                if let Some((_, children)) = stack.last_mut() {
                    children.push(Node::Leaf(item));
                } else {
                    roots.push(Node::Leaf(item));
                }
            }
        }
    }
    roots
}

// Remove only the escapes Marked removes before recursively tokenizing a link
// label or a table cell, while retaining a byte-boundary map to the input.
fn unescape_with_offsets(source: &str, escaped: &[u8]) -> (String, Vec<usize>) {
    let bytes = source.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut offsets = vec![0];
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() && escaped.contains(&bytes[i + 1]) {
            i += 1;
        }
        output.push(bytes[i]);
        i += 1;
        offsets.push(i);
    }
    (
        String::from_utf8(output).expect("only ASCII escapes removed"),
        offsets,
    )
}

fn token_nodes(
    tokens: &[Value],
    source: &str,
    offsets: &[usize],
    whole: &str,
    run_start: usize,
    cell_end: Option<usize>,
) -> Option<Vec<Node>> {
    let mut result = Vec::new();
    let mut cursor = 0usize;
    for token in tokens {
        if frame_expired() {
            return None;
        }
        let raw = token.get("raw")?.as_str()?;
        let end = cursor.checked_add(raw.len())?;
        if source.get(cursor..end)? != raw {
            return None;
        }
        let span = *offsets.get(cursor)?..*offsets.get(end)?;
        let kind = token.get("type")?.as_str()?;
        let text = token.get("text").and_then(Value::as_str).unwrap_or(raw);
        let make_children = || -> Option<Vec<Node>> {
            let children = token.get("tokens")?.as_array()?;
            // Marked unescapes brackets in link labels before running the
            // inline lexer, so child raw text is not always a source substring.
            let bracketed_link = kind == "link" && raw.starts_with('[');
            let (normalized, local_offsets) = if bracketed_link {
                unescape_with_offsets(raw, b"[]")
            } else {
                (raw.to_owned(), (0..=raw.len()).collect())
            };
            let search_start = if bracketed_link {
                normalized.find('[')? + 1
            } else {
                0
            };
            let pos = search_start + normalized.get(search_start..)?.find(text)?;
            let child_offsets = local_offsets
                .get(pos..=pos + text.len())?
                .iter()
                .map(|offset| offsets.get(cursor + offset).copied())
                .collect::<Option<Vec<_>>>()?;
            token_nodes(children, text, &child_offsets, whole, run_start, cell_end)
        };
        let group = match kind {
            "del" => Some((Tag::Strikethrough, TagEnd::Strikethrough)),
            "strong" => Some((Tag::Strong, TagEnd::Strong)),
            "em" => Some((Tag::Emphasis, TagEnd::Emphasis)),
            "link" => {
                let mut href = token.get("href")?.as_str()?.to_owned();
                // Marked emits bare URL/email tokens as links. Route only those
                // through the native destination guards; explicit links retain
                // their authored destination.
                if !raw.starts_with('[') && !raw.starts_with('<') {
                    let absolute_start = span.start;
                    let absolute_end = span.end;
                    let tail = whole.get(absolute_end..cell_end.unwrap_or(whole.len()))?;
                    let extra = tail.char_indices().nth(2).map_or(tail.len(), |(i, _)| i);
                    let candidate = whole.get(absolute_start..absolute_end + extra)?;
                    let safe = super::render::bare_links(candidate)
                        .into_iter()
                        .next()
                        .filter(|(range, _)| {
                            range.start == 0 && range.end == absolute_end - absolute_start
                        })
                        .and_then(|(_, url)| url)
                        .filter(|url| {
                            !url.starts_with("mailto:")
                                || !super::render::has_email_prefix(
                                    whole,
                                    absolute_start,
                                    run_start,
                                )
                        });
                    if let Some(url) = safe {
                        href = url;
                    } else {
                        // InlineHtml is rendered as literal text and is not
                        // passed through the bare-link detector a second time.
                        result.push(Node::Leaf((Event::InlineHtml(raw.to_owned().into()), span)));
                        cursor = end;
                        continue;
                    }
                }
                let title = token
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                Some((
                    Tag::Link {
                        link_type: LinkType::Inline,
                        dest_url: CowStr::from(href),
                        title: CowStr::from(title),
                        id: CowStr::from(format!("\0pi-marked-label:{text}")),
                    },
                    TagEnd::Link,
                ))
            }
            _ => None,
        };
        if let Some((tag, end_tag)) = group {
            result.push(Node::Group {
                start: (Event::Start(tag), span.clone()),
                children: make_children()?,
                end: (Event::End(end_tag), span),
            });
        } else {
            let event = match kind {
                "codespan" => Event::Code(text.to_owned().into()),
                "escape" | "text" => Event::Text(text.to_owned().into()),
                "br" => Event::HardBreak,
                "html" => Event::InlineHtml(raw.to_owned().into()),
                // Pi displays unhandled inline tokens, including images and
                // unsupported math, from their raw source.
                _ => Event::Text(raw.to_owned().into()),
            };
            result.push(Node::Leaf((event, span)));
        }
        cursor = end;
    }
    (cursor == source.len()).then_some(result)
}

#[derive(Clone, Copy)]
enum Container {
    Quote,
    Item(usize),
}

// Marked's block lexer removes container prefixes before inline lexing. Keep
// byte boundaries into the original document for rendering and link guards.
fn normalize_continuations(source: &str, containers: &[Container]) -> (String, Vec<usize>) {
    let bytes = source.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut offsets = vec![0];
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        out.push(ch);
        i += 1;
        offsets.push(i);
        if ch == b'\n' {
            for container in containers {
                match *container {
                    Container::Quote => {
                        let begin = i;
                        while i < bytes.len() && i - begin < 3 && bytes[i] == b' ' {
                            i += 1;
                        }
                        if bytes.get(i) == Some(&b'>') {
                            i += 1;
                            if bytes.get(i) == Some(&b' ') {
                                i += 1;
                            }
                        } else {
                            i = begin;
                        }
                    }
                    Container::Item(indent) => {
                        let begin = i;
                        while i < bytes.len() && i - begin < indent && bytes[i] == b' ' {
                            i += 1;
                        }
                    }
                }
            }
            *offsets.last_mut().expect("newline boundary") = i;
        }
    }
    (
        String::from_utf8(out).expect("only ASCII prefixes removed"),
        offsets,
    )
}

fn item_indent(original: &str, start: usize) -> Option<usize> {
    let line = original.get(start..)?.split('\n').next()?.as_bytes();
    let marker = if matches!(line.first(), Some(b'-' | b'+' | b'*')) {
        1
    } else {
        let digits = line.iter().take_while(|b| b.is_ascii_digit()).count();
        if digits == 0 || !matches!(line.get(digits), Some(b'.' | b')')) {
            return None;
        }
        digits + 1
    };
    let spaces = line[marker..].iter().take_while(|b| **b == b' ').count();
    (spaces > 0).then_some(marker + spaces.min(4))
}

fn inline_run(
    nodes: Vec<Node>,
    original: &str,
    refs: &RefDefs<'_>,
    cell_end: Option<usize>,
    containers: &[Container],
) -> Vec<Node> {
    let Some(first) = nodes.first() else {
        return nodes;
    };
    let mut start = first.range().start;
    let end = nodes.last().expect("not empty").range().end;
    // Pulldown excludes the escape byte from the first text event.
    while start > 0 && original.as_bytes()[start - 1] == b'\\' {
        start -= 1;
    }
    let Some(source) = original.get(start..end) else {
        return nodes;
    };
    if !source.contains("~~") || source.len() > MAX_RUN_BYTES || frame_expired() {
        return nodes;
    }
    // Marked's block table lexer removes a backslash before each pipe before
    // handing a cell to its inline lexer, including inside code spans.
    let (source, offsets) = normalize_continuations(source, containers);
    let (source, offsets) = if cell_end.is_some() {
        let (source, mapped) = unescape_with_offsets(&source, b"|");
        (source, mapped.into_iter().map(|i| offsets[i]).collect())
    } else {
        (source, offsets)
    };
    let offsets: Vec<usize> = offsets.into_iter().map(|offset| start + offset).collect();
    let tokens = LEXER.with(|lexer| {
        lexer
            .borrow()
            .as_ref()
            .and_then(|lexer| lexer.tokens(&source, refs))
    });
    tokens
        .and_then(|tokens| token_nodes(&tokens, &source, &offsets, original, start, cell_end))
        .unwrap_or(nodes)
}
fn rewrite(
    nodes: Vec<Node>,
    original: &str,
    refs: &RefDefs<'_>,
    cell_end: Option<usize>,
    containers: &[Container],
) -> Vec<Node> {
    let mut result = Vec::new();
    let mut inline = Vec::new();
    for node in nodes {
        if node.inline() {
            inline.push(node);
        } else {
            result.extend(inline_run(
                std::mem::take(&mut inline),
                original,
                refs,
                cell_end,
                containers,
            ));
            let node = match node {
                Node::Group {
                    start,
                    children,
                    end,
                } if !matches!(start.0, Event::Start(Tag::CodeBlock(_) | Tag::HtmlBlock)) => {
                    let child_cell_end = if matches!(start.0, Event::Start(Tag::TableCell)) {
                        Some(start.1.end)
                    } else {
                        cell_end
                    };
                    let mut child_containers = containers.to_vec();
                    match start.0 {
                        Event::Start(Tag::BlockQuote(_)) => child_containers.push(Container::Quote),
                        Event::Start(Tag::Item) => {
                            if let Some(indent) = item_indent(original, start.1.start) {
                                child_containers.push(Container::Item(indent));
                            }
                        }
                        _ => {}
                    }
                    Node::Group {
                        start,
                        children: rewrite(
                            children,
                            original,
                            refs,
                            child_cell_end,
                            &child_containers,
                        ),
                        end,
                    }
                }
                other => other,
            };
            result.push(node);
        }
    }
    result.extend(inline_run(inline, original, refs, cell_end, containers));
    result
}

pub(super) fn events(original: &str, parsed: &str, options: Options) -> Vec<Spanned> {
    let parser = Parser::new_ext(parsed, options);
    if original.len() > MAX_DOCUMENT_BYTES || frame_expired() || !original.contains("~~") {
        return parser
            .into_offset_iter()
            .map(|(event, range)| (event.into_static(), range))
            .collect();
    }
    let references = Parser::new_ext(original, options);
    let nodes = rewrite(
        tree(
            parser
                .into_offset_iter()
                .map(|(event, range)| (event.into_static(), range)),
        ),
        original,
        references.reference_definitions(),
        None,
        &[],
    );
    let mut output = Vec::new();
    for node in nodes {
        node.flatten(&mut output);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_frame_budget_applies_to_every_inline_run_and_is_restored() {
        let previous = FRAME_DEADLINE.with(|cell| cell.replace(Some(Instant::now())));
        let text = "~~first~~\n\n~~second~~";
        let result = events(text, text, Options::empty());
        assert!(
            !result
                .iter()
                .any(|(event, _)| matches!(event, Event::Start(Tag::Strikethrough)))
        );
        {
            let _budget = frame_budget();
            assert!(FRAME_DEADLINE.with(|cell| cell.get().unwrap() > Instant::now()));
        }
        assert!(FRAME_DEADLINE.with(|cell| cell.get().unwrap() <= Instant::now()));
        FRAME_DEADLINE.with(|cell| cell.set(previous));
    }

    #[test]
    fn oversized_runs_and_reference_maps_fall_back_without_lexing() {
        let lexer = InlineLexer::new().unwrap();
        let blank = Parser::new("");
        assert!(
            lexer
                .tokens(
                    &format!("~~{}", "x".repeat(MAX_RUN_BYTES)),
                    blank.reference_definitions()
                )
                .is_none()
        );
        let refs = (0..=MAX_REFS)
            .map(|i| format!("[r{i}]: https://example.com/{i}\n"))
            .collect::<String>();
        let parsed = Parser::new(&refs);
        assert_eq!(parsed.reference_definitions().iter().count(), MAX_REFS + 1);
        assert!(
            lexer
                .tokens("~~ok~~", parsed.reference_definitions())
                .is_none()
        );
        let document = format!("~~ok~~\n\n{}", "x".repeat(MAX_DOCUMENT_BYTES));
        let result = events(&document, &document, Options::empty());
        assert!(
            !result
                .iter()
                .any(|(event, _)| matches!(event, Event::Start(Tag::Strikethrough)))
        );
    }
}
