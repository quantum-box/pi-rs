//! Pinned Pi/Marked inline tokens, without rewriting source bytes or link URLs.
//!
//! Pulldown continues to parse blocks. Runs containing double tildes use the
//! actual pinned Marked inline lexer because its deletion precedence can end
//! inside a code span, emphasis span, or link destination parsed by pulldown.
use std::cell::RefCell;
use std::ops::Range;
use std::time::{Duration, Instant};

use pulldown_cmark::{CowStr, Event, LinkType, Options, Parser, RefDefs, Tag, TagEnd};
use rquickjs::{Context, Function, Runtime};
use serde_json::{Value, json};

type Spanned = (Event<'static>, Range<usize>);

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
        let links: serde_json::Map<String, Value> = refs
            .iter()
            .map(|(name, def)| {
                (
                    name.split(|ch: char| {
                        (ch.is_whitespace() && ch != '\u{0085}') || ch == '\u{feff}'
                    })
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_lowercase(),
                    json!({"href": def.dest.as_ref(), "title": def.title.as_deref()}),
                )
            })
            .collect();
        let links = serde_json::to_string(&links).ok()?;
        let deadline = Instant::now() + Duration::from_secs(1);
        self.runtime
            .set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)));
        let result = self.context.with(|ctx| {
            let function: Function<'_> = ctx.globals().get("piInlineTokens")?;
            function.call::<_, String>((text, links))
        });
        self.runtime.set_interrupt_handler(None);
        serde_json::from_str(&result.ok()?).ok()
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

fn token_nodes(tokens: &[Value], source: &str, offsets: &[usize]) -> Option<Vec<Node>> {
    let mut result = Vec::new();
    let mut cursor = 0usize;
    for token in tokens {
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
            let (normalized, local_offsets) = if kind == "link" {
                unescape_with_offsets(raw, b"[]")
            } else {
                (raw.to_owned(), (0..=raw.len()).collect())
            };
            let search_start = if kind == "link" {
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
            token_nodes(children, text, &child_offsets)
        };
        let group = match kind {
            "del" => Some((Tag::Strikethrough, TagEnd::Strikethrough)),
            "strong" => Some((Tag::Strong, TagEnd::Strong)),
            "em" => Some((Tag::Emphasis, TagEnd::Emphasis)),
            "link" => {
                let href = token.get("href")?.as_str()?.to_owned();
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

fn inline_run(nodes: Vec<Node>, original: &str, refs: &RefDefs<'_>, table_cell: bool) -> Vec<Node> {
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
    if !source.contains("~~") {
        return nodes;
    }
    // Marked's block table lexer removes a backslash before each pipe before
    // handing a cell to its inline lexer, including inside code spans.
    let (source, offsets) = if table_cell {
        unescape_with_offsets(source, b"|")
    } else {
        (source.to_owned(), (0..=source.len()).collect())
    };
    let offsets: Vec<usize> = offsets.into_iter().map(|offset| start + offset).collect();
    let tokens = LEXER.with(|lexer| {
        lexer
            .borrow()
            .as_ref()
            .and_then(|lexer| lexer.tokens(&source, refs))
    });
    tokens
        .and_then(|tokens| token_nodes(&tokens, &source, &offsets))
        .unwrap_or(nodes)
}
fn rewrite(nodes: Vec<Node>, original: &str, refs: &RefDefs<'_>, table_cell: bool) -> Vec<Node> {
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
                table_cell,
            ));
            let node = match node {
                Node::Group {
                    start,
                    children,
                    end,
                } if !matches!(start.0, Event::Start(Tag::CodeBlock(_) | Tag::HtmlBlock)) => {
                    let child_table_cell =
                        table_cell || matches!(start.0, Event::Start(Tag::TableCell));
                    Node::Group {
                        start,
                        children: rewrite(children, original, refs, child_table_cell),
                        end,
                    }
                }
                other => other,
            };
            result.push(node);
        }
    }
    result.extend(inline_run(inline, original, refs, table_cell));
    result
}

pub(super) fn events(original: &str, parsed: &str, options: Options) -> Vec<Spanned> {
    let parser = Parser::new_ext(parsed, options);
    if !original.contains("~~") {
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
        false,
    );
    let mut output = Vec::new();
    for node in nodes {
        node.flatten(&mut output);
    }
    output
}
