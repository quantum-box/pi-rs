//! Self-contained terminal renderer for Mermaid diagrams.
//!
//! Adapted from xai-org/grok-build at 2bdd1d6a6369de0e8c68132ea4539e9abd9e14a8.
//! Copyright 2023-2026 SpaceXAI. Apache-2.0, see licenses/grok-build-Apache-2.0.txt.
//! The native workbench applies its own theme and retains the Markdown source on fallback.

use std::collections::HashMap;

use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

pub(super) struct MermaidArt {
    pub styled_lines: Vec<Vec<(String, Cls)>>,
    pub plain_lines: Vec<String>,
}

const MAX_LABEL: usize = 28;
const PAD: usize = 1;
const GAP_X: usize = 3;
const GAP_Y: usize = 2;
/// Node labels wrap to at most this many display columns per line, and at most this many lines (overflow is truncated with an ellipsis).
const WRAP_WIDTH: usize = 24;
const MAX_LINES: usize = 4;
/// Identifier-boundary characters preferred as break points when a single word is too wide to fit, so it is not sliced mid-segment.
/// Mirrors `TOKEN_BREAK_CHARS` in `third_party/mermaid-to-svg/src/text_wrap.rs`.
/// The two renderers are deliberately independent, so keep these two in sync.
const LABEL_BREAK_CHARS: [char; 4] = ['_', '-', '.', '/'];
/// Sentinel marking the trailing column of a wide glyph (never emitted).
const CONT: char = '\u{0}';
const MAX_NODES: usize = 128;
const MAX_EDGES: usize = 512;
const MAX_GROUPS: usize = 24;
const MAX_GROUP_DEPTH: usize = 6;
const MAX_CANVAS_CELLS: usize = 1 << 21;

fn char_width(c: char) -> usize {
    UnicodeWidthChar::width(c).unwrap_or(0)
}

#[derive(Clone, Copy)]
enum Oversize {
    Width,
    Cells,
}

/// Render a mermaid source block, or `None` for blank input.
pub(super) fn render(src: &str, max_width: Option<usize>) -> Option<MermaidArt> {
    if src.trim().is_empty() {
        return None;
    }

    let outcome: Option<Result<MermaidArt, Oversize>> = parse_graph(src)
        .map(|graph| {
            if graph.groups.is_empty() {
                layout_flowchart(&graph, max_width)
            } else {
                render_grouped(&graph, max_width)
            }
        })
        .or_else(|| parse_state(src).map(|graph| layout_flowchart(&graph, max_width)))
        .or_else(|| parse_class(src).map(|(graph, infos)| render_class(&graph, &infos, max_width)))
        .or_else(|| parse_er(src).map(|(graph, infos)| render_class(&graph, &infos, max_width)))
        .or_else(|| parse_sequence(src).map(|seq| layout_sequence(&seq, max_width)));

    outcome?.ok()
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Rect,
    Round,
    Diamond,
}

struct Node {
    label: String,
    shape: Shape,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Head {
    None,
    Arrow,
    Circle,
    Cross,
    Triangle,
    DiamondFill,
    DiamondOpen,
}

#[derive(Clone, Copy, PartialEq)]
enum LineKind {
    Solid,
    Dotted,
    Thick,
}

struct Edge {
    from: usize,
    to: usize,
    label: Option<String>,
    head_to: Head,
    head_from: Head,
    line: LineKind,
}

#[derive(Clone, Copy, PartialEq)]
enum Dir {
    Down,
    Up,
    Right,
    Left,
}

struct Group {
    id: String,
    label: String,
    parent: Option<usize>,
}

struct Graph {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    index: HashMap<String, usize>,
    groups: Vec<Group>,
    node_group: Vec<Option<usize>>,
    cur_group: Option<usize>,
    over_cap: bool,
    warnings: Vec<String>,
    dir: Dir,
}

impl Graph {
    fn node_index(&mut self, id: &str, label: Option<&str>, shape: Shape) -> Option<usize> {
        if let Some(&i) = self.index.get(id) {
            if let Some(label) = label
                && let Some(node) = self.nodes.get_mut(i)
            {
                node.label = label.to_string();
                node.shape = shape;
            }
            return Some(i);
        }
        if self.nodes.len() >= MAX_NODES {
            self.over_cap = true;
            return None;
        }
        let label = label.unwrap_or(id).to_string();
        self.index.insert(id.to_string(), self.nodes.len());
        self.nodes.push(Node { label, shape });
        self.node_group.push(self.cur_group);
        Some(self.nodes.len() - 1)
    }

    fn node_label(&mut self, id: &str, label: &str) -> Option<usize> {
        if let Some(&i) = self.index.get(id) {
            if let Some(node) = self.nodes.get_mut(i) {
                node.label = label.to_string();
            }
            return Some(i);
        }
        self.node_index(id, Some(label), Shape::Round)
    }
}

fn parse_graph(src: &str) -> Option<Graph> {
    let mut statements: Vec<String> = Vec::new();
    for raw_line in src.lines() {
        split_statements(raw_line, &mut statements);
    }

    let header = statements.first()?;
    let mut header_tokens = header.split_whitespace();
    let kind = header_tokens.next()?.to_ascii_lowercase();
    if kind != "graph" && kind != "flowchart" {
        return None;
    }
    let dir = match header_tokens
        .next()
        .unwrap_or("TB")
        .to_ascii_uppercase()
        .as_str()
    {
        "LR" => Dir::Right,
        "RL" => Dir::Left,
        "BT" => Dir::Up,
        _ => Dir::Down,
    };

    let mut graph = Graph {
        nodes: Vec::new(),
        edges: Vec::new(),
        index: HashMap::new(),
        groups: Vec::new(),
        node_group: Vec::new(),
        cur_group: None,
        over_cap: false,
        warnings: Vec::new(),
        dir,
    };

    let mut stack: Vec<usize> = Vec::new();
    for st in statements.iter().skip(1) {
        let first_word = st.split_whitespace().next().unwrap_or("");
        match first_word.to_ascii_lowercase().as_str() {
            "subgraph" => {
                if graph.groups.len() >= MAX_GROUPS || stack.len() >= MAX_GROUP_DEPTH {
                    return None;
                }
                let Some(rest) = st.get("subgraph".len()..) else {
                    continue;
                };
                let (id, label) = parse_subgraph_decl(rest.trim());
                graph.groups.push(Group {
                    id,
                    label,
                    parent: stack.last().copied(),
                });
                stack.push(graph.groups.len() - 1);
                graph.cur_group = stack.last().copied();
                continue;
            }
            "end" => {
                stack.pop();
                graph.cur_group = stack.last().copied();
                continue;
            }
            "classdef" | "class" | "style" | "linkstyle" | "click" | "direction" => continue,
            _ => {}
        }
        if let Some(warning) = parse_statement(st, &mut graph) {
            graph.warnings.push(warning);
        }
        if graph.over_cap {
            return None;
        }
    }

    if graph.nodes.is_empty() {
        return None;
    }
    Some(graph)
}

fn parse_subgraph_decl(rest: &str) -> (String, String) {
    if let Some(q) = rest.strip_prefix('"')
        && let Some((label, _)) = q.split_once('"')
    {
        return (label.to_string(), decode_html_entities(label));
    }
    if let Some(open) = rest.find('[') {
        let Some(id) = rest.get(..open).map(str::trim) else {
            return (rest.to_string(), rest.to_string());
        };
        let Some(label) = rest.get(open + 1..).map(|s| s.trim_end_matches(']').trim()) else {
            return (rest.to_string(), rest.to_string());
        };
        let label = clean_label(label);
        if !id.is_empty() && !label.is_empty() {
            return (id.to_string(), label);
        }
    }
    (rest.to_string(), rest.to_string())
}

fn split_statements(line: &str, out: &mut Vec<String>) {
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                in_quotes = false;
            }
            cur.push(c);
        } else {
            match c {
                '"' => {
                    in_quotes = true;
                    cur.push(c);
                }
                '%' if chars.peek() == Some(&'%') => break,
                ';' => flush_statement(&mut cur, out),
                _ => cur.push(c),
            }
        }
    }
    flush_statement(&mut cur, out);
}

fn flush_statement(cur: &mut String, out: &mut Vec<String>) {
    let trimmed = cur.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
    cur.clear();
}

pub(super) fn flowchart_warnings(src: &str) -> Vec<String> {
    parse_graph(src).map_or_else(Vec::new, |graph| graph.warnings)
}

fn parse_statement(st: &str, graph: &mut Graph) -> Option<String> {
    let chars: Vec<char> = st.chars().collect();
    let mut i = 0;

    let Some((mut prev, ni)) = parse_node_group(&chars, i, graph) else {
        return Some(format!("dropped, expected a node: \"{st}\""));
    };
    i = ni;

    loop {
        i = skip_spaces(&chars, i);
        if i >= chars.len() {
            break;
        }
        let Some((left, right, line, label, ni)) = parse_link(&chars, i) else {
            let rest: String = chars[i..].iter().collect();
            return Some(format!("dropped, expected a link: \"{rest}\""));
        };
        i = skip_spaces(&chars, ni);
        let Some((next, ni)) = parse_node_group(&chars, i, graph) else {
            let rest: String = chars[i..].iter().collect();
            return Some(format!("dropped, expected a node: \"{rest}\""));
        };
        i = ni;
        for &f in &prev {
            for &t in &next {
                if graph.edges.len() >= MAX_EDGES {
                    graph.over_cap = true;
                    return None;
                }
                let (from, to, head_to, head_from) = if left == Head::Arrow && right != Head::Arrow
                {
                    (t, f, Head::Arrow, right)
                } else {
                    (f, t, right, left)
                };
                graph.edges.push(Edge {
                    from,
                    to,
                    label: label.clone(),
                    head_to,
                    head_from,
                    line,
                });
            }
        }
        prev = next;
    }
    None
}

fn parse_node_group(
    chars: &[char],
    start: usize,
    graph: &mut Graph,
) -> Option<(Vec<usize>, usize)> {
    let (first, mut i) = parse_node(chars, start, graph)?;
    let mut group = vec![first];
    loop {
        let j = skip_spaces(chars, i);
        if chars.get(j) != Some(&'&') {
            break;
        }
        let (next, k) = parse_node(chars, j + 1, graph)?;
        group.push(next);
        i = k;
    }
    Some((group, i))
}

fn skip_spaces(chars: &[char], mut i: usize) -> usize {
    while chars.get(i).is_some_and(|&c| c == ' ' || c == '\t') {
        i += 1;
    }
    i
}

fn is_id_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn parse_node(chars: &[char], start: usize, graph: &mut Graph) -> Option<(usize, usize)> {
    let mut i = skip_spaces(chars, start);
    let id_start = i;
    while chars.get(i).copied().is_some_and(is_id_char) {
        i += 1;
    }
    if i == id_start {
        return None;
    }
    let id: String = chars.get(id_start..i)?.iter().collect();

    let (shape, label, after) = match chars.get(i) {
        Some('[') => {
            if chars.get(i + 1) == Some(&'[') {
                read_shape(chars, i + 2, "]]", Shape::Rect)
            } else if chars.get(i + 1) == Some(&'(') {
                read_shape(chars, i + 2, ")]", Shape::Round)
            } else {
                read_shape(chars, i + 1, "]", Shape::Rect)
            }
        }
        Some('(') => {
            if chars.get(i + 1) == Some(&'(') {
                read_shape(chars, i + 2, "))", Shape::Round)
            } else if chars.get(i + 1) == Some(&'[') {
                read_shape(chars, i + 2, "])", Shape::Round)
            } else {
                read_shape(chars, i + 1, ")", Shape::Round)
            }
        }
        Some('{') => {
            if chars.get(i + 1) == Some(&'{') {
                read_shape(chars, i + 2, "}}", Shape::Diamond)
            } else {
                read_shape(chars, i + 1, "}", Shape::Diamond)
            }
        }
        Some('>') => read_shape(chars, i + 1, "]", Shape::Rect),
        _ => (None, None, i),
    };

    let shape = shape.unwrap_or(Shape::Rect);
    let label = label.as_deref();
    let idx = graph.node_index(&id, label, shape)?;
    let mut after = after;
    if chars
        .get(after..)
        .is_some_and(|rest| rest.starts_with(&[':', ':', ':']))
    {
        after += 3;
        while chars
            .get(after)
            .copied()
            .is_some_and(|c| is_id_char(c) || c == '-')
        {
            after += 1;
        }
    }
    Some((idx, after))
}

fn read_shape(
    chars: &[char],
    start: usize,
    closer: &str,
    shape: Shape,
) -> (Option<Shape>, Option<String>, usize) {
    let closer: Vec<char> = closer.chars().collect();
    let mut i = start;
    let mut text = String::new();
    let quoted = {
        let mut j = start;
        while matches!(chars.get(j), Some(' ') | Some('\t')) {
            j += 1;
        }
        chars.get(j) == Some(&'"')
    };
    let mut in_quotes = false;
    while i < chars.len() {
        let Some(&c) = chars.get(i) else {
            break;
        };
        if quoted && c == '"' {
            in_quotes = !in_quotes;
            text.push(c);
            i += 1;
            continue;
        }
        if !in_quotes
            && chars
                .get(i..)
                .is_some_and(|s| s.starts_with(closer.as_slice()))
        {
            let label = clean_label(&text);
            return (Some(shape), Some(label), i + closer.len());
        }
        text.push(c);
        i += 1;
    }
    (Some(shape), Some(clean_label(&text)), chars.len())
}

fn clean_label(raw: &str) -> String {
    let stripped = strip_html_tags(raw.trim());
    let trimmed = stripped.trim();
    let unquoted = trimmed
        .strip_prefix('"')
        .and_then(|t| t.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('\'')
                .and_then(|t| t.strip_suffix('\''))
        })
        .unwrap_or(trimmed)
        .trim();
    let text = if let Some(md) = unquoted.strip_prefix('`').and_then(|t| t.strip_suffix('`')) {
        strip_markdown(md.trim())
    } else {
        unquoted.to_string()
    };
    // Decode after tag-stripping so `<b>` is removed as markup while `&lt;b&gt;` survives as a literal `<b>`
    // One decode at the single return covers both paths
    decode_html_entities(&text)
}

const ENTITY_LOOKAHEAD: usize = 10;

// Label text decodes HTML entities once: via clean_label for bracketed labels, or explicitly at each direct-push sink.
fn decode_html_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        let Some(&c) = chars.get(i) else {
            break;
        };
        if c != '&' {
            out.push(c);
            i += 1;
            continue;
        }
        // Scan window (includes the terminating `;`) so a stray `&` or over-long run stays literal.
        let hi = (i + 1 + ENTITY_LOOKAHEAD).min(chars.len());
        let semi = (i + 1..hi).find(|&j| chars.get(j) == Some(&';'));
        let decoded = semi.and_then(|j| {
            let body: String = chars.get(i + 1..j)?.iter().collect();
            decode_entity_body(&body).map(|c| (c, j))
        });
        match decoded {
            // Resume past the `;`; the single pass never re-scans emitted text, so `&amp;lt;` decodes to the literal `&lt;` rather than to `<`
            Some((c, j)) => {
                out.push(c);
                i = j + 1;
            }
            None => {
                out.push('&');
                i += 1;
            }
        }
    }
    out
}

fn decode_entity_body(body: &str) -> Option<char> {
    match body {
        "lt" => Some('<'),
        "gt" => Some('>'),
        "amp" => Some('&'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        _ => {
            let num = body.strip_prefix('#')?;
            let code = match num.strip_prefix(['x', 'X']) {
                Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                None => num.parse::<u32>().ok()?,
            };
            // Reject control chars: NUL collides with the CONT sentinel and ESC would inject ANSI into scrollback.
            char::from_u32(code).filter(|c| !c.is_control())
        }
    }
}

fn strip_markdown(s: &str) -> String {
    let no_code: String = s.chars().filter(|&c| c != '`').collect();
    let no_strong = no_code.replace("**", "").replace("__", "");
    let chars: Vec<char> = no_strong.chars().collect();
    let mut out = String::with_capacity(no_strong.len());
    for (i, &c) in chars.iter().enumerate() {
        if (c == '*' || c == '_')
            && !(i > 0
                && i.checked_sub(1)
                    .and_then(|j| chars.get(j))
                    .is_some_and(|p| p.is_alphanumeric())
                && chars.get(i + 1).is_some_and(|n| n.is_alphanumeric()))
        {
            continue;
        }
        out.push(c);
    }
    out.trim().to_string()
}

const HTML_FORMAT_TAGS: &[&str] = &[
    "b", "strong", "i", "em", "u", "s", "strike", "del", "ins", "mark", "small", "big", "sub",
    "sup", "code", "kbd", "samp", "var", "tt", "span", "font", "q", "abbr", "cite", "pre",
];

fn strip_html_tags(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        if chars.get(i) == Some(&'<')
            && let Some((name, end)) = html_tag_at(&chars, i)
        {
            let lower = name.to_ascii_lowercase();
            if lower == "br" {
                out.push(' ');
                i = end;
                continue;
            }
            if HTML_FORMAT_TAGS.contains(&lower.as_str()) {
                i = end;
                continue;
            }
        }
        let Some(&c) = chars.get(i) else {
            break;
        };
        out.push(c);
        i += 1;
    }
    out
}

fn html_tag_at(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut i = start + 1;
    if chars.get(i) == Some(&'/') {
        i += 1;
    }
    let name_start = i;
    while chars.get(i).is_some_and(|c| c.is_ascii_alphanumeric()) {
        i += 1;
    }
    if i == name_start {
        return None;
    }
    let name: String = chars.get(name_start..i)?.iter().collect();
    while chars.get(i).is_some_and(|&c| c != '>') {
        if chars.get(i) == Some(&'<') {
            return None;
        }
        i += 1;
    }
    if chars.get(i) == Some(&'>') {
        Some((name, i + 1))
    } else {
        None
    }
}

fn is_link_char(c: char) -> bool {
    matches!(c, '-' | '.' | '=' | '<' | '>')
}

fn parse_link(
    chars: &[char],
    start: usize,
) -> Option<(Head, Head, LineKind, Option<String>, usize)> {
    let mut i = skip_spaces(chars, start);
    let mut left = Head::None;
    if let Some(&c) = chars.get(i)
        && matches!(c, 'o' | 'x')
        && matches!(chars.get(i + 1), Some('-' | '.' | '='))
    {
        left = if c == 'o' { Head::Circle } else { Head::Cross };
        i += 1;
    }
    let op_start = i;
    while chars
        .get(i)
        .is_some_and(|c| matches!(c, '-' | '.' | '=' | '<' | '>'))
    {
        i += 1;
    }
    if i == op_start {
        return None;
    }
    let op1: String = chars.get(op_start..i)?.iter().collect();
    if left == Head::None && op1.starts_with('<') {
        left = Head::Arrow;
    }
    let mut line = line_kind(&op1);
    let mut right = if op1.contains('>') {
        Head::Arrow
    } else {
        Head::None
    };
    if right == Head::None
        && let Some((head, ni)) = trailing_head(chars, i)
    {
        right = head;
        i = ni;
    }

    if chars.get(i) == Some(&'|') {
        i += 1;
        let l_start = i;
        while chars.get(i).is_some_and(|&c| c != '|') {
            i += 1;
        }
        let label = clean_label(&chars.get(l_start..i)?.iter().collect::<String>());
        if chars.get(i) == Some(&'|') {
            i += 1;
        }
        return Some((left, right, line, non_empty(label), i));
    }

    if right == Head::None {
        let text_start = skip_spaces(chars, i);
        let mut j = text_start;
        while j < chars.len() && chars.get(j).is_some_and(|&c| !is_link_char(c)) {
            j += 1;
        }
        if j < chars.len()
            && j > text_start
            && chars
                .get(j)
                .is_some_and(|c| matches!(c, '-' | '.' | '=' | '>'))
        {
            let text: String = chars.get(text_start..j)?.iter().collect();
            let op2_start = j;
            while j < chars.len() && chars.get(j).copied().is_some_and(is_link_char) {
                j += 1;
            }
            let op2: String = chars.get(op2_start..j)?.iter().collect();
            right = if op2.contains('>') {
                Head::Arrow
            } else if let Some((head, nj)) = trailing_head(chars, j) {
                j = nj;
                head
            } else {
                Head::None
            };
            if line == LineKind::Solid {
                line = line_kind(&op2);
            }
            return Some((left, right, line, non_empty(clean_label(&text)), j));
        }
    }

    Some((left, right, line, None, i))
}

fn line_kind(op: &str) -> LineKind {
    if op.contains('=') {
        LineKind::Thick
    } else if op.contains('.') {
        LineKind::Dotted
    } else {
        LineKind::Solid
    }
}

fn trailing_head(chars: &[char], i: usize) -> Option<(Head, usize)> {
    let head = match chars.get(i) {
        Some('o') => Head::Circle,
        Some('x') => Head::Cross,
        _ => return None,
    };
    match chars.get(i + 1) {
        None | Some(' ') | Some('\t') | Some('|') | Some('&') | Some(';') => Some((head, i + 1)),
        _ => None,
    }
}

fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

fn parse_state(src: &str) -> Option<Graph> {
    let mut statements: Vec<String> = Vec::new();
    for raw_line in src.lines() {
        split_statements(raw_line, &mut statements);
    }
    let header = statements.first()?;
    if !header
        .split_whitespace()
        .next()?
        .to_ascii_lowercase()
        .starts_with("statediagram")
    {
        return None;
    }

    let mut graph = Graph {
        nodes: Vec::new(),
        edges: Vec::new(),
        index: HashMap::new(),
        groups: Vec::new(),
        node_group: Vec::new(),
        cur_group: None,
        over_cap: false,
        warnings: Vec::new(),
        dir: Dir::Down,
    };

    let mut in_note = false;
    for st in statements.iter().skip(1) {
        if in_note {
            if st.eq_ignore_ascii_case("end note") {
                in_note = false;
            }
            continue;
        }
        let first = st.split_whitespace().next().unwrap_or("");
        match first.to_ascii_lowercase().as_str() {
            "direction" => {
                graph.dir = match st
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "LR" => Dir::Right,
                    "RL" => Dir::Left,
                    "BT" => Dir::Up,
                    _ => Dir::Down,
                };
            }
            "note" => {
                if !st.contains(':') {
                    in_note = true;
                }
            }
            "state" => parse_state_decl(st, &mut graph)?,
            "classdef" | "class" | "hide" | "scale" | "}" | "--" => {}
            _ => {
                if st.contains("-->") {
                    parse_transition(st, &mut graph)?;
                } else {
                    parse_state_desc(st, &mut graph)?;
                }
            }
        }
        if graph.over_cap {
            return None;
        }
    }

    if graph.nodes.is_empty() {
        return None;
    }
    Some(graph)
}

fn parse_state_decl(st: &str, graph: &mut Graph) -> Option<()> {
    let rest = st.get("state".len()..)?.trim().trim_end_matches('{').trim();
    if rest.is_empty() {
        return Some(());
    }
    if let Some(q) = rest.strip_prefix('"') {
        let (label, after) = q.split_once('"')?;
        let id = after
            .trim()
            .strip_prefix("as")
            .map(str::trim)
            .unwrap_or(label);
        graph.node_label(id, &decode_html_entities(label))?;
        return Some(());
    }
    let mut shape = Shape::Round;
    let mut id = rest;
    let mut stereotyped = false;
    if let Some(pos) = rest.find("<<") {
        let stereo = rest.get(pos + 2..)?.trim_end_matches(">>").trim();
        if stereo == "choice" {
            shape = Shape::Diamond;
        }
        id = rest.get(..pos)?.trim();
        stereotyped = true;
    }
    if id.is_empty() || id.contains(char::is_whitespace) {
        return None;
    }
    let label = if stereotyped { Some(id) } else { None };
    graph.node_index(id, label, shape)?;
    Some(())
}

fn parse_transition(st: &str, graph: &mut Graph) -> Option<()> {
    let mut rest = st;
    let mut prev: Option<usize> = None;
    while let Some((lhs, rhs)) = rest.split_once("-->") {
        let from_id = lhs.trim_end().trim_end_matches('-').trim();
        let from = match prev {
            Some(p) => {
                if !from_id.is_empty() {
                    return None;
                }
                p
            }
            None => {
                if from_id.is_empty() {
                    return None;
                }
                state_endpoint(graph, from_id, true)?
            }
        };
        let (to_part, tail) = match rhs.split_once("-->") {
            Some((t, _)) => (t, rhs.get(t.len()..)?),
            None => (rhs, ""),
        };
        let (to_part, label) = match to_part.split_once(':') {
            Some((t, l)) => (t, non_empty(decode_html_entities(l.trim()))),
            None => (to_part, None),
        };
        let to_id = to_part
            .trim_start()
            .trim_start_matches('>')
            .trim_end()
            .trim_end_matches('-')
            .trim();
        if to_id.is_empty() {
            return None;
        }
        let to = state_endpoint(graph, to_id, false)?;
        if graph.edges.len() >= MAX_EDGES {
            graph.over_cap = true;
            return Some(());
        }
        graph.edges.push(Edge {
            from,
            to,
            label,
            head_to: Head::Arrow,
            head_from: Head::None,
            line: LineKind::Solid,
        });
        prev = Some(to);
        rest = tail;
    }
    Some(())
}

fn state_endpoint(graph: &mut Graph, id: &str, is_source: bool) -> Option<usize> {
    if id == "[*]" {
        let key = if is_source { "[*]start" } else { "[*]end" };
        return graph.node_index(key, Some("●"), Shape::Round);
    }
    graph.node_index(id, None, Shape::Round)
}

fn parse_state_desc(st: &str, graph: &mut Graph) -> Option<()> {
    if let Some((id, desc)) = st.split_once(':') {
        let id = id.trim();
        let desc = desc.trim();
        if id.is_empty() || id.contains(char::is_whitespace) || desc.is_empty() {
            return None;
        }
        graph.node_label(id, &decode_html_entities(desc))?;
    } else if !st.contains(char::is_whitespace) {
        graph.node_index(st, None, Shape::Round)?;
    } else {
        return None;
    }
    Some(())
}

const MAX_MEMBERS: usize = 8;
const CLASS_OPS: &[(&str, Head, Head, LineKind)] = &[
    ("<|--", Head::Triangle, Head::None, LineKind::Solid),
    ("--|>", Head::None, Head::Triangle, LineKind::Solid),
    ("<|..", Head::Triangle, Head::None, LineKind::Dotted),
    ("..|>", Head::None, Head::Triangle, LineKind::Dotted),
    ("*--", Head::DiamondFill, Head::None, LineKind::Solid),
    ("--*", Head::None, Head::DiamondFill, LineKind::Solid),
    ("o--", Head::DiamondOpen, Head::None, LineKind::Solid),
    ("--o", Head::None, Head::DiamondOpen, LineKind::Solid),
    ("<--", Head::Arrow, Head::None, LineKind::Solid),
    ("-->", Head::None, Head::Arrow, LineKind::Solid),
    ("<..", Head::Arrow, Head::None, LineKind::Dotted),
    ("..>", Head::None, Head::Arrow, LineKind::Dotted),
    ("--", Head::None, Head::None, LineKind::Solid),
    ("..", Head::None, Head::None, LineKind::Dotted),
];

#[derive(Default, Clone)]
struct ClassInfo {
    annotation: Option<String>,
    attrs: Vec<String>,
    methods: Vec<String>,
}

fn parse_class(src: &str) -> Option<(Graph, Vec<ClassInfo>)> {
    let mut statements: Vec<String> = Vec::new();
    for raw_line in src.lines() {
        split_statements(raw_line, &mut statements);
    }
    let header = statements.first()?;
    if !header
        .split_whitespace()
        .next()?
        .to_ascii_lowercase()
        .starts_with("classdiagram")
    {
        return None;
    }

    let mut graph = Graph {
        nodes: Vec::new(),
        edges: Vec::new(),
        index: HashMap::new(),
        groups: Vec::new(),
        node_group: Vec::new(),
        cur_group: None,
        over_cap: false,
        warnings: Vec::new(),
        dir: Dir::Down,
    };
    let mut infos: Vec<ClassInfo> = Vec::new();
    let mut cur_class: Option<usize> = None;

    for st in statements.iter().skip(1) {
        if let Some(ci) = cur_class {
            if st == "}" {
                cur_class = None;
            } else if let Some(info) = infos.get_mut(ci) {
                push_member(info, st);
            }
            continue;
        }
        let first = st.split_whitespace().next().unwrap_or("");
        match first.to_ascii_lowercase().as_str() {
            "direction" => {
                graph.dir = match st
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "LR" => Dir::Right,
                    "RL" => Dir::Left,
                    "BT" => Dir::Up,
                    _ => Dir::Down,
                };
                continue;
            }
            "note" | "callback" | "click" | "link" | "style" | "cssclass" | "classdef"
            | "namespace" | "}" => continue,
            "class" => {
                let Some(rest) = st.get("class".len()..) else {
                    continue;
                };
                let rest = rest.trim();
                let (name, open) = match rest.strip_suffix('{') {
                    Some(n) => (n.trim(), true),
                    None => (rest, false),
                };
                if name.is_empty() || name.contains(char::is_whitespace) {
                    return None;
                }
                let idx = graph.node_index(name, None, Shape::Rect)?;
                sync_infos(&graph, &mut infos);
                if open {
                    cur_class = Some(idx);
                }
                continue;
            }
            _ => {}
        }
        if let Some(ann) = st.strip_prefix("<<") {
            let (ann, rest) = ann.split_once(">>")?;
            let name = rest.trim();
            if name.is_empty() || name.contains(char::is_whitespace) {
                return None;
            }
            let idx = graph.node_index(name, None, Shape::Rect)?;
            sync_infos(&graph, &mut infos);
            if let Some(info) = infos.get_mut(idx) {
                info.annotation = Some(ann.trim().to_string());
            }
            continue;
        }
        if let Some((from, to, head_from, head_to, line, label)) = parse_class_relation(st) {
            let f = graph.node_index(&from, None, Shape::Rect)?;
            sync_infos(&graph, &mut infos);
            let t = graph.node_index(&to, None, Shape::Rect)?;
            sync_infos(&graph, &mut infos);
            if graph.edges.len() >= MAX_EDGES {
                return None;
            }
            graph.edges.push(Edge {
                from: f,
                to: t,
                label,
                head_to,
                head_from,
                line,
            });
            continue;
        }
        if let Some((id, member)) = st.split_once(':') {
            let id = id.trim();
            let member = member.trim();
            if id.is_empty() || id.contains(char::is_whitespace) || member.is_empty() {
                return None;
            }
            let idx = graph.node_index(id, None, Shape::Rect)?;
            sync_infos(&graph, &mut infos);
            if let Some(info) = infos.get_mut(idx) {
                push_member(info, member);
            }
            continue;
        }
        return None;
    }

    if graph.nodes.is_empty() {
        return None;
    }
    sync_infos(&graph, &mut infos);
    Some((graph, infos))
}

fn sync_infos(graph: &Graph, infos: &mut Vec<ClassInfo>) {
    while infos.len() < graph.nodes.len() {
        infos.push(ClassInfo::default());
    }
}

fn push_member(info: &mut ClassInfo, raw: &str) {
    if let Some(ann) = raw.strip_prefix("<<") {
        if let Some((ann, _)) = ann.split_once(">>") {
            info.annotation = Some(ann.trim().to_string());
        }
        return;
    }
    let member = decode_html_entities(&display_generics(raw.trim()));
    let list = if member.contains('(') {
        &mut info.methods
    } else {
        &mut info.attrs
    };
    if list.len() < MAX_MEMBERS {
        list.push(member);
    } else if list.len() == MAX_MEMBERS {
        list.push("…".to_string());
    }
}

fn parse_class_relation(
    st: &str,
) -> Option<(String, String, Head, Head, LineKind, Option<String>)> {
    let chars: Vec<char> = st.chars().collect();
    let mut found: Option<(usize, &str, Head, Head, LineKind)> = None;
    'outer: for pos in 0..chars.len() {
        for &(op, hf, ht, line) in CLASS_OPS {
            if st
                .get(char_byte(st, pos)..)
                .is_some_and(|s| s.starts_with(op))
            {
                if op.starts_with('o')
                    && pos > 0
                    && pos
                        .checked_sub(1)
                        .and_then(|j| chars.get(j))
                        .copied()
                        .is_some_and(is_id_char)
                {
                    continue;
                }
                if op.ends_with('o')
                    && chars
                        .get(pos + op.chars().count())
                        .is_some_and(|&c| is_id_char(c))
                {
                    continue;
                }
                found = Some((pos, op, hf, ht, line));
                break 'outer;
            }
        }
    }
    let (pos, op, head_from, head_to, line) = found?;
    let lhs = st.get(..char_byte(st, pos))?.trim();
    let rhs = st.get(char_byte(st, pos) + op.len()..)?.trim();

    let (lhs, card_from) = strip_cardinality_suffix(lhs);
    let (rhs, card_to) = strip_cardinality_prefix(rhs);
    let (to_id, rel_label) = match rhs.split_once(':') {
        Some((t, l)) => (t.trim(), non_empty(decode_html_entities(l.trim()))),
        None => (rhs.trim(), None),
    };
    if lhs.is_empty()
        || to_id.is_empty()
        || lhs.contains(char::is_whitespace)
        || to_id.contains(char::is_whitespace)
    {
        return None;
    }
    let label = non_empty(
        [card_from, rel_label.unwrap_or_default(), card_to]
            .iter()
            .filter(|s| !s.is_empty())
            .cloned()
            .collect::<Vec<_>>()
            .join(" "),
    );
    Some((
        lhs.to_string(),
        to_id.to_string(),
        head_from,
        head_to,
        line,
        label,
    ))
}

fn char_byte(s: &str, char_pos: usize) -> usize {
    s.char_indices()
        .nth(char_pos)
        .map(|(b, _)| b)
        .unwrap_or(s.len())
}

fn strip_cardinality_suffix(s: &str) -> (&str, String) {
    let t = s.trim_end();
    if let Some(rest) = t.strip_suffix('"')
        && let Some(q) = rest.rfind('"')
    {
        let Some(left) = rest.get(..q) else {
            return (t, String::new());
        };
        let Some(right) = rest.get(q + 1..) else {
            return (t, String::new());
        };
        return (left.trim_end(), right.to_string());
    }
    (t, String::new())
}

fn strip_cardinality_prefix(s: &str) -> (&str, String) {
    let t = s.trim_start();
    if let Some(rest) = t.strip_prefix('"')
        && let Some(q) = rest.find('"')
    {
        let Some(right) = rest.get(q + 1..) else {
            return (t, String::new());
        };
        let Some(left) = rest.get(..q) else {
            return (t, String::new());
        };
        return (right.trim_start(), left.to_string());
    }
    (t, String::new())
}

fn display_generics(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut open = false;
    for c in s.chars() {
        if c == '~' {
            out.push(if open { '>' } else { '<' });
            open = !open;
        } else {
            out.push(c);
        }
    }
    out
}

fn parse_er(src: &str) -> Option<(Graph, Vec<ClassInfo>)> {
    let mut statements: Vec<String> = Vec::new();
    for raw_line in src.lines() {
        split_statements(raw_line, &mut statements);
    }
    let header = statements.first()?;
    if !header
        .split_whitespace()
        .next()?
        .eq_ignore_ascii_case("erdiagram")
    {
        return None;
    }

    let mut graph = Graph {
        nodes: Vec::new(),
        edges: Vec::new(),
        index: HashMap::new(),
        groups: Vec::new(),
        node_group: Vec::new(),
        cur_group: None,
        over_cap: false,
        warnings: Vec::new(),
        dir: Dir::Down,
    };
    let mut infos: Vec<ClassInfo> = Vec::new();
    let mut cur_entity: Option<usize> = None;

    for st in statements.iter().skip(1) {
        if let Some(ei) = cur_entity {
            if st == "}" {
                cur_entity = None;
            } else if let Some(info) = infos.get_mut(ei) {
                push_er_attribute(info, st);
            }
            continue;
        }
        if let Some((rel, label_part)) = split_er_relationship(st) {
            let tokens: Vec<&str> = rel.split_whitespace().collect();
            let [lhs, op, rhs] = tokens.as_slice() else {
                return None;
            };
            let (card_l, card_r, line) = parse_er_op(op)?;
            let f = er_entity(&mut graph, &mut infos, lhs)?;
            let t = er_entity(&mut graph, &mut infos, rhs)?;
            if graph.edges.len() >= MAX_EDGES {
                return None;
            }
            let rel_label = label_part.map(clean_label).unwrap_or_default();
            let label = non_empty(
                [card_l.to_string(), rel_label, card_r.to_string()]
                    .iter()
                    .filter(|s| !s.is_empty())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            graph.edges.push(Edge {
                from: f,
                to: t,
                label,
                head_to: Head::None,
                head_from: Head::None,
                line,
            });
            continue;
        }
        let (decl, open) = match st.strip_suffix('{') {
            Some(d) => (d.trim(), true),
            None => (st.as_str(), false),
        };
        if decl.is_empty() || decl.split_whitespace().count() != 1 {
            return None;
        }
        let idx = er_entity(&mut graph, &mut infos, decl)?;
        if open {
            cur_entity = Some(idx);
        }
    }

    if graph.nodes.is_empty() {
        return None;
    }
    sync_infos(&graph, &mut infos);
    Some((graph, infos))
}

fn er_entity(graph: &mut Graph, infos: &mut Vec<ClassInfo>, token: &str) -> Option<usize> {
    let idx = if let Some(open) = token.find('[') {
        let id = token.get(..open)?;
        let label = clean_label(token.get(open + 1..)?.trim_end_matches(']'));
        if id.is_empty() || label.is_empty() {
            return None;
        }
        graph.node_label(id, &label)?
    } else {
        graph.node_index(token, None, Shape::Rect)?
    };
    sync_infos(graph, infos);
    Some(idx)
}

fn split_er_relationship(st: &str) -> Option<(&str, Option<&str>)> {
    let (rel, label) = match st.split_once(':') {
        Some((r, l)) => (r, Some(l.trim())),
        None => (st, None),
    };
    let has_op = rel.split_whitespace().any(|t| parse_er_op(t).is_some());
    if has_op { Some((rel, label)) } else { None }
}

fn parse_er_op(tok: &str) -> Option<(&'static str, &'static str, LineKind)> {
    if !tok.is_ascii() || tok.len() != 6 {
        return None;
    }
    let line = match tok.get(2..4)? {
        "--" => LineKind::Solid,
        ".." => LineKind::Dotted,
        _ => return None,
    };
    Some((er_card(tok.get(..2)?)?, er_card(tok.get(4..6)?)?, line))
}

fn er_card(tok: &str) -> Option<&'static str> {
    match tok {
        "|o" | "o|" => Some("0..1"),
        "||" => Some("1"),
        "}o" | "o{" => Some("0..*"),
        "}|" | "|{" => Some("1..*"),
        _ => None,
    }
}

fn push_er_attribute(info: &mut ClassInfo, raw: &str) {
    let mut parts: Vec<&str> = Vec::new();
    for tok in raw.split_whitespace() {
        if tok.starts_with('"') {
            break;
        }
        parts.push(tok);
    }
    if parts.is_empty() {
        return;
    }
    let line = decode_html_entities(&parts.join(" "));
    if info.attrs.len() < MAX_MEMBERS {
        info.attrs.push(line);
    } else if info.attrs.len() == MAX_MEMBERS {
        info.attrs.push("…".to_string());
    }
}

fn render_class(
    graph: &Graph,
    infos: &[ClassInfo],
    max_width: Option<usize>,
) -> Result<MermaidArt, Oversize> {
    let extras: Vec<NodeExtra> = graph
        .nodes
        .iter()
        .zip(infos)
        .map(|(node, info)| {
            let mut title = Vec::new();
            if let Some(a) = &info.annotation {
                title.push(format!("«{a}»"));
            }
            title.push(display_generics(&node.label));
            NodeExtra::Compartments(vec![title, info.attrs.clone(), info.methods.clone()])
        })
        .collect();
    let mut canvas = layout_canvas(graph, &extras, max_width)?;
    match graph.dir {
        Dir::Up => canvas.flip_vertical(),
        Dir::Left => canvas.flip_horizontal(),
        _ => {}
    }
    let (styled_lines, plain_lines) = canvas.to_lines();
    Ok(MermaidArt {
        styled_lines,
        plain_lines,
    })
}

const U: u8 = 1;
const D: u8 = 2;
const L: u8 = 4;
const R: u8 = 8;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Cls {
    Empty,
    Border,
    Text,
    Edge,
    EdgeLabel,
}

const STY_DOT: u8 = 1;
const STY_THICK: u8 = 2;
const STY_SOLID: u8 = 4;

struct Canvas {
    w: usize,
    h: usize,
    ch: Vec<char>,
    cls: Vec<Cls>,
    mask: Vec<u8>,
    style: Vec<u8>,
    occupied: Vec<bool>,
    cur_style: u8,
}

impl Canvas {
    fn new(w: usize, h: usize) -> Self {
        let n = w * h;
        Self {
            w,
            h,
            ch: vec![' '; n],
            cls: vec![Cls::Empty; n],
            mask: vec![0; n],
            style: vec![0; n],
            occupied: vec![false; n],
            cur_style: STY_SOLID,
        }
    }

    fn idx(&self, x: usize, y: usize) -> usize {
        y * self.w + x
    }

    fn set(&mut self, x: usize, y: usize, c: char, cls: Cls) {
        if x >= self.w || y >= self.h {
            return;
        }
        let i = self.idx(x, y);
        if let Some(ch) = self.ch.get_mut(i) {
            *ch = c;
        }
        if let Some(cl) = self.cls.get_mut(i) {
            *cl = cls;
        }
    }

    fn add_bits(&mut self, x: usize, y: usize, bits: u8) {
        if x >= self.w || y >= self.h {
            return;
        }
        let i = self.idx(x, y);
        if self.occupied.get(i) == Some(&true) {
            return;
        }
        if let Some(mask) = self.mask.get_mut(i) {
            *mask |= bits;
        }
        if let Some(style) = self.style.get_mut(i) {
            *style |= self.cur_style;
        }
        if let Some(cls) = self.cls.get_mut(i)
            && *cls != Cls::Border
        {
            *cls = Cls::Edge;
        }
    }

    fn blit(&mut self, sub: &Canvas, ox: usize, oy: usize) {
        for sy in 0..sub.h {
            for sx in 0..sub.w {
                let (x, y) = (ox + sx, oy + sy);
                if x >= self.w || y >= self.h {
                    continue;
                }
                let si = sub.idx(sx, sy);
                let di = self.idx(x, y);
                let Some(&ch) = sub.ch.get(si) else {
                    continue;
                };
                let Some(&cls) = sub.cls.get(si) else {
                    continue;
                };
                let Some(&style) = sub.style.get(si) else {
                    continue;
                };
                if let Some(dch) = self.ch.get_mut(di) {
                    *dch = ch;
                }
                if let Some(dcl) = self.cls.get_mut(di) {
                    *dcl = cls;
                }
                if let Some(dst) = self.style.get_mut(di) {
                    *dst = style;
                }
                if let Some(occ) = self.occupied.get_mut(di) {
                    *occ = true;
                }
            }
        }
    }

    fn junction(&mut self, x: usize, y: usize, bits: u8) {
        if x >= self.w || y >= self.h {
            return;
        }
        let i = self.idx(x, y);
        if let Some(mask) = self.mask.get_mut(i) {
            *mask |= bits;
        }
        if let Some(cls) = self.cls.get_mut(i)
            && *cls != Cls::Border
        {
            *cls = Cls::Edge;
        }
    }

    fn seg_v(&mut self, x: usize, y0: usize, y1: usize) {
        let (a, b) = (y0.min(y1), y0.max(y1));
        for y in a..=b {
            let mut bits = 0;
            if y > a {
                bits |= U;
            }
            if y < b {
                bits |= D;
            }
            self.add_bits(x, y, bits);
        }
    }

    fn seg_h(&mut self, y: usize, x0: usize, x1: usize) {
        let (a, b) = (x0.min(x1), x0.max(x1));
        for x in a..=b {
            let mut bits = 0;
            if x > a {
                bits |= L;
            }
            if x < b {
                bits |= R;
            }
            self.add_bits(x, y, bits);
        }
    }

    fn finalize_mask(&mut self) {
        for i in 0..self.ch.len() {
            let Some(&mask) = self.mask.get(i) else {
                continue;
            };
            if mask != 0 && self.ch.get(i) == Some(&' ') {
                let c = mask_char(mask);
                let drawn = match self.style.get(i).copied() {
                    Some(STY_DOT) => dotted_char(c),
                    Some(STY_THICK) => thick_char(c),
                    _ => c,
                };
                if let Some(ch) = self.ch.get_mut(i) {
                    *ch = drawn;
                }
            }
        }
    }

    /// Mirror top-to-bottom for `BT` (rows reorder; within-row text is unaffected, so labels stay readable).
    /// Box-drawing glyphs flip too.
    fn flip_vertical(&mut self) {
        for y in 0..self.h / 2 {
            let y2 = self.h - 1 - y;
            for x in 0..self.w {
                let (i, j) = (self.idx(x, y), self.idx(x, y2));
                self.ch.swap(i, j);
                self.cls.swap(i, j);
            }
        }
        for c in self.ch.iter_mut() {
            *c = flip_glyph_v(*c);
        }
    }

    /// Mirror left-to-right for `RL`.
    /// Mirroring reverses each row, so after flipping glyphs we reverse each text/label run back to reading order.
    fn flip_horizontal(&mut self) {
        for y in 0..self.h {
            for x in 0..self.w / 2 {
                let x2 = self.w - 1 - x;
                let (i, j) = (self.idx(x, y), self.idx(x2, y));
                self.ch.swap(i, j);
                self.cls.swap(i, j);
            }
        }
        for c in self.ch.iter_mut() {
            *c = flip_glyph_h(*c);
        }
        for y in 0..self.h {
            let mut x = 0;
            while x < self.w {
                let Some(&cls) = self.cls.get(self.idx(x, y)) else {
                    break;
                };
                if cls == Cls::Text || cls == Cls::EdgeLabel {
                    let start = self.idx(x, y);
                    while x < self.w && self.cls.get(self.idx(x, y)) == Some(&cls) {
                        x += 1;
                    }
                    let end = self.idx(x, y);
                    if let Some(run) = self.ch.get_mut(start..end) {
                        run.reverse();
                    }
                } else {
                    x += 1;
                }
            }
        }
    }

    fn to_lines(&self) -> (Vec<Vec<(String, Cls)>>, Vec<String>) {
        let mut styled = Vec::with_capacity(self.h);
        let mut plain = Vec::with_capacity(self.h);
        for y in 0..self.h {
            let mut last = self.w;
            for x in (0..self.w).rev() {
                let Some(&c) = self.ch.get(self.idx(x, y)) else {
                    continue;
                };
                if c != ' ' && c != CONT {
                    last = x + 1;
                    break;
                }
            }
            let mut spans = Vec::new();
            let mut plain_row = String::new();
            let mut run = String::new();
            let mut run_cls = Cls::Empty;
            for x in 0..last {
                let i = self.idx(x, y);
                let Some(&c) = self.ch.get(i) else {
                    continue;
                };
                if c == CONT {
                    continue;
                }
                let Some(&cls) = self.cls.get(i) else {
                    continue;
                };
                plain_row.push(c);
                if cls != run_cls && !run.is_empty() {
                    spans.push((std::mem::take(&mut run), run_cls));
                }
                run_cls = cls;
                run.push(c);
            }
            if !run.is_empty() {
                spans.push((run, run_cls));
            }
            styled.push(spans);
            plain.push(plain_row.trim_end().to_string());
        }
        (styled, plain)
    }
}

fn mask_char(mask: u8) -> char {
    match mask {
        0 => ' ',
        m if m == U || m == D || m == U | D => '│',
        m if m == L || m == R || m == L | R => '─',
        m if m == D | R => '┌',
        m if m == D | L => '┐',
        m if m == U | R => '└',
        m if m == U | L => '┘',
        m if m == U | D | R => '├',
        m if m == U | D | L => '┤',
        m if m == D | L | R => '┬',
        m if m == U | L | R => '┴',
        _ => '┼',
    }
}

fn dotted_char(c: char) -> char {
    match c {
        '─' => '╌',
        '│' => '╎',
        other => other,
    }
}

fn thick_char(c: char) -> char {
    match c {
        '─' => '━',
        '│' => '┃',
        '┌' => '┏',
        '┐' => '┓',
        '└' => '┗',
        '┘' => '┛',
        '├' => '┣',
        '┤' => '┫',
        '┬' => '┳',
        '┴' => '┻',
        '┼' => '╋',
        other => other,
    }
}

fn flip_glyph_v(c: char) -> char {
    match c {
        '┌' => '└',
        '└' => '┌',
        '┐' => '┘',
        '┘' => '┐',
        '┏' => '┗',
        '┗' => '┏',
        '┓' => '┛',
        '┛' => '┓',
        '╭' => '╰',
        '╰' => '╭',
        '╮' => '╯',
        '╯' => '╮',
        '┬' => '┴',
        '┴' => '┬',
        '┳' => '┻',
        '┻' => '┳',
        '▼' => '▲',
        '▲' => '▼',
        '▽' => '△',
        '△' => '▽',
        other => other,
    }
}

fn flip_glyph_h(c: char) -> char {
    match c {
        '┌' => '┐',
        '┐' => '┌',
        '└' => '┘',
        '┘' => '└',
        '┏' => '┓',
        '┓' => '┏',
        '┗' => '┛',
        '┛' => '┗',
        '╭' => '╮',
        '╮' => '╭',
        '╰' => '╯',
        '╯' => '╰',
        '├' => '┤',
        '┤' => '├',
        '┣' => '┫',
        '┫' => '┣',
        '▶' => '◄',
        '◄' => '▶',
        '▷' => '◁',
        '◁' => '▷',
        other => other,
    }
}

struct Placed {
    x: usize,
    y: usize,
    w: usize,
    h: usize,
    cx: usize,
    cy: usize,
    rank: usize,
}

struct NodeSizes {
    box_w: Vec<usize>,
    box_h: Vec<usize>,
    lay_w: Vec<usize>,
    lay_h: Vec<usize>,
    extra_h: Vec<usize>,
    self_label_w: Vec<usize>,
}

fn layout_flowchart(graph: &Graph, max_width: Option<usize>) -> Result<MermaidArt, Oversize> {
    let extras: Vec<NodeExtra> = (0..graph.nodes.len()).map(|_| NodeExtra::Plain).collect();
    let mut canvas = layout_canvas(graph, &extras, max_width)?;
    match graph.dir {
        Dir::Up => canvas.flip_vertical(),
        Dir::Left => canvas.flip_horizontal(),
        _ => {}
    }
    let (styled_lines, plain_lines) = canvas.to_lines();
    Ok(MermaidArt {
        styled_lines,
        plain_lines,
    })
}

enum NodeExtra {
    Plain,
    Frame(Canvas),
    Compartments(Vec<Vec<String>>),
}

fn layout_canvas(
    graph: &Graph,
    extras: &[NodeExtra],
    max_width: Option<usize>,
) -> Result<Canvas, Oversize> {
    let n = graph.nodes.len();
    if n == 0 {
        return Err(Oversize::Cells);
    }

    let ranks = compute_ranks(graph);
    let max_rank = *ranks.iter().max().unwrap_or(&0);

    let mut by_rank: Vec<Vec<usize>> = vec![Vec::new(); max_rank + 1];
    for (idx, &r) in ranks.iter().enumerate() {
        if let Some(row) = by_rank.get_mut(r) {
            row.push(idx);
        }
    }
    order_ranks(&mut by_rank, &graph.edges, &ranks);

    let wrapped: Vec<Vec<String>> = graph
        .nodes
        .iter()
        .map(|node| wrap_label(&node.label, WRAP_WIDTH, MAX_LINES))
        .collect();
    let mut box_w: Vec<usize> = extras
        .iter()
        .zip(graph.nodes.iter())
        .zip(wrapped.iter())
        .map(|((extra, node), wrap)| match extra {
            NodeExtra::Frame(sub) => {
                let title_w = fit_label(&node.label, WRAP_WIDTH).width();
                (sub.w + 2).max(title_w + 4)
            }
            NodeExtra::Compartments(sections) => {
                sections
                    .iter()
                    .flatten()
                    .map(|l| l.width())
                    .max()
                    .unwrap_or(1)
                    .max(1)
                    + 2 * PAD
                    + 2
            }
            NodeExtra::Plain => {
                wrap.iter().map(|l| l.width()).max().unwrap_or(1).max(1) + 2 * PAD + 2
            }
        })
        .collect();
    let box_h: Vec<usize> = extras
        .iter()
        .zip(wrapped.iter())
        .map(|(extra, wrap)| match extra {
            NodeExtra::Frame(sub) => sub.h + 2,
            NodeExtra::Compartments(sections) => {
                let filled = sections.iter().filter(|s| !s.is_empty()).count();
                sections.iter().map(|s| s.len()).sum::<usize>() + filled.saturating_sub(1) + 2
            }
            NodeExtra::Plain => wrap.len() + 2,
        })
        .collect();

    let mut extra_h = vec![0usize; n];
    let mut self_label_w = vec![0usize; n];
    for e in &graph.edges {
        if e.from == e.to {
            if let Some(h) = extra_h.get_mut(e.from) {
                *h = 2;
            }
            if let Some(l) = &e.label
                && let Some(w) = self_label_w.get_mut(e.from)
            {
                *w = (*w).max(l.width().min(MAX_LABEL));
            }
        }
    }
    for (w, &h) in box_w.iter_mut().zip(&extra_h) {
        if h > 0 {
            *w = (*w).max(7);
        }
    }
    let lay_w: Vec<usize> = box_w
        .iter()
        .zip(&self_label_w)
        .map(|(&w, &sl)| w + if sl > 0 { 2 * (sl + 3) } else { 0 })
        .collect();
    let lay_h: Vec<usize> = box_h.iter().zip(&extra_h).map(|(&h, &e)| h + e).collect();
    let sizes = NodeSizes {
        box_w,
        box_h,
        lay_w,
        lay_h,
        extra_h,
        self_label_w,
    };

    let mut placed: Vec<Placed> = (0..n)
        .map(|_| Placed {
            x: 0,
            y: 0,
            w: 0,
            h: 0,
            cx: 0,
            cy: 0,
            rank: 0,
        })
        .collect();

    // BT/RL reuse the TD/LR layout, then flip the finished canvas (so text stays readable) into the bottom-up / right-to-left orientation
    let vertical = matches!(graph.dir, Dir::Down | Dir::Up);
    let plan = if vertical {
        place_td(&ranks, max_rank, &by_rank, &sizes, graph, &mut placed)
    } else {
        place_lr(&ranks, max_rank, &by_rank, &sizes, graph, &mut placed)
    };
    let (canvas_w, canvas_h) = plan.canvas;

    if let Some(mw) = max_width
        && canvas_w > mw
    {
        return Err(Oversize::Width);
    }
    if canvas_w.saturating_mul(canvas_h) > MAX_CANVAS_CELLS {
        return Err(Oversize::Cells);
    }

    let mut canvas = Canvas::new(canvas_w, canvas_h);
    for (idx, extra) in extras.iter().enumerate() {
        let Some(p) = placed.get(idx) else {
            continue;
        };
        match extra {
            NodeExtra::Frame(sub) => {
                let Some(node) = graph.nodes.get(idx) else {
                    continue;
                };
                draw_frame(&mut canvas, p, &node.label, sub)
            }
            NodeExtra::Compartments(sections) => draw_class_box(&mut canvas, p, sections),
            NodeExtra::Plain => {
                let Some(wrap) = wrapped.get(idx) else {
                    continue;
                };
                let Some(node) = graph.nodes.get(idx) else {
                    continue;
                };
                draw_box(&mut canvas, p, wrap, node.shape)
            }
        }
    }
    for (i, edge) in graph.edges.iter().enumerate() {
        canvas.cur_style = match edge.line {
            LineKind::Solid => STY_SOLID,
            LineKind::Dotted => STY_DOT,
            LineKind::Thick => STY_THICK,
        };
        if edge.from == edge.to {
            if let Some(p) = placed.get(edge.from) {
                route_self(&mut canvas, p, edge);
            }
            continue;
        }
        let (Some(from), Some(to)) = (placed.get(edge.from), placed.get(edge.to)) else {
            continue;
        };
        let adjacent = to.rank == from.rank + 1;
        let Some(&band) = plan.band_end.get(from.rank) else {
            continue;
        };
        let Some(&edge_bus) = plan.edge_bus.get(i) else {
            continue;
        };
        let Some(&edge_lane) = plan.edge_lane.get(i) else {
            continue;
        };
        let bus = band + edge_bus;
        let lane = plan.lane_base + edge_lane;
        match (vertical, adjacent) {
            (true, true) => route_forward(&mut canvas, from, to, edge, bus),
            (true, false) => route_back(&mut canvas, from, to, edge, lane),
            (false, true) => route_forward_lr(&mut canvas, from, to, edge, bus),
            (false, false) => route_back_lr(&mut canvas, from, to, edge, lane),
        }
    }

    canvas.finalize_mask();
    Ok(canvas)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Item {
    Node(usize),
    Group(usize),
}

fn render_grouped(graph: &Graph, max_width: Option<usize>) -> Result<MermaidArt, Oversize> {
    let mut proxy: HashMap<usize, usize> = HashMap::new();
    for (gi, g) in graph.groups.iter().enumerate() {
        if let Some(&ni) = graph.index.get(&g.id) {
            proxy.insert(ni, gi);
        }
    }

    let group_chain = |g: Option<usize>| -> Vec<usize> {
        let mut chain = Vec::new();
        let mut cur = g;
        while let Some(gi) = cur {
            chain.push(gi);
            cur = graph.groups.get(gi).and_then(|g| g.parent);
        }
        chain.reverse();
        chain
    };
    let endpoint = |n: usize| -> (Item, Vec<usize>) {
        match proxy.get(&n) {
            Some(&gi) => (
                Item::Group(gi),
                group_chain(graph.groups.get(gi).and_then(|g| g.parent)),
            ),
            None => (
                Item::Node(n),
                group_chain(graph.node_group.get(n).copied().flatten()),
            ),
        }
    };

    let mut scope_edges: HashMap<Option<usize>, Vec<(Item, Item, usize)>> = HashMap::new();
    let mut referenced: Vec<bool> = vec![false; graph.groups.len()];
    for (ei, e) in graph.edges.iter().enumerate() {
        let (item_f, chain_f) = endpoint(e.from);
        let (item_t, chain_t) = endpoint(e.to);
        let k = chain_f
            .iter()
            .zip(&chain_t)
            .take_while(|(a, b)| a == b)
            .count();
        let scope = k.checked_sub(1).and_then(|j| chain_f.get(j)).copied();
        let f = chain_f.get(k).copied().map(Item::Group).unwrap_or(item_f);
        let t = chain_t.get(k).copied().map(Item::Group).unwrap_or(item_t);
        if let Item::Group(gi) = f
            && let Some(flag) = referenced.get_mut(gi)
        {
            *flag = true;
        }
        if let Item::Group(gi) = t
            && let Some(flag) = referenced.get_mut(gi)
        {
            *flag = true;
        }
        scope_edges.entry(scope).or_default().push((f, t, ei));
    }

    let mut direct_nodes: HashMap<Option<usize>, Vec<usize>> = HashMap::new();
    for (ni, g) in graph.node_group.iter().enumerate() {
        if !proxy.contains_key(&ni) {
            direct_nodes.entry(*g).or_default().push(ni);
        }
    }
    let mut keep = vec![false; graph.groups.len()];
    for gi in (0..graph.groups.len()).rev() {
        let has_nodes = direct_nodes.get(&Some(gi)).is_some_and(|v| !v.is_empty());
        let has_children = (0..graph.groups.len()).any(|c| {
            graph.groups.get(c).is_some_and(|g| g.parent == Some(gi)) && keep.get(c) == Some(&true)
        });
        if let Some(slot) = keep.get_mut(gi) {
            *slot = has_nodes || has_children || referenced.get(gi) == Some(&true);
        }
    }

    let mut canvas = build_scope(graph, None, &scope_edges, &direct_nodes, &keep, max_width)?;
    match graph.dir {
        Dir::Up => canvas.flip_vertical(),
        Dir::Left => canvas.flip_horizontal(),
        _ => {}
    }
    let (styled_lines, plain_lines) = canvas.to_lines();
    Ok(MermaidArt {
        styled_lines,
        plain_lines,
    })
}

fn build_scope(
    graph: &Graph,
    scope: Option<usize>,
    scope_edges: &HashMap<Option<usize>, Vec<(Item, Item, usize)>>,
    direct_nodes: &HashMap<Option<usize>, Vec<usize>>,
    keep: &[bool],
    max_width: Option<usize>,
) -> Result<Canvas, Oversize> {
    let mut items: Vec<Item> = Vec::new();
    if let Some(nodes) = direct_nodes.get(&scope) {
        items.extend(nodes.iter().map(|&n| Item::Node(n)));
    }
    let child_groups: Vec<usize> = (0..graph.groups.len())
        .filter(|&gi| {
            graph.groups.get(gi).is_some_and(|g| g.parent == scope) && keep.get(gi) == Some(&true)
        })
        .collect();
    items.extend(child_groups.iter().map(|&gi| Item::Group(gi)));

    if items.is_empty() {
        return Ok(Canvas::new(1, 1));
    }

    let mut index_of: HashMap<Item, usize> = HashMap::new();
    let mut nodes: Vec<Node> = Vec::new();
    let mut extras: Vec<NodeExtra> = Vec::new();
    for item in &items {
        index_of.insert(*item, nodes.len());
        match item {
            Item::Node(ni) => {
                let Some(node) = graph.nodes.get(*ni) else {
                    continue;
                };
                nodes.push(Node {
                    label: node.label.clone(),
                    shape: node.shape,
                });
                extras.push(NodeExtra::Plain);
            }
            Item::Group(gi) => {
                let sub = build_scope(graph, Some(*gi), scope_edges, direct_nodes, keep, None)?;
                let Some(group) = graph.groups.get(*gi) else {
                    continue;
                };
                nodes.push(Node {
                    label: group.label.clone(),
                    shape: Shape::Rect,
                });
                extras.push(NodeExtra::Frame(sub));
            }
        }
    }

    let mut edges: Vec<Edge> = Vec::new();
    if let Some(list) = scope_edges.get(&scope) {
        for (f, t, ei) in list {
            let (Some(&fi), Some(&ti)) = (index_of.get(f), index_of.get(t)) else {
                continue;
            };
            let Some(e) = graph.edges.get(*ei) else {
                continue;
            };
            edges.push(Edge {
                from: fi,
                to: ti,
                label: e.label.clone(),
                head_to: e.head_to,
                head_from: e.head_from,
                line: e.line,
            });
        }
    }

    let synth = Graph {
        nodes,
        edges,
        index: HashMap::new(),
        groups: Vec::new(),
        node_group: Vec::new(),
        cur_group: None,
        over_cap: false,
        warnings: Vec::new(),
        dir: graph.dir,
    };
    layout_canvas(&synth, &extras, max_width)
}

fn draw_class_box(canvas: &mut Canvas, p: &Placed, sections: &[Vec<String>]) {
    draw_box(canvas, p, &[], Shape::Rect);
    let inner = p.w.saturating_sub(2 * PAD + 2).max(1);
    let mut row = p.y + 1;
    let mut first = true;
    for (si, section) in sections.iter().enumerate() {
        if section.is_empty() {
            continue;
        }
        if !first {
            canvas.set(p.x, row, '├', Cls::Border);
            for x in (p.x + 1)..(p.x + p.w - 1) {
                canvas.set(x, row, '─', Cls::Border);
            }
            canvas.set(p.x + p.w - 1, row, '┤', Cls::Border);
            row += 1;
        }
        first = false;
        for line in section {
            let text = fit_label(line, inner);
            let tx = if si == 0 {
                p.x + 1 + PAD + inner.saturating_sub(text.width()) / 2
            } else {
                p.x + 1 + PAD
            };
            draw_seq_text(canvas, &text, tx, row, Cls::Text);
            row += 1;
        }
    }
}

fn draw_frame(canvas: &mut Canvas, p: &Placed, title: &str, sub: &Canvas) {
    draw_box(canvas, p, &[], Shape::Rect);
    let t = fit_label(title, p.w.saturating_sub(4));
    draw_seq_text(canvas, &format!(" {t} "), p.x + 1, p.y, Cls::Text);
    let ox = p.x + 1 + (p.w - 2 - sub.w) / 2;
    let oy = p.y + 1 + (p.h - 2 - sub.h) / 2;
    canvas.blit(sub, ox, oy);
}

fn bus_spans_td(
    graph: &Graph,
    ranks: &[usize],
    centers: &[usize],
    r: usize,
    exact: bool,
) -> Vec<(usize, usize, usize, usize, usize)> {
    graph
        .edges
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            let (Some(&cf), Some(&ct), Some(&rf), Some(&rt)) = (
                centers.get(e.from),
                centers.get(e.to),
                ranks.get(e.from),
                ranks.get(e.to),
            ) else {
                return false;
            };
            let jogs = if exact { cf != ct } else { cf.abs_diff(ct) > 1 };
            e.from != e.to && rf == r && rt == r + 1 && jogs
        })
        .filter_map(|(i, e)| {
            let cf = *centers.get(e.from)?;
            let ct = *centers.get(e.to)?;
            let a = cf.min(ct);
            let b = cf.max(ct);
            Some((a, b, e.from, e.to, i))
        })
        .collect()
}

fn lane_spans(
    graph: &Graph,
    ranks: &[usize],
    placed: &[Placed],
    vertical: bool,
) -> Vec<(usize, usize, usize, usize, usize)> {
    graph
        .edges
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            e.from != e.to
                && ranks
                    .get(e.to)
                    .zip(ranks.get(e.from))
                    .is_some_and(|(&rt, &rf)| rt != rf + 1)
        })
        .filter_map(|(i, e)| {
            let (pf, pt) = (placed.get(e.from)?, placed.get(e.to)?);
            let (a, b) = if vertical {
                (pf.cy.min(pt.cy), pf.cy.max(pt.cy))
            } else {
                (pf.cx.min(pt.cx), pf.cx.max(pt.cx))
            };
            Some((a, b, e.from, e.to, i))
        })
        .collect()
}

fn place_td(
    ranks: &[usize],
    max_rank: usize,
    by_rank: &[Vec<usize>],
    sizes: &NodeSizes,
    graph: &Graph,
    placed: &mut [Placed],
) -> RoutePlan {
    let centers = assign_positions(by_rank, &sizes.lay_w, GAP_X, &graph.edges, ranks);

    let mut edge_bus = vec![0usize; graph.edges.len()];
    let mut bus_tracks = vec![0usize; max_rank + 1];
    for (r, tracks) in bus_tracks.iter_mut().enumerate().take(max_rank) {
        let spans = bus_spans_td(graph, ranks, &centers, r, false);
        if spans.is_empty() {
            continue;
        }
        let (assigned, count) = assign_tracks(&spans);
        for (idx, slot) in assigned {
            if let Some(bus) = edge_bus.get_mut(idx) {
                *bus = slot;
            }
        }
        *tracks = count;
    }

    let rank_h: Vec<usize> = by_rank
        .iter()
        .map(|row| {
            row.iter()
                .filter_map(|&i| {
                    Some(sizes.box_h.get(i).copied()? + sizes.extra_h.get(i).copied()?)
                })
                .max()
                .unwrap_or(3)
        })
        .collect();
    let mut rank_y = vec![0usize; max_rank + 1];
    for r in 1..=max_rank {
        let Some(prev) = r.checked_sub(1) else {
            continue;
        };
        let Some(&prev_bus) = bus_tracks.get(prev) else {
            continue;
        };
        let Some(&prev_y) = rank_y.get(prev) else {
            continue;
        };
        let Some(&prev_h) = rank_h.get(prev) else {
            continue;
        };
        let gap = GAP_Y.max(prev_bus + 1);
        if let Some(y) = rank_y.get_mut(r) {
            *y = prev_y + prev_h + gap;
        }
    }
    let canvas_h =
        rank_y.get(max_rank).copied().unwrap_or(0) + rank_h.get(max_rank).copied().unwrap_or(0);
    let band_end: Vec<usize> = (0..=max_rank)
        .filter_map(|r| Some(rank_y.get(r).copied()? + rank_h.get(r).copied()?))
        .collect();

    let mut diagram_w = 1;
    for (r, row) in by_rank.iter().enumerate() {
        for &idx in row {
            let Some(&w) = sizes.box_w.get(idx) else {
                continue;
            };
            let Some(&h) = sizes.box_h.get(idx) else {
                continue;
            };
            let Some(&cx) = centers.get(idx) else {
                continue;
            };
            let extra = sizes.extra_h.get(idx).copied().unwrap_or(0);
            let Some(&ry) = rank_y.get(r) else {
                continue;
            };
            let Some(&rh) = rank_h.get(r) else {
                continue;
            };
            let x = cx.saturating_sub(w / 2);
            let y = ry + (rh - h - extra) / 2;
            if let Some(slot) = placed.get_mut(idx) {
                *slot = Placed {
                    x,
                    y,
                    w,
                    h,
                    cx,
                    cy: y + h / 2,
                    rank: r,
                };
            }
            diagram_w = diagram_w.max(x + w);
            if extra > 0
                && let Some(&sl) = sizes.self_label_w.get(idx)
                && sl > 0
            {
                diagram_w = diagram_w.max(x + w + 2 + sl);
            }
        }
    }

    let mut content_w = diagram_w;
    for e in &graph.edges {
        if e.from == e.to {
            continue;
        }
        if let Some(label) = &e.label {
            let lw = label.width().min(MAX_LABEL);
            let adjacent = ranks
                .get(e.to)
                .zip(ranks.get(e.from))
                .is_some_and(|(&rt, &rf)| rt == rf + 1);
            if adjacent {
                if let Some(p) = placed.get(e.to) {
                    content_w = content_w.max(p.cx + 2 + lw);
                }
            } else {
                content_w = content_w.max(diagram_w + lw + 1);
            }
        }
    }

    let mut edge_lane = vec![0usize; graph.edges.len()];
    let lanes = lane_spans(graph, ranks, placed, true);
    let (canvas_w, lane_base) = if lanes.is_empty() {
        (content_w, 0)
    } else {
        let (assigned, count) = assign_tracks(&lanes);
        for (idx, slot) in assigned {
            if let Some(lane) = edge_lane.get_mut(idx) {
                *lane = slot;
            }
        }
        (content_w + 1 + count, content_w + 1)
    };

    RoutePlan {
        canvas: (canvas_w, canvas_h),
        band_end,
        edge_bus,
        lane_base,
        edge_lane,
    }
}

fn place_lr(
    ranks: &[usize],
    max_rank: usize,
    by_rank: &[Vec<usize>],
    sizes: &NodeSizes,
    graph: &Graph,
    placed: &mut [Placed],
) -> RoutePlan {
    let col_w: Vec<usize> = by_rank
        .iter()
        .map(|row| {
            row.iter()
                .filter_map(|&i| sizes.box_w.get(i).copied())
                .max()
                .unwrap_or(0)
        })
        .collect();

    let max_label = graph
        .edges
        .iter()
        .filter(|e| {
            e.from == e.to
                || ranks
                    .get(e.to)
                    .zip(ranks.get(e.from))
                    .is_some_and(|(&rt, &rf)| rt == rf + 1)
        })
        .filter_map(|e| e.label.as_ref().map(|l| l.width().min(MAX_LABEL)))
        .max()
        .unwrap_or(0);
    let base_gap = (GAP_X + 1).max(max_label + 3);

    let centers = assign_positions(by_rank, &sizes.lay_h, 1, &graph.edges, ranks);

    let mut edge_bus = vec![0usize; graph.edges.len()];
    let mut bus_tracks = vec![0usize; max_rank + 1];
    for (r, tracks) in bus_tracks.iter_mut().enumerate().take(max_rank) {
        let spans = bus_spans_td(graph, ranks, &centers, r, true);
        if spans.is_empty() {
            continue;
        }
        let (assigned, count) = assign_tracks(&spans);
        for (idx, slot) in assigned {
            if let Some(bus) = edge_bus.get_mut(idx) {
                *bus = slot;
            }
        }
        *tracks = count;
    }

    let mut rank_x = vec![0usize; max_rank + 1];
    for r in 1..=max_rank {
        let Some(prev) = r.checked_sub(1) else {
            continue;
        };
        let Some(&prev_bus) = bus_tracks.get(prev) else {
            continue;
        };
        let Some(&prev_x) = rank_x.get(prev) else {
            continue;
        };
        let Some(&prev_w) = col_w.get(prev) else {
            continue;
        };
        let gap = base_gap.max(prev_bus + 1);
        if let Some(x) = rank_x.get_mut(r) {
            *x = prev_x + prev_w + gap;
        }
    }
    let extra_right = by_rank
        .get(max_rank)
        .into_iter()
        .flatten()
        .filter(|&&i| {
            sizes.extra_h.get(i).copied().unwrap_or(0) > 0
                && sizes.self_label_w.get(i).copied().unwrap_or(0) > 0
        })
        .filter_map(|&i| sizes.self_label_w.get(i).map(|&sl| 2 + sl))
        .max()
        .unwrap_or(0);
    let canvas_w = rank_x.get(max_rank).copied().unwrap_or(0)
        + col_w.get(max_rank).copied().unwrap_or(0)
        + extra_right;
    let band_end: Vec<usize> = (0..=max_rank)
        .filter_map(|r| Some(rank_x.get(r).copied()? + col_w.get(r).copied()?))
        .collect();

    let mut diagram_h = 1;
    for (r, row) in by_rank.iter().enumerate() {
        let Some(&x) = rank_x.get(r) else {
            continue;
        };
        for &idx in row {
            let Some(&w) = sizes.box_w.get(idx) else {
                continue;
            };
            let Some(&h) = sizes.box_h.get(idx) else {
                continue;
            };
            let Some(&cy) = centers.get(idx) else {
                continue;
            };
            let extra = sizes.extra_h.get(idx).copied().unwrap_or(0);
            let y = cy.saturating_sub((h + extra) / 2);
            if let Some(slot) = placed.get_mut(idx) {
                *slot = Placed {
                    x,
                    y,
                    w,
                    h,
                    cx: x + w / 2,
                    cy: y + h / 2,
                    rank: r,
                };
            }
            diagram_h = diagram_h.max(y + h + extra);
        }
    }

    let mut edge_lane = vec![0usize; graph.edges.len()];
    let lanes = lane_spans(graph, ranks, placed, false);
    let (canvas_h, lane_base) = if lanes.is_empty() {
        (diagram_h, 0)
    } else {
        let (assigned, count) = assign_tracks(&lanes);
        for (idx, slot) in assigned {
            if let Some(lane) = edge_lane.get_mut(idx) {
                *lane = slot;
            }
        }
        (diagram_h + 1 + count, diagram_h + 1)
    };

    RoutePlan {
        canvas: (canvas_w, canvas_h),
        band_end,
        edge_bus,
        lane_base,
        edge_lane,
    }
}

struct RoutePlan {
    canvas: (usize, usize),
    band_end: Vec<usize>,
    edge_bus: Vec<usize>,
    lane_base: usize,
    edge_lane: Vec<usize>,
}

fn assign_tracks(spans: &[(usize, usize, usize, usize, usize)]) -> (Vec<(usize, usize)>, usize) {
    let mut sorted = spans.to_vec();
    sorted.sort_unstable();
    let mut tracks: Vec<Vec<(usize, usize, usize, usize)>> = Vec::new();
    let mut out = Vec::with_capacity(sorted.len());
    for &(s, e, f, t, idx) in &sorted {
        let compatible = |members: &Vec<(usize, usize, usize, usize)>| {
            members
                .iter()
                .all(|&(s2, e2, f2, t2)| e2 + 2 <= s || e + 2 <= s2 || f2 == f || t2 == t)
        };
        let slot = match tracks.iter().position(compatible) {
            Some(x) => x,
            None => {
                tracks.push(Vec::new());
                tracks.len() - 1
            }
        };
        if let Some(track) = tracks.get_mut(slot) {
            track.push((s, e, f, t));
        }
        out.push((idx, slot));
    }
    (out, tracks.len())
}

/// Reorder nodes within each rank to minimize edge crossings (Sugiyama-style barycenter sweeps).
/// Alternate down/up passes sort each rank by the mean position of its forward neighbours.
/// The pass keeps the ordering with the fewest crossings between adjacent ranks.
fn order_ranks(by_rank: &mut [Vec<usize>], edges: &[Edge], ranks: &[usize]) {
    let n = ranks.len();
    if by_rank.len() < 2 || n < 3 {
        return;
    }
    let mut parents: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    for e in edges {
        if e.from != e.to
            && ranks
                .get(e.to)
                .zip(ranks.get(e.from))
                .is_some_and(|(&rt, &rf)| rt > rf)
        {
            if let Some(p) = parents.get_mut(e.to) {
                p.push(e.from);
            }
            if let Some(c) = children.get_mut(e.from) {
                c.push(e.to);
            }
        }
    }

    let mut pos = vec![0usize; n];
    let set_pos = |by_rank: &[Vec<usize>], pos: &mut Vec<usize>| {
        for row in by_rank {
            for (i, &v) in row.iter().enumerate() {
                if let Some(slot) = pos.get_mut(v) {
                    *slot = i;
                }
            }
        }
    };
    set_pos(by_rank, &mut pos);

    let mut best: Vec<Vec<usize>> = by_rank.to_vec();
    let mut best_crossings = count_crossings(edges, ranks, &pos);
    if best_crossings == 0 {
        return;
    }

    for it in 0..8 {
        if it % 2 == 0 {
            for row in by_rank.iter_mut().skip(1) {
                sort_by_barycenter(row, &parents, &pos);
                for (i, &v) in row.iter().enumerate() {
                    if let Some(slot) = pos.get_mut(v) {
                        *slot = i;
                    }
                }
            }
        } else {
            let Some(prefix) = by_rank
                .len()
                .checked_sub(1)
                .and_then(|last| by_rank.get_mut(..last))
            else {
                continue;
            };
            for row in prefix.iter_mut().rev() {
                sort_by_barycenter(row, &children, &pos);
                for (i, &v) in row.iter().enumerate() {
                    if let Some(slot) = pos.get_mut(v) {
                        *slot = i;
                    }
                }
            }
        }
        let crossings = count_crossings(edges, ranks, &pos);
        if crossings < best_crossings {
            best_crossings = crossings;
            best = by_rank.to_vec();
        }
        if best_crossings == 0 {
            break;
        }
    }

    for (row, b) in by_rank.iter_mut().zip(best) {
        *row = b;
    }
}

fn sort_by_barycenter(row: &mut [usize], neigh: &[Vec<usize>], pos: &[usize]) {
    let mut keyed: Vec<(f64, usize)> = row
        .iter()
        .map(|&v| {
            let key = match neigh.get(v) {
                Some(ns) if !ns.is_empty() => {
                    ns.iter()
                        .filter_map(|&u| pos.get(u).map(|&p| p as f64))
                        .sum::<f64>()
                        / ns.len() as f64
                }
                _ => pos.get(v).copied().unwrap_or(0) as f64,
            };
            (key, v)
        })
        .collect();
    keyed.sort_by(|a, b| a.0.total_cmp(&b.0));
    for (slot, (_, v)) in row.iter_mut().zip(keyed) {
        *slot = v;
    }
}

fn count_crossings(edges: &[Edge], ranks: &[usize], pos: &[usize]) -> usize {
    let adjacent: Vec<(usize, usize, usize)> = edges
        .iter()
        .filter(|e| {
            e.from != e.to
                && ranks
                    .get(e.to)
                    .zip(ranks.get(e.from))
                    .is_some_and(|(&rt, &rf)| rt == rf + 1)
        })
        .filter_map(|e| Some((*ranks.get(e.from)?, *pos.get(e.from)?, *pos.get(e.to)?)))
        .collect();
    let mut crossings = 0;
    for (i, a) in adjacent.iter().enumerate() {
        let Some(rest) = adjacent.get(i + 1..) else {
            continue;
        };
        for b in rest {
            if a.0 == b.0 && ((a.1 < b.1 && a.2 > b.2) || (a.1 > b.1 && a.2 < b.2)) {
                crossings += 1;
            }
        }
    }
    crossings
}

/// Assign a center coordinate (along the cross-axis) to every node so nodes line up under their neighbours.
/// Iterative barycenter relaxation straightens chains and centers branches.
/// Each node drifts toward the average of its forward neighbours while ranks keep order and a minimum `sep` between boxes.
fn assign_positions(
    by_rank: &[Vec<usize>],
    size: &[usize],
    sep: usize,
    edges: &[Edge],
    ranks: &[usize],
) -> Vec<usize> {
    let n = size.len();
    let mut parents: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    for e in edges {
        if e.from != e.to
            && ranks
                .get(e.to)
                .zip(ranks.get(e.from))
                .is_some_and(|(&rt, &rf)| rt > rf)
        {
            if let Some(p) = parents.get_mut(e.to) {
                p.push(e.from);
            }
            if let Some(c) = children.get_mut(e.from) {
                c.push(e.to);
            }
        }
    }

    let mut pos = vec![0f64; n];
    for row in by_rank {
        let mut x = 0f64;
        for &v in row {
            let half = size.get(v).copied().unwrap_or(0) as f64 / 2.0;
            x += half;
            if let Some(slot) = pos.get_mut(v) {
                *slot = x;
            }
            x += half + sep as f64;
        }
    }

    for it in 0..10 {
        if it % 2 == 0 {
            for row in by_rank.iter() {
                relax_rank(row, &parents, &mut pos, size, sep);
            }
        } else {
            for row in by_rank.iter().rev() {
                relax_rank(row, &children, &mut pos, size, sep);
            }
        }
    }

    let min_left = (0..n)
        .filter_map(|v| Some(*pos.get(v)? - *size.get(v)? as f64 / 2.0))
        .fold(f64::INFINITY, f64::min);
    let min_left = if min_left.is_finite() { min_left } else { 0.0 };
    (0..n)
        .map(|v| {
            (pos.get(v).copied().unwrap_or(0.0) - min_left)
                .round()
                .max(0.0) as usize
        })
        .collect()
}

fn relax_rank(nodes: &[usize], neigh: &[Vec<usize>], pos: &mut [f64], size: &[usize], sep: usize) {
    let n = nodes.len();
    if n == 0 {
        return;
    }
    let desired: Vec<f64> = nodes
        .iter()
        .map(|&v| match neigh.get(v) {
            Some(ns) if !ns.is_empty() => {
                ns.iter().filter_map(|&u| pos.get(u).copied()).sum::<f64>() / ns.len() as f64
            }
            _ => pos.get(v).copied().unwrap_or(0.0),
        })
        .collect();

    let half = |i: usize| {
        nodes
            .get(i)
            .and_then(|&ni| size.get(ni))
            .copied()
            .unwrap_or(0) as f64
            / 2.0
    };
    let mut left = vec![0f64; n];
    let mut right = vec![0f64; n];
    for i in 0..n {
        let Some(&d) = desired.get(i) else {
            continue;
        };
        let val = if i == 0 {
            d
        } else {
            match i.checked_sub(1).and_then(|j| left.get(j)).copied() {
                Some(prev) => d.max(prev + half(i - 1) + sep as f64 + half(i)),
                None => d,
            }
        };
        if let Some(slot) = left.get_mut(i) {
            *slot = val;
        }
    }
    for i in (0..n).rev() {
        let Some(&d) = desired.get(i) else {
            continue;
        };
        let val = if i == n - 1 {
            d
        } else {
            match right.get(i + 1).copied() {
                Some(next) => d.min(next - half(i + 1) - sep as f64 - half(i)),
                None => d,
            }
        };
        if let Some(slot) = right.get_mut(i) {
            *slot = val;
        }
    }
    for i in 0..n {
        let Some(&ni) = nodes.get(i) else {
            continue;
        };
        let Some(&l) = left.get(i) else {
            continue;
        };
        let Some(&r) = right.get(i) else {
            continue;
        };
        if let Some(slot) = pos.get_mut(ni) {
            *slot = (l + r) / 2.0;
        }
    }
    for i in 1..n {
        let Some(prev) = i.checked_sub(1) else {
            continue;
        };
        let Some(&prev_n) = nodes.get(prev) else {
            continue;
        };
        let Some(&cur_n) = nodes.get(i) else {
            continue;
        };
        let Some(&prev_p) = pos.get(prev_n) else {
            continue;
        };
        let min_p = prev_p + half(prev) + sep as f64 + half(i);
        if let Some(slot) = pos.get_mut(cur_n)
            && *slot < min_p
        {
            *slot = min_p;
        }
    }
}

fn wrap_label(label: &str, width: usize, max_lines: usize) -> Vec<String> {
    let width = width.max(1);
    let char_w = |c: char| char_width(c).max(1);
    let mut lines: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut cur_w = 0usize;
    for word in label.split_whitespace() {
        let ww = word.width();
        if ww > width {
            if !cur.is_empty() {
                lines.push(std::mem::take(&mut cur));
            }
            let mut chunk = String::new();
            let mut chunk_w = 0usize;
            for ch in word.chars() {
                let cw = char_w(ch);
                if chunk_w + cw > width && !chunk.is_empty() {
                    // Prefer breaking after the last identifier boundary so a long token is not sliced mid-segment; fall back to a per-char break
                    let carry = match chunk.rfind(LABEL_BREAK_CHARS) {
                        Some(p) => chunk.split_off(p + 1),
                        None => String::new(),
                    };
                    lines.push(std::mem::take(&mut chunk));
                    chunk_w = carry.chars().map(char_w).sum();
                    chunk = carry;
                }
                chunk.push(ch);
                chunk_w += cw;
            }
            cur = chunk;
            cur_w = chunk_w;
        } else if cur.is_empty() {
            cur.push_str(word);
            cur_w = ww;
        } else if cur_w + 1 + ww <= width {
            cur.push(' ');
            cur.push_str(word);
            cur_w += 1 + ww;
        } else {
            lines.push(std::mem::take(&mut cur));
            cur.push_str(word);
            cur_w = ww;
        }
    }
    if !cur.is_empty() {
        lines.push(cur);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    if lines.len() > max_lines {
        lines.truncate(max_lines);
        if let Some(last) = lines.last_mut() {
            let target = width.saturating_sub(1).max(1);
            let mut s = String::new();
            let mut sw = 0usize;
            for ch in last.chars() {
                let cw = char_w(ch);
                if sw + cw > target {
                    break;
                }
                s.push(ch);
                sw += cw;
            }
            s.push('…');
            *last = s;
        }
    }
    lines
}

fn fit_label(label: &str, inner: usize) -> String {
    if label.width() <= inner {
        return label.to_string();
    }
    let mut out = String::new();
    let mut used = 0usize;
    for c in label.chars() {
        let cw = char_width(c);
        if used + cw + 1 > inner {
            break;
        }
        out.push(c);
        used += cw;
    }
    out.push('…');
    out
}

fn draw_box(canvas: &mut Canvas, p: &Placed, lines: &[String], shape: Shape) {
    let (x, y, w, h) = (p.x, p.y, p.w, p.h);
    let right = x + w - 1;
    let bottom = y + h - 1;

    let (tl, tr, bl, br) = match shape {
        Shape::Round | Shape::Diamond => ('╭', '╮', '╰', '╯'),
        Shape::Rect => ('┌', '┐', '└', '┘'),
    };
    canvas.set(x, y, tl, Cls::Border);
    canvas.set(right, y, tr, Cls::Border);
    canvas.set(x, bottom, bl, Cls::Border);
    canvas.set(right, bottom, br, Cls::Border);

    for cx in (x + 1)..right {
        canvas.add_bits(cx, y, L | R);
        canvas.add_bits(cx, bottom, L | R);
    }
    for cy in (y + 1)..bottom {
        canvas.add_bits(x, cy, U | D);
        canvas.add_bits(right, cy, U | D);
    }

    for cy in y..=bottom {
        for cx in x..=right {
            let i = canvas.idx(cx, cy);
            if let Some(occ) = canvas.occupied.get_mut(i) {
                *occ = true;
            }
        }
    }

    let inner = w.saturating_sub(2 * PAD + 2).max(1);
    for (li, line) in lines.iter().enumerate() {
        let row = y + 1 + li;
        let text = fit_label(line, inner);
        let tw = text.width();
        let text_x = x + 1 + PAD + inner.saturating_sub(tw) / 2;
        let mut cur = text_x;
        for c in text.chars() {
            let cw = char_width(c).max(1);
            canvas.set(cur, row, c, Cls::Text);
            // Wide glyphs (CJK, emoji) own a second column; mark it as a continuation so the line builder doesn't emit a stray space
            for k in 1..cw {
                canvas.set(cur + k, row, CONT, Cls::Text);
            }
            cur += cw;
        }
    }
}

fn route_forward(canvas: &mut Canvas, from: &Placed, to: &Placed, edge: &Edge, bus: usize) {
    let tx = to.cx;
    let bx = if from.cx.abs_diff(tx) <= 1 {
        tx
    } else {
        from.cx
    };
    let by = from.y + from.h - 1;
    let head_row = to.y - 1;

    canvas.junction(bx, by, D);
    canvas.seg_v(bx, by, bus);
    if bx == tx {
        canvas.seg_v(bx, bus, head_row);
    } else {
        canvas.seg_h(bus, bx, tx);
        canvas.seg_v(tx, bus, head_row);
    }

    if edge.head_to == Head::None {
        canvas.add_bits(tx, head_row, U);
    } else {
        canvas.set(tx, head_row, head_glyph(edge.head_to, '▼'), Cls::Edge);
    }
    if edge.head_from != Head::None {
        canvas.set(bx, by, head_glyph(edge.head_from, '▲'), Cls::Edge);
    }

    if let Some(label) = &edge.label {
        place_label(canvas, label, head_row, tx + 1);
    }
}

fn head_glyph(head: Head, arrow: char) -> char {
    match head {
        Head::Circle => 'o',
        Head::Cross => '×',
        Head::DiamondFill => '◆',
        Head::DiamondOpen => '◇',
        Head::Triangle => match arrow {
            '▼' => '▽',
            '▲' => '△',
            '◄' => '◁',
            '▶' => '▷',
            other => other,
        },
        _ => arrow,
    }
}

fn route_self(canvas: &mut Canvas, p: &Placed, edge: &Edge) {
    let bottom = p.y + p.h - 1;
    let exit_x = p.cx + 1;
    let ret_x = p.x + p.w - 2;
    if ret_x <= exit_x || bottom + 2 >= canvas.h {
        return;
    }
    let (v, h, bl, br) = match edge.line {
        LineKind::Dotted => ('╎', '╌', '╰', '╯'),
        LineKind::Thick => ('┃', '━', '┗', '┛'),
        LineKind::Solid => ('│', '─', '╰', '╯'),
    };
    canvas.junction(exit_x, bottom, D);
    canvas.set(exit_x, bottom + 1, v, Cls::Edge);
    canvas.set(exit_x, bottom + 2, bl, Cls::Edge);
    for x in (exit_x + 1)..ret_x {
        canvas.set(x, bottom + 2, h, Cls::Edge);
    }
    canvas.set(ret_x, bottom + 2, br, Cls::Edge);
    canvas.set(ret_x, bottom + 1, head_glyph(edge.head_to, '▲'), Cls::Edge);
    if let Some(label) = &edge.label {
        place_label(canvas, label, bottom + 1, p.x + p.w + 1);
    }
}

fn route_back(canvas: &mut Canvas, from: &Placed, to: &Placed, edge: &Edge, lane_x: usize) {
    let sx = from.x + from.w - 1;
    let sy = from.cy;
    let tx = to.x + to.w - 1;
    let tyc = to.cy;

    canvas.junction(sx, sy, R);
    canvas.seg_h(sy, sx, lane_x);
    canvas.seg_v(lane_x, sy, tyc);
    canvas.seg_h(tyc, tx + 1, lane_x);

    if edge.head_to == Head::None {
        canvas.add_bits(tx + 1, tyc, R);
    } else {
        canvas.set(tx + 1, tyc, head_glyph(edge.head_to, '◄'), Cls::Edge);
    }
    if edge.head_from != Head::None {
        canvas.set(sx, sy, head_glyph(edge.head_from, '◄'), Cls::Edge);
    }

    if let Some(label) = &edge.label {
        place_label(
            canvas,
            label,
            tyc.saturating_sub(1),
            lane_x.saturating_sub(label.width() + 1),
        );
    }
}

fn route_forward_lr(canvas: &mut Canvas, from: &Placed, to: &Placed, edge: &Edge, bus: usize) {
    let rx = from.x + from.w - 1;
    let ry = from.cy;
    let ly = to.cy;
    let head_col = to.x - 1;

    canvas.junction(rx, ry, R);
    canvas.seg_h(ry, rx, bus);
    if ry == ly {
        canvas.seg_h(ry, bus, head_col);
    } else {
        canvas.seg_v(bus, ry, ly);
        canvas.seg_h(ly, bus, head_col);
    }

    if edge.head_to == Head::None {
        canvas.add_bits(head_col, ly, R);
    } else {
        canvas.set(head_col, ly, head_glyph(edge.head_to, '▶'), Cls::Edge);
    }
    if edge.head_from != Head::None {
        canvas.set(rx, ry, head_glyph(edge.head_from, '◄'), Cls::Edge);
    }

    if let Some(label) = &edge.label {
        place_label(canvas, label, ly.saturating_sub(1), bus + 1);
    }
}

fn route_back_lr(canvas: &mut Canvas, from: &Placed, to: &Placed, edge: &Edge, lane_y: usize) {
    let sx = from.cx;
    let sy = from.y + from.h - 1;
    let tx = to.cx;
    let ty = to.y + to.h - 1;

    canvas.junction(sx, sy, D);
    canvas.seg_v(sx, sy, lane_y);
    canvas.seg_h(lane_y, sx, tx);
    canvas.seg_v(tx, lane_y, ty + 1);

    if edge.head_to == Head::None {
        canvas.add_bits(tx, ty + 1, D);
    } else {
        canvas.set(tx, ty + 1, head_glyph(edge.head_to, '▲'), Cls::Edge);
    }
    if edge.head_from != Head::None {
        canvas.set(sx, sy, head_glyph(edge.head_from, '▲'), Cls::Edge);
    }

    if let Some(label) = &edge.label {
        place_label(canvas, label, lane_y.saturating_sub(1), (sx + tx) / 2);
    }
}

fn place_label(canvas: &mut Canvas, label: &str, row: usize, start_x: usize) {
    if row >= canvas.h {
        return;
    }
    let text = fit_label(label, MAX_LABEL);
    let mut x = start_x;
    for c in text.chars() {
        let cw = char_width(c).max(1);
        if x + cw > canvas.w {
            break;
        }
        let blocked = (0..cw).any(|k| {
            let i = canvas.idx(x + k, row);
            canvas.ch.get(i).is_some_and(|&c| c != ' ')
                || canvas.mask.get(i).is_some_and(|&m| m != 0)
                || canvas.occupied.get(i) == Some(&true)
        });
        if blocked {
            break;
        }
        canvas.set(x, row, c, Cls::EdgeLabel);
        for k in 1..cw {
            canvas.set(x + k, row, CONT, Cls::EdgeLabel);
        }
        x += cw;
    }
}

fn compute_ranks(graph: &Graph) -> Vec<usize> {
    let n = graph.nodes.len();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indeg = vec![0usize; n];
    for e in &graph.edges {
        if e.from != e.to {
            if let Some(ch) = children.get_mut(e.from) {
                ch.push(e.to);
            }
            if let Some(d) = indeg.get_mut(e.to) {
                *d += 1;
            }
        }
    }

    let mut color = vec![0u8; n];
    let mut dag: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut order: Vec<usize> = Vec::with_capacity(n);

    let roots: Vec<usize> = (0..n).filter(|&i| indeg.get(i) == Some(&0)).collect();
    for start in roots.iter().copied().chain(0..n) {
        if color.get(start) == Some(&0) {
            dfs_dag(start, &children, &mut color, &mut dag, &mut order);
        }
    }

    let mut rank = vec![0usize; n];
    for &u in order.iter().rev() {
        let Some(vs) = dag.get(u) else {
            continue;
        };
        for &v in vs {
            let Some(&ru) = rank.get(u) else {
                continue;
            };
            if let Some(rv) = rank.get_mut(v) {
                *rv = (*rv).max(ru + 1);
            }
        }
    }
    rank
}

fn dfs_dag(
    start: usize,
    children: &[Vec<usize>],
    color: &mut [u8],
    dag: &mut [Vec<usize>],
    order: &mut Vec<usize>,
) {
    let mut stack: Vec<(usize, usize)> = vec![(start, 0)];
    if let Some(c) = color.get_mut(start) {
        *c = 1;
    }
    while let Some(frame) = stack.last_mut() {
        let u = frame.0;
        let kids = children.get(u).map(|c| c.as_slice()).unwrap_or(&[]);
        if frame.1 < kids.len() {
            let Some(&v) = kids.get(frame.1) else {
                break;
            };
            frame.1 += 1;
            if color.get(v) == Some(&1) {
                continue;
            }
            if let Some(d) = dag.get_mut(u) {
                d.push(v);
            }
            if color.get(v) == Some(&0) {
                if let Some(c) = color.get_mut(v) {
                    *c = 1;
                }
                stack.push((v, 0));
            }
        } else {
            if let Some(c) = color.get_mut(u) {
                *c = 2;
            }
            order.push(u);
            stack.pop();
        }
    }
}

const SEQ_GAP: usize = 5;
const SEQ_OPS: &[(&str, bool, SeqHead)] = &[
    ("-->>", true, SeqHead::Arrow),
    ("->>", false, SeqHead::Arrow),
    ("--x", true, SeqHead::Cross),
    ("-x", false, SeqHead::Cross),
    ("--)", true, SeqHead::Arrow),
    ("-)", false, SeqHead::Arrow),
    ("-->", true, SeqHead::Arrow),
    ("->", false, SeqHead::Arrow),
];

#[derive(Clone, Copy, PartialEq)]
enum SeqHead {
    Arrow,
    Cross,
}

enum NoteAnchor {
    Over(usize, usize),
    Left(usize),
    Right(usize),
}

enum SeqItem {
    Message {
        from: usize,
        to: usize,
        text: Option<String>,
        dashed: bool,
        head: SeqHead,
    },
    Note {
        anchor: NoteAnchor,
        text: String,
    },
    Divider {
        text: String,
    },
}

struct Sequence {
    labels: Vec<String>,
    index: HashMap<String, usize>,
    items: Vec<SeqItem>,
}

impl Sequence {
    fn participant(&mut self, id: &str, label: Option<&str>) -> Option<usize> {
        if let Some(&i) = self.index.get(id) {
            if let Some(label) = label
                && let Some(slot) = self.labels.get_mut(i)
            {
                *slot = label.to_string();
            }
            return Some(i);
        }
        if self.labels.len() >= MAX_NODES {
            return None;
        }
        self.index.insert(id.to_string(), self.labels.len());
        self.labels.push(label.unwrap_or(id).to_string());
        Some(self.labels.len() - 1)
    }
}

fn parse_sequence(src: &str) -> Option<Sequence> {
    let mut statements: Vec<String> = Vec::new();
    for raw_line in src.lines() {
        split_statements(raw_line, &mut statements);
    }
    let header = statements.first()?;
    if !header
        .split_whitespace()
        .next()?
        .eq_ignore_ascii_case("sequencediagram")
    {
        return None;
    }

    let mut seq = Sequence {
        labels: Vec::new(),
        index: HashMap::new(),
        items: Vec::new(),
    };
    let mut autonumber = false;
    let mut msg_count = 0usize;
    let mut blocks: Vec<bool> = Vec::new();

    for st in statements.iter().skip(1) {
        let first = st.split_whitespace().next().unwrap_or("");
        match first.to_ascii_lowercase().as_str() {
            "participant" | "actor" => {
                let Some(rest) = st.get(first.len()..) else {
                    continue;
                };
                let rest = rest.trim();
                if rest.is_empty() {
                    return None;
                }
                let (id, label) = match rest.split_once(" as ") {
                    Some((id, label)) => (id.trim(), Some(clean_label(label))),
                    None => (rest, None),
                };
                seq.participant(id, label.as_deref())?;
            }
            "autonumber" => autonumber = true,
            "activate" | "deactivate" | "create" | "destroy" | "title" | "acctitle"
            | "accdescr" | "links" | "link" | "properties" => {}
            "note" => {
                let Some(rest) = st.get(first.len()..) else {
                    continue;
                };
                let rest = rest.trim();
                let (text_part, anchor) = parse_note_anchor(rest, &mut seq)?;
                if seq.items.len() >= MAX_EDGES {
                    return None;
                }
                seq.items.push(SeqItem::Note {
                    anchor,
                    text: text_part,
                });
            }
            "loop" | "alt" | "opt" | "par" | "critical" | "break" | "else" | "and" | "option" => {
                if matches!(
                    first.to_ascii_lowercase().as_str(),
                    "else" | "and" | "option"
                ) {
                    if blocks.last() != Some(&true) {
                        continue;
                    }
                } else {
                    blocks.push(true);
                }
                if seq.items.len() >= MAX_EDGES {
                    return None;
                }
                seq.items.push(SeqItem::Divider {
                    text: decode_html_entities(st),
                });
            }
            "rect" | "box" => blocks.push(false),
            "end" => {
                if blocks.pop() == Some(true) {
                    if seq.items.len() >= MAX_EDGES {
                        return None;
                    }
                    seq.items.push(SeqItem::Divider {
                        text: "end".to_string(),
                    });
                }
            }
            _ => {
                let (from, to, mut text, dashed, head) = parse_seq_message(st, &mut seq)?;
                if autonumber {
                    msg_count += 1;
                    text = Some(match text {
                        Some(t) => format!("{msg_count}. {t}"),
                        None => format!("{msg_count}."),
                    });
                }
                if seq.items.len() >= MAX_EDGES {
                    return None;
                }
                seq.items.push(SeqItem::Message {
                    from,
                    to,
                    text,
                    dashed,
                    head,
                });
            }
        }
    }

    if seq.labels.is_empty() {
        return None;
    }
    Some(seq)
}

fn parse_note_anchor(rest: &str, seq: &mut Sequence) -> Option<(String, NoteAnchor)> {
    let lower = rest.to_ascii_lowercase();
    let suffix = |r: &str| rest.len().checked_sub(r.len()).and_then(|s| rest.get(s..));
    let (ids_and_text, kind) = if let Some(r) = lower.strip_prefix("over ") {
        (suffix(r)?, 0u8)
    } else if let Some(r) = lower.strip_prefix("left of ") {
        (suffix(r)?, 1)
    } else if let Some(r) = lower.strip_prefix("right of ") {
        (suffix(r)?, 2)
    } else {
        return None;
    };
    let (ids, text) = ids_and_text.split_once(':')?;
    let text = decode_html_entities(text.trim());
    let mut parts = ids.split(',').map(str::trim).filter(|s| !s.is_empty());
    let a = seq.participant(parts.next()?, None)?;
    let anchor = match kind {
        0 => {
            let b = match parts.next() {
                Some(id) => seq.participant(id, None)?,
                None => a,
            };
            NoteAnchor::Over(a.min(b), a.max(b))
        }
        1 => NoteAnchor::Left(a),
        _ => NoteAnchor::Right(a),
    };
    Some((text, anchor))
}

fn parse_seq_message(
    st: &str,
    seq: &mut Sequence,
) -> Option<(usize, usize, Option<String>, bool, SeqHead)> {
    let mut found: Option<(usize, &str, bool, SeqHead)> = None;
    for (pos, _) in st.char_indices() {
        for &(op, dashed, head) in SEQ_OPS {
            if st.get(pos..).is_some_and(|s| s.starts_with(op)) {
                found = Some((pos, op, dashed, head));
                break;
            }
        }
        if found.is_some() {
            break;
        }
    }
    let (pos, op, dashed, head) = found?;
    let from_id = st.get(..pos)?.trim();
    if from_id.is_empty() {
        return None;
    }
    let rest = st
        .get(pos + op.len()..)?
        .trim_start()
        .trim_start_matches(['+', '-']);
    let (to_id, text) = match rest.split_once(':') {
        Some((to, text)) => (to.trim(), non_empty(decode_html_entities(text.trim()))),
        None => (rest.trim(), None),
    };
    if to_id.is_empty() {
        return None;
    }
    let from = seq.participant(from_id, None)?;
    let to = seq.participant(to_id, None)?;
    Some((from, to, text, dashed, head))
}

fn note_geometry(xs: &[usize], anchor: &NoteAnchor, text_w: usize) -> Option<(usize, usize)> {
    match *anchor {
        NoteAnchor::Over(l, r) => {
            let &xl = xs.get(l)?;
            let &xr = xs.get(r)?;
            let center = (xl + xr) / 2;
            let w = (xr - xl + 5).max(text_w + 2 * PAD + 2);
            Some((center.saturating_sub(w / 2), w))
        }
        NoteAnchor::Left(i) => {
            let w = text_w + 2 * PAD + 2;
            Some((xs.get(i)?.saturating_sub(2 + w - 1), w))
        }
        NoteAnchor::Right(i) => Some((*xs.get(i)? + 2, text_w + 2 * PAD + 2)),
    }
}

fn layout_sequence(seq: &Sequence, max_width: Option<usize>) -> Result<MermaidArt, Oversize> {
    let n = seq.labels.len();
    let labels: Vec<String> = seq
        .labels
        .iter()
        .map(|l| fit_label(l, WRAP_WIDTH))
        .collect();
    let box_w: Vec<usize> = labels
        .iter()
        .map(|l| l.width().max(1) + 2 * PAD + 2)
        .collect();
    let box_h = 3usize;

    let item_text_w = |text: &Option<String>| text.as_deref().map(|t| t.width()).unwrap_or(0);

    let mut gaps: Vec<usize> = (0..n.saturating_sub(1))
        .filter_map(|i| {
            Some(SEQ_GAP.max(box_w.get(i)?.div_ceil(2) + box_w.get(i + 1)?.div_ceil(2) + 1))
        })
        .collect();

    let mut reqs: Vec<(usize, usize, usize)> = Vec::new();
    for item in &seq.items {
        match item {
            SeqItem::Message { from, to, text, .. } => {
                let tw = item_text_w(text);
                if from != to {
                    let (l, r) = (*from.min(to), *from.max(to));
                    reqs.push((l, r, (tw + 2).max(4)));
                } else if *from + 1 < n {
                    reqs.push((*from, *from + 1, 5 + tw + 2));
                }
            }
            SeqItem::Note { anchor, text } => {
                let tw = text.width();
                match *anchor {
                    NoteAnchor::Over(l, r) if l < r => reqs.push((l, r, tw.saturating_sub(1))),
                    NoteAnchor::Over(i, _) => {
                        let half = (tw + 4).div_ceil(2) + 2;
                        if i > 0 {
                            reqs.push((i - 1, i, half));
                        }
                        if i + 1 < n {
                            reqs.push((i, i + 1, half));
                        }
                    }
                    NoteAnchor::Left(i) if i > 0 => reqs.push((i - 1, i, tw + 7)),
                    NoteAnchor::Right(i) if i + 1 < n => reqs.push((i, i + 1, tw + 7)),
                    _ => {}
                }
            }
            SeqItem::Divider { .. } => {}
        }
    }
    reqs.sort_by_key(|&(l, r, _)| r - l);
    for (l, r, need) in reqs {
        let Some(slice) = gaps.get(l..r) else {
            continue;
        };
        let cur: usize = slice.iter().sum();
        if cur < need
            && let Some(g) = r.checked_sub(1).and_then(|j| gaps.get_mut(j))
        {
            *g += need - cur;
        }
    }

    let mut xs = vec![0usize; n];
    if let (Some(x0), Some(&w0)) = (xs.first_mut(), box_w.first()) {
        *x0 = w0 / 2;
    }
    for i in 1..n {
        let Some(prev) = i.checked_sub(1) else {
            continue;
        };
        let Some(&xp) = xs.get(prev) else {
            continue;
        };
        let Some(&g) = gaps.get(prev) else {
            continue;
        };
        if let Some(slot) = xs.get_mut(i) {
            *slot = xp + g;
        }
    }

    let mut canvas_w = xs
        .last()
        .zip(box_w.last())
        .map(|(&x, &w)| x + w.div_ceil(2) + 1)
        .unwrap_or(1);
    for item in &seq.items {
        match item {
            SeqItem::Message { from, to, text, .. } if from == to => {
                if let Some(&x) = xs.get(*from) {
                    canvas_w = canvas_w.max(x + 5 + item_text_w(text) + 1);
                }
            }
            SeqItem::Note { anchor, text } => {
                if let Some((x, w)) = note_geometry(&xs, anchor, text.width()) {
                    canvas_w = canvas_w.max(x + w + 1);
                }
            }
            SeqItem::Divider { text } => {
                canvas_w = canvas_w.max(text.width() + 4);
            }
            _ => {}
        }
    }

    let mut rows: Vec<usize> = Vec::with_capacity(seq.items.len());
    let mut y = box_h + 1;
    for item in &seq.items {
        rows.push(y);
        y += match item {
            SeqItem::Message { from, to, text, .. } => {
                if from == to {
                    4
                } else if text.is_some() {
                    3
                } else {
                    2
                }
            }
            SeqItem::Note { .. } => 4,
            SeqItem::Divider { .. } => 2,
        };
    }
    let bottom_top = y;
    let canvas_h = bottom_top + box_h;

    if let Some(mw) = max_width
        && canvas_w > mw
    {
        return Err(Oversize::Width);
    }
    if canvas_w.saturating_mul(canvas_h) > MAX_CANVAS_CELLS {
        return Err(Oversize::Cells);
    }

    let mut canvas = Canvas::new(canvas_w, canvas_h);
    for i in 0..n {
        let Some(&xi) = xs.get(i) else {
            continue;
        };
        let Some(&wi) = box_w.get(i) else {
            continue;
        };
        let Some(label) = labels.get(i) else {
            continue;
        };
        for by in [0, bottom_top] {
            let p = Placed {
                x: xi.saturating_sub(wi / 2),
                y: by,
                w: wi,
                h: box_h,
                cx: xi,
                cy: by + 1,
                rank: 0,
            };
            draw_box(&mut canvas, &p, std::slice::from_ref(label), Shape::Rect);
        }
    }
    for (item, &r) in seq.items.iter().zip(&rows) {
        if let SeqItem::Note { anchor, text } = item {
            let Some((x, w)) = note_geometry(&xs, anchor, text.width()) else {
                continue;
            };
            let p = Placed {
                x,
                y: r,
                w,
                h: 3,
                cx: x + w / 2,
                cy: r + 1,
                rank: 0,
            };
            draw_box(&mut canvas, &p, std::slice::from_ref(text), Shape::Rect);
        }
    }
    for &x in &xs {
        canvas.junction(x, box_h - 1, D);
        canvas.seg_v(x, box_h, bottom_top - 1);
        canvas.junction(x, bottom_top, U);
    }

    for (item, &r) in seq.items.iter().zip(&rows) {
        match item {
            SeqItem::Message {
                from,
                to,
                text,
                dashed,
                head,
            } => {
                let line_ch = if *dashed { '╌' } else { '─' };
                if from == to {
                    let Some(&x) = xs.get(*from) else {
                        continue;
                    };
                    canvas.junction(x, r, R);
                    canvas.set(x + 1, r, line_ch, Cls::Edge);
                    canvas.set(x + 2, r, line_ch, Cls::Edge);
                    canvas.set(x + 3, r, '╮', Cls::Edge);
                    canvas.set(x + 3, r + 1, '│', Cls::Edge);
                    canvas.set(
                        x + 1,
                        r + 2,
                        if *head == SeqHead::Cross { '×' } else { '◄' },
                        Cls::Edge,
                    );
                    canvas.set(x + 2, r + 2, line_ch, Cls::Edge);
                    canvas.set(x + 3, r + 2, '╯', Cls::Edge);
                    if let Some(t) = text {
                        draw_seq_text(&mut canvas, t, x + 5, r + 1, Cls::Text);
                    }
                } else {
                    let (Some(&x0), Some(&x1)) = (xs.get(*from), xs.get(*to)) else {
                        continue;
                    };
                    let rightward = x1 > x0;
                    let arrow_row = if text.is_some() { r + 1 } else { r };
                    let (lo, hi) = (x0.min(x1), x0.max(x1));
                    canvas.junction(x0, arrow_row, if rightward { R } else { L });
                    for x in (lo + 1)..hi {
                        canvas.set(x, arrow_row, line_ch, Cls::Edge);
                    }
                    let head_ch = match (head, rightward) {
                        (SeqHead::Cross, _) => '×',
                        (SeqHead::Arrow, true) => '▶',
                        (SeqHead::Arrow, false) => '◄',
                    };
                    let head_x = if rightward { x1 - 1 } else { x1 + 1 };
                    canvas.set(head_x, arrow_row, head_ch, Cls::Edge);
                    if let Some(t) = text {
                        let span = hi - lo - 1;
                        let t = fit_label(t, span.max(1));
                        let tx = lo + 1 + span.saturating_sub(t.width()) / 2;
                        draw_seq_text(&mut canvas, &t, tx, r, Cls::Text);
                    }
                }
            }
            SeqItem::Note { .. } => {}
            SeqItem::Divider { text } => {
                for x in 0..canvas_w {
                    canvas.set(x, r, '─', Cls::Edge);
                }
                let t = fit_label(text, canvas_w.saturating_sub(4));
                draw_seq_text(&mut canvas, &format!(" {t} "), 2, r, Cls::EdgeLabel);
            }
        }
    }

    canvas.finalize_mask();
    let (styled_lines, plain_lines) = canvas.to_lines();
    Ok(MermaidArt {
        styled_lines,
        plain_lines,
    })
}

fn draw_seq_text(canvas: &mut Canvas, text: &str, x: usize, y: usize, cls: Cls) {
    let mut cur = x;
    for c in text.chars() {
        let cw = char_width(c).max(1);
        for k in 0..cw {
            if cur + k < canvas.w && y < canvas.h {
                let i = canvas.idx(cur + k, y);
                if let Some(mask) = canvas.mask.get_mut(i) {
                    *mask = 0;
                }
            }
            canvas.set(cur + k, y, if k == 0 { c } else { CONT }, cls);
        }
        cur += cw;
    }
}
