//! Native port of Pi v1.5.0's terminal LaTeX parser.
//! Copyright (c) 2025 Mario Zechner; MIT, see assets/PI-LICENSE.
//!
//! An unsupported or incomplete expression returns `None` so the caller can
//! retain the original Markdown, including delimiters.
use super::{
    math_data as data,
    math_layout::{self, LayoutNode, MARKER_END, MARKER_START, PROTECTED_SPACE},
};
use unicode_width::UnicodeWidthStr;

const NAMED_START: char = '\u{f0004}';
const NAMED_END: char = '\u{f0005}';
const NEGATIVE_SPACE: char = '\0';
const MAX_DEPTH: usize = 64;
const MAX_SOURCE: usize = 16 * 1024;
const MAX_NODES: usize = 1024;

fn lookup<'a>(table: &'a [(&str, &str)], key: &str) -> Option<&'a str> {
    table
        .iter()
        .find_map(|(name, value)| (*name == key).then_some(*value))
}
fn js_whitespace(ch: char) -> bool {
    (ch.is_whitespace() && ch != '\u{85}') || ch == '\u{feff}'
}
fn trim_js(s: &str) -> &str {
    s.trim_matches(js_whitespace)
}
fn trim_end_js(s: &str) -> &str {
    s.trim_end_matches(js_whitespace)
}
fn normalize_script(value: &str) -> String {
    let value = trim_js(value);
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if matches!(chars[i], '=' | '+' | '-') {
            while out.ends_with(js_whitespace) {
                out.pop();
            }
            out.push(chars[i]);
            i += 1;
            while i < chars.len() && js_whitespace(chars[i]) {
                i += 1;
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}
fn unicode_script(value: &str, sub: bool) -> Option<String> {
    let table = if sub {
        data::SUBSCRIPTS
    } else {
        data::SUPERSCRIPTS
    };
    let mut out = String::new();
    for ch in normalize_script(value).chars() {
        out.push_str(lookup(table, &ch.to_string())?);
    }
    Some(out)
}
fn script(value: &str, sub: bool) -> String {
    let value = normalize_script(value);
    if let Some(value) = unicode_script(&value, sub) {
        return value;
    }
    let prefix = if sub { '_' } else { '^' };
    if value.chars().count() == 1
        || (sub && !value.is_empty() && value.bytes().all(|b| b.is_ascii_alphabetic()))
    {
        format!("{prefix}{value}")
    } else {
        format!("{prefix}({value})")
    }
}
fn fraction(numerator: &str, denominator: &str) -> String {
    let numerator = trim_js(numerator);
    let denominator = trim_js(denominator);
    let simple_n =
        !numerator.is_empty() && numerator.chars().all(|c| c.is_alphanumeric() || c == '.');
    let simple_d = (!denominator.is_empty()
        && denominator.chars().all(|c| c.is_numeric() || c == '.'))
        || denominator.chars().count() == 1;
    let n = if simple_n {
        numerator.to_owned()
    } else {
        format!("({numerator})")
    };
    let d = if simple_d {
        denominator.to_owned()
    } else {
        format!("({denominator})")
    };
    format!("{n}/{d}")
}
fn root(value: &str, sign: &str) -> String {
    let value = trim_js(value);
    if !value.is_empty() && value.chars().all(|c| c.is_alphanumeric() || c == '.') {
        format!("{sign}{value}")
    } else {
        format!("{sign}({value})")
    }
}

// Equivalent to Pi's split on `\\\\` and its optional `[spacing]` suffix.
fn environment_rows(body: &str) -> Vec<&str> {
    let mut rows = Vec::new();
    let mut start = 0;
    while let Some(offset) = body[start..].find("\\\\") {
        let at = start + offset;
        rows.push(&body[start..at]);
        start = at + 2;
        if body[start..].starts_with('[')
            && let Some(end) = body[start + 1..].find(']')
            && !body[start + 1..start + 1 + end].contains('\n')
        {
            start += end + 2;
        }
    }
    rows.push(&body[start..]);
    rows
}

fn strip_column_spec(body: &str) -> &str {
    let start = body.len() - body.trim_start_matches(js_whitespace).len();
    let rest = &body[start..];
    if let Some(group) = rest.strip_prefix('{')
        && let Some(end) = group.find('}')
    {
        &group[end + 1..]
    } else {
        body
    }
}

fn case_value(row: &[String]) -> &str {
    let text = row.first().map_or("", String::as_str);
    trim_end_js(text).strip_suffix(',').unwrap_or(text)
}
fn normalize_output(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut spaced = String::new();
    for (i, &ch) in chars.iter().enumerate() {
        if ch == NAMED_START {
            if i > 0
                && (chars[i - 1].is_alphanumeric()
                    || ")]}".contains(chars[i - 1])
                    || chars[i - 1] == MARKER_END)
            {
                spaced.push(' ');
            }
        } else if ch == NAMED_END {
            if chars
                .get(i + 1)
                .is_some_and(|c| c.is_alphanumeric() || *c == '√' || *c == MARKER_START)
            {
                spaced.push(' ');
            }
        } else {
            spaced.push(ch);
        }
    }
    let lines: Vec<String> = spaced
        .split('\n')
        .map(|line| {
            let mut out = String::new();
            let mut spaces = false;
            for c in line.chars() {
                if matches!(c, ' ' | '\t') {
                    spaces = true;
                } else {
                    if spaces {
                        out.push(' ');
                    }
                    spaces = false;
                    out.push(c);
                }
            }
            trim_js(&out).to_owned()
        })
        .collect();
    let last = lines.len().saturating_sub(1);
    let joined = lines
        .iter()
        .enumerate()
        .filter(|(i, l)| !l.is_empty() || (*i > 0 && *i < last))
        .map(|(_, l)| l.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    trim_js(&joined).to_owned()
}

struct Parser<'a, 'n> {
    source: &'a str,
    pos: usize,
    nodes: &'n mut Vec<LayoutNode>,
    display: bool,
    supported: bool,
    stack_fractions: bool,
    script_depth: usize,
    depth: usize,
}
impl<'a, 'n> Parser<'a, 'n> {
    fn new(source: &'a str, nodes: &'n mut Vec<LayoutNode>, display: bool, depth: usize) -> Self {
        Self {
            source,
            pos: 0,
            nodes,
            display,
            supported: true,
            stack_fractions: true,
            script_depth: 0,
            depth,
        }
    }
    fn peek(&self) -> Option<char> {
        self.source[self.pos..].chars().next()
    }
    fn pop(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }
    fn skip(&mut self, pred: impl Fn(char) -> bool) {
        while self.peek().is_some_and(&pred) {
            self.pop();
        }
    }
    fn push_node(&mut self, node: LayoutNode) -> String {
        if self.nodes.len() >= MAX_NODES {
            self.supported = false;
            return String::new();
        }
        let index = self.nodes.len();
        self.nodes.push(node);
        math_layout::marker(index)
    }
    fn raw(mut self) -> Option<String> {
        if self.source.len() > MAX_SOURCE || self.depth > MAX_DEPTH {
            return None;
        }
        let value = self.sequence(None);
        (self.supported && self.pos == self.source.len()).then(|| normalize_output(&value))
    }
    fn sequence(&mut self, end: Option<char>) -> String {
        if self.depth >= MAX_DEPTH {
            self.supported = false;
            return String::new();
        }
        self.depth += 1;
        let mut out = String::new();
        while let Some(c) = self.peek() {
            if super::strike::frame_expired() {
                self.supported = false;
                break;
            }
            if Some(c) == end {
                self.pop();
                self.depth -= 1;
                return out;
            }
            match c {
                '}' => {
                    self.supported = false;
                    break;
                }
                '{' => {
                    self.pop();
                    out.push_str(&self.sequence(Some('}')));
                }
                '\\' => {
                    let value = self.command();
                    if value == NEGATIVE_SPACE.to_string() {
                        out.truncate(trim_end_js(&out).len());
                        if out.ends_with(NAMED_END) {
                            out.pop();
                        }
                    } else {
                        out.push_str(&value);
                    }
                }
                '^' | '_' => {
                    self.pop();
                    out.truncate(trim_end_js(&out).len());
                    let value = self.scripts(c);
                    if out.ends_with(NAMED_END) {
                        out.pop();
                        out.push_str(&value);
                        out.push(NAMED_END);
                    } else {
                        out.push_str(&value);
                    }
                }
                c if js_whitespace(c) => {
                    self.skip(js_whitespace);
                    out.push(' ');
                }
                '=' | '<' | '>' => {
                    self.pop();
                    out.truncate(trim_end_js(&out).len());
                    out.push(' ');
                    out.push(c);
                    out.push(' ');
                }
                '&' => {
                    self.pop();
                }
                '~' => {
                    self.pop();
                    out.push(' ');
                }
                '.' => {
                    self.pop();
                    let mut used = false;
                    if out.ends_with(MARKER_END)
                        && let Some(start) = out.rfind(MARKER_START)
                    {
                        let content = &out
                            [start + MARKER_START.len_utf8()..out.len() - MARKER_END.len_utf8()];
                        if let Ok(index) = content.parse::<usize>()
                            && let Some(LayoutNode::Matrix { lines, .. }) =
                                self.nodes.get_mut(index)
                            && let Some(line) = lines.last_mut()
                        {
                            line.push('.');
                            used = true;
                        }
                    }
                    if !used {
                        out.push('.');
                    }
                }
                _ => {
                    self.pop();
                    out.push(c);
                }
            }
            if !self.supported {
                break;
            }
        }
        if end.is_some() {
            self.supported = false;
        }
        self.depth -= 1;
        out
    }
    fn scripts(&mut self, initial: char) -> String {
        let mut sub = None;
        let mut sup = None;
        let mut order = Vec::new();
        let mut kind = initial;
        loop {
            self.script_depth += 1;
            let value = self.required(false);
            self.script_depth -= 1;
            if kind == '_' {
                sub = Some(value);
            } else {
                sup = Some(value);
            };
            order.push(kind);
            if order.len() > 1 {
                break;
            }
            let save = self.pos;
            self.skip(js_whitespace);
            match self.peek() {
                Some(c @ ('_' | '^')) if c != initial => {
                    self.pop();
                    kind = c;
                }
                _ => {
                    self.pos = save;
                    break;
                }
            }
        }
        let sub_unicode = sub.as_deref().and_then(|s| unicode_script(s, true));
        let sup_unicode = sup.as_deref().and_then(|s| unicode_script(s, false));
        let can_layout = [sub.as_deref(), sup.as_deref()]
            .into_iter()
            .flatten()
            .all(|v| {
                !v.contains('/')
                    && (v.contains(MARKER_START)
                        || v.chars().count() <= 1
                        || v.chars()
                            .any(|c| c.is_ascii_uppercase() || c == '*' || c == '∗'))
            });
        let needs = self.display
            && can_layout
            && (self.script_depth > 0
                || (sub.is_some() && sub_unicode.is_none())
                || (sup.is_some() && sup_unicode.is_none()));
        if needs {
            return self.push_node(LayoutNode::Script {
                lower: sub.as_deref().map(normalize_output),
                upper: sup.as_deref().map(normalize_output),
            });
        }
        order
            .into_iter()
            .map(|c| {
                if c == '_' {
                    sub_unicode
                        .clone()
                        .unwrap_or_else(|| script(sub.as_deref().unwrap_or_default(), true))
                } else {
                    sup_unicode
                        .clone()
                        .unwrap_or_else(|| script(sup.as_deref().unwrap_or_default(), false))
                }
            })
            .collect()
    }
    fn required(&mut self, stack: bool) -> String {
        let previous = self.stack_fractions;
        self.stack_fractions &= stack;
        self.skip(js_whitespace);
        let value = match self.peek() {
            None => {
                self.supported = false;
                String::new()
            }
            Some('{') => {
                self.pop();
                self.sequence(Some('}'))
            }
            Some('\\') => self.command(),
            Some(_) => self.pop().unwrap().to_string(),
        };
        self.stack_fractions = previous;
        value
    }
    fn optional(&mut self) -> Option<String> {
        self.skip(|c| matches!(c, ' ' | '\t'));
        if self.peek() != Some('[') {
            return None;
        }
        self.pop();
        let Some(offset) = self.source[self.pos..].find(']') else {
            self.supported = false;
            return None;
        };
        let value = &self.source[self.pos..self.pos + offset];
        self.pos += offset + 1;
        Some(self.nested(value, true))
    }
    fn nested(&mut self, source: &str, stack: bool) -> String {
        match Parser::new(source, self.nodes, self.display && stack, self.depth + 1).raw() {
            Some(value) => value,
            None => {
                self.supported = false;
                source.to_owned()
            }
        }
    }
    fn command(&mut self) -> String {
        self.pop();
        let Some(first) = self.pop() else {
            self.supported = false;
            return String::new();
        };
        if matches!(first, '\n' | '\r') {
            if first == '\r' && self.peek() == Some('\n') {
                self.pop();
            }
            return " ".into();
        }
        let mut command = first.to_string();
        if first.is_ascii_alphabetic() {
            while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
                command.push(self.pop().unwrap());
            }
        }
        let c = command.as_str();
        if c == "\\" {
            return "\n".into();
        }
        if data::SPACING_COMMANDS.contains(&c) {
            return " ".into();
        }
        if data::NEGATIVE_SPACING_COMMANDS.contains(&c) {
            return NEGATIVE_SPACE.to_string();
        }
        if data::FONT_SWITCH_COMMANDS.contains(&c) {
            self.skip(js_whitespace);
            return String::new();
        }
        if data::IGNORED_COMMANDS.contains(&c) {
            return String::new();
        }
        if ["{", "}", "$", "%", "#", "_", "&"].contains(&c) {
            return command;
        }
        if c == "|" {
            return "‖".into();
        }
        if c == "not" {
            let value = self.required(false);
            let value = trim_js(&value);
            if let Some(symbol) = lookup(data::NEGATED_SYMBOLS, value) {
                return format!(" {symbol} ");
            }
            let mut chars = value.chars();
            let Some(first) = chars.next() else {
                self.supported = false;
                return String::new();
            };
            return format!(" {first}\u{338}{} ", chars.collect::<String>());
        }
        if data::LIMIT_OPERATORS.contains(&c) {
            return self.operator(c, true, true, true);
        }
        if let Some(symbol) = lookup(data::SYMBOLS, c) {
            if data::DISPLAY_LIMIT_SYMBOLS.contains(&c) {
                return self.operator(symbol, false, true, false);
            }
            return if c == "cdot" || c == "times" || data::RELATION_COMMANDS.contains(&c) {
                format!(" {symbol} ")
            } else {
                symbol.into()
            };
        }
        if data::NAMED_OPERATORS.contains(&c) {
            return format!("{NAMED_START}{c}{NAMED_END}");
        }
        if data::SIZE_COMMANDS.contains(&c) {
            return String::new();
        }
        if ["left", "middle", "right"].contains(&c) {
            if self.peek() == Some('.') {
                self.pop();
            }
            return String::new();
        }
        if ["frac", "dfrac", "tfrac"].contains(&c) {
            let stacked = self.display && self.stack_fractions && c != "tfrac";
            let numerator = self.required(!stacked);
            let denominator = self.required(!stacked);
            if stacked {
                return self.push_node(LayoutNode::Fraction {
                    numerator: normalize_output(&numerator),
                    denominator: normalize_output(&denominator),
                });
            }
            return fraction(&numerator, &denominator);
        }
        if c == "sqrt" {
            let degree = self.optional();
            let value = self.required(true);
            return match degree.as_deref().map(trim_js) {
                None | Some("2") => root(&value, "√"),
                Some("3") => root(&value, "∛"),
                Some("4") => root(&value, "∜"),
                Some(n) => format!("{}{}", script(n, false), root(&value, "√")),
            };
        }
        if ["boxed", "fbox"].contains(&c) {
            return format!("[{}]", trim_js(&self.required(true)));
        }
        if ["binom", "dbinom", "tbinom"].contains(&c) {
            let a = self.required(true);
            let b = self.required(true);
            return format!("({a} choose {b})");
        }
        if let Some(accent) = lookup(data::ACCENTS, c) {
            let value = self.required(true);
            return if value.chars().count() == 1 {
                format!("{value}{accent}")
            } else {
                format!("{c}({value})")
            };
        }
        if c == "mathbb" {
            let value = self.required(true);
            return value
                .chars()
                .map(|ch| {
                    lookup(data::BLACKBOARD, &ch.to_string())
                        .map_or_else(|| ch.to_string(), str::to_owned)
                })
                .collect();
        }
        if c == "operatorname" {
            let starred = self.peek() == Some('*');
            if starred {
                self.pop();
            }
            let value = normalize_output(&self.required(true));
            return self.operator(trim_js(&value), true, starred, true);
        }
        if ["mod", "bmod"].contains(&c) {
            return " mod ".into();
        }
        if ["pmod", "pod"].contains(&c) {
            let value = self.required(true);
            return if c == "pmod" {
                format!(" (mod {})", trim_js(&value))
            } else {
                format!(" ({})", trim_js(&value))
            };
        }
        if ["overset", "stackrel", "underset"].contains(&c) {
            let annotation = self.required(true);
            let value = self.required(true);
            return format!(
                "{}{}",
                trim_js(&value),
                script(&annotation, c == "underset")
            );
        }
        if data::PLAIN_WRAPPERS.contains(&c) {
            let value = self.required(true);
            return if c.starts_with("text") || c == "mbox" {
                value
            } else {
                trim_js(&value).into()
            };
        }
        if c == "begin" {
            return self.environment();
        }
        self.supported = false;
        format!("\\{command}")
    }
    fn operator(&mut self, value: &str, bracket: bool, limits: bool, spaced: bool) -> String {
        let mut limits = limits;
        let save = self.pos;
        self.skip(|c| matches!(c, ' ' | '\t'));
        let rest = &self.source[self.pos..];
        let modifier = if rest.starts_with("\\nolimits") {
            Some((false, 9))
        } else if rest.starts_with("\\limits") {
            Some((true, 7))
        } else {
            None
        };
        if let Some((setting, len)) = modifier
            && rest[len..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_ascii_alphabetic())
        {
            limits = setting;
            self.pos += len;
        } else {
            self.pos = save;
        }
        let mut lower = None;
        let mut upper = None;
        loop {
            let save = self.pos;
            self.skip(|c| matches!(c, ' ' | '\t'));
            let Some(kind @ ('_' | '^')) = self.peek() else {
                self.pos = save;
                break;
            };
            self.pop();
            let text = normalize_output(&self.required(false)).replace(' ', "");
            let slot = if kind == '_' { &mut lower } else { &mut upper };
            if slot.is_some() {
                self.supported = false;
            }
            *slot = Some(text);
        }
        if self.display && limits && (lower.is_some() || upper.is_some()) {
            return self.push_node(LayoutNode::Operator {
                operator: value.into(),
                lower,
                upper,
            });
        }
        let mut out = value.to_owned();
        if let Some(lower) = lower {
            if bracket {
                out.push_str(&format!("[{lower}]"));
            } else {
                out.push_str(&script(&lower, true));
            }
        }
        if let Some(upper) = upper {
            out.push_str(&script(&upper, false));
        }
        if spaced { format!(" {out} ") } else { out }
    }
    fn raw_group(&mut self) -> Option<String> {
        self.skip(|c| matches!(c, ' ' | '\t'));
        if self.pop() != Some('{') {
            self.supported = false;
            return None;
        }
        let start = self.pos;
        let mut depth = 1usize;
        while let Some(ch) = self.pop() {
            if ch == '\\' {
                self.pop();
                continue;
            }
            if ch == '{' {
                depth += 1;
            }
            if ch == '}' {
                depth -= 1;
            }
            if depth == 0 {
                return Some(self.source[start..self.pos - 1].to_owned());
            }
        }
        self.supported = false;
        None
    }

    fn environment(&mut self) -> String {
        let Some(name) = self.raw_group().filter(|s| !s.is_empty()) else {
            return String::new();
        };
        let end_marker = format!("\\end{{{name}}}");
        let Some(end) = self.source[self.pos..].find(&end_marker) else {
            self.supported = false;
            return String::new();
        };
        let body = self.source[self.pos..self.pos + end].to_owned();
        self.pos += end + end_marker.len();
        if ["equation", "equation*", "displaymath"].contains(&name.as_str()) {
            return trim_js(&self.nested(&body, true)).to_owned();
        }
        if [
            "aligned",
            "align",
            "align*",
            "alignedat",
            "alignat",
            "alignat*",
            "gather",
            "gathered",
            "multline",
            "multline*",
            "split",
        ]
        .contains(&name.as_str())
        {
            let aligned_at = ["alignedat", "alignat", "alignat*"].contains(&name.as_str());
            let body = if aligned_at {
                strip_column_spec(&body)
            } else {
                &body
            };
            let mut result = Vec::new();
            for row in environment_rows(body) {
                let cells: Vec<_> = row.split('&').collect();
                let input = if aligned_at {
                    cells
                        .chunks(2)
                        .map(|pair| pair.join(""))
                        .collect::<Vec<_>>()
                        .join(" ")
                } else {
                    cells.join("")
                };
                let rendered = self.nested(&input, true);
                let rendered = trim_js(&rendered);
                if !rendered.is_empty() {
                    result.push(rendered.to_owned());
                }
            }
            return result.join("\n");
        }
        if ["cases", "cases*"].contains(&name.as_str()) {
            return self.cases(&body);
        }
        if [
            "array",
            "matrix",
            "smallmatrix",
            "pmatrix",
            "bmatrix",
            "Bmatrix",
            "vmatrix",
            "Vmatrix",
        ]
        .contains(&name.as_str())
        {
            let body = if name == "array" {
                strip_column_spec(&body)
            } else {
                &body
            };
            return self.matrix(&name, body);
        }
        self.supported = false;
        body
    }

    fn environment_cells(&mut self, body: &str) -> Vec<Vec<String>> {
        let mut rows = Vec::new();
        for row in environment_rows(body) {
            let mut cells = Vec::new();
            for cell in row.split('&') {
                let rendered = self.nested(cell, false);
                cells.push(trim_js(&rendered).to_owned());
            }
            if cells.iter().any(|cell| !cell.is_empty()) {
                rows.push(cells);
            }
        }
        rows
    }

    fn cases(&mut self, body: &str) -> String {
        let rows = self.environment_cells(body);
        let max_width = rows
            .iter()
            .map(|row| UnicodeWidthStr::width(case_value(row)))
            .max()
            .unwrap_or(0);
        let mut contents = Vec::new();
        for row in &rows {
            let value = case_value(row);
            let condition = row.get(1).map_or("", String::as_str);
            if condition.is_empty() {
                contents.push(value.to_owned());
                continue;
            }
            let lower = condition.to_ascii_lowercase();
            let named = ["if", "when", "for", "otherwise"].iter().any(|word| {
                lower.strip_prefix(word).is_some_and(|rest| {
                    rest.chars()
                        .next()
                        .is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_')
                })
            });
            let padding = max_width.saturating_sub(UnicodeWidthStr::width(value));
            contents.push(format!(
                "{value}{}{condition_prefix}{condition}",
                PROTECTED_SPACE.to_string().repeat(padding),
                condition_prefix = if named { " " } else { " if " }
            ));
        }
        if contents.len() <= 1 {
            return contents
                .first()
                .map_or_else(String::new, |s| format!("⎧ {s}"));
        }
        let middle = contents.len() / 2;
        let mut visual: Vec<Option<String>> = contents.into_iter().map(Some).collect();
        if visual.len() % 2 == 0 {
            visual.insert(middle, None);
        }
        let last = visual.len() - 1;
        let lines = visual
            .into_iter()
            .enumerate()
            .map(|(i, content)| {
                let delimiter = if i == 0 {
                    "⎧"
                } else if i == last {
                    "⎩"
                } else {
                    "⎨"
                };
                content.map_or_else(
                    || delimiter.to_owned(),
                    |content| format!("{delimiter} {content}"),
                )
            })
            .collect();
        self.push_node(LayoutNode::Matrix {
            lines,
            baseline: middle,
        })
    }

    fn matrix(&mut self, name: &str, body: &str) -> String {
        let matrix = self.environment_cells(body);
        let count = matrix.iter().map(Vec::len).max().unwrap_or(0);
        let widths: Vec<_> = (0..count)
            .map(|column| {
                matrix
                    .iter()
                    .map(|row| UnicodeWidthStr::width(row.get(column).map_or("", String::as_str)))
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let rows: Vec<_> = matrix
            .iter()
            .map(|row| {
                (0..count)
                    .map(|column| {
                        let cell = row.get(column).map_or("", String::as_str);
                        format!(
                            "{cell}{}",
                            PROTECTED_SPACE.to_string().repeat(
                                widths[column].saturating_sub(UnicodeWidthStr::width(cell))
                            )
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" │ ")
            })
            .collect();
        let delimiters = match name {
            "pmatrix" => Some(["⎛", "⎞", "⎜", "⎟", "⎝", "⎠"]),
            "bmatrix" => Some(["⎡", "⎤", "⎢", "⎥", "⎣", "⎦"]),
            "Bmatrix" => Some(["⎧", "⎫", "⎨", "⎬", "⎩", "⎭"]),
            "vmatrix" => Some(["│"; 6]),
            "Vmatrix" => Some(["║"; 6]),
            _ => None,
        };
        let last = rows.len().saturating_sub(1);
        let lines: Vec<_> = rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| {
                if let Some(d) = delimiters {
                    let (left, right) = if i == 0 {
                        (d[0], d[1])
                    } else if i == last {
                        (d[4], d[5])
                    } else {
                        (d[2], d[3])
                    };
                    format!("{left} {row} {right}")
                } else {
                    row
                }
            })
            .collect();
        if lines.len() <= 1 {
            return lines.into_iter().next().unwrap_or_default();
        }
        self.push_node(LayoutNode::Matrix { lines, baseline: 0 })
    }
}

pub(super) fn render(source: &str, display: bool) -> Option<String> {
    let mut nodes = Vec::new();
    let rendered = Parser::new(source, &mut nodes, display, 0).raw()?;
    if nodes.is_empty() {
        return Some(rendered.replace(PROTECTED_SPACE, " "));
    }
    let layout = math_layout::render(&rendered, &nodes)?;
    let indentation = layout
        .lines
        .iter()
        .filter(|line| !trim_js(line).is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    let result = layout
        .lines
        .iter()
        .map(|line| line.get(indentation..).unwrap_or(line).trim_end())
        .collect::<Vec<_>>()
        .join("\n");
    Some(result.trim_end().replace(PROTECTED_SPACE, " "))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_pinned_pi_math_renderer_fixtures() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/math/pi-v150-render.json"
        ))
        .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let source = case["source"].as_str().unwrap();
            let display = case["display"].as_bool().unwrap();
            assert_eq!(
                render(source, display).as_deref(),
                case["expected"].as_str(),
                "source={source:?}, display={display}"
            );
        }
    }
    #[test]
    fn inline_math_matches_common_reference_expressions() {
        for (source, expected) in [
            (r"\mathbb{C}^3 \to \mathbb{C}^3", "ℂ³ → ℂ³"),
            (r"\frac{1}{2}", "1/2"),
            (r"F_1 = u^2", "F₁ = u²"),
            (r"s \to \infty", "s → ∞"),
            (r"\sqrt[3]{x}", "∛x"),
        ] {
            assert_eq!(render(source, false).as_deref(), Some(expected), "{source}");
        }
        assert_eq!(render(r"x+\unknown{y}", false), None);
        assert_eq!(render(r"\frac{1", false), None);
    }
    #[test]
    fn display_fraction_uses_stacked_layout() {
        assert_eq!(
            render(r"E \approx \frac{0.1\ \text{lux}}{100\ \text{lm/W}}", true).as_deref(),
            Some("    0.1 lux\nE ≈ ────────\n    100 lm/W")
        );
    }
    #[test]
    fn display_matrix_and_cases_match_pi_layout() {
        assert_eq!(
            render(
                r"A=\begin{pmatrix}\pi & 0\\0 & \frac{1}{\pi}\end{pmatrix}.",
                true
            )
            .as_deref(),
            Some("A = ⎛ π │ 0   ⎞\n    ⎝ 0 │ 1/π ⎠.")
        );
        assert_eq!(
            render(
                r"f(x)=\begin{cases}x, & if x<0\\-x & otherwise\end{cases}",
                true
            )
            .as_deref(),
            Some("       ⎧ x  if x < 0\nf(x) = ⎨\n       ⎩ -x otherwise")
        );
        assert_eq!(render(r"\begin{unknown}x\end{unknown}", true), None);
    }
}
