//! Incremental previews of tool-call arguments. The completed call is validated
//! independently; these values are only used for display and RPC snapshots.
use serde_json::{Map, Number, Value};

// Match serde_json's default nesting limit and keep path copies bounded.
const MAX_DEPTH: usize = 128;

#[derive(Clone)]
enum Segment {
    Key(String),
    Index(usize),
}
type Path = Vec<Segment>;

#[derive(Clone, Copy)]
enum Phase {
    Key,
    Colon,
    ObjectValue,
    ObjectComma,
    ArrayValue,
    ArrayComma,
}
struct Frame {
    path: Path,
    phase: Phase,
    key: String,
    len: usize,
}
struct StringToken {
    path: Option<Path>, // None is an object key.
    text: String,
    escape: String,
}
struct ScalarToken {
    path: Path,
    text: String,
}

#[derive(Default)]
pub(super) struct StreamingJson {
    root: Option<Value>,
    stack: Vec<Frame>,
    string: Option<StringToken>,
    scalar: Option<ScalarToken>,
    done: bool,
}

impl StreamingJson {
    pub fn append(&mut self, fragment: &str) -> Value {
        for c in fragment.chars() {
            if self.done {
                break;
            }
            self.character(c);
        }
        if let Some(token) = &self.string
            && let Some(path) = &token.path
        {
            let path = path.clone();
            let value = Value::String(token.text.clone());
            self.set(&path, value);
        }
        let mut root = self
            .root
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()));
        if let Some(scalar) = &self.scalar
            && let Some(value) = scalar_value(&scalar.text)
        {
            set_in(&mut root, &scalar.path, value);
        }
        root
    }

    fn character(&mut self, c: char) {
        if let Some(mut token) = self.string.take() {
            if string_character(&mut token, c) {
                if let Some(path) = token.path {
                    self.set(&path, Value::String(token.text));
                    if self.stack.is_empty() {
                        self.done = true;
                    }
                } else if let Some(frame) = self.stack.last_mut() {
                    frame.key = token.text;
                    frame.phase = Phase::Colon;
                }
            } else {
                self.string = Some(token);
            }
            return;
        }
        if let Some(mut scalar) = self.scalar.take() {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '+' | '-') {
                scalar.text.push(c);
                self.scalar = Some(scalar);
                return;
            }
            if let Some(value) = scalar_value(&scalar.text) {
                self.set(&scalar.path, value);
            }
            if self.stack.is_empty() {
                self.done = true;
                return;
            }
            self.character(c);
            return;
        }
        if c.is_whitespace() {
            return;
        }
        match self.stack.last().map(|frame| frame.phase) {
            Some(Phase::Key) if c == '}' => {
                self.close();
                return;
            }
            Some(Phase::Key) if c == '"' => {
                self.string = Some(StringToken {
                    path: None,
                    text: String::new(),
                    escape: String::new(),
                });
                return;
            }
            Some(Phase::Colon) if c == ':' => {
                self.stack.last_mut().unwrap().phase = Phase::ObjectValue;
                return;
            }
            Some(Phase::ObjectComma) if c == ',' => {
                self.stack.last_mut().unwrap().phase = Phase::Key;
                return;
            }
            Some(Phase::ObjectComma) if c == '}' => {
                self.close();
                return;
            }
            Some(Phase::ArrayComma) if c == ',' => {
                self.stack.last_mut().unwrap().phase = Phase::ArrayValue;
                return;
            }
            Some(Phase::ArrayComma) if c == ']' => {
                self.close();
                return;
            }
            Some(Phase::ArrayValue) if c == ']' => {
                self.close();
                return;
            }
            Some(Phase::ObjectValue | Phase::ArrayValue) | None => {}
            _ => return,
        }
        if !matches!(c, '{' | '[' | '"' | '-' | '0'..='9' | 't' | 'f' | 'n') {
            return;
        }
        if matches!(c, '{' | '[') && self.stack.len() >= MAX_DEPTH {
            // The final tool call is validated separately. Stop only the
            // preview when a provider sends an excessively nested value.
            self.done = true;
            return;
        }
        let path = self.slot();
        match c {
            '{' => {
                self.set(&path, Value::Object(Map::new()));
                self.stack.push(Frame {
                    path,
                    phase: Phase::Key,
                    key: String::new(),
                    len: 0,
                });
            }
            '[' => {
                self.set(&path, Value::Array(Vec::new()));
                self.stack.push(Frame {
                    path,
                    phase: Phase::ArrayValue,
                    key: String::new(),
                    len: 0,
                });
            }
            '"' => {
                self.set(&path, Value::String(String::new()));
                self.string = Some(StringToken {
                    path: Some(path),
                    text: String::new(),
                    escape: String::new(),
                });
            }
            _ => {
                self.scalar = Some(ScalarToken {
                    path,
                    text: c.to_string(),
                })
            }
        }
    }

    fn slot(&mut self) -> Path {
        let Some(frame) = self.stack.last_mut() else {
            return Vec::new();
        };
        let mut path = frame.path.clone();
        match frame.phase {
            Phase::ObjectValue => {
                path.push(Segment::Key(std::mem::take(&mut frame.key)));
                frame.phase = Phase::ObjectComma;
            }
            Phase::ArrayValue => {
                path.push(Segment::Index(frame.len));
                frame.len += 1;
                frame.phase = Phase::ArrayComma;
            }
            _ => unreachable!("value slot"),
        }
        path
    }
    fn set(&mut self, path: &[Segment], value: Value) {
        if path.is_empty() {
            self.root = Some(value);
        } else if let Some(root) = &mut self.root {
            set_in(root, path, value);
        }
    }
    fn close(&mut self) {
        self.stack.pop();
        if self.stack.is_empty() {
            self.done = true;
        }
    }
}

fn set_in(root: &mut Value, path: &[Segment], value: Value) {
    if path.is_empty() {
        *root = value;
        return;
    }
    let mut current = root;
    for segment in &path[..path.len() - 1] {
        current = match segment {
            Segment::Key(key) => match current.get_mut(key) {
                Some(value) => value,
                None => return,
            },
            Segment::Index(index) => match current.get_mut(*index) {
                Some(value) => value,
                None => return,
            },
        };
    }
    match (&path[path.len() - 1], current) {
        (Segment::Key(key), Value::Object(map)) => {
            map.insert(key.clone(), value);
        }
        (Segment::Index(index), Value::Array(array)) if *index == array.len() => array.push(value),
        (Segment::Index(index), Value::Array(array)) if *index < array.len() => {
            array[*index] = value
        }
        _ => {}
    }
}

fn scalar_value(text: &str) -> Option<Value> {
    for (literal, value) in [
        ("true", Value::Bool(true)),
        ("false", Value::Bool(false)),
        ("null", Value::Null),
    ] {
        if !text.is_empty() && literal.starts_with(text) {
            return Some(value);
        }
    }
    if let Ok(number) = text.parse::<Number>() {
        return Some(Value::Number(number));
    }
    let exponent = text.rfind(['e', 'E'])?;
    if matches!(&text[exponent + 1..], "" | "+" | "-") {
        return text[..exponent].parse::<Number>().ok().map(Value::Number);
    }
    None
}

/// Returns true when the terminating quote has been consumed.
fn string_character(token: &mut StringToken, c: char) -> bool {
    if token.escape.is_empty() {
        match c {
            '"' => return true,
            '\\' => token.escape.push(c),
            _ => token.text.push(c),
        }
        return false;
    }
    if token.escape == "\\" {
        match c {
            'u' => token.escape.push(c),
            '"' | '\\' | '/' => {
                token.text.push(c);
                token.escape.clear();
            }
            'b' | 'f' | 'n' | 'r' | 't' => {
                token.text.push(match c {
                    'b' => '\u{8}',
                    'f' => '\u{c}',
                    'n' => '\n',
                    'r' => '\r',
                    _ => '\t',
                });
                token.escape.clear();
            }
            _ => {
                token.text.push('\\');
                token.text.push(c);
                token.escape.clear();
            }
        }
        return false;
    }
    // Hold an incomplete Unicode escape, including a surrogate pair, until it
    // can be represented as a Unicode scalar without exposing broken UTF-8.
    token.escape.push(c);
    let escape = &token.escape;
    if escape.len() == 6 {
        let code = u16::from_str_radix(&escape[2..], 16).ok();
        if code.is_some_and(|code| (0xd800..=0xdbff).contains(&code)) {
            return false;
        }
        if let Some(character) = code.and_then(|code| char::from_u32(code as u32)) {
            token.text.push(character);
        } else {
            token.text.push_str(escape);
        }
        token.escape.clear();
    } else if escape.len() > 6
        && ((escape.len() == 7 && c != '\\')
            || (escape.len() == 8 && c != 'u')
            || escape.len() == 12)
    {
        let encoded = format!("\"{}\"", token.escape);
        if let Ok(decoded) = serde_json::from_str::<String>(&encoded) {
            token.text.push_str(&decoded);
        } else {
            token.text.push_str(&token.escape);
        }
        token.escape.clear();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn previews_nested_arguments_across_fragments() {
        let mut parser = StreamingJson::default();
        assert_eq!(parser.append("{\"path\":\"hel"), json!({"path":"hel"}));
        assert_eq!(
            parser.append("lo\",\"items\":[1,tr"),
            json!({"path":"hello","items":[1,true]})
        );
        assert_eq!(
            parser.append("ue,{\"x\":\"日"),
            json!({"path":"hello","items":[1,true,{"x":"日"}]})
        );
        assert_eq!(
            parser.append("本\"}]}"),
            json!({"path":"hello","items":[1,true,{"x":"日本"}]})
        );
    }
    #[test]
    fn holds_incomplete_keys_escapes_and_exponents() {
        let mut parser = StreamingJson::default();
        assert_eq!(parser.append("{\"x\":\"a\\u65"), json!({"x":"a"}));
        assert_eq!(parser.append("e5\\n"), json!({"x":"a日\n"}));
        assert_eq!(parser.append("\",\"unfinished"), json!({"x":"a日\n"}));
        assert_eq!(parser.append("\":1e-"), json!({"x":"a日\n","unfinished":1}));
        assert_eq!(parser.append("2}"), json!({"x":"a日\n","unfinished":0.01}));
    }
    #[test]
    fn supports_surrogate_pairs_split_across_chunks() {
        let mut parser = StreamingJson::default();
        assert_eq!(parser.append("{\"x\":\"\\ud83d"), json!({"x":""}));
        assert_eq!(parser.append("\\ude00\"}"), json!({"x":"😀"}));
    }

    #[test]
    fn complete_valid_arguments_survive_every_character_boundary() {
        for value in [
            json!({"path":"a\\b\"c\n日本😀", "empty":{}, "list":[]}),
            json!({"array":[true,false,null,-12,0.01,{"nested":["x", "y"]}]}),
            json!({"": "", "number": 12345}),
        ] {
            let serialized = serde_json::to_string(&value).unwrap();
            let mut parser = StreamingJson::default();
            let mut result = Value::Null;
            for character in serialized.chars() {
                result = parser.append(&character.to_string());
            }
            assert_eq!(result, value, "{serialized}");
        }
    }
}
