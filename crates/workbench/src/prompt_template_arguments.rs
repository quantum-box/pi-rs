//! Pi-compatible prompt template argument parsing and substitution.

fn is_prompt_space(character: char) -> bool {
    (character.is_whitespace() && character != '\u{0085}') || character == '\u{feff}'
}

fn parse_arguments(arguments: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut has_token = false;
    let mut quote = None;
    for character in arguments.chars() {
        if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                current.push(character);
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
            has_token = true;
        } else if is_prompt_space(character) {
            if std::mem::take(&mut has_token) {
                args.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
            has_token = true;
        }
    }
    if has_token {
        args.push(current);
    }
    args
}

/// Expand Pi prompt template placeholders using quoted command arguments.
/// Inserted argument text is not recursively expanded.
pub fn expand_arguments(body: &str, arguments: &str) -> String {
    static PLACEHOLDER: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let expression = PLACEHOLDER.get_or_init(|| {
        regex::Regex::new(r"\$\{([0-9]+|ARGUMENTS|@):-([^}]*)\}|\$\{@:([0-9]+)(?::([0-9]+))?\}|\$(ARGUMENTS|@|[0-9]+)")
            .expect("Pi argument placeholders")
    });
    let args = parse_arguments(arguments);
    let all = args.join(" ");
    let value = |key: &str| match key {
        "@" | "ARGUMENTS" => all.as_str(),
        _ => key
            .parse::<usize>()
            .ok()
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| args.get(index))
            .map_or("", String::as_str),
    };
    expression
        .replace_all(body, |captures: &regex::Captures<'_>| {
            if let Some(target) = captures.get(1) {
                let found = value(target.as_str());
                return if found.is_empty() {
                    captures[2].to_owned()
                } else {
                    found.to_owned()
                };
            }
            if let Some(start) = captures.get(3) {
                let start = start
                    .as_str()
                    .parse::<usize>()
                    .unwrap_or(usize::MAX)
                    .saturating_sub(1)
                    .min(args.len());
                let length = captures
                    .get(4)
                    .and_then(|length| length.as_str().parse::<usize>().ok())
                    .unwrap_or(usize::MAX);
                return args[start..start.saturating_add(length).min(args.len())].join(" ");
            }
            value(&captures[5]).to_owned()
        })
        .into_owned()
}
