//! Pi-compatible matching keeps unrelated file lines intact during fuzzy edits.
use pi_rs::{Error, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use unicode_normalization::UnicodeNormalization;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Edit {
    old_text: String,
    new_text: String,
}
struct Replacement {
    start: usize,
    end: usize,
    text: String,
}
struct Group {
    first_line: usize,
    end_line: usize,
    replacements: Vec<Replacement>,
}

fn lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
fn fuzzy(text: &str) -> String {
    text.nfkc()
        .collect::<String>()
        .split('\n')
        .map(|line| line.trim_end_matches(|c: char| c.is_whitespace() || c == '\u{feff}'))
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .map(|c| match c {
            '\u{2018}'..='\u{201b}' => '\'',
            '\u{201c}'..='\u{201f}' => '"',
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            _ => c,
        })
        .collect()
}
fn edits(arguments: &Value) -> Result<Vec<Edit>> {
    let mut value = arguments.get("edits").cloned().unwrap_or_else(|| json!([]));
    if let Some(encoded) = value.as_str() {
        value = serde_json::from_str(encoded)?;
    }
    if value.is_object() {
        value = json!([value]);
    }
    let mut edits: Vec<Edit> = serde_json::from_value(value)?;
    if let (Some(old), Some(new)) = (arguments["oldText"].as_str(), arguments["newText"].as_str()) {
        edits.push(Edit {
            old_text: old.into(),
            new_text: new.into(),
        });
    }
    if edits.is_empty() {
        return Err(Error::Config("at least one edit is required".into()));
    }
    for edit in &mut edits {
        edit.old_text = lf(&edit.old_text);
        edit.new_text = lf(&edit.new_text);
        if edit.old_text.is_empty() {
            return Err(Error::Config("oldText must not be empty".into()));
        }
    }
    Ok(edits)
}
fn replace(mut text: String, replacements: &[Replacement], offset: usize) -> String {
    for replacement in replacements.iter().rev() {
        text.replace_range(
            replacement.start - offset..replacement.end - offset,
            &replacement.text,
        );
    }
    text
}
fn preserve_lines(original: &str, base: &str, replacements: Vec<Replacement>) -> Result<String> {
    let original_lines: Vec<_> = original.split_inclusive('\n').collect();
    let mut offset = 0;
    let spans: Vec<_> = base
        .split_inclusive('\n')
        .map(|line| {
            let start = offset;
            offset += line.len();
            (start, offset)
        })
        .collect();
    if original_lines.len() != spans.len() {
        return Err(Error::Config(
            "normalized edit changed the line structure".into(),
        ));
    }
    let mut groups: Vec<Group> = Vec::new();
    for replacement in replacements {
        let first_line = spans.partition_point(|(_, end)| *end <= replacement.start);
        let end_line = spans.partition_point(|(_, end)| *end < replacement.end) + 1;
        if first_line >= spans.len() || end_line > spans.len() {
            return Err(Error::Config(
                "edit range is outside the original file".into(),
            ));
        }
        if let Some(group) = groups
            .last_mut()
            .filter(|group| first_line < group.end_line)
        {
            group.end_line = group.end_line.max(end_line);
            group.replacements.push(replacement);
        } else {
            groups.push(Group {
                first_line,
                end_line,
                replacements: vec![replacement],
            });
        }
    }
    let mut result = String::new();
    let mut next_line = 0;
    for group in groups {
        result.extend(original_lines[next_line..group.first_line].iter().copied());
        let start = spans[group.first_line].0;
        let end = spans[group.end_line - 1].1;
        result.push_str(&replace(
            base[start..end].into(),
            &group.replacements,
            start,
        ));
        next_line = group.end_line;
    }
    result.extend(original_lines[next_line..].iter().copied());
    Ok(result)
}
pub fn apply(original: &str, arguments: &Value) -> Result<(String, usize)> {
    let edits = edits(arguments)?;
    let count = edits.len();
    let (bom, body) = original
        .strip_prefix('\u{feff}')
        .map_or(("", original), |body| ("\u{feff}", body));
    let crlf = body
        .find('\n')
        .is_some_and(|index| body[..index].ends_with('\r'));
    let normalized = lf(body);
    let fuzzy_content = fuzzy(&normalized);
    let use_fuzzy = edits
        .iter()
        .any(|edit| !normalized.contains(&edit.old_text));
    let base = if use_fuzzy {
        &fuzzy_content
    } else {
        &normalized
    };
    let mut replacements = Vec::new();
    for edit in edits {
        let fuzzy_old = fuzzy(&edit.old_text);
        let occurrences = if fuzzy_old.is_empty() {
            base.match_indices(&edit.old_text).take(2).count()
        } else {
            fuzzy_content.match_indices(&fuzzy_old).take(2).count()
        };
        if occurrences > 1 {
            return Err(Error::Config(
                "oldText matches more than once after normalization".into(),
            ));
        }
        if use_fuzzy && fuzzy_old.is_empty() {
            return Err(Error::Config(
                "oldText was not found after normalization".into(),
            ));
        }
        let (start, length) = base
            .find(&edit.old_text)
            .map(|start| (start, edit.old_text.len()))
            .or_else(|| base.find(&fuzzy_old).map(|start| (start, fuzzy_old.len())))
            .ok_or_else(|| Error::Config("oldText was not found".into()))?;
        replacements.push(Replacement {
            start,
            end: start + length,
            text: edit.new_text,
        });
    }
    replacements.sort_by_key(|replacement| replacement.start);
    if replacements
        .windows(2)
        .any(|pair| pair[0].end > pair[1].start)
    {
        return Err(Error::Config("edits overlap in the original file".into()));
    }
    let changed = if use_fuzzy {
        preserve_lines(&normalized, base, replacements)?
    } else {
        replace(base.clone(), &replacements, 0)
    };
    if changed == normalized {
        return Err(Error::Config("replacement produced no changes".into()));
    }
    let restored = if crlf {
        changed.replace('\n', "\r\n")
    } else {
        changed
    };
    Ok((format!("{bom}{restored}"), count))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multiline_fuzzy_ranges_preserve_repeated_untouched_lines() {
        let source = "keep  \nleft “one”\nmiddle – two  \nright  \nkeep\t\n";
        assert_eq!(
            apply(
                source,
                &json!({"oldText":"\"one\"\nmiddle - two","newText":"value"})
            )
            .unwrap()
            .0,
            "keep  \nleft value\nright  \nkeep\t\n"
        );
        assert_eq!(
            apply("a", &json!({"edits":[],"oldText":"a","newText":"b"}))
                .unwrap()
                .0,
            "b"
        );
    }
    #[test]
    fn exact_whitespace_matches_remain_supported() {
        assert_eq!(
            apply("a  b", &json!({"oldText":"  ","newText":" "}))
                .unwrap()
                .0,
            "a b"
        );
    }
    #[test]
    fn edits_preserve_bom_crlf_and_unrelated_unicode_lines() {
        let source = "\u{feff}Keep ‘original’  \r\nSay “hello” — world  \r\nUnchanged Ａ\t\r\n";
        let (changed, count) = apply(
            source,
            &json!({"edits":[{"oldText":"Say \"hello\" - world","newText":"Say goodbye\nagain"}]}),
        )
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            changed,
            "\u{feff}Keep ‘original’  \r\nSay goodbye\r\nagain\r\nUnchanged Ａ\t\r\n"
        );
    }
    #[test]
    fn mixed_exact_and_fuzzy_edits_use_one_original_snapshot() {
        let (changed, _) = apply("Ａlpha  \n‘beta’  \nkeep\t\n", &json!({"edits":[{"oldText":"Ａlpha","newText":"‘beta’"},{"oldText":"'beta'","newText":"done"}]})).unwrap();
        assert_eq!(changed, "‘beta’\ndone\nkeep\t\n");
    }
    #[test]
    fn ambiguity_overlap_and_noop_leave_the_file_unchanged() {
        assert!(apply("Ａ\nA\n", &json!({"oldText":"A","newText":"b"})).is_err());
        assert!(
            apply(
                "abc",
                &json!({"edits":[{"oldText":"ab","newText":"x"},{"oldText":"bc","newText":"y"}]})
            )
            .is_err()
        );
        assert!(apply("abc", &json!({"oldText":"abc","newText":"abc"})).is_err());
    }
    #[test]
    fn edit_arguments_accept_single_objects_json_and_legacy_additions() {
        for edits in [
            json!({"oldText":"a","newText":"x"}),
            json!("[{\"oldText\":\"a\",\"newText\":\"x\"}]"),
        ] {
            assert_eq!(
                apply("ab", &json!({"edits":edits,"oldText":"b","newText":"y"})).unwrap(),
                ("xy".into(), 2)
            );
        }
    }
}
