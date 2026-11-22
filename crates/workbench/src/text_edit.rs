//! Pi-compatible matching keeps unrelated file lines intact during fuzzy edits.
use pi_rs::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use similar::{Algorithm, ChangeTag, DiffOp};
use unicode_normalization::UnicodeNormalization;

use crate::diff_sequence::{self, Tag};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Edit {
    old_text: String,
    new_text: String,
}
struct Replacement {
    edit_index: usize,
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
pub(super) fn normalized_content(text: &str) -> String {
    lf(text.strip_prefix('\u{feff}').unwrap_or(text))
}
pub(super) struct EditDiff<'a> {
    old: Vec<&'a str>,
    new: Vec<&'a str>,
    ops: Vec<DiffOp>,
    digits: usize,
}

impl<'a> EditDiff<'a> {
    pub(super) fn new(before: &'a str, after: &'a str) -> Self {
        let old: Vec<_> = before.split_inclusive('\n').collect();
        let new: Vec<_> = after.split_inclusive('\n').collect();
        let digits = before
            .split('\n')
            .count()
            .max(after.split('\n').count())
            .to_string()
            .len();

        // The buffered search can retain paths across many diagonals. Give it
        // a bounded amount of extra work on top of a full scan of both files.
        let mut remaining = old
            .len()
            .saturating_add(new.len())
            .saturating_add(1_000_000);
        let components = diff_sequence::align(
            &old,
            &new,
            |old, new| old == new,
            1024,
            || {
                if let Some(next) = remaining.checked_sub(1) {
                    remaining = next;
                    true
                } else {
                    false
                }
            },
        );
        let ops = components.map_or_else(
            || similar::capture_diff_slices(Algorithm::Myers, &old, &new),
            line_operations,
        );
        Self {
            old,
            new,
            ops,
            digits,
        }
    }

    pub(super) fn first_changed_line(&self) -> Option<usize> {
        self.ops
            .iter()
            .find(|op| !matches!(op, DiffOp::Equal { .. }))
            .map(|op| op.new_range().start + 1)
    }

    pub(super) fn unified_patch(&self, path: &str) -> String {
        use std::fmt::Write as _;

        let mut patch = format!("--- {path}\n+++ {path}\n");
        for hunk in similar::group_diff_ops(self.ops.clone(), 4) {
            let first = hunk.first().expect("hunk has operations");
            let last = hunk.last().expect("hunk has operations");
            let old_start = first.old_range().start;
            let new_start = first.new_range().start;
            let old_count = last.old_range().end - old_start;
            let new_count = last.new_range().end - new_start;
            // Pi prints both counts, even for a single line. Empty ranges use the
            // preceding line number, including zero at the start of a file.
            let _ = writeln!(
                patch,
                "@@ -{},{} +{},{} @@",
                old_start + usize::from(old_count > 0),
                old_count,
                new_start + usize::from(new_count > 0),
                new_count,
            );
            for op in hunk {
                for change in op.iter_changes(&self.old[..], &self.new[..]) {
                    let line = change.value();
                    let _ = write!(patch, "{}{line}", change.tag());
                    if !line.ends_with('\n') {
                        patch.push_str("\n\\ No newline at end of file\n");
                    }
                }
            }
        }
        patch
    }

    pub(super) fn numbered_diff(&self) -> String {
        use std::fmt::Write as _;
        let digits = self.digits;
        let mut output = String::new();
        let context = 4;
        for (index, op) in self.ops.iter().enumerate() {
            if matches!(op, DiffOp::Equal { .. }) {
                let range = op.old_range();
                let leading = if index > 0 {
                    context.min(range.len())
                } else {
                    0
                };
                let trailing = if index + 1 < self.ops.len() {
                    context.min(range.len() - leading)
                } else {
                    0
                };
                if leading + trailing == 0 {
                    continue;
                }
                for row in range.start..range.start + leading {
                    let text = self.old[row].trim_end_matches('\n');
                    let _ = writeln!(output, " {:>digits$} {text}", row + 1);
                }
                if leading + trailing < range.len() {
                    let _ = writeln!(output, " {:digits$} ...", "");
                }
                for row in range.end - trailing..range.end {
                    let text = self.old[row].trim_end_matches('\n');
                    let _ = writeln!(output, " {:>digits$} {text}", row + 1);
                }
            } else {
                for change in op.iter_changes(&self.old[..], &self.new[..]) {
                    let (marker, row) = match change.tag() {
                        ChangeTag::Delete => ('-', change.old_index().expect("deleted line")),
                        ChangeTag::Insert => ('+', change.new_index().expect("inserted line")),
                        ChangeTag::Equal => continue,
                    };
                    let text = change.value().trim_end_matches('\n');
                    let _ = writeln!(output, "{marker}{:>digits$} {text}", row + 1);
                }
            }
        }
        output.pop();
        output
    }
}

fn line_operations(components: Vec<(Tag, usize)>) -> Vec<DiffOp> {
    let (mut old, mut new) = (0, 0);
    components
        .into_iter()
        .map(|(tag, len)| {
            let op = match tag {
                Tag::Equal => DiffOp::Equal {
                    old_index: old,
                    new_index: new,
                    len,
                },
                Tag::Delete => DiffOp::Delete {
                    old_index: old,
                    new_index: new,
                    old_len: len,
                },
                Tag::Insert => DiffOp::Insert {
                    old_index: old,
                    new_index: new,
                    new_len: len,
                },
            };
            old = op.old_range().end;
            new = op.new_range().end;
            op
        })
        .collect()
}

fn fuzzy(text: &str) -> String {
    text.nfkc()
        .collect::<String>()
        .split('\n')
        // JavaScript trimEnd includes U+FEFF but not U+0085, unlike Rust's
        // Unicode whitespace predicate. This affects which edits may match.
        .map(|line| {
            line.trim_end_matches(|c: char| {
                (c.is_whitespace() && c != '\u{0085}') || c == '\u{feff}'
            })
        })
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
    let count = edits.len();
    let path = arguments["path"].as_str().unwrap_or_default();
    for (index, edit) in edits.iter_mut().enumerate() {
        edit.old_text = lf(&edit.old_text);
        edit.new_text = lf(&edit.new_text);
        if edit.old_text.is_empty() {
            let field = if count == 1 {
                "oldText".into()
            } else {
                format!("edits[{index}].oldText")
            };
            return Err(Error::Config(format!(
                "{field} must not be empty in {path}."
            )));
        }
    }
    Ok(edits)
}
pub fn prepare_arguments(mut arguments: Value) -> Result<Value> {
    let edits = edits(&arguments)?;
    let object = arguments
        .as_object_mut()
        .ok_or_else(|| Error::Config("edit arguments must be an object".into()))?;
    object.remove("oldText");
    object.remove("newText");
    object.insert("edits".into(), serde_json::to_value(edits)?);
    Ok(arguments)
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
fn preserve_lines(
    original: &str,
    base: &str,
    replacements: Vec<Replacement>,
) -> Result<Vec<Replacement>> {
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
    let original_spans: Vec<_> = {
        let mut offset = 0;
        original_lines
            .iter()
            .map(|line| {
                let start = offset;
                offset += line.len();
                (start, offset)
            })
            .collect()
    };
    Ok(groups
        .into_iter()
        .map(|group| {
            let start = spans[group.first_line].0;
            let end = spans[group.end_line - 1].1;
            Replacement {
                edit_index: group.replacements[0].edit_index,
                start: original_spans[group.first_line].0,
                end: original_spans[group.end_line - 1].1,
                text: replace(base[start..end].into(), &group.replacements, start),
            }
        })
        .collect())
}

// Replacement offsets refer to LF-normalized UTF-8. Map them to byte offsets in
// the original, then splice without rewriting untouched line endings.
fn apply_to_original(original: &str, replacements: &[Replacement]) -> String {
    let bytes = original.as_bytes();
    let mut original_offset = 0;
    let mut normalized_offset = 0;
    let mut position = 0;
    let mut result = String::new();
    for replacement in replacements {
        let mut advance = |target: usize| {
            while normalized_offset < target {
                if bytes[original_offset] == b'\r' && bytes.get(original_offset + 1) == Some(&b'\n')
                {
                    original_offset += 1;
                }
                original_offset += 1;
                normalized_offset += 1;
            }
            original_offset
        };
        let start = advance(replacement.start);
        push_splice(&mut result, &original[position..start]);
        if replacement.text.contains('\n') {
            let ending = line_ending_at(original, start);
            let text = replacement.text.replace('\n', ending);
            push_splice(&mut result, &text);
        } else {
            push_splice(&mut result, &replacement.text);
        }
        position = advance(replacement.end);
    }
    push_splice(&mut result, &original[position..]);
    result
}

fn push_splice(result: &mut String, part: &str) {
    if part.is_empty() {
        return;
    }
    // Preserve two normalized breaks when a lone CR meets an LF at a splice.
    if result.ends_with('\r') && part.starts_with('\n') {
        result.push('\n');
    }
    result.push_str(part);
}

fn line_ending_at(original: &str, offset: usize) -> &'static str {
    // Pi uses lone CR for inserted breaks only when the file has no LF.
    // Otherwise it selects the next LF (or the preceding one at EOF).
    let bytes = original.as_bytes();
    let line_feed = bytes[offset..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| offset + i)
        .or_else(|| bytes[..offset].iter().rposition(|&b| b == b'\n'));
    match line_feed {
        Some(i) if i > 0 && bytes[i - 1] == b'\r' => "\r\n",
        Some(_) => "\n",
        None if bytes.contains(&b'\r') => "\r",
        None => "\n",
    }
}
pub fn apply(original: &str, arguments: &Value) -> Result<(String, usize)> {
    let edits = edits(arguments)?;
    let count = edits.len();
    let (bom, body) = original
        .strip_prefix('\u{feff}')
        .map_or(("", original), |body| ("\u{feff}", body));
    let normalized = lf(body);
    let fuzzy_content = fuzzy(&normalized);
    let edits: Vec<_> = edits
        .into_iter()
        .map(|edit| {
            let fuzzy_old = fuzzy(&edit.old_text);
            (edit, fuzzy_old)
        })
        .collect();
    let use_fuzzy = edits.iter().any(|(edit, fuzzy_old)| {
        !normalized.contains(&edit.old_text)
            && !fuzzy_old.is_empty()
            && fuzzy_content.contains(fuzzy_old)
    });
    let path = arguments["path"].as_str().unwrap_or_default();
    let base = if use_fuzzy {
        &fuzzy_content
    } else {
        &normalized
    };
    let mut replacements = Vec::new();
    for (edit_index, (edit, fuzzy_old)) in edits.into_iter().enumerate() {
        let (start, length) = base.find(&edit.old_text)
            .map(|start| (start, edit.old_text.len()))
            .or_else(|| {
                if fuzzy_old.is_empty() {
                    None
                } else {
                    fuzzy_content.find(&fuzzy_old).map(|start| (start, fuzzy_old.len()))
                }
            })
            .ok_or_else(|| Error::Config(if count == 1 {
                format!("Could not find the exact text in {path}. The old text must match exactly including all whitespace and newlines.")
            } else {
                format!("Could not find edits[{edit_index}] in {path}. The oldText must match exactly including all whitespace and newlines.")
            }))?;
        let occurrences = if fuzzy_old.is_empty() {
            base.matches(&edit.old_text).count()
        } else {
            fuzzy_content.matches(&fuzzy_old).count()
        };
        if occurrences > 1 {
            return Err(Error::Config(if count == 1 {
                format!(
                    "Found {occurrences} occurrences of the text in {path}. The text must be unique. Please provide more context to make it unique."
                )
            } else {
                format!(
                    "Found {occurrences} occurrences of edits[{edit_index}] in {path}. Each oldText must be unique. Please provide more context to make it unique."
                )
            }));
        }
        replacements.push(Replacement {
            edit_index,
            start,
            end: start + length,
            text: edit.new_text,
        });
    }
    replacements.sort_by_key(|replacement| replacement.start);
    if let Some(pair) = replacements
        .windows(2)
        .find(|pair| pair[0].end > pair[1].start)
    {
        return Err(Error::Config(format!(
            "edits[{}] and edits[{}] overlap in {path}. Merge them into one edit or target disjoint regions.",
            pair[0].edit_index, pair[1].edit_index
        )));
    }
    let replacements = if use_fuzzy {
        preserve_lines(&normalized, base, replacements)?
    } else {
        replacements
    };
    let changed = replace(normalized.clone(), &replacements, 0);
    if changed == normalized {
        return Err(Error::Config(if count == 1 {
            format!(
                "No changes made to {path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
            )
        } else {
            format!("No changes made to {path}. The replacements produced identical content.")
        }));
    }
    let restored = if body == normalized {
        changed
    } else {
        apply_to_original(body, &replacements)
    };
    Ok((format!("{bom}{restored}"), count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_diff_matches_pi_alignment_for_repeated_lines() {
        let before = "a\nb\nc\nb\nc\na\na\nb\nc\nc\nb\nb\na\nc\nb\n";
        let after = "a\nb\nc\nb\nc\nb\na\nb\nc\nc\nb\nb\na\nc\nb\n";
        let diff = EditDiff::new(before, after);

        assert_eq!(diff.first_changed_line(), Some(6));
        assert_eq!(
            diff.numbered_diff(),
            "    ...\n  2 b\n  3 c\n  4 b\n  5 c\n+ 6 b\n  6 a\n- 7 a\n  8 b\n  9 c\n 10 c\n 11 b\n    ..."
        );
        assert_eq!(
            diff.unified_patch("fixture"),
            "--- fixture\n+++ fixture\n@@ -2,10 +2,10 @@\n b\n c\n b\n c\n+b\n a\n-a\n b\n c\n c\n b\n"
        );
    }

    #[test]
    fn unified_patch_matches_pi_single_line_range_and_newline_markers() {
        for (before, after, expected) in [
            ("old\n", "new\n", "@@ -1,1 +1,1 @@\n-old\n+new\n"),
            ("old\n", "", "@@ -1,1 +0,0 @@\n-old\n"),
            ("", "new\n", "@@ -0,0 +1,1 @@\n+new\n"),
            (
                "old",
                "new",
                "@@ -1,1 +1,1 @@\n-old\n\\ No newline at end of file\n+new\n\\ No newline at end of file\n",
            ),
            (
                "old",
                "old\n",
                "@@ -1,1 +1,1 @@\n-old\n\\ No newline at end of file\n+old\n",
            ),
            (
                "one\ntwo\n",
                "one\nnew\n",
                "@@ -1,2 +1,2 @@\n one\n-two\n+new\n",
            ),
        ] {
            let diff = EditDiff::new(before, after);
            assert_eq!(
                diff.unified_patch("fixture"),
                format!("--- fixture\n+++ fixture\n{expected}"),
                "{before:?} -> {after:?}"
            );
        }
    }
    #[test]
    fn fuzzy_edits_use_ecmascript_trailing_whitespace() {
        let arguments = json!({"path":"example","oldText":"alpha\nbeta","newText":"changed"});
        for text in ["alpha\u{0085}\nbeta", "alpha\u{0085} \nbeta"] {
            let error = apply(text, &arguments).unwrap_err();
            assert!(
                matches!(error, Error::Config(message) if message.starts_with("Could not find the exact text"))
            );
        }
        for whitespace in ["\u{feff}", "\u{00a0}", "\t"] {
            let input = format!("alpha{whitespace}\nbeta");
            assert_eq!(apply(&input, &arguments).unwrap().0, "changed");
        }
        assert_eq!(
            apply(
                "alpha\u{0085}\nbeta",
                &json!({"oldText":"alpha\u{0085}\nbeta","newText":"changed"})
            )
            .unwrap()
            .0,
            "changed"
        );
    }
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
    fn diagnostics_report_original_edit_indices_and_exact_occurrence_counts() {
        let repeated = apply(
            "one one one",
            &json!({"path":"example","oldText":"one","newText":"two"}),
        )
        .unwrap_err();
        assert!(
            matches!(repeated, Error::Config(message) if message.starts_with("Found 3 occurrences of the text in example."))
        );
        let overlap = apply("abcdef", &json!({"path":"example","edits":[{"oldText":"cde","newText":"x"},{"oldText":"abc","newText":"y"}]})).unwrap_err();
        assert!(
            matches!(overlap, Error::Config(message) if message.starts_with("edits[1] and edits[0] overlap in example."))
        );
        let missing = apply("abc", &json!({"path":"example","edits":[{"oldText":"a","newText":"x"},{"oldText":"z","newText":"y"}]})).unwrap_err();
        assert!(
            matches!(missing, Error::Config(message) if message.starts_with("Could not find edits[1] in example."))
        );
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
