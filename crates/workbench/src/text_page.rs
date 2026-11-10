//! Streaming text pages with Pi's line and byte limits.
use std::io::{BufRead, BufReader};
use std::path::Path;

use pi_rs::{Error, Result};

const MAX_BYTES: usize = 50 * 1024;
const MAX_LINES: usize = 2000;

pub(super) fn read(
    path: &Path,
    display_path: &str,
    offset: usize,
    limit: Option<usize>,
) -> Result<String> {
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(Error::Config("expected a regular text file".into()));
    }
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    let mut selected = Vec::new();
    let mut bytes = 0;
    let mut total_lines = 0;
    let mut byte_limited = false;
    let mut oversized_first = None;
    let mut last_newline = false;
    loop {
        let (length, newline) = line(&mut reader, &mut buffer)?;
        if length == 0 && total_lines != 0 && !last_newline {
            break;
        }
        let final_empty = length == 0;
        let index = total_lines;
        total_lines += 1;
        last_newline = newline;
        let trailing_newline_fits = final_empty
            && index == offset + selected.len()
            && selected.len() == MAX_LINES
            && bytes < MAX_BYTES;
        if index >= offset
            && selected.len() < limit.unwrap_or(usize::MAX)
            && (selected.len() < MAX_LINES || trailing_newline_fits)
            && !byte_limited
        {
            let length = length - usize::from(newline);
            let separator = usize::from(!selected.is_empty());
            if length + separator + bytes > MAX_BYTES {
                byte_limited = true;
                if selected.is_empty() {
                    oversized_first = Some(length);
                }
            } else {
                if newline {
                    buffer.pop();
                }
                let text = String::from_utf8_lossy(&buffer).into_owned();
                // Invalid UTF-8 expands when decoded, so check the actual output too.
                if text.len() + separator + bytes > MAX_BYTES {
                    byte_limited = true;
                    if selected.is_empty() {
                        oversized_first = Some(text.len());
                    }
                } else {
                    bytes += text.len() + separator;
                    selected.push(text);
                }
            }
        }
        if final_empty {
            break;
        }
    }
    if offset >= total_lines {
        return Err(Error::Config(format!(
            "Offset {} is beyond end of file ({total_lines} lines total)",
            offset.saturating_add(1)
        )));
    }
    if let Some(length) = oversized_first {
        return Ok(format!(
            "[Line {} is {}, exceeds 50.0KB limit. Use bash: sed -n '{}p' {display_path} | head -c {MAX_BYTES}]",
            offset + 1,
            format_size(length),
            offset + 1
        ));
    }
    let end = offset + selected.len();
    let mut output = selected.join("\n");
    if end < total_lines {
        if byte_limited || limit.is_none_or(|limit| limit > MAX_LINES) {
            output.push_str(&format!(
                "\n\n[Showing lines {}-{end} of {total_lines}{}. Use offset={} to continue.]",
                offset + 1,
                if byte_limited { " (50.0KB limit)" } else { "" },
                end + 1
            ));
        } else {
            output.push_str(&format!(
                "\n\n[{} more lines in file. Use offset={} to continue.]",
                total_lines - end,
                end + 1
            ));
        }
    }
    Ok(output)
}

fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

// Retain only a bounded prefix while counting the complete line.
fn line(reader: &mut impl BufRead, output: &mut Vec<u8>) -> std::io::Result<(usize, bool)> {
    output.clear();
    let mut length = 0;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok((length, false));
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(buffer.len(), |position| position + 1);
        let keep = (MAX_BYTES + 1).saturating_sub(output.len()).min(count);
        output.extend_from_slice(&buffer[..keep]);
        length += count;
        reader.consume(count);
        if newline.is_some() {
            return Ok((length, true));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pages_preserve_empty_lines_and_report_continuation() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "first\n日本語\nlast\n").unwrap();
        assert_eq!(
            read(file.path(), "fixture", 1, Some(1)).unwrap(),
            "日本語\n\n[2 more lines in file. Use offset=3 to continue.]"
        );
        assert_eq!(
            read(file.path(), "fixture", 0, None).unwrap(),
            "first\n日本語\nlast\n"
        );
        assert_eq!(read(file.path(), "fixture", 3, None).unwrap(), "");
        assert!(read(file.path(), "fixture", 4, None).is_err());
        std::fs::write(file.path(), "").unwrap();
        assert_eq!(read(file.path(), "fixture", 0, None).unwrap(), "");
    }
    #[test]
    fn large_files_and_long_lines_remain_pageable() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let content = format!("{}\nend", "界".repeat(2 * 1024 * 1024));
        std::fs::write(file.path(), content).unwrap();
        assert!(
            read(file.path(), "fixture", 0, None)
                .unwrap()
                .contains("exceeds 50.0KB")
        );
        assert_eq!(read(file.path(), "fixture", 1, None).unwrap(), "end");
        std::fs::write(file.path(), "line\n".repeat(3000)).unwrap();
        let page = read(file.path(), "fixture", 0, None).unwrap();
        assert!(page.contains("Showing lines 1-2000 of 3001"));
        assert!(page.contains("offset=2001"));
    }
    #[test]
    fn final_newline_does_not_consume_a_line_limit() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let content = "x\n".repeat(MAX_LINES);
        std::fs::write(file.path(), &content).unwrap();
        assert_eq!(read(file.path(), "fixture", 0, None).unwrap(), content);
        assert_eq!(
            read(file.path(), "fixture", 0, Some(MAX_LINES + 1)).unwrap(),
            content
        );
        let explicit = read(file.path(), "fixture", 0, Some(MAX_LINES)).unwrap();
        assert!(explicit.ends_with("[1 more lines in file. Use offset=2001 to continue.]"));
    }

    #[test]
    fn line_and_byte_boundary_has_the_reference_continuation_kind() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut content = "x\n".repeat(MAX_LINES - 1);
        content.push_str(&"z".repeat(MAX_BYTES - content.len()));
        content.push('\n');
        std::fs::write(file.path(), content).unwrap();
        let output = read(file.path(), "fixture", 0, None).unwrap();
        assert!(output.ends_with("[Showing lines 1-2000 of 2001. Use offset=2001 to continue.]"));
    }

    #[test]
    fn oversized_line_guidance_preserves_the_requested_path_and_size_units() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "x".repeat(2 * 1024 * 1024)).unwrap();
        assert_eq!(
            read(file.path(), "large.txt", 0, None).unwrap(),
            "[Line 1 is 2.0MB, exceeds 50.0KB limit. Use bash: sed -n '1p' large.txt | head -c 51200]"
        );
    }

    #[test]
    fn byte_limited_pages_do_not_split_unicode_or_lines() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let line = "界".repeat(10000);
        std::fs::write(file.path(), format!("{line}\n{line}\n")).unwrap();
        let page = read(file.path(), "fixture", 0, None).unwrap();
        assert!(page.starts_with(&line));
        assert!(!page.contains('\u{fffd}'));
        assert!(page.contains("offset=2"));
        assert!(page.len() < MAX_BYTES);
    }
}
