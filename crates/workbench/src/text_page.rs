//! Streaming text pages with Pi's line and byte limits.
use std::io::{BufRead, BufReader};
use std::path::Path;

use encoding_rs::{CoderResult, UTF_8};
use pi_rs::{CancellationToken, Error, Outcome, Result};
use serde_json::json;

use super::tool_output::{Limit, MAX_BYTES, MAX_LINES, Truncation, format_size};

pub(super) fn read(
    path: &Path,
    display_path: &str,
    offset: usize,
    limit: Option<usize>,
    cancellation: &CancellationToken,
) -> Result<Outcome> {
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(Error::Config("expected a regular text file".into()));
    }
    read_from(
        BufReader::new(file),
        display_path,
        offset,
        limit,
        cancellation,
    )
}

pub(super) fn read_from(
    mut reader: impl BufRead,
    display_path: &str,
    offset: usize,
    limit: Option<usize>,
    cancellation: &CancellationToken,
) -> Result<Outcome> {
    let mut buffer = Vec::new();
    let mut decoded = [0; 8192];
    let mut head = Vec::new();
    let mut head_bytes = 0;
    let mut first_line_bytes = 0;
    let mut input_lines = 0;
    let mut input_bytes = 0;
    let mut input_ends_in_empty_line = false;
    let mut file_lines = 0;
    let mut byte_limit = false;
    let mut last_newline = false;
    loop {
        let (length, newline) = line(&mut reader, &mut buffer, &mut decoded, cancellation)?;
        if length == 0 && file_lines != 0 && !last_newline {
            break;
        }
        let index = file_lines;
        file_lines += 1;
        last_newline = newline;
        let line_bytes = length - usize::from(newline);
        if index >= offset && input_lines < limit.unwrap_or(usize::MAX) {
            if input_lines == 0 {
                first_line_bytes = line_bytes;
            }
            let separator = usize::from(input_lines != 0);
            input_lines += 1;
            input_bytes += line_bytes + separator;
            input_ends_in_empty_line = line_bytes == 0;
            let keep_trailing_empty = head.len() == MAX_LINES && line_bytes == 0;
            if !byte_limit && (head.len() < MAX_LINES || keep_trailing_empty) {
                if head_bytes + line_bytes + separator > MAX_BYTES {
                    byte_limit = true;
                } else {
                    if newline {
                        buffer.pop();
                    }
                    let text = std::str::from_utf8(&buffer).expect("decoded UTF-8 line");
                    head_bytes += text.len() + separator;
                    head.push(text.to_owned());
                }
            }
        }
        if length == 0 {
            break;
        }
    }
    if offset >= file_lines {
        return Err(Error::Config(format!(
            "Offset {} is beyond end of file ({file_lines} lines total)",
            offset.saturating_add(1)
        )));
    }

    let total_lines = input_lines - usize::from(input_ends_in_empty_line);
    let truncated = total_lines > MAX_LINES || input_bytes > MAX_BYTES;
    if !truncated {
        let mut output = head.join("\n");
        let end = offset + input_lines;
        if end < file_lines {
            output.push_str(&format!(
                "\n\n[{} more lines in file. Use offset={} to continue.]",
                file_lines - end,
                end + 1
            ));
        }
        return Ok(Outcome::Succeeded(output));
    }

    head.truncate(total_lines.min(MAX_LINES));
    let content = head.join("\n");
    let first_line_exceeds_limit = first_line_bytes > MAX_BYTES;
    let truncated_by = if head.len() == MAX_LINES || head.len() == total_lines {
        Limit::Lines
    } else {
        Limit::Bytes
    };
    let details = json!({"truncation": Truncation {
        content: &content,
        truncated: true,
        truncated_by: Some(truncated_by),
        total_lines: total_lines as u64,
        total_bytes: input_bytes as u64,
        output_lines: head.len(),
        output_bytes: content.len(),
        last_line_partial: false,
        first_line_exceeds_limit,
        max_lines: MAX_LINES as u64,
        max_bytes: MAX_BYTES,
    }});
    let start = offset + 1;
    let output = if first_line_exceeds_limit {
        let display_path = shell_words::quote(display_path);
        format!(
            "[Line {start} is {}, exceeds 50.0KB limit. Use bash: sed -n '{start}p' {display_path} | head -c {MAX_BYTES}]",
            format_size(first_line_bytes as u64),
        )
    } else {
        let end = offset + head.len();
        let reason = if matches!(truncated_by, Limit::Bytes) {
            " (50.0KB limit)"
        } else {
            ""
        };
        format!(
            "{content}\n\n[Showing lines {start}-{end} of {file_lines}{reason}. Use offset={} to continue.]",
            end + 1
        )
    };
    Ok(Outcome::SucceededWithDetails { output, details })
}

// Decode complete lines while retaining only a bounded prefix for display.
fn line(
    reader: &mut impl BufRead,
    output: &mut Vec<u8>,
    decoded: &mut [u8],
    cancellation: &CancellationToken,
) -> Result<(usize, bool)> {
    output.clear();
    let mut decoder = UTF_8.new_decoder_without_bom_handling();
    let mut length = 0;
    loop {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let buffer = reader.fill_buf()?;
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(buffer.len(), |position| position + 1);
        let last = buffer.is_empty() || newline.is_some();
        let mut input = &buffer[..count];
        loop {
            let (result, consumed, written, _) = decoder.decode_to_utf8(input, decoded, last);
            let keep = (MAX_BYTES + 1).saturating_sub(output.len()).min(written);
            output.extend_from_slice(&decoded[..keep]);
            length += written;
            if result == CoderResult::InputEmpty {
                break;
            }
            input = &input[consumed..];
        }
        reader.consume(count);
        if last {
            return Ok((length, newline.is_some()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn read(path: &Path, name: &str, offset: usize, limit: Option<usize>) -> Result<String> {
        super::read(path, name, offset, limit, &CancellationToken::new())
            .map(|outcome| outcome.tool_content())
    }
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
    fn malformed_utf8_sizes_count_all_decoded_replacement_bytes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut bytes = vec![0xff; 60_000];
        bytes.extend_from_slice(b"\nnext");
        std::fs::write(file.path(), bytes).unwrap();
        assert_eq!(
            read(file.path(), "fixture", 0, None).unwrap(),
            "[Line 1 is 175.8KB, exceeds 50.0KB limit. Use bash: sed -n '1p' fixture | head -c 51200]"
        );
        assert_eq!(read(file.path(), "fixture", 1, None).unwrap(), "next");
        assert_eq!(format_size(50 * 1024 + 256), "50.3KB");
        assert_eq!(format_size(1024 * 1024 + 256 * 1024), "1.3MB");
    }

    #[test]
    fn streaming_utf8_keeps_boms_and_decodes_across_buffer_boundaries() {
        let mut source = b"\xef\xbb\xbf".to_vec();
        source.extend_from_slice("界".as_bytes());
        source.extend_from_slice(b"\xe2\x82\n\xf0\x9f");
        let mut reader = BufReader::with_capacity(2, source.as_slice());
        let mut output = Vec::new();
        let mut decoded = [0; 8];
        let expected = "\u{feff}界\u{fffd}\n";
        assert_eq!(
            line(
                &mut reader,
                &mut output,
                &mut decoded,
                &CancellationToken::new()
            )
            .unwrap(),
            (expected.len(), true)
        );
        assert_eq!(output, expected.as_bytes());
        assert_eq!(
            line(
                &mut reader,
                &mut output,
                &mut decoded,
                &CancellationToken::new()
            )
            .unwrap(),
            (3, false)
        );
        assert_eq!(output, "\u{fffd}".as_bytes());
    }

    #[test]
    fn cancellation_interrupts_decoding_inside_a_line() {
        struct CancelAfterRead {
            data: &'static [u8],
            token: CancellationToken,
        }
        impl std::io::Read for CancelAfterRead {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                self.data.read(output)
            }
        }
        impl BufRead for CancelAfterRead {
            fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
                Ok(&self.data[..self.data.len().min(2)])
            }
            fn consume(&mut self, amount: usize) {
                self.data = &self.data[amount..];
                self.token.cancel();
            }
        }
        let token = CancellationToken::new();
        let mut reader = CancelAfterRead {
            data: b"long line",
            token: token.clone(),
        };
        assert!(matches!(
            line(&mut reader, &mut Vec::new(), &mut [0; 8], &token),
            Err(Error::Cancelled)
        ));
        assert_eq!(reader.data, b"ng line");
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
