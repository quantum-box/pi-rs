//! Bounded shell output with a recoverable full transcript.
use std::io::Write;
use std::path::PathBuf;

use encoding_rs::{CoderResult, Decoder, UTF_8};
use serde_json::{Value, json};

use super::tool_output::{
    Limit, MAX_BYTES as DISPLAY_BYTES, MAX_LINES as DISPLAY_LINES, Truncation, format_size,
};

const SCRIPT_BYTES: usize = 1024 * 1024;

#[derive(Default)]
pub(super) struct Accumulator {
    head: Vec<u8>,
    tail: Vec<u8>,
    bytes: u64,
    raw_bytes: u64,
    preserve_bom: bool,
    display: DisplayTail,
    file: Option<tempfile::NamedTempFile>,
}

pub(super) struct Output {
    pub display: String,
    pub display_without_notice: String,
    pub script: String,
    pub script_truncated: bool,
    pub path: Option<PathBuf>,
    pub details: Option<Value>,
}

impl Accumulator {
    pub fn for_user_shell() -> Self {
        Self {
            preserve_bom: true,
            display: DisplayTail {
                decoder: UTF_8.new_decoder_without_bom_handling(),
                ..DisplayTail::default()
            },
            ..Self::default()
        }
    }

    pub fn record_raw_bytes(&mut self, count: usize) {
        self.raw_bytes = self.raw_bytes.saturating_add(count as u64);
    }

    pub fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.bytes += bytes.len() as u64;
        self.display.decode(bytes, false);
        self.spill(true)?;
        if let Some(file) = &mut self.file {
            file.write_all(bytes)?;
        }
        let head_remaining = SCRIPT_BYTES.saturating_sub(self.head.len());
        self.head
            .extend_from_slice(&bytes[..bytes.len().min(head_remaining)]);
        // Backends may send a single chunk much larger than the retained tail.
        // Discard the excess before appending so allocation stays bounded too.
        if bytes.len() >= SCRIPT_BYTES {
            self.tail.clear();
            self.tail
                .extend_from_slice(&bytes[bytes.len() - SCRIPT_BYTES..]);
        } else {
            let discard = self
                .tail
                .len()
                .saturating_add(bytes.len())
                .saturating_sub(SCRIPT_BYTES);
            if discard != 0 {
                self.tail.drain(..discard);
            }
            self.tail.extend_from_slice(bytes);
        }
        Ok(())
    }

    /// A bounded, ephemeral view for tool progress subscribers.
    pub fn partial_result(&self) -> Value {
        let (display, limit, lines, partial) = self.display.snapshot();
        let mut details = json!({});
        if let Some(limit) = limit {
            details["truncation"] = json!(Truncation {
                content: &display,
                truncated: true,
                truncated_by: Some(limit),
                total_lines: self.display.lines(),
                total_bytes: self.display.bytes,
                output_lines: lines,
                output_bytes: display.len(),
                last_line_partial: partial,
                first_line_exceeds_limit: false,
                max_lines: DISPLAY_LINES as u64,
                max_bytes: DISPLAY_BYTES,
            });
            if let Some(file) = &self.file {
                details["fullOutputPath"] = json!(file.path());
            }
        }
        json!({"content":[{"type":"text","text":display}], "details":details})
    }

    fn spill(&mut self, include_raw: bool) -> std::io::Result<()> {
        if self.file.is_none()
            && (self.bytes > DISPLAY_BYTES as u64
                || (include_raw && self.raw_bytes > DISPLAY_BYTES as u64 && self.bytes > 0)
                || self.display.is_truncated())
        {
            let mut file = tempfile::Builder::new()
                .prefix("pi-bash-")
                .suffix(".log")
                .tempfile()?;
            file.write_all(&self.head)?;
            self.file = Some(file);
        }
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<Output> {
        self.display.decode(&[], true);
        self.spill(false)?;
        let (display, limit, lines, partial) = self.display.snapshot();
        let script_truncated = self.bytes > SCRIPT_BYTES as u64;
        let script = if script_truncated {
            let half = SCRIPT_BYTES / 2;
            let head = decode_head(&self.head[..half]);
            let tail = decode_tail(&self.tail[self.tail.len() - half..]);
            format!(
                "{head}\n\n[... {} bytes omitted ...]\n\n{tail}",
                self.bytes - SCRIPT_BYTES as u64
            )
        } else {
            let text = String::from_utf8_lossy(&self.head);
            if self.preserve_bom {
                text.into_owned()
            } else {
                text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned()
            }
        };
        let path = if let Some(mut file) = self.file.take() {
            file.flush()?;
            let (_, path) = file.keep().map_err(|error| error.error)?;
            Some(path)
        } else {
            None
        };
        let display_without_notice = display.clone();
        let details = limit.map(|limit| {
            json!({
                "truncation": Truncation {
                    content: &display,
                    truncated: true,
                    truncated_by: Some(limit),
                    total_lines: self.display.lines(),
                    total_bytes: self.display.bytes,
                    output_lines: lines,
                    output_bytes: display.len(),
                    last_line_partial: partial,
                    first_line_exceeds_limit: false,
                    max_lines: DISPLAY_LINES as u64,
                    max_bytes: DISPLAY_BYTES,
                },
                "fullOutputPath": path,
            })
        });
        let display = match (limit, path.as_ref()) {
            (Some(limit), Some(path)) => {
                let end = self.display.lines();
                let notice = if partial {
                    format!(
                        "Showing last {} of line {end} (line is {})",
                        format_size(display.len() as u64),
                        format_size(self.display.last_line_bytes()),
                    )
                } else {
                    let start = end.saturating_sub(lines as u64) + 1;
                    let bytes = if matches!(limit, Limit::Bytes) {
                        " (50.0KB limit)"
                    } else {
                        ""
                    };
                    format!("Showing lines {start}-{end} of {end}{bytes}")
                };
                format!("{display}\n\n[{notice}. Full output: {}]", path.display())
            }
            _ => display,
        };
        Ok(Output {
            display,
            display_without_notice,
            script,
            script_truncated,
            path,
            details,
        })
    }
}

struct DisplayTail {
    decoder: Decoder,
    tail: String,
    starts_at_boundary: bool,
    bytes: u64,
    newlines: u64,
    current_line: u64,
}

impl Default for DisplayTail {
    fn default() -> Self {
        Self {
            decoder: UTF_8.new_decoder_with_bom_removal(),
            tail: String::new(),
            starts_at_boundary: true,
            bytes: 0,
            newlines: 0,
            current_line: 0,
        }
    }
}

impl DisplayTail {
    fn decode(&mut self, mut input: &[u8], finished: bool) {
        let mut buffer = [0; 8192];
        loop {
            let (result, consumed, written, _) =
                self.decoder.decode_to_utf8(input, &mut buffer, finished);
            let text = std::str::from_utf8(&buffer[..written]).expect("UTF-8 decoder output");
            self.bytes += written as u64;
            for chunk in text.split_inclusive('\n') {
                if chunk.ends_with('\n') {
                    self.newlines += 1;
                    self.current_line = 0;
                } else {
                    self.current_line += chunk.len() as u64;
                }
            }
            self.tail.push_str(text);
            if self.tail.len() > 4 * DISPLAY_BYTES {
                let mut start = self.tail.len() - 2 * DISPLAY_BYTES;
                while !self.tail.is_char_boundary(start) {
                    start += 1;
                }
                self.starts_at_boundary = self.tail.as_bytes()[start - 1] == b'\n';
                self.tail.drain(..start);
            }
            if result == CoderResult::InputEmpty {
                break;
            }
            input = &input[consumed..];
        }
    }

    fn lines(&self) -> u64 {
        self.newlines + u64::from(self.current_line != 0)
    }

    fn last_line_bytes(&self) -> u64 {
        self.current_line
    }

    fn is_truncated(&self) -> bool {
        self.bytes > DISPLAY_BYTES as u64 || self.lines() > DISPLAY_LINES as u64
    }

    fn snapshot(&self) -> (String, Option<Limit>, usize, bool) {
        if !self.is_truncated() {
            return (self.tail.clone(), None, self.lines() as usize, false);
        }
        let mut tail = self.tail.as_str();
        let mut skipped_partial_line = false;
        if !self.starts_at_boundary
            && let Some(index) = tail.find('\n')
            && index + 1 < tail.len()
        {
            tail = &tail[index + 1..];
            skipped_partial_line = true;
        }
        let mut selected = Vec::new();
        let mut bytes = 0;
        let mut partial = false;
        let mut limit = Limit::Lines;
        for line in tail.split_terminator('\n').rev().take(DISPLAY_LINES) {
            let next = line.len() + usize::from(!selected.is_empty());
            if bytes + next > DISPLAY_BYTES {
                limit = Limit::Bytes;
                if selected.is_empty() {
                    let mut start = line.len().saturating_sub(DISPLAY_BYTES);
                    while !line.is_char_boundary(start) {
                        start += 1;
                    }
                    selected.push(&line[start..]);
                    partial = true;
                }
                break;
            }
            selected.push(line);
            bytes += next;
        }
        if selected.len() == DISPLAY_LINES {
            limit = Limit::Lines;
        } else if skipped_partial_line {
            limit = Limit::Bytes;
        }
        selected.reverse();
        (selected.join("\n"), Some(limit), selected.len(), partial)
    }
}

fn decode_tail(bytes: &[u8]) -> String {
    let start = bytes
        .iter()
        .position(|byte| byte & 0xc0 != 0x80)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}
fn decode_head(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(error) if error.error_len().is_none() => {
            String::from_utf8_lossy(&bytes[..error.valid_up_to()]).into_owned()
        }
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn progress_snapshot_is_bounded_and_exposes_complete_output_path() {
        let mut output = Accumulator::default();
        let bytes = vec![b'x'; DISPLAY_BYTES + 200];
        output.append(&bytes).unwrap();
        let snapshot = output.partial_result();
        assert!(snapshot["content"][0]["text"].as_str().unwrap().len() <= DISPLAY_BYTES);
        assert_eq!(snapshot["details"]["truncation"]["truncated"], true);
        let path = snapshot["details"]["fullOutputPath"].as_str().unwrap();
        assert_eq!(std::fs::read(path).unwrap(), bytes);
        let finished = output.finish().unwrap();
        std::fs::remove_file(finished.path.unwrap()).unwrap();
    }
    #[test]
    fn small_output_retains_bytes_without_a_file() {
        let mut output = Accumulator::default();
        output.append("日本語\n".as_bytes()).unwrap();
        let output = output.finish().unwrap();
        assert_eq!(output.display, "日本語\n");
        assert_eq!(output.script, output.display);
        assert!(!output.script_truncated);
        assert!(output.path.is_none());
    }
    #[test]
    fn oversized_backend_chunk_keeps_only_bounded_tail_and_complete_output() {
        let mut accumulator = Accumulator::default();
        let first = vec![b'a'; SCRIPT_BYTES / 2];
        let mut large = vec![b'b'; 8 * SCRIPT_BYTES];
        large.extend_from_slice(b"\nlast line\n");
        accumulator.append(&first).unwrap();
        accumulator.append(&large).unwrap();
        assert_eq!(accumulator.tail.len(), SCRIPT_BYTES);
        assert!(accumulator.tail.capacity() <= 2 * SCRIPT_BYTES);
        let output = accumulator.finish().unwrap();
        assert!(output.script.starts_with(&"a".repeat(20)));
        assert!(output.script.ends_with("\nlast line\n"));
        assert!(output.display_without_notice.ends_with("last line"));
        assert!(output.script_truncated);
        assert_eq!(
            output.details.as_ref().unwrap()["truncation"]["truncated"],
            true
        );
        let path = output.path.unwrap();
        let saved = std::fs::read(&path).unwrap();
        assert_eq!(&saved[..first.len()], first);
        assert_eq!(&saved[first.len()..], large);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn line_limit_keeps_tail_and_publishes_complete_file() {
        let input = (1..=2100)
            .map(|line| format!("{line}\n"))
            .collect::<String>();
        let mut output = Accumulator::default();
        for chunk in input.as_bytes().chunks(97) {
            output.append(chunk).unwrap();
        }
        let output = output.finish().unwrap();
        assert!(output.display.starts_with("101\n102\n"));
        assert!(output.display.contains("lines 101-2100 of 2100"));
        assert_eq!(output.script, input);
        assert!(!output.script_truncated);
        let path = output.path.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), input);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn trailing_newline_at_the_byte_limit_uses_the_reference_limit_kind() {
        let input = format!("{}\n", "x".repeat(DISPLAY_BYTES));
        let mut accumulator = Accumulator::default();
        accumulator.append(input.as_bytes()).unwrap();
        let output = accumulator.finish().unwrap();
        let truncation = &output.details.as_ref().unwrap()["truncation"];
        assert_eq!(truncation["truncatedBy"], "lines");
        assert_eq!(truncation["outputBytes"], DISPLAY_BYTES);
        assert_eq!(output.display_without_notice, "x".repeat(DISPLAY_BYTES));
        assert!(
            output
                .display
                .contains("[Showing lines 1-1 of 1. Full output: ")
        );
        std::fs::remove_file(output.path.unwrap()).unwrap();
    }

    #[test]
    fn long_discarded_line_before_short_tail_reports_byte_limit() {
        let input = format!("{}\nlast\n", "x".repeat(300_000));
        let mut accumulator = Accumulator::default();
        for chunk in input.as_bytes().chunks(8192) {
            accumulator.append(chunk).unwrap();
        }
        let output = accumulator.finish().unwrap();
        let truncation = &output.details.as_ref().unwrap()["truncation"];
        assert_eq!(truncation["truncatedBy"], "bytes");
        assert_eq!(output.display_without_notice, "last");
        assert!(
            output
                .display
                .contains("Showing lines 2-2 of 2 (50.0KB limit)")
        );
        std::fs::remove_file(output.path.unwrap()).unwrap();
    }

    #[test]
    fn oversized_last_line_reports_reference_size_with_and_without_newline() {
        let text = "界".repeat(100_000);
        for (suffix, expected_size) in [("", "293.0KB"), ("\n", "0B")] {
            let mut accumulator = Accumulator::default();
            for chunk in text.as_bytes().chunks(313) {
                accumulator.append(chunk).unwrap();
            }
            accumulator.append(suffix.as_bytes()).unwrap();
            let output = accumulator.finish().unwrap();
            let details = output.details.unwrap();
            let truncation = &details["truncation"];
            assert_eq!(truncation["truncatedBy"], "bytes");
            assert_eq!(truncation["lastLinePartial"], true);
            assert_eq!(truncation["totalLines"], 1);
            assert_eq!(truncation["totalBytes"], 300000 + suffix.len());
            assert_eq!(truncation["outputBytes"], 51198);
            assert!(output.display.contains(&format!(
                "Showing last 50.0KB of line 1 (line is {expected_size})"
            )));
            assert!(!output.display.contains('�'));
            std::fs::remove_file(output.path.unwrap()).unwrap();
        }
    }

    #[test]
    fn large_multibyte_output_has_bounded_views_and_complete_file() {
        let input = format!("head\n{}\ntail\n", "界".repeat(SCRIPT_BYTES));
        let mut output = Accumulator::default();
        for chunk in input.as_bytes().chunks(8192) {
            output.append(chunk).unwrap();
        }
        let output = output.finish().unwrap();
        assert!(output.script.starts_with("head\n"));
        assert!(output.script.ends_with("\ntail\n"));
        assert!(!output.script.contains('\u{fffd}'));
        assert!(output.script.len() < SCRIPT_BYTES + 100);
        assert!(output.script_truncated);
        assert!(output.display.starts_with("tail"));
        let path = output.path.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), input);
        std::fs::remove_file(path).unwrap();
    }
}
