//! Bounded, streaming grep with matching and context line markers.
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use encoding_rs::{CoderResult, Encoding, UTF_8, UTF_16BE, UTF_16LE};

use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder, Sink, SinkMatch};
use pi_rs::{CancellationToken, Error, Outcome, Result};
use serde_json::{Map, Value, json};

use super::tool_output::{self, Limit, MAX_BYTES, Truncation, UNLIMITED_LINES};

pub(super) struct Grep {
    matcher: RegexMatcher,
    searcher: Searcher,
    output: Output,
    context: u64,
}
impl Grep {
    pub fn new(args: &Value, limit: usize) -> Result<Self> {
        let pattern = args["pattern"]
            .as_str()
            .ok_or_else(|| Error::Config("pattern must be a string".into()))?;
        let pattern = if args["literal"] == true {
            regex::escape(pattern)
        } else {
            pattern.to_owned()
        };
        let matcher = RegexMatcherBuilder::new()
            .case_insensitive(args["ignoreCase"] == true)
            .build(&pattern)
            .map_err(|error| Error::Config(error.to_string()))?;
        let context = args["context"]
            .as_u64()
            .unwrap_or_else(|| args["context"].as_f64().unwrap_or(0.0).max(0.0) as u64);
        Ok(Self {
            matcher,
            searcher: SearcherBuilder::new()
                .line_number(true)
                .binary_detection(BinaryDetection::quit(0))
                .heap_limit(Some(64 * 1024 * 1024))
                .build(),
            output: Output {
                limit,
                ..Output::default()
            },
            context,
        })
    }
    pub fn search(
        &mut self,
        path: &Path,
        display: &str,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        let file = SearchInput {
            file: std::fs::File::open(path)?,
            cancellation,
        };
        let mut matches = Vec::new();
        let result = self.searcher.search_reader(
            &self.matcher,
            file,
            MatchSink {
                display,
                output: &mut self.output,
                matches: &mut matches,
                with_context: self.context != 0,
            },
        );
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        result?;
        if self.context != 0 && !matches.is_empty() {
            render_context_or_unavailable(
                path,
                display,
                &matches,
                self.context,
                &mut self.output,
                cancellation,
            )?;
        }
        Ok(self.output.matches < self.output.limit)
    }
    pub fn finish(self) -> Outcome {
        let output = self.output;
        if output.matches == 0 {
            return Outcome::Succeeded("No matches found".into());
        }
        let text = output.lines.join("\n");
        let mut details = Map::new();
        let mut notices = Vec::new();
        if output.matches == output.limit {
            details.insert("matchLimitReached".into(), json!(output.limit));
            notices.push(format!(
                "{} matches limit reached. Use limit={} for more, or refine pattern",
                output.limit,
                output.limit.saturating_mul(2)
            ));
        }
        if output.total_bytes > MAX_BYTES as u64 {
            details.insert(
                "truncation".into(),
                json!(Truncation {
                    content: &text,
                    truncated: true,
                    truncated_by: Some(Limit::Bytes),
                    total_lines: output.total_lines,
                    total_bytes: output.total_bytes,
                    output_lines: output.lines.len(),
                    output_bytes: text.len(),
                    last_line_partial: false,
                    first_line_exceeds_limit: output.lines.is_empty(),
                    max_lines: UNLIMITED_LINES,
                    max_bytes: MAX_BYTES,
                }),
            );
            notices.push("50.0KB limit reached".into());
        }
        if output.long_lines {
            details.insert("linesTruncated".into(), json!(true));
            notices
                .push("Some lines truncated to 500 chars. Use read tool to see full lines".into());
        }
        tool_output::finish(text, details, &notices)
    }
}

struct SearchInput<'a> {
    file: std::fs::File,
    cancellation: &'a CancellationToken,
}

impl Read for SearchInput<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::other("Operation aborted"));
        }
        self.file.read(buffer)
    }
}

#[derive(Default)]
struct Output {
    lines: Vec<String>,
    total_bytes: u64,
    total_lines: u64,
    matches: usize,
    limit: usize,
    long_lines: bool,
    kept_bytes: usize,
    prefix_full: bool,
}
impl Output {
    fn render(&mut self, display: &str, line: &ContextLine, matched: bool) {
        if self.prefix_full {
            return;
        }
        let separator = usize::from(!self.lines.is_empty());
        let mark = if matched { ':' } else { '-' };
        let formatted = format!("{display}{mark}{}{mark} {}", line.number, line.text);
        if self
            .kept_bytes
            .saturating_add(separator)
            .saturating_add(formatted.len())
            > MAX_BYTES
        {
            self.prefix_full = true;
        } else {
            self.kept_bytes += separator + formatted.len();
            self.lines.push(formatted);
        }
    }
    fn line(&mut self, display: &str, line: &ContextLine, matched: bool) {
        self.long_lines |= line.truncated;
        let size = formatted_len(display, line);
        self.total_bytes = self
            .total_bytes
            .saturating_add(size + u64::from(self.total_lines > 0));
        self.total_lines = self.total_lines.saturating_add(1);
        self.render(display, line, matched);
    }
}

fn formatted_len(display: &str, line: &ContextLine) -> u64 {
    (display.len() + line.number.to_string().len() + 3 + line.text.len()) as u64
}

struct ContextLine {
    number: u64,
    text: String,
    truncated: bool,
}
impl ContextLine {
    fn new(number: u64, bytes: &[u8]) -> Self {
        let text = String::from_utf8_lossy(bytes).replace('\r', "");
        let text = text.trim_end_matches('\n');
        let mut units = text.encode_utf16();
        let prefix: Vec<_> = units.by_ref().take(500).collect();
        let truncated = units.next().is_some();
        let mut text = String::from_utf16_lossy(&prefix);
        if truncated {
            text.push_str("... [truncated]");
        }
        Self {
            number,
            text,
            truncated,
        }
    }
}

struct MatchSink<'a> {
    display: &'a str,
    output: &'a mut Output,
    matches: &'a mut Vec<u64>,
    with_context: bool,
}
impl Sink for MatchSink<'_> {
    type Error = io::Error;
    fn matched(&mut self, _: &Searcher, found: &SinkMatch<'_>) -> io::Result<bool> {
        let number = found.line_number().expect("line numbers enabled");
        self.output.matches += 1;
        if self.with_context {
            self.matches.push(number);
        } else {
            self.output
                .line(self.display, &ContextLine::new(number, found.bytes()), true);
        }
        Ok(self.output.matches < self.output.limit)
    }
}

// Decode a line in fixed-size chunks, keeping only the first 501 UTF-16
// units after CR removal. A long run of CRs must not hide later text.
fn read_context_line(
    reader: &mut impl BufRead,
    number: u64,
    encoding: &'static Encoding,
    cancellation: &CancellationToken,
) -> Result<(ContextLine, bool, bool)> {
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let mut decoded = [0u8; 8192];
    let mut units = Vec::with_capacity(501);
    let mut has_bytes = false;
    let mut pending_byte = None;
    loop {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let buf = reader.fill_buf()?;
        let newline = if encoding == UTF_8 {
            buf.iter().position(|&byte| byte == b'\n')
        } else {
            let mut end = None;
            for (index, &byte) in buf.iter().enumerate() {
                if let Some(first) = pending_byte.take() {
                    let pair = [first, byte];
                    if (encoding == UTF_16LE && pair == [b'\n', 0])
                        || (encoding == UTF_16BE && pair == [0, b'\n'])
                    {
                        end = Some(index);
                        break;
                    }
                } else {
                    pending_byte = Some(byte);
                }
            }
            end
        };
        let count = newline.map_or(buf.len(), |index| index + 1);
        let last = buf.is_empty() || newline.is_some();
        has_bytes |= count != 0;
        let mut input = &buf[..count];
        loop {
            let (status, consumed, written, _) = decoder.decode_to_utf8(input, &mut decoded, last);
            if units.len() < 501 {
                let text = std::str::from_utf8(&decoded[..written]).expect("decoded UTF-8");
                for ch in text.chars().filter(|&ch| ch != '\r' && ch != '\n') {
                    let mut encoded = [0u16; 2];
                    for &unit in ch.encode_utf16(&mut encoded).iter() {
                        if units.len() < 501 {
                            units.push(unit);
                        }
                    }
                    if units.len() == 501 {
                        break;
                    }
                }
            }
            if status == CoderResult::InputEmpty {
                break;
            }
            input = &input[consumed..];
        }
        reader.consume(count);
        if last {
            let truncated = units.len() > 500;
            units.truncate(500);
            let mut text = String::from_utf16_lossy(&units);
            if truncated {
                text.push_str("... [truncated]");
            }
            return Ok((
                ContextLine {
                    number,
                    text,
                    truncated,
                },
                has_bytes,
                newline.is_some(),
            ));
        }
    }
}

// Prefix totals at interval boundaries let overlapping blocks be counted in
// a linear scan after sorting the boundaries, even after the display prefix is full.
#[derive(Clone, Copy, Default)]
struct Prefix {
    lines: u64,
    bytes: u64,
    long: u64,
}
#[derive(Default)]
struct Block {
    start: u64,
    end: u64,
    offset: u64,
    before: Prefix,
    after: Prefix,
}

// Pi keeps the match if a subsequent context read fails. Preserve prior
// results and discard any partial context produced before an I/O failure.
fn render_context_or_unavailable(
    path: &Path,
    display: &str,
    matches: &[u64],
    context: u64,
    output: &mut Output,
    cancellation: &CancellationToken,
) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let before = (
        output.lines.len(),
        output.total_bytes,
        output.total_lines,
        output.long_lines,
        output.kept_bytes,
        output.prefix_full,
    );
    match render_context(path, display, matches, context, output, cancellation) {
        Ok(()) => Ok(()),
        Err(Error::Io(_)) if !cancellation.is_cancelled() => {
            output.lines.truncate(before.0);
            output.total_bytes = before.1;
            output.total_lines = before.2;
            output.long_lines = before.3;
            output.kept_bytes = before.4;
            output.prefix_full = before.5;
            for &number in matches {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                output.line(
                    display,
                    &ContextLine {
                        number,
                        text: "(unable to read file)".into(),
                        truncated: false,
                    },
                    true,
                );
            }
            Ok(())
        }
        Err(_) if cancellation.is_cancelled() => Err(Error::Cancelled),
        Err(error) => Err(error),
    }
}

fn render_context(
    path: &Path,
    display: &str,
    matches: &[u64],
    context: u64,
    output: &mut Output,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut blocks: Vec<Block> = matches
        .iter()
        .map(|&number| Block {
            start: number.saturating_sub(context).max(1),
            end: number.saturating_add(context),
            ..Block::default()
        })
        .collect();
    let mut events = Vec::with_capacity(blocks.len() * 2);
    for (index, block) in blocks.iter().enumerate() {
        events.push((block.start, index, false));
        events.push((block.end.saturating_add(1), index, true));
    }
    events.sort_unstable();
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    // Pi reads context with readFile(path, "utf-8"), even when ripgrep
    // detected a UTF-16 BOM while finding the match.
    let encoding = UTF_8;
    let mut prefix = Prefix::default();
    let mut event = 0;
    let mut number = 1u64;
    let mut previous_newline = false;
    loop {
        while event < events.len() && events[event].0 <= number {
            let (_, index, end) = events[event];
            if end {
                blocks[index].after = prefix;
            } else {
                blocks[index].before = prefix;
                blocks[index].offset = reader.stream_position()?;
            }
            event += 1;
        }
        if event == events.len() {
            break;
        }
        // Pi's context read is independent of the matcher's binary detection.
        let (line, has_bytes, newline) =
            read_context_line(&mut reader, number, encoding, cancellation)?;
        if !has_bytes && number > 1 && !previous_newline {
            break;
        }
        prefix.lines = prefix.lines.saturating_add(1);
        prefix.bytes = prefix
            .bytes
            .saturating_add(formatted_len(display, &line).saturating_add(1));
        prefix.long = prefix.long.saturating_add(u64::from(line.truncated));
        number = number.saturating_add(1);
        previous_newline = newline;
        if !has_bytes {
            break;
        }
    }
    while event < events.len() {
        let (_, index, end) = events[event];
        if end {
            blocks[index].after = prefix;
        } else {
            blocks[index].before = prefix;
            blocks[index].offset = reader.stream_position()?;
        }
        event += 1;
    }
    for (block, &center) in blocks.iter().zip(matches) {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let lines = block.after.lines - block.before.lines;
        let bytes = block.after.bytes - block.before.bytes;
        if lines > 0 {
            output.total_bytes = output
                .total_bytes
                .saturating_add(bytes - u64::from(output.total_lines == 0));
            output.total_lines = output.total_lines.saturating_add(lines);
            output.long_lines |= block.after.long > block.before.long;
        }
        if output.prefix_full {
            continue;
        }
        reader.seek(SeekFrom::Start(block.offset))?;
        for index in 0..lines {
            let number = block.start + index;
            let (line, _, _) = read_context_line(&mut reader, number, encoding, cancellation)?;
            output.render(display, &line, number == center);
            if output.prefix_full {
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reading_search_input_stops_after_cancellation() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"no matches").unwrap();
        let cancellation = CancellationToken::new();
        let mut input = SearchInput {
            file: std::fs::File::open(file.path()).unwrap(),
            cancellation: &cancellation,
        };
        let mut buffer = [0; 1];
        assert_eq!(input.read(&mut buffer).unwrap(), 1);
        cancellation.cancel();
        assert_eq!(
            input.read(&mut buffer).unwrap_err().to_string(),
            "Operation aborted"
        );
    }
    use serde_json::json;
    #[test]
    fn unavailable_context_keeps_matches_and_prior_output() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("removed");
        let token = CancellationToken::new();
        let mut output = Output {
            matches: 3,
            limit: 100,
            ..Output::default()
        };
        output.line("earlier", &ContextLine::new(1, b"earlier match"), true);
        render_context_or_unavailable(&missing, "removed", &[2, 5], 1, &mut output, &token)
            .unwrap();
        assert_eq!(
            output.lines.join("\n"),
            "earlier:1: earlier match\nremoved:2: (unable to read file)\nremoved:5: (unable to read file)"
        );
        assert_eq!(output.matches, 3);
        assert_eq!(output.total_lines, 3);
        assert_eq!(output.total_bytes, output.lines.join("\n").len() as u64);
    }

    #[test]
    fn cancelled_context_does_not_fall_back_to_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let mut output = Output::default();
        assert!(matches!(
            render_context_or_unavailable(
                &directory.path().join("removed"),
                "removed",
                &[1],
                1,
                &mut output,
                &token
            ),
            Err(Error::Cancelled)
        ));
        assert!(output.lines.is_empty());
    }

    #[test]
    fn overlapping_context_is_repeated_for_each_matching_line() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "before\nmatch one\nmatch two\nafter\n").unwrap();
        let mut grep = Grep::new(&json!({"pattern":"match", "context":1}), 100).unwrap();
        assert!(
            grep.search(file.path(), "text", &CancellationToken::new())
                .unwrap()
        );
        assert_eq!(
            grep.finish().tool_content(),
            "text-1- before\ntext:2: match one\ntext-3- match two\ntext-2- match one\ntext:3: match two\ntext-4- after"
        );
        let mut grep = Grep::new(&json!({"pattern":"match", "context":1}), 1).unwrap();
        assert!(
            !grep
                .search(file.path(), "text", &CancellationToken::new())
                .unwrap()
        );
        assert_eq!(
            grep.finish().tool_content(),
            "text-1- before\ntext:2: match one\ntext-3- match two\n\n[1 matches limit reached. Use limit=2 for more, or refine pattern]"
        );
    }

    #[test]
    fn context_is_flushed_at_end_of_file_and_gaps() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            "match first\nafter\ngap\ngap\nbefore\nmatch last",
        )
        .unwrap();
        let mut grep = Grep::new(&json!({"pattern":"match", "context":1}), 100).unwrap();
        assert!(
            grep.search(file.path(), "text", &CancellationToken::new())
                .unwrap()
        );
        assert_eq!(
            grep.finish().tool_content(),
            "text:1: match first\ntext-2- after\ntext-5- before\ntext:6: match last"
        );
    }

    #[test]
    fn context_over_one_thousand_lines_is_honored_and_byte_truncated() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let content = format!(
            "{}needle\n{}",
            "before\n".repeat(1001),
            "after\n".repeat(1001)
        );
        std::fs::write(file.path(), content).unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1001}), 100).unwrap();
        grep.search(file.path(), "text", &CancellationToken::new())
            .unwrap();
        let output = grep.finish().tool_content();
        assert!(output.starts_with("text-1- before\n"));
        assert!(output.contains("text:1002: needle\n"));
        assert!(output.ends_with("text-2003- after"));

        let content = format!(
            "{}needle\n{}",
            format!("{}\n", "x".repeat(40)).repeat(1001),
            "after\n".repeat(1001)
        );
        std::fs::write(file.path(), content).unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1001}), 100).unwrap();
        grep.search(file.path(), "text", &CancellationToken::new())
            .unwrap();
        let outcome = grep.finish();
        match outcome {
            Outcome::SucceededWithDetails { output, details } => {
                assert!(output.starts_with("text-1- "));
                assert!(output.ends_with("[50.0KB limit reached]"));
                assert_eq!(details["truncation"]["totalLines"], 2003);
                assert_eq!(details["truncation"]["truncatedBy"], "bytes");
            }
            other => panic!("expected truncated output, got {other:?}"),
        }
    }

    #[test]
    fn floating_context_values_and_large_overlapping_blocks_match_reference() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let lines: Vec<String> = (1..=2200)
            .map(|n| {
                if (1001..=1100).contains(&n) {
                    format!("match {n}")
                } else if n == 2100 {
                    "x".repeat(600)
                } else {
                    format!("line {n}")
                }
            })
            .collect();
        std::fs::write(file.path(), lines.join("\n")).unwrap();
        for context in [json!(1001.0), json!(1e20)] {
            let radius = if context == json!(1001.0) {
                1001usize
            } else {
                usize::MAX
            };
            let mut grep = Grep::new(&json!({"pattern":"match", "context":context}), 100).unwrap();
            grep.search(file.path(), "f", &CancellationToken::new())
                .unwrap();
            let mut rows = Vec::new();
            for center in 1001usize..=1100 {
                let start = center.saturating_sub(radius).max(1);
                let end = center.saturating_add(radius).min(lines.len());
                for n in start..=end {
                    let line = ContextLine::new(n as u64, lines[n - 1].as_bytes());
                    let mark = if n == center { ':' } else { '-' };
                    rows.push(format!("f{mark}{n}{mark} {}", line.text));
                }
            }
            let raw = rows.join("\n");
            let end = raw.as_bytes()[..=MAX_BYTES]
                .iter()
                .rposition(|b| *b == b'\n')
                .unwrap();
            match grep.finish() {
                Outcome::SucceededWithDetails { output, details } => {
                    assert_eq!(details["truncation"]["content"], &raw[..end]);
                    assert_eq!(details["truncation"]["totalLines"], rows.len());
                    assert_eq!(details["truncation"]["totalBytes"], raw.len());
                    assert_eq!(details["linesTruncated"], true);
                    assert!(output.contains("50.0KB limit reached"));
                }
                other => panic!("expected truncation, got {other:?}"),
            }
        }
    }

    #[test]
    fn huge_context_handles_large_no_match_and_late_match_files() {
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let block = format!("{}\n", "a".repeat(255)).repeat(4096);
        for _ in 0..65 {
            file.write_all(block.as_bytes()).unwrap();
        }
        file.flush().unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1e20}), 100).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        assert_eq!(grep.finish().tool_content(), "No matches found");
        file.write_all(b"needle\n").unwrap();
        file.flush().unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1e20}), 100).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        match grep.finish() {
            Outcome::SucceededWithDetails { output, details } => {
                assert!(output.starts_with("f-1- "));
                assert!(output.contains("50.0KB limit reached"));
                assert_eq!(details["truncation"]["totalLines"], 65 * 4096 + 2);
            }
            other => panic!("expected truncation, got {other:?}"),
        }
    }

    fn utf16_bytes(text: &str, little_endian: bool) -> Vec<u8> {
        let mut bytes = if little_endian {
            vec![0xff, 0xfe]
        } else {
            vec![0xfe, 0xff]
        };
        for unit in text.encode_utf16() {
            bytes.extend_from_slice(&if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            });
        }
        bytes
    }

    #[test]
    fn utf8_context_preserves_overlaps() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let text = "\u{0a01} first 🦀\r\nneedle one\nneedle two\nafter\n";
        std::fs::write(file.path(), text).unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1}), 100).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        assert_eq!(
            grep.finish().tool_content(),
            "f-1- \u{0a01} first 🦀\nf:2: needle one\nf-3- needle two\nf-2- needle one\nf:3: needle two\nf-4- after"
        );
    }

    #[test]
    fn utf8_bom_is_preserved_in_context_and_counts_toward_line_limit() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let text = format!("\u{feff}{}\nneedle\n", "x".repeat(500));
        std::fs::write(file.path(), text).unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1}), 100).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        match grep.finish() {
            Outcome::SucceededWithDetails { output, details } => {
                assert_eq!(
                    output,
                    format!(
                        "f-1- \u{feff}{}... [truncated]\nf:2: needle\nf-3- \n\n[Some lines truncated to 500 chars. Use read tool to see full lines]",
                        "x".repeat(499)
                    )
                );
                assert_eq!(details["linesTruncated"], true);
            }
            other => panic!("expected line truncation, got {other:?}"),
        }
        // The no-context path uses the searcher's BOM-stripped match text.
        std::fs::write(file.path(), "\u{feff}needle\n").unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle"}), 100).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        assert_eq!(grep.finish().tool_content(), "f:1: needle");
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1}), 100).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        assert_eq!(grep.finish().tool_content(), "f:1: \u{feff}needle\nf-2- ");
    }

    #[test]
    fn utf8_bom_contributes_bytes_to_each_overlapping_context_block() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let text = format!(
            "first\nneedle one\nneedle two\n{}",
            "padding\n".repeat(5000)
        );
        let mut outcomes = Vec::new();
        for text in [text.clone(), format!("\u{feff}{text}")] {
            std::fs::write(file.path(), text).unwrap();
            let mut grep = Grep::new(&json!({"pattern":"needle", "context":1e20}), 100).unwrap();
            grep.search(file.path(), "f", &CancellationToken::new())
                .unwrap();
            outcomes.push(grep.finish());
        }
        match (&outcomes[0], &outcomes[1]) {
            (
                Outcome::SucceededWithDetails { details: plain, .. },
                Outcome::SucceededWithDetails {
                    output,
                    details: bom,
                },
            ) => {
                assert!(output.starts_with("f-1- \u{feff}first\n"));
                assert_eq!(
                    bom["truncation"]["totalBytes"].as_u64().unwrap(),
                    plain["truncation"]["totalBytes"].as_u64().unwrap() + 6
                );
                assert_eq!(
                    bom["truncation"]["totalLines"],
                    plain["truncation"]["totalLines"]
                );
            }
            other => panic!("expected byte truncation, got {other:?}"),
        }
    }

    #[test]
    fn context_keeps_nul_and_following_lines_for_a_reported_match() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let text = "needle\0tail\nafter\n";
        let bytes = text.as_bytes().to_vec();
        std::fs::write(file.path(), bytes).unwrap();
        let mut output = Output {
            matches: 1,
            limit: 100,
            ..Output::default()
        };
        render_context(
            file.path(),
            "f",
            &[1],
            2,
            &mut output,
            &CancellationToken::new(),
        )
        .unwrap();
        let expected = "f:1: needle\0tail\nf-2- after\nf-3- ";
        assert_eq!(output.lines.join("\n"), expected);
        assert_eq!(output.total_lines, 3);
        assert_eq!(output.total_bytes, expected.len() as u64);
    }

    #[test]
    fn grep_context_includes_nul_and_later_lines_but_matcher_stops() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let text = format!("needle\n{}\n\0binary\nneedle after\n", "p".repeat(150_000));
        let bytes = text.as_bytes().to_vec();
        std::fs::write(file.path(), bytes).unwrap();
        for limit in [1, 100] {
            let mut grep = Grep::new(&json!({"pattern":"needle", "context":5}), limit).unwrap();
            grep.search(file.path(), "f", &CancellationToken::new())
                .unwrap();
            let outcome = grep.finish();
            let output = outcome.tool_content();
            assert!(output.starts_with("f:1: needle\nf-2- "));
            assert!(output.contains("f-3- \0binary\nf-4- needle after\nf-5- "));
            assert!(!output.contains("f:4:"));
        }
    }

    #[test]
    fn context_truncation_metadata_counts_lines_past_nul() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let text = format!(
            "needle\n{}\0binary\n{}\n",
            "padding\n".repeat(15000),
            "z".repeat(700)
        );
        let bytes = text.as_bytes().to_vec();
        std::fs::write(file.path(), bytes).unwrap();
        let mut grep = Grep::new(&json!({"pattern":"needle", "context":1e20}), 1).unwrap();
        grep.search(file.path(), "f", &CancellationToken::new())
            .unwrap();
        match grep.finish() {
            Outcome::SucceededWithDetails { output, details } => {
                let mut rows = std::iter::once("f:1: needle".to_owned())
                    .chain((2..=15001).map(|number| format!("f-{number}- padding")))
                    .collect::<Vec<_>>();
                rows.push("f-15002- \0binary".into());
                rows.push(format!("f-15003- {}... [truncated]", "z".repeat(500)));
                rows.push("f-15004- ".into());
                let raw = rows.join("\n");
                assert_eq!(details["truncation"]["totalLines"], 15004);
                assert_eq!(details["truncation"]["totalBytes"], raw.len());
                assert_eq!(details["linesTruncated"], true);
                assert!(output.contains("50.0KB limit reached"));
            }
            other => panic!("expected byte truncation, got {other:?}"),
        }
    }

    #[test]
    fn utf16_matches_use_node_utf8_context_decoding() {
        let file = tempfile::NamedTempFile::new().unwrap();
        for (little, expected) in [
            (true, "f:1: ��n\0e\0e\0d\0l\0e\0\nf-2- \0n\0e\0x\0t\0"),
            (false, "f:1: ��\0n\0e\0e\0d\0l\0e\0\nf-2- \0n\0e\0x\0t\0"),
        ] {
            std::fs::write(file.path(), utf16_bytes("needle\nnext\n", little)).unwrap();
            let mut direct = Grep::new(&json!({"pattern":"needle"}), 100).unwrap();
            direct
                .search(file.path(), "f", &CancellationToken::new())
                .unwrap();
            assert_eq!(direct.finish().tool_content(), "f:1: needle");
            let mut grep = Grep::new(&json!({"pattern":"needle", "context":1}), 100).unwrap();
            grep.search(file.path(), "f", &CancellationToken::new())
                .unwrap();
            assert_eq!(grep.finish().tool_content(), expected);
        }
    }

    #[test]
    fn utf16_context_handles_odd_buffer_boundaries_and_embedded_newline_bytes() {
        let text = "\u{0a01}🦀\nsecond\n";
        for (encoding, little_endian) in [(UTF_16LE, true), (UTF_16BE, false)] {
            let bytes = utf16_bytes(text, little_endian);
            let mut reader = BufReader::with_capacity(3, &bytes[2..]);
            let (first, _, newline) =
                read_context_line(&mut reader, 1, encoding, &CancellationToken::new()).unwrap();
            assert!(newline);
            assert_eq!(first.text, "\u{0a01}🦀");
            let (second, _, newline) =
                read_context_line(&mut reader, 2, encoding, &CancellationToken::new()).unwrap();
            assert!(newline);
            assert_eq!(second.text, "second");

            let bytes = utf16_bytes("a\0binary\n", little_endian);
            let mut reader = BufReader::with_capacity(3, &bytes[2..]);
            let (line, _, _) =
                read_context_line(&mut reader, 1, encoding, &CancellationToken::new()).unwrap();
            assert_eq!(line.text, "a\0binary");
        }
    }

    #[test]
    fn context_decoding_skips_arbitrarily_long_carriage_return_runs() {
        let input = format!("{}visible\n", "\r".repeat(100_000));
        let mut reader = BufReader::new(input.as_bytes());
        let (line, _, _) =
            read_context_line(&mut reader, 1, UTF_8, &CancellationToken::new()).unwrap();
        assert_eq!(line.text, "visible");
        assert!(!line.truncated);
    }

    #[test]
    fn context_includes_the_final_empty_line_after_a_trailing_newline() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "before\nmatch last\n").unwrap();
        let mut grep = Grep::new(&json!({"pattern":"match", "context":1}), 100).unwrap();
        assert!(
            grep.search(file.path(), "text", &CancellationToken::new())
                .unwrap()
        );
        assert_eq!(
            grep.finish().tool_content(),
            "text-1- before\ntext:2: match last\ntext-3- "
        );
    }

    #[test]
    fn line_limit_counts_utf16_units_including_surrogate_boundaries() {
        let line = ContextLine::new(1, "🦀".repeat(251).as_bytes());
        assert!(line.truncated);
        assert_eq!(line.text, format!("{}... [truncated]", "🦀".repeat(250)));
        let line = ContextLine::new(1, format!("{}🦀", "x".repeat(499)).as_bytes());
        assert!(line.truncated);
        assert_eq!(line.text, format!("{}�... [truncated]", "x".repeat(499)));
    }

    #[test]
    fn byte_limited_output_still_counts_matches_and_long_lines() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            format!(
                "{}match {}\n",
                format!("match {}\n", "x".repeat(490)).repeat(110),
                "x".repeat(600)
            ),
        )
        .unwrap();
        let mut grep = Grep::new(&json!({"pattern":"match"}), 111).unwrap();
        assert!(
            !grep
                .search(file.path(), "text", &CancellationToken::new())
                .unwrap()
        );
        let output = grep.finish().tool_content();
        assert!(output.ends_with("[111 matches limit reached. Use limit=222 for more, or refine pattern. 50.0KB limit reached. Some lines truncated to 500 chars. Use read tool to see full lines]"));
        let body = output.split("\n\n").next().unwrap();
        assert_eq!(body.len(), 51098);
        assert_eq!(body.lines().count(), 101);
    }

    #[test]
    fn streams_large_files_and_preserves_the_last_matches_context() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            format!(
                "{}before\nTARGET\nafter\nend\n",
                "padding padding\n".repeat(400_000)
            ),
        )
        .unwrap();
        let mut grep = Grep::new(
            &json!({"pattern":"target","ignoreCase":true,"context":1}),
            1,
        )
        .unwrap();
        assert!(
            !grep
                .search(file.path(), "large.txt", &CancellationToken::new())
                .unwrap()
        );
        let output = grep.finish().tool_content();
        assert!(output.contains("large.txt-400001- before"));
        assert!(output.contains("large.txt:400002: TARGET"));
        assert!(output.contains("large.txt-400003- after"));
        assert!(!output.contains("-400004-"));
    }
    #[test]
    fn literal_matching_and_long_lines_are_bounded() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), format!("a.b{}\naxb\n", "界".repeat(2000))).unwrap();
        let mut grep = Grep::new(&json!({"pattern":"a.b","literal":true}), 100).unwrap();
        assert!(
            grep.search(file.path(), "text", &CancellationToken::new())
                .unwrap()
        );
        let output = grep.finish().tool_content();
        assert!(output.starts_with("text:1: a.b"));
        assert!(output.contains("Some lines truncated"));
        assert!(!output.contains("axb"));
        assert!(output.len() < 2000);
        let mut grep = Grep::new(&json!({"pattern":"nothing"}), 100).unwrap();
        grep.search(file.path(), "text", &CancellationToken::new())
            .unwrap();
        assert_eq!(grep.finish().tool_content(), "No matches found");
    }
}
