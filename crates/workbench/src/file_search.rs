//! Bounded, streaming grep with matching and context line markers.
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use encoding_rs::{CoderResult, UTF_8};

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
            render_context(
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
    cancellation: &CancellationToken,
) -> Result<(ContextLine, bool, bool)> {
    let mut decoder = UTF_8.new_decoder_without_bom_handling();
    let mut decoded = [0u8; 8192];
    let mut units = Vec::with_capacity(501);
    let mut has_bytes = false;
    loop {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let buf = reader.fill_buf()?;
        let newline = buf.iter().position(|&byte| byte == b'\n');
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
        let (line, has_bytes, newline) = read_context_line(&mut reader, number, cancellation)?;
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
            let (line, _, _) = read_context_line(&mut reader, number, cancellation)?;
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

    #[test]
    fn context_decoding_skips_arbitrarily_long_carriage_return_runs() {
        let input = format!("{}visible\n", "\r".repeat(100_000));
        let mut reader = BufReader::new(input.as_bytes());
        let (line, _, _) = read_context_line(&mut reader, 1, &CancellationToken::new()).unwrap();
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
