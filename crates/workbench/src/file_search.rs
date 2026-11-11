//! Bounded, streaming grep with matching and context line markers.
use std::collections::VecDeque;
use std::io::{self, Read};
use std::path::Path;

use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{
    BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkFinish, SinkMatch,
};
use pi_rs::{CancellationToken, Error, Outcome, Result};
use serde_json::{Map, Value, json};

use super::tool_output::{self, Limit, MAX_BYTES, Truncation, UNLIMITED_LINES};

pub(super) struct Grep {
    matcher: RegexMatcher,
    searcher: Searcher,
    output: Output,
    context: usize,
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
        let context = args["context"].as_u64().unwrap_or(0).min(1000) as usize;
        Ok(Self {
            matcher,
            searcher: SearcherBuilder::new()
                .line_number(true)
                .before_context(context)
                .after_context(context)
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
        let result = self.searcher.search_reader(
            &self.matcher,
            file,
            FileSink {
                display,
                output: &mut self.output,
                context: self.context as u64,
                stop_after: None,
                lines: VecDeque::new(),
                pending: VecDeque::new(),
            },
        );
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        result?;
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
        if output.total_bytes > MAX_BYTES {
            details.insert(
                "truncation".into(),
                json!(Truncation {
                    content: &text,
                    truncated: true,
                    truncated_by: Some(Limit::Bytes),
                    total_lines: output.total_lines as u64,
                    total_bytes: output.total_bytes as u64,
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
    total_bytes: usize,
    total_lines: usize,
    matches: usize,
    limit: usize,
    long_lines: bool,
}
impl Output {
    fn line(&mut self, display: &str, line: &ContextLine, matched: bool) {
        self.long_lines |= line.truncated;
        let number = line.number;
        let text = &line.text;
        let line = if matched {
            format!("{display}:{number}: {text}")
        } else {
            format!("{display}-{number}- {text}")
        };
        self.total_bytes += line.len() + usize::from(self.total_lines > 0);
        self.total_lines += 1;
        if self.total_bytes <= MAX_BYTES {
            self.lines.push(line);
        }
    }
}

struct ContextLine {
    number: u64,
    text: String,
    truncated: bool,
    terminated: bool,
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
            terminated: bytes.ends_with(b"\n"),
        }
    }
}

struct FileSink<'a> {
    display: &'a str,
    output: &'a mut Output,
    context: u64,
    stop_after: Option<u64>,
    lines: VecDeque<ContextLine>,
    pending: VecDeque<u64>,
}
impl FileSink<'_> {
    fn flush(&mut self, through: u64) {
        while self
            .pending
            .front()
            .is_some_and(|center| center.saturating_add(self.context) <= through)
        {
            let center = self.pending.pop_front().expect("pending match");
            let start = center.saturating_sub(self.context);
            let end = center.saturating_add(self.context);
            for line in self
                .lines
                .iter()
                .filter(|line| line.number >= start && line.number <= end)
            {
                self.output.line(self.display, line, line.number == center);
            }
        }
    }

    fn accept(&mut self, number: u64, bytes: &[u8], matched: bool) -> bool {
        if self.stop_after.is_some_and(|last| number > last) {
            return false;
        }
        if matched && self.output.matches < self.output.limit {
            self.output.matches += 1;
            self.pending.push_back(number);
            if self.output.matches == self.output.limit {
                self.stop_after = Some(number.saturating_add(self.context));
            }
        }
        self.lines.push_back(ContextLine::new(number, bytes));
        self.flush(number);
        // Keep the preceding context for the next match and any unfinished block.
        let keep_from = self
            .pending
            .front()
            .copied()
            .unwrap_or(number)
            .saturating_sub(self.context);
        while self
            .lines
            .front()
            .is_some_and(|line| line.number < keep_from)
        {
            self.lines.pop_front();
        }
        self.stop_after.is_none_or(|last| number < last)
    }
}
impl Sink for FileSink<'_> {
    type Error = std::io::Error;
    fn matched(&mut self, _: &Searcher, found: &SinkMatch<'_>) -> std::io::Result<bool> {
        Ok(self.accept(
            found.line_number().expect("line numbers enabled"),
            found.bytes(),
            true,
        ))
    }
    fn context(&mut self, _: &Searcher, line: &SinkContext<'_>) -> std::io::Result<bool> {
        Ok(self.accept(
            line.line_number().expect("line numbers enabled"),
            line.bytes(),
            false,
        ))
    }
    fn finish(&mut self, _: &Searcher, _: &SinkFinish) -> std::io::Result<()> {
        // Pi's context view includes the final empty line after a trailing newline.
        if !self.pending.is_empty()
            && let Some(line) = self.lines.back().filter(|line| line.terminated)
        {
            self.lines
                .push_back(ContextLine::new(line.number.saturating_add(1), b""));
        }
        self.flush(u64::MAX);
        Ok(())
    }
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
