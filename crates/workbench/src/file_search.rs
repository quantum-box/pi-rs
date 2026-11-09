//! Bounded, streaming grep with matching and context line markers.
use std::path::Path;

use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkMatch};
use pi_rs::{Error, Result};
use serde_json::Value;

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
    pub fn search(&mut self, path: &Path, display: &str) -> Result<bool> {
        self.searcher.search_path(
            &self.matcher,
            path,
            FileSink {
                display,
                output: &mut self.output,
                context: self.context as u64,
                stop_after: None,
            },
        )?;
        Ok(self.output.matches < self.output.limit && !self.output.byte_limit)
    }
    pub fn finish(self) -> String {
        if self.output.matches == 0 && self.output.lines.is_empty() && !self.output.byte_limit {
            return "No matches found".into();
        }
        let mut text = self.output.lines.join("\n");
        let mut notices = Vec::new();
        if self.output.matches == self.output.limit {
            notices.push(format!(
                "{} matches limit reached. Use limit={} for more, or refine pattern",
                self.output.limit,
                self.output.limit.saturating_mul(2)
            ));
        }
        if self.output.byte_limit {
            notices.push("50 KiB limit reached".into());
        }
        if self.output.long_lines {
            notices.push("Some lines truncated to 500 chars. Use read to see full lines".into());
        }
        if !notices.is_empty() {
            text.push_str(&format!("\n\n[{}]", notices.join(". ")));
        }
        text
    }
}

#[derive(Default)]
struct Output {
    lines: Vec<String>,
    bytes: usize,
    matches: usize,
    limit: usize,
    byte_limit: bool,
    long_lines: bool,
}
impl Output {
    fn line(&mut self, display: &str, number: u64, bytes: &[u8], matched: bool) -> bool {
        let text = String::from_utf8_lossy(bytes).replace('\r', "");
        let text = text.trim_end_matches('\n');
        let mut characters = text.chars();
        let mut text: String = characters.by_ref().take(500).collect();
        if characters.next().is_some() {
            self.long_lines = true;
            text.push_str("... [truncated]");
        }
        let line = if matched {
            format!("{display}:{number}: {text}")
        } else {
            format!("{display}-{number}- {text}")
        };
        let added = line.len() + usize::from(!self.lines.is_empty());
        if self.bytes + added > 50 * 1024 {
            self.byte_limit = true;
            return false;
        }
        self.bytes += added;
        self.lines.push(line);
        true
    }
}
struct FileSink<'a> {
    display: &'a str,
    output: &'a mut Output,
    context: u64,
    stop_after: Option<u64>,
}
impl Sink for FileSink<'_> {
    type Error = std::io::Error;
    fn matched(&mut self, _: &Searcher, found: &SinkMatch<'_>) -> std::io::Result<bool> {
        let number = found.line_number().expect("line numbers enabled");
        let matched = self.output.matches < self.output.limit;
        if matched {
            self.output.matches += 1;
            if self.output.matches == self.output.limit {
                self.stop_after = Some(number.saturating_add(self.context));
            }
        }
        if self.stop_after.is_some_and(|last| number > last) {
            return Ok(false);
        }
        Ok(self
            .output
            .line(self.display, number, found.bytes(), matched)
            && self.stop_after.is_none_or(|last| number < last))
    }
    fn context(&mut self, _: &Searcher, line: &SinkContext<'_>) -> std::io::Result<bool> {
        let number = line.line_number().expect("line numbers enabled");
        if self.stop_after.is_some_and(|last| number > last) {
            return Ok(false);
        }
        Ok(self.output.line(self.display, number, line.bytes(), false)
            && self.stop_after.is_none_or(|last| number < last))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
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
        assert!(!grep.search(file.path(), "large.txt").unwrap());
        let output = grep.finish();
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
        assert!(grep.search(file.path(), "text").unwrap());
        let output = grep.finish();
        assert!(output.starts_with("text:1: a.b"));
        assert!(output.contains("Some lines truncated"));
        assert!(!output.contains("axb"));
        assert!(output.len() < 2000);
        let mut grep = Grep::new(&json!({"pattern":"nothing"}), 100).unwrap();
        grep.search(file.path(), "text").unwrap();
        assert_eq!(grep.finish(), "No matches found");
    }
}
