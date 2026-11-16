//! Opt-in bounded output for complete text and incremental streams.
//!
//! `bound_output` retains an exact raw slice; `OutputBuffer` also decodes byte
//! chunks and sanitizes its retained snapshot, matching upstream output rules.

/// Which end of the output to retain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retain {
    Head,
    Tail,
}

/// Independent UTF-8 byte and line limits. Either may be zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutputLimits {
    pub max_bytes: usize,
    pub max_lines: usize,
    pub retain: Retain,
}

/// A contiguous UTF-8 slice of the original output and its dropped counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoundedOutput<'a> {
    pub text: &'a str,
    pub bytes: usize,
    pub dropped_bytes: usize,
    pub dropped_lines: usize,
}

fn line_count(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }
    bytes.iter().filter(|&&b| b == b'\n').count() + usize::from(bytes.last() != Some(&b'\n'))
}

/// Keep a head or tail slice within both limits, preferring whole lines.
///
/// A trailing newline belongs to the preceding line. A single line exceeding
/// the byte limit is cut at a UTF-8 character boundary. Dropped lines are the
/// difference between the original and retained line counts, so dropping part
/// of a line does not necessarily increase `dropped_lines`.
///
/// The input must already be materialized; this function does not bound the
/// memory used to produce it and does not alter `StreamedToolAdapter`.
pub fn bound_output(text: &str, limits: OutputLimits) -> BoundedOutput<'_> {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let (start, end) = if limits.max_bytes == 0 || limits.max_lines == 0 {
        match limits.retain {
            Retain::Head => (0, 0),
            Retain::Tail => (len, len),
        }
    } else {
        match limits.retain {
            Retain::Head => {
                let mut end = bytes
                    .iter()
                    .enumerate()
                    .filter(|(_, b)| **b == b'\n')
                    .nth(limits.max_lines - 1)
                    .map_or(len, |(index, _)| index + 1);
                if end > limits.max_bytes {
                    end = match bytes[..limits.max_bytes].iter().rposition(|&b| b == b'\n') {
                        Some(index) => index + 1,
                        None => {
                            let mut end = limits.max_bytes;
                            while !text.is_char_boundary(end) {
                                end -= 1;
                            }
                            end
                        }
                    };
                }
                (0, end)
            }
            Retain::Tail => {
                let last = if bytes.last() == Some(&b'\n') {
                    len - 1
                } else {
                    len
                };
                let mut start = bytes[..last]
                    .iter()
                    .enumerate()
                    .rev()
                    .filter(|(_, b)| **b == b'\n')
                    .nth(limits.max_lines - 1)
                    .map_or(0, |(index, _)| index + 1);
                if len - start > limits.max_bytes {
                    let from = len - limits.max_bytes;
                    start = match bytes[from.saturating_sub(1)..]
                        .iter()
                        .position(|&b| b == b'\n')
                    {
                        Some(index) if from.saturating_sub(1) + index + 1 < len => {
                            from.saturating_sub(1) + index + 1
                        }
                        _ => {
                            let mut start = from;
                            while !text.is_char_boundary(start) {
                                start += 1;
                            }
                            start
                        }
                    };
                }
                (start, len)
            }
        }
    };
    let kept = &text[start..end];
    BoundedOutput {
        text: kept,
        bytes: kept.len(),
        dropped_bytes: len - kept.len(),
        dropped_lines: line_count(bytes) - line_count(kept.as_bytes()),
    }
}

/// A retained snapshot. Dropped counts refer to decoded text before sanitization.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutputSnapshot {
    pub text: String,
    pub dropped_bytes: usize,
    pub dropped_lines: usize,
}

/// Producer-supplied counts for decoded output omitted immediately before a
/// delivered chunk. The chunk and subsequent text must place the omitted text
/// entirely outside the tail window. Counts are measured before sanitization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputSkip {
    pub bytes: usize,
    pub newlines: usize,
    pub ends_with_newline: bool,
}

/// Persisted bounded-stream state for the current attempt. `finished` is the
/// window after flushing `pending_utf8` as a replacement character if needed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutputCheckpoint {
    pub version: u32,
    pub current: OutputSnapshot,
    pub finished: OutputSnapshot,
    pub pending_utf8: Vec<u8>,
}

#[derive(Clone)]
struct Chunk {
    text: String,
    newlines: usize,
}

/// Incremental bounded output with the slice and sanitization rules of upstream
/// `OutputBuffer`. Byte chunks use streaming UTF-8 replacement decoding.
/// Memory is bounded by the window plus the largest chunk since compaction.
#[derive(Clone)]
pub struct OutputBuffer {
    limits: OutputLimits,
    chunks: std::collections::VecDeque<Chunk>,
    stored_bytes: usize,
    stored_newlines: usize,
    total_bytes: usize,
    total_newlines: usize,
    ends_newline: bool,
    full: bool,
    pending: Vec<u8>,
    started: bool,
}

impl OutputBuffer {
    pub fn new(limits: OutputLimits) -> Self {
        Self {
            limits,
            chunks: Default::default(),
            stored_bytes: 0,
            stored_newlines: 0,
            total_bytes: 0,
            total_newlines: 0,
            ends_newline: true,
            full: false,
            pending: Vec::new(),
            started: false,
        }
    }

    pub fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    /// Append text, flushing an incomplete byte sequence as U+FFFD first.
    pub fn push_str(&mut self, text: &str) {
        self.flush_pending();
        if !text.is_empty() {
            self.started = true;
        }
        self.accept(text);
    }

    /// Append bytes, retaining an incomplete trailing UTF-8 sequence for the
    /// next byte chunk. A byte-order mark is stripped only at stream start.
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        let mut input = std::mem::take(&mut self.pending);
        input.extend_from_slice(bytes);
        let mut decoded = String::new();
        let mut remaining = input.as_slice();
        loop {
            match std::str::from_utf8(remaining) {
                Ok(text) => {
                    decoded.push_str(text);
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    decoded.push_str(
                        std::str::from_utf8(&remaining[..valid]).expect("valid UTF-8 prefix"),
                    );
                    match error.error_len() {
                        Some(length) => {
                            decoded.push('\u{fffd}');
                            remaining = &remaining[valid + length..];
                        }
                        None => {
                            self.pending.extend_from_slice(&remaining[valid..]);
                            break;
                        }
                    }
                }
            }
        }
        if !decoded.is_empty() {
            if !self.started && decoded.starts_with('\u{feff}') {
                decoded.drain(..'\u{feff}'.len_utf8());
            }
            self.started = true;
            self.accept(&decoded);
        }
    }

    /// Append text after producer-omitted decoded output. Only tail retention
    /// accepts skips; callers must ensure the delivered suffix exceeds the
    /// window, as required by upstream's producer skip contract.
    pub fn push_str_skipped(
        &mut self,
        text: &str,
        skipped: OutputSkip,
    ) -> Result<(), &'static str> {
        self.check_skip(skipped)?;
        self.flush_pending();
        self.skip(skipped);
        self.started = true;
        self.accept(text);
        Ok(())
    }

    /// Like `push_str_skipped`, but the delivered chunk is a UTF-8 byte stream.
    pub fn push_bytes_skipped(
        &mut self,
        bytes: &[u8],
        skipped: OutputSkip,
    ) -> Result<(), &'static str> {
        self.check_skip(skipped)?;
        self.flush_pending();
        self.skip(skipped);
        self.started = true;
        self.push_bytes(bytes);
        Ok(())
    }

    fn check_skip(&self, skipped: OutputSkip) -> Result<(), &'static str> {
        if self.limits.retain != Retain::Tail {
            return Err("skipped output requires tail retention");
        }
        if skipped.newlines > skipped.bytes || (skipped.ends_with_newline && skipped.newlines == 0)
        {
            return Err("invalid skipped output counts");
        }
        Ok(())
    }

    fn skip(&mut self, skipped: OutputSkip) {
        if skipped.bytes == 0 {
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(skipped.bytes);
        self.total_newlines = self.total_newlines.saturating_add(skipped.newlines);
        self.ends_newline = skipped.ends_with_newline;
        self.chunks.clear();
        self.stored_bytes = 0;
        self.stored_newlines = 0;
    }

    /// Capture the running and end-of-stream views and pending UTF-8 bytes.
    pub fn checkpoint(&self) -> OutputCheckpoint {
        let mut current = self.clone();
        let running = current.snapshot();
        let pending_utf8 = current.pending.clone();
        current.end();
        OutputCheckpoint {
            version: 1,
            current: running,
            finished: current.snapshot(),
            pending_utf8,
        }
    }

    /// Finish a byte stream, replacing an incomplete final sequence.
    pub fn end(&mut self) {
        self.flush_pending();
    }

    fn flush_pending(&mut self) {
        if !self.pending.is_empty() {
            self.pending.clear();
            self.started = true;
            self.accept("\u{fffd}");
        }
    }

    fn accept(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        let newlines = text.bytes().filter(|&b| b == b'\n').count();
        self.total_bytes = self.total_bytes.saturating_add(text.len());
        self.total_newlines = self.total_newlines.saturating_add(newlines);
        self.ends_newline = text.ends_with('\n');
        if self.full {
            return;
        }
        self.stored_bytes += text.len();
        self.stored_newlines += newlines;
        self.chunks.push_back(Chunk {
            text: text.to_owned(),
            newlines,
        });
        if self.limits.retain == Retain::Head {
            self.full = self.stored_bytes > self.limits.max_bytes
                || self.stored_newlines >= self.limits.max_lines;
        } else {
            while self.chunks.len() > 1 {
                let first = self.chunks.front().expect("nonempty");
                let bytes = self.stored_bytes - first.text.len();
                let lines = self.stored_newlines - first.newlines;
                if bytes <= self.limits.max_bytes.saturating_add(1)
                    && lines <= self.limits.max_lines.saturating_add(1)
                {
                    break;
                }
                self.chunks.pop_front();
                self.stored_bytes = bytes;
                self.stored_newlines = lines;
            }
        }
    }

    /// Return the current sanitized window. Incomplete UTF-8 is not included
    /// until completed, flushed by a text chunk, or ended.
    pub fn snapshot(&mut self) -> OutputSnapshot {
        let stored: String = self.chunks.iter().map(|c| c.text.as_str()).collect();
        let kept = bound_output(&stored, self.limits);
        let kept_lines = line_count(kept.text.as_bytes());
        let snapshot = OutputSnapshot {
            text: sanitize_output(kept.text),
            dropped_bytes: self.total_bytes.saturating_sub(kept.bytes),
            dropped_lines: (self.total_newlines + usize::from(!self.ends_newline))
                .saturating_sub(kept_lines),
        };
        if self.limits.retain == Retain::Tail {
            let mut byte_start = if stored.len() > self.limits.max_bytes {
                stored.len() - self.limits.max_bytes - 1
            } else {
                0
            };
            while !stored.is_char_boundary(byte_start) {
                byte_start -= 1;
            }
            let line_start = stored
                .bytes()
                .enumerate()
                .rev()
                .filter(|(_, b)| *b == b'\n')
                .nth(self.limits.max_lines)
                .map_or(0, |(index, _)| index);
            let margin = &stored[byte_start.max(line_start)..];
            self.chunks.clear();
            self.stored_bytes = margin.len();
            self.stored_newlines = margin.bytes().filter(|&b| b == b'\n').count();
            if !margin.is_empty() {
                self.chunks.push_back(Chunk {
                    text: margin.to_owned(),
                    newlines: self.stored_newlines,
                });
            }
        } else if self.chunks.len() > 1 {
            self.chunks.clear();
            self.chunks.push_back(Chunk {
                text: stored,
                newlines: self.stored_newlines,
            });
        }
        snapshot
    }
}

/// Remove the terminal sequences and controls stripped by upstream output buffers.
pub fn sanitize_output(text: &str) -> String {
    use std::sync::OnceLock;
    static ESCAPES: OnceLock<regex::Regex> = OnceLock::new();
    let escapes = ESCAPES.get_or_init(|| regex::Regex::new(
        r"\x1b[\]PX^_][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]|\x1b[\x20-\x2f]*[\x30-\x7e]"
    ).expect("valid terminal escape expression"));
    escapes
        .replace_all(text, "")
        .chars()
        .filter(|c| {
            !matches!(*c,
                '\u{0}'..='\u{8}' | '\u{b}'..='\u{1f}' | '\u{fff9}'..='\u{fffb}'
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(
        text: &str,
        retain: Retain,
        lines: usize,
        bytes: usize,
        expected: (&str, usize, usize),
    ) {
        let result = bound_output(
            text,
            OutputLimits {
                max_bytes: bytes,
                max_lines: lines,
                retain,
            },
        );
        assert_eq!(
            (result.text, result.dropped_bytes, result.dropped_lines),
            expected
        );
        assert_eq!(result.bytes, result.text.len());
    }

    #[test]
    fn skipped_tail_matches_full_stream_and_head_rejects_without_mutation() {
        let limits = OutputLimits {
            max_bytes: 1000,
            max_lines: 2,
            retain: Retain::Tail,
        };
        let mut full = OutputBuffer::new(limits);
        let mut skipped = OutputBuffer::new(limits);
        full.push_str("one\ntwo\nthree\nfour\nfive\nsix");
        skipped
            .push_str_skipped(
                "ee\nfour\nfive\nsix",
                OutputSkip {
                    bytes: 11,
                    newlines: 2,
                    ends_with_newline: false,
                },
            )
            .unwrap();
        assert_eq!(skipped.snapshot(), full.snapshot());
        assert_eq!(
            skipped.snapshot(),
            OutputSnapshot {
                text: "five\nsix".into(),
                dropped_bytes: 19,
                dropped_lines: 4
            }
        );
        let mut head = OutputBuffer::new(OutputLimits {
            retain: Retain::Head,
            ..limits
        });
        head.push_bytes(&[0xe2]);
        assert!(
            head.push_str_skipped(
                "bad",
                OutputSkip {
                    bytes: 3,
                    newlines: 1,
                    ends_with_newline: true
                }
            )
            .is_err()
        );
        head.push_bytes(&[0x82, 0xac]);
        assert_eq!(head.snapshot().text, "€");
    }

    #[test]
    fn skip_flushes_pending_and_preserves_producer_counts() {
        let mut b = OutputBuffer::new(OutputLimits {
            max_bytes: 5,
            max_lines: 1,
            retain: Retain::Tail,
        });
        b.push_bytes(&[0xe2]);
        b.push_bytes_skipped(
            b"final!",
            OutputSkip {
                bytes: 4,
                newlines: 1,
                ends_with_newline: true,
            },
        )
        .unwrap();
        assert_eq!(
            b.snapshot(),
            OutputSnapshot {
                text: "inal!".into(),
                dropped_bytes: 8,
                dropped_lines: 1
            }
        );
        assert!(
            b.push_bytes_skipped(
                b"x",
                OutputSkip {
                    bytes: 0,
                    newlines: 1,
                    ends_with_newline: true
                }
            )
            .is_err()
        );
        assert_eq!(b.snapshot().text, "inal!");
    }

    #[test]
    fn streaming_utf8_bom_invalid_bytes_and_sanitization() {
        let mut b = OutputBuffer::new(OutputLimits {
            max_bytes: 100,
            max_lines: 10,
            retain: Retain::Tail,
        });
        b.push_bytes(&[0xef]);
        b.push_bytes(&[0xbb, 0xbf, b'a', 0xf0, 0x9f]);
        b.push_bytes(&[0x99, 0x82, b'\n']);
        b.push_bytes(&[0xe2]);
        b.push_str("x");
        b.push_bytes(&[0xe2, 0x82]);
        b.end();
        assert_eq!(b.snapshot().text, "a🙂\n\u{fffd}x\u{fffd}");
        let mut b = OutputBuffer::new(OutputLimits {
            max_bytes: 100,
            max_lines: 10,
            retain: Retain::Head,
        });
        b.push_str("\u{feff}\x1b[3");
        b.push_str("1mERROR\x1b[0m\n\x07");
        let out = b.snapshot();
        assert_eq!(out.text, "\u{feff}ERROR\n");
        assert_eq!(out.dropped_bytes, 0);
    }

    #[test]
    fn streaming_matches_whole_output_across_chunks_and_snapshots() {
        for retain in [Retain::Head, Retain::Tail] {
            for max_bytes in 0..=16 {
                for max_lines in 0..=4 {
                    let limits = OutputLimits {
                        max_bytes,
                        max_lines,
                        retain,
                    };
                    let mut b = OutputBuffer::new(limits);
                    let mut whole = String::new();
                    for i in 0..150 {
                        let chunk = match i % 5 {
                            0 => "é",
                            1 => "x\n",
                            2 => "🙂\n",
                            3 => "\n",
                            _ => "a",
                        };
                        whole.push_str(chunk);
                        let bytes = chunk.as_bytes();
                        for byte in bytes {
                            b.push_bytes(std::slice::from_ref(byte));
                        }
                        if i % 3 == 0 {
                            let expected = bound_output(&whole, limits);
                            let actual = b.snapshot();
                            assert_eq!(
                                (
                                    actual.text.as_str(),
                                    actual.dropped_bytes,
                                    actual.dropped_lines
                                ),
                                (
                                    expected.text,
                                    expected.dropped_bytes,
                                    expected.dropped_lines
                                ),
                                "{limits:?} i={i}"
                            );
                        }
                    }
                    b.end();
                    let expected = bound_output(&whole, limits);
                    let actual = b.snapshot();
                    assert_eq!(
                        (
                            actual.text.as_str(),
                            actual.dropped_bytes,
                            actual.dropped_lines
                        ),
                        (
                            expected.text,
                            expected.dropped_bytes,
                            expected.dropped_lines
                        ),
                        "{limits:?}"
                    );
                    assert!(b.stored_bytes() <= max_bytes.saturating_add(8).max(8));
                }
            }
        }
    }

    #[test]
    fn upstream_bounds_and_counting_cases() {
        use Retain::{Head, Tail};
        check("a\nb\n", Head, 2, 1000, ("a\nb\n", 0, 0));
        check("a\nb", Tail, 2, 1000, ("a\nb", 0, 0));
        check("", Tail, 2, 1000, ("", 0, 0));
        check("ab\ncd\n", Head, 10, 0, ("", 6, 2));
        check("ab\ncd\n", Tail, 0, 1000, ("", 6, 2));
        check("a\nb\nc\n", Head, 2, 1000, ("a\nb\n", 2, 1));
        check("a\nb\nc\n", Tail, 2, 1000, ("b\nc\n", 2, 1));
        check("a\nb\nc", Tail, 2, 1000, ("b\nc", 2, 1));
        check("a\nb\nc\n\n", Tail, 3, 1000, ("b\nc\n\n", 2, 1));
        check("aa\nbb\ncc\n", Head, 10, 7, ("aa\nbb\n", 3, 1));
        check("aa\nbb\ncc\n", Tail, 10, 7, ("bb\ncc\n", 3, 1));
        check("ééé\n", Head, 10, 5, ("éé", 3, 0));
        check("x\néééé", Tail, 10, 5, ("éé", 6, 1));
        check("x\u{feff}a", Tail, 10, 4, ("\u{feff}a", 1, 0));
    }

    #[test]
    fn small_multiline_inputs_respect_limits_and_dropped_totals() {
        fn visit(text: &mut String, depth: usize) {
            for max_lines in 0..=3 {
                for max_bytes in 0..=text.len() + 1 {
                    for retain in [Retain::Head, Retain::Tail] {
                        let out = bound_output(
                            text,
                            OutputLimits {
                                max_bytes,
                                max_lines,
                                retain,
                            },
                        );
                        assert!(out.bytes <= max_bytes, "{text:?} {out:?}");
                        assert!(
                            line_count(out.text.as_bytes()) <= max_lines,
                            "{text:?} {out:?}"
                        );
                        assert_eq!(out.bytes + out.dropped_bytes, text.len());
                        assert_eq!(
                            out.dropped_lines + line_count(out.text.as_bytes()),
                            line_count(text.as_bytes())
                        );
                        assert!(match retain {
                            Retain::Head => text.starts_with(out.text),
                            Retain::Tail => text.ends_with(out.text),
                        });
                    }
                }
            }
            if depth == 0 {
                return;
            }
            for ch in ['a', '\n', 'é', '🙂'] {
                text.push(ch);
                visit(text, depth - 1);
                text.pop();
            }
        }
        visit(&mut String::new(), 4);
    }

    #[test]
    fn utf8_single_line_boundary_for_every_byte_limit() {
        for text in ["é中🙂", "👩‍💻", "a\u{feff}b", ""] {
            for max_bytes in 0..=text.len() + 1 {
                for retain in [Retain::Head, Retain::Tail] {
                    let out = bound_output(
                        text,
                        OutputLimits {
                            max_bytes,
                            max_lines: 1,
                            retain,
                        },
                    );
                    assert!(out.bytes <= max_bytes);
                    assert_eq!(out.bytes + out.dropped_bytes, text.len());
                    let expected = match retain {
                        Retain::Head => text
                            .char_indices()
                            .map(|(i, _)| i)
                            .chain([text.len()])
                            .filter(|&i| i <= max_bytes)
                            .max()
                            .unwrap(),
                        Retain::Tail => text
                            .char_indices()
                            .map(|(i, _)| i)
                            .chain([text.len()])
                            .find(|&i| text.len() - i <= max_bytes)
                            .unwrap(),
                    };
                    assert_eq!(
                        out.text,
                        match retain {
                            Retain::Head => &text[..expected],
                            Retain::Tail => &text[expected..],
                        }
                    );
                }
            }
        }
    }
}
