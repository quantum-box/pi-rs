//! Opt-in limits for complete UTF-8 tool output.
//!
//! This implements the slice rules of upstream `boundOutput`. It does not
//! accumulate a stream or sanitize terminal control sequences.

/// Which end of the output to retain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retain {
    Head,
    Tail,
}

/// Independent UTF-8 byte and line limits. Either may be zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
