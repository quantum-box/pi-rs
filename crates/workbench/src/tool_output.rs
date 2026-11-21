//! Pi's native tool truncation metadata and shared display limits.
use serde::Serialize;

pub(super) const MAX_BYTES: usize = 50 * 1024;
pub(super) const MAX_LINES: usize = 2000;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Limit {
    Lines,
    Bytes,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Truncation<'a> {
    pub content: &'a str,
    pub truncated: bool,
    pub truncated_by: Option<Limit>,
    pub total_lines: u64,
    pub total_bytes: u64,
    pub output_lines: usize,
    pub output_bytes: usize,
    pub last_line_partial: bool,
    pub first_line_exceeds_limit: bool,
    pub max_lines: u64,
    pub max_bytes: usize,
}

// JavaScript's shortest round-trip spelling for numeric tool arguments.
pub(super) fn js_number(value: f64) -> String {
    ryu_js::Buffer::new().format(value).to_owned()
}

pub(super) fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes}B");
    }
    let (scale, unit) = if bytes < 1024 * 1024 {
        (1024, "KB")
    } else {
        (1024 * 1024, "MB")
    };
    let tenths = (u128::from(bytes) * 10 + scale / 2) / scale;
    format!("{}.{}{unit}", tenths / 10, tenths % 10)
}

pub(super) const UNLIMITED_LINES: u64 = 9_007_199_254_740_991;

pub(super) fn finish(
    mut output: String,
    details: serde_json::Map<String, serde_json::Value>,
    notices: &[String],
) -> pi_rs::Outcome {
    if !notices.is_empty() {
        output.push_str("\n\n[");
        output.push_str(&notices.join(". "));
        output.push(']');
    }
    if details.is_empty() {
        pi_rs::Outcome::Succeeded(output)
    } else {
        pi_rs::Outcome::SucceededWithDetails {
            output,
            details: serde_json::Value::Object(details),
        }
    }
}

pub(super) fn listing(
    mut output: String,
    mut details: serde_json::Map<String, serde_json::Value>,
    mut notices: Vec<String>,
) -> pi_rs::Outcome {
    if output.len() > MAX_BYTES {
        let total_lines = output.split_terminator('\n').count();
        let total_bytes = output.len();
        let end = output.as_bytes()[..=MAX_BYTES]
            .iter()
            .rposition(|byte| *byte == b'\n');
        let first_line_exceeds_limit = end.is_none();
        output.truncate(end.unwrap_or(0));
        let output_lines = end.map_or(0, |_| {
            output.bytes().filter(|byte| *byte == b'\n').count() + 1
        });
        details.insert(
            "truncation".into(),
            serde_json::json!(Truncation {
                content: &output,
                truncated: true,
                truncated_by: Some(Limit::Bytes),
                total_lines: total_lines as u64,
                total_bytes: total_bytes as u64,
                output_lines,
                output_bytes: output.len(),
                last_line_partial: false,
                first_line_exceeds_limit,
                max_lines: UNLIMITED_LINES,
                max_bytes: MAX_BYTES,
            }),
        );
        notices.push("50.0KB limit reached".into());
    }
    finish(output, details, &notices)
}

/// Incremental listing formatter: retain only the prefix needed for display,
/// while counting the complete joined output for truncation metadata.
pub(super) struct Listing {
    prefix: Vec<u8>,
    total_bytes: u64,
    newlines: u64,
    ends_in_newline: bool,
    has_entries: bool,
}

impl Listing {
    pub fn new() -> Self {
        Self {
            prefix: Vec::new(),
            total_bytes: 0,
            newlines: 0,
            ends_in_newline: false,
            has_entries: false,
        }
    }

    pub fn push(&mut self, entry: &str) {
        if self.has_entries {
            self.append(b"\n");
        }
        self.append(entry.as_bytes());
        self.has_entries = true;
    }

    fn append(&mut self, bytes: &[u8]) {
        self.total_bytes = self.total_bytes.saturating_add(bytes.len() as u64);
        self.newlines = self
            .newlines
            .saturating_add(bytes.iter().filter(|&&b| b == b'\n').count() as u64);
        if let Some(last) = bytes.last() {
            self.ends_in_newline = *last == b'\n';
        }
        let keep = (MAX_BYTES + 1)
            .saturating_sub(self.prefix.len())
            .min(bytes.len());
        self.prefix.extend_from_slice(&bytes[..keep]);
    }

    pub fn finish(
        mut self,
        mut details: serde_json::Map<String, serde_json::Value>,
        mut notices: Vec<String>,
    ) -> pi_rs::Outcome {
        if self.total_bytes > MAX_BYTES as u64 {
            let end = self.prefix.iter().rposition(|&b| b == b'\n');
            self.prefix.truncate(end.unwrap_or(0));
            let output_lines = end.map_or(0, |_| {
                self.prefix.iter().filter(|&&b| b == b'\n').count() + 1
            });
            let output = String::from_utf8(self.prefix).expect("prefix ends at a newline");
            details.insert(
                "truncation".into(),
                serde_json::json!(Truncation {
                    content: &output,
                    truncated: true,
                    truncated_by: Some(Limit::Bytes),
                    total_lines: self
                        .newlines
                        .saturating_add(u64::from(!self.ends_in_newline)),
                    total_bytes: self.total_bytes,
                    output_lines,
                    output_bytes: output.len(),
                    last_line_partial: false,
                    first_line_exceeds_limit: end.is_none(),
                    max_lines: UNLIMITED_LINES,
                    max_bytes: MAX_BYTES,
                }),
            );
            notices.push("50.0KB limit reached".into());
            finish(output, details, &notices)
        } else {
            finish(
                String::from_utf8(self.prefix).expect("complete UTF-8"),
                details,
                &notices,
            )
        }
    }
}

#[cfg(test)]
mod listing_tests {
    use super::*;

    fn parts(outcome: pi_rs::Outcome) -> (String, Option<serde_json::Value>) {
        match outcome {
            pi_rs::Outcome::Succeeded(output) => (output, None),
            pi_rs::Outcome::SucceededWithDetails { output, details } => (output, Some(details)),
            _ => panic!("unexpected outcome"),
        }
    }

    #[test]
    fn incremental_matches_complete_listing_for_boundaries_and_newlines() {
        for values in [
            vec!["a".repeat(MAX_BYTES)],
            vec!["a".repeat(MAX_BYTES + 1)],
            vec![format!("{}\n", "x".repeat(MAX_BYTES)), "z".into()],
            vec!["α\nβ".into(), "".into(), "x".repeat(MAX_BYTES)],
            vec!["item".into(); 100_001],
        ] {
            let mut incremental = Listing::new();
            for value in &values {
                incremental.push(value);
            }
            assert!(incremental.prefix.len() <= MAX_BYTES + 1);
            assert_eq!(
                parts(incremental.finish(Default::default(), vec![])),
                parts(listing(values.join("\n"), Default::default(), vec![]))
            );
        }
    }

    #[test]
    fn incremental_totals_cross_32_bit_boundary_and_saturate() {
        let mut listing = Listing::new();
        listing.total_bytes = u64::from(u32::MAX) - 1;
        listing.newlines = u64::from(u32::MAX) - 1;
        listing.append(b"\n\nx");
        let (_, details) = parts(listing.finish(Default::default(), vec![]));
        let truncation = &details.unwrap()["truncation"];
        assert_eq!(truncation["totalBytes"], u64::from(u32::MAX) + 2);
        assert_eq!(truncation["totalLines"], u64::from(u32::MAX) + 2);

        let mut listing = Listing::new();
        listing.total_bytes = u64::MAX - 1;
        listing.newlines = u64::MAX;
        listing.append(b"xx");
        let (_, details) = parts(listing.finish(Default::default(), vec![]));
        let truncation = &details.unwrap()["truncation"];
        assert_eq!(truncation["totalBytes"], u64::MAX);
        assert_eq!(truncation["totalLines"], u64::MAX);
    }
}
