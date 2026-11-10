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
