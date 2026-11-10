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
