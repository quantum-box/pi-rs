//! Portable tool-call argument parsing and incremental previews.
//!
//! The parser does not perform tool execution or provide a browser runtime.
mod complete;
mod json_diagnostic;
mod streaming;

pub use complete::parse_tool_arguments;
pub use json_diagnostic::{JSON_DIAGNOSTIC_RETRY_LIMIT, json_syntax_diagnostic};
pub use streaming::StreamingJson;
