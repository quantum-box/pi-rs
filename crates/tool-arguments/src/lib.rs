//! Portable tool-call argument parsing and incremental previews.
//!
//! The parser does not perform tool execution or provide a browser runtime.
mod complete;
mod streaming;

pub use complete::parse_tool_arguments;
pub use streaming::StreamingJson;
