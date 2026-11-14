# Local shell output compatibility

The local and injected shell tools follow the pinned Pi v1.5.0 streaming output
accumulator when a retained tail starts inside a line. If its first newline is
also the end of the retained tail, the incomplete line is discarded and the
snapshot is empty. Truncation metadata still reports the complete decoded byte
and line counts, and the full transcript remains available at the reported path.
A successful result uses Pi's `(no output)` placeholder before the truncation
notice; timeout and cancellation notices retain their empty output prefix.

This differs from the case where the retained tail has no newline: an oversized
unterminated final line displays its UTF-8-safe suffix. Output below the rolling
buffer threshold can also retain the suffix of a newline-terminated long line,
matching the upstream streaming accumulator's buffer-boundary behavior.
When the retained snapshot fits both limits, its contents are preserved
verbatim, including trailing newlines and their byte counts.
