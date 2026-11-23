# Grep single-line patterns

Pi's local grep runs ripgrep without multiline mode. A pattern that requires a
line-feed character, including a literal pasted newline or `\n` in a regex,
returns ripgrep's multiline-mode diagnostic instead of reporting no matches.
Native local grep applies the same line-terminator restriction. Character
classes that contain other matchable characters continue to work.

This checkpoint covers local grep; injected remote grep delegates matching to
its backend.

Unrestricted local grep accepts numeric match limits like Pi: the effective
limit is at least 1, and a fractional limit stops at the next whole match.
Notices and details retain the effective JavaScript Number value; large JSON
numbers are rounded to binary64. Scoped and injected remote grep retain their
own limit validation. Context matches continue to spool to temporary files to
bound memory usage.
