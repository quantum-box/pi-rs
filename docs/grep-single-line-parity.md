# Grep single-line patterns

Pi's local grep runs ripgrep without multiline mode. A pattern that requires a
line-feed character, including a literal pasted newline or `\n` in a regex,
returns ripgrep's multiline-mode diagnostic instead of reporting no matches.
Native local grep applies the same line-terminator restriction. Character
classes that contain other matchable characters continue to work.

This checkpoint covers local grep; injected remote grep delegates matching to
its backend.
