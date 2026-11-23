# Markdown oracle fixtures

`pi-v150-trailing.json` records visible lines returned at width 80 by Pi
v1.5.0 (`f9a26365568127461c47a07a37dcb42c140a3ac5`), using identity theme
functions and trimming trailing terminal padding. The cases combine block
kinds with line endings and link reference definitions; they also include
quote and list boundaries, definitions before and between visible blocks,
and blank lines in unclosed code fences. List cases include multiple item
blocks, nested lists, ordered and task markers, definitions, quotes, and
closed and unclosed fences. The fixture captures textual layout and top-level
token type/raw boundaries, not ANSI styling or terminal output.

To regenerate, install the pinned Pi checkout's Node dependencies and run
`node generate-pi-v150-trailing.mjs /path/to/pi` from this directory. The
script rejects a checkout at a different commit.
It exports the private parser from a temporary source copy, leaving the Pi
checkout unchanged.
