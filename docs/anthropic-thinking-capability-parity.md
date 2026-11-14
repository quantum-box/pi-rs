# Anthropic model thinking capabilities

Compared with Pi v1.5.0 (`f9a26365568127461c47a07a37dcb42c140a3ac5`):
a configured Anthropic Messages model with `reasoning: true` and no
`thinkingLevelMap` exposes `off`, `minimal`, `low`, `medium` and `high`.
The terminal and RPC share these levels, so startup and selection retain a
supported requested level and cycling follows the same order. Explicit maps
still determine available levels; models configured without reasoning expose
only `off`. This change does not alter provider-specific request serialization.

The regression uses configured models offline and exercises startup selection,
cycling, non-reasoning metadata, and an explicit map.
