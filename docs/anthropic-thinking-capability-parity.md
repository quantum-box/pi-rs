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

Configured reasoning metadata also drives the Messages request when a Models
API is absent or restricted. Budgeted thinking is the default unless adaptive
thinking is explicitly selected. For adaptive thinking, an unmapped `minimal`
level uses Pi's `low` effort; an explicit `thinkingLevelMap` still wins.
The loopback fixture checks budgeted and adaptive request bodies without a
working Models API, including an explicit mapping.

When model discovery reports a smaller output limit, the request still respects
that limit while configured thinking takes precedence over discovered thinking
capabilities. Budgeted thinking uses the selected Pi level's budget; effort
maps affect adaptive thinking. Loopback coverage includes a 4,096-token model
limit and distinct mapped budgeted and adaptive cases.
