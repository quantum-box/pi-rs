# Upstream system theme fixtures

`upstream-v1.5.0.json` records `generateSystemThemeColors` results from Pi
v1.5.0, commit `f9a26365568127461c47a07a37dcb42c140a3ac5`. Each entry
contains the exact input and the complete output, including all 56 color roles,
dim roles, and appearance. Inputs cover indexed fallbacks, saturation zero,
dark palette, background-only dark and light, unreadable foreground, mid-gray,
black/white extremes, pastel colors, and partial saturation. The generator test
compares every role, dim flag, and appearance against these reference outputs.

The fixture was generated using Node 24 from the upstream `system-theme.ts`,
`colors.ts`, and `oklab.ts` sources, redirecting the package import to the two
local color modules without changing their implementations. The Dracula and
Solarized cases are taken from upstream's `system-theme.test.ts`.

Regenerate with Node 24 or newer:

```sh
node crates/workbench/tests/fixtures/system-theme/generate.mjs /path/to/pi > crates/workbench/tests/fixtures/system-theme/upstream-v1.5.0.json
```
