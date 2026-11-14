# Upstream pi compatibility

- Treat the upstream pi coding agent at https://github.com/earendil-works/pi as
  the reference for user-facing behavior, including the TUI, commands, and
  configuration. Prefer matching its current behavior when implementing or
  changing corresponding features.
- Before changing an upstream-compatible feature, check the relevant upstream
  implementation or documentation instead of assuming how it works. For TUI
  changes, compare the actual message rendering and theme behavior as well.
- Preserve intentional pi-rs extensions and documented differences. When
  matching upstream would conflict with an explicit user request or an existing
  pi-rs requirement, follow that requirement and explain the difference.
- Cover compatibility changes with relevant tests and update the parity
  documentation when the supported behavior or an intentional difference changes.
