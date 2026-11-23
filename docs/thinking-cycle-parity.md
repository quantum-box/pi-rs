# Thinking-level cycle

A model explicitly marked as reasoning-capable can have a thinking-level map
that disables every level except `off`. Cycling that model returns `off`, as in
Pi; it does not report that the model lacks thinking support. If `off` is
already selected, cycling does not write a redundant selection or emit a
selection hook. A model without known reasoning capability and only `off`
retains the native unsupported result. An empty capability list is handled
without indexing it.

When model discovery supplies an explicit reasoning capability, that adopted
value takes precedence over configured metadata when cycling. In particular,
an Anthropic Models API response with `thinking.supported: false` and only
`off` does not become a successful cycle merely because configuration says the
model supports reasoning. The selected capability travels with levels loaded
for the settings picker as well.

An unspecified native thinking level remains distinct from an explicit `off`
selection for provider request and journal purposes.
