# Thinking-level cycle

A model explicitly marked as reasoning-capable can have a thinking-level map
that disables every level except `off`. Cycling that model returns `off`, as in
Pi; it does not report that the model lacks thinking support. If `off` is
already selected, cycling does not write a redundant selection or emit a
selection hook. A model without known reasoning capability and only `off`
retains the native unsupported result. An empty capability list is handled
without indexing it.

An unspecified native thinking level remains distinct from an explicit `off`
selection for provider request and journal purposes.
