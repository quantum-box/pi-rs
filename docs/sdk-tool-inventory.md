# SDK tool inventory

`Registry::registered_tool_info` and `AgentSession::registered_tool_info` expose
the configured registry's tools with their schema, owning extension ID, the
extension's declared source path when available, and whether each tool is
selected for model advertisement. Request hooks can alter a particular request. Results are ordered by tool name and include inactive
tools. Inactive tools remain registered for nested calls, which are still
subject to host policy; the `active` flag is not an authorization decision.

The path is the value the extension reports through `command_source`; it may be
absent and does not by itself identify package, origin, or scope. This is a
bounded subset of pinned Pi's `getAllTools` source metadata. The session method
is a snapshot of its configured registry: a per-request `RegistryRefresher` can
supply a different runtime registry during a run, which this API does not
currently expose. Existing `tools()` callers continue to receive bare schemas.
