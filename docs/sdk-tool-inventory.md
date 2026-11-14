# SDK tool inventory

`Registry::registered_tool_info` and `AgentSession::registered_tool_info` expose
the configured registry's tools with their schema, owning extension ID, the
extension's declared source path when available, and whether each tool is
selected for model advertisement. Request hooks can alter a particular request.
Results are ordered by tool name and include inactive tools. Inactive tools remain registered for nested calls, which are still
subject to host policy; the `active` flag is not an authorization decision.

The path is the value the extension reports through `command_source`; it may be
absent and does not by itself identify package, origin, or scope. This is a
bounded subset of pinned Pi's `getAllTools` source metadata. The session method
is a snapshot of its configured registry. A per-request `RegistryRefresher` can
select a different registry during a run. Subscribers to `AgentSession::subscribe`
receive `SessionEvent::ToolInventory` snapshots tagged with the run ID and phase:
`RequestPreparation` is the registry selected before request hooks, while
`Dispatch` is the registry selected after the dispatch-time refresh. A recovered
recorded response has no new request-preparation event. Request hooks can alter
the eventual model request; dispatch also checks recorded advertised tools and
host policies, so neither event is an authorization decision. Events are
transient, have the usual broadcast lag behavior, and can contain private source
paths. They are not an idle or current-state query. Existing `tools()` callers
continue to receive bare schemas.
