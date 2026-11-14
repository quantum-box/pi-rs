# Extension command provenance

`Registry::registered_commands` reports whether each command's owner is an
in-process built-in, an inline extension, or a file-backed extension. In-process
extensions default to inline; host-supplied built-ins opt in through
`Extension::source_is_builtin`. A declared `command_source` path takes
precedence and produces file-backed provenance.

The RPC `get_commands` catalog uses Pi-style `builtin:<id>` and `<inline:<id>>`
synthetic paths for the first two cases. Inline sources use temporary scope;
built-in scope retains the host's existing user default. File-backed commands
continue to use their declared source path and the host's existing scope and
package heuristics. This does not yet provide full package source metadata for
file-backed extensions.
