# Native shell signal results

A local Bash or PowerShell process terminated by a Unix signal reports Pi's
shell convention: exit code `128 + signal`. The tool returns an error result
with the captured output, a `Command exited with code …` notice, and the same
code in structured output. Local Bash operations supplied to an embedded host
use the same conversion.

A custom backend that explicitly returns no exit code still produces
`Command terminated without an exit code`. Timeouts and cancellation retain
their dedicated errors and are not projected as ordinary signal exits.
