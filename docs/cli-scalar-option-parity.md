# Repeated CLI scalar options

Pi v1.5.0 processes its command line in order and assigns a later value over
an earlier value for scalar options. The native CLI does the same for
`--provider`, `--model`, `--models`, `--api-key`, `--session-dir`, `--name`
(`-n`), `--use-theme`, `--system-prompt`, and `--exclude-tools` (`-xt`). This
allows a caller to add an explicit final override to a set of default arguments.

Repeatable resource options such as `--skill`, `--extension`, `--theme`, and
`--append-system-prompt` continue to collect values. Arguments after `--`
remain positional. Native validation still applies to each parsed occurrence;
this change does not claim parity for malformed earlier values or all other
repeated options.
