`unsafe_first_park_216a.sqlite` was produced by the real `unsafe_process_fixture`
from pi-rs commit `216a7eb581227614ae0d09c8eccfb037857f0460`, with
`PI_UNSAFE_ASYNC_TEST_MODE=parked`. The fixture process was killed after it
reported its first committed unsafe park. SQLite's backup API copied the
complete database, including its history, into this standalone file.

Root task: `da2eeafe-c46e-45e2-b10f-9340416d225f`
Gate task: `35bdc178-6356-4825-a6e8-eb518f419688`
SHA-256: `a11627ce291c8afc7bba30efb75f6ed05ad751651fa45f6ccf8879e103657e51`

This historical format has the original attempt intent, joined park proposal,
committed park result, and park proof, but has no independent prepared-argument
digest. The upgrade test copies the fixture to a temporary directory and uses
only the current public host and driver to verify that wake fails closed.

`unsafe_second_park_216a.sqlite` was generated from the same commit using the
existing `TwoParks` tool and gate, with a temporary test-only driver that woke
its first park and killed the process after the second park committed. Its
production crate sources were unchanged. SQLite backup produced the standalone
file. The second parked attempt's intent contains its previous parked
checkpoint and prepared arguments; that is sufficient for a checked upgrade.

Root task: `5d55f889-2b38-4ed4-98b2-3e5407c00e46`
Second gate task: `eb023929-9980-4e32-a28b-2d184563ac1a`
SHA-256: `7f79cbbcc263f9a60a8b097fb6e412bdd2beab78909650e3f9e71db11a78ef52`
