# Local shell output parity

Compared with Pi v1.5.0 (`f9a26365568127461c47a07a37dcb42c140a3ac5`),
large shell results passed to codemode retain the first and last 512 KiB of
raw output. Each retained segment uses the reference's UTF-8 decoding: a
leading BOM is removed, malformed bytes are replaced, and an incomplete
sequence at the head boundary is held back. The saved full-output file retains
the original bytes. User shell output retains its existing BOM behavior.

The regression covers malformed data before a split UTF-8 sequence, BOMs at both
retained segment boundaries, the omission count and byte-exact recovery.
