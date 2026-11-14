# RPC JSONL input parity

Pi v1.5.0 (`f9a26365568127461c47a07a37dcb42c140a3ac5`) decodes RPC
stdin as UTF-8 with replacement, frames on LF only, and removes a trailing CR
from each record, including the final unterminated one. Native RPC follows the
same rules. An invalid UTF-8 byte inside a JSON string becomes U+FFFD; invalid
JSON still receives a parse error and later records are processed. U+2028 and
U+2029 remain data rather than record delimiters. I/O failures remain errors.

A chunked-reader regression and an offline CLI fixture cover replacement,
CRLF, Unicode separators, parse-error recovery and end-of-input framing.
