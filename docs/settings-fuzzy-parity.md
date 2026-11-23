# Settings search parity

The settings selector follows Pi v1.5.0's fuzzy filter: whitespace and `/`
separate search tokens, and every token must match the setting label. Tokens
can appear in a different order from the label. Matches are ranked by Pi's
fuzzy score, with original order retained for ties. The search uses the setting
name rather than its displayed current value.

The separator set follows ECMAScript whitespace, including U+FEFF and excluding
U+0085. This checkpoint covers the settings list; other selectors have their
own search behavior. Settings paste expands tabs to four spaces and removes
CR/LF, as Pi's single-line input does. Other pasted characters remain in the
search query; terminal rendering removes control characters.
