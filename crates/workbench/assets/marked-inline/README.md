# Pi inline Markdown lexer

`marked.js` is the minified ESM distribution of Marked 18.0.11, and `cjk.js`
is marked-cjk-friendly 0.1.2. These are the versions pinned by Pi v1.5.0.
Their final ESM exports were replaced with globals for the embedded QuickJS
runtime; their upstream MIT licenses are included alongside them.

`pi-inline.js` configures those upstream lexers with Pi's deletion, URL and
inline LaTeX tokenizers. Markdown and reference definitions are passed as data,
not evaluated as JavaScript. Pulldown still parses block structure; the inline
lexer is used for source runs containing double tildes so deletion can cross
boundaries that pulldown assigns to code spans, links and emphasis.
