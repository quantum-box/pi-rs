// Pi v1.5.0 inline tokenization. Marked and the CJK extension are vendored
// alongside their MIT licenses; source text is passed as data, never as code.
(() => {
  const { Marked, Tokenizer, Lexer } = MARKED;
  const deletion = /^(~~)(?=[^\s~])((?:\\.|[^\\])*?(?:\\.|[^\s~\\]))\1(?=[^~]|$)/;
  const punctuation = /(?:(?=\p{Punctuation})[\p{Script_Extensions=Han}\p{Script_Extensions=Hiragana}\p{Script_Extensions=Katakana}\p{Script_Extensions=Hangul}\p{Script_Extensions=Bopomofo}]|[，．：；！？（）［］｛｝“”‘’…—])/u;
  class PiTokenizer extends Tokenizer {
    del(src) {
      const match = deletion.exec(src);
      return match ? { type: 'del', raw: match[0], text: match[2], tokens: this.lexer.inlineTokens(match[2]) } : undefined;
    }
    url(src) {
      const token = super.url(src);
      if (!token) return undefined;
      const index = token.raw.search(punctuation);
      return index < 0 ? token : super.url(token.raw.slice(0, index));
    }
  }
  function escaped(source, index) {
    let n = 0;
    while (index > 0 && source[--index] === '\\') n++;
    return n % 2 === 1;
  }
  function latex(source) {
    let open = '', close = '';
    if (source.startsWith('$$')) { open = '$$'; close = '$$'; }
    else if (source.startsWith('\\(')) { open = '\\('; close = '\\)'; }
    else if (source.startsWith('\\[')) { open = '\\['; close = '\\]'; }
    else if (source.startsWith('$') && !/^\$\s/.test(source)) { open = '$'; close = '$'; }
    else return undefined;
    let end = source.indexOf(close, open.length);
    while (end >= 0 && escaped(source, end)) end = source.indexOf(close, end + close.length);
    if (end >= 0 && open === '$' && (/\s$/.test(source.slice(open.length, end)) || /^\d/.test(source.slice(end + 1)) || (/^[A-Z_][A-Z0-9_]*(?:[^A-Za-z0-9_\s])?$/.test(source.slice(open.length, end)) && /^[A-Za-z_][A-Za-z0-9_]*/.test(source.slice(end + 1))) || source.slice(open.length, end).includes('`'))) return undefined;
    if (end < 0) {
      const text = source.slice(open.length);
      if (open.startsWith('\\') || /\\[A-Za-z]+|[_^=+*/<>()[\]|±≤≥≠≈∈→⇒∞∫∑√-]/.test(text)) return { type: 'latex', raw: source, text, pending: true };
      return undefined;
    }
    const text = source.slice(open.length, end);
    if (!text || text.includes('\n')) return undefined;
    return { type: 'latex', raw: source.slice(0, end + close.length), text };
  }
  const marked = new Marked();
  marked.setOptions({ tokenizer: new PiTokenizer() });
  marked.use({
    tokenizer: { emStrong: cjk().tokenizer.emStrong },
    extensions: [{ name: 'latex', level: 'inline', start(source) {
      const positions = [source.indexOf('$'), source.indexOf('\\('), source.indexOf('\\[')].filter(i => i >= 0);
      return positions.length ? Math.min(...positions) : undefined;
    }, tokenizer: latex }],
  });
  globalThis.piInlineTokens = (text, linksJson) => {
    const lexer = new Lexer(marked.defaults);
    lexer.tokens.links = JSON.parse(linksJson);
    return JSON.stringify(lexer.inlineTokens(text));
  };
})();
