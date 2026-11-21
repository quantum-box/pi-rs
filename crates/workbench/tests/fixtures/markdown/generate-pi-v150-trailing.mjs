// Regenerate with: node generate-pi-v150-trailing.mjs /path/to/pi
// The Pi checkout needs its normal Node dependencies installed.
import { execFileSync } from 'node:child_process';
import { appendFileSync, cpSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const commit = 'f9a26365568127461c47a07a37dcb42c140a3ac5';
const root = resolve(process.argv[2] ?? '');
if (!process.argv[2] || execFileSync('git', ['-C', root, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim() !== commit) {
  throw new Error(`Provide a Pi checkout at ${commit}`);
}
// The parser is private upstream. Export it only from a temporary copy so the
// original checkout stays untouched and token boundaries can be inspected.
const temp = mkdtempSync(join(tmpdir(), 'pi-markdown-oracle-'));
process.on('exit', () => rmSync(temp, { recursive: true, force: true }));
cpSync(join(root, 'packages/tui/src'), join(temp, 'src'), { recursive: true });
symlinkSync(join(root, 'node_modules'), join(temp, 'node_modules'), 'dir');
const markdownFile = join(temp, 'src/components/markdown.ts');
appendFileSync(markdownFile, '\nexport { markdownParser };\n');
const { Markdown, markdownParser } = await import(pathToFileURL(markdownFile).href);
const theme = Object.fromEntries([
  'heading', 'link', 'linkUrl', 'code', 'codeBlock', 'codeBlockBorder',
  'quote', 'quoteBorder', 'hr', 'listBullet', 'bold', 'italic', 'strikethrough', 'underline',
].map(name => [name, text => text]));
const blocks = ['hello', '# head', '---', '- item', '> quoted', '```\nx\n```', '| A |\n|---|\n| x |', '$$x^2$$', '\\[\nx^2\n\\]', '[x][r]'];
const tails = ['', '\n', '\n\n', '\n\n\n', '\n\n[r]: https://e.co', '\n\n[r]: https://e.co\n', '\n\n[r]: https://e.co\n\n'];
const sources = blocks.flatMap(block => tails.map(tail => block + tail));
const extras = [
  'one\ntwo', 'one  \ntwo', 'one\r\ntwo', 'one\rtwo', '> one\n> two',
  '- one\n  two', '- one\ntwo', '- one  \n  two', '- **a\n  b**',
  '- a\n  - b\n  1. b', '- a\n\n  ```\n  x\n  ```\n  - b',
  '- outer\n\n  - inner\n\n    second\n\n  tail',
  '- outer\n\n  1. inner\n\n     next\n\n  tail\n\n- sibling',
  '- outer\n  - first\n    - nested\n\n      next\n\n    tail\n\n  end',
  '[r]: https://e.co', '[r]: https://e.co\n\n', '> quoted\n>\n',
  '- item\n\n  $$x^2$$\n\n', 'hello\n\n[r]: https://e.co\n\n[s]: https://s.co', '$$x$$\n[r]: https://e.co',
  '[r]: https://e.co\n\ntext', '[r]: https://e.co\n\n$$x^2$$\n\n[s]: https://s.co',
  'text\n\n[r]: https://e.co\n\n[s]: https://s.co\n\nend', 'text\n\n[r]: https://e.co\n\nend',
  '> text\n>\n> [r]: https://e.co', '> text\n>\n> [r]: https://e.co\n>\n',
  '- item\n\n  [r]: https://e.co\n\n', '```\na\n\n', '```\n\n', '    code\n\n',
  '- item\n\n  ```\n  x\n\n', '$$x^2$$\n\n\n', '$$x^2$$\n[r]: https://e.co', 'hello  \n\n',
  '# Hello\n\n[ref]: https://example.com', '# Hello\n\n[ref]: https://example.com\n\nworld',
  '# First\n\n## Last\n\n[ref]: https://example.com',
  '[before][ref]\n\n$$\\frac{1}{2}$$\n\n[after][ref]\n\n[ref]: https://example.org',
  '- one\n\n  second paragraph\n\n', '- one\n\n  ```js\n  x\n  ```\n\n',
  '- one\n\n  > quote\n\n', '- one\n  - nested\n\n- two\n',
  '1. one\n\n   second\n\n2. two\n\n', '- [x] task\n\n  more\n\n',
  '- [link][r]\n\n  [r]: https://e.co\n\n', '- item\n\n  [r]: https://e.co\n\n  end\n\n',
  '- item\n\n  ```\n  x\n  ```\n\n- next\n\n',
  'first\n\nsecond\n\n# heading\n\nlast\n\n',
  '[x][ref]\n\n[ref]: https://first.example\n\n[ref]: https://second.example\n\nend',
  'alpha\n\n[ref]:\n  https://e.co\n\n[x][ref]',
  '前文\n\n$$x^2$$\n\n後文\n\n',
  '> outer\n>\n> > inner\n>\n> end\n\n',
  '```text\nline\n\n```\n\nnext', '<div>\nx\n</div>\n\nnext',
  'hello  ', 'hello\n', '[x]: /a\n[x]: /b\n',
  'text\n\n[x]: /a\n[x]: /b\n\nend',
  '[x]: /a\n\n[x]: /b\n\n', 'text\n\n[x]: /a\n\n[x]: /b\n\nend',
  '[x]: /a\n\nhello\n\n[x]: /b\n\nend',
  '- >', '- ```\n  a\n\n\n  ```',
  '- first\n\n  # heading\n\n  next', '- # heading\n\n  next',
  '[x]: /a\n\n# h\n[X]: /b\n\nend',
  '[x][r]\n\n- outer\n  - inner\n\n    [r]: /url\n',
  '[r]: /first\n\n- [x][r]\n\n  [r]: /second\n',
  '- outer\n  - inner\n\n    [r]: /first\n\n[x][r]\n\n[r]: /second',
  '[x][r]\n\n> [r]: /url\n\n$$x^2$$',
  '- first\n\n  [r]: /one\n\ntext\n\n- [x][r]\n\n  [r]: /two',
  '- a\n  - b\n\n    [r]: /one\n\ntext\n\n- [x][r]\n\n  [r]: /two',
  '[x][r]\n\n- outer\n\n  [r]:\n    /url\n    "title"\n',
  '> [x][r]\n\n[r]: /url',
  '[x][r]\n\n$$\n[r]: /fake\n$$',
  '[x][r]\n$$x$$\n- outer\n  - inner\n\n    [r]: /url',
  '[x][r]\n\n$$\n\n[r]: /fake\n\n$$\n\n[r]: /real',
  '[https://e.co/a_b][r]\n\n[r]: /real',
  '[x][r]\n\n> $$\n>\n> [r]: /fake\n>\n> $$\n\n[r]: /real',
  '- [x][r]\n\n  $$\n\n  [r]: /fake\n\n  $$',
  '- [**bold**][r]\n\n  $$\n\n  [r]: /fake\n\n  $$',
];
for (const source of extras) if (!sources.includes(source)) sources.push(source);
const fixture = {
  reference: `Pi v1.5.0 ${commit} packages/tui/src/components/markdown.ts`,
  width: 80,
  narrowCases: [
    ['- > abc', 3],
    ['alpha beta gamma', 9],
    ['alpha **beta** gamma', 9],
    ['alpha beta\ngamma delta', 9],
    ['- alpha beta gamma', 11],
    ['- [x] alpha beta gamma', 14],
    ['> alpha beta gamma', 11],
    ['alpha chartreuse', 9],
    ['abc 日本語 test', 8],
    ['paint 🧑‍💻 next', 8],
    ['- one\n\n  ***', 14],
    ['1. before\n\n   ***\n\n   after', 14],
    ['- a\n  - b\n\n    ***\n  - c', 14],
    ['- one\n  ***\n  two', 14],
    ['- ***\n\n- two', 14],
  ].map(([source, width]) => ({
    source, width, lines: new Markdown(source, 0, 0, theme).render(width).map(line => line.trimEnd()),
  })),
  cases: sources.map(source => ({
    source,
    lines: new Markdown(source, 0, 0, theme).render(80).map(line => line.trimEnd()),
    tokens: markdownParser.lexer(source.replace(/\t/g, '   ')).map(({ type, raw }) => ({ type, raw })),
  })),
};
writeFileSync(new URL('pi-v150-trailing.json', import.meta.url), `${JSON.stringify(fixture, null, 2)}\n`);
