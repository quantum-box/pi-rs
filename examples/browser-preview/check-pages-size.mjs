import { readdir, stat } from 'node:fs/promises';
import path from 'node:path';

// Cloudflare Pages limits each individual asset to 25 MiB.
const limit = 25 * 1024 * 1024;
async function check(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) await check(file);
    else if (entry.isFile()) {
      const size = (await stat(file)).size;
      if (size > limit) throw new Error(`${file} is ${size} bytes; Pages limit is ${limit}`);
    }
  }
}
await check(process.argv[2]);
