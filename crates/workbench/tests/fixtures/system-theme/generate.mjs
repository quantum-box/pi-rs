// Usage: node generate.mjs /path/to/pi > upstream-v1.5.0.json
// Node 24+ runs the upstream TypeScript after resolving its local color imports.
import { mkdtempSync, readFileSync, writeFileSync, copyFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
const root = process.argv[2];
if (!root) throw new Error("Pass the path to a Pi v1.5.0 checkout");
const dir = mkdtempSync(join(tmpdir(), "pi-system-fixtures-"));
try {
  for (const name of ["colors.ts", "oklab.ts"]) copyFileSync(join(root, "packages/tui/src", name), join(dir, name));
  const system = readFileSync(join(root, "packages/coding-agent/src/modes/interactive/theme/system-theme.ts"), "utf8");
  writeFileSync(join(dir, "system-theme.ts"), system.replace('from "@earendil-works/pi-tui"', 'from "./bridge.ts"'));
  writeFileSync(join(dir, "bridge.ts"), "export * from './colors.ts';\nexport { oklabToOkhslLightness } from './oklab.ts';\n");
  writeFileSync(join(dir, "run.ts"), String.raw`import {generateSystemThemeColors} from './system-theme.ts';
const rgb = (hex: string) => ({r: parseInt(hex.slice(1,3),16),g:parseInt(hex.slice(3,5),16),b:parseInt(hex.slice(5,7),16)});
const dracula = {background:rgb('#282a36'),foreground:rgb('#f8f8f2'),palette:['#21222c','#ff5555','#50fa7b','#f1fa8c','#bd93f9','#ff79c6','#8be9fd','#f8f8f2','#6272a4','#ff6e6e','#69ff94','#ffffa5','#d6acff','#ff92df','#a4ffff','#ffffff'].map(rgb)};
const cases = { indexed: {}, indexedLight: {appearanceHint:'light'}, indexedGrayscale:{saturation:0}, dracula, draculaGrayscale:{...dracula,saturation:0}, solarizedLight:{background:rgb('#fdf6e3'),foreground:rgb('#657b83')}, backgroundOnly:{background:rgb('#1e1e1e')}, midGray:{background:rgb('#808080'),foreground:rgb('#ffffff')}, black:{background:rgb('#000000')}, white:{background:rgb('#ffffff')} };
const frappe = {background:rgb('#303446'),foreground:rgb('#c6d0f5'),palette:['#51576d','#e78284','#a6d189','#e5c890','#8caaee','#f4b8e4','#81c8be','#b5bfe2','#626880','#e67172','#8ec772','#d9ba73','#7b9ef0','#f2a4db','#5abfb5','#a5adce'].map(rgb)};
Object.assign(cases, {frappe, frappeHalf:{...frappe,saturation:0.5}, grayLight:{background:rgb('#aaaaaa'),foreground:rgb('#111111')}, blueBackground:{background:rgb('#183d68'),foreground:rgb('#eece99')}, redBackground:{background:rgb('#a34751'),foreground:rgb('#161924')}});
console.log(JSON.stringify(Object.entries(cases).map(([name,input])=>({name,input,output:generateSystemThemeColors(input)})),null,2));
`);
  const result = spawnSync(process.execPath, [join(dir, "run.ts")], { encoding: "utf8" });
  if (result.status !== 0) throw new Error(result.stderr || `Node exited ${result.status}`);
  process.stdout.write(result.stdout);
} finally { rmSync(dir, { recursive: true, force: true }); }
