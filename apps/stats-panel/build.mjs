// Builds the single-file Orgtree app HTML: panel.html with stats-lib.js inlined.
// node apps/stats-panel/build.mjs  ->  apps/stats-panel/dist/stats-panel.html
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const here = (p) => new URL(p, import.meta.url);

export async function build() {
  const [tpl, lib] = await Promise.all([readFile(here('./panel.html'), 'utf8'), readFile(here('./stats-lib.js'), 'utf8')]);
  if (!tpl.includes('/*STATS_LIB*/')) throw new Error('panel.html has no /*STATS_LIB*/ placeholder');
  return tpl.replace('/*STATS_LIB*/', () => lib);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const html = await build();
  await mkdir(here('./dist/'), { recursive: true });
  await writeFile(here('./dist/stats-panel.html'), html, 'utf8');
  console.log('wrote dist/stats-panel.html', Buffer.byteLength(html), 'bytes');
}
