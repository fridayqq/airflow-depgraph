// Take the README screenshots from the fictional snapshot, with the page served and the headless
// Chrome started as in record_demo.mjs:
//
//   node docs/screenshots.mjs docs/images/
//
// Writes page.png (the page after Fit, nothing selected) and impact.png ("what failed and what it
// blocks" with nothing selected), at 2x for a sharp README.
import {mkdirSync} from 'node:fs';
import {open, sleep} from './cdp.mjs';

const PAGE = 'http://127.0.0.1:8765/demo.html';
const OUT = (process.argv[2] || 'docs/images').replace(/\/?$/, '/');
mkdirSync(OUT, {recursive: true});

for (const [file, hash] of [['page.png', ''], ['impact.png', '#impact']]) {
  const p = await open(PAGE + hash, {cursor: false, scale: 2});
  await sleep(5000);   // let the layout settle
  await p.ev(`document.getElementById('fit').click()`); await sleep(1500);
  await p.shot(OUT + file);
  console.log('wrote', OUT + file);
  await p.close();
}
