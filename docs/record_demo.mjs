// Record the README demo (docs/images/demo.gif) from the fictional snapshot.
//
//   python3 docs/demo_snapshot.py demo.json
//   AIRFLOW_DEPGRAPH_HIGHLIGHT_LABEL=Reports airflow-depgraph render demo.json --highlight '_report$' -o demo.html
//   python3 -m http.server 8765 &                      # in the folder with demo.html
//   "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new \
//       --remote-debugging-port=9444 --remote-allow-origins='*' --user-data-dir=/tmp/depgraph-demo \
//       --window-size=1440,860 --hide-scrollbars --force-device-scale-factor=1 \
//       --disable-background-timer-throttling --disable-renderer-backgrounding about:blank &
//   node docs/record_demo.mjs frames/                 # ~45 s, writes frames/f00000.jpg …
//   ffmpeg -framerate 10 -i frames/f%05d.jpg -vf "scale=1100:-1:flags=lanczos,split[a][b];\
//       [a]palettegen=max_colors=160:stats_mode=diff[p];[b][p]paletteuse=dither=bayer:bayer_scale=5:\
//       diff_mode=rectangle" -loop 0 docs/images/demo.gif
//
// The script drives the page over the Chrome DevTools protocol (docs/cdp.mjs): it draws a cursor,
// presses Fit, searches for "report", picks the Finance and Marketing chips, selects revenue_model,
// switches "what failed and what it blocks" on, clicks the blocked exec_dashboard_report, switches
// the tests-only failures off and on, and ends on "failing and their chain". Needs node 22+.
import {writeFileSync, mkdirSync} from 'node:fs';
import {open, sleep} from './cdp.mjs';

const PAGE = 'http://127.0.0.1:8765/demo.html';
const FR = (process.argv[2] || 'frames').replace(/\/?$/, '/');
mkdirSync(FR, {recursive: true});

const p = await open(PAGE);

// Frames at about 10 per second while the scenario runs.
let n = 0, running = true;
const frames = (async () => { while (running) {
  const t0 = Date.now();
  await p.shot(`${FR}f${String(n++).padStart(5, '0')}.jpg`, 'jpeg', 88);
  await sleep(Math.max(0, 100 - (Date.now() - t0)));
} })();

await sleep(2200);
await p.go(p.fit, 600); await p.click(); await sleep(1300);
await p.go(p.search, 500); await p.click(); await sleep(250); await p.type('report'); await sleep(1800);
await p.clearSearch(); await sleep(400);
await p.go(p.fit, 500); await p.click(); await sleep(1000);
await p.go(p.chip('Finance'), 600); await p.click(); await sleep(1400);
await p.go(p.chip('Marketing'), 400); await p.click(); await sleep(1600);
await p.go(p.chip(''), 400); await p.click(); await sleep(700);
await p.go(p.node('dag:revenue_model'), 700); await p.click(); await sleep(1800);
await p.goXY(300, 300, 400); await p.click(); await sleep(500);            // empty canvas: deselect
await p.go(p.sw('impact'), 700); await p.click(); await sleep(3000);
await p.go(p.node('dag:exec_dashboard_report'), 800); await p.click(); await sleep(2600);
await p.goXY(300, 300, 400); await p.click(); await sleep(600);
await p.go(p.sw('kindTests'), 600); await p.click(); await sleep(1800);
await p.go(p.sw('kindTests'), 300); await p.click(); await sleep(1200);
await p.go(p.sw('impact'), 500); await p.click(); await sleep(400);
await p.go(p.sw('onlyFail'), 400); await p.click(); await sleep(2000);

running = false; await frames;
console.log(`${n} frames in ${FR}`);
await p.close();
