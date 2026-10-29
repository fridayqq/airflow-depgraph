// Record the README demo (docs/images/demo.gif) from the fictional snapshot.
//
//   python3 docs/demo_snapshot.py demo.json
//   AIRFLOW_DEPGRAPH_HIGHLIGHT_LABEL=Reports airflow-depgraph render demo.json --highlight '_report$' -o demo.html
//   python3 -m http.server 8765 &                      # in the folder with demo.html
//   "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new \
//       --remote-debugging-port=9444 --remote-allow-origins='*' --user-data-dir=/tmp/depgraph-demo \
//       --window-size=1440,860 --hide-scrollbars --force-device-scale-factor=1 about:blank &
//   node docs/record_demo.mjs frames/                 # ~25 s, writes frames/f00000.jpg …
//   ffmpeg -framerate 10 -i frames/f%05d.jpg -vf "scale=1200:-1:flags=lanczos,split[a][b];\
//       [a]palettegen=max_colors=200:stats_mode=diff[p];[b][p]paletteuse=dither=bayer:bayer_scale=5:\
//       diff_mode=rectangle" -loop 0 docs/images/demo.gif
//
// The script drives the page over the Chrome DevTools protocol: it draws a cursor, presses Fit,
// searches for "report", picks the Finance and Marketing chips, selects revenue_model, switches
// blast radius on and ends on "failing and their chain". Needs node 22+ (built-in WebSocket and fetch).
import {writeFileSync, mkdirSync} from 'node:fs';

const PORT = 9444, W = 1440, H = 860, PAGE = 'http://127.0.0.1:8765/demo.html';
const FR = (process.argv[2] || 'frames').replace(/\/?$/, '/');
mkdirSync(FR, {recursive: true});

async function connect(ws) {
  const sock = new WebSocket(ws);
  await new Promise((ok, no) => { sock.onopen = ok; sock.onerror = no; });
  let id = 0; const waits = new Map();
  sock.onmessage = e => { const m = JSON.parse(e.data); if (m.id && waits.has(m.id)) { waits.get(m.id)(m); waits.delete(m.id); } };
  return {
    send: (method, params = {}) => new Promise((ok, no) => {
      const i = ++id;
      waits.set(i, m => m.error ? no(new Error(method + ': ' + JSON.stringify(m.error))) : ok(m.result));
      sock.send(JSON.stringify({id: i, method, params}));
    }),
    close: () => sock.close(),
  };
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
const json = async path => (await fetch(`http://127.0.0.1:${PORT}${path}`)).json();

const br = await connect((await json('/json/version')).webSocketDebuggerUrl);
const {targetId} = await br.send('Target.createTarget', {url: 'about:blank', width: W, height: H, newWindow: true});
const pg = await connect((await json('/json')).find(t => t.id === targetId).webSocketDebuggerUrl);
await pg.send('Page.enable'); await pg.send('Runtime.enable');
await pg.send('Emulation.setDeviceMetricsOverride', {width: W, height: H, deviceScaleFactor: 1, mobile: false});
await pg.send('Page.navigate', {url: PAGE});
await sleep(1500);
const ev = async expr => (await pg.send('Runtime.evaluate', {expression: expr, returnByValue: true, awaitPromise: true})).result.value;

// A drawn cursor that follows the synthetic mouse: headless screenshots have none.
await ev(`(() => {
  const c = document.createElement('div');
  c.innerHTML = '<svg width="22" height="30" viewBox="0 0 22 30"><path d="M2 2 L2 24 L8 18 L12 28 L16 26 L12 17 L20 17 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>';
  Object.assign(c.style, {position: 'fixed', left: '0', top: '0', zIndex: 9999, pointerEvents: 'none', transform: 'translate(-40px,-40px)', filter: 'drop-shadow(0 1px 2px rgba(0,0,0,.4))'});
  document.body.appendChild(c);
  document.addEventListener('mousemove', e => { c.style.transform = 'translate(' + (e.clientX - 2) + 'px,' + (e.clientY - 2) + 'px)'; }, true);
  return 1; })()`);

// Frames at about 10 per second while the scenario runs.
let n = 0, running = true;
const frames = (async () => { while (running) {
  const t0 = Date.now();
  const s = await pg.send('Page.captureScreenshot', {format: 'jpeg', quality: 88});
  writeFileSync(`${FR}f${String(n++).padStart(5, '0')}.jpg`, Buffer.from(s.data, 'base64'));
  await sleep(Math.max(0, 100 - (Date.now() - t0)));
} })();

let mx = -40, my = -40;
const mouse = (type, x, y, extra = {}) => pg.send('Input.dispatchMouseEvent', {type, x, y, button: 'left', ...extra});
async function moveTo(x, y, ms = 500) {
  const steps = Math.max(6, Math.round(ms / 40)), sx = mx, sy = my;
  for (let i = 1; i <= steps; i++) {
    const t = i / steps, e = t * t * (3 - 2 * t);
    mx = sx + (x - sx) * e; my = sy + (y - sy) * e;
    await mouse('mouseMoved', mx, my); await sleep(40);
  }
}
async function click() { await mouse('mousePressed', mx, my, {clickCount: 1}); await sleep(70); await mouse('mouseReleased', mx, my, {clickCount: 1}); }
async function center(finder) {
  const r = await ev(`(() => { const el = (${finder}); if (!el) return null; const b = el.getBoundingClientRect(); return [b.left + b.width / 2, b.top + b.height / 2]; })()`);
  if (!r) throw new Error('not found: ' + finder);
  return r;
}
async function go(finder, ms) { const [x, y] = await center(finder); await moveTo(x, y, ms); }
async function type(text) { for (const ch of text) { await pg.send('Input.insertText', {text: ch}); await sleep(130); } }
const node = id => `[...document.querySelectorAll('g.n')].find(e => e.__data__ && e.__data__.id === '${id}')`;
const chip = v => `[...document.querySelectorAll('#facet .chip')].find(b => b.dataset.v === '${v}')`;
const sw = id => `document.getElementById('${id}').closest('label')`;
const fit = `document.getElementById('fit')`;

await sleep(2200);
await go(fit, 600); await click(); await sleep(1300);
await go(`document.getElementById('q')`, 500); await click(); await sleep(250); await type('report'); await sleep(1800);
await ev(`(() => { const q = document.getElementById('q'); q.value = ''; q.dispatchEvent(new Event('input', {bubbles: true})); q.blur(); return 1; })()`); await sleep(400);
await go(fit, 500); await click(); await sleep(1000);
await go(chip('Finance'), 600); await click(); await sleep(1400);
await go(chip('Marketing'), 400); await click(); await sleep(1600);
await go(chip(''), 400); await click(); await sleep(700);
await go(node('dag:revenue_model'), 700); await click(); await sleep(1800);
await go(sw('blast'), 700); await click(); await sleep(2200);
await go(sw('blast'), 300); await click(); await sleep(300);
await go(sw('onlyFail'), 400); await click(); await sleep(2000);

running = false; await frames;
console.log(`${n} frames in ${FR}`);
await br.send('Target.closeTarget', {targetId});
pg.close(); br.close();
