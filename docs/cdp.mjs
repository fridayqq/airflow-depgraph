// Chrome DevTools protocol helpers shared by record_demo.mjs and screenshots.mjs: open a page in
// the headless Chrome listening on PORT, evaluate JS, move a drawn cursor, click, type.
// Needs node 22+ (built-in WebSocket and fetch).
import {writeFileSync} from 'node:fs';

export const PORT = 9444, W = 1440, H = 860;
export const sleep = ms => new Promise(r => setTimeout(r, ms));

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
const json = async path => (await fetch(`http://127.0.0.1:${PORT}${path}`)).json();

// Opens url in a new W x H target and returns {ev, go, click, type, shot, close, ...finders}.
export async function open(url, {cursor = true, scale = 1} = {}) {
  const br = await connect((await json('/json/version')).webSocketDebuggerUrl);
  const {targetId} = await br.send('Target.createTarget', {url: 'about:blank', width: W, height: H, newWindow: true});
  const pg = await connect((await json('/json')).find(t => t.id === targetId).webSocketDebuggerUrl);
  await pg.send('Page.enable'); await pg.send('Runtime.enable');
  // Headless Chrome fires requestAnimationFrame only when a frame is produced (a screenshot forces
  // one), so d3's timers would run at the screenshot rate and the force layout would crawl or freeze.
  // Without requestAnimationFrame d3 falls back to setTimeout, which runs in real time.
  await pg.send('Page.addScriptToEvaluateOnNewDocument', {source: 'window.requestAnimationFrame = undefined;'});
  await pg.send('Emulation.setDeviceMetricsOverride', {width: W, height: H, deviceScaleFactor: scale, mobile: false});
  await pg.send('Page.navigate', {url});
  await sleep(1500);
  const ev = async expr => (await pg.send('Runtime.evaluate', {expression: expr, returnByValue: true, awaitPromise: true})).result.value;

  // A drawn cursor that follows the synthetic mouse: headless screenshots have none.
  if (cursor) await ev(`(() => {
    const c = document.createElement('div');
    c.innerHTML = '<svg width="22" height="30" viewBox="0 0 22 30"><path d="M2 2 L2 24 L8 18 L12 28 L16 26 L12 17 L20 17 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>';
    Object.assign(c.style, {position: 'fixed', left: '0', top: '0', zIndex: 9999, pointerEvents: 'none', transform: 'translate(-40px,-40px)', filter: 'drop-shadow(0 1px 2px rgba(0,0,0,.4))'});
    document.body.appendChild(c);
    document.addEventListener('mousemove', e => { c.style.transform = 'translate(' + (e.clientX - 2) + 'px,' + (e.clientY - 2) + 'px)'; }, true);
    return 1; })()`);

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
  async function goXY(x, y, ms) { await moveTo(x, y, ms); }
  async function type(text) { for (const ch of text) { await pg.send('Input.insertText', {text: ch}); await sleep(130); } }
  async function shot(file, format = 'png', quality) {
    const s = await pg.send('Page.captureScreenshot', {format, ...(quality ? {quality} : {})});
    writeFileSync(file, Buffer.from(s.data, 'base64'));
  }
  async function close() { await br.send('Target.closeTarget', {targetId}); pg.close(); br.close(); }

  // Finders: JS expressions that evaluate to the element.
  const node = id => `[...document.querySelectorAll('g.n')].find(e => e.__data__ && e.__data__.id === '${id}')`;
  const chip = v => `[...document.querySelectorAll('#facet .chip')].find(b => b.dataset.v === '${v}')`;
  const sw = id => `document.getElementById('${id}').closest('label')`;
  const fit = `document.getElementById('fit')`;
  const search = `document.getElementById('q')`;
  const clearSearch = () => ev(`(() => { const q = document.getElementById('q'); q.value = ''; q.dispatchEvent(new Event('input', {bubbles: true})); q.blur(); return 1; })()`);
  return {ev, go, goXY, click, type, shot, close, node, chip, sw, fit, search, clearSearch};
}
