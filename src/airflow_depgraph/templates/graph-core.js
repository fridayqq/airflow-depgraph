// Pure helpers of the dependency page: no DOM, no globals, inputs as arguments. render.py inlines
// this file into the page before the page script; node imports it for tests/js.

// "1 DAG", "2 DAGs".
function plural(n, one, many) { return `${n} ${n === 1 ? one : many}`; }

// Non-DAG nodes are links: assets, triggers and sensors.
function isLinkNode(node) { return node.type !== 'dag'; }
function failing(n) { return !!(n.fail && !n.fail.paused); }
function tagged(n, tags) { return (n.tags || []).some(t => tags.includes(t)); }
// Unlinked DAGs, and DAGs linked only by deploy guards while guards are hidden.
function looseNode(n, guardsOn) { return !!(n.unlinked || (n.guard_only && !guardsOn)); }

// Adjacency maps {out, inn} from edges. Guard edges are left out unless opts.guards.
function buildAdj(edges, nodesById, opts) {
  const guards = !!(opts && opts.guards);
  const out = new Map(), inn = new Map();
  for (const e of edges) {
    if (!nodesById.has(e.s) || !nodesById.has(e.t)) continue;
    if (e.guard && !guards) continue;
    if (!out.has(e.s)) out.set(e.s, []);
    if (!inn.has(e.t)) inn.set(e.t, []);
    out.get(e.s).push(e.t);
    inn.get(e.t).push(e.s);
  }
  return {out, inn};
}

// Nodes reachable from startIds along adj. Depth counts DAG hops; assets, triggers and sensors in between are free.
function reach(startIds, adj, depth, nodesById) {
  const keep = new Set(startIds);
  let frontier = [...startIds];
  for (let hop = 0; frontier.length && hop < depth; hop++) {
    const next = [], seenLinks = new Set(), stack = [...frontier];
    while (stack.length) {
      const x = stack.pop();
      for (const y of adj.get(x) || []) {
        if (nodesById.get(y).type === 'dag') {
          if (!keep.has(y)) { keep.add(y); next.push(y); }
        } else if (!seenLinks.has(y)) { seenLinks.add(y); keep.add(y); stack.push(y); }
      }
    }
    frontier = next;
  }
  return keep;
}

// Everything upstream and downstream of id.
function chain(id, outAdj, inAdj, nodesById) {
  return new Set([...reach([id], outAdj, Infinity, nodesById), ...reach([id], inAdj, Infinity, nodesById)]);
}

// DAG ids of other DAGs one edge or one link node away from id.
function neighbours(id, adj, nodesById) {
  const out = [];
  for (const m of [adj.out, adj.inn]) {
    for (const y of m.get(id) || []) {
      if (!isLinkNode(nodesById.get(y))) out.push(y);
      else for (const z of m.get(y) || []) if (!isLinkNode(nodesById.get(z))) out.push(z);
    }
  }
  return out;
}

// The filters: tag, active, connected, guards, blast radius and only failing. data = {nodes, edges,
// byId, tags}; state = {onlyTags, onlyActive, conn, showGuards, blast, onlyFail}. Returns the kept
// node ids and the edges between them.
function filterView(data, state) {
  const byId = data.byId, tags = data.tags || [];
  const adj = buildAdj(data.edges, byId, {guards: state.showGuards});
  const tagOn = tags.length > 0 && !!state.onlyTags, active = !!state.onlyActive, guards = !!state.showGuards;
  const okDag = n => !(active && n.paused);
  const dags = new Set(data.nodes.filter(n => n.type === 'dag' && !looseNode(n, guards) && (!tagOn || tagged(n, tags)) && okDag(n))
    .map(n => n.id));
  if (tagOn && state.conn) [...dags].forEach(id => neighbours(id, adj, byId).forEach(x => { if (okDag(byId.get(x))) dags.add(x); }));
  // A link node stays while it joins two kept nodes, or nothing around it was filtered out;
  // one left with no neighbour at all goes.
  let keep = new Set([...dags, ...data.nodes.filter(n => n.type !== 'dag' && (guards || n.kind !== 'guard')).map(n => n.id)]);
  for (let changed = true; changed;) {
    changed = false;
    for (const id of [...keep]) {
      if (!isLinkNode(byId.get(id))) continue;
      const around = [...(adj.out.get(id) || []), ...(adj.inn.get(id) || [])];  // guard edges only when shown
      const kept = around.filter(x => keep.has(x)).length;
      if (kept === 0 || (kept < 2 && kept < around.length)) { keep.delete(id); changed = true; }
    }
  }
  if (state.blast) {  // failing DAGs and everything downstream of them, no upstream
    const lit = new Set();
    data.nodes.filter(n => keep.has(n.id) && failing(n)).forEach(n => reach([n.id], adj.out, Infinity, byId).forEach(x => lit.add(x)));
    keep = new Set([...keep].filter(id => lit.has(id)));
  }
  if (state.onlyFail) {  // failing DAGs and their chains, within what the filters keep
    const lit = new Set();
    data.nodes.filter(n => keep.has(n.id) && failing(n)).forEach(n => chain(n.id, adj.out, adj.inn, byId).forEach(x => lit.add(x)));
    keep = new Set([...keep].filter(id => lit.has(id)));
  }
  return {keep, edges: data.edges.filter(e => keep.has(e.s) && keep.has(e.t))};
}

// Collapse dag -> link(s) -> dag into dag -> dag; via = the last link on the way. Returns the DAG ids
// among nodes and the collapsed edges.
function collapseLinks(nodes, edges, byId) {
  const adj = new Map(), coll = new Map();
  edges.forEach(e => { if (!adj.has(e.s)) adj.set(e.s, []); adj.get(e.s).push(e.t); });
  const add = (s, t, via, x) => {
    const k = s + '|' + t, f = {inf: !!byId.get(x || s).inferred, guard: x ? byId.get(x).kind === 'guard' : false};
    if (s === t) return;
    if (!coll.has(k)) coll.set(k, {s, t, via, ...f});
    else if (!f.inf && !f.guard) Object.assign(coll.get(k), {via, inf: false, guard: false});  // an explicit path wins
  };
  edges.forEach(e => {
    if (byId.get(e.s).type !== 'dag') return;
    if (byId.get(e.t).type === 'dag') { add(e.s, e.t, null, null); return; }
    const seen = new Set([e.t]), stack = [e.t];
    while (stack.length) {
      const x = stack.pop();
      for (const y of adj.get(x) || []) {
        if (byId.get(y).type === 'dag') add(e.s, y, byId.get(x).label, x);
        else if (!seen.has(y)) { seen.add(y); stack.push(y); }
      }
    }
  });
  return {keep: nodes.filter(id => byId.get(id).type === 'dag'), edges: [...coll.values()]};
}

// The URL fragment: q=<text>, then the flag names, then sel=<node id>. Values are URI-encoded.
function encodeHash(state) {
  const parts = [];
  if (state.q) parts.push('q=' + encodeURIComponent(state.q));
  parts.push(...(state.flags || []));
  if (state.sel) parts.push('sel=' + encodeURIComponent(state.sel));
  return parts.join(',');
}

// Inverse of encodeHash: {q, sel} (null when absent) and the other tokens as flags. A value that
// does not decode is kept raw.
function decodeHash(str) {
  const safe = s => { try { return decodeURIComponent(s); } catch (e) { return s; } };
  const out = {q: null, flags: [], sel: null};
  for (const t of String(str || '').replace(/^#/, '').split(',').filter(Boolean)) {
    if (t.startsWith('q=')) out.q = safe(t.slice(2));
    else if (t.startsWith('sel=')) out.sel = safe(t.slice(4));
    else out.flags.push(t);
  }
  return out;
}

// "[N+]text[+[N]]": leading +/digits = upstream (depth), trailing +/digits = downstream. No "+" = plain
// substring. re is the search pattern with groups: upstream depth, text, downstream depth.
function parseQuery(raw, re) {
  const t = raw.trim();
  if (!t) return null;
  const m = re.exec(t);
  if (!m || (m[1] === undefined && m[3] === undefined)) return {text: t.toLowerCase(), plus: false};
  const depth = s => s ? parseInt(s, 10) : Infinity;
  return {text: m[2].toLowerCase(), plus: true, up: m[1] !== undefined, upDepth: depth(m[1]), down: m[3] !== undefined, downDepth: depth(m[3])};
}

if (typeof module !== 'undefined') {
  module.exports = {plural, failing, tagged, looseNode, isLinkNode, buildAdj, reach, chain, neighbours,
    filterView, collapseLinks, encodeHash, decodeHash, parseQuery};
}
