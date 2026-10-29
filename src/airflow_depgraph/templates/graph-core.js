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
  if (state.v && state.v.length) parts.push('v=' + state.v.map(encodeURIComponent).join('|'));
  if (state.sel) parts.push('sel=' + encodeURIComponent(state.sel));
  return parts.join(',');
}

// Inverse of encodeHash: {q, sel} (null when absent), v (the chosen facet values) and the other
// tokens as flags. A value that does not decode is kept raw.
function decodeHash(str) {
  const safe = s => { try { return decodeURIComponent(s); } catch (e) { return s; } };
  const out = {q: null, flags: [], v: [], sel: null};
  for (const t of String(str || '').replace(/^#/, '').split(',').filter(Boolean)) {
    if (t.startsWith('q=')) out.q = safe(t.slice(2));
    else if (t.startsWith('sel=')) out.sel = safe(t.slice(4));
    else if (t.startsWith('v=')) out.v = t.slice(2).split('|').filter(Boolean).map(safe);
    else out.flags.push(t);
  }
  return out;
}

// A tag facet: tags shaped "<prefix><value>", e.g. vertical:Sales. facetOf lists the values among
// the DAG nodes with how many DAGs carry each, most first. detectFacet picks the prefix (up to and
// including the first ":") that has at least two values and covers the most DAGs; on a tie the one
// with fewer values (vertical: over subvertical:), then the first by name; null when there is none.
function facetOf(nodes, prefix) {
  if (!prefix) return null;
  const counts = new Map();
  for (const n of nodes) {
    if (n.type !== 'dag') continue;
    for (const t of n.tags || []) {
      if (!t.startsWith(prefix) || t.length === prefix.length) continue;
      const v = t.slice(prefix.length);
      counts.set(v, (counts.get(v) || 0) + 1);
    }
  }
  const values = [...counts].map(([name, count]) => ({name, count}))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  return {prefix, values};
}
function detectFacet(nodes) {
  const seen = new Map();  // prefix -> {dags: Set, values: Set}
  for (const n of nodes) {
    if (n.type !== 'dag') continue;
    for (const t of n.tags || []) {
      const i = t.indexOf(':');
      if (i <= 0 || i === t.length - 1) continue;
      const p = t.slice(0, i + 1);
      if (!seen.has(p)) seen.set(p, {dags: new Set(), values: new Set()});
      seen.get(p).dags.add(n.id);
      seen.get(p).values.add(t.slice(i + 1));
    }
  }
  let best = null;
  for (const [p, s] of seen) {
    if (s.values.size < 2) continue;
    const better = !best || s.dags.size > best.n || (s.dags.size === best.n && (s.values.size < best.v || (s.values.size === best.v && p < best.p)));
    if (better) best = {p, n: s.dags.size, v: s.values.size};
  }
  return best ? best.p : null;
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
    filterView, collapseLinks, encodeHash, decodeHash, parseQuery, facetOf, detectFacet};
}
