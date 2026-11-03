// Pure helpers of the dependency page: no DOM, no globals, inputs as arguments. render.py inlines
// this file into the page before the page script; node imports it for tests/js.

// "1 DAG", "2 DAGs".
function plural(n, one, many) { return `${n} ${n === 1 ? one : many}`; }

// Non-DAG nodes are links: assets, triggers and sensors.
function isLinkNode(node) { return node.type !== 'dag'; }
function failing(n) { return !!(n.fail && !n.fail.paused); }
// The kind of a failure for the page: ekind (set by impactKinds: a tests-only failure that blocks
// data is a pipeline failure) over the snapshot's kind.
function failKind(n) { return n.fail ? (n.fail.ekind || n.fail.kind || 'pipeline') : null; }
// failing, and of a kind that is switched on (kinds = {tests: bool, pipeline: bool}, or null for all).
function failingOf(n, kinds) { return failing(n) && (!kinds || kinds[failKind(n)] !== false); }
function tagged(n, tags) { return (n.tags || []).some(t => tags.includes(t)); }
// Unlinked DAGs, and DAGs linked only by deploy guards while guards are hidden.
function looseNode(n, guardsOn) { return !!(n.unlinked || (n.guard_only && !guardsOn)); }

// Adjacency maps {out, inn} from edges. Guard edges are left out unless opts.guards; soft asset
// edges (the DAG runs without the asset) are left out when opts.onlyHard.
function buildAdj(edges, nodesById, opts) {
  const guards = !!(opts && opts.guards), onlyHard = !!(opts && opts.onlyHard);
  const out = new Map(), inn = new Map();
  for (const e of edges) {
    if (!nodesById.has(e.s) || !nodesById.has(e.t)) continue;
    if (e.guard && !guards) continue;
    if (onlyHard && e.strength === 'soft') continue;
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

// The filters: tag, active, connected, guards, hard edges only, blast radius and only failing.
// data = {nodes, edges, byId, tags}; state = {onlyTags, onlyActive, conn, showGuards, onlyHard,
// blast, onlyFail, kinds}. Returns the kept node ids and the edges between them.
function filterView(data, state) {
  const failing = n => failingOf(n, state.kinds);
  const byId = data.byId, tags = data.tags || [];
  const onlyHard = !!state.onlyHard;
  const adj = buildAdj(data.edges, byId, {guards: state.showGuards, onlyHard});
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
  if (onlyHard) {  // an asset no kept DAG hard-depends on goes, then every node left without an edge
    for (const id of [...keep]) {
      const n = byId.get(id);
      if (n.type === 'asset' && !(adj.out.get(id) || []).some(x => keep.has(x))) keep.delete(id);
    }
    const linked = new Set();
    for (const e of data.edges) {
      if (keep.has(e.s) && keep.has(e.t) && e.strength !== 'soft' && !(e.guard && !guards)) { linked.add(e.s); linked.add(e.t); }
    }
    keep = new Set([...keep].filter(id => linked.has(id)));
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
  return {keep, edges: data.edges.filter(e => keep.has(e.s) && keep.has(e.t) && !(onlyHard && e.strength === 'soft'))};
}

// What the failing DAGs did to the rest: the DAGs they BLOCK and the DAGs that ran on UNVALIDATED data.
//
// A failed run blocks a consumer only when the outlet the consumer waits on was not produced:
// fail.outlets[link id] is false (true: produced; missing or null: unknown, which counts as not
// produced for a pipeline failure and as produced for a tests-only one; a sensor link on a
// failed run never passes). Hard asset edges, triggers and sensors carry the block, a soft
// consumer runs anyway; a blocked DAG produced nothing, so its hard consumers are blocked too.
// A consumer that ran since the failure (ran >= fail.date) is not blocked whatever the outlets.
// A tests-only failure whose outlets were produced marks the consumers that ran since as
// unvalidated, through every edge and on through the DAGs that ran since. A failing DAG reached
// this way is a consequence: still a root, with cause = the roots behind it.
//
// data = {nodes, edges, byId}; opts = {keep: Set of node ids the filters allow (null: all),
// guards, kinds}. Returns roots, per node role ('root' | 'blocked' | 'unvalidated'), hop (column:
// pure roots 0, links between columns at n - 0.5), the links to draw {s, t, kind}, cause, from (per
// affected node: [{root, kind, via}]), per root {blocked, unvalidated: [{id, hop, via}]}, and ekind
// (pipeline when the root blocks something).
function impact(data, opts) {
  const byId = data.byId, keep = opts.keep || null, kinds = opts.kinds || null;
  const inKeep = id => !keep || keep.has(id);
  const edges = data.edges.filter(e => inKeep(e.s) && inKeep(e.t));
  const adj = buildAdj(edges, byId, {guards: opts.guards});
  const strength = new Map(edges.map(e => [e.s + '|' + e.t, e.strength]));
  const roots = data.nodes.filter(n => n.type === 'dag' && inKeep(n.id) && failingOf(n, kinds)).map(n => n.id);
  const rootSet = new Set(roots);
  const at = n => Date.parse(n.ran || '');
  const ranSince = (id, since) => { const t = at(byId.get(id)); return !isNaN(t) && !isNaN(since) && t >= since; };
  const produced = (root, link) => {
    const o = root.fail.outlets ? root.fail.outlets[link] : undefined;
    if (o === true || o === false) return o;
    if (byId.get(link).type === 'sensor') return false;
    return root.fail.kind === 'tests';
  };
  // Consumers of x: [{id, via (link id or null), soft}], through chains of link nodes.
  const consumers = x => {
    const out = [], stack = [[x, null]], seen = new Set();
    while (stack.length) {
      const [a, via] = stack.pop();
      for (const b of adj.out.get(a) || []) {
        if (byId.get(b).type === 'dag') out.push({id: b, via, soft: strength.get(a + '|' + b) === 'soft'});
        else if (!seen.has(b)) { seen.add(b); stack.push([b, b]); }
      }
    }
    return out;
  };
  const role = new Map(roots.map(id => [id, 'root'])), cause = new Map(), byRoot = new Map(), links = new Map(), from = new Map();
  const addLink = (s, t, kind) => { const k = s + '|' + t; if (!links.has(k) || kind === 'blocked') links.set(k, {s, t, kind}); };
  const walk = (rootId, kind) => {
    const root = byId.get(rootId), since = at(root), hits = [];
    const seen = new Set([rootId]);
    let frontier = [rootId];
    for (let hop = 1; frontier.length; hop++) {
      const next = [];
      for (const x of frontier) {
        for (const c of consumers(x)) {
          const outlet = c.via || c.id;
          const ok = x === rootId ? (kind === 'blocked' ? !produced(root, outlet) : produced(root, outlet)) : true;
          if (!ok || seen.has(c.id)) continue;
          const ran = ranSince(c.id, since);
          if (kind === 'blocked' && (c.soft || ran || rootSet.has(c.id))) continue;
          if (kind === 'unvalidated' && !ran) continue;
          seen.add(c.id); next.push(c.id);
          hits.push({id: c.id, hop, via: c.via ? byId.get(c.via).label : null});
          if (c.via) { addLink(x, c.via, kind); addLink(c.via, c.id, kind); } else addLink(x, c.id, kind);
          if (!from.has(c.id)) from.set(c.id, []);
          from.get(c.id).push({root: rootId, kind, via: c.via || null});
          if (rootSet.has(c.id)) { if (!cause.has(c.id)) cause.set(c.id, []); cause.get(c.id).push(rootId); }
          else if (role.get(c.id) !== 'blocked') role.set(c.id, kind);
        }
      }
      frontier = next;
    }
    return hits;
  };
  for (const id of roots) {
    const blocked = walk(id, 'blocked');
    const unvalidated = byId.get(id).fail.kind === 'tests' ? walk(id, 'unvalidated') : [];
    byRoot.set(id, {blocked, unvalidated});
  }
  const ekind = new Map(roots.map(id => [id, byRoot.get(id).blocked.length ? 'pipeline' : (byId.get(id).fail.kind || 'pipeline')]));
  // Columns: a breadth-first walk over the drawn links from the roots nothing caused.
  const hop = new Map(roots.filter(id => !cause.has(id)).map(id => [id, 0]));
  const lout = new Map();
  for (const l of links.values()) { if (!lout.has(l.s)) lout.set(l.s, []); lout.get(l.s).push(l.t); }
  let frontier = [...hop.keys()];
  for (let h = 1; frontier.length; h++) {
    const next = [];
    for (const x of frontier) for (const y of lout.get(x) || []) {
      if (hop.has(y)) continue;
      if (byId.get(y).type === 'dag') { hop.set(y, h); next.push(y); } else { hop.set(y, h - 0.5); frontier.push(y); }
    }
    frontier = next;
  }
  roots.forEach(id => { if (!hop.has(id)) hop.set(id, 0); });   // a cycle of failures: no pure root
  return {roots, role, hop, links: [...links.values()], cause, from, byRoot, ekind,
    blocked: [...role].filter(([, r]) => r === 'blocked').map(([id]) => id),
    unvalidated: [...role].filter(([, r]) => r === 'unvalidated').map(([id]) => id)};
}

// Sets fail.ekind on every failing DAG node: pipeline when the failure blocks a DAG anywhere in the
// snapshot, else the snapshot's kind. Done once at load, so rings, badges and the kind filter agree.
function impactKinds(data) {
  const im = impact(data, {keep: null, guards: true, kinds: null});
  data.nodes.forEach(n => { if (n.fail) n.fail.ekind = im.ekind.get(n.id) || n.fail.kind || 'pipeline'; });
  return im;
}

// Collapse dag -> link(s) -> dag into dag -> dag; via = the last link on the way. Returns the DAG ids
// among nodes and the collapsed edges.
function collapseLinks(nodes, edges, byId) {
  const adj = new Map(), coll = new Map();
  edges.forEach(e => { if (!adj.has(e.s)) adj.set(e.s, []); adj.get(e.s).push({t: e.t, soft: e.strength === 'soft'}); });
  // A collapsed edge is soft when its last hop (link -> DAG) is; a hard path to the same DAG wins.
  const add = (s, t, via, x, soft) => {
    const k = s + '|' + t, f = {inf: !!byId.get(x || s).inferred, guard: x ? byId.get(x).kind === 'guard' : false};
    if (s === t) return;
    const cur = coll.get(k);
    if (!cur) coll.set(k, {s, t, via, ...f, ...(x ? {strength: soft ? 'soft' : 'hard'} : {})});
    else if (!f.inf && !f.guard) Object.assign(cur, {via, inf: false, guard: false, ...(x && !soft ? {strength: 'hard'} : {})});  // an explicit path wins
  };
  edges.forEach(e => {
    if (byId.get(e.s).type !== 'dag') return;
    if (byId.get(e.t).type === 'dag') { add(e.s, e.t, null, null, false); return; }
    const seen = new Set([e.t]), stack = [e.t];
    while (stack.length) {
      const x = stack.pop();
      for (const {t: y, soft} of adj.get(x) || []) {
        if (byId.get(y).type === 'dag') add(e.s, y, byId.get(x).label, x, soft);
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
  module.exports = {plural, failing, failKind, failingOf, tagged, looseNode, isLinkNode, buildAdj, reach, chain, neighbours,
    filterView, impact, impactKinds, collapseLinks, encodeHash, decodeHash, parseQuery, facetOf, detectFacet};
}
