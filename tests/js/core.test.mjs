// Tests for the page's pure helpers (src/airflow_depgraph/templates/graph-core.js). Run: node --test tests/js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const core = require('../../src/airflow_depgraph/templates/graph-core.js');
const SEARCH_RE = /^(?:(\d*)\+)?(.+?)(?:\+(\d*))?$/;   // the page's search pattern (render.SEARCH_PATTERN)

test('parseQuery: upstream, downstream, depth and plain text', () => {
  assert.deepEqual(core.parseQuery('+dag_2', SEARCH_RE),
    {text: 'dag_2', plus: true, up: true, upDepth: Infinity, down: false, downDepth: Infinity});
  assert.deepEqual(core.parseQuery('12_hourly+', SEARCH_RE),
    {text: '12_hourly', plus: true, up: false, upDepth: Infinity, down: true, downDepth: Infinity});
  assert.deepEqual(core.parseQuery('2+dag+3', SEARCH_RE),
    {text: 'dag', plus: true, up: true, upDepth: 2, down: true, downDepth: 3});
  assert.deepEqual(core.parseQuery('dag', SEARCH_RE), {text: 'dag', plus: false});
  assert.equal(core.parseQuery('   ', SEARCH_RE), null);
});

// a -> b -> c -> d through DAG nodes, with an asset between a and b (a hop through it is free).
const nodes = [
  {id: 'dag:a', type: 'dag'}, {id: 'dag:b', type: 'dag'}, {id: 'dag:c', type: 'dag'}, {id: 'dag:d', type: 'dag'},
  {id: 'asset:x', type: 'asset', label: 'x'},
];
const byId = new Map(nodes.map(n => [n.id, n]));

test('reach respects the depth in DAG hops; links in between are free', () => {
  const adj = new Map([['dag:a', ['asset:x']], ['asset:x', ['dag:b']], ['dag:b', ['dag:c']], ['dag:c', ['dag:d']]]);
  assert.deepEqual([...core.reach(['dag:a'], adj, 1, byId)].sort(), ['asset:x', 'dag:a', 'dag:b']);
  assert.deepEqual([...core.reach(['dag:a'], adj, 2, byId)].sort(), ['asset:x', 'dag:a', 'dag:b', 'dag:c']);
  assert.equal(core.reach(['dag:a'], adj, Infinity, byId).size, 5);
});

// d1 -> asset a1 -> d3 ; d2 -> asset a2 -> d3 ; d4 is unlinked.
const view = {
  nodes: [
    {id: 'dag:d1', type: 'dag', tags: ['t'], paused: false},
    {id: 'dag:d2', type: 'dag', tags: [], paused: true},
    {id: 'dag:d3', type: 'dag', tags: ['t'], paused: false},
    {id: 'dag:d4', type: 'dag', tags: ['t'], paused: false, unlinked: true},
    {id: 'asset:a1', type: 'asset', label: 'a1'},
    {id: 'asset:a2', type: 'asset', label: 'a2'},
  ],
  edges: [
    {s: 'dag:d1', t: 'asset:a1', guard: false}, {s: 'asset:a1', t: 'dag:d3', guard: false},
    {s: 'dag:d2', t: 'asset:a2', guard: false}, {s: 'asset:a2', t: 'dag:d3', guard: false},
  ],
  byId: new Map([
    ['dag:d1', {id: 'dag:d1', type: 'dag', tags: ['t'], paused: false}],
    ['dag:d2', {id: 'dag:d2', type: 'dag', tags: [], paused: true}],
    ['dag:d3', {id: 'dag:d3', type: 'dag', tags: ['t'], paused: false}],
    ['dag:d4', {id: 'dag:d4', type: 'dag', tags: ['t'], paused: false, unlinked: true}],
    ['asset:a1', {id: 'asset:a1', type: 'asset', label: 'a1'}],
    ['asset:a2', {id: 'asset:a2', type: 'asset', label: 'a2'}],
  ]),
  tags: ['t'],
};
const off = {onlyTags: false, onlyActive: false, conn: false, showGuards: false, blast: false, onlyFail: false};
const kept = (state) => [...core.filterView(view, {...off, ...state}).keep].sort();

test('filterView: no filters keeps every linked DAG and both links', () => {
  assert.deepEqual(kept({}), ['asset:a1', 'asset:a2', 'dag:d1', 'dag:d2', 'dag:d3']);
});

test('filterView: tag filter drops untagged DAGs and the links left with one neighbour', () => {
  assert.deepEqual(kept({onlyTags: true}), ['asset:a1', 'dag:d1', 'dag:d3']);
});

test('filterView: active filter drops paused DAGs', () => {
  assert.deepEqual(kept({onlyActive: true}), ['asset:a1', 'dag:d1', 'dag:d3']);
});

test('filterView: tags and active together', () => {
  assert.deepEqual(kept({onlyTags: true, onlyActive: true}), ['asset:a1', 'dag:d1', 'dag:d3']);
});

test('filterView: connected brings back neighbours that pass the filters', () => {
  assert.deepEqual(kept({onlyTags: true, conn: true}), ['asset:a1', 'asset:a2', 'dag:d1', 'dag:d2', 'dag:d3']);
});

test('filterView: returns the edges between kept nodes only', () => {
  const edges = core.filterView(view, {...off, onlyTags: true}).edges;
  assert.deepEqual(edges.map(e => `${e.s}>${e.t}`).sort(), ['asset:a1>dag:d3', 'dag:d1>asset:a1']);
});

// d1 -> a1 -> a2 -> d3 (two link nodes), and a direct d1 -> d4.
test('collapseLinks: two link nodes collapse to one DAG edge, direct DAG edges stay', () => {
  const b = new Map([
    ['dag:d1', {id: 'dag:d1', type: 'dag'}], ['dag:d3', {id: 'dag:d3', type: 'dag'}],
    ['dag:d4', {id: 'dag:d4', type: 'dag'}],
    ['asset:a1', {id: 'asset:a1', type: 'asset', label: 'a1'}],
    ['asset:a2', {id: 'asset:a2', type: 'asset', label: 'a2', inferred: true}],
  ]);
  const edges = [
    {s: 'dag:d1', t: 'asset:a1'}, {s: 'asset:a1', t: 'asset:a2'}, {s: 'asset:a2', t: 'dag:d3'},
    {s: 'dag:d1', t: 'dag:d4'},
  ];
  const out = core.collapseLinks(['dag:d1', 'dag:d3', 'dag:d4', 'asset:a1', 'asset:a2'], edges, b);
  assert.deepEqual(out.keep.sort(), ['dag:d1', 'dag:d3', 'dag:d4']);
  const via = e => `${e.s}>${e.t} via ${e.via} inf=${e.inf}`;
  assert.deepEqual(out.edges.map(via).sort(), ['dag:d1>dag:d3 via a2 inf=true', 'dag:d1>dag:d4 via null inf=false']);
});

test('encodeHash and decodeHash round-trip', () => {
  const state = {q: 'dag_a+', flags: ['onlyFail', 'hl', 'allLinks', 'conn'], sel: 'dag:x y'};
  const str = core.encodeHash(state);
  assert.equal(str, 'q=dag_a%2B,onlyFail,hl,allLinks,conn,sel=dag%3Ax%20y');
  assert.deepEqual(core.decodeHash('#' + str), {q: 'dag_a+', flags: ['onlyFail', 'hl', 'allLinks', 'conn'], v: [], sel: 'dag:x y'});
  assert.equal(core.encodeHash(core.decodeHash('#' + str)), str);
  assert.equal(core.encodeHash({q: '', flags: [], sel: null}), '');
});

test('hash carries the chosen facet values, commas and bars included', () => {
  const str = core.encodeHash({q: '', flags: ['blast'], v: ['Customer & Booking', 'a,b|c'], sel: null});
  assert.equal(str, 'blast,v=Customer%20%26%20Booking|a%2Cb%7Cc');
  assert.deepEqual(core.decodeHash(str).v, ['Customer & Booking', 'a,b|c']);
  assert.deepEqual(core.decodeHash('v=').v, []);
});

test('facetOf counts the values of one tag prefix among DAGs, most first', () => {
  const nodes = [
    {id: 'dag:a', type: 'dag', tags: ['vertical:Sales', 'prod']},
    {id: 'dag:b', type: 'dag', tags: ['vertical:Sales', 'vertical:Finance']},
    {id: 'dag:c', type: 'dag', tags: ['vertical:Finance']},
    {id: 'dag:d', type: 'dag', tags: ['vertical:Ops', 'vertical:']},
    {id: 'asset:1', type: 'asset', tags: ['vertical:Sales']},
  ];
  assert.deepEqual(core.facetOf(nodes, 'vertical:'), {prefix: 'vertical:', values: [
    {name: 'Finance', count: 2}, {name: 'Sales', count: 2}, {name: 'Ops', count: 1}]});
  assert.equal(core.facetOf(nodes, ''), null);
});

test('detectFacet picks the colon prefix with at least two values and the most DAGs', () => {
  const nodes = [
    {id: 'dag:a', type: 'dag', tags: ['vertical:Sales', 'team:x', 'prod']},
    {id: 'dag:b', type: 'dag', tags: ['vertical:Finance', 'team:x']},
    {id: 'dag:c', type: 'dag', tags: ['team:x', 'env:prod']},
    {id: 'dag:d', type: 'dag', tags: ['env:uat', ':odd', 'trailing:']},
  ];
  assert.equal(core.detectFacet(nodes), 'env:');     // team: has one value; env: and vertical: tie on 2 DAGs and 2 values, env: sorts first
  const tied = [
    {id: 'dag:a', type: 'dag', tags: ['vertical:Sales', 'subvertical:Web']},
    {id: 'dag:b', type: 'dag', tags: ['vertical:Sales', 'subvertical:Shop']},
    {id: 'dag:c', type: 'dag', tags: ['vertical:Finance', 'subvertical:Ledger']},
  ];
  assert.equal(core.detectFacet(tied), 'vertical:');  // same DAGs, fewer values wins
  assert.equal(core.detectFacet([{id: 'dag:a', type: 'dag', tags: ['prod']}]), null);
});

test('decodeHash keeps a broken value raw and reads no flags from an empty hash', () => {
  assert.equal(core.decodeHash('#q=bad%ZZ').q, 'bad%ZZ');
  assert.deepEqual(core.decodeHash(''), {q: null, flags: [], v: [], sel: null});
});

test('plural', () => {
  assert.equal(core.plural(1, 'DAG', 'DAGs'), '1 DAG');
  assert.equal(core.plural(0, 'DAG', 'DAGs'), '0 DAGs');
  assert.equal(core.plural(2, 'DAG', 'DAGs'), '2 DAGs');
});

// Guard edges: dag:b -> dag:a is a deploy guard; dag:a -> dag:b is a normal edge.
test('buildAdj leaves guard edges out by default and keeps them with {guards: true}', () => {
  const edges = [{s: 'dag:a', t: 'dag:b', guard: false}, {s: 'dag:b', t: 'dag:a', guard: true}];
  const byNode = new Map([['dag:a', {}], ['dag:b', {}]]);
  const off = core.buildAdj(edges, byNode, {guards: false});
  assert.deepEqual(off.out.get('dag:a'), ['dag:b']);
  assert.equal(off.out.has('dag:b'), false);
  assert.equal(off.inn.has('dag:a'), false);
  const on = core.buildAdj(edges, byNode, {guards: true});
  assert.deepEqual(on.out.get('dag:b'), ['dag:a']);
  assert.deepEqual(on.inn.get('dag:a'), ['dag:b']);
});

function viewOf(nodes, edges, tags = []) {
  return {nodes, edges, byId: new Map(nodes.map(n => [n.id, n])), tags};
}
const base = {onlyTags: false, onlyActive: false, conn: false, showGuards: false, blast: false, onlyFail: false};

test('filterView: guard link node and its edges appear only with showGuards', () => {
  const view = viewOf(
    [{id: 'dag:a', type: 'dag'}, {id: 'dag:b', type: 'dag'}, {id: 'sensor:g', type: 'sensor', kind: 'guard'}],
    [{s: 'dag:a', t: 'sensor:g', guard: true}, {s: 'sensor:g', t: 'dag:b', guard: true}]);
  const off = core.filterView(view, {...base, showGuards: false});
  assert.deepEqual([...off.keep].sort(), ['dag:a', 'dag:b']);
  assert.deepEqual(off.edges, []);
  const on = core.filterView(view, {...base, showGuards: true});
  assert.deepEqual([...on.keep].sort(), ['dag:a', 'dag:b', 'sensor:g']);
  assert.equal(on.edges.length, 2);
});

test('filterView: only failing keeps the failing DAG, its chain, and drops unrelated DAGs', () => {
  const view = viewOf(
    [{id: 'dag:u', type: 'dag'}, {id: 'asset:y', type: 'asset'}, {id: 'dag:f', type: 'dag', fail: {kind: 'pipeline', paused: false}},
     {id: 'asset:x', type: 'asset'}, {id: 'dag:d', type: 'dag'}, {id: 'dag:z', type: 'dag'}],
    [{s: 'dag:u', t: 'asset:y', guard: false}, {s: 'asset:y', t: 'dag:f', guard: false},
     {s: 'dag:f', t: 'asset:x', guard: false}, {s: 'asset:x', t: 'dag:d', guard: false}]);
  const out = core.filterView(view, {...base, onlyFail: true});
  assert.deepEqual([...out.keep].sort(), ['asset:x', 'asset:y', 'dag:d', 'dag:f', 'dag:u']);
  assert.equal(out.edges.length, 4);
});
