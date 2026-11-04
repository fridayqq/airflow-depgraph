// Tests for impact(): what the failing DAGs block and what ran on unvalidated data.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const core = require('../../src/airflow_depgraph/templates/graph-core.js');

const T0 = '2026-11-02T09:00:00Z', BEFORE = '2026-11-01T00:00:00Z', AFTER = '2026-11-02T10:00:00Z';
const dag = (id, extra) => ({id: 'dag:' + id, label: id, type: 'dag', tags: [], paused: false, down: 0, ...extra});
const link = (id, type, label) => ({id, type, label: label || id});
function data(nodes, edges) {
  return {nodes, edges, byId: new Map(nodes.map(n => [n.id, n]))};
}
const ids = list => list.map(h => h.id).sort();

// p (pipeline failure, run task failed) -> asset a -> c1 (hard, did not run), c2 (soft, ran),
// c1 -> asset b -> c3 (hard). c4 waits on p through a sensor.
function pipelineCase() {
  const nodes = [
    dag('p', {ran: T0, fail: {kind: 'pipeline', date: T0, outlets: {'asset:a': false}}}),
    link('asset:a', 'asset'), link('asset:b', 'asset'), link('sensor:c4:wait', 'sensor'),
    dag('c1', {ran: BEFORE}), dag('c2', {ran: AFTER}), dag('c3', {ran: BEFORE}), dag('c4', {ran: BEFORE}),
  ];
  const edges = [
    {s: 'dag:p', t: 'asset:a'}, {s: 'asset:a', t: 'dag:c1', strength: 'hard'}, {s: 'asset:a', t: 'dag:c2', strength: 'soft'},
    {s: 'dag:c1', t: 'asset:b'}, {s: 'asset:b', t: 'dag:c3', strength: 'hard'},
    {s: 'dag:p', t: 'sensor:c4:wait'}, {s: 'sensor:c4:wait', t: 'dag:c4'},
  ];
  return data(nodes, edges);
}

test('a missing outlet blocks the hard consumers and on through them; soft ones run anyway', () => {
  const im = core.impact(pipelineCase(), {});
  assert.deepEqual(im.roots, ['dag:p']);
  assert.deepEqual(im.blocked.sort(), ['dag:c1', 'dag:c3', 'dag:c4']);
  assert.deepEqual(im.unvalidated, []);
  assert.deepEqual(ids(im.byRoot.get('dag:p').blocked), ['dag:c1', 'dag:c3', 'dag:c4']);
  assert.deepEqual(im.byRoot.get('dag:p').blocked.find(h => h.id === 'dag:c3'), {id: 'dag:c3', hop: 2, via: 'asset:b'});
  assert.equal(im.ekind.get('dag:p'), 'pipeline');
  assert.deepEqual([im.hop.get('dag:p'), im.hop.get('asset:a'), im.hop.get('dag:c1'), im.hop.get('dag:c3')], [0, 0.5, 1, 2]);
  assert.ok(!im.hop.has('dag:c2'));
  assert.ok(im.links.every(l => l.kind === 'blocked'));
});

test('a consumer that ran since the failure is not blocked', () => {
  const d = pipelineCase();
  d.byId.get('dag:c1').ran = AFTER;
  const im = core.impact(d, {});
  assert.deepEqual(im.blocked.sort(), ['dag:c4']);     // c3 is behind c1, which ran
});

test('an unknown outlet counts as missing for a pipeline failure and produced for a tests one', () => {
  const d = pipelineCase();
  d.byId.get('dag:p').fail = {kind: 'pipeline', date: T0};
  assert.deepEqual(core.impact(d, {}).blocked.sort(), ['dag:c1', 'dag:c3', 'dag:c4']);
  d.byId.get('dag:p').fail = {kind: 'tests', date: T0};
  const im = core.impact(d, {});
  assert.deepEqual(im.blocked, ['dag:c4']);            // the sensor still waits for a success
  assert.deepEqual(im.unvalidated, ['dag:c2']);        // the only consumer that ran since
  assert.equal(im.ekind.get('dag:p'), 'pipeline');     // it blocks c4
});

// t (tests failed after the run task) -> asset a -> u1 (hard, ran), u2 (soft, ran), u3 (hard, did not run);
// u1 -> asset b -> u4 (ran) and -> f (failing itself, ran).
function testsCase() {
  const nodes = [
    dag('t', {ran: T0, fail: {kind: 'tests', date: T0, outlets: {'asset:a': true}}}),
    link('asset:a', 'asset'), link('asset:b', 'asset'),
    dag('u1', {ran: AFTER}), dag('u2', {ran: AFTER}), dag('u3', {ran: BEFORE}), dag('u4', {ran: AFTER}),
    dag('f', {ran: AFTER, fail: {kind: 'pipeline', date: AFTER, outlets: {}}}),
  ];
  const edges = [
    {s: 'dag:t', t: 'asset:a'}, {s: 'asset:a', t: 'dag:u1', strength: 'hard'}, {s: 'asset:a', t: 'dag:u2', strength: 'soft'},
    {s: 'asset:a', t: 'dag:u3', strength: 'hard'},
    {s: 'dag:u1', t: 'asset:b'}, {s: 'asset:b', t: 'dag:u4', strength: 'hard'}, {s: 'asset:b', t: 'dag:f', strength: 'hard'},
  ];
  return data(nodes, edges);
}

test('produced outlets of a tests failure: the consumers that ran since are unvalidated, on through them', () => {
  const im = core.impact(testsCase(), {});
  assert.deepEqual(im.roots.sort(), ['dag:f', 'dag:t']);
  assert.deepEqual(im.blocked, []);
  assert.deepEqual(im.unvalidated.sort(), ['dag:u1', 'dag:u2', 'dag:u4']);
  assert.equal(im.ekind.get('dag:t'), 'tests');
  assert.deepEqual(im.cause.get('dag:f'), ['dag:t']);  // f failed on unvalidated data: a consequence
  assert.equal(im.role.get('dag:f'), 'root');
  assert.deepEqual([im.hop.get('dag:t'), im.hop.get('dag:u1'), im.hop.get('dag:f')], [0, 1, 2]);
  assert.deepEqual(ids(im.byRoot.get('dag:t').unvalidated), ['dag:f', 'dag:u1', 'dag:u2', 'dag:u4']);
  assert.ok(im.links.every(l => l.kind === 'unvalidated'));
});

test('the kind filter drops roots; a dropped root can still be affected by another', () => {
  const im = core.impact(testsCase(), {kinds: {tests: true, pipeline: false}});
  assert.deepEqual(im.roots, ['dag:t']);
  assert.equal(im.role.get('dag:f'), 'unvalidated');
  const none = core.impact(testsCase(), {kinds: {tests: false, pipeline: true}});
  assert.deepEqual(none.roots, ['dag:f']);
  assert.deepEqual(none.unvalidated, []);
});

test('keep limits roots and consumers to what the filters allow', () => {
  const d = pipelineCase();
  const im = core.impact(d, {keep: new Set(['dag:p', 'asset:a', 'dag:c1'])});
  assert.deepEqual(im.blocked, ['dag:c1']);
});

test('impactKinds writes ekind on the nodes: a blocking tests failure becomes pipeline', () => {
  const d = pipelineCase();
  d.byId.get('dag:p').fail = {kind: 'tests', date: T0, outlets: {'asset:a': false}};
  core.impactKinds(d);
  assert.equal(d.byId.get('dag:p').fail.ekind, 'pipeline');
  assert.equal(core.failKind(d.byId.get('dag:p')), 'pipeline');
  assert.equal(core.failingOf(d.byId.get('dag:p'), {tests: true, pipeline: false}), false);
  const t = testsCase();
  core.impactKinds(t);
  assert.equal(t.byId.get('dag:t').fail.ekind, 'tests');
});

test('filterView onlyFail follows the kind filter', () => {
  const d = testsCase();
  d.edges = d.edges.filter(e => e.t !== 'dag:f');   // f is not in t's chain any more
  d.tags = [];
  const off = {onlyTags: false, onlyActive: false, conn: false, showGuards: false, blast: false, onlyFail: true};
  const all = core.filterView(d, off).keep;
  assert.ok(all.has('dag:t') && all.has('dag:f'));
  const only = core.filterView(d, {...off, kinds: {tests: false, pipeline: true}}).keep;
  assert.ok(!only.has('dag:t') && only.has('dag:f'));
});

test('the failure time is the failed run, not a newer run of the root that is still running', () => {
  const d = testsCase();
  const t = d.nodes.find(n => n.id === 'dag:t');
  t.ran = AFTER;   // running again since AFTER; it failed at T0
  const im = core.impact(d, {});
  assert.deepEqual(im.unvalidated.sort(), ['dag:u1', 'dag:u2', 'dag:u4']);   // u1, u2 ran between T0 and AFTER
});
