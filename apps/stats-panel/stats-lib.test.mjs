// node --test apps/stats-panel/stats-lib.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const L = require('./stats-lib.js');

test('decodes UTF-8 base64 JSON', () => {
  const b64 = Buffer.from(JSON.stringify({ v: 1, text: 'héllo ✓' }), 'utf8').toString('base64');
  assert.deepEqual(L.decodeSnapshot(b64), { v: 1, text: 'héllo ✓' });
});

test('countdowns', () => {
  const now = Date.parse('2026-11-03T00:00:00.000Z');
  assert.equal(L.formatCountdown(L.secondsUntil('2026-11-03T00:00:45.000Z', now)), '45 s');
  assert.equal(L.formatCountdown(L.secondsUntil('2026-11-03T00:01:05.000Z', now)), '1:05');
  assert.equal(L.formatCountdown(3600 + 5 * 60), '1 h 05 min');
  assert.equal(L.formatCountdown(0), 'now');
  assert.equal(L.formatCountdown(-3), 'now');
  assert.equal(L.formatCountdown(NaN), 'now');
  assert.equal(L.formatCountdown(0.2), '1 s');
});

test('gate label and severity', () => {
  assert.equal(L.gateLabel({ min_interval_s: 600, min_donation_usd: 0 }), 'Gate · 10 min · $0');
  assert.equal(L.gateLabel({ min_interval_s: 0, min_donation_usd: 2.5 }), 'Gate · no wait · $2.50');
  assert.equal(L.gateLabel({ min_interval_s: 270, min_donation_usd: 1 }), 'Gate · 4.5 min · $1');
  assert.equal(L.gateLabel({ min_interval_s: 5400, min_donation_usd: 0 }), 'Gate · 1.5 h · $0');
  assert.equal(L.gateLabel({ min_interval_s: 30, min_donation_usd: 0 }), 'Gate · 30 s · $0');
  assert.equal(L.gateLabel(null), 'Gate · unknown');
  assert.equal(L.gateSeverity({ min_interval_s: 0, min_donation_usd: 0 }), 0);
  assert.equal(L.gateSeverity({ min_interval_s: 300, min_donation_usd: 0 }), 0.5);
  assert.equal(L.gateSeverity({ min_interval_s: 0, min_donation_usd: 100 }), 1);
});

test('staleness', () => {
  const now = Date.parse('2026-11-03T00:01:00.000Z');
  assert.equal(L.isStale('2026-11-03T00:00:45.000Z', now), false);
  assert.equal(L.isStale('2026-11-03T00:00:29.000Z', now), true);
  assert.equal(L.isStale('garbage', now), true);
  assert.equal(L.isStale(undefined, now), true);
});

test('status chips', () => {
  assert.deepEqual(L.statusChip('forwarded'), { label: 'sent', kind: 'ok' });
  assert.deepEqual(L.statusChip('held:global_cap'), { label: 'held · global cap', kind: 'held' });
  assert.deepEqual(L.statusChip('filtered'), { label: 'filtered', kind: 'filtered' });
  assert.deepEqual(L.statusChip(undefined), { label: 'unknown', kind: 'filtered' });
});

test('the built panel inlines the library and never uses innerHTML for data', async () => {
  const { readFile } = await import('node:fs/promises');
  const { build } = await import('./build.mjs');
  const html = await build();
  assert.ok(html.includes('function decodeSnapshot'), 'library inlined');
  assert.ok(!html.includes('/*STATS_LIB*/'), 'placeholder replaced');
  assert.ok(!/innerHTML|outerHTML|insertAdjacentHTML|document\.write/.test(html), 'no HTML injection sinks');
  assert.ok(!/\bfetch\(|WebSocket|XMLHttpRequest/.test(html), 'no network calls');
  const tpl = await readFile(new URL('./panel.html', import.meta.url), 'utf8');
  assert.ok(tpl.includes('stream-manager/stats.json'));
});
