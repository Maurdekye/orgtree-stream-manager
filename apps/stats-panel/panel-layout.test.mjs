// node --test apps/stats-panel/panel-layout.test.mjs
// USER 2026-11-05 m87e53ef60dbe: the panel is watched, not used — one view, no tabs, nothing to click.
import test from 'node:test';
import assert from 'node:assert/strict';
import { build } from './build.mjs';

const html = await build();

test('no tabs or clickable controls', () => {
  assert.doesNotMatch(html, /role="tab/);
  assert.doesNotMatch(html, /<nav[\s>]/);
  assert.doesNotMatch(html, /<button[\s>]/);
  assert.doesNotMatch(html, /addEventListener\(\s*'click'/);
});

test('all four sections are always shown', () => {
  for (const id of ['chat', 'donations', 'limits', 'moderation']) {
    assert.match(html, new RegExp('<section[^>]*id="sec-' + id + '"'));
  }
  assert.doesNotMatch(html, /section\s*\{[^}]*display:\s*none/);
});

test('textContent-only rendering and no network', () => {
  assert.doesNotMatch(html, /innerHTML|outerHTML|insertAdjacentHTML|document\.write/);
  assert.doesNotMatch(html, /fetch\(|XMLHttpRequest|WebSocket|EventSource|https?:\/\//);
});

test('a sleeping stream shows a SLEEPING chip (auto-sleep plan rev 2)', () => {
  assert.match(html, /\.chip\.sleeping\s*\{/);
  assert.match(html, /st === 'sleeping'/);
});
