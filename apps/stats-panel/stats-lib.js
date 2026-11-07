// Stream manager stats panel: pure helpers (plan section 11). No DOM, no network.
// Loaded by Node for tests (module.exports) and inlined into the panel HTML (window.StatsLib).
(function (root) {
  'use strict';

  /** Decodes the base64 bytes of stats.json (UTF-8 JSON) into an object. */
  function decodeSnapshot(base64) {
    const bin = atob(base64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return JSON.parse(new TextDecoder('utf-8').decode(bytes));
  }

  /** Seconds from nowMs until an ISO time (negative when past); NaN for a bad time. */
  function secondsUntil(iso, nowMs) {
    const t = Date.parse(iso);
    return Number.isNaN(t) ? NaN : (t - nowMs) / 1000;
  }

  /** A countdown: "now", "45 s", "1:05", "1 h 05 min". */
  function formatCountdown(seconds) {
    if (!(seconds > 0)) return 'now';
    const s = Math.ceil(seconds);
    if (s < 60) return s + ' s';
    if (s < 3600) return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
    return Math.floor(s / 3600) + ' h ' + String(Math.floor((s % 3600) / 60)).padStart(2, '0') + ' min';
  }

  function trim(n) {
    return Number.isInteger(n) ? String(n) : String(Math.round(n * 10) / 10);
  }

  /** The gate's minimum time: "no wait", "30 s", "2 min", "4.5 min", "1.5 h". */
  function formatInterval(seconds) {
    if (!(seconds > 0)) return 'no wait';
    if (seconds < 60) return trim(seconds) + ' s';
    if (seconds < 3600) return trim(seconds / 60) + ' min';
    return trim(seconds / 3600) + ' h';
  }

  function formatUsd(x) {
    if (typeof x !== 'number' || !Number.isFinite(x)) return '?';
    return Number.isInteger(x) ? String(x) : x.toFixed(2);
  }

  /** "Gate · 10 min · $0". */
  function gateLabel(gate) {
    if (!gate) return 'Gate · unknown';
    return 'Gate · ' + formatInterval(gate.min_interval_s) + ' · $' + formatUsd(gate.min_donation_usd);
  }

  /** 0 at no wait / $0, rising to 1 at 10 min or $5 (for the chip's tint). */
  function gateSeverity(gate) {
    if (!gate) return 0;
    const m = (gate.min_interval_s || 0) / 600;
    const d = (gate.min_donation_usd || 0) / 5;
    return Math.max(0, Math.min(1, Math.max(m, d)));
  }

  /** True when the snapshot is older than maxAgeS (default 30 s) or unreadable. */
  function isStale(atIso, nowMs, maxAgeS) {
    const age = -secondsUntil(atIso, nowMs);
    return !(age <= (maxAgeS === undefined ? 30 : maxAgeS));
  }

  /** Chat status chip: {label, kind} with kind ok | held | filtered. */
  function statusChip(status) {
    if (status === 'forwarded') return { label: 'sent', kind: 'ok' };
    if (typeof status === 'string' && status.indexOf('held:') === 0) {
      return { label: 'held · ' + status.slice(5).replace('_', ' '), kind: 'held' };
    }
    return { label: String(status || 'unknown'), kind: 'filtered' };
  }

  const api = { decodeSnapshot, secondsUntil, formatCountdown, formatInterval, formatUsd, gateLabel, gateSeverity, isStale, statusChip };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.StatsLib = api;
})(typeof self !== 'undefined' ? self : this);
