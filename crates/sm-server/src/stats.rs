//! Plan §11: the stats snapshot (`<org scratch>/stream-manager/stats.json`) that the
//! Orgtree app panel reads with the files SDK every 5 s. Bounded lists, a size cap that
//! fits one 64 KiB read, and atomic replacement so a reader never sees half a file.

use std::collections::VecDeque;
use std::path::Path;

use serde_json::{json, Map, Value};

use crate::iso8601;

/// One files-SDK read is at most 65,536 bytes; stay well under it.
pub const MAX_BYTES: usize = 60_000;
const CHAT_KEEP: usize = 150;
const DONATIONS_KEEP: usize = 50;
const MODERATION_KEEP: usize = 50;
const GATE_HISTORY_KEEP: usize = 50;
/// At most this many rate-limit rows (the caller sorts them soonest first).
const LIMITS_KEEP: usize = 200;
/// Viewer names and messages in the snapshot are outside input (review round 1, finding 11).
pub const TRUST: &str = "untrusted-viewer-content";

#[derive(Debug, Clone, PartialEq)]
pub struct LimitRow {
    pub viewer: String,
    pub name: String,
    pub platform: String,
    pub credit_usd: f64,
    pub cooldown_s: f64,
    pub max_chars: u32,
    pub next_allowed_at: Option<f64>,
    pub held_until: Option<f64>,
}

#[derive(Debug, Default)]
pub struct Stats {
    chat: VecDeque<Value>,
    donations: VecDeque<Value>,
    moderation: VecDeque<Value>,
    gate_history: VecDeque<Value>,
    state: Value,
    gate: Value,
    health: Value,
    limits: Vec<LimitRow>,
    session_usd: f64,
    by_source: Map<String, Value>,
    /// (Pacific day, units used) of YouTube's daily quota, when YouTube is configured.
    youtube_quota: Option<(String, u64)>,
}

fn push(q: &mut VecDeque<Value>, v: Value, keep: usize) {
    q.push_back(v);
    while q.len() > keep {
        q.pop_front();
    }
}

fn opt_time(t: Option<f64>) -> Value {
    t.map_or(Value::Null, |x| json!(iso8601(x)))
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    /// `status`: forwarded | held:<reason> | filtered.
    pub fn chat(&mut self, ts: f64, platform: &str, viewer: &str, text: &str, status: &str, flags: Vec<String>) {
        push(
            &mut self.chat,
            json!({"ts": iso8601(ts), "platform": platform, "viewer": viewer, "text": text, "status": status, "flags": flags}),
            CHAT_KEEP,
        );
    }

    /// `usd` None means the donation couldn't be valued: listed, not totalled.
    pub fn donation(&mut self, ts: f64, platform: Option<&str>, source: &str, donor: &str, usd: Option<f64>, text: Option<&str>) {
        let usd = usd.filter(|x| x.is_finite() && *x > 0.0);
        if let Some(u) = usd {
            self.session_usd += u;
            let prev = self.by_source.get(source).and_then(Value::as_f64).unwrap_or(0.0);
            self.by_source.insert(source.to_string(), json!(prev + u));
        }
        push(
            &mut self.donations,
            json!({"ts": iso8601(ts), "platform": platform, "source": source, "donor": donor, "usd": usd, "text": text}),
            DONATIONS_KEEP,
        );
    }

    pub fn moderation(&mut self, ts: f64, action: &str, target: &str, by: &str, reason: &str) {
        push(
            &mut self.moderation,
            json!({"ts": iso8601(ts), "action": action, "target": target, "by": by, "reason": reason}),
            MODERATION_KEEP,
        );
    }

    pub fn gate_change(&mut self, ts: f64, value: &str, from: f64, to: f64, by: &str, reason: &str) {
        push(
            &mut self.gate_history,
            json!({"ts": iso8601(ts), "value": value, "from": from, "to": to, "by": by, "reason": reason}),
            GATE_HISTORY_KEEP,
        );
    }

    pub fn set_state(&mut self, v: Value) {
        self.state = v;
    }

    pub fn set_gate(&mut self, v: Value) {
        self.gate = v;
    }

    pub fn set_health(&mut self, v: Value) {
        self.health = v;
    }

    /// YouTube's quota use today (youtube-spec.md section 5); shown in the snapshot under `youtube`.
    pub fn set_youtube_quota(&mut self, day: &str, used: u64) {
        self.youtube_quota = Some((day.to_string(), used));
    }

    /// Viewers currently waiting (cooldown) or held; computed by the caller each tick.
    pub fn set_limits(&mut self, mut rows: Vec<LimitRow>) {
        rows.truncate(LIMITS_KEEP);
        self.limits = rows;
    }

    #[allow(clippy::too_many_arguments)]
    fn build(&self, now: f64, chat: &[Value], donations: &[Value], moderation: &[Value], gates: &[Value], limits: &[LimitRow], truncated: bool) -> Value {
        let limits: Vec<Value> = limits
            .iter()
            .map(|l| {
                json!({"viewer": l.viewer, "name": l.name, "platform": l.platform, "credit_usd": l.credit_usd,
                       "cooldown_s": l.cooldown_s, "max_chars": l.max_chars,
                       "next_allowed_at": opt_time(l.next_allowed_at), "held_until": opt_time(l.held_until)})
            })
            .collect();
        let mut v = json!({
            "v": 1,
            "trust": TRUST,
            "at": iso8601(now),
            "state": self.state,
            "gate": self.gate,
            "gate_history": gates,
            "chat": chat,
            "donations": donations,
            "totals": {"session_usd": self.session_usd, "by_source": self.by_source},
            "limits": limits,
            "moderation": moderation,
            "health": self.health,
        });
        if let Some((day, used)) = &self.youtube_quota {
            v["youtube"] = json!({"quota_used": used, "quota_day": day});
        }
        if truncated {
            v["truncated"] = json!(true);
        }
        v
    }

    /// The snapshot, dropping the oldest chat, then donations, moderation and gate
    /// history entries until it fits in [`MAX_BYTES`] (then `truncated: true`).
    pub fn snapshot(&self, now: f64) -> Value {
        let mut chat: Vec<Value> = self.chat.iter().cloned().collect();
        let mut donations: Vec<Value> = self.donations.iter().cloned().collect();
        let mut moderation: Vec<Value> = self.moderation.iter().cloned().collect();
        let mut gates: Vec<Value> = self.gate_history.iter().cloned().collect();
        let mut limits: Vec<LimitRow> = self.limits.clone();
        let mut truncated = false;
        loop {
            let v = self.build(now, &chat, &donations, &moderation, &gates, &limits, truncated);
            let size = serde_json::to_vec(&v).map(|b| b.len()).unwrap_or(usize::MAX);
            if size <= MAX_BYTES {
                return v;
            }
            truncated = true;
            let list = if chat.len() > 1 {
                &mut chat
            } else if !donations.is_empty() {
                &mut donations
            } else if !moderation.is_empty() {
                &mut moderation
            } else if !gates.is_empty() {
                &mut gates
            } else if limits.len() > 1 {
                // Keep the soonest; drop from the end.
                let n = (limits.len() / 10).max(1);
                limits.truncate(limits.len() - n);
                continue;
            } else if !chat.is_empty() {
                &mut chat
            } else {
                return v; // nothing left to drop
            };
            // Drop a tenth (at least one) of the oldest entries per round.
            let n = (list.len() / 10).max(1);
            list.drain(..n);
        }
    }

    /// The snapshot as JSON bytes (take these under the bridge lock, write outside it).
    pub fn snapshot_bytes(&self, now: f64) -> std::io::Result<Vec<u8>> {
        serde_json::to_vec(&self.snapshot(now)).map_err(std::io::Error::other)
    }

    /// Writes the snapshot to a temp file beside `path`, then renames it over `path`.
    /// Returns the bytes written.
    pub fn write_atomic(&self, now: f64, path: &Path) -> std::io::Result<usize> {
        write_bytes_atomic(path, &self.snapshot_bytes(now)?)
    }
}

/// Temp file plus rename, retried briefly while a reader holds the file on Windows.
pub fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<usize> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    std::fs::write(&tmp, bytes)?;
    // A reader can briefly hold the file open on Windows; retry the replace a few times.
    let mut last = None;
    for _ in 0..5 {
        match std::fs::rename(&tmp, path) {
            Ok(()) => return Ok(bytes.len()),
            Err(e) => {
                last = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    }
    let _ = std::fs::remove_file(&tmp);
    Err(last.unwrap_or_else(|| std::io::Error::other("rename failed")))
}
