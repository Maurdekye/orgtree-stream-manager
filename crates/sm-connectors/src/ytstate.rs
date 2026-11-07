//! YouTube state shared by the connector, the executor and the bridge (youtube-spec.md §5, §6):
//! the per-Pacific-day quota counter and the book of ban ids. Both are plain in-memory
//! values behind a mutex; sm-bin persists them through the store.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::Deserialize;

use crate::youtube::YtRequest;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The n-th Sunday (1-based) of a month, as days since the epoch.
fn nth_sunday(y: i64, m: i64, n: i64) -> i64 {
    let first = days_from_civil(y, m, 1);
    let weekday = (first + 4).rem_euclid(7); // 1970-01-01 was a Thursday; 0 = Sunday
    first + (7 - weekday) % 7 + 7 * (n - 1)
}

/// US Pacific time: UTC-8, UTC-7 from the second Sunday of March (02:00 local) to the first
/// Sunday of November (02:00 local).
fn pacific_offset_s(unix_s: i64) -> i64 {
    let (year, ..) = civil_from_days(unix_s.div_euclid(86_400));
    let dst_start = nth_sunday(year, 3, 2) * 86_400 + 10 * 3600; // 02:00 PST = 10:00Z
    let dst_end = nth_sunday(year, 11, 1) * 86_400 + 9 * 3600; // 02:00 PDT = 09:00Z
    if unix_s >= dst_start && unix_s < dst_end {
        -7 * 3600
    } else {
        -8 * 3600
    }
}

/// The calendar day (`YYYY-MM-DD`) in Pacific time, when YouTube's daily quota resets [doc].
pub fn pacific_day(unix_s: f64) -> String {
    let t = unix_s.floor() as i64;
    let local = t + pacific_offset_s(t);
    let (y, m, d) = civil_from_days(local.div_euclid(86_400));
    format!("{y:04}-{m:02}-{d:02}")
}

/// Units per request kind (youtube-spec.md §5). The documented values; `[youtube.costs]`
/// can override them in config.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct YtCosts {
    pub discovery: u64,
    pub list: u64,
    pub stream: u64,
    pub insert: u64,
    pub ban_insert: u64,
    pub delete: u64,
    pub transition: u64,
}

impl Default for YtCosts {
    fn default() -> Self {
        YtCosts { discovery: 1, list: 5, stream: 5, insert: 50, ban_insert: 200, delete: 50, transition: 50 }
    }
}

impl YtCosts {
    pub fn for_request(&self, r: &YtRequest) -> u64 {
        let p = r.path.split('?').next().unwrap_or("");
        match (r.method, p) {
            (_, "/liveBroadcasts/transition") => self.transition,
            ("GET", "/liveBroadcasts") => self.discovery,
            ("GET", "/liveChat/messages/stream") => self.stream,
            ("GET", "/liveChat/messages") => self.list,
            ("POST", "/liveChat/bans") => self.ban_insert,
            ("DELETE", _) => self.delete,
            ("POST", _) => self.insert,
            _ => self.list,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaLevel {
    Ok,
    /// 80% or more of the day's units are used.
    Warn80,
    /// 95% or more: replies on YouTube are paused.
    Low95,
}

struct QuotaState {
    day: String,
    used: u64,
}

/// Units spent today (a Pacific day), against the daily allowance.
pub struct YtQuota {
    limit: u64,
    costs: YtCosts,
    state: Mutex<QuotaState>,
}

impl YtQuota {
    pub fn new(limit: u64) -> Self {
        Self::with_costs(limit, YtCosts::default())
    }

    pub fn with_costs(limit: u64, costs: YtCosts) -> Self {
        YtQuota { limit, costs, state: Mutex::new(QuotaState { day: String::new(), used: 0 }) }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    fn roll(&self, now: f64, s: &mut QuotaState) {
        let day = pacific_day(now);
        if s.day != day {
            s.day = day;
            s.used = 0;
        }
    }

    pub fn record(&self, now: f64, units: u64) {
        let mut s = lock(&self.state);
        self.roll(now, &mut s);
        s.used = s.used.saturating_add(units);
    }

    /// Counts the cost of one request.
    pub fn record_request(&self, now: f64, r: &YtRequest) {
        self.record(now, self.costs.for_request(r));
    }

    pub fn used(&self, now: f64) -> u64 {
        let mut s = lock(&self.state);
        self.roll(now, &mut s);
        s.used
    }

    pub fn level(&self, now: f64) -> QuotaLevel {
        let used = self.used(now) as f64;
        let limit = self.limit.max(1) as f64;
        if used >= 0.95 * limit {
            QuotaLevel::Low95
        } else if used >= 0.8 * limit {
            QuotaLevel::Warn80
        } else {
            QuotaLevel::Ok
        }
    }

    /// (Pacific day, units used) as of `now`.
    pub fn snapshot(&self, now: f64) -> (String, u64) {
        let mut s = lock(&self.state);
        self.roll(now, &mut s);
        (s.day.clone(), s.used)
    }

    /// Restores a persisted count; a count from an earlier day is dropped on first use.
    pub fn restore(&self, day: &str, used: u64) {
        let mut s = lock(&self.state);
        s.day = day.to_string();
        s.used = used;
    }
}

/// (channel id, ban id, expires_at for a timeout).
pub type BanRow = (String, String, Option<f64>);

/// The ban ids YouTube returned, keyed by channel id, so a ban can be lifted (§6).
#[derive(Default)]
pub struct YtBans {
    map: Mutex<HashMap<String, (String, Option<f64>)>>,
}

impl YtBans {
    pub fn put(&self, channel: &str, ban_id: &str, expires_at: Option<f64>) {
        lock(&self.map).insert(channel.to_string(), (ban_id.to_string(), expires_at));
    }

    pub fn has(&self, channel: &str, now: f64) -> bool {
        lock(&self.map).get(channel).is_some_and(|(_, exp)| exp.is_none_or(|e| e > now))
    }

    /// Removes the record and returns its ban id, unless it was a timeout that has expired.
    pub fn take(&self, channel: &str, now: f64) -> Option<String> {
        let (id, exp) = lock(&self.map).remove(channel)?;
        exp.is_none_or(|e| e > now).then_some(id)
    }

    /// Like [`take`](Self::take), with the expiry: (ban id, expires_at). For an unban that may fail.
    pub fn take_record(&self, channel: &str, now: f64) -> Option<(String, Option<f64>)> {
        let (id, exp) = lock(&self.map).remove(channel)?;
        exp.is_none_or(|e| e > now).then_some((id, exp))
    }

    /// Puts a record taken with `take_record` back exactly as it was; dropped (false) if its expiry has passed.
    pub fn restore(&self, channel: &str, record: (String, Option<f64>), now: f64) -> bool {
        let (id, exp) = record;
        if exp.is_some_and(|e| e <= now) {
            return false;
        }
        lock(&self.map).insert(channel.to_string(), (id, exp));
        true
    }

    /// Drops records of timeouts that have expired; returns how many.
    pub fn prune(&self, now: f64) -> usize {
        let mut m = lock(&self.map);
        let before = m.len();
        m.retain(|_, (_, exp)| exp.is_none_or(|e| e > now));
        before - m.len()
    }

    pub fn export(&self) -> Vec<BanRow> {
        let mut rows: Vec<BanRow> = lock(&self.map).iter().map(|(c, (id, exp))| (c.clone(), id.clone(), *exp)).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    pub fn import(&self, rows: Vec<BanRow>) {
        let mut m = lock(&self.map);
        for (c, id, exp) in rows {
            m.insert(c, (id, exp));
        }
    }
}
