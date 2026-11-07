//! Stream manager storage (plan §2): one SQLite file with the event log, per-subscriber
//! replay cursors (§4.1), viewers, the credit ledger and an append-only audit.
//! Times are seconds as `f64` (Unix time; credit grants use effective time).

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq)]
pub struct StoredEvent {
    pub seq: u64,
    pub ts: f64,
    pub kind: String,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ViewerRow {
    pub first_seen: f64,
    pub paid_sub: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditRow {
    pub ts: f64,
    pub actor: String,
    pub action: String,
    pub detail: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PruneReport {
    pub chat_deleted: usize,
    pub donation_texts_redacted: usize,
}

const SCHEMA: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS events (
    seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts   REAL NOT NULL,
    kind TEXT NOT NULL,
    body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_kind_ts ON events(kind, ts);
CREATE TABLE IF NOT EXISTS cursors (
    subscriber TEXT PRIMARY KEY,
    last_seq   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS viewers (
    id         TEXT PRIMARY KEY,
    first_seen REAL NOT NULL,
    paid_sub   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS credit (
    viewer TEXT NOT NULL,
    t_eff  REAL NOT NULL,
    usd    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS credit_viewer ON credit(viewer);
CREATE TABLE IF NOT EXISTS audit (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     REAL NOT NULL,
    actor  TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS yt_bans (
    channel    TEXT PRIMARY KEY,
    ban_id     TEXT NOT NULL,
    expires_at REAL
);
CREATE TABLE IF NOT EXISTS yt_quota (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    day  TEXT NOT NULL,
    used INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS audit_no_update BEFORE UPDATE ON audit
    BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END;
CREATE TRIGGER IF NOT EXISTS audit_no_delete BEFORE DELETE ON audit
    BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END;
"#;

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        Self::init(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        // journal_mode returns a row ("wal", or "memory" for in-memory databases).
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Store { conn })
    }

    /// Appends an event and returns its sequence number. AUTOINCREMENT keeps numbers
    /// increasing even after old rows are pruned.
    pub fn append_event(&self, ts: f64, kind: &str, body: &Value) -> Result<u64> {
        self.conn.execute("INSERT INTO events (ts, kind, body) VALUES (?1, ?2, ?3)", params![ts, kind, body.to_string()])?;
        Ok(self.conn.last_insert_rowid() as u64)
    }

    /// The highest sequence number ever issued (0 when none).
    pub fn head_seq(&self) -> Result<u64> {
        let from_seq: Option<i64> =
            self.conn.query_row("SELECT seq FROM sqlite_sequence WHERE name = 'events'", [], |r| r.get(0)).optional()?;
        Ok(from_seq.unwrap_or(0).max(0) as u64)
    }

    pub fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<StoredEvent>> {
        let mut st = self.conn.prepare_cached("SELECT seq, ts, kind, body FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2")?;
        let rows = st.query_map(params![after_seq as i64, limit as i64], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, ts, kind, body) = row?;
            out.push(StoredEvent { seq: seq as u64, ts, kind, body: serde_json::from_str(&body)? });
        }
        Ok(out)
    }

    pub fn cursor(&self, subscriber: &str) -> Result<Option<u64>> {
        let v: Option<i64> = self
            .conn
            .query_row("SELECT last_seq FROM cursors WHERE subscriber = ?1", params![subscriber], |r| r.get(0))
            .optional()?;
        Ok(v.map(|x| x as u64))
    }

    /// Moves a subscriber's cursor forward; a lower value is ignored.
    pub fn set_cursor(&self, subscriber: &str, seq: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO cursors (subscriber, last_seq) VALUES (?1, ?2)
             ON CONFLICT(subscriber) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)",
            params![subscriber, seq as i64],
        )?;
        Ok(())
    }

    /// Records a viewer; `first_seen` is kept from the first call, `paid_sub` follows the latest.
    pub fn upsert_viewer(&self, id: &str, first_seen: f64, paid_sub: bool) -> Result<()> {
        self.conn.execute(
            "INSERT INTO viewers (id, first_seen, paid_sub) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET paid_sub = excluded.paid_sub",
            params![id, first_seen, paid_sub as i64],
        )?;
        Ok(())
    }

    pub fn viewer(&self, id: &str) -> Result<Option<ViewerRow>> {
        Ok(self
            .conn
            .query_row("SELECT first_seen, paid_sub FROM viewers WHERE id = ?1", params![id], |r| {
                Ok(ViewerRow { first_seen: r.get(0)?, paid_sub: r.get::<_, i64>(1)? != 0 })
            })
            .optional()?)
    }

    pub fn add_credit(&self, viewer: &str, t_eff: f64, usd: f64) -> Result<()> {
        self.conn.execute("INSERT INTO credit (viewer, t_eff, usd) VALUES (?1, ?2, ?3)", params![viewer, t_eff, usd])?;
        Ok(())
    }

    /// (effective time, USD) grants for one viewer, oldest first.
    pub fn credit_grants(&self, viewer: &str) -> Result<Vec<(f64, f64)>> {
        let mut st = self.conn.prepare_cached("SELECT t_eff, usd FROM credit WHERE viewer = ?1 ORDER BY t_eff, rowid")?;
        let rows = st.query_map(params![viewer], |r| Ok((r.get::<_, f64>(0)?, r.get::<_, f64>(1)?)))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// §2: deletes chat events older than the retention and blanks the text of older
    /// donations (amounts and other events stay).
    pub fn prune(&self, now: f64, chat_retention_s: f64) -> Result<PruneReport> {
        let cutoff = now - chat_retention_s;
        let chat_deleted = self.conn.execute("DELETE FROM events WHERE kind = 'chat' AND ts < ?1", params![cutoff])?;
        let donation_texts_redacted = self.conn.execute(
            "UPDATE events SET body = json_set(body, '$.text', NULL)
             WHERE kind = 'donation' AND ts < ?1 AND json_extract(body, '$.text') IS NOT NULL",
            params![cutoff],
        )?;
        Ok(PruneReport { chat_deleted, donation_texts_redacted })
    }

    pub fn audit(&self, ts: f64, actor: &str, action: &str, detail: &Value) -> Result<()> {
        self.conn.execute(
            "INSERT INTO audit (ts, actor, action, detail) VALUES (?1, ?2, ?3, ?4)",
            params![ts, actor, action, detail.to_string()],
        )?;
        Ok(())
    }

    /// The newest `limit` audit rows, oldest first.
    pub fn audit_rows(&self, limit: usize) -> Result<Vec<AuditRow>> {
        let mut st = self.conn.prepare_cached(
            "SELECT ts, actor, action, detail FROM (SELECT * FROM audit ORDER BY id DESC LIMIT ?1) ORDER BY id",
        )?;
        let rows = st.query_map(params![limit as i64], |r| {
            Ok((r.get::<_, f64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (ts, actor, action, detail) = row?;
            out.push(AuditRow { ts, actor, action, detail: serde_json::from_str(&detail)? });
        }
        Ok(out)
    }

    /// YouTube ban ids by channel (youtube-spec.md section 6): replaces the stored set with `rows`
    /// (channel id, ban id, expiry of a timeout).
    pub fn yt_bans_replace(&self, rows: &[(String, String, Option<f64>)]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM yt_bans", [])?;
        for (channel, ban_id, expires_at) in rows {
            tx.execute("INSERT INTO yt_bans (channel, ban_id, expires_at) VALUES (?1, ?2, ?3)", params![channel, ban_id, expires_at])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn yt_bans_load(&self) -> Result<Vec<(String, String, Option<f64>)>> {
        let mut st = self.conn.prepare_cached("SELECT channel, ban_id, expires_at FROM yt_bans ORDER BY channel")?;
        let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, Option<f64>>(2)?)))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// YouTube's units used on one Pacific day (section 5); only the latest day is kept.
    pub fn yt_quota_save(&self, day: &str, used: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO yt_quota (id, day, used) VALUES (1, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET day = excluded.day, used = excluded.used",
            params![day, used as i64],
        )?;
        Ok(())
    }

    pub fn yt_quota_load(&self) -> Result<Option<(String, u64)>> {
        Ok(self
            .conn
            .query_row("SELECT day, used FROM yt_quota WHERE id = 1", [], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64)))
            .optional()?)
    }

    /// Test hook: tries to modify the audit table. It must fail (append-only triggers).
    #[doc(hidden)]
    pub fn try_tamper_audit_for_tests(&self) -> Result<()> {
        self.conn.execute("UPDATE audit SET actor = 'x'", [])?;
        self.conn.execute("DELETE FROM audit", [])?;
        Ok(())
    }
}
