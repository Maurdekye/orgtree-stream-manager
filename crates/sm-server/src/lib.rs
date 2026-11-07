//! The org WebSocket protocol (plan §4): bearer tokens with scopes, a fixed subscribe
//! hello, per-subscriber replay cursors, the event envelope, and command dispatch with
//! idempotency. Orgtree websocket watchdogs are the clients.

pub mod stats;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use subtle::ConstantTimeEq;
use tokio::sync::broadcast::{self, error::RecvError};

use sm_core::pipeline::RateWindow;
use sm_core::protocol::{parse_command, CmdCache, Command};
use sm_store::{StoredEvent, Store};

/// Event kinds that carry viewer-supplied text; they get `trust: untrusted_viewer_input`.
const VIEWER_KINDS: &[&str] = &["chat", "chat.mod", "donation", "chat.overflow"];
/// Kinds a subscriber gets only when its hello names them (the every-message moderation feed
/// would otherwise flood every unfiltered listener).
const OPT_IN_KINDS: &[&str] = &["chat.mod"];
/// The only commands the `moderate` scope may send.
const MODERATE_CMDS: &[&str] = &["mod.ban", "mod.timeout"];
/// Envelope keys a body may not override.
const RESERVED: &[&str] = &["v", "seq", "ts", "kind", "trust", "replay"];
const CATCH_UP_LIMIT: usize = 100_000;
/// Echoed command ids and names are cut to this many characters.
const ECHO_MAX: usize = 100;
/// Refusals answered per connection per minute; the rest are dropped.
const REFUSALS_PER_MIN: usize = 5;

fn cap(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scopes {
    pub read: bool,
    pub control: bool,
    /// The moderation-bot scope: mod.ban and mod.timeout only (USER m3e0438a5d6ca).
    pub moderate: bool,
}

/// Named bridge tokens (§10). Compared in constant time.
pub struct TokenTable {
    tokens: Vec<(String, Vec<u8>, Scopes)>,
}

impl TokenTable {
    /// (name, secret, scopes) per token.
    pub fn new(tokens: Vec<(String, String, Scopes)>) -> Self {
        TokenTable { tokens: tokens.into_iter().map(|(n, t, s)| (n, t.into_bytes(), s)).collect() }
    }

    pub fn check(&self, presented: &str) -> Option<(String, Scopes)> {
        let p = presented.as_bytes();
        let mut found = None;
        for (name, secret, scopes) in &self.tokens {
            if bool::from(secret.as_slice().ct_eq(p)) {
                found = Some((name.clone(), *scopes));
            }
        }
        found
    }
}

pub struct ServerConfig {
    pub tokens: TokenTable,
    /// At most this many events are replayed after a reconnect (§4.1: 200).
    pub replay_max: usize,
    /// Only events this recent are replayed (§4.1: 15 minutes).
    pub replay_window_s: f64,
    pub hello_timeout_s: f64,
}

/// Runs one validated command. Ok carries `detail`; Err carries (error code, detail).
pub trait CommandHandler: Send + Sync {
    fn handle(&self, actor: &str, cmd: &Command) -> Result<Value, (String, String)>;
}

pub type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;

#[derive(Clone)]
struct Out {
    seq: u64,
    kind: String,
    text: Arc<str>,
}

pub struct Hub {
    store: Mutex<Store>,
    cfg: ServerConfig,
    handler: Arc<dyn CommandHandler>,
    clock: Clock,
    tx: broadcast::Sender<Out>,
    cache: Mutex<CmdCache>,
}

struct Subscription {
    name: String,
    /// `<token name>/<subscriber>`: cursors are kept per token, so a read token can't move
    /// a control subscriber's cursor by reusing its name (review round 1, finding 7).
    key: String,
    kinds: Option<HashSet<String>>,
}

impl Subscription {
    fn wants(&self, kind: &str) -> bool {
        match &self.kinds {
            None => !OPT_IN_KINDS.contains(&kind),
            Some(k) => k.contains(kind),
        }
    }
}

type Sink = SplitSink<WebSocket, Message>;

impl Hub {
    pub fn new(store: Store, cfg: ServerConfig, handler: Arc<dyn CommandHandler>, clock: Clock) -> Arc<Self> {
        let (tx, _) = broadcast::channel(1024);
        Arc::new(Hub { store: Mutex::new(store), cfg, handler, clock, tx, cache: Mutex::new(CmdCache::new(1000)) })
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new().route("/v1/events", get(ws_handler)).with_state(self.clone())
    }

    fn st(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Logs an event and sends it to every live subscriber. Returns its seq.
    ///
    /// Panics if the event log can't be written: the bridge must not forward what it
    /// can't replay.
    pub fn publish(&self, kind: &str, body: Value) -> u64 {
        let now = (self.clock)();
        let store = self.st();
        let seq = store.append_event(now, kind, &body).expect("event log write failed");
        let text = envelope(seq, now, kind, &body, false).to_string();
        // Sent while the store lock is held, so live order always matches seq order.
        let _ = self.tx.send(Out { seq, kind: kind.to_string(), text: text.into() });
        seq
    }

    /// §2 retention: deletes chat older than `retention_s` and blanks old donation text.
    pub fn prune(&self, retention_s: f64) -> Option<sm_store::PruneReport> {
        let now = (self.clock)();
        self.st().prune(now, retention_s).ok()
    }

    async fn serve(self: Arc<Self>, socket: WebSocket, token_name: String, scopes: Scopes) {
        let (mut sink, mut stream) = socket.split();
        let hello = tokio::time::timeout(Duration::from_secs_f64(self.cfg.hello_timeout_s), next_text(&mut stream)).await;
        let sub = match hello {
            Ok(Some(text)) => parse_hello(&text).map(|mut s| {
                s.key = format!("{token_name}/{}", s.name);
                s
            }),
            _ => None,
        };
        let Some(sub) = sub else {
            let _ = sink.close().await;
            return;
        };
        let mut rx = self.tx.subscribe();
        let Some(mut last_sent) = self.replay(&sub, &mut sink).await else {
            return;
        };
        let mut refusals = RateWindow::new(0.0, REFUSALS_PER_MIN, 60.0);
        loop {
            tokio::select! {
                r = rx.recv() => match r {
                    Ok(out) => {
                        if out.seq <= last_sent {
                            continue;
                        }
                        last_sent = out.seq;
                        if sub.wants(&out.kind) && sink.send(Message::Text(out.text.to_string().into())).await.is_err() {
                            break;
                        }
                        let _ = self.st().set_cursor(&sub.key, out.seq);
                    }
                    Err(RecvError::Lagged(_)) => match self.catch_up(&sub, &mut sink, last_sent).await {
                        Some(s) => last_sent = s,
                        None => break,
                    },
                    Err(RecvError::Closed) => break,
                },
                m = stream.next() => match m {
                    Some(Ok(Message::Text(t))) => {
                        // A refusal goes only to this connection, rate limited.
                        if let Some(reply) = self.on_text(&token_name, scopes, t.as_str()) {
                            if refusals.try_acquire((self.clock)()).is_ok()
                                && sink.send(Message::Text(reply.to_string().into())).await.is_err()
                            {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                },
            }
        }
    }

    /// §4.1: replays what this subscriber missed (at most `replay_max`, at most
    /// `replay_window_s` old), preceded by `replay.gap` when more was lost. A subscriber
    /// seen for the first time starts at the head with no history. Returns the seq the
    /// connection is caught up to, or None if the socket failed.
    async fn replay(&self, sub: &Subscription, sink: &mut Sink) -> Option<u64> {
        let now = (self.clock)();
        let (head, gap, items) = {
            let store = self.st();
            let head = store.head_seq().ok()?;
            match store.cursor(&sub.key).ok()? {
                None => {
                    store.set_cursor(&sub.key, head).ok()?;
                    (head, 0, Vec::new())
                }
                Some(c) => {
                    let wanted: Vec<StoredEvent> =
                        store.events_after(c, CATCH_UP_LIMIT).ok()?.into_iter().filter(|e| e.seq <= head && sub.wants(&e.kind)).collect();
                    let total = wanted.len();
                    let cutoff = now - self.cfg.replay_window_s;
                    let recent: Vec<StoredEvent> = wanted.into_iter().filter(|e| e.ts >= cutoff).collect();
                    let skip = recent.len().saturating_sub(self.cfg.replay_max);
                    let kept: Vec<StoredEvent> = recent.into_iter().skip(skip).collect();
                    (head, total - kept.len(), kept)
                }
            }
        };
        if gap > 0 {
            let g = json!({"v": 1, "kind": "replay.gap", "ts": iso8601(now), "missed": gap});
            sink.send(Message::Text(g.to_string().into())).await.ok()?;
        }
        for e in &items {
            let text = envelope(e.seq, e.ts, &e.kind, &e.body, true).to_string();
            sink.send(Message::Text(text.into())).await.ok()?;
        }
        self.st().set_cursor(&sub.key, head).ok()?;
        Some(head)
    }

    /// After the live channel lagged: send everything after `last_sent` from the log.
    async fn catch_up(&self, sub: &Subscription, sink: &mut Sink, last_sent: u64) -> Option<u64> {
        let rows = self.st().events_after(last_sent, CATCH_UP_LIMIT).ok()?;
        let mut last = last_sent;
        for e in rows {
            if sub.wants(&e.kind) {
                let text = envelope(e.seq, e.ts, &e.kind, &e.body, false).to_string();
                sink.send(Message::Text(text.into())).await.ok()?;
            }
            last = e.seq;
        }
        self.st().set_cursor(&sub.key, last).ok()?;
        Some(last)
    }

    /// Handles one incoming message. Returns a private reply for this connection only
    /// (a refusal for a token without the control scope); everything else is published.
    fn on_text(&self, token_name: &str, scopes: Scopes, text: &str) -> Option<Value> {
        let v = serde_json::from_str::<Value>(text).ok()?;
        if v.get("type").and_then(Value::as_str) != Some("cmd") {
            return None;
        }
        let raw_id = v.get("cmd_id").and_then(Value::as_str).map(|s| cap(s, ECHO_MAX));
        let cmd_name = cap(v.get("cmd").and_then(Value::as_str).unwrap_or(""), ECHO_MAX);
        let moderating = !scopes.control && scopes.moderate && MODERATE_CMDS.contains(&cmd_name.as_str());
        if !scopes.control && !moderating {
            if scopes.moderate {
                let now = (self.clock)();
                let _ = self.st().audit(now, &format!("token:{token_name}"), &format!("refused:{cmd_name}"), &json!({"cmd_id": raw_id}));
            }
            // Not logged, not broadcast: a read token can't fill the log or other
            // subscribers' feeds (review round 1, finding 7).
            return Some(json!({"v": 1, "kind": "command.result", "private": true, "cmd_id": raw_id, "ok": false,
                               "error": "forbidden", "detail": "this token has no control scope"}));
        }
        let (id, cmd) = match parse_command(text) {
            Ok(x) => x,
            Err(e) if cmd_name == "org.turns" => {
                // The auto-sleep relay's state reports stay off the shared feed (plan rev 2).
                return Some(json!({"v": 1, "kind": "command.result", "private": true, "cmd_id": raw_id, "ok": false,
                                   "error": e.code, "detail": e.detail}));
            }
            Err(e) => {
                let id = e.cmd_id.as_deref().map(|s| cap(s, ECHO_MAX));
                self.publish_result(token_name, &cmd_name, json!({"cmd_id": id, "ok": false, "error": e.code, "detail": e.detail}));
                return None;
            }
        };
        if matches!(cmd, Command::OrgTurns { .. }) {
            // A state report the relay's watchdogs resend with one static cmd_id: never cached as a
            // repeat, never broadcast. Only a refusal comes back, privately.
            return match self.handler.handle(token_name, &cmd) {
                Ok(_) => None,
                Err((code, detail)) => Some(json!({"v": 1, "kind": "command.result", "private": true, "cmd_id": id, "ok": false,
                                                   "error": code, "detail": detail})),
            };
        }
        let cached = self.cache.lock().unwrap_or_else(|e| e.into_inner()).get(&id).cloned();
        if let Some(mut prev) = cached {
            prev["repeat"] = json!(true);
            self.publish("command.result", prev);
            return None;
        }
        let res = match &cmd {
            Command::Ping => Ok(json!({"seq_head": self.st().head_seq().unwrap_or(0)})),
            other => self.handler.handle(token_name, other),
        };
        let body = match res {
            Ok(detail) => json!({"cmd_id": id, "ok": true, "error": null, "detail": detail}),
            Err((code, detail)) => json!({"cmd_id": id, "ok": false, "error": code, "detail": detail}),
        };
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).put(id, body.clone());
        self.publish_result(token_name, &cmd_name, body);
        None
    }

    /// The newest audit rows (tests and diagnostics).
    pub fn audit_tail(&self, limit: usize) -> Vec<sm_store::AuditRow> {
        self.st().audit_rows(limit).unwrap_or_default()
    }

    fn publish_result(&self, token_name: &str, cmd_name: &str, body: Value) {
        let now = (self.clock)();
        let _ = self.st().audit(now, &format!("token:{token_name}"), &format!("cmd:{cmd_name}"), &body);
        self.publish("command.result", body);
    }
}

async fn ws_handler(State(hub): State<Arc<Hub>>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim);
    let Some((name, scopes)) = presented.and_then(|p| hub.cfg.tokens.check(p)) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !scopes.read {
        return StatusCode::FORBIDDEN.into_response();
    }
    ws.on_upgrade(move |socket| hub.serve(socket, name, scopes))
}

async fn next_text(stream: &mut SplitStream<WebSocket>) -> Option<String> {
    loop {
        match stream.next().await? {
            Ok(Message::Text(t)) => return Some(t.to_string()),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn parse_hello(text: &str) -> Option<Subscription> {
    let v: Value = serde_json::from_str(text).ok()?;
    if v.get("type")?.as_str()? != "subscribe" || v.get("v")?.as_u64()? != 1 {
        return None;
    }
    let name = v.get("subscriber")?.as_str()?;
    if !valid_name(name) {
        return None;
    }
    let kinds = match v.get("kinds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(a)) => Some(a.iter().map(|k| k.as_str().map(str::to_string)).collect::<Option<HashSet<_>>>()?),
        Some(_) => return None,
    };
    Some(Subscription { name: name.to_string(), key: name.to_string(), kinds })
}

/// §4.2 envelope: `v`, `seq`, `ts` (ISO 8601 UTC), `kind`, `trust` for viewer content,
/// then the body's own fields, then `replay` when replayed.
pub fn envelope(seq: u64, ts: f64, kind: &str, body: &Value, replay: bool) -> Value {
    let mut m = Map::new();
    m.insert("v".into(), json!(1));
    m.insert("seq".into(), json!(seq));
    m.insert("ts".into(), json!(iso8601(ts)));
    m.insert("kind".into(), json!(kind));
    if VIEWER_KINDS.contains(&kind) {
        m.insert("trust".into(), json!("untrusted_viewer_input"));
    }
    if let Value::Object(b) = body {
        for (k, val) in b {
            if !RESERVED.contains(&k.as_str()) {
                m.insert(k.clone(), val.clone());
            }
        }
    }
    if replay {
        m.insert("replay".into(), json!(true));
    }
    Value::Object(m)
}

/// Unix seconds → `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub fn iso8601(t: f64) -> String {
    let ms_total = (t * 1000.0).round() as i64;
    let secs = ms_total.div_euclid(1000);
    let ms = ms_total.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z", sod / 3600, sod % 3600 / 60, sod % 60)
}

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian (H. Hinnant).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { yoe + era * 400 + 1 } else { yoe + era * 400 }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_dates() {
        assert_eq!(iso8601(0.0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601(1_700_000_000.0), "2023-11-14T22:13:20.000Z");
        assert_eq!(iso8601(951_782_400.5), "2000-02-29T00:00:00.500Z");
        assert_eq!(iso8601(-1.0), "1969-12-31T23:59:59.000Z");
    }

    #[test]
    fn envelope_keeps_reserved_keys() {
        let e = envelope(7, 0.0, "chat", &json!({"seq": 99, "kind": "x", "trust": "trusted", "text": "hi"}), true);
        assert_eq!(e["seq"], 7);
        assert_eq!(e["kind"], "chat");
        assert_eq!(e["trust"], "untrusted_viewer_input");
        assert_eq!(e["text"], "hi");
        assert_eq!(e["replay"], true);
        assert!(envelope(1, 0.0, "moderation", &json!({}), false).get("trust").is_none());
    }

    #[test]
    fn hello_validation() {
        assert!(parse_hello(r#"{"type":"subscribe","v":1,"subscriber":"listener-chat","kinds":["chat"]}"#).is_some());
        assert!(parse_hello(r#"{"type":"subscribe","v":1,"subscriber":"a b"}"#).is_none());
        assert!(parse_hello(r#"{"type":"subscribe","v":2,"subscriber":"a"}"#).is_none());
        assert!(parse_hello(r#"{"type":"subscribe","v":1,"subscriber":"a","kinds":[1]}"#).is_none());
        assert!(parse_hello(r#"{"type":"cmd","v":1,"subscriber":"a"}"#).is_none());
    }

    #[test]
    fn tokens_match_exactly() {
        let t = TokenTable::new(vec![("r".into(), "abc".into(), Scopes { read: true, control: false, moderate: false })]);
        assert_eq!(t.check("abc").map(|x| x.0), Some("r".to_string()));
        assert!(t.check("abcd").is_none());
        assert!(t.check("ab").is_none());
        assert!(t.check("").is_none());
    }
}
