//! Streamlabs Socket API (plan §3.3): socket.io v2 over Engine.IO 3 [doc]. The subset we
//! need: `0{…}` open (pingInterval/pingTimeout), `40` namespace connect, the CLIENT pings
//! with `2` and the server answers `3`, events arrive as `42["event",{…}]`, `41`/`1` end.
//! The socket token travels in the URL query, so the URL is never logged or reported.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::model::InEvent;
use sm_core::donation::{streamlabs_counts, Source};
use sm_core::protocol::Platform;

const COMPONENT: &str = "streamlabs";
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const DEDUPE_KEEP: usize = 5000;
/// §3.3: a donor name links to a chatter seen within this window.
pub const LINK_WINDOW_S: f64 = 86_400.0;

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Open { ping_interval_ms: u64, ping_timeout_ms: u64 },
    Connected,
    Ping,
    Pong,
    Event { name: String, data: Value },
    Disconnect,
    Other,
}

pub fn parse_packet(t: &str) -> Packet {
    if let Some(rest) = t.strip_prefix("42") {
        let Ok(Value::Array(a)) = serde_json::from_str::<Value>(rest) else { return Packet::Other };
        return match (a.first().and_then(Value::as_str), a.get(1)) {
            (Some(name), Some(data)) => Packet::Event { name: name.to_string(), data: data.clone() },
            _ => Packet::Other,
        };
    }
    match t {
        "40" => Packet::Connected,
        "41" | "1" => Packet::Disconnect,
        "2" => Packet::Ping,
        "3" => Packet::Pong,
        _ => match t.strip_prefix('0').and_then(|j| serde_json::from_str::<Value>(j).ok()) {
            Some(v) => Packet::Open {
                ping_interval_ms: v["pingInterval"].as_u64().unwrap_or(25_000),
                ping_timeout_ms: v["pingTimeout"].as_u64().unwrap_or(5_000),
            },
            None => Packet::Other,
        },
    }
}

fn text_of(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn num(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

fn sub_tier(v: &Value) -> u8 {
    match v.as_str() {
        Some("2000") => 2,
        Some("3000") => 3,
        Some("1000") | Some("Prime") => 1,
        _ => 0,
    }
}

/// Maps one socket `event` payload to (dedupe id, event) pairs, applying the relay rule:
/// relays of native-platform events count only while that native connector is off.
/// Donor names are free text, so `viewer_id` stays None here; see [`link_donor`].
pub fn map_event(data: &Value, native_twitch: bool, native_youtube: bool) -> Vec<(String, InEvent)> {
    let Some(kind) = data["type"].as_str() else { return Vec::new() };
    let relay = data["for"].as_str();
    if !streamlabs_counts(kind, relay, native_twitch, native_youtube) {
        return Vec::new();
    }
    let Some(msgs) = data["message"].as_array() else { return Vec::new() };
    let event_id = data["event_id"].as_str();
    let mut out = Vec::new();
    for (i, m) in msgs.iter().enumerate() {
        let id = match text_of(&m["id"]).or_else(|| text_of(&m["_id"])) {
            Some(x) => format!("sl:{x}"),
            None => match event_id {
                Some(e) => format!("sl:{e}#{i}"),
                None => continue,
            },
        };
        let display = m["name"].as_str().or(m["from"].as_str()).map(str::to_string);
        let (platform, source, text) = match (relay, kind) {
            (None, "donation") => {
                let (Some(amount), Some(currency)) = (text_of(&m["amount"]), m["currency"].as_str()) else { continue };
                (None, Source::Streamlabs { amount, currency: currency.to_string() }, m["message"].as_str().map(str::to_string))
            }
            (Some("twitch_account"), "bits") => {
                let Some(bits) = num(&m["amount"]) else { continue };
                (Some(Platform::Twitch), Source::TwitchBits { bits }, m["message"].as_str().map(str::to_string))
            }
            (Some("twitch_account"), "subscription") => (
                Some(Platform::Twitch),
                Source::TwitchSub { tier: sub_tier(&m["sub_plan"]), count: 1 },
                m["message"].as_str().map(str::to_string),
            ),
            (Some("youtube_account"), "superchat") => {
                let (Some(amount_micros), Some(currency)) = (num(&m["amount"]), m["currency"].as_str()) else { continue };
                (
                    Some(Platform::Youtube),
                    Source::YouTubeSuperChat { amount_micros, currency: currency.to_string() },
                    m["comment"].as_str().map(str::to_string),
                )
            }
            // YouTube membership relays carry no price; the native connector handles those.
            _ => continue,
        };
        out.push((id.clone(), InEvent::Donation { platform, viewer_id: None, display, source, text, external_id: id }));
    }
    out
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecentChatter {
    pub viewer_id: String,
    pub last_seen: f64,
}

/// §3.3: a donor name links to a viewer only when it exactly (case-insensitively) matches
/// a chatter seen in the last 24 h. `recent` is keyed by lowercased chat name. A spoofed
/// name can only gift credit to someone else.
pub fn link_donor(name: &str, recent: &HashMap<String, RecentChatter>, now: f64) -> Option<String> {
    let key = name.trim().to_lowercase();
    if key.is_empty() {
        return None;
    }
    recent.get(&key).filter(|c| now - c.last_seen <= LINK_WINDOW_S).map(|c| c.viewer_id.clone())
}

pub struct StreamlabsConfig {
    /// `wss://sockets.streamlabs.com/socket.io/?EIO=3&transport=websocket&token=…`.
    pub url: String,
    pub native_twitch: bool,
    pub native_youtube: bool,
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum End {
    OutClosed,
    Lost(&'static str),
}

async fn next_packet(ws: &mut Ws) -> Option<Packet> {
    loop {
        match ws.next().await? {
            Ok(Message::Text(t)) => return Some(parse_packet(t.as_str())),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

async fn health(out: &mpsc::Sender<InEvent>, up: bool, detail: &str) {
    let _ = out.send(InEvent::Health { component: COMPONENT, up, detail: detail.to_string() }).await;
}

/// Runs until the event receiver is dropped (or the task is aborted).
pub async fn run_streamlabs(cfg: StreamlabsConfig, out: mpsc::Sender<InEvent>) {
    let mut seen: (HashSet<String>, VecDeque<String>) = (HashSet::new(), VecDeque::new());
    let mut backoff = 1.0f64;
    loop {
        match connect_async(cfg.url.as_str()).await {
            Ok((ws, _)) => match session(&cfg, ws, &out, &mut seen, &mut backoff).await {
                End::OutClosed => return,
                End::Lost(why) => health(&out, false, why).await,
            },
            // The error text could echo the URL, which holds the token: report a fixed text.
            Err(_) => health(&out, false, "connect failed").await,
        }
        if out.is_closed() {
            return;
        }
        tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
        backoff = (backoff * 2.0).min(60.0);
    }
}

async fn session(
    cfg: &StreamlabsConfig,
    mut ws: Ws,
    out: &mpsc::Sender<InEvent>,
    seen: &mut (HashSet<String>, VecDeque<String>),
    backoff: &mut f64,
) -> End {
    let (ping_iv, ping_to) = match tokio::time::timeout(OPEN_TIMEOUT, next_packet(&mut ws)).await {
        Ok(Some(Packet::Open { ping_interval_ms, ping_timeout_ms })) => {
            (Duration::from_millis(ping_interval_ms.max(10)), Duration::from_millis(ping_timeout_ms))
        }
        _ => return End::Lost("no open packet"),
    };
    *backoff = 1.0;
    health(out, true, "connected").await;
    let mut ticker = tokio::time::interval_at(Instant::now() + ping_iv, ping_iv);
    let mut awaiting_pong: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Some(since) = awaiting_pong {
                    if since.elapsed() > ping_to {
                        let _ = ws.close(None).await;
                        return End::Lost("no pong");
                    }
                } else {
                    if ws.send(Message::text("2")).await.is_err() {
                        return End::Lost("send failed");
                    }
                    awaiting_pong = Some(Instant::now());
                }
            }
            p = next_packet(&mut ws) => match p {
                None => return End::Lost("connection closed"),
                Some(Packet::Pong) => awaiting_pong = None,
                Some(Packet::Ping) => {
                    if ws.send(Message::text("3")).await.is_err() {
                        return End::Lost("send failed");
                    }
                }
                Some(Packet::Disconnect) => {
                    let _ = ws.close(None).await;
                    return End::Lost("server disconnected");
                }
                Some(Packet::Event { name, data }) if name == "event" => {
                    for (id, ev) in map_event(&data, cfg.native_twitch, cfg.native_youtube) {
                        if !seen.0.insert(id.clone()) {
                            continue;
                        }
                        seen.1.push_back(id);
                        while seen.1.len() > DEDUPE_KEEP {
                            if let Some(old) = seen.1.pop_front() {
                                seen.0.remove(&old);
                            }
                        }
                        if out.send(ev).await.is_err() {
                            return End::OutClosed;
                        }
                    }
                }
                Some(_) => {}
            },
        }
    }
}
