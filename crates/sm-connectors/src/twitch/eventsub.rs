//! EventSub over WebSocket (plan §3.1). Session rules [doc]: subscribe within 10 s of the
//! welcome; no message for longer than the keepalive window means the connection is lost
//! (reconnect and resubscribe); `session_reconnect` gives a URL to dial as is, and the old
//! socket is closed only after the new one's welcome. Subscriptions survive a reconnect.
//! Notifications can be redelivered, so message ids are de-duplicated.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::model::InEvent;
use sm_core::donation::Source;
use sm_core::protocol::Platform;

const WELCOME_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_WELCOME_TIMEOUT: Duration = Duration::from_secs(30);
const DEDUPE_KEEP: usize = 2000;
const COMPONENT: &str = "twitch_eventsub";

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Welcome { session_id: String, keepalive_s: u64 },
    Keepalive,
    /// `event` is None for types we don't act on (or a gifted sub's recipient).
    Notification { message_id: String, event: Option<InEvent> },
    Reconnect { url: String },
    Revocation { sub_type: String, status: String },
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubSpec {
    pub sub_type: String,
    pub version: String,
    pub condition: Value,
}

/// The subscriptions of plan §3.1. Chat needs the bot's user id as `user_id`.
pub fn default_specs(broadcaster_id: &str, bot_user_id: &str) -> Vec<SubSpec> {
    let b = json!({"broadcaster_user_id": broadcaster_id});
    let mk = |t: &str, c: &Value| SubSpec { sub_type: t.to_string(), version: "1".to_string(), condition: c.clone() };
    vec![
        mk("channel.chat.message", &json!({"broadcaster_user_id": broadcaster_id, "user_id": bot_user_id})),
        mk("channel.bits.use", &b),
        mk("channel.subscribe", &b),
        mk("channel.subscription.message", &b),
        mk("channel.subscription.gift", &b),
        mk("stream.online", &b),
        mk("stream.offline", &b),
    ]
}

fn s(v: &Value) -> Option<String> {
    v.as_str().map(str::to_string)
}

fn tier(v: &Value) -> u8 {
    match v.as_str() {
        Some("1000") => 1,
        Some("2000") => 2,
        Some("3000") => 3,
        _ => 0,
    }
}

fn viewer(id: &Value) -> Option<String> {
    id.as_str().filter(|x| !x.is_empty()).map(|x| format!("twitch:{x}"))
}

fn map_event(ty: &str, e: &Value, message_id: &str) -> Option<InEvent> {
    let external_id = format!("tw-evt:{message_id}");
    match ty {
        "channel.chat.message" => {
            let badges: Vec<String> =
                e["badges"].as_array().map(|a| a.iter().filter_map(|b| s(&b["set_id"])).collect()).unwrap_or_default();
            let paid_sub = badges.iter().any(|b| b == "subscriber" || b == "founder");
            Some(InEvent::Chat {
                platform: Platform::Twitch,
                viewer_id: viewer(&e["chatter_user_id"])?,
                login: s(&e["chatter_user_login"]).unwrap_or_default(),
                display: s(&e["chatter_user_name"]).unwrap_or_default(),
                text: s(&e["message"]["text"]).unwrap_or_default(),
                msg_id: format!("tw:{}", e["message_id"].as_str()?),
                roles: badges,
                paid_sub,
            })
        }
        // Bits are counted here only, never from a chat message's `cheer` (no double count).
        "channel.bits.use" => Some(InEvent::Donation {
            platform: Some(Platform::Twitch),
            viewer_id: viewer(&e["user_id"]),
            display: s(&e["user_name"]),
            source: Source::TwitchBits { bits: e["bits"].as_u64()? },
            text: s(&e["message"]["text"]),
            external_id,
        }),
        "channel.subscribe" => {
            if e["is_gift"].as_bool() == Some(true) {
                return None; // the gift event credits the gifter
            }
            Some(InEvent::Donation {
                platform: Some(Platform::Twitch),
                viewer_id: viewer(&e["user_id"]),
                display: s(&e["user_name"]),
                source: Source::TwitchSub { tier: tier(&e["tier"]), count: 1 },
                text: None,
                external_id,
            })
        }
        "channel.subscription.message" => Some(InEvent::Donation {
            platform: Some(Platform::Twitch),
            viewer_id: viewer(&e["user_id"]),
            display: s(&e["user_name"]),
            source: Source::TwitchSub { tier: tier(&e["tier"]), count: 1 },
            text: s(&e["message"]["text"]),
            external_id,
        }),
        "channel.subscription.gift" => Some(InEvent::Donation {
            platform: Some(Platform::Twitch),
            viewer_id: viewer(&e["user_id"]),
            display: s(&e["user_name"]),
            source: Source::TwitchSub { tier: tier(&e["tier"]), count: u32::try_from(e["total"].as_u64()?).ok()? },
            text: None,
            external_id,
        }),
        "stream.online" => Some(InEvent::StreamState { platform: Platform::Twitch, live: true }),
        "stream.offline" => Some(InEvent::StreamState { platform: Platform::Twitch, live: false }),
        _ => None,
    }
}

pub fn parse_frame(text: &str) -> Result<Frame, String> {
    let v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let mt = v["metadata"]["message_type"].as_str().ok_or("no message_type")?;
    Ok(match mt {
        "session_welcome" => {
            let sess = &v["payload"]["session"];
            Frame::Welcome {
                session_id: s(&sess["id"]).ok_or("welcome without a session id")?,
                keepalive_s: sess["keepalive_timeout_seconds"].as_u64().unwrap_or(10),
            }
        }
        "session_keepalive" => Frame::Keepalive,
        "session_reconnect" => {
            Frame::Reconnect { url: s(&v["payload"]["session"]["reconnect_url"]).ok_or("reconnect without a url")? }
        }
        "revocation" => Frame::Revocation {
            sub_type: s(&v["payload"]["subscription"]["type"]).unwrap_or_default(),
            status: s(&v["payload"]["subscription"]["status"]).unwrap_or_default(),
        },
        "notification" => {
            let message_id = s(&v["metadata"]["message_id"]).ok_or("notification without a message id")?;
            let ty = v["payload"]["subscription"]["type"].as_str().or(v["metadata"]["subscription_type"].as_str()).unwrap_or("");
            let event = map_event(ty, &v["payload"]["event"], &message_id);
            Frame::Notification { message_id, event }
        }
        _ => Frame::Other,
    })
}

pub struct EventSubConfig {
    /// `wss://eventsub.wss.twitch.tv/ws` in production.
    pub url: String,
    pub subscriptions: Vec<SubSpec>,
    /// Extra slack on top of the session's keepalive window.
    pub keepalive_grace_s: f64,
}

/// Creates the subscriptions for a new session (Helix `POST /eventsub/subscriptions`).
#[async_trait::async_trait]
pub trait EventSubSubscriber: Send + Sync {
    async fn subscribe(&self, session_id: &str, specs: &[SubSpec]) -> Result<(), String>;
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum End {
    /// The receiver of events is gone: stop for good.
    OutClosed,
    Lost(String),
}

struct Dedupe {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl Dedupe {
    fn new() -> Self {
        Dedupe { seen: HashSet::new(), order: VecDeque::new() }
    }

    /// True the first time an id is seen.
    fn insert(&mut self, id: &str) -> bool {
        if !self.seen.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        while self.order.len() > DEDUPE_KEEP {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

async fn next_frame(ws: &mut Ws) -> Option<Frame> {
    loop {
        match ws.next().await? {
            Ok(Message::Text(t)) => {
                if let Ok(f) = parse_frame(t.as_str()) {
                    return Some(f);
                }
            }
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

fn scheme(url: &str) -> &str {
    url.split("://").next().unwrap_or("")
}

async fn health(out: &mpsc::Sender<InEvent>, up: bool, detail: impl Into<String>) {
    let _ = out.send(InEvent::Health { component: COMPONENT, up, detail: detail.into() }).await;
}

/// Runs until the event receiver is dropped (or the task is aborted), reconnecting with
/// backoff (1 s doubling to 60 s) when dialing fails.
pub async fn run_eventsub(cfg: EventSubConfig, subscriber: Arc<dyn EventSubSubscriber>, out: mpsc::Sender<InEvent>) {
    let mut dedupe = Dedupe::new();
    let mut backoff = 1.0f64;
    loop {
        let ws = match connect_async(cfg.url.as_str()).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                health(&out, false, format!("connect failed: {e}")).await;
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(60.0);
                continue;
            }
        };
        match session(&cfg, subscriber.as_ref(), &out, &mut dedupe, ws, &mut backoff).await {
            End::OutClosed => return,
            End::Lost(why) => health(&out, false, why).await,
        }
        if out.is_closed() {
            return;
        }
    }
}

async fn forward(out: &mpsc::Sender<InEvent>, dedupe: &mut Dedupe, f: Frame) -> Result<(), End> {
    let ev = match f {
        Frame::Notification { message_id, event } => {
            if !dedupe.insert(&message_id) {
                return Ok(());
            }
            event
        }
        Frame::Revocation { sub_type, status } => Some(InEvent::Revoked { sub_type, status }),
        _ => None,
    };
    if let Some(e) = ev {
        out.send(e).await.map_err(|_| End::OutClosed)?;
    }
    Ok(())
}

async fn session(
    cfg: &EventSubConfig,
    subscriber: &dyn EventSubSubscriber,
    out: &mpsc::Sender<InEvent>,
    dedupe: &mut Dedupe,
    mut ws: Ws,
    backoff: &mut f64,
) -> End {
    let (session_id, mut keepalive) = match tokio::time::timeout(WELCOME_TIMEOUT, next_frame(&mut ws)).await {
        Ok(Some(Frame::Welcome { session_id, keepalive_s })) => (session_id, keepalive_s),
        _ => return End::Lost("no welcome".into()),
    };
    if let Err(e) = subscriber.subscribe(&session_id, &cfg.subscriptions).await {
        let _ = ws.close(None).await;
        return End::Lost(format!("subscribe failed: {e}"));
    }
    *backoff = 1.0;
    health(out, true, "connected").await;
    loop {
        let window = Duration::from_secs_f64(keepalive as f64 + cfg.keepalive_grace_s);
        let f = match tokio::time::timeout(window, next_frame(&mut ws)).await {
            Err(_) => {
                let _ = ws.close(None).await;
                return End::Lost("keepalive timeout".into());
            }
            Ok(None) => return End::Lost("connection closed".into()),
            Ok(Some(f)) => f,
        };
        match f {
            Frame::Reconnect { url } => {
                if scheme(&url) != scheme(&cfg.url) {
                    return End::Lost("reconnect url with a different scheme refused".into());
                }
                match migrate(&url, &mut ws, out, dedupe).await {
                    Ok((new_ws, ka)) => {
                        let _ = ws.close(None).await;
                        ws = new_ws;
                        keepalive = ka;
                    }
                    Err(end) => return end,
                }
            }
            other => {
                if let Err(end) = forward(out, dedupe, other).await {
                    return end;
                }
            }
        }
    }
}

/// Dials the reconnect URL and keeps reading the old socket until the new one welcomes.
async fn migrate(url: &str, old: &mut Ws, out: &mpsc::Sender<InEvent>, dedupe: &mut Dedupe) -> Result<(Ws, u64), End> {
    let (mut new_ws, _) = connect_async(url).await.map_err(|e| End::Lost(format!("reconnect dial failed: {e}")))?;
    let deadline = tokio::time::sleep(RECONNECT_WELCOME_TIMEOUT);
    tokio::pin!(deadline);
    let mut old_alive = true;
    loop {
        tokio::select! {
            f = next_frame(&mut new_ws) => match f {
                Some(Frame::Welcome { keepalive_s, .. }) => return Ok((new_ws, keepalive_s)),
                Some(other) => forward(out, dedupe, other).await?,
                None => return Err(End::Lost("reconnect socket closed before welcome".into())),
            },
            f = next_frame(old), if old_alive => match f {
                Some(other) => forward(out, dedupe, other).await?,
                None => old_alive = false,
            },
            _ = &mut deadline => return Err(End::Lost("no welcome on the reconnect url".into())),
        }
    }
}
