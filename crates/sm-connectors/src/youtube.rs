//! YouTube (plan §3.2): Data API v3 request shapes, chat-page mapping, and a quota-aware
//! poller over `liveChatMessages.list`. `streamList` (server streaming, 5 units a call
//! [doc]) can replace the poller behind the same events later.
//!
//! The quota is the binding limit: the default project allowance is 10,000 units a day
//! [verify], and `list` is taken to cost 5 units [verify]. So the poll interval spreads
//! the allowance (minus a reserve for moderation and chat actions) over the expected
//! stream length, and is never shorter than the server's `pollingIntervalMillis`.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::model::InEvent;
use sm_core::donation::Source;
use sm_core::protocol::Platform;

/// YouTube's own chat message limit [verify].
pub const MAX_CHAT_CHARS: usize = 200;
const COMPONENT: &str = "youtube_chat";
const AUTH_COMPONENT: &str = "youtube_auth";
/// What the token source answers when Google rejects the refresh token (`invalid_grant`): the
/// owner has to run `setup youtube` again (youtube-spec.md section 2).
pub const AUTH_EXPIRED_ERR: &str = "invalid_grant";
pub const AUTH_EXPIRED_DETAIL: &str = "YouTube sign-in expired: run setup youtube again";
const DEDUPE_KEEP: usize = 5000;

/// A request relative to https://www.googleapis.com/youtube/v3.
#[derive(Debug, Clone, PartialEq)]
pub struct YtRequest {
    pub method: &'static str,
    pub path: String,
    pub body: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum YtError {
    #[error("bad argument: {0}")]
    BadArg(&'static str),
}

/// Percent-encodes everything but RFC 3986 unreserved characters.
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn req(method: &'static str, path: String, body: Option<Value>) -> YtRequest {
    YtRequest { method, path, body }
}

pub fn active_broadcasts() -> YtRequest {
    req("GET", format!("/liveBroadcasts?part={}&broadcastStatus=active", enc("snippet,status")), None)
}

pub fn chat_list(live_chat_id: &str, page_token: Option<&str>) -> YtRequest {
    let mut p = format!("/liveChat/messages?liveChatId={}&part={}&maxResults=2000", enc(live_chat_id), enc("snippet,authorDetails"));
    if let Some(t) = page_token {
        p.push_str(&format!("&pageToken={}", enc(t)));
    }
    req("GET", p, None)
}

/// `liveChatMessages.streamList` over HTTP: the connection stays open and messages are
/// pushed (youtube-spec.md section 3).
pub fn chat_stream(live_chat_id: &str, page_token: Option<&str>) -> YtRequest {
    let mut p = format!("/liveChat/messages/stream?liveChatId={}&part={}&maxResults=500", enc(live_chat_id), enc("snippet,authorDetails"));
    if let Some(t) = page_token {
        p.push_str(&format!("&pageToken={}", enc(t)));
    }
    req("GET", p, None)
}

pub fn chat_insert(live_chat_id: &str, text: &str) -> Result<YtRequest, YtError> {
    let n = text.chars().count();
    if n == 0 || n > MAX_CHAT_CHARS {
        return Err(YtError::BadArg("message must be 1-200 characters"));
    }
    Ok(req(
        "POST",
        "/liveChat/messages?part=snippet".into(),
        Some(json!({"snippet": {"liveChatId": live_chat_id, "type": "textMessageEvent", "textMessageDetails": {"messageText": text}}})),
    ))
}

/// `message_id` is YouTube's own id (without our `yt:` prefix).
pub fn chat_delete(message_id: &str) -> Result<YtRequest, YtError> {
    if message_id.is_empty() {
        return Err(YtError::BadArg("message id is required"));
    }
    Ok(req("DELETE", format!("/liveChat/messages?id={}", enc(message_id)), None))
}

/// A temporary ban (timeout) when `duration_s` is set, a permanent one otherwise. The
/// response's ban id is needed to lift it (`unban`).
pub fn ban(live_chat_id: &str, channel_id: &str, duration_s: Option<u32>) -> Result<YtRequest, YtError> {
    let mut snippet = json!({"liveChatId": live_chat_id, "type": "permanent", "bannedUserDetails": {"channelId": channel_id}});
    if let Some(d) = duration_s {
        if d == 0 {
            return Err(YtError::BadArg("ban duration must be at least 1 second"));
        }
        snippet["type"] = json!("temporary");
        snippet["banDurationSeconds"] = json!(d);
    }
    Ok(req("POST", "/liveChat/bans?part=snippet".into(), Some(json!({ "snippet": snippet }))))
}

pub fn unban(ban_id: &str) -> Result<YtRequest, YtError> {
    if ban_id.is_empty() {
        return Err(YtError::BadArg("ban id is required"));
    }
    Ok(req("DELETE", format!("/liveChat/bans?id={}", enc(ban_id)), None))
}

/// Ends the broadcast: "YouTube stops transmitting video" [doc].
pub fn transition_complete(broadcast_id: &str) -> YtRequest {
    req("POST", format!("/liveBroadcasts/transition?broadcastStatus=complete&id={}&part=status", enc(broadcast_id)), None)
}

/// The API reports no membership price, so a configured one is used (§6.3).
#[derive(Debug, Clone, PartialEq)]
pub struct MembershipPrice {
    pub amount_micros: u64,
    pub currency: String,
}

impl Default for MembershipPrice {
    fn default() -> Self {
        MembershipPrice { amount_micros: 4_990_000, currency: "USD".into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatPage {
    pub next_page_token: Option<String>,
    pub polling_interval_ms: u64,
    pub offline: bool,
    /// (YouTube message id, mapped event or None for types we don't act on).
    pub items: Vec<(String, Option<InEvent>)>,
}

fn s(v: &Value) -> Option<String> {
    v.as_str().map(str::to_string)
}

/// amountMicros arrives as a string in the API; accept a number too.
fn micros(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str().and_then(|x| x.parse().ok()))
}

fn map_item(it: &Value, id: &str, price: &MembershipPrice) -> Option<InEvent> {
    let sn = &it["snippet"];
    let ad = &it["authorDetails"];
    let channel = ad["channelId"].as_str().or(sn["authorChannelId"].as_str())?;
    let viewer_id = Some(format!("youtube:{channel}"));
    let display = s(&ad["displayName"]);
    let external_id = format!("yt-msg:{id}");
    let membership = |n: u64, text: Option<String>| InEvent::Donation {
        platform: Some(Platform::Youtube),
        viewer_id: viewer_id.clone(),
        display: display.clone(),
        source: Source::YouTubeMembership { amount_micros: price.amount_micros.saturating_mul(n), currency: price.currency.clone() },
        text,
        external_id: external_id.clone(),
    };
    match sn["type"].as_str()? {
        "textMessageEvent" => {
            let mut roles = Vec::new();
            for (flag, role) in [("isChatOwner", "owner"), ("isChatModerator", "moderator"), ("isChatSponsor", "member"), ("isVerified", "verified")] {
                if ad[flag].as_bool() == Some(true) {
                    roles.push(role.to_string());
                }
            }
            Some(InEvent::Chat {
                platform: Platform::Youtube,
                viewer_id: viewer_id.clone()?,
                login: channel.to_string(),
                display: display.clone().unwrap_or_default(),
                text: s(&sn["textMessageDetails"]["messageText"]).or_else(|| s(&sn["displayMessage"])).unwrap_or_default(),
                msg_id: format!("yt:{id}"),
                roles,
                paid_sub: ad["isChatSponsor"].as_bool() == Some(true),
            })
        }
        "superChatEvent" => {
            let d = &sn["superChatDetails"];
            Some(InEvent::Donation {
                platform: Some(Platform::Youtube),
                viewer_id,
                display,
                source: Source::YouTubeSuperChat { amount_micros: micros(&d["amountMicros"])?, currency: s(&d["currency"])? },
                text: s(&d["userComment"]),
                external_id,
            })
        }
        "superStickerEvent" => {
            let d = &sn["superStickerDetails"];
            Some(InEvent::Donation {
                platform: Some(Platform::Youtube),
                viewer_id,
                display,
                source: Source::YouTubeSuperChat { amount_micros: micros(&d["amountMicros"])?, currency: s(&d["currency"])? },
                text: None,
                external_id,
            })
        }
        "newSponsorEvent" => Some(membership(1, None)),
        "membershipGiftingEvent" => Some(membership(sn["membershipGiftingDetails"]["giftMembershipsCount"].as_u64()?, None)),
        "memberMilestoneChatEvent" => Some(membership(1, s(&sn["memberMilestoneChatDetails"]["userComment"]))),
        "chatEndedEvent" => Some(InEvent::StreamState { platform: Platform::Youtube, live: false }),
        // giftMembershipReceivedEvent: the recipient; the gifter was credited.
        _ => None,
    }
}

pub fn parse_chat_page(body: &str, price: &MembershipPrice) -> Result<ChatPage, String> {
    let v: Value = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let items = v["items"]
        .as_array()
        .map(|a| a.iter().filter_map(|it| it["id"].as_str().map(|id| (id.to_string(), map_item(it, id, price)))).collect())
        .unwrap_or_default();
    Ok(ChatPage {
        next_page_token: s(&v["nextPageToken"]),
        polling_interval_ms: v["pollingIntervalMillis"].as_u64().unwrap_or(5000),
        offline: !v["offlineAt"].is_null(),
        items,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaBudget {
    pub units_per_day: u64,
    pub reserved_for_actions: u64,
    pub list_cost: u64,
    /// Hours of streaming the list budget must cover.
    pub budget_hours: f64,
}

impl Default for QuotaBudget {
    fn default() -> Self {
        QuotaBudget { units_per_day: 10_000, reserved_for_actions: 2_000, list_cost: 5, budget_hours: 8.0 }
    }
}

/// Seconds between list calls: the budget spread evenly, never faster than the server asks.
pub fn poll_interval_s(b: &QuotaBudget, server_ms: u64) -> f64 {
    let window = b.budget_hours * 3600.0;
    let calls = b.units_per_day.saturating_sub(b.reserved_for_actions) / b.list_cost.max(1);
    let spread = if calls == 0 { window } else { window / calls as f64 };
    spread.max(server_ms as f64 / 1000.0)
}

pub fn error_reason(body: &str) -> Option<String> {
    let v: Value = serde_json::from_str(body).ok()?;
    v.pointer("/error/errors/0/reason").and_then(Value::as_str).map(str::to_string)
}

/// How chat comes in: pushed by `streamList`, or polled with `list` (the fallback).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatMode {
    Stream,
    #[default]
    Poll,
}

/// Cuts a byte stream into its JSON objects: concatenated objects and newline-delimited
/// objects both work, and an object may arrive split across chunks (youtube-spec.md §3).
#[derive(Default)]
pub struct StreamFramer {
    buf: String,
}

impl StreamFramer {
    /// Adds a chunk; returns each complete object found, as text. An error means the data is
    /// not JSON objects at all.
    pub fn push(&mut self, chunk: &str) -> Result<Vec<String>, String> {
        self.buf.push_str(chunk);
        let mut out = Vec::new();
        let mut start = 0;
        let mut bad = None;
        {
            let mut de = serde_json::Deserializer::from_str(&self.buf).into_iter::<serde::de::IgnoredAny>();
            loop {
                match de.next() {
                    Some(Ok(_)) => {
                        let end = de.byte_offset();
                        out.push(self.buf[start..end].trim().to_string());
                        start = end;
                    }
                    Some(Err(e)) if e.is_eof() => break,
                    Some(Err(e)) => {
                        bad = Some(e.to_string());
                        break;
                    }
                    None => {
                        start = self.buf.len();
                        break;
                    }
                }
            }
        }
        if let Some(e) = bad {
            return Err(e);
        }
        self.buf.drain(..start);
        Ok(out)
    }
}

/// Sends one request (adding the OAuth bearer) and returns (status, body).
#[async_trait::async_trait]
pub trait YtHttp: Send + Sync {
    async fn call(&self, req: &YtRequest) -> Result<(u16, String), String>;

    /// A request whose response body arrives in pieces over time (`streamList`): the status
    /// and a receiver of body chunks, closed when the server ends the stream. The default
    /// delivers the whole body as one chunk.
    async fn stream(&self, req: &YtRequest) -> Result<(u16, mpsc::Receiver<String>), String> {
        let (status, body) = self.call(req).await?;
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(body).await;
        Ok((status, rx))
    }
}

#[derive(Debug, Clone)]
pub struct YtConfig {
    pub chat_mode: ChatMode,
    pub budget: QuotaBudget,
    pub membership: MembershipPrice,
    /// Seconds between broadcast lookups in the fast window (after the bridge starts, or Twitch goes live).
    pub no_broadcast_retry_s: f64,
    pub discovery_fast_window_s: f64,
    /// Seconds between lookups outside the fast window.
    pub discovery_slow_s: f64,
    pub error_backoff_s: f64,
    pub quota_backoff_s: f64,
    /// Wait after a "sent too quickly" error.
    pub rate_limit_wait_s: f64,
    /// Never reconnect a stream sooner than this.
    pub stream_min_wait_s: f64,
    /// A stream with no bytes for this long is closed and reconnected as a clean end (a quiet chat
    /// looks the same as a hung connection; youtube-spec.md Amendment 2).
    pub stream_idle_s: f64,
    /// This many failures within `failure_window_s` mean `error_backoff_s`.
    pub failures_before_backoff: usize,
    pub failure_window_s: f64,
}

impl Default for YtConfig {
    fn default() -> Self {
        YtConfig {
            chat_mode: ChatMode::Poll,
            budget: QuotaBudget::default(),
            membership: MembershipPrice::default(),
            no_broadcast_retry_s: 30.0,
            discovery_fast_window_s: 900.0,
            discovery_slow_s: 600.0,
            error_backoff_s: 60.0,
            quota_backoff_s: 3600.0,
            rate_limit_wait_s: 10.0,
            stream_min_wait_s: 2.0,
            stream_idle_s: 300.0,
            failures_before_backoff: 3,
            failure_window_s: 300.0,
        }
    }
}

struct Dedupe {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl Dedupe {
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

/// (broadcast id, live chat id, the channel's own id) of the first active broadcast.
pub fn find_chat(body: &str) -> Option<(String, String, Option<String>)> {
    let v: Value = serde_json::from_str(body).ok()?;
    let b = v["items"].as_array()?.iter().find(|b| b["snippet"]["liveChatId"].is_string())?;
    Some((s(&b["id"])?, s(&b["snippet"]["liveChatId"])?, s(&b["snippet"]["channelId"])))
}

async fn sleep_s(s: f64) {
    tokio::time::sleep(Duration::from_secs_f64(s.max(0.0))).await;
}

/// Waits between broadcast lookups: 30 s inside the fast window, 10 minutes outside it. A
/// Twitch stream going live starts a new fast window and ends the wait at once.
async fn discovery_wait(cfg: &YtConfig, fast_until: &mut Instant, twitch_live: &mut Option<watch::Receiver<bool>>) {
    let now = Instant::now();
    let wait = if now < *fast_until { cfg.no_broadcast_retry_s } else { cfg.discovery_slow_s };
    let deadline = now + Duration::from_secs_f64(wait.max(0.0));
    let Some(rx) = twitch_live.as_mut() else {
        tokio::time::sleep_until(deadline).await;
        return;
    };
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return,
            r = rx.changed() => match r {
                Ok(()) => {
                    if *rx.borrow_and_update() {
                        *fast_until = Instant::now() + Duration::from_secs_f64(cfg.discovery_fast_window_s);
                        return;
                    }
                }
                Err(_) => {
                    tokio::time::sleep_until(deadline).await;
                    return;
                }
            },
        }
    }
}

struct ChatState {
    dedupe: Dedupe,
    token: Option<String>,
    /// No page seen yet on this connection chain: history from before we joined.
    first_page: bool,
    healthy: bool,
}

/// Forwards one page's events. Ok(true) when the chat went offline; Err when the receiver is gone.
async fn ingest(st: &mut ChatState, page: ChatPage, out: &mpsc::Sender<InEvent>, detail: &str) -> Result<bool, ()> {
    if !st.healthy {
        st.healthy = true;
        out.send(InEvent::Health { component: COMPONENT, up: true, detail: detail.into() }).await.map_err(|_| ())?;
    }
    for (id, ev) in page.items {
        // The first page is history from before we joined: remember, don't forward.
        if st.dedupe.insert(&id) && !st.first_page {
            if let Some(e) = ev {
                out.send(e).await.map_err(|_| ())?;
            }
        }
    }
    st.first_page = false;
    if page.next_page_token.is_some() {
        st.token = page.next_page_token;
    }
    if page.offline {
        out.send(InEvent::StreamState { platform: Platform::Youtube, live: false }).await.map_err(|_| ())?;
        return Ok(true);
    }
    Ok(false)
}

/// Runs until the event receiver is dropped (or the task is aborted).
pub async fn run_youtube(cfg: YtConfig, http: Arc<dyn YtHttp>, out: mpsc::Sender<InEvent>) {
    run_youtube_with(cfg, http, out, None).await
}

/// `twitch_live`: whether Twitch is live (simulcast); a rising edge starts a fast broadcast
/// lookup window (youtube-spec.md §4).
pub async fn run_youtube_with(
    cfg: YtConfig,
    http: Arc<dyn YtHttp>,
    out: mpsc::Sender<InEvent>,
    mut twitch_live: Option<watch::Receiver<bool>>,
) {
    let mut st = ChatState { dedupe: Dedupe { seen: HashSet::new(), order: VecDeque::new() }, token: None, first_page: true, healthy: false };
    let mut chat: Option<String> = None;
    let mut fast_until = Instant::now() + Duration::from_secs_f64(cfg.discovery_fast_window_s);
    let mut fails: VecDeque<Instant> = VecDeque::new();
    let mut last_polling_ms: u64 = 0;
    macro_rules! send {
        ($e:expr) => {
            if out.send($e).await.is_err() {
                return;
            }
        };
    }
    macro_rules! down {
        ($detail:expr) => {{
            st.healthy = false;
            send!(InEvent::Health { component: COMPONENT, up: false, detail: $detail });
        }};
    }
    // The sign-in was rejected: say so once a minute at most (the token source makes no call while parked).
    macro_rules! auth_down {
        () => {{
            send!(InEvent::Health { component: AUTH_COMPONENT, up: false, detail: AUTH_EXPIRED_DETAIL.to_string() });
            sleep_s(cfg.error_backoff_s).await;
        }};
    }
    // A stream failure: reconnect soon, or back off after several within the window.
    macro_rules! failure {
        ($detail:expr) => {{
            down!($detail);
            let now = Instant::now();
            fails.push_back(now);
            while fails.front().is_some_and(|t| now.duration_since(*t) > Duration::from_secs_f64(cfg.failure_window_s)) {
                fails.pop_front();
            }
            if fails.len() >= cfg.failures_before_backoff {
                fails.clear();
                sleep_s(cfg.error_backoff_s).await;
            } else {
                sleep_s(cfg.stream_min_wait_s.max(last_polling_ms as f64 / 1000.0)).await;
            }
        }};
    }
    // The same answers to an error reply in either mode.
    macro_rules! error_reply {
        ($status:expr, $reason:expr, $what:expr) => {{
            let reason: String = $reason;
            match reason.as_str() {
                "liveChatEnded" | "liveChatNotFound" | "liveChatDisabled" => {
                    send!(InEvent::StreamState { platform: Platform::Youtube, live: false });
                    chat = None;
                }
                r if is_quota(r) => {
                    down!(format!("YouTube quota exceeded ({r}); waiting {} s", cfg.quota_backoff_s));
                    sleep_s(cfg.quota_backoff_s).await;
                }
                r if is_rate_limit(r) => {
                    down!(format!("{} sent too quickly ({r}); waiting {} s", $what, cfg.rate_limit_wait_s));
                    sleep_s(cfg.rate_limit_wait_s).await;
                }
                _ => {
                    if cfg.chat_mode == ChatMode::Stream {
                        failure!(format!("{} failed: HTTP {} {}", $what, $status, reason));
                    } else {
                        down!(format!("{} failed: HTTP {} {}", $what, $status, reason));
                        sleep_s(cfg.error_backoff_s).await;
                    }
                }
            }
        }};
    }
    loop {
        let Some(live_chat_id) = chat.clone() else {
            match http.call(&active_broadcasts()).await {
                Ok((200, body)) => match find_chat(&body) {
                    Some((broadcast_id, live_chat_id, channel_id)) => {
                        send!(InEvent::BroadcastFound { platform: Platform::Youtube, broadcast_id, live_chat_id: live_chat_id.clone(), channel_id });
                        chat = Some(live_chat_id);
                        st.token = None;
                        st.first_page = true;
                    }
                    None => {
                        down!("no active broadcast".to_string());
                        discovery_wait(&cfg, &mut fast_until, &mut twitch_live).await;
                    }
                },
                Ok((status, body)) => {
                    let reason = error_reason(&body).unwrap_or_default();
                    down!(format!("broadcast lookup failed: HTTP {status} {reason}"));
                    let wait = if is_quota(&reason) { cfg.quota_backoff_s } else { cfg.error_backoff_s };
                    sleep_s(wait).await;
                }
                Err(e) if e == AUTH_EXPIRED_ERR => auth_down!(),
                Err(e) => {
                    down!(format!("broadcast lookup failed: {e}"));
                    sleep_s(cfg.error_backoff_s).await;
                }
            }
            continue;
        };
        if cfg.chat_mode == ChatMode::Poll {
            match http.call(&chat_list(&live_chat_id, st.token.as_deref())).await {
                Ok((200, body)) => match parse_chat_page(&body, &cfg.membership) {
                    Ok(page) => {
                        let polling = page.polling_interval_ms;
                        match ingest(&mut st, page, &out, "polling").await {
                            Err(()) => return,
                            Ok(true) => chat = None,
                            Ok(false) => {}
                        }
                        sleep_s(poll_interval_s(&cfg.budget, polling)).await;
                    }
                    Err(e) => {
                        down!(format!("unreadable chat page: {e}"));
                        sleep_s(cfg.error_backoff_s).await;
                    }
                },
                Ok((status, body)) => error_reply!(status, error_reason(&body).unwrap_or_default(), "chat poll"),
                Err(e) if e == AUTH_EXPIRED_ERR => auth_down!(),
                Err(e) => {
                    down!(format!("chat poll failed: {e}"));
                    sleep_s(cfg.error_backoff_s).await;
                }
            }
            continue;
        }
        match http.stream(&chat_stream(&live_chat_id, st.token.as_deref())).await {
            Err(e) if e == AUTH_EXPIRED_ERR => auth_down!(),
            Err(e) => failure!(format!("chat stream failed: {e}")),
            Ok((200, mut rx)) => {
                let mut framer = StreamFramer::default();
                let mut went_offline = false;
                let mut bad: Option<String> = None;
                'read: loop {
                    let chunk = match tokio::time::timeout(Duration::from_secs_f64(cfg.stream_idle_s), rx.recv()).await {
                        Ok(Some(c)) => c,
                        // The server ended the stream, or it was silent too long: a clean end.
                        Ok(None) | Err(_) => break 'read,
                    };
                    match framer.push(&chunk) {
                        Err(e) => {
                            bad = Some(e);
                            break 'read;
                        }
                        Ok(objects) => {
                            for obj in objects {
                                match parse_chat_page(&obj, &cfg.membership) {
                                    Ok(page) => {
                                        last_polling_ms = page.polling_interval_ms;
                                        match ingest(&mut st, page, &out, "streaming").await {
                                            Err(()) => return,
                                            Ok(true) => {
                                                went_offline = true;
                                                break 'read;
                                            }
                                            Ok(false) => {}
                                        }
                                    }
                                    Err(e) => {
                                        bad = Some(e);
                                        break 'read;
                                    }
                                }
                            }
                        }
                    }
                }
                if went_offline {
                    chat = None;
                } else if let Some(e) = bad {
                    failure!(format!("unreadable chat stream: {e}"));
                } else {
                    // The server ended the stream: reconnect with the saved token.
                    sleep_s(cfg.stream_min_wait_s.max(last_polling_ms as f64 / 1000.0)).await;
                }
            }
            Ok((status, mut rx)) => {
                let mut body = String::new();
                while let Some(c) = rx.recv().await {
                    body.push_str(&c);
                }
                error_reply!(status, error_reason(&body).unwrap_or_default(), "chat stream");
            }
        }
    }
}

fn is_quota(reason: &str) -> bool {
    matches!(reason, "quotaExceeded" | "dailyLimitExceeded")
}

fn is_rate_limit(reason: &str) -> bool {
    reason == "rateLimitExceeded"
}
