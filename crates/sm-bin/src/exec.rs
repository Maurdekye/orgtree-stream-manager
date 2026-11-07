//! Executors: run the bridge's platform [`Action`]s against Twitch Helix, the YouTube Data
//! API and OBS. Also the production `EventSubSubscriber` and `YtHttp` used by the
//! connectors.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use sm_bridge::Action;
use sm_connectors::obs::{ObsClient, ObsError};
use sm_connectors::twitch::eventsub::{EventSubSubscriber, SubSpec};
use sm_connectors::twitch::helix::{self, HelixRequest};
use sm_connectors::youtube::{self, YtHttp, YtRequest};
use sm_connectors::ytstate::{YtBans, YtQuota};
use sm_core::protocol::Platform;

use crate::http::{HttpClient, HttpReq};
use crate::overlay::OverlayHub;
use crate::tokens::TokenSource;

pub struct TwitchCtx {
    pub api_base: String,
    pub client_id: String,
    pub broadcaster_id: String,
    pub bot_user_id: String,
    pub tokens: Arc<TokenSource>,
}

pub struct YtCtx {
    pub api_base: String,
    pub tokens: Arc<TokenSource>,
    /// (broadcast id, live chat id) once the poller found them.
    pub chat: Arc<Mutex<Option<(String, String)>>>,
    /// The ban ids YouTube returned (youtube-spec.md section 6).
    pub bans: Arc<YtBans>,
    /// Units spent today; every request made is counted (section 5).
    pub quota: Option<Arc<YtQuota>>,
}

fn now_s() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

pub struct ObsCtx {
    pub url: String,
    pub password: Option<String>,
    /// effect id → OBS media input.
    pub media_inputs: HashMap<String, String>,
    client: tokio::sync::Mutex<Option<ObsClient>>,
}

impl ObsCtx {
    pub fn new(url: String, password: Option<String>, media_inputs: HashMap<String, String>) -> Self {
        ObsCtx { url, password, media_inputs, client: tokio::sync::Mutex::new(None) }
    }
}

/// The overlay page (OBS browser source) and which effects animate on it.
pub struct OverlayCtx {
    pub hub: Arc<OverlayHub>,
    /// effect id → animation name.
    pub animations: HashMap<String, String>,
}

pub struct Executor {
    pub http: Arc<HttpClient>,
    pub twitch: Option<TwitchCtx>,
    pub youtube: Option<YtCtx>,
    pub obs: Option<ObsCtx>,
    pub overlay: Option<OverlayCtx>,
}

fn ok2xx(status: u16, what: &str) -> Result<(), String> {
    match status {
        200..=299 => Ok(()),
        429 => Err(format!("{what}: rate limited")),
        s => Err(format!("{what}: HTTP {s}")),
    }
}

/// One Helix call with Client-Id and bearer; a 401 refreshes the token and retries once.
pub async fn helix_send(http: &HttpClient, t: &TwitchCtx, req: &HelixRequest) -> Result<(u16, String), String> {
    for attempt in 0..2 {
        let bearer = t.tokens.bearer().await?;
        let mut headers = vec![("Client-Id".to_string(), t.client_id.clone()), ("Authorization".to_string(), format!("Bearer {bearer}"))];
        if req.body.is_some() {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        let (status, body) = http
            .send(HttpReq {
                method: req.method,
                url: format!("{}{}", t.api_base, req.path),
                headers,
                body: req.body.as_ref().map(|b| b.to_string().into_bytes()),
            })
            .await?;
        if status == 401 && attempt == 0 {
            t.tokens.invalidate().await;
            continue;
        }
        return Ok((status, body));
    }
    unreachable!("the loop returns on its second pass")
}

/// One YouTube call with the bearer; a 401 refreshes the token and retries once. Each request made
/// is counted against the day's quota.
pub async fn yt_send(http: &HttpClient, y: &YtCtx, req: &YtRequest) -> Result<(u16, String), String> {
    for attempt in 0..2 {
        let bearer = y.tokens.bearer().await?;
        if let Some(q) = &y.quota {
            q.record_request(now_s(), req);
        }
        let mut headers = vec![("Authorization".to_string(), format!("Bearer {bearer}"))];
        if req.body.is_some() {
            headers.push(("Content-Type".into(), "application/json".into()));
        }
        let (status, body) = http
            .send(HttpReq {
                method: req.method,
                url: format!("{}{}", y.api_base, req.path),
                headers,
                body: req.body.as_ref().map(|b| b.to_string().into_bytes()),
            })
            .await?;
        if status == 401 && attempt == 0 {
            y.tokens.invalidate().await;
            continue;
        }
        return Ok((status, body));
    }
    unreachable!("the loop returns on its second pass")
}

/// A YouTube call whose response keeps arriving (`streamList`); same bearer, 401 retry and quota count.
pub async fn yt_stream(http: &HttpClient, y: &YtCtx, req: &YtRequest) -> Result<(u16, tokio::sync::mpsc::Receiver<String>), String> {
    for attempt in 0..2 {
        let bearer = y.tokens.bearer().await?;
        if let Some(q) = &y.quota {
            q.record_request(now_s(), req);
        }
        let (status, rx) = http
            .stream(HttpReq {
                method: req.method,
                url: format!("{}{}", y.api_base, req.path),
                headers: vec![("Authorization".to_string(), format!("Bearer {bearer}"))],
                body: None,
            })
            .await?;
        if status == 401 && attempt == 0 {
            y.tokens.invalidate().await;
            continue;
        }
        return Ok((status, rx));
    }
    unreachable!("the loop returns on its second pass")
}

impl Executor {
    fn tw(&self) -> Result<&TwitchCtx, String> {
        self.twitch.as_ref().ok_or_else(|| "Twitch is not configured".to_string())
    }

    fn yt(&self) -> Result<&YtCtx, String> {
        self.youtube.as_ref().ok_or_else(|| "YouTube is not configured".to_string())
    }

    fn yt_chat(y: &YtCtx) -> Result<(String, String), String> {
        y.chat.lock().unwrap_or_else(|e| e.into_inner()).clone().ok_or_else(|| "no YouTube live chat known yet".to_string())
    }

    async fn helix(&self, req: Result<HelixRequest, helix::HelixError>, what: &str) -> Result<(), String> {
        let t = self.tw()?;
        let req = req.map_err(|e| e.to_string())?;
        let (status, _) = helix_send(&self.http, t, &req).await?;
        ok2xx(status, what)
    }

    async fn ytreq(&self, req: Result<YtRequest, youtube::YtError>, what: &str) -> Result<(), String> {
        let y = self.yt()?;
        let req = req.map_err(|e| e.to_string())?;
        let (status, body) = yt_send(&self.http, y, &req).await?;
        ok2xx(status, what).map_err(|e| match youtube::error_reason(&body) {
            Some(r) => format!("{e} ({r})"),
            None => e,
        })
    }

    pub async fn run(&self, a: &Action) -> Result<(), String> {
        match a {
            Action::Chat { platform: Platform::Twitch, text, reply_to } => {
                let t = self.tw()?;
                let req = helix::send_chat(&t.broadcaster_id, &t.bot_user_id, text, reply_to.as_deref()).map_err(|e| e.to_string())?;
                let (status, body) = helix_send(&self.http, t, &req).await?;
                helix::parse_send_chat(status, &body).map(|_| ()).map_err(|e| e.to_string())
            }
            Action::Chat { platform: Platform::Youtube, text, .. } => {
                let (_, live_chat) = Self::yt_chat(self.yt()?)?;
                self.ytreq(youtube::chat_insert(&live_chat, text), "YouTube chat").await
            }
            Action::Ban { platform: Platform::Twitch, user, duration_s, reason } => {
                let t = self.tw()?;
                self.helix(helix::ban(&t.broadcaster_id, &t.bot_user_id, user, *duration_s, reason), "Twitch ban").await
            }
            Action::Ban { platform: Platform::Youtube, user, duration_s, .. } => {
                let y = self.yt()?;
                let (_, live_chat) = Self::yt_chat(y)?;
                let req = youtube::ban(&live_chat, user, *duration_s).map_err(|e| e.to_string())?;
                let (status, body) = yt_send(&self.http, y, &req).await?;
                ok2xx(status, "YouTube ban").map_err(|e| match youtube::error_reason(&body) {
                    Some(r) => format!("{e} ({r})"),
                    None => e,
                })?;
                // Keep the ban id so the ban can be lifted (section 6).
                if let Some(id) = serde_json::from_str::<Value>(&body).ok().and_then(|v| v["id"].as_str().map(str::to_string)) {
                    y.bans.put(user, &id, duration_s.map(|d| now_s() + d as f64));
                }
                Ok(())
            }
            Action::Unban { platform: Platform::Twitch, user } => {
                let t = self.tw()?;
                self.helix(helix::unban(&t.broadcaster_id, &t.bot_user_id, user), "Twitch unban").await
            }
            Action::Unban { platform: Platform::Youtube, user } => {
                let y = self.yt()?;
                let record = y
                    .bans
                    .take_record(user, now_s())
                    .ok_or_else(|| "no ban of this viewer is known (made outside the bridge or before this version)".to_string())?;
                let r = self.ytreq(youtube::unban(&record.0), "YouTube unban").await;
                if r.is_err() {
                    // Keep the record exactly as it was (same expiry) so it can be tried again.
                    y.bans.restore(user, record, now_s());
                }
                r
            }
            Action::Delete { platform: Platform::Twitch, message_id } => {
                let t = self.tw()?;
                self.helix(helix::delete_message(&t.broadcaster_id, &t.bot_user_id, message_id), "Twitch delete").await
            }
            Action::Delete { platform: Platform::Youtube, message_id } => self.ytreq(youtube::chat_delete(message_id), "YouTube delete").await,
            Action::SlowMode { seconds } => {
                let t = self.tw()?;
                self.helix(helix::slow_mode(&t.broadcaster_id, &t.bot_user_id, *seconds), "Twitch slow mode").await
            }
            Action::YoutubeEnd { broadcast_id } => self.ytreq(Ok(youtube::transition_complete(broadcast_id)), "YouTube end").await,
            Action::PlayEffect { effect, donor_label, usd } => {
                // An effect can be a sound (OBS media input), an overlay animation, or both.
                let mut played = false;
                if let Some(ov) = &self.overlay {
                    if let Some(anim) = ov.animations.get(effect) {
                        ov.hub.send(effect, anim, donor_label.as_deref(), *usd);
                        played = true;
                    }
                }
                if self.obs.as_ref().is_some_and(|o| o.media_inputs.contains_key(effect)) {
                    self.obs_run(a).await?;
                    played = true;
                }
                if played {
                    Ok(())
                } else {
                    Err(format!("effect {effect} has no OBS media input or overlay animation"))
                }
            }
            Action::ObsTransition { .. } | Action::ObsScene { .. } | Action::ObsStop => self.obs_run(a).await,
        }
    }

    async fn obs_run(&self, a: &Action) -> Result<(), String> {
        let o = self.obs.as_ref().ok_or_else(|| "OBS is not configured".to_string())?;
        if let Action::PlayEffect { effect, .. } = a {
            if !o.media_inputs.contains_key(effect) {
                return Err(format!("effect {effect} has no OBS media input"));
            }
        }
        let mut guard = o.client.lock().await;
        if guard.is_none() {
            *guard = Some(ObsClient::connect(&o.url, o.password.as_deref()).await.map_err(|e| e.to_string())?);
        }
        let c = guard.as_ref().expect("connected above");
        let r = match a {
            Action::ObsTransition { name, duration_ms } => c.set_transition(name, *duration_ms).await,
            Action::ObsScene { scene } => c.set_program_scene(scene).await,
            Action::ObsStop => c.stop_stream().await,
            Action::PlayEffect { effect, .. } => c.restart_media(&o.media_inputs[effect]).await,
            _ => return Err("not an OBS action".into()),
        };
        if matches!(r, Err(ObsError::Closed | ObsError::Timeout)) {
            *guard = None; // reconnect next time
        }
        r.map_err(|e| e.to_string())
    }
}

/// Creates the EventSub subscriptions for a new WebSocket session.
pub struct HelixSubscriber {
    pub http: Arc<HttpClient>,
    pub ctx: TwitchCtx,
}

#[async_trait::async_trait]
impl EventSubSubscriber for HelixSubscriber {
    async fn subscribe(&self, session_id: &str, specs: &[SubSpec]) -> Result<(), String> {
        for spec in specs {
            let req = helix::create_subscription(spec, session_id);
            let (status, body) = helix_send(&self.http, &self.ctx, &req).await?;
            // 202 Accepted; 409 means it already exists.
            if !(status == 202 || status == 200 || status == 409) {
                let msg = serde_json::from_str::<Value>(&body).ok().and_then(|v| v["message"].as_str().map(str::to_string)).unwrap_or_default();
                return Err(format!("{}: HTTP {status} {msg}", spec.sub_type));
            }
        }
        Ok(())
    }
}

/// The YouTube poller's HTTP layer.
pub struct YtHttpImpl {
    pub http: Arc<HttpClient>,
    pub ctx: YtCtx,
}

#[async_trait::async_trait]
impl YtHttp for YtHttpImpl {
    async fn call(&self, req: &YtRequest) -> Result<(u16, String), String> {
        yt_send(&self.http, &self.ctx, req).await
    }

    async fn stream(&self, req: &YtRequest) -> Result<(u16, tokio::sync::mpsc::Receiver<String>), String> {
        yt_stream(&self.http, &self.ctx, req).await
    }
}
