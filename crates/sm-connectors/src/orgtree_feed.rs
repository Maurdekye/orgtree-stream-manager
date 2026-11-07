//! The built-in Orgtree live feed (Orgtree 4.5.6, docs/rust-engine/live-feed.md): an untokened
//! WebSocket on the host's 127.0.0.1:47830 that sends the machine-wide count of agents in a turn
//! (`agents.active`, every agent counted). It replaces the relay watchdogs' `org.turns` reports
//! (USER 2026-11-06 13:34:25Z m77693a85fa7b).
//!
//! The feed sends the current count on every connect, so a reconnect resyncs. A lost or silent
//! feed reports `FeedLost`, which holds the current sleep state: it never wakes a sleeping stream
//! and never lets an awake one fall asleep.

use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, HOST};
use tokio_tungstenite::tungstenite::Message;

use crate::model::InEvent;
use sm_core::sleep::TurnsReport;

const COMPONENT: &str = "orgtree_feed";

#[derive(Debug, Clone)]
pub struct OrgtreeFeedConfig {
    /// From the container: `ws://host.docker.internal:47830/v1/events`.
    pub url: String,
    /// The feed refuses any Host but `127.0.0.1:<port>` / `localhost:<port>` (DNS-rebinding guard).
    pub host: String,
    /// The feed sends a state at least every 30 s; this much silence counts as lost (doc: 75 s).
    pub silence_s: f64,
    /// Reconnect backoff: 1 s doubling to this.
    pub max_backoff_s: f64,
}

/// The `agents.active` count of a `state` or `event` message; None for anything else.
pub fn parse_count(text: &str) -> Option<u64> {
    let v: Value = serde_json::from_str(text).ok()?;
    if !matches!(v["type"].as_str(), Some("state" | "event")) || v["name"] != "agents.active" {
        return None;
    }
    v["payload"]["count"].as_u64()
}

async fn report(out: &mpsc::Sender<InEvent>, r: TurnsReport) -> bool {
    out.send(InEvent::OrgTurns(r)).await.is_ok()
}

async fn health(out: &mpsc::Sender<InEvent>, up: bool, detail: &str) -> bool {
    out.send(InEvent::Health { component: COMPONENT, up, detail: detail.to_string() }).await.is_ok()
}

pub async fn run_orgtree_feed(cfg: OrgtreeFeedConfig, out: mpsc::Sender<InEvent>) {
    let mut backoff = 1.0f64;
    // Report a loss once per outage, not once per failed retry.
    let mut held = false;
    loop {
        let why = match session(&cfg, &out, &mut backoff, &mut held).await {
            Err(()) => return, // the bridge has gone
            Ok(why) => why,
        };
        if !held {
            held = true;
            if !report(&out, TurnsReport::FeedLost).await {
                return;
            }
        }
        if !health(&out, false, why).await {
            return;
        }
        tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
        backoff = (backoff * 2.0).min(cfg.max_backoff_s.max(1.0));
    }
}

/// One connection. Ok(reason) when it ends, Err when the bridge's channel closed.
async fn session(cfg: &OrgtreeFeedConfig, out: &mpsc::Sender<InEvent>, backoff: &mut f64, held: &mut bool) -> Result<&'static str, ()> {
    let Ok(mut req) = cfg.url.as_str().into_client_request() else { return Ok("bad feed url") };
    let Ok(host) = HeaderValue::from_str(&cfg.host) else { return Ok("bad feed host") };
    req.headers_mut().insert(HOST, host);
    let Ok((mut ws, _)) = connect_async(req).await else { return Ok("connect failed") };
    let silence = Duration::from_secs_f64(cfg.silence_s.max(0.1));
    let mut up = false;
    loop {
        let msg = match tokio::time::timeout(silence, ws.next()).await {
            Err(_) => return Ok("feed silent"),
            Ok(None) | Ok(Some(Err(_))) => return Ok("feed closed"),
            Ok(Some(Ok(m))) => m,
        };
        if !up {
            up = true;
            *backoff = 1.0;
            if !health(out, true, "connected").await {
                return Err(());
            }
        }
        let Message::Text(t) = msg else { continue };
        if let Some(n) = parse_count(t.as_str()) {
            *held = false;
            let r = if n > 0 { TurnsReport::Active } else { TurnsReport::Idle };
            if !report(out, r).await {
                return Err(());
            }
        }
    }
}
