//! The overlay page (plan §18): OBS loads `/overlay` as a browser source; effects arrive
//! over `/overlay/ws`. `/healthz` answers "ok" for the container health check.
//! Labels are already filtered by the bridge; the page sets them as text only.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use serde_json::json;
use tokio::sync::broadcast::{self, error::RecvError};

const PAGE: &str = include_str!("overlay.html");

pub struct OverlayHub {
    tx: broadcast::Sender<String>,
}

impl OverlayHub {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(64);
        Arc::new(OverlayHub { tx })
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/overlay", get(|| async { Html(PAGE) }))
            .route("/overlay/ws", get(ws))
            .route("/healthz", get(|| async { "ok" }))
            .with_state(self.clone())
    }

    /// Pushes one effect to every connected overlay page; returns how many received it.
    pub fn send(&self, effect: &str, animation: &str, label: Option<&str>, usd: Option<f64>) -> usize {
        let msg = json!({"effect": effect, "animation": animation, "label": label, "usd": usd}).to_string();
        self.tx.send(msg).unwrap_or(0)
    }
}

async fn ws(State(hub): State<Arc<OverlayHub>>, up: WebSocketUpgrade) -> impl IntoResponse {
    let rx = hub.tx.subscribe();
    up.on_upgrade(move |socket| pump(socket, rx))
}

async fn pump(mut socket: WebSocket, mut rx: broadcast::Receiver<String>) {
    loop {
        tokio::select! {
            m = rx.recv() => match m {
                Ok(t) => {
                    if socket.send(Message::Text(t.into())).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            },
            r = socket.recv() => match r {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            },
        }
    }
}
