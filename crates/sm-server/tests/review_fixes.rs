//! Review round 1, server findings 7, 10, 11.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sm_core::protocol::Command;
use sm_server::stats::{LimitRow, Stats, MAX_BYTES};
use sm_server::{CommandHandler, Hub, Scopes, ServerConfig, TokenTable};
use sm_store::Store;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Ok_;
impl CommandHandler for Ok_ {
    fn handle(&self, _: &str, _: &Command) -> Result<Value, (String, String)> {
        Ok(json!({}))
    }
}

const READ: &str = "read-token-0123456789abcdef0123456789";
const CONTROL: &str = "control-token-0123456789abcdef01234567";

async fn rig() -> (Arc<Hub>, String) {
    let clock = Arc::new(Mutex::new(1_000_000.0f64));
    let tokens = TokenTable::new(vec![
        ("listener-read".into(), READ.into(), Scopes { read: true, control: false, moderate: false }),
        ("listener-control".into(), CONTROL.into(), Scopes { read: true, control: true, moderate: false }),
    ]);
    let cfg = ServerConfig { tokens, replay_max: 200, replay_window_s: 900.0, hello_timeout_s: 5.0 };
    let hub = Hub::new(Store::open_in_memory().unwrap(), cfg, Arc::new(Ok_), Arc::new(move || *clock.lock().unwrap()));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/v1/events", l.local_addr().unwrap());
    let r = hub.router();
    tokio::spawn(async move { axum::serve(l, r).await.unwrap() });
    (hub, url)
}

async fn connect(url: &str, token: &str, sub: &str, kinds: Option<&[&str]>) -> Ws {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert("Authorization", format!("Bearer {token}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let mut h = json!({"type":"subscribe","v":1,"subscriber":sub});
    if let Some(k) = kinds {
        h["kinds"] = json!(k);
    }
    ws.send(Message::text(h.to_string())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    ws
}

async fn next(ws: &mut Ws, ms: u64) -> Option<Value> {
    loop {
        match tokio::time::timeout(Duration::from_millis(ms), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return Some(serde_json::from_str(t.as_str()).unwrap()),
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

// Finding 7a: a read token can't move another token's cursor by reusing its subscriber name.
#[tokio::test]
async fn cursors_are_kept_per_token_and_subscriber() {
    let (hub, url) = rig().await;
    let mut ctl = connect(&url, CONTROL, "shared", None).await;
    hub.publish("chat", json!({"n": 1}));
    assert_eq!(next(&mut ctl, 2000).await.unwrap()["n"], 1);
    ctl.close(None).await.unwrap();
    drop(ctl);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut rd = connect(&url, READ, "shared", None).await;
    hub.publish("chat", json!({"n": 2}));
    assert_eq!(next(&mut rd, 2000).await.unwrap()["n"], 2);
    drop(rd);
    let mut ctl = connect(&url, CONTROL, "shared", None).await;
    let r = next(&mut ctl, 2000).await.expect("the control subscriber's missed event is replayed");
    assert_eq!((r["n"].as_i64(), r["replay"].as_bool()), (Some(2), Some(true)));
}

// Finding 7b: refused commands are answered only to the sender, capped and rate limited.
#[tokio::test]
async fn forbidden_answers_are_private_capped_and_rate_limited() {
    let (_hub, url) = rig().await;
    let mut watcher = connect(&url, CONTROL, "watcher", Some(&["command.result"])).await;
    let mut rd = connect(&url, READ, "reader", Some(&["command.result"])).await;
    let long_id = "i".repeat(5000);
    let cmd = json!({"v":1,"type":"cmd","cmd_id": long_id, "cmd": "x".repeat(5000)}).to_string();
    for _ in 0..10 {
        rd.send(Message::text(cmd.clone())).await.unwrap();
    }
    let mut got = Vec::new();
    while let Some(v) = next(&mut rd, 500).await {
        got.push(v);
    }
    assert!(!got.is_empty() && got.len() <= 5, "at most 5 refusals a minute, got {}", got.len());
    let r = &got[0];
    assert_eq!((r["kind"].as_str(), r["error"].as_str()), (Some("command.result"), Some("forbidden")));
    assert!(r["cmd_id"].as_str().unwrap().chars().count() <= 100);
    assert!(r.get("seq").is_none(), "private answers are not logged events");
    assert!(next(&mut watcher, 500).await.is_none(), "other subscribers never see them");
}

fn row(i: usize) -> LimitRow {
    LimitRow {
        viewer: format!("twitch:{i}"),
        name: format!("viewer-name-{i}"),
        platform: "twitch".into(),
        credit_usd: 1.0,
        cooldown_s: i as f64,
        max_chars: 141,
        next_allowed_at: Some(1000.0 + i as f64),
        held_until: None,
    }
}

// Finding 10: limits are bounded and trimmed like the other lists.
#[test]
fn limits_are_bounded_and_trimmed() {
    let mut s = Stats::new();
    s.set_limits((0..3000).map(row).collect());
    let snap = s.snapshot(1.0);
    let bytes = serde_json::to_vec(&snap).unwrap().len();
    assert!(bytes <= MAX_BYTES, "{bytes}");
    let limits = snap["limits"].as_array().unwrap();
    assert!(!limits.is_empty() && limits.len() <= 200);
    assert_eq!(limits[0]["viewer"], "twitch:0", "the soonest are kept");
}

// Finding 11: the snapshot says its content is untrusted.
#[test]
fn snapshot_carries_a_trust_marker() {
    assert_eq!(Stats::new().snapshot(0.0)["trust"], "untrusted-viewer-content");
}
