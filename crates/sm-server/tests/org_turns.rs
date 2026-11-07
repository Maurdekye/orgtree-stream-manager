//! org.turns (auto-sleep relay, plan rev 2): the relay's watchdog fire actions send the same static
//! command again and again, so it is exempt from the cmd_id repeat cache. It is a state report, not
//! an action: on success nothing is published or broadcast (no wake for any listener's dog).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sm_core::protocol::Command;
use sm_core::sleep::TurnsReport;
use sm_server::{CommandHandler, Hub, Scopes, ServerConfig, TokenTable};
use sm_store::Store;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Default)]
struct FakeHandler {
    calls: Mutex<Vec<Command>>,
}

impl CommandHandler for FakeHandler {
    fn handle(&self, _actor: &str, cmd: &Command) -> Result<Value, (String, String)> {
        self.calls.lock().unwrap().push(cmd.clone());
        Ok(json!({"done": true}))
    }
}

const READ: &str = "read-token-0123456789abcdef0123456789";
const CONTROL: &str = "control-token-0123456789abcdef01234567";

async fn rig() -> (Arc<FakeHandler>, String) {
    let handler = Arc::new(FakeHandler::default());
    let tokens = TokenTable::new(vec![
        ("health-read".into(), READ.into(), Scopes { read: true, control: false, moderate: false }),
        ("relay-control".into(), CONTROL.into(), Scopes { read: true, control: true, moderate: false }),
    ]);
    let cfg = ServerConfig { tokens, replay_max: 200, replay_window_s: 900.0, hello_timeout_s: 5.0 };
    let hub = Hub::new(Store::open_in_memory().unwrap(), cfg, handler.clone(), Arc::new(|| 1_000_000.0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = hub.router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    std::mem::forget(hub);
    (handler, format!("ws://{addr}/v1/events"))
}

async fn connect(url: &str, token: &str, subscriber: &str) -> Ws {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert("Authorization", format!("Bearer {token}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(Message::text(json!({"type":"subscribe","v":1,"subscriber":subscriber}).to_string())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.expect("timed out").unwrap().unwrap();
        if let Message::Text(t) = m {
            return serde_json::from_str(t.as_str()).unwrap();
        }
    }
}

async fn expect_silence(ws: &mut Ws) {
    let r = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;
    assert!(r.is_err(), "expected no message, got {r:?}");
}

const IDLE: &str = r#"{"v":1,"type":"cmd","cmd_id":"org-turns","cmd":"org.turns","args":{"state":"idle"}}"#;
const ACTIVE: &str = r#"{"v":1,"type":"cmd","cmd_id":"org-turns","cmd":"org.turns","args":{"state":"active"}}"#;

#[tokio::test]
async fn a_static_cmd_id_runs_every_time_and_nothing_is_broadcast() {
    let (h, url) = rig().await;
    let mut watcher = connect(&url, READ, "health").await;
    let mut relay = connect(&url, CONTROL, "relay").await;
    for m in [IDLE, ACTIVE, IDLE, IDLE] {
        relay.send(Message::text(m)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let calls = h.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![
            Command::OrgTurns { state: TurnsReport::Idle },
            Command::OrgTurns { state: TurnsReport::Active },
            Command::OrgTurns { state: TurnsReport::Idle },
            Command::OrgTurns { state: TurnsReport::Idle },
        ]
    );
    expect_silence(&mut watcher).await;
    expect_silence(&mut relay).await;
}

#[tokio::test]
async fn org_turns_still_needs_the_control_scope_and_reports_bad_args_privately() {
    let (h, url) = rig().await;
    let mut watcher = connect(&url, READ, "health").await;
    let mut other = connect(&url, READ, "other").await;
    watcher.send(Message::text(IDLE)).await.unwrap();
    let r = next_json(&mut watcher).await;
    assert_eq!(r["error"], "forbidden");
    assert!(h.calls.lock().unwrap().is_empty());
    let mut relay = connect(&url, CONTROL, "relay").await;
    relay.send(Message::text(r#"{"v":1,"type":"cmd","cmd_id":"org-turns","cmd":"org.turns","args":{"state":"busy"}}"#)).await.unwrap();
    let bad = next_json(&mut relay).await;
    assert_eq!((bad["ok"].as_bool(), bad["error"].as_str(), bad["private"].as_bool()), (Some(false), Some("bad_args"), Some(true)));
    expect_silence(&mut other).await;
}
