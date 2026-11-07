//! Plan §4: the org WebSocket protocol over a real loopback socket, with a fake command
//! handler and a fake clock. No outside services.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sm_core::protocol::Command;
use sm_server::{CommandHandler, Hub, Scopes, ServerConfig, TokenTable};
use sm_store::Store;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Default)]
struct FakeHandler {
    calls: Mutex<Vec<(String, Command)>>,
}

impl CommandHandler for FakeHandler {
    fn handle(&self, actor: &str, cmd: &Command) -> Result<Value, (String, String)> {
        self.calls.lock().unwrap().push((actor.to_string(), cmd.clone()));
        match cmd {
            Command::ModBan { .. } => Err(("platform_refused".into(), "fake refusal".into())),
            _ => Ok(json!({"done": true})),
        }
    }
}

struct Rig {
    hub: Arc<Hub>,
    handler: Arc<FakeHandler>,
    clock: Arc<Mutex<f64>>,
    url: String,
}

const READ: &str = "read-token-0123456789abcdef0123456789";
const CONTROL: &str = "control-token-0123456789abcdef01234567";

async fn rig() -> Rig {
    let clock = Arc::new(Mutex::new(1_000_000.0));
    let c2 = clock.clone();
    let handler = Arc::new(FakeHandler::default());
    let tokens = TokenTable::new(vec![
        ("listener-read".into(), READ.into(), Scopes { read: true, control: false, moderate: false }),
        ("listener-control".into(), CONTROL.into(), Scopes { read: true, control: true, moderate: false }),
    ]);
    let cfg = ServerConfig { tokens, replay_max: 200, replay_window_s: 900.0, hello_timeout_s: 5.0 };
    let hub = Hub::new(Store::open_in_memory().unwrap(), cfg, handler.clone(), Arc::new(move || *c2.lock().unwrap()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = hub.router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Rig { hub, handler, clock, url: format!("ws://{addr}/v1/events") }
}

async fn connect(url: &str, token: Option<&str>) -> Result<Ws, tokio_tungstenite::tungstenite::Error> {
    let mut req = url.into_client_request().unwrap();
    if let Some(t) = token {
        req.headers_mut().insert("Authorization", format!("Bearer {t}").parse().unwrap());
    }
    tokio_tungstenite::connect_async(req).await.map(|(ws, _)| ws)
}

async fn hello(ws: &mut Ws, subscriber: &str, kinds: Option<&[&str]>) {
    let mut h = json!({"type":"subscribe","v":1,"subscriber":subscriber});
    if let Some(k) = kinds {
        h["kinds"] = json!(k);
    }
    ws.send(Message::text(h.to_string())).await.unwrap();
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

/// The server has handled the hello once it has registered the subscriber's cursor.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn refuses_missing_or_wrong_token_with_401() {
    let r = rig().await;
    for t in [None, Some("nope")] {
        match connect(&r.url, t).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => assert_eq!(resp.status(), 401),
            other => panic!("expected 401, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn live_events_in_order_with_envelope_and_kind_filter() {
    let r = rig().await;
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "listener-chat", Some(&["chat", "donation"])).await;
    settle().await;
    r.hub.publish("chat", json!({"text":"one"}));
    r.hub.publish("moderation", json!({"action":"timeout"}));
    r.hub.publish("donation", json!({"text":"two"}));
    let a = next_json(&mut ws).await;
    let b = next_json(&mut ws).await;
    assert_eq!(a["kind"], "chat");
    assert_eq!(a["text"], "one");
    assert_eq!(a["v"], 1);
    assert_eq!(a["trust"], "untrusted_viewer_input");
    assert!(a["ts"].as_str().unwrap().ends_with('Z'));
    assert_eq!(b["kind"], "donation");
    assert!(b["seq"].as_u64().unwrap() > a["seq"].as_u64().unwrap());
    assert!(a.get("replay").is_none());
    expect_silence(&mut ws).await; // moderation was filtered out
}

#[tokio::test]
async fn reconnect_replays_only_what_was_missed() {
    let r = rig().await;
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "listener-chat", None).await;
    settle().await;
    r.hub.publish("chat", json!({"text":"seen"}));
    assert_eq!(next_json(&mut ws).await["text"], "seen");
    ws.close(None).await.unwrap();
    drop(ws);
    settle().await;
    r.hub.publish("chat", json!({"text":"missed-1"}));
    r.hub.publish("chat", json!({"text":"missed-2"}));
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "listener-chat", None).await;
    let m1 = next_json(&mut ws).await;
    let m2 = next_json(&mut ws).await;
    assert_eq!((m1["text"].as_str(), m2["text"].as_str()), (Some("missed-1"), Some("missed-2")));
    assert_eq!(m1["replay"], true);
    expect_silence(&mut ws).await;
    // Live again after the replay.
    r.hub.publish("chat", json!({"text":"live"}));
    let l = next_json(&mut ws).await;
    assert_eq!(l["text"], "live");
    assert!(l.get("replay").is_none());
}

#[tokio::test]
async fn a_new_subscriber_gets_no_history() {
    let r = rig().await;
    r.hub.publish("chat", json!({"text":"before"}));
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "brand-new", None).await;
    expect_silence(&mut ws).await;
}

#[tokio::test]
async fn replay_is_bounded_and_reports_a_gap() {
    let r = rig().await;
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "listener-chat", None).await;
    settle().await;
    ws.close(None).await.unwrap();
    drop(ws);
    settle().await;
    // 10 events older than the 15-minute window, then 250 recent ones.
    for i in 0..10 {
        r.hub.publish("chat", json!({"i": i}));
    }
    *r.clock.lock().unwrap() += 1000.0;
    for i in 10..260 {
        r.hub.publish("chat", json!({"i": i}));
    }
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "listener-chat", None).await;
    let gap = next_json(&mut ws).await;
    assert_eq!(gap["kind"], "replay.gap");
    assert_eq!(gap["missed"], 60);
    let first = next_json(&mut ws).await;
    assert_eq!(first["i"], 60);
    let mut last = first;
    for _ in 0..199 {
        last = next_json(&mut ws).await;
    }
    assert_eq!(last["i"], 259);
    expect_silence(&mut ws).await;
}

#[tokio::test]
async fn commands_need_control_scope() {
    let r = rig().await;
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    hello(&mut ws, "reader", Some(&["command.result"])).await;
    settle().await;
    ws.send(Message::text(r#"{"v":1,"type":"cmd","cmd_id":"c1","cmd":"ping"}"#)).await.unwrap();
    let res = next_json(&mut ws).await;
    assert_eq!(res["kind"], "command.result");
    assert_eq!(res["cmd_id"], "c1");
    assert_eq!(res["ok"], false);
    assert_eq!(res["error"], "forbidden");
    assert!(r.handler.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn commands_dispatch_once_per_cmd_id_and_report_errors() {
    let r = rig().await;
    let mut ws = connect(&r.url, Some(CONTROL)).await.unwrap();
    hello(&mut ws, "listener-control", Some(&["command.result"])).await;
    settle().await;
    let cmd = r#"{"v":1,"type":"cmd","cmd_id":"t1","cmd":"mod.timeout","args":{"viewer":"twitch:42","duration_s":600,"reason":"spam"}}"#;
    ws.send(Message::text(cmd)).await.unwrap();
    let res = next_json(&mut ws).await;
    assert_eq!((res["cmd_id"].as_str(), res["ok"].as_bool()), (Some("t1"), Some(true)));
    assert_eq!(res["detail"], json!({"done": true}));
    ws.send(Message::text(cmd)).await.unwrap();
    let again = next_json(&mut ws).await;
    assert_eq!(again["ok"], true);
    assert_eq!(again["repeat"], true);
    {
        let calls = r.handler.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "a resent cmd_id must not run twice");
        assert_eq!(calls[0].0, "listener-control");
    }
    // Handler refusal.
    ws.send(Message::text(r#"{"v":1,"type":"cmd","cmd_id":"b1","cmd":"mod.ban","args":{"viewer":"twitch:42","reason":"abuse"}}"#)).await.unwrap();
    let ban = next_json(&mut ws).await;
    assert_eq!((ban["ok"].as_bool(), ban["error"].as_str()), (Some(false), Some("platform_refused")));
    // Validation failure keeps the cmd_id.
    ws.send(Message::text(r#"{"v":1,"type":"cmd","cmd_id":"x9","cmd":"mod.slow_mode","args":{"seconds":2}}"#)).await.unwrap();
    let bad = next_json(&mut ws).await;
    assert_eq!((bad["cmd_id"].as_str(), bad["error"].as_str()), (Some("x9"), Some("bad_args")));
    // Ping is answered by the server itself, with the head seq.
    ws.send(Message::text(r#"{"v":1,"type":"cmd","cmd_id":"p1","cmd":"ping"}"#)).await.unwrap();
    let pong = next_json(&mut ws).await;
    assert_eq!(pong["ok"], true);
    assert!(pong["detail"]["seq_head"].as_u64().is_some());
    assert_eq!(r.handler.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_connection_without_a_valid_hello_is_closed() {
    let r = rig().await;
    let mut ws = connect(&r.url, Some(READ)).await.unwrap();
    ws.send(Message::text(r#"{"type":"subscribe","v":1,"subscriber":"bad name!"}"#)).await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                _ => {}
            }
        }
    })
    .await;
    assert!(end.is_ok(), "server should close the connection");
}
