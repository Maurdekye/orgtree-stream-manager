//! The `moderate` token scope (moderation-bot pool): mod.ban and mod.timeout only. Every other
//! command gets a private refusal and an audit entry. `chat.mod` is opt-in: only a subscriber that
//! names it in its hello's kinds receives it, so unfiltered listeners are not flooded.

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
    calls: Mutex<Vec<Command>>,
}

impl CommandHandler for FakeHandler {
    fn handle(&self, _actor: &str, cmd: &Command) -> Result<Value, (String, String)> {
        self.calls.lock().unwrap().push(cmd.clone());
        Ok(json!({"done": true}))
    }
}

const READ: &str = "read-token-0123456789abcdef0123456789";
const MODBOT: &str = "modbot-token-0123456789abcdef01234567";

async fn rig() -> (Arc<Hub>, Arc<FakeHandler>, String) {
    let handler = Arc::new(FakeHandler::default());
    let tokens = TokenTable::new(vec![
        ("modread".into(), READ.into(), Scopes { read: true, control: false, moderate: false }),
        ("modbot".into(), MODBOT.into(), Scopes { read: true, control: false, moderate: true }),
    ]);
    let cfg = ServerConfig { tokens, replay_max: 200, replay_window_s: 900.0, hello_timeout_s: 5.0 };
    let hub = Hub::new(Store::open_in_memory().unwrap(), cfg, handler.clone(), Arc::new(|| 1_000_000.0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = hub.router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (hub, handler, format!("ws://{addr}/v1/events"))
}

async fn connect(url: &str, token: &str, subscriber: &str, kinds: Option<&[&str]>) -> Ws {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert("Authorization", format!("Bearer {token}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let mut h = json!({"type":"subscribe","v":1,"subscriber":subscriber});
    if let Some(k) = kinds {
        h["kinds"] = json!(k);
    }
    ws.send(Message::text(h.to_string())).await.unwrap();
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

fn cmd(id: &str, name: &str, args: Value) -> String {
    json!({"v":1,"type":"cmd","cmd_id":id,"cmd":name,"args":args}).to_string()
}

#[tokio::test]
async fn moderate_allows_only_ban_and_timeout() {
    let (hub, h, url) = rig().await;
    let mut bot = connect(&url, MODBOT, "modbot-1", Some(&["command.result"])).await;
    bot.send(Message::text(cmd("b1", "mod.ban", json!({"viewer":"twitch:9","reason":"spam bot"})))).await.unwrap();
    assert_eq!(next_json(&mut bot).await["ok"], true);
    bot.send(Message::text(cmd("t1", "mod.timeout", json!({"viewer":"twitch:9","duration_s":600,"reason":"repeated prompt injection"}))))
        .await
        .unwrap();
    assert_eq!(next_json(&mut bot).await["ok"], true);
    let refused = [
        cmd("c1", "chat.say", json!({"text":"hi"})),
        cmd("e1", "stream.end", json!({"confirm":"END","reason":"x"})),
        cmd("a1", "stream.afk", json!({"on":true,"reason":"x"})),
        cmd("o1", "org.turns", json!({"state":"idle"})),
        cmd("g1", "gate.set", json!({"min_interval_s":0,"reason":"x"})),
        cmd("u1", "mod.unban", json!({"viewer":"twitch:9"})),
        cmd("d1", "mod.delete", json!({"msg_id":"tw:1"})),
        cmd("p1", "ping", json!({})),
    ];
    // At most 5 refusal replies per connection per minute (review round 1): a fresh connection for the rest.
    let mut bot2 = connect(&url, MODBOT, "modbot-2", Some(&["command.result"])).await;
    for (i, r) in refused.iter().enumerate() {
        let bot = if i < 5 { &mut bot } else { &mut bot2 };
        bot.send(Message::text(r.clone())).await.unwrap();
        let res = next_json(bot).await;
        assert_eq!(
            (res["ok"].as_bool(), res["error"].as_str(), res["private"].as_bool()),
            (Some(false), Some("forbidden"), Some(true)),
            "{r}"
        );
    }
    assert_eq!(h.calls.lock().unwrap().len(), 2, "only ban and timeout reached the handler");
    let audit = hub.audit_tail(50);
    let refusals = audit.iter().filter(|a| a.actor == "token:modbot" && a.action.starts_with("refused:")).count();
    assert_eq!(refusals, refused.len());
}

#[tokio::test]
async fn read_only_still_cannot_moderate() {
    let (_hub, h, url) = rig().await;
    let mut r = connect(&url, READ, "modread", Some(&["command.result"])).await;
    r.send(Message::text(cmd("b1", "mod.ban", json!({"viewer":"twitch:9","reason":"spam"})))).await.unwrap();
    assert_eq!(next_json(&mut r).await["error"], "forbidden");
    assert!(h.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn chat_mod_reaches_only_subscribers_that_ask_for_it() {
    let (hub, _h, url) = rig().await;
    let mut unfiltered = connect(&url, READ, "health", None).await;
    let mut feed = connect(&url, READ, "stream-mod", Some(&["chat.mod"])).await;
    hub.publish("chat.mod", json!({"platform":"twitch","viewer":{"id":"twitch:9"},"text":"x","exempt":false,"status":"forwarded"}));
    let m = next_json(&mut feed).await;
    assert_eq!(m["kind"], "chat.mod");
    assert_eq!(m["trust"], "untrusted_viewer_input");
    expect_silence(&mut unfiltered).await;
}
