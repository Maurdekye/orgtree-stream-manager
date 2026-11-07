//! EventSub session loop (plan §3.1) and the obs-websocket v5 client (§3.4), each against
//! a fake server on 127.0.0.1. No outside services.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

use sm_connectors::model::InEvent;
use sm_connectors::obs::{obs_auth, ObsClient, ObsError};
use sm_connectors::twitch::eventsub::{run_eventsub, EventSubConfig, EventSubSubscriber, SubSpec};

type ServerWs = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

/// Accepts connections and hands each to the test.
async fn fake_server() -> (String, mpsc::Receiver<ServerWs>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", l.local_addr().unwrap());
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            if let Ok(ws) = tokio_tungstenite::accept_async(s).await {
                if tx.send(ws).await.is_err() {
                    break;
                }
            }
        }
    });
    (url, rx)
}

async fn next_conn(rx: &mut mpsc::Receiver<ServerWs>) -> ServerWs {
    tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("no connection").unwrap()
}

fn welcome(id: &str, keepalive: u64) -> Message {
    Message::text(
        json!({"metadata":{"message_id":format!("w-{id}"),"message_type":"session_welcome"},
               "payload":{"session":{"id":id,"status":"connected","keepalive_timeout_seconds":keepalive}}})
        .to_string(),
    )
}

fn chat(message_id: &str, text: &str) -> Message {
    Message::text(
        json!({"metadata":{"message_id":message_id,"message_type":"notification","subscription_type":"channel.chat.message"},
               "payload":{"subscription":{"type":"channel.chat.message"},
                          "event":{"chatter_user_id":"42","chatter_user_login":"v","chatter_user_name":"V",
                                   "message_id":format!("c-{message_id}"),"message":{"text":text},"badges":[]}}})
        .to_string(),
    )
}

#[derive(Default)]
struct FakeSubscriber {
    sessions: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl EventSubSubscriber for FakeSubscriber {
    async fn subscribe(&self, session_id: &str, _specs: &[SubSpec]) -> Result<(), String> {
        self.sessions.lock().unwrap().push(session_id.to_string());
        Ok(())
    }
}

async fn next_event(rx: &mut mpsc::Receiver<InEvent>) -> InEvent {
    loop {
        let e = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("no event").unwrap();
        if !matches!(e, InEvent::Health { .. }) {
            return e;
        }
    }
}

fn chat_text(e: &InEvent) -> &str {
    match e {
        InEvent::Chat { text, .. } => text,
        other => panic!("{other:?}"),
    }
}

async fn wait_for<F: Fn() -> bool>(f: F) {
    for _ in 0..100 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("condition not reached");
}

#[tokio::test]
async fn subscribes_on_welcome_forwards_notifications_and_drops_duplicates() {
    let (url, mut conns) = fake_server().await;
    let sub = Arc::new(FakeSubscriber::default());
    let (tx, mut rx) = mpsc::channel(64);
    let cfg = EventSubConfig { url, subscriptions: vec![], keepalive_grace_s: 2.0 };
    let task = tokio::spawn(run_eventsub(cfg, sub.clone(), tx));
    let mut c = next_conn(&mut conns).await;
    c.send(welcome("S1", 10)).await.unwrap();
    wait_for(|| sub.sessions.lock().unwrap().len() == 1).await;
    assert_eq!(sub.sessions.lock().unwrap()[0], "S1");
    c.send(chat("n1", "first")).await.unwrap();
    c.send(chat("n1", "first again (redelivered)")).await.unwrap();
    c.send(chat("n2", "second")).await.unwrap();
    assert_eq!(chat_text(&next_event(&mut rx).await), "first");
    assert_eq!(chat_text(&next_event(&mut rx).await), "second");
    task.abort();
}

#[tokio::test]
async fn keepalive_silence_reconnects_and_resubscribes() {
    let (url, mut conns) = fake_server().await;
    let sub = Arc::new(FakeSubscriber::default());
    let (tx, mut rx) = mpsc::channel(64);
    let cfg = EventSubConfig { url, subscriptions: vec![], keepalive_grace_s: 0.5 };
    let task = tokio::spawn(run_eventsub(cfg, sub.clone(), tx));
    let mut c1 = next_conn(&mut conns).await;
    c1.send(welcome("S1", 1)).await.unwrap();
    // Say nothing more: after 1 s + 0.5 s grace the runner must dial again.
    let mut c2 = next_conn(&mut conns).await;
    c2.send(welcome("S2", 10)).await.unwrap();
    wait_for(|| sub.sessions.lock().unwrap().len() == 2).await;
    assert_eq!(*sub.sessions.lock().unwrap(), vec!["S1".to_string(), "S2".to_string()]);
    c2.send(chat("n9", "after reconnect")).await.unwrap();
    assert_eq!(chat_text(&next_event(&mut rx).await), "after reconnect");
    drop(c1);
    task.abort();
}

#[tokio::test]
async fn session_reconnect_moves_without_resubscribing_and_keeps_old_until_new_welcome() {
    let (url, mut conns) = fake_server().await;
    let (url_b, mut conns_b) = fake_server().await;
    let sub = Arc::new(FakeSubscriber::default());
    let (tx, mut rx) = mpsc::channel(64);
    let cfg = EventSubConfig { url, subscriptions: vec![], keepalive_grace_s: 2.0 };
    let task = tokio::spawn(run_eventsub(cfg, sub.clone(), tx));
    let mut old = next_conn(&mut conns).await;
    old.send(welcome("S1", 10)).await.unwrap();
    wait_for(|| sub.sessions.lock().unwrap().len() == 1).await;
    let reconnect = json!({"metadata":{"message_id":"r1","message_type":"session_reconnect"},
                           "payload":{"session":{"id":"S1","status":"reconnecting","reconnect_url":url_b}}});
    old.send(Message::text(reconnect.to_string())).await.unwrap();
    let mut new = next_conn(&mut conns_b).await;
    // The old socket still delivers until the new one says welcome.
    old.send(chat("o1", "from old")).await.unwrap();
    assert_eq!(chat_text(&next_event(&mut rx).await), "from old");
    new.send(welcome("S1", 10)).await.unwrap();
    // The runner closes the old socket after the new welcome.
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match old.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                _ => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "old socket should be closed");
    new.send(chat("n1", "from new")).await.unwrap();
    assert_eq!(chat_text(&next_event(&mut rx).await), "from new");
    assert_eq!(sub.sessions.lock().unwrap().len(), 1, "a reconnect keeps its subscriptions");
    task.abort();
}

// obs-websocket v5 docs example: password "supersecretpassword".
const SALT: &str = "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=";
const CHALLENGE: &str = "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=";
const EXPECTED_AUTH: &str = "1Ct943GAT+6YQUUX47Ia/ncufilbe6+oD6lY+5kaCu4=";

#[test]
fn obs_auth_matches_the_documented_example() {
    assert_eq!(obs_auth("supersecretpassword", SALT, CHALLENGE), EXPECTED_AUTH);
}

async fn recv_json(ws: &mut ServerWs) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await.expect("timeout").unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(t.as_str()).unwrap(),
            _ => continue,
        }
    }
}

/// A fake OBS: hello with auth, checks Identify, answers requests.
async fn fake_obs(mut ws: ServerWs, seen: Arc<Mutex<Vec<Value>>>) {
    ws.send(Message::text(
        json!({"op":0,"d":{"obsWebSocketVersion":"5.0.0","rpcVersion":1,"authentication":{"challenge":CHALLENGE,"salt":SALT}}}).to_string(),
    ))
    .await
    .unwrap();
    let ident = recv_json(&mut ws).await;
    if ident["op"] != 1 || ident["d"]["authentication"] != EXPECTED_AUTH || ident["d"]["rpcVersion"] != 1 {
        let _ = ws.close(Some(CloseFrame { code: CloseCode::Library(4009), reason: "Authentication failed.".into() })).await;
        return;
    }
    ws.send(Message::text(json!({"op":2,"d":{"negotiatedRpcVersion":1}}).to_string())).await.unwrap();
    loop {
        let Ok(Some(Ok(Message::Text(t)))) = tokio::time::timeout(Duration::from_secs(10), ws.next()).await else { return };
        let req: Value = serde_json::from_str(t.as_str()).unwrap();
        seen.lock().unwrap().push(req.clone());
        let d = &req["d"];
        let ok = d["requestType"] != "Nope";
        let mut resp = json!({"op":7,"d":{"requestType":d["requestType"],"requestId":d["requestId"],
                     "requestStatus":{"result":ok,"code": if ok {100} else {204}}}});
        if !ok {
            resp["d"]["requestStatus"]["comment"] = json!("unknown request");
        }
        if d["requestType"] == "GetCurrentProgramScene" {
            resp["d"]["responseData"] = json!({"currentProgramSceneName":"Main"});
        }
        ws.send(Message::text(resp.to_string())).await.unwrap();
    }
}

#[tokio::test]
async fn obs_client_authenticates_and_correlates_requests() {
    let (url, mut conns) = fake_server().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s2 = seen.clone();
    tokio::spawn(async move {
        let ws = next_conn(&mut conns).await;
        fake_obs(ws, s2).await;
    });
    let obs = ObsClient::connect(&url, Some("supersecretpassword")).await.unwrap();
    let scene = obs.request("GetCurrentProgramScene", json!({})).await.unwrap();
    assert_eq!(scene["currentProgramSceneName"], "Main");
    obs.set_program_scene("AFK").await.unwrap();
    obs.set_transition("Fade", 120_000).await.unwrap();
    obs.restart_media("airhorn").await.unwrap();
    obs.stop_stream().await.unwrap();
    match obs.request("Nope", json!({})).await {
        Err(ObsError::Request { code, comment }) => assert_eq!((code, comment.as_str()), (204, "unknown request")),
        other => panic!("{other:?}"),
    }
    let seen = seen.lock().unwrap();
    let types: Vec<&str> = seen.iter().map(|r| r["d"]["requestType"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "GetCurrentProgramScene",
            "SetCurrentProgramScene",
            "SetCurrentSceneTransition",
            "SetCurrentSceneTransitionDuration",
            "TriggerMediaInputAction",
            "StopStream",
            "Nope"
        ]
    );
    assert_eq!(seen[1]["d"]["requestData"], json!({"sceneName":"AFK"}));
    assert_eq!(seen[3]["d"]["requestData"], json!({"transitionDuration":120000}));
    assert_eq!(seen[4]["d"]["requestData"]["inputName"], "airhorn");
    assert!(seen.iter().all(|r| r["op"] == 6));
    let ids: std::collections::HashSet<&str> = seen.iter().map(|r| r["d"]["requestId"].as_str().unwrap()).collect();
    assert_eq!(ids.len(), seen.len(), "request ids are unique");
}

#[tokio::test]
async fn obs_wrong_password_is_auth_failed_and_missing_password_is_refused() {
    let (url, mut conns) = fake_server().await;
    tokio::spawn(async move {
        for _ in 0..2 {
            let ws = next_conn(&mut conns).await;
            tokio::spawn(fake_obs(ws, Arc::new(Mutex::new(Vec::new()))));
        }
    });
    assert!(matches!(ObsClient::connect(&url, Some("wrong")).await, Err(ObsError::AuthFailed)));
    assert!(matches!(ObsClient::connect(&url, None).await, Err(ObsError::AuthRequired)));
}

#[tokio::test]
async fn obs_unreachable_is_a_connect_error() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    assert!(matches!(ObsClient::connect(&format!("ws://{addr}"), None).await, Err(ObsError::Connect(_))));
}
