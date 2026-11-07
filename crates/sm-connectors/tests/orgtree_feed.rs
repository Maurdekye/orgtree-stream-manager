//! The built-in Orgtree live feed (USER 2026-11-06 m77693a85fa7b "set up the stream manager to use
//! the new live feed"): an untokened connection, `agents.active` counts, reconnect + resync, and
//! a lost feed holds the current state (item stream-bridge-switch-from-the-liaison-s-relay-wa).

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

use sm_connectors::model::InEvent;
use sm_connectors::orgtree_feed::{parse_count, run_orgtree_feed, OrgtreeFeedConfig};
use sm_core::sleep::TurnsReport;

#[test]
fn counts_come_from_state_and_event_messages_only() {
    assert_eq!(parse_count(r#"{"v":1,"type":"state","name":"agents.active","payload":{"count":3,"scope":"all"},"reason":"connect","seq":2}"#), Some(3));
    assert_eq!(parse_count(r#"{"v":1,"type":"event","name":"agents.active","payload":{"count":0,"previous":1,"scope":"all"},"seq":9}"#), Some(0));
    assert_eq!(parse_count(r#"{"v":1,"type":"state","name":"agents.live","payload":{"count":5},"seq":3}"#), None);
    assert_eq!(parse_count(r#"{"v":1,"type":"hello","subject":"public","kinds":["agents.active"],"seq":1}"#), None);
    assert_eq!(parse_count(r#"{"v":1,"type":"heartbeat","seq":4}"#), None);
    assert_eq!(parse_count(r#"{"v":1,"type":"state","name":"agents.active","payload":{"count":-1}}"#), None);
    assert_eq!(parse_count("not json"), None);
}

async fn next_turns(rx: &mut mpsc::Receiver<InEvent>) -> TurnsReport {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("event in time").expect("open") {
            InEvent::OrgTurns(r) => return r,
            _ => continue,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connects_with_loopback_host_reports_counts_and_holds_then_resyncs() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let (host_tx, mut host_rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        for round in 0..2 {
            let (s, _) = l.accept().await.unwrap();
            let host_tx = host_tx.clone();
            let cb = move |req: &Request, resp: Response| {
                let _ = host_tx.send(req.headers().get("host").and_then(|h| h.to_str().ok()).unwrap_or("").to_string());
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(s, cb).await.unwrap();
            ws.send(Message::text(r#"{"v":1,"type":"hello","subject":"public","kinds":["agents.active"],"event_scope":"all","seq":1}"#)).await.unwrap();
            let first = if round == 0 { 2 } else { 0 };
            ws.send(Message::text(format!(r#"{{"v":1,"type":"state","name":"agents.active","payload":{{"count":{first},"scope":"all"}},"reason":"connect","seq":2}}"#))).await.unwrap();
            if round == 0 {
                ws.send(Message::text(r#"{"v":1,"type":"event","name":"agents.active","payload":{"count":0,"previous":2,"scope":"all"},"seq":3}"#)).await.unwrap();
                ws.send(Message::text(r#"{"v":1,"type":"event","name":"agents.active","payload":{"count":1,"previous":0,"scope":"all"},"seq":4}"#)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(ws); // the feed goes away
            } else {
                while ws.next().await.is_some() {}
            }
        }
    });
    let (tx, mut rx) = mpsc::channel(64);
    let cfg = OrgtreeFeedConfig {
        url: format!("ws://localhost:{port}/v1/events"),
        host: format!("127.0.0.1:{port}"),
        silence_s: 75.0,
        max_backoff_s: 1.0,
    };
    tokio::spawn(run_orgtree_feed(cfg, tx));
    assert_eq!(next_turns(&mut rx).await, TurnsReport::Active, "count 2 on connect");
    assert_eq!(next_turns(&mut rx).await, TurnsReport::Idle, "count 0");
    assert_eq!(next_turns(&mut rx).await, TurnsReport::Active, "count 1");
    assert_eq!(next_turns(&mut rx).await, TurnsReport::FeedLost, "the drop holds the state");
    assert_eq!(next_turns(&mut rx).await, TurnsReport::Idle, "reconnect resyncs from the connect state");
    assert_eq!(host_rx.recv().await.unwrap(), format!("127.0.0.1:{port}"), "the feed's Host check needs the loopback host");
    assert_eq!(host_rx.recv().await.unwrap(), format!("127.0.0.1:{port}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_feed_is_dropped_and_held() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(s).await.unwrap();
        ws.send(Message::text(r#"{"v":1,"type":"state","name":"agents.active","payload":{"count":1,"scope":"all"},"reason":"connect","seq":2}"#)).await.unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await; // then nothing: no heartbeat
        drop(ws);
    });
    let (tx, mut rx) = mpsc::channel(64);
    let cfg = OrgtreeFeedConfig { url: format!("ws://127.0.0.1:{port}/v1/events"), host: format!("127.0.0.1:{port}"), silence_s: 0.5, max_backoff_s: 1.0 };
    tokio::spawn(run_orgtree_feed(cfg, tx));
    assert_eq!(next_turns(&mut rx).await, TurnsReport::Active);
    assert_eq!(next_turns(&mut rx).await, TurnsReport::FeedLost, "silence past silence_s counts as lost");
}
