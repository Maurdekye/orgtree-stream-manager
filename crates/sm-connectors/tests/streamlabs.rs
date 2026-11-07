//! Plan §3.3: Streamlabs Socket API (socket.io v2 / Engine.IO 3) [doc], the relay rule
//! (no double counting with native connectors), donor linking, and the session loop
//! against a fake server on 127.0.0.1. Relay payload fields are [verify].

use std::collections::HashMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use sm_connectors::model::InEvent;
use sm_connectors::streamlabs::{link_donor, map_event, parse_packet, run_streamlabs, Packet, RecentChatter, StreamlabsConfig};
use sm_core::donation::Source;
use sm_core::protocol::Platform;

#[test]
fn engine_io_packets() {
    assert_eq!(
        parse_packet(r#"0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":5000}"#),
        Packet::Open { ping_interval_ms: 25000, ping_timeout_ms: 5000 }
    );
    assert_eq!(parse_packet("40"), Packet::Connected);
    assert_eq!(parse_packet("3"), Packet::Pong);
    assert_eq!(parse_packet("2"), Packet::Ping);
    assert_eq!(parse_packet("41"), Packet::Disconnect);
    assert_eq!(parse_packet("1"), Packet::Disconnect);
    match parse_packet(r#"42["event",{"type":"donation"}]"#) {
        Packet::Event { name, data } => {
            assert_eq!(name, "event");
            assert_eq!(data["type"], "donation");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(parse_packet("42[broken"), Packet::Other);
    assert_eq!(parse_packet(""), Packet::Other);
}

fn donation_event(event_id: &str, id: &str) -> serde_json::Value {
    json!({"type":"donation","event_id":event_id,
           "message":[{"id":id,"name":"Kind Donor","from":"Kind Donor","amount":"13.37","currency":"USD",
                       "formatted_amount":"$13.37","message":"keep it up","_id":"x"}]})
}

#[test]
fn own_donations_map_and_relays_follow_the_rule() {
    let evs = map_event(&donation_event("e1", "d1"), true, true);
    assert_eq!(evs.len(), 1);
    match &evs[0].1 {
        InEvent::Donation { platform, viewer_id, display, source, text, external_id } => {
            assert_eq!(*platform, None);
            assert_eq!(*viewer_id, None, "donor names are unverified; linking happens separately");
            assert_eq!(display.as_deref(), Some("Kind Donor"));
            assert_eq!(*source, Source::Streamlabs { amount: "13.37".into(), currency: "USD".into() });
            assert_eq!(text.as_deref(), Some("keep it up"));
            assert_eq!(external_id, "sl:d1");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(evs[0].0, "sl:d1");

    let bits = json!({"type":"bits","for":"twitch_account","event_id":"e2",
                      "message":[{"_id":"b1","name":"cheerer","amount":"500","message":"cheer500 hi"}]});
    assert!(map_event(&bits, true, true).is_empty(), "native Twitch on: relay ignored");
    let evs = map_event(&bits, false, true);
    match &evs[0].1 {
        InEvent::Donation { platform, source, .. } => {
            assert_eq!(*platform, Some(Platform::Twitch));
            assert_eq!(*source, Source::TwitchBits { bits: 500 });
        }
        other => panic!("{other:?}"),
    }
    let sub = json!({"type":"subscription","for":"twitch_account","message":[{"_id":"s1","name":"subber","sub_plan":"2000","months":3}]});
    match &map_event(&sub, false, true)[0].1 {
        InEvent::Donation { source, .. } => assert_eq!(*source, Source::TwitchSub { tier: 2, count: 1 }),
        other => panic!("{other:?}"),
    }
    let sc = json!({"type":"superchat","for":"youtube_account",
                    "message":[{"_id":"y1","name":"fan","amount":"5000000","currency":"EUR","comment":"hello"}]});
    assert!(map_event(&sc, true, true).is_empty());
    match &map_event(&sc, true, false)[0].1 {
        InEvent::Donation { platform, source, text, .. } => {
            assert_eq!(*platform, Some(Platform::Youtube));
            assert_eq!(*source, Source::YouTubeSuperChat { amount_micros: 5_000_000, currency: "EUR".into() });
            assert_eq!(text.as_deref(), Some("hello"));
        }
        other => panic!("{other:?}"),
    }
    for kind in ["follow", "host", "raid"] {
        let e = json!({"type":kind,"for":"twitch_account","message":[{"_id":"f","name":"n"}]});
        assert!(map_event(&e, false, false).is_empty(), "{kind} never counts");
    }
    // A donation without an id can't be de-duplicated, so it falls back to the event id.
    let no_id = json!({"type":"donation","event_id":"e9","message":[{"name":"n","amount":"1","currency":"USD"}]});
    assert_eq!(map_event(&no_id, true, true)[0].0, "sl:e9#0");
    assert!(map_event(&json!({"type":"donation"}), true, true).is_empty());
}

#[test]
fn donors_link_only_to_an_exact_recent_chatter() {
    let mut recent = HashMap::new();
    recent.insert("kind donor".to_string(), RecentChatter { viewer_id: "twitch:42".into(), last_seen: 1000.0 });
    recent.insert("old".to_string(), RecentChatter { viewer_id: "youtube:UC9".into(), last_seen: 0.0 });
    assert_eq!(link_donor("Kind Donor", &recent, 2000.0).as_deref(), Some("twitch:42"));
    assert_eq!(link_donor("  kind DONOR ", &recent, 2000.0).as_deref(), Some("twitch:42"));
    assert_eq!(link_donor("Kind Donor2", &recent, 2000.0), None);
    assert_eq!(link_donor("Kind", &recent, 2000.0), None);
    assert_eq!(link_donor("old", &recent, 86_400.0 + 1.0), None, "seen more than 24 h ago");
    assert_eq!(link_donor("", &recent, 0.0), None);
}

type ServerWs = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

async fn fake_server() -> (String, mpsc::Receiver<ServerWs>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/socket.io/?EIO=3&transport=websocket", l.local_addr().unwrap());
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

async fn next_donation(rx: &mut mpsc::Receiver<InEvent>) -> String {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("no event").unwrap() {
            InEvent::Donation { external_id, .. } => return external_id,
            _ => continue,
        }
    }
}

async fn open(ws: &mut ServerWs, ping_ms: u64, timeout_ms: u64) {
    let o = format!(r#"0{{"sid":"s","upgrades":[],"pingInterval":{ping_ms},"pingTimeout":{timeout_ms}}}"#);
    ws.send(Message::text(o)).await.unwrap();
    ws.send(Message::text("40")).await.unwrap();
}

#[tokio::test]
async fn session_forwards_events_dedupes_and_answers_with_pings() {
    let (url, mut conns) = fake_server().await;
    let (tx, mut rx) = mpsc::channel(64);
    let task = tokio::spawn(run_streamlabs(StreamlabsConfig { url, native_twitch: true, native_youtube: true }, tx));
    let mut ws = next_conn(&mut conns).await;
    open(&mut ws, 200, 1000).await;
    let ev = |e: serde_json::Value| Message::text(format!("42{}", json!(["event", e])));
    ws.send(ev(donation_event("e1", "d1"))).await.unwrap();
    ws.send(ev(donation_event("e1", "d1"))).await.unwrap(); // redelivered
    ws.send(ev(donation_event("e2", "d2"))).await.unwrap();
    assert_eq!(next_donation(&mut rx).await, "sl:d1");
    assert_eq!(next_donation(&mut rx).await, "sl:d2");
    // The client pings ("2") every pingInterval; answer one.
    let ping = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Text(t))) = ws.next().await {
                if t.as_str() == "2" {
                    return;
                }
            }
        }
    })
    .await;
    assert!(ping.is_ok(), "client must ping");
    ws.send(Message::text("3")).await.unwrap();
    task.abort();
}

#[tokio::test]
async fn missing_pongs_or_a_disconnect_packet_cause_a_redial() {
    let (url, mut conns) = fake_server().await;
    let (tx, mut rx) = mpsc::channel(64);
    let task = tokio::spawn(run_streamlabs(StreamlabsConfig { url, native_twitch: true, native_youtube: true }, tx));
    let mut c1 = next_conn(&mut conns).await;
    open(&mut c1, 100, 100).await; // never answer pings
    let mut c2 = next_conn(&mut conns).await;
    open(&mut c2, 10_000, 10_000).await;
    c2.send(Message::text("41")).await.unwrap(); // server-side namespace disconnect
    let mut c3 = next_conn(&mut conns).await;
    open(&mut c3, 10_000, 10_000).await;
    c3.send(Message::text(format!("42{}", json!(["event", donation_event("e5", "d5")])))).await.unwrap();
    assert_eq!(next_donation(&mut rx).await, "sl:d5");
    drop((c1, c2));
    task.abort();
}
