//! Plan §3.2: YouTube request shapes, chat-page mapping, the quota-aware poll interval,
//! and the poller against a scripted fake HTTP layer (paused tokio time, no network).
//! Field names follow the YouTube Data API v3 reference [verify at the live trial].

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::Instant;

use sm_connectors::model::InEvent;
use sm_connectors::youtube::{
    self, error_reason, parse_chat_page, poll_interval_s, run_youtube, MembershipPrice, QuotaBudget, YtConfig, YtHttp, YtRequest,
};
use sm_core::donation::Source;
use sm_core::protocol::Platform;

#[test]
fn request_shapes_and_query_encoding() {
    let r = youtube::active_broadcasts();
    assert_eq!((r.method, r.path.as_str()), ("GET", "/liveBroadcasts?part=snippet%2Cstatus&broadcastStatus=active"));
    let r = youtube::chat_list("Cg0+K/x=", Some("tok 1"));
    assert_eq!(r.path, "/liveChat/messages?liveChatId=Cg0%2BK%2Fx%3D&part=snippet%2CauthorDetails&maxResults=2000&pageToken=tok%201");
    assert_eq!(youtube::chat_list("L1", None).path, "/liveChat/messages?liveChatId=L1&part=snippet%2CauthorDetails&maxResults=2000");
    let r = youtube::chat_insert("L1", "hello").unwrap();
    assert_eq!((r.method, r.path.as_str()), ("POST", "/liveChat/messages?part=snippet"));
    assert_eq!(
        r.body.unwrap(),
        json!({"snippet":{"liveChatId":"L1","type":"textMessageEvent","textMessageDetails":{"messageText":"hello"}}})
    );
    assert!(youtube::chat_insert("L1", "").is_err());
    assert!(youtube::chat_insert("L1", &"x".repeat(201)).is_err());
    let r = youtube::chat_delete("abc").unwrap();
    assert_eq!((r.method, r.path.as_str()), ("DELETE", "/liveChat/messages?id=abc"));
    assert!(youtube::chat_delete("").is_err());
    let r = youtube::ban("L1", "UCx", Some(300)).unwrap();
    assert_eq!((r.method, r.path.as_str()), ("POST", "/liveChat/bans?part=snippet"));
    assert_eq!(
        r.body.unwrap(),
        json!({"snippet":{"liveChatId":"L1","type":"temporary","banDurationSeconds":300,"bannedUserDetails":{"channelId":"UCx"}}})
    );
    assert_eq!(
        youtube::ban("L1", "UCx", None).unwrap().body.unwrap(),
        json!({"snippet":{"liveChatId":"L1","type":"permanent","bannedUserDetails":{"channelId":"UCx"}}})
    );
    assert!(youtube::ban("L1", "UCx", Some(0)).is_err());
    assert_eq!(youtube::unban("ban-1").unwrap().path, "/liveChat/bans?id=ban-1");
    let r = youtube::transition_complete("B1");
    assert_eq!((r.method, r.path.as_str()), ("POST", "/liveBroadcasts/transition?broadcastStatus=complete&id=B1&part=status"));
}

fn item(id: &str, ty: &str, details: serde_json::Value, sponsor: bool) -> serde_json::Value {
    let mut snippet = json!({"type": ty, "liveChatId": "L1", "authorChannelId": "UC1", "publishedAt": "2026-11-03T00:00:00Z"});
    if let serde_json::Value::Object(d) = details {
        for (k, v) in d {
            snippet[k] = v;
        }
    }
    json!({"id": id, "snippet": snippet,
           "authorDetails": {"channelId": "UC1", "displayName": "Viewer One", "isChatOwner": false,
                             "isChatSponsor": sponsor, "isChatModerator": true, "isVerified": false}})
}

#[test]
fn chat_page_maps_every_type_we_use() {
    let price = MembershipPrice { amount_micros: 4_990_000, currency: "USD".into() };
    let body = json!({
        "nextPageToken": "P2", "pollingIntervalMillis": 5000,
        "items": [
            item("m1", "textMessageEvent", json!({"textMessageDetails": {"messageText": "hi"}}), true),
            item("m2", "superChatEvent", json!({"superChatDetails": {"amountMicros": "5000000", "currency": "EUR", "userComment": "great"}}), false),
            item("m3", "superStickerEvent", json!({"superStickerDetails": {"amountMicros": "2000000", "currency": "USD"}}), false),
            item("m4", "newSponsorEvent", json!({"newSponsorDetails": {"memberLevelName": "Fan"}}), true),
            item("m5", "membershipGiftingEventDetails_placeholder", json!({}), false),
            item("m6", "membershipGiftingEvent", json!({"membershipGiftingDetails": {"giftMembershipsCount": 3}}), false),
            item("m7", "memberMilestoneChatEvent", json!({"memberMilestoneChatDetails": {"userComment": "1 year!", "memberMonth": 12}}), true),
            item("m8", "giftMembershipReceivedEvent", json!({}), false),
            item("m9", "chatEndedEvent", json!({}), false)
        ]
    });
    let page = parse_chat_page(&body.to_string(), &price).unwrap();
    assert_eq!(page.next_page_token.as_deref(), Some("P2"));
    assert_eq!(page.polling_interval_ms, 5000);
    assert!(!page.offline);
    let ev: Vec<(&str, &Option<InEvent>)> = page.items.iter().map(|(id, e)| (id.as_str(), e)).collect();
    assert_eq!(ev.len(), 9);
    match ev[0].1 {
        Some(InEvent::Chat { platform, viewer_id, display, text, msg_id, roles, paid_sub, .. }) => {
            assert_eq!(*platform, Platform::Youtube);
            assert_eq!(viewer_id, "youtube:UC1");
            assert_eq!(display, "Viewer One");
            assert_eq!(text, "hi");
            assert_eq!(msg_id, "yt:m1");
            assert!(roles.contains(&"moderator".to_string()) && roles.contains(&"member".to_string()));
            assert!(paid_sub);
        }
        other => panic!("{other:?}"),
    }
    let donation = |i: usize| match ev[i].1 {
        Some(InEvent::Donation { source, text, viewer_id, external_id, .. }) => (source.clone(), text.clone(), viewer_id.clone(), external_id.clone()),
        other => panic!("item {i}: {other:?}"),
    };
    let (s, t, v, x) = donation(1);
    assert_eq!(s, Source::YouTubeSuperChat { amount_micros: 5_000_000, currency: "EUR".into() });
    assert_eq!((t.as_deref(), v.as_deref(), x.as_str()), (Some("great"), Some("youtube:UC1"), "yt-msg:m2"));
    assert_eq!(donation(2).0, Source::YouTubeSuperChat { amount_micros: 2_000_000, currency: "USD".into() });
    assert_eq!(donation(3).0, Source::YouTubeMembership { amount_micros: 4_990_000, currency: "USD".into() });
    assert!(ev[4].1.is_none());
    assert_eq!(donation(5).0, Source::YouTubeMembership { amount_micros: 3 * 4_990_000, currency: "USD".into() });
    let (s, t, ..) = donation(6);
    assert_eq!((s, t.as_deref()), (Source::YouTubeMembership { amount_micros: 4_990_000, currency: "USD".into() }, Some("1 year!")));
    assert!(ev[7].1.is_none(), "the gift's recipient is not credited; the gifter was");
    assert_eq!(*ev[8].1, Some(InEvent::StreamState { platform: Platform::Youtube, live: false }));

    let off = parse_chat_page(&json!({"items": [], "offlineAt": "2026-11-03T01:00:00Z", "pollingIntervalMillis": 1000}).to_string(), &price).unwrap();
    assert!(off.offline);
    assert!(parse_chat_page("nope", &price).is_err());
}

#[test]
fn poll_interval_spreads_the_quota_over_the_stream() {
    let b = QuotaBudget::default(); // 10,000 units, 2,000 kept for actions, 5 per list, 8 h
    assert!((poll_interval_s(&b, 5000) - 18.0).abs() < 1e-9);
    assert!((poll_interval_s(&b, 30_000) - 30.0).abs() < 1e-9, "never faster than the server asks");
    let short = QuotaBudget { budget_hours: 1.0, ..b.clone() };
    assert!((poll_interval_s(&short, 1000) - 2.25).abs() < 1e-9);
    let broken = QuotaBudget { units_per_day: 100, reserved_for_actions: 200, ..b };
    assert!(poll_interval_s(&broken, 0) >= 28_800.0, "no list budget left: poll once per budget window");
}

#[test]
fn error_reasons_are_read_from_the_body() {
    let b = json!({"error":{"code":403,"errors":[{"reason":"quotaExceeded","message":"x"}]}}).to_string();
    assert_eq!(error_reason(&b).as_deref(), Some("quotaExceeded"));
    assert_eq!(error_reason("{}"), None);
    assert_eq!(error_reason("not json"), None);
}

/// Scripted fake of the HTTP layer. Records each request's path and the (paused) time.
struct FakeHttp {
    script: Mutex<VecDeque<(u16, String)>>,
    calls: Mutex<Vec<(String, Instant)>>,
}

impl FakeHttp {
    fn new(script: Vec<(u16, serde_json::Value)>) -> Arc<Self> {
        Arc::new(FakeHttp {
            script: Mutex::new(script.into_iter().map(|(s, b)| (s, b.to_string())).collect()),
            calls: Mutex::new(Vec::new()),
        })
    }
    fn paths(&self) -> Vec<String> {
        self.calls.lock().unwrap().iter().map(|c| c.0.clone()).collect()
    }
}

#[async_trait::async_trait]
impl YtHttp for FakeHttp {
    async fn call(&self, req: &YtRequest) -> Result<(u16, String), String> {
        self.calls.lock().unwrap().push((req.path.clone(), Instant::now()));
        Ok(self.script.lock().unwrap().pop_front().unwrap_or((503, "{}".into())))
    }
}

fn page(token: &str, ids: &[&str]) -> serde_json::Value {
    json!({"nextPageToken": token, "pollingIntervalMillis": 5000,
           "items": ids.iter().map(|i| item(i, "textMessageEvent", json!({"textMessageDetails":{"messageText": i}}), false)).collect::<Vec<_>>()})
}

async fn collect(rx: &mut mpsc::Receiver<InEvent>, n: usize) -> Vec<InEvent> {
    let mut v = Vec::new();
    while v.len() < n {
        let e = tokio::time::timeout(Duration::from_secs(100_000), rx.recv()).await.expect("timed out").unwrap();
        if !matches!(e, InEvent::Health { .. }) {
            v.push(e);
        }
    }
    v
}

fn texts(v: &[InEvent]) -> Vec<String> {
    v.iter()
        .map(|e| match e {
            InEvent::Chat { text, .. } => text.clone(),
            InEvent::BroadcastFound { broadcast_id, live_chat_id, .. } => format!("found {broadcast_id}/{live_chat_id}"),
            InEvent::StreamState { live, .. } => format!("live={live}"),
            other => format!("{other:?}"),
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn poller_finds_the_chat_skips_backlog_dedupes_and_paces_itself() {
    let http = FakeHttp::new(vec![
        (200, json!({"items": []})),
        (200, json!({"items": [{"id": "B1", "snippet": {"liveChatId": "L1"}}]})),
        (200, page("P1", &["old-1", "old-2"])),
        (200, page("P2", &["m3", "m4"])),
        (200, page("P3", &["m4", "m5"])),
        (403, json!({"error":{"errors":[{"reason":"liveChatEnded"}]}})),
    ]);
    let (tx, mut rx) = mpsc::channel(64);
    let task = tokio::spawn(run_youtube(YtConfig::default(), http.clone(), tx));
    let got = collect(&mut rx, 5).await;
    assert_eq!(texts(&got), vec!["found B1/L1", "m3", "m4", "m5", "live=false"]);
    let paths = http.paths();
    assert!(paths[0].starts_with("/liveBroadcasts?") && paths[1].starts_with("/liveBroadcasts?"));
    assert!(paths[2].ends_with("maxResults=2000"), "first page has no token: {}", paths[2]);
    assert!(paths[3].ends_with("pageToken=P1") && paths[4].ends_with("pageToken=P2") && paths[5].ends_with("pageToken=P3"));
    let calls = http.calls.lock().unwrap();
    assert!(calls[1].1 - calls[0].1 >= Duration::from_secs(30), "no broadcast: retry after 30 s");
    for i in 3..6 {
        assert!(calls[i].1 - calls[i - 1].1 >= Duration::from_secs(18), "polls are paced by the quota budget");
    }
    drop(calls);
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn quota_exhaustion_backs_off_for_an_hour() {
    let http = FakeHttp::new(vec![
        (200, json!({"items": [{"id": "B1", "snippet": {"liveChatId": "L1"}}]})),
        (403, json!({"error":{"errors":[{"reason":"quotaExceeded"}]}})),
        (200, page("P1", &[])),
    ]);
    let (tx, mut rx) = mpsc::channel(64);
    let task = tokio::spawn(run_youtube(YtConfig::default(), http.clone(), tx));
    let mut saw_quota = false;
    while !saw_quota {
        match tokio::time::timeout(Duration::from_secs(100_000), rx.recv()).await.unwrap().unwrap() {
            InEvent::Health { up: false, detail, .. } if detail.contains("quota") => saw_quota = true,
            _ => {}
        }
    }
    // Let it make its next call.
    while http.calls.lock().unwrap().len() < 3 {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
    let calls = http.calls.lock().unwrap();
    assert!(calls[2].1 - calls[1].1 >= Duration::from_secs(3600));
    assert!(calls[2].0.contains("liveChatId=L1"), "keeps the same chat after a quota wait");
    drop(calls);
    task.abort();
}
