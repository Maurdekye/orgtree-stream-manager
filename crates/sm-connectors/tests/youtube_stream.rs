//! YouTube Live spec (stream-plan-opus youtube-spec.md) §3 chat in by server streaming,
//! §4 discovery cadence, §2 sign-in parking, against a scripted fake HTTP layer with paused
//! tokio time. No network, no real YouTube call.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use sm_connectors::model::InEvent;
use sm_connectors::youtube::{
    self, run_youtube_with, ChatMode, YtConfig, YtHttp, YtRequest, AUTH_EXPIRED_DETAIL, AUTH_EXPIRED_ERR,
};

enum Reply {
    /// 200 with these body chunks, then a clean end of the stream.
    Chunks(Vec<String>),
    /// The request itself fails (network error).
    Err(String),
    /// A non-200 answer with a JSON body.
    Http(u16, Value),
    /// 200 with these chunks, then the connection stays open and silent.
    ChunksThenSilent(Vec<String>),
}

struct FakeYt {
    discovery: Mutex<VecDeque<Value>>,
    default_discovery: Value,
    streams: Mutex<VecDeque<Reply>>,
    calls: Mutex<Vec<(String, Instant)>>,
    fail_all: Option<String>,
    /// Keeps silent streams open.
    open: Mutex<Vec<mpsc::Sender<String>>>,
}

fn found() -> Value {
    json!({"items": [{"id": "B1", "snippet": {"liveChatId": "L1", "channelId": "UCself"}}]})
}

impl FakeYt {
    fn new(discovery: Vec<Value>, default_discovery: Value, streams: Vec<Reply>) -> Arc<Self> {
        Arc::new(FakeYt {
            discovery: Mutex::new(discovery.into()),
            default_discovery,
            streams: Mutex::new(streams.into()),
            calls: Mutex::new(Vec::new()),
            fail_all: None,
            open: Mutex::new(Vec::new()),
        })
    }
    fn failing(err: &str) -> Arc<Self> {
        Arc::new(FakeYt {
            discovery: Mutex::new(VecDeque::new()),
            default_discovery: json!({"items": []}),
            streams: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
            fail_all: Some(err.to_string()),
            open: Mutex::new(Vec::new()),
        })
    }
    fn times(&self, prefix: &str) -> Vec<Instant> {
        self.calls.lock().unwrap().iter().filter(|c| c.0.starts_with(prefix)).map(|c| c.1).collect()
    }
    fn paths(&self) -> Vec<String> {
        self.calls.lock().unwrap().iter().map(|c| c.0.clone()).collect()
    }
}

#[async_trait::async_trait]
impl YtHttp for FakeYt {
    async fn call(&self, req: &YtRequest) -> Result<(u16, String), String> {
        self.calls.lock().unwrap().push((req.path.clone(), Instant::now()));
        if let Some(e) = &self.fail_all {
            return Err(e.clone());
        }
        if req.path.starts_with("/liveBroadcasts") {
            let v = self.discovery.lock().unwrap().pop_front().unwrap_or_else(|| self.default_discovery.clone());
            return Ok((200, v.to_string()));
        }
        Err("unexpected non-stream call".into())
    }

    async fn stream(&self, req: &YtRequest) -> Result<(u16, mpsc::Receiver<String>), String> {
        self.calls.lock().unwrap().push((req.path.clone(), Instant::now()));
        let reply = self.streams.lock().unwrap().pop_front().unwrap_or(Reply::Err("script ended".into()));
        match reply {
            Reply::Err(e) => Err(e),
            Reply::Chunks(chunks) => {
                let (tx, rx) = mpsc::channel(chunks.len().max(1));
                for c in chunks {
                    tx.try_send(c).unwrap();
                }
                Ok((200, rx))
            }
            Reply::ChunksThenSilent(chunks) => {
                let (tx, rx) = mpsc::channel(chunks.len().max(1));
                for c in chunks {
                    tx.try_send(c).unwrap();
                }
                self.open.lock().unwrap().push(tx);
                Ok((200, rx))
            }
            Reply::Http(status, body) => {
                let (tx, rx) = mpsc::channel(1);
                tx.try_send(body.to_string()).unwrap();
                Ok((status, rx))
            }
        }
    }
}

fn item(id: &str) -> Value {
    json!({"id": id,
           "snippet": {"type": "textMessageEvent", "liveChatId": "L1", "authorChannelId": "UC1", "textMessageDetails": {"messageText": id}},
           "authorDetails": {"channelId": "UC1", "displayName": "Viewer", "isChatOwner": false, "isChatSponsor": false, "isChatModerator": false, "isVerified": false}})
}

fn page(token: &str, ids: &[&str], polling_ms: u64) -> Value {
    json!({"nextPageToken": token, "pollingIntervalMillis": polling_ms, "items": ids.iter().map(|i| item(i)).collect::<Vec<_>>()})
}

fn stream_cfg() -> YtConfig {
    YtConfig { chat_mode: ChatMode::Stream, ..YtConfig::default() }
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

#[test]
fn stream_request_shape() {
    let r = youtube::chat_stream("Cg0+K/x=", Some("tok 1"));
    assert_eq!(r.method, "GET");
    assert_eq!(r.path, "/liveChat/messages/stream?liveChatId=Cg0%2BK%2Fx%3D&part=snippet%2CauthorDetails&maxResults=500&pageToken=tok%201");
    assert_eq!(youtube::chat_stream("L1", None).path, "/liveChat/messages/stream?liveChatId=L1&part=snippet%2CauthorDetails&maxResults=500");
}

// Red test 2.
#[tokio::test(start_paused = true)]
async fn stream_list_concatenated_and_ndjson() {
    let backlog = page("P1", &["old-1", "old-2"], 5000).to_string();
    let live = page("P2", &["a", "b"], 5000).to_string();
    let glued = format!("{backlog}{live}");
    // Two objects glued together, the second cut in the middle of a chunk boundary.
    let (c1, c2) = glued.split_at(backlog.len() + 25);
    let nd = format!("{}\n{}\n", page("P3", &["b", "c"], 5000), page("P4", &["d"], 5000));
    let http = FakeYt::new(
        vec![],
        found(),
        vec![Reply::Chunks(vec![c1.to_string(), c2.to_string()]), Reply::Chunks(vec![nd])],
    );
    let (tx, mut rx) = mpsc::channel(256);
    let task = tokio::spawn(run_youtube_with(stream_cfg(), http.clone(), tx, None));
    let got = collect(&mut rx, 5).await;
    assert_eq!(texts(&got), vec!["found B1/L1", "a", "b", "c", "d"], "backlog skipped on the fresh connect; nothing skipped on the token reconnect; the repeated b dropped");
    let paths = http.paths();
    assert!(paths[0].starts_with("/liveBroadcasts?"));
    assert!(paths[1].starts_with("/liveChat/messages/stream?liveChatId=L1") && !paths[1].contains("pageToken"), "{}", paths[1]);
    assert!(paths[2].ends_with("pageToken=P2"), "reconnects with the last token: {}", paths[2]);
    task.abort();
}

// Red test 3.
#[tokio::test(start_paused = true)]
async fn stream_reconnect_backoff() {
    let http = FakeYt::new(
        vec![],
        found(),
        vec![
            Reply::Chunks(vec![page("P1", &[], 1000).to_string()]), // 1: clean end
            Reply::Err("boom".into()),                              // 2
            Reply::Err("boom".into()),                              // 3
            Reply::Err("boom".into()),                              // 4: third failure within 5 minutes
            Reply::Http(403, json!({"error": {"errors": [{"reason": "quotaExceeded"}]}})), // 5
            Reply::Http(403, json!({"error": {"errors": [{"reason": "rateLimitExceeded"}]}})), // 6
            Reply::Chunks(vec![page("P2", &["z"], 1000).to_string()]), // 7
        ],
    );
    let (tx, mut rx) = mpsc::channel(256);
    let task = tokio::spawn(run_youtube_with(stream_cfg(), http.clone(), tx, None));
    while http.times("/liveChat").len() < 7 {
        tokio::time::sleep(Duration::from_secs(10)).await;
        while rx.try_recv().is_ok() {}
    }
    let t = http.times("/liveChat");
    let gap = |i: usize| t[i] - t[i - 1];
    assert!(gap(1) >= Duration::from_secs(2) && gap(1) < Duration::from_secs(5), "clean end waits max(2 s, server interval): {:?}", gap(1));
    assert!(gap(2) >= Duration::from_secs(2) && gap(2) < Duration::from_secs(5), "first failure: {:?}", gap(2));
    assert!(gap(3) >= Duration::from_secs(2) && gap(3) < Duration::from_secs(5), "second failure: {:?}", gap(3));
    assert!(gap(4) >= Duration::from_secs(60) && gap(4) < Duration::from_secs(3600), "third failure in 5 min: back off 60 s: {:?}", gap(4));
    assert!(gap(5) >= Duration::from_secs(3600), "quota: 3600 s: {:?}", gap(5));
    assert!(gap(6) >= Duration::from_secs(10) && gap(6) < Duration::from_secs(60), "sent too quickly: 10 s: {:?}", gap(6));
    task.abort();
}

// Red test 4.
#[tokio::test(start_paused = true)]
async fn discovery_cadence() {
    let none = json!({"items": []});
    let http = FakeYt::new(vec![], none.clone(), vec![]);
    let (tx, mut rx) = mpsc::channel(8192);
    let (wtx, wrx) = watch::channel(false);
    let start = Instant::now();
    let task = tokio::spawn(run_youtube_with(stream_cfg(), http.clone(), tx, Some(wrx)));
    tokio::time::sleep(Duration::from_secs(3600)).await;
    let rel: Vec<u64> = http.times("/liveBroadcasts").iter().map(|t| (*t - start).as_secs()).collect();
    for w in rel.windows(2) {
        if w[1] <= 900 {
            assert_eq!(w[1] - w[0], 30, "30 s for the first 15 minutes after the bridge starts: {rel:?}");
        } else if w[0] >= 900 {
            assert_eq!(w[1] - w[0], 600, "then every 10 minutes while nothing is live: {rel:?}");
        }
    }
    assert!(rel.len() >= 31 + 3, "{rel:?}");

    // Twitch goes live: another 15 minutes at 30 s, starting at once.
    let edge = Instant::now();
    let before = http.times("/liveBroadcasts").len();
    wtx.send(true).unwrap();
    tokio::time::sleep(Duration::from_secs(1900)).await;
    let after: Vec<u64> = http.times("/liveBroadcasts")[before..].iter().map(|t| (*t - edge).as_secs()).collect();
    assert!(after[0] <= 30, "a lookup right after Twitch goes live: {after:?}");
    for w in after.windows(2) {
        if w[1] <= 900 {
            assert_eq!(w[1] - w[0], 30, "{after:?}");
        } else if w[0] >= 900 {
            assert_eq!(w[1] - w[0], 600, "{after:?}");
        }
    }
    while rx.try_recv().is_ok() {}
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn no_lookups_once_a_broadcast_is_found() {
    let http = FakeYt::new(vec![json!({"items": []}), json!({"items": []}), found()], found(), vec![]);
    let (tx, mut rx) = mpsc::channel(8192);
    let task = tokio::spawn(run_youtube_with(stream_cfg(), http.clone(), tx, None));
    tokio::time::sleep(Duration::from_secs(7200)).await;
    assert_eq!(http.times("/liveBroadcasts").len(), 3, "found on the third look, then no more lookups until the chat ends");
    while rx.try_recv().is_ok() {}
    task.abort();
}

// Red test 9, connector half: the sign-in expiry becomes a health state, once a minute at most.
#[tokio::test(start_paused = true)]
async fn invalid_grant_parks_youtube_health() {
    let http = FakeYt::failing(AUTH_EXPIRED_ERR);
    let (tx, mut rx) = mpsc::channel(256);
    let task = tokio::spawn(run_youtube_with(stream_cfg(), http.clone(), tx, None));
    loop {
        match tokio::time::timeout(Duration::from_secs(100_000), rx.recv()).await.unwrap().unwrap() {
            InEvent::Health { component: "youtube_auth", up, detail } => {
                assert!(!up);
                assert_eq!(detail, "YouTube sign-in expired: run setup youtube again");
                assert_eq!(detail, AUTH_EXPIRED_DETAIL);
                break;
            }
            _ => {}
        }
    }
    tokio::time::sleep(Duration::from_secs(59)).await;
    assert_eq!(http.paths().len(), 1, "no call spam while the sign-in is expired");
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(http.paths().len(), 2, "one retry a minute (the token source answers without contacting Google while parked)");
    task.abort();
}

// Amendment 2: a stream with no data for 300 s is a clean end (a quiet chat looks the same):
// reconnect with the saved token, no failure counted, no health down.
#[tokio::test(start_paused = true)]
async fn an_idle_stream_reconnects_after_300_s_without_counting_a_failure() {
    let http = FakeYt::new(
        vec![],
        found(),
        vec![
            Reply::ChunksThenSilent(vec![page("P1", &[], 1000).to_string()]),
            Reply::ChunksThenSilent(vec![]),
            Reply::ChunksThenSilent(vec![]),
            Reply::ChunksThenSilent(vec![]),
            Reply::Chunks(vec![page("P2", &["z"], 1000).to_string()]),
        ],
    );
    let (tx, mut rx) = mpsc::channel(256);
    let task = tokio::spawn(run_youtube_with(stream_cfg(), http.clone(), tx, None));
    let mut downs = 0;
    while http.times("/liveChat").len() < 5 {
        tokio::time::sleep(Duration::from_secs(10)).await;
        while let Ok(e) = rx.try_recv() {
            if matches!(e, InEvent::Health { up: false, .. }) {
                downs += 1;
            }
        }
    }
    let t = http.times("/liveChat");
    for i in 1..5 {
        let g = t[i] - t[i - 1];
        assert!(g >= Duration::from_secs(300) && g < Duration::from_secs(310), "reconnect {i} after the idle timeout, no 60 s backoff: {g:?}");
    }
    assert_eq!(downs, 0, "an idle stream is not a health problem");
    assert!(http.paths().iter().filter(|p| p.contains("/stream")).skip(1).all(|p| p.ends_with("pageToken=P1")), "{:?}", http.paths());
    task.abort();
}
