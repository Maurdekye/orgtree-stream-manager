//! Token refresh and the platform executors against a fake API on 127.0.0.1 (plain HTTP;
//! production uses the same client over HTTPS). No outside services.

use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;
use serde_json::{json, Value};

use sm_bin::exec::{Executor, TwitchCtx, YtCtx};
use sm_bin::http::HttpClient;
use sm_bin::tokens::{TokenConfig, TokenSource};
use sm_bridge::Action;
use sm_core::protocol::Platform;

#[derive(Default)]
struct Seen {
    token_calls: usize,
    token_bodies: Vec<String>,
    requests: Vec<(String, String, Option<String>, Option<String>, String)>, // method, path?query, auth, client-id, body
}

type S = Arc<Mutex<Seen>>;

async fn handle(State(s): State<S>, req: Request) -> impl IntoResponse {
    let method = req.method().to_string();
    let pq = req.uri().path_and_query().map(|p| p.to_string()).unwrap_or_default();
    let auth = req.headers().get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
    let cid = req.headers().get("client-id").and_then(|v| v.to_str().ok()).map(str::to_string);
    let body = String::from_utf8(axum::body::to_bytes(req.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap();
    let mut g = s.lock().unwrap();
    if pq.starts_with("/token") {
        g.token_calls += 1;
        g.token_bodies.push(body);
        let n = g.token_calls;
        return (StatusCode::OK, json!({"access_token": format!("A{n}"), "expires_in": 3600, "token_type": "bearer"}).to_string());
    }
    g.requests.push((method, pq.clone(), auth.clone(), cid, body));
    if auth.as_deref() == Some("Bearer A1") && pq.starts_with("/helix/moderation") {
        return (StatusCode::UNAUTHORIZED, "{}".to_string());
    }
    if pq.starts_with("/helix/chat/messages") {
        return (
            StatusCode::OK,
            json!({"data":[{"message_id":"","is_sent":false,"drop_reason":{"code":"msg_duplicate","message":"dup"}}]}).to_string(),
        );
    }
    (StatusCode::OK, "{}".to_string())
}

async fn fake_api() -> (String, S) {
    let s: S = Arc::new(Mutex::new(Seen::default()));
    let app = Router::new().fallback(handle).with_state(s.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (base, s)
}

fn tokens(base: &str, http: &Arc<HttpClient>) -> Arc<TokenSource> {
    Arc::new(TokenSource::new(
        TokenConfig {
            token_url: format!("{base}/token"),
            client_id: "cid".into(),
            client_secret: "csecret".into(),
            refresh_token: "rtok".into(),
        },
        http.clone(),
    ))
}

#[tokio::test]
async fn tokens_refresh_lazily_and_after_invalidation() {
    let (base, seen) = fake_api().await;
    let http = Arc::new(HttpClient::new());
    let t = tokens(&base, &http);
    assert_eq!(t.bearer().await.unwrap(), "A1");
    assert_eq!(t.bearer().await.unwrap(), "A1", "cached until it nears expiry");
    t.invalidate().await;
    assert_eq!(t.bearer().await.unwrap(), "A2");
    let g = seen.lock().unwrap();
    assert_eq!(g.token_calls, 2);
    let b = &g.token_bodies[0];
    for part in ["grant_type=refresh_token", "refresh_token=rtok", "client_id=cid", "client_secret=csecret"] {
        assert!(b.contains(part), "{b}");
    }
}

#[tokio::test]
async fn twitch_actions_send_helix_requests_and_retry_once_on_401() {
    let (base, seen) = fake_api().await;
    let http = Arc::new(HttpClient::new());
    let exec = Executor {
        http: http.clone(),
        twitch: Some(TwitchCtx {
            api_base: base.clone(),
            client_id: "cid".into(),
            broadcaster_id: "100".into(),
            bot_user_id: "200".into(),
            tokens: tokens(&base, &http),
        }),
        youtube: None,
        obs: None,
        overlay: None,
    };
    exec.run(&Action::Ban { platform: Platform::Twitch, user: "42".into(), duration_s: Some(600), reason: "spam".into() }).await.unwrap();
    {
        let g = seen.lock().unwrap();
        assert_eq!(g.token_calls, 2, "a 401 refreshes the token once");
        assert_eq!(g.requests.len(), 2);
        let (m, pq, auth, cid, body) = &g.requests[1];
        assert_eq!((m.as_str(), pq.as_str()), ("POST", "/helix/moderation/bans?broadcaster_id=100&moderator_id=200"));
        assert_eq!((auth.as_deref(), cid.as_deref()), (Some("Bearer A2"), Some("cid")));
        assert_eq!(serde_json::from_str::<Value>(body).unwrap(), json!({"data":{"user_id":"42","duration":600,"reason":"spam"}}));
    }
    let e = exec.run(&Action::Chat { platform: Platform::Twitch, text: "hi".into(), reply_to: None }).await.unwrap_err();
    assert!(e.contains("msg_duplicate"), "{e}");
    exec.run(&Action::SlowMode { seconds: 10 }).await.unwrap();
    assert!(seen.lock().unwrap().requests.last().unwrap().1.starts_with("/helix/chat/settings?"));
    // No OBS configured: OBS actions say so instead of pretending.
    assert!(exec.run(&Action::ObsStop).await.unwrap_err().contains("OBS"));
}

#[tokio::test]
async fn youtube_actions_use_the_live_chat_and_report_what_is_unsupported() {
    let (base, seen) = fake_api().await;
    let http = Arc::new(HttpClient::new());
    let chat = Arc::new(Mutex::new(None));
    let exec = Executor {
        http: http.clone(),
        twitch: None,
        youtube: Some(YtCtx {
            api_base: format!("{base}/yt"),
            tokens: tokens(&base, &http),
            chat: chat.clone(),
            bans: Arc::new(sm_connectors::ytstate::YtBans::default()),
            quota: None,
        }),
        obs: None,
        overlay: None,
    };
    let say = Action::Chat { platform: Platform::Youtube, text: "hello".into(), reply_to: None };
    assert!(exec.run(&say).await.unwrap_err().contains("live chat"));
    *chat.lock().unwrap() = Some(("B1".to_string(), "L1".to_string()));
    exec.run(&say).await.unwrap();
    exec.run(&Action::YoutubeEnd { broadcast_id: "B1".into() }).await.unwrap();
    exec.run(&Action::Ban { platform: Platform::Youtube, user: "UC1".into(), duration_s: None, reason: "r".into() }).await.unwrap();
    assert!(exec.run(&Action::Unban { platform: Platform::Youtube, user: "UC1".into() }).await.is_err());
    assert!(exec.run(&Action::Ban { platform: Platform::Twitch, user: "1".into(), duration_s: None, reason: "r".into() }).await.unwrap_err().contains("Twitch"));
    let g = seen.lock().unwrap();
    let paths: Vec<&str> = g.requests.iter().map(|r| r.1.as_str()).collect();
    assert_eq!(
        paths,
        vec!["/yt/liveChat/messages?part=snippet", "/yt/liveBroadcasts/transition?broadcastStatus=complete&id=B1&part=status", "/yt/liveChat/bans?part=snippet"]
    );
    assert_eq!(serde_json::from_str::<Value>(&g.requests[0].4).unwrap()["snippet"]["liveChatId"], "L1");
    assert!(g.requests.iter().all(|r| r.2.as_deref() == Some("Bearer A1")));
}
