//! YouTube Live spec (youtube-spec.md): red tests 1 (device-flow scope), 7 (ban id kept and
//! used), 9 (invalid_grant parks the sign-in), 10 (config keys) and Amendment 1 E5 (the
//! config-to-YtConfig mapping). A fake Google/YouTube on 127.0.0.1; no outside service.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;
use serde_json::{json, Value};

use sm_bin::config::load_config;
use sm_bin::exec::{Executor, YtCtx};
use sm_bin::http::HttpClient;
use sm_bin::setup as smsetup;
use sm_bin::tokens::{TokenConfig, TokenSource};
use sm_bridge::Action;
use sm_connectors::youtube::{ChatMode, AUTH_EXPIRED_ERR};
use sm_connectors::ytstate::{YtBans, YtQuota};
use sm_core::protocol::Platform;
use sm_store::Store;

fn now_s() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

// Red test 1.
#[test]
fn youtube_scope_is_device_allowed() {
    assert_eq!(smsetup::YOUTUBE_SCOPES, &["https://www.googleapis.com/auth/youtube"]);
}

#[derive(Default)]
struct Seen {
    token_calls: usize,
    token_bodies: Vec<String>,
    requests: Vec<(String, String, String)>, // method, path?query, body
    /// The first N token requests answer invalid_grant.
    reject_first: usize,
    /// DELETE of a ban answers 500.
    fail_delete: bool,
}

type S = Arc<Mutex<Seen>>;

async fn handle(State(s): State<S>, req: Request) -> impl IntoResponse {
    let method = req.method().to_string();
    let pq = req.uri().path_and_query().map(|p| p.to_string()).unwrap_or_default();
    let body = String::from_utf8(axum::body::to_bytes(req.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap();
    let mut g = s.lock().unwrap();
    if pq.starts_with("/token") {
        g.token_calls += 1;
        g.token_bodies.push(body);
        if g.token_calls <= g.reject_first {
            return (StatusCode::BAD_REQUEST, json!({"error": "invalid_grant", "error_description": "Token has been expired or revoked."}).to_string());
        }
        let n = g.token_calls;
        return (StatusCode::OK, json!({"access_token": format!("A{n}"), "expires_in": 3600, "token_type": "Bearer"}).to_string());
    }
    g.requests.push((method.clone(), pq.clone(), body.clone()));
    if g.fail_delete && method == "DELETE" {
        return (StatusCode::INTERNAL_SERVER_ERROR, "{}".to_string());
    }
    if method == "POST" && pq.starts_with("/yt/liveChat/bans") {
        let channel = serde_json::from_str::<Value>(&body).unwrap()["snippet"]["bannedUserDetails"]["channelId"].as_str().unwrap().to_string();
        return (StatusCode::OK, json!({"kind": "youtube#liveChatBan", "id": format!("BAN-{channel}")}).to_string());
    }
    (StatusCode::OK, "{}".to_string())
}

async fn fake_api(reject_first: usize) -> (String, S) {
    let s: S = Arc::new(Mutex::new(Seen { reject_first, ..Seen::default() }));
    let app = Router::new().fallback(handle).with_state(s.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (base, s)
}

fn token_cfg(base: &str) -> TokenConfig {
    TokenConfig { token_url: format!("{base}/token"), client_id: "cid".into(), client_secret: "csecret".into(), refresh_token: "rtok".into() }
}

// Red test 7, executor half: the ban id from the insert response is kept; unban deletes with it.
#[tokio::test]
async fn unban_uses_stored_ban_id() {
    let (base, seen) = fake_api(0).await;
    let http = Arc::new(HttpClient::new());
    let (chat, bans, quota) = (Arc::new(Mutex::new(Some(("B1".to_string(), "L1".to_string())))), Arc::new(YtBans::default()), Arc::new(YtQuota::new(10_000)));
    let exec = Executor {
        http: http.clone(),
        twitch: None,
        youtube: Some(YtCtx {
            api_base: format!("{base}/yt"),
            tokens: Arc::new(TokenSource::new(token_cfg(&base), http.clone())),
            chat,
            bans: bans.clone(),
            quota: Some(quota.clone()),
        }),
        obs: None,
        overlay: None,
    };
    exec.run(&Action::Ban { platform: Platform::Youtube, user: "UC1".into(), duration_s: Some(300), reason: "r".into() }).await.unwrap();
    exec.run(&Action::Ban { platform: Platform::Youtube, user: "UC2".into(), duration_s: None, reason: "r".into() }).await.unwrap();
    let t = now_s();
    let book = bans.export();
    let uc1 = book.iter().find(|b| b.0 == "UC1").unwrap();
    let uc2 = book.iter().find(|b| b.0 == "UC2").unwrap();
    assert_eq!(uc1.1, "BAN-UC1");
    assert!(uc1.2.unwrap() > t && uc1.2.unwrap() <= t + 301.0, "a timeout expires after its duration: {:?}", uc1.2);
    assert_eq!((uc2.1.as_str(), uc2.2), ("BAN-UC2", None), "a permanent ban never expires");

    exec.run(&Action::Unban { platform: Platform::Youtube, user: "UC1".into() }).await.unwrap();
    assert!(!bans.has("UC1", now_s()));
    let err = exec.run(&Action::Unban { platform: Platform::Youtube, user: "UC1".into() }).await.unwrap_err();
    assert!(err.contains("no ban"), "{err}");
    let g = seen.lock().unwrap();
    let last = g.requests.last().unwrap();
    assert_eq!((last.0.as_str(), last.1.as_str()), ("DELETE", "/yt/liveChat/bans?id=BAN-UC1"));
    assert_eq!(g.requests.len(), 3, "the second unban makes no call");
    // Quota is counted per request: 2 bans (200 each) + 1 unban (50).
    assert_eq!(quota.used(now_s()), 450);
}

// The ban book and the quota survive a restart through the store.
#[test]
fn the_store_keeps_bans_and_the_quota_day() {
    let s = Store::open_in_memory().unwrap();
    assert_eq!(s.yt_bans_load().unwrap(), vec![]);
    s.yt_bans_replace(&[("UC1".into(), "BAN1".into(), None), ("UC2".into(), "BAN2".into(), Some(500.0))]).unwrap();
    let mut rows = s.yt_bans_load().unwrap();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(rows, vec![("UC1".to_string(), "BAN1".to_string(), None), ("UC2".to_string(), "BAN2".to_string(), Some(500.0))]);
    s.yt_bans_replace(&[("UC1".into(), "BAN1".into(), None)]).unwrap();
    assert_eq!(s.yt_bans_load().unwrap().len(), 1, "replace drops the removed records");
    assert_eq!(s.yt_quota_load().unwrap(), None);
    s.yt_quota_save("2026-11-06", 1234).unwrap();
    s.yt_quota_save("2026-11-06", 2345).unwrap();
    assert_eq!(s.yt_quota_load().unwrap(), Some(("2026-11-06".to_string(), 2345)));
}

// Red test 9, token half.
#[tokio::test]
async fn invalid_grant_parks_youtube() {
    let (base, seen) = fake_api(1).await;
    let http = Arc::new(HttpClient::new());
    let path = std::env::temp_dir().join(format!("sm-yt-park-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let t = TokenSource::new(token_cfg(&base), http.clone())
        .with_store(path.clone())
        .with_park_on_invalid_grant()
        .with_park_recheck(Duration::ZERO);
    assert_eq!(t.bearer().await.unwrap_err(), AUTH_EXPIRED_ERR);
    assert_eq!(seen.lock().unwrap().token_calls, 1);
    for _ in 0..3 {
        assert_eq!(t.bearer().await.unwrap_err(), AUTH_EXPIRED_ERR);
    }
    assert_eq!(seen.lock().unwrap().token_calls, 1, "parked: no further refresh attempts while the token file is unchanged");
    // The operator signs in again: the token file changes, and the next call uses it.
    std::fs::write(&path, json!({"refresh_token": "newtok"}).to_string()).unwrap();
    assert_eq!(t.bearer().await.unwrap(), "A2");
    let g = seen.lock().unwrap();
    assert_eq!(g.token_calls, 2);
    assert!(g.token_bodies[1].contains("refresh_token=newtok"), "{}", g.token_bodies[1]);
    drop(g);
    let _ = std::fs::remove_file(&path);
}

// Red test 10, and Amendment 1 E5.
#[test]
fn config_youtube_keys() {
    let c = load_config(
        "[youtube]\nenabled = true\nchat_mode = \"poll\"\nbudget_hours = 12.0\nmembership_usd = 9.99\n\
         [youtube.costs]\ndiscovery = 1\nlist = 5\nstream = 5\ninsert = 50\nban_insert = 250\ndelete = 50\ntransition = 50\n",
    )
    .unwrap();
    assert!(c.youtube.enabled);
    assert_eq!(c.youtube.costs.ban_insert, 250);
    assert_eq!(c.youtube.costs.discovery, 1);
    let y = c.youtube.to_yt_config();
    assert_eq!(y.chat_mode, ChatMode::Poll);
    assert_eq!(y.budget.budget_hours, 12.0);
    assert_eq!(y.membership.amount_micros, 9_990_000);

    // Defaults: streamed chat, a 24 h poll budget (the poll fallback), the documented costs.
    let d = load_config("[youtube]\nenabled = true\n").unwrap();
    let y = d.youtube.to_yt_config();
    assert_eq!(y.chat_mode, ChatMode::Stream);
    assert_eq!(y.budget.budget_hours, 24.0);
    assert_eq!((d.youtube.costs.list, d.youtube.costs.stream, d.youtube.costs.insert, d.youtube.costs.ban_insert), (5, 5, 50, 200));

    // Unknown keys and bad values are still refused.
    assert!(load_config("[youtube]\nchat_modee = \"stream\"\n").unwrap_err().contains("chat_modee"));
    assert!(load_config("[youtube.costs]\nlistt = 5\n").unwrap_err().contains("listt"));
    assert!(load_config("[youtube]\nchat_mode = \"carrier-pigeon\"\n").is_err());
}

// Amendment 2: a failed unban of a timeout comes back with the same expiry.
#[tokio::test]
async fn a_failed_unban_keeps_the_record_and_its_expiry() {
    let (base, seen) = fake_api(0).await;
    let http = Arc::new(HttpClient::new());
    let bans = Arc::new(YtBans::default());
    let exec = Executor {
        http: http.clone(),
        twitch: None,
        youtube: Some(YtCtx {
            api_base: format!("{base}/yt"),
            tokens: Arc::new(TokenSource::new(token_cfg(&base), http.clone())),
            chat: Arc::new(Mutex::new(Some(("B1".to_string(), "L1".to_string())))),
            bans: bans.clone(),
            quota: None,
        }),
        obs: None,
        overlay: None,
    };
    exec.run(&Action::Ban { platform: Platform::Youtube, user: "UC1".into(), duration_s: Some(300), reason: "r".into() }).await.unwrap();
    exec.run(&Action::Ban { platform: Platform::Youtube, user: "UC2".into(), duration_s: None, reason: "r".into() }).await.unwrap();
    let before = bans.export();
    seen.lock().unwrap().fail_delete = true;
    assert!(exec.run(&Action::Unban { platform: Platform::Youtube, user: "UC1".into() }).await.is_err());
    assert!(exec.run(&Action::Unban { platform: Platform::Youtube, user: "UC2".into() }).await.is_err());
    assert_eq!(bans.export(), before, "same ban ids and the same expiries (a permanent ban stays permanent)");
}
