//! Phase 6c: refresh-token persistence, bridge-token generation, the RFC 8628 device-code
//! sign-in (Twitch and Google response shapes [verify]), the consent record, and the
//! overlay server. Fakes on 127.0.0.1 only.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;
use futures_util::StreamExt;
use serde_json::{json, Value};

use sm_bin::http::HttpClient;
use sm_bin::overlay::OverlayHub;
use sm_bin::setup::{gen_bridge_token, write_consent, DeviceFlow};
use sm_bin::tokens::{TokenConfig, TokenSource};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("sm-6c-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[derive(Default)]
struct Seen {
    bodies: Vec<(String, String)>,
    polls: usize,
}

type S = Arc<Mutex<Seen>>;

/// /token rotates refresh tokens; /device + /poll emulate a device-code flow that is
/// pending once, then asks to slow down, then succeeds.
async fn handle(State(s): State<S>, req: Request) -> impl IntoResponse {
    let path = req.uri().path().to_string();
    let body = String::from_utf8(axum::body::to_bytes(req.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap();
    let mut g = s.lock().unwrap();
    g.bodies.push((path.clone(), body.clone()));
    match path.as_str() {
        "/token" => {
            let n = g.bodies.iter().filter(|b| b.0 == "/token").count();
            (StatusCode::OK, json!({"access_token": format!("A{n}"), "expires_in": 3600, "refresh_token": format!("R{}", n + 1)}).to_string())
        }
        "/device" => (
            StatusCode::OK,
            json!({"device_code": "DEV", "user_code": "ABCD-EFGH", "verification_uri": "https://example.test/activate", "interval": 1, "expires_in": 600}).to_string(),
        ),
        "/google-device" => (
            StatusCode::OK,
            json!({"device_code": "GDEV", "user_code": "WXYZ", "verification_url": "https://example.test/device", "interval": 1, "expires_in": 600}).to_string(),
        ),
        "/poll" => {
            g.polls += 1;
            match g.polls {
                1 => (StatusCode::BAD_REQUEST, json!({"status": 400, "message": "authorization_pending"}).to_string()),
                2 => (StatusCode::BAD_REQUEST, json!({"error": "slow_down"}).to_string()),
                _ => (StatusCode::OK, json!({"access_token": "AT", "refresh_token": "RT-final", "expires_in": 14000}).to_string()),
            }
        }
        "/poll-denied" => (StatusCode::BAD_REQUEST, json!({"error": "access_denied"}).to_string()),
        _ => (StatusCode::NOT_FOUND, "{}".to_string()),
    }
}

async fn fake() -> (String, S) {
    let s: S = Arc::new(Mutex::new(Seen::default()));
    let app = Router::new().fallback(handle).with_state(s.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (base, s)
}

#[tokio::test]
async fn rotated_refresh_tokens_are_persisted_and_reused() {
    let (base, seen) = fake().await;
    let dir = tmp("persist");
    let store = dir.join("twitch-bot.json");
    let http = Arc::new(HttpClient::new());
    let cfg = || TokenConfig { token_url: format!("{base}/token"), client_id: "cid".into(), client_secret: "cs".into(), refresh_token: "R1".into() };
    let t = TokenSource::new(cfg(), http.clone()).with_store(store.clone());
    assert_eq!(t.bearer().await.unwrap(), "A1");
    let saved: Value = serde_json::from_slice(&std::fs::read(&store).unwrap()).unwrap();
    assert_eq!(saved["refresh_token"], "R2");
    // A restart starts from the saved token, not the original one from secrets.toml.
    let t2 = TokenSource::new(cfg(), http).with_store(store.clone());
    assert_eq!(t2.bearer().await.unwrap(), "A2");
    let bodies: Vec<String> = seen.lock().unwrap().bodies.iter().filter(|b| b.0 == "/token").map(|b| b.1.clone()).collect();
    assert!(bodies[0].contains("refresh_token=R1"));
    assert!(bodies[1].contains("refresh_token=R2"), "{}", bodies[1]);
    let names: Vec<String> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    assert_eq!(names, vec!["twitch-bot.json".to_string()], "no temp files: {names:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn bridge_tokens_are_long_random_and_url_safe() {
    let a = gen_bridge_token();
    let b = gen_bridge_token();
    assert_eq!(a.len(), 43, "256 bits in unpadded base64url");
    assert_ne!(a, b);
    assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'), "{a}");
}

#[tokio::test]
async fn device_flow_waits_slows_down_and_returns_the_refresh_token() {
    let (base, seen) = fake().await;
    let http = HttpClient::new();
    let flow = DeviceFlow {
        device_url: format!("{base}/device"),
        token_url: format!("{base}/poll"),
        client_id: "cid".into(),
        client_secret: Some("cs".into()),
        scopes: vec!["user:read:chat".into(), "user:write:chat".into()],
    };
    let start = flow.start(&http).await.unwrap();
    assert_eq!((start.user_code.as_str(), start.verification_uri.as_str()), ("ABCD-EFGH", "https://example.test/activate"));
    let t0 = std::time::Instant::now();
    let tokens = flow.poll(&http, &start).await.unwrap();
    assert_eq!(tokens.refresh_token, "RT-final");
    assert!(t0.elapsed() >= Duration::from_secs(1) + Duration::from_secs(6), "interval 1 s, then +5 s after slow_down");
    let g = seen.lock().unwrap();
    let dev = &g.bodies.iter().find(|b| b.0 == "/device").unwrap().1;
    assert!(dev.contains("client_id=cid") && dev.contains("scopes=user%3Aread%3Achat%20user%3Awrite%3Achat") && dev.contains("scope=user%3Aread%3Achat%20user%3Awrite%3Achat"), "{dev}");
    let poll = &g.bodies.iter().find(|b| b.0 == "/poll").unwrap().1;
    assert!(poll.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code") && poll.contains("device_code=DEV"), "{poll}");
}

#[tokio::test]
async fn device_flow_reads_google_shapes_and_stops_on_denial() {
    let (base, _) = fake().await;
    let http = HttpClient::new();
    let flow = DeviceFlow {
        device_url: format!("{base}/google-device"),
        token_url: format!("{base}/poll-denied"),
        client_id: "gid".into(),
        client_secret: Some("gs".into()),
        scopes: vec!["https://www.googleapis.com/auth/youtube.force-ssl".into()],
    };
    let start = flow.start(&http).await.unwrap();
    assert_eq!(start.verification_uri, "https://example.test/device");
    assert!(flow.poll(&http, &start).await.unwrap_err().contains("access_denied"));
}

#[test]
fn consent_is_recorded_once_with_what_was_agreed() {
    let dir = tmp("consent");
    let p = dir.join("consent.json");
    write_consent(&p, "UC123 / 100000001", 1_700_000_000.0).unwrap();
    let v: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
    assert_eq!(v["channels"], "UC123 / 100000001");
    assert_eq!(v["at"], "2023-11-14T22:13:20.000Z");
    assert!(v["agreed_to"].as_array().unwrap().iter().any(|x| x.as_str().unwrap().contains("moderation")));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn overlay_serves_the_page_health_and_pushes_effects() {
    let hub = OverlayHub::new();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let app = hub.router();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let http = HttpClient::new();
    let get = |p: &str| sm_bin::http::HttpReq { method: "GET", url: format!("http://{addr}{p}"), headers: vec![], body: None };
    let (st, page) = http.send(get("/overlay")).await.unwrap();
    assert_eq!(st, 200);
    assert!(page.contains("/overlay/ws") && page.contains("textContent") && !page.contains("innerHTML"));
    assert_eq!(http.send(get("/healthz")).await.unwrap(), (200, "ok".to_string()));
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/overlay/ws")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(hub.send("confetti", "confetti", Some("Kind Donor"), Some(5.0)), 1, "one overlay listening");
    let m = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
    let v: Value = serde_json::from_str(m.to_text().unwrap()).unwrap();
    assert_eq!(v, json!({"effect": "confetti", "animation": "confetti", "label": "Kind Donor", "usd": 5.0}));
}
