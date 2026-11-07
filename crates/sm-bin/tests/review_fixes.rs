//! Review round 1, program findings 12 and 13b.

use std::sync::{Arc, Mutex};

use axum::Router;
use serde_json::json;
use sm_bin::config::load_config;
use sm_bin::http::HttpClient;
use sm_bin::tokens::{TokenConfig, TokenSource};

// Finding 13b: listening addresses default to loopback.
#[test]
fn default_bind_addresses_are_loopback() {
    let c = load_config("").unwrap();
    assert_eq!(c.server.events_addr, "127.0.0.1:8787");
    assert_eq!(c.server.overlay_addr, "127.0.0.1:8788");
}

// Finding 12: every new token value reaches the secret sink (for the outbound filter),
// and the saved token file is owner-only on unix.
#[tokio::test]
async fn new_token_values_reach_the_secret_sink() {
    let app = Router::new().fallback(|| async { json!({"access_token":"ACCESS-1","expires_in":3600,"refresh_token":"REFRESH-2"}).to_string() });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let mut dir = std::env::temp_dir();
    dir.push(format!("sm-rf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let s2 = seen.clone();
    let t = TokenSource::new(
        TokenConfig { token_url: format!("{base}/token"), client_id: "c".into(), client_secret: "s".into(), refresh_token: "REFRESH-1".into() },
        Arc::new(HttpClient::new()),
    )
    .with_store(dir.join("t.json"))
    .with_secret_sink(Arc::new(move |v: &str| s2.lock().unwrap().push(v.to_string())));
    t.bearer().await.unwrap();
    let got = seen.lock().unwrap().clone();
    assert!(got.contains(&"ACCESS-1".to_string()) && got.contains(&"REFRESH-2".to_string()), "{got:?}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.join("t.json")).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "owner-only: {mode:o}");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
