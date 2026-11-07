//! OAuth access tokens from a refresh token (Twitch and Google use the same grant).
//! Refreshes lazily when the token is missing or within a minute of expiry, and again
//! after the caller reports a 401. A rotated refresh token is kept in memory.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use sm_connectors::youtube::AUTH_EXPIRED_ERR;

use crate::http::{HttpClient, HttpReq};

const MARGIN: Duration = Duration::from_secs(60);

pub struct TokenConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

struct State {
    access: Option<(String, Instant)>,
    refresh_token: String,
    /// Set after Google rejected the refresh token (`invalid_grant`): the token file's modification time
    /// then, and when it was last looked at. No refresh is attempted until the file changes.
    parked: Option<(Option<std::time::SystemTime>, Instant)>,
}

pub struct TokenSource {
    cfg: TokenConfig,
    http: Arc<HttpClient>,
    state: tokio::sync::Mutex<State>,
    /// Where a rotated refresh token is kept across restarts (in /data, plan §10).
    store: Option<PathBuf>,
    sink: Option<SecretSink>,
    /// Park on `invalid_grant` instead of retrying (YouTube; youtube-spec.md section 2).
    park_on_invalid_grant: bool,
    park_recheck: Duration,
}

/// Owner-only (0600 on unix; Windows relies on the folder's ACL), flushed to disk, then
/// renamed over the old file (review round 1, finding 12).
fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Receives every new token value, so the outbound filter can block it (finding 12).
pub type SecretSink = Arc<dyn Fn(&str) + Send + Sync>;

pub(crate) fn form_enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn stored_refresh_token(path: &std::path::Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let rt = serde_json::from_slice::<Value>(&bytes).ok()?["refresh_token"].as_str()?.to_string();
    (!rt.is_empty()).then_some(rt)
}

fn mtime(path: &std::path::Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

impl TokenSource {
    pub fn new(cfg: TokenConfig, http: Arc<HttpClient>) -> Self {
        let refresh_token = cfg.refresh_token.clone();
        TokenSource { cfg, http, state: tokio::sync::Mutex::new(State { access: None, refresh_token, parked: None }), store: None, sink: None, park_on_invalid_grant: false, park_recheck: Duration::from_secs(60) }
    }

    /// Keeps the refresh token in `path`; a token saved there earlier wins over the one
    /// from secrets.toml (it is the newer one after a rotation).
    pub fn with_store(mut self, path: PathBuf) -> Self {
        if let Some(rt) = stored_refresh_token(&path) {
            self.state.get_mut().refresh_token = rt;
        }
        self.store = Some(path);
        self
    }

    /// After Google answers `invalid_grant`, stop refreshing and answer `AUTH_EXPIRED_ERR` until the
    /// token file (`with_store`) changes: the owner has signed in again.
    pub fn with_park_on_invalid_grant(mut self) -> Self {
        self.park_on_invalid_grant = true;
        self
    }

    /// How often a parked source looks at the token file (60 s by default).
    pub fn with_park_recheck(mut self, every: Duration) -> Self {
        self.park_recheck = every;
        self
    }

    /// Every access token and rotated refresh token is passed to `sink`.
    pub fn with_secret_sink(mut self, sink: SecretSink) -> Self {
        self.sink = Some(sink);
        self
    }

    pub async fn bearer(&self) -> Result<String, String> {
        let mut s = self.state.lock().await;
        if let Some((t, exp)) = &s.access {
            if Instant::now() + MARGIN < *exp {
                return Ok(t.clone());
            }
        }
        if let Some((parked_mtime, last_check)) = s.parked {
            let Some(path) = self.store.as_ref() else { return Err(AUTH_EXPIRED_ERR.to_string()) };
            if last_check.elapsed() < self.park_recheck {
                return Err(AUTH_EXPIRED_ERR.to_string());
            }
            let now_mtime = mtime(path);
            if now_mtime == parked_mtime {
                s.parked = Some((parked_mtime, Instant::now()));
                return Err(AUTH_EXPIRED_ERR.to_string());
            }
            // The owner signed in again: use the new refresh token.
            if let Some(rt) = stored_refresh_token(path) {
                s.refresh_token = rt;
            }
            s.parked = None;
        }
        let body = format!(
            "grant_type=refresh_token&refresh_token={}&client_id={}&client_secret={}",
            form_enc(&s.refresh_token),
            form_enc(&self.cfg.client_id),
            form_enc(&self.cfg.client_secret)
        );
        let (status, text) = self
            .http
            .send(HttpReq {
                method: "POST",
                url: self.cfg.token_url.clone(),
                headers: vec![
                    ("Content-Type".into(), "application/x-www-form-urlencoded".into()),
                    ("Accept".into(), "application/json".into()),
                ],
                body: Some(body.into_bytes()),
            })
            .await?;
        if status != 200 {
            if self.park_on_invalid_grant && serde_json::from_str::<Value>(&text).ok().is_some_and(|v| v["error"] == "invalid_grant") {
                s.access = None;
                s.parked = Some((self.store.as_deref().and_then(mtime), Instant::now()));
                return Err(AUTH_EXPIRED_ERR.to_string());
            }
            // The body can echo request details; report only the status.
            return Err(format!("token refresh failed: HTTP {status}"));
        }
        let v: Value = serde_json::from_str(&text).map_err(|_| "token refresh returned bad JSON".to_string())?;
        let token = v["access_token"].as_str().filter(|t| !t.is_empty()).ok_or("token refresh returned no access_token")?.to_string();
        let ttl = v["expires_in"].as_u64().unwrap_or(3600);
        if let Some(sink) = &self.sink {
            sink(&token);
        }
        if let Some(rt) = v["refresh_token"].as_str().filter(|t| !t.is_empty()) {
            if rt != s.refresh_token {
                s.refresh_token = rt.to_string();
                if let Some(sink) = &self.sink {
                    sink(rt);
                }
                if let Some(p) = &self.store {
                    let body = serde_json::json!({"refresh_token": rt}).to_string();
                    write_atomic(p, body.as_bytes()).map_err(|e| format!("saving the rotated refresh token failed: {e}"))?;
                }
            }
        }
        s.access = Some((token.clone(), Instant::now() + Duration::from_secs(ttl)));
        Ok(token)
    }

    /// Forget the cached access token (after a 401).
    pub async fn invalidate(&self) {
        self.state.lock().await.access = None;
    }
}
