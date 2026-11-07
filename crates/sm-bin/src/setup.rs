//! Setup pieces (plan §2): bridge-token generation, the RFC 8628 device-code sign-in
//! used for Twitch and Google, and the channel owner's consent record. Setup is run by
//! the user, never by an agent.

use std::path::Path;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::{json, Value};

use crate::http::{HttpClient, HttpReq};
use crate::tokens::form_enc;

/// Twitch scopes [doc]: the bot reads and sends chat and moderates.
pub const TWITCH_BOT_SCOPES: &[&str] = &[
    "user:read:chat",
    "user:write:chat",
    "user:bot",
    "moderator:manage:banned_users",
    "moderator:manage:chat_messages",
    "moderator:manage:chat_settings",
];
/// Twitch scopes [doc]: the broadcaster authorizes bits, subs and the bot.
pub const TWITCH_BROADCASTER_SCOPES: &[&str] = &["bits:read", "channel:read:subscriptions", "channel:bot"];
/// YouTube: the device flow ("TV and Limited Input" clients) allows only `youtube` and
/// `youtube.readonly` among the YouTube scopes, and the live chat and ban methods accept `youtube`
/// [doc: developers.google.com/identity/protocols/oauth2/limited-input-device].
pub const YOUTUBE_SCOPES: &[&str] = &["https://www.googleapis.com/auth/youtube"];

pub const TWITCH_DEVICE_URL: &str = "https://id.twitch.tv/oauth2/device"; // [verify]
pub const TWITCH_TOKEN_URL: &str = "https://id.twitch.tv/oauth2/token";
pub const GOOGLE_DEVICE_URL: &str = "https://oauth2.googleapis.com/device/code"; // [verify]
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// A 256-bit random token in unpadded base64url (43 characters).
pub fn gen_bridge_token() -> String {
    let rng = ring::rand::SystemRandom::new();
    let mut b = [0u8; 32];
    ring::rand::SecureRandom::fill(&rng, &mut b).expect("the OS random number generator failed");
    URL_SAFE_NO_PAD.encode(b)
}

pub struct DeviceFlow {
    pub device_url: String,
    pub token_url: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeviceStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub interval_s: u64,
    pub expires_in_s: u64,
}

pub struct DeviceTokens {
    pub access_token: String,
    pub refresh_token: String,
}

/// Never prints the tokens themselves.
impl std::fmt::Debug for DeviceTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeviceTokens { <redacted> }")
    }
}

fn form(pairs: &[(&str, &str)]) -> Vec<u8> {
    pairs.iter().map(|(k, v)| format!("{k}={}", form_enc(v))).collect::<Vec<_>>().join("&").into_bytes()
}

async fn post_form(http: &HttpClient, url: &str, body: Vec<u8>) -> Result<(u16, Value), String> {
    let (status, text) = http
        .send(HttpReq {
            method: "POST",
            url: url.to_string(),
            headers: vec![
                ("Content-Type".into(), "application/x-www-form-urlencoded".into()),
                ("Accept".into(), "application/json".into()),
            ],
            body: Some(body),
        })
        .await?;
    let v = serde_json::from_str(&text).unwrap_or(Value::Null);
    Ok((status, v))
}

impl DeviceFlow {
    /// Asks for a device code. Twitch reads `scopes`, Google reads `scope`; both are sent.
    pub async fn start(&self, http: &HttpClient) -> Result<DeviceStart, String> {
        let scopes = self.scopes.join(" ");
        let (status, v) = post_form(http, &self.device_url, form(&[("client_id", &self.client_id), ("scopes", &scopes), ("scope", &scopes)])).await?;
        if status != 200 {
            return Err(format!("device code request failed: HTTP {status} {}", v["message"].as_str().or(v["error"].as_str()).unwrap_or("")));
        }
        let s = |k: &str| v[k].as_str().map(str::to_string);
        Ok(DeviceStart {
            device_code: s("device_code").ok_or("no device_code in the response")?,
            user_code: s("user_code").ok_or("no user_code in the response")?,
            verification_uri: s("verification_uri").or_else(|| s("verification_url")).ok_or("no verification address in the response")?,
            interval_s: v["interval"].as_u64().unwrap_or(5).max(1),
            expires_in_s: v["expires_in"].as_u64().unwrap_or(1800),
        })
    }

    /// Polls until the user approves, at the server's interval (+5 s after `slow_down`).
    pub async fn poll(&self, http: &HttpClient, s: &DeviceStart) -> Result<DeviceTokens, String> {
        let deadline = Instant::now() + Duration::from_secs(s.expires_in_s);
        let mut interval = s.interval_s;
        let scopes = self.scopes.join(" ");
        loop {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            if Instant::now() > deadline {
                return Err("the sign-in code expired; run setup again".into());
            }
            let mut pairs = vec![
                ("client_id", self.client_id.as_str()),
                ("device_code", s.device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("scopes", scopes.as_str()),
            ];
            if let Some(cs) = &self.client_secret {
                pairs.push(("client_secret", cs.as_str()));
            }
            let (status, v) = post_form(http, &self.token_url, form(&pairs)).await?;
            if status == 200 {
                let refresh_token = v["refresh_token"].as_str().filter(|t| !t.is_empty()).ok_or("signed in, but no refresh_token was returned")?;
                return Ok(DeviceTokens {
                    access_token: v["access_token"].as_str().unwrap_or("").to_string(),
                    refresh_token: refresh_token.to_string(),
                });
            }
            // Google puts the code in `error`, Twitch in `message`.
            let code = v["error"].as_str().or(v["message"].as_str()).unwrap_or("unknown");
            match code {
                "authorization_pending" => {}
                "slow_down" => interval += 5,
                other => return Err(format!("sign-in failed: {other}")),
            }
        }
    }
}

/// What the channel owner agrees to (YouTube API Services Developer Policies require prior,
/// specific and express consent to automated actions [doc]).
pub const CONSENT_ITEMS: &[&str] = &[
    "automated chat replies by AI agents on these channels",
    "automated moderation: timeouts, bans, message deletion and slow mode",
    "automated stream control: AFK on and off, and ending the stream",
    "donation-triggered sound and overlay effects",
    "manage your YouTube account (read and post live chat, moderate it, and end the broadcast when asked)",
];

pub fn write_consent(path: &Path, channels: &str, now: f64) -> std::io::Result<()> {
    let v = json!({
        "channels": channels,
        "at": sm_server::iso8601(now),
        "agreed_to": CONSENT_ITEMS,
        "basis": "recorded by the channel owner with `stream-manager consent`",
    });
    std::fs::write(path, serde_json::to_vec_pretty(&v).map_err(std::io::Error::other)?)
}
