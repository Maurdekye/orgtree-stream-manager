//! secrets.toml (plan §10): read from the read-only secrets mount, outside every Orgtree
//! folder. Debug output never shows a value.

use serde::Deserialize;
use sm_server::{Scopes, TokenTable};

pub const MIN_BRIDGE_TOKEN_CHARS: usize = 32;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwitchSecret {
    pub client_secret: String,
    /// The bot account's refresh token (chat, moderation).
    pub refresh_token: String,
    /// The broadcaster's refresh token, for bits, subs and stream state, which need the
    /// broadcaster's own authorization. Omit when the bot is the broadcaster.
    #[serde(default)]
    pub broadcaster_refresh_token: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoogleSecret {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamlabsSecret {
    pub socket_token: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObsSecret {
    pub password: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeToken {
    pub name: String,
    pub token: String,
    #[serde(default)]
    pub control: bool,
    /// mod.ban and mod.timeout only (the moderation-bot pool).
    #[serde(default)]
    pub moderate: bool,
}

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Secrets {
    pub twitch: Option<TwitchSecret>,
    pub youtube: Option<GoogleSecret>,
    pub streamlabs: Option<StreamlabsSecret>,
    pub obs: Option<ObsSecret>,
    #[serde(rename = "bridge_token")]
    pub bridge_tokens: Vec<BridgeToken>,
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set = |b: bool| if b { "set" } else { "absent" };
        f.debug_struct("Secrets")
            .field("twitch", &set(self.twitch.is_some()))
            .field("youtube", &set(self.youtube.is_some()))
            .field("streamlabs", &set(self.streamlabs.is_some()))
            .field("obs", &set(self.obs.is_some()))
            .field("bridge_tokens", &self.bridge_tokens.iter().map(|t| t.name.as_str()).collect::<Vec<_>>())
            .finish()
    }
}

pub fn load_secrets(text: &str) -> Result<Secrets, String> {
    // The parse error could quote a value; report only its position.
    let s: Secrets = toml::from_str(text).map_err(|e| match e.span() {
        Some(r) => format!("secrets.toml is invalid near byte {}", r.start),
        None => "secrets.toml is invalid".to_string(),
    })?;
    for t in &s.bridge_tokens {
        if t.name.is_empty() || !t.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err("bridge_token names are letters, digits, - or _".into());
        }
        if t.token.chars().count() < MIN_BRIDGE_TOKEN_CHARS {
            return Err(format!("bridge_token {}: token must be at least {MIN_BRIDGE_TOKEN_CHARS} characters", t.name));
        }
        if t.control && t.moderate {
            return Err(format!("bridge_token {}: control and moderate are separate tokens", t.name));
        }
    }
    Ok(s)
}

impl Secrets {
    /// Every secret value, for the outbound chat filter (§9.2).
    pub fn all_values(&self) -> Vec<String> {
        let mut v = Vec::new();
        if let Some(t) = &self.twitch {
            v.extend([t.client_secret.clone(), t.refresh_token.clone()]);
            v.extend(t.broadcaster_refresh_token.clone());
        }
        if let Some(y) = &self.youtube {
            v.extend([y.client_secret.clone(), y.refresh_token.clone()]);
        }
        if let Some(s) = &self.streamlabs {
            v.push(s.socket_token.clone());
        }
        if let Some(o) = &self.obs {
            v.push(o.password.clone());
        }
        v.extend(self.bridge_tokens.iter().map(|t| t.token.clone()));
        v.retain(|x| !x.is_empty());
        v
    }

    pub fn token_table(&self) -> TokenTable {
        TokenTable::new(
            self.bridge_tokens.iter().map(|t| (t.name.clone(), t.token.clone(), Scopes { read: true, control: t.control, moderate: t.moderate })).collect(),
        )
    }
}
