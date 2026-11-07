//! config.toml (plan §2, §17). Every key has a default; unknown keys are refused.

use std::collections::HashMap;

use serde::Deserialize;
use sm_bridge::BridgeConfig;
use sm_connectors::youtube::{ChatMode, MembershipPrice, QuotaBudget, YtConfig};
use sm_connectors::ytstate::YtCosts;
use sm_core::effects::DonationRule;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerCfg {
    pub events_addr: String,
    /// The overlay page (OBS browser source) and /healthz.
    pub overlay_addr: String,
    pub data_dir: String,
    pub stats_path: String,
    pub replay_max: usize,
    pub replay_window_s: f64,
    pub chat_retention_days: f64,
}

impl Default for ServerCfg {
    fn default() -> Self {
        ServerCfg {
            // Loopback by default (review round 1, finding 13b); the container's config
            // sets 0.0.0.0 and compose publishes on 127.0.0.1 only.
            events_addr: "127.0.0.1:8787".into(),
            overlay_addr: "127.0.0.1:8788".into(),
            data_dir: "/data".into(),
            stats_path: "/out/stats.json".into(),
            replay_max: 200,
            replay_window_s: 900.0,
            chat_retention_days: 7.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TwitchCfg {
    pub enabled: bool,
    pub client_id: String,
    pub broadcaster_id: String,
    pub bot_user_id: String,
    pub eventsub_url: String,
    pub api_base: String,
    pub token_url: String,
}

impl Default for TwitchCfg {
    fn default() -> Self {
        TwitchCfg {
            enabled: false,
            client_id: String::new(),
            broadcaster_id: String::new(),
            bot_user_id: String::new(),
            eventsub_url: "wss://eventsub.wss.twitch.tv/ws".into(),
            api_base: "https://api.twitch.tv".into(),
            token_url: "https://id.twitch.tv/oauth2/token".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct YoutubeCfg {
    pub enabled: bool,
    pub api_base: String,
    pub token_url: String,
    pub units_per_day: u64,
    pub reserved_for_actions: u64,
    pub list_cost: u64,
    /// Poll mode only: hours of streaming the list budget must cover.
    pub budget_hours: f64,
    pub membership_usd: f64,
    /// `stream` (server streaming, the default) or `poll` (the fallback).
    pub chat_mode: ChatMode,
    /// Units per request kind, for the quota counter.
    pub costs: YtCosts,
}

impl Default for YoutubeCfg {
    fn default() -> Self {
        YoutubeCfg {
            enabled: false,
            api_base: "https://www.googleapis.com/youtube/v3".into(),
            token_url: "https://oauth2.googleapis.com/token".into(),
            units_per_day: 10_000,
            reserved_for_actions: 2_000,
            list_cost: 5,
            budget_hours: 24.0,
            membership_usd: 4.99,
            chat_mode: ChatMode::Stream,
            costs: YtCosts::default(),
        }
    }
}

impl YoutubeCfg {
    /// The connector's settings (youtube-spec.md section 7, Amendment 1 E5).
    pub fn to_yt_config(&self) -> YtConfig {
        YtConfig {
            chat_mode: self.chat_mode,
            budget: QuotaBudget {
                units_per_day: self.units_per_day,
                reserved_for_actions: self.reserved_for_actions,
                list_cost: self.list_cost,
                budget_hours: self.budget_hours,
            },
            membership: MembershipPrice { amount_micros: (self.membership_usd * 1_000_000.0).round() as u64, currency: "USD".into() },
            ..YtConfig::default()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamlabsCfg {
    pub enabled: bool,
    /// Without the token; it is appended from secrets.toml.
    pub socket_url: String,
}

impl Default for StreamlabsCfg {
    fn default() -> Self {
        StreamlabsCfg { enabled: false, socket_url: "wss://sockets.streamlabs.com/socket.io/?EIO=3&transport=websocket".into() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObsCfg {
    pub enabled: bool,
    pub url: String,
    pub afk_scene: String,
    pub live_scene: String,
    pub fade_transition: String,
    pub cut_transition: String,
}

impl Default for ObsCfg {
    fn default() -> Self {
        ObsCfg {
            enabled: true,
            url: "ws://host.docker.internal:4455".into(),
            afk_scene: "AFK".into(),
            live_scene: "Live".into(),
            fade_transition: "Fade".into(),
            cut_transition: "Cut".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BridgeCfg {
    pub chat_say_enabled: bool,
    pub disclosure_line: Option<String>,
    pub afk_line: Option<String>,
    pub afk_quiet_min: f64,
    pub afk_fade_s: f64,
    pub drops_campaign: bool,
    pub gate_start_interval_s: f64,
    pub gate_start_donation_usd: f64,
    pub auto_relax: bool,
    pub global_cap_per_min: usize,
    pub half_life_min: f64,
    pub sub_floor_usd: f64,
    pub bits_per_usd: f64,
    pub sub_tier_usd: [f64; 3],
    pub deny: Vec<String>,
    /// Off unless set: `stream.end` is refused (Phase 7 guard).
    pub stream_end_enabled: bool,
}

impl Default for BridgeCfg {
    fn default() -> Self {
        BridgeCfg {
            chat_say_enabled: true,
            disclosure_line: None,
            afk_line: None,
            afk_quiet_min: 30.0,
            afk_fade_s: 120.0,
            drops_campaign: false,
            gate_start_interval_s: 120.0,
            gate_start_donation_usd: 0.0,
            auto_relax: true,
            global_cap_per_min: 18,
            half_life_min: 30.0,
            sub_floor_usd: 1.0,
            bits_per_usd: 100.0,
            sub_tier_usd: [5.0, 10.0, 25.0],
            deny: Vec::new(),
            stream_end_enabled: false,
        }
    }
}

/// Auto-sleep (USER m66df61bea159, plan rev 2). Ships off.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SleepCfg {
    pub enabled: bool,
    pub scene: String,
    pub idle_min: f64,
    pub min_sleep_s: f64,
}

impl Default for SleepCfg {
    fn default() -> Self {
        SleepCfg { enabled: false, scene: "Sleeping".into(), idle_min: 10.0, min_sleep_s: 15.0 }
    }
}

/// The Orgtree live feed (USER 2026-11-06 m77693a85fa7b): the turn count for auto-sleep.
/// Ships off; the relay's `org.turns` command keeps working either way.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OrgtreeFeedCfg {
    pub enabled: bool,
    pub url: String,
    /// The Host header the feed accepts (`127.0.0.1:<port>`).
    pub host: String,
}

impl Default for OrgtreeFeedCfg {
    fn default() -> Self {
        OrgtreeFeedCfg { enabled: false, url: "ws://host.docker.internal:47830/v1/events".into(), host: "127.0.0.1:47830".into() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectCfg {
    pub id: String,
    #[serde(default)]
    pub cooldown_s: f64,
    /// The OBS media source that plays this effect (sounds, §18).
    pub obs_media_input: Option<String>,
    /// The overlay page animation (confetti, pulse, banner) for this effect.
    pub overlay: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerCfg,
    pub twitch: TwitchCfg,
    pub youtube: YoutubeCfg,
    pub streamlabs: StreamlabsCfg,
    pub obs: ObsCfg,
    pub bridge: BridgeCfg,
    pub sleep: SleepCfg,
    pub orgtree_feed: OrgtreeFeedCfg,
    pub currency: HashMap<String, f64>,
    #[serde(rename = "effect")]
    pub effects: Vec<EffectCfg>,
    #[serde(rename = "donation_effect")]
    pub donation_effects: Vec<DonationRule>,
}

fn nonneg(x: f64, name: &str) -> Result<(), String> {
    if x.is_finite() && x >= 0.0 {
        Ok(())
    } else {
        Err(format!("{name} must be a number of at least 0"))
    }
}

pub fn load_config(text: &str) -> Result<Config, String> {
    let c: Config = toml::from_str(text).map_err(|e| e.to_string())?;
    let b = &c.bridge;
    nonneg(b.gate_start_interval_s, "bridge.gate_start_interval_s")?;
    nonneg(b.gate_start_donation_usd, "bridge.gate_start_donation_usd")?;
    nonneg(b.afk_fade_s, "bridge.afk_fade_s")?;
    if !(b.afk_quiet_min > 0.0) {
        return Err("bridge.afk_quiet_min must be above 0".into());
    }
    if !(b.half_life_min > 0.0) || !(b.bits_per_usd > 0.0) {
        return Err("bridge.half_life_min and bridge.bits_per_usd must be above 0".into());
    }
    if !(c.sleep.idle_min.is_finite() && c.sleep.idle_min > 0.0) {
        return Err("sleep.idle_min must be above 0".into());
    }
    nonneg(c.sleep.min_sleep_s, "sleep.min_sleep_s")?;
    if c.sleep.scene.trim().is_empty() {
        return Err("sleep.scene must not be empty".into());
    }
    for (cur, r) in &c.currency {
        if !(r.is_finite() && *r > 0.0) {
            return Err(format!("currency.{cur} must be above 0"));
        }
    }
    for e in &c.effects {
        if e.id.is_empty() || !e.id.chars().all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_') {
            return Err(format!("effect id {:?} must be lowercase letters, digits, - or _", e.id));
        }
        nonneg(e.cooldown_s, "effect.cooldown_s")?;
        if e.obs_media_input.is_none() && e.overlay.is_none() {
            return Err(format!("effect {:?} needs obs_media_input, overlay or both", e.id));
        }
    }
    for r in &c.donation_effects {
        if !c.effects.iter().any(|e| e.id == r.effect) {
            return Err(format!("donation_effect refers to unknown effect {:?}", r.effect));
        }
        nonneg(r.min_usd, "donation_effect.min_usd")?;
    }
    if c.twitch.enabled {
        let t = &c.twitch;
        let missing: Vec<&str> = [("client_id", &t.client_id), ("broadcaster_id", &t.broadcaster_id), ("bot_user_id", &t.bot_user_id)]
            .into_iter()
            .filter(|(_, v)| v.is_empty())
            .map(|(k, _)| k)
            .collect();
        if !missing.is_empty() {
            return Err(format!("twitch is enabled but missing: {}", missing.join(", ")));
        }
    }
    Ok(c)
}

impl Config {
    /// The bridge's settings. `secrets` are every secret value (for the outbound filter).
    pub fn bridge_config(&self, secrets: Vec<String>) -> BridgeConfig {
        let mut b = BridgeConfig::default();
        let c = &self.bridge;
        b.curve = Default::default();
        b.credit.half_life_s = c.half_life_min * 60.0;
        b.credit.sub_floor_usd = c.sub_floor_usd;
        b.gate.start_interval_s = c.gate_start_interval_s;
        b.gate.start_donation_usd = c.gate_start_donation_usd;
        b.gate.auto_relax = c.auto_relax;
        b.afk.quiet_s = c.afk_quiet_min * 60.0;
        b.afk.fade_s = c.afk_fade_s;
        b.afk.drops_campaign = c.drops_campaign;
        b.sleep.enabled = self.sleep.enabled;
        b.sleep.idle_s = self.sleep.idle_min * 60.0;
        b.sleep.min_sleep_s = self.sleep.min_sleep_s;
        b.sleep_scene = self.sleep.scene.clone();
        b.values.bits_per_usd = c.bits_per_usd;
        b.values.sub_tier_usd = c.sub_tier_usd;
        for (cur, r) in &self.currency {
            b.values.set_rate(cur, *r);
        }
        b.effects = self.effects.iter().map(|e| (e.id.clone(), e.cooldown_s)).collect();
        b.donation_effects = self.donation_effects.clone();
        if self.twitch.enabled && !self.twitch.bot_user_id.is_empty() {
            b.bot_viewer_ids = vec![format!("twitch:{}", self.twitch.bot_user_id)];
        }
        b.global_cap_per_min = c.global_cap_per_min;
        b.deny = c.deny.clone();
        b.secrets = secrets;
        b.chat_say_enabled = c.chat_say_enabled;
        b.stream_end_enabled = c.stream_end_enabled;
        if let Some(l) = &c.disclosure_line {
            b.disclosure_line = l.clone();
        }
        if let Some(l) = &c.afk_line {
            b.afk_line = l.clone();
        }
        b.afk_scene = self.obs.afk_scene.clone();
        b.live_scene = self.obs.live_scene.clone();
        b.fade_transition = self.obs.fade_transition.clone();
        b.cut_transition = self.obs.cut_transition.clone();
        b
    }

    /// effect id → overlay animation.
    pub fn effect_overlays(&self) -> HashMap<String, String> {
        self.effects.iter().filter_map(|e| e.overlay.clone().map(|a| (e.id.clone(), a))).collect()
    }

    /// effect id → OBS media input name.
    pub fn effect_media_inputs(&self) -> HashMap<String, String> {
        self.effects.iter().filter_map(|e| e.obs_media_input.clone().map(|i| (e.id.clone(), i))).collect()
    }
}
