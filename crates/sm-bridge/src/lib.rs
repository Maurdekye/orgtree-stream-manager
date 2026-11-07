//! The bridge: everything between the connectors and the org.
//!
//! - [`Bridge::on_event`]: a connector event (chat, donation, stream state, health) goes
//!   through the pipeline, gate, credit, AFK and effect rules; org events are published.
//! - [`Bridge::on_command`]: an agent command (already validated by `sm-core::protocol`)
//!   changes local state or produces platform [`Action`]s.
//! - [`Bridge::tick`]: timers (AFK fade, gate auto-relax, overflow summary, quiet report,
//!   stats refresh).
//!
//! Pure: no I/O, explicit times. Executors outside run the returned actions.
//! A command's `command.result` reports what the bridge decided ("queued" for platform
//! work); a platform's own refusal arrives later as a `system.health`/`action` event.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Map, Value};
use sm_connectors::model::InEvent;
use sm_connectors::streamlabs::{link_donor, RecentChatter};
use sm_connectors::ytstate::{QuotaLevel, YtBans, YtQuota};
use sm_core::afk::{Activity, AfkAction, AfkKind, AfkParams, AfkTracker};
use sm_core::sleep::{SleepAction, SleepParams, SleepTracker};
use sm_core::credit::{Credit, CreditParams, EffectiveClock};
use sm_core::curve::{max_chars, CurveParams};
use sm_core::donation::{Source, ValueTable};
use sm_core::effects::{choose_donation_effect, Choice, DonationRule, EffectCooldowns};
use sm_core::gate::{Actor, Gate, GateChange, GateError, GateParams, GateValue};
use sm_core::metrics::InterruptionMeter;
use sm_core::outbound::{OutboundFilter, RepeatGuard};
use sm_core::pipeline::{
    on_donation, process_chat, process_donation_text, ChatCtx, DonationText, Outcome, RateWindow, ViewerState,
};
use sm_core::protocol::{Command, GateValueArg, Platform};
use sm_core::text::{flags_for, grapheme_count, normalize, Flag, FlagContext};
use sm_server::iso8601;
use sm_server::stats::{LimitRow, Stats};

/// Where org events go (the server's hub in production).
pub trait Publisher {
    fn publish(&self, kind: &str, body: Value) -> u64;
}

impl Publisher for sm_server::Hub {
    fn publish(&self, kind: &str, body: Value) -> u64 {
        sm_server::Hub::publish(self, kind, body)
    }
}

/// Platform work for the executors.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Chat { platform: Platform, text: String, reply_to: Option<String> },
    /// A timeout when `duration_s` is set, a permanent ban otherwise.
    Ban { platform: Platform, user: String, duration_s: Option<u32>, reason: String },
    Unban { platform: Platform, user: String },
    Delete { platform: Platform, message_id: String },
    /// Twitch only.
    SlowMode { seconds: u32 },
    ObsTransition { name: String, duration_ms: u64 },
    ObsScene { scene: String },
    /// Stops the OBS output, which ends every platform's stream (Twitch has no API for it).
    ObsStop,
    YoutubeEnd { broadcast_id: String },
    PlayEffect { effect: String, donor_label: Option<String>, usd: Option<f64> },
}

/// One platform's live session (USER m768f41d60ef1).
#[derive(Debug, Clone, Copy, PartialEq)]
struct Session {
    start: f64,
    seen: f64,
    /// Restored from a bare-number record (before the fix): judged by `disclosure_session_max_s`.
    legacy: bool,
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub curve: CurveParams,
    pub credit: CreditParams,
    pub gate: GateParams,
    pub afk: AfkParams,
    /// Auto-sleep (USER m66df61bea159); ships off. When on, it replaces the automatic AFK.
    pub sleep: SleepParams,
    pub values: ValueTable,
    /// Effect catalogue: id → cooldown seconds (§18).
    pub effects: HashMap<String, f64>,
    pub donation_effects: Vec<DonationRule>,
    /// Viewer ids of the bridge's own bot accounts (their messages are dropped).
    pub bot_viewer_ids: Vec<String>,
    pub global_cap_per_min: usize,
    pub chat_min_gap_s: f64,
    pub chat_per_min: usize,
    pub moderation_per_min: usize,
    pub stream_end_min_gap_s: f64,
    /// `stream.end` (OBS StopStream, YouTube end) is refused unless this is true (Phase 7 guard, coordinator m0c328f887e68).
    pub stream_end_enabled: bool,
    /// At most one agent `stream.afk` per this many seconds (review round 1, finding 5).
    pub afk_command_min_gap_s: f64,
    pub secrets: Vec<String>,
    pub deny: Vec<String>,
    pub chat_say_enabled: bool,
    pub disclosure_line: String,
    /// A persisted disclosure older than this belongs to a past session (USER ma80a334356d2).
    pub disclosure_session_max_s: f64,
    /// A session continues across a restart within this long of "last seen live" (USER m768f41d60ef1).
    pub session_gap_s: f64,
    /// How often "last seen live" is refreshed while live (each refresh rewrites the record).
    pub seen_every_s: f64,
    pub afk_line: String,
    pub afk_scene: String,
    pub sleep_scene: String,
    pub live_scene: String,
    pub fade_transition: String,
    pub cut_transition: String,
    pub quiet_report_s: f64,
    pub overflow_every_s: f64,
    pub repeat_window_s: f64,
    pub repeat_similarity: f64,
    pub max_label_graphemes: usize,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        BridgeConfig {
            curve: CurveParams::default(),
            credit: CreditParams::default(),
            gate: GateParams::default(),
            afk: AfkParams::default(),
            sleep: SleepParams::default(),
            values: ValueTable::default(),
            effects: HashMap::new(),
            donation_effects: Vec::new(),
            bot_viewer_ids: Vec::new(),
            global_cap_per_min: 18,
            chat_min_gap_s: 10.0,
            chat_per_min: 4,
            moderation_per_min: 30,
            stream_end_min_gap_s: 600.0,
            stream_end_enabled: false,
            afk_command_min_gap_s: 60.0,
            secrets: Vec::new(),
            deny: Vec::new(),
            chat_say_enabled: true,
            disclosure_line: "Chat replies on this stream come from AI agents.".into(),
            disclosure_session_max_s: 12.0 * 3600.0,
            session_gap_s: 900.0,
            seen_every_s: 60.0,
            afk_line: "The org is away right now; donations are still received.".into(),
            afk_scene: "AFK".into(),
            sleep_scene: "Sleeping".into(),
            live_scene: "Live".into(),
            fade_transition: "Fade".into(),
            cut_transition: "Cut".into(),
            quiet_report_s: 900.0,
            overflow_every_s: 60.0,
            repeat_window_s: 600.0,
            repeat_similarity: 0.8,
            max_label_graphemes: 25,
        }
    }
}

struct Viewer {
    state: ViewerState,
    credit: Credit,
    name: String,
    platform: Platform,
}

pub type CmdResult = Result<Value, (String, String)>;

pub struct Bridge {
    cfg: BridgeConfig,
    gate: Gate,
    afk: AfkTracker,
    sleep: SleepTracker,
    /// Set when the startup check found no usable sleep scene.
    sleep_disabled: bool,
    eff: EffectiveClock,
    afk_since: Option<f64>,
    viewers: HashMap<String, Viewer>,
    recent: HashMap<String, RecentChatter>,
    meter: InterruptionMeter,
    global_cap: RateWindow,
    chat_rates: HashMap<&'static str, RateWindow>,
    mod_rate: RateWindow,
    last_end: Option<f64>,
    repeat: RepeatGuard,
    filter: OutboundFilter,
    cooldowns: EffectCooldowns,
    stats: Stats,
    youtube: Option<(String, String)>,
    /// YouTube's daily quota use and the ban ids it returned, shared with the executor (youtube-spec.md sections 5, 6).
    yt_quota: Option<Arc<YtQuota>>,
    yt_bans: Option<Arc<YtBans>>,
    /// The Pacific day and the highest quota warning (0, 80 or 95) already sent for it.
    yt_warned: (String, u8),
    live: HashMap<&'static str, bool>,
    /// The current live session per platform: when it started (the disclosure went out) and when the
    /// stream was last seen live (persisted by sm-bin; USER m768f41d60ef1).
    disclosed_at: HashMap<&'static str, Session>,
    health: Map<String, Value>,
    held: Map<String, Value>,
    last_overflow: f64,
    last_quiet: f64,
    last_afk_cmd: Option<f64>,
    /// The platforms' own accounts learned at runtime (the YouTube channel), whose
    /// messages come back as chat and are ignored.
    self_ids: Vec<String>,
    gate_by: String,
    gate_reason: String,
    /// When the gate last changed in this live session (kept across restarts by sm-bin).
    gate_changed_at: Option<f64>,
}

fn pname(p: Platform) -> &'static str {
    match p {
        Platform::Twitch => "twitch",
        Platform::Youtube => "youtube",
    }
}

fn split_viewer(id: &str) -> Option<(Platform, &str)> {
    if let Some(u) = id.strip_prefix("twitch:") {
        Some((Platform::Twitch, u))
    } else {
        id.strip_prefix("youtube:").map(|u| (Platform::Youtube, u))
    }
}

fn err(code: &str, detail: impl Into<String>) -> CmdResult {
    Err((code.to_string(), detail.into()))
}

fn amount_json(source: &Source, usd: Option<f64>) -> Value {
    let mut a = json!({ "usd": usd });
    match source {
        Source::TwitchBits { bits } => a["bits"] = json!(bits),
        Source::TwitchSub { tier, count } => {
            a["tier"] = json!(tier);
            a["count"] = json!(count);
        }
        Source::YouTubeSuperChat { amount_micros, currency } | Source::YouTubeMembership { amount_micros, currency } => {
            a["value"] = json!(*amount_micros as f64 / 1_000_000.0);
            a["currency"] = json!(currency);
        }
        Source::Streamlabs { amount, currency } => {
            a["value"] = json!(amount);
            a["currency"] = json!(currency);
        }
    }
    a
}

impl Bridge {
    pub fn new(cfg: BridgeConfig, now: f64) -> Self {
        Bridge {
            gate: Gate::new(&cfg.gate),
            afk: AfkTracker::new(now),
            sleep: SleepTracker::new(now),
            sleep_disabled: false,
            eff: EffectiveClock::default(),
            afk_since: None,
            viewers: HashMap::new(),
            recent: HashMap::new(),
            meter: InterruptionMeter::default(),
            global_cap: RateWindow::new(0.0, cfg.global_cap_per_min, 60.0),
            chat_rates: HashMap::new(),
            mod_rate: RateWindow::new(0.0, cfg.moderation_per_min, 60.0),
            last_end: None,
            repeat: RepeatGuard::new(cfg.repeat_window_s, cfg.repeat_similarity),
            filter: OutboundFilter::new(cfg.secrets.clone(), cfg.deny.clone()),
            cooldowns: EffectCooldowns::default(),
            stats: Stats::new(),
            youtube: None,
            yt_quota: None,
            yt_bans: None,
            yt_warned: (String::new(), 0),
            live: HashMap::new(),
            disclosed_at: HashMap::new(),
            gate_changed_at: None,
            health: Map::new(),
            held: Map::new(),
            last_overflow: now,
            last_quiet: f64::NEG_INFINITY,
            last_afk_cmd: None,
            self_ids: Vec::new(),
            gate_by: "start".into(),
            gate_reason: String::new(),
            cfg,
        }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Shares YouTube's quota counter and ban book with the executor.
    pub fn set_youtube_state(&mut self, quota: Arc<YtQuota>, bans: Arc<YtBans>) {
        self.yt_quota = Some(quota);
        self.yt_bans = Some(bans);
    }

    /// Adds a secret learned after start (a rotated or new token) to the outbound filter
    /// (review round 1, finding 12). Keeps at most 256 learned secrets.
    pub fn add_secret(&mut self, secret: &str) {
        if secret.chars().count() < 8 || self.cfg.secrets.iter().any(|s| s == secret) {
            return;
        }
        self.cfg.secrets.push(secret.to_string());
        let configured = self.cfg.secrets.len();
        if configured > 512 {
            self.cfg.secrets.drain(..configured - 512);
        }
        self.filter = OutboundFilter::new(self.cfg.secrets.clone(), self.cfg.deny.clone());
    }

    fn is_self(&self, viewer_id: &str) -> bool {
        self.cfg.bot_viewer_ids.iter().any(|b| b == viewer_id) || self.self_ids.iter().any(|b| b == viewer_id)
    }

    fn is_live(&self, p: Platform) -> bool {
        self.live.get(pname(p)).copied().unwrap_or(false)
    }

    fn gate_json(&self, now: f64, quiet: bool) -> Value {
        let mut g = json!({
            "min_interval_s": self.gate.min_interval_s,
            "min_donation_usd": self.gate.min_donation_usd,
            "start": {"min_interval_s": self.cfg.gate.start_interval_s, "min_donation_usd": self.cfg.gate.start_donation_usd},
            "by": self.gate_by,
            "reason": self.gate_reason,
            "relax_at": self.gate.relax_at.map(iso8601),
            "interruptions_per_min_5": self.meter.per_min_5(now),
            "interruptions_15": self.meter.last_15(now),
        });
        if quiet {
            g["quiet"] = json!(true);
        }
        g
    }

    fn donor_label(&self, display: Option<&str>, now: f64) -> String {
        let d = normalize(display.unwrap_or(""));
        if d.is_empty() || grapheme_count(&d) > self.cfg.max_label_graphemes || self.filter.check(&d).is_err() {
            return "Someone".into();
        }
        let flags = flags_for(&d, &FlagContext { now, first_seen: f64::NEG_INFINITY, recent: &[] });
        if flags.contains(&Flag::Link) || flags.contains(&Flag::InjectionSuspect) {
            return "Someone".into();
        }
        d
    }

    // ---- AFK ----

    fn activity(&mut self, now: f64, a: Activity, p: &dyn Publisher) -> Vec<Action> {
        let afk_actions = self.afk.activity(&self.cfg.afk, now, a);
        self.apply_afk(now, afk_actions, p)
    }

    /// The AFK line, through the same checks as agent chat: filter, repeat guard (so it
    /// posts at most once per repeat window) and each platform's rate (finding 5).
    fn afk_lines(&mut self, now: f64) -> Vec<Action> {
        let line = self.cfg.afk_line.clone();
        if !self.cfg.chat_say_enabled || self.filter.check(&line).is_err() || self.repeat.check(now, &line).is_err() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for pl in [Platform::Twitch, Platform::Youtube] {
            if !self.is_live(pl) || (pl == Platform::Youtube && self.youtube.is_none()) {
                continue;
            }
            let (gap, per) = (self.cfg.chat_min_gap_s, self.cfg.chat_per_min);
            let rw = self.chat_rates.entry(pname(pl)).or_insert_with(|| RateWindow::new(gap, per, 60.0));
            if rw.try_acquire(now).is_ok() {
                out.push(Action::Chat { platform: pl, text: line.clone(), reply_to: None });
            }
        }
        if !out.is_empty() {
            self.repeat.record(now, &line);
        }
        out
    }

    fn apply_afk(&mut self, now: f64, actions: Vec<AfkAction>, p: &dyn Publisher) -> Vec<Action> {
        let mut out = Vec::new();
        for a in actions {
            match a {
                AfkAction::FadeIn { fade_s } => {
                    out.push(Action::ObsTransition { name: self.cfg.fade_transition.clone(), duration_ms: (fade_s * 1000.0).round() as u64 });
                    out.push(Action::ObsScene { scene: self.cfg.afk_scene.clone() });
                    self.enter_afk(now, "auto", p, &mut out);
                }
                AfkAction::CutToAfk => {
                    out.push(Action::ObsScene { scene: self.cfg.afk_scene.clone() });
                    self.enter_afk(now, "manual", p, &mut out);
                }
                AfkAction::CutBack => {
                    out.push(Action::ObsTransition { name: self.cfg.cut_transition.clone(), duration_ms: 0 });
                    out.push(Action::ObsScene { scene: self.cfg.live_scene.clone() });
                    self.eff.resume(now);
                    if let Some(s) = self.afk_since.take() {
                        self.gate.shift_relax(now - s);
                    }
                    p.publish("stream.state", json!({"state": "live", "since": iso8601(now)}));
                }
                AfkAction::WarnDrops => {
                    p.publish(
                        "system.warning",
                        json!({"code": "afk_during_drops", "detail": "AFK started while drops_campaign is set; Drops campaigns may not allow AFK."}),
                    );
                }
            }
        }
        out
    }

    fn enter_afk(&mut self, now: f64, kind: &str, p: &dyn Publisher, out: &mut Vec<Action>) {
        self.eff.pause(now);
        if self.afk_since.is_none() {
            self.afk_since = Some(now);
        }
        let lines = self.afk_lines(now);
        out.extend(lines);
        p.publish("stream.state", json!({"state": "afk", "afk_kind": kind, "since": iso8601(now)}));
    }

    fn sleep_params(&self) -> SleepParams {
        let mut p = self.cfg.sleep;
        if self.sleep_disabled {
            p.enabled = false;
        }
        p
    }

    /// The session record to persist: `{"twitch": {"start": <unix s>, "seen": <unix s>}, ...}`. A legacy
    /// entry (a bare number, from images before USER m768f41d60ef1) is written back unchanged.
    pub fn disclosure_record(&self) -> Value {
        Value::Object(
            self.disclosed_at
                .iter()
                .map(|(k, s)| (k.to_string(), if s.legacy { json!(s.start) } else { json!({"start": s.start, "seen": s.seen}) }))
                .collect(),
        )
    }

    /// The platform's session continues at `now`: seen live within `session_gap_s` (a legacy record:
    /// within `disclosure_session_max_s` of its disclosure).
    fn continuing(&self, k: &str, now: f64) -> bool {
        self.disclosed_at.get(k).is_some_and(|s| {
            if s.legacy {
                now - s.start < self.cfg.disclosure_session_max_s
            } else {
                now - s.seen < self.cfg.session_gap_s
            }
        })
    }

    /// Restores a persisted record after a restart; unknown platforms and bad values are ignored.
    pub fn restore_disclosure(&mut self, record: &Value) {
        for pl in [Platform::Twitch, Platform::Youtube] {
            let k = pname(pl);
            let fin = |v: &Value| v.as_f64().filter(|t| t.is_finite());
            match record.get(k) {
                Some(v) if v.is_number() => {
                    if let Some(t) = fin(v) {
                        self.disclosed_at.insert(k, Session { start: t, seen: t, legacy: true });
                    }
                }
                Some(v) => {
                    if let (Some(start), Some(seen)) = (fin(&v["start"]), fin(&v["seen"])) {
                        self.disclosed_at.insert(k, Session { start, seen, legacy: false });
                    }
                }
                None => {}
            }
        }
    }

    /// The gate to persist within a live session (liaison me6d4d70ab422); null when it never changed.
    pub fn gate_record(&self) -> Value {
        match self.gate_changed_at {
            None => Value::Null,
            Some(t) => json!({
                "min_interval_s": self.gate.min_interval_s,
                "min_donation_usd": self.gate.min_donation_usd,
                "relax_at": self.gate.relax_at,
                "by": self.gate_by,
                "reason": self.gate_reason,
                "changed_at": t,
            }),
        }
    }

    /// Restores a persisted gate after a restart, if a live session continues across it (restore the
    /// session record first) and the record is well formed (USER m768f41d60ef1).
    pub fn restore_gate(&mut self, record: &Value, now: f64) {
        let num = |k: &str| record.get(k).and_then(Value::as_f64).filter(|x| x.is_finite() && *x >= 0.0);
        let (Some(i), Some(d), Some(t)) = (num("min_interval_s"), num("min_donation_usd"), num("changed_at")) else { return };
        if ![Platform::Twitch, Platform::Youtube].iter().any(|pl| self.continuing(pname(*pl), now)) {
            return;
        }
        self.gate.min_interval_s = i;
        self.gate.min_donation_usd = d;
        self.gate.relax_at = record.get("relax_at").and_then(Value::as_f64).filter(|x| x.is_finite());
        self.gate_by = record.get("by").and_then(Value::as_str).unwrap_or("restored").to_string();
        self.gate_reason = record.get("reason").and_then(Value::as_str).unwrap_or("").to_string();
        self.gate_changed_at = Some(t);
    }

    /// The startup check found the sleep scene unusable: sleep stays off, with a warning.
    pub fn disable_sleep(&mut self, code: &str, detail: &str, p: &dyn Publisher) {
        self.sleep_disabled = true;
        p.publish("system.warning", json!({"code": code, "detail": detail}));
    }

    /// Plain scene switches only: the sleep scene's own transition override makes the slow fade,
    /// and nothing here touches the user's transition settings or ends the stream.
    fn apply_sleep(&mut self, now: f64, actions: Vec<SleepAction>, p: &dyn Publisher) -> Vec<Action> {
        let mut out = Vec::new();
        for a in actions {
            match a {
                SleepAction::Sleep => {
                    out.push(Action::ObsScene { scene: self.cfg.sleep_scene.clone() });
                    p.publish("stream.state", json!({"state": "sleeping", "since": iso8601(now)}));
                }
                SleepAction::Wake => {
                    out.push(Action::ObsScene { scene: self.cfg.live_scene.clone() });
                    p.publish("stream.state", json!({"state": "live", "since": iso8601(now)}));
                }
            }
        }
        out
    }

    // ---- events ----

    pub fn on_event(&mut self, now: f64, ev: InEvent, p: &dyn Publisher) -> Vec<Action> {
        let mut acts = Vec::new();
        match ev {
            InEvent::Chat { platform, viewer_id, login, display, text, msg_id, roles, paid_sub } => {
                // Our own accounts' messages (the AFK line, replies, the disclosure) come back
                // through the platforms: drop them before they count as activity (finding 1).
                if self.is_self(&viewer_id) {
                    return acts;
                }
                acts.extend(self.activity(now, Activity::ViewerChat, p));
                for k in [display.trim().to_lowercase(), login.trim().to_lowercase()] {
                    if !k.is_empty() {
                        self.recent.insert(k, RecentChatter { viewer_id: viewer_id.clone(), last_seen: now });
                    }
                }
                let t_eff = self.eff.effective(now);
                let is_bot = false; // own accounts were dropped above
                let afk = self.afk.is_afk();
                let (outcome, credit, first_seen) = {
                    let v = self.viewers.entry(viewer_id.clone()).or_insert_with(|| Viewer {
                        state: ViewerState::new(now),
                        credit: Credit::new(paid_sub),
                        name: display.clone(),
                        platform,
                    });
                    v.credit.paid_sub = paid_sub;
                    v.name = display.clone();
                    let credit = v.credit.value(&self.cfg.credit, t_eff);
                    let first_seen = v.state.first_seen;
                    let mut ctx = ChatCtx {
                        now,
                        afk,
                        is_bot,
                        gate: &self.gate,
                        curve: &self.cfg.curve,
                        credit_usd: credit,
                        viewer: &mut v.state,
                        global_cap: &mut self.global_cap,
                        meter: &mut self.meter,
                    };
                    (process_chat(&mut ctx, &text), credit, first_seen)
                };
                // The moderation-bot feed (USER m3e0438a5d6ca "every single message unconditionally"):
                // every viewer message once, forwarded or held, with an exemption flag for the
                // broadcaster and moderators. Opt-in for subscribers (sm-server OPT_IN_KINDS).
                let status = match &outcome {
                    Outcome::Forward(_) => "forwarded".to_string(),
                    Outcome::Held(r) => format!("held:{}", r.as_str()),
                    Outcome::Dropped(_) => "dropped".to_string(),
                };
                let exempt = roles.iter().any(|r| r == "broadcaster" || r == "owner" || r == "moderator");
                p.publish(
                    "chat.mod",
                    json!({
                        "platform": pname(platform),
                        "viewer": {"id": viewer_id, "name": display, "roles": roles},
                        "msg_id": msg_id,
                        "text": text,
                        "exempt": exempt,
                        "status": status,
                    }),
                );
                match outcome {
                    Outcome::Forward(f) => {
                        let flags: Vec<String> =
                            serde_json::to_value(&f.flags).ok().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
                        self.stats.chat(now, pname(platform), &display, &f.text, "forwarded", flags.clone());
                        let gate = self.gate_json(now, false);
                        p.publish(
                            "chat",
                            json!({
                                "platform": pname(platform),
                                "viewer": {"id": viewer_id, "name": display, "roles": roles, "first_seen": iso8601(first_seen)},
                                "msg_id": msg_id,
                                "text": f.text,
                                "truncated": f.truncated,
                                "orig_chars": f.orig_chars,
                                "limits": {"credit_usd": credit, "cooldown_s": f.cooldown_s, "max_chars": f.max_chars},
                                "flags": flags,
                                "gate": {"min_interval_s": gate["min_interval_s"], "min_donation_usd": gate["min_donation_usd"],
                                         "interruptions_per_min_5": gate["interruptions_per_min_5"], "interruptions_15": gate["interruptions_15"]},
                            }),
                        );
                    }
                    Outcome::Held(r) => {
                        let n = self.held.get(r.as_str()).and_then(Value::as_u64).unwrap_or(0);
                        self.held.insert(r.as_str().to_string(), json!(n + 1));
                        // Held text stays out of stats.json (finding 11).
                        self.stats.chat(now, pname(platform), &display, "", &format!("held:{}", r.as_str()), vec![]);
                    }
                    Outcome::Dropped(_) => {}
                }
            }
            InEvent::Donation { platform, viewer_id, display, source, text, external_id } => {
                acts.extend(self.activity(now, Activity::Donation, p));
                let usd = self.cfg.values.usd(&source);
                let vid = viewer_id.or_else(|| {
                    if platform.is_none() {
                        display.as_deref().and_then(|d| link_donor(d, &self.recent, now))
                    } else {
                        None
                    }
                });
                let t_eff = self.eff.effective(now);
                let credit_after = match &vid {
                    Some(id) => {
                        let pl = split_viewer(id).map(|x| x.0).or(platform).unwrap_or(Platform::Twitch);
                        let v = self.viewers.entry(id.clone()).or_insert_with(|| Viewer {
                            state: ViewerState::new(now),
                            credit: Credit::new(false),
                            name: display.clone().unwrap_or_default(),
                            platform: pl,
                        });
                        if let Some(u) = usd {
                            v.credit.add(t_eff, u);
                        }
                        let c = v.credit.value(&self.cfg.credit, t_eff);
                        on_donation(&mut v.state, now, &self.cfg.curve, &self.gate, c);
                        c
                    }
                    None => usd.unwrap_or(0.0),
                };
                let (text_out, text_held) = match text.as_deref() {
                    Some(t) => match process_donation_text(&self.gate, &self.cfg.curve, credit_after, t) {
                        DonationText::Forward { text, .. } => (Some(text), false),
                        DonationText::Held => (None, true),
                        DonationText::Empty => (None, false),
                    },
                    None => (None, false),
                };
                let effect = match choose_donation_effect(&self.cfg.donation_effects, &self.cooldowns, now, usd.unwrap_or(0.0), source.name()) {
                    Choice::Play(id) => {
                        let cd = self.cfg.effects.get(&id).copied().unwrap_or(0.0);
                        self.cooldowns.started(&id, now, cd);
                        let label = self.donor_label(display.as_deref(), now);
                        acts.push(Action::PlayEffect { effect: id.clone(), donor_label: Some(label), usd });
                        json!({"id": id, "status": "played"})
                    }
                    Choice::Cooldown => json!({"id": null, "status": "cooldown"}),
                    Choice::None => json!({"id": null, "status": "none"}),
                };
                let donor = display.clone().unwrap_or_else(|| "anonymous".into());
                self.stats.donation(now, platform.map(pname), source.name(), &donor, usd, text_out.as_deref());
                p.publish(
                    "donation",
                    json!({
                        "platform": platform.map(pname),
                        "source": source.name(),
                        "amount": amount_json(&source, usd),
                        "viewer": vid.as_ref().map(|id| json!({"id": id})),
                        "donor_name": display,
                        "text": text_out,
                        "text_held": text_held,
                        "credit_after": credit_after,
                        "effect": effect,
                        "external_id": external_id,
                    }),
                );
            }
            InEvent::StreamState { platform, live } => self.set_live(now, platform, live, p, &mut acts),
            InEvent::BroadcastFound { broadcast_id, live_chat_id, channel_id, .. } => {
                // An active broadcast with a live chat is a live YouTube stream (finding 2).
                self.youtube = Some((broadcast_id, live_chat_id));
                if let Some(c) = channel_id {
                    let id = format!("youtube:{c}");
                    if !self.self_ids.contains(&id) {
                        self.self_ids.push(id);
                    }
                }
                self.set_live(now, Platform::Youtube, true, p, &mut acts);
            }
            InEvent::Revoked { sub_type, status } => {
                p.publish("system.health", json!({"component": "twitch_eventsub", "status": "revoked", "detail": format!("{sub_type}: {status}")}));
            }
            InEvent::OrgTurns(r) => {
                let sp = self.sleep_params();
                let s = self.sleep.report(&sp, now, r);
                acts.extend(self.apply_sleep(now, s, p));
            }
            InEvent::Health { component, up, detail } => {
                let status = if up { "up" } else { "down" };
                self.health.insert(component.to_string(), json!(status));
                p.publish("system.health", json!({"component": component, "status": status, "detail": detail}));
            }
        }
        acts
    }

    fn set_live(&mut self, now: f64, platform: Platform, live: bool, p: &dyn Publisher, acts: &mut Vec<Action>) {
        let k = pname(platform);
        let was = self.live.insert(k, live).unwrap_or(false);
        // At most once per live session (USER ma80a334356d2), across bridge restarts too.
        let disclosed = self.continuing(k, now);
        if live && !was {
            if disclosed {
                if let Some(s) = self.disclosed_at.get_mut(k) {
                    s.seen = now;
                    s.legacy = false;
                }
            } else {
                if self.cfg.chat_say_enabled {
                    acts.push(Action::Chat { platform, text: self.cfg.disclosure_line.clone(), reply_to: None });
                }
                self.disclosed_at.insert(k, Session { start: now, seen: now, legacy: false });
            }
        }
        if !live {
            self.disclosed_at.remove(k);
            if !self.live.values().any(|&l| l) {
                self.gate_changed_at = None; // the live session is over: the next one starts from the start values
            }
            if platform == Platform::Youtube {
                self.youtube = None;
            }
        }
        if live != was {
            p.publish("stream.state", json!({"platform": k, "state": if live { "live" } else { "offline" }, "by": "platform"}));
        }
    }

    // ---- commands ----

    pub fn on_command(&mut self, now: f64, actor: &str, cmd: &Command, p: &dyn Publisher) -> (CmdResult, Vec<Action>) {
        let by = format!("agent:{actor}");
        let mut acts = Vec::new();
        if !matches!(cmd, Command::StreamAfk { .. } | Command::Ping | Command::OrgTurns { .. }) {
            acts.extend(self.activity(now, Activity::AgentCommand, p));
            let sp = self.sleep_params();
            let s = self.sleep.activity(&sp, now);
            acts.extend(self.apply_sleep(now, s, p));
        }
        let r = self.command(now, &by, cmd, p, &mut acts);
        (r, acts)
    }

    fn command(&mut self, now: f64, by: &str, cmd: &Command, p: &dyn Publisher, acts: &mut Vec<Action>) -> CmdResult {
        match cmd {
            Command::Ping => Ok(json!({})),
            Command::StreamEnd { reason, platforms } => {
                if !self.cfg.stream_end_enabled {
                    p.publish("system.warning", json!({"code": "stream_end_disabled", "detail": "stream.end refused: disabled in config (bridge.stream_end_enabled)", "by": by, "reason": reason}));
                    return err("disabled", "stream.end is disabled in config");
                }
                if let Some(t) = self.last_end {
                    if now - t < self.cfg.stream_end_min_gap_s {
                        return err("rate_limited", format!("stream.end at most once per {} s", self.cfg.stream_end_min_gap_s));
                    }
                }
                self.last_end = Some(now);
                let include_yt = platforms.as_ref().is_none_or(|ps| ps.contains(&Platform::Youtube));
                if include_yt {
                    if let Some((b, _)) = &self.youtube {
                        acts.push(Action::YoutubeEnd { broadcast_id: b.clone() });
                    }
                }
                acts.push(Action::ObsStop);
                p.publish("stream.state", json!({"state": "ending", "by": by, "reason": reason, "since": iso8601(now)}));
                Ok(json!({"ending": true}))
            }
            Command::StreamAfk { on, .. } => {
                if let Some(t) = self.last_afk_cmd {
                    if now - t < self.cfg.afk_command_min_gap_s {
                        return err("rate_limited", format!("stream.afk at most once per {} s", self.cfg.afk_command_min_gap_s));
                    }
                }
                self.last_afk_cmd = Some(now);
                let was = self.afk.is_afk();
                let a = self.afk.set_manual(&self.cfg.afk, now, *on);
                acts.extend(self.apply_afk(now, a, p));
                if was != self.afk.is_afk() {
                    self.sleep.afk_changed(now, *on);
                }
                Ok(json!({"afk": on}))
            }
            Command::OrgTurns { state } => {
                let sp = self.sleep_params();
                let s = self.sleep.report(&sp, now, *state);
                acts.extend(self.apply_sleep(now, s, p));
                Ok(json!({}))
            }
            Command::FxTrigger { effect, .. } => {
                let Some(&cd) = self.cfg.effects.get(effect) else { return err("unknown_effect", format!("{effect} is not in the catalogue")) };
                if self.cooldowns.is_cooling(effect, now) {
                    return err("cooldown", format!("{effect} is cooling down"));
                }
                self.cooldowns.started(effect, now, cd);
                acts.push(Action::PlayEffect { effect: effect.clone(), donor_label: None, usd: None });
                Ok(json!({"queued": true}))
            }
            Command::ChatSay { text, platforms, reply_to } => self.chat_say(now, text, platforms.as_deref(), reply_to.as_deref(), acts),
            Command::ModTimeout { viewer, duration_s, reason } => self.moderate(now, by, "timeout", viewer, Some(*duration_s), reason, p, acts),
            Command::ModBan { viewer, reason } => self.moderate(now, by, "ban", viewer, None, reason, p, acts),
            Command::ModUnban { viewer, reason } => {
                let Some((pl, user)) = split_viewer(viewer) else { return err("bad_args", "unknown viewer id") };
                if let Some(Err(rule)) = reason.as_deref().map(|r| self.filter.check(r)) {
                    return err("outbound_filtered", rule);
                }
                if pl == Platform::Youtube && !self.yt_bans.as_ref().is_some_and(|b| b.has(user, now)) {
                    return err("not_found", "no ban of this viewer is known (made outside the bridge or before this version)");
                }
                acts.push(Action::Unban { platform: pl, user: user.to_string() });
                self.publish_mod(now, p, "unban", viewer, by, reason.as_deref().unwrap_or(""));
                Ok(json!({"queued": true}))
            }
            Command::ModDelete { msg_id } => {
                let (pl, id) = if let Some(x) = msg_id.strip_prefix("tw:") {
                    (Platform::Twitch, x)
                } else if let Some(x) = msg_id.strip_prefix("yt:") {
                    (Platform::Youtube, x)
                } else {
                    return err("bad_msg_id", "message ids start with tw: or yt:");
                };
                acts.push(Action::Delete { platform: pl, message_id: id.to_string() });
                self.publish_mod(now, p, "delete", msg_id, by, "");
                Ok(json!({"queued": true}))
            }
            Command::ModSlowMode { seconds } => {
                acts.push(Action::SlowMode { seconds: *seconds });
                self.publish_mod(now, p, "slow_mode", &seconds.to_string(), by, "");
                Ok(json!({"queued": true}))
            }
            Command::GateTighten { value, reason, hold_min } => {
                let r = self.gate.tighten(&self.cfg.gate, gate_value(*value), now, *hold_min, Actor::Agent);
                self.after_gate(now, r, by, reason, p)
            }
            Command::GateLoosen { value, reason } => {
                let r = self.gate.loosen(&self.cfg.gate, gate_value(*value), now, Actor::Agent);
                self.after_gate(now, r, by, reason, p)
            }
            Command::GateSet { min_interval_s, min_donation_usd, reason, hold_min } => {
                let r = self.gate.set(&self.cfg.gate, *min_interval_s, *min_donation_usd, now, *hold_min, Actor::Agent);
                self.after_gate(now, r, by, reason, p)
            }
            Command::LimitsHold { viewer, minutes } => {
                let Some((pl, _)) = split_viewer(viewer) else { return err("bad_args", "unknown viewer id") };
                let v = self.viewers.entry(viewer.clone()).or_insert_with(|| Viewer {
                    state: ViewerState::new(now),
                    credit: Credit::new(false),
                    name: viewer.clone(),
                    platform: pl,
                });
                v.state.held_until = Some(now + *minutes as f64 * 60.0);
                self.publish_mod(now, p, "hold", viewer, by, &format!("{minutes} min"));
                Ok(json!({"held_until": iso8601(now + *minutes as f64 * 60.0)}))
            }
            Command::LimitsRelease { viewer } => {
                if let Some(v) = self.viewers.get_mut(viewer) {
                    v.state.held_until = None;
                }
                self.publish_mod(now, p, "release", viewer, by, "");
                Ok(json!({"released": true}))
            }
        }
    }

    fn chat_say(&mut self, now: f64, text: &str, platforms: Option<&[Platform]>, reply_to: Option<&str>, acts: &mut Vec<Action>) -> CmdResult {
        if !self.cfg.chat_say_enabled {
            return err("chat_say_disabled", "agent chat replies are switched off by the operator");
        }
        if let Err(rule) = self.filter.check(text) {
            return err("outbound_filtered", rule);
        }
        if let Err(rule) = self.repeat.check(now, text) {
            return err(rule, "too similar to a recent reply");
        }
        let wanted: Vec<Platform> = platforms.map(|p| p.to_vec()).unwrap_or_else(|| vec![Platform::Twitch, Platform::Youtube]);
        let mut targets: Vec<Platform> = wanted
            .into_iter()
            .filter(|&pl| self.is_live(pl) && (pl != Platform::Youtube || self.youtube.is_some()))
            .collect();
        if targets.is_empty() {
            return err("not_live", "no requested platform is live");
        }
        // YouTube's quota is nearly spent: replies there are paused (the disclosure and moderation are not).
        let mut skipped = Map::new();
        if targets.contains(&Platform::Youtube) && self.yt_quota.as_ref().is_some_and(|q| q.level(now) == QuotaLevel::Low95) {
            targets.retain(|&pl| pl != Platform::Youtube);
            skipped.insert("youtube".into(), json!("quota_low"));
            if targets.is_empty() {
                return err("quota_low", "YouTube quota is at 95% or more: replies on YouTube are paused until it resets at 00:00 Pacific");
            }
        }
        if targets.contains(&Platform::Youtube) && text.chars().count() > sm_connectors::youtube::MAX_CHAT_CHARS {
            return err("too_long_for_youtube", "YouTube chat allows 200 characters");
        }
        // Check every target before spending any slot (finding 13d).
        let (gap, per) = (self.cfg.chat_min_gap_s, self.cfg.chat_per_min);
        for &pl in &targets {
            let rw = self.chat_rates.entry(pname(pl)).or_insert_with(|| RateWindow::new(gap, per, 60.0));
            if let Err(wait) = rw.check(now) {
                return err("rate_limited", format!("{}: retry in {:.0} s", pname(pl), wait.ceil()));
            }
        }
        for &pl in &targets {
            let _ = self.chat_rates.get_mut(pname(pl)).map(|rw| rw.try_acquire(now));
        }
        self.repeat.record(now, text);
        for &pl in &targets {
            acts.push(Action::Chat {
                platform: pl,
                text: text.to_string(),
                // YouTube chat has no replies: the text goes out plain.
                reply_to: if pl == Platform::Twitch { reply_to.and_then(|r| r.strip_prefix("tw:")).map(str::to_string) } else { None },
            });
        }
        let mut res = json!({"sent_to": targets.iter().map(|&p| pname(p)).collect::<Vec<_>>()});
        if !skipped.is_empty() {
            res["skipped"] = Value::Object(skipped);
        }
        Ok(res)
    }

    #[allow(clippy::too_many_arguments)]
    fn moderate(
        &mut self,
        now: f64,
        by: &str,
        action: &str,
        viewer: &str,
        duration_s: Option<u32>,
        reason: &str,
        p: &dyn Publisher,
        acts: &mut Vec<Action>,
    ) -> CmdResult {
        let Some((pl, user)) = split_viewer(viewer) else { return err("bad_args", "unknown viewer id") };
        // The reason is shown to the platform's moderators and the viewer (finding 6).
        if let Err(rule) = self.filter.check(reason) {
            return err("outbound_filtered", rule);
        }
        if pl == Platform::Youtube && self.youtube.is_none() {
            return err("no_live_chat", "no YouTube live chat is known yet");
        }
        if let Err(wait) = self.mod_rate.try_acquire(now) {
            return err("rate_limited", format!("moderation: retry in {:.0} s", wait.ceil()));
        }
        acts.push(Action::Ban { platform: pl, user: user.to_string(), duration_s, reason: reason.to_string() });
        self.publish_mod(now, p, action, viewer, by, reason);
        Ok(json!({"queued": true}))
    }

    fn publish_mod(&mut self, now: f64, p: &dyn Publisher, action: &str, target: &str, by: &str, reason: &str) {
        self.stats.moderation(now, action, target, by, reason);
        p.publish("moderation", json!({"action": action, "target": target, "by": by, "reason": reason}));
    }

    fn after_gate(&mut self, now: f64, r: Result<GateChange, GateError>, by: &str, reason: &str, p: &dyn Publisher) -> CmdResult {
        match r {
            Err(GateError::Damped { retry_after_s }) => err("damped", format!("one gate change a minute; retry in {:.0} s", retry_after_s.ceil())),
            Err(GateError::OutOfRange(name)) => err("bad_args", name),
            Ok(ch) => {
                if ch.changed {
                    self.gate_by = by.to_string();
                    self.gate_reason = reason.to_string();
                    self.record_gate_change(now, &ch, by, reason, p);
                }
                Ok(json!({"changed": ch.changed, "min_interval_s": ch.to.0, "min_donation_usd": ch.to.1}))
            }
        }
    }

    fn record_gate_change(&mut self, now: f64, ch: &GateChange, by: &str, reason: &str, p: &dyn Publisher) {
        self.gate_changed_at = Some(now);
        if ch.from.0 != ch.to.0 {
            self.stats.gate_change(now, "min_interval", ch.from.0, ch.to.0, by, reason);
        }
        if ch.from.1 != ch.to.1 {
            self.stats.gate_change(now, "min_donation", ch.from.1, ch.to.1, by, reason);
        }
        let g = self.gate_json(now, false);
        p.publish("gate.state", g);
    }

    // ---- timers ----

    pub fn tick(&mut self, now: f64, p: &dyn Publisher) -> Vec<Action> {
        // "Last seen live", about once a minute, so a restart can tell the session continues.
        for (k, s) in self.disclosed_at.iter_mut() {
            if self.live.get(k).copied().unwrap_or(false) && (s.legacy || now - s.seen >= self.cfg.seen_every_s) {
                s.seen = now;
                s.legacy = false;
            }
        }
        let sp = self.sleep_params();
        // With sleep configured, the idle path is sleep and AFK is manual only, even when the
        // startup check turned sleep off (no surprise automatic AFK).
        let afk_actions = if self.cfg.sleep.enabled { Vec::new() } else { self.afk.tick(&self.cfg.afk, now) };
        let mut acts = self.apply_afk(now, afk_actions, p);
        let s = self.sleep.tick(&sp, now, self.afk.is_afk());
        acts.extend(self.apply_sleep(now, s, p));
        if !self.afk.is_afk() {
            if let Some(ch) = self.gate.auto_relax(&self.cfg.gate, now, self.meter.last_15(now)) {
                self.gate_by = "auto-relax".into();
                self.gate_reason = "nothing forwarded for 15 minutes".into();
                let reason = self.gate_reason.clone();
                self.record_gate_change(now, &ch, "auto-relax", &reason, p);
            }
        }
        if now - self.last_overflow >= self.cfg.overflow_every_s {
            if !self.held.is_empty() {
                p.publish(
                    "chat.overflow",
                    json!({"held": self.held, "interruptions_per_min_5": self.meter.per_min_5(now), "interruptions_15": self.meter.last_15(now)}),
                );
                self.held.clear();
            }
            self.last_overflow = now;
        }
        let tight = self.gate.min_interval_s > 0.0 || self.gate.min_donation_usd > 0.0;
        if tight && self.meter.last_15(now) == 0 && now - self.last_quiet >= self.cfg.quiet_report_s {
            p.publish("gate.state", self.gate_json(now, true));
            self.last_quiet = now;
        }
        self.youtube_quota_tick(now, p);
        self.refresh_stats(now);
        acts
    }

    /// One warning per crossing of 80% and of 95% per Pacific day; the daily reset says nothing
    /// (youtube-spec.md section 5, Amendment 1 E4). Also feeds stats.json.
    fn youtube_quota_tick(&mut self, now: f64, p: &dyn Publisher) {
        let Some(q) = self.yt_quota.clone() else { return };
        let (day, used) = q.snapshot(now);
        self.stats.set_youtube_quota(&day, used);
        if self.yt_warned.0 != day {
            self.yt_warned = (day, 0);
        }
        let (level, limit) = (q.level(now), q.limit());
        let crossed = match level {
            QuotaLevel::Ok => 0,
            QuotaLevel::Warn80 => 80,
            QuotaLevel::Low95 => 95,
        };
        if crossed > self.yt_warned.1 {
            self.yt_warned.1 = crossed;
            let mut detail = format!("YouTube quota {crossed}% used ({used} of {limit} units); resets at 00:00 Pacific");
            if crossed == 95 {
                detail.push_str("; replies on YouTube are paused");
            }
            p.publish("system.warning", json!({"code": "youtube_quota", "component": "youtube_quota", "detail": detail}));
        }
    }

    fn platform_state(&self, pl: Platform) -> &'static str {
        if !self.is_live(pl) {
            "offline"
        } else if self.afk.is_afk() {
            "afk"
        } else if self.sleep.is_asleep() {
            "sleeping"
        } else {
            "live"
        }
    }

    fn refresh_stats(&mut self, now: f64) {
        self.stats.set_gate(self.gate_json(now, false));
        let (tw, yt) = (self.platform_state(Platform::Twitch), self.platform_state(Platform::Youtube));
        let combined = if tw == "afk" || yt == "afk" {
            "afk"
        } else if tw == "sleeping" || yt == "sleeping" {
            "sleeping"
        } else if tw == "live" || yt == "live" {
            "live"
        } else {
            "offline"
        };
        let afk_kind = self.afk.kind().map(|k| match k {
            AfkKind::Auto => "auto",
            AfkKind::Manual => "manual",
        });
        self.stats.set_state(json!({"combined": combined, "twitch": tw, "youtube": yt, "afk_kind": afk_kind, "sleep": self.sleep.state_label(&self.sleep_params())}));
        self.stats.set_health(Value::Object(self.health.clone()));
        let t_eff = self.eff.effective(now);
        let mut rows: Vec<LimitRow> = self
            .viewers
            .iter()
            .filter(|(_, v)| v.state.next_allowed_at > now || v.state.held_until.is_some_and(|h| h > now))
            .map(|(id, v)| {
                let c = v.credit.value(&self.cfg.credit, t_eff);
                LimitRow {
                    viewer: id.clone(),
                    name: v.name.clone(),
                    platform: pname(v.platform).to_string(),
                    credit_usd: c,
                    cooldown_s: (v.state.next_allowed_at - now).max(0.0),
                    max_chars: max_chars(&self.cfg.curve, c),
                    next_allowed_at: (v.state.next_allowed_at > now).then_some(v.state.next_allowed_at),
                    held_until: v.state.held_until.filter(|&h| h > now),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.cooldown_s.total_cmp(&b.cooldown_s));
        self.stats.set_limits(rows);
    }
}

fn gate_value(v: GateValueArg) -> GateValue {
    match v {
        GateValueArg::MinInterval => GateValue::MinInterval,
        GateValueArg::MinDonation => GateValue::MinDonation,
    }
}
