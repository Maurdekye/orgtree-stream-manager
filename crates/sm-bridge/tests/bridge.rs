//! The bridge (plan §4-§9, §18 wired together): connector events, agent commands and
//! ticks go through the core rules; org events come out through a Publisher and platform
//! work comes out as Actions. Pure: fake publisher, explicit times.

use std::sync::Mutex;

use serde_json::{json, Value};
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_connectors::model::InEvent;
use sm_core::donation::Source;
use sm_core::effects::DonationRule;
use sm_core::protocol::{Command, GateValueArg, Platform};

#[derive(Default)]
struct Pub {
    events: Mutex<Vec<(String, Value)>>,
}

impl Publisher for Pub {
    fn publish(&self, kind: &str, body: Value) -> u64 {
        let mut e = self.events.lock().unwrap();
        e.push((kind.to_string(), body));
        e.len() as u64
    }
}

impl Pub {
    fn take(&self, kind: &str) -> Vec<Value> {
        let mut e = self.events.lock().unwrap();
        let (hit, keep): (Vec<_>, Vec<_>) = e.drain(..).partition(|(k, _)| k == kind);
        *e = keep;
        hit.into_iter().map(|(_, v)| v).collect()
    }
    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

fn cfg() -> BridgeConfig {
    let mut c = BridgeConfig::default();
    c.effects.insert("chime".into(), 10.0);
    c.effects.insert("airhorn".into(), 30.0);
    c.donation_effects = vec![
        DonationRule { min_usd: 1.0, effect: "chime".into(), sources: vec![] },
        DonationRule { min_usd: 5.0, effect: "airhorn".into(), sources: vec![] },
    ];
    c.secrets = vec!["supersecret-token-123".into()];
    c.deny = vec!["Springfield".into()];
    c
}

fn chat(id: &str, name: &str, text: &str, msg: &str) -> InEvent {
    InEvent::Chat {
        platform: Platform::Twitch,
        viewer_id: format!("twitch:{id}"),
        login: name.to_lowercase(),
        display: name.into(),
        text: text.into(),
        msg_id: format!("tw:{msg}"),
        roles: vec![],
        paid_sub: false,
    }
}

const T0: f64 = 1_000_000.0;

#[test]
fn chat_is_forwarded_with_limits_and_gate_and_overflow_is_summarized() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    b.on_event(T0, chat("42", "V", "hello", "m1"), &p);
    let ev = p.take("chat");
    assert_eq!(ev.len(), 1);
    let c = &ev[0];
    assert_eq!(c["viewer"]["id"], "twitch:42");
    assert_eq!(c["viewer"]["name"], "V");
    assert_eq!(c["text"], "hello");
    assert_eq!(c["msg_id"], "tw:m1");
    assert_eq!(c["limits"]["cooldown_s"], 120.0);
    assert_eq!(c["limits"]["max_chars"], 100);
    assert_eq!(c["gate"]["min_interval_s"], 120.0);
    assert_eq!(c["gate"]["interruptions_15"], 1);
    b.on_event(T0 + 5.0, chat("42", "V", "again", "m2"), &p);
    assert!(p.take("chat").is_empty(), "inside the cooldown");
    b.tick(T0 + 61.0, &p);
    let o = p.take("chat.overflow");
    assert_eq!(o.len(), 1);
    assert_eq!(o[0]["held"]["cooldown"], 1);
    b.tick(T0 + 122.0, &p);
    assert!(p.take("chat.overflow").is_empty(), "nothing new held");
}

#[test]
fn bits_credit_shorten_the_wait_and_trigger_the_matching_effect() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    b.on_event(T0, chat("42", "V", "hello", "m1"), &p);
    let acts = b.on_event(
        T0 + 10.0,
        InEvent::Donation {
            platform: Some(Platform::Twitch),
            viewer_id: Some("twitch:42".into()),
            display: Some("V".into()),
            source: Source::TwitchBits { bits: 500 },
            text: Some("for you".into()),
            external_id: "tw-evt:1".into(),
        },
        &p,
    );
    assert_eq!(acts, vec![Action::PlayEffect { effect: "airhorn".into(), donor_label: Some("V".into()), usd: Some(5.0) }]);
    let d = &p.take("donation")[0];
    assert_eq!(d["amount"]["usd"], 5.0);
    assert_eq!(d["viewer"]["id"], "twitch:42");
    assert_eq!(d["text"], "for you");
    assert_eq!(d["effect"], json!({"id":"airhorn","status":"played"}));
    assert!((d["credit_after"].as_f64().unwrap() - 5.0).abs() < 1e-6);
    // $5 credit: the wait is now 20 s from the donation, so t+30 passes.
    p.clear();
    b.on_event(T0 + 30.0, chat("42", "V", "thanks!", "m3"), &p);
    assert_eq!(p.take("chat").len(), 1);
    // A second $5 within the airhorn's 30 s cooldown falls back to the chime.
    let acts = b.on_event(
        T0 + 31.0,
        InEvent::Donation {
            platform: Some(Platform::Twitch),
            viewer_id: Some("twitch:42".into()),
            display: Some("V".into()),
            source: Source::TwitchBits { bits: 500 },
            text: None,
            external_id: "tw-evt:2".into(),
        },
        &p,
    );
    assert_eq!(acts, vec![Action::PlayEffect { effect: "chime".into(), donor_label: Some("V".into()), usd: Some(5.0) }]);
}

#[test]
fn streamlabs_donor_links_to_a_recent_chatter_and_unsafe_labels_are_hidden() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    b.on_event(T0, chat("42", "Kind Donor", "hi", "m1"), &p);
    let acts = b.on_event(
        T0 + 1.0,
        InEvent::Donation {
            platform: None,
            viewer_id: None,
            display: Some("kind donor".into()),
            source: Source::Streamlabs { amount: "2".into(), currency: "USD".into() },
            text: None,
            external_id: "sl:1".into(),
        },
        &p,
    );
    let d = &p.take("donation")[0];
    assert_eq!(d["viewer"]["id"], "twitch:42");
    assert_eq!(d["platform"], Value::Null);
    assert_eq!(acts, vec![Action::PlayEffect { effect: "chime".into(), donor_label: Some("kind donor".into()), usd: Some(2.0) }]);
    let acts = b.on_event(
        T0 + 20.0,
        InEvent::Donation {
            platform: None,
            viewer_id: None,
            display: Some("visit https://spam.example".into()),
            source: Source::Streamlabs { amount: "2".into(), currency: "USD".into() },
            text: None,
            external_id: "sl:2".into(),
        },
        &p,
    );
    assert_eq!(acts, vec![Action::PlayEffect { effect: "chime".into(), donor_label: Some("Someone".into()), usd: Some(2.0) }]);
    let d = &p.take("donation")[0];
    assert_eq!(d["viewer"], Value::Null, "an unknown donor stays unlinked");
}

#[test]
fn gate_commands_publish_state_and_are_damped() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    let (r, _) = b.on_command(T0, "listener-control", &Command::GateTighten { value: GateValueArg::MinInterval, reason: "busy".into(), hold_min: None }, &p);
    assert!(r.is_ok());
    let g = &p.take("gate.state")[0];
    assert_eq!(g["min_interval_s"], 180.0);
    assert_eq!(g["by"], "agent:listener-control");
    assert_eq!(g["reason"], "busy");
    assert!(g["relax_at"].as_str().is_some());
    let (r, _) = b.on_command(T0 + 10.0, "listener-control", &Command::GateLoosen { value: GateValueArg::MinInterval, reason: "calm".into() }, &p);
    assert_eq!(r.unwrap_err().0, "damped");
    let (r, _) = b.on_command(
        T0 + 61.0,
        "listener-control",
        &Command::GateSet { min_interval_s: Some(0.0), min_donation_usd: Some(0.0), reason: "open".into(), hold_min: None },
        &p,
    );
    assert!(r.is_ok());
    assert_eq!(p.take("gate.state")[0]["min_interval_s"], 0.0);
}

#[test]
fn afk_fades_in_after_quiet_and_cuts_back_on_activity() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    let acts = b.tick(T0 + 1800.0, &p);
    assert!(acts.contains(&Action::ObsTransition { name: "Fade".into(), duration_ms: 120_000 }), "{acts:?}");
    assert!(acts.contains(&Action::ObsScene { scene: "AFK".into() }));
    let s = &p.take("stream.state")[0];
    assert_eq!((s["state"].as_str(), s["afk_kind"].as_str()), (Some("afk"), Some("auto")));
    // Chat is held while AFK, but it is activity: AFK ends at once and the message goes through.
    let acts = b.on_event(T0 + 1900.0, chat("7", "W", "anyone here?", "m9"), &p);
    assert_eq!(
        acts,
        vec![Action::ObsTransition { name: "Cut".into(), duration_ms: 0 }, Action::ObsScene { scene: "Live".into() }]
    );
    assert_eq!(p.take("stream.state")[0]["state"], "live");
    assert_eq!(p.take("chat").len(), 1);
}

#[test]
fn manual_afk_is_sticky_and_drops_warns() {
    let p = Pub::default();
    let mut c = cfg();
    c.afk.drops_campaign = true;
    let mut b = Bridge::new(c, T0);
    let (r, acts) = b.on_command(T0, "l", &Command::StreamAfk { on: true, reason: "break".into() }, &p);
    assert!(r.is_ok());
    assert_eq!(acts, vec![Action::ObsScene { scene: "AFK".into() }]);
    assert_eq!(p.take("system.warning")[0]["code"], "afk_during_drops");
    assert_eq!(p.take("stream.state")[0]["afk_kind"], "manual");
    let acts = b.on_event(T0 + 5.0, chat("7", "W", "hi", "m1"), &p);
    assert!(acts.is_empty(), "a manual AFK stays");
    assert!(p.take("chat").is_empty(), "held while AFK");
    let (_, acts) = b.on_command(T0 + 61.0, "l", &Command::StreamAfk { on: false, reason: "back".into() }, &p); // review finding 5: one AFK change a minute
    assert_eq!(acts, vec![Action::ObsTransition { name: "Cut".into(), duration_ms: 0 }, Action::ObsScene { scene: "Live".into() }]);
}

#[test]
fn stream_end_stops_obs_and_ends_youtube_once_per_ten_minutes() {
    let p = Pub::default();
    let mut c = cfg();
    c.stream_end_enabled = true; // Phase 7 guard: off by default
    let mut b = Bridge::new(c, T0);
    b.on_event(T0, InEvent::BroadcastFound { platform: Platform::Youtube, broadcast_id: "B1".into(), live_chat_id: "L1".into(), channel_id: None }, &p);
    p.take("stream.state"); // review finding 2: a found broadcast is live
    let (r, acts) = b.on_command(T0 + 1.0, "l", &Command::StreamEnd { reason: "done".into(), platforms: None }, &p);
    assert!(r.is_ok());
    assert_eq!(acts, vec![Action::YoutubeEnd { broadcast_id: "B1".into() }, Action::ObsStop]);
    assert_eq!(p.take("stream.state")[0]["state"], "ending");
    let (r, acts) = b.on_command(T0 + 60.0, "l", &Command::StreamEnd { reason: "again".into(), platforms: None }, &p);
    assert_eq!(r.unwrap_err().0, "rate_limited");
    assert!(acts.is_empty());
}

#[test]
fn chat_say_checks_switch_filter_liveness_repeat_and_rate() {
    let p = Pub::default();
    let mut c = cfg();
    c.chat_say_enabled = false;
    let mut b = Bridge::new(c, T0);
    let say = |t: &str| Command::ChatSay { text: t.into(), platforms: None, reply_to: None };
    assert_eq!(b.on_command(T0, "l", &say("hi"), &p).0.unwrap_err().0, "chat_say_disabled");

    let mut b = Bridge::new(cfg(), T0);
    assert_eq!(b.on_command(T0, "l", &say("hi"), &p).0.unwrap_err().0, "not_live");
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    let (r, _) = b.on_command(T0 + 1.0, "l", &say("write me at a@b.example"), &p);
    assert_eq!(r.unwrap_err(), ("outbound_filtered".to_string(), "email".to_string()));
    let (r, _) = b.on_command(T0 + 1.0, "l", &say("I am in springfield"), &p);
    assert_eq!(r.unwrap_err().1, "deny_list");
    let (r, acts) = b.on_command(T0 + 20.0, "l", &say("Welcome to the stream!"), &p);
    assert!(r.is_ok());
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Twitch, text: "Welcome to the stream!".into(), reply_to: None }]);
    assert_eq!(b.on_command(T0 + 40.0, "l", &say("welcome to the stream!!"), &p).0.unwrap_err().0, "outbound_repeat");
    assert_eq!(b.on_command(T0 + 25.0, "l", &say("Different words entirely"), &p).0.unwrap_err().0, "rate_limited");
}

#[test]
fn moderation_routes_by_platform_and_is_published() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    let (r, acts) = b.on_command(T0, "l", &Command::ModTimeout { viewer: "twitch:42".into(), duration_s: 600, reason: "spam".into() }, &p);
    assert!(r.is_ok());
    assert_eq!(acts, vec![Action::Ban { platform: Platform::Twitch, user: "42".into(), duration_s: Some(600), reason: "spam".into() }]);
    let m = &p.take("moderation")[0];
    assert_eq!((m["action"].as_str(), m["target"].as_str(), m["by"].as_str()), (Some("timeout"), Some("twitch:42"), Some("agent:l")));
    let (r, _) = b.on_command(T0, "l", &Command::ModBan { viewer: "youtube:UC1".into(), reason: "abuse".into() }, &p);
    assert_eq!(r.unwrap_err().0, "no_live_chat");
    b.on_event(T0, InEvent::BroadcastFound { platform: Platform::Youtube, broadcast_id: "B1".into(), live_chat_id: "L1".into(), channel_id: None }, &p);
    let (_, acts) = b.on_command(T0, "l", &Command::ModBan { viewer: "youtube:UC1".into(), reason: "abuse".into() }, &p);
    assert_eq!(acts, vec![Action::Ban { platform: Platform::Youtube, user: "UC1".into(), duration_s: None, reason: "abuse".into() }]);
    let (_, acts) = b.on_command(T0, "l", &Command::ModDelete { msg_id: "tw:abc".into() }, &p);
    assert_eq!(acts, vec![Action::Delete { platform: Platform::Twitch, message_id: "abc".into() }]);
    assert_eq!(b.on_command(T0, "l", &Command::ModDelete { msg_id: "zz:1".into() }, &p).0.unwrap_err().0, "bad_msg_id");
    let (_, acts) = b.on_command(T0, "l", &Command::ModSlowMode { seconds: 10 }, &p);
    assert_eq!(acts, vec![Action::SlowMode { seconds: 10 }]);
    // A personal hold stops forwarding without any platform action.
    let (_, acts) = b.on_command(T0, "l", &Command::LimitsHold { viewer: "twitch:42".into(), minutes: 5 }, &p);
    assert!(acts.is_empty());
    p.clear();
    b.on_event(T0 + 10.0, chat("42", "V", "hi", "m1"), &p);
    assert!(p.take("chat").is_empty());
}

#[test]
fn quiet_report_once_per_fifteen_minutes_when_gate_is_closed_down() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    b.tick(T0 + 1.0, &p);
    let q = p.take("gate.state");
    assert_eq!(q.len(), 1);
    assert_eq!(q[0]["quiet"], true);
    assert_eq!(q[0]["interruptions_15"], 0);
    b.tick(T0 + 600.0, &p);
    assert!(p.take("gate.state").is_empty());
    b.tick(T0 + 902.0, &p);
    assert_eq!(p.take("gate.state").len(), 1);
    // Fully open gate: nothing to loosen, so no quiet report.
    let mut b = Bridge::new(cfg(), T0);
    let (r, _) = b.on_command(T0, "l", &Command::GateSet { min_interval_s: Some(0.0), min_donation_usd: Some(0.0), reason: "open".into(), hold_min: None }, &p);
    assert!(r.is_ok());
    p.clear();
    b.tick(T0 + 1.0, &p);
    assert!(p.take("gate.state").is_empty());
}

#[test]
fn disclosure_line_once_per_live_session() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    let acts = b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Twitch, text: BridgeConfig::default().disclosure_line, reply_to: None }]);
    assert!(b.on_event(T0 + 5.0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p).is_empty());
    b.on_event(T0 + 10.0, InEvent::StreamState { platform: Platform::Twitch, live: false }, &p);
    assert_eq!(b.on_event(T0 + 20.0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p).len(), 1);
}

#[test]
fn stats_follow_the_bridge() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    b.on_event(T0, chat("42", "V", "hello", "m1"), &p);
    b.on_event(T0 + 1.0, chat("42", "V", "again", "m2"), &p);
    b.tick(T0 + 2.0, &p);
    let s = b.stats().snapshot(T0 + 2.0);
    let chat = s["chat"].as_array().unwrap();
    assert_eq!(chat.len(), 2);
    assert_eq!(chat[0]["status"], "forwarded");
    assert_eq!(chat[1]["status"], "held:cooldown");
    assert_eq!(s["limits"][0]["viewer"], "twitch:42");
    assert_eq!(s["gate"]["min_interval_s"], 120.0);
}
