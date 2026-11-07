//! Review round 1 (coordinator, 2026-11-03), bridge findings 1, 2, 4, 5, 6, 11, 12, 13d.

use std::sync::Mutex;

use serde_json::Value;
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_connectors::model::InEvent;
use sm_core::protocol::{Command, Platform};

#[derive(Default)]
struct Pub(Mutex<Vec<(String, Value)>>);

impl Publisher for Pub {
    fn publish(&self, kind: &str, body: Value) -> u64 {
        let mut g = self.0.lock().unwrap();
        g.push((kind.into(), body));
        g.len() as u64
    }
}

impl Pub {
    fn take(&self, kind: &str) -> Vec<Value> {
        let mut g = self.0.lock().unwrap();
        let (hit, keep): (Vec<_>, Vec<_>) = g.drain(..).partition(|(k, _)| k == kind);
        *g = keep;
        hit.into_iter().map(|x| x.1).collect()
    }
}

const T0: f64 = 1_000_000.0;

fn chat(platform: Platform, id: &str, text: &str) -> InEvent {
    let prefix = if platform == Platform::Twitch { "twitch" } else { "youtube" };
    InEvent::Chat {
        platform,
        viewer_id: format!("{prefix}:{id}"),
        login: id.into(),
        display: id.into(),
        text: text.into(),
        msg_id: format!("x:{id}"),
        roles: vec![],
        paid_sub: false,
    }
}

fn found(channel: Option<&str>) -> InEvent {
    InEvent::BroadcastFound { platform: Platform::Youtube, broadcast_id: "B1".into(), live_chat_id: "L1".into(), channel_id: channel.map(str::to_string) }
}

fn say(t: &str) -> Command {
    Command::ChatSay { text: t.into(), platforms: None, reply_to: None }
}

// Finding 1 (HIGH): the bot's own AFK line echoing back must not end the AFK.
#[test]
fn the_bots_own_messages_are_not_activity() {
    let p = Pub::default();
    let mut c = BridgeConfig::default();
    c.bot_viewer_ids = vec!["twitch:bot".into()];
    let mut b = Bridge::new(c, T0);
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    let acts = b.tick(T0 + 1800.0, &p);
    assert!(acts.iter().any(|a| matches!(a, Action::ObsScene { scene } if scene == "AFK")), "{acts:?}");
    let echo = b.on_event(T0 + 1801.0, chat(Platform::Twitch, "bot", "The org is away right now; donations are still received."), &p);
    assert!(echo.is_empty(), "the echo must not cut back: {echo:?}");
    assert_eq!(b.stats().snapshot(T0 + 1802.0)["state"]["twitch"], "afk");
    assert!(p.take("chat").is_empty());
}

// Finding 2 (HIGH): finding a YouTube broadcast means YouTube is live.
#[test]
fn a_found_youtube_broadcast_is_live() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    let acts = b.on_event(T0, found(None), &p);
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Youtube, text: BridgeConfig::default().disclosure_line, reply_to: None }]);
    assert_eq!(p.take("stream.state")[0]["state"], "live");
    let (r, acts) = b.on_command(T0 + 20.0, "l", &say("hello youtube"), &p);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Youtube, text: "hello youtube".into(), reply_to: None }]);
    b.tick(T0 + 21.0, &p);
    assert_eq!(b.stats().snapshot(T0 + 21.0)["state"]["youtube"], "live");
    // The same broadcast found again is not a new live session.
    assert!(b.on_event(T0 + 30.0, found(None), &p).is_empty());
}

// Finding 4: the channel's own messages (posted through the API) come back as chat.
#[test]
fn the_youtube_channels_own_messages_are_dropped() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, found(Some("UCself")), &p);
    p.take("stream.state");
    b.on_event(T0 + 1.0, chat(Platform::Youtube, "UCself", "Chat replies on this stream come from AI agents."), &p);
    assert!(p.take("chat").is_empty());
    b.on_event(T0 + 2.0, chat(Platform::Youtube, "UCviewer", "hi"), &p);
    assert_eq!(p.take("chat").len(), 1);
}

// Finding 5: AFK lines go through the outbound checks; stream.afk is rate limited.
#[test]
fn afk_lines_are_rate_limited_and_stream_afk_cycling_is_refused() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    let afk_line = |acts: &[Action]| acts.iter().filter(|a| matches!(a, Action::Chat { text, .. } if text.contains("away"))).count();
    let (r, acts) = b.on_command(T0 + 100.0, "l", &Command::StreamAfk { on: true, reason: "break".into() }, &p);
    assert!(r.is_ok());
    assert_eq!(afk_line(&acts), 1);
    let (r, _) = b.on_command(T0 + 110.0, "l", &Command::StreamAfk { on: false, reason: "back".into() }, &p);
    assert_eq!(r.unwrap_err().0, "rate_limited", "one AFK change a minute");
    b.on_command(T0 + 170.0, "l", &Command::StreamAfk { on: false, reason: "back".into() }, &p).0.unwrap();
    let (_, acts) = b.on_command(T0 + 240.0, "l", &Command::StreamAfk { on: true, reason: "again".into() }, &p);
    assert_eq!(afk_line(&acts), 0, "the same AFK line within 10 minutes is held back by the repeat guard");
}

// Finding 6: spacing and zero-width characters don't get secrets past the filter, and
// moderation reasons are filtered too.
#[test]
fn filter_sees_through_spacing_and_covers_moderation_reasons() {
    let p = Pub::default();
    let mut c = BridgeConfig::default();
    c.secrets = vec!["supersecret-token-123".into()];
    let mut b = Bridge::new(c, T0);
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    for t in ["it is super\u{200B}secret-token-123", "s u p e r s e c r e t - t o k e n", "mail a @ b . example", "at 192 . 168 . 1 . 20"] {
        assert_eq!(b.on_command(T0 + 20.0, "l", &say(t), &p).0.unwrap_err().0, "outbound_filtered", "{t}");
    }
    let (r, acts) = b.on_command(T0, "l", &Command::ModTimeout { viewer: "twitch:1".into(), duration_s: 60, reason: "write to x@y.example".into() }, &p);
    assert_eq!(r.unwrap_err().0, "outbound_filtered");
    assert!(acts.is_empty());
}

// Finding 12: secrets learned later (rotated tokens) join the filter.
#[test]
fn added_secrets_are_filtered() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    b.add_secret("rotated-refresh-token-xyz");
    assert_eq!(b.on_command(T0 + 20.0, "l", &say("rotated-refresh-token-xyz"), &p).0.unwrap_err().0, "outbound_filtered");
}

// Finding 13d: a rate-limited YouTube doesn't use up a Twitch slot.
#[test]
fn chat_say_checks_every_platforms_rate_before_spending_any() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    b.on_event(T0, found(None), &p);
    b.on_command(T0 + 20.0, "l", &Command::ChatSay { text: "only youtube".into(), platforms: Some(vec![Platform::Youtube]), reply_to: None }, &p).0.unwrap();
    // Both platforms: YouTube is inside its 10 s gap, so nothing is sent...
    assert_eq!(b.on_command(T0 + 25.0, "l", &say("to both"), &p).0.unwrap_err().0, "rate_limited");
    // ...and Twitch's slot is still free.
    let (r, _) = b.on_command(T0 + 25.0, "l", &Command::ChatSay { text: "only twitch".into(), platforms: Some(vec![Platform::Twitch]), reply_to: None }, &p);
    assert!(r.is_ok(), "{r:?}");
}

// Finding 11: held messages' bodies stay out of stats.json.
#[test]
fn held_message_bodies_are_not_kept_in_stats() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, chat(Platform::Twitch, "v", "first"), &p);
    b.on_event(T0 + 1.0, chat(Platform::Twitch, "v", "held secret plan"), &p);
    let s = b.stats().snapshot(T0 + 2.0);
    assert_eq!(s["trust"], "untrusted-viewer-content");
    let chat = s["chat"].as_array().unwrap();
    assert_eq!(chat[1]["status"], "held:cooldown");
    assert_eq!(chat[1]["text"], "");
}
