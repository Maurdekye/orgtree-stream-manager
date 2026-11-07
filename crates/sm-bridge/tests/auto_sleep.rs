//! Auto-sleep in the bridge (USER 2026-11-05 15:04:39Z m66df61bea159; plan rev 2, coordinator GO
//! mebb231d1b1b3). Sleep is a plain switch to the Sleeping scene (the user's per-scene transition
//! override makes the fade); it never touches transitions and never ends the stream.

use std::sync::Mutex;

use serde_json::Value;
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_core::protocol::Command;
use sm_core::sleep::{SleepParams, TurnsReport};

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

fn cfg() -> BridgeConfig {
    let mut c = BridgeConfig::default();
    c.sleep = SleepParams { enabled: true, idle_s: 600.0, min_sleep_s: 15.0 };
    c.live_scene = "App".into();
    c
}

fn turns(b: &mut Bridge, now: f64, s: TurnsReport, p: &Pub) -> Vec<Action> {
    let (r, acts) = b.on_command(now, "relay", &Command::OrgTurns { state: s }, p);
    assert!(r.is_ok(), "{r:?}");
    acts
}

fn no_transition(acts: &[Action]) {
    assert!(acts.iter().all(|a| !matches!(a, Action::ObsTransition { .. } | Action::ObsStop | Action::YoutubeEnd { .. })), "{acts:?}");
}

#[test]
fn defaults_ship_sleep_off_with_the_sleeping_scene() {
    let c = BridgeConfig::default();
    assert!(!c.sleep.enabled);
    assert_eq!(c.sleep_scene, "Sleeping");
}

#[test]
fn idle_for_ten_minutes_switches_to_sleeping_and_a_turn_switches_back() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    assert!(turns(&mut b, T0 + 1.0, TurnsReport::Idle, &p).is_empty());
    assert!(b.tick(T0 + 600.0, &p).iter().all(|a| !matches!(a, Action::ObsScene { .. })));
    let acts = b.tick(T0 + 601.0, &p);
    assert_eq!(acts, vec![Action::ObsScene { scene: "Sleeping".into() }]);
    no_transition(&acts);
    let st = p.take("stream.state");
    assert_eq!(st.last().unwrap()["state"], "sleeping");
    let acts = turns(&mut b, T0 + 700.0, TurnsReport::Active, &p);
    assert_eq!(acts, vec![Action::ObsScene { scene: "App".into() }]);
    no_transition(&acts);
    assert_eq!(p.take("stream.state").last().unwrap()["state"], "live");
}

#[test]
fn sleep_replaces_the_automatic_afk() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    // AfkParams default quiet is 30 min: with sleep on it must not fire.
    let acts = b.tick(T0 + 100_000.0, &p);
    assert!(acts.is_empty(), "{acts:?}");
    assert!(p.take("stream.state").is_empty());
}

#[test]
fn org_turns_is_not_agent_activity_for_afk_and_needs_no_relay() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    let acts = turns(&mut b, T0 + 1.0, TurnsReport::Active, &p);
    assert!(acts.is_empty(), "no chat line, no scene change while awake: {acts:?}");
    assert!(p.take("chat.say").is_empty());
}

#[test]
fn other_agent_commands_wake_it() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    turns(&mut b, T0, TurnsReport::Idle, &p);
    b.tick(T0 + 600.0, &p);
    let (_, acts) = b.on_command(T0 + 700.0, "l", &Command::ModSlowMode { seconds: 0 }, &p);
    assert!(acts.contains(&Action::ObsScene { scene: "App".into() }), "{acts:?}");
}

#[test]
fn manual_afk_wins_over_sleep() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    turns(&mut b, T0, TurnsReport::Idle, &p);
    let (_, acts) = b.on_command(T0 + 10.0, "l", &Command::StreamAfk { on: true, reason: "test".into() }, &p);
    assert!(acts.contains(&Action::ObsScene { scene: "AFK".into() }));
    assert!(b.tick(T0 + 5000.0, &p).iter().all(|a| !matches!(a, Action::ObsScene { .. })), "no sleep during AFK");
    assert!(turns(&mut b, T0 + 5001.0, TurnsReport::Active, &p).is_empty(), "a turn doesn't end a manual AFK");
    turns(&mut b, T0 + 5002.0, TurnsReport::Idle, &p);
    let (_, acts) = b.on_command(T0 + 6000.0, "l", &Command::StreamAfk { on: false, reason: "test".into() }, &p);
    assert!(acts.contains(&Action::ObsScene { scene: "App".into() }));
    assert!(b.tick(T0 + 6599.0, &p).iter().all(|a| !matches!(a, Action::ObsScene { .. })));
    assert_eq!(b.tick(T0 + 6600.0, &p), vec![Action::ObsScene { scene: "Sleeping".into() }]);
}

#[test]
fn stream_end_stays_refused_with_sleep_on() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    let (r, acts) = b.on_command(T0 + 1.0, "l", &Command::StreamEnd { reason: "idle".into(), platforms: None }, &p);
    assert_eq!(r.unwrap_err().0, "disabled");
    assert!(!acts.contains(&Action::ObsStop));
}

#[test]
fn a_missing_sleeping_scene_turns_sleep_off_with_a_warning() {
    let p = Pub::default();
    let mut b = Bridge::new(cfg(), T0);
    b.disable_sleep("sleep_scene_missing", "OBS has no scene named Sleeping", &p);
    let w = p.take("system.warning");
    assert_eq!(w.len(), 1);
    assert_eq!(w[0]["code"], "sleep_scene_missing");
    turns(&mut b, T0, TurnsReport::Idle, &p);
    assert!(b.tick(T0 + 100_000.0, &p).is_empty());
}

#[test]
fn org_turns_with_sleep_off_is_accepted_and_does_nothing() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    assert!(turns(&mut b, T0, TurnsReport::Idle, &p).is_empty());
    assert!(b.tick(T0 + 1000.0, &p).iter().all(|a| !matches!(a, Action::ObsScene { .. })));
}
