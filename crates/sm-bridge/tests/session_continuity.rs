//! USER 2026-11-06 13:50:12Z m768f41d60ef1 "Fix it" (via @liaison-opus m5a47a90024b5): the saved gate,
//! and the disclosure record, follow the live stream session, not the time of their last change.
//! A session continues while the bridge keeps seeing the stream live: it saves "last seen live" about
//! once a minute, and a restart within `session_gap_s` (15 min) of it continues the same session,
//! however long the stream has run. 2026-11-06 13:47Z: a stream live for 21 h lost its gate and
//! repeated the disclosure on a restart, because both expired 12 h after their last change.

use std::sync::Mutex;

use serde_json::{json, Value};
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

const T0: f64 = 1_000_000.0;
const H: f64 = 3600.0;

fn live(b: &mut Bridge, now: f64, on: bool, p: &Pub) -> Vec<Action> {
    b.on_event(now, InEvent::StreamState { platform: Platform::Twitch, live: on }, p)
}

fn disclosures(acts: &[Action]) -> usize {
    acts.iter().filter(|a| matches!(a, Action::Chat { text, .. } if *text == BridgeConfig::default().disclosure_line)).count()
}

fn open(b: &mut Bridge, now: f64, p: &Pub) {
    let cmd = Command::GateSet { min_interval_s: Some(0.0), min_donation_usd: Some(0.0), reason: "open".into(), hold_min: None };
    let (r, _) = b.on_command(now, "l", &cmd, p);
    assert!(r.is_ok(), "{r:?}");
}

fn interval(b: &Bridge, now: f64) -> f64 {
    b.stats().snapshot(now)["gate"]["min_interval_s"].as_f64().unwrap()
}

/// A bridge that has been live since T0, with the gate opened at T0+10, ticking once a minute until `until`.
fn long_session(until: f64, p: &Pub) -> Bridge {
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    assert_eq!(disclosures(&live(&mut b, T0, true, p)), 1);
    open(&mut b, T0 + 10.0, p);
    let mut t = T0 + 60.0;
    while t <= until {
        b.tick(t, p);
        t += 60.0;
    }
    b
}

fn restart(first: &Bridge, now: f64) -> Bridge {
    let mut second = Bridge::new(BridgeConfig::default(), now);
    second.restore_disclosure(&first.disclosure_record());
    second.restore_gate(&first.gate_record(), now);
    second
}

#[test]
fn the_default_session_gap_is_fifteen_minutes() {
    assert_eq!(BridgeConfig::default().session_gap_s, 900.0);
}

#[test]
fn a_restart_after_a_21_hour_session_keeps_the_disclosure_and_the_gate() {
    let p = Pub::default();
    let first = long_session(T0 + 21.0 * H, &p);
    let now = T0 + 21.0 * H + 40.0;
    let mut second = restart(&first, now);
    assert_eq!(disclosures(&live(&mut second, now + 1.0, true, &p)), 0, "same session: no second disclosure");
    second.tick(now + 2.0, &p);
    assert_eq!(interval(&second, now + 2.0), 0.0, "same session: the opened gate is kept");
}

#[test]
fn a_bridge_down_longer_than_the_gap_starts_a_new_session() {
    let p = Pub::default();
    let first = long_session(T0 + 2.0 * H, &p);
    let now = T0 + 2.0 * H + 901.0 + 60.0;
    let mut second = restart(&first, now);
    let start = BridgeConfig::default().gate.start_interval_s;
    assert_eq!(disclosures(&live(&mut second, now + 1.0, true, &p)), 1, "a new session discloses");
    second.tick(now + 2.0, &p);
    assert_eq!(interval(&second, now + 2.0), start, "a new session starts from the start values");
}

#[test]
fn a_gate_is_not_restored_without_a_continuing_session() {
    let p = Pub::default();
    let mut first = Bridge::new(BridgeConfig::default(), T0);
    open(&mut first, T0, &p); // never live
    let mut second = restart(&first, T0 + 60.0);
    second.tick(T0 + 61.0, &p);
    assert_eq!(interval(&second, T0 + 61.0), BridgeConfig::default().gate.start_interval_s);
}

#[test]
fn last_seen_is_saved_about_once_a_minute_not_every_tick() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    live(&mut b, T0, true, &p);
    let r0 = b.disclosure_record();
    b.tick(T0 + 1.0, &p);
    b.tick(T0 + 2.0, &p);
    assert_eq!(b.disclosure_record(), r0, "no change every second (the record is written when it changes)");
    b.tick(T0 + 61.0, &p);
    assert_eq!(b.disclosure_record()["twitch"]["seen"], json!(T0 + 61.0));
    assert_eq!(b.disclosure_record()["twitch"]["start"], json!(T0));
}

#[test]
fn a_legacy_number_record_keeps_its_twelve_hour_window() {
    // The deployed image writes {"twitch": <unix s>}: within 12 h of it the session continues.
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0 + 3.0 * H);
    b.restore_disclosure(&json!({"twitch": T0}));
    assert_eq!(disclosures(&live(&mut b, T0 + 3.0 * H, true, &p)), 0);
    let mut old = Bridge::new(BridgeConfig::default(), T0 + 13.0 * H);
    old.restore_disclosure(&json!({"twitch": T0}));
    assert_eq!(disclosures(&live(&mut old, T0 + 13.0 * H, true, &p)), 1);
}
