//! @liaison-opus 2026-11-05 15:37Z (me6d4d70ab422): every restart reset the gate to its start, so the
//! listener re-loosened it after each one. The gate is now kept across bridge restarts within a live
//! session, like the disclosure record (sm-bin persists it in /data). Going offline ends the session.

use std::sync::Mutex;

use serde_json::Value;
use sm_bridge::{Bridge, BridgeConfig, Publisher};
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

fn open(b: &mut Bridge, now: f64, p: &Pub) {
    let cmd = Command::GateSet { min_interval_s: Some(0.0), min_donation_usd: Some(0.0), reason: "open".into(), hold_min: None };
    let (r, _) = b.on_command(now, "l", &cmd, p);
    assert!(r.is_ok(), "{r:?}");
}

fn interval(b: &Bridge) -> f64 {
    b.stats().snapshot(T0)["gate"]["min_interval_s"].as_f64().unwrap()
}

#[test]
fn a_restart_keeps_the_gate_within_the_live_session() {
    let p = Pub::default();
    let mut first = Bridge::new(BridgeConfig::default(), T0);
    first.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    open(&mut first, T0 + 10.0, &p);
    let record = first.gate_record();
    assert!(!record.is_null());
    let mut second = Bridge::new(BridgeConfig::default(), T0 + 600.0);
    // The gate follows the live session (USER m768f41d60ef1): the disclosure record carries it.
    second.restore_disclosure(&first.disclosure_record());
    second.restore_gate(&record, T0 + 600.0);
    second.tick(T0 + 600.0, &p);
    assert_eq!(interval(&second), 0.0, "the opened gate survives the restart");
}

#[test]
fn without_a_change_there_is_nothing_to_keep() {
    let b = Bridge::new(BridgeConfig::default(), T0);
    assert!(b.gate_record().is_null());
}

#[test]
fn going_offline_ends_the_session() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    open(&mut b, T0 + 10.0, &p);
    b.on_event(T0 + 100.0, InEvent::StreamState { platform: Platform::Twitch, live: false }, &p);
    assert!(b.gate_record().is_null());
}

#[test]
fn an_old_or_garbled_record_is_ignored() {
    let p = Pub::default();
    let mut first = Bridge::new(BridgeConfig::default(), T0);
    open(&mut first, T0, &p);
    let record = first.gate_record();
    let start = BridgeConfig::default().gate.start_interval_s;
    let mut later = Bridge::new(BridgeConfig::default(), T0 + 13.0 * 3600.0);
    later.restore_gate(&record, T0 + 13.0 * 3600.0);
    later.tick(T0 + 13.0 * 3600.0, &p);
    assert_eq!(interval(&later), start);
    let mut bad = Bridge::new(BridgeConfig::default(), T0);
    bad.restore_gate(&serde_json::json!({"min_interval_s": -5, "min_donation_usd": "x", "changed_at": T0}), T0);
    bad.tick(T0, &p);
    assert_eq!(interval(&bad), start);
}
