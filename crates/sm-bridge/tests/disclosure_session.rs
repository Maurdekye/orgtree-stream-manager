//! USER 2026-11-05 15:31:22Z ma80a334356d2: "dont send a "Chat replies on this stream come from ai
//! agents" message every connection". The disclosure goes out at most once per live session per
//! platform. The record survives a bridge restart (sm-bin persists it in /data); an offline event
//! ends the session; a record older than `disclosure_session_max_s` is treated as a past session.

use std::sync::Mutex;

use serde_json::Value;
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_connectors::model::InEvent;
use sm_core::protocol::Platform;

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

fn live(b: &mut Bridge, now: f64, on: bool, p: &Pub) -> Vec<Action> {
    b.on_event(now, InEvent::StreamState { platform: Platform::Twitch, live: on }, p)
}

fn disclosures(acts: &[Action]) -> usize {
    acts.iter().filter(|a| matches!(a, Action::Chat { text, .. } if *text == BridgeConfig::default().disclosure_line)).count()
}

#[test]
fn the_default_session_window_is_twelve_hours() {
    assert_eq!(BridgeConfig::default().disclosure_session_max_s, 12.0 * 3600.0);
}

#[test]
fn a_restarted_bridge_does_not_repeat_the_disclosure_in_the_same_session() {
    let p = Pub::default();
    let mut first = Bridge::new(BridgeConfig::default(), T0);
    assert_eq!(disclosures(&live(&mut first, T0, true, &p)), 1);
    let record = first.disclosure_record();
    // Restart: a new bridge restores the record, then learns the stream is live.
    let mut second = Bridge::new(BridgeConfig::default(), T0 + 600.0);
    second.restore_disclosure(&record);
    assert_eq!(disclosures(&live(&mut second, T0 + 601.0, true, &p)), 0);
}

#[test]
fn going_offline_ends_the_session_and_the_next_stream_discloses_again() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    live(&mut b, T0, true, &p);
    live(&mut b, T0 + 100.0, false, &p);
    assert_eq!(b.disclosure_record(), serde_json::json!({}), "offline clears the persisted record");
    assert_eq!(disclosures(&live(&mut b, T0 + 200.0, true, &p)), 1);
}

#[test]
fn an_old_record_counts_as_a_past_session() {
    let p = Pub::default();
    let mut first = Bridge::new(BridgeConfig::default(), T0);
    live(&mut first, T0, true, &p);
    let record = first.disclosure_record();
    let mut later = Bridge::new(BridgeConfig::default(), T0 + 13.0 * 3600.0);
    later.restore_disclosure(&record);
    assert_eq!(disclosures(&live(&mut later, T0 + 13.0 * 3600.0, true, &p)), 1);
}

#[test]
fn a_garbled_record_is_ignored() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.restore_disclosure(&serde_json::json!({"twitch": "yesterday", "nonsense": 5}));
    assert_eq!(disclosures(&live(&mut b, T0, true, &p)), 1);
}
