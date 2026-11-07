//! Phase 7 trial guard (coordinator m0c328f887e68, 2026-11-05): stream.end is refused unless
//! the config explicitly enables it. Refusing is the default, so a missing key refuses too.

use std::sync::Mutex;

use serde_json::Value;
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_core::protocol::Command;

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

fn end() -> Command {
    Command::StreamEnd { reason: "test".into(), platforms: None }
}

#[test]
fn stream_end_is_refused_by_default_with_a_logged_warning() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    let (r, acts) = b.on_command(T0 + 1.0, "l", &end(), &p);
    assert_eq!(r.unwrap_err().0, "disabled");
    assert!(!acts.contains(&Action::ObsStop), "no StopStream");
    assert!(acts.iter().all(|a| !matches!(a, Action::ObsStop | Action::YoutubeEnd { .. })));
    assert!(p.take("stream.state").is_empty(), "no 'ending' state");
    let w = p.take("system.warning");
    assert_eq!(w.len(), 1);
    assert_eq!(w[0]["code"], "stream_end_disabled");
}

#[test]
fn stream_end_works_when_enabled() {
    let p = Pub::default();
    let mut c = BridgeConfig::default();
    c.stream_end_enabled = true;
    let mut b = Bridge::new(c, T0);
    let (r, acts) = b.on_command(T0 + 1.0, "l", &end(), &p);
    assert!(r.is_ok());
    assert!(acts.contains(&Action::ObsStop));
}
