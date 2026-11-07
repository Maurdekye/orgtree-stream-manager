//! The command-handler adapter between the server hub and the bridge.

use std::sync::{Arc, Mutex};

use serde_json::Value;
use sm_bin::handler::BridgeHandler;
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_core::protocol::{Command, Platform};
use sm_server::CommandHandler;

#[derive(Default)]
struct Pub(Mutex<Vec<(String, Value)>>);

impl Publisher for Pub {
    fn publish(&self, kind: &str, body: Value) -> u64 {
        let mut g = self.0.lock().unwrap();
        g.push((kind.into(), body));
        g.len() as u64
    }
}

#[tokio::test]
async fn commands_reach_the_bridge_and_actions_reach_the_executor_queue() {
    let bridge = Arc::new(Mutex::new(Bridge::new(BridgeConfig::default(), 1000.0)));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let h = BridgeHandler::new(bridge, Arc::new(|| 1000.0), tx);
    let cmd = Command::ModTimeout { viewer: "twitch:42".into(), duration_s: 60, reason: "spam".into() };
    assert_eq!(h.handle("listener-control", &cmd).unwrap_err().0, "not_ready", "no publisher yet");
    assert!(rx.try_recv().is_err());
    let p = Arc::new(Pub::default());
    h.set_publisher(p.clone());
    let r = h.handle("listener-control", &cmd).unwrap();
    assert_eq!(r["queued"], true);
    assert_eq!(
        rx.try_recv().unwrap(),
        Action::Ban { platform: Platform::Twitch, user: "42".into(), duration_s: Some(60), reason: "spam".into() }
    );
    let g = p.0.lock().unwrap();
    assert_eq!(g[0].0, "moderation");
    assert_eq!(g[0].1["by"], "agent:listener-control");
}
