//! Moderation-bot feed (USER 2026-11-05 15:16:12Z m3e0438a5d6ca "receives every single message
//! unconditionally"): every viewer message, forwarded or held, is published once as `chat.mod`
//! with the viewer id, platform, roles and a moderator/broadcaster exemption flag.

use std::sync::Mutex;

use serde_json::Value;
use sm_bridge::{Bridge, BridgeConfig, Publisher};
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

impl Pub {
    fn take(&self, kind: &str) -> Vec<Value> {
        let mut g = self.0.lock().unwrap();
        let (hit, keep): (Vec<_>, Vec<_>) = g.drain(..).partition(|(k, _)| k == kind);
        *g = keep;
        hit.into_iter().map(|x| x.1).collect()
    }
}

fn chat(id: &str, text: &str, msg: &str, roles: &[&str]) -> InEvent {
    InEvent::Chat {
        platform: Platform::Twitch,
        viewer_id: format!("twitch:{id}"),
        login: format!("user{id}"),
        display: format!("User{id}"),
        text: text.into(),
        msg_id: format!("tw:{msg}"),
        roles: roles.iter().map(|r| r.to_string()).collect(),
        paid_sub: false,
    }
}

const T0: f64 = 1_000_000.0;

#[test]
fn every_message_forwarded_or_held_is_published_once_as_chat_mod() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0 + 1.0, chat("7", "buy followers at example dot com", "m1", &[]), &p);
    // The same viewer at once again: held by the rate limit, never forwarded as `chat`.
    b.on_event(T0 + 1.5, chat("7", "buy followers at example dot com", "m2", &[]), &p);
    let fwd = p.take("chat");
    assert_eq!(fwd.len(), 1, "only the first is forwarded to the listener");
    let all = p.take("chat.mod");
    assert_eq!(all.len(), 2, "the moderation feed sees both");
    let m = &all[1];
    assert_eq!(m["platform"], "twitch");
    assert_eq!(m["viewer"]["id"], "twitch:7");
    assert_eq!(m["viewer"]["name"], "User7");
    assert_eq!(m["msg_id"], "tw:m2");
    assert_eq!(m["text"], "buy followers at example dot com");
    assert_eq!(m["exempt"], false);
    assert!(m["status"].as_str().unwrap().starts_with("held:"), "{m}");
    assert_eq!(all[0]["status"], "forwarded");
}

#[test]
fn moderators_and_the_broadcaster_are_marked_exempt() {
    let p = Pub::default();
    let mut b = Bridge::new(BridgeConfig::default(), T0);
    b.on_event(T0, chat("1", "hi", "a", &["moderator"]), &p);
    b.on_event(T0 + 20.0, chat("2", "hi", "b", &["broadcaster", "subscriber"]), &p);
    b.on_event(T0 + 40.0, chat("3", "hi", "c", &["vip"]), &p);
    let all = p.take("chat.mod");
    let ex: Vec<bool> = all.iter().map(|m| m["exempt"].as_bool().unwrap()).collect();
    assert_eq!(ex, vec![true, true, false]);
    assert_eq!(all[1]["viewer"]["roles"], serde_json::json!(["broadcaster", "subscriber"]));
}
