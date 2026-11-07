//! YouTube Live spec (youtube-spec.md) §4 simulcast behaviour, §5 quota guard, §6 unban:
//! red tests 5, 6 (bridge half), 7 (bridge half), 8 and Amendment 1 (E1, E2, E4).

use std::sync::{Arc, Mutex};

use serde_json::Value;
use sm_bridge::{Action, Bridge, BridgeConfig, Publisher};
use sm_connectors::model::InEvent;
use sm_connectors::ytstate::{pacific_day, YtBans, YtQuota};
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

/// 2026-11-06 22:00Z: 14:00 Pacific on the 6th.
const TN: f64 = 1_794_002_400.0;
const DAY: f64 = 86_400.0;

fn yt_chat(roles: &[&str], channel: &str, text: &str, msg: &str) -> InEvent {
    InEvent::Chat {
        platform: Platform::Youtube,
        viewer_id: format!("youtube:{channel}"),
        login: channel.into(),
        display: channel.into(),
        text: text.into(),
        msg_id: format!("yt:{msg}"),
        roles: roles.iter().map(|r| r.to_string()).collect(),
        paid_sub: false,
    }
}

fn yt_found() -> InEvent {
    InEvent::BroadcastFound { platform: Platform::Youtube, broadcast_id: "B1".into(), live_chat_id: "L1".into(), channel_id: Some("UCself".into()) }
}

fn say(text: &str, platforms: Option<Vec<Platform>>, reply_to: Option<&str>) -> Command {
    Command::ChatSay { text: text.into(), platforms, reply_to: reply_to.map(str::to_string) }
}

fn rig() -> (Bridge, Pub, Arc<YtQuota>, Arc<YtBans>) {
    let mut b = Bridge::new(BridgeConfig::default(), TN);
    let (q, bans) = (Arc::new(YtQuota::new(10_000)), Arc::new(YtBans::default()));
    b.set_youtube_state(q.clone(), bans.clone());
    (b, Pub::default(), q, bans)
}

// Red test 5.
#[test]
fn owner_role_exempt() {
    let (mut b, p, ..) = rig();
    b.on_event(TN, yt_found(), &p);
    b.on_event(TN + 1.0, yt_chat(&["owner"], "UC1", "hello", "a"), &p);
    b.on_event(TN + 30.0, yt_chat(&["moderator"], "UC2", "hello", "b"), &p);
    b.on_event(TN + 60.0, yt_chat(&[], "UC3", "hello", "c"), &p);
    b.on_event(TN + 90.0, yt_chat(&["member"], "UC4", "hello", "d"), &p);
    let all = p.take("chat.mod");
    let exempt: Vec<(String, bool)> =
        all.iter().map(|m| (m["viewer"]["id"].as_str().unwrap().to_string(), m["exempt"].as_bool().unwrap())).collect();
    assert_eq!(
        exempt,
        vec![("youtube:UC1".into(), true), ("youtube:UC2".into(), true), ("youtube:UC3".into(), false), ("youtube:UC4".into(), false)]
    );
}

fn quota_warnings(p: &Pub) -> Vec<String> {
    p.take("system.warning")
        .into_iter()
        .filter(|w| w["code"] == "youtube_quota")
        .map(|w| {
            assert_eq!(w["component"], "youtube_quota");
            w["detail"].as_str().unwrap().to_string()
        })
        .collect()
}

// Red test 6, bridge half.
#[test]
fn quota_counter_and_guard() {
    let (mut b, p, q, _) = rig();
    b.on_event(TN, yt_found(), &p);
    q.record(TN, 8_000);
    b.tick(TN + 1.0, &p);
    assert_eq!(quota_warnings(&p), vec!["YouTube quota 80% used (8000 of 10000 units); resets at 00:00 Pacific"]);
    b.tick(TN + 2.0, &p);
    assert!(quota_warnings(&p).is_empty(), "once per crossing");
    // At 80% replies still go.
    let (r, acts) = b.on_command(TN + 20.0, "l", &say("still fine", Some(vec![Platform::Youtube]), None), &p);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Youtube, text: "still fine".into(), reply_to: None }]);

    q.record(TN + 3.0, 1_500);
    b.tick(TN + 4.0, &p);
    assert_eq!(
        quota_warnings(&p),
        vec!["YouTube quota 95% used (9500 of 10000 units); resets at 00:00 Pacific; replies on YouTube are paused"]
    );
    let snap = b.stats().snapshot(TN + 4.0);
    assert_eq!(snap["youtube"]["quota_used"], 9_500);
    assert_eq!(snap["youtube"]["quota_day"], pacific_day(TN));

    // At 95% a reply to YouTube is refused; moderation still goes.
    let (r, acts) = b.on_command(TN + 60.0, "l", &say("paused now", Some(vec![Platform::Youtube]), None), &p);
    assert_eq!(r.unwrap_err().0, "quota_low");
    assert!(acts.iter().all(|a| !matches!(a, Action::Chat { .. })), "{acts:?}");
    let (r, acts) =
        b.on_command(TN + 70.0, "l", &Command::ModTimeout { viewer: "youtube:UC9".into(), duration_s: 60, reason: "spam".into() }, &p);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(acts, vec![Action::Ban { platform: Platform::Youtube, user: "UC9".into(), duration_s: Some(60), reason: "spam".into() }]);

    // The next Pacific day: the count resets, and the reset itself says nothing.
    b.tick(TN + DAY, &p);
    assert!(quota_warnings(&p).is_empty());
    assert_eq!(b.stats().snapshot(TN + DAY)["youtube"]["quota_used"], 0);
    q.record(TN + DAY, 8_000);
    b.tick(TN + DAY + 1.0, &p);
    assert_eq!(quota_warnings(&p).len(), 1, "a new day, a new crossing");
}

// Amendment 1, E1: a reply to both platforms at 95% goes to Twitch only.
#[test]
fn a_reply_to_both_platforms_skips_youtube_at_95_percent() {
    let (mut b, p, q, _) = rig();
    b.on_event(TN, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    b.on_event(TN, yt_found(), &p);
    q.record(TN, 9_600);
    b.tick(TN + 1.0, &p);
    let (r, acts) = b.on_command(TN + 60.0, "l", &say("to everyone", None, None), &p);
    let v = r.unwrap();
    assert_eq!(v["sent_to"], serde_json::json!(["twitch"]));
    assert_eq!(v["skipped"]["youtube"], "quota_low");
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Twitch, text: "to everyone".into(), reply_to: None }]);
}

// Amendment 1, E2: the disclosure always goes out.
#[test]
fn the_disclosure_goes_out_even_at_95_percent() {
    let (mut b, p, q, _) = rig();
    q.record(TN, 9_900);
    let acts = b.on_event(TN, yt_found(), &p);
    assert_eq!(acts, vec![Action::Chat { platform: Platform::Youtube, text: BridgeConfig::default().disclosure_line, reply_to: None }]);
}

// Red test 7, bridge half.
#[test]
fn unban_uses_stored_ban_id() {
    let (mut b, p, _, bans) = rig();
    b.on_event(TN, yt_found(), &p);
    bans.put("UCx", "BAN1", None);
    let (r, acts) = b.on_command(TN + 10.0, "l", &Command::ModUnban { viewer: "youtube:UCx".into(), reason: None }, &p);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(acts, vec![Action::Unban { platform: Platform::Youtube, user: "UCx".into() }]);
    let (r, acts) = b.on_command(TN + 20.0, "l", &Command::ModUnban { viewer: "youtube:UCnone".into(), reason: None }, &p);
    let (code, detail) = r.unwrap_err();
    assert_eq!(code, "not_found");
    assert_eq!(detail, "no ban of this viewer is known (made outside the bridge or before this version)");
    assert!(acts.is_empty());
    // An expired timeout is not a ban to lift.
    bans.put("UCt", "BAN2", Some(TN + 30.0));
    let (r, _) = b.on_command(TN + 40.0, "l", &Command::ModUnban { viewer: "youtube:UCt".into(), reason: None }, &p);
    assert_eq!(r.unwrap_err().0, "not_found");
}

// Red test 8.
#[test]
fn reply_to_ignored_on_youtube() {
    let (mut b, p, ..) = rig();
    b.on_event(TN, InEvent::StreamState { platform: Platform::Twitch, live: true }, &p);
    b.on_event(TN, yt_found(), &p);
    let (r, acts) = b.on_command(TN + 60.0, "l", &say("answering you", None, Some("tw:m1")), &p);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(
        acts,
        vec![
            Action::Chat { platform: Platform::Twitch, text: "answering you".into(), reply_to: Some("m1".into()) },
            Action::Chat { platform: Platform::Youtube, text: "answering you".into(), reply_to: None },
        ]
    );
    // A YouTube message id as reply target: YouTube has no replies, so it is sent plain.
    let (r, acts) = b.on_command(TN + 200.0, "l", &say("and another thing", None, Some("yt:m2")), &p);
    assert!(r.is_ok(), "{r:?}");
    let yt: Vec<&Action> = acts.iter().filter(|a| matches!(a, Action::Chat { platform: Platform::Youtube, .. })).collect();
    assert_eq!(yt, vec![&Action::Chat { platform: Platform::Youtube, text: "and another thing".into(), reply_to: None }]);
}
