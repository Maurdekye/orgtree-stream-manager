//! Plan §3.1: EventSub frames → connector events, and Helix request shapes.
//! Payload shapes follow https://dev.twitch.tv/docs/eventsub/ (field names [verify] at
//! the live trial; fixtures here are hand-written, not captured).

use serde_json::json;
use sm_connectors::model::InEvent;
use sm_connectors::twitch::eventsub::{default_specs, parse_frame, Frame};
use sm_connectors::twitch::helix;
use sm_core::donation::Source;
use sm_core::protocol::Platform;

fn notification(id: &str, ty: &str, event: serde_json::Value) -> String {
    json!({
        "metadata": {"message_id": id, "message_type": "notification", "message_timestamp": "2026-11-03T00:00:00Z",
                     "subscription_type": ty, "subscription_version": "1"},
        "payload": {"subscription": {"type": ty, "version": "1"}, "event": event}
    })
    .to_string()
}

#[test]
fn welcome_keepalive_reconnect_revocation() {
    let w = json!({"metadata":{"message_id":"a","message_type":"session_welcome"},
                   "payload":{"session":{"id":"S1","status":"connected","keepalive_timeout_seconds":10,"reconnect_url":null}}});
    assert_eq!(parse_frame(&w.to_string()).unwrap(), Frame::Welcome { session_id: "S1".into(), keepalive_s: 10 });
    let k = json!({"metadata":{"message_id":"b","message_type":"session_keepalive"},"payload":{}});
    assert_eq!(parse_frame(&k.to_string()).unwrap(), Frame::Keepalive);
    let r = json!({"metadata":{"message_id":"c","message_type":"session_reconnect"},
                   "payload":{"session":{"id":"S1","status":"reconnecting","reconnect_url":"wss://x.example/ws?id=2"}}});
    assert_eq!(parse_frame(&r.to_string()).unwrap(), Frame::Reconnect { url: "wss://x.example/ws?id=2".into() });
    let v = json!({"metadata":{"message_id":"d","message_type":"revocation"},
                   "payload":{"subscription":{"type":"channel.chat.message","status":"authorization_revoked"}}});
    assert_eq!(
        parse_frame(&v.to_string()).unwrap(),
        Frame::Revocation { sub_type: "channel.chat.message".into(), status: "authorization_revoked".into() }
    );
    assert!(parse_frame("not json").is_err());
}

#[test]
fn chat_message_maps_roles_and_paid_sub() {
    let f = parse_frame(&notification(
        "m1",
        "channel.chat.message",
        json!({"broadcaster_user_id":"100","chatter_user_id":"42","chatter_user_login":"some_viewer",
               "chatter_user_name":"Some_Viewer","message_id":"abc-1","message":{"text":"hello there","fragments":[]},
               "badges":[{"set_id":"subscriber","id":"6","info":"6"},{"set_id":"vip","id":"1","info":""}]}),
    ))
    .unwrap();
    match f {
        Frame::Notification { message_id, event: Some(InEvent::Chat { platform, viewer_id, login, display, text, msg_id, roles, paid_sub }) } => {
            assert_eq!(message_id, "m1");
            assert_eq!(platform, Platform::Twitch);
            assert_eq!(viewer_id, "twitch:42");
            assert_eq!((login.as_str(), display.as_str()), ("some_viewer", "Some_Viewer"));
            assert_eq!(text, "hello there");
            assert_eq!(msg_id, "tw:abc-1");
            assert_eq!(roles, vec!["subscriber".to_string(), "vip".to_string()]);
            assert!(paid_sub);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn bits_count_only_from_bits_use_not_from_cheer_chat() {
    let f = parse_frame(&notification(
        "m2",
        "channel.bits.use",
        json!({"broadcaster_user_id":"100","user_id":"42","user_login":"v","user_name":"V","bits":500,"type":"cheer",
               "message":{"text":"Cheer500 great stream","fragments":[]}}),
    ))
    .unwrap();
    match f {
        Frame::Notification { event: Some(InEvent::Donation { viewer_id, source, text, external_id, .. }), .. } => {
            assert_eq!(viewer_id.as_deref(), Some("twitch:42"));
            assert_eq!(source, Source::TwitchBits { bits: 500 });
            assert_eq!(text.as_deref(), Some("Cheer500 great stream"));
            assert_eq!(external_id, "tw-evt:m2");
        }
        other => panic!("{other:?}"),
    }
    // The chat message carrying the cheer is plain chat (no second donation).
    let chat = parse_frame(&notification(
        "m3",
        "channel.chat.message",
        json!({"chatter_user_id":"42","chatter_user_login":"v","chatter_user_name":"V","message_id":"x",
               "message":{"text":"Cheer500 great stream"},"badges":[],"cheer":{"bits":500}}),
    ))
    .unwrap();
    assert!(matches!(chat, Frame::Notification { event: Some(InEvent::Chat { .. }), .. }));
}

#[test]
fn subs_resubs_and_gifts_without_double_counting() {
    let sub = |gift: bool| {
        parse_frame(&notification(
            "s",
            "channel.subscribe",
            json!({"user_id":"7","user_login":"a","user_name":"A","tier":"2000","is_gift":gift}),
        ))
        .unwrap()
    };
    match sub(false) {
        Frame::Notification { event: Some(InEvent::Donation { source, viewer_id, .. }), .. } => {
            assert_eq!(source, Source::TwitchSub { tier: 2, count: 1 });
            assert_eq!(viewer_id.as_deref(), Some("twitch:7"));
        }
        other => panic!("{other:?}"),
    }
    // A gifted sub's recipient isn't credited: the gift event credits the gifter.
    assert!(matches!(sub(true), Frame::Notification { event: None, .. }));

    let gift = parse_frame(&notification(
        "g",
        "channel.subscription.gift",
        json!({"user_id":"9","user_login":"g","user_name":"G","total":5,"tier":"1000","is_anonymous":false}),
    ))
    .unwrap();
    match gift {
        Frame::Notification { event: Some(InEvent::Donation { source, viewer_id, .. }), .. } => {
            assert_eq!(source, Source::TwitchSub { tier: 1, count: 5 });
            assert_eq!(viewer_id.as_deref(), Some("twitch:9"));
        }
        other => panic!("{other:?}"),
    }
    let anon = parse_frame(&notification(
        "g2",
        "channel.subscription.gift",
        json!({"user_id":null,"user_login":null,"user_name":null,"total":1,"tier":"3000","is_anonymous":true}),
    ))
    .unwrap();
    assert!(matches!(anon, Frame::Notification { event: Some(InEvent::Donation { viewer_id: None, .. }), .. }));

    let resub = parse_frame(&notification(
        "r",
        "channel.subscription.message",
        json!({"user_id":"7","user_login":"a","user_name":"A","tier":"1000","message":{"text":"12 months!"},"cumulative_months":12}),
    ))
    .unwrap();
    match resub {
        Frame::Notification { event: Some(InEvent::Donation { source, text, .. }), .. } => {
            assert_eq!(source, Source::TwitchSub { tier: 1, count: 1 });
            assert_eq!(text.as_deref(), Some("12 months!"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn stream_online_offline_and_unknown_types() {
    let on = parse_frame(&notification("o", "stream.online", json!({"broadcaster_user_id":"100","type":"live"}))).unwrap();
    assert!(matches!(on, Frame::Notification { event: Some(InEvent::StreamState { live: true, .. }), .. }));
    let off = parse_frame(&notification("f", "stream.offline", json!({"broadcaster_user_id":"100"}))).unwrap();
    assert!(matches!(off, Frame::Notification { event: Some(InEvent::StreamState { live: false, .. }), .. }));
    let other = parse_frame(&notification("u", "channel.follow", json!({}))).unwrap();
    assert!(matches!(other, Frame::Notification { event: None, .. }));
}

#[test]
fn default_subscriptions_fit_the_cost_budget() {
    let specs = default_specs("100", "200");
    let types: Vec<&str> = specs.iter().map(|s| s.sub_type.as_str()).collect();
    for t in [
        "channel.chat.message",
        "channel.bits.use",
        "channel.subscribe",
        "channel.subscription.message",
        "channel.subscription.gift",
        "stream.online",
        "stream.offline",
    ] {
        assert!(types.contains(&t), "{t}");
    }
    let chat = specs.iter().find(|s| s.sub_type == "channel.chat.message").unwrap();
    assert_eq!(chat.condition, json!({"broadcaster_user_id":"100","user_id":"200"}));
    // EventSub WebSocket: total cost at most 10 [doc]; user-authorized types cost 0.
    assert!(specs.len() <= 300);
}

#[test]
fn helix_requests_have_the_documented_shapes() {
    let spec = &default_specs("100", "200")[0];
    let r = helix::create_subscription(spec, "S1");
    assert_eq!((r.method, r.path.as_str()), ("POST", "/helix/eventsub/subscriptions"));
    let b = r.body.unwrap();
    assert_eq!(b["transport"], json!({"method":"websocket","session_id":"S1"}));
    assert_eq!(b["type"], spec.sub_type.as_str());

    let r = helix::send_chat("100", "200", "hi there", Some("abc")).unwrap();
    assert_eq!((r.method, r.path.as_str()), ("POST", "/helix/chat/messages"));
    assert_eq!(r.body.unwrap(), json!({"broadcaster_id":"100","sender_id":"200","message":"hi there","reply_parent_message_id":"abc"}));

    let r = helix::ban("100", "200", "42", Some(600), "spam").unwrap();
    assert_eq!((r.method, r.path.as_str()), ("POST", "/helix/moderation/bans?broadcaster_id=100&moderator_id=200"));
    assert_eq!(r.body.unwrap(), json!({"data":{"user_id":"42","duration":600,"reason":"spam"}}));
    let r = helix::ban("100", "200", "42", None, "abuse").unwrap();
    assert_eq!(r.body.unwrap(), json!({"data":{"user_id":"42","reason":"abuse"}}));

    let r = helix::unban("100", "200", "42").unwrap();
    assert_eq!((r.method, r.path.as_str()), ("DELETE", "/helix/moderation/bans?broadcaster_id=100&moderator_id=200&user_id=42"));

    let r = helix::delete_message("100", "200", "abc-1").unwrap();
    assert_eq!(r.path, "/helix/moderation/chat?broadcaster_id=100&moderator_id=200&message_id=abc-1");
    // Never a request without message_id (that would clear the whole chat) [doc].
    assert!(helix::delete_message("100", "200", "").is_err());

    let r = helix::slow_mode("100", "200", 10).unwrap();
    assert_eq!((r.method, r.path.as_str()), ("PATCH", "/helix/chat/settings?broadcaster_id=100&moderator_id=200"));
    assert_eq!(r.body.unwrap(), json!({"slow_mode":true,"slow_mode_wait_time":10}));
    assert_eq!(helix::slow_mode("100", "200", 0).unwrap().body.unwrap(), json!({"slow_mode":false}));
    assert!(helix::slow_mode("100", "200", 2).is_err());

    // IDs go into query strings, so only plain ids are accepted.
    assert!(helix::ban("100", "200", "42&x=1", None, "r").is_err());
    assert!(helix::delete_message("100", "200", "a b").is_err());
    assert!(helix::send_chat("100", "200", &"x".repeat(501), None).is_err());
}

#[test]
fn send_chat_response_reports_drops() {
    assert_eq!(helix::parse_send_chat(200, r#"{"data":[{"message_id":"x","is_sent":true}]}"#), Ok("x".to_string()));
    let e = helix::parse_send_chat(200, r#"{"data":[{"message_id":"","is_sent":false,"drop_reason":{"code":"msg_duplicate","message":"dup"}}]}"#)
        .unwrap_err();
    assert_eq!(e, helix::HelixError::Dropped("msg_duplicate".into()));
    assert_eq!(helix::parse_send_chat(429, "{}"), Err(helix::HelixError::RateLimited));
    assert!(matches!(helix::parse_send_chat(401, "{}"), Err(helix::HelixError::Status(401))));
}
